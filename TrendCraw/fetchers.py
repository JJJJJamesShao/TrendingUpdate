"""
AI Nexus - Source Fetchers
============================
Async fetchers for all source types: RSS, Reddit JSON, Hacker News API, ArXiv.
Uses Jina Reader API for rendering JS-heavy pages when needed.

All fetchers return a list of RawArticle dataclass instances.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from time import mktime
from typing import Any

import aiohttp
import feedparser

from config import (
    SourceConfig,
    SourceTier,
    DEFAULT_USER_AGENT,
    JINA_API_KEY,
    JINA_BASE_URL,
    JINA_SNIPPET_MIN_LENGTH,
    HN_FILTER_KEYWORDS,
    REQUEST_TIMEOUT_SECONDS,
    MAX_CONCURRENT_REQUESTS,
    FETCH_MAX_RETRIES,
    FETCH_RETRY_BACKOFF,
    CONTENT_MAX_LENGTH,
    CONTENT_FETCH_CONCURRENCY,
    CONTENT_FETCH_TIMEOUT,
)
from utils import log, strip_html, truncate, extract_article_text


# ---------------------------------------------------------------------------
# Raw Article Data Model (output of all fetchers)
# ---------------------------------------------------------------------------
@dataclass
class RawArticle:
    """Unified representation of a fetched article before processing."""
    title: str
    url: str
    source_name: str
    tier: SourceTier
    category: str
    published_at: datetime
    content_snippet: str = ""   # Brief content / description from RSS feed
    full_content: str = ""      # Full article text (fetched separately)


# ---------------------------------------------------------------------------
# Semaphore for concurrency control
# ---------------------------------------------------------------------------
_semaphore: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    return _semaphore


# ---------------------------------------------------------------------------
# Helper: Async HTTP GET with retry + exponential backoff
# ---------------------------------------------------------------------------
async def _fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    source_name: str = "",
    max_retries: int = FETCH_MAX_RETRIES,
    backoff_base: float = FETCH_RETRY_BACKOFF,
) -> str | None:
    """Perform an HTTP GET with automatic retry on transient failures.

    Returns the response body as text, or None on total failure.
    Retries on: 429 (rate-limit), 5xx (server error), timeouts, connection errors.
    """
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    merged_headers = {"User-Agent": DEFAULT_USER_AGENT}
    if headers:
        merged_headers.update(headers)

    tag = f"[{source_name}]" if source_name else ""

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, timeout=timeout, headers=merged_headers) as resp:
                if resp.status == 200:
                    return await resp.text()

                # Retryable status codes
                if resp.status in (429, 500, 502, 503, 504):
                    wait = backoff_base ** attempt
                    log.warning(
                        "%s HTTP %d (attempt %d/%d) — retrying in %.1fs",
                        tag, resp.status, attempt, max_retries, wait,
                    )
                    await asyncio.sleep(wait)
                    continue

                # Non-retryable HTTP error (e.g. 403 Forbidden, 404 Not Found)
                log.warning("%s HTTP %d — non-retryable, skipping", tag, resp.status)
                return None

        except asyncio.TimeoutError:
            wait = backoff_base ** attempt
            log.warning(
                "%s Timeout (attempt %d/%d) — retrying in %.1fs",
                tag, attempt, max_retries, wait,
            )
            await asyncio.sleep(wait)

        except aiohttp.ClientError as e:
            wait = backoff_base ** attempt
            log.warning(
                "%s Connection error (attempt %d/%d): %s — retrying in %.1fs",
                tag, attempt, max_retries, e, wait,
            )
            await asyncio.sleep(wait)

    log.error("%s All %d attempts failed for %s", tag, max_retries, url[:100])
    return None


# ---------------------------------------------------------------------------
# Helper: Parse datetime from various feed formats
# ---------------------------------------------------------------------------
def _parse_published_date(entry: Any) -> datetime:
    """Try to extract a timezone-aware published datetime from a feed entry.

    Handles:
    - time.struct_time from feedparser (published_parsed / updated_parsed)
    - RFC 2822 date strings (published / updated)
    - ISO 8601 date strings
    Falls back to current UTC time.
    """
    # Approach 1: feedparser pre-parsed struct_time
    for key in ("published_parsed", "updated_parsed"):
        parsed = entry.get(key)
        if parsed:
            try:
                return datetime.fromtimestamp(mktime(parsed), tz=timezone.utc)
            except Exception:
                continue

    # Approach 2: Raw date strings
    for key in ("published", "updated"):
        raw = entry.get(key, "")
        if not raw:
            continue
        # Try RFC 2822 (most RSS feeds)
        try:
            return parsedate_to_datetime(raw).astimezone(timezone.utc)
        except Exception:
            pass
        # Try ISO 8601 (some Atom feeds)
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

    # Fallback
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Helper: Extract best snippet from a feed entry
# ---------------------------------------------------------------------------
def _extract_snippet(entry: Any) -> str:
    """Extract and clean the best content snippet from an RSS/Atom entry.

    Tries multiple fields, strips HTML, and truncates.
    """
    # Try content:encoded first (often has the richest text)
    if hasattr(entry, "content") and entry.content:
        raw = entry.content[0].get("value", "")
    else:
        # Fall back to summary / description
        raw = entry.get("summary", "") or entry.get("description", "")

    cleaned = strip_html(raw)
    return truncate(cleaned, max_length=500)


# ---------------------------------------------------------------------------
# Fetcher: RSS / Atom Feeds (Production — used by ALL Tier 1 sources)
# ---------------------------------------------------------------------------
async def fetch_rss(
    session: aiohttp.ClientSession,
    source: SourceConfig,
    *,
    enrich_via_jina: bool = True,
) -> list[RawArticle]:
    """Fetch and parse an RSS/Atom feed with retry, HTML cleaning, and
    optional Jina Reader enrichment for articles with thin content.

    Features:
    - Retries with exponential backoff (429 / 5xx / timeout).
    - Custom User-Agent to avoid bot blocking.
    - HTML tag stripping + entity decoding on snippets.
    - feedparser `bozo` flag check for malformed feeds.
    - Jina Reader fallback when direct RSS fetch fails (403/404).
    - Optional Jina snippet enrichment for thin content.

    Args:
        session:  Shared aiohttp session.
        source:   SourceConfig for this feed.
        enrich_via_jina:  If True, articles with short snippets
                          will be enriched via Jina Reader API.

    Returns:
        List of RawArticle instances.
    """
    sem = _get_semaphore()
    async with sem:
        body = await _fetch_with_retry(
            session,
            source.url,
            headers=source.headers if source.headers else None,
            source_name=source.name,
        )

    # ---- Jina fallback: if direct fetch fails, try via Jina Reader ----
    if body is None:
        if JINA_API_KEY:
            return await _fetch_rss_via_jina(session, source)
        log.warning("[%s] Direct fetch failed and no JINA_API_KEY set — skipping", source.name)
        return []

    # ---- Parse the feed ----
    articles = _parse_feed_body(body, source)

    log.info(
        "[%s] Parsed %d entries from direct RSS fetch",
        source.name, len(articles),
    )

    # ---- Optional: Enrich thin snippets via Jina ----
    if enrich_via_jina and JINA_API_KEY and articles:
        thin = [a for a in articles if len(a.content_snippet) < JINA_SNIPPET_MIN_LENGTH]
        if thin:
            log.info(
                "[%s] %d/%d articles have thin snippets — enriching via Jina",
                source.name, len(thin), len(articles),
            )
            jina_tasks = [
                _enrich_snippet_via_jina(session, article)
                for article in thin
            ]
            await asyncio.gather(*jina_tasks)

    return articles


# ---------------------------------------------------------------------------
# Helper: Parse a raw RSS/Atom body into RawArticle list
# ---------------------------------------------------------------------------
def _parse_feed_body(body: str, source: SourceConfig) -> list[RawArticle]:
    """Parse RSS/Atom XML body text into a list of RawArticle instances.

    Shared by both direct fetch and Jina fallback paths.
    """
    feed = feedparser.parse(body)

    # Check for malformed feed (bozo flag)
    if feed.bozo:
        exc = feed.get("bozo_exception")
        if not feed.entries:
            log.warning(
                "[%s] Malformed feed with no entries (bozo: %s) — skipping",
                source.name, exc,
            )
            return []
        log.debug(
            "[%s] Feed has bozo flag (%s) but %d entries recovered",
            source.name, exc, len(feed.entries),
        )

    articles: list[RawArticle] = []

    for entry in feed.entries:
        title = strip_html(entry.get("title", "")).strip()
        link = entry.get("link", "").strip()
        if not title or not link:
            continue

        snippet = _extract_snippet(entry)

        articles.append(RawArticle(
            title=title,
            url=link,
            source_name=source.name,
            tier=source.tier,
            category=source.category,
            published_at=_parse_published_date(entry),
            content_snippet=snippet,
        ))

    feed_title = feed.feed.get("title", "untitled")[:50] if feed.feed else "untitled"
    log.debug("[%s] feedparser extracted %d entries (feed: %s)", source.name, len(articles), feed_title)
    return articles


async def _enrich_snippet_via_jina(
    session: aiohttp.ClientSession,
    article: RawArticle,
) -> None:
    """Fetch full article content via Jina Reader and update the snippet in-place."""
    content = await fetch_via_jina(session, article.url)
    if content and len(content) > len(article.content_snippet):
        article.content_snippet = truncate(content, max_length=800)
        log.debug(
            "[%s] Jina enriched '%s' → %d chars",
            article.source_name, article.title[:40], len(article.content_snippet),
        )


# ---------------------------------------------------------------------------
# Fallback: RSS via Jina Reader (for Cloudflare-blocked feeds)
# ---------------------------------------------------------------------------
async def _fetch_rss_via_jina(
    session: aiohttp.ClientSession,
    source: SourceConfig,
) -> list[RawArticle]:
    """Fallback strategy: fetch the RSS XML through Jina Reader.

    Some corporate blogs (e.g., OpenAI) serve their RSS behind Cloudflare,
    returning 403 to non-browser clients. Jina renders the page server-side,
    bypassing the protection. If the response is valid XML/RSS, feedparser
    can parse it directly.

    If the Jina response is Markdown (i.e., blog listing), we skip —
    this fallback only works when Jina returns the raw RSS XML.
    """
    log.info("[%s] Attempting RSS fetch via Jina Reader fallback...", source.name)

    jina_url = f"{JINA_BASE_URL}{source.url}"
    headers: dict[str, str] = {
        "Accept": "application/xml, application/rss+xml, text/xml",
        "User-Agent": DEFAULT_USER_AGENT,
    }
    if JINA_API_KEY:
        headers["Authorization"] = f"Bearer {JINA_API_KEY}"

    sem = _get_semaphore()
    async with sem:
        body = await _fetch_with_retry(
            session,
            jina_url,
            headers=headers,
            source_name=f"{source.name}/Jina",
            max_retries=2,
        )

    if not body:
        log.warning("[%s] Jina fallback also failed", source.name)
        return []

    # Try to parse as RSS — if Jina returned rendered Markdown, feedparser
    # will produce a bozo result with zero entries, which we handle gracefully
    feed = feedparser.parse(body)

    if not feed.entries:
        log.warning(
            "[%s] Jina fallback returned non-RSS content (%d chars) — skipping",
            source.name, len(body),
        )
        return []

    articles: list[RawArticle] = []
    for entry in feed.entries:
        title = strip_html(entry.get("title", "")).strip()
        link = entry.get("link", "").strip()
        if not title or not link:
            continue

        snippet = _extract_snippet(entry)
        articles.append(RawArticle(
            title=title,
            url=link,
            source_name=source.name,
            tier=source.tier,
            category=source.category,
            published_at=_parse_published_date(entry),
            content_snippet=snippet,
        ))

    log.info("[%s] Jina fallback recovered %d entries", source.name, len(articles))
    return articles


# ---------------------------------------------------------------------------
# Fetcher: Reddit JSON
# ---------------------------------------------------------------------------
async def fetch_reddit(session: aiohttp.ClientSession, source: SourceConfig) -> list[RawArticle]:
    """Fetch new posts from a Reddit subreddit via JSON API."""
    sem = _get_semaphore()
    async with sem:
        try:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
            headers = {**source.headers} if source.headers else {}
            async with session.get(source.url, timeout=timeout, headers=headers) as resp:
                if resp.status != 200:
                    log.warning("[%s] HTTP %d - skipping", source.name, resp.status)
                    return []
                data = await resp.json()
        except Exception as e:
            log.error("[%s] Fetch failed: %s", source.name, e)
            return []

    articles: list[RawArticle] = []
    posts = data.get("data", {}).get("children", [])

    for post in posts:
        pdata = post.get("data", {})
        title = pdata.get("title", "").strip()
        url = pdata.get("url", "").strip()
        permalink = pdata.get("permalink", "")
        selftext = pdata.get("selftext", "")[:500]

        if not title:
            continue
        # Use permalink as URL if the post links to itself
        if not url or "reddit.com" in url:
            url = f"https://www.reddit.com{permalink}"

        created_utc = pdata.get("created_utc", 0)
        published = datetime.fromtimestamp(created_utc, tz=timezone.utc) if created_utc else datetime.now(timezone.utc)

        articles.append(RawArticle(
            title=title,
            url=url,
            source_name=source.name,
            tier=source.tier,
            category=source.category,
            published_at=published,
            content_snippet=selftext,
        ))

    log.info("[%s] Fetched %d posts from Reddit", source.name, len(articles))
    return articles


# ---------------------------------------------------------------------------
# Fetcher: Hacker News API (filtered by keywords)
# ---------------------------------------------------------------------------
async def fetch_hackernews(session: aiohttp.ClientSession, source: SourceConfig) -> list[RawArticle]:
    """Fetch top/new stories from HN API, filtered by AI-related keywords."""
    sem = _get_semaphore()
    base = source.url  # https://hacker-news.firebaseio.com/v0

    async with sem:
        try:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
            async with session.get(f"{base}/newstories.json", timeout=timeout) as resp:
                if resp.status != 200:
                    log.warning("[%s] HTTP %d fetching story IDs", source.name, resp.status)
                    return []
                story_ids: list[int] = await resp.json()
        except Exception as e:
            log.error("[%s] Failed to fetch story IDs: %s", source.name, e)
            return []

    # Only check the most recent 100 stories to stay within limits
    story_ids = story_ids[:100]

    async def _fetch_item(sid: int) -> dict[str, Any] | None:
        async with _get_semaphore():
            try:
                timeout = aiohttp.ClientTimeout(total=15)
                async with session.get(f"{base}/item/{sid}.json", timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except Exception:
                pass
            return None

    # Fetch story details in parallel
    tasks = [_fetch_item(sid) for sid in story_ids]
    results = await asyncio.gather(*tasks)

    articles: list[RawArticle] = []
    keywords_lower = [kw.lower() for kw in HN_FILTER_KEYWORDS]

    for item in results:
        if not item or item.get("type") != "story":
            continue
        title = item.get("title", "")
        url = item.get("url", "")

        # Filter: must match at least one keyword
        title_lower = title.lower()
        if not any(kw in title_lower for kw in keywords_lower):
            continue

        if not url:
            url = f"https://news.ycombinator.com/item?id={item.get('id', '')}"

        created = item.get("time", 0)
        published = datetime.fromtimestamp(created, tz=timezone.utc) if created else datetime.now(timezone.utc)

        articles.append(RawArticle(
            title=title,
            url=url,
            source_name=source.name,
            tier=source.tier,
            category=source.category,
            published_at=published,
            content_snippet="",
        ))

    log.info("[%s] Fetched %d AI-related stories from HN", source.name, len(articles))
    return articles


# ---------------------------------------------------------------------------
# Fetcher: ArXiv (via arxiv library)
# ---------------------------------------------------------------------------
async def fetch_arxiv(source: SourceConfig) -> list[RawArticle]:
    """Fetch recent papers from ArXiv for a given category (e.g., cs.AI, cs.CL).

    Uses the `arxiv` library (sync) wrapped in asyncio.to_thread.
    """
    import arxiv

    category = source.url  # e.g., "cs.AI"

    def _query() -> list[RawArticle]:
        search = arxiv.Search(
            query=f"cat:{category}",
            max_results=50,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending,
        )

        articles: list[RawArticle] = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=48)

        for result in search.results():
            pub_date = result.published.replace(tzinfo=timezone.utc) if result.published.tzinfo is None else result.published
            if pub_date < cutoff:
                continue  # Skip papers older than 48h

            articles.append(RawArticle(
                title=result.title.strip(),
                url=result.entry_id,
                source_name=source.name,
                tier=source.tier,
                category=source.category,
                published_at=pub_date,
                content_snippet=result.summary[:500] if result.summary else "",
            ))
        return articles

    try:
        articles = await asyncio.to_thread(_query)
        log.info("[%s] Fetched %d recent papers", source.name, len(articles))
        return articles
    except Exception as e:
        log.error("[%s] ArXiv fetch failed: %s", source.name, e)
        return []


# ---------------------------------------------------------------------------
# Fetcher: Jina Reader (for JS-rendered pages)
# ---------------------------------------------------------------------------
async def fetch_via_jina(session: aiohttp.ClientSession, url: str) -> str:
    """Fetch a URL's content as clean Markdown via Jina Reader API.

    Used for:
    - Pages that need JS rendering.
    - Enriching thin RSS snippets with full article text.

    Returns clean plaintext/markdown, or empty string on failure.
    """
    jina_url = f"{JINA_BASE_URL}{url}"
    headers: dict[str, str] = {
        "Accept": "text/plain",
        "User-Agent": DEFAULT_USER_AGENT,
    }
    if JINA_API_KEY:
        headers["Authorization"] = f"Bearer {JINA_API_KEY}"

    sem = _get_semaphore()
    async with sem:
        body = await _fetch_with_retry(
            session,
            jina_url,
            headers=headers,
            source_name="Jina",
            max_retries=2,  # Jina is external; don't retry too many times
        )

    if body:
        log.debug("Jina fetched %d chars from %s", len(body), url[:60])
        return strip_html(body)
    return ""


# ---------------------------------------------------------------------------
# Dispatcher: Route source to the correct fetcher
# ---------------------------------------------------------------------------
async def fetch_source(session: aiohttp.ClientSession, source: SourceConfig) -> list[RawArticle]:
    """Dispatch a source config to the appropriate fetcher."""
    match source.source_type:
        case "rss":
            return await fetch_rss(session, source)
        case "reddit_json":
            return await fetch_reddit(session, source)
        case "hn_api":
            return await fetch_hackernews(session, source)
        case "arxiv":
            return await fetch_arxiv(source)
        case _:
            log.warning("Unknown source type '%s' for %s", source.source_type, source.name)
            return []


# ---------------------------------------------------------------------------
# Top-level: Fetch ALL sources concurrently
# ---------------------------------------------------------------------------
async def fetch_all_sources(sources: list[SourceConfig]) -> list[RawArticle]:
    """Fetch articles from all configured sources in parallel.

    Creates a shared aiohttp session with proper defaults (User-Agent,
    connection limits) and dispatches all sources concurrently.

    Returns a flat list of all RawArticle instances.
    """
    log.info("=" * 60)
    log.info("Starting parallel fetch of %d sources...", len(sources))
    log.info("=" * 60)

    # Configure connection pool and default headers
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2, ttl_dns_cache=300)
    default_headers = {"User-Agent": DEFAULT_USER_AGENT}

    async with aiohttp.ClientSession(connector=connector, headers=default_headers) as session:
        tasks = [fetch_source(session, src) for src in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_articles: list[RawArticle] = []
    errors = 0

    for source, result in zip(sources, results):
        if isinstance(result, Exception):
            log.error("[%s] Unexpected error: %s", source.name, result)
            errors += 1
        else:
            all_articles.extend(result)

    log.info("=" * 60)
    log.info(
        "Fetch complete: %d articles from %d sources (%d errors)",
        len(all_articles), len(sources), errors,
    )
    log.info("=" * 60)
    return all_articles


# ---------------------------------------------------------------------------
# Content Fetching: Fetch full article text for selected articles
# ---------------------------------------------------------------------------
async def fetch_article_content(
    session: aiohttp.ClientSession,
    article: RawArticle,
) -> str:
    """Fetch the full article text for a single article.

    Strategy (in order of preference):
    1. Jina Reader — best quality, handles JS rendering, returns clean Markdown.
    2. Direct HTTP — fetch raw HTML, extract text with enhanced cleaner.
    3. Fall back to existing content_snippet if both fail.

    Returns cleaned article text (truncated to CONTENT_MAX_LENGTH).
    """
    url = article.url
    tag = f"[Content:{article.source_name}]"
    MIN_CONTENT_CHARS = 100  # minimum chars to consider content valid

    # ── Strategy 1: Jina Reader (preferred, but requires API key for reliability) ──
    if JINA_API_KEY:
        jina_text = await _fetch_content_via_jina(session, url, tag)
        if jina_text and len(jina_text) > MIN_CONTENT_CHARS:
            return truncate(jina_text, max_length=CONTENT_MAX_LENGTH, suffix="")

    # ── Strategy 2: Direct HTTP + HTML extraction ──
    direct_text = await _fetch_content_direct(session, url, tag)
    if direct_text and len(direct_text) > MIN_CONTENT_CHARS:
        return truncate(direct_text, max_length=CONTENT_MAX_LENGTH, suffix="")

    # ── Strategy 3: Jina without API key as last resort (rate-limited) ──
    if not JINA_API_KEY:
        jina_text = await _fetch_content_via_jina(session, url, tag)
        if jina_text and len(jina_text) > MIN_CONTENT_CHARS:
            return truncate(jina_text, max_length=CONTENT_MAX_LENGTH, suffix="")

    # ── Fallback: use existing snippet ──
    if article.content_snippet:
        log.debug("%s Using RSS snippet as content (%d chars)", tag, len(article.content_snippet))
        return article.content_snippet

    log.debug("%s No content obtained for %s", tag, url[:80])
    return ""


async def _fetch_content_via_jina(
    session: aiohttp.ClientSession,
    url: str,
    tag: str,
) -> str:
    """Fetch article content via Jina Reader API (returns clean Markdown)."""
    jina_url = f"{JINA_BASE_URL}{url}"
    headers: dict[str, str] = {
        "Accept": "text/plain",
        "User-Agent": DEFAULT_USER_AGENT,
    }
    if JINA_API_KEY:
        headers["Authorization"] = f"Bearer {JINA_API_KEY}"

    try:
        timeout = aiohttp.ClientTimeout(total=CONTENT_FETCH_TIMEOUT)
        async with session.get(jina_url, headers=headers, timeout=timeout) as resp:
            if resp.status == 200:
                text = await resp.text()
                cleaned = strip_html(text)
                log.debug("%s Jina fetched %d chars", tag, len(cleaned))
                return cleaned
            else:
                log.debug("%s Jina HTTP %d", tag, resp.status)
    except Exception as e:
        log.debug("%s Jina failed: %s", tag, str(e)[:80])
    return ""


async def _fetch_content_direct(
    session: aiohttp.ClientSession,
    url: str,
    tag: str,
) -> str:
    """Fetch article content via direct HTTP and extract text from HTML."""
    try:
        timeout = aiohttp.ClientTimeout(total=CONTENT_FETCH_TIMEOUT)
        headers = {"User-Agent": DEFAULT_USER_AGENT}
        async with session.get(url, headers=headers, timeout=timeout) as resp:
            if resp.status == 200:
                content_type = resp.headers.get("Content-Type", "")
                # Only process HTML pages
                if "html" in content_type.lower() or "text" in content_type.lower():
                    raw_html = await resp.text()
                    cleaned = extract_article_text(raw_html)
                    log.debug("%s Direct fetch %d chars → %d cleaned", tag, len(raw_html), len(cleaned))
                    return cleaned
                else:
                    log.debug("%s Non-HTML content type: %s", tag, content_type[:50])
            else:
                log.debug("%s Direct HTTP %d", tag, resp.status)
    except Exception as e:
        log.debug("%s Direct fetch failed: %s", tag, str(e)[:80])
    return ""


async def fetch_contents_batch(
    articles: list[RawArticle],
    session: aiohttp.ClientSession | None = None,
) -> None:
    """Fetch full article content for a batch of articles (in parallel).

    Updates each article's `full_content` field in-place.
    """
    if not articles:
        return

    own_session = session is None
    if own_session:
        connector = aiohttp.TCPConnector(limit=CONTENT_FETCH_CONCURRENCY)
        session = aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": DEFAULT_USER_AGENT},
        )

    sem = asyncio.Semaphore(CONTENT_FETCH_CONCURRENCY)

    async def _fetch_one(article: RawArticle) -> None:
        async with sem:
            content = await fetch_article_content(session, article)
            article.full_content = content

    log.info("Fetching full content for %d articles...", len(articles))
    tasks = [_fetch_one(a) for a in articles]
    await asyncio.gather(*tasks, return_exceptions=True)

    fetched = sum(1 for a in articles if a.full_content)
    log.info("Content fetch complete: %d/%d articles have full content", fetched, len(articles))

    if fetched < len(articles) and not JINA_API_KEY:
        log.warning(
            "  %d articles missing content. Set JINA_API_KEY for better coverage "
            "(renders JS pages, bypasses bot protection).",
            len(articles) - fetched,
        )

    if own_session:
        await session.close()


# ---------------------------------------------------------------------------
# Convenience: Fetch only Tier 1 sources
# ---------------------------------------------------------------------------
async def fetch_tier1_sources() -> list[RawArticle]:
    """Fetch articles from Tier 1 (Corporate Labs & Official) sources only.

    Useful for testing and incremental development.
    """
    from config import TIER_1_SOURCES
    return await fetch_all_sources(TIER_1_SOURCES)
