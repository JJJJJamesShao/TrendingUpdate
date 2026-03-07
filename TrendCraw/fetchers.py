"""
AI Nexus - Source Fetchers
============================
Async fetchers for all source types: RSS, Reddit JSON, Hacker News API, ArXiv.
Multi-strategy content extraction: html2text, trafilatura, LLM fallback.

All fetchers return a list of RawArticle dataclass instances.
"""

from __future__ import annotations

import asyncio
import re
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
    QWEN_API_KEY,
    HN_FILTER_KEYWORDS,
    HN_MIN_SCORE,
    HN_MIN_COMMENTS,
    HN_TOP_N,
    HN_CONTENT_MAX_LENGTH,
    HN_AI_SCORE_THRESHOLD,
    HN_EXCLUDED_DOMAINS,
    REQUEST_TIMEOUT_SECONDS,
    MAX_CONCURRENT_REQUESTS,
    FETCH_MAX_RETRIES,
    FETCH_RETRY_BACKOFF,
    CONTENT_MAX_LENGTH,
    CONTENT_FETCH_CONCURRENCY,
    CONTENT_FETCH_TIMEOUT,
    LLM_EXTRACT_MAX_INPUT,
)
from utils import log, strip_html, truncate

# ---------------------------------------------------------------------------
# Optional dependencies for content extraction (graceful degradation)
# ---------------------------------------------------------------------------
try:
    import trafilatura
    _HAS_TRAFILATURA = True
except ImportError:
    _HAS_TRAFILATURA = False

try:
    import html2text as _html2text_mod
    _HAS_HTML2TEXT = True
except ImportError:
    _HAS_HTML2TEXT = False


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
async def fetch_hackernews(
    session: aiohttp.ClientSession,
    source: SourceConfig,
    *,
    apply_stage3_llm: bool = False,
) -> list[RawArticle]:
    """
    Fetch HN stories with Three-Stage Funnel filtering.

    Stage 1: Metadata & Rule Filter (zero-cost, ultra-fast)
      - Score threshold: HN score > 100
      - Comment threshold: comments > 30
      - Keyword match: title contains AI-related keywords (50+ terms)
      - Domain filter: exclude Twitter/X, YouTube, Reddit, Facebook, etc.
      - Scope: Only top HN_TOP_N (500) hot stories

    Stage 2: Content Extraction (low-cost)
      - Fetch article body content
      - Strip HTML tags, scripts, styles
      - Limit content length (~8000 chars ≈ 2000 tokens)
      - Filter out articles that fail to extract

    Stage 3: AI Semantic Scoring (core filter) — optional, controlled by caller
      - AI scoring standard:
        - 8-10: Hard-core AI open-source projects, high-quality architecture discussions,
                major model releases, technical innovations
        - 5-7:  Valuable tech sharing, tutorials, medium-quality open-source projects
        - 0-4:  Ordinary AI business news, hype, marketing content, low-quality reposts
      - Threshold: Only keep articles with AI score >= 6
      - Output: Chinese summary, technical tags, scoring rationale

    Args:
        session:          Shared aiohttp session
        source:           SourceConfig for HN
        apply_stage3_llm: If True, apply Stage 3 AI scoring filter (requires QWEN_API_KEY)

    Returns:
        List of filtered RawArticle instances
    """
    import re
    from urllib.parse import urlparse

    sem = _get_semaphore()
    base = source.url  # https://hacker-news.firebaseio.com/v0

    # =========================================================================
    # Stage 1: Metadata & Rule Filter
    # =========================================================================
    log.info("[%s] Stage 1: Fetching top %d HN stories...", source.name, HN_TOP_N)

    async with sem:
        try:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
            async with session.get(f"{base}/topstories.json", timeout=timeout) as resp:
                if resp.status != 200:
                    log.warning("[%s] HTTP %d fetching story IDs", source.name, resp.status)
                    return []
                story_ids: list[int] = await resp.json()
        except Exception as e:
            log.error("[%s] Failed to fetch story IDs: %s", source.name, e)
            return []

    # Only check top N stories
    story_ids = story_ids[:HN_TOP_N]

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

    # Pre-compile keyword patterns (case-insensitive)
    keywords_lower = [kw.lower() for kw in HN_FILTER_KEYWORDS]

    # Domain exclusion helper
    def _is_excluded_domain(url: str) -> bool:
        if not url:
            return False  # HN posts without URL are discussion threads, skip later
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove www. prefix for matching
            domain = domain.replace("www.", "")
            return any(excluded in domain for excluded in HN_EXCLUDED_DOMAINS)
        except Exception:
            return False

    articles: list[RawArticle] = []
    stage1_passed = 0

    for item in results:
        if not item or item.get("type") != "story":
            continue

        title = item.get("title", "")
        url = item.get("url", "")
        score = item.get("score", 0) or 0
        descendants = item.get("descendants", 0) or 0  # Comment count

        # ── Stage 1 Filters ────────────────────────────────────────────────
        # Filter 1: Score threshold
        if score < HN_MIN_SCORE:
            continue

        # Filter 2: Comment threshold
        if descendants < HN_MIN_COMMENTS:
            continue

        # Filter 3: Skip discussion threads (Ask HN, Tell HN)
        title_lower = title.lower()
        if title_lower.startswith("ask hn:") or title_lower.startswith("tell hn:"):
            continue

        # Filter 4: Keyword match
        if not any(kw in title_lower for kw in keywords_lower):
            continue

        # Filter 5: Domain exclusion (social media, low-quality)
        if _is_excluded_domain(url):
            continue

        stage1_passed += 1

        # Build URL (HN posts without URL link to discussion)
        if not url:
            url = f"https://news.ycombinator.com/item?id={item.get('id', '')}"

        created = item.get("time", 0)
        published = datetime.fromtimestamp(created, tz=timezone.utc) if created else datetime.now(timezone.utc)

        # Store metadata for Stage 3
        article = RawArticle(
            title=title,
            url=url,
            source_name=source.name,
            tier=source.tier,
            category=source.category,
            published_at=published,
            content_snippet="",
            full_content="",
        )
        # Attach HN metadata for later stages
        article._hn_score = score
        article._hn_comments = descendants
        articles.append(article)

    log.info(
        "[%s] Stage 1 complete: %d → %d stories (score>%d, comments>%d, keywords, domain filter)",
        source.name, len(results), len(articles), HN_MIN_SCORE, HN_MIN_COMMENTS,
    )

    if not articles:
        return []

    # =========================================================================
    # Stage 2: Content Extraction
    # =========================================================================
    log.info("[%s] Stage 2: Extracting content from %d articles...", source.name, len(articles))

    # Fetch content with limited concurrency
    content_sem = asyncio.Semaphore(CONTENT_FETCH_CONCURRENCY)

    async def _fetch_content(article: RawArticle) -> None:
        """Fetch and extract article content."""
        # Skip HN discussion URLs (no external content)
        if "news.ycombinator.com" in article.url:
            article.full_content = f"HN Discussion: {article.title}"
            return

        async with content_sem:
            try:
                timeout = aiohttp.ClientTimeout(total=CONTENT_FETCH_TIMEOUT)
                headers = {"User-Agent": DEFAULT_USER_AGENT}
                async with session.get(article.url, timeout=timeout, headers=headers) as resp:
                    if resp.status != 200:
                        return
                    html = await resp.text()

                    # Extract text using multi-strategy pipeline
                    # Strategy 1: Heuristic HTML isolation + html2text
                    content = _extract_article_text(html)

                    # Strategy 2: trafilatura fallback
                    if not content or len(content) < 200:
                        if _HAS_TRAFILATURA:
                            content = trafilatura.extract(html) or ""

                    # Limit content length
                    if content and len(content) > HN_CONTENT_MAX_LENGTH:
                        content = content[:HN_CONTENT_MAX_LENGTH]

                    article.full_content = content or ""

            except Exception as e:
                log.debug("[%s] Failed to fetch content for %s: %s", source.name, article.url[:50], e)

    # Fetch content for all articles
    await asyncio.gather(*[_fetch_content(a) for a in articles])

    # Filter: must have extracted content (>100 chars)
    before_stage2 = len(articles)
    articles = [a for a in articles if a.full_content and len(a.full_content.strip()) > 100]
    stage2_dropped = before_stage2 - len(articles)

    log.info(
        "[%s] Stage 2 complete: %d → %d articles (extracted %d, dropped %d)",
        source.name, before_stage2, len(articles),
        sum(1 for a in articles if a.full_content), stage2_dropped,
    )

    if not articles:
        return []

    # =========================================================================
    # Stage 3: AI Semantic Scoring (optional)
    # =========================================================================
    if apply_stage3_llm and QWEN_API_KEY:
        log.info("[%s] Stage 3: AI semantic scoring with Qwen LLM...", source.name)
        articles = await _hn_ai_scoring_batch(articles, session)
        log.info(
            "[%s] Stage 3 complete: %d articles retained (AI score >= %d)",
            source.name, len(articles), HN_AI_SCORE_THRESHOLD,
        )

    return articles


# ---------------------------------------------------------------------------
# HN Stage 3: AI Semantic Scoring with Qwen LLM
# ---------------------------------------------------------------------------
_HN_SCORING_PROMPT = """You are an expert AI/ML technical reviewer evaluating Hacker News articles.

TASK:
Score each article based on technical depth and innovation. Be critical — most AI news is hype.

SCORING RUBRIC:
- 8-10 points: Hard-core AI open-source projects, high-quality architecture discussions,
               major model releases, genuine technical innovations
- 5-7 points:  Valuable tech sharing, tutorials, medium-quality open-source projects
- 0-4 points:  Ordinary AI business news, hype, marketing, low-quality reposts

THRESHOLD: Only articles scoring >= 6 should be kept.

ARTICLE TO EVALUATE:
Title: {title}
HN Score: {hn_score} | Comments: {hn_comments}
Content:
{content}

Respond with ONLY valid JSON (no markdown, no reasoning):
{{
  "ai_score": 7,
  "is_worthy": true,
  "reason_zh": "简短的中文评分理由",
  "tech_tags": ["LLM", "开源项目", "架构设计"],
  "summary_zh": "2-3 句中文摘要"
}}

If score < 6, set is_worthy: false."""


async def _hn_ai_scoring_batch(
    articles: list[RawArticle],
    session: aiohttp.ClientSession,
) -> list[RawArticle]:
    """Stage 3: AI semantic scoring batch processing."""
    from TrendCraw.llm import chat_completion, parse_json_response

    sem = asyncio.Semaphore(3)  # Limit concurrency
    results: list[RawArticle] = []

    async def _score_one(article: RawArticle) -> RawArticle | None:
        async with sem:
            content = article.full_content[:6000]  # Limit input
            prompt = _HN_SCORING_PROMPT.format(
                title=article.title,
                hn_score=getattr(article, "_hn_score", 0),
                hn_comments=getattr(article, "_hn_comments", 0),
                content=content,
            )

            response = await chat_completion(
                prompt,
                system_prompt="You are a strict AI/ML technical reviewer. Respond with JSON only.",
                temperature=0.1,
                max_tokens=500,
                session=session,
            )

            result = parse_json_response(response)

            if result and result.get("is_worthy", False) and result.get("ai_score", 0) >= HN_AI_SCORE_THRESHOLD:
                # Store scoring metadata
                article._ai_score = result.get("ai_score", 0)
                article._ai_reason = result.get("reason_zh", "")
                article._ai_tags = result.get("tech_tags", [])
                article._ai_summary = result.get("summary_zh", "")
                return article
            else:
                log.debug(
                    "[HN] Filtered '%s' (score=%s)",
                    article.title[:40],
                    result.get("ai_score", "parse_failed") if result else "parse_failed",
                )
                return None

    scored = await asyncio.gather(*[_score_one(a) for a in articles])
    return [a for a in scored if a is not None]


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

            abstract = (result.summary or "").strip()
            # Build a rich full_content from ArXiv metadata so the LLM
            # can produce high-quality research summaries.
            authors = ", ".join(a.name for a in (result.authors or [])[:10])
            categories = ", ".join(result.categories or [])
            full_text_parts = []
            if authors:
                full_text_parts.append(f"Authors: {authors}")
            if categories:
                full_text_parts.append(f"Categories: {categories}")
            if result.pdf_url:
                full_text_parts.append(f"PDF: {result.pdf_url}")
            full_text_parts.append(f"\nAbstract:\n{abstract}")
            full_content = "\n".join(full_text_parts)

            articles.append(RawArticle(
                title=result.title.strip(),
                url=result.entry_id,
                source_name=source.name,
                tier=source.tier,
                category=source.category,
                published_at=pub_date,
                content_snippet=abstract[:500] if abstract else "",
                full_content=full_content,
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
async def fetch_source(
    session: aiohttp.ClientSession,
    source: SourceConfig,
    *,
    hn_apply_stage3: bool = False,
) -> list[RawArticle]:
    """Dispatch a source config to the appropriate fetcher.

    Args:
        session:          Shared aiohttp session
        source:           SourceConfig to fetch
        hn_apply_stage3:  If True, apply HN Stage 3 AI scoring (requires QWEN_API_KEY)
    """
    match source.source_type:
        case "rss":
            return await fetch_rss(session, source)
        case "reddit_json":
            return await fetch_reddit(session, source)
        case "hn_api":
            return await fetch_hackernews(session, source, apply_stage3_llm=hn_apply_stage3)
        case "arxiv":
            return await fetch_arxiv(source)
        case _:
            log.warning("Unknown source type '%s' for %s", source.source_type, source.name)
            return []


# ---------------------------------------------------------------------------
# Top-level: Fetch ALL sources concurrently
# ---------------------------------------------------------------------------
async def fetch_all_sources(
    sources: list[SourceConfig],
    *,
    hn_apply_stage3: bool = False,
) -> list[RawArticle]:
    """Fetch articles from all configured sources in parallel.

    Creates a shared aiohttp session with proper defaults (User-Agent,
    connection limits) and dispatches all sources concurrently.

    Args:
        sources:          List of SourceConfig to fetch
        hn_apply_stage3:  If True, apply HN Stage 3 AI scoring (requires QWEN_API_KEY)

    Returns a flat list of all RawArticle instances.
    """
    log.info("=" * 60)
    log.info("Starting parallel fetch of %d sources...", len(sources))
    log.info("=" * 60)

    # Configure connection pool and default headers
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2, ttl_dns_cache=300)
    default_headers = {"User-Agent": DEFAULT_USER_AGENT}

    async with aiohttp.ClientSession(connector=connector, headers=default_headers) as session:
        tasks = [fetch_source(session, src, hn_apply_stage3=hn_apply_stage3) for src in sources]
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


# ===========================================================================
# Content Extraction — Multi-strategy pipeline (zero-cost priority)
# ===========================================================================
# Strategy order:
#   S1  Heuristic HTML isolation + html2text  →  Markdown with images  [FREE]
#   S2  trafilatura smart extraction          →  plain text            [FREE]
#   S3  html2text on cleaned full page        →  noisy Markdown        [FREE]
#   S4  LLM extraction via MiniMax            →  Markdown              [token cost]
#   --  RSS snippet fallback
# ===========================================================================

# ── Regex patterns for HTML cleaning ──────────────────────────────────────
_BOILERPLATE_RE = re.compile(
    r"<(script|style|noscript|iframe|svg|form)[^>]*>.*?</\1>",
    re.DOTALL | re.IGNORECASE,
)
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
_NAV_BLOCKS_RE = re.compile(
    r"<(nav|footer|header|aside)\b[^>]*>.*?</\1>",
    re.DOTALL | re.IGNORECASE,
)

# Boilerplate text patterns to strip from Markdown output
_SKIP_LINE_PATTERNS = [
    "skip to content", "skip to main", "cookie policy", "accept all cookies",
    "privacy policy", "terms of service", "terms of use",
    "sign up", "sign in", "log in", "register",
    "subscribe to", "newsletter signup", "get updates",
    "share this", "share on", "follow us on",
    "copyright ©", "all rights reserved",
    "toggle navigation", "menu", "search for:",
    "text settings", "story text", "subscribers only",
    "advertisement", "sponsored content", "read more articles",
]

# Common CSS class names for the main article content block
_CONTENT_CLASSES = [
    "entry-content", "post-content", "article-content", "article-body",
    "blog-post-content", "main-content", "story-body", "post-body",
    "rich-text", "prose", "markdown-body", "content-body",
]


# ── HTML cleaning helpers ─────────────────────────────────────────────────
def _clean_html_for_extraction(raw_html: str) -> str:
    """Remove non-content HTML elements while preserving article body + images."""
    html = _HTML_COMMENT_RE.sub("", raw_html)
    html = _BOILERPLATE_RE.sub("", html)
    html = _NAV_BLOCKS_RE.sub("", html)
    return html


def _isolate_main_content(raw_html: str) -> str:
    """Use heuristics to locate the main article content block.

    Checks (in priority order):
      1. <article> tag  — standard semantic HTML5
      2. <main> tag     — broader semantic block
      3. role="main"    — ARIA role
      4. Common CSS class patterns (WordPress, Ghost, Medium, etc.)

    Returns the inner HTML of the matched block, or empty string.
    """
    html_lower = raw_html.lower()

    # ── Priority 1: <article> ──
    idx = html_lower.find("<article")
    if idx != -1:
        end = html_lower.rfind("</article>")
        if end > idx and (end - idx) > 200:
            return raw_html[idx : end + len("</article>")]

    # ── Priority 2: <main> ──
    idx = html_lower.find("<main")
    if idx != -1:
        end = html_lower.rfind("</main>")
        if end > idx and (end - idx) > 200:
            return raw_html[idx : end + len("</main>")]

    # ── Priority 3: role="main" ──
    m = re.search(r'<(\w+)[^>]*\brole=["\']main["\'][^>]*>', raw_html, re.IGNORECASE)
    if m:
        tag_name = m.group(1)
        end_tag = f"</{tag_name}>"
        end_idx = raw_html.lower().rfind(end_tag.lower(), m.end())
        if end_idx > m.start() and (end_idx - m.start()) > 200:
            return raw_html[m.start() : end_idx + len(end_tag)]

    # ── Priority 4: common content class names ──
    cls_pattern = "|".join(re.escape(c) for c in _CONTENT_CLASSES)
    class_re = re.compile(
        rf'<(div|section)[^>]*\bclass="[^"]*\b(?:{cls_pattern})\b[^"]*"[^>]*>',
        re.IGNORECASE,
    )
    m = class_re.search(raw_html)
    if m:
        tag_name = m.group(1).lower()
        # Walk forward counting nesting depth to find matching close tag
        start_pos = m.start()
        rest = raw_html[start_pos:]
        open_tag = f"<{tag_name}"
        close_tag = f"</{tag_name}>"
        depth, i = 0, 0
        while i < len(rest):
            chunk_low = rest[i:].lower()
            if chunk_low.startswith(open_tag):
                depth += 1
                i += len(open_tag)
            elif chunk_low.startswith(close_tag):
                depth -= 1
                if depth == 0:
                    block = rest[: i + len(close_tag)]
                    if len(block) > 200:
                        return block
                    break
                i += len(close_tag)
            else:
                i += 1

    return ""


# ── Markdown conversion helpers ───────────────────────────────────────────
def _html_to_markdown(html_content: str, base_url: str = "") -> str:
    """Convert HTML to clean Markdown via html2text.

    Preserves: images ![alt](url), links [text](url), headings, lists.
    """
    if not _HAS_HTML2TEXT or not html_content:
        return ""

    h = _html2text_mod.HTML2Text()
    h.body_width = 0              # don't hard-wrap lines
    h.ignore_links = False
    h.ignore_images = False       # keep images!
    h.ignore_emphasis = False
    h.skip_internal_links = True
    h.unicode_snob = True
    h.protect_links = True
    h.wrap_links = False
    h.single_line_break = False
    if base_url:
        h.baseurl = base_url

    markdown = h.handle(html_content)
    return _clean_markdown_output(markdown)


def _clean_markdown_output(text: str) -> str:
    """Post-process html2text output: collapse whitespace, strip boilerplate.

    IMPORTANT: Lines containing Markdown images ``![`` or headings ``#``
    are always preserved — the boilerplate filter only applies to short,
    non-content lines (navigation items, footers, cookie banners, etc.).
    """
    if not text:
        return ""

    lines = text.split("\n")
    cleaned: list[str] = []
    blank_count = 0

    for line in lines:
        stripped = line.strip()

        # ── Always preserve lines with Markdown images or headings ──
        if "![" in stripped or stripped.startswith("#"):
            cleaned.append(line)
            blank_count = 0
            continue

        # ── Boilerplate filter — only for SHORT lines (likely nav/UI) ──
        # Long lines (>120 chars) are almost certainly article paragraphs,
        # so we never strip them even if they happen to contain a keyword.
        if stripped and len(stripped) < 120:
            lower = stripped.lower()
            if any(p in lower for p in _SKIP_LINE_PATTERNS):
                continue

        # Collapse consecutive blank lines (max 2)
        if not stripped:
            blank_count += 1
            if blank_count <= 2:
                cleaned.append("")
            continue
        blank_count = 0

        cleaned.append(line)

    result = "\n".join(cleaned).strip()
    # Final pass: collapse 4+ newlines → 3
    result = re.sub(r"\n{4,}", "\n\n\n", result)
    return result


# ── Extraction strategy wrappers ──────────────────────────────────────────
def _extract_with_trafilatura(raw_html: str, url: str) -> str:
    """Extract article content using trafilatura (smart heuristic engine)."""
    if not _HAS_TRAFILATURA:
        return ""
    try:
        result = trafilatura.extract(
            raw_html,
            url=url,
            include_images=True,
            include_links=True,
            include_formatting=True,
            favor_recall=True,
        )
        return result or ""
    except Exception as e:
        log.debug("trafilatura extraction failed: %s", str(e)[:80])
        return ""


async def _extract_with_llm(
    session: aiohttp.ClientSession,
    raw_html: str,
    title: str,
    url: str,
) -> str:
    """Use MiniMax LLM to extract article content from HTML.

    Expensive fallback — only invoked when rule-based methods fail.
    Outputs clean Markdown with image references preserved.
    """
    from llm import chat_completion

    if not QWEN_API_KEY:
        return ""

    # Clean & truncate to stay within token budget
    cleaned = _clean_html_for_extraction(raw_html)
    if len(cleaned) > LLM_EXTRACT_MAX_INPUT:
        cleaned = cleaned[:LLM_EXTRACT_MAX_INPUT]

    prompt = f"""Extract the main article content from the HTML below and output clean Markdown.

ARTICLE TITLE: {title}
SOURCE URL: {url}

HTML:
{cleaned}

─── EXTRACTION RULES ───
INCLUDE:
- The complete main article body text
- Section headings → ## or ### Markdown syntax
- Images → ![description](full_image_url)
  • Use the alt text as description (or "image" if none)
  • Keep the full src URL exactly as-is
- Important hyperlinks → [link text](url)
- Code blocks → wrap in triple-backtick fences
- Lists → Markdown bullet or numbered lists

EXCLUDE:
- Navigation menus, breadcrumbs
- Page headers, footers, sidebars
- Advertisements, banners, social sharing buttons
- Comment sections, "Related articles"
- Cookie notices, popups, author bios

OUTPUT FORMAT:
- Clean Markdown ONLY — start directly with the article content
- Preserve the original text faithfully (do NOT summarize or rephrase)
- If no meaningful article content exists, respond with exactly: NO_CONTENT"""

    response = await chat_completion(
        prompt,
        system_prompt=(
            "You are a precision web content extractor. "
            "Output clean Markdown only. No commentary, no preamble."
        ),
        temperature=0,
        max_tokens=3000,
        session=session,
    )

    if response and response.strip() != "NO_CONTENT":
        return response.strip()
    return ""


# ── Raw HTML fetcher ──────────────────────────────────────────────────────
async def _fetch_raw_html(
    session: aiohttp.ClientSession,
    url: str,
    tag: str,
) -> str:
    """Fetch raw HTML from a URL via direct HTTP GET.

    Returns the full HTML string, or empty string on failure.
    """
    try:
        timeout = aiohttp.ClientTimeout(total=CONTENT_FETCH_TIMEOUT)
        headers = {"User-Agent": DEFAULT_USER_AGENT}
        async with session.get(url, headers=headers, timeout=timeout) as resp:
            if resp.status == 200:
                ct = resp.headers.get("Content-Type", "")
                if "html" in ct.lower() or "text" in ct.lower():
                    text = await resp.text()
                    log.debug("%s Fetched %d chars raw HTML", tag, len(text))
                    return text
                log.debug("%s Non-HTML content-type: %s", tag, ct[:50])
            else:
                log.debug("%s HTTP %d", tag, resp.status)
    except Exception as e:
        log.debug("%s HTML fetch failed: %s", tag, str(e)[:80])
    return ""


# ── Main content extraction pipeline ─────────────────────────────────────
async def fetch_article_content(
    session: aiohttp.ClientSession,
    article: RawArticle,
) -> str:
    """Fetch & extract the full article text for a single article.

    Zero-cost pipeline (strategies tried in order):
      S1: Heuristic HTML isolation + html2text  → Markdown with images  [FREE]
      S2: trafilatura smart extraction           → plain text            [FREE]
      S3: html2text on cleaned full page         → noisy Markdown        [FREE]
      S4: LLM extraction via MiniMax             → Markdown              [token cost]
      --: RSS snippet fallback

    Returns cleaned article content (truncated to CONTENT_MAX_LENGTH).
    """
    url = article.url
    tag = f"[Content:{article.source_name}]"
    MIN_CHARS = 100  # minimum chars to accept extracted content

    # ── Step 0: Fetch raw HTML ──
    raw_html = await _fetch_raw_html(session, url, tag)

    if not raw_html:
        # Direct fetch failed — use snippet if available
        if article.content_snippet:
            return article.content_snippet
        log.debug("%s No HTML fetched and no snippet for %s", tag, url[:80])
        return ""

    # ── S1 + S2: Run both, pick the best ──────────────────────────
    # S1: heuristic isolation + html2text  (preserves images as ![alt](url))
    s1_md = ""
    main_block = _isolate_main_content(raw_html)
    if main_block:
        cleaned_block = _clean_html_for_extraction(main_block)
        s1_md = _html_to_markdown(cleaned_block, base_url=url)

    # S2: trafilatura (better content detection, but loses image URLs)
    s2_text = _extract_with_trafilatura(raw_html, url)

    # Image-aware comparison:
    #   - S1 (html2text) preserves images as ![alt](url)
    #   - S2 (trafilatura) typically strips image URLs
    #   - If S1 has images, strongly prefer it (require S2 to be 3x longer)
    #   - If S1 has no images, prefer S2 if it has 1.5x more content
    best = ""
    best_label = ""

    if s1_md and len(s1_md) > MIN_CHARS:
        best, best_label = s1_md, "S1"

    if s2_text and len(s2_text) > MIN_CHARS:
        s1_has_images = "![" in best if best else False
        if s1_has_images:
            # S1 has images — only override if S2 has dramatically more text
            if len(s2_text) > len(best) * 3:
                best, best_label = s2_text, "S2"
        else:
            # S1 has no images — prefer S2 if it has moderately more content
            if not best or len(s2_text) > len(best) * 1.5:
                best, best_label = s2_text, "S2"

    if best:
        log.debug("%s %s: %d chars (images: %s)", tag, best_label, len(best), "![" in best)
        return truncate(best, max_length=CONTENT_MAX_LENGTH, suffix="")

    # ── S3: html2text on full cleaned page (noisier but comprehensive) ──
    cleaned_full = _clean_html_for_extraction(raw_html)
    md_full = _html_to_markdown(cleaned_full, base_url=url)
    if md_full and len(md_full) > MIN_CHARS:
        log.debug("%s S3 (full-page html2text): %d chars", tag, len(md_full))
        return truncate(md_full, max_length=CONTENT_MAX_LENGTH, suffix="")

    # ── S4: LLM extraction (expensive last resort) ──
    llm_text = await _extract_with_llm(session, raw_html, article.title, url)
    if llm_text and len(llm_text) > MIN_CHARS:
        log.debug("%s S4 (LLM extraction): %d chars", tag, len(llm_text))
        return truncate(llm_text, max_length=CONTENT_MAX_LENGTH, suffix="")

    # ── Fallback: RSS snippet ──
    if article.content_snippet:
        log.debug("%s Fallback (RSS snippet): %d chars", tag, len(article.content_snippet))
        return article.content_snippet

    log.debug("%s No content obtained for %s", tag, url[:80])
    return ""


# ── Batch content fetcher ─────────────────────────────────────────────────
async def fetch_contents_batch(
    articles: list[RawArticle],
    session: aiohttp.ClientSession | None = None,
) -> None:
    """Fetch full article content for a batch of articles in parallel.

    Updates each article's ``full_content`` field in-place.
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
        if article.full_content:  # Already has content (e.g., ArXiv abstract)
            return
        async with sem:
            content = await fetch_article_content(session, article)
            article.full_content = content

    log.info("Fetching full content for %d articles...", len(articles))
    tasks = [_fetch_one(a) for a in articles]
    await asyncio.gather(*tasks, return_exceptions=True)

    fetched = sum(1 for a in articles if a.full_content)
    log.info("Content fetch complete: %d/%d articles have full content", fetched, len(articles))

    if fetched < len(articles):
        log.info(
            "  %d articles without content (JS-rendered pages, paywalls, etc.)",
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
