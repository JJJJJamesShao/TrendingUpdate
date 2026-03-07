"""
TrendCraw — AI News Aggregation Component
============================================
One-call interface for the root orchestrator.

Usage from the main agent (TrendingUpdate root):

    import asyncio
    from Database import news_db
    from TrendCraw import crawl_and_process

    # 1. Get dedup data from DB
    known_urls   = news_db.get_recent_urls(hours=24)
    known_titles = news_db.get_recent_titles(hours=24)

    # 2. Crawl + deduplicate + summarize (one call)
    articles = asyncio.run(crawl_and_process(known_urls, known_titles))

    # 3. Persist to DB
    rows = [a.to_db_dict() for a in articles]
    inserted = news_db.insert_news_items(rows)
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import aiohttp

from config import (
    get_sources,
    QWEN_API_KEY,
    DEFAULT_USER_AGENT,
    MAX_CONCURRENT_REQUESTS,
    ARTICLE_MAX_AGE_HOURS,
)
from fetchers import fetch_all_sources, fetch_contents_batch, RawArticle
from processors import (
    ProcessedArticle,
    dedup_by_url,
    dedup_by_fuzzy_title,
    cluster_articles_with_llm,
    enrich_all,
)
from utils import log

# Re-export for external consumers
__all__ = ["crawl_and_process", "ProcessedArticle"]


async def crawl_and_process(
    known_urls: set[str] | None = None,
    known_titles: list[str] | None = None,
    *,
    skip_llm_cluster: bool = False,
    skip_enrichment: bool = False,
    enrich_limit: int = 0,
    max_age_hours: int = 0,
    include_daily_feeds: bool = False,
) -> list[ProcessedArticle]:
    """One-call interface: Fetch → Deduplicate → Summarize → Return.

    This is the single entry point the root agent should call.
    TrendCraw does NOT touch the database — the caller handles persistence.

    Args:
        known_urls:       Set of original_url strings from DB for exact dedup.
                          Pass None or empty set to skip URL dedup.
        known_titles:     List of title strings from DB for fuzzy dedup.
                          Pass None or empty list to skip fuzzy dedup.
        skip_llm_cluster: If True, skip the LLM semantic clustering step.
        skip_enrichment:  If True, skip LLM summarization (return unprocessed articles).
        enrich_limit:     If > 0, only enrich the first N articles (saves API quota).
        max_age_hours:    Override freshness filter (0 = use config default).
        include_daily_feeds: If True, also fetch 92 OPML blog feeds (for daily run).

    Returns:
        List of ProcessedArticle ready for database insertion via .to_db_dict().
    """
    start = time.time()
    sources = get_sources(include_daily_feeds=include_daily_feeds)

    log.info("=" * 60)
    log.info("  TrendCraw Pipeline — Starting")
    log.info("  Sources: %d (%s) | URL dedup pool: %d | Title dedup pool: %d",
             len(sources),
             "hot+daily" if include_daily_feeds else "hot only",
             len(known_urls) if known_urls else 0,
             len(known_titles) if known_titles else 0)
    log.info("=" * 60)

    # ── Step 1: Fetch all sources ──────────────────────────────
    log.info(">>> STEP 1: Fetching from %d sources...", len(sources))
    raw_articles = await fetch_all_sources(sources)
    log.info("Fetched %d raw articles", len(raw_articles))

    if not raw_articles:
        log.warning("No articles fetched. Pipeline ending early.")
        return []

    # ── Step 1b: Freshness filter — discard old articles ──────
    # ArXiv papers are exempt: they publish in daily batches (~24-36h ago)
    # and the ArXiv fetcher already applies its own 48h cutoff.
    age_hours = max_age_hours if max_age_hours > 0 else ARTICLE_MAX_AGE_HOURS
    cutoff = datetime.now(timezone.utc) - timedelta(hours=age_hours)
    before_filter = len(raw_articles)
    raw_articles = [
        a for a in raw_articles
        if a.published_at >= cutoff or "arxiv" in a.source_name.lower()
    ]
    log.info(
        "Freshness filter (max %dh, ArXiv exempt): %d → %d (discarded %d old articles)",
        age_hours, before_filter, len(raw_articles),
        before_filter - len(raw_articles),
    )

    if not raw_articles:
        log.warning("No recent articles after freshness filter.")
        return []

    # ── Step 1c: Sort by newest first ─────────────────────────
    raw_articles.sort(key=lambda a: a.published_at, reverse=True)
    log.info("Sorted by publish time (newest first): %s … %s",
             raw_articles[0].published_at.strftime("%Y-%m-%d"),
             raw_articles[-1].published_at.strftime("%Y-%m-%d"))

    # ── Step 2a: Exact URL dedup ───────────────────────────────
    articles = raw_articles
    if known_urls:
        log.info(">>> STEP 2a: URL deduplication...")
        articles = dedup_by_url(articles, known_urls)
        if not articles:
            log.info("All articles already in DB. Done.")
            return []

    # ── Step 2b: Fuzzy title dedup (rapidfuzz) ─────────────────
    log.info(">>> STEP 2b: Fuzzy title deduplication (rapidfuzz)...")
    articles = dedup_by_fuzzy_title(articles, known_titles or [])
    if not articles:
        log.info("All articles filtered by fuzzy match. Done.")
        return []

    # ── Step 2c: LLM semantic clustering (optional) ────────────
    if not skip_llm_cluster and QWEN_API_KEY and len(articles) > 1:
        log.info(">>> STEP 2c: LLM semantic clustering...")
        connector = aiohttp.TCPConnector(limit=5)
        async with aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": DEFAULT_USER_AGENT},
        ) as session:
            articles = await cluster_articles_with_llm(articles, session)
        if not articles:
            log.info("No unique events after clustering. Done.")
            return []
    elif skip_llm_cluster:
        log.info(">>> STEP 2c: LLM clustering SKIPPED (flag)")
    elif not QWEN_API_KEY:
        log.info(">>> STEP 2c: LLM clustering SKIPPED (no QWEN_API_KEY)")

    # ── Step 2d: Fetch full article content ─────────────────────
    # Fetch content for ALL articles first. ArXiv papers already have
    # full_content (abstract) and are automatically skipped.
    log.info(">>> STEP 2d: Fetching full article content...")
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    ) as session:
        await fetch_contents_batch(articles, session)

    # ── Step 2e: Content quality gate ─────────────────────────
    # Quality-first policy: ONLY publish articles where we have the
    # original text to produce a proper summary.
    before_content_gate = len(articles)
    articles = [a for a in articles if a.full_content and len(a.full_content.strip()) > 100]
    content_dropped = before_content_gate - len(articles)
    if content_dropped:
        log.info(
            "Content gate: dropped %d/%d articles without original text",
            content_dropped, before_content_gate,
        )

    if not articles:
        log.warning("No articles with full content. Pipeline ending early.")
        return []

    # ── Step 2f: Apply enrich limit (AFTER content gate) ──────
    # Now we only count articles that actually have content.
    if enrich_limit > 0 and len(articles) > enrich_limit:
        log.info("Limiting to %d / %d articles with content (enrich_limit=%d)",
                 enrich_limit, len(articles), enrich_limit)
        articles = articles[:enrich_limit]

    # ── Step 3: Editorial screening + structured summary ──────
    # The LLM now does BOTH filtering (is_newsworthy?) and content
    # generation (structured summary) in one call.  Non-newsworthy
    # articles (Ask HN, Reddit Q&A, off-topic) are rejected here.
    if not skip_enrichment and QWEN_API_KEY:
        log.info(">>> STEP 3: Editorial screening + content generation (Qwen LLM)...")
        connector = aiohttp.TCPConnector(limit=5)
        async with aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": DEFAULT_USER_AGENT},
        ) as session:
            results = await enrich_all(articles, session)
    else:
        reason = "flag" if skip_enrichment else "no QWEN_API_KEY"
        log.info(">>> STEP 3: Enrichment SKIPPED (%s) — returning raw articles", reason)
        results = [
            ProcessedArticle(
                title=a.title,
                original_url=a.url,
                source_name=a.source_name,
                published_at=a.published_at,
                category=a.category,
                summary=a.title,
                content=a.full_content or a.content_snippet or a.title,
                is_processed=False,
            )
            for a in articles
        ]

    # ── Step 4: Quality gate — no empty content allowed ───────
    before_gate = len(results)
    results = [r for r in results if r.content and len(r.content.strip()) > 20]
    empty_removed = before_gate - len(results)
    if empty_removed:
        log.info("Quality gate: removed %d articles with empty content", empty_removed)

    # ── Summary ────────────────────────────────────────────────
    elapsed = time.time() - start
    processed_count = sum(1 for r in results if r.is_processed)

    log.info("=" * 60)
    log.info("  TrendCraw Pipeline — Complete")
    log.info("  Raw fetched  : %d", len(raw_articles))
    log.info("  After dedup  : %d", len(articles))
    log.info("  Published    : %d", len(results))
    log.info("  Enriched     : %d / %d", processed_count, len(results))
    log.info("  Elapsed      : %.1fs", elapsed)
    log.info("=" * 60)

    return results
