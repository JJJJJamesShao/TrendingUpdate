#!/usr/bin/env python3
"""
AI Nexus - Tier 1 Fetcher Test Script
=======================================
Standalone script to verify that all 8 Tier 1 RSS sources
can be fetched, parsed, and cleaned correctly.

Usage:
    cd TrendCraw
    python test_tier1.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone

from config import TIER_1_SOURCES
from fetchers import fetch_all_sources, RawArticle
from utils import log


def _age_label(dt: datetime) -> str:
    """Human-readable age label like '2h ago' or '3d ago'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    hours = delta.total_seconds() / 3600
    if hours < 1:
        return f"{int(delta.total_seconds() / 60)}m ago"
    if hours < 24:
        return f"{hours:.0f}h ago"
    return f"{delta.days}d ago"


async def run_test() -> bool:
    """Fetch all Tier 1 sources and print a diagnostic report."""
    log.info("=" * 70)
    log.info("  TIER 1 FETCHER TEST — %d sources", len(TIER_1_SOURCES))
    log.info("=" * 70)

    # Fetch
    articles = await fetch_all_sources(TIER_1_SOURCES)

    # Group results by source
    by_source: dict[str, list[RawArticle]] = {}
    for a in articles:
        by_source.setdefault(a.source_name, []).append(a)

    # ---- Per-source report ----
    log.info("")
    log.info("-" * 70)
    log.info("  PER-SOURCE RESULTS")
    log.info("-" * 70)

    total_ok = 0
    total_fail = 0

    for source in TIER_1_SOURCES:
        found = by_source.get(source.name, [])
        status = "OK" if found else "FAIL"

        if found:
            total_ok += 1
        else:
            total_fail += 1

        log.info("")
        log.info("  [%s] %s — %d articles", status, source.name, len(found))
        log.info("       URL: %s", source.url)

        if found:
            # Show latest 3 articles
            sorted_articles = sorted(found, key=lambda a: a.published_at, reverse=True)
            for i, article in enumerate(sorted_articles[:3], 1):
                snippet_len = len(article.content_snippet)
                log.info(
                    "       %d. [%s] %s",
                    i, _age_label(article.published_at), article.title[:70],
                )
                log.info(
                    "          URL: %s",
                    article.url[:80],
                )
                log.info(
                    "          Snippet: %d chars — %s",
                    snippet_len,
                    article.content_snippet[:100] + "..." if snippet_len > 100 else article.content_snippet or "(empty)",
                )
            if len(sorted_articles) > 3:
                log.info("       ... and %d more", len(sorted_articles) - 3)

    # ---- Summary ----
    log.info("")
    log.info("=" * 70)
    log.info("  SUMMARY")
    log.info("=" * 70)
    log.info("  Sources OK   : %d / %d", total_ok, len(TIER_1_SOURCES))
    log.info("  Sources FAIL : %d / %d", total_fail, len(TIER_1_SOURCES))
    log.info("  Total articles: %d", len(articles))

    # Snippet quality stats
    with_snippet = sum(1 for a in articles if len(a.content_snippet) >= 80)
    log.info(
        "  Articles with good snippet (>=80 chars): %d / %d (%.0f%%)",
        with_snippet, len(articles),
        (with_snippet / len(articles) * 100) if articles else 0,
    )

    # Date range
    if articles:
        oldest = min(a.published_at for a in articles)
        newest = max(a.published_at for a in articles)
        log.info("  Date range: %s → %s", oldest.strftime("%Y-%m-%d %H:%M"), newest.strftime("%Y-%m-%d %H:%M"))

    log.info("=" * 70)

    if total_fail > 0:
        log.warning(
            "  %d source(s) returned 0 articles. Check URLs or network.",
            total_fail,
        )

    return total_fail == 0


def main() -> int:
    log.info("Starting Tier 1 fetcher test...\n")
    try:
        success = asyncio.run(run_test())
        return 0 if success else 1
    except KeyboardInterrupt:
        log.warning("Test interrupted.")
        return 130
    except Exception as e:
        log.critical("Test crashed: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
