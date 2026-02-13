#!/usr/bin/env python3
"""
AI Nexus — Main Pipeline Orchestrator
========================================
Entry point for both GitHub Actions and local testing.

Modes:
  python main.py                  Full pipeline: DB dedup → Crawl → Enrich → Persist
  python main.py --dry-run        Local test:    Crawl → Dedup (no DB, no LLM, no persist)
  python main.py --dry-run --llm  Local test:    Crawl → Dedup → LLM enrich (no DB, no persist)
  python main.py --limit 5        Limit LLM enrichment to N articles (saves API quota)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import os
import time

# ---------------------------------------------------------------------------
# Path setup — TrendCraw uses direct imports (from config import ...)
# so its directory must be on sys.path
# ---------------------------------------------------------------------------
_root = os.path.dirname(os.path.abspath(__file__))
_trendcraw = os.path.join(_root, "TrendCraw")
sys.path.insert(0, _root)
sys.path.insert(0, _trendcraw)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AI Nexus — AI News Aggregation Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                  # Full pipeline (GitHub Actions)
  python main.py --dry-run        # Fetch only, no DB/LLM/persist
  python main.py --dry-run --llm  # Fetch + LLM enrich, no DB/persist
  python main.py --limit 3        # Enrich max 3 articles
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Local test mode: skip DB read/write, skip LLM by default",
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Enable LLM enrichment in --dry-run mode (requires MINIMAX_API_KEY)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Limit LLM enrichment to N articles (0 = no limit)",
    )
    parser.add_argument(
        "--skip-cluster",
        action="store_true",
        help="Skip LLM semantic clustering step",
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=0,
        metavar="HOURS",
        help="Only process articles from the last N hours (0 = use config default: 3h)",
    )
    return parser.parse_args()


def run_full_pipeline(args: argparse.Namespace) -> int:
    """Full pipeline for GitHub Actions: DB dedup → Crawl → Enrich → Persist."""
    from Database import news_db
    from TrendCraw import crawl_and_process
    from utils import log

    log.info("=" * 60)
    log.info("  AI Nexus — FULL PIPELINE (production)")
    log.info("=" * 60)

    # Step 0: Ensure table exists
    log.info(">>> Ensuring news_items table exists...")
    try:
        news_db.ensure_table_exists()
        log.info("Table check passed.")
    except Exception as e:
        log.critical("Database connection failed: %s", e)
        return 1

    # Step 1: Load dedup data from DB
    log.info(">>> Loading dedup data from Supabase...")
    try:
        known_urls = news_db.get_recent_urls(hours=24)
        known_titles = news_db.get_recent_titles(hours=24)
        log.info("Loaded %d URLs and %d titles for dedup", len(known_urls), len(known_titles))
    except Exception as e:
        log.error("Failed to load dedup data: %s — proceeding without dedup", e)
        known_urls = set()
        known_titles = []

    # Step 2: Crawl + process
    articles = asyncio.run(crawl_and_process(
        known_urls=known_urls,
        known_titles=known_titles,
        skip_llm_cluster=args.skip_cluster,
        enrich_limit=args.limit,
        max_age_hours=args.max_age,
    ))

    if not articles:
        log.info("No new articles to insert. Pipeline done.")
        return 0

    # Step 3: Sort newest first, apply limit if set
    articles.sort(key=lambda a: a.published_at, reverse=True)
    if args.limit > 0 and len(articles) > args.limit:
        log.info("Limiting output to %d / %d articles", args.limit, len(articles))
        articles = articles[:args.limit]

    # Step 4: Persist to DB
    log.info(">>> Persisting %d articles to Supabase...", len(articles))
    try:
        rows = [a.to_db_dict() for a in articles]
        inserted = news_db.insert_news_items(rows)
        log.info("Inserted %d new rows into news_items", inserted)
    except Exception as e:
        log.error("Database insert failed: %s", e)
        return 1

    return 0


def run_dry_run(args: argparse.Namespace) -> int:
    """Local test mode: Crawl + dedup (no DB), optionally enrich with LLM."""
    from TrendCraw import crawl_and_process
    from utils import log

    enable_llm = args.llm
    skip_enrich = not enable_llm

    log.info("=" * 60)
    log.info("  AI Nexus — DRY RUN (local test)")
    log.info("  LLM enrich: %s | Limit: %s | DB: SKIPPED",
             "ON" if enable_llm else "OFF",
             args.limit if args.limit > 0 else "none")
    log.info("=" * 60)

    # Crawl with no DB dedup data
    articles = asyncio.run(crawl_and_process(
        known_urls=None,
        known_titles=None,
        skip_llm_cluster=args.skip_cluster or not enable_llm,
        skip_enrichment=skip_enrich,
        enrich_limit=args.limit,
        max_age_hours=args.max_age,
    ))

    if not articles:
        log.info("No articles returned.")
        return 0

    # Sort by publish time (newest first) and apply limit
    articles.sort(key=lambda a: a.published_at, reverse=True)
    if args.limit > 0 and len(articles) > args.limit:
        articles = articles[:args.limit]

    # Print results
    log.info("")
    log.info("-" * 60)
    log.info("  RESULTS (%d articles)", len(articles))
    log.info("-" * 60)

    for i, a in enumerate(articles[:20], 1):  # Show max 20
        processed_tag = "[OK]" if a.is_processed else "[RAW]"
        log.info("")
        log.info("  %d. %s %s", i, processed_tag, a.title[:70])
        log.info("     Source: %s | Category: %s", a.source_name, a.category)
        log.info("     URL: %s", a.original_url[:80])
        if a.content:
            log.info("     Content: %d chars", len(a.content))
        if a.summary:
            # Show first 150 chars of summary
            log.info("     Summary: %s", a.summary[:150] + ("..." if len(a.summary) > 150 else ""))

    if len(articles) > 20:
        log.info("")
        log.info("  ... and %d more (showing first 20)", len(articles) - 20)

    log.info("")
    log.info("=" * 60)
    log.info("  Dry run complete. %d articles ready (not persisted).", len(articles))
    log.info("=" * 60)

    return 0


def main() -> int:
    args = parse_args()
    start = time.time()

    try:
        if args.dry_run:
            code = run_dry_run(args)
        else:
            code = run_full_pipeline(args)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        return 130
    except Exception as e:
        print(f"\nFATAL: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

    elapsed = time.time() - start
    print(f"\nTotal elapsed: {elapsed:.1f}s | Exit code: {code}")
    return code


if __name__ == "__main__":
    sys.exit(main())
