"""
AI Nexus - Content Processors
================================
Deduplication (URL + rapidfuzz + LLM clustering) and content enrichment.

This module is DB-agnostic — it receives known URLs/titles as parameters
and returns processed results. The root orchestrator handles all DB I/O.

LLM calls use MiniMax API via llm.py. Prompts are in English
(target audience is international / overseas users).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp
from rapidfuzz import fuzz

from config import (
    DEDUP_SIMILARITY_THRESHOLD,
    LLM_CLUSTER_BATCH_SIZE,
    CONTENT_SNIPPET_FOR_LLM,
)
from fetchers import RawArticle
from llm import chat_completion, parse_json_response, parse_json_array
from utils import log, normalize_title


# ---------------------------------------------------------------------------
# Processed Article (output of the full pipeline)
# ---------------------------------------------------------------------------
@dataclass
class ProcessedArticle:
    """Article ready for database insertion after dedup + enrichment."""
    title: str
    original_url: str
    source_name: str
    published_at: datetime
    category: str = "General"
    summary: str | None = None
    content: str | None = None
    is_processed: bool = False

    def to_db_dict(self) -> dict[str, Any]:
        """Convert to a dict matching Database/news_db.insert_news_items() schema."""
        return {
            "title": self.title,
            "original_url": self.original_url,
            "source_name": self.source_name,
            "summary": self.summary,
            "content": self.content,
            "published_at": self.published_at.isoformat(),
            "category": self.category,
            "is_processed": self.is_processed,
        }


# ===================================================================
# STEP 1: Exact URL Deduplication
# ===================================================================
def dedup_by_url(
    articles: list[RawArticle],
    known_urls: set[str],
) -> list[RawArticle]:
    """Remove articles whose URL already exists in the database.

    Args:
        articles:   Freshly fetched articles.
        known_urls: Set of original_url strings from DB (last 24h).
    """
    before = len(articles)
    filtered = [a for a in articles if a.url not in known_urls]
    removed = before - len(filtered)
    log.info("URL dedup: %d → %d (removed %d known URLs)", before, len(filtered), removed)
    return filtered


# ===================================================================
# STEP 2: Fuzzy Title Deduplication (rapidfuzz)
# ===================================================================
def dedup_by_fuzzy_title(
    articles: list[RawArticle],
    known_titles: list[str],
    threshold: int = DEDUP_SIMILARITY_THRESHOLD,
) -> list[RawArticle]:
    """Remove articles whose titles are too similar to existing DB entries.

    Uses rapidfuzz token_sort_ratio — robust against word reordering
    (e.g., "OpenAI releases GPT-5" vs "GPT-5 released by OpenAI").

    Also deduplicates within the current batch itself, keeping the
    article from the highest-authority (lowest tier number) source.

    Args:
        articles:      Freshly fetched articles (after URL dedup).
        known_titles:  List of title strings from DB (last 24h).
        threshold:     Similarity score cutoff (0–100). Default 85.
    """
    if not articles:
        return []

    before = len(articles)

    # --- Phase A: Filter against existing DB titles ---
    normalized_existing = [normalize_title(t) for t in known_titles] if known_titles else []
    after_db_filter: list[RawArticle] = []

    for article in articles:
        norm_title = normalize_title(article.title)
        is_dup = False

        for existing in normalized_existing:
            score = fuzz.token_sort_ratio(norm_title, existing)
            if score >= threshold:
                log.debug(
                    "Fuzzy dup vs DB (%.0f%%): '%s' ≈ '%s'",
                    score, article.title[:50], existing[:50],
                )
                is_dup = True
                break

        if not is_dup:
            after_db_filter.append(article)

    db_removed = before - len(after_db_filter)

    # --- Phase B: Deduplicate within the current batch ---
    # Keep first occurrence per "cluster" (sorted by tier → timestamp)
    seen_normalized: list[str] = []
    final: list[RawArticle] = []

    # Sort: prefer higher authority (lower tier), then earlier publish time
    sorted_articles = sorted(after_db_filter, key=lambda a: (a.tier, a.published_at))

    for article in sorted_articles:
        norm_title = normalize_title(article.title)
        is_batch_dup = False

        for seen in seen_normalized:
            score = fuzz.token_sort_ratio(norm_title, seen)
            if score >= threshold:
                log.debug(
                    "Fuzzy dup within batch (%.0f%%): '%s' ≈ '%s'",
                    score, article.title[:50], seen[:50],
                )
                is_batch_dup = True
                break

        if not is_batch_dup:
            final.append(article)
            seen_normalized.append(norm_title)

    batch_removed = len(after_db_filter) - len(final)

    log.info(
        "Fuzzy dedup: %d → %d (DB filter: -%d, batch dedup: -%d)",
        before, len(final), db_removed, batch_removed,
    )
    return final


# ===================================================================
# STEP 3: LLM-Based Semantic Clustering
# ===================================================================
async def cluster_articles_with_llm(
    articles: list[RawArticle],
    session: aiohttp.ClientSession | None = None,
) -> list[RawArticle]:
    """Use MiniMax LLM to group articles by event and select the best source.

    Selection rules:
    - Prefer Tier 1 sources (official / corporate).
    - If same tier, prefer earliest publication timestamp.

    Processes in batches of LLM_CLUSTER_BATCH_SIZE.
    """
    if len(articles) <= 1:
        return articles

    all_selected: list[RawArticle] = []

    for batch_start in range(0, len(articles), LLM_CLUSTER_BATCH_SIZE):
        batch = articles[batch_start:batch_start + LLM_CLUSTER_BATCH_SIZE]
        selected = await _cluster_batch(batch, session)
        all_selected.extend(selected)

    log.info("LLM clustering: %d → %d unique events", len(articles), len(all_selected))
    return all_selected


async def _cluster_batch(
    batch: list[RawArticle],
    session: aiohttp.ClientSession | None = None,
) -> list[RawArticle]:
    """Send a batch of articles to MiniMax for semantic grouping."""
    items_text = ""
    for i, article in enumerate(batch):
        items_text += (
            f"[{i}] Title: {article.title}\n"
            f"     Source: {article.source_name} (Tier {article.tier})\n"
            f"     Published: {article.published_at.isoformat()}\n\n"
        )

    prompt = f"""You are a news deduplication engine. Below are {len(batch)} news articles.

TASK:
1. Group articles that cover the SAME event or topic.
2. For each group, select ONE "primary" article using these rules:
   - Prefer lower Tier number (Tier 1 > Tier 2 > Tier 3 > Tier 4).
   - If same tier, prefer the earliest published timestamp.
3. Unique articles (no duplicates) should be kept as-is.

ARTICLES:
{items_text}

RESPOND with ONLY a JSON array of the selected article indices. Example: [0, 3, 5]
No explanation, no markdown, just the JSON array."""

    response = await chat_completion(
        prompt,
        system_prompt="You are a precise deduplication engine. Output only valid JSON.",
        temperature=0,
        max_tokens=200,
        session=session,
    )

    indices = parse_json_array(response)
    if indices:
        valid = [batch[i] for i in indices if isinstance(i, int) and 0 <= i < len(batch)]
        if valid:
            return valid

    log.warning("LLM clustering parse failed — keeping all %d items in batch", len(batch))
    return batch


# ===================================================================
# STEP 4: Content Enrichment (Summarize via MiniMax)
# ===================================================================
async def enrich_article(
    article: RawArticle,
    session: aiohttp.ClientSession | None = None,
) -> ProcessedArticle:
    """Use MiniMax to generate a summary, category, and importance score.

    Uses the best available content: full_content > content_snippet > title only.
    Prompts are in ENGLISH — the product targets international users.
    """
    # Pick the best available content for the LLM prompt
    if article.full_content:
        content_for_llm = article.full_content[:CONTENT_SNIPPET_FOR_LLM]
    elif article.content_snippet:
        content_for_llm = article.content_snippet[:CONTENT_SNIPPET_FOR_LLM]
    else:
        content_for_llm = "(no content available — summarize from title only)"

    prompt = f"""Analyze the following AI/tech news article and provide a structured summary.

TITLE: {article.title}
SOURCE: {article.source_name}
PUBLISHED: {article.published_at.strftime("%Y-%m-%d")}
CONTENT:
{content_for_llm}

Respond with a JSON object containing:
{{
  "summary": "A concise 2-3 sentence summary highlighting the key points and significance",
  "category": "One of: AI, LLM, Hardware, Research, Industry, General",
  "importance": 7
}}

Rules:
- summary: Write in clear, professional English. Focus on WHAT happened and WHY it matters.
- category: Choose the most specific applicable category.
- importance: Integer 1-10 (10 = groundbreaking release/discovery, 1 = minor update).

CRITICAL: Return ONLY the raw JSON object. Do NOT wrap in markdown. Do NOT include any reasoning, thinking, or explanation. Start your response with {{ and end with }}."""

    response = await chat_completion(
        prompt,
        system_prompt=(
            "You are a senior AI industry analyst. "
            "Respond with ONLY valid JSON. No reasoning, no explanations, no markdown."
        ),
        temperature=0.1,
        max_tokens=800,
        session=session,
    )

    # Determine stored content: full_content > snippet > empty
    stored_content = article.full_content or article.content_snippet or None

    result = parse_json_response(response)

    if result:
        return ProcessedArticle(
            title=article.title,
            original_url=article.url,
            source_name=article.source_name,
            published_at=article.published_at,
            category=result.get("category", article.category),
            summary=result.get("summary", ""),
            content=stored_content,
            is_processed=True,
        )

    # Fallback: insert without enrichment but still store content
    log.warning("Enrichment failed for '%s' — inserting unprocessed", article.title[:50])
    return ProcessedArticle(
        title=article.title,
        original_url=article.url,
        source_name=article.source_name,
        published_at=article.published_at,
        category=article.category,
        summary=None,
        content=stored_content,
        is_processed=False,
    )


async def enrich_all(
    articles: list[RawArticle],
    session: aiohttp.ClientSession | None = None,
) -> list[ProcessedArticle]:
    """Enrich all articles with LLM-generated summaries.

    Processes sequentially to respect MiniMax rate limits.
    """
    log.info("Starting enrichment for %d articles...", len(articles))
    items: list[ProcessedArticle] = []

    for i, article in enumerate(articles, 1):
        log.info("  Enriching [%d/%d]: %s", i, len(articles), article.title[:60])
        item = await enrich_article(article, session)
        items.append(item)

    processed = sum(1 for it in items if it.is_processed)
    log.info("Enrichment complete: %d/%d successfully processed", processed, len(items))
    return items
