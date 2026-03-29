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

import asyncio

import aiohttp
import markdown
from rapidfuzz import fuzz

from config import (
    DEDUP_SIMILARITY_THRESHOLD,
    LLM_CLUSTER_BATCH_SIZE,
    CONTENT_SNIPPET_FOR_LLM,
    LLM_ENRICH_CONCURRENCY,
    QWEN_API_KEY,
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
    insight: str = ""
    tags: list[str] = []
    is_processed: bool = False

    def to_db_dict(self) -> dict[str, Any]:
        """Convert to a dict matching Database/news_db.insert_news_items() schema."""
        return {
            "title": self.title,
            "original_url": self.original_url,
            "source_name": self.source_name,
            "summary": self.summary,
            "content": self.content,
            "insight": self.insight,
            "tags": self.tags,
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
    """Use Qwen LLM to group articles by event and select the best source.

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
# STEP 4: Editorial Screening + Structured Summary (Combined LLM Call)
# ===================================================================


def _generate_fallback_content(article: RawArticle, summary: str | None) -> str:
    """Generate minimal Markdown when LLM enrichment fails. Use #### for section headings (→ <h4>)."""
    parts = [f"#### Overview\n\n{article.title}"]
    if summary:
        parts.append(f"\n\n{summary}")
    elif article.content_snippet:
        parts.append(f"\n\n{article.content_snippet[:500]}")
    parts.append(
        f"\n\n#### Source\n\n"
        f"- **{article.source_name}** — Published {article.published_at.strftime('%Y-%m-%d')}\n"
        f"- [Read original article]({article.url})"
    )
    return "\n".join(parts)


def _build_arxiv_prompt(article: RawArticle, content: str) -> tuple[str, str]:
    """Build specialized prompt for ArXiv research paper analysis."""
    prompt = f"""Analyze the following AI/ML research paper and produce a professional academic summary.

─── PAPER ───
Title: {article.title}
Source: {article.source_name}
Published: {article.published_at.strftime("%Y-%m-%d")}
URL: {article.url}
───
{content}
────────────────

Produce a comprehensive research summary in Markdown. Use #### (four hashes) for section headings so they render as subheadings.

#### Problem Statement
What problem does this paper address? What gap in existing research does it fill? (2-3 sentences)

#### Proposed Approach
What method, model, or framework do the authors propose? Describe the core idea clearly. (3-5 sentences)

#### Key Innovations
- What is novel about this work compared to prior art?
- List 3-5 specific technical contributions

#### Methodology & Architecture
Describe the technical approach: model architecture, training procedure, datasets used, loss functions, or theoretical framework. Include specific numbers (parameters, layers, training steps) when available.

#### Results & Benchmarks
- Report ALL quantitative results mentioned in the abstract
- Include benchmark names, metrics, and scores
- Note comparisons with baselines or prior state-of-the-art

#### Significance & Implications
Why does this paper matter? What are the practical implications for the AI community?

TARGET LENGTH: 1500-2500 characters. Be thorough and precise. Use only standard Markdown: ####, **bold**, - lists, [links](url).

═══ TAGS & INSIGHT ═══
- tags: Pick 1-3 most relevant tags from: ["LLM", "Agents", "Infra", "Tools", "Research", "Industry", "Business"]
- insight: A sharp, opinionated one-sentence insight (≤20 words) on how this impacts developers or AI business

═══ RESPONSE FORMAT ═══
Return ONLY valid JSON:
{{
  "is_newsworthy": true,
  "reject_reason": "",
  "category": "Research",
  "importance": 7,
  "summary": "2-3 sentence summary: what the paper proposes and its key result",
  "content": "#### Problem Statement\\n...\\n\\n#### Proposed Approach\\n...\\n\\n...",
  "tags": ["Research", "LLM"],
  "insight": "This architecture could halve inference costs for large-scale deployments."
}}

If the paper is NOT about AI/ML (e.g., pure mathematics, biology without ML):
{{
  "is_newsworthy": false,
  "reject_reason": "Not AI/ML research",
  "category": "General",
  "importance": 0,
  "summary": "",
  "content": "",
  "tags": [],
  "insight": ""
}}"""

    sys_prompt = (
        "You are an AI/ML research scientist writing paper summaries for a professional audience. "
        "Extract ALL technical details from the abstract. Be precise with numbers and methodology. "
        "Your 'insight' must be highly opinionated, sharp, and insightful — not generic. "
        "Respond with ONLY valid JSON. No reasoning, no markdown fences."
    )
    return prompt, sys_prompt


def _build_news_prompt(article: RawArticle, content: str) -> tuple[str, str]:
    """Build editorial screening + summary prompt for general AI news."""
    prompt = f"""Analyze the article below. First decide whether to PUBLISH, then produce a comprehensive summary.

─── ARTICLE ───
Title: {article.title}
Source: {article.source_name}
Published: {article.published_at.strftime("%Y-%m-%d")}
───
{content}
────────────────

═══ STEP 1: TOPIC SCREENING ═══

The PRIMARY subject of the article must be about AI/ML technology, products, research, or the AI industry.

PUBLISH (is_newsworthy = true) — the article's CORE topic is:
• New AI/ML model releases, major version updates, or AI product launches
• Research papers presenting novel AI/ML findings or breakthroughs
• Open-source AI/ML project launches or significant milestones
• AI industry moves: acquisitions, partnerships, regulation, policy affecting AI
• Benchmark results, performance comparisons between AI systems
• Technical deep-dives into AI architectures, training methods, or deployment
• New AI developer tools, frameworks, or infrastructure
• Discoveries or discussions about specific AI systems (e.g., Claude, GPT, LLaMA)

REJECT (is_newsworthy = false):
• Articles that merely MENTION AI/tech companies or figures but are NOT about AI technology (e.g., personal scandals, lawsuits, celebrity gossip involving tech CEOs)
• User questions, polls, or community discussion threads ("Ask HN:", Reddit Q&A)
• Job postings, hiring threads, career advice
• Articles about politics, crime, or entertainment that only tangentially reference AI
• Personal opinions or blog posts without substantive technical analysis
• Vague rumors, unverified speculation, memes
• Routine minor patches, changelogs, or trivial updates

KEY TEST: If you remove "AI/ML" from the article, does the core story still stand as non-AI news? If yes → REJECT.

═══ STEP 2: COMPREHENSIVE SUMMARY (only if publishable) ═══

Write a DETAILED editorial summary based on ALL the content provided. The reader will NOT read the original article — your summary must be a complete, standalone piece that captures every important point.

- Extract and distill ALL key information from the original text
- Target 1500-3000 characters
- Include every specific number, metric, benchmark, date, and direct quote
- Do NOT fabricate or speculate beyond what the source material states

Section structure (use #### for section headings so they render as subheadings):

#### Overview
Comprehensive introduction: who, what, when, why. (3-5 sentences)

#### Key Highlights
- 5-8 bullet points covering ALL major points from the original article
- Include ALL specific numbers, metrics, benchmarks, dates, and direct quotes

#### Technical Details
(For technical content — skip ONLY for pure business news)
Detailed coverage: architecture, methodology, performance metrics, key innovations

#### Impact & Significance
What this means for the AI industry, developers, researchers, and end users.

Use only standard Markdown: ####, **bold**, - lists, [links](url). No HTML.

═══ TAGS & INSIGHT ═══
- tags: Pick 1-3 most relevant tags from: ["LLM", "Agents", "Infra", "Tools", "Research", "Industry", "Business"]
- insight: A sharp, opinionated one-sentence insight (≤20 words) on how this impacts developers or AI business

═══ RESPONSE FORMAT ═══
Return ONLY valid JSON (no markdown fences, no reasoning):
{{
  "is_newsworthy": true,
  "reject_reason": "",
  "category": "AI or LLM or Hardware or Research or Industry",
  "importance": 7,
  "summary": "2-3 sentence executive summary for the article card",
  "content": "#### Overview\\n...\\n\\n#### Key Highlights\\n- ...\\n\\n...",
  "tags": ["LLM", "Industry"],
  "insight": "Open-source alternatives will force proprietary vendors to justify their pricing."
}}

If NOT publishable:
{{
  "is_newsworthy": false,
  "reject_reason": "brief reason",
  "category": "General",
  "importance": 0,
  "summary": "",
  "content": "",
  "tags": [],
  "insight": ""
}}"""

    sys_prompt = (
        "You are a senior AI industry editor writing for an expert readership. "
        "Produce thorough, detailed article summaries that capture ALL key information "
        "from the source material. Your 'insight' must be highly opinionated, sharp, and insightful — not generic. "
        "Respond with ONLY valid JSON. No reasoning, no markdown fences."
    )
    return prompt, sys_prompt


async def enrich_article(
    article: RawArticle,
    session: aiohttp.ClientSession | None = None,
) -> ProcessedArticle | None:
    """Evaluate news-worthiness and generate structured editorial summary.

    Uses DIFFERENT prompts for:
    - ArXiv research papers → specialized academic extraction
    - General news articles → editorial screening + summary

    Returns:
        ProcessedArticle with LLM-generated content, or
        None if the article is not newsworthy (rejected by editorial filter).
    """
    is_arxiv = "arxiv" in article.source_name.lower()

    # Pick the best available content for the LLM prompt
    if article.full_content:
        content_for_llm = article.full_content[:CONTENT_SNIPPET_FOR_LLM]
    elif article.content_snippet:
        content_for_llm = article.content_snippet[:CONTENT_SNIPPET_FOR_LLM]
    else:
        content_for_llm = ""

    if is_arxiv:
        prompt, sys_prompt = _build_arxiv_prompt(article, content_for_llm)
    else:
        prompt, sys_prompt = _build_news_prompt(article, content_for_llm)

    response = await chat_completion(
        prompt,
        system_prompt=sys_prompt,
        temperature=0.2,
        max_tokens=2000,
        session=session,
    )

    result = parse_json_response(response)

    if result:
        # ── Editorial rejection ──
        if not result.get("is_newsworthy", True):
            reason = result.get("reject_reason", "not newsworthy")
            log.info("  → Rejected: %s", reason[:80])
            return None

        content_md = result.get("content", "")
        summary = result.get("summary", "")
        insight = result.get("insight", "")
        tags = result.get("tags", [])

        # Ensure content is not empty — fallback if LLM returned thin content
        if not content_md or len(content_md.strip()) < 50:
            content_md = _generate_fallback_content(article, summary)

        # Markdown → HTML fragment for frontend prose (no wrapper tags)
        content = markdown.markdown(content_md)

        # Ensure summary is not empty
        if not summary:
            summary = f"{article.title} — from {article.source_name}."

        # Ensure tags is a valid list
        if not isinstance(tags, list):
            tags = []

        return ProcessedArticle(
            title=article.title,
            original_url=article.url,
            source_name=article.source_name,
            published_at=article.published_at,
            category=result.get("category", article.category),
            summary=summary,
            content=content,
            insight=insight,
            tags=tags,
            is_processed=True,
        )

    # ── LLM call failed entirely — use fallback content ──
    log.warning("Enrichment failed for '%s' — generating fallback", article.title[:50])
    fallback_md = _generate_fallback_content(article, None)
    content = markdown.markdown(fallback_md)
    return ProcessedArticle(
        title=article.title,
        original_url=article.url,
        source_name=article.source_name,
        published_at=article.published_at,
        category=article.category,
        summary=f"{article.title} — from {article.source_name}.",
        content=content,
        insight="",
        tags=[],
        is_processed=False,
    )


async def enrich_all(
    articles: list[RawArticle],
    session: aiohttp.ClientSession | None = None,
) -> list[ProcessedArticle]:
    """Enrich all articles with LLM-generated editorial summaries.

    Runs with limited concurrency (LLM_ENRICH_CONCURRENCY) to balance
    speed and rate limits. Non-newsworthy articles are filtered out.
    """
    log.info("Starting enrichment for %d articles (concurrency=%d)...",
             len(articles), LLM_ENRICH_CONCURRENCY)

    sem = asyncio.Semaphore(LLM_ENRICH_CONCURRENCY)
    total = len(articles)

    async def _enrich_one(i: int, article: RawArticle) -> ProcessedArticle | None:
        async with sem:
            log.info("  Enriching [%d/%d]: %s", i, total, article.title[:60])
            return await enrich_article(article, session)

    tasks = [_enrich_one(i, a) for i, a in enumerate(articles, 1)]
    raw_results = await asyncio.gather(*tasks)

    # Filter: remove rejected (None) and empty-content articles
    results = [r for r in raw_results if r is not None and r.content]

    rejected = sum(1 for r in raw_results if r is None)
    processed = sum(1 for r in results if r.is_processed)

    log.info("Enrichment complete: %d published, %d rejected, %d total input",
             len(results), rejected, total)
    return results
