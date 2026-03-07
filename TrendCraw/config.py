"""
AI Nexus - Configuration & Data Source Definitions
===================================================
All RSS feeds, API endpoints, and source metadata are defined here.
Source tiers determine authority for deduplication selection logic.

NOTE: Database config lives in TrendingUpdate/.env and is accessed via
      Database/news_db.py — TrendCraw does NOT touch the DB directly.
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from enum import IntEnum
from dotenv import load_dotenv

# Load .env from project root (TrendingUpdate/.env)
_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=_env_path)

# ---------------------------------------------------------------------------
# Environment Variables
# ---------------------------------------------------------------------------
# Qwen LLM API (OpenAI-compatible endpoint)
QWEN_API_KEY: str = os.getenv("QWEN_API_KEY", "")
QWEN_MODEL: str = os.getenv("QWEN_MODEL", "qwen-plus")
QWEN_BASE_URL: str = os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
QWEN_API_URL: str = f"{QWEN_BASE_URL}/chat/completions"

# Jina Reader (optional, for JS-heavy pages)
JINA_API_KEY: str = os.getenv("JINA_API_KEY", "")

# Logging
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
JINA_BASE_URL = "https://r.jina.ai/"
DEDUP_SIMILARITY_THRESHOLD = 85  # rapidfuzz score threshold (0-100)
DEDUP_TIME_WINDOW_HOURS = 24     # Only compare against items from last N hours
LLM_CLUSTER_BATCH_SIZE = 25      # Max items per LLM clustering call
REQUEST_TIMEOUT_SECONDS = 10     # Per-request timeout for aiohttp (was 15)
MAX_CONCURRENT_REQUESTS = 20     # Semaphore limit for parallel fetches
LLM_REQUEST_TIMEOUT = 90         # Timeout for LLM API calls (increased for Qwen)
LLM_ENRICH_CONCURRENCY = 20     # Parallel LLM enrichment calls

# ── Freshness filter ──
# Only process articles published within the last N hours.
# For 2h cron: 4h gives safe overlap. For first-run seeding: use --max-age 168 (7d).
ARTICLE_MAX_AGE_HOURS = 4

# ── Content fetching & extraction ──
CONTENT_MAX_LENGTH = 10000       # Max chars to store in the `content` column (Markdown)
CONTENT_SNIPPET_FOR_LLM = 6000  # Max content chars sent to LLM for summarization
CONTENT_FETCH_CONCURRENCY = 20  # Parallel content page fetches
CONTENT_FETCH_TIMEOUT = 20      # Seconds per content page fetch
LLM_EXTRACT_MAX_INPUT = 12000   # Max chars of cleaned HTML to send to LLM extractor

# HTTP defaults — many corporate blogs block bare aiohttp user-agents
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; AI-Nexus/2.0; +https://github.com/ai-nexus)"
)

# Retry configuration for transient failures
FETCH_MAX_RETRIES = 1            # Total attempts (was 2 — fail fast)
FETCH_RETRY_BACKOFF = 1.0        # Exponential backoff base (was 1.5)

# Jina content fallback: fetch full text via Jina when snippet is shorter than this
JINA_SNIPPET_MIN_LENGTH = 80     # Characters

# Reddit requires a custom User-Agent
REDDIT_USER_AGENT = "AI-Nexus-Bot/2.0 (News Aggregation Research)"

# =============================================================================
# Hacker News Three-Stage Funnel Configuration
# =============================================================================

# Stage 1: Keyword filter (50+ AI-related terms)
HN_FILTER_KEYWORDS = [
    # Core AI/ML
    "AI", "ML", "LLM", "AGI", "ANN", "CNN", "RNN", "GAN", "VAE", "MoE",
    "Transformer", "Diffusion", "neural network", "deep learning", "machine learning",

    # Hot topics
    "Agent", "Agents", "RAG", "ReAct", "CoT", "fine-tuning", "prompt", "in-context",
    "RLHF", "alignment", "hallucination", "emergent", "reasoning",

    # Models & Companies
    "GPT", "Claude", "Gemini", "Llama", "Mistral", "PaLM", "Gemini", "Bard",
    "OpenAI", "Anthropic", "DeepMind", "Cohere", "Midjourney", "Stability AI",

    # Technical terms
    "inference", "token", "embedding", "vector", "quantization", "distillation",
    "attention", "backbone", "encoder", "decoder", "fine-tune",

    # Applications
    "copilot", "chatbot", "autocomplete", "code generation", "text-to-image",
    "text-to-video", "voice cloning", "speech synthesis",
]

# Stage 1: Score and comment thresholds
HN_MIN_SCORE = 100        # Minimum HN score
HN_MIN_COMMENTS = 30      # Minimum comment count
HN_TOP_N = 500            # Only check top N hot stories

# Stage 2: Content extraction limit (~8000 chars ≈ 2000 tokens)
HN_CONTENT_MAX_LENGTH = 8000

# Stage 3: AI semantic scoring threshold (only keep articles with score >= 6)
HN_AI_SCORE_THRESHOLD = 6

# Domains to exclude (social media, low-quality sources)
HN_EXCLUDED_DOMAINS = [
    "twitter.com", "x.com", "youtube.com", "youtu.be",
    "reddit.com", "facebook.com", "instagram.com", "tiktok.com",
    "linkedin.com", "medium.com",  # Often low-quality reposts
]


# ---------------------------------------------------------------------------
# Source Tier Enum (Lower number = Higher authority)
# ---------------------------------------------------------------------------
class SourceTier(IntEnum):
    """Source authority level. Tier 1 is most authoritative."""
    TIER_1_OFFICIAL = 1   # Corporate Labs & Official Blogs
    TIER_2_MEDIA = 2      # Tech News & Media
    TIER_3_COMMUNITY = 3  # Developer & Community
    TIER_4_RESEARCH = 4   # Papers & Research
    TIER_5_DAILY_BLOGS = 5  # High-quality blogs (OPML); fetched only in daily run


# ---------------------------------------------------------------------------
# Source Data Class
# ---------------------------------------------------------------------------
@dataclass
class SourceConfig:
    """Configuration for a single news source."""
    name: str
    url: str
    tier: SourceTier
    source_type: str = "rss"         # "rss" | "reddit_json" | "hn_api" | "arxiv"
    category: str = "General"
    headers: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# ALL Data Sources (AGENTS.md §2 - Fetch ALL, do NOT sample)
# ---------------------------------------------------------------------------

# ---- Tier 1: Corporate Labs & Official (High Authority) ----
TIER_1_SOURCES: list[SourceConfig] = [
    SourceConfig(
        name="OpenAI Blog",
        url="https://openai.com/blog/rss.xml",
        tier=SourceTier.TIER_1_OFFICIAL,
        category="AI",
    ),
    # Anthropic blogs — disabled: community RSS mirrors consistently timeout
    # Re-enable when official RSS feeds become available
    # SourceConfig(name="Anthropic (Claude Blog)", url="...", tier=SourceTier.TIER_1_OFFICIAL, category="AI"),
    # SourceConfig(name="Anthropic Research", url="...", tier=SourceTier.TIER_1_OFFICIAL, category="Research"),
    SourceConfig(
        name="Google DeepMind",
        url="https://deepmind.google/blog/rss.xml",
        tier=SourceTier.TIER_1_OFFICIAL,
        category="AI",
    ),
    SourceConfig(
        name="Microsoft Research",
        url="https://www.microsoft.com/en-us/research/feed/",
        tier=SourceTier.TIER_1_OFFICIAL,
        category="AI",
    ),
    # Hugging Face Blog — disabled: consistently times out (Cloudflare blocks)
    # SourceConfig(name="Hugging Face Blog", url="https://huggingface.co/blog/feed.xml", tier=SourceTier.TIER_1_OFFICIAL, category="AI"),
    SourceConfig(
        name="NVIDIA Blog (AI)",
        url="https://blogs.nvidia.com/blog/category/deep-learning/feed/",
        tier=SourceTier.TIER_1_OFFICIAL,
        category="Hardware",
    ),
    SourceConfig(
        name="AWS Machine Learning",
        url="https://aws.amazon.com/blogs/machine-learning/feed/",
        tier=SourceTier.TIER_1_OFFICIAL,
        category="AI",
    ),
]

# ---- Tier 2: Tech News & Media (High Volume) ----
TIER_2_SOURCES: list[SourceConfig] = [
    SourceConfig(
        name="TechCrunch (AI)",
        url="https://techcrunch.com/category/artificial-intelligence/feed/",
        tier=SourceTier.TIER_2_MEDIA,
        category="AI",
    ),
    # The Verge — disabled: RSS returns 0 entries (format changed?)
    # SourceConfig(name="The Verge (AI)", url="https://www.theverge.com/rss/artificial-intelligence/index.xml", tier=SourceTier.TIER_2_MEDIA, category="AI"),
    SourceConfig(
        name="VentureBeat (AI)",
        url="https://venturebeat.com/category/ai/feed/",
        tier=SourceTier.TIER_2_MEDIA,
        category="AI",
    ),
    SourceConfig(
        name="MIT Technology Review",
        url="https://www.technologyreview.com/topic/artificial-intelligence/feed",
        tier=SourceTier.TIER_2_MEDIA,
        category="AI",
    ),
    SourceConfig(
        name="Ars Technica (AI)",
        url="https://arstechnica.com/tag/ai/feed/",
        tier=SourceTier.TIER_2_MEDIA,
        category="AI",
    ),
    SourceConfig(
        name="Wired (AI)",
        url="https://www.wired.com/feed/tag/ai/latest/rss",
        tier=SourceTier.TIER_2_MEDIA,
        category="AI",
    ),
]

# ---- Tier 3: Developer & Community (High Technicality) ----
TIER_3_SOURCES: list[SourceConfig] = [
    SourceConfig(
        name="Hacker News",
        url="https://hacker-news.firebaseio.com/v0",
        tier=SourceTier.TIER_3_COMMUNITY,
        source_type="hn_api",
        category="AI",
    ),
    # Reddit — disabled: RSS consistently returns 403/timeout (aggressive bot blocking)
    # SourceConfig(name="Reddit r/LocalLLaMA", url="https://www.reddit.com/r/LocalLLaMA/new/.rss", tier=SourceTier.TIER_3_COMMUNITY, source_type="rss", category="LLM"),
    # SourceConfig(name="Reddit r/MachineLearning", url="https://www.reddit.com/r/MachineLearning/new/.rss", tier=SourceTier.TIER_3_COMMUNITY, source_type="rss", category="AI"),
    SourceConfig(
        name="Simon Willison's Weblog",
        url="https://simonwillison.net/atom/entries/",
        tier=SourceTier.TIER_3_COMMUNITY,
        category="AI",
    ),
    # Lil'Log — disabled: connection error (site blocks non-browser clients)
    # SourceConfig(name="Lil'Log (Lilian Weng)", url="https://lilianweng.github.io/index.xml", tier=SourceTier.TIER_3_COMMUNITY, category="AI"),
    SourceConfig(
        name="Last Week in AI",
        url="https://lastweekin.ai/feed",
        tier=SourceTier.TIER_3_COMMUNITY,
        category="AI",
    ),
]

# ---- Tier 4: Paper & Research ----
TIER_4_SOURCES: list[SourceConfig] = [
    SourceConfig(
        name="ArXiv CS.AI",
        url="cs.AI",
        tier=SourceTier.TIER_4_RESEARCH,
        source_type="arxiv",
        category="Research",
    ),
    SourceConfig(
        name="ArXiv CS.CL",
        url="cs.CL",
        tier=SourceTier.TIER_4_RESEARCH,
        source_type="arxiv",
        category="Research",
    ),
    # Hugging Face Daily Papers — removed (returns 401, requires HF Token)
]

# ---------------------------------------------------------------------------
# Daily blog feeds (92 high-quality blogs from OPML) — fetched only when
# FETCH_DAILY_FEEDS=1 or --daily-feeds (e.g. once per day 18:00–21:00 UTC).
# ---------------------------------------------------------------------------
OPML_PATH = os.path.join(os.path.dirname(__file__), "..", "emschwartz_rss_feed.opml")


def _load_opml_sources(path: str) -> list[SourceConfig]:
    """Parse OPML and return list of SourceConfig (RSS only). Normalizes http→https."""
    if not os.path.isfile(path):
        return []
    try:
        tree = ET.parse(path)
        root = tree.getroot()
    except ET.ParseError:
        return []
    # OPML 2.0: outline elements; xmlUrl in outline (or nested).
    # Handle both <outline xmlUrl="..."> and nested <outline><outline xmlUrl="...">
    configs: list[SourceConfig] = []
    for elem in root.iter():
        if elem.tag.endswith("outline"):
            url = elem.get("xmlUrl") or elem.get("url")
            if not url or not url.strip():
                continue
            url = url.strip()
            if url.startswith("http://"):
                url = "https://" + url[7:]
            name = (elem.get("title") or elem.get("text") or "").strip() or url
            configs.append(
                SourceConfig(
                    name=name[:80],
                    url=url,
                    tier=SourceTier.TIER_5_DAILY_BLOGS,
                    source_type="rss",
                    category="General",
                )
            )
    return configs


# Lazy load to avoid file I/O on import when not using daily feeds
_DAILY_BLOG_SOURCES: list[SourceConfig] | None = None


def get_daily_blog_sources() -> list[SourceConfig]:
    """Load and return the 92 OPML blog sources (cached)."""
    global _DAILY_BLOG_SOURCES
    if _DAILY_BLOG_SOURCES is None:
        _DAILY_BLOG_SOURCES = _load_opml_sources(OPML_PATH)
    return _DAILY_BLOG_SOURCES


def get_sources(include_daily_feeds: bool = False) -> list[SourceConfig]:
    """Return sources to fetch. include_daily_feeds=True adds 92 OPML blogs (for daily run)."""
    hot = TIER_1_SOURCES + TIER_2_SOURCES + TIER_3_SOURCES + TIER_4_SOURCES
    if not include_daily_feeds:
        return hot
    return hot + get_daily_blog_sources()


# ---------------------------------------------------------------------------
# Aggregate: hot sources only (every 2h cron). Full = hot + daily blogs.
# ---------------------------------------------------------------------------
ALL_SOURCES: list[SourceConfig] = (
    TIER_1_SOURCES + TIER_2_SOURCES + TIER_3_SOURCES + TIER_4_SOURCES
)
