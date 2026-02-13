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
from dataclasses import dataclass, field
from enum import IntEnum
from dotenv import load_dotenv

# Load .env from project root (TrendingUpdate/.env)
_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=_env_path)

# ---------------------------------------------------------------------------
# Environment Variables
# ---------------------------------------------------------------------------
# MiniMax LLM API (OpenAI-compatible endpoint)
MINIMAX_API_KEY: str = os.getenv("MINIMAX_API_KEY", "")
MINIMAX_MODEL: str = os.getenv("MINIMAX_MODEL", "MiniMax-M2.5")
MINIMAX_BASE_URL: str = os.getenv("MINIMAX_BASE_URL", "https://api.minimaxi.com/v1")
MINIMAX_API_URL: str = f"{MINIMAX_BASE_URL}/chat/completions"

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
REQUEST_TIMEOUT_SECONDS = 30     # Per-request timeout for aiohttp
MAX_CONCURRENT_REQUESTS = 10     # Semaphore limit for parallel fetches
LLM_REQUEST_TIMEOUT = 60         # Timeout for LLM API calls (seconds)

# ── Freshness filter ──
# Discard articles published more than N days ago.
# Prevents ingesting years-old blog archives (e.g. OpenAI's 842 posts from 2015).
ARTICLE_MAX_AGE_DAYS = 7

# ── Content fetching ──
CONTENT_MAX_LENGTH = 8000        # Max chars to store in the `content` column
CONTENT_SNIPPET_FOR_LLM = 3000  # Max content chars sent to LLM for summarization
CONTENT_FETCH_CONCURRENCY = 5   # Parallel content page fetches
CONTENT_FETCH_TIMEOUT = 20      # Seconds per content page fetch

# HTTP defaults — many corporate blogs block bare aiohttp user-agents
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; AI-Nexus/2.0; +https://github.com/ai-nexus)"
)

# Retry configuration for transient failures
FETCH_MAX_RETRIES = 3            # Total attempts = 1 initial + retries
FETCH_RETRY_BACKOFF = 2.0        # Exponential backoff base (seconds)

# Jina content fallback: fetch full text via Jina when snippet is shorter than this
JINA_SNIPPET_MIN_LENGTH = 80     # Characters

# Reddit requires a custom User-Agent
REDDIT_USER_AGENT = "AI-Nexus-Bot/2.0 (News Aggregation Research)"

# Hacker News API filter keywords
HN_FILTER_KEYWORDS = ["GPT", "LLM", "Transformer", "AI", "OpenAI", "Claude",
                       "Gemini", "Llama", "Mistral", "neural", "deep learning"]


# ---------------------------------------------------------------------------
# Source Tier Enum (Lower number = Higher authority)
# ---------------------------------------------------------------------------
class SourceTier(IntEnum):
    """Source authority level. Tier 1 is most authoritative."""
    TIER_1_OFFICIAL = 1   # Corporate Labs & Official Blogs
    TIER_2_MEDIA = 2      # Tech News & Media
    TIER_3_COMMUNITY = 3  # Developer & Community
    TIER_4_RESEARCH = 4   # Papers & Research


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
    SourceConfig(
        name="Anthropic (Claude Blog)",
        # Anthropic doesn't serve a standard RSS; use community-maintained mirror
        url="https://raw.githubusercontent.com/Olshansk/rss-feeds/main/feeds/feed_claude.xml",
        tier=SourceTier.TIER_1_OFFICIAL,
        category="AI",
    ),
    SourceConfig(
        name="Anthropic Research",
        url="https://raw.githubusercontent.com/Olshansk/rss-feeds/main/feeds/feed_anthropic_research.xml",
        tier=SourceTier.TIER_1_OFFICIAL,
        category="Research",
    ),
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
    SourceConfig(
        name="Hugging Face Blog",
        url="https://huggingface.co/blog/feed.xml",
        tier=SourceTier.TIER_1_OFFICIAL,
        category="AI",
    ),
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
    SourceConfig(
        name="The Verge (AI)",
        url="https://www.theverge.com/rss/artificial-intelligence/index.xml",
        tier=SourceTier.TIER_2_MEDIA,
        category="AI",
    ),
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
    SourceConfig(
        name="Reddit r/LocalLLaMA",
        url="https://www.reddit.com/r/LocalLLaMA/new/.json",
        tier=SourceTier.TIER_3_COMMUNITY,
        source_type="reddit_json",
        category="LLM",
        headers={"User-Agent": REDDIT_USER_AGENT},
    ),
    SourceConfig(
        name="Reddit r/MachineLearning",
        url="https://www.reddit.com/r/MachineLearning/new/.json",
        tier=SourceTier.TIER_3_COMMUNITY,
        source_type="reddit_json",
        category="AI",
        headers={"User-Agent": REDDIT_USER_AGENT},
    ),
    SourceConfig(
        name="Simon Willison's Weblog",
        url="https://simonwillison.net/atom/entries/",
        tier=SourceTier.TIER_3_COMMUNITY,
        category="AI",
    ),
    SourceConfig(
        name="Lil'Log (Lilian Weng)",
        url="https://lilianweng.github.io/index.xml",
        tier=SourceTier.TIER_3_COMMUNITY,
        category="AI",
    ),
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
    SourceConfig(
        name="Hugging Face Daily Papers",
        url="https://huggingface.co/papers/feed",
        tier=SourceTier.TIER_4_RESEARCH,
        category="Research",
    ),
]

# ---------------------------------------------------------------------------
# Aggregate all sources
# ---------------------------------------------------------------------------
ALL_SOURCES: list[SourceConfig] = (
    TIER_1_SOURCES + TIER_2_SOURCES + TIER_3_SOURCES + TIER_4_SOURCES
)
