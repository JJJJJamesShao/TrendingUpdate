"""
AI Nexus - Shared Utilities
============================
Logging setup, text normalization, HTML cleaning, and helper functions.
"""

from __future__ import annotations

import html
import logging
import re
import string
import sys
from datetime import datetime, timezone

from config import LOG_LEVEL


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logger(name: str = "ai_nexus") -> logging.Logger:
    """Create a configured logger with timestamped console output.

    Designed for clear GitHub Actions log readability.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Avoid duplicate handlers on re-import

    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# Singleton logger for the project
log = setup_logger()


# ---------------------------------------------------------------------------
# HTML / Text Cleaning
# ---------------------------------------------------------------------------
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")
# Regex to remove entire non-content blocks (scripts, styles, nav, footer, etc.)
_BLOCK_REMOVE_RE = re.compile(
    r"<(script|style|nav|footer|header|aside|iframe|noscript|svg)"
    r"[^>]*>.*?</\1>",
    re.DOTALL | re.IGNORECASE,
)


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities from a string.

    RSS summaries frequently contain <p>, <a>, <img>, CDATA wrappers, etc.
    This gives us clean plaintext for LLM input.
    """
    if not text:
        return ""
    # Remove CDATA wrappers if present
    text = text.replace("<![CDATA[", "").replace("]]>", "")
    # Strip all HTML tags
    text = _HTML_TAG_RE.sub(" ", text)
    # Decode HTML entities (&amp; -> &, &#39; -> ', etc.)
    text = html.unescape(text)
    # Collapse whitespace
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def extract_article_text(raw_html: str) -> str:
    """Extract readable article text from raw HTML page content.

    Unlike strip_html (for RSS snippets), this is designed for full web pages:
    - Removes <script>, <style>, <nav>, <footer>, <header>, <aside> blocks entirely
    - Strips remaining HTML tags
    - Decodes entities
    - Preserves paragraph structure (double newline)
    """
    if not raw_html:
        return ""
    # Remove CDATA wrappers
    text = raw_html.replace("<![CDATA[", "").replace("]]>", "")
    # Remove non-content blocks first
    text = _BLOCK_REMOVE_RE.sub("", text)
    # Convert <br>, <p>, <div>, <h*> to newlines for paragraph structure
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|h[1-6]|article|section|li)>", "\n\n", text, flags=re.IGNORECASE)
    # Strip remaining HTML tags
    text = _HTML_TAG_RE.sub(" ", text)
    # Decode HTML entities
    text = html.unescape(text)
    # Clean up each line, preserve paragraph breaks
    lines = text.split("\n")
    cleaned: list[str] = []
    for line in lines:
        line = _WHITESPACE_RE.sub(" ", line).strip()
        if line:
            cleaned.append(line)
    return "\n".join(cleaned)


def truncate(text: str, max_length: int = 500, suffix: str = "...") -> str:
    """Truncate text to max_length, appending suffix if truncated.

    Tries to break at a word boundary for cleaner output.
    """
    if not text or len(text) <= max_length:
        return text
    # Try to find last space before the limit
    cut_at = text.rfind(" ", 0, max_length)
    if cut_at == -1:
        cut_at = max_length
    return text[:cut_at] + suffix


# ---------------------------------------------------------------------------
# Text Normalization (for deduplication)
# ---------------------------------------------------------------------------
def normalize_title(title: str) -> str:
    """Normalize a title for fuzzy comparison.

    - Lowercase
    - Strip punctuation
    - Collapse whitespace
    """
    title = title.lower().strip()
    title = title.translate(str.maketrans("", "", string.punctuation))
    title = re.sub(r"\s+", " ", title)
    return title


# ---------------------------------------------------------------------------
# Time Helpers
# ---------------------------------------------------------------------------
def utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


def is_within_hours(dt: datetime, hours: int) -> bool:
    """Check if a datetime is within the last N hours from now."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = utc_now() - dt
    return delta.total_seconds() <= hours * 3600
