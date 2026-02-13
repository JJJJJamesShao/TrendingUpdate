"""
Database - News Items CRUD
============================
Centralized psycopg2 operations for the news_items table.
Used by the root main agent to read/write news data.
All other modules (TrendCraw, etc.) should NOT import this directly —
they receive data via function parameters from the orchestrator.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Load .env from project root
_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=_env_path)

# ---------------------------------------------------------------------------
# DB Connection Config (from .env)
# ---------------------------------------------------------------------------
_DB_CONFIG = {
    "user": os.getenv("user", ""),
    "password": os.getenv("password", ""),
    "host": os.getenv("host", ""),
    "port": os.getenv("port", "6543"),
    "dbname": os.getenv("dbname", "postgres"),
}


def _get_connection() -> psycopg2.extensions.connection:
    """Create a new database connection."""
    return psycopg2.connect(
        user=_DB_CONFIG["user"],
        password=_DB_CONFIG["password"],
        host=_DB_CONFIG["host"],
        port=_DB_CONFIG["port"],
        dbname=_DB_CONFIG["dbname"],
    )


# ---------------------------------------------------------------------------
# Read Operations
# ---------------------------------------------------------------------------
def get_recent_urls(hours: int = 24) -> set[str]:
    """Fetch all original_url values from the last N hours.

    Used for exact-URL deduplication before processing.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT original_url FROM news_items WHERE created_at >= %s",
                (cutoff,),
            )
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def get_recent_titles(hours: int = 24) -> list[str]:
    """Fetch all titles from the last N hours.

    Used for fuzzy-title deduplication (rapidfuzz).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT title FROM news_items WHERE created_at >= %s",
                (cutoff,),
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Write Operations
# ---------------------------------------------------------------------------
def insert_news_items(items: list[dict[str, Any]]) -> int:
    """Batch-insert news items with ON CONFLICT DO NOTHING (idempotent).

    Each item dict should have keys:
        title, original_url, source_name, summary, content, published_at, category, is_processed

    Returns the number of rows actually inserted.
    """
    if not items:
        return 0

    sql = """
        INSERT INTO news_items
            (title, original_url, source_name, summary, content, published_at, category, is_processed)
        VALUES
            (%(title)s, %(original_url)s, %(source_name)s, %(summary)s, %(content)s, %(published_at)s, %(category)s, %(is_processed)s)
        ON CONFLICT (original_url) DO NOTHING
    """

    conn = _get_connection()
    inserted = 0
    try:
        with conn.cursor() as cur:
            for item in items:
                cur.execute(sql, item)
                inserted += cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return inserted


def mark_processed(original_url: str, summary: str) -> None:
    """Update an existing news item as processed with its summary."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE news_items SET is_processed = TRUE, summary = %s WHERE original_url = %s",
                (summary, original_url),
            )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Table Bootstrap (idempotent)
# ---------------------------------------------------------------------------
def ensure_table_exists() -> None:
    """Create the news_items table if it doesn't already exist.

    Safe to call on every pipeline run — uses IF NOT EXISTS.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS news_items (
        id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        title          TEXT NOT NULL,
        original_url   TEXT UNIQUE NOT NULL,
        source_name    TEXT NOT NULL,
        summary        TEXT,
        content        TEXT,
        published_at   TIMESTAMPTZ NOT NULL,
        created_at     TIMESTAMPTZ DEFAULT now(),
        category       TEXT DEFAULT 'General',
        is_processed   BOOLEAN DEFAULT FALSE
    );
    """
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
            # Migration: add content column if table existed before this version
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'news_items' AND column_name = 'content'
                    ) THEN
                        ALTER TABLE news_items ADD COLUMN content TEXT;
                    END IF;
                END $$;
            """)
        conn.commit()
    finally:
        conn.close()
