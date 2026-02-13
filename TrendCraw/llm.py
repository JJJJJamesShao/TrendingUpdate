"""
AI Nexus - MiniMax LLM Client
================================
Async wrapper for the MiniMax chat completion API.
Uses aiohttp for non-blocking calls within the async pipeline.
"""

from __future__ import annotations

import json
from typing import Any

import aiohttp

from config import (
    MINIMAX_API_KEY,
    MINIMAX_MODEL,
    MINIMAX_API_URL,
    LLM_REQUEST_TIMEOUT,
)
from utils import log


# ---------------------------------------------------------------------------
# Core: Call MiniMax Chat API
# ---------------------------------------------------------------------------
async def chat_completion(
    prompt: str,
    *,
    system_prompt: str = "You are a professional AI news analyst.",
    temperature: float = 0.3,
    max_tokens: int = 1024,
    session: aiohttp.ClientSession | None = None,
) -> str:
    """Send a chat completion request to MiniMax and return the response text.

    Args:
        prompt:         User message content.
        system_prompt:  System instruction (default: news analyst role).
        temperature:    Sampling temperature (0–1). Lower = more deterministic.
        max_tokens:     Maximum response length in tokens.
        session:        Optional shared aiohttp session. Creates one if None.

    Returns:
        The assistant's response text, or empty string on failure.
    """
    if not MINIMAX_API_KEY:
        log.error("MINIMAX_API_KEY is not set — cannot call LLM")
        return ""

    headers = {
        "Authorization": f"Bearer {MINIMAX_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": MINIMAX_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()

    try:
        timeout = aiohttp.ClientTimeout(total=LLM_REQUEST_TIMEOUT)
        async with session.post(
            MINIMAX_API_URL, json=payload, headers=headers, timeout=timeout
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                log.error("MiniMax API error HTTP %d: %s", resp.status, body[:300])
                return ""

            data: dict[str, Any] = await resp.json()

        # Check for API-level errors (OpenAI-compatible format)
        if "error" in data:
            err = data["error"]
            log.error(
                "MiniMax API error: %s (type: %s)",
                err.get("message", "unknown"),
                err.get("type", "unknown"),
            )
            return ""

        # Extract content (OpenAI-compatible response)
        choices = data.get("choices", [])
        if not choices:
            log.warning("MiniMax returned empty choices")
            return ""

        content = choices[0].get("message", {}).get("content", "")

        # Log token usage
        usage = data.get("usage", {})
        log.debug(
            "MiniMax tokens — prompt: %d, completion: %d, total: %d",
            usage.get("prompt_tokens", 0),
            usage.get("completion_tokens", 0),
            usage.get("total_tokens", 0),
        )

        return content.strip()

    except Exception as e:
        log.error("MiniMax API call failed: %s", e)
        return ""
    finally:
        if own_session:
            await session.close()


# ---------------------------------------------------------------------------
# Convenience: Parse JSON from LLM response
# ---------------------------------------------------------------------------
def parse_json_response(text: str) -> dict[str, Any] | None:
    """Extract and parse JSON from an LLM response.

    Handles common cases:
    - Clean JSON string
    - JSON wrapped in ```json ... ``` code fences
    - JSON with leading/trailing text
    """
    if not text:
        return None

    # Strip code fences
    cleaned = text.strip()
    if cleaned.startswith("```"):
        # Remove opening fence (with optional language tag)
        cleaned = cleaned.split("\n", 1)[-1] if "\n" in cleaned else cleaned[3:]
        # Remove closing fence
        if "```" in cleaned:
            cleaned = cleaned.rsplit("```", 1)[0]
        cleaned = cleaned.strip()

    # Try direct parse
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Try to find JSON object in the text
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(cleaned[start:end + 1])
        except json.JSONDecodeError:
            pass

    log.warning("Failed to parse JSON from LLM response: %s", cleaned[:200])
    return None


# ---------------------------------------------------------------------------
# Convenience: Parse JSON array from LLM response
# ---------------------------------------------------------------------------
def parse_json_array(text: str) -> list[Any] | None:
    """Extract and parse a JSON array from an LLM response."""
    if not text:
        return None

    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[-1] if "\n" in cleaned else cleaned[3:]
        if "```" in cleaned:
            cleaned = cleaned.rsplit("```", 1)[0]
        cleaned = cleaned.strip()

    try:
        result = json.loads(cleaned)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    # Try to find array brackets
    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            result = json.loads(cleaned[start:end + 1])
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    log.warning("Failed to parse JSON array from LLM response: %s", cleaned[:200])
    return None
