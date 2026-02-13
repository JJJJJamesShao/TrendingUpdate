"""Test <think> tag stripping."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "TrendCraw"))
from llm import _strip_think_tags, parse_json_response

# Test 1: Complete think block before JSON
resp1 = (
    "<think>\nLet me analyze this...\nThe article is about X\n</think>\n"
    '{"summary": "test summary", "category": "AI", "importance": 7}'
)
r1 = parse_json_response(resp1)
assert r1 is not None and r1["summary"] == "test summary", f"FAIL Test 1: {r1}"
print(f"Test 1 OK: {r1}")

# Test 2: Truncated think (ran out of tokens, no JSON)
resp2 = "<think>\nLet me analyze this article. The title mentions..."
r2 = parse_json_response(resp2)
assert r2 is None, f"FAIL Test 2: expected None, got {r2}"
print(f"Test 2 OK: None (truncated think correctly returns None)")

# Test 3: Normal JSON (no think)
resp3 = '{"summary": "works", "category": "LLM", "importance": 5}'
r3 = parse_json_response(resp3)
assert r3 is not None and r3["category"] == "LLM", f"FAIL Test 3: {r3}"
print(f"Test 3 OK: {r3}")

# Test 4: Think with code fences
resp4 = (
    "<think>\nAnalyzing...\n</think>\n"
    "```json\n"
    '{"summary": "fenced", "category": "Research", "importance": 8}\n'
    "```"
)
r4 = parse_json_response(resp4)
assert r4 is not None and r4["summary"] == "fenced", f"FAIL Test 4: {r4}"
print(f"Test 4 OK: {r4}")

print("\nAll tests passed!")
