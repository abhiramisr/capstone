"""Output guardrail — validates response quality before returning to user.

Checks for SQL/prompt leakage, PII exposure, data grounding, and hedging.
Blocks responses that fail critical validation.

Usage:
    from src.guardrails.output_guardrail import check_output_safety, has_critical_issues

    issues = check_output_safety(answer="...", sql="...", preview_rows=[...])
    if has_critical_issues(issues):
        print("Blocked!")
"""

from __future__ import annotations

import re
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Pattern definitions
# ---------------------------------------------------------------------------

# SQL fragments that should never appear in a user-facing prose answer
_SQL_LEAK_PATTERNS = [
    r"\bSELECT\b.{0,200}\bFROM\b",
    r"\bWHERE\b.{0,100}\b(?:AND|OR|=|!=|<|>)\b",
    r"\bGROUP\s+BY\b",
    r"\bORDER\s+BY\b",
    r"\bHAVING\b.{0,100}\b(?:COUNT|SUM|AVG|MAX|MIN)\b",
    r"\b(?:INNER|LEFT|RIGHT|FULL\s+OUTER)\s+JOIN\b",
    r"\bINSERT\s+INTO\b",
    r"\bUPDATE\b.{0,50}\bSET\b",
    r"\bDELETE\s+FROM\b",
    r"\bDROP\s+(?:TABLE|DATABASE|VIEW)\b",
    r"\bCREATE\s+(?:TABLE|VIEW|INDEX)\b",
    r"\bALTER\s+TABLE\b",
    r"\bTRUNCATE\s+TABLE\b",
]

# System-prompt or instruction text leaking into the answer
_SYSTEM_PROMPT_LEAK_PATTERNS = [
    r"\byou\s+are\s+(?:a|an)\s+\w+\s+(?:agent|assistant|model|bot)\b",
    r"\byour\s+(?:task|role|job|purpose|goal|objective)\s+is\b",
    r"\bsystem\s+prompt\b",
    r"\b(?:do\s+not|don't|never)\s+(?:reveal|disclose|share|mention|repeat)\b",
    r"<\s*(?:system|prompt|instructions?|context)\s*>",
    r"\[INST\]|<<SYS>>|\[SYS\]|\[\/INST\]",
    r"\bI\s+am\s+(?:a|an)\s+(?:language\s+)?(?:model|AI|LLM)\b",
    r"\bas\s+(?:a|an)\s+AI(?:\s+language)?\s+model\b",
    r"\bcontext\s+window\b",
    r"\btraining\s+data\b",
]

# PII patterns — email, US phone, SSN
_PII_PATTERNS: dict[str, re.Pattern[str]] = {
    "email address": re.compile(
        r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
    ),
    "phone number": re.compile(
        r"\b(?:\+?1[-.\s]?)?\(?[2-9]\d{2}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
    ),
    "SSN": re.compile(
        r"\b\d{3}-\d{2}-\d{4}\b"
    ),
}

# Hedging phrases that suggest ungrounded claims
_HEDGING_PATTERNS = [
    r"\bi think\b",
    r"\bprobably\b",
    r"\bmight be\b",
    r"\bcould be\b",
    r"\bperhaps\b",
    r"\bnot sure\b",
    r"\bguessing\b",
]

_COMPILED_SQL_LEAK = [re.compile(p, re.IGNORECASE) for p in _SQL_LEAK_PATTERNS]
_COMPILED_SYSTEM_PROMPT_LEAK = [re.compile(p, re.IGNORECASE) for p in _SYSTEM_PROMPT_LEAK_PATTERNS]


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_sql_and_prompt_leakage(answer: str) -> list[str]:
    """Detect raw SQL or system-prompt text leaking into the user-facing answer."""
    issues: list[str] = []
    for pattern in _COMPILED_SQL_LEAK:
        if pattern.search(answer):
            issues.append("CRITICAL: answer contains raw SQL fragments")
            break
    for pattern in _COMPILED_SYSTEM_PROMPT_LEAK:
        if pattern.search(answer):
            issues.append("CRITICAL: answer contains system prompt / instruction leakage")
            break
    return issues


def _check_pii(answer: str) -> list[str]:
    """Scan the answer for PII (email, phone, SSN)."""
    issues: list[str] = []
    for pii_type, pattern in _PII_PATTERNS.items():
        if pattern.search(answer):
            issues.append(f"CRITICAL: answer contains a {pii_type}")
    return issues


def _check_grounding(
    answer: str,
    preview_rows: Optional[list[dict[str, Any]]],
) -> list[str]:
    """Verify that numeric values in the answer are traceable to query results."""
    issues: list[str] = []

    has_number = bool(re.search(r"\d+", answer))
    if not has_number and preview_rows:
        issues.append("WARNING: Answer contains no numeric data despite having query results")
        return issues

    if preview_rows and len(preview_rows) > 0:
        result_values: set[str] = set()
        for row in preview_rows[:5]:
            for v in row.values():
                if v is not None and str(v) != "[REDACTED]":
                    val_str = str(v).strip()
                    if len(val_str) >= 2:
                        result_values.add(val_str)

        found_grounding = False
        for val in result_values:
            if val in answer:
                found_grounding = True
                break

        if not found_grounding and has_number:
            issues.append("WARNING: Answer contains numbers that don't appear in query results")

    return issues


def _check_hedging(answer: str) -> list[str]:
    """Check for hedging language that suggests ungrounded claims."""
    lower_answer = answer.lower()
    for pattern in _HEDGING_PATTERNS:
        if re.search(pattern, lower_answer):
            return [f"WARNING: Hedging language detected: {pattern}"]
    return []


# ---------------------------------------------------------------------------
# Public API (matches orchestrator's expected interface)
# ---------------------------------------------------------------------------

def check_output_safety(
    answer: str,
    sql: str = "",
    analysis: str = "",
    preview_rows: Optional[list[dict[str, Any]]] = None,
) -> list[str]:
    """Validate response quality and grounding.

    Runs checks in order of severity:
      1. Empty answer (critical)
      2. SQL / prompt leakage (critical)
      3. PII detection (critical)
      4. Data grounding (warning)
      5. Hedging language (warning)
      6. Analysis / SQL consistency (warning)

    Returns a list of issues found. Empty list means the response is safe.
    """
    issues: list[str] = []

    if not answer or not answer.strip():
        issues.append("CRITICAL: Answer is empty")
        return issues

    issues.extend(_check_sql_and_prompt_leakage(answer))
    issues.extend(_check_pii(answer))
    issues.extend(_check_grounding(answer, preview_rows))
    issues.extend(_check_hedging(answer))

    if analysis and analysis.strip() and not sql.strip():
        issues.append("WARNING: Analysis present but no SQL query recorded")

    return issues


def has_critical_issues(issues: list[str]) -> bool:
    """Check if any issues are critical (blocking)."""
    return any(issue.startswith("CRITICAL:") for issue in issues)
