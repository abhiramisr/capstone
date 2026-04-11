"""Output guardrail — validates response quality before returning to user.

Checks that every response has grounded data citations and that the answer
is not empty or fabricated. Blocks responses that fail critical validation.
"""

from __future__ import annotations

import re
from typing import Any


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


def check_output_safety(
    answer: str,
    sql: str = "",
    analysis: str = "",
    preview_rows: list[dict[str, Any]] | None = None,
) -> list[str]:
    """Validate response quality and grounding.

    Returns a list of issues found. Empty list means the response is safe.

    Parameters
    ----------
    answer:
        The final answer text to validate.
    sql:
        The SQL query that produced the results.
    analysis:
        The analysis text (if any).
    preview_rows:
        The preview result rows for grounding verification.
    """
    issues: list[str] = []

    # Critical: answer must not be empty
    if not answer or not answer.strip():
        issues.append("CRITICAL: Answer is empty")
        return issues  # No point checking further

    # Check for data citation — answer should contain at least one number
    has_number = bool(re.search(r"\d+", answer))
    if not has_number and preview_rows:
        issues.append("WARNING: Answer contains no numeric data despite having query results")

    # If analysis exists, SQL should also be present
    if analysis and analysis.strip() and not sql.strip():
        issues.append("WARNING: Analysis present but no SQL query recorded")

    # Check for hedging language
    lower_answer = answer.lower()
    for pattern in _HEDGING_PATTERNS:
        if re.search(pattern, lower_answer):
            issues.append(f"WARNING: Hedging language detected: {pattern}")
            break  # Only flag once

    # Check grounding — if we have preview rows, verify the answer references
    # at least one value from the results
    if preview_rows and len(preview_rows) > 0:
        # Collect string representations of values from the first few rows
        result_values: set[str] = set()
        for row in preview_rows[:5]:
            for v in row.values():
                if v is not None and str(v) != "[REDACTED]":
                    val_str = str(v).strip()
                    if len(val_str) >= 2:  # Skip trivially short values
                        result_values.add(val_str)

        # Check if the answer references at least one result value
        found_grounding = False
        for val in result_values:
            if val in answer:
                found_grounding = True
                break

        if not found_grounding and has_number:
            # Numbers exist but none match the results — possible fabrication
            issues.append("WARNING: Answer contains numbers that don't appear in query results")

    return issues


def has_critical_issues(issues: list[str]) -> bool:
    """Check if any issues are critical (blocking)."""
    return any(issue.startswith("CRITICAL:") for issue in issues)
