"""Input guardrail — deterministic prompt-injection screening.

Detects prompt injection, instruction override attempts, role manipulation,
encoded payloads, and dangerous SQL keywords. This is a hard block — not advisory.
"""

from __future__ import annotations

import re

# Patterns that indicate suspicious but potentially legitimate queries
SUSPICIOUS_PATTERNS = [
    r"drop\s+table",
    r"delete\s+from",
    r"insert\s+into",
    r"update\s+\w+\s+set",
    r";\s*(drop|delete|insert|update|alter|create)",
    r"exfiltrate",
    r"run\s+tool",
]

# Patterns that strongly indicate prompt injection — always hard block
INJECTION_PATTERNS = [
    r"ignore\s+(previous|above|all)\s+instructions",
    r"you\s+are\s+now",
    r"forget\s+(everything|all|previous)",
    r"act\s+as\s+(a|an)\b",
    r"system\s*:?\s*prompt",
    r"reveal\s+(your|the)\s+(instructions|prompt|system)",
    r"disregard\s+(previous|above|all|your)",
    r"override\s+(previous|all|your)",
    r"new\s+instructions?\s*:",
    r"base64[^a-z]",
    r"\\x[0-9a-fA-F]{2}",
    r"<script|javascript:|onclick|onerror",
]

# Combined pattern list for backward compatibility
ALL_PATTERNS = SUSPICIOUS_PATTERNS + INJECTION_PATTERNS


def check_input_safety(question: str) -> list[str]:
    """Basic deterministic prompt-injection screening.

    Returns a list of detected suspicious patterns. Empty list means safe.
    """
    flags: list[str] = []
    lower = question.lower()
    for pattern in ALL_PATTERNS:
        if re.search(pattern, lower):
            flags.append(f"Suspicious pattern: {pattern}")
    return flags


def is_hard_block(flags: list[str]) -> bool:
    """Determine if the detected flags warrant a hard block.

    Returns True if any flag matches an injection-related pattern
    (as opposed to a suspicious but potentially legitimate SQL keyword).
    """
    if not flags:
        return False
    for flag in flags:
        # Extract the pattern from the flag string
        pattern = flag.replace("Suspicious pattern: ", "")
        if pattern in INJECTION_PATTERNS:
            return True
    return False
