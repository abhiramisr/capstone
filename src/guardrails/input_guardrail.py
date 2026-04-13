"""Input guardrail — deterministic prompt-injection screening.

Detects prompt injection, instruction override attempts, role manipulation,
encoded payloads, schema exfiltration, and dangerous SQL keywords.

Usage:
    from src.guardrails.input_guardrail import check_input_safety, is_hard_block

    issues = check_input_safety("Show me total revenue by region")
    if issues and is_hard_block(issues):
        print("Blocked!")
"""

from __future__ import annotations

import base64
import re
import unicodedata
from typing import List, Optional


# ---------------------------------------------------------------------------
# Pattern definitions
# ---------------------------------------------------------------------------

# Hard-block patterns: strong indicators of prompt injection — always block
INJECTION_PATTERNS: list[str] = [
    r"\bignore\b.{0,40}\b(?:previous|prior|above|all)\b.{0,40}\b(?:instructions?|prompts?|rules?|directives?)\b",
    r"\bdisregard\b.{0,40}\b(?:instructions?|prompts?|rules?|context)\b",
    r"\byou\s+are\s+now\b",
    r"\bact\s+as\b.{0,30}\b(?:a\s+)?(?:different|new|another|unrestricted)\b",
    r"\bnew\s+(?:role|persona|instructions?|prompt)\b",
    r"\bforget\b.{0,30}\b(?:everything|all|your|previous)\b",
    r"\boverride\b.{0,40}\b(?:instructions?|rules?|settings?|system)\b",
    r"\bsystem\s*prompt\b",
    r"\bjailbreak\b",
    r"\bdo\s+anything\s+now\b",
    r"\bdan\s+mode\b",
    r"\breveal\s+(?:your|the)\s+(?:instructions|prompt|system)\b",
    r"\bnew\s+instructions?\s*:",
    r"\\x[0-9a-fA-F]{2}",
    r"<script|javascript:|onclick|onerror",
]

# Soft-block patterns: suspicious SQL keywords that may be legitimate
SUSPICIOUS_PATTERNS: list[str] = [
    r"\bdrop\s+table\b",
    r"\bdrop\s+database\b",
    r"\bdelete\s+from\b",
    r"\btruncate\s+(?:table\s+)?\w",
    r"\binsert\s+into\b",
    r"\bupdate\s+\w+\s+set\b",
    r"\balter\s+table\b",
    r"\bcreate\s+table\b",
    r"\bcreate\s+or\s+replace\b",
    r"\bgrant\s+\w",
    r"\bexec(?:ute)?\s*\(",
    r"\bxp_cmdshell\b",
    r";\s*(?:drop|delete|insert|update|alter|create)",
]

# SQL injection tautologies and comment sequences
_SQL_INJECTION_PATTERNS: list[str] = [
    r"--\s",
    r";\s*--",
    r"\bor\s*1\s*=\s*1\b",
    r"\band\s*1\s*=\s*1\b",
    r"'\s*or\s*'",
    r"'\s*;\s*",
    r"\bor\s*\(\s*1\s*=\s*1\s*\)",
    r"\bwaitfor\s+delay\b",
    r"\bpg_sleep\b",
    r"\bsleep\s*\(",
    r"\bbenchmark\s*\(",
]

# Role confusion / privilege escalation
_ROLE_CONFUSION_PATTERNS: list[str] = [
    r"\bi\s+am\b.{0,40}\b(?:an?\s+)?(?:admin(?:istrator)?|developer|dev|superuser|root|dba|system|operator)\b",
    r"\bas\s+(?:an?\s+)?(?:admin(?:istrator)?|developer|dev|superuser|root|dba|system)\b",
    r"\bmy\s+role\s+is\b.{0,40}\b(?:admin|developer|system|root)\b",
    r"\bauthorized\s+(?:user|personnel|admin)\b",
    r"\bi\s+have\s+(?:admin|root|elevated|special)\s+(?:access|privileges?|permissions?)\b",
    r"\bwith\s+(?:admin|root|elevated)\s+(?:privileges?|permissions?|rights?)\b",
]

# Schema exfiltration
_SCHEMA_EXFILTRATION_PATTERNS: list[str] = [
    r"\b(?:show|list|print|display|reveal|dump|give\s+me|tell\s+me)\b.{0,50}\b(?:tables?|columns?|schema|database\s+structure|your\s+(?:instructions?|prompt|rules?|system))\b",
    r"\blist\s+all\s+(?:tables?|columns?|databases?)\b",
    r"\byour\s+(?:instructions?|system\s*prompt|rules?|directives?|constraints?)\b",
    r"\bprint\s+your\b",
    r"\brepeat\s+(?:your|the)\s+(?:instructions?|prompt|rules?)\b",
    r"\bshow\s+me\s+your\b",
    r"\bwhat\s+are\s+your\s+(?:instructions?|rules?|guidelines?|constraints?)\b",
    r"\binformation_schema\b",
    r"\bpg_catalog\b",
    r"\bsys\.(?:tables|columns|objects)\b",
]

# Combined for backward compatibility
ALL_PATTERNS = SUSPICIOUS_PATTERNS + INJECTION_PATTERNS


# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

def _compile(patterns: list[str]) -> list[re.Pattern[str]]:
    return [re.compile(p, re.IGNORECASE) for p in patterns]


_COMPILED_INJECTION = _compile(INJECTION_PATTERNS)
_COMPILED_SUSPICIOUS = _compile(SUSPICIOUS_PATTERNS)
_COMPILED_SQL_INJECTION = _compile(_SQL_INJECTION_PATTERNS)
_COMPILED_ROLE_CONFUSION = _compile(_ROLE_CONFUSION_PATTERNS)
_COMPILED_EXFILTRATION = _compile(_SCHEMA_EXFILTRATION_PATTERNS)


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _normalize_unicode(text: str) -> str:
    """Normalize Unicode to NFKC and strip zero-width/invisible characters."""
    normalized = unicodedata.normalize("NFKC", text)
    normalized = re.sub(r"[\u00ad\u200b-\u200f\u202a-\u202e\ufeff]", "", normalized)
    return normalized


def _try_decode_base64(text: str) -> Optional[str]:
    """Attempt to decode base64-looking substrings (>= 16 chars).

    Returns decoded content if successful, None otherwise.
    """
    candidates = re.findall(r"[A-Za-z0-9+/]{16,}={0,2}", text)
    for candidate in candidates[:5]:  # limit to avoid excessive processing
        try:
            decoded = base64.b64decode(candidate).decode("utf-8", errors="strict")
            if decoded.isprintable() and len(decoded) <= 2000:
                return decoded
        except Exception:
            continue
    return None


def _matches_any(text: str, patterns: list[re.Pattern[str]]) -> Optional[str]:
    """Return the matched pattern string if any pattern matches, else None."""
    for p in patterns:
        if p.search(text):
            return p.pattern
    return None


# ---------------------------------------------------------------------------
# Individual checks — each returns a list of flag strings
# ---------------------------------------------------------------------------

def _check_prompt_injection(text: str) -> list[str]:
    pattern = _matches_any(text, _COMPILED_INJECTION)
    if pattern:
        return [f"INJECTION: prompt injection attempt ({pattern})"]
    return []


def _check_sql_injection(text: str) -> list[str]:
    flags: list[str] = []
    pattern = _matches_any(text, _COMPILED_SUSPICIOUS)
    if pattern:
        flags.append(f"Suspicious pattern: {pattern}")
    pattern = _matches_any(text, _COMPILED_SQL_INJECTION)
    if pattern:
        flags.append(f"INJECTION: SQL injection attempt ({pattern})")
    return flags


def _check_role_confusion(text: str) -> list[str]:
    pattern = _matches_any(text, _COMPILED_ROLE_CONFUSION)
    if pattern:
        return [f"INJECTION: role confusion / privilege escalation ({pattern})"]
    return []


def _check_schema_exfiltration(text: str) -> list[str]:
    pattern = _matches_any(text, _COMPILED_EXFILTRATION)
    if pattern:
        return [f"INJECTION: schema exfiltration attempt ({pattern})"]
    return []


def _check_encoded_attacks(original_text: str) -> list[str]:
    """Check for obfuscated attacks via Unicode normalization or base64."""
    normalized = _normalize_unicode(original_text)
    if normalized != original_text:
        for checker in (_check_prompt_injection, _check_sql_injection,
                        _check_role_confusion, _check_schema_exfiltration):
            flags = checker(normalized)
            if any(f.startswith("INJECTION:") for f in flags):
                return ["INJECTION: obfuscated attack detected (Unicode normalization)"]

    decoded = _try_decode_base64(original_text)
    if decoded:
        for checker in (_check_prompt_injection, _check_sql_injection,
                        _check_role_confusion, _check_schema_exfiltration):
            flags = checker(decoded)
            if any(f.startswith("INJECTION:") for f in flags):
                return ["INJECTION: obfuscated attack detected (base64 payload)"]

    return []


# ---------------------------------------------------------------------------
# Public API (matches orchestrator's expected interface)
# ---------------------------------------------------------------------------

def check_input_safety(question: str) -> list[str]:
    """Validate user input before it reaches the LLM or query layer.

    Runs six checks:
      1. Prompt injection
      2. SQL injection (keywords + tautologies)
      3. Role confusion / privilege escalation
      4. Schema / system-prompt exfiltration
      5. Encoded / obfuscated attacks (Unicode + base64)

    Returns a list of detected issues. Empty list means safe.
    """
    if not isinstance(question, str):
        return ["INJECTION: input must be a string"]

    text = question.strip()
    if not text:
        return ["INJECTION: empty input"]

    flags: list[str] = []
    flags.extend(_check_prompt_injection(text))
    flags.extend(_check_sql_injection(text))
    flags.extend(_check_role_confusion(text))
    flags.extend(_check_schema_exfiltration(text))
    flags.extend(_check_encoded_attacks(text))
    return flags


def is_hard_block(flags: list[str]) -> bool:
    """Determine if the detected flags warrant a hard block.

    Returns True if any flag is an injection-class issue (prefixed with
    "INJECTION:") as opposed to a suspicious-but-possibly-legitimate keyword.
    """
    return any(f.startswith("INJECTION:") for f in flags)
