"""
Input guardrail for a Natural Language to SQL agent operating against a financial
data warehouse. Validates user questions before they reach the LLM or query layer.

Usage:
    from guardrails.input_guardrail import check_input

    result = check_input("Show me total revenue by region")
    if not result["allowed"]:
        print(result["reason"])
"""

import base64
import re
import unicodedata


# ---------------------------------------------------------------------------
# Pattern definitions
# ---------------------------------------------------------------------------

_PROMPT_INJECTION_PATTERNS = [
    r"\bignore\b.{0,40}\b(previous|prior|above|all)\b.{0,40}\b(instructions?|prompts?|rules?|directives?)\b",
    r"\bdisregard\b.{0,40}\b(instructions?|prompts?|rules?|context)\b",
    r"\byou\s+are\s+now\b",
    r"\bact\s+as\b.{0,30}\b(a\s+)?(different|new|another|unrestricted)\b",
    r"\bnew\s+(role|persona|instructions?|prompt)\b",
    r"\bforget\b.{0,30}\b(everything|all|your|previous)\b",
    r"\boverride\b.{0,40}\b(instructions?|rules?|settings?|system)\b",
    r"\bsystem\s*prompt\b",
    r"\bjailbreak\b",
    r"\bdo\s+anything\s+now\b",
    r"\bdan\s+mode\b",
]

_SQL_INJECTION_PHRASES = [
    r"\bdrop\s+table\b",
    r"\bdrop\s+database\b",
    r"\bdelete\s+from\b",
    r"\btruncate\s+(table\s+)?\w",
    r"\binsert\s+into\b",
    r"\bupdate\s+\w+\s+set\b",
    r"\balter\s+table\b",
    r"\bcreate\s+table\b",
    r"\bcreate\s+or\s+replace\b",
    r"\bgrant\s+\w",
    r"\bexec(ute)?\s*\(",
    r"\bxp_cmdshell\b",
    r"--\s",           # SQL line comment
    r";\s*--",
    r"\bor\s+1\s*=\s*1\b",
    r"\band\s+1\s*=\s*1\b",
    r"'\s*or\s*'",
    r"'\s*;\s*",
]

_ROLE_CONFUSION_PATTERNS = [
    r"\bi\s+am\b.{0,40}\b(an?\s+)?(admin(istrator)?|developer|dev|superuser|root|dba|system|operator)\b",
    r"\bas\s+(an?\s+)?(admin(istrator)?|developer|dev|superuser|root|dba|system)\b",
    r"\bmy\s+role\s+is\b.{0,40}\b(admin|developer|system|root)\b",
    r"\bauthorized\s+(user|personnel|admin)\b",
    r"\bi\s+have\s+(admin|root|elevated|special)\s+(access|privileges?|permissions?)\b",
    r"\bwith\s+(admin|root|elevated)\s+(privileges?|permissions?|rights?)\b",
]

_SCHEMA_EXFILTRATION_PATTERNS = [
    r"\b(show|list|print|display|reveal|dump|give\s+me|tell\s+me)\b.{0,50}\b(tables?|columns?|schema|database\s+structure|your\s+(instructions?|prompt|rules?|system))\b",
    r"\bwhat\s+tables?\b",
    r"\bwhat\s+columns?\b",
    r"\blist\s+all\s+(tables?|columns?|databases?)\b",
    r"\byour\s+(instructions?|system\s*prompt|rules?|directives?|constraints?)\b",
    r"\bprint\s+your\b",
    r"\brepeat\s+(your|the)\s+(instructions?|prompt|rules?)\b",
    r"\bshow\s+me\s+your\b",
    r"\bwhat\s+are\s+your\s+(instructions?|rules?|guidelines?|constraints?)\b",
    r"\binformation_schema\b",
    r"\bpg_catalog\b",
    r"\bsys\.(tables|columns|objects)\b",
]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _compile(patterns: list[str]) -> list[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in patterns]


_COMPILED_INJECTION      = _compile(_PROMPT_INJECTION_PATTERNS)
_COMPILED_SQL_INJECTION  = _compile(_SQL_INJECTION_PHRASES)
_COMPILED_ROLE_CONFUSION = _compile(_ROLE_CONFUSION_PATTERNS)
_COMPILED_EXFILTRATION   = _compile(_SCHEMA_EXFILTRATION_PATTERNS)


def _normalize_unicode(text: str) -> str:
    """
    Normalize Unicode to its canonical composed form and strip non-ASCII
    lookalike characters that might be used to bypass keyword matching.
    Returns a cleaned string safe for pattern matching.
    """
    normalized = unicodedata.normalize("NFKC", text)
    # Replace zero-width characters and soft hyphens
    normalized = re.sub(r"[\u00ad\u200b-\u200f\u202a-\u202e\ufeff]", "", normalized)
    return normalized


def _try_decode_base64(text: str) -> str | None:
    """
    Attempt to decode any base64-looking substrings found in the text.
    Returns decoded content if successful, None otherwise.
    Base64 blobs of ≥16 characters are considered candidates.
    """
    candidates = re.findall(r"[A-Za-z0-9+/]{16,}={0,2}", text)
    for candidate in candidates:
        try:
            decoded = base64.b64decode(candidate).decode("utf-8", errors="strict")
            if decoded.isprintable():
                return decoded
        except Exception:
            continue
    return None


def _matches_any(text: str, patterns: list[re.Pattern]) -> bool:
    return any(p.search(text) for p in patterns)


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_prompt_injection(text: str) -> dict:
    """
    Detect attempts to override or hijack the agent's system instructions.
    Looks for phrases like 'ignore previous instructions', 'you are now',
    'disregard your rules', or known jailbreak keywords.
    """
    if _matches_any(text, _COMPILED_INJECTION):
        return {"allowed": False, "reason": "Blocked: prompt injection attempt detected."}
    return {"allowed": True}


def _check_encoded_attack(original_text: str) -> dict:
    """
    Detect obfuscated malicious payloads using base64 encoding or Unicode
    lookalike substitutions. Decodes/normalizes the input and re-runs
    injection and SQL checks against the cleaned version.
    """
    # Check Unicode normalization reveals something suspicious
    normalized = _normalize_unicode(original_text)
    if normalized != original_text:
        for check in (_check_prompt_injection, _check_sql_injection,
                      _check_role_confusion, _check_schema_exfiltration):
            result = check(normalized)
            if not result["allowed"]:
                return {
                    "allowed": False,
                    "reason": "Blocked: obfuscated/encoded attack detected (Unicode normalization).",
                }

    # Check base64-decoded content
    decoded = _try_decode_base64(original_text)
    if decoded:
        for check in (_check_prompt_injection, _check_sql_injection,
                      _check_role_confusion, _check_schema_exfiltration):
            result = check(decoded)
            if not result["allowed"]:
                return {
                    "allowed": False,
                    "reason": "Blocked: obfuscated/encoded attack detected (base64 payload).",
                }

    return {"allowed": True}


def _check_sql_injection(text: str) -> dict:
    """
    Detect natural-language SQL injection attempts. Flags phrases that map
    to destructive or manipulative SQL operations such as DROP TABLE,
    DELETE FROM, TRUNCATE, INSERT INTO, comment sequences (--), and
    classic tautologies like OR 1=1.
    """
    if _matches_any(text, _COMPILED_SQL_INJECTION):
        return {"allowed": False, "reason": "Blocked: SQL injection attempt detected."}
    return {"allowed": True}


def _check_role_confusion(text: str) -> dict:
    """
    Detect attempts by the user to claim a privileged identity (admin,
    developer, DBA, root, etc.) in order to unlock restricted behaviour
    or bypass authorization controls.
    """
    if _matches_any(text, _COMPILED_ROLE_CONFUSION):
        return {"allowed": False, "reason": "Blocked: role confusion / privilege escalation attempt detected."}
    return {"allowed": True}


def _check_schema_exfiltration(text: str) -> dict:
    """
    Detect attempts to extract schema metadata, system prompt contents, or
    internal instructions. Flags requests to list tables/columns, reveal
    the system prompt, or query database catalog views directly.
    """
    if _matches_any(text, _COMPILED_EXFILTRATION):
        return {"allowed": False, "reason": "Blocked: attempt to exfiltrate schema or system prompt detected."}
    return {"allowed": True}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check_input(user_question: str) -> dict:
    """
    Evaluate a user question before it is passed to the NL-to-SQL agent.

    Runs five sequential checks:
      1. Prompt injection
      2. Encoded / obfuscated attacks
      3. SQL injection via natural language
      4. Role confusion / privilege escalation
      5. Schema or system-prompt exfiltration

    Args:
        user_question: The raw question string submitted by the user.

    Returns:
        {"allowed": True} if the question is considered safe, or
        {"allowed": False, "reason": "<explanation>"} if it is blocked.
    """
    if not isinstance(user_question, str):
        return {"allowed": False, "reason": "Blocked: input must be a string."}

    question = user_question.strip()

    if not question:
        return {"allowed": False, "reason": "Blocked: empty input."}

    checks = [
        _check_prompt_injection,
        _check_sql_injection,
        _check_role_confusion,
        _check_schema_exfiltration,
    ]

    for check in checks:
        result = check(question)
        if not result["allowed"]:
            return result

    # Encoded-attack check runs last because it internally re-runs the above checks
    encoded_result = _check_encoded_attack(question)
    if not encoded_result["allowed"]:
        return encoded_result

    return {"allowed": True}
