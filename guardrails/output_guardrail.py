"""
Output guardrail for a Natural Language to SQL RAG agent. Validates agent
responses after generation and before the answer is returned to the user.

Expected response shape:
    {
        "answer":    str,          # natural-language answer shown to the user
        "citations": list,         # source passages that ground the answer
        "sql":       str           # the SQL query that was executed
    }

Usage:
    from guardrails.output_guardrail import check_output

    result = check_output(response)
    if not result["allowed"]:
        print(result["reason"])
    else:
        show_to_user(result["response"])
"""

import re


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_answer(response: dict) -> str:
    return response.get("answer") or ""


def _get_citations(response: dict) -> list:
    return response.get("citations") or []


def _citation_corpus(citations: list) -> str:
    """
    Flatten all citation entries into one searchable string.
    Handles citations that are plain strings or dicts with varied key names.
    """
    parts = []
    for c in citations:
        if isinstance(c, str):
            parts.append(c)
        elif isinstance(c, dict):
            # Probe common field names used by different RAG frameworks
            for key in ("text", "content", "snippet", "passage", "body", "value", "source"):
                if key in c:
                    parts.append(str(c[key]))
                    break
            else:
                # Fallback: gather all string values
                parts.extend(str(v) for v in c.values() if isinstance(v, str))
    return " ".join(parts)


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
_PII_PATTERNS: dict[str, re.Pattern] = {
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

# Numeric tokens worth grounding: currency, percentages, and 4+ digit numbers
_NUMERIC_TOKEN_RE = re.compile(
    r"(?:"
    r"\$[\d,]+(?:\.\d+)?(?:\s?(?:million|billion|trillion|[MBTKk]))?"  # $1.2M / $1,234
    r"|£[\d,]+(?:\.\d+)?(?:\s?(?:million|billion|trillion|[MBTKk]))?"   # £500K
    r"|€[\d,]+(?:\.\d+)?(?:\s?(?:million|billion|trillion|[MBTKk]))?"   # €2B
    r"|[\d,]+(?:\.\d+)?%"                                               # 12.3%
    r"|\b\d{4,}\b"                                                      # 4+ digit plain number
    r")"
)

# Multi-word proper nouns: two or more consecutive title-cased words (≥2 chars each)
_NAMED_ENTITY_RE = re.compile(
    r"\b([A-Z][a-zA-Z]{1,}(?:\s+[A-Z][a-zA-Z]{1,})+)\b"
)

_COMPILED_SQL_LEAK          = [re.compile(p, re.IGNORECASE) for p in _SQL_LEAK_PATTERNS]
_COMPILED_SYSTEM_PROMPT_LEAK = [re.compile(p, re.IGNORECASE) for p in _SYSTEM_PROMPT_LEAK_PATTERNS]


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_missing_citations(response: dict) -> dict:
    """
    Ensure the response includes at least one citation. Answers with no
    citations cannot be verified as grounded and must be blocked entirely,
    as they may represent hallucinations or unverified content.
    """
    citations = _get_citations(response)
    if not citations:
        return {
            "allowed": False,
            "reason": "Blocked: response contains no citations; answer cannot be verified.",
        }
    return {"allowed": True}


def _check_grounding(response: dict) -> dict:
    """
    Verify that specific numeric values and multi-word named entities
    appearing in the answer are traceable to the citation corpus. Extracts
    currency amounts, percentages, large numbers, and proper-noun phrases
    from the answer and checks each against the combined citation text.
    Unrecognised values that appear in the answer but nowhere in the
    citations are treated as potential hallucinations.
    """
    answer = _get_answer(response)
    corpus = _citation_corpus(_get_citations(response))
    # Normalise commas so "1,234" matches "1234" in either source
    corpus_lower = corpus.lower().replace(",", "")

    ungrounded: list[str] = []

    for match in _NUMERIC_TOKEN_RE.finditer(answer):
        token = match.group(0).replace(",", "")
        if token.lower() not in corpus_lower:
            ungrounded.append(match.group(0))

    for match in _NAMED_ENTITY_RE.finditer(answer):
        entity = match.group(1)
        if entity.lower() not in corpus.lower():
            ungrounded.append(entity)

    if ungrounded:
        sample = ", ".join(dict.fromkeys(ungrounded))   # deduplicate, preserve order
        return {
            "allowed": False,
            "reason": (
                f"Blocked: answer references values not found in citations "
                f"(possible hallucination): {sample}."
            ),
        }
    return {"allowed": True}


def _check_sql_and_prompt_leakage(response: dict) -> dict:
    """
    Detect raw SQL fragments or system-prompt text that has leaked into the
    user-facing answer. SQL clauses (SELECT...FROM, GROUP BY, JOIN, etc.) and
    instruction-style phrases ('your task is', 'system prompt', model
    self-references) should never appear in prose shown to a user.
    """
    answer = _get_answer(response)

    for pattern in _COMPILED_SQL_LEAK:
        if pattern.search(answer):
            return {
                "allowed": False,
                "reason": "Blocked: answer contains raw SQL fragments that must not be shown to users.",
            }

    for pattern in _COMPILED_SYSTEM_PROMPT_LEAK:
        if pattern.search(answer):
            return {
                "allowed": False,
                "reason": "Blocked: answer contains system prompt or instruction text leakage.",
            }

    return {"allowed": True}


def _check_pii(response: dict) -> dict:
    """
    Scan the answer for personally identifiable information using regular
    expressions. Detects email addresses, US-format phone numbers, and
    Social Security Numbers (SSNs). Any match causes the response to be
    blocked to prevent inadvertent disclosure of personal data.
    """
    answer = _get_answer(response)

    for pii_type, pattern in _PII_PATTERNS.items():
        if pattern.search(answer):
            return {
                "allowed": False,
                "reason": f"Blocked: answer contains a {pii_type}, which must not be disclosed.",
            }

    return {"allowed": True}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check_output(response: dict) -> dict:
    """
    Validate an agent response before it is returned to the user.

    Runs four sequential checks in order of increasing cost:
      1. Missing citations        — fast structural check
      2. SQL / prompt leakage     — regex scan of the answer
      3. PII detection            — regex scan for personal data
      4. Grounding                — cross-reference answer values with citations

    Args:
        response: Dict with keys ``answer`` (str), ``citations`` (list),
                  and ``sql`` (str).

    Returns:
        ``{"allowed": True,  "response": response}`` when the response is safe.
        ``{"allowed": False, "reason": "<explanation>"}`` when it is blocked.
    """
    if not isinstance(response, dict):
        return {"allowed": False, "reason": "Blocked: response must be a dict."}

    checks = [
        _check_missing_citations,
        _check_sql_and_prompt_leakage,
        _check_pii,
        _check_grounding,
    ]

    for check in checks:
        result = check(response)
        if not result["allowed"]:
            return result

    return {"allowed": True, "response": response}
