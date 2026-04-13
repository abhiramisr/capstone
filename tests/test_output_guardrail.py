"""Tests for the output guardrail — validates response quality checks."""

from src.guardrails.output_guardrail import check_output_safety, has_critical_issues


# ---------------------------------------------------------------------------
# Valid responses
# ---------------------------------------------------------------------------

def test_valid_response_passes():
    """A well-formed response with numbers and data references should pass."""
    issues = check_output_safety(
        answer="Total revenue was $1,234,567 across 5 categories.",
        sql="SELECT SUM(Total_Amount) FROM retail_transactions_typed",
        analysis="Revenue analysis complete.",
        preview_rows=[{"Total_Amount": 1234567}],
    )
    assert not has_critical_issues(issues)


def test_grounded_answer_passes():
    """An answer referencing actual result values should not flag grounding."""
    issues = check_output_safety(
        answer="The top category is Electronics with 1500 transactions.",
        sql="SELECT Product_Category, COUNT(*) FROM t GROUP BY 1",
        preview_rows=[{"Product_Category": "Electronics", "count": 1500}],
    )
    assert not has_critical_issues(issues)


# ---------------------------------------------------------------------------
# Empty / missing answer (critical)
# ---------------------------------------------------------------------------

def test_empty_answer_flagged():
    issues = check_output_safety(answer="", sql="SELECT 1")
    assert has_critical_issues(issues)


def test_whitespace_answer_flagged():
    issues = check_output_safety(answer="   \n\t  ")
    assert has_critical_issues(issues)


# ---------------------------------------------------------------------------
# SQL leakage (critical)
# ---------------------------------------------------------------------------

def test_sql_leak_select_from():
    issues = check_output_safety(
        answer="The query SELECT revenue FROM sales returned results.",
        sql="SELECT revenue FROM sales",
    )
    assert has_critical_issues(issues)
    assert any("SQL fragments" in i for i in issues)


def test_sql_leak_group_by():
    issues = check_output_safety(
        answer="Use GROUP BY category to aggregate.",
        sql="SELECT * FROM t",
    )
    assert has_critical_issues(issues)


def test_sql_leak_join():
    issues = check_output_safety(
        answer="The INNER JOIN between orders and customers shows...",
        sql="SELECT * FROM t",
    )
    assert has_critical_issues(issues)


# ---------------------------------------------------------------------------
# System prompt leakage (critical)
# ---------------------------------------------------------------------------

def test_system_prompt_leak():
    issues = check_output_safety(
        answer="I am an AI language model and cannot help with that.",
        sql="SELECT 1",
    )
    assert has_critical_issues(issues)
    assert any("system prompt" in i.lower() or "instruction" in i.lower() for i in issues)


def test_system_prompt_role_leak():
    issues = check_output_safety(
        answer="You are a SQL assistant agent designed to...",
        sql="SELECT 1",
    )
    assert has_critical_issues(issues)


# ---------------------------------------------------------------------------
# PII detection (critical)
# ---------------------------------------------------------------------------

def test_pii_email():
    issues = check_output_safety(
        answer="Contact john@example.com for details. Revenue is 500.",
        sql="SELECT * FROM t",
        preview_rows=[{"revenue": 500}],
    )
    assert has_critical_issues(issues)
    assert any("email" in i.lower() for i in issues)


def test_pii_phone():
    issues = check_output_safety(
        answer="Call (555) 123-4567 for support. Total is 100.",
        sql="SELECT * FROM t",
        preview_rows=[{"total": 100}],
    )
    assert has_critical_issues(issues)
    assert any("phone" in i.lower() for i in issues)


def test_pii_ssn():
    issues = check_output_safety(
        answer="The SSN is 123-45-6789.",
        sql="SELECT * FROM t",
    )
    assert has_critical_issues(issues)
    assert any("SSN" in i for i in issues)


# ---------------------------------------------------------------------------
# Grounding / hedging (warning, not critical)
# ---------------------------------------------------------------------------

def test_no_numbers_flagged():
    issues = check_output_safety(
        answer="Sales were high across many categories.",
        sql="SELECT * FROM t",
        preview_rows=[{"Total_Amount": 500}],
    )
    assert any("no numeric" in i.lower() for i in issues)
    assert not has_critical_issues(issues)


def test_hedging_language_flagged():
    issues = check_output_safety(
        answer="I think the total might be around 500.",
        sql="SELECT SUM(x) FROM t",
        preview_rows=[{"x": 500}],
    )
    assert any("hedging" in i.lower() for i in issues)
    assert not has_critical_issues(issues)


def test_ungrounded_numbers_flagged():
    issues = check_output_safety(
        answer="The total was 9999 transactions.",
        sql="SELECT COUNT(*) FROM t",
        preview_rows=[{"count": 1500}],
    )
    assert any("don't appear" in i.lower() for i in issues)
    assert not has_critical_issues(issues)


def test_no_sql_with_analysis_flagged():
    issues = check_output_safety(
        answer="Revenue is $1000.",
        sql="",
        analysis="Detailed analysis here.",
    )
    assert any("no SQL" in i for i in issues)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def test_has_critical_issues_helper():
    assert has_critical_issues(["CRITICAL: something bad"]) is True
    assert has_critical_issues(["WARNING: minor thing"]) is False
    assert has_critical_issues([]) is False
