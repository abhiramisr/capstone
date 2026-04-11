"""Tests for the output guardrail — validates response quality checks."""

from src.guardrails.output_guardrail import check_output_safety, has_critical_issues


def test_valid_response_passes():
    """A well-formed response with numbers and data references should pass."""
    issues = check_output_safety(
        answer="Total revenue was $1,234,567 across 5 categories.",
        sql="SELECT SUM(Total_Amount) FROM retail_transactions_typed",
        analysis="Revenue analysis complete.",
        preview_rows=[{"Total_Amount": 1234567}],
    )
    # Should have no critical issues
    assert not has_critical_issues(issues)


def test_empty_answer_flagged():
    """An empty answer should be flagged as critical."""
    issues = check_output_safety(answer="", sql="SELECT 1")
    assert len(issues) >= 1
    assert any("CRITICAL" in i for i in issues)
    assert has_critical_issues(issues)


def test_whitespace_answer_flagged():
    """A whitespace-only answer should be flagged as critical."""
    issues = check_output_safety(answer="   \n\t  ")
    assert has_critical_issues(issues)


def test_no_numbers_flagged():
    """An answer without numbers when results exist should be warned."""
    issues = check_output_safety(
        answer="Sales were high across many categories.",
        sql="SELECT * FROM t",
        preview_rows=[{"Total_Amount": 500}],
    )
    assert any("no numeric" in i.lower() for i in issues)


def test_hedging_language_flagged():
    """Hedging phrases should trigger a warning."""
    issues = check_output_safety(
        answer="I think the total might be around 500.",
        sql="SELECT SUM(x) FROM t",
        preview_rows=[{"x": 500}],
    )
    assert any("hedging" in i.lower() for i in issues)


def test_no_sql_with_analysis_flagged():
    """Analysis without SQL should be warned."""
    issues = check_output_safety(
        answer="Revenue is $1000.",
        sql="",
        analysis="Detailed analysis here.",
    )
    assert any("no SQL" in i for i in issues)


def test_grounded_answer_passes():
    """An answer referencing actual result values should not flag grounding."""
    issues = check_output_safety(
        answer="The top category is Electronics with 1500 transactions.",
        sql="SELECT Product_Category, COUNT(*) FROM t GROUP BY 1",
        preview_rows=[{"Product_Category": "Electronics", "count": 1500}],
    )
    # Should not have critical issues
    assert not has_critical_issues(issues)


def test_has_critical_issues_helper():
    """Test the has_critical_issues helper function."""
    assert has_critical_issues(["CRITICAL: something bad"]) is True
    assert has_critical_issues(["WARNING: minor thing"]) is False
    assert has_critical_issues([]) is False
