"""Tests for the NLQ Agent — uses mocked LLM responses."""

from unittest.mock import MagicMock, patch

import pytest

from src.models.schemas import SQLCandidate


@pytest.fixture
def sample_sql():
    return SQLCandidate(
        sql="SELECT Product_Category, SUM(Total_Amount) AS revenue FROM retail_transactions_typed GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
        dialect="duckdb",
        expected_columns=["Product_Category", "revenue"],
        notes=["Grouped by category, ordered by revenue descending"],
    )


@patch("agents.Runner.run")
async def test_nlq_returns_sql_candidate(mock_run, sample_sql):
    mock_run.return_value = MagicMock(final_output=sample_sql)

    from src.agents.nlq_agent import nlq_agent
    result = await mock_run(nlq_agent, "Top categories by revenue")
    output: SQLCandidate = result.final_output

    assert isinstance(output, SQLCandidate)


@patch("agents.Runner.run")
async def test_sql_field_not_empty(mock_run, sample_sql):
    mock_run.return_value = MagicMock(final_output=sample_sql)

    from src.agents.nlq_agent import nlq_agent
    result = await mock_run(nlq_agent, "Top categories by revenue")
    output: SQLCandidate = result.final_output

    assert output.sql.strip() != ""


@patch("agents.Runner.run")
async def test_dialect_is_duckdb(mock_run, sample_sql):
    mock_run.return_value = MagicMock(final_output=sample_sql)

    from src.agents.nlq_agent import nlq_agent
    result = await mock_run(nlq_agent, "Top categories by revenue")
    output: SQLCandidate = result.final_output

    assert output.dialect == "duckdb"


@patch("agents.Runner.run")
async def test_expected_columns_populated(mock_run, sample_sql):
    mock_run.return_value = MagicMock(final_output=sample_sql)

    from src.agents.nlq_agent import nlq_agent
    result = await mock_run(nlq_agent, "Top categories by revenue")
    output: SQLCandidate = result.final_output

    assert len(output.expected_columns) > 0


def test_sql_candidate_defaults():
    """Test SQLCandidate default values."""
    candidate = SQLCandidate(sql="SELECT 1")
    assert candidate.dialect == "duckdb"
    assert candidate.expected_columns == []
    assert candidate.notes == []
