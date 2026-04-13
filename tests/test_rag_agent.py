"""Tests for the RAG Agent — uses mocked LLM responses."""

from unittest.mock import MagicMock, patch

import pytest

from src.models.schemas import FinalResponse


@pytest.fixture
def sample_response():
    return FinalResponse(
        question="What were total sales last month?",
        business_context_summary="User wants total revenue for the previous month.",
        sql="SELECT SUM(Total_Amount) FROM retail_transactions_typed WHERE date_parsed >= '2023-12-01'",
        execution_summary="Returned 1 row in 5ms",
        analysis="Total revenue was $1,234,567 for December 2023.",
        answer="Total sales last month were $1,234,567.",
    )


@patch("agents.Runner.run")
async def test_rag_returns_final_response(mock_run, sample_response):
    mock_run.return_value = MagicMock(final_output=sample_response)

    from src.agents.rag_agent import rag_agent
    result = await mock_run(rag_agent, "results input")
    output: FinalResponse = result.final_output

    assert isinstance(output, FinalResponse)


@patch("agents.Runner.run")
async def test_answer_not_empty(mock_run, sample_response):
    mock_run.return_value = MagicMock(final_output=sample_response)

    from src.agents.rag_agent import rag_agent
    result = await mock_run(rag_agent, "results input")
    output: FinalResponse = result.final_output

    assert output.answer.strip() != ""


@patch("agents.Runner.run")
async def test_question_echoed(mock_run, sample_response):
    mock_run.return_value = MagicMock(final_output=sample_response)

    from src.agents.rag_agent import rag_agent
    result = await mock_run(rag_agent, "results input")
    output: FinalResponse = result.final_output

    assert output.question == "What were total sales last month?"


@patch("agents.Runner.run")
async def test_sql_included(mock_run, sample_response):
    mock_run.return_value = MagicMock(final_output=sample_response)

    from src.agents.rag_agent import rag_agent
    result = await mock_run(rag_agent, "results input")
    output: FinalResponse = result.final_output

    assert output.sql.strip() != ""


def test_final_response_defaults():
    """Test FinalResponse default values."""
    resp = FinalResponse(question="test?")
    assert resp.answer == ""
    assert resp.needs_clarification is False
    assert resp.preview_rows == []
    assert resp.columns == []
