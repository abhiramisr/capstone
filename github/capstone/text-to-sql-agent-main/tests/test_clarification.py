"""Tests for the Clarification Agent — uses mocked LLM responses."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models.schemas import ClarificationResult


@pytest.fixture
def clear_result():
    return ClarificationResult(
        is_clear=True,
        clarifying_question="",
        original_question="What were total sales last month?",
        interpreted_intent="Sum of Total_Amount for the previous calendar month",
        confidence=0.95,
    )


@pytest.fixture
def unclear_result():
    return ClarificationResult(
        is_clear=False,
        clarifying_question="Could you specify which metric you're interested in — revenue, number of orders, or something else?",
        original_question="Tell me about the data",
        interpreted_intent="",
        confidence=0.3,
    )


@patch("agents.Runner.run")
async def test_clear_question_passes(mock_run, clear_result):
    mock_run.return_value = MagicMock(final_output=clear_result)

    from src.agents.clarification import clarification_agent
    result = await mock_run(clarification_agent, "What were total sales last month?")
    output: ClarificationResult = result.final_output

    assert output.is_clear is True
    assert output.clarifying_question == ""
    assert output.confidence > 0.5


@patch("agents.Runner.run")
async def test_unclear_question_returns_clarification(mock_run, unclear_result):
    mock_run.return_value = MagicMock(final_output=unclear_result)

    from src.agents.clarification import clarification_agent
    result = await mock_run(clarification_agent, "Tell me about the data")
    output: ClarificationResult = result.final_output

    assert output.is_clear is False
    assert len(output.clarifying_question) > 0


@patch("agents.Runner.run")
async def test_original_question_echoed(mock_run, clear_result):
    mock_run.return_value = MagicMock(final_output=clear_result)

    from src.agents.clarification import clarification_agent
    result = await mock_run(clarification_agent, "What were total sales last month?")
    output: ClarificationResult = result.final_output

    assert output.original_question == "What were total sales last month?"


def test_confidence_range():
    """Confidence must be between 0 and 1."""
    result = ClarificationResult(
        is_clear=True,
        original_question="test",
        confidence=0.85,
    )
    assert 0.0 <= result.confidence <= 1.0
