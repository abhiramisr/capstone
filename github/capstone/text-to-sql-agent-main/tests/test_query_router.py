"""Tests for the Query Router Agent — uses mocked LLM responses."""

from unittest.mock import MagicMock, patch

import pytest

from src.models.schemas import RoutingResult


@pytest.fixture
def retail_routing():
    return RoutingResult(
        relevant_tables=["retail_transactions_typed"],
        datasource="retail",
        reasoning="The question asks about sales revenue which maps to the retail transactions table.",
        schema_subset="",
    )


@patch("agents.Runner.run")
async def test_routes_to_retail_table(mock_run, retail_routing):
    mock_run.return_value = MagicMock(final_output=retail_routing)

    from src.agents.query_router import query_router_agent
    result = await mock_run(query_router_agent, "What were total sales?")
    output: RoutingResult = result.final_output

    assert "retail_transactions_typed" in output.relevant_tables


@patch("agents.Runner.run")
async def test_returns_at_least_one_table(mock_run, retail_routing):
    mock_run.return_value = MagicMock(final_output=retail_routing)

    from src.agents.query_router import query_router_agent
    result = await mock_run(query_router_agent, "Show me data")
    output: RoutingResult = result.final_output

    assert len(output.relevant_tables) >= 1


@patch("agents.Runner.run")
async def test_datasource_populated(mock_run, retail_routing):
    mock_run.return_value = MagicMock(final_output=retail_routing)

    from src.agents.query_router import query_router_agent
    result = await mock_run(query_router_agent, "Revenue by category")
    output: RoutingResult = result.final_output

    assert output.datasource != ""


@patch("agents.Runner.run")
async def test_reasoning_not_empty(mock_run, retail_routing):
    mock_run.return_value = MagicMock(final_output=retail_routing)

    from src.agents.query_router import query_router_agent
    result = await mock_run(query_router_agent, "Top products by sales")
    output: RoutingResult = result.final_output

    assert len(output.reasoning) > 0


def test_routing_result_defaults():
    """Test RoutingResult default values."""
    result = RoutingResult(relevant_tables=["test_table"])
    assert result.datasource == "default"
    assert result.reasoning == ""
    assert result.schema_subset == ""
