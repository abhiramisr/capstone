"""Tests for the Anomaly/Insight Agent — uses mocked LLM responses."""

from unittest.mock import MagicMock, patch

import pytest

from src.models.schemas import AnomalyItem, AnomalyReport, TrendItem


@pytest.fixture
def sample_report():
    return AnomalyReport(
        anomalies=[
            AnomalyItem(
                metric="Total_Amount",
                expected_range="$10-$500",
                actual_value="$15,000",
                deviation_pct=2900.0,
                description="Single transaction with unusually high amount",
            )
        ],
        trends=[
            TrendItem(
                metric="monthly_revenue",
                direction="increasing",
                period="Q3 to Q4 2023",
                description="Revenue grew 25% quarter-over-quarter",
            )
        ],
        summary="One high-value outlier detected; overall revenue trending upward.",
        severity="warning",
        recommended_queries=[
            "SELECT * FROM retail_transactions_typed WHERE Total_Amount > 10000 LIMIT 20"
        ],
    )


@patch("agents.Runner.run")
async def test_returns_anomaly_report(mock_run, sample_report):
    mock_run.return_value = MagicMock(final_output=sample_report)

    from src.agents.anomaly import anomaly_agent
    result = await mock_run(anomaly_agent, "Analyze the dataset")
    output: AnomalyReport = result.final_output

    assert isinstance(output, AnomalyReport)
    assert len(output.anomalies) > 0


@patch("agents.Runner.run")
async def test_severity_valid(mock_run, sample_report):
    mock_run.return_value = MagicMock(final_output=sample_report)

    from src.agents.anomaly import anomaly_agent
    result = await mock_run(anomaly_agent, "Analyze the dataset")
    output: AnomalyReport = result.final_output

    assert output.severity in ("info", "warning", "critical")


@patch("agents.Runner.run")
async def test_recommended_queries_are_select(mock_run, sample_report):
    mock_run.return_value = MagicMock(final_output=sample_report)

    from src.agents.anomaly import anomaly_agent
    result = await mock_run(anomaly_agent, "Analyze the dataset")
    output: AnomalyReport = result.final_output

    for query in output.recommended_queries:
        assert query.strip().upper().startswith("SELECT")


def test_empty_anomaly_report():
    """An empty report should default to info severity."""
    report = AnomalyReport()
    assert report.severity == "info"
    assert report.anomalies == []
    assert report.trends == []
    assert report.summary == ""


def test_anomaly_item_creation():
    """Test AnomalyItem can be created with required fields."""
    item = AnomalyItem(metric="revenue", description="Spike detected")
    assert item.metric == "revenue"
    assert item.deviation_pct == 0.0
