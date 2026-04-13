"""Anomaly/Insight Agent — proactive trend and anomaly detection.

Runs independently on a schedule, not triggered by user queries.
Monitors data for anomalies and trends, surfaces proactive alerts.
"""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.connectors.azure_openai_connector import get_model_name
from src.models.schemas import AnomalyReport
from src.prompts import load_prompt

anomaly_agent = Agent(
    name="AnomalyAgent",
    instructions=load_prompt("anomaly"),
    model=get_model_name("gpt-4o"),
    output_type=AgentOutputSchema(AnomalyReport, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0.2),
)