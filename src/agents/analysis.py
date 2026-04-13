"""AnalysisAgent — writes a structured analytical report from query results."""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.models.schemas import AnalysisReport
from src.prompts import load_prompt

analysis_agent = Agent(
    name="AnalysisAgent",
    instructions=load_prompt("analysis"),
    model="gpt-4o",
    output_type=AgentOutputSchema(AnalysisReport, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)
