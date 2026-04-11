"""SynthesisAgent — merges all outputs into the final user-facing response."""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.models.schemas import FinalResponse
from src.prompts import load_prompt

synthesis_agent = Agent(
    name="SynthesisAgent",
    instructions=load_prompt("synthesis"),
    model="gpt-4o",
    output_type=AgentOutputSchema(FinalResponse, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)
