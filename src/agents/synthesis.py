"""SynthesisAgent — merges all outputs into the final user-facing response."""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.connectors.azure_openai_connector import get_model_name
from src.models.schemas import FinalResponse
from src.prompts import load_prompt

synthesis_agent = Agent(
    name="SynthesisAgent",
    instructions=load_prompt("synthesis"),
    model=get_model_name("gpt-4o"),
    output_type=AgentOutputSchema(FinalResponse, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)
