"""TechnicalSpecAgent — rewrites the business context into a precise SQL task spec."""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.connectors.azure_openai_connector import get_model_name
from src.models.schemas import TechnicalSpec
from src.prompts import load_prompt

technical_spec_agent = Agent(
    name="TechnicalSpecAgent",
    instructions=load_prompt("technical_spec"),
    model=get_model_name("gpt-4o-mini"),
    output_type=AgentOutputSchema(TechnicalSpec, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)