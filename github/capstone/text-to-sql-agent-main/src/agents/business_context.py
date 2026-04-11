"""BusinessContextAgent — interprets the business intent behind a user question."""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.models.schemas import BusinessContext
from src.prompts import load_prompt

business_context_agent = Agent(
    name="BusinessContextAgent",
    instructions=load_prompt("business_context"),
    model="gpt-4o-mini",
    output_type=AgentOutputSchema(BusinessContext, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)
