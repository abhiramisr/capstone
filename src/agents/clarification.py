"""Clarification Agent — detects vague queries and asks follow-up questions.

Receives a user question and decides if it is clear enough to generate SQL.
If unclear, returns a clarifying question. If clear, passes to Query Router.
"""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.connectors.azure_openai_connector import get_model_name
from src.models.schemas import ClarificationResult
from src.prompts import load_prompt

clarification_agent = Agent(
    name="ClarificationAgent",
    instructions=load_prompt("clarification"),
    model=get_model_name("gpt-4o-mini"),
    output_type=AgentOutputSchema(ClarificationResult, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)