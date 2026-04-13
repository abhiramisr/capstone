"""NLQ Agent — converts natural language questions to SQL.

Merges the responsibilities of BusinessContextAgent + TechnicalSpecAgent + SQLWriterAgent
into a single focused agent. Takes a user question + schema, produces a SQLCandidate.
"""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.connectors.azure_openai_connector import get_model_name
from src.models.schemas import SQLCandidate
from src.prompts import load_prompt

nlq_agent = Agent(
    name="NLQAgent",
    instructions=load_prompt("nlq_agent"),
    model=get_model_name("gpt-4o-mini"),
    output_type=AgentOutputSchema(SQLCandidate, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)