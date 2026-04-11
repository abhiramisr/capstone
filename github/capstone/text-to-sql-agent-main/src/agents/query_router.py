"""Query Router Agent — dynamically routes to correct tables/data source.

Given a clear question, reads the schema config and decides which tables
are relevant. Returns a list of relevant tables to the NLQ Agent.
"""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.models.schemas import RoutingResult
from src.prompts import load_prompt

query_router_agent = Agent(
    name="QueryRouterAgent",
    instructions=load_prompt("query_router"),
    model="gpt-4o-mini",
    output_type=AgentOutputSchema(RoutingResult, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)
