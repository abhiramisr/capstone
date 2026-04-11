"""SQLWriterAgent — generates a single DuckDB SQL query from the technical spec."""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.models.schemas import SQLCandidate
from src.prompts import load_prompt

sql_writer_agent = Agent(
    name="SQLWriterAgent",
    instructions=load_prompt("sql_writer"),
    model="gpt-4o-mini",
    output_type=AgentOutputSchema(SQLCandidate, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)
