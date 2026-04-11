"""SQLGuardrailEvaluatorAgent — advisory safety/correctness review of SQL."""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.models.schemas import ValidationResult
from src.prompts import load_prompt

sql_evaluator_agent = Agent(
    name="SQLGuardrailEvaluatorAgent",
    instructions=load_prompt("sql_evaluator"),
    model="gpt-4o-mini",
    output_type=AgentOutputSchema(ValidationResult, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)
