"""SQLGuardrailEvaluatorAgent — advisory safety/correctness review of SQL."""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.connectors.azure_openai_connector import get_model_name
from src.models.schemas import ValidationResult
from src.prompts import load_prompt

sql_evaluator_agent = Agent(
    name="SQLGuardrailEvaluatorAgent",
    instructions=load_prompt("sql_evaluator"),
    model=get_model_name("gpt-4o-mini"),
    output_type=AgentOutputSchema(ValidationResult, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)