"""RAG Agent — converts query results to a grounded, cited answer.

Merges the responsibilities of AnalysisAgent + SynthesisAgent into a single
focused agent. Takes question + SQL + raw results, produces a FinalResponse.
"""

from agents import Agent, ModelSettings
from agents.agent_output import AgentOutputSchema

from src.models.schemas import FinalResponse
from src.prompts import load_prompt

rag_agent = Agent(
    name="RAGAgent",
    instructions=load_prompt("rag_agent"),
    model="gpt-4o",
    output_type=AgentOutputSchema(FinalResponse, strict_json_schema=False),
    model_settings=ModelSettings(temperature=0),
)
