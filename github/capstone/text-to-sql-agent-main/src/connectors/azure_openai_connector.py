"""Azure AI Foundry connector — routes LLM calls through Azure endpoint.

Replaces direct OpenAI API calls with Azure AI Foundry deployment.
All LLM agents route through this connector when Azure env vars are set.

Environment variables:
    AZURE_OPENAI_ENDPOINT: Azure OpenAI resource endpoint URL
    AZURE_OPENAI_API_KEY: Azure OpenAI API key
    AZURE_OPENAI_API_VERSION: API version (default: 2024-12-01-preview)
    AZURE_DEPLOYMENT_GPT4O: Deployment name for gpt-4o model
    AZURE_DEPLOYMENT_GPT4O_MINI: Deployment name for gpt-4o-mini model
"""

from __future__ import annotations

import os
from functools import lru_cache

# Check if Azure is configured
USE_AZURE = bool(os.getenv("AZURE_OPENAI_ENDPOINT"))

# Deployment name mapping — maps base model names to Azure deployment names
DEPLOYMENT_MAP: dict[str, str] = {}


def _init_deployment_map() -> dict[str, str]:
    """Build the model-to-deployment mapping from environment variables."""
    return {
        "gpt-4o": os.getenv("AZURE_DEPLOYMENT_GPT4O", "gpt-4o"),
        "gpt-4o-mini": os.getenv("AZURE_DEPLOYMENT_GPT4O_MINI", "gpt-4o-mini"),
    }


def get_model_name(base_model: str) -> str:
    """Return the Azure deployment name for a base model, or the original name."""
    if not USE_AZURE:
        return base_model
    if not DEPLOYMENT_MAP:
        DEPLOYMENT_MAP.update(_init_deployment_map())
    return DEPLOYMENT_MAP.get(base_model, base_model)


@lru_cache(maxsize=1)
def get_azure_run_config():
    """Return a RunConfig with Azure OpenAI provider, or None if Azure is not configured.

    When Azure env vars are present, returns a RunConfig that uses
    AsyncAzureOpenAI as the underlying client. When absent, returns None
    (the caller should use the default OpenAI configuration).
    """
    if not USE_AZURE:
        return None

    try:
        from openai import AsyncAzureOpenAI
        from agents import RunConfig
        from agents.models.openai_provider import OpenAIProvider

        endpoint = os.environ["AZURE_OPENAI_ENDPOINT"]
        api_key = os.environ["AZURE_OPENAI_API_KEY"]
        api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")

        azure_client = AsyncAzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version,
        )

        provider = OpenAIProvider(
            openai_client=azure_client,
            use_responses=False,
        )

        return RunConfig(model_provider=provider)

    except ImportError:
        print("[WARN] openai package with Azure support not installed. Falling back to direct OpenAI.")
        return None
    except KeyError as e:
        print(f"[WARN] Missing Azure env var: {e}. Falling back to direct OpenAI.")
        return None
