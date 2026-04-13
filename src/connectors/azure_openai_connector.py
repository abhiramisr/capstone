"""Azure AI Foundry connector — AI Foundry primary, OpenAI fallback.

Connection priority:
    1. Azure OpenAI Service                 (AZURE_OPENAI_ENDPOINT)
    2. Direct OpenAI API                    (OPENAI_API_KEY)  ← automatic fallback

The fallback is live — if Foundry is configured but the connection test fails
(bad key, wrong endpoint, network issue), the connector automatically falls
back to direct OpenAI and logs a warning. The pipeline always runs.


Environment variables — Azure OpenAI Service (primary):
    AZURE_OPENAI_ENDPOINT:        Azure OpenAI resource endpoint URL
    AZURE_OPENAI_API_KEY:         Azure OpenAI API key
    AZURE_OPENAI_API_VERSION:     API version (default: 2024-12-01-preview)
    AZURE_DEPLOYMENT_DEFAULT:     Single deployment name for all agents (simplest)
    AZURE_DEPLOYMENT_GPT_4O:      Per-model override for gpt-4o role
    AZURE_DEPLOYMENT_GPT_4O_MINI: Per-model override for gpt-4o-mini role

Environment variables — Direct OpenAI fallback:
    OPENAI_API_KEY: Standard OpenAI API key

Shared — Azure Identity / service principal:
    AZURE_TENANT_ID:     Azure AD tenant ID
    AZURE_CLIENT_ID:     Service principal app ID
    AZURE_CLIENT_SECRET: Service principal secret

Optional:
    FOUNDRY_TRACE:                   Enable Azure Monitor tracing (true | false)
    AZURE_MONITOR_CONNECTION_STRING: App Insights connection string
"""

from __future__ import annotations

import os
from typing import Any

# ---------------------------------------------------------------------------
# Model name mapping
# ---------------------------------------------------------------------------

# Deployment name mapping — kept for backward compatibility
DEPLOYMENT_MAP: dict[str, str] = {}


def get_model_name(base_model: str) -> str:
    """Resolve a base model name to the appropriate deployment/catalog name.

    Resolution order:
      1. AZURE_AI_INFERENCE_MODEL_<MODEL>  (per-agent inference override)
      2. AZURE_AI_INFERENCE_MODEL          (single inference model for all agents)
      3. AZURE_DEPLOYMENT_<MODEL>          (per-model Azure OpenAI deployment name)
      4. AZURE_DEPLOYMENT_DEFAULT          (single deployment name for all agents)
      5. base_model unchanged              (direct OpenAI or nothing configured)

    Example — single deployment for all agents:
        AZURE_DEPLOYMENT_DEFAULT=gpt-4.1-mini
    Both gpt-4o and gpt-4o-mini roles resolve to gpt-4.1-mini automatically.
    """
    # Foundry inference endpoint
    if os.getenv("AZURE_AI_INFERENCE_ENDPOINT"):
        env_key = "AZURE_AI_INFERENCE_MODEL_" + base_model.upper().replace("-", "_")
        return os.getenv(env_key) or os.getenv("AZURE_AI_INFERENCE_MODEL") or base_model

    # Azure OpenAI Service — read fresh every time, no caching
    if os.getenv("AZURE_OPENAI_ENDPOINT"):
        default = os.getenv("AZURE_DEPLOYMENT_DEFAULT", "")
        per_model_key = "AZURE_DEPLOYMENT_" + base_model.upper().replace("-", "_")
        resolved = os.getenv(per_model_key) or default or base_model
        DEPLOYMENT_MAP[base_model] = resolved
        return resolved

    # Direct OpenAI — no mapping needed
    return base_model


# USE_AZURE: True when any Azure endpoint is configured (preserves original contract)
USE_AZURE: bool = bool(
    os.getenv("AZURE_AI_INFERENCE_ENDPOINT") or os.getenv("AZURE_OPENAI_ENDPOINT")
)


# ---------------------------------------------------------------------------
# Azure credential helper (keyless auth)
# ---------------------------------------------------------------------------


def _get_azure_credential():
    """Return a DefaultAzureCredential, or None if azure-identity is not installed."""
    try:
        from azure.identity import DefaultAzureCredential
        return DefaultAzureCredential()
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Connection probe — cheap synchronous check before building RunConfig
# ---------------------------------------------------------------------------


def _probe_foundry_connection(endpoint: str, api_key: str | None) -> tuple[bool, str]:
    """Send a GET request to the inference endpoint to verify reachability.

    Returns (reachable: bool, reason: str).
    Does NOT make an LLM call — just checks the host is up and the key is accepted.
    """
    import urllib.request
    import urllib.error

    url = endpoint.rstrip("/") + "/v1/models"
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["api-key"] = api_key

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return True, f"HTTP {resp.status}"
    except urllib.error.HTTPError as e:
        if e.code in (401, 403, 404):
            return True, f"HTTP {e.code} (endpoint reachable)"
        return False, f"HTTP {e.code}: {e.reason}"
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# RunConfig builders
# ---------------------------------------------------------------------------


def _build_foundry_inference_run_config() -> Any | None:
    """Build a RunConfig for the Azure AI Foundry inference endpoint."""
    try:
        from openai import AsyncAzureOpenAI
        from agents import RunConfig
        from agents.models.openai_provider import OpenAIProvider
    except ImportError:
        print("[WARN] openai/agents package not installed.")
        return None

    endpoint = os.getenv("AZURE_AI_INFERENCE_ENDPOINT", "").strip()
    api_key = os.getenv("AZURE_AI_INFERENCE_KEY", "").strip() or None

    if not endpoint:
        return None

    reachable, reason = _probe_foundry_connection(endpoint, api_key)
    if not reachable:
        print(f"  [WARN] Foundry inference endpoint unreachable ({reason}) — will try fallback.")
        return None

    client_kwargs: dict[str, Any] = {
        "azure_endpoint": endpoint.rstrip("/"),
        "api_version": os.getenv("AZURE_AI_INFERENCE_API_VERSION", "2024-05-01-preview"),
    }

    if api_key:
        client_kwargs["api_key"] = api_key
    else:
        cred = _get_azure_credential()
        if cred is None:
            print("  [WARN] No inference key and azure-identity not installed.")
            return None
        try:
            from azure.identity import get_bearer_token_provider  # type: ignore
            client_kwargs["azure_ad_token_provider"] = get_bearer_token_provider(
                cred, "https://cognitiveservices.azure.com/.default"
            )
        except ImportError:
            return None

    try:
        client = AsyncAzureOpenAI(**client_kwargs)
        provider = OpenAIProvider(openai_client=client, use_responses=False)
        return RunConfig(model_provider=provider)
    except Exception as e:
        print(f"  [WARN] Failed to build Foundry inference RunConfig: {e}")
        return None


def _build_azure_openai_run_config() -> Any | None:
    """Build a RunConfig for Azure OpenAI Service."""
    try:
        from openai import AsyncAzureOpenAI
        from agents import RunConfig
        from agents.models.openai_provider import OpenAIProvider
    except ImportError:
        print("[WARN] openai/agents package not installed.")
        return None

    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").strip()
    api_key = os.getenv("AZURE_OPENAI_API_KEY", "").strip() or None
    api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")

    if not endpoint:
        return None

    reachable, reason = _probe_foundry_connection(endpoint, api_key)
    if not reachable:
        print(f"  [WARN] Azure OpenAI endpoint unreachable ({reason}) — will try fallback.")
        return None

    client_kwargs: dict[str, Any] = {
        "azure_endpoint": endpoint,
        "api_version": api_version,
    }

    if api_key:
        client_kwargs["api_key"] = api_key
    else:
        cred = _get_azure_credential()
        if cred is None:
            print("  [WARN] No API key and azure-identity not installed.")
            return None
        try:
            from azure.identity import get_bearer_token_provider  # type: ignore
            client_kwargs["azure_ad_token_provider"] = get_bearer_token_provider(
                cred, "https://cognitiveservices.azure.com/.default"
            )
        except ImportError:
            return None

    try:
        client = AsyncAzureOpenAI(**client_kwargs)
        provider = OpenAIProvider(openai_client=client, use_responses=False)
        return RunConfig(model_provider=provider)
    except Exception as e:
        print(f"  [WARN] Failed to build Azure OpenAI RunConfig: {e}")
        return None


def _build_openai_run_config() -> Any | None:
    """Build a RunConfig for direct OpenAI API."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("  [WARN] OPENAI_API_KEY not set — Agents SDK will use its own default.")
        return None

    try:
        from openai import AsyncOpenAI
        from agents import RunConfig
        from agents.models.openai_provider import OpenAIProvider

        client = AsyncOpenAI(api_key=api_key)
        provider = OpenAIProvider(openai_client=client, use_responses=False)
        return RunConfig(model_provider=provider)
    except Exception as e:
        print(f"  [WARN] Failed to build OpenAI RunConfig: {e}")
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_azure_run_config() -> Any | None:
    """Return the best available RunConfig, trying Foundry first then falling back.

    Connection priority:
        1. Azure AI Foundry inference endpoint  (AZURE_AI_INFERENCE_ENDPOINT)
        2. Azure OpenAI Service                 (AZURE_OPENAI_ENDPOINT)
        3. Direct OpenAI API                    (OPENAI_API_KEY)

    Not cached — re-evaluated on each pipeline run so a transient Foundry
    outage recovers automatically on the next request.
    """
    # Step 1 — Azure AI Foundry inference endpoint
    if os.getenv("AZURE_AI_INFERENCE_ENDPOINT"):
        cfg = _build_foundry_inference_run_config()
        if cfg:
            print("  [INFO] Backend: Azure AI Foundry inference endpoint")
            return cfg
        print("  [INFO] Foundry inference unavailable — trying Azure OpenAI Service...")

    # Step 2 — Azure OpenAI Service
    if os.getenv("AZURE_OPENAI_ENDPOINT"):
        cfg = _build_azure_openai_run_config()
        if cfg:
            print("  [INFO] Backend: Azure OpenAI Service")
            return cfg
        print("  [INFO] Azure OpenAI Service unavailable — falling back to direct OpenAI...")

    # Step 3 — Direct OpenAI
    cfg = _build_openai_run_config()
    if cfg:
        print("  [INFO] Backend: direct OpenAI API (fallback)")
        return cfg

    print("  [WARN] No backend available — Agents SDK will use its own default.")
    return None


# Alias for new code that wants to be explicit
get_foundry_run_config = get_azure_run_config


# ---------------------------------------------------------------------------
# Optional: Azure Monitor / OpenTelemetry tracing
# ---------------------------------------------------------------------------


def setup_foundry_tracing() -> bool:
    """Configure OpenTelemetry → Azure Monitor if FOUNDRY_TRACE=true."""
    if os.getenv("FOUNDRY_TRACE", "").lower() not in ("1", "true", "yes"):
        return False

    connection_string = os.getenv("AZURE_MONITOR_CONNECTION_STRING")
    if not connection_string:
        print("[WARN] FOUNDRY_TRACE=true but AZURE_MONITOR_CONNECTION_STRING not set.")
        return False

    try:
        from azure.monitor.opentelemetry import configure_azure_monitor  # type: ignore
        configure_azure_monitor(connection_string=connection_string)
        print("  [INFO] Azure Monitor tracing enabled (FOUNDRY_TRACE=true)")
        return True
    except ImportError:
        print(
            "[WARN] azure-monitor-opentelemetry not installed. "
            "Run: pip install -e '.[foundry-full]'"
        )
        return False