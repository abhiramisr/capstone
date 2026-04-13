"""Tests for azure_openai_connector — Foundry-first with OpenAI fallback.

All tests run without live Azure credentials. They verify:
  - USE_AZURE reflects presence of Azure endpoint vars
  - get_model_name resolves correctly per env configuration
  - get_azure_run_config tries Foundry, then Azure OpenAI, then OpenAI
  - Each step falls back cleanly when probe fails or vars are missing
  - get_foundry_run_config is an alias for get_azure_run_config
  - _probe_foundry_connection handles HTTP errors correctly
"""

from __future__ import annotations

import importlib
import os
import sys
from unittest import mock

import pytest


def _reload():
    """Reload the connector module so env var changes take effect."""
    mod_name = "src.connectors.azure_openai_connector"
    if mod_name in sys.modules:
        del sys.modules[mod_name]
    return importlib.import_module(mod_name)


# ---------------------------------------------------------------------------
# USE_AZURE
# ---------------------------------------------------------------------------

class TestUseAzure:
    def test_false_when_no_azure_vars(self):
        env = {"AZURE_AI_INFERENCE_ENDPOINT": "", "AZURE_OPENAI_ENDPOINT": ""}
        with mock.patch.dict(os.environ, env, clear=False):
            m = _reload()
            assert m.USE_AZURE is False

    def test_true_when_inference_endpoint_set(self):
        env = {"AZURE_AI_INFERENCE_ENDPOINT": "https://x.inference.ai.azure.com", "AZURE_OPENAI_ENDPOINT": ""}
        with mock.patch.dict(os.environ, env, clear=False):
            m = _reload()
            assert m.USE_AZURE is True

    def test_true_when_openai_endpoint_set(self):
        env = {"AZURE_AI_INFERENCE_ENDPOINT": "", "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com"}
        with mock.patch.dict(os.environ, env, clear=False):
            m = _reload()
            assert m.USE_AZURE is True


# ---------------------------------------------------------------------------
# get_model_name
# ---------------------------------------------------------------------------

class TestGetModelName:
    def test_passthrough_when_no_azure_vars(self):
        """Direct OpenAI path — model name returned unchanged."""
        env = {"AZURE_AI_INFERENCE_ENDPOINT": "", "AZURE_OPENAI_ENDPOINT": ""}
        with mock.patch.dict(os.environ, env, clear=False):
            m = _reload()
            assert m.get_model_name("gpt-4o-mini") == "gpt-4o-mini"
            assert m.get_model_name("gpt-4o") == "gpt-4o"

    def test_inference_uses_model_var(self):
        """Inference path — AZURE_AI_INFERENCE_MODEL applies to all agents."""
        env = {
            "AZURE_AI_INFERENCE_ENDPOINT": "https://x.inference.ai.azure.com",
            "AZURE_OPENAI_ENDPOINT": "",
            "AZURE_AI_INFERENCE_MODEL": "Phi-4",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            m = _reload()
            assert m.get_model_name("gpt-4o-mini") == "Phi-4"
            assert m.get_model_name("gpt-4o") == "Phi-4"

    def test_inference_per_model_override(self):
        """Inference path — per-model env var takes priority over global."""
        env = {
            "AZURE_AI_INFERENCE_ENDPOINT": "https://x.inference.ai.azure.com",
            "AZURE_OPENAI_ENDPOINT": "",
            "AZURE_AI_INFERENCE_MODEL": "Phi-4",
            # Key format: "gpt-4o".upper().replace("-","_") → "GPT_4O"
            "AZURE_AI_INFERENCE_MODEL_GPT_4O": "gpt-4o",
            "AZURE_AI_INFERENCE_MODEL_GPT_4O_MINI": "Phi-4-mini-instruct",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            m = _reload()
            assert m.get_model_name("gpt-4o") == "gpt-4o"
            assert m.get_model_name("gpt-4o-mini") == "Phi-4-mini-instruct"

    def test_azure_openai_deployment_default(self):
        """AZURE_DEPLOYMENT_DEFAULT covers all agent roles with one var.
        This is the recommended setup when one deployment serves everything.
        e.g. AZURE_DEPLOYMENT_DEFAULT=gpt-4.1-mini
        """
        env = {
            "AZURE_AI_INFERENCE_ENDPOINT": "",
            "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com",
            "AZURE_DEPLOYMENT_DEFAULT": "gpt-4.1-mini",
            "AZURE_DEPLOYMENT_GPT_4O": "",
            "AZURE_DEPLOYMENT_GPT_4O_MINI": "",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            m = _reload()
            assert m.get_model_name("gpt-4o") == "gpt-4.1-mini"
            assert m.get_model_name("gpt-4o-mini") == "gpt-4.1-mini"

    def test_azure_openai_per_model_overrides_default(self):
        """Per-model var takes priority over AZURE_DEPLOYMENT_DEFAULT.
        Useful when adding a stronger model for a specific agent later.
        """
        env = {
            "AZURE_AI_INFERENCE_ENDPOINT": "",
            "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com",
            "AZURE_DEPLOYMENT_DEFAULT": "gpt-4.1-mini",
            "AZURE_DEPLOYMENT_GPT_4O": "gpt-4.1",        # stronger model for gpt-4o role
            "AZURE_DEPLOYMENT_GPT_4O_MINI": "",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            m = _reload()
            assert m.get_model_name("gpt-4o") == "gpt-4.1"          # per-model wins
            assert m.get_model_name("gpt-4o-mini") == "gpt-4.1-mini" # falls back to default

    def test_azure_openai_falls_back_to_base_name(self):
        """When no deployment vars are set, returns the base model name unchanged."""
        env = {
            "AZURE_AI_INFERENCE_ENDPOINT": "",
            "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com",
            "AZURE_DEPLOYMENT_DEFAULT": "",
            "AZURE_DEPLOYMENT_GPT_4O": "",
            "AZURE_DEPLOYMENT_GPT_4O_MINI": "",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            m = _reload()
            assert m.get_model_name("gpt-4o") == "gpt-4o"
            assert m.get_model_name("gpt-4o-mini") == "gpt-4o-mini"


# ---------------------------------------------------------------------------
# get_azure_run_config — fallback chain
# ---------------------------------------------------------------------------

class TestRunConfigFallback:
    def _make_mock_run_config(self):
        return object()

    def test_returns_foundry_when_probe_succeeds(self):
        """When Foundry probe passes, returns Foundry RunConfig without trying others."""
        m = _reload()
        sentinel = self._make_mock_run_config()

        with mock.patch.dict(os.environ, {
            "AZURE_AI_INFERENCE_ENDPOINT": "https://x.inference.ai.azure.com",
            "AZURE_AI_INFERENCE_KEY": "test-key",
        }, clear=False):
            with mock.patch.object(m, "_probe_foundry_connection", return_value=(True, "HTTP 200")):
                with mock.patch.object(m, "_build_foundry_inference_run_config", return_value=sentinel):
                    result = m.get_azure_run_config()
        assert result is sentinel

    def test_falls_back_to_azure_openai_when_foundry_fails(self):
        """When Foundry build returns None, tries Azure OpenAI Service next."""
        m = _reload()
        sentinel = self._make_mock_run_config()

        with mock.patch.dict(os.environ, {
            "AZURE_AI_INFERENCE_ENDPOINT": "https://x.inference.ai.azure.com",
            "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com",
            "AZURE_OPENAI_API_KEY": "aoai-key",
        }, clear=False):
            with mock.patch.object(m, "_build_foundry_inference_run_config", return_value=None):
                with mock.patch.object(m, "_build_azure_openai_run_config", return_value=sentinel):
                    result = m.get_azure_run_config()
        assert result is sentinel

    def test_falls_back_to_openai_when_both_azure_fail(self):
        """When both Azure backends fail, falls back to direct OpenAI."""
        m = _reload()
        sentinel = self._make_mock_run_config()

        with mock.patch.dict(os.environ, {
            "AZURE_AI_INFERENCE_ENDPOINT": "https://x.inference.ai.azure.com",
            "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com",
            "OPENAI_API_KEY": "sk-test",
        }, clear=False):
            with mock.patch.object(m, "_build_foundry_inference_run_config", return_value=None):
                with mock.patch.object(m, "_build_azure_openai_run_config", return_value=None):
                    with mock.patch.object(m, "_build_openai_run_config", return_value=sentinel):
                        result = m.get_azure_run_config()
        assert result is sentinel

    def test_returns_none_when_all_backends_fail(self):
        """Returns None (not raises) when every backend is unavailable."""
        m = _reload()

        with mock.patch.dict(os.environ, {
            "AZURE_AI_INFERENCE_ENDPOINT": "",
            "AZURE_OPENAI_ENDPOINT": "",
            "OPENAI_API_KEY": "",
        }, clear=False):
            with mock.patch.object(m, "_build_openai_run_config", return_value=None):
                result = m.get_azure_run_config()
        assert result is None

    def test_skips_foundry_when_endpoint_not_set(self):
        """Does not attempt Foundry when AZURE_AI_INFERENCE_ENDPOINT is absent."""
        m = _reload()
        sentinel = self._make_mock_run_config()

        with mock.patch.dict(os.environ, {
            "AZURE_AI_INFERENCE_ENDPOINT": "",
            "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com",
            "AZURE_OPENAI_API_KEY": "key",
        }, clear=False):
            with mock.patch.object(m, "_build_foundry_inference_run_config") as mock_foundry:
                with mock.patch.object(m, "_build_azure_openai_run_config", return_value=sentinel):
                    result = m.get_azure_run_config()
        mock_foundry.assert_not_called()
        assert result is sentinel

    def test_no_cached_result_stale_after_failure(self):
        """Not cached — a second call after failure retries the backends."""
        m = _reload()
        sentinel = self._make_mock_run_config()

        with mock.patch.dict(os.environ, {
            "AZURE_AI_INFERENCE_ENDPOINT": "",
            "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com",
        }, clear=False):
            # First call fails
            with mock.patch.object(m, "_build_azure_openai_run_config", return_value=None):
                with mock.patch.object(m, "_build_openai_run_config", return_value=None):
                    first = m.get_azure_run_config()
            assert first is None

            # Second call succeeds — no stale cache blocking it
            with mock.patch.object(m, "_build_azure_openai_run_config", return_value=sentinel):
                second = m.get_azure_run_config()
        assert second is sentinel

    def test_alias_is_same_function(self):
        """get_foundry_run_config must be the exact same object as get_azure_run_config."""
        m = _reload()
        assert m.get_foundry_run_config is m.get_azure_run_config


# ---------------------------------------------------------------------------
# _probe_foundry_connection
# ---------------------------------------------------------------------------

class TestProbeFoundryConnection:
    def test_returns_true_on_200(self):
        """HTTP 200 means endpoint is fully reachable."""
        import urllib.request
        m = _reload()

        class FakeResponse:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *a): pass

        with mock.patch("urllib.request.urlopen", return_value=FakeResponse()):
            reachable, reason = m._probe_foundry_connection("https://x.azure.com", "key")
        assert reachable is True
        assert "200" in reason

    def test_returns_true_on_401(self):
        """HTTP 401 means host is up — key is wrong but endpoint is reachable."""
        import urllib.error
        m = _reload()
        err = urllib.error.HTTPError(url="", code=401, msg="Unauthorized", hdrs=None, fp=None)
        with mock.patch("urllib.request.urlopen", side_effect=err):
            reachable, reason = m._probe_foundry_connection("https://x.azure.com", "bad-key")
        assert reachable is True
        assert "401" in reason

    def test_returns_true_on_403(self):
        """HTTP 403 means host is up — permissions issue, not a connectivity failure."""
        import urllib.error
        m = _reload()
        err = urllib.error.HTTPError(url="", code=403, msg="Forbidden", hdrs=None, fp=None)
        with mock.patch("urllib.request.urlopen", side_effect=err):
            reachable, reason = m._probe_foundry_connection("https://x.azure.com", "key")
        assert reachable is True

    def test_returns_true_on_404(self):
        """HTTP 404 means host is up — path differs but endpoint is reachable."""
        import urllib.error
        m = _reload()
        err = urllib.error.HTTPError(url="", code=404, msg="Not Found", hdrs=None, fp=None)
        with mock.patch("urllib.request.urlopen", side_effect=err):
            reachable, reason = m._probe_foundry_connection("https://x.azure.com", "key")
        assert reachable is True

    def test_returns_false_on_500(self):
        """HTTP 500 is a server error — treat as unreachable."""
        import urllib.error
        m = _reload()
        err = urllib.error.HTTPError(url="", code=500, msg="Internal Server Error", hdrs=None, fp=None)
        with mock.patch("urllib.request.urlopen", side_effect=err):
            reachable, reason = m._probe_foundry_connection("https://x.azure.com", "key")
        assert reachable is False
        assert "500" in reason

    def test_returns_false_on_connection_error(self):
        """Connection refused / DNS failure means unreachable."""
        m = _reload()
        with mock.patch("urllib.request.urlopen", side_effect=OSError("Connection refused")):
            reachable, reason = m._probe_foundry_connection("https://x.azure.com", None)
        assert reachable is False
        assert "Connection refused" in reason

    def test_returns_false_on_timeout(self):
        """Timeout means unreachable."""
        import socket
        m = _reload()
        with mock.patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
            reachable, reason = m._probe_foundry_connection("https://x.azure.com", None)
        assert reachable is False