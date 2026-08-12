"""Tests for the opt-in ClaudeSDKBackend (Claude Agent SDK).

Split into three groups by what they need:
- Contract/flag tests run everywhere (no SDK, no network) — they exercise the
  scaffold's shape: name, AgentResult contract, [1m] model-string passthrough,
  and create_backend flag selection/fallback.
- SDK-present tests are guarded with importorskip — they only run once the human
  installs `claude-agent-sdk`.
- SDK-absent tests assert the graceful fallback and the fail-loud env flag.
"""

import importlib.util

import pytest

from juvenal.backends import (
    AgentResult,
    Backend,
    ClaudeBackend,
    ClaudeSDKBackend,
    InteractiveResult,
    create_backend,
)

SDK_INSTALLED = (
    importlib.util.find_spec("claude_agent_sdk") is not None or importlib.util.find_spec("claude_code_sdk") is not None
)


class TestClaudeSDKBackendContract:
    """Shape checks that hold whether or not the SDK is installed."""

    def test_name_is_claude_sdk(self):
        assert ClaudeSDKBackend().name() == "claude-sdk"

    def test_is_a_backend(self):
        assert isinstance(ClaudeSDKBackend(), Backend)

    def test_run_agent_signature_matches_claude_backend(self):
        import inspect

        sdk_sig = inspect.signature(ClaudeSDKBackend().run_agent)
        cli_sig = inspect.signature(ClaudeBackend().run_agent)
        assert list(sdk_sig.parameters) == list(cli_sig.parameters)

    def test_resume_agent_signature_matches_claude_backend(self):
        import inspect

        sdk_sig = inspect.signature(ClaudeSDKBackend().resume_agent)
        cli_sig = inspect.signature(ClaudeBackend().resume_agent)
        assert list(sdk_sig.parameters) == list(cli_sig.parameters)

    def test_sdk_available_reflects_import_state(self):
        assert ClaudeSDKBackend().sdk_available is SDK_INSTALLED

    def test_agent_result_contract_is_shared(self):
        # The runner is vendor-neutral: it only reads these AgentResult fields,
        # so any backend (CLI or SDK) must produce the same shape.
        result = AgentResult(
            exit_code=0,
            output="ok",
            transcript="ok",
            duration=0.1,
            input_tokens=10,
            output_tokens=20,
            session_id="s-1",
            rate_limit_status=None,
        )
        assert result.exit_code == 0
        assert result.session_id == "s-1"
        assert result.input_tokens == 10
        assert result.output_tokens == 20
        assert result.rate_limit_status is None

    def test_interactive_result_contract_is_shared(self):
        res = InteractiveResult(session_id="abc", exit_code=0)
        assert res.session_id == "abc"
        assert res.exit_code == 0


@pytest.mark.skipif(SDK_INSTALLED, reason="covers the SDK-absent scaffold path")
class TestClaudeSDKBackendWithoutSDK:
    """When the SDK is missing, run/resume must raise a clear, actionable error
    rather than a bare ImportError deep in a query loop."""

    def test_run_agent_raises_runtime_error(self):
        backend = ClaudeSDKBackend()
        with pytest.raises(RuntimeError, match="claude-agent-sdk"):
            backend.run_agent("hi", working_dir="/tmp")

    def test_resume_agent_raises_runtime_error(self):
        backend = ClaudeSDKBackend()
        with pytest.raises(RuntimeError, match="claude-agent-sdk"):
            backend.resume_agent("sess-1", "hi", working_dir="/tmp")


@pytest.mark.skipif(not SDK_INSTALLED, reason="Claude Agent SDK not installed")
class TestClaudeSDKBackendWithSDK:
    """Only run once the human installs the SDK. These still avoid the network by
    stopping at the not-yet-implemented query loop; the true end-to-end check is
    tests/test_e2e_claude.py with a backend='claude-sdk' variant (see
    SDK_AVAILABILITY.md)."""

    def test_run_agent_reaches_query_loop(self):
        backend = ClaudeSDKBackend()
        # _drive_sdk is the human's fill-in point; until implemented it raises
        # NotImplementedError, proving run_agent routed past the SDK-missing guard.
        with pytest.raises(NotImplementedError):
            backend.run_agent("hi", working_dir="/tmp")

    def test_resume_agent_reaches_query_loop(self):
        backend = ClaudeSDKBackend()
        with pytest.raises(NotImplementedError):
            backend.resume_agent("sess-1", "hi", working_dir="/tmp")


class TestClaudeSDKModelRouting:
    """The SDK backend must accept the same model strings as the CLI backend,
    including the `[1m]` 1M-context suffix. The runner routes `claude-sdk` roles
    through _resolve_model, which must mirror `claude` and preserve `[1m]`."""

    def test_claude_sdk_inherits_claude_role_defaults(self):
        from juvenal.dynamic.runner import _resolve_model

        for role in ("captain", "worker", "verifier", "reporter", "analyst"):
            assert _resolve_model("claude-sdk", role, None) == _resolve_model("claude", role, None)

    def test_claude_sdk_captain_default_keeps_1m_suffix(self):
        from juvenal.dynamic.runner import _resolve_model

        assert _resolve_model("claude-sdk", "captain", None) == "claude-opus-4-7[1m]"

    def test_explicit_1m_model_passes_through(self):
        from juvenal.dynamic.runner import _resolve_model

        assert _resolve_model("claude-sdk", "worker", "claude-opus-4-8[1m]") == "claude-opus-4-8[1m]"


class TestCreateBackendClaudeSDK:
    """Flag selection and fallback — runnable without the SDK."""

    def test_create_backend_returns_backend_instance(self):
        backend = create_backend("claude-sdk")
        assert isinstance(backend, Backend)

    def test_create_backend_falls_back_to_cli_when_sdk_absent(self, monkeypatch):
        monkeypatch.delenv("JUVENAL_BACKEND_SDK", raising=False)
        backend = create_backend("claude-sdk")
        if SDK_INSTALLED:
            assert isinstance(backend, ClaudeSDKBackend)
        else:
            # Transparent fallback keeps existing workflows running.
            assert isinstance(backend, ClaudeBackend)

    @pytest.mark.skipif(SDK_INSTALLED, reason="fail-loud only matters when SDK is absent")
    def test_env_flag_forces_loud_failure_when_sdk_absent(self, monkeypatch):
        monkeypatch.setenv("JUVENAL_BACKEND_SDK", "1")
        with pytest.raises(RuntimeError, match="claude-agent-sdk"):
            create_backend("claude-sdk")
