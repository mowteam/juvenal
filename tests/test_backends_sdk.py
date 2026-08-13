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


class _FakeClaudeSDK:
    """Minimal in-memory stand-in for the claude_agent_sdk module.

    Reproduces the typed message classes and the async `query` generator the
    drive loop iterates, so unit tests exercise ClaudeSDKBackend._drive_sdk
    end-to-end without a network call or the real SDK installed. Message classes
    are instance attributes so the backend's isinstance() checks resolve against
    the same objects the tests construct.
    """

    class ClaudeAgentOptions:
        def __init__(self, **kwargs):
            self.kwargs = dict(kwargs)
            self.__dict__.update(kwargs)

    class TextBlock:
        def __init__(self, text):
            self.text = text

    class ThinkingBlock:
        def __init__(self, thinking, signature=""):
            self.thinking = thinking
            self.signature = signature

    class ToolUseBlock:
        def __init__(self, id, name, input):
            self.id = id
            self.name = name
            self.input = input

    class AssistantMessage:
        def __init__(self, content, error=None):
            self.content = content
            self.error = error

    class SystemMessage:
        def __init__(self, subtype, data=None):
            self.subtype = subtype
            self.data = data or {}

    class ResultMessage:
        def __init__(self, session_id, usage=None, is_error=False, api_error_status=None, result=None):
            self.session_id = session_id
            self.usage = usage
            self.is_error = is_error
            self.api_error_status = api_error_status
            self.result = result

    class RateLimitInfo:
        def __init__(self, status):
            self.status = status

    class RateLimitEvent:
        def __init__(self, rate_limit_info):
            self.rate_limit_info = rate_limit_info

    class CLINotFoundError(Exception):
        pass

    def __init__(self, script):
        # `script` is a list of messages (or a callable capturing options) the
        # fake query yields in order.
        self._script = script
        self.captured_options = None
        self.captured_prompt = None

    def query(self, *, prompt, options):
        self.captured_options = options
        self.captured_prompt = prompt
        script = self._script

        async def _gen():
            for message in script:
                yield message

        return _gen()


class TestClaudeSDKBackendDriveLoop:
    """Exercise _drive_sdk against a mocked SDK — no network, no real SDK needed.

    The true end-to-end parity check is tests/test_e2e_claude.py with a
    backend='claude-sdk' variant (see docs/backends/claude-sdk-integration.md)."""

    def test_run_agent_maps_result_to_agent_result(self):
        sdk = _FakeClaudeSDK([])
        sdk._script = [
            sdk.AssistantMessage([sdk.TextBlock("hello world")]),
            sdk.ResultMessage(
                session_id="sess-xyz",
                usage={"input_tokens": 11, "output_tokens": 22},
                result="hello world",
            ),
        ]
        backend = ClaudeSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent(
            "USER",
            working_dir="/work",
            model="claude-opus-4-8[1m]",
            system_prompt="SYS",
            session_id="sess-xyz",
        )
        assert result.exit_code == 0
        assert result.output == "hello world"
        assert result.session_id == "sess-xyz"
        assert result.input_tokens == 11
        assert result.output_tokens == 22
        assert result.rate_limit_status is None

    def test_run_agent_builds_options_faithfully(self):
        sdk = _FakeClaudeSDK([])
        sdk._script = [
            sdk.ResultMessage(session_id="s1", usage={"input_tokens": 1, "output_tokens": 2}, result="ok"),
        ]
        backend = ClaudeSDKBackend()
        backend._sdk = sdk
        backend.run_agent(
            "USER",
            working_dir="/work",
            model="claude-opus-4-8[1m]",
            system_prompt="SYS",
            session_id="pre-alloc",
            hooks_config={"permissions": {"deny": ["Write(//x/**)"]}},
        )
        opts = sdk.captured_options.kwargs
        # Model string passes through unchanged, including the [1m] suffix, so the
        # SDK-driven `claude --model` grants the same 1M context as the CLI path.
        assert opts["model"] == "claude-opus-4-8[1m]"
        assert opts["cwd"] == "/work"
        assert opts["system_prompt"] == "SYS"
        assert opts["permission_mode"] == "bypassPermissions"
        assert opts["settings"] == '{"permissions": {"deny": ["Write(//x/**)"]}}'
        assert opts["session_id"] == "pre-alloc"
        assert "resume" not in opts
        # The dynamic per-call content rides in as the query prompt (stdin parity).
        assert sdk.captured_prompt == "USER"

    def test_resume_agent_uses_resume_kwarg(self):
        sdk = _FakeClaudeSDK([])
        sdk._script = [sdk.ResultMessage(session_id="resume-sid", usage={}, result="ok")]
        backend = ClaudeSDKBackend()
        backend._sdk = sdk
        result = backend.resume_agent("resume-sid", "hi", working_dir="/w")
        opts = sdk.captured_options.kwargs
        assert opts["resume"] == "resume-sid"
        assert "session_id" not in opts
        # Resumed sessions inherit the original system prompt; none is re-sent.
        assert "system_prompt" not in opts
        assert result.session_id == "resume-sid"

    def test_run_agent_maps_api_error_status_to_rate_limit(self):
        sdk = _FakeClaudeSDK([])
        sdk._script = [
            sdk.ResultMessage(session_id="s3", usage={}, is_error=True, api_error_status=429, result=None),
        ]
        backend = ClaudeSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent("hi", working_dir="/w", session_id="s3")
        assert result.rate_limit_status == 429
        assert result.exit_code == 1

    def test_run_agent_maps_assistant_rate_limit_error(self):
        sdk = _FakeClaudeSDK([])
        sdk._script = [
            sdk.AssistantMessage([sdk.TextBlock("partial")], error="rate_limit"),
            sdk.ResultMessage(session_id="s4", usage={}, is_error=True, result=None),
        ]
        backend = ClaudeSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent("hi", working_dir="/w", session_id="s4")
        assert result.rate_limit_status == 429

    def test_run_agent_maps_rejected_rate_limit_event(self):
        sdk = _FakeClaudeSDK([])
        sdk._script = [
            sdk.RateLimitEvent(sdk.RateLimitInfo(status="rejected")),
            sdk.ResultMessage(session_id="s6", usage={}, is_error=True, result=None),
        ]
        backend = ClaudeSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent("hi", working_dir="/w", session_id="s6")
        assert result.rate_limit_status == 429

    def test_informational_rate_limit_event_does_not_set_429(self):
        # The SDK emits a RateLimitEvent reporting remaining quota on every turn,
        # including successful ones. status="allowed"/"allowed_warning" must NOT
        # be mistaken for a 429 or the runner would back off after a clean turn.
        sdk = _FakeClaudeSDK([])
        for status in ("allowed", "allowed_warning"):
            sdk._script = [
                sdk.RateLimitEvent(sdk.RateLimitInfo(status=status)),
                sdk.AssistantMessage([sdk.TextBlock("ok")]),
                sdk.ResultMessage(session_id="s6", usage={}, result="ok"),
            ]
            backend = ClaudeSDKBackend()
            backend._sdk = sdk
            result = backend.run_agent("hi", working_dir="/w", session_id="s6")
            assert result.rate_limit_status is None, status
            assert result.exit_code == 0

    def test_run_agent_timeout_returns_exit_one(self):
        import asyncio

        sdk = _FakeClaudeSDK([])

        def slow_query(*, prompt, options):
            sdk.captured_options = options

            async def _gen():
                await asyncio.sleep(5)
                yield sdk.ResultMessage(session_id="s5", usage={}, result="late")

            return _gen()

        sdk.query = slow_query
        backend = ClaudeSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent("hi", working_dir="/w", timeout=1, session_id="s5")
        assert result.exit_code == 1
        assert "timed out" in result.output

    def test_run_agent_reports_tool_use_and_thinking_in_transcript(self):
        sdk = _FakeClaudeSDK([])
        sdk._script = [
            sdk.AssistantMessage(
                [
                    sdk.ThinkingBlock("pondering"),
                    sdk.ToolUseBlock("t1", "Bash", {"command": "ls"}),
                    sdk.TextBlock("done"),
                ]
            ),
            sdk.ResultMessage(session_id="s7", usage={}, result="done"),
        ]
        backend = ClaudeSDKBackend()
        backend._sdk = sdk
        captured_display: list[str] = []
        result = backend.run_agent("hi", working_dir="/w", session_id="s7", display_callback=captured_display.append)
        assert "[tool: Bash]" in result.transcript
        assert any("[thinking]" in line for line in captured_display)
        # Only TextBlock text counts as assistant output.
        assert result.output == "done"


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

    def test_pwn2own_models_pass_through_both_sdk_backends(self):
        from juvenal.dynamic.runner import _resolve_model

        assert _resolve_model("claude-sdk", "captain", "claude-opus-5") == "claude-opus-5"
        assert _resolve_model("codex-sdk", "worker", "gpt-5.6-sol") == "gpt-5.6-sol"


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
