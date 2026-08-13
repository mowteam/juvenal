"""Unit tests for backend helper functions and factory."""

import subprocess
import threading
from unittest.mock import MagicMock, patch

import pytest

from juvenal.backends import (
    AgentResult,
    Backend,
    ClaudeBackend,
    ClaudeSDKBackend,
    CodexBackend,
    CodexSDKBackend,
    InteractiveResult,
    _claude_input_tokens,
    _extend_with_settings,
    _extract_claude_tokens,
    _extract_codex_tokens,
    _parse_json_event,
    _process_claude_event,
    _process_codex_event,
    create_backend,
)


class DummyBackend(Backend):
    def name(self) -> str:
        return "dummy"

    def run_agent(
        self, prompt, working_dir, display_callback=None, timeout=None, env=None, model=None, system_prompt=None
    ):
        raise NotImplementedError


class FakeProc:
    def __init__(self, on_kill=None, on_wait=None):
        self.on_kill = on_kill
        self.on_wait = on_wait
        self.kill_calls = 0
        self.wait_calls = 0

    def kill(self):
        self.kill_calls += 1
        if self.on_kill:
            self.on_kill()

    def wait(self):
        self.wait_calls += 1
        if self.on_wait:
            self.on_wait()
        return 0


class TestCreateBackend:
    def test_claude(self):
        # SDK is the default when installed; either way it's a Claude backend.
        backend = create_backend("claude")
        assert isinstance(backend, (ClaudeSDKBackend, ClaudeBackend))
        assert backend.name() in ("claude", "claude-sdk")

    def test_codex(self):
        backend = create_backend("codex")
        assert isinstance(backend, (CodexSDKBackend, CodexBackend))
        assert backend.name() in ("codex", "codex-sdk")

    def test_sdk_is_default_when_installed(self):
        # The whole point of the flip: bare `claude`/`codex` prefer the SDK backend.
        if ClaudeSDKBackend().sdk_available:
            assert isinstance(create_backend("claude"), ClaudeSDKBackend)
        if CodexSDKBackend().sdk_available:
            assert isinstance(create_backend("codex"), CodexSDKBackend)
        if not ClaudeSDKBackend().sdk_available and not CodexSDKBackend().sdk_available:
            pytest.skip("no SDK installed; default-flip exercised only when an SDK is present")

    def test_no_sdk_env_forces_cli_backends(self, monkeypatch):
        # Escape hatch: JUVENAL_BACKEND_NO_SDK=1 forces the subprocess CLI backends.
        monkeypatch.setenv("JUVENAL_BACKEND_NO_SDK", "1")
        assert type(create_backend("claude")) is ClaudeBackend
        assert type(create_backend("codex")) is CodexBackend
        assert create_backend("claude").name() == "claude"
        assert create_backend("codex").name() == "codex"

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown backend"):
            create_backend("gpt")

    def test_claude_sdk_returns_backend(self):
        # Either the SDK backend (if installed) or a transparent CLI fallback —
        # never an error on the default path.
        backend = create_backend("claude-sdk")
        assert isinstance(backend, (ClaudeSDKBackend, ClaudeBackend))

    def test_codex_sdk_returns_backend(self):
        # Opt-in codex-sdk resolves to the SDK backend when installed, else falls
        # back transparently to the subprocess CodexBackend — never an error on the
        # default path (SDK not installed here).
        backend = create_backend("codex-sdk")
        assert isinstance(backend, (CodexSDKBackend, CodexBackend))

    def test_codex_sdk_fail_loud_when_forced_without_sdk(self, monkeypatch):
        # JUVENAL_BACKEND_CODEX_SDK=1 forces the SDK path; without the SDK installed
        # it must raise rather than silently downgrade to subprocess.
        monkeypatch.setenv("JUVENAL_BACKEND_CODEX_SDK", "1")
        if CodexSDKBackend().sdk_available:
            pytest.skip("Codex SDK installed; fail-loud path is exercised only when absent")
        with pytest.raises(RuntimeError, match="openai-codex"):
            create_backend("codex-sdk")


class TestCodexSDKBackend:
    """The codex-sdk backend is feature-flagged and only its SDK query loop
    (_drive_codex_sdk) needs the real SDK. Everything else — name, contract
    parity, the guarded seam — must hold offline without the SDK installed."""

    def test_name(self):
        assert CodexSDKBackend().name() == "codex-sdk"

    def test_run_agent_signature_matches_codex_backend(self):
        # The runner is backend-agnostic: codex-sdk must accept the same run_agent
        # kwargs as the subprocess CodexBackend so a mixed run swaps cleanly.
        import inspect

        sdk_sig = inspect.signature(CodexSDKBackend().run_agent)
        cli_sig = inspect.signature(CodexBackend().run_agent)
        assert list(sdk_sig.parameters.keys()) == list(cli_sig.parameters.keys())

    def test_resume_agent_signature_matches_codex_backend(self):
        import inspect

        sdk_sig = inspect.signature(CodexSDKBackend().resume_agent)
        cli_sig = inspect.signature(CodexBackend().resume_agent)
        assert list(sdk_sig.parameters.keys()) == list(cli_sig.parameters.keys())

    def test_run_agent_without_sdk_raises_runtime_error(self):
        backend = CodexSDKBackend()
        if backend.sdk_available:
            pytest.skip("Codex SDK installed; missing-SDK path not exercised")
        with pytest.raises(RuntimeError, match="pip install openai-codex"):
            backend.run_agent("hi", working_dir="/tmp")

    def test_resume_agent_without_sdk_raises_runtime_error(self):
        backend = CodexSDKBackend()
        if backend.sdk_available:
            pytest.skip("Codex SDK installed; missing-SDK path not exercised")
        with pytest.raises(RuntimeError, match="pip install openai-codex"):
            backend.resume_agent("thread-123", "hi", working_dir="/tmp")

    def test_run_agent_folds_system_prompt_before_reaching_sdk(self):
        # Codex has no system-prompt slot; the backend must fold it into the user
        # message (it reaches the drive seam with the merged prompt).
        backend = CodexSDKBackend()
        backend._sdk = object()
        captured = {}

        def fake_drive(**kwargs):
            captured.update(kwargs)
            raise NotImplementedError("stub")

        backend._drive_codex_sdk = fake_drive
        with pytest.raises(NotImplementedError):
            backend.run_agent("DYNAMIC", working_dir="/tmp", system_prompt="ROLE")
        assert captured["prompt"] == "ROLE\n\nDYNAMIC"
        assert captured["resume_thread_id"] is None

    def test_resume_agent_passes_thread_id_to_drive(self):
        backend = CodexSDKBackend()
        backend._sdk = object()
        captured = {}

        def fake_drive(**kwargs):
            captured.update(kwargs)
            raise NotImplementedError("stub")

        backend._drive_codex_sdk = fake_drive
        with pytest.raises(NotImplementedError):
            backend.resume_agent("thread-abc", "hi", working_dir="/tmp")
        assert captured["resume_thread_id"] == "thread-abc"

    def test_run_agent_returns_agent_result_contract(self):
        # When the SDK loop is implemented it must return the same AgentResult
        # fields the runner reads; this pins that contract via a stubbed drive.
        backend = CodexSDKBackend()
        backend._sdk = object()
        backend._drive_codex_sdk = lambda **kwargs: AgentResult(
            exit_code=0,
            output="done",
            transcript="t",
            duration=0.1,
            input_tokens=5,
            output_tokens=7,
            session_id="thread-xyz",
        )
        result = backend.run_agent("hi", working_dir="/tmp")
        assert isinstance(result, AgentResult)
        assert result.exit_code == 0
        assert result.session_id == "thread-xyz"
        assert result.input_tokens == 5
        assert result.output_tokens == 7

    def test_resume_agent_preserves_thread_id_when_sdk_returns_none(self):
        backend = CodexSDKBackend()
        backend._sdk = object()
        backend._drive_codex_sdk = lambda **kwargs: AgentResult(
            exit_code=0, output="", transcript="", duration=0.1, session_id=None
        )
        result = backend.resume_agent("thread-keep", "hi", working_dir="/tmp")
        assert result.session_id == "thread-keep"


class _FakeCodexSDK:
    """In-memory stand-in for the official openai_codex module.

    Reproduces just the surface CodexSDKBackend._drive_codex_sdk touches:
    Codex()/thread_start/thread_resume returning a thread whose run() yields a
    TurnResult, plus the Sandbox/ApprovalMode enums and is_retryable_error. Lets
    the drive loop run to completion in a unit test without launching the real
    Codex app-server or hitting the network.
    """

    class Sandbox:
        full_access = "full-access"
        read_only = "read-only"
        workspace_write = "workspace-write"

    class ApprovalMode:
        auto_review = "auto_review"
        deny_all = "deny_all"

    class CodexConfig:
        def __init__(self, cwd=None, env=None):
            self.cwd = cwd
            self.env = env

    class TurnStatus:
        def __init__(self, value):
            self.value = value

    class _Breakdown:
        def __init__(self, input_tokens, output_tokens):
            self.input_tokens = input_tokens
            self.output_tokens = output_tokens

    class _Usage:
        def __init__(self, total):
            self.total = total

    class TurnResult:
        def __init__(self, final_response, status, usage=None, error=None):
            self.final_response = final_response
            self.status = status
            self.usage = usage
            self.error = error

    class ServerBusyError(Exception):
        pass

    class _Thread:
        def __init__(self, sdk, thread_id, response, status, usage, error, exc, capture):
            self._sdk = sdk
            self.id = thread_id
            self._response = response
            self._status = status
            self._usage = usage
            self._error = error
            self._exc = exc
            self._capture = capture

        def run(self, prompt):
            if self._capture is not None:
                self._capture["prompt"] = prompt
            if self._exc is not None:
                raise self._exc
            sdk = self._sdk
            usage = sdk._Usage(sdk._Breakdown(*self._usage)) if self._usage else None
            return sdk.TurnResult(self._response, sdk.TurnStatus(self._status), usage, self._error)

    class Codex:
        def __init__(self, config):
            self.config = config
            self.closed = False
            self.start_args = None
            self.resume_args = None
            _FakeCodexSDK._active._instances.append(self)

        def thread_start(self, sandbox=None, approval_mode=None, model=None):
            self.start_args = {"sandbox": sandbox, "approval_mode": approval_mode, "model": model}
            return _FakeCodexSDK._active._make_thread()

        def thread_resume(self, thread_id, sandbox=None, model=None):
            self.resume_args = {"thread_id": thread_id, "sandbox": sandbox, "model": model}
            return _FakeCodexSDK._active._make_thread()

        def close(self):
            self.closed = True

    _active = None

    def __init__(
        self,
        *,
        thread_id="thread-abc",
        response="done",
        status="completed",
        usage=(15, 25),
        error=None,
        exc=None,
    ):
        self._thread_id = thread_id
        self._response = response
        self._status = status
        self._usage = usage
        self._error = error
        self._exc = exc
        self._instances = []
        self.capture = {}
        self.is_retryable_error = lambda e: isinstance(e, _FakeCodexSDK.ServerBusyError)
        _FakeCodexSDK._active = self

    def _make_thread(self):
        return _FakeCodexSDK._Thread(
            self, self._thread_id, self._response, self._status, self._usage, self._error, self._exc, self.capture
        )

    @property
    def instances(self):
        return self._instances


class TestCodexSDKDriveLoop:
    """Exercise _drive_codex_sdk against a mocked openai_codex — no app-server, no
    network. The true end-to-end parity check is tests/test_e2e_codex.py with a
    backend='codex-sdk' variant (see docs/backends/codex-sdk-exploration.md)."""

    def test_run_agent_maps_turn_result(self):
        sdk = _FakeCodexSDK(thread_id="thread-abc", response="the answer", usage=(15, 25))
        backend = CodexSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent("hello codex", working_dir="/work", model="gpt-5-codex", system_prompt="ROLE")
        assert result.exit_code == 0
        assert result.output == "the answer"
        assert result.session_id == "thread-abc"
        assert result.input_tokens == 15
        assert result.output_tokens == 25
        # Codex has no system-prompt slot: the role folds into the user message.
        assert sdk.capture["prompt"] == "ROLE\n\nhello codex"

    def test_run_agent_passes_full_autonomy_options(self):
        sdk = _FakeCodexSDK()
        backend = CodexSDKBackend()
        backend._sdk = sdk
        backend.run_agent("hi", working_dir="/work", model="gpt-5-codex")
        codex = sdk.instances[0]
        # full_access + auto_review == the CLI's --dangerously-bypass-approvals-and-sandbox.
        assert codex.start_args["sandbox"] == "full-access"
        assert codex.start_args["approval_mode"] == "auto_review"
        assert codex.start_args["model"] == "gpt-5-codex"
        assert codex.config.cwd == "/work"
        assert isinstance(codex.config.env, dict)
        # The client is torn down (app-server stopped) after the turn.
        assert codex.closed is True

    def test_resume_agent_resumes_thread(self):
        sdk = _FakeCodexSDK(thread_id="thread-resumed")
        backend = CodexSDKBackend()
        backend._sdk = sdk
        result = backend.resume_agent("thread-xyz", "again", working_dir="/w", model="gpt-5-codex")
        codex = sdk.instances[0]
        assert codex.resume_args["thread_id"] == "thread-xyz"
        assert codex.resume_args["sandbox"] == "full-access"
        assert result.session_id == "thread-resumed"

    def test_resume_agent_preserves_thread_id_when_thread_has_none(self):
        sdk = _FakeCodexSDK(thread_id=None)
        backend = CodexSDKBackend()
        backend._sdk = sdk
        result = backend.resume_agent("keep-me", "hi", working_dir="/w")
        assert result.session_id == "keep-me"

    def test_failed_status_maps_to_nonzero_exit(self):
        sdk = _FakeCodexSDK(response="", status="failed", error=type("E", (), {"message": "boom"})())
        backend = CodexSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent("x", working_dir="/w")
        assert result.exit_code == 1
        assert "boom" in result.output
        assert "boom" in result.transcript

    def test_server_busy_maps_to_rate_limit(self):
        sdk = _FakeCodexSDK(exc=_FakeCodexSDK.ServerBusyError("overloaded"))
        backend = CodexSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent("x", working_dir="/w")
        assert result.exit_code == 1
        assert result.rate_limit_status == 429
        assert "overloaded" in result.output

    def test_generic_error_is_not_a_rate_limit(self):
        sdk = _FakeCodexSDK(exc=RuntimeError("auth failed"))
        backend = CodexSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent("x", working_dir="/w")
        assert result.exit_code == 1
        assert result.rate_limit_status is None
        assert "auth failed" in result.output

    def test_timeout_tears_down_client(self):
        import time

        class _SlowSDK(_FakeCodexSDK):
            class _Thread(_FakeCodexSDK._Thread):
                def run(self, prompt):
                    time.sleep(10)
                    raise RuntimeError("should not reach")

            def _make_thread(self):
                return _SlowSDK._Thread(self, "slow-thread", "", "completed", None, None, None, self.capture)

        sdk = _SlowSDK()
        backend = CodexSDKBackend()
        backend._sdk = sdk
        result = backend.run_agent("x", working_dir="/w", timeout=1)
        assert result.exit_code == 1
        assert "timed out" in result.output
        assert result.session_id == "slow-thread"
        assert sdk.instances[0].closed is True


class TestParseJsonEvent:
    def test_valid_json_object(self):
        assert _parse_json_event('{"type": "assistant"}') == {"type": "assistant"}

    def test_non_json_line(self):
        assert _parse_json_event("plain text output") is None

    def test_invalid_json(self):
        assert _parse_json_event("{broken json") is None

    def test_json_array_returns_none(self):
        assert _parse_json_event("[1, 2, 3]") is None

    def test_json_string_returns_none(self):
        assert _parse_json_event('"just a string"') is None

    def test_empty_line(self):
        assert _parse_json_event("") is None

    def test_whitespace_before_json(self):
        assert _parse_json_event('  {"type": "x"}') == {"type": "x"}


class TestProcessClaudeEvent:
    def test_assistant_text(self):
        display, assistant = _process_claude_event({"type": "assistant", "message": "hello"})
        assert display == "hello"
        assert assistant == "hello"

    def test_assistant_dict_message(self):
        display, assistant = _process_claude_event({"type": "assistant", "message": {"content": "hi"}})
        assert display == "hi"
        assert assistant == "hi"

    def test_assistant_list_message(self):
        event = {
            "type": "assistant",
            "message": [{"type": "text", "text": "part1"}, {"type": "text", "text": "part2"}],
        }
        display, assistant = _process_claude_event(event)
        assert "part1" in display
        assert "part2" in display

    def test_content_block_delta(self):
        display, assistant = _process_claude_event({"type": "content_block_delta", "delta": {"text": "chunk"}})
        assert display == "chunk"
        assert assistant == ""

    def test_result_event(self):
        display, assistant = _process_claude_event({"type": "result", "result": "final output"})
        assert display == "final output"
        assert assistant == "final output"

    def test_result_success_subtype(self):
        display, assistant = _process_claude_event({"type": "result", "subtype": "success"})
        assert display == ""
        assert assistant == ""

    def test_tool_use(self):
        display, assistant = _process_claude_event({"type": "tool_use", "name": "Write"})
        assert "Write" in display
        assert assistant == ""

    def test_system_event(self):
        display, assistant = _process_claude_event({"type": "system", "message": "init"})
        assert "init" in display
        assert assistant == ""

    def test_unknown_event(self):
        display, assistant = _process_claude_event({"type": "unknown_type"})
        assert display == ""
        assert assistant == ""


class TestProcessCodexEvent:
    def test_agent_message(self):
        event = {"type": "item.completed", "item": {"type": "agent_message", "text": "done"}}
        display, assistant = _process_codex_event(event)
        assert display == "done"
        assert assistant == "done"

    def test_reasoning(self):
        event = {"type": "item.completed", "item": {"type": "reasoning", "text": "thinking..."}}
        display, assistant = _process_codex_event(event)
        assert "thinking" in display
        assert assistant == ""

    def test_tool_call(self):
        event = {"type": "item.completed", "item": {"type": "tool_call", "name": "shell"}}
        display, assistant = _process_codex_event(event)
        assert "shell" in display
        assert assistant == ""

    def test_turn_completed_with_usage(self):
        event = {"type": "turn.completed", "usage": {"input_tokens": 100, "output_tokens": 50}}
        display, assistant = _process_codex_event(event)
        assert "100" in display
        assert "50" in display
        assert assistant == ""

    def test_turn_completed_no_usage(self):
        display, assistant = _process_codex_event({"type": "turn.completed"})
        assert display == ""

    def test_unknown_event(self):
        display, assistant = _process_codex_event({"type": "something.else"})
        assert display == ""
        assert assistant == ""


class TestExtractClaudeTokens:
    def test_result_with_usage(self):
        event = {"type": "result", "usage": {"input_tokens": 500, "output_tokens": 200}}
        assert _extract_claude_tokens(event) == (500, 200)

    def test_result_no_usage(self):
        assert _extract_claude_tokens({"type": "result"}) == (0, 0)

    def test_non_result_event(self):
        assert _extract_claude_tokens({"type": "assistant", "usage": {"input_tokens": 100}}) == (0, 0)

    def test_input_counts_cache_read_and_creation(self):
        # Anthropic reports cached input separately; all three are billed input.
        event = {
            "type": "result",
            "usage": {
                "input_tokens": 6,
                "cache_read_input_tokens": 1000,
                "cache_creation_input_tokens": 200,
                "output_tokens": 50,
            },
        }
        assert _extract_claude_tokens(event) == (1206, 50)

    def test_claude_input_tokens_helper_tolerates_missing_cache_fields(self):
        assert _claude_input_tokens({"input_tokens": 10}) == 10
        assert _claude_input_tokens({"input_tokens": 6, "cache_read_input_tokens": 1000}) == 1006
        assert _claude_input_tokens({}) == 0


class TestExtractCodexTokens:
    def test_turn_completed_with_usage(self):
        event = {"type": "turn.completed", "usage": {"input_tokens": 300, "output_tokens": 100}}
        assert _extract_codex_tokens(event) == (300, 100)

    def test_turn_completed_no_usage(self):
        assert _extract_codex_tokens({"type": "turn.completed"}) == (0, 0)

    def test_non_turn_event(self):
        assert _extract_codex_tokens({"type": "item.completed"}) == (0, 0)


class TestInteractiveResult:
    def test_dataclass_fields(self):
        result = InteractiveResult(session_id="abc-123", exit_code=0)
        assert result.session_id == "abc-123"
        assert result.exit_code == 0

    def test_nonzero_exit(self):
        result = InteractiveResult(session_id="def-456", exit_code=1)
        assert result.exit_code == 1


class TestRunInteractive:
    def test_codex_raises_not_implemented(self):
        backend = CodexBackend()
        with pytest.raises(NotImplementedError, match="codex.*does not support interactive"):
            backend.run_interactive("prompt", "/tmp")


class TestKillActive:
    def test_kill_active_empty(self):
        backend = ClaudeBackend()
        backend.kill_active()  # should not raise
        assert backend._active_procs == []

    def test_register_unregister_concurrent(self):
        backend = DummyBackend()
        thread_count = 8
        register_barrier = threading.Barrier(thread_count + 1)
        unregister_barrier = threading.Barrier(thread_count + 1)
        done_barrier = threading.Barrier(thread_count + 1)
        errors = []

        def worker(index):
            proc = FakeProc()
            try:
                backend._register_proc(proc)
                register_barrier.wait()
                unregister_barrier.wait()
                backend._unregister_proc(proc)
                done_barrier.wait()
            except Exception as exc:  # pragma: no cover - failure path only
                errors.append((index, exc))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(thread_count)]
        for thread in threads:
            thread.start()

        register_barrier.wait()
        with backend._proc_lock:
            assert len(backend._active_procs) == thread_count

        unregister_barrier.wait()
        done_barrier.wait()
        for thread in threads:
            thread.join()

        assert errors == []
        assert backend._active_procs == []

    def test_kill_active_safe_when_registry_changes_during_iteration(self):
        backend = DummyBackend()
        late_proc = FakeProc()
        first_proc = FakeProc(on_kill=lambda: backend._register_proc(late_proc))
        second_proc = FakeProc(on_wait=lambda: backend._unregister_proc(first_proc))

        backend._register_proc(first_proc)
        backend._register_proc(second_proc)

        backend.kill_active()

        assert first_proc.kill_calls == 1
        assert first_proc.wait_calls == 1
        assert second_proc.kill_calls == 1
        assert second_proc.wait_calls == 1
        assert late_proc.kill_calls == 1
        assert late_proc.wait_calls == 1
        assert backend._active_procs == []


def _stub_popen() -> MagicMock:
    """Build a Popen mock whose process exits cleanly with no events.

    The returned mock has stdout iterable as an empty pipe, stderr readable
    as empty, and wait() returning 0. Lets us inspect Popen call kwargs
    without touching real subprocesses."""
    mock_proc = MagicMock()
    mock_proc.stdout = iter([])
    mock_proc.stderr = MagicMock()
    mock_proc.stderr.read.return_value = ""
    mock_proc.wait.return_value = 0
    mock_proc.returncode = 0
    mock_proc.stdin = MagicMock()
    return mock_proc


class TestSubprocessStdinIsolation:
    """Non-interactive agents must NOT inherit the parent tty's stdin —
    otherwise they race the chat dashboard's stdin reader for keystrokes.
    When the prompt is fed via stdin (to avoid E2BIG on long argv), use a
    pipe; otherwise use DEVNULL."""

    def test_claude_run_agent_pipes_prompt_via_stdin(self):
        backend = ClaudeBackend()
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.run_agent("hi", working_dir="/tmp")
        kwargs = popen.call_args.kwargs
        # Pipe so the prompt can be written to stdin (bypasses argv length
        # cap). The subprocess does not race the parent tty because the pipe
        # is closed as soon as the prompt is delivered.
        assert kwargs.get("stdin") is subprocess.PIPE
        # Prompt must NOT be on the command line (would hit MAX_ARG_STRLEN
        # on long-running runs).
        cmd = popen.call_args.args[0] if popen.call_args.args else popen.call_args.kwargs["args"]
        assert "hi" not in cmd

    def test_claude_resume_agent_pipes_prompt_via_stdin(self):
        backend = ClaudeBackend()
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.resume_agent(
                "1d3f0c80-3a0b-4f0c-bfba-5b18e3f9a1e2",
                "hi",
                working_dir="/tmp",
            )
        kwargs = popen.call_args.kwargs
        assert kwargs.get("stdin") is subprocess.PIPE
        cmd = popen.call_args.args[0] if popen.call_args.args else popen.call_args.kwargs["args"]
        assert "hi" not in cmd

    def test_codex_run_agent_pipes_stdin_for_prompt(self):
        backend = CodexBackend()
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.run_agent("hi", working_dir="/tmp")
        kwargs = popen.call_args.kwargs
        assert kwargs.get("stdin") is subprocess.PIPE


class TestSystemPromptRouting:
    """The system_prompt argument must land in the system role at session
    creation, not duplicated into the user message via stdin. Claude does
    this via --append-system-prompt-file pointing at .juvenal/prompts/<sid>.md;
    Codex has no separate slot and folds it into the user message."""

    def test_claude_run_agent_writes_system_prompt_file(self, tmp_path):
        backend = ClaudeBackend()
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.run_agent(
                "user message",
                working_dir=str(tmp_path),
                system_prompt="STATIC ROLE TEXT",
            )

        cmd = popen.call_args.args[0] if popen.call_args.args else popen.call_args.kwargs["args"]
        assert "--append-system-prompt-file" in cmd
        flag_index = cmd.index("--append-system-prompt-file")
        prompt_path = cmd[flag_index + 1]
        assert "/.juvenal/prompts/" in prompt_path
        from pathlib import Path

        assert Path(prompt_path).read_text(encoding="utf-8") == "STATIC ROLE TEXT"

    def test_claude_run_agent_omits_flag_when_system_prompt_none(self, tmp_path):
        backend = ClaudeBackend()
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.run_agent("user message", working_dir=str(tmp_path))
        cmd = popen.call_args.args[0] if popen.call_args.args else popen.call_args.kwargs["args"]
        assert "--append-system-prompt-file" not in cmd
        # No file should have been created.
        prompts_dir = tmp_path / ".juvenal" / "prompts"
        assert not prompts_dir.exists() or not any(prompts_dir.iterdir())

    def test_claude_run_agent_keeps_user_message_on_stdin(self, tmp_path):
        backend = ClaudeBackend()
        stub = _stub_popen()
        with patch("juvenal.backends.subprocess.Popen", return_value=stub):
            backend.run_agent(
                "ONLY THE DYNAMIC PAYLOAD",
                working_dir=str(tmp_path),
                system_prompt="STATIC ROLE",
            )
        # The user message goes via stdin write; the system prompt must NOT
        # also be written to stdin (else it would be duplicated user content).
        write_calls = [c.args[0] for c in stub.stdin.write.call_args_list]
        joined = "".join(write_calls)
        assert "ONLY THE DYNAMIC PAYLOAD" in joined
        assert "STATIC ROLE" not in joined

    def test_codex_run_agent_folds_system_prompt_into_user_message(self, tmp_path):
        backend = CodexBackend()
        stub = _stub_popen()
        with patch("juvenal.backends.subprocess.Popen", return_value=stub):
            backend.run_agent(
                "DYNAMIC PAYLOAD",
                working_dir=str(tmp_path),
                system_prompt="STATIC ROLE",
            )
        write_calls = [c.args[0] for c in stub.stdin.write.call_args_list]
        joined = "".join(write_calls)
        assert "STATIC ROLE" in joined
        assert "DYNAMIC PAYLOAD" in joined
        assert joined.index("STATIC ROLE") < joined.index("DYNAMIC PAYLOAD")

    def test_claude_run_agent_handles_prompt_larger_than_argv_limit(self):
        """A prompt larger than Linux's 128KB MAX_ARG_STRLEN must not be
        passed via argv. Regression for the multi-hour analysis run that
        crashed with [Errno 7] Argument list too long after the captain
        prompt accumulated past 128KB."""
        backend = ClaudeBackend()
        big_prompt = "X" * (200 * 1024)  # 200KB — well past argv cap
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.run_agent(big_prompt, working_dir="/tmp")
        cmd = popen.call_args.args[0] if popen.call_args.args else popen.call_args.kwargs["args"]
        # No argv entry may be the giant prompt.
        for entry in cmd:
            assert big_prompt not in entry


def _cmd_from(popen):
    return popen.call_args.args[0] if popen.call_args.args else popen.call_args.kwargs["args"]


class TestSettingsInjection:
    """hooks_config rides into the Claude CLI as a `--settings <json>` fragment.
    The confirmed CLI mechanism is `--settings` (accepts an inline JSON string
    merged over on-disk settings); role guardrails are `permissions.deny` globs."""

    def test_extend_with_settings_appends_json(self):
        cmd = ["claude"]
        _extend_with_settings(cmd, {"permissions": {"deny": ["Write(//x/**)"]}})
        assert "--settings" in cmd
        payload = cmd[cmd.index("--settings") + 1]
        import json

        assert json.loads(payload) == {"permissions": {"deny": ["Write(//x/**)"]}}

    def test_extend_with_settings_noop_when_none(self):
        cmd = ["claude"]
        _extend_with_settings(cmd, None)
        assert "--settings" not in cmd

    def test_extend_with_settings_noop_when_empty(self):
        cmd = ["claude"]
        _extend_with_settings(cmd, {})
        assert "--settings" not in cmd

    def test_claude_run_agent_passes_hooks_config_as_settings(self, tmp_path):
        backend = ClaudeBackend()
        hooks = {"permissions": {"deny": ["Write(//out/**)"]}}
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.run_agent("hi", working_dir=str(tmp_path), hooks_config=hooks)
        cmd = _cmd_from(popen)
        assert "--settings" in cmd
        import json

        assert json.loads(cmd[cmd.index("--settings") + 1]) == hooks

    def test_claude_run_agent_omits_settings_when_no_hooks(self, tmp_path):
        backend = ClaudeBackend()
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.run_agent("hi", working_dir=str(tmp_path))
        assert "--settings" not in _cmd_from(popen)

    def test_claude_resume_agent_reapplies_hooks_config(self, tmp_path):
        backend = ClaudeBackend()
        hooks = {"permissions": {"deny": ["Edit"]}}
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.resume_agent(
                "1d3f0c80-3a0b-4f0c-bfba-5b18e3f9a1e2",
                "hi",
                working_dir=str(tmp_path),
                hooks_config=hooks,
            )
        cmd = _cmd_from(popen)
        assert "--settings" in cmd

    def test_codex_run_agent_ignores_hooks_config(self, tmp_path):
        backend = CodexBackend()
        with patch("juvenal.backends.subprocess.Popen", return_value=_stub_popen()) as popen:
            backend.run_agent(
                "hi",
                working_dir=str(tmp_path),
                hooks_config={"permissions": {"deny": ["Write"]}},
            )
        # Codex has no settings-injection equivalent; the flag must not appear.
        assert "--settings" not in _cmd_from(popen)
