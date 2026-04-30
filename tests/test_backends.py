"""Unit tests for backend helper functions and factory."""

import subprocess
import threading
from unittest.mock import MagicMock, patch

import pytest

from juvenal.backends import (
    Backend,
    ClaudeBackend,
    CodexBackend,
    InteractiveResult,
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
        backend = create_backend("claude")
        assert isinstance(backend, ClaudeBackend)
        assert backend.name() == "claude"

    def test_codex(self):
        backend = create_backend("codex")
        assert isinstance(backend, CodexBackend)
        assert backend.name() == "codex"

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown backend"):
            create_backend("gpt")


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
