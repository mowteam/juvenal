"""AI backend subprocess management — Claude and Codex."""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any


@dataclass
class AgentResult:
    """Result from running an agent subprocess."""

    exit_code: int
    output: str  # final assistant messages
    transcript: str  # full transcript including tool calls
    duration: float  # seconds
    input_tokens: int = 0
    output_tokens: int = 0
    session_id: str | None = None
    # Set when the Claude CLI surfaces a 429 in the final `result` event
    # (api_error_status). The runner uses this to distinguish a real upstream
    # rate limit from a generic crash so backoff cadence only fires when
    # warranted.
    rate_limit_status: int | None = None


@dataclass
class InteractiveResult:
    """Result from an interactive terminal session."""

    session_id: str
    exit_code: int


class Backend(ABC):
    """Abstract base for AI agent backends."""

    def __init__(self):
        self._active_procs: list[subprocess.Popen] = []
        self._proc_lock = Lock()

    def _register_proc(self, proc: subprocess.Popen) -> None:
        """Record a subprocess in the active registry."""
        with self._proc_lock:
            self._active_procs.append(proc)

    def _unregister_proc(self, proc: subprocess.Popen) -> None:
        """Remove a subprocess from the active registry if still present."""
        with self._proc_lock:
            try:
                self._active_procs.remove(proc)
            except ValueError:
                pass

    def kill_active(self) -> None:
        """Kill all active agent subprocesses."""
        while True:
            with self._proc_lock:
                procs = list(self._active_procs)
                self._active_procs.clear()

            if not procs:
                return

            for proc in procs:
                try:
                    proc.kill()
                    proc.wait()
                except (ProcessLookupError, OSError):
                    pass

    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def run_agent(
        self,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        system_prompt: str | None = None,
        session_id: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        """Run an agent with the given prompt. Returns AgentResult.

        ``model`` is an opaque CLI model identifier (e.g. ``claude-opus-4-7[1m]``,
        ``claude-sonnet-4-6``, or a Codex model name). When ``None`` the backend's
        CLI default is used.

        ``system_prompt`` is optional content placed in the system role at session
        start. When provided, the Claude backend writes it to a file under
        ``.juvenal/prompts/<session_id>.md`` and passes ``--append-system-prompt-file``.
        ``prompt`` (the user message via stdin) carries only dynamic per-call
        content. Backends that don't support a separate system prompt may ignore
        this argument.

        ``session_id`` lets the caller pre-allocate the session id so it can be
        persisted to state before the subprocess starts streaming, surviving a
        Ctrl-C or crash mid-call. When ``None`` the backend generates one.
        Backends that don't accept an externally chosen session id may ignore
        this argument.

        ``hooks_config`` is an optional Claude Code settings fragment (e.g.
        ``{"permissions": {"deny": ["Write(//abs/**)"]}}``) enforcing role-based
        tool-use guardrails at the CLI level. The Claude backend passes it to the
        CLI via ``--settings``; backends without a settings-injection mechanism
        ignore it.
        """
        ...

    def resume_agent(
        self,
        session_id: str,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        """Resume an existing agent session. Default falls back to run_agent.

        Resumed sessions inherit the system prompt set at the original
        ``run_agent`` call; resume callers must not pass ``system_prompt``
        themselves. ``hooks_config`` is re-applied on resume because CLI
        ``--settings`` are per-invocation, not persisted with the session.
        """
        return self.run_agent(
            prompt, working_dir, display_callback, timeout, env, model=model, hooks_config=hooks_config
        )

    def run_interactive(
        self,
        prompt: str,
        working_dir: str,
        env: dict[str, str] | None = None,
        model: str | None = None,
    ) -> InteractiveResult:
        """Run an interactive terminal session. Default raises NotImplementedError."""
        raise NotImplementedError(f"{self.name()} backend does not support interactive mode")

    def resume_interactive(
        self,
        session_id: str,
        working_dir: str,
        env: dict[str, str] | None = None,
        model: str | None = None,
    ) -> InteractiveResult:
        """Open an interactive TUI on an existing session. Hands the terminal
        directly to the underlying CLI's TUI; the parent process blocks until
        the user exits. Default raises NotImplementedError."""
        raise NotImplementedError(f"{self.name()} backend does not support interactive resume")

    def _run_inherited_stdio(
        self,
        cmd: list[str],
        working_dir: str,
        env: dict[str, str] | None,
    ) -> int:
        """Spawn `cmd` with inherited stdio so the user drives its native TUI.
        Restores terminal state and foreground process group on exit. Returns
        the subprocess exit code."""
        import sys

        proc_env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
        if env:
            proc_env.update(env)

        saved_termios = None
        try:
            import termios

            if sys.stdin.isatty():
                saved_termios = termios.tcgetattr(sys.stdin)
        except (ImportError, termios.error):
            pass

        proc = subprocess.Popen(cmd, cwd=working_dir, env=proc_env)
        self._register_proc(proc)
        try:
            proc.wait()
        finally:
            self._unregister_proc(proc)
            if saved_termios is not None:
                try:
                    termios.tcsetattr(sys.stdin, termios.TCSADRAIN, saved_termios)
                except termios.error:
                    pass
            try:
                if sys.stdin.isatty():
                    os.tcsetpgrp(sys.stdin.fileno(), os.getpgrp())
            except OSError:
                pass

        return proc.returncode


class ClaudeBackend(Backend):
    """Claude CLI backend using stream-json output."""

    def name(self) -> str:
        return "claude"

    def probe_rate_limit(self, working_dir: str, env: dict[str, str] | None = None, timeout: int = 60) -> bool:
        """Run a one-shot probe to check whether the Claude rate limit has cleared.

        Returns True when the probe succeeds (limit cleared), False on a 429.
        Used by the runner to wake from rate-limit backoff with a real signal
        instead of a fixed-time guess. The probe is small ("ok") so its token
        cost is negligible.
        """
        cmd = [
            "claude",
            "-p",
            "--output-format",
            "stream-json",
            "--dangerously-skip-permissions",
            "--verbose",
            "--max-turns",
            "1",
        ]
        result = self._run_claude_process(
            cmd,
            working_dir=working_dir,
            display_callback=None,
            timeout=timeout,
            env=env,
            stdin_input="ok",
        )
        return result.exit_code == 0 and result.rate_limit_status != 429

    def run_agent(
        self,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        system_prompt: str | None = None,
        session_id: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        if session_id is None:
            session_id = str(uuid.uuid4())
        cmd = [
            "claude",
            "-p",
            "--output-format",
            "stream-json",
            "--dangerously-skip-permissions",
            "--verbose",
            "--session-id",
            session_id,
        ]
        _extend_with_settings(cmd, hooks_config)
        if model:
            cmd.extend(["--model", model])
        # When a system_prompt is provided, write it to a file under
        # .juvenal/prompts/ and load it via --append-system-prompt-file. This
        # places the role + workflow scope in the system role at session start
        # rather than the user role, where it stays anchored across the whole
        # conversation. Dynamic per-call content stays on stdin as the user
        # message.
        if system_prompt is not None:
            prompts_dir = Path(working_dir) / ".juvenal" / "prompts"
            prompts_dir.mkdir(parents=True, exist_ok=True)
            system_path = prompts_dir / f"{session_id}.md"
            system_path.write_text(system_prompt, encoding="utf-8")
            cmd.extend(["--append-system-prompt-file", str(system_path)])
        # Pipe the prompt via stdin instead of argv. Linux's MAX_ARG_STRLEN
        # caps each argv entry at 128KB, which long-running analysis runs
        # blow past once the captain's accumulated state (frontier summary,
        # claim deltas, mental model) inflates. claude --print reads stdin
        # when no positional prompt is given.
        result = self._run_claude_process(cmd, working_dir, display_callback, timeout, env, stdin_input=prompt)
        result.session_id = session_id
        return result

    def resume_agent(
        self,
        session_id: str,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        cmd = [
            "claude",
            "-p",
            "--output-format",
            "stream-json",
            "--dangerously-skip-permissions",
            "--verbose",
            "--resume",
            session_id,
        ]
        _extend_with_settings(cmd, hooks_config)
        if model:
            cmd.extend(["--model", model])
        result = self._run_claude_process(cmd, working_dir, display_callback, timeout, env, stdin_input=prompt)
        result.session_id = session_id
        return result

    def run_interactive(
        self,
        prompt: str,
        working_dir: str,
        env: dict[str, str] | None = None,
        model: str | None = None,
    ) -> InteractiveResult:
        session_id = str(uuid.uuid4())
        cmd = [
            "claude",
            "--session-id",
            session_id,
            "--dangerously-skip-permissions",
            "--verbose",
        ]
        if model:
            cmd.extend(["--model", model])
        cmd.append(prompt)
        exit_code = self._run_inherited_stdio(cmd, working_dir, env)
        return InteractiveResult(session_id=session_id, exit_code=exit_code)

    def resume_interactive(
        self,
        session_id: str,
        working_dir: str,
        env: dict[str, str] | None = None,
        model: str | None = None,
    ) -> InteractiveResult:
        cmd = [
            "claude",
            "--resume",
            session_id,
            "--dangerously-skip-permissions",
            "--verbose",
        ]
        if model:
            cmd.extend(["--model", model])
        exit_code = self._run_inherited_stdio(cmd, working_dir, env)
        return InteractiveResult(session_id=session_id, exit_code=exit_code)

    def _run_claude_process(
        self,
        cmd: list[str],
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        stdin_input: str | None = None,
    ) -> AgentResult:
        # Strip CLAUDECODE env var so juvenal can be invoked from inside Claude Code
        proc_env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
        if env:
            proc_env.update(env)

        start = time.time()
        proc = subprocess.Popen(
            cmd,
            cwd=working_dir,
            # Pipe when feeding a prompt via stdin; otherwise detach from the
            # parent tty so the subprocess can't race the chat reader for
            # keystrokes. Pipe avoids E2BIG on long captain prompts that
            # exceed Linux's 128KB MAX_ARG_STRLEN per argv entry.
            stdin=subprocess.PIPE if stdin_input is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=proc_env,
        )
        self._register_proc(proc)
        if stdin_input is not None:
            try:
                proc.stdin.write(stdin_input)
                proc.stdin.close()
            except (BrokenPipeError, OSError):
                pass

        transcript_lines: list[str] = []
        assistant_messages: list[str] = []
        total_input_tokens = 0
        total_output_tokens = 0
        rate_limit_status: int | None = None

        try:
            for raw_line in proc.stdout:
                if timeout and (time.time() - start) > timeout:
                    proc.kill()
                    proc.wait()
                    return AgentResult(
                        exit_code=1,
                        output=f"Agent timed out after {timeout}s",
                        transcript="\n".join(transcript_lines),
                        duration=time.time() - start,
                        input_tokens=total_input_tokens,
                        output_tokens=total_output_tokens,
                    )
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                event = _parse_json_event(line)
                if event:
                    display_text, assistant_text = _process_claude_event(event)
                    inp, out = _extract_claude_tokens(event)
                    total_input_tokens += inp
                    total_output_tokens += out
                    if (
                        event.get("type") == "result"
                        and event.get("is_error")
                        and isinstance(event.get("api_error_status"), int)
                    ):
                        rate_limit_status = event["api_error_status"]
                    if display_text:
                        transcript_lines.append(display_text)
                        if display_callback:
                            display_callback(display_text)
                    if assistant_text:
                        assistant_messages.append(assistant_text)
                else:
                    transcript_lines.append(line)
                    if display_callback:
                        display_callback(line)
        except Exception:
            proc.kill()
            proc.wait()
            raise

        stderr_output = proc.stderr.read()
        returncode = proc.wait()
        duration = time.time() - start
        self._unregister_proc(proc)

        if stderr_output:
            transcript_lines.append(f"[stderr] {stderr_output}")

        output = "\n".join(assistant_messages)
        if returncode != 0 and not output:
            output = stderr_output or "\n".join(transcript_lines)

        return AgentResult(
            exit_code=returncode,
            output=output,
            transcript="\n".join(transcript_lines),
            duration=duration,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            rate_limit_status=rate_limit_status,
        )


def _load_claude_agent_sdk() -> Any | None:
    """Import the Claude Agent SDK, returning the module or None if absent.

    Ships as `claude_agent_sdk` (the renamed `claude_code_sdk`); we try both so a
    machine with either installed works. Absence is expected on the default
    subprocess path, so callers treat None as "SDK backend unavailable".
    """
    for module_name in ("claude_agent_sdk", "claude_code_sdk"):
        try:
            return __import__(module_name)
        except ImportError:
            continue
    return None


class ClaudeSDKBackend(Backend):
    """Claude backend driving the Claude Agent SDK in-process instead of a CLI subprocess.

    Opt-in via `backend: claude-sdk` (or `JUVENAL_BACKEND_SDK=1`); the subprocess
    `ClaudeBackend` remains the default. Targets the session-expiration cold-restart
    gap: the SDK keeps session state in-process, so a resume either works or raises
    rather than silently starting a fresh CLI session without the original system
    prompt. Accepts the same `model=` strings as `ClaudeBackend`, including the
    `[1m]` 1M-context suffix, and returns the identical `AgentResult` contract.
    """

    def __init__(self) -> None:
        super().__init__()
        self._sdk = _load_claude_agent_sdk()

    @property
    def sdk_available(self) -> bool:
        return self._sdk is not None

    def name(self) -> str:
        return "claude-sdk"

    def run_agent(
        self,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        system_prompt: str | None = None,
        session_id: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        if session_id is None:
            session_id = str(uuid.uuid4())
        result = self._run_sdk_query(
            prompt=prompt,
            working_dir=working_dir,
            display_callback=display_callback,
            timeout=timeout,
            env=env,
            model=model,
            system_prompt=system_prompt,
            session_id=session_id,
            hooks_config=hooks_config,
            resume=False,
        )
        result.session_id = session_id
        return result

    def resume_agent(
        self,
        session_id: str,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        result = self._run_sdk_query(
            prompt=prompt,
            working_dir=working_dir,
            display_callback=display_callback,
            timeout=timeout,
            env=env,
            model=model,
            # Resumed sessions inherit the system prompt from the original run.
            system_prompt=None,
            session_id=session_id,
            hooks_config=hooks_config,
            resume=True,
        )
        result.session_id = session_id
        return result

    def _run_sdk_query(
        self,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None,
        timeout: int | None,
        env: dict[str, str] | None,
        model: str | None,
        system_prompt: str | None,
        session_id: str,
        hooks_config: dict[str, Any] | None,
        resume: bool,
    ) -> AgentResult:
        if self._sdk is None:
            raise RuntimeError(
                "claude-sdk backend requires the Claude Agent SDK. Install with: pip install claude-agent-sdk"
            )
        # SDK wiring (options build, async event drain, AgentResult mapping) lives
        # in _drive_sdk so it can be filled in and E2E-verified by a human once the
        # SDK is installed; the contract shape above is what the runner depends on.
        return self._drive_sdk(
            prompt=prompt,
            working_dir=working_dir,
            display_callback=display_callback,
            timeout=timeout,
            env=env,
            model=model,
            system_prompt=system_prompt,
            session_id=session_id,
            hooks_config=hooks_config,
            resume=resume,
        )

    def _drive_sdk(
        self,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None,
        timeout: int | None,
        env: dict[str, str] | None,
        model: str | None,
        system_prompt: str | None,
        session_id: str,
        hooks_config: dict[str, Any] | None,
        resume: bool,
    ) -> AgentResult:
        raise NotImplementedError(
            "ClaudeSDKBackend._drive_sdk is not implemented yet. "
            "Fill in the Claude Agent SDK query loop and verify against "
            "tests/test_e2e_claude.py before relying on this backend."
        )


class CodexBackend(Backend):
    """Codex CLI backend using NDJSON streaming."""

    def name(self) -> str:
        return "codex"

    def resume_interactive(
        self,
        session_id: str,
        working_dir: str,
        env: dict[str, str] | None = None,
        model: str | None = None,
    ) -> InteractiveResult:
        cmd = ["npx", "@openai/codex@latest", "resume", session_id]
        if model:
            cmd.extend(["--model", model])
        exit_code = self._run_inherited_stdio(cmd, working_dir, env)
        return InteractiveResult(session_id=session_id, exit_code=exit_code)

    def run_agent(
        self,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        system_prompt: str | None = None,
        session_id: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        # Codex assigns its own thread_id post-hoc; the externally chosen
        # session_id parameter is accepted for interface parity and ignored.
        # Codex has no Claude-settings equivalent, so hooks_config is a no-op.
        del session_id, hooks_config
        # Codex does not currently expose a separate system-prompt slot; if a
        # caller passes one, fold it into the user message so the content is
        # not silently dropped.
        if system_prompt is not None:
            prompt = f"{system_prompt}\n\n{prompt}"
        cmd = [
            "npx",
            "@openai/codex@latest",
            "exec",
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check",
        ]
        if model:
            cmd.extend(["--model", model])
        cmd.append("-")
        return self._run_codex_process(cmd, working_dir, display_callback, timeout, env, stdin_input=prompt)

    def resume_agent(
        self,
        session_id: str,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        del hooks_config  # Codex has no Claude-settings equivalent.
        cmd = [
            "npx",
            "@openai/codex@latest",
            "exec",
            "resume",
            session_id,
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check",
        ]
        if model:
            cmd.extend(["--model", model])
        cmd.append("-")
        result = self._run_codex_process(cmd, working_dir, display_callback, timeout, env, stdin_input=prompt)
        result.session_id = session_id
        return result

    def _run_codex_process(
        self,
        cmd: list[str],
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        stdin_input: str | None = None,
    ) -> AgentResult:
        proc_env = dict(os.environ)
        if env:
            proc_env.update(env)

        start = time.time()
        proc = subprocess.Popen(
            cmd,
            cwd=working_dir,
            # Pipe when feeding a prompt; otherwise detach from the parent tty
            # so the subprocess can't race the chat reader for keystrokes.
            stdin=subprocess.PIPE if stdin_input else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=proc_env,
        )
        self._register_proc(proc)

        if stdin_input:
            proc.stdin.write(stdin_input)
            proc.stdin.close()

        transcript_lines: list[str] = []
        assistant_messages: list[str] = []
        total_input_tokens = 0
        total_output_tokens = 0
        thread_id: str | None = None

        try:
            for raw_line in proc.stdout:
                if timeout and (time.time() - start) > timeout:
                    proc.kill()
                    proc.wait()
                    return AgentResult(
                        exit_code=1,
                        output=f"Agent timed out after {timeout}s",
                        transcript="\n".join(transcript_lines),
                        duration=time.time() - start,
                        input_tokens=total_input_tokens,
                        output_tokens=total_output_tokens,
                    )
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                event = _parse_json_event(line)
                if event:
                    # Capture thread_id from thread.started event
                    if event.get("type") == "thread.started" and "thread_id" in event:
                        thread_id = event["thread_id"]
                    display_text, assistant_text = _process_codex_event(event)
                    inp, out = _extract_codex_tokens(event)
                    total_input_tokens += inp
                    total_output_tokens += out
                    if display_text:
                        transcript_lines.append(display_text)
                        if display_callback:
                            display_callback(display_text)
                    if assistant_text:
                        assistant_messages.append(assistant_text)
                else:
                    transcript_lines.append(line)
                    if display_callback:
                        display_callback(line)
        except Exception:
            proc.kill()
            proc.wait()
            raise

        returncode = proc.wait()
        duration = time.time() - start
        self._unregister_proc(proc)

        output = "\n".join(assistant_messages)
        if returncode != 0 and not output:
            output = "\n".join(transcript_lines)

        return AgentResult(
            exit_code=returncode,
            output=output,
            transcript="\n".join(transcript_lines),
            duration=duration,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            session_id=thread_id,
        )


def _load_codex_sdk() -> Any | None:
    """Import the Codex Python SDK, returning the module or None if absent.

    Ships as `openai_codex_sdk` (`pip install openai-codex-sdk`); some early builds
    exposed it as `openai_codex`, so we try both. Absence is expected on the default
    subprocess path, so callers treat None as "SDK backend unavailable".
    """
    for module_name in ("openai_codex_sdk", "openai_codex"):
        try:
            return __import__(module_name)
        except ImportError:
            continue
    return None


class CodexSDKBackend(Backend):
    """Codex backend driving the OpenAI Codex Python SDK in-process instead of `npx`.

    Opt-in via `backend: codex-sdk` (or `JUVENAL_BACKEND_CODEX_SDK=1`); the subprocess
    `CodexBackend` remains the default. Targets the `npx @openai/codex@latest` startup
    races (ENOTEMPTY/ETXTBSY on parallel spawns) and transient auth.json 401s: the SDK
    talks to a persistent local Codex app-server over JSON-RPC and reuses existing Codex
    auth, so there's no per-call npx unpack. Sessions resume by thread id via the SDK's
    `resume_thread`, mapping onto Codex's `~/.codex/sessions` store, and it returns the
    identical `AgentResult` contract (session_id carries the Codex thread id).
    """

    def __init__(self) -> None:
        super().__init__()
        self._sdk = _load_codex_sdk()

    @property
    def sdk_available(self) -> bool:
        return self._sdk is not None

    def name(self) -> str:
        return "codex-sdk"

    def run_agent(
        self,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        system_prompt: str | None = None,
        session_id: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        # Codex has no separate system-prompt slot; fold it into the user message so
        # it is not silently dropped, matching subprocess CodexBackend behavior. Codex
        # assigns its own thread id, so the externally chosen session_id is ignored, as
        # is hooks_config (no Claude-settings equivalent).
        del session_id, hooks_config
        if system_prompt is not None:
            prompt = f"{system_prompt}\n\n{prompt}"
        return self._run_codex_sdk_query(
            prompt=prompt,
            working_dir=working_dir,
            display_callback=display_callback,
            timeout=timeout,
            env=env,
            model=model,
            resume_thread_id=None,
        )

    def resume_agent(
        self,
        session_id: str,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None = None,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
        model: str | None = None,
        hooks_config: dict[str, Any] | None = None,
    ) -> AgentResult:
        del hooks_config  # Codex has no Claude-settings equivalent.
        result = self._run_codex_sdk_query(
            prompt=prompt,
            working_dir=working_dir,
            display_callback=display_callback,
            timeout=timeout,
            env=env,
            model=model,
            resume_thread_id=session_id,
        )
        # Preserve the caller's thread id if the SDK didn't surface a fresh one.
        if result.session_id is None:
            result.session_id = session_id
        return result

    def resume_interactive(
        self,
        session_id: str,
        working_dir: str,
        env: dict[str, str] | None = None,
        model: str | None = None,
    ) -> InteractiveResult:
        # No in-process interactive TUI over the SDK; hand off to the Codex CLI so the
        # user still gets a terminal on the resumed thread.
        return CodexBackend().resume_interactive(session_id, working_dir, env=env, model=model)

    def _run_codex_sdk_query(
        self,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None,
        timeout: int | None,
        env: dict[str, str] | None,
        model: str | None,
        resume_thread_id: str | None,
    ) -> AgentResult:
        if self._sdk is None:
            raise RuntimeError(
                "codex-sdk backend requires the OpenAI Codex Python SDK. Install with: pip install openai-codex-sdk"
            )
        # SDK wiring (Codex()/start_thread/resume_thread, run_streamed event drain,
        # AgentResult mapping) lives in _drive_codex_sdk so it can be filled in and
        # E2E-verified by a human once the SDK is installed; the contract shape above
        # is what the runner depends on.
        return self._drive_codex_sdk(
            prompt=prompt,
            working_dir=working_dir,
            display_callback=display_callback,
            timeout=timeout,
            env=env,
            model=model,
            resume_thread_id=resume_thread_id,
        )

    def _drive_codex_sdk(
        self,
        prompt: str,
        working_dir: str,
        display_callback: Callable[[str], None] | None,
        timeout: int | None,
        env: dict[str, str] | None,
        model: str | None,
        resume_thread_id: str | None,
    ) -> AgentResult:
        raise NotImplementedError(
            "CodexSDKBackend._drive_codex_sdk is not implemented yet. "
            "Fill in the OpenAI Codex Python SDK thread loop (Codex().start_thread / "
            "resume_thread, run_streamed event drain) and verify against "
            "tests/test_e2e_codex.py before relying on this backend."
        )


def _extend_with_settings(cmd: list[str], hooks_config: dict[str, Any] | None) -> None:
    """Append `--settings <json>` to `cmd` when a Claude settings fragment is given.

    The Claude CLI's `--settings` flag accepts an inline JSON string and merges it
    over on-disk settings; role guardrails ride in as `{"permissions": {"deny": [...]}}`.
    """
    if hooks_config:
        cmd.extend(["--settings", json.dumps(hooks_config)])


def create_backend(name: str) -> Backend:
    """Factory to create a backend by name.

    `claude-sdk` and `codex-sdk` are opt-in and fall back to their subprocess backends
    (`ClaudeBackend` / `CodexBackend`) when the corresponding SDK isn't installed, so
    existing workflows never break. Set `JUVENAL_BACKEND_SDK=1` / `JUVENAL_BACKEND_CODEX_SDK=1`
    to fail loud instead of falling back — for the human verifying the SDK path.
    """
    if name == "claude":
        return ClaudeBackend()
    elif name == "codex":
        return CodexBackend()
    elif name == "claude-sdk":
        backend = ClaudeSDKBackend()
        if backend.sdk_available:
            return backend
        if os.environ.get("JUVENAL_BACKEND_SDK", "0") == "1":
            raise RuntimeError(
                "backend 'claude-sdk' requested with JUVENAL_BACKEND_SDK=1 but the "
                "Claude Agent SDK is not installed. Install with: pip install claude-agent-sdk"
            )
        return ClaudeBackend()
    elif name == "codex-sdk":
        codex_sdk_backend = CodexSDKBackend()
        if codex_sdk_backend.sdk_available:
            return codex_sdk_backend
        if os.environ.get("JUVENAL_BACKEND_CODEX_SDK", "0") == "1":
            raise RuntimeError(
                "backend 'codex-sdk' requested with JUVENAL_BACKEND_CODEX_SDK=1 but the "
                "OpenAI Codex Python SDK is not installed. Install with: pip install openai-codex-sdk"
            )
        return CodexBackend()
    else:
        raise ValueError(f"Unknown backend: {name!r}. Must be 'claude', 'claude-sdk', 'codex', or 'codex-sdk'.")


def _parse_json_event(line: str) -> dict | None:
    """Try to parse a line as a JSON event."""
    line = line.strip()
    if not line.startswith("{"):
        return None
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _process_claude_event(event: dict) -> tuple[str, str]:
    """Process a Claude stream-json event.

    Returns (display_text, assistant_text).
    """
    event_type = event.get("type", "")

    # Claude stream-json types
    if event_type == "assistant":
        text = event.get("message", "")
        if isinstance(text, dict):
            text = text.get("content", "")
        if isinstance(text, list):
            parts = []
            for block in text:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(block.get("text", ""))
            text = "\n".join(parts)
        if text:
            return text, text
        return "", ""

    if event_type == "content_block_delta":
        delta = event.get("delta", {})
        text = delta.get("text", "")
        return text, ""

    if event_type == "result":
        # Final result message
        text = event.get("result", "")
        if text:
            return text, text
        # Handle subtype
        subtype = event.get("subtype", "")
        if subtype == "success":
            return "", ""
        return "", ""

    if event_type == "tool_use":
        tool_name = event.get("name", event.get("tool", "unknown"))
        return f"[tool: {tool_name}]", ""

    if event_type == "system":
        msg = event.get("message", "")
        return f"[system] {msg}" if msg else "", ""

    return "", ""


def _process_codex_event(event: dict) -> tuple[str, str]:
    """Process a Codex NDJSON event.

    Returns (display_text, assistant_text).
    """
    event_type = event.get("type", "")

    if event_type == "item.completed":
        item = event.get("item", {})
        item_type = item.get("type", "")
        text = item.get("text", "")

        if item_type == "reasoning":
            return f"[thinking] {text[:200]}", ""
        elif item_type == "agent_message":
            return text, text
        elif item_type == "tool_call":
            tool_name = item.get("name", "unknown")
            return f"[tool: {tool_name}]", ""
        elif text:
            return text, text
        return "", ""

    if event_type == "turn.completed":
        usage = event.get("usage", {})
        if usage:
            inp = usage.get("input_tokens", 0)
            out = usage.get("output_tokens", 0)
            return f"[tokens: {inp} in, {out} out]", ""
        return "", ""

    return "", ""


def _extract_claude_tokens(event: dict) -> tuple[int, int]:
    """Extract token usage from a Claude event. Returns (input_tokens, output_tokens)."""
    if event.get("type") == "result":
        usage = event.get("usage", {})
        if usage:
            return usage.get("input_tokens", 0), usage.get("output_tokens", 0)
    return 0, 0


def _extract_codex_tokens(event: dict) -> tuple[int, int]:
    """Extract token usage from a Codex event. Returns (input_tokens, output_tokens)."""
    if event.get("type") == "turn.completed":
        usage = event.get("usage", {})
        if usage:
            return usage.get("input_tokens", 0), usage.get("output_tokens", 0)
    return 0, 0
