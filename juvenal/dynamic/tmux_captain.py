"""Tmux-based interactive captain session for analysis phases."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import uuid
from pathlib import Path
from threading import Event, Thread
from typing import Any, Callable


class TmuxCaptainSession:
    """Manage a Claude Code captain running inside a tmux session."""

    def __init__(
        self,
        session_name: str,
        working_dir: Path,
        dispatch_file: Path,
        results_file: Path,
        *,
        model: str | None = None,
        claude_path: str | None = None,
    ) -> None:
        self.session_name = session_name
        self.working_dir = working_dir
        self.dispatch_file = dispatch_file
        self.results_file = results_file
        self.model = model
        self.claude_path = claude_path or shutil.which("claude") or "claude"
        # Claude Code requires a UUID for --session-id.
        self.session_id = str(uuid.uuid4())
        self._started = False

    def _build_claude_cmd(self) -> str:
        parts = [
            shlex.quote(self.claude_path),
            f"--session-id={self.session_id}",
            "--dangerously-skip-permissions",
            "--verbose",
        ]
        if self.model:
            parts.extend(["--model", shlex.quote(self.model)])
        return " ".join(parts)

    def start(self, prompt: str, *, env: dict[str, str] | None = None) -> None:
        """Start the captain as a Claude Code session inside a detached tmux session."""
        prompt_file = self.working_dir / ".juvenal" / "captain-prompt.md"
        prompt_file.parent.mkdir(parents=True, exist_ok=True)
        prompt_file.write_text(prompt, encoding="utf-8")

        claude_cmd = self._build_claude_cmd()

        cmd = [
            "tmux",
            "new-session",
            "-d",
            "-s",
            self.session_name,
            "-c",
            str(self.working_dir),
            claude_cmd,
        ]

        run_env = dict(os.environ)
        # Allow nested tmux sessions (when juvenal is run from inside tmux)
        run_env.pop("TMUX", None)
        run_env.pop("TMUX_PANE", None)
        if env:
            run_env.update(env)

        result = subprocess.run(cmd, env=run_env, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to start tmux session '{self.session_name}': "
                f"{result.stderr.strip() or result.stdout.strip() or f'exit code {result.returncode}'}"
            )
        self._started = True

        # Claude Code may show setup dialogs (trust prompt, text style, etc.) before the input prompt.
        # Poll the pane content and dismiss each dialog until we see the input prompt.
        self._wait_for_input_prompt()

        # Now send the initial mission prompt
        read_instruction = f"Read and follow the instructions in {prompt_file} — that is your mission."
        self.inject(read_instruction)

    def inject(self, text: str) -> None:
        """Send text into the tmux session as if the user typed it."""
        if not self.is_alive():
            return
        # Send the text, then Enter twice (Claude Code TUI needs double Enter to submit)
        full_text = text.replace("\n", " ")[:4000]  # Flatten + limit length
        subprocess.run(
            ["tmux", "send-keys", "-t", self.session_name, full_text, "Enter", "Enter"],
            capture_output=True,
        )

    def _capture_pane(self) -> str:
        """Capture the current tmux pane content as plain text."""
        result = subprocess.run(
            ["tmux", "capture-pane", "-t", self.session_name, "-p", "-S", "-30"],
            capture_output=True,
            text=True,
        )
        return result.stdout if result.returncode == 0 else ""

    def _send_key(self, *keys: str) -> None:
        """Send raw keys to the tmux session."""
        subprocess.run(
            ["tmux", "send-keys", "-t", self.session_name, *keys],
            capture_output=True,
        )

    def _wait_for_input_prompt(self, max_attempts: int = 30, interval: float = 2.0) -> None:
        """Poll the pane and dismiss Claude Code setup dialogs until the input prompt appears."""
        import time as _time

        for _ in range(max_attempts):
            _time.sleep(interval)
            if not self.is_alive():
                return

            pane = self._capture_pane()

            # Detect the input prompt (❯ or > at start of line)
            if "❯" in pane or "\n> " in pane:
                return

            # Trust/accept prompt — select the acceptance option
            if "Yes, I trust" in pane or "Yes, I accept" in pane:
                self._send_key("Enter")
                continue

            # Setup dialogs (text style, getting started, etc.)
            if "Choose the text style" in pane or "Let's get started" in pane or "Enter to confirm" in pane:
                self._send_key("Enter")
                continue

            # If pane has content but no recognizable dialog, send Enter as a generic dismissal
            if pane.strip():
                self._send_key("Enter")
                continue

    def is_alive(self) -> bool:
        """Check if the tmux session still exists."""
        result = subprocess.run(
            ["tmux", "has-session", "-t", self.session_name],
            capture_output=True,
        )
        return result.returncode == 0

    def attach_to_current(self) -> None:
        """Move the captain window into the current tmux session (if inside tmux)."""
        current_session = os.environ.get("TMUX")
        if not current_session or not self.is_alive():
            return
        # link-window brings the captain window into the current session as a new window
        subprocess.run(
            ["tmux", "link-window", "-s", f"{self.session_name}:0", "-a"],
            capture_output=True,
        )
        # Switch to the newly linked window
        subprocess.run(
            ["tmux", "select-window", "-t", ":.+"],
            capture_output=True,
        )

    def kill(self) -> None:
        """Kill the tmux session."""
        subprocess.run(
            ["tmux", "kill-session", "-t", self.session_name],
            capture_output=True,
        )
        self._started = False


class FileWatcher:
    """Monitor a JSONL file for new lines on a background thread."""

    def __init__(self, path: Path, callback: Callable[[dict[str, Any]], None]) -> None:
        self.path = path
        self._callback = callback
        self._stop_event = Event()
        self._thread: Thread | None = None
        self._position = 0

    def start(self) -> None:
        """Start the background file watcher thread."""
        # Initialize file position to current end (skip existing content on resume)
        if self.path.exists():
            self._position = self.path.stat().st_size
        self._stop_event.clear()
        self._thread = Thread(target=self._watch_loop, name="juvenal-dispatch-watcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Signal the watcher to stop."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _watch_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._poll_file()
            except Exception:
                pass  # Resilient to transient file errors
            self._stop_event.wait(0.5)

    def _poll_file(self) -> None:
        if not self.path.exists():
            return
        size = self.path.stat().st_size
        if size <= self._position:
            return
        with open(self.path, encoding="utf-8") as f:
            f.seek(self._position)
            new_content = f.read()
            self._position = f.tell()
        for line in new_content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                if isinstance(data, dict):
                    self._callback(data)
            except json.JSONDecodeError:
                continue  # Skip malformed lines


__all__ = ["FileWatcher", "TmuxCaptainSession"]
