"""Tmux-based interactive captain session for analysis phases."""

from __future__ import annotations

import json
import os
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
    ) -> None:
        self.session_name = session_name
        self.working_dir = working_dir
        self.dispatch_file = dispatch_file
        self.results_file = results_file
        self.session_id = f"juvenal-captain-{uuid.uuid4().hex[:12]}"
        self._started = False

    def start(self, prompt: str, *, env: dict[str, str] | None = None) -> None:
        """Start the captain as a Claude Code session inside a detached tmux session."""
        # Write prompt to a temp file so it can be arbitrarily long
        prompt_file = self.working_dir / ".juvenal" / "captain-prompt.md"
        prompt_file.parent.mkdir(parents=True, exist_ok=True)
        prompt_file.write_text(prompt, encoding="utf-8")

        import shutil
        import time as _time

        claude_path = shutil.which("claude") or "claude"
        # Start claude in interactive TUI mode (no positional prompt arg).
        # The initial message is sent via send-keys after the session starts.
        claude_cmd = f"{claude_path} --session-id={self.session_id} --dangerously-skip-permissions --verbose"

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

        # Wait for claude TUI to initialize, then send the initial prompt
        _time.sleep(2.0)
        read_instruction = f"Read and follow the instructions in {prompt_file} — that is your mission."
        self.inject(read_instruction)

    def inject(self, text: str) -> None:
        """Send text into the tmux session as if the user typed it."""
        if not self.is_alive():
            return
        # Use tmux send-keys to inject. Split into lines to avoid issues with long text.
        for line in text.splitlines():
            subprocess.run(
                ["tmux", "send-keys", "-t", self.session_name, line, "Enter"],
                capture_output=True,
            )

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
