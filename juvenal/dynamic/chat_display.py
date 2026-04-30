"""Line-scrolling chat dashboard for the dynamic analysis runner.

Renders captain output, worker/verifier events, and acknowledged directives
to stdout as they happen. Designed to coexist with `sys.stdin.readline()`-based
user input — events scroll past, the user's typed line stays put, no live
redraw fights with terminal echo. Rich `Live` was attempted earlier and
removed because it cannot cleanly share the cursor with raw stdin reads.
"""

from __future__ import annotations

import time
from collections import deque
from threading import Lock
from typing import Iterable


class ChatDashboard:
    """Print-driven dashboard. Render hooks emit one line per event."""

    def __init__(self, *, history_size: int = 200, chat_history_size: int = 8) -> None:
        self._chat_history: deque[str] = deque(maxlen=chat_history_size)
        self._captain_turn_index = 0
        self._lock = Lock()
        self._running = False
        # State for filtering CAPTAIN_JSON blocks out of streamed chunks: the
        # structured output is for the runner, not for human eyes. We render
        # one placeholder line per block instead of the full JSON.
        self._suppressing_captain_json = False

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
        print(
            "\n[chat] interactive analysis started — type directives at any moment.\n"
            "  /focus TEXT  /ignore path:PFX  /ignore symbol:NAME  /target TEXT\n"
            "  /ask TEXT  /now  /show captain  /chat  /summary  /stop  /wrap\n"
            "  any free-form text becomes a `note` directive\n"
            "  /chat hands you the native claude / codex TUI on the captain's session\n",
            flush=True,
        )

    def stop(self) -> None:
        with self._lock:
            if not self._running:
                return
            self._running = False
        print("\n[chat] interactive analysis ended\n", flush=True)

    def is_running(self) -> bool:
        return self._running

    def render_captain(
        self,
        *,
        message_to_user: str,
        mental_model_summary: str,
        open_questions: Iterable[str],
        turn_index: int,
    ) -> None:
        with self._lock:
            self._captain_turn_index = turn_index
        first_line = (message_to_user.strip().splitlines() or [""])[0][:200]
        print(f"\n[captain turn {turn_index}] {first_line}", flush=True)

    def render_event(self, *, kind: str, text: str, ts: float | None = None) -> None:
        stamp = time.strftime("%H:%M:%S", time.localtime(ts if ts is not None else time.time()))
        print(f"{stamp} {kind} {text}", flush=True)

    def render_captain_chunk(self, text: str) -> None:
        """Print a single streamed chunk from the captain mid-turn.

        Backends emit one chunk per stream-json event. The text is already
        formatted by the backend's _process_*_event helper (assistant messages
        as-is; tool calls as `[tool: name]`). We indent so chunks visually
        nest under the most recent `[captain turn N]` header. Lines that
        belong to a CAPTAIN_JSON block are suppressed and replaced with a
        single placeholder — the structured output is what the runner parses,
        not what the user wants to read.
        """
        if not text:
            return
        for line in text.splitlines():
            if "CAPTAIN_JSON_BEGIN" in line:
                self._suppressing_captain_json = True
                print("  [captain → emitting CAPTAIN_JSON …]", flush=True)
                continue
            if "CAPTAIN_JSON_END" in line:
                self._suppressing_captain_json = False
                continue
            if self._suppressing_captain_json:
                continue
            print(f"  {line}", flush=True)

    def render_frontier(self, counts: dict[str, int], active_targets: Iterable[tuple[str, str]]) -> None:
        # No-op in line-scrolling mode — frontier counts would spam the terminal
        # on every loop tick. Use /show captain or read state to inspect.
        return

    def render_chat_input(self, history: Iterable[str]) -> None:
        with self._lock:
            self._chat_history = deque(history, maxlen=self._chat_history.maxlen)

    def show_captain_full(
        self,
        *,
        message_to_user: str,
        mental_model_summary: str,
        open_questions: Iterable[str],
    ) -> None:
        print(f"\n--- /show captain (turn {self._captain_turn_index}) ---", flush=True)
        print(f"Message: {message_to_user.strip() or '(none)'}", flush=True)
        print("Mental model:", flush=True)
        print(f"{mental_model_summary.strip() or '(none)'}", flush=True)
        questions = list(open_questions)
        if questions:
            print("Open questions:", flush=True)
            for index, question in enumerate(questions, start=1):
                print(f"  {index}. {question}", flush=True)
        print("---\n", flush=True)


def make_chat_dashboard(*, plain: bool = False, history_size: int = 200) -> ChatDashboard:
    """Construct the chat dashboard. The `plain` arg is accepted for backward
    compatibility but ignored — the dashboard is always line-scrolling because
    Rich Live cannot share the cursor with line-buffered stdin reads."""

    return ChatDashboard(history_size=history_size)


__all__ = ["ChatDashboard", "make_chat_dashboard"]
