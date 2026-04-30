"""Line-scrolling chat dashboard for the dynamic analysis runner.

Renders captain output, worker/verifier events, and acknowledged directives
to stdout as they happen. Designed to coexist with `sys.stdin.readline()`-based
user input — events scroll past, the user's typed line stays put, no live
redraw fights with terminal echo. Rich `Live` was attempted earlier and
removed because it cannot cleanly share the cursor with raw stdin reads.
"""

from __future__ import annotations

import re
import time
from collections import deque
from threading import Lock
from typing import Iterable

# Markdown code-fence line, optionally with a language tag (```text, ```bash).
_MARKDOWN_FENCE_RE = re.compile(r"^\s*```[\w-]*\s*$")


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
        # Markdown fences often wrap CAPTAIN_JSON_BEGIN..END. Hold a fence
        # opener line in suspense until we know whether it's wrapping the
        # JSON (drop it) or wrapping something else (flush it).
        self._pending_fence_line: str | None = None
        # When CAPTAIN_JSON_END has just been seen, drop the next fence line
        # (the fence closer that wrapped the JSON).
        self._expect_fence_close: bool = False
        # Claude Code's stream-json emits `assistant` events with cumulative
        # content (each new event contains the entire response so far, not
        # just the new delta). Track the last forwarded chunk so we can print
        # only the new suffix on subsequent cumulative events.
        self._last_streamed_chunk: str = ""

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
        """End-of-turn state update.

        The captain's message_to_user already streamed line-by-line via
        render_captain_chunk as the response was generated. Printing it again
        here would duplicate it on screen. Just reset per-turn streaming
        state for the next captain turn and emit a brief boundary marker."""
        with self._lock:
            self._captain_turn_index = turn_index
            self._last_streamed_chunk = ""
            self._suppressing_captain_json = False
            self._pending_fence_line = None
            self._expect_fence_close = False
        print(f"\n[captain turn {turn_index} ✓]", flush=True)

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

        Claude Code's stream-json emits cumulative assistant events: chunk N+1
        contains chunk N's content plus the newly-streamed delta. We dedupe
        by checking if the new chunk is a strict superset of what we've
        already printed and emitting only the new suffix.
        """
        if not text:
            return

        # Cumulative-event dedup. If the new chunk is identical to or a
        # prefix of the last, drop it. If it's a strict superset, print only
        # the suffix that hasn't been streamed yet.
        if text == self._last_streamed_chunk:
            return
        if self._last_streamed_chunk and text.startswith(self._last_streamed_chunk):
            new_text = text[len(self._last_streamed_chunk) :]
            self._last_streamed_chunk = text
        elif self._last_streamed_chunk and self._last_streamed_chunk.startswith(text):
            return  # already shown
        else:
            new_text = text
            self._last_streamed_chunk = text

        for line in new_text.splitlines():
            if "CAPTAIN_JSON_BEGIN" in line:
                # The fence opener (if any) was just wrapping the JSON — drop it.
                self._pending_fence_line = None
                self._suppressing_captain_json = True
                print("  [captain → emitting CAPTAIN_JSON …]", flush=True)
                continue
            if "CAPTAIN_JSON_END" in line:
                self._suppressing_captain_json = False
                self._expect_fence_close = True
                continue
            if self._suppressing_captain_json:
                continue
            if _MARKDOWN_FENCE_RE.match(line):
                if self._expect_fence_close:
                    # The fence closer wrapped the JSON — drop it.
                    self._expect_fence_close = False
                    continue
                # Could be a fence opener wrapping JSON (which we'd drop) or
                # wrapping something else (which we'd flush). Hold it until
                # the next non-blank line tells us which.
                if self._pending_fence_line is not None:
                    print(f"  {self._pending_fence_line}", flush=True)
                self._pending_fence_line = line
                continue
            # Non-fence, non-marker content. Blank lines don't decide whether
            # the pending fence wraps JSON — they can sit between the fence
            # and CAPTAIN_JSON_BEGIN. Only a non-blank line forces us to
            # commit (flush the pending fence as real content).
            if line.strip():
                if self._pending_fence_line is not None:
                    print(f"  {self._pending_fence_line}", flush=True)
                    self._pending_fence_line = None
                if self._expect_fence_close:
                    self._expect_fence_close = False
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
