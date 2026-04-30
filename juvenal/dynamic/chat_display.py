"""Rich Live chat dashboard for the dynamic analysis runner."""

from __future__ import annotations

import sys
import time
from collections import deque
from threading import Lock
from typing import Iterable

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


_EVENT_GLYPHS: dict[str, str] = {
    "captain.turn": "•",
    "captain.starting": "↻",
    "captain.error": "!",
    "worker.completed": "✓",
    "worker.no_findings": "·",
    "worker.blocked": "⛌",
    "worker.exhausted": "⨯",
    "claim.verified": "✓",
    "claim.rejected": "✗",
    "verifier.passed": "✓",
    "verifier.failed": "✗",
    "directive.received": ">>",
    "directive.applied": ">>",
    "info": "i",
}


class ChatDashboard:
    """Rich-based live chat dashboard for analysis runs.

    Render-only. The runner calls render hooks; the dashboard does not read
    stdin and does not mutate runner state. Hooks are thread-safe via a Lock.
    """

    def __init__(
        self,
        *,
        console: "Console | None" = None,
        history_size: int = 200,
        chat_history_size: int = 8,
        refresh_per_second: float = 4.0,
    ) -> None:
        if not RICH_AVAILABLE:
            raise RuntimeError("rich is required for ChatDashboard; use _PlainChatDashboard instead")
        self._console = console or Console()
        self._refresh_per_second = refresh_per_second
        self._lock = Lock()
        self._events: deque[tuple[float, str, str]] = deque(maxlen=history_size)
        self._chat_history: deque[str] = deque(maxlen=chat_history_size)
        self._captain_turn_index: int = 0
        self._captain_message: str = ""
        self._captain_mental_model: str = ""
        self._captain_open_questions: list[str] = []
        self._frontier_counts: dict[str, int] = {}
        self._frontier_active: list[tuple[str, str]] = []
        self._live: "Live | None" = None
        self._running = False

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._live = Live(
                self._build_layout(),
                console=self._console,
                refresh_per_second=self._refresh_per_second,
                transient=False,
                auto_refresh=True,
            )
            self._live.start()
            self._running = True

    def stop(self) -> None:
        with self._lock:
            if not self._running or self._live is None:
                return
            self._live.stop()
            self._live = None
            self._running = False

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
            self._captain_message = message_to_user
            self._captain_mental_model = mental_model_summary
            self._captain_open_questions = list(open_questions)
            self._refresh_locked()

    def render_event(self, *, kind: str, text: str, ts: float | None = None) -> None:
        with self._lock:
            self._events.append((ts if ts is not None else time.time(), kind, text))
            self._refresh_locked()

    def render_frontier(self, counts: dict[str, int], active_targets: Iterable[tuple[str, str]]) -> None:
        with self._lock:
            self._frontier_counts = dict(counts)
            self._frontier_active = list(active_targets)
            self._refresh_locked()

    def render_chat_input(self, history: Iterable[str]) -> None:
        with self._lock:
            self._chat_history = deque(history, maxlen=self._chat_history.maxlen)
            self._refresh_locked()

    def show_captain_full(
        self,
        *,
        message_to_user: str,
        mental_model_summary: str,
        open_questions: Iterable[str],
    ) -> None:
        with self._lock:
            was_running = self._running and self._live is not None
            if was_running:
                self._live.stop()
                self._live = None
                self._running = False

            body = Text()
            body.append(f"Captain turn: {self._captain_turn_index}\n\n", style="bold cyan")
            body.append("Message to user:\n", style="bold")
            body.append(f"{message_to_user.strip() or '(none)'}\n\n")
            body.append("Mental model:\n", style="bold")
            body.append(f"{mental_model_summary.strip() or '(none)'}\n\n")
            body.append("Open questions:\n", style="bold")
            questions = list(open_questions)
            if not questions:
                body.append("  (none)\n")
            else:
                for index, question in enumerate(questions, start=1):
                    body.append(f"  {index}. {question}\n")
            self._console.print(Panel(body, title="/show captain", border_style="cyan"))

            if was_running:
                self._live = Live(
                    self._build_layout(),
                    console=self._console,
                    refresh_per_second=self._refresh_per_second,
                    transient=False,
                    auto_refresh=True,
                )
                self._live.start()
                self._running = True

    def _refresh_locked(self) -> None:
        if self._live is not None:
            self._live.update(self._build_layout())

    def _build_layout(self) -> "Layout":
        layout = Layout(name="root")
        layout.split_column(
            Layout(self._build_captain_panel(), name="captain", ratio=4),
            Layout(self._build_event_panel(), name="events", ratio=5),
            Layout(self._build_input_panel(), name="input", ratio=1),
        )
        return layout

    def _build_captain_panel(self) -> "Panel":
        body = Text()
        body.append(f"Turn {self._captain_turn_index}", style="bold cyan")
        if self._captain_message:
            body.append(f"  ·  {self._captain_message.strip().splitlines()[0][:120]}", style="dim")
        body.append("\n\n")
        body.append("Mental model:\n", style="bold")
        for line in (self._captain_mental_model.strip() or "(none yet)").splitlines()[:14]:
            body.append(f"  {line}\n")
        if self._captain_open_questions:
            body.append("\nOpen questions:\n", style="bold")
            for index, question in enumerate(self._captain_open_questions[:4], start=1):
                body.append(f"  {index}. {question[:200]}\n")
        frontier_line = "  ".join(f"{status}={count}" for status, count in sorted(self._frontier_counts.items()))
        if frontier_line:
            body.append("\nFrontier: ", style="bold")
            body.append(frontier_line)
        return Panel(body, title="Captain", border_style="cyan")

    def _build_event_panel(self) -> "Panel":
        body = Text()
        if not self._events:
            body.append("(no events yet — workers and verifiers will appear here)", style="dim")
        else:
            for ts, kind, text in list(self._events)[-30:]:
                glyph = _EVENT_GLYPHS.get(kind, "·")
                stamp = time.strftime("%H:%M:%S", time.localtime(ts))
                style = self._style_for_event(kind)
                body.append(f"{stamp} {glyph} ", style="dim")
                body.append(f"{kind:<22}", style=style)
                body.append(f" {text}\n")
        return Panel(body, title="Events", border_style="green")

    def _build_input_panel(self) -> "Panel":
        body = Text()
        if self._chat_history:
            for line in list(self._chat_history)[-self._chat_history.maxlen :]:
                body.append(f">>> {line}\n", style="dim")
        body.append(">>> ", style="bold")
        return Panel(
            body,
            title="Chat (/focus /ignore /target /ask /now /show captain /summary /stop /wrap)",
            border_style="magenta",
        )

    @staticmethod
    def _style_for_event(kind: str) -> str:
        if kind.endswith("verified") or kind.endswith("passed") or kind == "worker.completed":
            return "green"
        if kind.endswith("rejected") or kind.endswith("failed") or kind == "captain.error":
            return "red"
        if kind == "worker.no_findings" or kind == "worker.blocked" or kind == "worker.exhausted":
            return "yellow"
        if kind.startswith("captain"):
            return "cyan"
        if kind.startswith("directive"):
            return "magenta"
        return "white"


class _PlainChatDashboard:
    """Plain-text fallback for non-tty / --plain runs.

    Implements the same render-hook surface as ChatDashboard so the runner does
    not branch on dashboard type.
    """

    def __init__(self, *, history_size: int = 200, chat_history_size: int = 8) -> None:
        self._chat_history: deque[str] = deque(maxlen=chat_history_size)
        self._captain_turn_index = 0
        self._lock = Lock()
        self._running = False

    def start(self) -> None:
        with self._lock:
            self._running = True
        print("[chat] dashboard started (plain mode)", flush=True)

    def stop(self) -> None:
        with self._lock:
            if not self._running:
                return
            self._running = False
        print("[chat] dashboard stopped", flush=True)

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
        first_line = (message_to_user.strip().splitlines() or [""])[0][:160]
        print(f"[captain turn {turn_index}] {first_line}", flush=True)

    def render_event(self, *, kind: str, text: str, ts: float | None = None) -> None:
        stamp = time.strftime("%H:%M:%S", time.localtime(ts if ts is not None else time.time()))
        print(f"{stamp} {kind} {text}", flush=True)

    def render_frontier(self, counts: dict[str, int], active_targets: Iterable[tuple[str, str]]) -> None:
        # Plain mode skips frontier polls — too noisy.
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


def make_chat_dashboard(*, plain: bool = False, history_size: int = 200) -> "ChatDashboard | _PlainChatDashboard":
    """Construct the appropriate dashboard for the current environment."""

    if plain or not RICH_AVAILABLE or not sys.stdout.isatty():
        return _PlainChatDashboard(history_size=history_size)
    return ChatDashboard(history_size=history_size)


__all__ = ["ChatDashboard", "_PlainChatDashboard", "make_chat_dashboard"]
