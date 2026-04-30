"""Tests for the chat dashboard rendering surface."""

from __future__ import annotations

import io

import pytest

from juvenal.dynamic.chat_display import ChatDashboard, _PlainChatDashboard, make_chat_dashboard

rich = pytest.importorskip("rich")
from rich.console import Console  # noqa: E402


def _make_dashboard(history_size: int = 200) -> tuple[ChatDashboard, io.StringIO]:
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=True, width=160, color_system=None)
    dashboard = ChatDashboard(console=console, history_size=history_size, refresh_per_second=30.0)
    return dashboard, buf


def test_dashboard_start_stop_idempotent():
    dashboard, _ = _make_dashboard()
    dashboard.start()
    assert dashboard.is_running()
    dashboard.start()  # idempotent
    assert dashboard.is_running()
    dashboard.stop()
    assert not dashboard.is_running()
    dashboard.stop()  # idempotent


def test_dashboard_renders_captain_panel():
    dashboard, buf = _make_dashboard()
    dashboard.start()
    try:
        dashboard.render_captain(
            message_to_user="Pivoting to codecs.",
            mental_model_summary="SUBSYSTEMS:\n  - net/parser [covered]",
            open_questions=["Why is parse_chunk reachable?"],
            turn_index=4,
        )
    finally:
        dashboard.stop()
    output = buf.getvalue()
    assert "Turn 4" in output
    assert "Pivoting to codecs." in output
    assert "net/parser" in output


def test_dashboard_event_stream_rolls_at_capacity():
    dashboard, buf = _make_dashboard(history_size=3)
    dashboard.start()
    try:
        for index in range(5):
            dashboard.render_event(kind="info", text=f"event-{index}")
    finally:
        dashboard.stop()
    output = buf.getvalue()
    # First two events should have rolled off; last three remain.
    assert "event-0" not in output
    assert "event-1" not in output
    assert "event-2" in output
    assert "event-3" in output
    assert "event-4" in output


def test_dashboard_show_captain_full_round_trips_through_live():
    dashboard, buf = _make_dashboard()
    dashboard.start()
    try:
        dashboard.render_captain(
            message_to_user="halfway done",
            mental_model_summary="SUBSYSTEMS:\n  - net [active]",
            open_questions=["Q1", "Q2"],
            turn_index=2,
        )
        dashboard.show_captain_full(
            message_to_user="halfway done",
            mental_model_summary="SUBSYSTEMS:\n  - net [active]",
            open_questions=["Q1", "Q2"],
        )
        # Live should have restarted; render_event still works.
        dashboard.render_event(kind="info", text="post-show")
        assert dashboard.is_running()
    finally:
        dashboard.stop()
    output = buf.getvalue()
    assert "/show captain" in output
    assert "Q1" in output
    assert "post-show" in output


def test_dashboard_chat_input_panel_includes_history():
    dashboard, buf = _make_dashboard()
    dashboard.start()
    try:
        dashboard.render_chat_input(["/focus parser", "/now"])
    finally:
        dashboard.stop()
    output = buf.getvalue()
    assert "/focus parser" in output
    assert "/now" in output


def test_plain_dashboard_falls_back_to_print(capsys):
    dashboard = _PlainChatDashboard()
    dashboard.start()
    dashboard.render_captain(
        message_to_user="reading the tree",
        mental_model_summary="SUBSYSTEMS:\n  - net [active]",
        open_questions=[],
        turn_index=1,
    )
    dashboard.render_event(kind="claim.verified", text="claim-1")
    dashboard.show_captain_full(
        message_to_user="reading the tree",
        mental_model_summary="SUBSYSTEMS:\n  - net [active]",
        open_questions=["Q1"],
    )
    dashboard.stop()
    captured = capsys.readouterr().out
    assert "[chat] dashboard started" in captured
    assert "[captain turn 1]" in captured
    assert "claim.verified claim-1" in captured
    assert "/show captain" in captured
    assert "[chat] dashboard stopped" in captured


def test_make_chat_dashboard_returns_plain_for_non_tty(monkeypatch):
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    dashboard = make_chat_dashboard()
    assert isinstance(dashboard, _PlainChatDashboard)


def test_make_chat_dashboard_returns_plain_when_requested():
    dashboard = make_chat_dashboard(plain=True)
    assert isinstance(dashboard, _PlainChatDashboard)
