"""Tests for the line-scrolling chat dashboard."""

from __future__ import annotations

from juvenal.dynamic.chat_display import ChatDashboard, make_chat_dashboard


def test_dashboard_start_stop_is_idempotent(capsys):
    dashboard = ChatDashboard()
    dashboard.start()
    assert dashboard.is_running()
    dashboard.start()  # idempotent
    dashboard.stop()
    assert not dashboard.is_running()
    dashboard.stop()  # idempotent
    captured = capsys.readouterr().out
    # Start banner appears once; stop banner appears once.
    assert captured.count("[chat] interactive analysis started") == 1
    assert captured.count("[chat] interactive analysis ended") == 1


def test_dashboard_renders_captain_message(capsys):
    dashboard = ChatDashboard()
    dashboard.start()
    dashboard.render_captain(
        message_to_user="Pivoting to codecs.",
        mental_model_summary="SUBSYSTEMS:\n  - net/parser [covered]",
        open_questions=["Why is parse_chunk reachable?"],
        turn_index=4,
    )
    dashboard.stop()
    captured = capsys.readouterr().out
    assert "[captain turn 4] Pivoting to codecs." in captured


def test_dashboard_renders_events_with_timestamp(capsys):
    dashboard = ChatDashboard()
    dashboard.start()
    dashboard.render_event(kind="claim.verified", text="claim-1")
    dashboard.render_event(kind="target.completed", text="target=foo")
    dashboard.stop()
    captured = capsys.readouterr().out
    assert "claim.verified claim-1" in captured
    assert "target.completed target=foo" in captured


def test_show_captain_full_prints_full_state(capsys):
    dashboard = ChatDashboard()
    dashboard.start()
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
    dashboard.stop()
    captured = capsys.readouterr().out
    assert "/show captain (turn 2)" in captured
    assert "halfway done" in captured
    assert "1. Q1" in captured
    assert "2. Q2" in captured


def test_dashboard_render_frontier_is_a_no_op(capsys):
    dashboard = ChatDashboard()
    dashboard.start()
    capsys.readouterr()  # discard start banner
    dashboard.render_frontier({"queued": 3, "running": 1}, [("t1", "running")])
    captured = capsys.readouterr().out
    assert captured == ""


def test_make_chat_dashboard_always_returns_chat_dashboard():
    assert isinstance(make_chat_dashboard(), ChatDashboard)
    assert isinstance(make_chat_dashboard(plain=True), ChatDashboard)
    assert isinstance(make_chat_dashboard(plain=False), ChatDashboard)
