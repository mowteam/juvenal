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


def test_render_captain_chunk_suppresses_captain_json_block(capsys):
    """The CAPTAIN_JSON block is the runner's structured input — surface a
    single placeholder so the user can read the captain's reasoning without
    pages of structured output drowning it out."""
    dashboard = ChatDashboard()
    dashboard.start()
    capsys.readouterr()  # discard banner
    dashboard.render_captain_chunk("I will read the parser source.")
    dashboard.render_captain_chunk("[tool: Read]")
    dashboard.render_captain_chunk(
        'CAPTAIN_JSON_BEGIN\n{"message_to_user":"long","enqueue_targets":[]}\nCAPTAIN_JSON_END'
    )
    dashboard.render_captain_chunk("Trailing text after the block.")
    dashboard.stop()
    captured = capsys.readouterr().out
    assert "I will read the parser source." in captured
    assert "[tool: Read]" in captured
    assert "[captain → emitting CAPTAIN_JSON …]" in captured
    assert "enqueue_targets" not in captured
    assert "message_to_user" not in captured
    assert "CAPTAIN_JSON_BEGIN" not in captured
    assert "CAPTAIN_JSON_END" not in captured
    assert "Trailing text after the block." in captured


def test_render_captain_chunk_suppresses_captain_json_split_across_chunks(capsys):
    """The BEGIN/END markers can land in different stream chunks; suppression
    must persist across chunks until END is seen."""
    dashboard = ChatDashboard()
    dashboard.start()
    capsys.readouterr()
    dashboard.render_captain_chunk("CAPTAIN_JSON_BEGIN\n{")
    dashboard.render_captain_chunk('  "termination_state": "continue",')
    dashboard.render_captain_chunk('  "enqueue_targets": []')
    dashboard.render_captain_chunk("}\nCAPTAIN_JSON_END")
    dashboard.render_captain_chunk("post-block text")
    dashboard.stop()
    captured = capsys.readouterr().out
    assert "termination_state" not in captured
    assert "enqueue_targets" not in captured
    assert "post-block text" in captured
    assert captured.count("[captain → emitting CAPTAIN_JSON …]") == 1


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
