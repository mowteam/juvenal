"""Unit tests for TmuxCaptainSession command construction."""

from __future__ import annotations

import uuid
from pathlib import Path

from juvenal.dynamic.tmux_captain import TmuxCaptainSession


def _make_session(tmp_path: Path, *, model: str | None = None) -> TmuxCaptainSession:
    return TmuxCaptainSession(
        session_name="juvenal-test",
        working_dir=tmp_path,
        dispatch_file=tmp_path / "dispatch.jsonl",
        results_file=tmp_path / "results.jsonl",
        model=model,
        claude_path="/usr/local/bin/claude",
    )


def test_session_id_is_a_valid_uuid(tmp_path):
    """Claude Code rejects non-UUID --session-id values; regression for the silent
    `success=True` exit when the tmux captain died on launch."""
    session = _make_session(tmp_path)
    parsed = uuid.UUID(session.session_id)
    assert str(parsed) == session.session_id


def test_build_claude_cmd_includes_session_id_and_required_flags(tmp_path):
    session = _make_session(tmp_path)
    cmd = session._build_claude_cmd()
    assert f"--session-id={session.session_id}" in cmd
    assert "--dangerously-skip-permissions" in cmd
    assert "--verbose" in cmd


def test_build_claude_cmd_includes_model_when_set(tmp_path):
    session = _make_session(tmp_path, model="claude-opus-4-7[1m]")
    cmd = session._build_claude_cmd()
    assert "--model" in cmd
    # Model contains shell metacharacters — confirm it is shell-quoted.
    assert "'claude-opus-4-7[1m]'" in cmd


def test_build_claude_cmd_omits_model_when_none(tmp_path):
    session = _make_session(tmp_path, model=None)
    cmd = session._build_claude_cmd()
    assert "--model" not in cmd


def test_claude_path_is_shell_quoted(tmp_path):
    session = TmuxCaptainSession(
        session_name="juvenal-test",
        working_dir=tmp_path,
        dispatch_file=tmp_path / "dispatch.jsonl",
        results_file=tmp_path / "results.jsonl",
        claude_path="/path with spaces/claude",
    )
    cmd = session._build_claude_cmd()
    assert "'/path with spaces/claude'" in cmd
