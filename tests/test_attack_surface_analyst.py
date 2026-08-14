"""Tests for the attack-surface analyst feature."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from juvenal.display import Display
from juvenal.dynamic.models import (
    AttackSurfaceState,
    VerificationRecord,
    WorkerAttempt,
)
from juvenal.dynamic.runner import (
    _SESSION_STALENESS_THRESHOLD_SECONDS,
    DynamicAnalysisRunner,
)
from juvenal.dynamic.state import DynamicSessionState
from juvenal.workflow import (
    AnalysisConfig,
    AnalystSpec,
    Phase,
    Workflow,
    _parse_analyst_spec,
    load_workflow,
)
from tests.conftest import MockBackend
from tests.test_dynamic_runner import _captain_output

# --- Workflow YAML parsing ----------------------------------------------------


def test_parse_analyst_spec_minimal():
    spec = _parse_analyst_spec(
        {"prompt": "do the thing"},
        phase_id="analyze",
        default_backend="claude",
        default_model=None,
        yaml_path=None,
    )
    assert spec.backend == "claude"
    assert spec.model is None
    assert spec.prompt == "do the thing"
    assert spec.enabled is True
    assert spec.max_duration_seconds == 1800


def test_parse_analyst_spec_full_override():
    spec = _parse_analyst_spec(
        {
            "backend": "codex",
            "model": "claude-sonnet-4-6",
            "prompt": "investigate",
            "enabled": False,
            "max_duration_seconds": 300,
        },
        phase_id="analyze",
        default_backend="claude",
        default_model=None,
        yaml_path=None,
    )
    assert spec.backend == "codex"
    assert spec.model == "claude-sonnet-4-6"
    assert spec.enabled is False
    assert spec.max_duration_seconds == 300


def test_parse_analyst_spec_rejects_both_prompt_and_prompt_file():
    with pytest.raises(ValueError, match="cannot set both prompt and prompt_file"):
        _parse_analyst_spec(
            {"prompt": "x", "prompt_file": "y.md"},
            phase_id="p",
            default_backend="claude",
            default_model=None,
            yaml_path=Path("/tmp"),
        )


def test_parse_analyst_spec_rejects_unknown_keys():
    with pytest.raises(ValueError, match="unknown keys"):
        _parse_analyst_spec(
            {"prompt": "x", "garbage": True},
            phase_id="p",
            default_backend="claude",
            default_model=None,
            yaml_path=None,
        )


def test_parse_analyst_spec_rejects_invalid_backend():
    with pytest.raises(ValueError, match="must be one of"):
        _parse_analyst_spec(
            {"prompt": "x", "backend": "ollama"},
            phase_id="p",
            default_backend="claude",
            default_model=None,
            yaml_path=None,
        )


def test_workflow_yaml_with_analyst_block(tmp_path):
    yaml_path = tmp_path / "wf.yaml"
    yaml_path.write_text(
        """
name: t
backend: claude
phases:
  - id: hunt
    type: analysis
    prompt: "Look for bugs."
    analysis:
      captain_backend: claude
      worker_backend: claude
      verifier_backend: claude
      analyst:
        backend: claude
        model: claude-opus-4-7[1m]
        max_duration_seconds: 600
        prompt: "Investigate the project."
"""
    )
    wf = load_workflow(yaml_path)
    config = wf.phases[0].analysis
    assert config.analyst is not None
    assert config.analyst.backend == "claude"
    assert config.analyst.model == "claude-opus-4-7[1m]"
    assert config.analyst.max_duration_seconds == 600
    assert "Investigate the project." in config.analyst.prompt


def test_workflow_yaml_without_analyst_block(tmp_path):
    yaml_path = tmp_path / "wf.yaml"
    yaml_path.write_text(
        """
name: t
backend: claude
phases:
  - id: hunt
    type: analysis
    prompt: "Look for bugs."
    analysis:
      captain_backend: claude
"""
    )
    wf = load_workflow(yaml_path)
    assert wf.phases[0].analysis.analyst is None


# --- Validate-time printout ---------------------------------------------------


def test_validate_prints_analyst_when_configured(tmp_path, capsys):
    yaml_path = tmp_path / "wf.yaml"
    yaml_path.write_text(
        """
name: t
backend: claude
phases:
  - id: hunt
    type: analysis
    prompt: "."
    analysis:
      analyst:
        backend: claude
        prompt: "x"
        max_duration_seconds: 900
"""
    )
    from juvenal.cli import build_parser, cmd_validate

    parser = build_parser()
    args = parser.parse_args(["validate", str(yaml_path)])
    args.plain = True
    cmd_validate(args)
    out = capsys.readouterr().out
    assert "analyst: claude" in out
    assert "max_duration: 900s" in out


def test_validate_prints_analyst_disabled_when_absent(tmp_path, capsys):
    yaml_path = tmp_path / "wf.yaml"
    yaml_path.write_text(
        """
name: t
backend: claude
phases:
  - id: hunt
    type: analysis
    prompt: "."
"""
    )
    from juvenal.cli import build_parser, cmd_validate

    parser = build_parser()
    args = parser.parse_args(["validate", str(yaml_path)])
    args.plain = True
    cmd_validate(args)
    out = capsys.readouterr().out
    assert "analyst: disabled" in out


# --- Runner integration -------------------------------------------------------


@pytest.fixture
def patched_backend(monkeypatch):
    """Yield a MockBackend with `create_backend` monkeypatched for the test's lifetime."""

    backend = MockBackend()
    monkeypatch.setattr("juvenal.dynamic.runner.create_backend", lambda name: backend)
    return backend


def _make_runner(tmp_path, backend: MockBackend, *, analyst_spec: AnalystSpec | None) -> DynamicAnalysisRunner:
    phase = Phase(
        id="analyze",
        type="analysis",
        prompt="Repo mission text.",
        analysis=AnalysisConfig(
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
            analyst=analyst_spec,
        ),
    )
    workflow = Workflow(name="t", phases=[phase], working_dir=str(tmp_path))
    state_file = tmp_path / "analysis-state.json"

    return DynamicAnalysisRunner(
        phase=phase,
        workflow=workflow,
        state_file=state_file,
        run_mode="fresh",
        display=Display(plain=True),
        interactive=False,
        interaction_channel=None,
    )


def test_runner_skips_analyst_when_no_spec(tmp_path, patched_backend):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    runner.state.save()
    runner._maybe_start_analyst()
    assert runner._analyst_future is None
    assert runner.state.attack_surface.status == "pending"


def test_runner_skips_analyst_when_disabled(tmp_path, patched_backend):
    spec = AnalystSpec(prompt="brief", enabled=False)
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.save()
    runner._maybe_start_analyst()
    assert runner._analyst_future is None


def test_analyst_success_writes_brief_and_subagent_and_marks_ready(tmp_path, patched_backend):
    patched_backend.add_role_response(role="analyst", output="# Project Brief\n\nThe project is X.")
    spec = AnalystSpec(prompt="Investigate {working_dir} given mission {mission}.")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.save()
    runner._maybe_start_analyst()
    assert runner._analyst_future is not None
    runner._analyst_future.result(timeout=5.0)
    drained = runner._drain_analyst_future()
    assert drained, "analyst future never completed"

    state = runner.state.attack_surface
    assert state.status == "ready"
    assert state.brief is not None and "Project Brief" in state.brief
    assert state.brief_path is not None and Path(state.brief_path).read_text().startswith("# Project Brief")
    assert state.subagent_path is not None
    subagent_path = Path(state.subagent_path)
    assert subagent_path.is_file()
    subagent_content = subagent_path.read_text()
    assert "name: attack-surface" in subagent_content
    assert "WebFetch" in subagent_content
    assert "PROJECT_BRIEF_BEGIN" in subagent_content
    assert "pure-DoS is a bug, not a bug-bounty vulnerability" in subagent_content
    assert ("analyst", "claude-opus-4-7[1m]") in patched_backend.model_calls


def test_analyst_failure_marks_failed_and_does_not_block(tmp_path, patched_backend):
    patched_backend.add_role_response(role="analyst", exit_code=1, output="boom")
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.save()
    runner._maybe_start_analyst()
    runner._analyst_future.result(timeout=5.0)
    drained = runner._drain_analyst_future()
    assert drained
    state = runner.state.attack_surface
    assert state.status == "failed"
    assert state.error is not None
    assert "boom" in state.error or "exited" in state.error
    assert state.subagent_path is None
    assert not (tmp_path / ".claude" / "agents" / "attack-surface.md").exists()


def test_brief_block_pending_on_unstarted_runner(tmp_path, patched_backend):
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    block = runner._project_brief_block()
    assert "PROJECT_BRIEF: not ready" in block
    assert "attack-surface" in block.lower()


def test_brief_block_failed_includes_error_and_subagent_advice(tmp_path, patched_backend):
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.attack_surface = AttackSurfaceState(status="failed", error="rate limit")
    block = runner._project_brief_block()
    assert "BRIEF" in block.upper()
    assert "rate limit" in block
    assert "attack-surface" in block.lower()


def test_brief_block_ready_inserts_brief_text(tmp_path, patched_backend):
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.attack_surface = AttackSurfaceState(status="ready", brief="# Brief\nbody")
    block = runner._project_brief_block()
    assert "# Brief\nbody" in block
    assert "attack-surface" in block.lower()


def test_brief_block_reflects_on_disk_amendments(tmp_path, patched_backend):
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    brief_file = tmp_path / "output" / ".attack-surface-brief.md"
    brief_file.parent.mkdir(parents=True, exist_ok=True)
    brief_file.write_text("# Brief\nTreat Link D as mandatory.", encoding="utf-8")
    runner.state.attack_surface = AttackSurfaceState(
        status="ready",
        brief="# Brief\nTreat Link D as mandatory.",
        brief_path=str(brief_file),
    )
    assert "Treat Link D as mandatory." in runner._project_brief_block()

    brief_file.write_text("# Brief\nThere is no Link D.", encoding="utf-8")
    block = runner._project_brief_block()
    assert "There is no Link D." in block
    assert "Treat Link D as mandatory." not in block


def test_brief_block_falls_back_to_snapshot_when_file_unreadable(tmp_path, patched_backend):
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.attack_surface = AttackSurfaceState(
        status="ready",
        brief="# Brief\nsnapshot body",
        brief_path=str(tmp_path / "output" / "missing.md"),
    )
    assert "snapshot body" in runner._project_brief_block()


def test_brief_block_empty_when_analyst_not_configured(tmp_path, patched_backend):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    assert runner._project_brief_block() == ""


def test_brief_injected_into_worker_system_prompt_when_ready(tmp_path, patched_backend):
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.attack_surface = AttackSurfaceState(status="ready", brief="# Project Brief\nfoo")
    sysprompt = runner._worker_system_prompt()
    assert "# Project Brief" in sysprompt
    assert "attack-surface" in sysprompt.lower()


# --- Resume behavior ----------------------------------------------------------


def test_resume_running_analyst_marked_failed_not_retried():
    """Analyst running at shutdown becomes 'failed' on resume — does NOT auto-retry.

    The user's contract is that the analyst runs once per phase lifetime, not once
    per resume. We sticky-fail interrupted analysts so the next resume proceeds
    without paying for another expensive analyst run.
    """
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        path = Path(tmp.name)
    state = DynamicSessionState(state_file=path)
    state.attack_surface = AttackSurfaceState(status="running", started_at=1.0)
    state.normalize_for_resume()
    assert state.attack_surface.status == "failed"
    assert state.attack_surface.completed_at is not None
    assert "interrupted-before-completion" in state.attack_surface.error
    assert "force a retry" in state.attack_surface.error


def test_resume_ready_analyst_is_sticky():
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        path = Path(tmp.name)
    state = DynamicSessionState(state_file=path)
    state.attack_surface = AttackSurfaceState(status="ready", brief="kept")
    state.normalize_for_resume()
    assert state.attack_surface.status == "ready"
    assert state.attack_surface.brief == "kept"


def test_resume_failed_analyst_is_sticky():
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        path = Path(tmp.name)
    state = DynamicSessionState(state_file=path)
    state.attack_surface = AttackSurfaceState(status="failed", error="prior crash")
    state.normalize_for_resume()
    assert state.attack_surface.status == "failed"
    assert state.attack_surface.error == "prior crash"


def test_maybe_start_analyst_skips_when_already_ready(tmp_path, patched_backend):
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.attack_surface = AttackSurfaceState(status="ready", brief="kept")
    runner._maybe_start_analyst()
    assert runner._analyst_future is None
    assert runner.state.attack_surface.status == "ready"


def test_maybe_start_analyst_skips_when_failed(tmp_path, patched_backend):
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.attack_surface = AttackSurfaceState(status="failed", error="prior")
    runner._maybe_start_analyst()
    assert runner._analyst_future is None
    assert runner.state.attack_surface.status == "failed"


# --- Full-loop run with analyst -----------------------------------------------


def test_full_run_with_analyst_completes(tmp_path, patched_backend):
    patched_backend.add_role_response(role="analyst", output="# Brief\nstuff")
    patched_backend.add_role_response(
        role="captain",
        output=_captain_output(termination_state="complete", termination_reason="done"),
        session_id="captain-session",
    )
    spec = AnalystSpec(prompt="brief please")
    phase = Phase(
        id="analyze",
        type="analysis",
        prompt="Mission text.",
        analysis=AnalysisConfig(
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
            analyst=spec,
        ),
    )
    workflow = Workflow(name="t", phases=[phase], working_dir=str(tmp_path))
    state_file = tmp_path / "analysis-state.json"
    runner = DynamicAnalysisRunner(
        phase=phase,
        workflow=workflow,
        state_file=state_file,
        run_mode="fresh",
        display=Display(plain=True),
        interactive=False,
        interaction_channel=None,
    )
    result = runner.run()
    assert result.success
    final = DynamicSessionState.load(state_file)
    assert final.attack_surface.status == "ready"
    assert final.attack_surface.brief == "# Brief\nstuff"
    analyst_count = sum(1 for r, _ in patched_backend.role_calls if r == "analyst")
    assert analyst_count == 1, f"expected 1 analyst call, got {analyst_count}"


# --- Wait-for-analyst gate ----------------------------------------------------


def test_wait_for_analyst_returns_immediately_when_no_future(tmp_path, patched_backend):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    assert runner._wait_for_analyst() is True


def test_wait_for_analyst_drains_completed_future(tmp_path, patched_backend):
    """The wait drains the analyst future before returning, so workers see ready/failed state."""
    patched_backend.add_role_response(role="analyst", output="# Brief\nready")
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.save()
    runner._maybe_start_analyst()
    assert runner._wait_for_analyst() is True
    assert runner.state.attack_surface.status == "ready"
    assert runner._analyst_future is None


def test_wait_for_analyst_returns_false_on_shutdown(tmp_path, patched_backend):
    """If shutdown_event is set during wait, return False to abort scheduling."""
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.save()
    runner._maybe_start_analyst()
    # Simulate shutdown right after analyst kicks off but before it completes.
    runner._shutdown_event.set()

    # Replace the future with one that never completes (we don't want the actual
    # mock backend to resolve it instantly and bypass the shutdown branch).
    class _NeverDone:
        def done(self):
            return False

    runner._analyst_future = _NeverDone()
    assert runner._wait_for_analyst() is False


def test_full_run_blocks_until_analyst_finishes_before_captain(tmp_path, patched_backend):
    """Workers/captain must not run until the analyst future drains.

    We register the analyst response to fire BEFORE the captain response. If the
    runner respected the wait, the order of role_calls in the backend will be
    ['analyst', 'captain', ...]. If not, the captain may race ahead.
    """
    patched_backend.add_role_response(role="analyst", output="# Brief\nready")
    patched_backend.add_role_response(
        role="captain",
        output=_captain_output(termination_state="complete", termination_reason="done"),
        session_id="captain-session",
    )
    spec = AnalystSpec(prompt="x")
    phase = Phase(
        id="analyze",
        type="analysis",
        prompt="Mission text.",
        analysis=AnalysisConfig(
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
            analyst=spec,
        ),
    )
    workflow = Workflow(name="t", phases=[phase], working_dir=str(tmp_path))
    state_file = tmp_path / "analysis-state.json"
    runner = DynamicAnalysisRunner(
        phase=phase,
        workflow=workflow,
        state_file=state_file,
        run_mode="fresh",
        display=Display(plain=True),
        interactive=False,
        interaction_channel=None,
    )
    runner.run()
    role_order = [r for r, _ in patched_backend.role_calls if r in ("analyst", "captain")]
    assert role_order[0] == "analyst", f"analyst must run before captain; got {role_order}"


# --- Stale session detection --------------------------------------------------


def _make_state_with_completed_attempt(
    state,
    *,
    session_id,
    completed_at,
    parent_session_id=None,
    status: str = "completed",
):
    """Helper: attach a worker attempt to the state.

    By default this records a SESSION-CREATING attempt (no parent_session_id)
    that completed successfully, so the staleness check counts its
    ``completed_at`` as a successful-use timestamp.

    Pass ``status="failed"`` to record an attempt that crashed / had a
    parse error — those do NOT count as successful uses, so they don't
    refresh the staleness signal.
    """
    state.worker_attempts[f"a-{session_id}-{len(state.worker_attempts)}"] = WorkerAttempt(
        attempt_id=f"a-{session_id}-{len(state.worker_attempts)}",
        target_id="t1",
        generation=1,
        backend="claude",
        session_id=session_id,
        status=status,
        started_at=completed_at - 10,
        completed_at=completed_at,
        parent_session_id=parent_session_id,
    )


def test_session_is_stale_returns_false_when_unknown(tmp_path, patched_backend):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    assert runner._session_is_stale("never-seen") is False


def test_session_is_stale_returns_false_when_recent(tmp_path, patched_backend):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    _make_state_with_completed_attempt(runner.state, session_id="fresh", completed_at=time.time() - 60)
    assert runner._session_is_stale("fresh") is False


def test_session_is_stale_returns_true_when_old(tmp_path, patched_backend):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    _make_state_with_completed_attempt(
        runner.state,
        session_id="old",
        completed_at=time.time() - _SESSION_STALENESS_THRESHOLD_SECONDS - 60,
    )
    assert runner._session_is_stale("old") is True


def test_session_is_stale_uses_verification_started_at(tmp_path, patched_backend):
    """Verifier session staleness uses the session-creating attempt's started_at."""
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    runner.state.verifications["v1"] = VerificationRecord(
        verification_id="v1",
        claim_id="c1",
        target_id="t1",
        generation=1,
        backend="claude",
        verifier_role="analysis-verifier",
        session_id="ver-session",
        status="passed",
        disposition="verified",
        reason="ok",
        rejection_class=None,
        raw_output="",
        started_at=time.time() - _SESSION_STALENESS_THRESHOLD_SECONDS - 60,
        completed_at=time.time() - _SESSION_STALENESS_THRESHOLD_SECONDS - 30,
        parent_session_id=None,  # session-creating verification
    )
    assert runner._session_is_stale("ver-session") is True


def test_session_is_stale_ignores_recent_failed_retries_of_old_session(tmp_path, patched_backend):
    """The openthread bug: a stale session being --resumed in a tight loop, all
    retries returning malformed output (status="failed"). Failed retries must
    NOT refresh the staleness signal — only SUCCESSFUL uses count as evidence
    that the session is alive."""
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    now = time.time()
    last_success_time = now - _SESSION_STALENESS_THRESHOLD_SECONDS - 3600  # > threshold ago

    # Last successful use — only this counts toward the staleness signal.
    _make_state_with_completed_attempt(
        runner.state,
        session_id="zombie",
        completed_at=last_success_time,
        parent_session_id=None,
        status="completed",
    )
    # Many recent failed retries (zombie session won't take new --resume).
    # These must NOT refresh the signal.
    for offset in (60, 30, 10):
        _make_state_with_completed_attempt(
            runner.state,
            session_id="zombie",
            completed_at=now - offset,
            parent_session_id="zombie",
            status="failed",
        )

    assert runner._session_is_stale("zombie") is True, (
        "session whose last SUCCESSFUL use is older than threshold should be stale "
        "even when recent FAILED retries refresh completed_at"
    )


def test_session_is_stale_returns_false_when_recent_success_with_old_creation(tmp_path, patched_backend):
    """The user's openthread regression: a long-running phase keeps using the
    same captain/worker sessions across multi-day runs. The session was CREATED
    days ago, but it has been actively (and successfully) used in the last
    few hours. The runner must NOT cold-restart on the next resume."""
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    now = time.time()
    creation_time = now - _SESSION_STALENESS_THRESHOLD_SECONDS - 86400  # 1d past threshold

    # Original session-creating attempt — long ago.
    _make_state_with_completed_attempt(
        runner.state,
        session_id="long-lived",
        completed_at=creation_time,
        parent_session_id=None,
        status="completed",
    )
    # Recent successful resume — last use was a few hours ago.
    _make_state_with_completed_attempt(
        runner.state,
        session_id="long-lived",
        completed_at=now - 4 * 3600,  # 4h ago
        parent_session_id="long-lived",
        status="completed",
    )

    assert runner._session_is_stale("long-lived") is False, (
        "session with a recent SUCCESSFUL use should not be flagged stale regardless of how long ago it was created"
    )


def test_worker_with_stale_parent_session_falls_back_to_run_agent(tmp_path, patched_backend, monkeypatch):
    """When parent session is stale, _execute_worker_attempt MUST use run_agent (not resume_agent),
    pass the system_prompt, AND pass session_id=None so the backend allocates a fresh UUID
    (passing the stale UUID via --session-id makes claude error 'Error: Session ID … already in use')."""
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    # Seed a stale parent session via its session-creating attempt.
    _make_state_with_completed_attempt(
        runner.state,
        session_id="stale-parent",
        completed_at=time.time() - _SESSION_STALENESS_THRESHOLD_SECONDS - 60,
        parent_session_id=None,
    )
    attempt = WorkerAttempt(
        attempt_id="retry-1",
        target_id="t1",
        generation=1,
        backend="claude",
        session_id="stale-parent",  # matches what scheduling would assign
        status="queued",
        started_at=None,
        completed_at=None,
        parent_session_id="stale-parent",
    )
    patched_backend.add_role_response(role="worker", output="ignored")

    # Capture the session_id passed to run_agent.
    captured_session_ids = []
    real_run_agent = patched_backend.run_agent

    def spy_run_agent(*args, **kwargs):
        captured_session_ids.append(kwargs.get("session_id"))
        return real_run_agent(*args, **kwargs)

    monkeypatch.setattr(patched_backend, "run_agent", spy_run_agent)

    runner._execute_worker_attempt(attempt, prompt="task body", system_prompt="WORKER_SYS_PROMPT")

    # No resume call should have been made.
    assert len(patched_backend.resume_calls) == 0, "expected cold-restart, not --resume"
    # session_id MUST be None so backend allocates a fresh UUID.
    assert captured_session_ids == [None], f"cold-restart must pass session_id=None, got {captured_session_ids}"
    # And the run_agent call should have carried the system_prompt.
    sysprompts = [sp for r, sp in patched_backend.system_prompt_calls if r == "worker"]
    assert sysprompts and sysprompts[-1] == "WORKER_SYS_PROMPT"


def test_stale_session_warning_is_debounced_per_session(tmp_path, patched_backend):
    """N retries against the same stale parent session emit ONE warning, not N."""
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    _make_state_with_completed_attempt(
        runner.state,
        session_id="zombie",
        completed_at=time.time() - _SESSION_STALENESS_THRESHOLD_SECONDS - 60,
        parent_session_id=None,
    )

    emitted: list[str] = []

    def capture(msg: str) -> None:
        emitted.append(msg)

    runner._emit_analyst_message = capture  # type: ignore[assignment]

    for i in range(5):
        attempt = WorkerAttempt(
            attempt_id=f"retry-{i}",
            target_id="t1",
            generation=1,
            backend="claude",
            session_id="zombie",
            status="queued",
            started_at=None,
            completed_at=None,
            parent_session_id="zombie",
        )
        patched_backend.add_role_response(role="worker", output="ignored")
        runner._execute_worker_attempt(attempt, prompt="task", system_prompt="SYS")

    stale_warnings = [m for m in emitted if "stale" in m]
    assert len(stale_warnings) == 1, f"expected one debounced warning, got {len(stale_warnings)}: {stale_warnings}"


def test_worker_with_fresh_parent_session_uses_resume(tmp_path, patched_backend):
    """When parent session is recent, the worker MUST resume (preserving prior context)."""
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    _make_state_with_completed_attempt(
        runner.state,
        session_id="fresh-parent",
        completed_at=time.time() - 60,
    )
    attempt = WorkerAttempt(
        attempt_id="retry-2",
        target_id="t1",
        generation=1,
        backend="claude",
        session_id="new-session",
        status="queued",
        started_at=None,
        completed_at=None,
        parent_session_id="fresh-parent",
    )
    patched_backend.add_role_response(role="worker", output="ignored")

    runner._execute_worker_attempt(attempt, prompt="task body", system_prompt="WORKER_SYS_PROMPT")

    assert len(patched_backend.resume_calls) == 1
    assert patched_backend.resume_calls[0][0] == "fresh-parent"


def test_verifier_with_stale_parent_session_falls_back_to_run_agent(tmp_path, patched_backend, monkeypatch):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    runner.state.verifications["v-old"] = VerificationRecord(
        verification_id="v-old",
        claim_id="c1",
        target_id="t1",
        generation=1,
        backend="claude",
        verifier_role="analysis-verifier",
        session_id="stale-verifier",
        status="passed",
        disposition="verified",
        reason="ok",
        rejection_class=None,
        raw_output="",
        started_at=time.time() - _SESSION_STALENESS_THRESHOLD_SECONDS - 120,
        completed_at=time.time() - _SESSION_STALENESS_THRESHOLD_SECONDS - 60,
        parent_session_id=None,  # session creator
    )
    new_verification = VerificationRecord(
        verification_id="v-new",
        claim_id="c1",
        target_id="t1",
        generation=1,
        backend="claude",
        verifier_role="analysis-verifier",
        session_id="stale-verifier",  # was set to parent for resume
        status="pending",
        disposition=None,
        reason="",
        rejection_class=None,
        raw_output="",
        started_at=None,
        completed_at=None,
        parent_session_id="stale-verifier",
        verifier_name="default",
        verifier_index=0,
    )
    patched_backend.add_role_response(role="verifier", output="ignored")

    captured_session_ids = []
    real_run_agent = patched_backend.run_agent

    def spy_run_agent(*args, **kwargs):
        captured_session_ids.append(kwargs.get("session_id"))
        return real_run_agent(*args, **kwargs)

    monkeypatch.setattr(patched_backend, "run_agent", spy_run_agent)

    runner._execute_verifier(new_verification, prompt="claim packet", system_prompt="VERIFIER_SYS_PROMPT")

    assert len(patched_backend.resume_calls) == 0
    assert captured_session_ids == [None], (
        f"verifier cold-restart must pass session_id=None, got {captured_session_ids}"
    )
    sysprompts = [sp for r, sp in patched_backend.system_prompt_calls if r == "verifier"]
    assert sysprompts and sysprompts[-1] == "VERIFIER_SYS_PROMPT"


# --- Drain-on-shutdown --------------------------------------------------------


def test_finalize_analyst_on_shutdown_records_failure_when_subprocess_killed(tmp_path, patched_backend):
    """If the analyst's claude subprocess is killed (kill_active during shutdown), the future
    should still resolve with an error, and _finalize_analyst_on_shutdown must drain it so
    state goes ready/failed (not stuck at running)."""
    # Backend returns an error result simulating a killed claude subprocess.
    patched_backend.add_role_response(role="analyst", exit_code=1, output="killed mid-flight")
    spec = AnalystSpec(prompt="x")
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=spec)
    runner.state.save()
    runner._maybe_start_analyst()
    # Don't drain via the normal main loop — directly call shutdown finalizer.
    runner._finalize_analyst_on_shutdown()
    assert runner.state.attack_surface.status == "failed"
    assert runner.state.attack_surface.error is not None
    assert "killed mid-flight" in runner.state.attack_surface.error or "exit" in runner.state.attack_surface.error


def test_finalize_analyst_on_shutdown_is_safe_when_no_future(tmp_path, patched_backend):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    runner._finalize_analyst_on_shutdown()  # should not raise
    assert runner.state.attack_surface.status == "pending"


# --- Rate-limit error classification ------------------------------------------


def test_error_looks_like_rate_limit_recognizes_signatures(tmp_path, patched_backend):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    assert runner._error_looks_like_rate_limit("You've hit your limit · resets 12:30am") is True
    assert runner._error_looks_like_rate_limit("monthly usage limit exceeded") is True
    assert runner._error_looks_like_rate_limit("you're out of extra usage") is True
    assert runner._error_looks_like_rate_limit("HTTP 429") is True


def test_error_looks_like_rate_limit_rejects_parse_failures(tmp_path, patched_backend):
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    assert runner._error_looks_like_rate_limit("WORKER_JSON block not found") is False
    assert runner._error_looks_like_rate_limit("verification output must include a VERDICT line") is False
    assert runner._error_looks_like_rate_limit("worker report identity mismatch") is False
    assert runner._error_looks_like_rate_limit("future crashed: AttributeError") is False
    assert runner._error_looks_like_rate_limit(None) is False


def test_error_looks_like_rate_limit_uses_agent_result_429(tmp_path, patched_backend):
    from juvenal.backends import AgentResult

    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    rl_result = AgentResult(
        exit_code=1,
        output="",
        transcript="",
        duration=0.0,
        input_tokens=0,
        output_tokens=0,
        rate_limit_status=429,
    )
    assert runner._error_looks_like_rate_limit(rl_result) is True
    fresh_result = AgentResult(
        exit_code=1, output="some parse failure", transcript="", duration=0.0, input_tokens=0, output_tokens=0
    )
    assert runner._error_looks_like_rate_limit(fresh_result) is False


def test_record_infrastructure_error_skips_backoff_for_parse_failures(tmp_path, patched_backend, monkeypatch):
    """Many consecutive WORKER_JSON parse failures must NOT trigger _rate_limit_backoff.

    This is the bug from the openthread run: 5 stale-session parse failures in
    a row would put the runner to sleep for minutes, even though backoff
    cannot recover from a stale session.
    """
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    runner.config.max_consecutive_errors = 3  # tighten so the test is fast
    backoff_calls = []
    monkeypatch.setattr(runner, "_rate_limit_backoff", lambda: backoff_calls.append(True))

    for _ in range(10):
        runner._record_infrastructure_error("worker returned malformed structured output: WORKER_JSON block not found")

    assert backoff_calls == [], "parse-failure errors must not trigger rate-limit backoff"
    # Counter should still increment (so other code paths can observe).
    assert runner._consecutive_errors == 10


def test_record_infrastructure_error_triggers_backoff_for_actual_rate_limit(tmp_path, patched_backend, monkeypatch):
    """An actual rate-limit string SHOULD eventually trigger backoff."""
    runner = _make_runner(tmp_path, patched_backend, analyst_spec=None)
    runner.config.max_consecutive_errors = 3
    backoff_calls = []
    monkeypatch.setattr(runner, "_rate_limit_backoff", lambda: backoff_calls.append(True))

    for _ in range(3):
        runner._record_infrastructure_error("worker exited with code 1: You've hit your limit · resets 1am")

    assert len(backoff_calls) >= 1, "rate-limit-like errors should trigger backoff after threshold"
