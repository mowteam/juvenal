"""A failed phase must record why it failed, not just that it did."""

from __future__ import annotations

from unittest.mock import patch

from juvenal.engine import Engine
from juvenal.execution import PhaseResult
from juvenal.state import PipelineState
from juvenal.workflow import AnalysisConfig, Phase, Workflow
from tests.conftest import MockBackend


def _analysis_workflow(tmp_path) -> Workflow:
    return Workflow(
        name="analysis-run",
        working_dir=str(tmp_path),
        phases=[Phase(id="hunt", type="analysis", prompt="Hunt.", analysis=AnalysisConfig())],
        max_bounces=3,
    )


def _engine(tmp_path, workflow) -> Engine:
    engine = Engine(workflow, state_file=str(tmp_path / "state.json"))
    engine.backend = MockBackend()
    return engine


def test_failed_analysis_phase_records_its_reason(tmp_path):
    """The regression: the phase went to `failed` with failure_contexts empty."""
    engine = _engine(tmp_path, _analysis_workflow(tmp_path))
    reason = "backend quota exhausted, so no agent work can proceed; resume once it resets"

    with patch.object(engine, "_run_analysis", return_value=PhaseResult(success=False, failure_context=reason)):
        assert engine.run() == 1

    phase = engine.state.phases["hunt"]
    assert phase.status == "failed"
    assert engine.state.get_failure_context("hunt") == reason
    assert phase.failure_contexts[-1]["context"] == reason


def test_recorded_reason_survives_reload_and_feeds_the_next_attempt(tmp_path):
    """`_run_analysis` reads the stored context back on the next attempt, so it
    has to survive a round trip through the state file."""
    workflow = _analysis_workflow(tmp_path)
    engine = _engine(tmp_path, workflow)
    reason = "captain output remained malformed after repair"

    with patch.object(engine, "_run_analysis", return_value=PhaseResult(success=False, failure_context=reason)):
        engine.run()

    reloaded = PipelineState.load(str(tmp_path / "state.json"))
    assert reloaded.get_failure_context("hunt") == reason


def test_ctrl_c_reason_is_recorded(tmp_path):
    """The interrupt path carries its own context and must not be swallowed."""
    engine = _engine(tmp_path, _analysis_workflow(tmp_path))

    with patch.object(
        engine,
        "_run_analysis",
        return_value=PhaseResult(success=False, failure_context="interrupted by user (Ctrl-C)"),
    ):
        engine.run()

    assert "Ctrl-C" in engine.state.get_failure_context("hunt")


def test_failure_without_a_reason_records_nothing(tmp_path):
    """A phase that fails with no stated reason must not fabricate one."""
    engine = _engine(tmp_path, _analysis_workflow(tmp_path))

    with patch.object(engine, "_run_analysis", return_value=PhaseResult(success=False, failure_context="")):
        assert engine.run() == 1

    assert engine.state.phases["hunt"].status == "failed"
    assert engine.state.phases["hunt"].failure_contexts == []


def test_earlier_failure_contexts_are_preserved(tmp_path):
    """failure_contexts is append-only history, not a single slot."""
    workflow = _analysis_workflow(tmp_path)
    engine = _engine(tmp_path, workflow)
    engine.state.set_failure_context("hunt", "first failure", attempt=1)

    with patch.object(
        engine, "_run_analysis", return_value=PhaseResult(success=False, failure_context="second failure")
    ):
        engine.run()

    contexts = [entry["context"] for entry in engine.state.phases["hunt"].failure_contexts]
    assert contexts == ["first failure", "second failure"]
