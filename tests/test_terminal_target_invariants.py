"""Terminal targets stay terminal, and outcome rationale reaches the captain."""

from __future__ import annotations

import time
from unittest.mock import patch

from juvenal.display import Display
from juvenal.dynamic.models import ClaimRecord, CodeLocation, TargetRecord, VerificationRecord
from juvenal.dynamic.runner import DynamicAnalysisRunner
from juvenal.dynamic.state import DynamicSessionState
from juvenal.workflow import AnalysisConfig, Phase, VerifierSpec, Workflow
from tests.conftest import MockBackend


def _make_runner(tmp_path, *, config: AnalysisConfig | None = None) -> DynamicAnalysisRunner:
    config = config or AnalysisConfig(
        max_workers=1,
        max_verifiers=1,
        verifiers=[
            VerifierSpec(name="scope", prompt="scope check"),
            VerifierSpec(name="impact", prompt="impact check"),
        ],
    )
    phase = Phase(id="analyze", type="analysis", prompt="Mission.", analysis=config)
    workflow = Workflow(name="analysis", phases=[phase], working_dir=str(tmp_path))
    backend = MockBackend()
    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        return DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=tmp_path / "analysis-state.json",
            run_mode="fresh",
            display=Display(plain=True),
            interactive=False,
        )


def _target(status: str = "blocked") -> TargetRecord:
    now = time.time()
    return TargetRecord(
        target_id="target-1",
        title="Analyze target-1",
        kind="security",
        priority=50,
        status=status,
        source="captain",
        scope_paths=["src/app.py"],
        scope_symbols=["app"],
        instructions="Inspect the target.",
        depends_on_claim_ids=[],
        spawn_reason="captain requested it",
        generation=1,
        active_generation=1,
        active_attempt_id=None,
        deferred_until_turn=None,
        pending_verification_ids=[],
        accepted_claim_ids=[],
        rejected_claim_ids=[],
        created_at=now,
        updated_at=now,
    )


def _claim(status: str = "verifying") -> ClaimRecord:
    return ClaimRecord(
        claim_id="target-1-g1-claim-c1",
        worker_claim_id="c1",
        target_id="target-1",
        attempt_id="target-1-g1-attempt-1",
        generation=1,
        kind="input-validation",
        subcategory="missing-check",
        summary="Missing validation.",
        assertion="The path lacks validation.",
        severity="medium",
        worker_confidence="medium",
        primary_location=CodeLocation(path="src/app.py", line=10, symbol="app", role="sink"),
        locations=[CodeLocation(path="src/app.py", line=10, symbol="app", role="sink")],
        preconditions=[],
        candidate_code_refs=[],
        related_claim_ids=[],
        audit_artifact_id="art-1",
        status=status,
        verification_ids=["v1"],
        rejection_class=None,
        verified_at=None,
        rejected_at=None,
    )


def _passed_verification() -> VerificationRecord:
    now = time.time()
    return VerificationRecord(
        verification_id="v1",
        claim_id="target-1-g1-claim-c1",
        target_id="target-1",
        generation=1,
        backend="claude",
        verifier_role="analysis-verifier",
        session_id="s1",
        status="passed",
        disposition="verified",
        reason="ok",
        rejection_class=None,
        raw_output="VERDICT: PASS",
        started_at=now,
        completed_at=now,
        verifier_name="scope",
        verifier_index=0,
    )


def test_terminal_target_does_not_pull_a_verifier(tmp_path):
    """A claim left mid-chain on a blocked target must not resurrect it."""
    runner = _make_runner(tmp_path)
    target = _target(status="blocked")
    runner.state.targets[target.target_id] = target
    claim = _claim(status="verifying")
    runner.state.claims[claim.claim_id] = claim
    verification = _passed_verification()
    runner.state.verifications[verification.verification_id] = verification

    runner._schedule_verifiers()

    assert target.status == "blocked"
    assert target.pending_verification_ids == []
    assert len(runner.state.verifications) == 1, "no new verification may be scheduled"


def test_live_target_still_continues_its_chain(tmp_path):
    """The guard must not stall a normal mid-chain claim."""
    runner = _make_runner(tmp_path)
    target = _target(status="verifying")
    runner.state.targets[target.target_id] = target
    claim = _claim(status="verifying")
    runner.state.claims[claim.claim_id] = claim
    verification = _passed_verification()
    runner.state.verifications[verification.verification_id] = verification

    runner._schedule_verifiers()

    assert len(runner.state.verifications) == 2
    # The same call dispatches what it schedules, so the new record is already
    # running rather than pending.
    scheduled = [v for v in runner.state.verifications.values() if v.verification_id != "v1"]
    assert len(scheduled) == 1
    assert scheduled[0].verifier_index == 1, "chain resumes at the next unfinished step"
    assert scheduled[0].status in {"pending", "running"}


def test_terminal_target_does_not_pull_a_claim_retry(tmp_path):
    runner = _make_runner(tmp_path)
    target = _target(status="blocked")
    runner.state.targets[target.target_id] = target
    claim = _claim(status="rejected")
    runner.state.claims[claim.claim_id] = claim
    runner._pending_claim_retries.append((target.target_id, claim.claim_id))

    runner._schedule_workers()

    assert target.status == "blocked"
    assert target.active_attempt_id is None
    assert runner.state.worker_attempts == {}
    assert runner._pending_claim_retries == [], "the retry is dropped, not left pending forever"


def test_resume_does_not_revive_a_claim_on_a_terminal_target(tmp_path):
    """normalize_for_resume must leave a terminal target's claim alone.

    The failed verifications carry no `rejected` disposition (they crashed
    rather than returning a verdict), so the mid-chain PASS used to promote the
    claim back to `verifying` even though its target was blocked.
    """
    state_file = tmp_path / "analysis-state.json"
    state = DynamicSessionState(state_file=state_file)
    target = _target(status="blocked")
    state.targets[target.target_id] = target
    claim = _claim(status="rejected")
    claim.rejection_class = "out-of-scope"
    state.claims[claim.claim_id] = claim
    passed = _passed_verification()
    state.verifications[passed.verification_id] = passed
    crashed = _passed_verification()
    crashed.verification_id = "v2"
    crashed.status = "failed"
    crashed.disposition = None
    crashed.verifier_name = "impact"
    crashed.verifier_index = 1
    crashed.error = "verifier exited with code 1: backend refused"
    state.verifications[crashed.verification_id] = crashed
    claim.verification_ids = ["v1", "v2"]

    state.normalize_for_resume(verifier_chain_length=2)

    assert target.status == "blocked"
    assert claim.status == "rejected", "a terminal target's claim must not be promoted"
    assert claim.rejection_class == "out-of-scope"


def test_no_findings_summary_reaches_the_captain_delta(tmp_path):
    """The worker's rationale for finding nothing must survive to the captain."""
    state_file = tmp_path / "analysis-state.json"
    state = DynamicSessionState(state_file=state_file)
    state.append_event(
        "target.no_findings",
        target_id="target-1",
        generation=1,
        summary="Measured the shipped image: the requirement diff is empty, so this line is closed.",
    )
    state.append_event("target.blocked", target_id="target-2", generation=1, blocker="no source for the binary")

    delta = state.pending_captain_delta()

    assert delta.no_findings_target_ids == ["target-1"]
    assert "requirement diff is empty" in delta.target_outcome_notes["target-1"]
    assert delta.target_outcome_notes["target-2"] == "no source for the binary"


def test_outcome_notes_are_bounded(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "analysis-state.json")
    state.append_event("target.no_findings", target_id="target-1", generation=1, summary="x" * 9000)
    delta = state.pending_captain_delta()
    assert len(delta.target_outcome_notes["target-1"]) == 2000


def test_no_findings_without_a_summary_adds_no_note(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "analysis-state.json")
    state.append_event("target.no_findings", target_id="target-1", generation=1, summary="   ")
    delta = state.pending_captain_delta()
    assert delta.target_outcome_notes == {}
