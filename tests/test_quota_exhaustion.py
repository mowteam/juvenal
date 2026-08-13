"""Tests for backend quota exhaustion (spent credits, not a timed throttle)."""

from __future__ import annotations

import time
from unittest.mock import patch

from juvenal.backends import AgentResult
from juvenal.display import Display
from juvenal.dynamic.models import (
    ClaimRecord,
    CodeLocation,
    TargetRecord,
    VerificationRecord,
    WorkerAttempt,
)
from juvenal.dynamic.runner import DynamicAnalysisRunner, _WorkerExecutionResult
from juvenal.workflow import AnalysisConfig, Phase, Workflow
from tests.conftest import MockBackend

# Verbatim from a Codex worker that ran out of credits mid-analysis.
CODEX_USAGE_LIMIT = (
    "worker exited with code 1: You've hit your usage limit. Visit "
    "https://chatgpt.com/codex/settings/usage to purchase more credits or try "
    "again at Aug 19th, 2026 8:33 PM."
)


def _make_runner(tmp_path, *, config: AnalysisConfig | None = None) -> DynamicAnalysisRunner:
    config = config or AnalysisConfig(max_workers=1, max_verifiers=1)
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


def _seed_target(runner: DynamicAnalysisRunner, *, error_retry_count: int) -> TargetRecord:
    now = time.time()
    target = TargetRecord(
        target_id="target-1",
        title="Analyze target-1",
        kind="security",
        priority=50,
        status="running",
        source="captain",
        scope_paths=["src/app.py"],
        scope_symbols=["app"],
        instructions="Inspect the target.",
        depends_on_claim_ids=[],
        spawn_reason="captain requested it",
        generation=1,
        active_generation=1,
        active_attempt_id="attempt-1",
        deferred_until_turn=None,
        pending_verification_ids=[],
        accepted_claim_ids=[],
        rejected_claim_ids=[],
        created_at=now,
        updated_at=now,
        error_retry_count=error_retry_count,
    )
    runner.state.targets[target.target_id] = target
    return target


def _seed_attempt(runner: DynamicAnalysisRunner) -> WorkerAttempt:
    attempt = WorkerAttempt(
        attempt_id="attempt-1",
        target_id="target-1",
        generation=1,
        backend="codex",
        session_id="thread-1",
        status="running",
        started_at=time.time(),
        completed_at=None,
        parent_session_id="thread-1",
    )
    runner.state.worker_attempts[attempt.attempt_id] = attempt
    return attempt


def _worker_failure(error: str) -> _WorkerExecutionResult:
    return _WorkerExecutionResult(
        attempt_id="attempt-1",
        target_id="target-1",
        generation=1,
        agent_result=AgentResult(exit_code=1, output=error, transcript=error, duration=1.0, session_id="thread-1"),
        report=None,
        error=error,
    )


def test_codex_usage_limit_is_not_classified_as_a_rate_limit(tmp_path):
    """It must not take the backoff path: the reset can be days out."""
    runner = _make_runner(tmp_path)
    assert runner._error_looks_like_rate_limit(CODEX_USAGE_LIMIT) is False
    # A 429 attached to the same message must not override the classification.
    result = AgentResult(
        exit_code=1,
        output=CODEX_USAGE_LIMIT,
        transcript=CODEX_USAGE_LIMIT,
        duration=1.0,
        rate_limit_status=429,
    )
    assert runner._error_looks_like_rate_limit(result) is False
    # Claude's timed limits keep using the backoff path.
    assert runner._error_looks_like_rate_limit("You've hit your limit · resets 12:30am") is True


def test_quota_exhaustion_never_sleeps(tmp_path):
    runner = _make_runner(tmp_path)
    with patch.object(runner, "_rate_limit_backoff") as backoff:
        for _ in range(runner.config.max_consecutive_errors + 2):
            runner._record_infrastructure_error(CODEX_USAGE_LIMIT)
    backoff.assert_not_called()


def test_worker_quota_exhaustion_requeues_target_without_charging_retries(tmp_path):
    """The regression: a spent-quota worker crash must not consume the target's
    error budget, and must never mark the target blocked."""
    runner = _make_runner(tmp_path)
    target = _seed_target(runner, error_retry_count=runner.config.max_worker_retries)
    _seed_attempt(runner)

    runner._apply_worker_result(_worker_failure(CODEX_USAGE_LIMIT))

    assert target.status == "queued"
    assert target.error_retry_count == runner.config.max_worker_retries
    assert not [e for e in runner.state.events if e.event_type == "target.blocked"]
    assert "quota exhausted" in runner._terminal_failure
    assert runner._should_terminate()[0] is True


def test_worker_crash_that_is_not_quota_still_charges_retries(tmp_path):
    runner = _make_runner(tmp_path)
    target = _seed_target(runner, error_retry_count=runner.config.max_worker_retries)
    _seed_attempt(runner)

    runner._apply_worker_result(_worker_failure("worker exited with code 1: segfault"))

    assert target.status == "blocked"
    assert target.error_retry_count == runner.config.max_worker_retries + 1
    assert runner._terminal_failure == ""


def _seed_claim_and_verification(runner: DynamicAnalysisRunner) -> tuple[ClaimRecord, VerificationRecord]:
    now = time.time()
    claim = ClaimRecord(
        claim_id="target-1-g1-claim-c1",
        worker_claim_id="c1",
        target_id="target-1",
        attempt_id="attempt-1",
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
        status="verifying",
        verification_ids=["v1"],
        rejection_class=None,
        verified_at=None,
        rejected_at=None,
    )
    runner.state.claims[claim.claim_id] = claim
    verification = VerificationRecord(
        verification_id="v1",
        claim_id=claim.claim_id,
        target_id="target-1",
        generation=1,
        backend="codex",
        verifier_role="analysis-verifier",
        session_id="v-thread-1",
        status="failed",
        disposition=None,
        reason="",
        rejection_class=None,
        raw_output="",
        started_at=now,
        completed_at=now,
        error=CODEX_USAGE_LIMIT.replace("worker", "verifier"),
    )
    runner.state.verifications[verification.verification_id] = verification
    return claim, verification


def test_verifier_quota_exhaustion_does_not_charge_retry_budget(tmp_path):
    runner = _make_runner(tmp_path)
    target = _seed_target(runner, error_retry_count=runner.config.max_worker_retries)
    target.pending_verification_ids = ["v1"]
    claim, verification = _seed_claim_and_verification(runner)

    runner._handle_verifier_error(verification, claim, target)

    assert target.error_retry_count == runner.config.max_worker_retries
    # The claim survives for a retry instead of being rejected as unverifiable.
    assert claim.status == "verifying"
    assert len(claim.verification_ids) == 2
    assert "quota exhausted" in runner._terminal_failure


def test_analyst_quota_exhaustion_leaves_the_brief_retryable(tmp_path):
    """`failed` analyst state is sticky across resumes, so a quota refusal must
    park it back at `pending` rather than burning the one attempt."""
    runner = _make_runner(tmp_path)
    runner.state.attack_surface.status = "running"
    runner.state.attack_surface.backend = "codex"

    runner._record_analyst_failure(f"analyst exited with code 1: {CODEX_USAGE_LIMIT}")

    assert runner.state.attack_surface.status == "pending"
    assert runner.state.attack_surface.backend == "codex"
    assert "quota exhausted" in runner._terminal_failure


def test_quota_terminal_failure_is_announced_once(tmp_path):
    runner = _make_runner(tmp_path)
    assert runner._note_quota_exhaustion(CODEX_USAGE_LIMIT) is True
    first = runner._terminal_failure
    # Later callers still get the refund signal, and the message is unchanged.
    assert runner._note_quota_exhaustion(CODEX_USAGE_LIMIT) is True
    assert runner._terminal_failure == first
    # The upstream text (including when to come back) rides along.
    assert "Aug 19th, 2026 8:33 PM" in runner._terminal_failure
