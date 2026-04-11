"""Unit tests for dynamic analysis session state."""

from __future__ import annotations

from juvenal.dynamic.models import (
    CaptainTurn,
    ClaimRecord,
    CodeLocation,
    TargetRecord,
    UserDirective,
    VerificationRecord,
    WorkerAttempt,
    WorkerClaimArtifact,
)
from juvenal.dynamic.state import DynamicSessionState


def make_location(path: str = "src/app.py", line: int = 10, symbol: str | None = "fn") -> CodeLocation:
    return CodeLocation(path=path, line=line, symbol=symbol, role=None)


def make_target(
    target_id: str = "target-1",
    *,
    status: str = "queued",
    generation: int = 1,
    active_generation: int | None = None,
    active_attempt_id: str | None = None,
    deferred_until_turn: int | None = None,
    pending_verification_ids: list[str] | None = None,
    accepted_claim_ids: list[str] | None = None,
    rejected_claim_ids: list[str] | None = None,
) -> TargetRecord:
    return TargetRecord(
        target_id=target_id,
        title=f"Analyze {target_id}",
        kind="security",
        priority=50,
        status=status,
        source="captain",
        scope_paths=["src/app.py"],
        scope_symbols=["fn"],
        instructions="Inspect the target.",
        depends_on_claim_ids=[],
        spawn_reason="captain requested it",
        generation=generation,
        active_generation=generation if active_generation is None else active_generation,
        active_attempt_id=active_attempt_id,
        deferred_until_turn=deferred_until_turn,
        pending_verification_ids=list(pending_verification_ids or []),
        accepted_claim_ids=list(accepted_claim_ids or []),
        rejected_claim_ids=list(rejected_claim_ids or []),
        created_at=100.0,
        updated_at=100.0,
    )


def make_attempt(
    attempt_id: str = "attempt-1",
    *,
    target_id: str = "target-1",
    generation: int = 1,
    status: str = "queued",
    started_at: float | None = 101.0,
    completed_at: float | None = None,
    session_id: str | None = "worker-session",
) -> WorkerAttempt:
    return WorkerAttempt(
        attempt_id=attempt_id,
        target_id=target_id,
        generation=generation,
        backend="codex",
        session_id=session_id,
        status=status,
        started_at=started_at,
        completed_at=completed_at,
    )


def make_claim(
    claim_id: str = "claim-1",
    *,
    target_id: str = "target-1",
    attempt_id: str = "attempt-1",
    generation: int = 1,
    status: str = "proposed",
    rejection_class: str | None = None,
    verified_at: float | None = None,
    rejected_at: float | None = None,
    verification_ids: list[str] | None = None,
) -> ClaimRecord:
    location = make_location()
    return ClaimRecord(
        claim_id=claim_id,
        worker_claim_id=f"worker-{claim_id}",
        target_id=target_id,
        attempt_id=attempt_id,
        generation=generation,
        kind="memory-safety",
        subcategory="overflow",
        summary=f"summary for {claim_id}",
        assertion=f"assertion for {claim_id}",
        severity="high",
        worker_confidence="medium",
        primary_location=location,
        locations=[location],
        preconditions=[],
        candidate_code_refs=[location],
        related_claim_ids=[],
        audit_artifact_id=f"artifact-{claim_id}",
        status=status,
        verification_ids=list(verification_ids or []),
        rejection_class=rejection_class,
        verified_at=verified_at,
        rejected_at=rejected_at,
    )


def make_verification(
    verification_id: str = "verify-1",
    *,
    claim_id: str = "claim-1",
    target_id: str = "target-1",
    generation: int = 1,
    status: str = "pending",
    disposition: str | None = None,
    session_id: str | None = None,
    started_at: float | None = None,
    completed_at: float | None = None,
    rejection_class: str | None = None,
) -> VerificationRecord:
    return VerificationRecord(
        verification_id=verification_id,
        claim_id=claim_id,
        target_id=target_id,
        generation=generation,
        backend="claude",
        verifier_role="security-reviewer",
        session_id=session_id,
        status=status,
        disposition=disposition,
        reason="verification result",
        rejection_class=rejection_class,
        raw_output="raw verifier output",
        started_at=started_at,
        completed_at=completed_at,
    )


def make_artifact(artifact_id: str = "artifact-1", *, claim_id: str = "claim-1") -> WorkerClaimArtifact:
    return WorkerClaimArtifact(
        artifact_id=artifact_id,
        claim_id=claim_id,
        worker_reasoning="reasoning trail",
        worker_trace=[make_location(path="src/core.py", line=22, symbol="parse")],
        commands_run=["pytest tests/test_core.py -q"],
        counterevidence_checked=["validated bounds check"],
        follow_up_hints=["inspect sibling parser"],
    )


def make_directive(directive_id: str = "dir-1", *, status: str = "pending") -> UserDirective:
    return UserDirective(
        directive_id=directive_id,
        kind="focus",
        text="look at the parser",
        status=status,
        created_at=200.0,
        acknowledged_at=None,
    )


def make_turn(
    *,
    acknowledged_directive_ids: list[str] | None = None,
    defer_target_ids: list[str] | None = None,
) -> CaptainTurn:
    return CaptainTurn(
        message_to_user="captain update",
        acknowledged_directive_ids=list(acknowledged_directive_ids or []),
        mental_model_summary="mental model",
        open_questions=["what is next?"],
        enqueue_targets=[],
        defer_target_ids=list(defer_target_ids or []),
        termination_state="continue",
        termination_reason="still working",
    )


def test_atomic_save_and_load_round_trip(tmp_path):
    state_file = tmp_path / "dynamic-state.json"
    state = DynamicSessionState(state_file=state_file)
    state.captain.session_id = "captain-session"
    state.control.wrap_requested = True
    state.targets["target-1"] = make_target()
    state.save()

    loaded = DynamicSessionState.load(state_file)

    assert loaded.captain.session_id == "captain-session"
    assert loaded.control.wrap_requested is True
    assert loaded.targets["target-1"].title == "Analyze target-1"
    assert not state_file.with_name(f"{state_file.name}.tmp").exists()


def test_worker_claim_artifact_round_trip(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    artifact = make_artifact()

    state.store_worker_artifact(artifact)
    loaded = DynamicSessionState.load(state.state_file)

    assert loaded.worker_artifacts["artifact-1"] == artifact


def test_resume_normalization_rewrites_running_workers_and_queues_targets(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    state.worker_attempts["attempt-1"] = make_attempt(status="running")
    state.targets["target-1"] = make_target(status="running", active_attempt_id="attempt-1")

    state.normalize_for_resume()
    loaded = DynamicSessionState.load(state.state_file)

    attempt = loaded.worker_attempts["attempt-1"]
    target = loaded.targets["target-1"]
    assert attempt.status == "failed"
    assert attempt.completed_at is not None
    assert attempt.error == "interrupted-before-worker-completion"
    assert target.status == "queued"
    assert target.active_attempt_id is None
    assert target.generation == 1


def test_resume_normalization_rewrites_running_verifications_to_pending(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    state.targets["target-1"] = make_target(status="verifying")
    state.claims["claim-1"] = make_claim(status="verifying")
    state.verifications["verify-1"] = make_verification(
        status="running",
        disposition="verified",
        session_id="verify-session",
        started_at=111.0,
        completed_at=112.0,
    )

    state.normalize_for_resume()
    loaded = DynamicSessionState.load(state.state_file)

    verification = loaded.verifications["verify-1"]
    assert verification.status == "pending"
    assert verification.session_id is None
    assert verification.started_at is None
    assert verification.completed_at is None
    assert verification.disposition is None
    assert verification.error == "requeued-after-interrupted-verification"
    assert loaded.claims["claim-1"].status == "verifying"


def test_stop_requested_resume_holds_new_work_and_requests_immediate_stop(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    state.control.stop_requested = True
    state.targets["queued-target"] = make_target(target_id="queued-target", status="queued")
    state.targets["retry-target"] = make_target(target_id="retry-target", status="requeue_pending")
    state.targets["verifying-target"] = make_target(target_id="verifying-target", status="verifying")
    state.verifications["verify-1"] = make_verification(
        verification_id="verify-1",
        target_id="verifying-target",
        claim_id="claim-1",
        status="pending",
    )

    state.normalize_for_resume()

    assert state.targets["queued-target"].status == "deferred"
    assert state.targets["queued-target"].deferred_until_turn is None
    assert state.targets["retry-target"].status == "deferred"
    assert state.targets["retry-target"].deferred_until_turn is None
    assert state.targets["verifying-target"].status == "verifying"
    assert state.resume_control_action() == ("stop", "analysis stopped by user")


def test_normalize_rebuilds_pending_verification_ids_from_active_generation(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    state.targets["target-1"] = make_target(
        status="verifying",
        generation=2,
        active_generation=2,
        pending_verification_ids=["stale-id"],
    )
    state.verifications["verify-active"] = make_verification(
        verification_id="verify-active",
        claim_id="claim-active",
        generation=2,
        status="pending",
    )
    state.verifications["verify-passed"] = make_verification(
        verification_id="verify-passed",
        claim_id="claim-active",
        generation=2,
        status="passed",
        disposition="verified",
        completed_at=300.0,
    )
    state.verifications["verify-stale"] = make_verification(
        verification_id="verify-stale",
        claim_id="claim-stale",
        generation=1,
        status="pending",
    )
    state.verifications["verify-other-target"] = make_verification(
        verification_id="verify-other-target",
        claim_id="claim-other",
        target_id="target-2",
        generation=2,
        status="pending",
    )

    state.normalize_for_resume()

    assert state.targets["target-1"].pending_verification_ids == ["verify-active"]
    assert state.targets["target-1"].status == "verifying"


def test_wrap_requested_resume_drains_pending_verifications_but_holds_new_targets(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    state.control.wrap_requested = True
    state.control.wrap_summary_pending = True
    state.targets["queued-target"] = make_target(target_id="queued-target", status="queued")
    state.targets["verifying-target"] = make_target(target_id="verifying-target", status="verifying")
    state.claims["claim-1"] = make_claim(claim_id="claim-1", target_id="verifying-target", status="verifying")
    state.verifications["verify-1"] = make_verification(
        verification_id="verify-1",
        target_id="verifying-target",
        claim_id="claim-1",
        status="running",
        session_id="verify-session",
        started_at=111.0,
    )

    state.normalize_for_resume()

    assert state.targets["queued-target"].status == "deferred"
    assert state.targets["queued-target"].deferred_until_turn is None
    assert state.verifications["verify-1"].status == "pending"
    assert state.targets["verifying-target"].pending_verification_ids == ["verify-1"]
    assert state.resume_control_action() == ("drain", "")


def test_wrap_requested_resume_does_not_drain_persisted_queued_worker_attempts(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    state.control.wrap_requested = True
    state.control.wrap_summary_pending = True
    state.worker_attempts["attempt-1"] = make_attempt(
        attempt_id="attempt-1",
        target_id="target-1",
        status="queued",
        started_at=None,
        session_id=None,
    )
    state.targets["target-1"] = make_target(status="running", active_attempt_id="attempt-1")

    state.normalize_for_resume()

    assert state.targets["target-1"].status == "deferred"
    assert state.targets["target-1"].active_attempt_id is None
    assert state.resume_control_action() == ("summarize", "")


def test_wrap_requested_resume_requests_summary_then_finish_after_summary(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    state.control.wrap_requested = True
    state.control.wrap_summary_pending = True
    state.targets["queued-target"] = make_target(target_id="queued-target", status="queued", deferred_until_turn=3)

    state.normalize_for_resume()

    assert state.targets["queued-target"].status == "deferred"
    assert state.targets["queued-target"].deferred_until_turn is None
    assert state.resume_control_action() == ("summarize", "")

    state.control.wrap_summary_pending = False
    assert state.resume_control_action() == ("finish", "")


def test_pending_captain_delta_redelivers_unread_events_only(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    state.targets["target-1"] = make_target(status="queued")
    state.targets["target-2"] = make_target(target_id="target-2", status="exhausted")
    state.append_event("claim.verified", claim_id="claim-1", target_id="target-1", generation=1)
    state.append_event("claim.rejected", claim_id="claim-2", target_id="target-1", generation=1)
    state.append_event("directive.received", directive_id="dir-1")
    state.append_event("target.completed", target_id="target-1", generation=1)
    state.append_event("target.exhausted", target_id="target-2", generation=1)
    state.captain.last_delivered_event_seq = 1
    state.save()

    delta = DynamicSessionState.load(state.state_file).pending_captain_delta()

    assert delta.verified_claim_ids == []
    assert delta.rejected_claim_ids == ["claim-2"]
    assert delta.pending_directive_ids == ["dir-1"]
    assert delta.completed_target_ids == []
    assert delta.exhausted_target_ids == ["target-2"]
    assert delta.frontier_counts["queued"] == 1
    assert delta.frontier_counts["exhausted"] == 1


def test_stale_generation_is_preserved_for_audit_but_ignored_for_scheduling(tmp_path):
    state = DynamicSessionState(state_file=tmp_path / "dynamic-state.json")
    state.targets["target-1"] = make_target(status="verifying", generation=2, active_generation=2)
    state.claims["claim-stale"] = make_claim(
        claim_id="claim-stale",
        generation=1,
        status="rejected",
        rejection_class="false-positive",
        rejected_at=250.0,
    )
    state.claims["claim-active"] = make_claim(claim_id="claim-active", generation=2)
    state.verifications["verify-stale"] = make_verification(
        verification_id="verify-stale",
        claim_id="claim-stale",
        generation=1,
        status="pending",
    )
    state.verifications["verify-active"] = make_verification(
        verification_id="verify-active",
        claim_id="claim-active",
        generation=2,
        status="passed",
        disposition="verified",
        completed_at=400.0,
    )

    state.normalize_for_resume()

    target = state.targets["target-1"]
    assert target.status == "completed"
    assert target.pending_verification_ids == []
    assert target.accepted_claim_ids == ["claim-active"]
    assert target.rejected_claim_ids == []
    assert state.claims["claim-stale"].status == "rejected"
    assert "verify-stale" in state.verifications
    assert state.verifications["verify-stale"].status == "pending"


def test_ignore_rules_persist_across_restart(tmp_path):
    state_file = tmp_path / "dynamic-state.json"
    state = DynamicSessionState(state_file=state_file)
    state.ignored_path_prefixes.extend(["src/generated/", "vendor/"])
    state.ignored_symbols.extend(["LegacyParser", "unsafe_copy"])
    state.save()

    loaded = DynamicSessionState.load(state_file)

    assert loaded.ignored_path_prefixes == ["src/generated/", "vendor/"]
    assert loaded.ignored_symbols == ["LegacyParser", "unsafe_copy"]


def test_deferred_target_persists_and_requeues_on_next_turn(tmp_path):
    state_file = tmp_path / "dynamic-state.json"
    state = DynamicSessionState(state_file=state_file)
    state.targets["target-1"] = make_target(status="queued")

    state.record_captain_turn(make_turn(defer_target_ids=["target-1"]), delivered_event_seq=0)
    loaded = DynamicSessionState.load(state_file)
    assert loaded.targets["target-1"].status == "deferred"
    assert loaded.targets["target-1"].deferred_until_turn == 1

    loaded.record_captain_turn(make_turn(), delivered_event_seq=0)
    reloaded = DynamicSessionState.load(state_file)
    assert reloaded.targets["target-1"].status == "queued"
    assert reloaded.targets["target-1"].deferred_until_turn is None


def test_exhausted_target_persists_and_redelivers_to_captain(tmp_path):
    state_file = tmp_path / "dynamic-state.json"
    state = DynamicSessionState(state_file=state_file)
    state.targets["target-1"] = make_target(status="exhausted")
    exhausted_seq = state.append_event("target.exhausted", target_id="target-1", generation=1)

    loaded = DynamicSessionState.load(state_file)
    assert loaded.pending_captain_delta().exhausted_target_ids == ["target-1"]

    loaded.record_captain_turn(make_turn(), delivered_event_seq=exhausted_seq)
    reloaded = DynamicSessionState.load(state_file)
    assert reloaded.targets["target-1"].status == "exhausted"
    assert reloaded.pending_captain_delta().exhausted_target_ids == []


def test_directive_persistence_and_acknowledgment(tmp_path):
    state_file = tmp_path / "dynamic-state.json"
    state = DynamicSessionState(state_file=state_file)
    state.directives["dir-1"] = make_directive()
    directive_seq = state.append_event("directive.received", directive_id="dir-1")

    loaded = DynamicSessionState.load(state_file)
    assert loaded.pending_captain_delta().pending_directive_ids == ["dir-1"]

    loaded.record_captain_turn(make_turn(acknowledged_directive_ids=["dir-1"]), delivered_event_seq=directive_seq)
    reloaded = DynamicSessionState.load(state_file)

    directive = reloaded.directives["dir-1"]
    assert directive.status == "acknowledged"
    assert directive.acknowledged_at is not None
    assert reloaded.pending_captain_delta().pending_directive_ids == []
    assert any(
        event.event_type == "directive.acknowledged" and event.directive_id == "dir-1" for event in reloaded.events
    )
