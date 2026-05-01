"""Atomic JSON persistence for dynamic analysis session state."""

from __future__ import annotations

import json
import os
import time
import types
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Union, get_args, get_origin, get_type_hints

from juvenal.dynamic.models import (
    CaptainDelta,
    CaptainState,
    CaptainTurn,
    ClaimRecord,
    DynamicEvent,
    RunControl,
    TargetRecord,
    UserDirective,
    VerificationRecord,
    WorkerAttempt,
    WorkerClaimArtifact,
)

_ACTIVE_ATTEMPT_STATUSES = frozenset({"running"})
_CAPTAIN_EVENT_TYPES = frozenset(
    {
        "claim.verified",
        "claim.rejected",
        "claim.retry_scheduled",
        "target.no_findings",
        "target.blocked",
        "target.exhausted",
        "directive.received",
    }
)
_FRONTIER_STATUSES = (
    "queued",
    "running",
    "verifying",
    "deferred",
    "completed",
    "no_findings",
    "blocked",
    "requeue_pending",
    "exhausted",
)
_PRESERVED_TARGET_STATUSES = frozenset({"no_findings", "blocked", "exhausted"})
_WRAP_BLOCKED_TARGET_STATUSES = frozenset({"queued", "deferred", "requeue_pending"})
_UNION_ORIGINS = {Union, types.UnionType}


def _default_captain_state() -> CaptainState:
    return CaptainState(
        session_id=None,
        turn_index=0,
        last_delivered_event_seq=0,
        last_message_to_user="",
        mental_model_summary="",
        open_questions=[],
        last_turn_completed_at=None,
    )


def _default_run_control() -> RunControl:
    return RunControl(stop_requested=False, wrap_requested=False, wrap_summary_pending=False)


def _dataclass_from_dict(model_cls: type[Any], data: dict[str, Any]) -> Any:
    hints = get_type_hints(model_cls)
    kwargs: dict[str, Any] = {}
    for model_field in fields(model_cls):
        if model_field.name not in data:
            continue
        kwargs[model_field.name] = _coerce_value(hints.get(model_field.name, model_field.type), data[model_field.name])
    return model_cls(**kwargs)


def _coerce_value(annotation: Any, value: Any) -> Any:
    if value is None:
        return None

    origin = get_origin(annotation)
    if origin in _UNION_ORIGINS:
        options = [option for option in get_args(annotation) if option is not type(None)]
        for option in options:
            try:
                return _coerce_value(option, value)
            except (TypeError, ValueError):
                continue
        return value

    if origin is list:
        (item_type,) = get_args(annotation) or (Any,)
        return [_coerce_value(item_type, item) for item in value]

    if origin is dict:
        key_type, value_type = get_args(annotation) or (Any, Any)
        return {_coerce_value(key_type, key): _coerce_value(value_type, item) for key, item in value.items()}

    if isinstance(annotation, type) and is_dataclass(annotation):
        if not isinstance(value, dict):
            raise TypeError(f"expected mapping for {annotation.__name__}, got {type(value).__name__}")
        return _dataclass_from_dict(annotation, value)

    return value


def _load_dataclass_mapping(data: dict[str, Any], model_cls: type[Any]) -> dict[str, Any]:
    return {key: _dataclass_from_dict(model_cls, value) for key, value in data.items()}


def _load_dataclass_list(data: list[dict[str, Any]], model_cls: type[Any]) -> list[Any]:
    return [_dataclass_from_dict(model_cls, item) for item in data]


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


@dataclass
class DynamicSessionState:
    """Complete dynamic analysis phase state with atomic persistence."""

    state_file: Path
    captain: CaptainState = field(default_factory=_default_captain_state)
    control: RunControl = field(default_factory=_default_run_control)
    targets: dict[str, TargetRecord] = field(default_factory=dict)
    worker_attempts: dict[str, WorkerAttempt] = field(default_factory=dict)
    claims: dict[str, ClaimRecord] = field(default_factory=dict)
    worker_artifacts: dict[str, WorkerClaimArtifact] = field(default_factory=dict)
    verifications: dict[str, VerificationRecord] = field(default_factory=dict)
    directives: dict[str, UserDirective] = field(default_factory=dict)
    ignored_path_prefixes: list[str] = field(default_factory=list)
    ignored_symbols: list[str] = field(default_factory=list)
    events: list[DynamicEvent] = field(default_factory=list)
    _lock: RLock = field(init=False, repr=False, default_factory=RLock)

    @classmethod
    def load(cls, state_file: str | Path) -> DynamicSessionState:
        """Load dynamic state from disk, or return a fresh empty session."""

        state = cls(state_file=Path(state_file))
        if not state.state_file.exists():
            return state

        data = json.loads(state.state_file.read_text(encoding="utf-8"))
        if "captain" in data:
            state.captain = _dataclass_from_dict(CaptainState, data["captain"])
        if "control" in data:
            state.control = _dataclass_from_dict(RunControl, data["control"])
        state.targets = _load_dataclass_mapping(data.get("targets", {}), TargetRecord)
        state.worker_attempts = _load_dataclass_mapping(data.get("worker_attempts", {}), WorkerAttempt)
        state.claims = _load_dataclass_mapping(data.get("claims", {}), ClaimRecord)
        state.worker_artifacts = _load_dataclass_mapping(data.get("worker_artifacts", {}), WorkerClaimArtifact)
        state.verifications = _load_dataclass_mapping(data.get("verifications", {}), VerificationRecord)
        state.directives = _load_dataclass_mapping(data.get("directives", {}), UserDirective)
        state.ignored_path_prefixes = list(data.get("ignored_path_prefixes", []))
        state.ignored_symbols = list(data.get("ignored_symbols", []))
        state.events = _load_dataclass_list(data.get("events", []), DynamicEvent)
        return state

    def save(self) -> None:
        """Atomically save the current state to disk."""

        with self._lock:
            payload = json.dumps(self._to_dict(), indent=2, sort_keys=True) + "\n"
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.state_file.with_name(f"{self.state_file.name}.tmp")
            with open(tmp_path, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self.state_file)

    def normalize_for_resume(self, *, verifier_chain_length: int | None = None) -> None:
        """Rewrite interrupted in-flight work into deterministic schedulable state.

        If ``verifier_chain_length`` is provided, claims are only promoted to ``verified``
        when their highest-passed verifier index equals ``verifier_chain_length - 1``.
        Mid-chain passes leave the claim in ``verifying`` so the next chain step runs.
        """

        with self._lock:
            now = time.time()
            interrupted_attempt_ids: set[str] = set()

            for attempt in self.worker_attempts.values():
                if attempt.status != "running":
                    continue
                interrupted_attempt_ids.add(attempt.attempt_id)
                attempt.status = "failed"
                if attempt.completed_at is None:
                    attempt.completed_at = now
                attempt.error = "interrupted-before-worker-completion"

            for target in self.targets.values():
                if target.active_attempt_id in interrupted_attempt_ids:
                    if target.status == "running":
                        target.status = "queued"
                    target.active_attempt_id = None
                    target.updated_at = now
                    continue

                if not self._has_active_attempt(target):
                    target.active_attempt_id = None

            for verification in self.verifications.values():
                if verification.status != "running":
                    continue
                verification.status = "pending"
                # Preserve the session id and route the next attempt through
                # resume_agent so the verifier picks up where the interrupted
                # call left off rather than cold-restarting.
                if verification.session_id and not verification.parent_session_id:
                    verification.parent_session_id = verification.session_id
                verification.started_at = None
                verification.completed_at = None
                verification.error = "requeued-after-interrupted-verification"
                verification.disposition = None

            for target in self.targets.values():
                active_generation = target.active_generation
                target.pending_verification_ids = sorted(
                    verification.verification_id
                    for verification in self.verifications.values()
                    if verification.target_id == target.target_id
                    and verification.generation == active_generation
                    and verification.status == "pending"
                )

            for claim in self.claims.values():
                target = self.targets.get(claim.target_id)
                if target is None or target.active_generation != claim.generation:
                    continue

                relevant = self._claim_verifications(claim, target.active_generation)
                rejected = self._latest_terminal_verification(relevant, status="failed", disposition="rejected")
                verified = self._latest_terminal_verification(relevant, status="passed", disposition="verified")
                has_pending = any(verification.status == "pending" for verification in relevant)
                passed_indices = {
                    verification.verifier_index
                    for verification in relevant
                    if verification.status == "passed" and verification.disposition == "verified"
                }
                if verifier_chain_length is None:
                    chain_complete = verified is not None
                else:
                    chain_complete = bool(passed_indices) and max(passed_indices) == verifier_chain_length - 1

                if rejected is not None:
                    claim.status = "rejected"
                    claim.rejection_class = rejected.rejection_class
                    claim.rejected_at = rejected.completed_at
                    claim.verified_at = None
                    if rejected.verifier_name:
                        claim.failing_verifier_name = rejected.verifier_name
                elif verified is not None and chain_complete:
                    claim.status = "verified"
                    claim.rejection_class = None
                    claim.verified_at = verified.completed_at
                    claim.rejected_at = None
                elif verified is not None or has_pending:
                    # Mid-chain: at least one verifier passed but the chain isn't complete,
                    # or a verification is still pending. Keep the claim in verifying so
                    # the runner schedules the next chain step.
                    claim.status = "verifying"
                    claim.rejection_class = None
                    claim.verified_at = None
                    claim.rejected_at = None

            for target in self.targets.values():
                active_claims = self._active_generation_claims(target)
                leaf_claims = self._leaf_claims(active_claims)
                target.accepted_claim_ids = sorted(
                    claim.claim_id for claim in leaf_claims if claim.status == "verified"
                )
                target.rejected_claim_ids = sorted(
                    claim.claim_id for claim in leaf_claims if claim.status == "rejected"
                )

                if target.status == "deferred":
                    continue

                if target.status in _PRESERVED_TARGET_STATUSES:
                    continue

                if target.pending_verification_ids:
                    target.status = "verifying"
                elif leaf_claims and all(claim.status == "verified" for claim in leaf_claims):
                    target.status = "completed"
                elif any(claim.status in ("proposed", "verifying") for claim in leaf_claims):
                    target.status = "verifying"
                elif self._has_active_attempt(target):
                    target.status = "running"
                elif target.rejected_claim_ids:
                    # Rejected claims exist — runner will schedule retries if budget allows
                    target.status = "queued"
                else:
                    target.status = "queued"

            self._apply_resume_control_rewrite_locked(now)
            self.save()

    def append_event(self, event_type: str, **payload: Any) -> int:
        """Persist one dynamic event and return its assigned sequence number."""

        with self._lock:
            event = self._append_event_locked(event_type, **payload)
            self.save()
            return event.seq

    def pending_captain_delta(self) -> CaptainDelta:
        """Build the unread captain delta from events newer than the delivery cursor."""

        with self._lock:
            unread_events = [
                event
                for event in self.events
                if event.seq > self.captain.last_delivered_event_seq and event.event_type in _CAPTAIN_EVENT_TYPES
            ]
            frontier_counts = {status: 0 for status in _FRONTIER_STATUSES}
            for target in self.targets.values():
                frontier_counts[target.status] = frontier_counts.get(target.status, 0) + 1

            return CaptainDelta(
                verified_claim_ids=_dedupe_preserve_order(
                    [
                        event.claim_id
                        for event in unread_events
                        if event.event_type == "claim.verified" and event.claim_id
                    ]
                ),
                rejected_claim_ids=_dedupe_preserve_order(
                    [
                        event.claim_id
                        for event in unread_events
                        if event.event_type == "claim.rejected" and event.claim_id
                    ]
                ),
                completed_target_ids=[],
                no_findings_target_ids=_dedupe_preserve_order(
                    [
                        event.target_id
                        for event in unread_events
                        if event.event_type == "target.no_findings" and event.target_id
                    ]
                ),
                blocked_target_ids=_dedupe_preserve_order(
                    [
                        event.target_id
                        for event in unread_events
                        if event.event_type == "target.blocked" and event.target_id
                    ]
                ),
                exhausted_target_ids=_dedupe_preserve_order(
                    [
                        event.target_id
                        for event in unread_events
                        if event.event_type == "target.exhausted" and event.target_id
                    ]
                ),
                pending_directive_ids=_dedupe_preserve_order(
                    [
                        event.directive_id
                        for event in unread_events
                        if event.event_type == "directive.received" and event.directive_id
                    ]
                ),
                frontier_counts=frontier_counts,
            )

    def record_captain_turn(self, turn: CaptainTurn, delivered_event_seq: int) -> None:
        """Persist one successful captain turn and advance the event delivery cursor."""

        with self._lock:
            now = time.time()
            self._promote_due_deferred_targets_locked(now)
            next_turn_index = self.captain.turn_index + 1

            for target_id in turn.defer_target_ids:
                target = self.targets.get(target_id)
                if target is None or target.status != "queued":
                    continue
                target.status = "deferred"
                target.deferred_until_turn = next_turn_index
                target.updated_at = now
                self._append_event_locked(
                    "target.deferred",
                    target_id=target.target_id,
                    generation=target.active_generation,
                    deferred_until_turn=next_turn_index,
                )

            for directive_id in turn.acknowledged_directive_ids:
                directive = self.directives.get(directive_id)
                if directive is None:
                    continue
                if directive.status == "pending":
                    directive.status = "acknowledged"
                    directive.acknowledged_at = now
                    self._append_event_locked("directive.acknowledged", directive_id=directive_id)

            self.captain.turn_index = next_turn_index
            self.captain.last_message_to_user = turn.message_to_user
            self.captain.mental_model_summary = turn.mental_model_summary
            self.captain.open_questions = list(turn.open_questions)
            self.captain.last_turn_completed_at = now
            self.captain.last_delivered_event_seq = max(self.captain.last_delivered_event_seq, delivered_event_seq)
            self.save()

    def resume_control_action(self) -> tuple[str, str]:
        """Return the deterministic resume action implied by persisted /stop and /wrap flags."""

        with self._lock:
            if self.control.stop_requested:
                return ("stop", "analysis stopped by user")
            if not self.control.wrap_requested:
                return ("continue", "")
            if self._has_active_resume_work_locked():
                return ("drain", "")
            if self.control.wrap_summary_pending:
                return ("summarize", "")
            return ("finish", "")

    def store_worker_artifact(self, artifact: WorkerClaimArtifact) -> None:
        """Persist one worker claim artifact in the audit store."""

        with self._lock:
            self.worker_artifacts[artifact.artifact_id] = artifact
            self.save()

    def _active_generation_claims(self, target: TargetRecord) -> list[ClaimRecord]:
        active_generation = target.active_generation
        return [
            claim
            for claim in self.claims.values()
            if claim.target_id == target.target_id and claim.generation == active_generation
        ]

    def _leaf_claims(self, claims: list[ClaimRecord]) -> list[ClaimRecord]:
        """Return only the leaf claims in each retry chain (latest retry or never-retried)."""
        superseded_ids: set[str] = set()
        for claim in claims:
            if claim.retry_of_claim_id is not None:
                superseded_ids.add(claim.retry_of_claim_id)
        return [claim for claim in claims if claim.claim_id not in superseded_ids]

    def _apply_resume_control_rewrite_locked(self, now: float) -> None:
        if not (self.control.stop_requested or self.control.wrap_requested):
            return

        for target in self.targets.values():
            if target.status not in _WRAP_BLOCKED_TARGET_STATUSES:
                continue
            target.status = "deferred"
            target.deferred_until_turn = None
            target.updated_at = now

    def _append_event_locked(self, event_type: str, **payload: Any) -> DynamicEvent:
        seq = max((event.seq for event in self.events), default=0) + 1
        event = DynamicEvent(
            seq=seq,
            event_type=event_type,
            target_id=payload.pop("target_id", None),
            claim_id=payload.pop("claim_id", None),
            directive_id=payload.pop("directive_id", None),
            generation=payload.pop("generation", None),
            payload=payload,
            created_at=time.time(),
        )
        self.events.append(event)
        return event

    def _claim_verifications(self, claim: ClaimRecord, active_generation: int | None) -> list[VerificationRecord]:
        return [
            verification
            for verification in self.verifications.values()
            if verification.claim_id == claim.claim_id and verification.generation == active_generation
        ]

    def _has_active_attempt(self, target: TargetRecord) -> bool:
        if target.active_attempt_id is None:
            return False
        attempt = self.worker_attempts.get(target.active_attempt_id)
        return attempt is not None and attempt.status in _ACTIVE_ATTEMPT_STATUSES

    def _has_active_resume_work_locked(self) -> bool:
        if any(self._has_active_attempt(target) for target in self.targets.values()):
            return True

        for verification in self.verifications.values():
            if verification.status != "pending":
                continue
            target = self.targets.get(verification.target_id)
            if target is None:
                continue
            if target.active_generation == verification.generation:
                return True
        return False

    def _latest_terminal_verification(
        self,
        verifications: list[VerificationRecord],
        *,
        status: str,
        disposition: str,
    ) -> VerificationRecord | None:
        candidates = [
            verification
            for verification in verifications
            if verification.status == status and verification.disposition == disposition
        ]
        if not candidates:
            return None
        return max(
            candidates, key=lambda verification: (verification.completed_at or 0.0, verification.verification_id)
        )

    def _promote_due_deferred_targets_locked(self, now: float) -> None:
        for target in self.targets.values():
            if target.status != "deferred":
                continue
            if target.deferred_until_turn is None:
                continue
            if target.deferred_until_turn <= self.captain.turn_index:
                target.status = "queued"
                target.deferred_until_turn = None
                target.updated_at = now

    def _to_dict(self) -> dict[str, Any]:
        return {
            "captain": asdict(self.captain),
            "control": asdict(self.control),
            "targets": {target_id: asdict(target) for target_id, target in self.targets.items()},
            "worker_attempts": {attempt_id: asdict(attempt) for attempt_id, attempt in self.worker_attempts.items()},
            "claims": {claim_id: asdict(claim) for claim_id, claim in self.claims.items()},
            "worker_artifacts": {
                artifact_id: asdict(artifact) for artifact_id, artifact in self.worker_artifacts.items()
            },
            "verifications": {
                verification_id: asdict(verification) for verification_id, verification in self.verifications.items()
            },
            "directives": {directive_id: asdict(directive) for directive_id, directive in self.directives.items()},
            "ignored_path_prefixes": list(self.ignored_path_prefixes),
            "ignored_symbols": list(self.ignored_symbols),
            "events": [asdict(event) for event in self.events],
        }
