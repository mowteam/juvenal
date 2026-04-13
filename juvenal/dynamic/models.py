"""Dataclasses for the dynamic analysis engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class CodeLocation:
    path: str
    line: int
    symbol: str | None = None
    role: str | None = None


@dataclass
class TargetProposal:
    target_id: str
    title: str
    kind: str
    priority: int
    scope_paths: list[str]
    scope_symbols: list[str]
    instructions: str
    depends_on_claim_ids: list[str]
    spawn_reason: str


@dataclass
class CaptainTurn:
    message_to_user: str
    acknowledged_directive_ids: list[str]
    mental_model_summary: str
    open_questions: list[str]
    enqueue_targets: list[TargetProposal]
    defer_target_ids: list[str]
    termination_state: Literal["continue", "complete"]
    termination_reason: str


@dataclass
class CaptainState:
    session_id: str | None
    turn_index: int
    last_delivered_event_seq: int
    last_message_to_user: str
    mental_model_summary: str
    open_questions: list[str]
    last_turn_completed_at: float | None


@dataclass
class RunControl:
    stop_requested: bool
    wrap_requested: bool
    wrap_summary_pending: bool


@dataclass
class TargetRecord:
    target_id: str
    title: str
    kind: str
    priority: int
    status: Literal[
        "queued",
        "running",
        "verifying",
        "deferred",
        "completed",
        "no_findings",
        "blocked",
        "requeue_pending",
        "exhausted",
    ]
    source: Literal["captain", "user", "retry"]
    scope_paths: list[str]
    scope_symbols: list[str]
    instructions: str
    depends_on_claim_ids: list[str]
    spawn_reason: str
    generation: int
    active_generation: int | None
    active_attempt_id: str | None
    deferred_until_turn: int | None
    pending_verification_ids: list[str]
    accepted_claim_ids: list[str]
    rejected_claim_ids: list[str]
    created_at: float
    updated_at: float
    error_retry_count: int = 0


@dataclass
class WorkerAttempt:
    attempt_id: str
    target_id: str
    generation: int
    backend: str
    session_id: str | None
    status: Literal["queued", "running", "completed", "failed"]
    started_at: float | None
    completed_at: float | None
    error: str = ""
    retry_claim_id: str | None = None


@dataclass
class ProposedClaim:
    worker_claim_id: str
    kind: str
    subcategory: str | None
    summary: str
    assertion: str
    severity: Literal["low", "medium", "high", "critical"]
    worker_confidence: Literal["low", "medium", "high"]
    primary_location: CodeLocation
    locations: list[CodeLocation]
    preconditions: list[str]
    candidate_code_refs: list[CodeLocation]
    reasoning: str
    trace: list[CodeLocation]
    commands_run: list[str]
    counterevidence_checked: list[str]
    follow_up_hints: list[str]
    related_claim_ids: list[str]


@dataclass
class WorkerReport:
    schema_version: int
    task_id: str
    target_id: str
    outcome: Literal["claims", "no_findings", "blocked"]
    summary: str
    claims: list[ProposedClaim]
    blocker: str | None
    follow_up_hints: list[str]


@dataclass
class ClaimRecord:
    claim_id: str
    worker_claim_id: str
    target_id: str
    attempt_id: str
    generation: int
    kind: str
    subcategory: str | None
    summary: str
    assertion: str
    severity: Literal["low", "medium", "high", "critical"]
    worker_confidence: Literal["low", "medium", "high"]
    primary_location: CodeLocation
    locations: list[CodeLocation]
    preconditions: list[str]
    candidate_code_refs: list[CodeLocation]
    related_claim_ids: list[str]
    audit_artifact_id: str
    status: Literal["proposed", "verifying", "verified", "rejected", "invalid", "superseded"]
    verification_ids: list[str]
    rejection_class: str | None
    verified_at: float | None
    rejected_at: float | None
    retry_count: int = 0
    retry_of_claim_id: str | None = None
    retry_claim_ids: list[str] = field(default_factory=list)


@dataclass
class WorkerClaimArtifact:
    artifact_id: str
    claim_id: str
    worker_reasoning: str
    worker_trace: list[CodeLocation]
    commands_run: list[str]
    counterevidence_checked: list[str]
    follow_up_hints: list[str]


@dataclass
class VerifierClaimPacket:
    claim_id: str
    target_id: str
    kind: str
    subcategory: str | None
    summary: str
    assertion: str
    primary_location: CodeLocation
    locations: list[CodeLocation]
    preconditions: list[str]
    candidate_code_refs: list[CodeLocation]
    related_claim_ids: list[str]


@dataclass
class VerificationRecord:
    verification_id: str
    claim_id: str
    target_id: str
    generation: int
    backend: str
    verifier_role: str
    session_id: str | None
    status: Literal["pending", "running", "passed", "failed", "superseded"]
    disposition: Literal["verified", "rejected"] | None
    reason: str
    rejection_class: str | None
    raw_output: str
    started_at: float | None
    completed_at: float | None
    error: str = ""
    follow_up_action: str | None = None
    follow_up_strategy: str | None = None


@dataclass
class UserDirective:
    directive_id: str
    kind: Literal["focus", "ignore", "target", "ask", "summary", "stop", "wrap", "note"]
    text: str
    status: Literal["pending", "acknowledged", "applied"]
    created_at: float
    acknowledged_at: float | None


@dataclass
class DynamicEvent:
    seq: int
    event_type: Literal[
        "target.discovered",
        "target.started",
        "target.deferred",
        "target.completed",
        "target.no_findings",
        "target.blocked",
        "target.exhausted",
        "claim.proposed",
        "claim.verified",
        "claim.rejected",
        "claim.retry_scheduled",
        "directive.received",
        "directive.acknowledged",
    ]
    target_id: str | None
    claim_id: str | None
    directive_id: str | None
    generation: int | None
    payload: dict[str, Any]
    created_at: float


@dataclass
class CaptainDelta:
    verified_claim_ids: list[str]
    rejected_claim_ids: list[str]
    completed_target_ids: list[str]
    no_findings_target_ids: list[str]
    blocked_target_ids: list[str]
    exhausted_target_ids: list[str]
    pending_directive_ids: list[str]
    frontier_counts: dict[str, int]


__all__ = [
    "CaptainDelta",
    "CaptainState",
    "CaptainTurn",
    "ClaimRecord",
    "CodeLocation",
    "DynamicEvent",
    "ProposedClaim",
    "RunControl",
    "TargetProposal",
    "TargetRecord",
    "UserDirective",
    "VerificationRecord",
    "VerifierClaimPacket",
    "WorkerAttempt",
    "WorkerClaimArtifact",
    "WorkerReport",
]
