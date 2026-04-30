"""Protocol parsing helpers for the dynamic analysis engine."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Literal, Sequence

from juvenal.checkers import extract_json_block
from juvenal.dynamic.models import (
    CaptainTurn,
    ClaimRecord,
    CodeLocation,
    ProposedClaim,
    TargetProposal,
    UserDirective,
    VerifierClaimPacket,
    WorkerReport,
)

_CAPTAIN_BEGIN = "CAPTAIN_JSON_BEGIN"
_CAPTAIN_END = "CAPTAIN_JSON_END"
_WORKER_BEGIN = "WORKER_JSON_BEGIN"
_WORKER_END = "WORKER_JSON_END"


def _extract_required_mapping(output: str, begin_marker: str, end_marker: str, label: str) -> dict[str, Any]:
    payload = extract_json_block(output, begin_marker, end_marker)
    if payload is not None:
        return payload
    if begin_marker not in output or end_marker not in output:
        raise ValueError(f"{label} block not found")
    raise ValueError(f"{label} block must contain valid JSON object")


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return value


def _require_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    return value


def _require_non_empty_string(value: Any, field_name: str) -> str:
    text = _require_string(value, field_name).strip()
    if not text:
        raise ValueError(f"{field_name} must not be empty")
    return text


def _require_optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field_name)


def _require_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_positive_int(value: Any, field_name: str) -> int:
    number = _require_int(value, field_name)
    if number <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return number


def _require_string_list(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    items: list[str] = []
    for index, item in enumerate(value):
        items.append(_require_string(item, f"{field_name}[{index}]"))
    return items


def _optional_string_list(value: Any, field_name: str) -> list[str]:
    """Like _require_string_list, but treats null/missing as []. Used for
    optional list fields where LLMs frequently substitute null for an empty list."""
    if value is None:
        return []
    return _require_string_list(value, field_name)


def _require_literal(value: Any, field_name: str, allowed: tuple[str, ...]) -> str:
    text = _require_string(value, field_name)
    if text not in allowed:
        raise ValueError(f"{field_name} must be one of {allowed}, got {text!r}")
    return text


def _require_schema_version(value: Any, field_name: str) -> int:
    version = _require_int(value, field_name)
    if version != 1:
        raise ValueError(f"{field_name} must be 1, got {version}")
    return version


def _validate_unique(items: Sequence[str], field_name: str) -> None:
    if len(set(items)) != len(items):
        raise ValueError(f"{field_name} must not contain duplicates")


def _parse_code_location(value: Any, field_name: str) -> CodeLocation:
    payload = _require_mapping(value, field_name)
    return CodeLocation(
        path=_require_non_empty_string(payload.get("path"), f"{field_name}.path"),
        line=_require_positive_int(payload.get("line"), f"{field_name}.line"),
        symbol=_require_optional_string(payload.get("symbol"), f"{field_name}.symbol"),
        role=_require_optional_string(payload.get("role"), f"{field_name}.role"),
    )


def _parse_code_location_list(value: Any, field_name: str) -> list[CodeLocation]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return [_parse_code_location(item, f"{field_name}[{index}]") for index, item in enumerate(value)]


def _parse_target_proposal(value: Any, field_name: str) -> TargetProposal:
    payload = _require_mapping(value, field_name)
    depends_on_claim_ids = _require_string_list(
        payload.get("depends_on_claim_ids"),
        f"{field_name}.depends_on_claim_ids",
    )
    _validate_unique(depends_on_claim_ids, f"{field_name}.depends_on_claim_ids")
    return TargetProposal(
        target_id=_require_non_empty_string(payload.get("target_id"), f"{field_name}.target_id"),
        title=_require_non_empty_string(payload.get("title"), f"{field_name}.title"),
        kind=_require_non_empty_string(payload.get("kind"), f"{field_name}.kind"),
        priority=_require_int(payload.get("priority"), f"{field_name}.priority"),
        scope_paths=_require_string_list(payload.get("scope_paths"), f"{field_name}.scope_paths"),
        scope_symbols=_require_string_list(payload.get("scope_symbols"), f"{field_name}.scope_symbols"),
        instructions=_require_non_empty_string(payload.get("instructions"), f"{field_name}.instructions"),
        depends_on_claim_ids=depends_on_claim_ids,
        spawn_reason=_require_non_empty_string(payload.get("spawn_reason"), f"{field_name}.spawn_reason"),
    )


def _parse_claim(value: Any, field_name: str) -> ProposedClaim:
    payload = _require_mapping(value, field_name)
    related_claim_ids = _require_string_list(payload.get("related_claim_ids"), f"{field_name}.related_claim_ids")
    _validate_unique(related_claim_ids, f"{field_name}.related_claim_ids")
    return ProposedClaim(
        worker_claim_id=_require_non_empty_string(payload.get("worker_claim_id"), f"{field_name}.worker_claim_id"),
        kind=_require_non_empty_string(payload.get("kind"), f"{field_name}.kind"),
        subcategory=_require_optional_string(payload.get("subcategory"), f"{field_name}.subcategory"),
        summary=_require_non_empty_string(payload.get("summary"), f"{field_name}.summary"),
        assertion=_require_non_empty_string(payload.get("assertion"), f"{field_name}.assertion"),
        severity=_require_literal(
            payload.get("severity"),
            f"{field_name}.severity",
            ("low", "medium", "high", "critical"),
        ),
        worker_confidence=_require_literal(
            payload.get("worker_confidence"),
            f"{field_name}.worker_confidence",
            ("low", "medium", "high"),
        ),
        primary_location=_parse_code_location(payload.get("primary_location"), f"{field_name}.primary_location"),
        locations=_parse_code_location_list(payload.get("locations"), f"{field_name}.locations"),
        preconditions=_require_string_list(payload.get("preconditions"), f"{field_name}.preconditions"),
        candidate_code_refs=_parse_code_location_list(
            payload.get("candidate_code_refs"),
            f"{field_name}.candidate_code_refs",
        ),
        reasoning=_require_non_empty_string(payload.get("reasoning"), f"{field_name}.reasoning"),
        trace=_parse_code_location_list(payload.get("trace"), f"{field_name}.trace"),
        commands_run=_require_string_list(payload.get("commands_run"), f"{field_name}.commands_run"),
        counterevidence_checked=_require_string_list(
            payload.get("counterevidence_checked"),
            f"{field_name}.counterevidence_checked",
        ),
        follow_up_hints=_require_string_list(payload.get("follow_up_hints"), f"{field_name}.follow_up_hints"),
        related_claim_ids=related_claim_ids,
    )


def parse_captain_output(output: str) -> CaptainTurn:
    """Parse a captain response into a structured turn."""

    payload = _extract_required_mapping(output, _CAPTAIN_BEGIN, _CAPTAIN_END, "CAPTAIN_JSON")
    acknowledged_directive_ids = _optional_string_list(
        payload.get("acknowledged_directive_ids"),
        "CAPTAIN_JSON.acknowledged_directive_ids",
    )
    _validate_unique(acknowledged_directive_ids, "CAPTAIN_JSON.acknowledged_directive_ids")
    enqueue_targets_raw = payload.get("enqueue_targets")
    if enqueue_targets_raw is None:
        enqueue_targets_raw = []
    elif not isinstance(enqueue_targets_raw, list):
        raise ValueError("CAPTAIN_JSON.enqueue_targets must be a list")
    enqueue_targets = [
        _parse_target_proposal(item, f"CAPTAIN_JSON.enqueue_targets[{index}]")
        for index, item in enumerate(enqueue_targets_raw)
    ]
    _validate_unique([target.target_id for target in enqueue_targets], "CAPTAIN_JSON.enqueue_targets.target_id")
    defer_target_ids = _optional_string_list(payload.get("defer_target_ids"), "CAPTAIN_JSON.defer_target_ids")
    _validate_unique(defer_target_ids, "CAPTAIN_JSON.defer_target_ids")
    mental_model = payload.get("mental_model_summary", payload.get("mental_model"))
    if "termination_state" in payload or "termination_reason" in payload:
        termination_state = payload.get("termination_state")
        termination_reason = payload.get("termination_reason")
    else:
        termination = _require_mapping(payload.get("termination"), "CAPTAIN_JSON.termination")
        termination_state = termination.get("state")
        termination_reason = termination.get("reason")
    return CaptainTurn(
        message_to_user=_require_string(payload.get("message_to_user"), "CAPTAIN_JSON.message_to_user"),
        acknowledged_directive_ids=acknowledged_directive_ids,
        mental_model_summary=_require_string(mental_model, "CAPTAIN_JSON.mental_model_summary"),
        open_questions=_optional_string_list(payload.get("open_questions"), "CAPTAIN_JSON.open_questions"),
        enqueue_targets=enqueue_targets,
        defer_target_ids=defer_target_ids,
        termination_state=_require_literal(
            termination_state,
            "CAPTAIN_JSON.termination_state",
            ("continue", "complete"),
        ),
        termination_reason=_require_string(termination_reason, "CAPTAIN_JSON.termination_reason"),
    )


def parse_worker_output(output: str) -> WorkerReport:
    """Parse a worker response into a structured report."""

    payload = _extract_required_mapping(output, _WORKER_BEGIN, _WORKER_END, "WORKER_JSON")
    claims_raw = payload.get("claims")
    if not isinstance(claims_raw, list):
        raise ValueError("WORKER_JSON.claims must be a list")
    claims = [_parse_claim(item, f"WORKER_JSON.claims[{index}]") for index, item in enumerate(claims_raw)]
    _validate_unique([claim.worker_claim_id for claim in claims], "WORKER_JSON.claims.worker_claim_id")
    outcome = _require_literal(payload.get("outcome"), "WORKER_JSON.outcome", ("claims", "no_findings", "blocked"))
    blocker = _require_optional_string(payload.get("blocker"), "WORKER_JSON.blocker")
    if outcome == "claims" and not claims:
        raise ValueError("WORKER_JSON.claims must not be empty when outcome is 'claims'")
    if outcome != "claims" and claims:
        raise ValueError("WORKER_JSON.claims must be empty unless outcome is 'claims'")
    if outcome == "blocked" and (blocker is None or not blocker.strip()):
        raise ValueError("WORKER_JSON.blocker must be provided when outcome is 'blocked'")
    return WorkerReport(
        schema_version=_require_schema_version(payload.get("schema_version"), "WORKER_JSON.schema_version"),
        task_id=_require_non_empty_string(payload.get("task_id"), "WORKER_JSON.task_id"),
        target_id=_require_non_empty_string(payload.get("target_id"), "WORKER_JSON.target_id"),
        outcome=outcome,
        summary=_require_string(payload.get("summary"), "WORKER_JSON.summary"),
        claims=claims,
        blocker=blocker,
        follow_up_hints=_require_string_list(payload.get("follow_up_hints"), "WORKER_JSON.follow_up_hints"),
    )


def claim_to_verifier_packet(claim: ClaimRecord) -> VerifierClaimPacket:
    """Build the scrubbed allowlist payload passed to verifiers."""

    return VerifierClaimPacket(
        claim_id=claim.claim_id,
        target_id=claim.target_id,
        kind=claim.kind,
        subcategory=claim.subcategory,
        summary=claim.summary,
        assertion=claim.assertion,
        primary_location=claim.primary_location,
        locations=claim.locations,
        preconditions=claim.preconditions,
        candidate_code_refs=claim.candidate_code_refs,
        related_claim_ids=claim.related_claim_ids,
    )


def parse_user_directive(raw_text: str, *, directive_id: str) -> UserDirective:
    """Parse one review-point input line into a persisted directive."""

    text = raw_text.strip()
    if not text:
        raise ValueError("directive text must not be empty")

    kind: Literal["focus", "ignore", "target", "ask", "summary", "stop", "wrap", "note", "now", "show", "chat"]
    payload = text

    if text.startswith("/"):
        command, _, remainder = text.partition(" ")
        payload = remainder.strip()
        normalized = command.lower()
        if normalized == "/focus":
            if not payload:
                raise ValueError("/focus requires text")
            kind = "focus"
        elif normalized == "/ignore":
            if not payload:
                raise ValueError("/ignore requires text")
            if not payload.startswith("path:") and not payload.startswith("symbol:"):
                raise ValueError("/ignore must use 'path:' or 'symbol:' prefixes")
            kind = "ignore"
        elif normalized == "/target":
            if not payload:
                raise ValueError("/target requires text")
            kind = "target"
        elif normalized == "/ask":
            if not payload:
                raise ValueError("/ask requires text")
            kind = "ask"
        elif normalized == "/summary":
            kind = "summary"
        elif normalized == "/stop":
            kind = "stop"
        elif normalized == "/wrap":
            kind = "wrap"
        elif normalized == "/now":
            if payload:
                raise ValueError("/now does not accept arguments")
            kind = "now"
        elif normalized == "/show":
            if payload != "captain":
                raise ValueError("/show currently supports only 'captain' (e.g. /show captain)")
            kind = "show"
        elif normalized == "/chat":
            if payload:
                raise ValueError("/chat does not accept arguments")
            kind = "chat"
        else:
            raise ValueError(f"unsupported directive command: {command}")
    else:
        kind = "note"

    return UserDirective(
        directive_id=directive_id,
        kind=kind,
        text=payload,
        status="pending",
        created_at=time.time(),
        acknowledged_at=None,
    )


def validate_target_scope(paths: Sequence[str], working_dir: Path) -> None:
    """Reject target scope paths that escape the repository root."""

    root = working_dir.resolve()
    for index, raw_path in enumerate(paths):
        if not isinstance(raw_path, str):
            raise ValueError(f"scope_paths[{index}] must be a string")
        if not raw_path.strip():
            raise ValueError(f"scope_paths[{index}] must not be empty")
        candidate = Path(raw_path)
        resolved = (root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"scope path {raw_path!r} is outside working directory {root}") from exc


__all__ = [
    "claim_to_verifier_packet",
    "parse_captain_output",
    "parse_user_directive",
    "parse_worker_output",
    "validate_target_scope",
]
