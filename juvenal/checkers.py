"""Checker utilities — verdict and verification parsing."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Literal

# Pattern: VERDICT: FAIL(bounce-target): reason
_FAIL_WITH_TARGET = re.compile(r"^VERDICT:\s*FAIL\(([^)]+)\):\s*(.*)")
# Pattern: VERDICT: FAIL: reason  (no target)
_FAIL_PLAIN = re.compile(r"^VERDICT:\s*FAIL:\s*(.*)")
# Pattern: VERDICT: FAIL  (no reason, no target)
_FAIL_BARE = re.compile(r"^VERDICT:\s*FAIL\s*$")


NO_VERDICT_REASON = "checker did not emit a VERDICT line"


@dataclass
class VerificationReport:
    passed: bool
    reason: str
    claim_id: str
    target_id: str
    verifier_role: str
    backend: str
    disposition: Literal["verified", "rejected"]
    rejection_class: str | None
    summary: str
    follow_up_action: str | None
    follow_up_strategy: str | None
    raw_json: dict[str, Any] | None


def parse_verdict(output: str) -> tuple[bool, str, str | None]:
    """Parse VERDICT from agent output, scanning backwards.

    Supports two FAIL formats:
    - VERDICT: FAIL: reason           (no bounce target)
    - VERDICT: FAIL(target-id): reason  (agent-guided bounce target)

    Returns (passed, reason, bounce_target).
    bounce_target is None for PASS or when the agent didn't specify one.
    """
    for line in reversed(output.splitlines()):
        line = line.strip()
        if line.startswith("VERDICT: PASS"):
            return True, "", None

        m = _FAIL_WITH_TARGET.match(line)
        if m:
            return False, m.group(2).strip() or "unspecified", m.group(1).strip()

        m = _FAIL_PLAIN.match(line)
        if m:
            return False, m.group(1).strip() or "unspecified", None

        if _FAIL_BARE.match(line):
            return False, "unspecified", None

    return False, NO_VERDICT_REASON, None


def extract_json_block(output: str, begin_marker: str, end_marker: str) -> dict[str, Any] | None:
    """Extract the last marked JSON object from command output."""

    begin = output.rfind(begin_marker)
    if begin == -1:
        return None
    start = begin + len(begin_marker)
    end = output.find(end_marker, start)
    if end == -1:
        return None
    raw_json = output[start:end].strip()
    if not raw_json:
        return None
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def parse_verification_report(output: str) -> VerificationReport:
    """Parse structured verifier output for analysis-mode verification."""

    passed, reason, _ = parse_verdict(output)
    if reason == NO_VERDICT_REASON:
        raise ValueError("verification output must include a VERDICT line")

    payload = extract_json_block(output, "VERIFICATION_JSON_BEGIN", "VERIFICATION_JSON_END")
    disposition: Literal["verified", "rejected"] = "verified" if passed else "rejected"
    if payload is None:
        return VerificationReport(
            passed=passed,
            reason=reason,
            claim_id="",
            target_id="",
            verifier_role="",
            backend="",
            disposition=disposition,
            rejection_class=None,
            summary=reason,
            follow_up_action=None,
            follow_up_strategy=None,
            raw_json=None,
        )

    schema_version = payload.get("schema_version")
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        raise ValueError("VERIFICATION_JSON.schema_version must be an integer")
    if schema_version != 1:
        raise ValueError(f"VERIFICATION_JSON.schema_version must be 1, got {schema_version}")

    claim_id = payload.get("claim_id")
    target_id = payload.get("target_id")
    verifier_role = payload.get("verifier_role")
    backend = payload.get("backend")
    payload_disposition = payload.get("disposition")
    summary = payload.get("summary")
    rejection_class = payload.get("rejection_class")

    if not isinstance(claim_id, str) or not claim_id:
        raise ValueError("VERIFICATION_JSON.claim_id must be a non-empty string")
    if not isinstance(target_id, str) or not target_id:
        raise ValueError("VERIFICATION_JSON.target_id must be a non-empty string")
    if not isinstance(verifier_role, str) or not verifier_role:
        raise ValueError("VERIFICATION_JSON.verifier_role must be a non-empty string")
    if not isinstance(backend, str) or not backend:
        raise ValueError("VERIFICATION_JSON.backend must be a non-empty string")
    if payload_disposition not in {"verified", "rejected"}:
        raise ValueError("VERIFICATION_JSON.disposition must be 'verified' or 'rejected'")
    if not isinstance(summary, str):
        raise ValueError("VERIFICATION_JSON.summary must be a string")
    if rejection_class is not None and not isinstance(rejection_class, str):
        raise ValueError("VERIFICATION_JSON.rejection_class must be a string or null")

    if passed and payload_disposition != "verified":
        raise ValueError("VERIFICATION_JSON disposition disagrees with VERDICT: PASS")
    if not passed and payload_disposition != "rejected":
        raise ValueError("VERIFICATION_JSON disposition disagrees with VERDICT: FAIL")

    follow_up_action = payload.get("follow_up_action")
    follow_up_strategy = payload.get("follow_up_strategy")
    follow_up_recommendation = payload.get("follow_up_recommendation")
    if follow_up_recommendation is not None:
        if not isinstance(follow_up_recommendation, dict):
            raise ValueError("VERIFICATION_JSON.follow_up_recommendation must be an object")
        if follow_up_action is None:
            follow_up_action = follow_up_recommendation.get("action")
        if follow_up_strategy is None:
            follow_up_strategy = follow_up_recommendation.get("strategy")

    if follow_up_action is not None and not isinstance(follow_up_action, str):
        raise ValueError("VERIFICATION_JSON.follow_up_action must be a string or null")
    if follow_up_strategy is not None and not isinstance(follow_up_strategy, str):
        raise ValueError("VERIFICATION_JSON.follow_up_strategy must be a string or null")

    return VerificationReport(
        passed=passed,
        reason=reason,
        claim_id=claim_id,
        target_id=target_id,
        verifier_role=verifier_role,
        backend=backend,
        disposition=payload_disposition,
        rejection_class=rejection_class,
        summary=summary,
        follow_up_action=follow_up_action,
        follow_up_strategy=follow_up_strategy,
        raw_json=payload,
    )
