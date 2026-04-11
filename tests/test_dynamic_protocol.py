"""Unit tests for dynamic analysis protocol helpers."""

from __future__ import annotations

from dataclasses import asdict

import pytest

from juvenal.checkers import parse_verification_report
from juvenal.dynamic import (
    claim_to_verifier_packet,
    parse_captain_output,
    parse_user_directive,
    parse_worker_output,
    validate_target_scope,
)
from juvenal.dynamic.models import ClaimRecord, CodeLocation


def _build_claim_record() -> ClaimRecord:
    return ClaimRecord(
        claim_id="claim-1",
        worker_claim_id="c1",
        target_id="target-1",
        attempt_id="attempt-1",
        generation=1,
        kind="integer-overflow",
        subcategory="allocation-size-wrap",
        summary="Length arithmetic can wrap before allocation.",
        assertion="User-controlled len reaches unchecked allocation sizing.",
        severity="high",
        worker_confidence="medium",
        primary_location=CodeLocation(path="src/net/parser.c", line=133, symbol="parse_frame"),
        locations=[
            CodeLocation(path="src/net/header.c", line=72, symbol="decode_header", role="source"),
            CodeLocation(path="src/net/parser.c", line=138, symbol="parse_frame", role="allocation"),
        ],
        preconditions=["Attacker controls packet length."],
        candidate_code_refs=[
            CodeLocation(path="src/net/header.c", line=72),
            CodeLocation(path="src/net/parser.c", line=133),
        ],
        related_claim_ids=["claim-0"],
        audit_artifact_id="artifact-1",
        status="proposed",
        verification_ids=[],
        rejection_class=None,
        verified_at=None,
        rejected_at=None,
    )


class TestParseCaptainOutput:
    def test_valid_captain_json(self):
        output = """
notes
CAPTAIN_JSON_BEGIN
{
  "message_to_user": "Need one more pass through the parser helpers.",
  "acknowledged_directive_ids": ["dir-1"],
  "mental_model": "Frame parsing is concentrated in src/net.",
  "open_questions": ["Is there a checked add helper?"],
  "enqueue_targets": [
    {
      "target_id": "target-1",
      "title": "Audit parse_frame",
      "kind": "memory-safety",
      "priority": 90,
      "scope_paths": ["src/net/parser.c"],
      "scope_symbols": ["parse_frame"],
      "instructions": "Trace untrusted lengths into allocation sites.",
      "depends_on_claim_ids": ["claim-0"],
      "spawn_reason": "Header decoding already looks suspicious."
    }
  ],
  "defer_target_ids": ["target-2"],
  "termination_state": "continue",
  "termination_reason": "More entry points remain."
}
CAPTAIN_JSON_END
"""
        turn = parse_captain_output(output)

        assert turn.message_to_user == "Need one more pass through the parser helpers."
        assert turn.mental_model_summary == "Frame parsing is concentrated in src/net."
        assert turn.enqueue_targets[0].target_id == "target-1"
        assert turn.enqueue_targets[0].depends_on_claim_ids == ["claim-0"]
        assert turn.defer_target_ids == ["target-2"]
        assert turn.termination_state == "continue"
        assert turn.termination_reason == "More entry points remain."

    def test_legacy_nested_termination_still_parses(self):
        output = """
CAPTAIN_JSON_BEGIN
{
  "message_to_user": "",
  "acknowledged_directive_ids": [],
  "mental_model_summary": "Parser work remains localized.",
  "open_questions": [],
  "enqueue_targets": [],
  "defer_target_ids": [],
  "termination": {"state": "complete", "reason": "No in-scope work remains."}
}
CAPTAIN_JSON_END
"""
        turn = parse_captain_output(output)

        assert turn.termination_state == "complete"
        assert turn.termination_reason == "No in-scope work remains."

    def test_malformed_captain_json_raises(self):
        output = "CAPTAIN_JSON_BEGIN\n{not-json}\nCAPTAIN_JSON_END"
        with pytest.raises(ValueError, match="CAPTAIN_JSON block must contain valid JSON object"):
            parse_captain_output(output)


class TestParseWorkerOutput:
    def test_valid_worker_json(self):
        output = """
WORKER_JSON_BEGIN
{
  "schema_version": 1,
  "task_id": "task-1",
  "target_id": "target-1",
  "outcome": "claims",
  "summary": "One overflow path appears plausible.",
  "claims": [
    {
      "worker_claim_id": "c1",
      "kind": "integer-overflow",
      "subcategory": "allocation-size-wrap",
      "summary": "Unchecked addition before malloc().",
      "assertion": "User-controlled len is added to a constant without checked arithmetic.",
      "severity": "high",
      "worker_confidence": "medium",
      "primary_location": {"path": "src/net/parser.c", "line": 133, "symbol": "parse_frame"},
      "locations": [{"path": "src/net/parser.c", "line": 138, "symbol": "parse_frame", "role": "allocation"}],
      "preconditions": ["Attacker controls packet length."],
      "candidate_code_refs": [{"path": "src/net/parser.c", "line": 133}],
      "reasoning": "No dominating bounds check was found.",
      "trace": [{"path": "src/net/header.c", "line": 72, "role": "source"}],
      "commands_run": ["rg \\"parse_frame|malloc\\" src/net"],
      "counterevidence_checked": ["No checked add helper was found."],
      "follow_up_hints": ["Inspect sibling parser helpers."],
      "related_claim_ids": []
    }
  ],
  "blocker": null,
  "follow_up_hints": ["Search for other parse_frame callers."]
}
WORKER_JSON_END
"""
        report = parse_worker_output(output)

        assert report.schema_version == 1
        assert report.outcome == "claims"
        assert report.claims[0].worker_claim_id == "c1"
        assert report.claims[0].primary_location.path == "src/net/parser.c"
        assert report.follow_up_hints == ["Search for other parse_frame callers."]

    def test_malformed_worker_json_raises(self):
        output = """
WORKER_JSON_BEGIN
{
  "schema_version": 1,
  "task_id": "task-1",
  "target_id": "target-1",
  "outcome": "claims",
  "summary": "No claims included.",
  "claims": [],
  "blocker": null,
  "follow_up_hints": []
}
WORKER_JSON_END
"""
        with pytest.raises(ValueError, match="must not be empty when outcome is 'claims'"):
            parse_worker_output(output)


class TestVerificationParsing:
    def test_verifier_json_and_verdict_agree(self):
        output = """
VERIFICATION_JSON_BEGIN
{
  "schema_version": 1,
  "claim_id": "claim-1",
  "target_id": "target-1",
  "verifier_role": "memory-safety",
  "backend": "claude",
  "disposition": "rejected",
  "rejection_class": "guard-found",
  "summary": "Caller clamps the reported length before allocation.",
  "follow_up_action": "retry-target",
  "follow_up_strategy": "call-graph-partition"
}
VERIFICATION_JSON_END
VERDICT: FAIL: caller-side guard found
"""
        report = parse_verification_report(output)

        assert report.passed is False
        assert report.claim_id == "claim-1"
        assert report.disposition == "rejected"
        assert report.rejection_class == "guard-found"
        assert report.follow_up_action == "retry-target"
        assert report.follow_up_strategy == "call-graph-partition"

    def test_verifier_json_and_verdict_mismatch_raises(self):
        output = """
VERIFICATION_JSON_BEGIN
{
  "schema_version": 1,
  "claim_id": "claim-1",
  "target_id": "target-1",
  "verifier_role": "memory-safety",
  "backend": "claude",
  "disposition": "rejected",
  "rejection_class": "guard-found",
  "summary": "Caller clamps the reported length before allocation."
}
VERIFICATION_JSON_END
VERDICT: PASS
"""
        with pytest.raises(ValueError, match="disagrees with VERDICT: PASS"):
            parse_verification_report(output)


class TestVerifierPacket:
    def test_allowlist_serializer_excludes_worker_only_fields(self):
        packet = claim_to_verifier_packet(_build_claim_record())
        payload = asdict(packet)

        assert payload == {
            "claim_id": "claim-1",
            "target_id": "target-1",
            "kind": "integer-overflow",
            "subcategory": "allocation-size-wrap",
            "summary": "Length arithmetic can wrap before allocation.",
            "assertion": "User-controlled len reaches unchecked allocation sizing.",
            "primary_location": {"path": "src/net/parser.c", "line": 133, "symbol": "parse_frame", "role": None},
            "locations": [
                {"path": "src/net/header.c", "line": 72, "symbol": "decode_header", "role": "source"},
                {"path": "src/net/parser.c", "line": 138, "symbol": "parse_frame", "role": "allocation"},
            ],
            "preconditions": ["Attacker controls packet length."],
            "candidate_code_refs": [
                {"path": "src/net/header.c", "line": 72, "symbol": None, "role": None},
                {"path": "src/net/parser.c", "line": 133, "symbol": None, "role": None},
            ],
            "related_claim_ids": ["claim-0"],
        }
        assert "worker_confidence" not in payload
        assert "audit_artifact_id" not in payload


class TestScopeValidation:
    def test_validate_target_scope_rejects_out_of_scope_path(self, tmp_path):
        validate_target_scope(["src/net/parser.c"], tmp_path)

        with pytest.raises(ValueError, match="outside working directory"):
            validate_target_scope(["../escape.py"], tmp_path)


@pytest.mark.parametrize(
    ("raw_text", "expected_kind", "expected_text"),
    [
        ("/focus parser entry points", "focus", "parser entry points"),
        ("/ignore path:src/generated", "ignore", "path:src/generated"),
        ("/target audit parse_frame callers", "target", "audit parse_frame callers"),
        ("/ask Which parser reaches malloc first?", "ask", "Which parser reaches malloc first?"),
        ("/summary", "summary", ""),
        ("/stop", "stop", ""),
        ("/wrap", "wrap", ""),
        ("look at the TLS handshake parser", "note", "look at the TLS handshake parser"),
    ],
)
def test_parse_user_directive_commands(raw_text: str, expected_kind: str, expected_text: str):
    directive = parse_user_directive(raw_text, directive_id="dir-1")

    assert directive.directive_id == "dir-1"
    assert directive.kind == expected_kind
    assert directive.text == expected_text
    assert directive.status == "pending"
    assert directive.acknowledged_at is None
    assert directive.created_at > 0
