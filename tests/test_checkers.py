"""Unit tests for checker utilities."""

import pytest

from juvenal.checkers import NO_VERDICT_REASON, extract_json_block, parse_verdict, parse_verification_report
from juvenal.workflow import Phase, make_command_check_prompt


class TestParseVerdict:
    def test_pass(self):
        passed, reason, target = parse_verdict("notes\nVERDICT: PASS")
        assert passed is True
        assert reason == ""
        assert target is None

    def test_fail(self):
        passed, reason, target = parse_verdict("VERDICT: FAIL: broken")
        assert passed is False
        assert reason == "broken"
        assert target is None

    def test_missing_verdict(self):
        passed, reason, target = parse_verdict("no verdict here")
        assert passed is False
        assert reason == NO_VERDICT_REASON
        assert target is None


class TestExtractJsonBlock:
    def test_extracts_object_between_markers(self):
        output = 'noise\nJSON_BEGIN\n{"key": "value"}\nJSON_END\nmore'
        assert extract_json_block(output, "JSON_BEGIN", "JSON_END") == {"key": "value"}

    def test_returns_none_for_malformed_json(self):
        output = "JSON_BEGIN\n{bad json}\nJSON_END"
        assert extract_json_block(output, "JSON_BEGIN", "JSON_END") is None


class TestParseVerificationReport:
    def test_plain_verdict_falls_back_to_minimal_report(self):
        report = parse_verification_report("VERDICT: FAIL: unsupported claim")

        assert report.passed is False
        assert report.reason == "unsupported claim"
        assert report.claim_id == ""
        assert report.disposition == "rejected"
        assert report.raw_json is None

    def test_structured_report_supports_nested_follow_up_recommendation(self):
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
  "summary": "Caller clamps the length before allocation.",
  "follow_up_recommendation": {
    "action": "retry-target",
    "strategy": "call-graph-partition",
    "reason": "Inspect other callers."
  }
}
VERIFICATION_JSON_END
VERDICT: FAIL: guard found
"""
        report = parse_verification_report(output)

        assert report.claim_id == "claim-1"
        assert report.follow_up_action == "retry-target"
        assert report.follow_up_strategy == "call-graph-partition"
        assert report.summary == "Caller clamps the length before allocation."

    def test_missing_verdict_raises(self):
        with pytest.raises(ValueError, match="must include a VERDICT line"):
            parse_verification_report("VERIFICATION_JSON_BEGIN\n{}\nVERIFICATION_JSON_END")


class TestCommandPromptChecks:
    def test_render_check_prompt_includes_command(self):
        phase = Phase(id="review", type="check", prompt=make_command_check_prompt("pytest -q"))
        prompt = phase.render_check_prompt()
        assert "pytest -q" in prompt
        assert "VERDICT: PASS" in prompt

    def test_render_check_prompt_substitutes_vars_in_command(self):
        phase = Phase(id="review", type="check", prompt=make_command_check_prompt("pytest {{TARGET}} -q"))
        prompt = phase.render_check_prompt(vars={"TARGET": "tests/unit"})
        assert "pytest tests/unit -q" in prompt
