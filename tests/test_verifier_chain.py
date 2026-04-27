"""Tests for the multi-verifier chain feature in analysis phases."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from juvenal.display import Display
from juvenal.dynamic.runner import DynamicAnalysisRunner
from juvenal.dynamic.state import DynamicSessionState
from juvenal.workflow import (
    AnalysisConfig,
    Phase,
    ReporterSpec,
    VerifierSpec,
    Workflow,
    _parse_analysis_config,
)
from tests.conftest import MockBackend


def _captain_output(
    *,
    enqueue_targets: list[dict] | None = None,
    termination_state: str = "continue",
    termination_reason: str = "more work remains",
) -> str:
    payload = {
        "message_to_user": "",
        "acknowledged_directive_ids": [],
        "mental_model_summary": "Current analysis model.",
        "open_questions": [],
        "enqueue_targets": enqueue_targets or [],
        "defer_target_ids": [],
        "termination_state": termination_state,
        "termination_reason": termination_reason,
    }
    return f"CAPTAIN_JSON_BEGIN\n{json.dumps(payload, indent=2)}\nCAPTAIN_JSON_END"


def _target(target_id: str) -> dict:
    return {
        "target_id": target_id,
        "title": f"Inspect {target_id}",
        "kind": "module-level",
        "priority": 90,
        "scope_paths": ["src/app.py"],
        "scope_symbols": ["app"],
        "instructions": f"Analyze {target_id}.",
        "depends_on_claim_ids": [],
        "spawn_reason": f"Captain queued {target_id}.",
    }


def _claim_output(task_id: str, target_id: str, *, worker_claim_id: str = "c1") -> str:
    payload = {
        "schema_version": 1,
        "task_id": task_id,
        "target_id": target_id,
        "outcome": "claims",
        "summary": "One candidate issue.",
        "claims": [
            {
                "worker_claim_id": worker_claim_id,
                "kind": "input-validation",
                "subcategory": "missing-check",
                "summary": "Missing validation path.",
                "assertion": "The code path lacks an expected validation check.",
                "severity": "medium",
                "worker_confidence": "medium",
                "primary_location": {"path": "src/app.py", "line": 10, "symbol": "app", "role": "sink"},
                "locations": [{"path": "src/app.py", "line": 10, "symbol": "app", "role": "sink"}],
                "preconditions": ["Input reaches the code path."],
                "candidate_code_refs": [{"path": "src/app.py", "line": 10, "symbol": None, "role": None}],
                "reasoning": "The expected validation branch is absent.",
                "trace": [{"path": "src/app.py", "line": 10, "symbol": "app", "role": "sink"}],
                "commands_run": ['rg "app" src/app.py'],
                "counterevidence_checked": ["No guard was present nearby."],
                "follow_up_hints": [],
                "related_claim_ids": [],
            }
        ],
        "blocker": None,
        "follow_up_hints": [],
    }
    return f"WORKER_JSON_BEGIN\n{json.dumps(payload, indent=2)}\nWORKER_JSON_END"


def _no_findings_output(task_id: str, target_id: str) -> str:
    payload = {
        "schema_version": 1,
        "task_id": task_id,
        "target_id": target_id,
        "outcome": "no_findings",
        "summary": "No issue found in scope.",
        "claims": [],
        "blocker": None,
        "follow_up_hints": [],
    }
    return f"WORKER_JSON_BEGIN\n{json.dumps(payload, indent=2)}\nWORKER_JSON_END"


def _verification_output(
    claim_id: str,
    target_id: str,
    *,
    disposition: str,
    rejection_class: str | None = None,
    summary: str | None = None,
) -> str:
    payload = {
        "schema_version": 1,
        "claim_id": claim_id,
        "target_id": target_id,
        "verifier_role": "analysis-verifier",
        "backend": "claude",
        "disposition": disposition,
        "rejection_class": rejection_class,
        "summary": summary or ("Supported." if disposition == "verified" else "Rejected."),
        "follow_up_action": None,
        "follow_up_strategy": None,
    }
    verdict = "VERDICT: PASS" if disposition == "verified" else "VERDICT: FAIL: claim rejected"
    return f"VERIFICATION_JSON_BEGIN\n{json.dumps(payload, indent=2)}\nVERIFICATION_JSON_END\n{verdict}"


def _run_runner(tmp_path, backend: MockBackend, *, config: AnalysisConfig, run_mode: str = "fresh"):
    phase = Phase(id="analyze", type="analysis", prompt="Mission.", analysis=config)
    workflow = Workflow(name="analysis", phases=[phase], working_dir=str(tmp_path))
    state_file = tmp_path / "analysis-state.json"

    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        runner = DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode=run_mode,
            display=Display(plain=True),
            interactive=False,
        )
        result = runner.run()
    return result, DynamicSessionState.load(state_file), backend


def _three_verifier_config() -> AnalysisConfig:
    return AnalysisConfig(
        max_workers=1,
        max_verifiers=1,
        max_worker_retries=1,
        verifiers=[
            VerifierSpec(name="poc", backend="claude", prompt="PoC verifier scope."),
            VerifierSpec(name="scope", backend="claude", prompt="Scope verifier scope."),
            VerifierSpec(name="novelty", backend="claude", prompt="Novelty verifier scope."),
        ],
    )


# --- end-to-end chain behavior --------------------------------------------------


def test_chain_advances_through_all_verifiers_on_pass(tmp_path):
    backend = MockBackend()
    backend.add_role_response("captain", output=_captain_output(enqueue_targets=[_target("target-1")]))
    backend.add_role_response("worker", output=_claim_output("target-1-g1-attempt-1", "target-1"))
    # Three sequential verifier passes (queue them per-verifier-name)
    for name in ("poc", "scope", "novelty"):
        backend.add_role_response(
            f"verifier:{name}",
            output=_verification_output("target-1-g1-claim-c1", "target-1", disposition="verified"),
        )
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    result, state, _ = _run_runner(tmp_path, backend, config=_three_verifier_config())

    assert result.success is True
    claim = state.claims["target-1-g1-claim-c1"]
    assert claim.status == "verified"
    assert claim.failing_verifier_name is None
    # Three verifications recorded in chain order
    assert len(claim.verification_ids) == 3
    indices = [state.verifications[v_id].verifier_index for v_id in claim.verification_ids]
    names = [state.verifications[v_id].verifier_name for v_id in claim.verification_ids]
    assert indices == [0, 1, 2]
    assert names == ["poc", "scope", "novelty"]
    assert state.targets["target-1"].status == "completed"


def test_chain_fails_at_first_verifier_records_name(tmp_path):
    backend = MockBackend()
    backend.add_role_response("captain", output=_captain_output(enqueue_targets=[_target("target-1")]))
    backend.add_role_response("worker", output=_claim_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "verifier:poc",
        output=_verification_output(
            "target-1-g1-claim-c1",
            "target-1",
            disposition="rejected",
            rejection_class="insufficient-evidence",
        ),
    )
    # Captain turn 2 sees the rejection
    backend.add_role_response("captain", output=_captain_output(termination_reason="Retry pending."))
    # Retry worker: no findings — original rejection stands
    retry_attempt_id = "target-1-g1-retry-target-1-g1-claim-c1-1"
    backend.add_role_response("worker", output=_no_findings_output(retry_attempt_id, "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    result, state, _ = _run_runner(tmp_path, backend, config=_three_verifier_config())

    assert result.success is True
    claim = state.claims["target-1-g1-claim-c1"]
    assert claim.status == "rejected"
    assert claim.failing_verifier_name == "poc"
    # Only the poc verifier ran; scope and novelty never invoked.
    assert len(claim.verification_ids) == 1
    only = state.verifications[claim.verification_ids[0]]
    assert only.verifier_name == "poc"
    assert only.verifier_index == 0


def test_chain_fails_at_middle_verifier(tmp_path):
    backend = MockBackend()
    backend.add_role_response("captain", output=_captain_output(enqueue_targets=[_target("target-1")]))
    backend.add_role_response("worker", output=_claim_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "verifier:poc",
        output=_verification_output("target-1-g1-claim-c1", "target-1", disposition="verified"),
    )
    backend.add_role_response(
        "verifier:scope",
        output=_verification_output(
            "target-1-g1-claim-c1",
            "target-1",
            disposition="rejected",
            rejection_class="scope-too-broad",
        ),
    )
    backend.add_role_response("captain", output=_captain_output(termination_reason="Retry pending."))
    retry_attempt_id = "target-1-g1-retry-target-1-g1-claim-c1-1"
    backend.add_role_response("worker", output=_no_findings_output(retry_attempt_id, "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    result, state, _ = _run_runner(tmp_path, backend, config=_three_verifier_config())

    assert result.success is True
    claim = state.claims["target-1-g1-claim-c1"]
    assert claim.status == "rejected"
    assert claim.failing_verifier_name == "scope"
    # PoC and scope ran (2 records); novelty did not.
    indices = sorted(state.verifications[v_id].verifier_index for v_id in claim.verification_ids)
    assert indices == [0, 1]


def test_backward_compat_single_verifier_via_verifier_backend(tmp_path):
    """No `verifiers:` block — falls back to a synthesized single 'default' verifier."""
    backend = MockBackend()
    backend.add_role_response("captain", output=_captain_output(enqueue_targets=[_target("target-1")]))
    backend.add_role_response("worker", output=_claim_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "verifier",
        output=_verification_output("target-1-g1-claim-c1", "target-1", disposition="verified"),
    )
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)
    result, state, _ = _run_runner(tmp_path, backend, config=config)

    assert result.success is True
    claim = state.claims["target-1-g1-claim-c1"]
    assert claim.status == "verified"
    assert len(claim.verification_ids) == 1
    only = state.verifications[claim.verification_ids[0]]
    assert only.verifier_name == "default"
    assert only.verifier_index == 0


def test_resume_mid_chain_continues_from_next_verifier(tmp_path):
    """Persist a state with one passed verification at index 0 and resume."""
    state_file = tmp_path / "analysis-state.json"
    state = DynamicSessionState(state_file)

    # Manually seed a target + claim + one passed verification at index 0.
    from juvenal.dynamic.models import (
        ClaimRecord,
        CodeLocation,
        TargetRecord,
        VerificationRecord,
    )

    now = 1.0
    target = TargetRecord(
        target_id="target-1",
        title="Inspect target-1",
        kind="module-level",
        priority=90,
        status="verifying",
        source="captain",
        scope_paths=["src/app.py"],
        scope_symbols=["app"],
        instructions="Analyze.",
        depends_on_claim_ids=[],
        spawn_reason="captain",
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
    state.targets[target.target_id] = target

    claim = ClaimRecord(
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
        status="verifying",
        verification_ids=["v1"],
        rejection_class=None,
        verified_at=None,
        rejected_at=None,
    )
    state.claims[claim.claim_id] = claim

    v1 = VerificationRecord(
        verification_id="v1",
        claim_id=claim.claim_id,
        target_id=target.target_id,
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
        verifier_name="poc",
        verifier_index=0,
    )
    state.verifications[v1.verification_id] = v1
    state.save()

    # Now resume — the chain has 3 verifiers. Mid-chain claim must stay verifying.
    backend = MockBackend()
    backend.add_role_response(
        "verifier:scope",
        output=_verification_output(claim.claim_id, target.target_id, disposition="verified"),
    )
    backend.add_role_response(
        "verifier:novelty",
        output=_verification_output(claim.claim_id, target.target_id, disposition="verified"),
    )
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    config = _three_verifier_config()
    phase = Phase(id="analyze", type="analysis", prompt="Mission.", analysis=config)
    workflow = Workflow(name="analysis", phases=[phase], working_dir=str(tmp_path))
    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        runner = DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode="resume",
            display=Display(plain=True),
            interactive=False,
        )
        result = runner.run()

    final = DynamicSessionState.load(state_file)
    assert result.success is True
    final_claim = final.claims[claim.claim_id]
    assert final_claim.status == "verified"
    indices = sorted(final.verifications[v_id].verifier_index for v_id in final_claim.verification_ids)
    assert indices == [0, 1, 2]


# --- YAML parser validation -----------------------------------------------------


def test_yaml_parses_three_verifiers(tmp_path):
    raw = {
        "verifiers": [
            {"name": "poc", "backend": "claude", "prompt": "p1"},
            {"name": "scope", "backend": "codex", "prompt": "p2"},
            {"name": "novelty", "backend": "claude", "prompt_file": "novelty.md"},
        ],
    }
    (tmp_path / "novelty.md").write_text("from-file")
    cfg = _parse_analysis_config(raw, phase_id="p", yaml_path=tmp_path)
    assert cfg is not None
    assert [v.name for v in cfg.verifiers] == ["poc", "scope", "novelty"]
    assert [v.backend for v in cfg.verifiers] == ["claude", "codex", "claude"]
    assert cfg.verifiers[2].prompt == "from-file"


def test_yaml_rejects_both_verifiers_and_verifier_backend():
    raw = {
        "verifier_backend": "claude",
        "verifiers": [{"name": "poc", "prompt": "x"}],
    }
    with pytest.raises(ValueError, match="not both"):
        _parse_analysis_config(raw, phase_id="p")


def test_yaml_rejects_empty_verifiers_list():
    raw = {"verifiers": []}
    with pytest.raises(ValueError, match="cannot be empty"):
        _parse_analysis_config(raw, phase_id="p")


def test_yaml_rejects_duplicate_verifier_names():
    raw = {
        "verifiers": [
            {"name": "poc", "prompt": "p1"},
            {"name": "poc", "prompt": "p2"},
        ],
    }
    with pytest.raises(ValueError, match="duplicate name"):
        _parse_analysis_config(raw, phase_id="p")


def test_yaml_rejects_verifier_without_prompt():
    raw = {"verifiers": [{"name": "poc"}]}
    with pytest.raises(ValueError, match="must set prompt"):
        _parse_analysis_config(raw, phase_id="p")


def test_yaml_rejects_invalid_verifier_name():
    raw = {"verifiers": [{"name": "bad name!", "prompt": "p"}]}
    with pytest.raises(ValueError, match="must match"):
        _parse_analysis_config(raw, phase_id="p")


def _make_runner(tmp_path, *, config: AnalysisConfig | None = None) -> DynamicAnalysisRunner:
    config = config or AnalysisConfig(max_workers=1, max_verifiers=1)
    phase = Phase(id="analyze", type="analysis", prompt="Mission.", analysis=config)
    workflow = Workflow(name="analysis", phases=[phase], working_dir=str(tmp_path))
    state_file = tmp_path / "analysis-state.json"
    backend = MockBackend()
    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        return DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode="fresh",
            display=Display(plain=True),
            interactive=False,
        )


def test_rate_limit_backoff_caps_single_wait_at_one_hour(tmp_path):
    """Single wait must never exceed 3600 seconds even after many failures."""
    runner = _make_runner(tmp_path)
    sleeps: list[float] = []
    with patch("juvenal.dynamic.runner.time.sleep", side_effect=lambda d: sleeps.append(d)):
        # Force backoff_count high so 60*2**count >> 3600
        runner._backoff_count = 20
        runner._rate_limit_backoff()
    assert sum(sleeps) <= 3600 + 1


def test_rate_limit_backoff_caps_total_at_five_hours(tmp_path):
    """Cumulative wait across calls is capped at 5 hours, then run fails."""
    runner = _make_runner(tmp_path)
    total_slept: list[float] = []
    with patch("juvenal.dynamic.runner.time.sleep", side_effect=lambda d: total_slept.append(d)):
        # Force each call to use the full 1-hour single cap. Five fill the 5-hour budget;
        # the sixth must refuse and terminate the run.
        for _ in range(6):
            runner._backoff_count = 20  # 60 * 2**20 >> 3600 → capped at 1h
            runner._rate_limit_backoff()
    cumulative = sum(total_slept)
    assert cumulative <= 5 * 3600 + 1
    # The 6th call should have set terminal_failure (no further sleeps after the cap).
    assert runner._terminal_failure
    assert "budget exhausted" in runner._terminal_failure


def test_verifier_prompt_renders_template_vars(tmp_path):
    """A verifier `prompt` containing {{VAR}} renders against workflow.vars."""
    config = AnalysisConfig(
        verifiers=[VerifierSpec(name="scope", backend="claude", prompt="Scope says: {{ BOUNTY_SCOPE }}")],
    )
    phase = Phase(id="analyze", type="analysis", prompt="Mission.", analysis=config)
    workflow = Workflow(
        name="analysis",
        phases=[phase],
        working_dir=str(tmp_path),
        vars={"BOUNTY_SCOPE": "src/ only"},
    )
    state_file = tmp_path / "analysis-state.json"
    backend = MockBackend()

    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        runner = DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode="fresh",
            display=Display(plain=True),
            interactive=False,
        )
        rendered = runner._rendered_verifier_prompts["scope"]

    assert "src/ only" in rendered


# --- Reporter step --------------------------------------------------------------


def _three_verifier_with_reporter() -> AnalysisConfig:
    return AnalysisConfig(
        max_workers=1,
        max_verifiers=1,
        max_worker_retries=1,
        verifiers=[
            VerifierSpec(name="poc", backend="claude", prompt="PoC verifier scope."),
            VerifierSpec(name="scope", backend="claude", prompt="Scope verifier scope."),
            VerifierSpec(name="novelty", backend="claude", prompt="Novelty verifier scope."),
        ],
        reporter=ReporterSpec(backend="claude", prompt="Reporter scope."),
    )


def _queue_full_chain_pass(backend: MockBackend, *, target_id: str = "target-1") -> str:
    """Queue a captain → worker → 3 verifier passes. Returns the resulting claim_id."""
    backend.add_role_response("captain", output=_captain_output(enqueue_targets=[_target(target_id)]))
    backend.add_role_response("worker", output=_claim_output(f"{target_id}-g1-attempt-1", target_id))
    claim_id = f"{target_id}-g1-claim-c1"
    for name in ("poc", "scope", "novelty"):
        backend.add_role_response(
            f"verifier:{name}",
            output=_verification_output(claim_id, target_id, disposition="verified"),
        )
    return claim_id


def test_reporter_runs_after_final_verifier_pass(tmp_path):
    backend = MockBackend()
    claim_id = _queue_full_chain_pass(backend)
    # Reporter side effect: write the report.md the runner verifies for.
    report_dir = tmp_path / "output" / claim_id

    def _write_report(prompt: str, env: dict | None) -> None:
        report_dir.mkdir(parents=True, exist_ok=True)
        (report_dir / "report.md").write_text("# Bug report\n\nDetails.")
        (report_dir / "poc").write_text("trigger payload")

    backend.add_role_side_effect("reporter", _write_report)
    backend.add_role_response("reporter", output="done")
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    result, state, _ = _run_runner(tmp_path, backend, config=_three_verifier_with_reporter())

    assert result.success is True
    claim = state.claims[claim_id]
    assert claim.status == "verified"
    assert claim.reported_at is not None
    assert (report_dir / "report.md").is_file()
    assert (report_dir / "poc").is_file()
    # Reporter was invoked exactly once; the role_calls log proves it.
    reporter_calls = [r for r, _ in backend.role_calls if r == "reporter"]
    assert len(reporter_calls) == 1
    # And a claim.reported event was appended to the persistent event log.
    assert any(event.event_type == "claim.reported" for event in state.events)


def test_reporter_skipped_on_verifier_fail(tmp_path):
    backend = MockBackend()
    backend.add_role_response("captain", output=_captain_output(enqueue_targets=[_target("target-1")]))
    backend.add_role_response("worker", output=_claim_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "verifier:poc",
        output=_verification_output(
            "target-1-g1-claim-c1", "target-1", disposition="rejected", rejection_class="insufficient-evidence"
        ),
    )
    backend.add_role_response("captain", output=_captain_output(termination_reason="Retry pending."))
    retry_attempt_id = "target-1-g1-retry-target-1-g1-claim-c1-1"
    backend.add_role_response("worker", output=_no_findings_output(retry_attempt_id, "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    result, state, _ = _run_runner(tmp_path, backend, config=_three_verifier_with_reporter())

    assert result.success is True
    claim = state.claims["target-1-g1-claim-c1"]
    assert claim.status == "rejected"
    assert claim.reported_at is None
    reporter_calls = [r for r, _ in backend.role_calls if r == "reporter"]
    assert reporter_calls == []
    assert not (tmp_path / "output").exists() or not list((tmp_path / "output").iterdir())


def test_reporter_retried_when_report_md_missing(tmp_path):
    """If the agent claims success but does not write report.md, runner retries."""
    backend = MockBackend()
    claim_id = _queue_full_chain_pass(backend)
    # First reporter call: don't create the file. Second: create it.
    backend.add_role_response("reporter", output="first try, but I forgot to write")
    backend.add_role_response("reporter", output="second try, writing now")
    report_dir = tmp_path / "output" / claim_id

    def _write_report_late(prompt: str, env: dict | None) -> None:
        report_dir.mkdir(parents=True, exist_ok=True)
        (report_dir / "report.md").write_text("# Bug report")

    # Side-effect on the SECOND reporter call only (first call has no side effect).
    backend.add_role_side_effect("reporter", lambda p, e: None)
    backend.add_role_side_effect("reporter", _write_report_late)
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    result, state, _ = _run_runner(tmp_path, backend, config=_three_verifier_with_reporter())

    assert result.success is True
    claim = state.claims[claim_id]
    assert claim.reported_at is not None
    reporter_calls = [r for r, _ in backend.role_calls if r == "reporter"]
    assert len(reporter_calls) == 2  # one failed, one succeeded
    assert (report_dir / "report.md").is_file()


def test_reporter_idempotent_on_resume(tmp_path):
    """Second runner pass with reported_at already set must not invoke the reporter."""
    backend = MockBackend()
    claim_id = _queue_full_chain_pass(backend)

    def _write_report(prompt: str, env: dict | None) -> None:
        (tmp_path / "output" / claim_id).mkdir(parents=True, exist_ok=True)
        (tmp_path / "output" / claim_id / "report.md").write_text("# Initial")

    backend.add_role_side_effect("reporter", _write_report)
    backend.add_role_response("reporter", output="done")
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )
    result1, state1, _ = _run_runner(tmp_path, backend, config=_three_verifier_with_reporter())
    assert result1.success is True
    assert state1.claims[claim_id].reported_at is not None

    # Now resume on top of the saved state. No new captain/worker/verifier responses needed
    # because the run is already terminal-complete; we just re-create the runner and call run().
    backend2 = MockBackend()
    # Provide one termination-complete captain response in case the resumed runner asks the captain.
    backend2.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Already done."),
    )

    phase = Phase(id="analyze", type="analysis", prompt="Mission.", analysis=_three_verifier_with_reporter())
    workflow = Workflow(name="analysis", phases=[phase], working_dir=str(tmp_path))
    state_file = tmp_path / "analysis-state.json"
    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend2):
        runner = DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode="resume",
            display=Display(plain=True),
            interactive=False,
        )
        result2 = runner.run()
    state2 = DynamicSessionState.load(state_file)

    assert result2.success is True
    # Reporter must not have run again.
    reporter_calls = [r for r, _ in backend2.role_calls if r == "reporter"]
    assert reporter_calls == []
    assert state2.claims[claim_id].reported_at == state1.claims[claim_id].reported_at


def test_reporter_not_invoked_when_unconfigured(tmp_path):
    """Workflows without `reporter:` continue to behave as before."""
    backend = MockBackend()
    _queue_full_chain_pass(backend)
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )
    config = _three_verifier_config()  # no reporter
    result, state, _ = _run_runner(tmp_path, backend, config=config)
    assert result.success is True
    claim = state.claims["target-1-g1-claim-c1"]
    assert claim.status == "verified"
    assert claim.reported_at is None
    assert [r for r, _ in backend.role_calls if r == "reporter"] == []


# --- YAML parser cases for reporter --------------------------------------------


def test_yaml_parses_reporter_block(tmp_path):
    raw = {
        "verifiers": [{"name": "poc", "prompt": "p"}],
        "reporter": {"backend": "claude", "prompt": "Write the report."},
    }
    cfg = _parse_analysis_config(raw, phase_id="p")
    assert cfg is not None
    assert cfg.reporter is not None
    assert cfg.reporter.backend == "claude"
    assert cfg.reporter.prompt == "Write the report."


def test_yaml_reporter_prompt_file_resolves(tmp_path):
    (tmp_path / "report.md").write_text("from-file")
    raw = {
        "verifiers": [{"name": "poc", "prompt": "p"}],
        "reporter": {"prompt_file": "report.md"},
    }
    cfg = _parse_analysis_config(raw, phase_id="p", yaml_path=tmp_path)
    assert cfg is not None and cfg.reporter is not None
    assert cfg.reporter.prompt == "from-file"


def test_yaml_rejects_reporter_with_both_prompt_and_prompt_file():
    raw = {
        "verifiers": [{"name": "poc", "prompt": "p"}],
        "reporter": {"prompt": "x", "prompt_file": "y.md"},
    }
    with pytest.raises(ValueError, match="cannot set both"):
        _parse_analysis_config(raw, phase_id="p")


def test_yaml_rejects_reporter_without_prompt():
    raw = {
        "verifiers": [{"name": "poc", "prompt": "p"}],
        "reporter": {"backend": "claude"},
    }
    with pytest.raises(ValueError, match="must set prompt"):
        _parse_analysis_config(raw, phase_id="p")


def test_reporter_prompt_renders_template_vars(tmp_path):
    config = AnalysisConfig(
        reporter=ReporterSpec(backend="claude", prompt="Bug ID prefix: {{ PREFIX }}"),
    )
    phase = Phase(id="analyze", type="analysis", prompt="Mission.", analysis=config)
    workflow = Workflow(
        name="analysis",
        phases=[phase],
        working_dir=str(tmp_path),
        vars={"PREFIX": "BUG-"},
    )
    state_file = tmp_path / "analysis-state.json"
    backend = MockBackend()
    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        runner = DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode="fresh",
            display=Display(plain=True),
            interactive=False,
        )
    assert "BUG-" in runner._rendered_reporter_prompt


# --- Status printout coverage --------------------------------------------------


def test_chain_progress_helper_phrases():
    """Direct test of the formatter that drives the per-claim status suffix."""
    from juvenal.dynamic.models import (
        ClaimRecord,
        CodeLocation,
        VerificationRecord,
    )
    from juvenal.dynamic.state import DynamicSessionState
    from juvenal.state import _format_claim_chain_progress

    dss = DynamicSessionState(state_file=None)  # type: ignore[arg-type]
    base_claim = dict(
        worker_claim_id="c1",
        target_id="t1",
        attempt_id="a1",
        generation=1,
        kind="memory",
        subcategory=None,
        summary="x",
        assertion="x",
        severity="medium",
        worker_confidence="medium",
        primary_location=CodeLocation(path="x.c", line=1),
        locations=[],
        preconditions=[],
        candidate_code_refs=[],
        related_claim_ids=[],
        audit_artifact_id="art",
        rejection_class=None,
        verified_at=None,
        rejected_at=None,
    )

    def _verification(v_id: str, idx: int, name: str, *, status: str, disposition: str | None) -> VerificationRecord:
        return VerificationRecord(
            verification_id=v_id,
            claim_id="c1",
            target_id="t1",
            generation=1,
            backend="claude",
            verifier_role="analysis-verifier",
            session_id=None,
            status=status,
            disposition=disposition,
            reason="",
            rejection_class=None,
            raw_output="",
            started_at=None,
            completed_at=None,
            verifier_name=name,
            verifier_index=idx,
        )

    # Verifying, mid-chain (poc passed, scope is running, novelty pending)
    verifying_claim = ClaimRecord(claim_id="c1", status="verifying", verification_ids=["v0", "v1"], **base_claim)
    dss.verifications = {
        "v0": _verification("v0", 0, "poc", status="passed", disposition="verified"),
        "v1": _verification("v1", 1, "scope", status="running", disposition=None),
    }
    assert _format_claim_chain_progress(verifying_claim, dss, reporter_configured=False) == "@ scope (step 2)"

    # Rejected by named verifier
    rejected_claim = ClaimRecord(
        claim_id="c1",
        status="rejected",
        verification_ids=["v0"],
        failing_verifier_name="poc",
        **base_claim,
    )
    dss.verifications = {"v0": _verification("v0", 0, "poc", status="failed", disposition="rejected")}
    assert _format_claim_chain_progress(rejected_claim, dss, reporter_configured=False) == "rejected by poc"

    # Verified + reported
    import time as _time

    reported_claim = ClaimRecord(
        claim_id="c1",
        status="verified",
        verification_ids=["v0"],
        reported_at=_time.time(),
        **base_claim,
    )
    assert _format_claim_chain_progress(reported_claim, dss, reporter_configured=True) == "reported"

    # Verified, reporter configured, report not yet written
    pending_claim = ClaimRecord(claim_id="c1", status="verified", verification_ids=["v0"], **base_claim)
    assert _format_claim_chain_progress(pending_claim, dss, reporter_configured=True) == "report pending"

    # Verified, no reporter configured
    plain_claim = ClaimRecord(claim_id="c1", status="verified", verification_ids=["v0"], **base_claim)
    assert _format_claim_chain_progress(plain_claim, dss, reporter_configured=False) == ""


def test_validate_printout_lists_chain_and_reporter(tmp_path, capsys):
    """`juvenal validate` should surface chain composition and reporter state."""
    yaml_content = """\
name: chain-test
backend: claude
phases:
  - id: hunt
    type: analysis
    prompt: "Look for bugs."
    analysis:
      captain_backend: claude
      worker_backend: claude
      verifiers:
        - name: poc
          backend: claude
          prompt: "PoC."
        - name: scope
          backend: codex
          prompt: "Scope."
      reporter:
        backend: claude
        prompt: "Write the report."
"""
    yaml_path = tmp_path / "wf.yaml"
    yaml_path.write_text(yaml_content)

    from juvenal.cli import build_parser, cmd_validate

    parser = build_parser()
    args = parser.parse_args(["validate", str(yaml_path)])
    args.plain = True
    cmd_validate(args)
    out = capsys.readouterr().out
    assert "verifier chain (2)" in out
    assert "poc(claude)" in out
    assert "scope(codex)" in out
    assert "reporter: enabled" in out


def test_print_status_renders_chain_and_reporter(tmp_path):
    """Status table renders without error and includes the reporter summary."""
    import io
    import time as _time

    from rich.console import Console as _RichConsole

    from juvenal.dynamic.models import (
        ClaimRecord,
        CodeLocation,
        TargetRecord,
        VerificationRecord,
    )
    from juvenal.dynamic.state import DynamicSessionState
    from juvenal.state import PipelineState

    state_file = tmp_path / "state.json"
    state = PipelineState(state_file=state_file)
    ps = state._ensure_phase("analyze")
    ps.phase_type = "analysis"
    ps.analysis_state_file = ".juvenal-state-analyze-analysis.json"
    ps.status = "running"
    ps.started_at = _time.time() - 60
    state.save()

    dss = DynamicSessionState(state_file=tmp_path / ".juvenal-state-analyze-analysis.json")
    now = _time.time()
    dss.targets["t1"] = TargetRecord(
        target_id="t1",
        title="parser-overflow",
        kind="module-level",
        priority=90,
        status="completed",
        source="captain",
        scope_paths=["src/p.c"],
        scope_symbols=[],
        instructions="",
        depends_on_claim_ids=[],
        spawn_reason="x",
        generation=1,
        active_generation=1,
        active_attempt_id=None,
        deferred_until_turn=None,
        pending_verification_ids=[],
        accepted_claim_ids=["c1"],
        rejected_claim_ids=[],
        created_at=now,
        updated_at=now,
    )
    dss.claims["c1"] = ClaimRecord(
        claim_id="c1",
        worker_claim_id="wc1",
        target_id="t1",
        attempt_id="a1",
        generation=1,
        kind="memory",
        subcategory=None,
        summary="OOB write",
        assertion="OOB",
        severity="high",
        worker_confidence="high",
        primary_location=CodeLocation(path="src/p.c", line=10),
        locations=[],
        preconditions=[],
        candidate_code_refs=[],
        related_claim_ids=[],
        audit_artifact_id="art1",
        status="verified",
        verification_ids=["v0"],
        rejection_class=None,
        verified_at=now,
        rejected_at=None,
        reported_at=now,
    )
    dss.verifications["v0"] = VerificationRecord(
        verification_id="v0",
        claim_id="c1",
        target_id="t1",
        generation=1,
        backend="claude",
        verifier_role="analysis-verifier",
        session_id=None,
        status="passed",
        disposition="verified",
        reason="ok",
        rejection_class=None,
        raw_output="",
        started_at=now,
        completed_at=now,
        verifier_name="novelty",
        verifier_index=2,
    )
    dss.save()

    # Capture printed output via a Rich console pointed at a buffer.
    from juvenal.state import _format_claim_chain_progress

    # Sanity-check the helper directly using the persisted dss before rendering.
    assert _format_claim_chain_progress(dss.claims["c1"], dss, reporter_configured=True) == "reported"

    buf = io.StringIO()
    console = _RichConsole(file=buf, force_terminal=False, width=240)
    # Patch the module-level Console so print_status writes into our buffer.
    with patch("juvenal.state.Console", return_value=console):
        state.print_status()
    rendered = buf.getvalue().replace("\n", " ")
    assert "1 reported" in rendered
