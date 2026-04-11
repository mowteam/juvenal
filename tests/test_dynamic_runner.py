"""Integration-style tests for the dynamic analysis runner."""

from __future__ import annotations

import json
from unittest.mock import patch

from juvenal.display import Display
from juvenal.dynamic.runner import DynamicAnalysisRunner
from juvenal.dynamic.state import DynamicSessionState
from juvenal.workflow import AnalysisConfig, Phase, Workflow
from tests.conftest import MockBackend


def _captain_output(
    *,
    enqueue_targets: list[dict] | None = None,
    defer_target_ids: list[str] | None = None,
    termination_state: str = "continue",
    termination_reason: str = "more work remains",
    message_to_user: str = "",
) -> str:
    payload = {
        "message_to_user": message_to_user,
        "acknowledged_directive_ids": [],
        "mental_model_summary": "Current analysis model.",
        "open_questions": [],
        "enqueue_targets": enqueue_targets or [],
        "defer_target_ids": defer_target_ids or [],
        "termination_state": termination_state,
        "termination_reason": termination_reason,
    }
    return f"CAPTAIN_JSON_BEGIN\n{json.dumps(payload, indent=2)}\nCAPTAIN_JSON_END"


def _target(target_id: str, *, priority: int = 90) -> dict:
    return {
        "target_id": target_id,
        "title": f"Inspect {target_id}",
        "kind": "module-level",
        "priority": priority,
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


def _blocked_output(task_id: str, target_id: str) -> str:
    payload = {
        "schema_version": 1,
        "task_id": task_id,
        "target_id": target_id,
        "outcome": "blocked",
        "summary": "Could not complete the scoped analysis.",
        "claims": [],
        "blocker": "Required context was missing.",
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
        "summary": summary
        or ("The claim is supported by the code." if disposition == "verified" else "The claim is not supported."),
        "follow_up_action": None,
        "follow_up_strategy": None,
    }
    verdict = "VERDICT: PASS" if disposition == "verified" else "VERDICT: FAIL: claim rejected"
    return f"VERIFICATION_JSON_BEGIN\n{json.dumps(payload, indent=2)}\nVERIFICATION_JSON_END\n{verdict}"


def _run_runner(
    tmp_path,
    backend: MockBackend,
    *,
    run_mode: str = "fresh",
    config: AnalysisConfig | None = None,
):
    phase = Phase(
        id="analyze",
        type="analysis",
        prompt="Analyze the repository for security issues.",
        analysis=config or AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1),
    )
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


def test_bootstrap_worker_verifier_pass_and_complete(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_claim_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "verifier",
        output=_verification_output("target-1-g1-claim-c1", "target-1", disposition="verified"),
    )
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="No further work remains."),
    )

    result, state, _ = _run_runner(tmp_path, backend)

    assert result.success is True
    assert state.captain.turn_index == 2
    assert state.targets["target-1"].status == "completed"
    assert state.claims["target-1-g1-claim-c1"].status == "verified"
    assert any(event.event_type == "target.completed" for event in state.events)


def test_verifier_fail_reopens_target_with_new_generation(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_claim_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "verifier",
        output=_verification_output(
            "target-1-g1-claim-c1",
            "target-1",
            disposition="rejected",
            rejection_class="guard-found",
            summary="A guard defeats the reported issue.",
        ),
    )
    backend.add_role_response("captain", output=_captain_output(termination_reason="Retrying the target first."))
    backend.add_role_response("worker", output=_no_findings_output("target-1-g2-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Retry produced no findings."),
    )

    result, state, _ = _run_runner(tmp_path, backend)

    assert result.success is True
    assert state.targets["target-1"].generation == 2
    assert state.targets["target-1"].active_generation == 2
    assert state.targets["target-1"].status == "no_findings"
    assert state.claims["target-1-g1-claim-c1"].status == "rejected"
    assert len(state.worker_attempts) == 2


def test_worker_no_findings(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="No findings remain."),
    )

    result, state, _ = _run_runner(tmp_path, backend)

    assert result.success is True
    assert state.targets["target-1"].status == "no_findings"
    assert any(event.event_type == "target.no_findings" for event in state.events)


def test_worker_blocked(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_blocked_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="The scoped target is blocked."),
    )

    result, state, _ = _run_runner(tmp_path, backend)

    assert result.success is True
    assert state.targets["target-1"].status == "blocked"
    assert any(event.event_type == "target.blocked" for event in state.events)


def test_target_reaches_exhausted_after_retry_budget_exhaustion(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_claim_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "verifier",
        output=_verification_output(
            "target-1-g1-claim-c1",
            "target-1",
            disposition="rejected",
            rejection_class="insufficient-evidence",
        ),
    )
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Retry budget is exhausted."),
    )

    result, state, _ = _run_runner(
        tmp_path,
        backend,
        config=AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=0),
    )

    assert result.success is True
    assert state.targets["target-1"].status == "exhausted"
    assert any(event.event_type == "target.exhausted" for event in state.events)


def test_captain_defer_target_ids(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(
            enqueue_targets=[_target("target-1", priority=90), _target("target-2", priority=80)],
            defer_target_ids=["target-2"],
        ),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response("captain", output=_captain_output(termination_reason="Deferred work can run now."))
    backend.add_role_response("worker", output=_no_findings_output("target-2-g1-attempt-1", "target-2"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="All deferred work is drained."),
    )

    result, state, backend = _run_runner(tmp_path, backend)

    worker_prompts = [prompt for role, prompt in backend.role_calls if role == "worker"]
    assert result.success is True
    assert any(event.event_type == "target.deferred" and event.target_id == "target-2" for event in state.events)
    assert len(worker_prompts) == 2
    assert '"target_id": "target-1"' in worker_prompts[0]
    assert '"target_id": "target-2"' in worker_prompts[1]


def test_malformed_captain_output_repairs_then_succeeds(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output="CAPTAIN_JSON_BEGIN\n{bad json}\nCAPTAIN_JSON_END",
        session_id="captain-s1",
    )
    backend.add_role_response("captain", output=_captain_output(enqueue_targets=[_target("target-1")]))
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(
            termination_state="complete",
            termination_reason="Repair succeeded and the target is done.",
        ),
    )

    result, state, backend = _run_runner(tmp_path, backend)

    assert result.success is True
    assert state.captain.turn_index == 2
    assert len(backend.resume_calls) == 2
    assert backend.resume_calls[0][0] == "captain-s1"


def test_empty_frontier_and_captain_complete(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="No in-scope targets were discovered."),
        session_id="captain-s1",
    )

    result, state, backend = _run_runner(tmp_path, backend)

    assert result.success is True
    assert state.captain.turn_index == 1
    assert state.targets == {}
    assert [role for role, _prompt in backend.role_calls] == ["captain"]
