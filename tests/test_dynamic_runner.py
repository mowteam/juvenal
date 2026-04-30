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
    acknowledged_directive_ids: list[str] | None = None,
    termination_state: str = "continue",
    termination_reason: str = "more work remains",
    message_to_user: str = "",
) -> str:
    payload = {
        "message_to_user": message_to_user,
        "acknowledged_directive_ids": acknowledged_directive_ids or [],
        "mental_model_summary": "Current analysis model.",
        "open_questions": [],
        "enqueue_targets": enqueue_targets or [],
        "defer_target_ids": defer_target_ids or [],
        "termination_state": termination_state,
        "termination_reason": termination_reason,
    }
    return f"CAPTAIN_JSON_BEGIN\n{json.dumps(payload, indent=2)}\nCAPTAIN_JSON_END"


def _target(
    target_id: str,
    *,
    priority: int = 90,
    scope_paths: list[str] | None = None,
    scope_symbols: list[str] | None = None,
) -> dict:
    return {
        "target_id": target_id,
        "title": f"Inspect {target_id}",
        "kind": "module-level",
        "priority": priority,
        "scope_paths": scope_paths or ["src/app.py"],
        "scope_symbols": scope_symbols or ["app"],
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


class ScriptedInteractionChannel:
    def __init__(self, responses: list[list[str]] | None = None):
        self._responses = list(responses or [])
        self.started = False
        self.stopped = False
        self.poll_calls = 0

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def poll(self, timeout: float) -> list[str]:
        self.poll_calls += 1
        if self._responses:
            return self._responses.pop(0)
        return []


class FakeChatDashboard:
    """Captures every render-hook call for assertions."""

    def __init__(self):
        self.start_calls = 0
        self.stop_calls = 0
        self.captain_renders: list[dict] = []
        self.events: list[tuple[str, str]] = []
        self.frontier_calls: list[dict] = []
        self.chat_input_calls: list[list[str]] = []
        self.show_captain_calls: list[dict] = []
        self.captain_chunks: list[str] = []
        self._running = False

    def start(self) -> None:
        self.start_calls += 1
        self._running = True

    def stop(self) -> None:
        self.stop_calls += 1
        self._running = False

    def is_running(self) -> bool:
        return self._running

    def render_captain(self, *, message_to_user, mental_model_summary, open_questions, turn_index) -> None:
        self.captain_renders.append(
            {
                "message_to_user": message_to_user,
                "mental_model_summary": mental_model_summary,
                "open_questions": list(open_questions),
                "turn_index": turn_index,
            }
        )

    def render_event(self, *, kind, text, ts=None) -> None:
        self.events.append((kind, text))

    def render_frontier(self, counts, active_targets) -> None:
        self.frontier_calls.append({"counts": dict(counts), "active": list(active_targets)})

    def render_chat_input(self, history) -> None:
        self.chat_input_calls.append(list(history))

    def show_captain_full(self, *, message_to_user, mental_model_summary, open_questions) -> None:
        self.show_captain_calls.append(
            {
                "message_to_user": message_to_user,
                "mental_model_summary": mental_model_summary,
                "open_questions": list(open_questions),
            }
        )

    def render_captain_chunk(self, text: str) -> None:
        self.captain_chunks.append(text)


class ChatScriptedChannel:
    """Like ScriptedInteractionChannel but for chat mode: yields one batch of
    lines per poll() call, even when poll is repeatedly called with timeout=0."""

    def __init__(self, batches: list[list[str]] | None = None):
        self._batches = list(batches or [])
        self.started = False
        self.stopped = False
        self.poll_calls = 0

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def poll(self, timeout: float) -> list[str]:
        self.poll_calls += 1
        if self._batches:
            return self._batches.pop(0)
        return []


def _run_runner(
    tmp_path,
    backend: MockBackend,
    *,
    run_mode: str = "fresh",
    config: AnalysisConfig | None = None,
    interactive: bool = False,
    interaction_channel: ScriptedInteractionChannel | None = None,
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
            interactive=interactive,
            interaction_channel=interaction_channel,
        )
        result = runner.run()
    return result, DynamicSessionState.load(state_file), backend


def _run_chat_runner(
    tmp_path,
    backend: MockBackend,
    *,
    chat_channel: ChatScriptedChannel,
    dashboard: FakeChatDashboard | None = None,
    config: AnalysisConfig | None = None,
):
    """Run a real DynamicAnalysisRunner through the _run_chat() branch.

    The runner sees `interactive=True` and no injected interaction channel
    (its constructor builds one), but we then swap our ChatScriptedChannel in
    and inject a fake dashboard. This routes through _run_chat()."""
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
            run_mode="fresh",
            display=Display(plain=True),
            interactive=True,
            chat_dashboard=dashboard,
        )
        # Replace the auto-created stdin channel with our scripted one.
        runner._interaction_channel = chat_channel
        result = runner.run()
    return result, DynamicSessionState.load(state_file), backend, runner


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


def test_verifier_fail_triggers_claim_scoped_retry(tmp_path):
    """When a verifier rejects a claim, a scoped retry worker runs for that claim."""
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
    # Captain turn 2: sees claim rejected + retry scheduled
    backend.add_role_response("captain", output=_captain_output(termination_reason="Claim retry in progress."))
    # Claim retry worker confirms no findings — original rejection stands
    retry_attempt_id = "target-1-g1-retry-target-1-g1-claim-c1-1"
    backend.add_role_response("worker", output=_no_findings_output(retry_attempt_id, "target-1"))
    # Captain turn 3: sees exhausted target and completes
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Retry produced no findings."),
    )

    result, state, _ = _run_runner(tmp_path, backend)

    assert result.success is True
    # Generation stays at 1 (no generation bump in claim-scoped model)
    assert state.targets["target-1"].generation == 1
    assert state.targets["target-1"].active_generation == 1
    assert state.targets["target-1"].status == "exhausted"
    assert state.claims["target-1-g1-claim-c1"].status == "rejected"
    # Two worker attempts: original + claim retry
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


def test_all_terminal_targets_with_non_complete_captain_succeeds_gracefully(tmp_path):
    """When every target reaches a terminal state but the captain refuses to declare 'complete',
    treat the analysis as successful rather than burning an expensive run.
    """
    backend = MockBackend()
    # Turn 1: enqueue a single target.
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-s1",
    )
    # Worker reports no findings → target reaches no_findings (terminal).
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    # Turn 2: captain sees no_findings event but does NOT request completion. With the frontier
    # now empty and no further work, the runner should treat this as a graceful end.
    for _ in range(5):
        backend.add_role_response(
            "captain",
            output=_captain_output(termination_reason="More avenues remain (in the captain's mind)."),
        )

    result, state, _ = _run_runner(tmp_path, backend)

    assert result.success is True
    assert state.targets["target-1"].status == "no_findings"


def test_ignore_path_directive_makes_matching_targets_ineligible(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(
            enqueue_targets=[
                _target("target-1", priority=100, scope_paths=["src/app.py"]),
                _target("target-2", priority=50, scope_paths=["src/generated/cache.py"]),
            ]
        ),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(
            termination_state="complete",
            termination_reason="Ignored generated code is out of scope.",
        ),
    )
    interaction = ScriptedInteractionChannel([["/ignore path:src/generated/"], []])

    result, state, backend = _run_runner(
        tmp_path,
        backend,
        config=AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1, interaction_timeout=0.01),
        interactive=True,
        interaction_channel=interaction,
    )

    worker_prompts = [prompt for role, prompt in backend.role_calls if role == "worker"]
    assert result.success is True
    assert state.ignored_path_prefixes == ["src/generated/"]
    assert state.targets["target-2"].status == "queued"
    assert len(worker_prompts) == 1
    assert '"target_id": "target-1"' in worker_prompts[0]
    assert all('"target_id": "target-2"' not in prompt for prompt in worker_prompts)
    assert state.directives["dir-1"].kind == "ignore"
    assert state.directives["dir-1"].status == "applied"


def test_ignore_symbol_directive_makes_matching_targets_ineligible(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(
            enqueue_targets=[
                _target("target-1", priority=100, scope_symbols=["app"]),
                _target("target-2", priority=50, scope_symbols=["LegacyParser"]),
            ]
        ),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Ignored symbol is out of scope."),
    )
    interaction = ScriptedInteractionChannel([["/ignore symbol:LegacyParser"], []])

    result, state, backend = _run_runner(
        tmp_path,
        backend,
        config=AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1, interaction_timeout=0.01),
        interactive=True,
        interaction_channel=interaction,
    )

    worker_prompts = [prompt for role, prompt in backend.role_calls if role == "worker"]
    assert result.success is True
    assert state.ignored_symbols == ["LegacyParser"]
    assert state.targets["target-2"].status == "queued"
    assert len(worker_prompts) == 1
    assert '"target_id": "target-1"' in worker_prompts[0]
    assert all('"target_id": "target-2"' not in prompt for prompt in worker_prompts)
    assert state.directives["dir-1"].kind == "ignore"
    assert state.directives["dir-1"].status == "applied"


def test_target_directive_creates_user_sourced_target(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(
            termination_state="continue",
            termination_reason="Waiting for more direction before selecting a target.",
            message_to_user="I have not picked a concrete target yet.",
        ),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_no_findings_output("user-target-1-g1-attempt-1", "user-target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(
            termination_state="complete",
            termination_reason="The user-supplied target is complete.",
        ),
    )
    interaction = ScriptedInteractionChannel([["/target inspect the config loader"], []])

    result, state, backend = _run_runner(
        tmp_path,
        backend,
        config=AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1, interaction_timeout=0.01),
        interactive=True,
        interaction_channel=interaction,
    )

    user_targets = [target for target in state.targets.values() if target.source == "user"]
    worker_prompts = [prompt for role, prompt in backend.role_calls if role == "worker"]
    assert result.success is True
    assert len(user_targets) == 1
    assert user_targets[0].target_id == "user-target-1"
    assert user_targets[0].title == "inspect the config loader"
    assert user_targets[0].priority == 100
    assert user_targets[0].kind == "user-target"
    assert worker_prompts and '"target_id": "user-target-1"' in worker_prompts[0]
    assert state.directives["dir-1"].kind == "target"
    assert state.directives["dir-1"].status == "applied"


def test_summary_directive_triggers_captain_turn(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(
            termination_state="continue",
            termination_reason="Need a user-directed summary request before wrapping up.",
            message_to_user="Ask if you want a summary before I stop.",
        ),
        session_id="captain-s1",
    )
    backend.add_role_response(
        "captain",
        output=_captain_output(
            acknowledged_directive_ids=["dir-1"],
            termination_state="complete",
            termination_reason="Summary delivered.",
            message_to_user="Summary requested by the user.",
        ),
    )
    interaction = ScriptedInteractionChannel([["/summary"]])

    result, state, backend = _run_runner(
        tmp_path,
        backend,
        config=AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1, interaction_timeout=0.01),
        interactive=True,
        interaction_channel=interaction,
    )

    assert result.success is True
    assert state.captain.turn_index == 2
    assert len(backend.resume_calls) == 1
    assert backend.resume_calls[0][0] == "captain-s1"
    assert state.directives["dir-1"].kind == "summary"
    assert state.directives["dir-1"].status == "acknowledged"


def test_stop_directive_ends_run_immediately(tmp_path):
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-s1",
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    interaction = ScriptedInteractionChannel([["/stop"]])

    result, state, backend = _run_runner(
        tmp_path,
        backend,
        config=AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1, interaction_timeout=0.01),
        interactive=True,
        interaction_channel=interaction,
    )

    assert result.success is False
    assert result.failure_context == "analysis stopped by user"
    assert state.control.stop_requested is True
    assert state.directives["dir-1"].kind == "stop"
    assert state.directives["dir-1"].status == "applied"
    assert [role for role, _prompt in backend.role_calls].count("captain") == 1


def test_wrap_directive_drains_active_work_then_completes(tmp_path):
    """In interactive mode, /wrap sent immediately after captain turn prevents new workers."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(
            enqueue_targets=[_target("target-1", priority=100), _target("target-2", priority=50)],
        ),
        session_id="captain-s1",
    )
    # Wrap summary captain turn (no active work since /wrap blocks scheduling)
    backend.add_role_response(
        "captain",
        output=_captain_output(
            termination_state="complete",
            termination_reason="Wrapped before any work started.",
            message_to_user="Here is the final wrap summary.",
        ),
    )
    interaction = ScriptedInteractionChannel([["/wrap"], []])

    result, state, backend = _run_runner(
        tmp_path,
        backend,
        config=AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1, interaction_timeout=0.01),
        interactive=True,
        interaction_channel=interaction,
    )

    assert result.success is True
    assert state.control.wrap_requested is True
    assert state.control.wrap_summary_pending is False
    assert state.directives["dir-1"].kind == "wrap"
    assert state.directives["dir-1"].status == "applied"


def test_worker_crash_does_not_kill_analysis(tmp_path):
    """A worker crash blocks the target but other targets can continue."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1"), _target("target-2")]),
        session_id="captain-s1",
    )
    # target-1 worker crashes (exit code 1)
    backend.add_role_response("worker", output="CRASH", exit_code=1)
    # target-1 retry also crashes (exhausts budget with max_worker_retries=1)
    backend.add_role_response("worker", output="CRASH AGAIN", exit_code=1)
    # target-2 worker succeeds
    backend.add_role_response("worker", output=_no_findings_output("target-2-g1-attempt-1", "target-2"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    result, state, _ = _run_runner(tmp_path, backend)

    assert result.success is True
    assert state.targets["target-1"].status == "blocked"
    assert state.targets["target-2"].status == "no_findings"


def test_claim_retry_worker_produces_verified_replacement(tmp_path):
    """A claim retry worker can produce a new claim that passes verification."""
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
            "target-1-g1-claim-c1", "target-1", disposition="rejected", rejection_class="guard-found"
        ),
    )
    # Captain sees the rejection
    backend.add_role_response("captain", output=_captain_output(termination_reason="Retry in progress."))
    # Retry worker produces a new claim with stronger evidence
    retry_attempt_id = "target-1-g1-retry-target-1-g1-claim-c1-1"
    backend.add_role_response(
        "worker",
        output=_claim_output(retry_attempt_id, "target-1", worker_claim_id="c1-retry"),
    )
    # Verifier approves the retry claim
    backend.add_role_response(
        "verifier",
        output=_verification_output("target-1-g1-retry-c1-retry", "target-1", disposition="verified"),
    )
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Retry succeeded."),
    )

    result, state, _ = _run_runner(tmp_path, backend)

    assert result.success is True
    assert state.targets["target-1"].status == "completed"
    # Original claim was rejected, retry claim was verified
    assert state.claims["target-1-g1-claim-c1"].status == "rejected"
    retry_claim = state.claims.get("target-1-g1-retry-c1-retry")
    assert retry_claim is not None
    assert retry_claim.status == "verified"
    assert retry_claim.retry_of_claim_id == "target-1-g1-claim-c1"
    assert retry_claim.retry_count == 1


def test_dependencies_satisfied_walks_retry_chain(tmp_path):
    """A dep on a rejected claim must be satisfied if any descendant retry claim is verified.

    Regression for: dependent targets enqueued against a claim_id that later got rejected and
    re-verified via a retry claim with a different id were stuck queued forever, because
    `_dependencies_satisfied` checked only the literal claim_id without walking retry_claim_ids.
    """
    from juvenal.dynamic.models import ClaimRecord, CodeLocation, TargetRecord

    state_file = tmp_path / "analysis-state.json"
    backend = MockBackend()
    backend.add_role_response("captain", output=_captain_output(termination_state="complete"))

    phase = Phase(
        id="analyze",
        type="analysis",
        prompt="Analyze.",
        analysis=AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1),
    )
    workflow = Workflow(name="analysis", phases=[phase], working_dir=str(tmp_path))

    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        runner = DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode="fresh",
            display=Display(plain=True),
            interactive=False,
            interaction_channel=None,
        )

    now = 1.0
    loc = CodeLocation(path="src/app.py", line=1, symbol="app", role="sink")

    def _make_claim(claim_id: str, *, status: str, retry_of: str | None, retry_ids: list[str]) -> ClaimRecord:
        return ClaimRecord(
            claim_id=claim_id,
            worker_claim_id=claim_id.split("-")[-1],
            target_id="target-1",
            attempt_id="attempt-x",
            generation=1,
            kind="k",
            subcategory=None,
            summary="s",
            assertion="a",
            severity="medium",
            worker_confidence="medium",
            primary_location=loc,
            locations=[loc],
            preconditions=[],
            candidate_code_refs=[],
            related_claim_ids=[],
            audit_artifact_id="art-1",
            status=status,
            verification_ids=[],
            rejection_class=None,
            verified_at=now if status == "verified" else None,
            rejected_at=now if status == "rejected" else None,
            retry_of_claim_id=retry_of,
            retry_claim_ids=list(retry_ids),
        )

    # Original claim: rejected, links to retry claim.
    runner.state.claims["target-1-g1-claim-c1"] = _make_claim(
        "target-1-g1-claim-c1", status="rejected", retry_of=None, retry_ids=["target-1-g1-retry-c1"]
    )
    # Retry claim: verified.
    runner.state.claims["target-1-g1-retry-c1"] = _make_claim(
        "target-1-g1-retry-c1", status="verified", retry_of="target-1-g1-claim-c1", retry_ids=[]
    )
    # Dependent target with deps on the rejected claim id.
    dep_target = TargetRecord(
        target_id="target-2",
        title="dep",
        kind="module-level",
        priority=90,
        status="queued",
        source="captain",
        scope_paths=["src/app.py"],
        scope_symbols=["app"],
        instructions="x",
        depends_on_claim_ids=["target-1-g1-claim-c1"],
        spawn_reason="x",
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

    # Bug case: rejected dep with verified retry → must be satisfied.
    assert runner._dependencies_satisfied(dep_target) is True

    # If the retry is also rejected and there is no further retry, the dep is NOT satisfied.
    runner.state.claims["target-1-g1-retry-c1"].status = "rejected"
    runner.state.claims["target-1-g1-retry-c1"].verified_at = None
    assert runner._dependencies_satisfied(dep_target) is False

    # Multi-level retry chain: rejected → rejected → verified should still satisfy.
    runner.state.claims["target-1-g1-retry-c1"].retry_claim_ids = ["target-1-g1-retry-c2"]
    runner.state.claims["target-1-g1-retry-c2"] = _make_claim(
        "target-1-g1-retry-c2", status="verified", retry_of="target-1-g1-retry-c1", retry_ids=[]
    )
    assert runner._dependencies_satisfied(dep_target) is True

    # Missing dep id (claim doesn't exist) → not satisfied.
    dep_target.depends_on_claim_ids = ["does-not-exist"]
    assert runner._dependencies_satisfied(dep_target) is False

    # No deps → trivially satisfied.
    dep_target.depends_on_claim_ids = []
    assert runner._dependencies_satisfied(dep_target) is True


def test_consecutive_errors_backoff_and_retry(tmp_path):
    """Consecutive infrastructure errors trigger backoff sleep, then retry (not fatal exit)."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-s1",
    )
    # 3 crashes trigger backoff, then worker succeeds on attempt 4
    backend.add_role_response("worker", output="CRASH", exit_code=1)
    backend.add_role_response("worker", output="CRASH", exit_code=1)
    backend.add_role_response("worker", output="CRASH", exit_code=1)
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-4", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    # Mock time.sleep so the backoff doesn't actually wait.
    with patch("juvenal.dynamic.runner.time.sleep"):
        result, state, _ = _run_runner(
            tmp_path,
            backend,
            config=AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=10, max_consecutive_errors=3),
        )

    assert result.success is True
    assert state.targets["target-1"].status == "no_findings"


def test_premature_complete_is_overridden_until_floor_met(tmp_path):
    """Captain declares `complete` before min_captain_turns is reached.
    The engine overrides, prepends a continue nudge to the next captain prompt,
    and only accepts `complete` after the floor is met.
    """
    backend = MockBackend()
    for _ in range(3):
        backend.add_role_response(
            "captain",
            output=_captain_output(termination_state="complete", termination_reason="No more work."),
        )

    config = AnalysisConfig(
        max_workers=1,
        max_verifiers=1,
        max_worker_retries=1,
        min_captain_turns=3,
        min_terminal_targets_before_complete=0,
        max_premature_completes=10,
    )
    result, state, _ = _run_runner(tmp_path, backend, config=config)

    assert result.success is True
    assert state.captain.turn_index == 3

    captain_prompts = [prompt for role, prompt in backend.role_calls if role == "captain"]
    assert len(captain_prompts) == 3
    assert "Continue nudge" not in captain_prompts[0]
    assert "Continue nudge" in captain_prompts[1]
    assert "Continue nudge" in captain_prompts[2]
    assert "override #1" in captain_prompts[1]
    assert "override #2" in captain_prompts[2]


def test_completion_floors_met_accepts_complete(tmp_path):
    """When floors are met after the first captain turn, `complete` is accepted."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    config = AnalysisConfig(
        max_workers=1,
        max_verifiers=1,
        max_worker_retries=1,
        min_captain_turns=1,
        min_terminal_targets_before_complete=0,
    )
    result, state, _ = _run_runner(tmp_path, backend, config=config)

    assert result.success is True
    assert state.captain.turn_index == 1
    captain_prompts = [prompt for role, prompt in backend.role_calls if role == "captain"]
    assert len(captain_prompts) == 1
    assert "Continue nudge" not in captain_prompts[0]


def test_soft_escape_after_max_premature_completes(tmp_path):
    """After max_premature_completes consecutive overrides, the engine accepts complete.
    Prevents an infinite nudge loop on dry codebases when floors cannot be met.
    """
    backend = MockBackend()
    for _ in range(3):
        backend.add_role_response(
            "captain",
            output=_captain_output(termination_state="complete", termination_reason="Nothing left."),
        )

    config = AnalysisConfig(
        max_workers=1,
        max_verifiers=1,
        max_worker_retries=1,
        min_captain_turns=100,
        min_terminal_targets_before_complete=0,
        max_premature_completes=2,
    )
    result, state, _ = _run_runner(tmp_path, backend, config=config)

    assert result.success is True
    assert state.captain.turn_index == 3

    captain_prompts = [prompt for role, prompt in backend.role_calls if role == "captain"]
    assert len(captain_prompts) == 3
    assert "Continue nudge" in captain_prompts[1]
    assert "Continue nudge" in captain_prompts[2]


def test_continue_nudge_counter_resets_when_captain_returns_continue(tmp_path):
    """If the captain pivots to `continue` after a nudge, the premature-complete counter
    resets — a single dry stretch followed by productive turns should not accelerate the
    soft escape later in the run.
    """
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done early."),
    )
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Now actually done."),
    )

    config = AnalysisConfig(
        max_workers=1,
        max_verifiers=1,
        max_worker_retries=1,
        min_captain_turns=3,
        min_terminal_targets_before_complete=0,
        max_premature_completes=1,
    )
    result, state, _ = _run_runner(tmp_path, backend, config=config)

    assert result.success is True
    assert state.captain.turn_index == 3
    assert state.targets["target-1"].status == "no_findings"


def test_chat_mode_dashboard_starts_and_stops_around_run(tmp_path):
    """The chat dashboard's start() and stop() hooks fire exactly once each."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    dashboard = FakeChatDashboard()
    chat = ChatScriptedChannel()
    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)

    result, _state, _backend, _runner = _run_chat_runner(
        tmp_path, backend, chat_channel=chat, dashboard=dashboard, config=config
    )

    assert result.success is True
    assert dashboard.start_calls == 1
    assert dashboard.stop_calls == 1
    assert chat.started is True
    assert chat.stopped is True


def test_chat_mode_renders_captain_turn_and_emits_events(tmp_path):
    """After a captain turn finishes, the dashboard sees render_captain plus
    a captain.turn event with the new turn index."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(
            enqueue_targets=[_target("target-1")],
            message_to_user="Pivoting to parsers.",
        ),
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    dashboard = FakeChatDashboard()
    chat = ChatScriptedChannel()
    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)

    result, state, _backend, _runner = _run_chat_runner(
        tmp_path, backend, chat_channel=chat, dashboard=dashboard, config=config
    )

    assert result.success is True
    assert state.captain.turn_index == 2
    assert any(call["turn_index"] == 1 for call in dashboard.captain_renders)
    assert any(kind == "captain.turn" for kind, _ in dashboard.events)
    assert any(kind == "captain.starting" for kind, _ in dashboard.events)


def test_chat_mode_now_directive_forces_captain_turn(tmp_path):
    """A `/now` directive forces a captain turn even if no event delta has fired."""
    backend = MockBackend()
    # Turn 1: enqueue, but say continue with no work for the second turn.
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    # Turn 2: triggered by event delta from no_findings.
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_reason="no_findings absorbed"),
    )
    # Turn 3: must be /now-forced (no new event delta otherwise).
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="user demanded"),
    )

    dashboard = FakeChatDashboard()
    # /now arrives between turns 2 and 3. Use enough empty batches so the
    # second turn's "starting" event has time to fire before /now.
    chat = ChatScriptedChannel(batches=[[], [], [], [], [], [], [], [], ["/now"]])
    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)

    result, state, _backend, _runner = _run_chat_runner(
        tmp_path, backend, chat_channel=chat, dashboard=dashboard, config=config
    )

    assert result.success is True
    assert state.captain.turn_index == 3
    # Confirm /now was applied as a directive.
    now_directives = [d for d in state.directives.values() if d.kind == "now"]
    assert len(now_directives) == 1
    assert now_directives[0].status == "applied"


def test_chat_mode_show_captain_invokes_dashboard_hook(tmp_path):
    """A `/show captain` directive calls dashboard.show_captain_full and
    persists the directive as applied."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(
            enqueue_targets=[_target("target-1")],
            message_to_user="Halfway through the parser audit.",
        ),
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    dashboard = FakeChatDashboard()
    chat = ChatScriptedChannel(batches=[[], [], [], [], ["/show captain"]])
    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)

    result, state, _backend, _runner = _run_chat_runner(
        tmp_path, backend, chat_channel=chat, dashboard=dashboard, config=config
    )

    assert result.success is True
    assert len(dashboard.show_captain_calls) == 1
    show_directives = [d for d in state.directives.values() if d.kind == "show"]
    assert len(show_directives) == 1
    assert show_directives[0].status == "applied"


def test_chat_mode_continuous_directive_ingestion(tmp_path):
    """Multiple directives streamed across iterations are all parsed and applied
    without a bounded review window."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    dashboard = FakeChatDashboard()
    chat = ChatScriptedChannel(
        batches=[
            [
                "/focus parser entry points",
                "take a look at the tls handshake",
                "/ignore path:vendor/",
            ],
        ]
    )
    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)

    result, state, _backend, _runner = _run_chat_runner(
        tmp_path, backend, chat_channel=chat, dashboard=dashboard, config=config
    )

    assert result.success is True
    kinds = sorted(d.kind for d in state.directives.values())
    assert kinds == ["focus", "ignore", "note"]
    # Dashboard saw a directive.applied event for each.
    applied_events = [text for kind, text in dashboard.events if kind == "directive.applied"]
    assert len(applied_events) == 3


def test_chat_mode_streams_captain_chunks_to_dashboard(tmp_path):
    """Captain's stream-json events flow through display_callback into
    dashboard.render_captain_chunk so the user sees the captain thinking."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )
    backend.add_role_chunks(
        "captain",
        [
            "Reading the codebase to understand the parser surface.",
            "[tool: Read]",
            "[tool: Grep]",
            "Identified one obvious bug. Marking complete.",
        ],
    )

    dashboard = FakeChatDashboard()
    chat = ChatScriptedChannel()
    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)

    result, _state, _backend, _runner = _run_chat_runner(
        tmp_path, backend, chat_channel=chat, dashboard=dashboard, config=config
    )

    assert result.success is True
    assert dashboard.captain_chunks == [
        "Reading the codebase to understand the parser surface.",
        "[tool: Read]",
        "[tool: Grep]",
        "Identified one obvious bug. Marking complete.",
    ]


def test_chat_directive_pauses_for_native_resume_interactive(tmp_path):
    """`/chat` after the first turn calls backend.resume_interactive with the
    captain's session_id. The next captain turn's prompt includes a re-priming
    prefix instructing the captain to return to CAPTAIN_JSON output."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-session-abc",
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="post-chat done"),
    )

    dashboard = FakeChatDashboard()
    chat = ChatScriptedChannel(batches=[[], [], [], ["/chat"]])
    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)

    result, state, _backend, _runner = _run_chat_runner(
        tmp_path, backend, chat_channel=chat, dashboard=dashboard, config=config
    )

    assert result.success is True
    assert backend.interactive_calls == ["resume:captain-session-abc"]
    chat_directives = [d for d in state.directives.values() if d.kind == "chat"]
    assert len(chat_directives) == 1
    assert chat_directives[0].status == "applied"
    captain_prompts = [prompt for role, prompt in backend.role_calls if role == "captain"]
    # The post-chat captain turn must include the re-priming prefix.
    assert any("Resuming from free-form chat" in prompt for prompt in captain_prompts)


def test_chat_directive_no_session_yet_is_a_no_op(tmp_path):
    """If the captain has no session_id yet (first turn not finished),
    `/chat` is a no-op that emits an info event and clears the pending flag."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    dashboard = FakeChatDashboard()
    chat = ChatScriptedChannel(batches=[["/chat"]])
    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)

    result, _state, _backend, _runner = _run_chat_runner(
        tmp_path, backend, chat_channel=chat, dashboard=dashboard, config=config
    )

    assert result.success is True
    assert backend.interactive_calls == []
    info_events = [text for kind, text in dashboard.events if kind == "info"]
    assert any("/chat skipped" in text for text in info_events)


def test_batch_mode_does_not_stream_chunks(tmp_path):
    """Batch mode (no dashboard) does not pass display_callback, so MockBackend's
    on_chunk hook never fires. This guards the carve-out: streaming is chat-only."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )
    backend.add_role_chunks("captain", ["chunk-a", "chunk-b"])

    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)
    result, _state, _ = _run_runner(tmp_path, backend, config=config)

    assert result.success is True
    captain_chunks = [text for role, text in backend.chunk_calls if role == "captain"]
    assert captain_chunks == []
