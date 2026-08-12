"""Integration-style tests for the dynamic analysis runner."""

from __future__ import annotations

import json
from concurrent.futures import Future
from unittest.mock import patch

from juvenal.display import Display
from juvenal.dynamic.models import (
    ClaimRecord,
    CodeLocation,
    TargetRecord,
    VerificationRecord,
    WorkerAttempt,
)
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
        analysis=config
        or AnalysisConfig(
            # Default test config opts into legacy independent-pool mode so
            # max_workers=1 forces strict serial dispatch (which most existing
            # tests rely on for predictable response-queue ordering). Tests
            # that exercise the shared-budget path opt in explicitly via
            # AnalysisConfig(shared_agent_budget=True, max_agents=N).
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
        ),
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
        analysis=config
        or AnalysisConfig(
            # Default test config opts into legacy independent-pool mode so
            # max_workers=1 forces strict serial dispatch (which most existing
            # tests rely on for predictable response-queue ordering). Tests
            # that exercise the shared-budget path opt in explicitly via
            # AnalysisConfig(shared_agent_budget=True, max_agents=N).
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
        ),
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
        config=AnalysisConfig(
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
            interaction_timeout=0.01,
        ),
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
        config=AnalysisConfig(
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
            interaction_timeout=0.01,
        ),
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
        config=AnalysisConfig(
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
            interaction_timeout=0.01,
        ),
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
        config=AnalysisConfig(
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
            interaction_timeout=0.01,
        ),
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
        config=AnalysisConfig(
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
            interaction_timeout=0.01,
        ),
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
        config=AnalysisConfig(
            shared_agent_budget=False,
            max_workers=1,
            max_verifiers=1,
            max_worker_retries=1,
            interaction_timeout=0.01,
        ),
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

    # Skip the backoff sleep so the test runs fast.
    with (
        patch("juvenal.dynamic.runner.time.sleep"),
        patch(
            "juvenal.dynamic.runner.DynamicAnalysisRunner._sleep_with_shutdown",
            return_value=False,
        ),
    ):
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


def test_chat_mode_keyboard_interrupt_kills_active_and_returns_failure(tmp_path, monkeypatch):
    """A Ctrl-C during the chat loop must call kill_active so subprocess
    threads can exit, return success=False with an `interrupted` context, and not
    leave the user pressing Ctrl-C repeatedly waiting for executor threads."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    from juvenal.dynamic.runner import DynamicAnalysisRunner

    kill_calls: list[int] = []
    original_kill = DynamicAnalysisRunner.kill_active

    def spy_kill(self):
        kill_calls.append(1)
        original_kill(self)

    monkeypatch.setattr(DynamicAnalysisRunner, "kill_active", spy_kill)

    raised: list[bool] = [False]

    def boom(_self):
        if raised[0]:
            return False
        raised[0] = True
        raise KeyboardInterrupt

    monkeypatch.setattr(DynamicAnalysisRunner, "_drain_completed_futures", boom)

    dashboard = FakeChatDashboard()
    chat = ChatScriptedChannel()
    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)

    result, _state, _backend, _runner = _run_chat_runner(
        tmp_path, backend, chat_channel=chat, dashboard=dashboard, config=config
    )

    assert result.success is False
    assert "interrupted" in result.failure_context.lower()
    # kill_active is called both in the except-clause AND the finally block;
    # 2+ calls confirm both paths fire and that it's safely idempotent.
    assert len(kill_calls) >= 2


def test_captain_context_files_written_each_turn(tmp_path):
    """frontier.json, mental_model.md, claims.json must be written to
    .juvenal/ before every captain turn so the captain can Read them on
    demand instead of receiving everything in the prompt."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)
    result, _state, _ = _run_runner(tmp_path, backend, config=config)

    assert result.success is True
    ctx = tmp_path / ".juvenal"
    assert (ctx / "frontier.json").exists()
    assert (ctx / "mental_model.md").exists()
    assert (ctx / "claims.json").exists()
    # frontier.json must contain the dispatched target on turn 2 (when it was queued).
    # By end of run the target is terminal so frontier is empty; mental_model still has content.
    mental = (ctx / "mental_model.md").read_text()
    assert "Captain mental model" in mental


def test_first_captain_prompt_includes_role_and_mission_subsequent_does_not(tmp_path):
    """Turn 1 routes the role + mission through the system prompt; subsequent
    turns rely on the session-inherited system prompt and ship only the
    per-turn delta plus pointers to .juvenal/ context files via stdin."""
    backend = MockBackend()
    backend.add_role_response(
        "captain",
        output=_captain_output(enqueue_targets=[_target("target-1")]),
        session_id="captain-session-x",
    )
    backend.add_role_response("worker", output=_no_findings_output("target-1-g1-attempt-1", "target-1"))
    backend.add_role_response(
        "captain",
        output=_captain_output(termination_state="complete", termination_reason="Done."),
    )

    config = AnalysisConfig(max_workers=1, max_verifiers=1, max_worker_retries=1)
    result, _state, _ = _run_runner(tmp_path, backend, config=config)

    assert result.success is True
    captain_prompts = [prompt for role, prompt in backend.role_calls if role == "captain"]
    captain_system_prompts = [system for role, system in backend.system_prompt_calls if role == "captain"]
    assert len(captain_prompts) == 2
    # Turn 1 hits run_agent (system_prompt set); turn 2 hits resume_agent (no
    # system_prompt recorded). Only one system prompt is captured here.
    assert len(captain_system_prompts) == 1

    # Turn 1: role + mission live in the system prompt, not the user message.
    assert captain_system_prompts[0] is not None
    assert "You are the captain for Juvenal's dynamic" in captain_system_prompts[0]
    assert "Mission:" in captain_system_prompts[0]
    assert "You are the captain for Juvenal's dynamic" not in captain_prompts[0]
    assert "Mission:" not in captain_prompts[0]

    # Turn 2: role + mission absent from the user message (session inherits).
    assert "You are the captain for Juvenal's dynamic" not in captain_prompts[1]
    assert "Mission:" not in captain_prompts[1]

    # Both turns reference the canonical state files.
    for prompt in captain_prompts:
        assert "frontier.json" in prompt
        assert "mental_model.md" in prompt
        assert "claims.json" in prompt


def test_per_turn_prompt_carries_event_ids_not_full_payloads(tmp_path):
    """The slimmer prompt must include claim/target IDs in the delta so the
    captain knows what changed, but should NOT inline the full claim payload
    (assertion text, primary_location, etc.) — that's what claims.json is for."""
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
    captain_prompts = [prompt for role, prompt in backend.role_calls if role == "captain"]
    # Turn 2 prompt should reference the verified claim ID (so captain knows
    # what changed) but not its full assertion / preconditions / reasoning.
    turn2 = captain_prompts[1]
    verified_claim_ids = [c.claim_id for c in state.claims.values() if c.status == "verified"]
    assert verified_claim_ids
    assert any(cid in turn2 for cid in verified_claim_ids)
    # Full claim payload bloat (one of the long fields) must not appear inline.
    assert "The expected validation branch is absent." not in turn2  # the reasoning field
    # And no inline frontier with full instructions.
    assert "Analyze target-1." not in turn2  # the instructions field


def test_rate_limit_backoff_bails_out_on_shutdown(tmp_path):
    """When kill_active fires while a background thread is sleeping in
    _rate_limit_backoff, the sleep returns within milliseconds. Without this,
    Ctrl-C leaves the captain executor thread sleeping for up to 60+ seconds
    and Python's atexit hook hangs joining it."""
    import time as _time
    from threading import Thread
    from unittest.mock import patch

    from juvenal.display import Display
    from juvenal.dynamic.runner import DynamicAnalysisRunner
    from juvenal.workflow import AnalysisConfig, Phase, Workflow

    backend = MockBackend()
    phase = Phase(id="analyze", type="analysis", prompt="x", analysis=AnalysisConfig())
    workflow = Workflow(name="x", phases=[phase], working_dir=str(tmp_path))
    state_file = tmp_path / "state.json"

    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        runner = DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode="fresh",
            display=Display(plain=True),
            interactive=False,
        )

    # Force a backoff that would otherwise sleep 60s.
    runner._consecutive_errors = 1
    runner._backoff_count = 0

    started = _time.monotonic()
    backoff_thread = Thread(target=runner._rate_limit_backoff, daemon=True)
    backoff_thread.start()
    # Give the thread a moment to enter the wait, then signal shutdown.
    _time.sleep(0.05)
    runner.kill_active()
    backoff_thread.join(timeout=1.0)
    elapsed = _time.monotonic() - started

    assert not backoff_thread.is_alive(), "rate-limit backoff did not bail on shutdown"
    assert elapsed < 1.0, f"backoff took {elapsed:.2f}s; expected near-instant exit"


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


def _make_unstarted_runner(tmp_path, config: AnalysisConfig) -> DynamicAnalysisRunner:
    """Build a runner without invoking .run(); used for direct-method tests
    that poke at the scheduler without orchestrating a full mocked loop."""
    phase = Phase(id="analyze", type="analysis", prompt="x", analysis=config)
    workflow = Workflow(name="x", phases=[phase], working_dir=str(tmp_path))
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


def test_shared_budget_caps_combined_dispatch(tmp_path):
    """Under shared_agent_budget the available worker slots are reduced by
    in-flight verifier futures: a saturated verifier pool means workers
    cannot dispatch."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=3)
    runner = _make_unstarted_runner(tmp_path, config)
    # Pre-seed 3 sentinel verifier futures (saturate the combined budget).
    for _ in range(3):
        runner._verifier_futures[Future()] = "v"

    assert runner._available_worker_slots() == 0
    assert runner._available_verifier_slots() == 0

    # Free one verifier slot — workers can fill it.
    finished = next(iter(runner._verifier_futures))
    runner._verifier_futures.pop(finished)
    assert runner._available_worker_slots() == 1
    assert runner._available_verifier_slots() == 1


def test_shared_budget_verifier_priority(tmp_path):
    """When shared budget is on and both schedulers run in the loop,
    verifier dispatch happens first (its scheduler is invoked first), so a
    proposed claim consumes the next slot before a queued target."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=2)
    runner = _make_unstarted_runner(tmp_path, config)
    # 1 in-flight worker; 1 slot free.
    runner._worker_futures[Future()] = "w"

    # In shared mode: both schedulers see 1 slot available.
    assert runner._available_worker_slots() == 1
    assert runner._available_verifier_slots() == 1

    # If a verifier dispatch consumes that slot, worker dispatch sees 0.
    runner._verifier_futures[Future()] = "v"
    assert runner._available_worker_slots() == 0


def test_legacy_mode_pools_remain_independent(tmp_path):
    """With shared_agent_budget=False, workers and verifiers do not compete
    for the same budget — each pool has its own independent cap."""
    config = AnalysisConfig(shared_agent_budget=False, max_workers=2, max_verifiers=3)
    runner = _make_unstarted_runner(tmp_path, config)
    # Fully saturate the verifier pool — workers must still have full headroom.
    for _ in range(3):
        runner._verifier_futures[Future()] = "v"
    assert runner._available_worker_slots() == 2
    assert runner._available_verifier_slots() == 0


def test_shared_mode_reporters_do_not_consume_agent_budget(tmp_path):
    """Reporters are on a separate pool by design and must not be counted
    against the worker+verifier shared budget."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=2)
    runner = _make_unstarted_runner(tmp_path, config)
    # Saturate the agent budget with verifiers + workers.
    runner._worker_futures[Future()] = "w"
    runner._verifier_futures[Future()] = "v"
    assert runner._available_worker_slots() == 0
    # Reporter scheduler still has a separate pool; pre-seed a reporter
    # future and confirm the agent counters are unchanged.
    runner._reporter_futures[Future()] = "r"
    assert runner._available_worker_slots() == 0
    assert runner._available_verifier_slots() == 0


def test_shared_mode_default_is_on(tmp_path):
    """The default AnalysisConfig has shared_agent_budget=True so new
    workflows get the verifier-priority behavior without explicit opt-in."""
    config = AnalysisConfig()
    assert config.shared_agent_budget is True
    runner = _make_unstarted_runner(tmp_path, config)
    assert runner._max_agents == config.max_agents
    assert runner._max_worker_cap == config.max_agents
    assert runner._max_verifier_cap == config.max_agents


# ---------------------------------------------------------------------------
# System-prompt split tests
#
# Each per-role _build_*_prompt now returns (system, user). The runner
# routes ``system`` through Claude's --append-system-prompt-file flag so
# the framework role + workflow scope live in the system role rather than
# being prepended to every stdin user message. These tests pin the split.
# ---------------------------------------------------------------------------


def _system_split_target(target_id: str = "target-x") -> TargetRecord:
    return TargetRecord(
        target_id=target_id,
        title=f"Inspect {target_id}",
        kind="module-level",
        priority=80,
        status="queued",
        source="captain",
        scope_paths=["src/app.py"],
        scope_symbols=["app"],
        instructions=f"Look at {target_id}.",
        depends_on_claim_ids=[],
        spawn_reason="Test fixture.",
        generation=1,
        active_generation=1,
        active_attempt_id=None,
        deferred_until_turn=None,
        pending_verification_ids=[],
        accepted_claim_ids=[],
        rejected_claim_ids=[],
        created_at=1.0,
        updated_at=1.0,
    )


def _system_split_attempt(attempt_id: str, target_id: str) -> WorkerAttempt:
    return WorkerAttempt(
        attempt_id=attempt_id,
        target_id=target_id,
        generation=1,
        backend="claude",
        session_id=None,
        status="queued",
        started_at=None,
        completed_at=None,
    )


def _system_split_claim(claim_id: str, target_id: str) -> ClaimRecord:
    loc = CodeLocation(path="src/app.py", line=10, symbol="app", role="sink")
    return ClaimRecord(
        claim_id=claim_id,
        worker_claim_id="c1",
        target_id=target_id,
        attempt_id="attempt-x",
        generation=1,
        kind="memory-safety",
        subcategory=None,
        summary="Hypothetical OOB.",
        assertion="Buffer copy past end.",
        severity="medium",
        worker_confidence="medium",
        primary_location=loc,
        locations=[loc],
        preconditions=[],
        candidate_code_refs=[],
        related_claim_ids=[],
        audit_artifact_id="art-1",
        status="proposed",
        verification_ids=[],
        rejection_class=None,
        verified_at=None,
        rejected_at=None,
    )


def _system_split_verification(
    claim_id: str, target_id: str, *, verifier_name: str, verifier_index: int
) -> VerificationRecord:
    return VerificationRecord(
        verification_id=f"verif-{verifier_name}",
        claim_id=claim_id,
        target_id=target_id,
        generation=1,
        backend="claude",
        verifier_role="default",
        session_id=None,
        status="pending",
        disposition=None,
        reason="",
        rejection_class=None,
        raw_output="",
        started_at=None,
        completed_at=None,
        verifier_name=verifier_name,
        verifier_index=verifier_index,
    )


def test_build_captain_prompt_first_turn_routes_role_and_mission_to_system(tmp_path):
    """Turn 1 must put the captain role + mission into system_prompt;
    user_prompt carries only the per-turn delta (turn index, mode, files)."""
    config = AnalysisConfig()
    runner = _make_unstarted_runner(tmp_path, config)
    assert runner.state.captain.turn_index == 0

    system_prompt, user_prompt = runner._build_captain_prompt()

    assert "You are the captain for Juvenal's dynamic" in system_prompt
    assert "Mission:" in system_prompt
    assert "You are the captain for Juvenal's dynamic" not in user_prompt
    assert "Mission:" not in user_prompt
    assert "Captain turn: 1" in user_prompt
    assert "frontier.json" in user_prompt


def test_build_captain_prompt_subsequent_turn_returns_empty_system(tmp_path):
    """On resume turns the system prompt is inherited from the original
    run_agent call; the builder must return an empty system_prompt so the
    runner does not double-apply it."""
    config = AnalysisConfig()
    runner = _make_unstarted_runner(tmp_path, config)
    runner.state.captain.turn_index = 1

    system_prompt, user_prompt = runner._build_captain_prompt()

    assert system_prompt == ""
    assert "You are the captain for Juvenal's dynamic" not in user_prompt
    assert "Mission:" not in user_prompt
    assert "Captain turn: 2" in user_prompt


def test_build_worker_prompt_routes_role_and_workflow_scope_to_system(tmp_path):
    """The worker system prompt must contain BOTH the framework role
    (analysis-worker.md) AND the workflow's worker_prompt scope. Dynamic
    task data (task packet, repo root) stays on stdin."""
    config = AnalysisConfig(
        worker_prompt="## Workflow worker scope\nHunt memory-safety bugs only.",
    )
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-x")
    runner.state.targets[target.target_id] = target
    attempt = _system_split_attempt("attempt-x", target.target_id)

    system_prompt, user_prompt = runner._build_worker_prompt(target, attempt)

    assert "You are a scoped analysis worker for Juvenal's dynamic" in system_prompt
    assert "Hunt memory-safety bugs only." in system_prompt
    assert "Repository root:" in user_prompt
    assert "Task packet:" in user_prompt
    assert "You are a scoped analysis worker" not in user_prompt
    assert "Hunt memory-safety bugs only." not in user_prompt


def test_build_worker_prompt_omits_workflow_scope_when_unset(tmp_path):
    """When the workflow defines no worker_prompt, the system prompt is
    just the framework role — the empty workflow scope must not produce
    spurious blank lines or stray scope markers."""
    config = AnalysisConfig()
    assert config.worker_prompt == ""
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-x")
    runner.state.targets[target.target_id] = target
    attempt = _system_split_attempt("attempt-x", target.target_id)

    system_prompt, _user_prompt = runner._build_worker_prompt(target, attempt)

    assert "You are a scoped analysis worker" in system_prompt
    # No trailing scope content — system prompt ends at the framework role.
    assert system_prompt == runner._worker_role_prompt


def test_build_verifier_prompt_routes_role_and_per_spec_scope_to_system(tmp_path):
    """The verifier system prompt must include the framework verifier role
    AND the per-spec scope (rendered from yaml). Dynamic claim data stays
    on stdin."""
    from juvenal.workflow import VerifierSpec

    config = AnalysisConfig(
        verifiers=[
            VerifierSpec(name="attack-surface", backend="claude", prompt="Filter design critiques."),
            VerifierSpec(name="poc", backend="claude", prompt="Reproduce on production."),
        ],
    )
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-x")
    runner.state.targets[target.target_id] = target
    claim = _system_split_claim("claim-x", target.target_id)
    runner.state.claims[claim.claim_id] = claim
    verification = _system_split_verification(
        claim.claim_id, target.target_id, verifier_name="attack-surface", verifier_index=0
    )

    system_prompt, user_prompt = runner._build_verifier_prompt(target, claim, verification)

    assert "You are an independent verifier for Juvenal's dynamic" in system_prompt
    assert "Filter design critiques." in system_prompt
    # The other verifier's scope must NOT leak into this system prompt.
    assert "Reproduce on production." not in system_prompt
    assert "Repository root:" in user_prompt
    assert "Scrubbed claim packet:" in user_prompt
    assert "You are an independent verifier" not in user_prompt
    assert "Filter design critiques." not in user_prompt


def test_build_reporter_prompt_routes_preamble_and_workflow_scope_to_system(tmp_path):
    """The reporter system prompt holds the hardcoded preamble plus the
    yaml reporter prompt. The dynamic claim packet ships via stdin."""
    from juvenal.workflow import ReporterSpec

    config = AnalysisConfig(
        reporter=ReporterSpec(backend="claude", prompt="## Specialized reporter scope\nWrite for VRP triagers."),
    )
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-x")
    runner.state.targets[target.target_id] = target
    claim = _system_split_claim("claim-x", target.target_id)
    runner.state.claims[claim.claim_id] = claim

    system_prompt, user_prompt = runner._build_reporter_prompt(claim, target)

    assert "You are the reporter agent for Juvenal's dynamic" in system_prompt
    assert "Write for VRP triagers." in system_prompt
    # report_dir / bug_id directives belong in system (constant for this call).
    assert "Report directory" in system_prompt
    assert "Bug id:" in system_prompt
    assert "Claim packet:" in user_prompt
    assert "You are the reporter agent" not in user_prompt
    assert "Write for VRP triagers." not in user_prompt


def test_build_worker_prompt_provides_scratch_dir_and_keeps_workers_out_of_output(tmp_path):
    """Workers must receive a private scratch directory under .juvenal/scratch/
    and must NOT be told to write under output/. The reporter is the only
    agent that writes to output/."""
    config = AnalysisConfig()
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-x")
    runner.state.targets[target.target_id] = target
    attempt = _system_split_attempt("attempt-x", target.target_id)

    system_prompt, user_prompt = runner._build_worker_prompt(target, attempt)

    expected_scratch_rel = f".juvenal/scratch/{attempt.attempt_id}"
    # The user prompt advertises the scratch dir explicitly.
    assert expected_scratch_rel in user_prompt
    assert "Scratch directory" in user_prompt
    # Task packet carries scratch_dir.
    assert f'"scratch_dir": "{expected_scratch_rel}"' in user_prompt
    # Worker must not be steered toward output/.
    assert "output/<bug-id>" not in system_prompt
    assert "output/<bug-id>" not in user_prompt
    # Pre-creation: the scratch dir must exist after the prompt is built so
    # the worker can write into it without first mkdir'ing.
    assert (tmp_path / ".juvenal" / "scratch" / attempt.attempt_id).is_dir()


def test_build_reporter_prompt_references_scratch_dir_for_artifact_copy(tmp_path):
    """The reporter must be told both the report dir AND the worker scratch
    dir, so it can copy any PoC artifacts the worker dropped during the
    investigation."""
    from juvenal.workflow import ReporterSpec

    config = AnalysisConfig(reporter=ReporterSpec(backend="claude", prompt=""))
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-x")
    runner.state.targets[target.target_id] = target
    claim = _system_split_claim("claim-x", target.target_id)
    runner.state.claims[claim.claim_id] = claim
    attempt = _system_split_attempt(claim.attempt_id, target.target_id)
    runner.state.worker_attempts[attempt.attempt_id] = attempt

    system_prompt, _user_prompt = runner._build_reporter_prompt(claim, target)

    expected_scratch_rel = f".juvenal/scratch/{attempt.attempt_id}"
    assert "Worker scratch directory" in system_prompt
    assert expected_scratch_rel in system_prompt
    # The reporter's instructions explicitly tell it to copy from scratch.
    assert "Copy" in system_prompt or "copy" in system_prompt
    # And not to write outside the report dir.
    assert "Do NOT write outside" in system_prompt


def test_reconcile_orphaned_running_attempt_frees_slot(tmp_path):
    """A worker attempt persisted as running with NO tracking future is an
    orphan — its slot leaks from the budget pool until the run is restarted.
    `_reconcile_orphaned_running_state` must detect that mismatch and revert
    the attempt + target to a re-schedulable shape."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4)
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-orphan")
    target.status = "running"
    target.active_attempt_id = "attempt-orphan"
    runner.state.targets[target.target_id] = target
    attempt = _system_split_attempt("attempt-orphan", target.target_id)
    attempt.status = "running"
    runner.state.worker_attempts[attempt.attempt_id] = attempt
    # Critically: NO entry in runner._worker_futures for this attempt.

    progressed = runner._reconcile_orphaned_running_state()

    assert progressed is True
    assert runner.state.worker_attempts["attempt-orphan"].status == "failed"
    # Target was reset to a re-schedulable state.
    assert runner.state.targets[target.target_id].active_attempt_id is None
    assert runner.state.targets[target.target_id].status in {"queued", "blocked"}


def test_reconcile_orphaned_running_attempt_skips_live_future(tmp_path):
    """Don't touch an attempt that DOES have a tracking future — that's the
    normal in-flight case, not an orphan."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4)
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-live")
    target.status = "running"
    target.active_attempt_id = "attempt-live"
    runner.state.targets[target.target_id] = target
    attempt = _system_split_attempt("attempt-live", target.target_id)
    attempt.status = "running"
    runner.state.worker_attempts[attempt.attempt_id] = attempt
    # Pretend a tracking future exists.
    runner._worker_futures[Future()] = "attempt-live"

    progressed = runner._reconcile_orphaned_running_state()

    assert progressed is False
    assert runner.state.worker_attempts["attempt-live"].status == "running"
    assert runner.state.targets[target.target_id].active_attempt_id == "attempt-live"


def test_trust_model_verifier_prefers_subagent_body_when_flag_set(tmp_path):
    """A verifier with `use_attack_surface_subagent: true` must replace its
    YAML prompt with the body of `.claude/agents/attack-surface.md` and skip
    the standard project-brief block (since the brief is embedded inside the
    subagent body)."""
    from juvenal.workflow import VerifierSpec

    config = AnalysisConfig(
        verifiers=[
            VerifierSpec(
                name="trust-model",
                backend="claude",
                prompt="YAML-FALLBACK-SCOPE",
                use_attack_surface_subagent=True,
            ),
        ],
    )
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-x")
    runner.state.targets[target.target_id] = target
    claim = _system_split_claim("claim-x", target.target_id)
    runner.state.claims[claim.claim_id] = claim
    verification = _system_split_verification(
        claim.claim_id, target.target_id, verifier_name="trust-model", verifier_index=0
    )

    # Materialize a subagent file so the runner finds it.
    agents_dir = tmp_path / ".claude" / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    (agents_dir / "attack-surface.md").write_text(
        "---\nname: attack-surface\ntools: Read, Grep\n---\n\nSUBAGENT-BODY-CONTENT\n",
        encoding="utf-8",
    )

    system_prompt, _user_prompt = runner._build_verifier_prompt(target, claim, verification)

    assert "SUBAGENT-BODY-CONTENT" in system_prompt
    # YAML scope must NOT be used when the subagent body is loaded.
    assert "YAML-FALLBACK-SCOPE" not in system_prompt
    # Frontmatter must be stripped — `name: attack-surface` lives in the
    # YAML header and should not appear in the rendered prompt.
    assert "name: attack-surface" not in system_prompt
    # And the verifier-mode framing is added.
    assert "verifier mode" in system_prompt


def test_trust_model_verifier_falls_back_to_yaml_when_subagent_missing(tmp_path):
    """When `use_attack_surface_subagent: true` but the subagent file does
    not exist (analyst failed / hasn't run), the runner falls back to the
    YAML scope so the verifier still has actionable guidance."""
    from juvenal.workflow import VerifierSpec

    config = AnalysisConfig(
        verifiers=[
            VerifierSpec(
                name="trust-model",
                backend="claude",
                prompt="YAML-FALLBACK-SCOPE",
                use_attack_surface_subagent=True,
            ),
        ],
    )
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-x")
    runner.state.targets[target.target_id] = target
    claim = _system_split_claim("claim-x", target.target_id)
    runner.state.claims[claim.claim_id] = claim
    verification = _system_split_verification(
        claim.claim_id, target.target_id, verifier_name="trust-model", verifier_index=0
    )

    # No subagent file written.
    assert not (tmp_path / ".claude" / "agents" / "attack-surface.md").exists()

    system_prompt, _user_prompt = runner._build_verifier_prompt(target, claim, verification)

    assert "YAML-FALLBACK-SCOPE" in system_prompt


def test_other_verifiers_ignore_subagent_body_even_when_file_present(tmp_path):
    """A verifier WITHOUT `use_attack_surface_subagent` must keep using its
    YAML scope, even if the subagent file exists. The flag is opt-in
    per-verifier."""
    from juvenal.workflow import VerifierSpec

    config = AnalysisConfig(
        verifiers=[
            VerifierSpec(
                name="attack-surface",
                backend="claude",
                prompt="YAML-ATTACK-SURFACE-SCOPE",
                use_attack_surface_subagent=False,
            ),
        ],
    )
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-x")
    runner.state.targets[target.target_id] = target
    claim = _system_split_claim("claim-x", target.target_id)
    runner.state.claims[claim.claim_id] = claim
    verification = _system_split_verification(
        claim.claim_id, target.target_id, verifier_name="attack-surface", verifier_index=0
    )

    agents_dir = tmp_path / ".claude" / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    (agents_dir / "attack-surface.md").write_text(
        "---\nname: attack-surface\n---\n\nSUBAGENT-BODY-CONTENT\n", encoding="utf-8"
    )

    system_prompt, _user_prompt = runner._build_verifier_prompt(target, claim, verification)

    assert "YAML-ATTACK-SURFACE-SCOPE" in system_prompt
    assert "SUBAGENT-BODY-CONTENT" not in system_prompt


def test_reconcile_orphaned_running_verification_reverts_to_pending(tmp_path):
    """A verification persisted as running with NO tracking future must be
    reverted to pending so the next scheduling tick picks it up."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4)
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-v")
    runner.state.targets[target.target_id] = target
    claim = _system_split_claim("claim-v", target.target_id)
    runner.state.claims[claim.claim_id] = claim
    verification = _system_split_verification(
        claim.claim_id, target.target_id, verifier_name="attack-surface", verifier_index=0
    )
    verification.status = "running"
    verification.session_id = "stale-session-uuid"
    verification.started_at = 1.0
    runner.state.verifications[verification.verification_id] = verification
    # No tracking future.

    progressed = runner._reconcile_orphaned_running_state()

    assert progressed is True
    reverted = runner.state.verifications[verification.verification_id]
    assert reverted.status == "pending"
    assert reverted.started_at is None
    assert reverted.completed_at is None
    # Session id preserved so the resumed verifier inherits the prior context.
    assert reverted.parent_session_id == "stale-session-uuid"


def test_concurrent_retries_for_same_target_serialize(tmp_path):
    """Two pending claim retries belonging to the same target must NOT
    dispatch concurrently — the second `_start_claim_retry_attempt` would
    overwrite the first's `target.active_attempt_id`, and on completion both
    workers would hit the mismatch guard in `_apply_worker_result` and have
    their reports silently discarded, leaving the target wedged at
    `status="running"` with `active_attempt_id=None`. Production regression
    observed in the openthread bug-bounty run: targets with two sibling
    rejected claims accumulated 12 completed retry attempts whose reports
    never updated state, while the target stayed stuck in the frontier."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4)
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-share")
    runner.state.targets[target.target_id] = target
    claim_a = _system_split_claim("claim-a", target.target_id)
    claim_a.status = "rejected"
    claim_b = _system_split_claim("claim-b", target.target_id)
    claim_b.status = "rejected"
    claim_b.worker_claim_id = "c2"
    runner.state.claims[claim_a.claim_id] = claim_a
    runner.state.claims[claim_b.claim_id] = claim_b
    runner._pending_claim_retries = [
        (target.target_id, claim_a.claim_id),
        (target.target_id, claim_b.claim_id),
    ]

    dispatched: list[str] = []

    def _fake_start(t, c):
        attempt_id = f"{t.target_id}-retry-{c.claim_id}-{len(dispatched) + 1}"
        attempt = _system_split_attempt(attempt_id, t.target_id)
        attempt.status = "running"
        attempt.retry_claim_id = c.claim_id
        runner.state.worker_attempts[attempt_id] = attempt
        t.status = "running"
        t.active_attempt_id = attempt_id
        dispatched.append(attempt_id)
        return attempt

    with (
        patch.object(runner, "_start_claim_retry_attempt", side_effect=_fake_start),
        patch.object(runner, "_build_claim_retry_prompt", return_value=("", "")),
        patch.object(runner._worker_executor, "submit", return_value=Future()),
    ):
        runner._schedule_workers()

    # Only one retry dispatched on this tick — the sibling stays queued.
    assert len(dispatched) == 1
    assert runner._pending_claim_retries == [(target.target_id, claim_b.claim_id)]
    assert runner.state.targets[target.target_id].active_attempt_id == dispatched[0]


def test_apply_worker_result_mismatch_does_not_clobber_active_attempt(tmp_path):
    """When a stale worker attempt completes after the target has moved on
    to a different active attempt, the mismatch path must NOT clear
    `target.active_attempt_id` — that pointer belongs to the live successor.
    Regression for the wedge: clobbering active_attempt_id here orphans the
    successor (its own completion will then mismatch and the target stays
    at status="running" with no live attempt forever)."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4)
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-mismatch")
    target.status = "running"
    target.active_attempt_id = "attempt-live"
    runner.state.targets[target.target_id] = target

    stale = _system_split_attempt("attempt-stale", target.target_id)
    stale.status = "running"
    runner.state.worker_attempts["attempt-stale"] = stale
    live = _system_split_attempt("attempt-live", target.target_id)
    live.status = "running"
    runner.state.worker_attempts["attempt-live"] = live

    from juvenal.backends import AgentResult
    from juvenal.dynamic.models import WorkerReport
    from juvenal.dynamic.runner import _WorkerExecutionResult

    result = _WorkerExecutionResult(
        attempt_id="attempt-stale",
        target_id=target.target_id,
        generation=stale.generation,
        report=WorkerReport(
            schema_version=1,
            task_id="attempt-stale",
            target_id=target.target_id,
            outcome="no_findings",
            summary="No issue.",
            claims=[],
            blocker=None,
            follow_up_hints=[],
        ),
        agent_result=AgentResult(
            exit_code=0, output="", transcript="", duration=0.0, input_tokens=0, output_tokens=0, session_id="s"
        ),
        error="",
    )

    runner._apply_worker_result(result)

    # The live attempt's pointer must be preserved.
    assert runner.state.targets[target.target_id].active_attempt_id == "attempt-live"
    # The stale attempt itself was marked completed.
    assert runner.state.worker_attempts["attempt-stale"].status == "completed"


def test_reconcile_orphaned_running_target_with_no_attempt(tmp_path):
    """A target stuck at `status="running"` with `active_attempt_id=None`
    (or pointing at a non-running attempt) is invisible to both the
    scheduler (which only picks `status=="queued"`) and the existing
    orphan-attempt loop (which only walks `worker_attempts`). The target
    reconciler must reset it to a re-schedulable shape so the work resumes
    instead of leaking the slot indefinitely."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4, max_worker_retries=2)
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-wedged")
    target.status = "running"
    target.active_attempt_id = None
    runner.state.targets[target.target_id] = target

    progressed = runner._reconcile_orphaned_running_state()

    assert progressed is True
    recovered = runner.state.targets[target.target_id]
    assert recovered.status == "queued"
    assert recovered.active_attempt_id is None
    assert recovered.error_retry_count == 1


def test_reconcile_orphaned_running_target_blocks_after_budget_exhausted(tmp_path):
    """If the wedged-running target has already burned its retry budget,
    the reconciler must block it rather than re-queue — otherwise the same
    failure mode would recycle indefinitely."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4, max_worker_retries=1)
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-burned")
    target.status = "running"
    target.active_attempt_id = None
    target.error_retry_count = 1  # next bump pushes past max
    runner.state.targets[target.target_id] = target

    progressed = runner._reconcile_orphaned_running_state()

    assert progressed is True
    recovered = runner.state.targets[target.target_id]
    assert recovered.status == "blocked"
    assert recovered.active_attempt_id is None


def test_claim_retry_no_findings_burns_full_budget_and_exhausts(tmp_path):
    """A retry worker returning `no_findings` confirms the rejection — it's
    a kill, not a budget tick. The runner consumes the full remaining
    budget so the claim immediately exhausts and the target rolls up to
    `exhausted` via _refresh_target_after_verification. Re-running the
    same investigation N more times just to re-confirm the worker's
    confirmation wastes tokens."""
    from juvenal.backends import AgentResult
    from juvenal.dynamic.models import WorkerReport
    from juvenal.dynamic.runner import _WorkerExecutionResult

    config = AnalysisConfig(shared_agent_budget=True, max_agents=4, max_worker_retries=10)
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-kill")
    target.status = "running"
    runner.state.targets[target.target_id] = target

    claim = _system_split_claim("claim-kill", target.target_id)
    claim.status = "rejected"
    claim.retry_count = 5  # 5 budget remaining
    runner.state.claims[claim.claim_id] = claim

    attempt = _system_split_attempt("attempt-kill", target.target_id)
    attempt.status = "running"
    attempt.retry_claim_id = claim.claim_id
    target.active_attempt_id = attempt.attempt_id
    runner.state.worker_attempts[attempt.attempt_id] = attempt

    result = _WorkerExecutionResult(
        attempt_id=attempt.attempt_id,
        target_id=target.target_id,
        generation=attempt.generation,
        report=WorkerReport(
            schema_version=1,
            task_id=attempt.attempt_id,
            target_id=target.target_id,
            outcome="no_findings",
            summary="Worker confirms no evidence.",
            claims=[],
            blocker=None,
            follow_up_hints=[],
        ),
        agent_result=AgentResult(
            exit_code=0, output="", transcript="", duration=0.0, input_tokens=0, output_tokens=0, session_id="s"
        ),
        error="",
    )

    runner._apply_worker_result(result)

    # Full budget burned — claim is at max immediately, no further retries.
    assert runner.state.claims[claim.claim_id].retry_count == 10
    assert runner._pending_claim_retries == []
    # Target rolls up to exhausted on this same call.
    assert runner.state.targets[target.target_id].status == "exhausted"


def test_claim_retry_blocked_also_kills(tmp_path):
    """`blocked` retry result is treated identically to `no_findings`:
    worker can't continue → rejection stands → kill. Same exhaust semantics."""
    from juvenal.backends import AgentResult
    from juvenal.dynamic.models import WorkerReport
    from juvenal.dynamic.runner import _WorkerExecutionResult

    config = AnalysisConfig(shared_agent_budget=True, max_agents=4, max_worker_retries=10)
    runner = _make_unstarted_runner(tmp_path, config)
    target = _system_split_target("target-blk")
    target.status = "running"
    runner.state.targets[target.target_id] = target

    claim = _system_split_claim("claim-blk", target.target_id)
    claim.status = "rejected"
    claim.retry_count = 2
    runner.state.claims[claim.claim_id] = claim

    attempt = _system_split_attempt("attempt-blk", target.target_id)
    attempt.status = "running"
    attempt.retry_claim_id = claim.claim_id
    target.active_attempt_id = attempt.attempt_id
    runner.state.worker_attempts[attempt.attempt_id] = attempt

    result = _WorkerExecutionResult(
        attempt_id=attempt.attempt_id,
        target_id=target.target_id,
        generation=attempt.generation,
        report=WorkerReport(
            schema_version=1,
            task_id=attempt.attempt_id,
            target_id=target.target_id,
            outcome="blocked",
            summary="Cannot proceed.",
            claims=[],
            blocker="missing context",
            follow_up_hints=[],
        ),
        agent_result=AgentResult(
            exit_code=0, output="", transcript="", duration=0.0, input_tokens=0, output_tokens=0, session_id="s"
        ),
        error="",
    )

    runner._apply_worker_result(result)

    assert runner.state.claims[claim.claim_id].retry_count == 10
    assert runner._pending_claim_retries == []
    assert runner.state.targets[target.target_id].status == "exhausted"
    # And the target rolls up to exhausted via _refresh_target_after_verification.
    assert runner.state.targets[target.target_id].status == "exhausted"


def test_sweep_dead_dep_targets_blocks_when_dep_exhausted(tmp_path):
    """A queued target whose dep claim is rejected with no retry budget AND
    no live retry chain is unsatisfiable — the sweep must mark it blocked
    so the frontier doesn't carry permanent garbage. Production regression:
    in the openthread run, 55 captain-enqueued PoC/build targets sat queued
    forever waiting for parent claims that exhausted their retry budget
    without ever reaching `verified`."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4, max_worker_retries=2)
    runner = _make_unstarted_runner(tmp_path, config)

    parent = _system_split_target("target-parent")
    parent.status = "exhausted"
    runner.state.targets[parent.target_id] = parent
    parent_claim = _system_split_claim("claim-parent", parent.target_id)
    parent_claim.status = "rejected"
    parent_claim.retry_count = 2  # budget == max
    runner.state.claims[parent_claim.claim_id] = parent_claim

    dependent = _system_split_target("target-dependent")
    dependent.status = "queued"
    dependent.depends_on_claim_ids = [parent_claim.claim_id]
    runner.state.targets[dependent.target_id] = dependent

    progressed = runner._sweep_dead_dep_targets()

    assert progressed is True
    blocked = runner.state.targets[dependent.target_id]
    assert blocked.status == "blocked"
    # Event recorded so the captain sees the blocker on the next delta.
    blocker_events = [
        e for e in runner.state.events if e.event_type == "target.blocked" and e.target_id == "target-dependent"
    ]
    assert len(blocker_events) == 1
    assert "claim-parent" in blocker_events[0].payload.get("blocker", "")


def test_sweep_dead_dep_targets_leaves_alive_dep_alone(tmp_path):
    """If the dep claim is rejected but still has retry budget OR is
    actively being retried, the dep is NOT yet unverifiable — the
    dependent target stays queued so the next retry can resolve it."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4, max_worker_retries=10)
    runner = _make_unstarted_runner(tmp_path, config)

    parent = _system_split_target("target-parent")
    runner.state.targets[parent.target_id] = parent
    parent_claim = _system_split_claim("claim-parent", parent.target_id)
    parent_claim.status = "rejected"
    parent_claim.retry_count = 5  # well below max=10
    runner.state.claims[parent_claim.claim_id] = parent_claim

    dependent = _system_split_target("target-dependent")
    dependent.status = "queued"
    dependent.depends_on_claim_ids = [parent_claim.claim_id]
    runner.state.targets[dependent.target_id] = dependent

    progressed = runner._sweep_dead_dep_targets()

    assert progressed is False
    assert runner.state.targets[dependent.target_id].status == "queued"


def test_sweep_dead_dep_targets_walks_retry_chain(tmp_path):
    """A dep is satisfied (alive) if any descendant in its retry chain is
    still verifiable. The sweep must walk `retry_claim_ids` before deciding
    the dep is dead, mirroring the satisfaction walk in
    `_dependencies_satisfied`."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4, max_worker_retries=2)
    runner = _make_unstarted_runner(tmp_path, config)

    parent = _system_split_target("target-parent")
    runner.state.targets[parent.target_id] = parent

    # Original dep claim: rejected, exhausted budget — would be dead alone.
    original = _system_split_claim("claim-original", parent.target_id)
    original.status = "rejected"
    original.retry_count = 2  # at max
    # ...but a retry claim is still proposed and pending verification.
    original.retry_claim_ids = ["claim-retry"]
    runner.state.claims[original.claim_id] = original

    retry = _system_split_claim("claim-retry", parent.target_id)
    retry.worker_claim_id = "c-retry"
    retry.status = "proposed"
    retry.retry_of_claim_id = original.claim_id
    runner.state.claims[retry.claim_id] = retry

    dependent = _system_split_target("target-dependent")
    dependent.status = "queued"
    dependent.depends_on_claim_ids = [original.claim_id]
    runner.state.targets[dependent.target_id] = dependent

    progressed = runner._sweep_dead_dep_targets()

    assert progressed is False
    assert runner.state.targets[dependent.target_id].status == "queued"


def test_normalize_captain_targets_emits_drop_event_on_id_collision(tmp_path):
    """When the captain proposes a target_id that already exists (likely a
    terminal-status target the captain can't see in frontier.json), the
    proposal is silently filtered. The runner must emit a
    `captain.proposal_dropped` event so (a) the user sees it in the
    interactive dashboard and (b) the captain sees it on its next delta
    and self-corrects. Production regression: in the openthread run, the
    captain proposed batches of 12 targets that all evaporated because
    their ids collided with old terminal targets — captain had no
    feedback and just kept retrying the same pattern."""
    from juvenal.dynamic.models import CaptainTurn, TargetProposal

    config = AnalysisConfig(shared_agent_budget=True, max_agents=4)
    runner = _make_unstarted_runner(tmp_path, config)

    existing = _system_split_target("target-collide")
    existing.status = "no_findings"
    runner.state.targets[existing.target_id] = existing

    src_path = tmp_path / "src"
    src_path.mkdir()
    (src_path / "app.py").write_text("# stub\n", encoding="utf-8")

    turn = CaptainTurn(
        message_to_user="",
        acknowledged_directive_ids=[],
        mental_model_summary="",
        open_questions=[],
        enqueue_targets=[
            TargetProposal(
                target_id="target-collide",
                title="Collides with the existing terminal target id",
                kind="module-level",
                priority=80,
                scope_paths=["src/app.py"],
                scope_symbols=[],
                instructions="Doesn't matter; will be dropped.",
                depends_on_claim_ids=[],
                spawn_reason="Test fixture.",
            ),
        ],
        defer_target_ids=[],
        termination_state="continue",
        termination_reason="",
    )

    normalized = runner._normalize_captain_targets(turn)

    assert normalized == []
    drop_events = [e for e in runner.state.events if e.event_type == "captain.proposal_dropped"]
    assert len(drop_events) == 1
    assert drop_events[0].target_id == "target-collide"
    assert "already-exists" in (drop_events[0].payload or {}).get("reason", "")


def test_pending_captain_delta_includes_dropped_proposals(tmp_path):
    """The CaptainDelta surfaces dropped proposals so the captain sees them
    on its next turn and can fix the cause."""
    from juvenal.dynamic.models import CaptainTurn, TargetProposal

    config = AnalysisConfig(shared_agent_budget=True, max_agents=4)
    runner = _make_unstarted_runner(tmp_path, config)

    existing = _system_split_target("target-already")
    existing.status = "blocked"
    runner.state.targets[existing.target_id] = existing
    src_path = tmp_path / "src"
    src_path.mkdir()
    (src_path / "app.py").write_text("# stub\n", encoding="utf-8")

    turn = CaptainTurn(
        message_to_user="",
        acknowledged_directive_ids=[],
        mental_model_summary="",
        open_questions=[],
        enqueue_targets=[
            TargetProposal(
                target_id="target-already",
                title="Collides",
                kind="module-level",
                priority=80,
                scope_paths=["src/app.py"],
                scope_symbols=[],
                instructions="Will be dropped.",
                depends_on_claim_ids=[],
                spawn_reason="Test fixture.",
            ),
        ],
        defer_target_ids=[],
        termination_state="continue",
        termination_reason="",
    )

    runner._normalize_captain_targets(turn)
    delta = runner.state.pending_captain_delta()

    assert any(d["target_id"] == "target-already" for d in delta.dropped_proposals)


def test_should_terminate_succeeds_when_some_exhausted_but_claims_verified(tmp_path):
    """All-terminal frontier with at least one verified claim is a SUCCESS,
    even if some targets exhausted along the way. Production regression: in
    the openthread run, 23 completed targets + 28 verified claims + 11
    exhausted targets exited with `analysis exhausted retry budget across
    all targets` because the failure check tripped on the exhausted count
    before the all-terminal-success check could fire."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4)
    runner = _make_unstarted_runner(tmp_path, config)
    # Bump captain past turn 0 so the secondary all-terminal check is reachable.
    runner.state.captain.turn_index = 5
    runner._last_captain_snapshot = runner._captain_snapshot()

    completed = _system_split_target("target-completed")
    completed.status = "completed"
    runner.state.targets[completed.target_id] = completed
    verified_claim = _system_split_claim("claim-verified", completed.target_id)
    verified_claim.status = "verified"
    runner.state.claims[verified_claim.claim_id] = verified_claim

    exhausted = _system_split_target("target-exhausted")
    exhausted.status = "exhausted"
    runner.state.targets[exhausted.target_id] = exhausted

    # Captain has consumed everything (no pending delta).
    runner.state.captain.last_delivered_event_seq = max((e.seq for e in runner.state.events), default=0)

    terminate, success, reason = runner._should_terminate()

    assert terminate is True
    assert success is True
    assert reason == ""


def test_should_terminate_fails_when_no_verified_claims(tmp_path):
    """A run where every target hits terminal AND no claim was ever
    verified is a genuine failure — the analysis produced nothing."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4)
    runner = _make_unstarted_runner(tmp_path, config)
    runner.state.captain.turn_index = 5
    runner._last_captain_snapshot = runner._captain_snapshot()

    exhausted = _system_split_target("target-exhausted")
    exhausted.status = "exhausted"
    runner.state.targets[exhausted.target_id] = exhausted
    rejected_claim = _system_split_claim("claim-rejected", exhausted.target_id)
    rejected_claim.status = "rejected"
    rejected_claim.retry_count = 10
    runner.state.claims[rejected_claim.claim_id] = rejected_claim

    runner.state.captain.last_delivered_event_seq = max((e.seq for e in runner.state.events), default=0)

    terminate, success, reason = runner._should_terminate()

    assert terminate is True
    assert success is False
    assert "exhausted retry budget" in reason


def test_sweep_dead_dep_targets_treats_terminal_target_claim_as_dead(tmp_path):
    """A rejected dep claim with retry budget remaining is functionally dead
    if its target is in a terminal status (blocked/exhausted/no_findings/
    completed) — `_rebuild_pending_claim_retries` excludes terminal-target
    claims from the retry queue, so the budget is unreachable. The sweep
    must treat such claims as dead so dependents can be blocked instead of
    deadlocking the queue. Production regression: in the openthread run,
    27 queued targets sat with 0 workers in flight because their dep
    claims showed retry_count=4/10 (looked alive) but the claims' targets
    were already blocked (so no retry would ever fire)."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4, max_worker_retries=10)
    runner = _make_unstarted_runner(tmp_path, config)

    parent = _system_split_target("target-parent")
    parent.status = "blocked"  # terminal — its claims are stranded
    runner.state.targets[parent.target_id] = parent
    parent_claim = _system_split_claim("claim-parent", parent.target_id)
    parent_claim.status = "rejected"
    parent_claim.retry_count = 4  # well below max=10, looks alive
    runner.state.claims[parent_claim.claim_id] = parent_claim

    dependent = _system_split_target("target-dependent")
    dependent.status = "queued"
    dependent.depends_on_claim_ids = [parent_claim.claim_id]
    runner.state.targets[dependent.target_id] = dependent

    progressed = runner._sweep_dead_dep_targets()

    assert progressed is True
    assert runner.state.targets[dependent.target_id].status == "blocked"


def test_sweep_dead_dep_targets_respects_pending_retry_queue(tmp_path):
    """If the dep claim is exhausted on disk but is sitting in the runtime
    retry queue (e.g. just re-queued by a no_findings retry result before
    the next dispatch tick), the sweep must NOT block — there's still a
    pending attempt that may produce a verified retry claim."""
    config = AnalysisConfig(shared_agent_budget=True, max_agents=4, max_worker_retries=2)
    runner = _make_unstarted_runner(tmp_path, config)

    parent = _system_split_target("target-parent")
    runner.state.targets[parent.target_id] = parent
    parent_claim = _system_split_claim("claim-parent", parent.target_id)
    parent_claim.status = "rejected"
    parent_claim.retry_count = 2  # at max
    runner.state.claims[parent_claim.claim_id] = parent_claim

    dependent = _system_split_target("target-dependent")
    dependent.status = "queued"
    dependent.depends_on_claim_ids = [parent_claim.claim_id]
    runner.state.targets[dependent.target_id] = dependent

    # Sitting in the runtime queue, hasn't dispatched yet.
    runner._pending_claim_retries = [(parent.target_id, parent_claim.claim_id)]

    progressed = runner._sweep_dead_dep_targets()

    assert progressed is False
    assert runner.state.targets[dependent.target_id].status == "queued"


def _bare_runner(tmp_path, backend=None):
    """Construct a runner without running it, for direct method-level assertions."""
    backend = backend or MockBackend()
    phase = Phase(id="analyze", type="analysis", prompt="x", analysis=AnalysisConfig())
    workflow = Workflow(name="x", phases=[phase], working_dir=str(tmp_path))
    state_file = tmp_path / "state.json"
    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        return DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode="fresh",
            display=Display(plain=True),
            interactive=False,
        )


class TestHooksForRole:
    def test_worker_denies_output_writes(self, tmp_path):
        runner = _bare_runner(tmp_path)
        cfg = runner._hooks_for_role("worker")
        deny = cfg["permissions"]["deny"]
        output_dir = runner.working_dir / "output"
        assert f"Write(//{output_dir}/**)" in deny
        assert f"Edit(//{output_dir}/**)" in deny

    def test_verifier_denies_output_writes(self, tmp_path):
        runner = _bare_runner(tmp_path)
        cfg = runner._hooks_for_role("verifier")
        deny = cfg["permissions"]["deny"]
        output_dir = runner.working_dir / "output"
        assert f"Write(//{output_dir}/**)" in deny
        # Verifier must remain able to build/run a PoC in-tree, so source writes
        # are NOT denied — only the reporter-owned output/ tree is off-limits.
        assert "Write" not in deny
        assert "Edit" not in deny

    def test_reporter_denies_scratch_writes_but_not_report_dir(self, tmp_path):
        runner = _bare_runner(tmp_path)
        scratch = tmp_path / ".juvenal" / "scratch" / "task-1"
        cfg = runner._hooks_for_role("reporter", scratch_dir=scratch)
        deny = cfg["permissions"]["deny"]
        assert f"Write(//{scratch}/**)" in deny
        assert f"Edit(//{scratch}/**)" in deny
        # The report dir itself is never denied.
        report_dir = runner.working_dir / "output"
        assert not any(str(report_dir) in rule and "scratch" not in rule for rule in deny)

    def test_reporter_without_scratch_is_unrestricted(self, tmp_path):
        runner = _bare_runner(tmp_path)
        assert runner._hooks_for_role("reporter", scratch_dir=None) is None

    def test_captain_and_analyst_unrestricted(self, tmp_path):
        runner = _bare_runner(tmp_path)
        assert runner._hooks_for_role("captain") is None
        assert runner._hooks_for_role("analyst") is None


def test_worker_dispatch_passes_hooks_config_to_backend(tmp_path):
    """The runner threads the worker guardrail through to the backend call."""
    backend = MockBackend()
    runner = _bare_runner(tmp_path, backend=backend)
    # _get_backend caches per name; seed it so the dispatch call routes to the mock.
    runner._backend_by_name[runner.config.worker_backend] = backend
    attempt = WorkerAttempt(
        attempt_id="task-1",
        target_id="t1",
        generation=0,
        backend="claude",
        session_id="sess-1",
        status="queued",
        started_at=None,
        completed_at=None,
    )
    runner._execute_worker_attempt(attempt, "prompt body", system_prompt="sys")
    worker_calls = [(role, cfg) for role, cfg in backend.hooks_config_calls if cfg is not None]
    assert worker_calls, "worker call did not carry a hooks_config"
    _, cfg = worker_calls[-1]
    output_dir = runner.working_dir / "output"
    assert f"Write(//{output_dir}/**)" in cfg["permissions"]["deny"]
