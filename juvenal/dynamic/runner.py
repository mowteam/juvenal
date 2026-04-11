"""Dynamic analysis runner for captain/worker/verifier orchestration."""

from __future__ import annotations

import json
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Literal

from juvenal.backends import AgentResult, Backend, create_backend
from juvenal.checkers import VerificationReport, parse_verification_report
from juvenal.display import Display
from juvenal.dynamic.models import (
    CaptainTurn,
    ClaimRecord,
    TargetRecord,
    VerificationRecord,
    WorkerAttempt,
    WorkerClaimArtifact,
    WorkerReport,
)
from juvenal.dynamic.protocol import (
    claim_to_verifier_packet,
    parse_captain_output,
    parse_worker_output,
    validate_target_scope,
)
from juvenal.dynamic.state import DynamicSessionState
from juvenal.execution import PhaseResult
from juvenal.workflow import AnalysisConfig, Phase, Workflow

_CAPTAIN_EVENT_TYPES = frozenset(
    {
        "claim.verified",
        "claim.rejected",
        "target.no_findings",
        "target.blocked",
        "target.exhausted",
        "directive.received",
    }
)
_NON_TERMINAL_STATUSES = frozenset({"queued", "running", "verifying", "deferred", "requeue_pending"})
_RUNNING_STATUSES = frozenset({"running", "verifying"})
_IDLE_SLEEP_SECONDS = 0.05


@dataclass
class _WorkerExecutionResult:
    attempt_id: str
    target_id: str
    generation: int
    agent_result: AgentResult
    report: WorkerReport | None
    error: str | None


@dataclass
class _VerifierExecutionResult:
    verification_id: str
    claim_id: str
    target_id: str
    generation: int
    agent_result: AgentResult
    report: VerificationReport | None
    error: str | None


class DynamicAnalysisRunner:
    """Deterministic runner for one dynamic analysis phase."""

    def __init__(
        self,
        *,
        phase: Phase,
        workflow: Workflow,
        state_file: Path,
        run_mode: Literal["fresh", "resume", "reset"],
        display: Display,
        interactive: bool,
        failure_context: str = "",
    ) -> None:
        self.phase = phase
        self.workflow = workflow
        self.state_file = Path(state_file)
        self.run_mode = run_mode
        self.display = display
        self.interactive = interactive
        self.failure_context = failure_context
        self.config = phase.analysis or AnalysisConfig()
        self.working_dir = Path(workflow.working_dir).resolve()

        self.state = (
            DynamicSessionState.load(self.state_file) if run_mode == "resume" else DynamicSessionState(self.state_file)
        )
        self._backend_by_name: dict[str, Backend] = {}
        self._backend_lock = Lock()
        self._worker_executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        self._verifier_executor = ThreadPoolExecutor(max_workers=self.config.max_verifiers)
        self._worker_futures: dict[Future[_WorkerExecutionResult], str] = {}
        self._verifier_futures: dict[Future[_VerifierExecutionResult], str] = {}
        self._captain_termination_state: Literal["continue", "complete"] = "continue"
        self._captain_termination_reason = ""
        self._last_captain_snapshot: tuple[Any, ...] | None = None
        self._terminal_failure = ""
        self.total_input_tokens = 0
        self.total_output_tokens = 0

        prompts_dir = Path(__file__).resolve().parent.parent / "prompts"
        self._captain_role_prompt = (prompts_dir / "captain-analysis.md").read_text(encoding="utf-8")
        self._worker_role_prompt = (prompts_dir / "analysis-worker.md").read_text(encoding="utf-8")
        self._verifier_role_prompt = (prompts_dir / "analysis-verifier.md").read_text(encoding="utf-8")

    def run(self) -> PhaseResult:
        """Run the dynamic analysis loop to completion or deterministic failure."""

        try:
            if self.run_mode == "resume":
                self.state.normalize_for_resume()
            else:
                self.state = DynamicSessionState(self.state_file)
                self.state.save()

            while True:
                terminate, success, reason = self._should_terminate()
                if terminate:
                    if not success:
                        self.kill_active()
                    return PhaseResult(success=success, failure_context=reason if not success else "")

                made_progress = False
                made_progress |= self._drain_completed_futures()
                made_progress |= self._schedule_verifiers()
                made_progress |= self._schedule_workers()

                if self._needs_captain_turn():
                    self._run_captain_turn()
                    made_progress = True

                terminate, success, reason = self._should_terminate()
                if terminate:
                    if not success:
                        self.kill_active()
                    return PhaseResult(success=success, failure_context=reason if not success else "")

                if not made_progress:
                    time.sleep(_IDLE_SLEEP_SECONDS)
        finally:
            self._worker_executor.shutdown(wait=False, cancel_futures=True)
            self._verifier_executor.shutdown(wait=False, cancel_futures=True)

    def kill_active(self) -> None:
        """Kill all active subprocesses owned by the runner."""

        for backend in set(self._backend_by_name.values()):
            backend.kill_active()

    def _needs_captain_turn(self) -> bool:
        if self._terminal_failure:
            return False
        if self.state.control.stop_requested:
            return False
        if self.state.control.wrap_requested:
            return self.state.control.wrap_summary_pending and not self._has_active_runtime_work()

        current_snapshot = self._captain_snapshot()
        if self.state.captain.turn_index == 0:
            return True

        delta = self.state.pending_captain_delta()
        if (
            delta.verified_claim_ids
            or delta.rejected_claim_ids
            or delta.no_findings_target_ids
            or delta.blocked_target_ids
            or delta.exhausted_target_ids
            or delta.pending_directive_ids
        ):
            return True

        frontier = self._frontier_targets()
        if not frontier:
            return current_snapshot != self._last_captain_snapshot

        return len(frontier) < self.config.max_workers and current_snapshot != self._last_captain_snapshot

    def _run_captain_turn(self) -> None:
        summary_only = self.state.control.wrap_requested and self.state.control.wrap_summary_pending
        prompt = self._build_captain_prompt(summary_only=summary_only)
        backend = self._get_backend(self.config.captain_backend)
        session_id = self.state.captain.session_id

        if session_id:
            result = backend.resume_agent(
                session_id,
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("captain"),
            )
        else:
            result = backend.run_agent(
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("captain"),
            )

        if result.session_id:
            self.state.captain.session_id = result.session_id
            self.state.save()

        self._add_tokens(result)
        if result.exit_code != 0:
            self._terminal_failure = f"captain exited with code {result.exit_code}: {result.output[-2000:]}"
            return

        try:
            turn = parse_captain_output(result.output)
        except ValueError as exc:
            turn = self._repair_captain_turn(result, str(exc), summary_only=summary_only)
            if turn is None:
                return

        delivered_event_seq = self._last_deliverable_event_seq()
        normalized_targets = self._normalize_captain_targets(turn)
        if not summary_only:
            for target in normalized_targets:
                self.state.targets[target.target_id] = target
                self.state.append_event(
                    "target.discovered",
                    target_id=target.target_id,
                    generation=target.active_generation,
                    source=target.source,
                )

        self.state.record_captain_turn(turn, delivered_event_seq)
        if summary_only:
            self.state.control.wrap_summary_pending = False
            self.state.save()

        self._captain_termination_state = turn.termination_state
        self._captain_termination_reason = turn.termination_reason
        self._last_captain_snapshot = self._captain_snapshot()

    def _repair_captain_turn(
        self,
        initial_result: AgentResult,
        parse_error: str,
        *,
        summary_only: bool,
    ) -> CaptainTurn | None:
        session_id = initial_result.session_id or self.state.captain.session_id
        if not session_id:
            self._terminal_failure = f"captain returned malformed output without resumable session: {parse_error}"
            return None

        backend = self._get_backend(self.config.captain_backend)
        last_error = parse_error
        for _ in range(self.config.max_captain_repairs):
            repair_prompt = (
                "Your previous response could not be parsed.\n"
                f"Parser error: {last_error}\n\n"
                "Return exactly one valid CAPTAIN_JSON block that satisfies the required schema.\n"
            )
            if summary_only:
                repair_prompt += (
                    "This is the final wrap summary turn. Do not enqueue new targets and set "
                    '`termination_state` to "complete".\n'
                )
            result = backend.resume_agent(
                session_id,
                repair_prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("captain"),
            )
            if result.session_id:
                self.state.captain.session_id = result.session_id
                self.state.save()
            self._add_tokens(result)
            if result.exit_code != 0:
                self._terminal_failure = f"captain repair exited with code {result.exit_code}: {result.output[-2000:]}"
                return None
            try:
                return parse_captain_output(result.output)
            except ValueError as exc:
                last_error = str(exc)

        self._terminal_failure = f"captain output remained malformed after repair: {last_error}"
        return None

    def _schedule_workers(self) -> bool:
        if self._terminal_failure or self.state.control.stop_requested or self.state.control.wrap_requested:
            return False

        now = time.time()
        changed = False
        for target in self.state.targets.values():
            if target.status != "requeue_pending":
                continue
            if self._is_target_ignored(target):
                continue
            target.status = "queued"
            target.updated_at = now
            changed = True
        if changed:
            self.state.save()

        available = self.config.max_workers - len(self._worker_futures)
        if available <= 0:
            return changed

        queued_targets = [
            target
            for target in self.state.targets.values()
            if target.status == "queued"
            and not self._is_target_ignored(target)
            and self._dependencies_satisfied(target)
        ]
        queued_targets.sort(key=lambda target: (-target.priority, target.created_at, target.target_id))

        scheduled = False
        for target in queued_targets[:available]:
            attempt = self._start_worker_attempt(target)
            prompt = self._build_worker_prompt(target, attempt)
            future = self._worker_executor.submit(self._execute_worker_attempt, attempt, prompt)
            self._worker_futures[future] = attempt.attempt_id
            scheduled = True
        return changed or scheduled

    def _schedule_verifiers(self) -> bool:
        if self._terminal_failure or self.state.control.stop_requested:
            return False

        changed = False
        now = time.time()
        for claim in self.state.claims.values():
            target = self.state.targets.get(claim.target_id)
            if target is None or claim.status != "proposed":
                continue
            if target.active_generation != claim.generation:
                continue
            if claim.verification_ids:
                continue
            verification = VerificationRecord(
                verification_id=self._next_verification_id(claim.claim_id),
                claim_id=claim.claim_id,
                target_id=claim.target_id,
                generation=claim.generation,
                backend=self.config.verifier_backend,
                verifier_role="analysis-verifier",
                session_id=None,
                status="pending",
                disposition=None,
                reason="",
                rejection_class=None,
                raw_output="",
                started_at=None,
                completed_at=None,
            )
            self.state.verifications[verification.verification_id] = verification
            claim.verification_ids.append(verification.verification_id)
            claim.status = "verifying"
            if verification.verification_id not in target.pending_verification_ids:
                target.pending_verification_ids.append(verification.verification_id)
            target.status = "verifying"
            target.updated_at = now
            changed = True

        if changed:
            self.state.save()

        available = self.config.max_verifiers - len(self._verifier_futures)
        if available <= 0:
            return changed

        pending: list[VerificationRecord] = []
        for verification in self.state.verifications.values():
            if verification.status != "pending":
                continue
            target = self.state.targets.get(verification.target_id)
            if target is None or target.active_generation != verification.generation:
                continue
            pending.append(verification)

        pending.sort(
            key=lambda verification: (
                -self.state.targets[verification.target_id].priority,
                self.state.targets[verification.target_id].created_at,
                verification.verification_id,
            )
        )

        scheduled = False
        for verification in pending[:available]:
            claim = self.state.claims[verification.claim_id]
            target = self.state.targets[verification.target_id]
            verification.status = "running"
            verification.started_at = time.time()
            self.state.save()
            prompt = self._build_verifier_prompt(target, claim)
            future = self._verifier_executor.submit(self._execute_verifier, verification, prompt)
            self._verifier_futures[future] = verification.verification_id
            scheduled = True
        return changed or scheduled

    def _drain_completed_futures(self) -> bool:
        progressed = False

        for future, attempt_id in list(self._worker_futures.items()):
            if not future.done():
                continue
            progressed = True
            self._worker_futures.pop(future, None)
            try:
                result = future.result()
            except Exception as exc:  # pragma: no cover - defensive, worker wrapper catches normally
                self._terminal_failure = f"worker future {attempt_id} crashed: {exc}"
                continue
            self._apply_worker_result(result)

        for future, verification_id in list(self._verifier_futures.items()):
            if not future.done():
                continue
            progressed = True
            self._verifier_futures.pop(future, None)
            try:
                result = future.result()
            except Exception as exc:  # pragma: no cover - defensive, verifier wrapper catches normally
                self._terminal_failure = f"verifier future {verification_id} crashed: {exc}"
                continue
            self._apply_verifier_result(result)

        return progressed

    def _should_terminate(self) -> tuple[bool, bool, str]:
        if self._terminal_failure:
            return True, False, self._terminal_failure

        action, reason = self.state.resume_control_action()
        if action == "stop":
            return True, False, reason
        if action == "finish":
            return True, True, ""

        frontier = self._frontier_targets()
        if self._captain_termination_state == "complete" and not frontier and not self._has_active_runtime_work():
            return True, True, ""

        if not frontier and not self._has_active_runtime_work():
            delta = self.state.pending_captain_delta()
            all_terminal = bool(self.state.targets) and all(
                self._is_terminal_target(target) or self._is_target_ignored(target)
                for target in self.state.targets.values()
            )
            if (
                all_terminal
                and any(target.status == "exhausted" for target in self.state.targets.values())
                and not (
                    delta.verified_claim_ids
                    or delta.rejected_claim_ids
                    or delta.no_findings_target_ids
                    or delta.blocked_target_ids
                    or delta.exhausted_target_ids
                    or delta.pending_directive_ids
                )
            ):
                return True, False, "analysis exhausted retry budget across all targets"

            if (
                self.state.captain.turn_index > 0
                and not (
                    delta.verified_claim_ids
                    or delta.rejected_claim_ids
                    or delta.no_findings_target_ids
                    or delta.blocked_target_ids
                    or delta.exhausted_target_ids
                    or delta.pending_directive_ids
                )
                and self._last_captain_snapshot == self._captain_snapshot()
                and self._captain_termination_state != "complete"
            ):
                return True, False, "captain left the frontier empty without requesting completion"

        return False, False, ""

    def _build_captain_prompt(self, *, summary_only: bool = False) -> str:
        delta = self.state.pending_captain_delta()
        pending_directives = [
            asdict(self.state.directives[directive_id])
            for directive_id in delta.pending_directive_ids
            if directive_id in self.state.directives
        ]
        frontier_summary = {
            "counts": delta.frontier_counts,
            "active_targets": [self._target_prompt_summary(target) for target in self._frontier_targets()],
        }
        delta_payload = {
            "verified_claims": [self._claim_delta_payload(claim_id) for claim_id in delta.verified_claim_ids],
            "rejected_claims": [self._claim_delta_payload(claim_id) for claim_id in delta.rejected_claim_ids],
            "no_findings_targets": [
                self._target_delta_payload(target_id) for target_id in delta.no_findings_target_ids
            ],
            "blocked_targets": [self._target_delta_payload(target_id) for target_id in delta.blocked_target_ids],
            "exhausted_targets": [self._target_delta_payload(target_id) for target_id in delta.exhausted_target_ids],
        }
        mission = self.phase.render_prompt(failure_context=self.failure_context, vars=self.workflow.vars)
        mode_note = (
            "This is a final wrap summary turn. Do not enqueue new targets and set termination_state to complete."
            if summary_only
            else "Plan the next bounded analysis work."
        )
        return (
            f"{self._captain_role_prompt}\n\n"
            f"Mission:\n{mission}\n\n"
            f"Repository root: {self.working_dir}\n"
            f"Captain turn: {self.state.captain.turn_index + 1}\n"
            f"Mode: {mode_note}\n\n"
            "Current mental model:\n"
            f"{self.state.captain.mental_model_summary or '(none yet)'}\n\n"
            "Open questions:\n"
            f"{json.dumps(self.state.captain.open_questions, indent=2)}\n\n"
            "Pending user directives:\n"
            f"{json.dumps(pending_directives, indent=2)}\n\n"
            "Frontier summary:\n"
            f"{json.dumps(frontier_summary, indent=2)}\n\n"
            "Event delta since the last captain turn:\n"
            f"{json.dumps(delta_payload, indent=2)}\n"
        )

    def _build_worker_prompt(self, target: TargetRecord, attempt: WorkerAttempt) -> str:
        task_packet = {
            "task_id": attempt.attempt_id,
            "target_id": target.target_id,
            "generation": attempt.generation,
            "title": target.title,
            "kind": target.kind,
            "priority": target.priority,
            "scope_paths": target.scope_paths,
            "scope_symbols": target.scope_symbols,
            "instructions": target.instructions,
            "spawn_reason": target.spawn_reason,
            "allow_repo_tools": self.config.allow_repo_tools,
        }
        return (
            f"{self._worker_role_prompt}\n\n"
            f"Repository root: `{self.working_dir}`\n\n"
            "Task packet:\n"
            f"```text\n{json.dumps(task_packet, indent=2)}\n```\n\n"
            "Verified dependencies:\n"
            f"```text\n{json.dumps(self._verified_dependency_payload(target), indent=2)}\n```\n\n"
            "Retry feedback or prior rejection context:\n"
            f"```text\n{json.dumps(self._retry_feedback_payload(target), indent=2)}\n```\n\n"
            "Code context pack:\n"
            f"```text\n{json.dumps(self._code_context_payload(target), indent=2)}\n```\n"
        )

    def _build_verifier_prompt(self, target: TargetRecord, claim: ClaimRecord) -> str:
        packet = asdict(claim_to_verifier_packet(claim))
        return (
            f"{self._verifier_role_prompt}\n\n"
            f"Repository root: `{self.working_dir}`\n\n"
            "Target context:\n"
            f"```text\n{json.dumps(self._target_prompt_summary(target), indent=2)}\n```\n\n"
            "Verified dependencies:\n"
            f"```text\n{json.dumps(self._verified_dependency_payload(target), indent=2)}\n```\n\n"
            "Scrubbed claim packet:\n"
            f"```text\n{json.dumps(packet, indent=2)}\n```\n\n"
            "Code context pack:\n"
            f"```text\n{json.dumps(self._code_context_payload(target), indent=2)}\n```\n"
        )

    def _execute_worker_attempt(self, attempt: WorkerAttempt, prompt: str) -> _WorkerExecutionResult:
        backend = self._get_backend(self.config.worker_backend)
        result = backend.run_agent(
            prompt,
            working_dir=str(self.working_dir),
            timeout=self.phase.timeout,
            env=self._role_env("worker"),
        )
        if result.exit_code != 0:
            return _WorkerExecutionResult(
                attempt_id=attempt.attempt_id,
                target_id=attempt.target_id,
                generation=attempt.generation,
                agent_result=result,
                report=None,
                error=f"worker exited with code {result.exit_code}: {result.output[-2000:]}",
            )
        try:
            report = parse_worker_output(result.output)
        except ValueError as exc:
            return _WorkerExecutionResult(
                attempt_id=attempt.attempt_id,
                target_id=attempt.target_id,
                generation=attempt.generation,
                agent_result=result,
                report=None,
                error=f"worker returned malformed structured output: {exc}",
            )
        return _WorkerExecutionResult(
            attempt_id=attempt.attempt_id,
            target_id=attempt.target_id,
            generation=attempt.generation,
            agent_result=result,
            report=report,
            error=None,
        )

    def _execute_verifier(self, verification: VerificationRecord, prompt: str) -> _VerifierExecutionResult:
        backend = self._get_backend(self.config.verifier_backend)
        result = backend.run_agent(
            prompt,
            working_dir=str(self.working_dir),
            timeout=self.phase.timeout,
            env=self._role_env("verifier"),
        )
        if result.exit_code != 0:
            return _VerifierExecutionResult(
                verification_id=verification.verification_id,
                claim_id=verification.claim_id,
                target_id=verification.target_id,
                generation=verification.generation,
                agent_result=result,
                report=None,
                error=f"verifier exited with code {result.exit_code}: {result.output[-2000:]}",
            )
        try:
            report = parse_verification_report(result.output)
        except ValueError as exc:
            return _VerifierExecutionResult(
                verification_id=verification.verification_id,
                claim_id=verification.claim_id,
                target_id=verification.target_id,
                generation=verification.generation,
                agent_result=result,
                report=None,
                error=f"verifier returned malformed structured output: {exc}",
            )
        return _VerifierExecutionResult(
            verification_id=verification.verification_id,
            claim_id=verification.claim_id,
            target_id=verification.target_id,
            generation=verification.generation,
            agent_result=result,
            report=report,
            error=None,
        )

    def _apply_worker_result(self, result: _WorkerExecutionResult) -> None:
        self._add_tokens(result.agent_result)
        attempt = self.state.worker_attempts.get(result.attempt_id)
        target = self.state.targets.get(result.target_id)
        if attempt is None or target is None:
            return

        attempt.session_id = result.agent_result.session_id
        attempt.completed_at = time.time()

        if result.error:
            attempt.status = "failed"
            attempt.error = result.error
            if target.active_attempt_id == attempt.attempt_id:
                target.active_attempt_id = None
                target.updated_at = time.time()
            self.state.save()
            self._terminal_failure = result.error
            return

        report = result.report
        if report is None:
            self._terminal_failure = "worker finished without a parsed report"
            self.state.save()
            return
        if report.task_id != attempt.attempt_id or report.target_id != target.target_id:
            attempt.status = "failed"
            attempt.error = (
                f"worker report identity mismatch: expected task {attempt.attempt_id}/{target.target_id}, "
                f"got {report.task_id}/{report.target_id}"
            )
            self.state.save()
            self._terminal_failure = attempt.error
            return

        attempt.status = "completed"
        attempt.error = ""
        if target.active_generation != attempt.generation or target.active_attempt_id != attempt.attempt_id:
            target.active_attempt_id = None
            target.updated_at = time.time()
            self.state.save()
            return

        target.active_attempt_id = None
        target.updated_at = time.time()

        if report.outcome == "no_findings":
            target.status = "no_findings"
            self.state.append_event("target.no_findings", target_id=target.target_id, generation=attempt.generation)
            self.state.save()
            return

        if report.outcome == "blocked":
            target.status = "blocked"
            self.state.append_event(
                "target.blocked",
                target_id=target.target_id,
                generation=attempt.generation,
                blocker=report.blocker or "",
            )
            self.state.save()
            return

        now = time.time()
        target.status = "verifying"
        for proposed_claim in report.claims:
            claim_id = f"{target.target_id}-g{attempt.generation}-claim-{proposed_claim.worker_claim_id}"
            artifact_id = f"{claim_id}-artifact"
            claim = ClaimRecord(
                claim_id=claim_id,
                worker_claim_id=proposed_claim.worker_claim_id,
                target_id=target.target_id,
                attempt_id=attempt.attempt_id,
                generation=attempt.generation,
                kind=proposed_claim.kind,
                subcategory=proposed_claim.subcategory,
                summary=proposed_claim.summary,
                assertion=proposed_claim.assertion,
                severity=proposed_claim.severity,
                worker_confidence=proposed_claim.worker_confidence,
                primary_location=proposed_claim.primary_location,
                locations=list(proposed_claim.locations),
                preconditions=list(proposed_claim.preconditions),
                candidate_code_refs=list(proposed_claim.candidate_code_refs),
                related_claim_ids=list(proposed_claim.related_claim_ids),
                audit_artifact_id=artifact_id,
                status="proposed",
                verification_ids=[],
                rejection_class=None,
                verified_at=None,
                rejected_at=None,
            )
            artifact = WorkerClaimArtifact(
                artifact_id=artifact_id,
                claim_id=claim_id,
                worker_reasoning=proposed_claim.reasoning,
                worker_trace=list(proposed_claim.trace),
                commands_run=list(proposed_claim.commands_run),
                counterevidence_checked=list(proposed_claim.counterevidence_checked),
                follow_up_hints=list(proposed_claim.follow_up_hints),
            )
            self.state.claims[claim.claim_id] = claim
            self.state.store_worker_artifact(artifact)
            self.state.append_event(
                "claim.proposed",
                target_id=target.target_id,
                claim_id=claim.claim_id,
                generation=attempt.generation,
            )
            target.updated_at = now

        self.state.save()

    def _apply_verifier_result(self, result: _VerifierExecutionResult) -> None:
        self._add_tokens(result.agent_result)
        verification = self.state.verifications.get(result.verification_id)
        claim = self.state.claims.get(result.claim_id)
        target = self.state.targets.get(result.target_id)
        if verification is None or claim is None or target is None:
            return

        verification.session_id = result.agent_result.session_id
        verification.completed_at = time.time()
        verification.raw_output = result.agent_result.output

        if result.error:
            verification.status = "failed"
            verification.error = result.error
            self.state.save()
            self._terminal_failure = result.error
            return

        report = result.report
        if report is None:
            verification.status = "failed"
            verification.error = "verifier finished without a parsed report"
            self.state.save()
            self._terminal_failure = verification.error
            return

        if report.claim_id != claim.claim_id or report.target_id != target.target_id or report.raw_json is None:
            verification.status = "failed"
            verification.error = (
                f"verifier report identity mismatch: expected claim {claim.claim_id}/{target.target_id}, "
                f"got {report.claim_id}/{report.target_id}"
            )
            self.state.save()
            self._terminal_failure = verification.error
            return

        if target.active_generation != verification.generation:
            verification.status = "superseded"
            verification.disposition = report.disposition
            verification.reason = report.summary
            verification.rejection_class = report.rejection_class
            if claim.status in {"proposed", "verifying"}:
                claim.status = "superseded"
            self.state.save()
            return

        if verification.verification_id in target.pending_verification_ids:
            target.pending_verification_ids.remove(verification.verification_id)

        if report.passed:
            verification.status = "passed"
            verification.disposition = "verified"
            verification.reason = report.summary
            verification.rejection_class = None
            claim.status = "verified"
            claim.rejection_class = None
            claim.verified_at = verification.completed_at
            claim.rejected_at = None
            self.state.append_event(
                "claim.verified",
                target_id=target.target_id,
                claim_id=claim.claim_id,
                generation=verification.generation,
            )
            self._refresh_target_after_verification(target)
            self.state.save()
            return

        verification.status = "failed"
        verification.disposition = "rejected"
        verification.reason = report.summary or report.reason
        verification.rejection_class = report.rejection_class
        claim.status = "rejected"
        claim.rejection_class = report.rejection_class
        claim.rejected_at = verification.completed_at
        claim.verified_at = None
        self.state.append_event(
            "claim.rejected",
            target_id=target.target_id,
            claim_id=claim.claim_id,
            generation=verification.generation,
        )

        next_generation = verification.generation + 1
        if (next_generation - 1) > self.config.max_worker_retries:
            self._supersede_active_generation(target, rejected_claim_id=claim.claim_id)
            target.status = "exhausted"
            target.active_attempt_id = None
            target.pending_verification_ids = []
            target.rejected_claim_ids = [claim.claim_id]
            target.updated_at = time.time()
            self.state.append_event("target.exhausted", target_id=target.target_id, generation=verification.generation)
            self.state.save()
            return

        self._supersede_active_generation(target, rejected_claim_id=claim.claim_id)
        target.generation = next_generation
        target.active_generation = next_generation
        target.status = "requeue_pending"
        target.source = "retry"
        target.active_attempt_id = None
        target.deferred_until_turn = None
        target.pending_verification_ids = []
        target.accepted_claim_ids = []
        target.rejected_claim_ids = []
        target.updated_at = time.time()
        self.state.save()

    def _start_worker_attempt(self, target: TargetRecord) -> WorkerAttempt:
        generation = target.active_generation or target.generation or 1
        attempt = WorkerAttempt(
            attempt_id=self._next_attempt_id(target.target_id, generation),
            target_id=target.target_id,
            generation=generation,
            backend=self.config.worker_backend,
            session_id=None,
            status="running",
            started_at=time.time(),
            completed_at=None,
        )
        target.status = "running"
        target.active_attempt_id = attempt.attempt_id
        target.active_generation = generation
        target.updated_at = time.time()
        self.state.worker_attempts[attempt.attempt_id] = attempt
        self.state.append_event(
            "target.started",
            target_id=target.target_id,
            generation=generation,
            attempt_id=attempt.attempt_id,
        )
        self.state.save()
        return attempt

    def _normalize_captain_targets(self, turn: CaptainTurn) -> list[TargetRecord]:
        now = time.time()
        normalized: list[TargetRecord] = []
        seen_ids: set[str] = set()
        for proposal in turn.enqueue_targets:
            if proposal.target_id in seen_ids or proposal.target_id in self.state.targets:
                continue
            try:
                validate_target_scope(proposal.scope_paths, self.working_dir)
            except ValueError:
                continue
            if any(dependency_id not in self.state.claims for dependency_id in proposal.depends_on_claim_ids):
                continue
            seen_ids.add(proposal.target_id)
            normalized.append(
                TargetRecord(
                    target_id=proposal.target_id,
                    title=proposal.title,
                    kind=proposal.kind,
                    priority=max(0, min(100, proposal.priority)),
                    status="queued",
                    source="captain",
                    scope_paths=list(proposal.scope_paths),
                    scope_symbols=list(proposal.scope_symbols),
                    instructions=proposal.instructions,
                    depends_on_claim_ids=list(proposal.depends_on_claim_ids),
                    spawn_reason=proposal.spawn_reason,
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
            )
        return normalized

    def _frontier_targets(self) -> list[TargetRecord]:
        targets: list[TargetRecord] = []
        for target in self.state.targets.values():
            if self._is_target_ignored(target):
                continue
            if target.status not in _NON_TERMINAL_STATUSES:
                continue
            if self.state.control.wrap_requested and target.status in {"queued", "deferred", "requeue_pending"}:
                continue
            targets.append(target)
        return targets

    def _has_active_runtime_work(self) -> bool:
        if self._worker_futures or self._verifier_futures:
            return True
        for verification in self.state.verifications.values():
            if verification.status not in {"pending", "running"}:
                continue
            target = self.state.targets.get(verification.target_id)
            if target is not None and target.active_generation == verification.generation:
                return True
        return any(target.status in _RUNNING_STATUSES for target in self.state.targets.values())

    def _captain_snapshot(self) -> tuple[Any, ...]:
        frontier_counts: dict[str, int] = {}
        for target in self._frontier_targets():
            frontier_counts[target.status] = frontier_counts.get(target.status, 0) + 1
        unread_event_seq = self._last_deliverable_event_seq()
        return (
            tuple(sorted(frontier_counts.items())),
            unread_event_seq,
            self.state.control.stop_requested,
            self.state.control.wrap_requested,
            self.state.control.wrap_summary_pending,
        )

    def _last_deliverable_event_seq(self) -> int:
        return max(
            (
                event.seq
                for event in self.state.events
                if event.seq > self.state.captain.last_delivered_event_seq and event.event_type in _CAPTAIN_EVENT_TYPES
            ),
            default=self.state.captain.last_delivered_event_seq,
        )

    def _verified_dependency_payload(self, target: TargetRecord) -> list[dict[str, Any]]:
        dependency_ids = set(target.depends_on_claim_ids)
        payload: list[dict[str, Any]] = []
        for claim in self.state.claims.values():
            if claim.status != "verified":
                continue
            if claim.claim_id in dependency_ids or claim.target_id == target.target_id:
                payload.append(self._claim_prompt_summary(claim))
        return payload

    def _retry_feedback_payload(self, target: TargetRecord) -> list[dict[str, Any]]:
        if (target.active_generation or 1) <= 1:
            return []
        previous_generation = (target.active_generation or 1) - 1
        feedback: list[dict[str, Any]] = []
        for claim in self.state.claims.values():
            if (
                claim.target_id != target.target_id
                or claim.generation != previous_generation
                or claim.status != "rejected"
            ):
                continue
            record = self._claim_prompt_summary(claim)
            record["rejection_reason"] = self._latest_rejection_reason(claim)
            record["rejection_class"] = claim.rejection_class
            feedback.append(record)
        return feedback

    def _code_context_payload(self, target: TargetRecord) -> dict[str, Any]:
        return {
            "scope_paths": target.scope_paths,
            "scope_symbols": target.scope_symbols,
            "working_dir": str(self.working_dir),
        }

    def _target_prompt_summary(self, target: TargetRecord) -> dict[str, Any]:
        return {
            "target_id": target.target_id,
            "title": target.title,
            "kind": target.kind,
            "priority": target.priority,
            "status": target.status,
            "generation": target.active_generation,
            "scope_paths": target.scope_paths,
            "scope_symbols": target.scope_symbols,
            "instructions": target.instructions,
        }

    def _claim_prompt_summary(self, claim: ClaimRecord) -> dict[str, Any]:
        return {
            "claim_id": claim.claim_id,
            "target_id": claim.target_id,
            "generation": claim.generation,
            "kind": claim.kind,
            "subcategory": claim.subcategory,
            "summary": claim.summary,
            "assertion": claim.assertion,
            "severity": claim.severity,
            "primary_location": asdict(claim.primary_location),
            "candidate_code_refs": [asdict(location) for location in claim.candidate_code_refs],
        }

    def _claim_delta_payload(self, claim_id: str) -> dict[str, Any]:
        claim = self.state.claims.get(claim_id)
        if claim is None:
            return {"claim_id": claim_id}
        payload = self._claim_prompt_summary(claim)
        payload["status"] = claim.status
        payload["rejection_class"] = claim.rejection_class
        payload["rejection_reason"] = self._latest_rejection_reason(claim)
        return payload

    def _target_delta_payload(self, target_id: str) -> dict[str, Any]:
        target = self.state.targets.get(target_id)
        if target is None:
            return {"target_id": target_id}
        payload = self._target_prompt_summary(target)
        last_event = next((event for event in reversed(self.state.events) if event.target_id == target_id), None)
        if last_event is not None and last_event.payload:
            payload["event_payload"] = dict(last_event.payload)
        return payload

    def _refresh_target_after_verification(self, target: TargetRecord) -> None:
        active_claims = [
            claim
            for claim in self.state.claims.values()
            if claim.target_id == target.target_id and claim.generation == target.active_generation
        ]
        target.accepted_claim_ids = sorted(claim.claim_id for claim in active_claims if claim.status == "verified")
        target.rejected_claim_ids = sorted(claim.claim_id for claim in active_claims if claim.status == "rejected")
        target.updated_at = time.time()

        if target.pending_verification_ids:
            target.status = "verifying"
            return
        if active_claims and all(claim.status == "verified" for claim in active_claims):
            target.status = "completed"
            self.state.append_event("target.completed", target_id=target.target_id, generation=target.active_generation)
            return
        if target.active_attempt_id:
            target.status = "running"
            return
        target.status = "queued"

    def _supersede_active_generation(self, target: TargetRecord, *, rejected_claim_id: str) -> None:
        active_generation = target.active_generation
        for claim in self.state.claims.values():
            if claim.target_id != target.target_id or claim.generation != active_generation:
                continue
            if claim.claim_id == rejected_claim_id:
                continue
            if claim.status in {"proposed", "verifying"}:
                claim.status = "superseded"

        for verification in self.state.verifications.values():
            if verification.target_id != target.target_id or verification.generation != active_generation:
                continue
            if verification.status in {"pending", "running"}:
                verification.status = "superseded"
                verification.disposition = None
                verification.completed_at = verification.completed_at or time.time()
                verification.error = "superseded-after-target-requeue"

    def _latest_rejection_reason(self, claim: ClaimRecord) -> str | None:
        candidates = [
            verification
            for verification in self.state.verifications.values()
            if verification.claim_id == claim.claim_id and verification.disposition == "rejected"
        ]
        if not candidates:
            return None
        latest = max(
            candidates,
            key=lambda verification: (verification.completed_at or 0.0, verification.verification_id),
        )
        return latest.reason

    def _is_terminal_target(self, target: TargetRecord) -> bool:
        return target.status not in _NON_TERMINAL_STATUSES

    def _is_target_ignored(self, target: TargetRecord) -> bool:
        for prefix in self.state.ignored_path_prefixes:
            if any(path == prefix or path.startswith(prefix) for path in target.scope_paths):
                return True
        for symbol in self.state.ignored_symbols:
            if symbol in target.scope_symbols:
                return True
        return False

    def _dependencies_satisfied(self, target: TargetRecord) -> bool:
        return all(
            dependency_id in self.state.claims and self.state.claims[dependency_id].status == "verified"
            for dependency_id in target.depends_on_claim_ids
        )

    def _next_attempt_id(self, target_id: str, generation: int) -> str:
        existing = [
            attempt
            for attempt in self.state.worker_attempts.values()
            if attempt.target_id == target_id and attempt.generation == generation
        ]
        return f"{target_id}-g{generation}-attempt-{len(existing) + 1}"

    def _next_verification_id(self, claim_id: str) -> str:
        existing = [record for record in self.state.verifications.values() if record.claim_id == claim_id]
        return f"{claim_id}-verification-{len(existing) + 1}"

    def _get_backend(self, name: str) -> Backend:
        with self._backend_lock:
            backend = self._backend_by_name.get(name)
            if backend is None:
                backend = create_backend(name)
                self._backend_by_name[name] = backend
            return backend

    def _add_tokens(self, result: AgentResult) -> None:
        self.total_input_tokens += result.input_tokens
        self.total_output_tokens += result.output_tokens

    def _role_env(self, role: str) -> dict[str, str] | None:
        env = dict(self.phase.env)
        env["JUVENAL_ANALYSIS_ROLE"] = role
        return env
