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
from juvenal.dynamic.interaction import UserInteractionChannel
from juvenal.dynamic.models import (
    CaptainTurn,
    ClaimRecord,
    TargetRecord,
    UserDirective,
    VerificationRecord,
    WorkerAttempt,
    WorkerClaimArtifact,
    WorkerReport,
)
from juvenal.dynamic.protocol import (
    claim_to_verifier_packet,
    parse_captain_output,
    parse_user_directive,
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
        "claim.retry_scheduled",
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
        interaction_channel: UserInteractionChannel | None = None,
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
        self._last_review_snapshot: tuple[Any, ...] | None = None
        self._last_review_event_seq = 0
        self._last_reviewed_turn_index = 0
        self._terminal_failure = ""
        self._pending_claim_retries: list[tuple[str, str]] = []  # [(target_id, claim_id)]
        self._consecutive_errors = 0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self._interaction_channel = interaction_channel if interactive else None
        if self._interaction_channel is None and interactive:
            self._interaction_channel = UserInteractionChannel()

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

            self._rebuild_pending_claim_retries()
            self._last_review_event_seq = max((event.seq for event in self.state.events), default=0)
            self._last_reviewed_turn_index = self.state.captain.turn_index
            self._last_review_snapshot = self._review_snapshot()
            if self._interaction_channel is not None:
                self._interaction_channel.start()

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
                made_progress |= self._apply_review_point()

                terminate, success, reason = self._should_terminate()
                if terminate:
                    if not success:
                        self.kill_active()
                    return PhaseResult(success=success, failure_context=reason if not success else "")

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
            if self._interaction_channel is not None:
                self._interaction_channel.stop()
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

        # Targets with pending claim retries should be serviced by the retry path, not re-scheduled
        targets_with_retries = {t for t, _ in self._pending_claim_retries}
        queued_targets = [
            target
            for target in self.state.targets.values()
            if target.status == "queued"
            and not self._is_target_ignored(target)
            and self._dependencies_satisfied(target)
            and target.target_id not in targets_with_retries
        ]
        queued_targets.sort(key=lambda target: (-target.priority, target.created_at, target.target_id))

        scheduled = False
        for target in queued_targets[:available]:
            attempt = self._start_worker_attempt(target)
            prompt = self._build_worker_prompt(target, attempt)
            future = self._worker_executor.submit(self._execute_worker_attempt, attempt, prompt)
            self._worker_futures[future] = attempt.attempt_id
            scheduled = True

        # Process pending claim retries within remaining budget
        available = self.config.max_workers - len(self._worker_futures)
        if available > 0 and self._pending_claim_retries:
            retries = self._pending_claim_retries[:available]
            self._pending_claim_retries = self._pending_claim_retries[available:]
            for target_id, claim_id in retries:
                target = self.state.targets.get(target_id)
                claim = self.state.claims.get(claim_id)
                if target is None or claim is None or claim.status != "rejected":
                    continue
                if self._is_target_ignored(target):
                    continue
                attempt = self._start_claim_retry_attempt(target, claim)
                prompt = self._build_claim_retry_prompt(target, claim, attempt)
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
                attempt = self.state.worker_attempts.get(attempt_id)
                if attempt is not None:
                    attempt.status = "failed"
                    attempt.error = f"future crashed: {exc}"
                    target = self.state.targets.get(attempt.target_id)
                    if target is not None and target.active_attempt_id == attempt_id:
                        target.active_attempt_id = None
                        target.error_retry_count += 1
                        if target.error_retry_count > self.config.max_worker_retries:
                            target.status = "blocked"
                        else:
                            target.status = "queued"
                        target.updated_at = time.time()
                    self.state.save()
                self._record_infrastructure_error()
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
                verification = self.state.verifications.get(verification_id)
                if verification is not None:
                    verification.status = "failed"
                    verification.error = f"future crashed: {exc}"
                    claim = self.state.claims.get(verification.claim_id)
                    target = self.state.targets.get(verification.target_id)
                    if claim is not None and target is not None:
                        self._handle_verifier_error(verification, claim, target)
                continue
            self._apply_verifier_result(result)

        return progressed

    def _apply_review_point(self) -> bool:
        if self._interaction_channel is None or self.state.control.stop_requested:
            return False

        current_snapshot = self._review_snapshot()
        if self._last_review_snapshot == current_snapshot:
            return False

        self.display.pause()
        try:
            self._print_review_summary()
            timeout = self.config.interaction_timeout
            print(
                f"Review window: {timeout:.1f}s | /focus /ignore path:... /ignore symbol:... "
                "/target ... /ask ... /summary /stop /wrap",
                flush=True,
            )
            lines = self._interaction_channel.poll(timeout)
        finally:
            self.display.resume()

        changed = False
        for line in lines:
            directive_id = self._next_directive_id()
            try:
                directive = parse_user_directive(line, directive_id=directive_id)
            except ValueError as exc:
                print(f"Ignoring invalid directive {line!r}: {exc}", flush=True)
                continue
            changed |= self._persist_directive(directive)

        self._last_review_event_seq = max((event.seq for event in self.state.events), default=0)
        self._last_reviewed_turn_index = self.state.captain.turn_index
        self._last_review_snapshot = self._review_snapshot()
        return changed

    def _review_snapshot(self) -> tuple[Any, ...]:
        counts = self._review_target_counts()
        return (
            max((event.seq for event in self.state.events), default=0),
            self.state.captain.turn_index,
            tuple(sorted(counts.items())),
            self.state.control.stop_requested,
            self.state.control.wrap_requested,
            self.state.control.wrap_summary_pending,
        )

    def _print_review_summary(self) -> None:
        verified, rejected = self._review_claim_updates()
        focus = self._focus_area_summaries()
        counts = self._review_target_counts()
        message = self.state.captain.last_message_to_user.strip() or self.state.captain.mental_model_summary.strip()
        if not message:
            message = "(no captain summary yet)"

        print("\n[analysis review]", flush=True)
        print(f"Captain: {message}", flush=True)
        print(f"Focus: {', '.join(focus) if focus else '(none)'}", flush=True)
        print(f"Verified: {', '.join(verified) if verified else 'none'}", flush=True)
        print(f"Rejected: {', '.join(rejected) if rejected else 'none'}", flush=True)
        print(
            f"Targets: queued={counts['queued']} running={counts['running']} verifying={counts['verifying']}",
            flush=True,
        )
        print(f"Remaining retry budget: {self._remaining_retry_budget()}", flush=True)

    def _review_claim_updates(self) -> tuple[list[str], list[str]]:
        verified: list[str] = []
        rejected: list[str] = []
        for event in self.state.events:
            if event.seq <= self._last_review_event_seq or event.claim_id is None:
                continue
            claim = self.state.claims.get(event.claim_id)
            if claim is None:
                summary = event.claim_id
            else:
                summary = claim.summary
                if event.event_type == "claim.rejected" and claim.rejection_class:
                    summary = f"{summary} [{claim.rejection_class}]"
            if event.event_type == "claim.verified":
                verified.append(summary)
            elif event.event_type == "claim.rejected":
                rejected.append(summary)
        return verified[:3], rejected[:3]

    def _focus_area_summaries(self) -> list[str]:
        active_targets = [
            target
            for target in self.state.targets.values()
            if not self._is_target_ignored(target) and target.status in _NON_TERMINAL_STATUSES
        ]
        active_targets.sort(key=lambda target: (-target.priority, target.created_at, target.target_id))
        focus = [f"{target.title} [{target.status}]" for target in active_targets[:3]]
        if focus:
            return focus
        return list(self.state.captain.open_questions[:3])

    def _review_target_counts(self) -> dict[str, int]:
        counts = {"queued": 0, "running": 0, "verifying": 0}
        for target in self.state.targets.values():
            if self._is_target_ignored(target):
                continue
            if target.status in counts:
                counts[target.status] += 1
        return counts

    def _remaining_retry_budget(self) -> int:
        remaining = 0
        for target in self.state.targets.values():
            if self._is_target_ignored(target) or self._is_terminal_target(target):
                continue
            for claim in self._active_claims_for_target(target):
                if claim.status == "rejected":
                    remaining += max(0, self.config.max_worker_retries - claim.retry_count)
        return remaining

    def _persist_directive(self, directive: UserDirective) -> bool:
        if directive.kind == "ignore":
            return self._apply_ignore_directive(directive)
        if directive.kind == "target":
            return self._apply_user_target_directive(directive)
        if directive.kind == "stop":
            return self._apply_stop_directive(directive)
        if directive.kind == "wrap":
            return self._apply_wrap_directive(directive)
        return self._queue_captain_directive(directive)

    def _apply_ignore_directive(self, directive: UserDirective) -> bool:
        now = time.time()
        text = directive.text.strip()
        if text.startswith("path:"):
            prefix = text.removeprefix("path:").strip()
            if not prefix:
                print(f"Ignoring invalid directive {text!r}: empty path prefix", flush=True)
                return False
            if prefix not in self.state.ignored_path_prefixes:
                self.state.ignored_path_prefixes.append(prefix)
        elif text.startswith("symbol:"):
            symbol = text.removeprefix("symbol:").strip()
            if not symbol:
                print(f"Ignoring invalid directive {text!r}: empty symbol name", flush=True)
                return False
            if symbol not in self.state.ignored_symbols:
                self.state.ignored_symbols.append(symbol)
        else:
            print(f"Ignoring invalid directive {text!r}: unsupported /ignore target", flush=True)
            return False

        directive.status = "applied"
        self.state.directives[directive.directive_id] = directive
        for target in self.state.targets.values():
            if self._is_target_ignored(target):
                target.updated_at = now
        self.state.save()
        return True

    def _apply_user_target_directive(self, directive: UserDirective) -> bool:
        now = time.time()
        directive.status = "applied"
        self.state.directives[directive.directive_id] = directive
        target = TargetRecord(
            target_id=self._next_user_target_id(),
            title=directive.text,
            kind="user-target",
            priority=100,
            status="queued",
            source="user",
            scope_paths=[],
            scope_symbols=[],
            instructions=directive.text,
            depends_on_claim_ids=[],
            spawn_reason="User requested targeted analysis.",
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
        self.state.targets[target.target_id] = target
        self.state.append_event(
            "target.discovered",
            target_id=target.target_id,
            generation=target.active_generation,
            source=target.source,
        )
        self.state.save()
        return True

    def _apply_stop_directive(self, directive: UserDirective) -> bool:
        directive.status = "applied"
        self.state.directives[directive.directive_id] = directive
        self.state.control.stop_requested = True
        self.state.save()
        self.kill_active()
        return True

    def _apply_wrap_directive(self, directive: UserDirective) -> bool:
        directive.status = "applied"
        self.state.directives[directive.directive_id] = directive
        self.state.control.wrap_requested = True
        self.state.control.wrap_summary_pending = True
        self.state.save()
        return True

    def _queue_captain_directive(self, directive: UserDirective) -> bool:
        self.state.directives[directive.directive_id] = directive
        self.state.append_event("directive.received", directive_id=directive.directive_id)
        return True

    def _next_directive_id(self) -> str:
        return f"dir-{len(self.state.directives) + 1}"

    def _next_user_target_id(self) -> str:
        existing = [target for target in self.state.targets.values() if target.source == "user"]
        return f"user-target-{len(existing) + 1}"

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
                if (
                    self._interaction_channel is not None
                    and self._last_reviewed_turn_index < self.state.captain.turn_index
                ):
                    return False, False, ""
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
                target.error_retry_count += 1
                if target.error_retry_count > self.config.max_worker_retries:
                    target.status = "blocked"
                    self.state.append_event(
                        "target.blocked",
                        target_id=target.target_id,
                        generation=attempt.generation,
                        blocker=result.error,
                    )
                else:
                    target.status = "queued"
                target.updated_at = time.time()
            self.state.save()
            self._record_infrastructure_error()
            return

        report = result.report
        if report is None:
            attempt.status = "failed"
            attempt.error = "worker finished without a parsed report"
            if target.active_attempt_id == attempt.attempt_id:
                target.active_attempt_id = None
                target.error_retry_count += 1
                if target.error_retry_count > self.config.max_worker_retries:
                    target.status = "blocked"
                    self.state.append_event(
                        "target.blocked",
                        target_id=target.target_id,
                        generation=attempt.generation,
                        blocker=attempt.error,
                    )
                else:
                    target.status = "queued"
                target.updated_at = time.time()
            self.state.save()
            self._record_infrastructure_error()
            return
        if report.task_id != attempt.attempt_id or report.target_id != target.target_id:
            attempt.status = "failed"
            attempt.error = (
                f"worker report identity mismatch: expected task {attempt.attempt_id}/{target.target_id}, "
                f"got {report.task_id}/{report.target_id}"
            )
            if target.active_attempt_id == attempt.attempt_id:
                target.active_attempt_id = None
                target.error_retry_count += 1
                if target.error_retry_count > self.config.max_worker_retries:
                    target.status = "blocked"
                    self.state.append_event(
                        "target.blocked",
                        target_id=target.target_id,
                        generation=attempt.generation,
                        blocker=attempt.error,
                    )
                else:
                    target.status = "queued"
                target.updated_at = time.time()
            self.state.save()
            self._record_infrastructure_error()
            return

        attempt.status = "completed"
        attempt.error = ""
        self._record_success()
        if target.active_generation != attempt.generation or target.active_attempt_id != attempt.attempt_id:
            target.active_attempt_id = None
            target.updated_at = time.time()
            self.state.save()
            return

        target.active_attempt_id = None
        target.updated_at = time.time()

        # Claim retry worker handling
        if attempt.retry_claim_id is not None:
            self._apply_claim_retry_result(target, attempt, report)
            return

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
            self._handle_verifier_error(verification, claim, target)
            return

        report = result.report
        if report is None:
            verification.status = "failed"
            verification.error = "verifier finished without a parsed report"
            self._handle_verifier_error(verification, claim, target)
            return

        if report.claim_id != claim.claim_id or report.target_id != target.target_id or report.raw_json is None:
            verification.status = "failed"
            verification.error = (
                f"verifier report identity mismatch: expected claim {claim.claim_id}/{target.target_id}, "
                f"got {report.claim_id}/{report.target_id}"
            )
            self._handle_verifier_error(verification, claim, target)
            return

        # Verifier returned a valid structured response — reset infrastructure error counter
        self._record_success()

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
            verification.follow_up_action = report.follow_up_action
            verification.follow_up_strategy = report.follow_up_strategy
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
        verification.follow_up_action = report.follow_up_action
        verification.follow_up_strategy = report.follow_up_strategy
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

        # Claim-scoped retry: only retry the rejected claim, not the whole target
        if claim.retry_count < self.config.max_worker_retries:
            self._pending_claim_retries.append((target.target_id, claim.claim_id))
            self.state.append_event(
                "claim.retry_scheduled",
                target_id=target.target_id,
                claim_id=claim.claim_id,
                generation=verification.generation,
            )

        self._refresh_target_after_verification(target)
        self.state.save()

    def _apply_claim_retry_result(self, target: TargetRecord, attempt: WorkerAttempt, report: WorkerReport) -> None:
        """Handle a completed claim retry worker."""
        original_claim = self.state.claims.get(attempt.retry_claim_id or "")

        if report.outcome in ("no_findings", "blocked"):
            # Retry worker confirms the claim was false or can't determine — original rejection stands
            # Consume the retry budget so _refresh_target_after_verification sees it as exhausted
            if original_claim is not None:
                original_claim.retry_count += 1
            self._refresh_target_after_verification(target)
            self.state.save()
            return

        # outcome == "claims" — create new claims linked to the original
        now = time.time()
        generation = attempt.generation
        for proposed_claim in report.claims:
            claim_id = f"{target.target_id}-g{generation}-retry-{proposed_claim.worker_claim_id}"
            artifact_id = f"{claim_id}-artifact"
            retry_count = (original_claim.retry_count + 1) if original_claim else 1
            claim = ClaimRecord(
                claim_id=claim_id,
                worker_claim_id=proposed_claim.worker_claim_id,
                target_id=target.target_id,
                attempt_id=attempt.attempt_id,
                generation=generation,
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
                retry_count=retry_count,
                retry_of_claim_id=attempt.retry_claim_id,
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
                generation=generation,
            )
            # Link original claim to its retry successor
            if original_claim is not None:
                original_claim.retry_claim_ids.append(claim.claim_id)
            target.updated_at = now

        target.status = "verifying"
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

    def _start_claim_retry_attempt(self, target: TargetRecord, claim: ClaimRecord) -> WorkerAttempt:
        """Start a worker attempt scoped to retrying a single rejected claim."""
        generation = target.active_generation or target.generation or 1
        existing = [
            a
            for a in self.state.worker_attempts.values()
            if a.target_id == target.target_id and a.retry_claim_id == claim.claim_id
        ]
        attempt = WorkerAttempt(
            attempt_id=f"{target.target_id}-g{generation}-retry-{claim.claim_id}-{len(existing) + 1}",
            target_id=target.target_id,
            generation=generation,
            backend=self.config.worker_backend,
            session_id=None,
            status="running",
            started_at=time.time(),
            completed_at=None,
            retry_claim_id=claim.claim_id,
        )
        target.status = "running"
        target.active_attempt_id = attempt.attempt_id
        target.updated_at = time.time()
        self.state.worker_attempts[attempt.attempt_id] = attempt
        self.state.save()
        return attempt

    def _build_claim_retry_prompt(self, target: TargetRecord, claim: ClaimRecord, attempt: WorkerAttempt) -> str:
        """Build a worker prompt scoped to re-investigating a single rejected claim."""
        rejection_reason = self._latest_rejection_reason(claim) or "No specific reason provided."
        rejection_chain = self._get_rejection_chain(claim)
        latest_verification = self._latest_rejection_verification(claim)
        follow_up_action = latest_verification.follow_up_action if latest_verification else None
        follow_up_strategy = latest_verification.follow_up_strategy if latest_verification else None
        verified_siblings = [
            self._claim_prompt_summary(c)
            for c in self._active_claims_for_target(target)
            if c.claim_id != claim.claim_id and c.status == "verified"
        ]
        rejected_detail = self._claim_prompt_summary(claim)
        rejected_detail["rejection_reason"] = rejection_reason
        rejected_detail["rejection_class"] = claim.rejection_class
        rejected_detail["follow_up_action"] = follow_up_action
        rejected_detail["follow_up_strategy"] = follow_up_strategy
        task_packet = {
            "task_id": attempt.attempt_id,
            "target_id": target.target_id,
            "generation": attempt.generation,
            "retry_mode": True,
            "retry_claim_id": claim.claim_id,
            "retry_attempt": claim.retry_count + 1,
            "max_retries": self.config.max_worker_retries,
        }
        return (
            f"{self._worker_role_prompt}\n\n"
            f"Repository root: `{self.working_dir}`\n\n"
            "## CLAIM RETRY MODE — VERIFIER CHALLENGE\n\n"
            "You are responding to a verifier challenge on a rejected claim. This is a dialog:\n"
            "the verifier is pushing you toward stronger evidence. Read the full rejection chain\n"
            "below to understand what has already been tried and what specific challenges the\n"
            "verifier raised.\n\n"
            "Do NOT repeat the same approach that was already rejected. Address the verifier's\n"
            "specific feedback. If the verifier said a guard exists, either prove the guard is\n"
            "bypassable or find a different path. If the verifier said evidence was insufficient,\n"
            "provide concrete dynamic proof (PoC, test output, tool results).\n\n"
            "Task packet:\n"
            f"```text\n{json.dumps(task_packet, indent=2)}\n```\n\n"
            "Rejected claim (your original submission):\n"
            f"```text\n{json.dumps(rejected_detail, indent=2)}\n```\n\n"
            "Full rejection chain (all prior attempts and verifier feedback, oldest first):\n"
            f"```text\n{json.dumps(rejection_chain, indent=2)}\n```\n\n"
            "Verified claims on this target (for context only — do NOT re-report these):\n"
            f"```text\n{json.dumps(verified_siblings, indent=2)}\n```\n\n"
            "Target context:\n"
            f"```text\n{json.dumps(self._target_prompt_summary(target), indent=2)}\n```\n\n"
            "Code context pack:\n"
            f"```text\n{json.dumps(self._code_context_payload(target), indent=2)}\n```\n\n"
            "Instructions:\n"
            "- Study the FULL rejection chain — do NOT repeat approaches that were already rejected\n"
            "- Address the verifier's SPECIFIC challenge (rejection_reason and follow_up hints)\n"
            "- If the verifier identified a guard/mitigation, either prove it is bypassable or find"
            " a different attack path\n"
            "- If the verifier said evidence was insufficient, provide a concrete PoC or dynamic proof\n"
            "- Each retry must present genuinely NEW evidence or a different investigation approach\n"
            '- If after honest re-examination the claim was genuinely false, report outcome: "no_findings"\n'
            "- Use the same WORKER_JSON_BEGIN/END output format\n"
        )

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
        if self._pending_claim_retries:
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
        leaf_claims = self._active_claims_for_target(target)
        target.accepted_claim_ids = sorted(claim.claim_id for claim in leaf_claims if claim.status == "verified")
        target.rejected_claim_ids = sorted(claim.claim_id for claim in leaf_claims if claim.status == "rejected")
        target.updated_at = time.time()

        if target.pending_verification_ids:
            target.status = "verifying"
            return
        if any(claim.status in ("proposed", "verifying") for claim in leaf_claims):
            target.status = "verifying"
            return
        if self._has_active_attempt(target):
            target.status = "running"
            return
        if leaf_claims and all(claim.status == "verified" for claim in leaf_claims):
            target.status = "completed"
            self.state.append_event("target.completed", target_id=target.target_id, generation=target.active_generation)
            return

        # Check for retryable rejected claims
        retryable = [
            claim
            for claim in leaf_claims
            if claim.status == "rejected"
            and claim.retry_count < self.config.max_worker_retries
            and not self._has_pending_retry(claim)
        ]
        if retryable:
            target.status = "queued"
            return

        # Check for exhausted rejected claims (no retries left)
        exhausted_rejected = [
            claim
            for claim in leaf_claims
            if claim.status == "rejected" and claim.retry_count >= self.config.max_worker_retries
        ]
        if exhausted_rejected:
            target.status = "exhausted"
            self.state.append_event("target.exhausted", target_id=target.target_id, generation=target.active_generation)
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

    def _handle_verifier_error(
        self, verification: VerificationRecord, claim: ClaimRecord, target: TargetRecord
    ) -> None:
        """Handle a verifier crash: retry verification or treat as inconclusive rejection."""
        target.error_retry_count += 1
        if verification.verification_id in target.pending_verification_ids:
            target.pending_verification_ids.remove(verification.verification_id)
        if target.error_retry_count <= self.config.max_worker_retries:
            new_verification = VerificationRecord(
                verification_id=self._next_verification_id(claim.claim_id),
                claim_id=claim.claim_id,
                target_id=claim.target_id,
                generation=verification.generation,
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
            self.state.verifications[new_verification.verification_id] = new_verification
            claim.verification_ids.append(new_verification.verification_id)
            if new_verification.verification_id not in target.pending_verification_ids:
                target.pending_verification_ids.append(new_verification.verification_id)
        else:
            claim.status = "rejected"
            claim.rejection_class = "verification-error"
            claim.rejected_at = time.time()
            self.state.append_event(
                "claim.rejected",
                target_id=target.target_id,
                claim_id=claim.claim_id,
                generation=verification.generation,
            )
            self._refresh_target_after_verification(target)
        target.updated_at = time.time()
        self.state.save()
        self._record_infrastructure_error()

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

    def _latest_rejection_verification(self, claim: ClaimRecord) -> VerificationRecord | None:
        """Return the latest rejected VerificationRecord for a claim."""
        candidates = [
            v for v in self.state.verifications.values() if v.claim_id == claim.claim_id and v.disposition == "rejected"
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda v: (v.completed_at or 0.0, v.verification_id))

    def _get_rejection_chain(self, claim: ClaimRecord) -> list[dict[str, Any]]:
        """Walk the retry chain backwards to collect all prior rejection records, oldest first."""
        chain: list[dict[str, Any]] = []
        current: ClaimRecord | None = claim
        while current is not None:
            rejections = [
                v
                for v in self.state.verifications.values()
                if v.claim_id == current.claim_id and v.disposition == "rejected"
            ]
            for v in sorted(rejections, key=lambda x: x.completed_at or 0.0):
                chain.append(
                    {
                        "claim_id": v.claim_id,
                        "attempt": current.retry_count,
                        "rejection_class": v.rejection_class,
                        "reason": v.reason,
                        "follow_up_action": v.follow_up_action,
                        "follow_up_strategy": v.follow_up_strategy,
                    }
                )
            parent_id = current.retry_of_claim_id
            current = self.state.claims.get(parent_id) if parent_id else None
        chain.reverse()
        return chain

    def _active_claims_for_target(self, target: TargetRecord) -> list[ClaimRecord]:
        """Return the leaf claims for a target (latest in each retry chain)."""
        active_claims = [
            claim
            for claim in self.state.claims.values()
            if claim.target_id == target.target_id and claim.generation == target.active_generation
        ]
        superseded_ids: set[str] = set()
        for claim in active_claims:
            if claim.retry_of_claim_id is not None:
                superseded_ids.add(claim.retry_of_claim_id)
        return [claim for claim in active_claims if claim.claim_id not in superseded_ids]

    def _has_pending_retry(self, claim: ClaimRecord) -> bool:
        """Check if a rejected claim already has a pending retry in the queue or in flight."""
        if (claim.target_id, claim.claim_id) in [(t, c) for t, c in self._pending_claim_retries]:
            return True
        for retry_id in claim.retry_claim_ids:
            retry_claim = self.state.claims.get(retry_id)
            if retry_claim is not None and retry_claim.status not in ("rejected", "verified"):
                return True
        return False

    def _has_active_attempt(self, target: TargetRecord) -> bool:
        """Check if the target has an active (running) worker attempt."""
        if target.active_attempt_id is None:
            return False
        attempt = self.state.worker_attempts.get(target.active_attempt_id)
        return attempt is not None and attempt.status == "running"

    def _rebuild_pending_claim_retries(self) -> None:
        """Rebuild the in-memory claim retry queue from persisted state (for resume)."""
        self._pending_claim_retries = []
        for target in self.state.targets.values():
            if self._is_target_ignored(target) or target.status in ("completed", "no_findings", "blocked", "exhausted"):
                continue
            for claim in self._active_claims_for_target(target):
                if (
                    claim.status == "rejected"
                    and claim.retry_count < self.config.max_worker_retries
                    and not self._has_pending_retry(claim)
                ):
                    self._pending_claim_retries.append((target.target_id, claim.claim_id))

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

    def _record_infrastructure_error(self) -> None:
        """Track a worker/verifier infrastructure failure (crash, malformed output, non-zero exit).

        Does NOT count verifier rejections — those are normal operation.
        When consecutive infrastructure errors hit the threshold, triggers a clean pause
        so the run can be resumed later (e.g., after API rate limits reset).
        """
        self._consecutive_errors += 1
        if self._consecutive_errors >= self.config.max_consecutive_errors:
            self._terminal_failure = (
                f"Pausing: {self._consecutive_errors} consecutive infrastructure errors "
                f"(likely rate limit or API outage). State saved — resume with --resume."
            )

    def _record_success(self) -> None:
        """Reset the consecutive error counter on any successful worker/verifier completion."""
        self._consecutive_errors = 0

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
