"""Dynamic analysis runner for captain/worker/verifier orchestration."""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Event, Lock
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
from juvenal.workflow import AnalysisConfig, Phase, ReporterSpec, VerifierSpec, Workflow, apply_vars

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
_TERMINAL_TARGET_STATUSES = frozenset({"completed", "no_findings", "blocked", "exhausted"})
_RUNNING_STATUSES = frozenset({"running", "verifying"})
_DASHBOARD_EVENT_KINDS = frozenset(
    {
        "claim.verified",
        "claim.rejected",
        "claim.retry_scheduled",
        "target.discovered",
        "target.completed",
        "target.no_findings",
        "target.blocked",
        "target.exhausted",
        "target.deferred",
        "directive.received",
        "directive.acknowledged",
    }
)
_IDLE_SLEEP_SECONDS = 0.05


def _flush_stdin_buffer() -> None:
    """Drop any buffered-but-unread input on stdin. Called on Ctrl-C / phase
    exit so partially-typed lines from the chat reader don't bleed into the
    parent shell's prompt."""

    if not sys.stdin.isatty():
        return
    try:
        import termios

        termios.tcflush(sys.stdin, termios.TCIFLUSH)
    except (ImportError, OSError):
        pass
    except Exception:
        pass


_DEFAULT_CONTINUE_NUDGE = (
    "## Continue nudge — engine override\n\n"
    "The engine has REJECTED your `complete` declaration (override #{consecutive} of "
    "{max_premature_completes} before the engine accepts the soft escape).\n\n"
    "Status: {turns} captain turn(s) elapsed; {terminal} target(s) reached terminal state. "
    "Configured floors: >= {min_captain_turns} captain turns AND >= {min_terminal_targets} "
    "terminal targets.\n\n"
    "Required actions for THIS turn:\n"
    "1. Update `mental_model_summary` with structured coverage accounting:\n"
    "   - `SUBSYSTEMS:` list each in-scope subsystem with a status tag "
    "(`untouched` | `active` | `covered` | `dry-hole`) and a one-line note.\n"
    "   - `ENTRY POINTS:` list each externally reachable entry point with a status tag.\n"
    "   - `UNCOVERED SURFACE:` enumerate every subsystem, file, or entry point not yet "
    "investigated. This list MUST be empty before you may declare `complete`.\n"
    "2. Apply the variant-analysis policy when seeding follow-up targets:\n"
    "   - Verified claim: spawn targets in the surrounding subsystem (siblings, callers, callees, "
    "related modules, structurally identical patterns elsewhere). Do NOT respawn the same bug.\n"
    "   - Rejected claim: spawn targets for alternate paths to the same sink, sibling code that "
    "may LACK the verifier-identified guard, or a different vulnerability class on the same "
    "surface. Rejection is negative evidence on a path, not on the surface.\n"
    "   - No-findings target: do NOT re-investigate; only spawn an adjacent fresh-angle target if "
    "you have a concrete reason.\n"
    "   - Blocked target: do NOT respawn until the blocker is addressed (different build path, "
    "static-only approach, alternative tooling).\n"
    "3. Enqueue at least 8 new targets pivoting to UNCOVERED SURFACE or following the "
    "variant-analysis rules above.\n"
    '4. Return `termination_state: "continue"`. Do not declare `complete` again until both '
    "floors are met AND `UNCOVERED SURFACE` is empty.\n"
)

# Per-backend, per-role default model. When the YAML does not specify a model
# (and no role-level override is set), this picks the right tier so users
# don't have to write model identifiers in every workflow. Captain and worker
# get the strongest tier (long context, high reasoning); verifier and reporter
# get a faster/cheaper tier since their tasks are more bounded. Codex uses
# whatever the CLI defaults to — we don't pick a model on its behalf.
_DEFAULT_MODELS_BY_BACKEND_AND_ROLE: dict[str, dict[str, str | None]] = {
    "claude": {
        "captain": "claude-opus-4-7[1m]",
        "worker": "claude-opus-4-7[1m]",
        "verifier": "claude-sonnet-4-6",
        "reporter": "claude-sonnet-4-6",
    },
    "codex": {
        "captain": None,
        "worker": None,
        "verifier": None,
        "reporter": None,
    },
}


def _resolve_model(backend: str, role: str, configured: str | None) -> str | None:
    """Resolve the effective model for a (backend, role).

    Priority: explicit YAML override > backend/role default > CLI default (None).
    """
    if configured is not None:
        return configured
    return _DEFAULT_MODELS_BY_BACKEND_AND_ROLE.get(backend, {}).get(role)


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


@dataclass
class _ReporterExecutionResult:
    claim_id: str
    target_id: str
    generation: int
    agent_result: AgentResult
    error: str | None


_MAX_REPORTER_ATTEMPTS = 3


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
        chat_dashboard: Any = None,
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
        self._injected_chat_dashboard: Any = chat_dashboard

        self.state = (
            DynamicSessionState.load(self.state_file) if run_mode == "resume" else DynamicSessionState(self.state_file)
        )
        self._backend_by_name: dict[str, Backend] = {}
        self._backend_lock = Lock()
        # Resolve effective parallel-agent capacity. In shared mode, both
        # pools are sized at max_agents so either role can use the full
        # budget; the actual cap is enforced at scheduling time. In legacy
        # mode, pools are sized at the per-role limits and enforced
        # independently.
        if self.config.shared_agent_budget:
            self._max_agents = self.config.max_agents
            self._max_worker_cap = self.config.max_agents
            self._max_verifier_cap = self.config.max_agents
        else:
            self._max_agents = self.config.max_workers + self.config.max_verifiers
            self._max_worker_cap = self.config.max_workers
            self._max_verifier_cap = self.config.max_verifiers
        self._worker_executor = ThreadPoolExecutor(max_workers=self._max_worker_cap)
        self._verifier_executor = ThreadPoolExecutor(max_workers=self._max_verifier_cap)
        self._reporter_executor = ThreadPoolExecutor(max_workers=max(1, self.config.max_workers))
        self._worker_futures: dict[Future[_WorkerExecutionResult], str] = {}
        self._verifier_futures: dict[Future[_VerifierExecutionResult], str] = {}
        self._reporter_futures: dict[Future[_ReporterExecutionResult], str] = {}
        self._pending_reporter_claim_ids: list[str] = []
        self._reporter_attempts: dict[str, int] = {}
        self._captain_termination_state: Literal["continue", "complete"] = "continue"
        self._captain_termination_reason = ""
        self._pending_continue_nudge: str = ""
        self._consecutive_premature_completes: int = 0
        self._last_captain_snapshot: tuple[Any, ...] | None = None
        self._last_review_snapshot: tuple[Any, ...] | None = None
        self._last_review_event_seq = 0
        self._last_reviewed_turn_index = 0
        self._terminal_failure = ""
        self._pending_claim_retries: list[tuple[str, str]] = []  # [(target_id, claim_id)]
        self._consecutive_errors = 0
        self._backoff_count = 0
        self._total_backoff_seconds = 0.0
        # Set on shutdown (Ctrl-C, kill_active). Background threads in
        # _rate_limit_backoff use this to interrupt their sleep loop so the
        # process can exit promptly instead of waiting up to an hour for a
        # rate-limit timer to elapse.
        self._shutdown_event = Event()
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self._interaction_channel = interaction_channel if interactive else None
        self._injected_interaction_channel = interaction_channel is not None and interactive
        if self._interaction_channel is None and interactive:
            self._interaction_channel = UserInteractionChannel()

        self._dashboard: Any = None
        self._captain_executor: ThreadPoolExecutor | None = None
        self._captain_future: Future[None] | None = None
        self._chat_history: list[str] = []
        self._force_captain_turn: bool = False
        self._chat_pending: bool = False
        self._post_chat_reprime: bool = False
        self._last_dashboard_event_seq: int = 0

        prompts_dir = Path(__file__).resolve().parent.parent / "prompts"
        self._captain_role_prompt = (prompts_dir / "captain-analysis.md").read_text(encoding="utf-8")
        self._worker_role_prompt = (prompts_dir / "analysis-worker.md").read_text(encoding="utf-8")
        self._verifier_role_prompt = (prompts_dir / "analysis-verifier.md").read_text(encoding="utf-8")

        if self.config.verifiers:
            self._verifier_chain: list[VerifierSpec] = list(self.config.verifiers)
        else:
            self._verifier_chain = [VerifierSpec(name="default", backend=self.config.verifier_backend, prompt="")]
        seen_names: set[str] = set()
        for spec in self._verifier_chain:
            if spec.name in seen_names:
                raise ValueError(f"Phase '{self.phase.id}': verifier chain has duplicate name {spec.name!r}")
            seen_names.add(spec.name)
        self._rendered_verifier_prompts: dict[str, str] = {
            spec.name: apply_vars(spec.prompt, self.workflow.vars) if spec.prompt else ""
            for spec in self._verifier_chain
        }

        self._reporter_spec: ReporterSpec | None = self.config.reporter
        self._rendered_reporter_prompt: str = ""
        if self._reporter_spec is not None and self._reporter_spec.prompt:
            self._rendered_reporter_prompt = apply_vars(self._reporter_spec.prompt, self.workflow.vars)

    def run(self) -> PhaseResult:
        """Run the dynamic analysis loop to completion or deterministic failure."""

        if self.run_mode == "resume":
            self.state.normalize_for_resume(verifier_chain_length=len(self._verifier_chain))
        else:
            self.state = DynamicSessionState(self.state_file)
            self.state.save()

        self._rebuild_pending_claim_retries()
        self._rebuild_pending_reporter_claim_ids()

        # Chat dashboard: --interactive without an injected test channel.
        # Tests inject a ScriptedInteractionChannel and route through _run_batch
        # to keep deterministic-ordering semantics.
        if self.interactive and not self._injected_interaction_channel:
            return self._run_chat()
        return self._run_batch()

    def _run_batch(self) -> PhaseResult:
        """Batch execution: captain runs as programmatic turns (non-interactive)."""

        try:
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
                made_progress |= self._schedule_reporters()
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
        except KeyboardInterrupt:
            print("\nInterrupted (Ctrl-C). Killing active subprocesses…", flush=True)
            self.kill_active()
            return PhaseResult(success=False, failure_context="interrupted by user (Ctrl-C)")
        finally:
            self.kill_active()
            if self._interaction_channel is not None:
                self._interaction_channel.stop()
            _flush_stdin_buffer()
            self._worker_executor.shutdown(wait=False, cancel_futures=True)
            self._verifier_executor.shutdown(wait=False, cancel_futures=True)
            self._reporter_executor.shutdown(wait=False, cancel_futures=True)

    def _run_chat(self) -> PhaseResult:
        """Chat-dashboard execution: captain runs on a background thread; the user
        types directives at any moment via a Rich Live dashboard."""

        from juvenal.dynamic.chat_display import make_chat_dashboard

        self.display.pause()
        if self._injected_chat_dashboard is not None:
            self._dashboard = self._injected_chat_dashboard
        else:
            plain = getattr(self.display, "_plain", False)
            self._dashboard = make_chat_dashboard(plain=plain)
        self._captain_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="juvenal-captain")
        self._last_dashboard_event_seq = max((event.seq for event in self.state.events), default=0)

        try:
            if self._interaction_channel is not None:
                self._interaction_channel.start()
            self._dashboard.start()
            self._paint_dashboard()

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
                made_progress |= self._schedule_reporters()
                made_progress |= self._apply_continuous_directives()
                made_progress |= self._drain_captain_future()

                if self._chat_pending and self._captain_future is None:
                    self._enter_chat_mode()
                    made_progress = True

                terminate, success, reason = self._should_terminate()
                if terminate:
                    if not success:
                        self.kill_active()
                    return PhaseResult(success=success, failure_context=reason if not success else "")

                if self._captain_future is None and (self._force_captain_turn or self._needs_captain_turn()):
                    self._dispatch_captain_turn()
                    self._force_captain_turn = False
                    made_progress = True

                self._emit_pending_dashboard_events()
                self._paint_dashboard()

                if not made_progress:
                    time.sleep(_IDLE_SLEEP_SECONDS)
        except KeyboardInterrupt:
            print("\n[chat] interrupted (Ctrl-C). Killing active subprocesses…", flush=True)
            self.kill_active()
            return PhaseResult(success=False, failure_context="interrupted by user (Ctrl-C)")
        finally:
            # Always kill subprocesses first so their wait() calls return and
            # the executor threads (which are non-daemon) can exit. Without
            # this, Ctrl-C requires multiple presses because Python won't
            # exit while a thread is blocked in subprocess.Popen.wait().
            self.kill_active()
            if self._captain_executor is not None:
                self._captain_executor.shutdown(wait=False, cancel_futures=True)
            if self._dashboard is not None:
                try:
                    self._dashboard.stop()
                except Exception:
                    pass
            if self._interaction_channel is not None:
                self._interaction_channel.stop()
            # Flush any partial input the user typed before Ctrl-C — otherwise
            # it gets fed to the parent shell when juvenal exits.
            _flush_stdin_buffer()
            self._worker_executor.shutdown(wait=False, cancel_futures=True)
            self._verifier_executor.shutdown(wait=False, cancel_futures=True)
            self._reporter_executor.shutdown(wait=False, cancel_futures=True)

    def _apply_continuous_directives(self) -> bool:
        if self._interaction_channel is None or self.state.control.stop_requested:
            return False
        lines = self._interaction_channel.poll(0.0)
        if not lines:
            return False
        changed = False
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            self._chat_history.append(stripped)
            directive_id = self._next_directive_id()
            try:
                directive = parse_user_directive(stripped, directive_id=directive_id)
            except ValueError as exc:
                if self._dashboard is not None:
                    self._dashboard.render_event(
                        kind="info",
                        text=f"ignored {stripped!r}: {exc}",
                    )
                continue
            applied = self._persist_directive(directive)
            changed |= applied
            if self._dashboard is not None and applied:
                self._dashboard.render_event(
                    kind="directive.applied",
                    text=f"{directive.kind} {directive.text}".strip(),
                )
        if self._dashboard is not None:
            self._dashboard.render_chat_input(self._chat_history[-8:])
        return changed

    def _dispatch_captain_turn(self) -> None:
        if self._captain_executor is None or self._captain_future is not None:
            return
        if self._dashboard is not None:
            self._dashboard.render_event(
                kind="captain.starting",
                text=f"turn #{self.state.captain.turn_index + 1}",
            )
        self._captain_future = self._captain_executor.submit(self._run_captain_turn)

    def _captain_chunk_callback(self) -> Callable[[str], None] | None:
        """Return a backend display_callback that streams to the dashboard.

        Active only when a chat dashboard is mounted (chat mode). In batch
        mode there is no dashboard and the callback is None.
        """
        dashboard = self._dashboard
        if dashboard is None or not hasattr(dashboard, "render_captain_chunk"):
            return None

        def on_chunk(text: str) -> None:
            try:
                dashboard.render_captain_chunk(text)
            except Exception:
                pass

        return on_chunk

    def _drain_captain_future(self) -> bool:
        if self._captain_future is None:
            return False
        if not self._captain_future.done():
            return False
        future = self._captain_future
        self._captain_future = None
        try:
            future.result()
        except Exception as exc:
            if self._dashboard is not None:
                self._dashboard.render_event(kind="captain.error", text=str(exc))
            self._terminal_failure = f"captain turn raised: {exc}"
            return True
        if self._dashboard is not None:
            self._dashboard.render_captain(
                message_to_user=self.state.captain.last_message_to_user,
                mental_model_summary=self.state.captain.mental_model_summary,
                open_questions=list(self.state.captain.open_questions),
                turn_index=self.state.captain.turn_index,
            )
            self._dashboard.render_event(
                kind="captain.turn",
                text=f"turn #{self.state.captain.turn_index} finished",
            )
        return True

    def _emit_pending_dashboard_events(self) -> None:
        if self._dashboard is None:
            return
        for event in self.state.events:
            if event.seq <= self._last_dashboard_event_seq:
                continue
            if event.event_type not in _DASHBOARD_EVENT_KINDS:
                self._last_dashboard_event_seq = event.seq
                continue
            text = self._format_event_for_dashboard(event)
            self._dashboard.render_event(kind=event.event_type, text=text)
            self._last_dashboard_event_seq = event.seq

    def _format_event_for_dashboard(self, event: Any) -> str:
        parts: list[str] = []
        if event.target_id:
            parts.append(f"target={event.target_id}")
        if event.claim_id:
            parts.append(f"claim={event.claim_id}")
        if event.directive_id:
            parts.append(f"directive={event.directive_id}")
        return " ".join(parts) or event.event_type

    def _paint_dashboard(self) -> None:
        if self._dashboard is None:
            return
        counts: dict[str, int] = {}
        for target in self._frontier_targets():
            counts[target.status] = counts.get(target.status, 0) + 1
        active = [(target.target_id, target.status) for target in self._frontier_targets()]
        self._dashboard.render_frontier(counts, active)

    def kill_active(self) -> None:
        """Kill all active subprocesses owned by the runner and signal any
        background threads (e.g. rate-limit sleep) to bail out promptly."""

        self._shutdown_event.set()
        for backend in set(self._backend_by_name.values()):
            backend.kill_active()

    def _needs_captain_turn(self) -> bool:
        if self._terminal_failure:
            return False
        if self.state.control.stop_requested:
            return False
        if self.state.control.wrap_requested:
            return self.state.control.wrap_summary_pending and not self._has_active_runtime_work()

        if self._pending_continue_nudge and not self._has_active_runtime_work():
            return True

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

        # Heuristic threshold for "ask captain for more targets" — keep this
        # tied to max_workers (a pacing knob) regardless of the shared/legacy
        # mode, so behavior stays predictable and the test fixtures that
        # set max_workers=1 to force serial dispatch still work.
        return len(frontier) < self.config.max_workers and current_snapshot != self._last_captain_snapshot

    def _run_captain_turn(self) -> None:
        summary_only = self.state.control.wrap_requested and self.state.control.wrap_summary_pending
        prompt = self._build_captain_prompt(summary_only=summary_only)
        backend = self._get_backend(self.config.captain_backend)
        session_id = self.state.captain.session_id
        display_callback = self._captain_chunk_callback()

        captain_model = _resolve_model(self.config.captain_backend, "captain", self.config.captain_model)
        if session_id:
            result = backend.resume_agent(
                session_id,
                prompt,
                working_dir=str(self.working_dir),
                display_callback=display_callback,
                timeout=self.phase.timeout,
                env=self._role_env("captain"),
                model=captain_model,
            )
        else:
            result = backend.run_agent(
                prompt,
                working_dir=str(self.working_dir),
                display_callback=display_callback,
                timeout=self.phase.timeout,
                env=self._role_env("captain"),
                model=captain_model,
            )

        if result.session_id:
            self.state.captain.session_id = result.session_id
            self.state.save()

        self._add_tokens(result)
        if result.exit_code != 0:
            # Captain crash — likely rate limit. Backoff and retry on next loop iteration.
            self._consecutive_errors += 1
            self._rate_limit_backoff()
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
        if turn.termination_state == "continue":
            self._consecutive_premature_completes = 0
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
                display_callback=self._captain_chunk_callback(),
                timeout=self.phase.timeout,
                env=self._role_env("captain"),
                model=_resolve_model(self.config.captain_backend, "captain", self.config.captain_model),
            )
            if result.session_id:
                self.state.captain.session_id = result.session_id
                self.state.save()
            self._add_tokens(result)
            if result.exit_code != 0:
                # Captain repair crash — likely rate limit
                self._consecutive_errors += 1
                self._rate_limit_backoff()
                return None
            try:
                return parse_captain_output(result.output)
            except ValueError as exc:
                last_error = str(exc)

        self._terminal_failure = f"captain output remained malformed after repair: {last_error}"
        return None

    def _available_worker_slots(self) -> int:
        """Slots available for new worker dispatch.

        In shared mode: limited by both the combined budget (max_agents minus
        all in-flight worker+verifier futures) and the per-role pool cap (which
        equals max_agents in shared mode, so this is effectively just the
        combined budget).
        In legacy mode: per-role budget independent of verifier dispatch.
        """
        worker_role_avail = self._max_worker_cap - len(self._worker_futures)
        if not self.config.shared_agent_budget:
            return max(0, worker_role_avail)
        in_flight = len(self._worker_futures) + len(self._verifier_futures)
        combined_avail = self._max_agents - in_flight
        return max(0, min(worker_role_avail, combined_avail))

    def _available_verifier_slots(self) -> int:
        """Slots available for new verifier dispatch.

        Verifier scheduling runs before worker scheduling in the main loop, so
        in shared mode verifiers naturally preempt workers — newly proposed
        claims get dispatched ahead of newly enqueued targets within the same
        budget.
        """
        verifier_role_avail = self._max_verifier_cap - len(self._verifier_futures)
        if not self.config.shared_agent_budget:
            return max(0, verifier_role_avail)
        in_flight = len(self._worker_futures) + len(self._verifier_futures)
        combined_avail = self._max_agents - in_flight
        return max(0, min(verifier_role_avail, combined_avail))

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

        available = self._available_worker_slots()
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
        available = self._available_worker_slots()
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
        chain_length = len(self._verifier_chain)
        for claim in self.state.claims.values():
            target = self.state.targets.get(claim.target_id)
            if target is None:
                continue
            if claim.status not in {"proposed", "verifying"}:
                continue
            if target.active_generation != claim.generation:
                continue

            claim_verifications = [
                self.state.verifications[v_id] for v_id in claim.verification_ids if v_id in self.state.verifications
            ]
            if any(v.status in {"pending", "running"} for v in claim_verifications):
                continue
            passed_indices = {
                v.verifier_index for v in claim_verifications if v.disposition == "verified" and v.status == "passed"
            }
            next_index = max(passed_indices) + 1 if passed_indices else 0
            if next_index >= chain_length:
                continue

            spec = self._verifier_chain[next_index]
            verification = VerificationRecord(
                verification_id=self._next_verification_id(claim.claim_id),
                claim_id=claim.claim_id,
                target_id=claim.target_id,
                generation=claim.generation,
                backend=spec.backend,
                verifier_role="analysis-verifier",
                session_id=None,
                status="pending",
                disposition=None,
                reason="",
                rejection_class=None,
                raw_output="",
                started_at=None,
                completed_at=None,
                verifier_name=spec.name,
                verifier_index=next_index,
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

        available = self._available_verifier_slots()
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
            prompt = self._build_verifier_prompt(target, claim, verification)
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

        for future, claim_id in list(self._reporter_futures.items()):
            if not future.done():
                continue
            progressed = True
            self._reporter_futures.pop(future, None)
            try:
                result = future.result()
            except Exception as exc:  # pragma: no cover - defensive, reporter wrapper catches normally
                claim = self.state.claims.get(claim_id)
                if claim is not None:
                    fake = _ReporterExecutionResult(
                        claim_id=claim.claim_id,
                        target_id=claim.target_id,
                        generation=claim.generation,
                        agent_result=AgentResult(
                            exit_code=1, output="", transcript="", duration=0.0, input_tokens=0, output_tokens=0
                        ),
                        error=f"future crashed: {exc}",
                    )
                    self._apply_reporter_result(fake)
                continue
            self._apply_reporter_result(result)

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
        if directive.kind == "now":
            return self._apply_now_directive(directive)
        if directive.kind == "show":
            return self._apply_show_directive(directive)
        if directive.kind == "chat":
            return self._apply_chat_directive(directive)
        return self._queue_captain_directive(directive)

    def _apply_chat_directive(self, directive: UserDirective) -> bool:
        directive.status = "applied"
        self.state.directives[directive.directive_id] = directive
        self.state.save()
        self._chat_pending = True
        if self._dashboard is not None:
            if self._captain_future is not None:
                msg = "/chat queued — will hand off to native TUI when current captain turn finishes"
            else:
                msg = "/chat queued — handing off to native TUI on next loop tick"
            self._dashboard.render_event(kind="info", text=msg)
        return True

    def _enter_chat_mode(self) -> None:
        """Suspend the dashboard and hand the terminal to the backend's native
        interactive TUI (claude --resume <id> or codex resume <id>) so the user
        can chat with the captain directly. On exit, restart the dashboard and
        flag the next captain turn for re-priming back to the structured
        protocol."""

        self._chat_pending = False
        session_id = self.state.captain.session_id
        if not session_id:
            if self._dashboard is not None:
                self._dashboard.render_event(
                    kind="info",
                    text="/chat skipped: captain has no session yet (run for one turn first)",
                )
            return

        backend = self._get_backend(self.config.captain_backend)
        captain_model = _resolve_model(self.config.captain_backend, "captain", self.config.captain_model)

        # Suspend dashboard + interaction channel so the native TUI owns the
        # terminal cleanly. Both restart in `finally`.
        if self._dashboard is not None:
            try:
                self._dashboard.stop()
            except Exception:
                pass
        if self._interaction_channel is not None:
            try:
                self._interaction_channel.stop()
            except Exception:
                pass

        print(
            f"\n[chat] handing terminal to {backend.name()} (session {session_id[:8]}…). "
            "Type your messages directly. Exit the TUI (Ctrl+D, /exit, or whatever the CLI "
            "supports) to return to Juvenal.\n",
            flush=True,
        )

        try:
            backend.resume_interactive(
                session_id,
                working_dir=str(self.working_dir),
                env=self._role_env("captain"),
                model=captain_model,
            )
        except NotImplementedError as exc:
            print(f"[chat] {exc}", flush=True)
        except Exception as exc:
            print(f"[chat] failed: {exc}", flush=True)
        finally:
            print("\n[chat] returning to Juvenal-driven analysis.\n", flush=True)
            self._post_chat_reprime = True
            if self._interaction_channel is not None and not self._injected_interaction_channel:
                try:
                    self._interaction_channel.start()
                except Exception:
                    pass
            if self._dashboard is not None:
                try:
                    self._dashboard.start()
                except Exception:
                    pass

    def _apply_now_directive(self, directive: UserDirective) -> bool:
        directive.status = "applied"
        self.state.directives[directive.directive_id] = directive
        self.state.save()
        self._force_captain_turn = True
        return True

    def _apply_show_directive(self, directive: UserDirective) -> bool:
        directive.status = "applied"
        self.state.directives[directive.directive_id] = directive
        self.state.save()
        topic = directive.text.strip()
        if topic == "captain" and self._dashboard is not None:
            self._dashboard.show_captain_full(
                message_to_user=self.state.captain.last_message_to_user,
                mental_model_summary=self.state.captain.mental_model_summary,
                open_questions=list(self.state.captain.open_questions),
            )
        return True

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
            if self._completion_floors_met():
                return True, True, ""
            if self._consecutive_premature_completes >= self.config.max_premature_completes:
                print(
                    "Captain repeatedly declared `complete` despite floors "
                    f"({self._consecutive_premature_completes} consecutive overrides); "
                    "accepting completion via soft escape.",
                    flush=True,
                )
                return True, True, ""
            self._consecutive_premature_completes += 1
            self._pending_continue_nudge = self._compose_continue_nudge()
            self._captain_termination_state = "continue"
            self._captain_termination_reason = ""
            return False, False, ""

        if not frontier and not self._has_active_runtime_work():
            if self._pending_continue_nudge:
                return False, False, ""
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
                # If every target reached a terminal state, the analysis itself is done — the
                # captain just failed its protocol obligation to declare "complete". Don't burn
                # a long-running analysis (often hours of compute and a usable set of verified
                # findings) over a captain protocol slip. Log a warning and exit cleanly.
                if all_terminal:
                    print(
                        "Captain did not request completion despite all targets reaching "
                        "terminal states; treating analysis as complete.",
                        flush=True,
                    )
                    return True, True, ""
                return True, False, "captain left the frontier empty without requesting completion"

        return False, False, ""

    def _count_terminal_targets(self) -> int:
        return sum(
            1
            for target in self.state.targets.values()
            if target.status in _TERMINAL_TARGET_STATUSES and not self._is_target_ignored(target)
        )

    def _completion_floors_met(self) -> bool:
        if self.state.captain.turn_index < self.config.min_captain_turns:
            return False
        if self._count_terminal_targets() < self.config.min_terminal_targets_before_complete:
            return False
        return True

    def _compose_continue_nudge(self) -> str:
        template = self.config.continue_nudge or _DEFAULT_CONTINUE_NUDGE
        try:
            return template.format(
                turns=self.state.captain.turn_index,
                terminal=self._count_terminal_targets(),
                min_captain_turns=self.config.min_captain_turns,
                min_terminal_targets=self.config.min_terminal_targets_before_complete,
                max_premature_completes=self.config.max_premature_completes,
                consecutive=self._consecutive_premature_completes,
            )
        except (KeyError, IndexError, ValueError):
            return template

    def _captain_context_dir(self) -> Path:
        return self.working_dir / ".juvenal"

    def _write_captain_context_files(self) -> None:
        """Persist the canonical captain-context state to .juvenal/ so the
        captain can Read / Grep them on demand instead of receiving everything
        re-stuffed into every prompt. Coding agents extract from files better
        than they parse a 100KB blob — this lets the captain pull what it
        actually needs and keeps the per-turn prompt focused on what's NEW."""

        ctx = self._captain_context_dir()
        ctx.mkdir(parents=True, exist_ok=True)

        # frontier.json — current non-terminal targets with full instructions.
        frontier = {
            "counts": self._frontier_count_dict(),
            "active_targets": [self._target_prompt_summary(target) for target in self._frontier_targets()],
        }
        self._atomic_write(ctx / "frontier.json", json.dumps(frontier, indent=2, sort_keys=True))

        # mental_model.md — captain's most recent structured mental model.
        mental = self.state.captain.mental_model_summary or "(none yet)"
        open_qs = self.state.captain.open_questions
        body = f"# Captain mental model\n\nTurn: {self.state.captain.turn_index}\n\n## Mental model\n\n{mental}\n"
        if open_qs:
            body += "\n## Open questions\n\n" + "\n".join(f"- {q}" for q in open_qs) + "\n"
        self._atomic_write(ctx / "mental_model.md", body)

        # claims.json — every verified + rejected claim with full detail. Used
        # for variant-analysis lookups and for confirming what's been found.
        claims = {
            "verified": [
                self._claim_full_payload(claim) for claim in self.state.claims.values() if claim.status == "verified"
            ],
            "rejected": [
                self._claim_full_payload(claim) for claim in self.state.claims.values() if claim.status == "rejected"
            ],
        }
        self._atomic_write(ctx / "claims.json", json.dumps(claims, indent=2, sort_keys=True))

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)

    def _frontier_count_dict(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for target in self._frontier_targets():
            counts[target.status] = counts.get(target.status, 0) + 1
        return counts

    def _claim_full_payload(self, claim: ClaimRecord) -> dict[str, Any]:
        payload = self._claim_prompt_summary(claim)
        payload["status"] = claim.status
        payload["target_id"] = claim.target_id
        payload["rejection_class"] = claim.rejection_class
        payload["rejection_reason"] = self._latest_rejection_reason(claim)
        return payload

    def _build_captain_prompt(self, *, summary_only: bool = False) -> str:
        nudge = self._pending_continue_nudge
        self._pending_continue_nudge = ""
        post_chat = self._post_chat_reprime
        self._post_chat_reprime = False

        # Persist canonical state to .juvenal/ before this turn so the captain
        # can Read them on demand instead of getting them re-stuffed in the
        # prompt. Frontier and claims grow unbounded across turns; refeeding
        # them inline buries the per-turn signal under noise and (separately)
        # blew past Linux's argv cap on long runs before we piped via stdin.
        self._write_captain_context_files()

        delta = self.state.pending_captain_delta()
        pending_directives = [
            asdict(self.state.directives[directive_id])
            for directive_id in delta.pending_directive_ids
            if directive_id in self.state.directives
        ]
        delta_summary = {
            "verified_claims": list(delta.verified_claim_ids),
            "rejected_claims": list(delta.rejected_claim_ids),
            "no_findings_targets": list(delta.no_findings_target_ids),
            "blocked_targets": list(delta.blocked_target_ids),
            "exhausted_targets": list(delta.exhausted_target_ids),
            "frontier_counts": delta.frontier_counts,
        }
        mission = self.phase.render_prompt(failure_context=self.failure_context, vars=self.workflow.vars)
        mode_note = (
            "This is a final wrap summary turn. Do not enqueue new targets and set termination_state to complete."
            if summary_only
            else "Plan the next bounded analysis work."
        )

        ctx = self._captain_context_dir()
        is_first_turn = self.state.captain.turn_index == 0
        files_block = (
            "Canonical state files (read on demand):\n"
            f"  - {ctx / 'frontier.json'} — current non-terminal targets with full instructions\n"
            f"  - {ctx / 'mental_model.md'} — your most recent mental model\n"
            f"  - {ctx / 'claims.json'} — every verified and rejected claim with full detail\n"
            "  These files are rewritten before every captain turn. Use Read / Grep to pull "
            "specific items when you need them — do not assume the prompt contains complete state.\n"
        )

        if is_first_turn:
            prompt = (
                f"{self._captain_role_prompt}\n\n"
                f"Mission:\n{mission}\n\n"
                f"Repository root: {self.working_dir}\n"
                f"Captain turn: 1\n"
                f"Mode: {mode_note}\n\n"
                f"{files_block}\n"
                "Pending user directives:\n"
                f"{json.dumps(pending_directives, indent=2)}\n"
            )
        else:
            prompt = (
                f"Captain turn: {self.state.captain.turn_index + 1}\n"
                f"Mode: {mode_note}\n\n"
                f"{files_block}\n"
                "Event delta since your last turn (claim/target IDs only — read claims.json "
                "and frontier.json for details):\n"
                f"{json.dumps(delta_summary, indent=2)}\n\n"
                "Pending user directives:\n"
                f"{json.dumps(pending_directives, indent=2)}\n"
            )

        if nudge and not summary_only:
            prompt = f"{nudge}\n\n{prompt}"
        if post_chat and not summary_only:
            prompt = (
                "## Resuming from free-form chat\n\n"
                "The user just had a free-form interactive conversation with you in their "
                "terminal. Acknowledge any directions they gave you in `message_to_user`, "
                "then RETURN to the structured analysis protocol — your next response must "
                "include exactly one CAPTAIN_JSON block as defined in the role prompt. Do "
                "not respond conversationally; the runner only consumes structured output "
                "going forward.\n\n"
            ) + prompt
        return prompt

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

    def _build_verifier_prompt(self, target: TargetRecord, claim: ClaimRecord, verification: VerificationRecord) -> str:
        packet = asdict(claim_to_verifier_packet(claim))
        mission = self.phase.render_prompt(failure_context=self.failure_context, vars=self.workflow.vars)
        chain_length = len(self._verifier_chain)
        spec = self._verifier_chain[verification.verifier_index]
        rendered_scope = self._rendered_verifier_prompts.get(spec.name, "")
        scope_block = ""
        if rendered_scope:
            scope_block = f"Specialized verifier scope:\n```text\n{rendered_scope}\n```\n\n"

        passed_names: list[str] = []
        for v_id in claim.verification_ids:
            v = self.state.verifications.get(v_id)
            if v is None or v.verification_id == verification.verification_id:
                continue
            if v.disposition == "verified" and v.status == "passed":
                passed_names.append(v.verifier_name or "default")
        next_name = (
            self._verifier_chain[verification.verifier_index + 1].name
            if verification.verifier_index + 1 < chain_length
            else None
        )
        chain_context = {
            "you_are": f"verifier {verification.verifier_index + 1} of {chain_length}",
            "your_name": verification.verifier_name or "default",
            "earlier_verifiers_passed": passed_names,
            "next_verifier": next_name if next_name else "(none — final verifier)",
        }
        return (
            f"{self._verifier_role_prompt}\n\n"
            f"{scope_block}"
            f"Repository root: `{self.working_dir}`\n\n"
            "Chain context:\n"
            f"```text\n{json.dumps(chain_context, indent=2)}\n```\n\n"
            "Mission scope and context (from the analysis phase configuration):\n"
            f"```text\n{mission}\n```\n\n"
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
        worker_model = _resolve_model(self.config.worker_backend, "worker", self.config.worker_model)
        if attempt.parent_session_id:
            # Claim retry: resume the prior worker's session so its context
            # (codebase reading, build state, prior reasoning) carries over
            # and the rejection feedback arrives as a continuation rather
            # than a cold restart.
            result = backend.resume_agent(
                attempt.parent_session_id,
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("worker"),
                model=worker_model,
            )
        else:
            result = backend.run_agent(
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("worker"),
                model=worker_model,
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
        backend = self._get_backend(verification.backend)
        spec = self._verifier_chain[verification.verifier_index]
        result = backend.run_agent(
            prompt,
            working_dir=str(self.working_dir),
            timeout=self.phase.timeout,
            env=self._role_env("verifier", verifier_name=verification.verifier_name),
            model=_resolve_model(spec.backend, "verifier", spec.model),
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

    # --- Reporter (post-verification per-claim report writer) ----------------

    def _bug_id_for_claim(self, claim: ClaimRecord) -> str:
        """Stable directory name for a verified claim's report output."""
        return claim.claim_id

    def _report_dir_for_claim(self, claim: ClaimRecord) -> Path:
        return self.working_dir / "output" / self._bug_id_for_claim(claim)

    def _build_reporter_prompt(self, claim: ClaimRecord, target: TargetRecord) -> str:
        bug_id = self._bug_id_for_claim(claim)
        report_dir = self._report_dir_for_claim(claim)
        # Collect every passing verifier's structured output for the agent's reference.
        verifier_summaries: list[dict[str, Any]] = []
        for v_id in claim.verification_ids:
            v = self.state.verifications.get(v_id)
            if v is None:
                continue
            if v.disposition != "verified" or v.status != "passed":
                continue
            verifier_summaries.append(
                {
                    "verifier_name": v.verifier_name or "default",
                    "verifier_index": v.verifier_index,
                    "summary": v.reason,
                    "follow_up_action": v.follow_up_action,
                    "follow_up_strategy": v.follow_up_strategy,
                }
            )
        packet = asdict(claim_to_verifier_packet(claim))
        worker_artifact = self.state.worker_artifacts.get(claim.audit_artifact_id)
        artifact_payload = asdict(worker_artifact) if worker_artifact is not None else None
        scope_block = ""
        if self._rendered_reporter_prompt:
            scope_block = f"Specialized reporter scope:\n```text\n{self._rendered_reporter_prompt}\n```\n\n"
        return (
            "You are the reporter agent for Juvenal's dynamic `analysis` phase.\n"
            "A claim has passed every verifier in the chain. Your job is to write a "
            "human-readable per-bug report and copy/include any PoC artifacts so the "
            "finding is durably captured on disk.\n\n"
            f"{scope_block}"
            f"Repository root: `{self.working_dir}`\n"
            f"Report directory (you MUST create this and write into it): `{report_dir}`\n"
            f"Bug id: `{bug_id}`\n\n"
            "Required output:\n"
            f"- Create the directory `{report_dir}` if it does not already exist.\n"
            f"- Write a Markdown file at `{report_dir}/report.md` that includes:\n"
            "  - Title (one line)\n"
            "  - Severity (critical / high / medium / low) with one-sentence justification\n"
            "  - Primary location (file:line) and any secondary locations\n"
            "  - Description: what the bug is and why it matters\n"
            "  - Proof of Concept: the exact reproduction steps, input, or script. "
            "Include any sanitizer/crash output verbatim if present in the claim packet.\n"
            "  - Impact: what an attacker can achieve\n"
            "  - Verifier consensus: a brief note that each verifier passed (poc, scope, novelty, etc.)\n"
            f"- Place any concrete PoC artifacts (input files, scripts, payloads) into `{report_dir}/`. "
            f"At minimum, write a `{report_dir}/poc` file (or `poc.<ext>`) capturing the trigger.\n"
            "- Overwriting an existing report at this path is acceptable — this step is idempotent.\n\n"
            "Do NOT write outside the report directory except for transient working files. "
            "Do NOT modify project source. After writing, exit cleanly. No structured-output "
            "block is expected from you — the runner verifies success by checking that "
            f"`{report_dir}/report.md` exists.\n\n"
            "Claim packet:\n"
            f"```text\n{json.dumps(packet, indent=2)}\n```\n\n"
            "Worker artifact (reasoning, trace, commands):\n"
            f"```text\n{json.dumps(artifact_payload, indent=2, default=str)}\n```\n\n"
            "Target context:\n"
            f"```text\n{json.dumps(self._target_prompt_summary(target), indent=2)}\n```\n\n"
            "Verifier consensus (all of these PASSED):\n"
            f"```text\n{json.dumps(verifier_summaries, indent=2)}\n```\n\n"
            "Code context pack:\n"
            f"```text\n{json.dumps(self._code_context_payload(target), indent=2)}\n```\n"
        )

    def _schedule_reporters(self) -> bool:
        """Submit reporter agent runs for any verified-but-not-reported claims."""
        if self._reporter_spec is None:
            return False
        if self._terminal_failure or self.state.control.stop_requested:
            return False
        if not self._pending_reporter_claim_ids:
            return False
        # Bound reporter parallelism by the executor's worker count.
        available = max(1, self.config.max_workers) - len(self._reporter_futures)
        if available <= 0:
            return False

        scheduled = False
        remaining: list[str] = []
        for claim_id in self._pending_reporter_claim_ids:
            if available <= 0:
                remaining.append(claim_id)
                continue
            if claim_id in self._reporter_futures.values():
                continue
            claim = self.state.claims.get(claim_id)
            if claim is None or claim.status != "verified" or claim.reported_at is not None:
                continue
            target = self.state.targets.get(claim.target_id)
            if target is None:
                continue
            attempts = self._reporter_attempts.get(claim_id, 0)
            if attempts >= _MAX_REPORTER_ATTEMPTS:
                # Give up for this run; leave reported_at unset so resume can try again.
                continue
            prompt = self._build_reporter_prompt(claim, target)
            future = self._reporter_executor.submit(self._execute_reporter, claim, prompt)
            self._reporter_futures[future] = claim_id
            self._reporter_attempts[claim_id] = attempts + 1
            available -= 1
            scheduled = True

        self._pending_reporter_claim_ids = remaining
        return scheduled

    def _execute_reporter(self, claim: ClaimRecord, prompt: str) -> _ReporterExecutionResult:
        spec_backend = self._reporter_spec.backend if self._reporter_spec else "claude"
        spec_model = self._reporter_spec.model if self._reporter_spec else None
        backend = self._get_backend(spec_backend)
        result = backend.run_agent(
            prompt,
            working_dir=str(self.working_dir),
            timeout=self.phase.timeout,
            env=self._role_env("reporter"),
            model=_resolve_model(spec_backend, "reporter", spec_model),
        )
        if result.exit_code != 0:
            return _ReporterExecutionResult(
                claim_id=claim.claim_id,
                target_id=claim.target_id,
                generation=claim.generation,
                agent_result=result,
                error=f"reporter exited with code {result.exit_code}: {result.output[-2000:]}",
            )
        report_md = self._report_dir_for_claim(claim) / "report.md"
        if not report_md.is_file():
            return _ReporterExecutionResult(
                claim_id=claim.claim_id,
                target_id=claim.target_id,
                generation=claim.generation,
                agent_result=result,
                error=f"reporter completed but {report_md} does not exist",
            )
        return _ReporterExecutionResult(
            claim_id=claim.claim_id,
            target_id=claim.target_id,
            generation=claim.generation,
            agent_result=result,
            error=None,
        )

    def _apply_reporter_result(self, result: _ReporterExecutionResult) -> None:
        self._add_tokens(result.agent_result)
        claim = self.state.claims.get(result.claim_id)
        if claim is None:
            return
        if result.error:
            # Leave reported_at unset; _schedule_reporters will retry up to _MAX_REPORTER_ATTEMPTS.
            attempts = self._reporter_attempts.get(claim.claim_id, 0)
            if attempts < _MAX_REPORTER_ATTEMPTS:
                if claim.claim_id not in self._pending_reporter_claim_ids:
                    self._pending_reporter_claim_ids.append(claim.claim_id)
            else:
                print(
                    f"\n[juvenal] reporter for claim {claim.claim_id} failed after "
                    f"{_MAX_REPORTER_ATTEMPTS} attempts: {result.error}",
                    flush=True,
                )
            return
        claim.reported_at = time.time()
        self.state.append_event(
            "claim.reported",
            target_id=claim.target_id,
            claim_id=claim.claim_id,
            generation=claim.generation,
        )
        self.state.save()

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

            is_final = verification.verifier_index == len(self._verifier_chain) - 1
            if not is_final:
                # More verifiers in the chain. Keep the claim in `verifying`; the next
                # _schedule_verifiers tick will create the next chain step. Do NOT call
                # _refresh_target_after_verification here — the target is not done yet.
                claim.status = "verifying"
                self.state.save()
                return

            claim.status = "verified"
            claim.rejection_class = None
            claim.failing_verifier_name = None
            claim.verified_at = verification.completed_at
            claim.rejected_at = None
            self.state.append_event(
                "claim.verified",
                target_id=target.target_id,
                claim_id=claim.claim_id,
                generation=verification.generation,
            )
            self._refresh_target_after_verification(target)
            if (
                self._reporter_spec is not None
                and claim.reported_at is None
                and claim.claim_id not in self._pending_reporter_claim_ids
                and claim.claim_id not in self._reporter_futures.values()
            ):
                self._pending_reporter_claim_ids.append(claim.claim_id)
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
        claim.failing_verifier_name = verification.verifier_name
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
        # Resume from the most recent ancestor session so the worker keeps its
        # context (codebase mental model, build state, prior reasoning).
        # Priority: latest direct retry of this claim → original attempt that
        # produced the claim → no resume (cold start).
        parent_session_id: str | None = None
        if existing:
            most_recent = max(existing, key=lambda a: a.started_at or 0.0)
            parent_session_id = most_recent.session_id
        if parent_session_id is None:
            origin = self.state.worker_attempts.get(claim.attempt_id)
            if origin is not None:
                parent_session_id = origin.session_id
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
            parent_session_id=parent_session_id,
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
        failing_verifier_name = (
            claim.failing_verifier_name
            or (latest_verification.verifier_name if latest_verification else "")
            or "default"
        )
        failing_verifier_index = latest_verification.verifier_index if latest_verification else 0
        chain_length = len(self._verifier_chain)
        verified_siblings = [
            self._claim_prompt_summary(c)
            for c in self._active_claims_for_target(target)
            if c.claim_id != claim.claim_id and c.status == "verified"
        ]
        rejected_detail = self._claim_prompt_summary(claim)
        rejected_detail["rejection_reason"] = rejection_reason
        rejected_detail["rejection_class"] = claim.rejection_class
        rejected_detail["failing_verifier_name"] = failing_verifier_name
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
            f"Your previous claim was REJECTED by the **{failing_verifier_name}** verifier "
            f"(verifier {failing_verifier_index + 1} of {chain_length} in this analysis chain). "
            "Address that verifier's specific scope. If you push past their concern, the next "
            "verifier in the chain will then run.\n\n"
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
            f"- The rejecting verifier was the **{failing_verifier_name}** verifier;"
            " address their specific scope, not the other verifiers' scopes\n"
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
        if self._worker_futures or self._verifier_futures or self._reporter_futures:
            return True
        if self._pending_claim_retries or self._pending_reporter_claim_ids:
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
                backend=verification.backend,
                verifier_role=verification.verifier_role,
                session_id=None,
                status="pending",
                disposition=None,
                reason="",
                rejection_class=None,
                raw_output="",
                started_at=None,
                completed_at=None,
                verifier_name=verification.verifier_name,
                verifier_index=verification.verifier_index,
            )
            self.state.verifications[new_verification.verification_id] = new_verification
            claim.verification_ids.append(new_verification.verification_id)
            if new_verification.verification_id not in target.pending_verification_ids:
                target.pending_verification_ids.append(new_verification.verification_id)
        else:
            claim.status = "rejected"
            claim.rejection_class = "verification-error"
            claim.failing_verifier_name = verification.verifier_name
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
                        "verifier_name": v.verifier_name or "default",
                        "verifier_index": v.verifier_index,
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

    def _rebuild_pending_reporter_claim_ids(self) -> None:
        """Re-queue verified-but-not-reported claims after resume.

        No-op if no reporter is configured. Idempotent: a claim already in the
        queue or with an in-flight reporter future is not requeued.
        """
        self._pending_reporter_claim_ids = []
        if self._reporter_spec is None:
            return
        in_flight = set(self._reporter_futures.values())
        queued = set(self._pending_reporter_claim_ids)
        for claim in self.state.claims.values():
            if claim.status != "verified":
                continue
            if claim.reported_at is not None:
                continue
            if claim.claim_id in in_flight or claim.claim_id in queued:
                continue
            self._pending_reporter_claim_ids.append(claim.claim_id)

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
        When consecutive errors hit the threshold, saves state and sleeps with
        exponential backoff (likely rate limit or API outage), then resets and continues.
        """
        self._consecutive_errors += 1
        if self._consecutive_errors >= self.config.max_consecutive_errors:
            self._rate_limit_backoff()

    def _rate_limit_backoff(self) -> None:
        """Sleep with exponential backoff, save state, then reset error counter to continue.

        Starts at 60s and doubles each consecutive backoff (60, 120, 240, ...) up to 1 hour
        per wait. Cumulative wait time across the run is capped at 5 hours; once that budget
        is exhausted the run gives up rather than waiting longer (the upstream rate limit
        resets at most every 5 hours, so further sleeping is wasted time).
        Resets on the next successful operation. The user can Ctrl+C at any time during the
        sleep — state is already saved so --resume will pick up where it left off.
        """
        self.state.save()
        max_total = self.config.max_total_backoff_seconds
        max_single = self.config.max_single_backoff_seconds
        remaining_budget = max_total - self._total_backoff_seconds
        if remaining_budget <= 0:
            self._terminal_failure = (
                f"rate-limit backoff budget exhausted: slept "
                f"{self._total_backoff_seconds / 3600:.1f}h consecutively without progress "
                f"(cap: {max_total / 3600:.0f}h, configurable via analysis.max_total_backoff_seconds). "
                "State saved; resume later."
            )
            print(f"\n[juvenal] {self._terminal_failure}", flush=True)
            return

        delay = min(60 * (2**self._backoff_count), max_single)
        delay = min(delay, remaining_budget)
        self._backoff_count += 1
        minutes = delay / 60
        cumulative_minutes = (self._total_backoff_seconds + delay) / 60
        print(
            f"\n[juvenal] {self._consecutive_errors} consecutive errors — "
            f"likely rate limit. Sleeping {minutes:.0f}m before retrying "
            f"(cumulative: {cumulative_minutes:.0f}m of {max_total / 60:.0f}m cap). "
            "State saved (Ctrl+C to exit, --resume to continue later).",
            flush=True,
        )
        # Use a threading.Event so kill_active / Ctrl-C can interrupt this
        # sleep on a background executor thread (time.sleep is uninterruptible
        # from outside the thread; Event.wait is not).
        if self._sleep_with_shutdown(delay):
            self._total_backoff_seconds += delay
            return
        self._total_backoff_seconds += delay
        self._consecutive_errors = 0

    def _sleep_with_shutdown(self, seconds: float) -> bool:
        """Sleep up to `seconds`. Returns True if the shutdown event fires
        first. Carved out so tests can patch this single method to skip
        backoff waits without having to patch time.sleep or Event.wait."""

        return self._shutdown_event.wait(seconds)

    def _record_success(self) -> None:
        """Reset error and backoff counters on any successful agent run.

        Crucially this also zeroes _total_backoff_seconds so the cumulative
        cap means "consecutive backoff time without progress," not "total
        backoff in run." A 12-hour productive run with ~5h of waits sprinkled
        between successful turns must not crash on the cap."""
        self._consecutive_errors = 0
        self._backoff_count = 0
        self._total_backoff_seconds = 0.0

    def _dependencies_satisfied(self, target: TargetRecord) -> bool:
        def verified_via_retries(claim_id: str, seen: set[str]) -> bool:
            if claim_id in seen:
                return False
            seen.add(claim_id)
            claim = self.state.claims.get(claim_id)
            if claim is None:
                return False
            if claim.status == "verified":
                return True
            return any(verified_via_retries(rid, seen) for rid in claim.retry_claim_ids)

        return all(verified_via_retries(dep_id, set()) for dep_id in target.depends_on_claim_ids)

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

    def _role_env(self, role: str, *, verifier_name: str = "") -> dict[str, str] | None:
        env = dict(self.phase.env)
        env["JUVENAL_ANALYSIS_ROLE"] = role
        if role == "verifier" and verifier_name:
            env["JUVENAL_ANALYSIS_VERIFIER_NAME"] = verifier_name
        return env
