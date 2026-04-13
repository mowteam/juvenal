"""Atomic JSON state persistence for pipeline resume."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock

from rich.console import Console
from rich.panel import Panel
from rich.table import Table


@dataclass
class PhaseState:
    """State for a single phase."""

    phase_id: str
    status: str = "pending"  # pending, running, completed, failed
    attempt: int = 0
    failure_contexts: list[dict] = field(default_factory=list)
    logs: list[dict] = field(default_factory=list)
    started_at: float | None = None
    completed_at: float | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    baseline_sha: str | None = None  # git HEAD before first implement run
    phase_type: str | None = None  # "implement", "check", "workflow", "analysis"
    analysis_state_file: str | None = None  # child state file for analysis phases


@dataclass
class PipelineState:
    """Complete pipeline state with atomic persistence."""

    state_file: Path
    phases: dict[str, PhaseState] = field(default_factory=dict)
    started_at: float | None = None
    completed_at: float | None = None
    _lock: RLock = field(init=False, repr=False, default_factory=RLock)

    def set_attempt(self, phase_id: str, attempt: int) -> None:
        with self._lock:
            ps = self._ensure_phase(phase_id)
            ps.attempt = attempt
            ps.status = "running"
            if ps.started_at is None:
                ps.started_at = time.time()
            self.save()

    def mark_completed(self, phase_id: str) -> None:
        with self._lock:
            ps = self._ensure_phase(phase_id)
            ps.status = "completed"
            ps.completed_at = time.time()
            self.save()

    def mark_failed(self, phase_id: str) -> None:
        with self._lock:
            ps = self._ensure_phase(phase_id)
            ps.status = "failed"
            ps.completed_at = time.time()
            self.save()

    def set_failure_context(self, phase_id: str, context: str, attempt: int | None = None) -> None:
        with self._lock:
            ps = self._ensure_phase(phase_id)
            entry: dict = {
                "context": context,
                "timestamp": time.time(),
            }
            if attempt is not None:
                entry["attempt"] = attempt
            ps.failure_contexts.append(entry)
            self.save()

    def get_failure_context(self, phase_id: str) -> str:
        """Return the most recent failure context, or empty string."""
        ps = self.phases.get(phase_id)
        if ps and ps.failure_contexts:
            return ps.failure_contexts[-1]["context"]
        return ""

    def log_step(
        self, phase_id: str, attempt: int, step: str, output: str, input: str = "", transcript: str = ""
    ) -> None:
        with self._lock:
            ps = self._ensure_phase(phase_id)
            entry: dict = {
                "attempt": attempt,
                "step": step,
                "output": output,
                "timestamp": time.time(),
            }
            if input:
                entry["input"] = input
            if transcript:
                entry["transcript"] = transcript
            ps.logs.append(entry)
            self.save()

    def add_tokens(self, phase_id: str, input_tokens: int, output_tokens: int) -> None:
        """Accumulate token usage for a phase."""
        with self._lock:
            ps = self._ensure_phase(phase_id)
            ps.input_tokens += input_tokens
            ps.output_tokens += output_tokens
            self.save()

    def total_tokens(self) -> tuple[int, int]:
        """Return (total_input_tokens, total_output_tokens) across all phases."""
        inp = sum(ps.input_tokens for ps in self.phases.values())
        out = sum(ps.output_tokens for ps in self.phases.values())
        return inp, out

    def invalidate_from(self, phase_id: str, scope: set[str] | None = None) -> None:
        """Invalidate this phase and all subsequent phases (for bounce targets).

        Preserves attempt count, baseline_sha (cumulative across bounces),
        failure_contexts (append-only history, new context set separately after
        invalidation by the engine loop).

        If scope is provided, only invalidate phases whose ID is in the scope set.
        This prevents lane A's bounce from clobbering lane B's state.
        """
        with self._lock:
            found = False
            for pid, ps in self.phases.items():
                if pid == phase_id:
                    found = True
                if found:
                    if scope is not None and pid not in scope:
                        continue
                    ps.status = "pending"
                    ps.started_at = None
                    ps.completed_at = None
            self.save()

    def get_resume_phase_index(self, phases: list) -> int:
        """Find the first non-completed phase index for resuming."""
        for i, phase in enumerate(phases):
            ps = self.phases.get(phase.id)
            if ps is None or ps.status != "completed":
                return i
        return len(phases)

    def save(self) -> None:
        """Atomic save: write to tmp, fsync, rename.

        Thread-safe: acquires _lock if not already held (RLock is reentrant,
        so callers that already hold the lock can call save() safely).
        """
        with self._lock:
            data = self._to_dict()
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.state_file.with_name(f"{self.state_file.name}.tmp")
            payload = json.dumps(data, indent=2, sort_keys=True) + "\n"
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.state_file)

    @classmethod
    def load(cls, state_file: str | Path | None) -> PipelineState:
        """Load state from file, or return empty state."""
        if state_file is None:
            state_file = Path(".juvenal-state.json")
        state_file = Path(state_file)
        state = cls(state_file=state_file)
        if state_file.exists():
            data = json.loads(state_file.read_text())
            state.started_at = data.get("started_at")
            state.completed_at = data.get("completed_at")
            for pid, pdata in data.get("phases", {}).items():
                # Backwards compat: migrate scalar failure_context to list
                fc_raw = pdata.get("failure_contexts", [])
                if not fc_raw and pdata.get("failure_context"):
                    fc_raw = [{"context": pdata["failure_context"], "timestamp": 0}]
                state.phases[pid] = PhaseState(
                    phase_id=pid,
                    status=pdata.get("status", "pending"),
                    attempt=pdata.get("attempt", 0),
                    failure_contexts=fc_raw,
                    logs=pdata.get("logs", []),
                    started_at=pdata.get("started_at"),
                    completed_at=pdata.get("completed_at"),
                    input_tokens=pdata.get("input_tokens", 0),
                    output_tokens=pdata.get("output_tokens", 0),
                    baseline_sha=pdata.get("baseline_sha"),
                    phase_type=pdata.get("phase_type"),
                    analysis_state_file=pdata.get("analysis_state_file"),
                )
        return state

    def print_status(self) -> None:
        """Print a Rich-formatted status table with analysis sub-detail."""
        console = Console()
        table = Table(title="Juvenal Pipeline Status")
        table.add_column("Phase", style="cyan")
        table.add_column("Status", style="bold")
        table.add_column("Attempts", justify="right")
        table.add_column("Duration", justify="right")

        for pid, ps in self.phases.items():
            status_style = {"completed": "green", "running": "yellow", "failed": "red", "pending": "dim"}.get(
                ps.status, "dim"
            )
            duration = ""
            if ps.started_at and ps.completed_at:
                dur = ps.completed_at - ps.started_at
                duration = f"{dur:.1f}s"
            elif ps.started_at:
                dur = time.time() - ps.started_at
                duration = f"{dur:.1f}s (running)"
            table.add_row(pid, f"[{status_style}]{ps.status}[/]", str(ps.attempt), duration)

        console.print(table)

        # Render analysis sub-detail for each analysis phase
        for pid, ps in self.phases.items():
            if ps.phase_type != "analysis" or not ps.analysis_state_file:
                continue
            result = self._render_analysis_detail(ps.analysis_state_file)
            if result is None:
                continue
            detail_table, summary = result
            console.print()
            console.print(Panel(detail_table, title=f"[cyan]{pid}[/] Analysis Detail", border_style="dim"))
            console.print(
                f"  {summary['total']} targets | "
                f"{summary['completed']} completed | "
                f"{summary['blocked']} blocked/exhausted | "
                f"{summary['verifying']} verifying | "
                f"{summary['running']} running | "
                f"{summary['claims_verified']} claims verified | "
                f"{summary['claims_rejected']} claims rejected"
            )

    def _render_analysis_detail(self, analysis_state_file: str) -> tuple[Table, dict[str, int]] | None:
        """Load analysis child state and render a nested detail table."""
        from rich.box import SIMPLE

        from juvenal.dynamic.state import DynamicSessionState

        state_path = self.state_file.parent / analysis_state_file
        if not state_path.exists():
            return None

        dss = DynamicSessionState.load(state_path)
        if not dss.targets:
            return None

        detail = Table(show_header=True, box=SIMPLE, padding=(0, 1))
        detail.add_column("Target", style="cyan")
        detail.add_column("Status")
        detail.add_column("Gen", justify="right")
        detail.add_column("Claims")

        _target_styles = {
            "completed": "green",
            "running": "yellow",
            "verifying": "blue",
            "queued": "dim",
            "no_findings": "dim green",
            "blocked": "red",
            "exhausted": "dim red",
            "deferred": "dim yellow",
            "requeue_pending": "yellow",
        }
        _claim_styles = {
            "verified": "green",
            "rejected": "red",
            "verifying": "blue",
            "proposed": "dim",
            "superseded": "dim",
        }

        sorted_targets = sorted(dss.targets.values(), key=lambda t: (-t.priority, t.created_at, t.target_id))

        summary: dict[str, int] = {
            "total": len(sorted_targets),
            "completed": 0,
            "blocked": 0,
            "verifying": 0,
            "running": 0,
            "claims_verified": 0,
            "claims_rejected": 0,
        }

        for target in sorted_targets:
            target_claims = [
                c
                for c in dss.claims.values()
                if c.target_id == target.target_id and c.generation == target.active_generation
            ]
            # Filter to leaf claims for summary counts
            superseded_ids: set[str] = set()
            for c in target_claims:
                if c.retry_of_claim_id is not None:
                    superseded_ids.add(c.retry_of_claim_id)
            leaf_claims = [c for c in target_claims if c.claim_id not in superseded_ids]

            n_verified = sum(1 for c in leaf_claims if c.status == "verified")
            n_rejected = sum(1 for c in leaf_claims if c.status == "rejected")
            n_pending = sum(1 for c in leaf_claims if c.status in ("proposed", "verifying"))

            if leaf_claims:
                claims_text = f"{n_verified} verified  {n_rejected} rejected  {n_pending} pending"
            elif target.status == "running":
                claims_text = "(worker active)"
            else:
                claims_text = "-"

            target_style = _target_styles.get(target.status, "dim")
            detail.add_row(
                (target.title or target.target_id)[:50],
                f"[{target_style}]{target.status}[/]",
                str(target.active_generation or target.generation),
                claims_text,
            )

            # Claim detail rows
            for claim in leaf_claims:
                claim_style = _claim_styles.get(claim.status, "dim")
                retry_text = f" retry {claim.retry_count}" if claim.retry_count > 0 else ""
                detail.add_row(
                    f"  [dim]{claim.summary[:45]}[/dim]",
                    f"[{claim_style}]{claim.status}[/]",
                    "",
                    f"[dim]{claim.severity}{retry_text}[/dim]",
                )

            # Update summary counters
            if target.status == "completed":
                summary["completed"] += 1
            elif target.status in ("blocked", "exhausted"):
                summary["blocked"] += 1
            elif target.status == "verifying":
                summary["verifying"] += 1
            elif target.status == "running":
                summary["running"] += 1
            summary["claims_verified"] += n_verified
            summary["claims_rejected"] += n_rejected

        return detail, summary

    def _ensure_phase(self, phase_id: str) -> PhaseState:
        if phase_id not in self.phases:
            self.phases[phase_id] = PhaseState(phase_id=phase_id)
        return self.phases[phase_id]

    def _to_dict(self) -> dict:
        return {
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "phases": {
                pid: {
                    "status": ps.status,
                    "attempt": ps.attempt,
                    "failure_contexts": ps.failure_contexts,
                    "logs": ps.logs,
                    "started_at": ps.started_at,
                    "completed_at": ps.completed_at,
                    "input_tokens": ps.input_tokens,
                    "output_tokens": ps.output_tokens,
                    "baseline_sha": ps.baseline_sha,
                    "phase_type": ps.phase_type,
                    "analysis_state_file": ps.analysis_state_file,
                }
                for pid, ps in self.phases.items()
            },
        }
