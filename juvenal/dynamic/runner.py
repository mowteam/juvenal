"""Dynamic analysis runner for captain/worker/verifier orchestration."""

from __future__ import annotations

import json
import sys
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Event, Lock
from typing import Any, Literal

from juvenal.backends import AgentResult, Backend, create_backend
from juvenal.checkers import VerificationReport, extract_json_block, parse_verification_report
from juvenal.display import Display
from juvenal.dynamic.interaction import UserInteractionChannel
from juvenal.dynamic.models import (
    AttackSurfaceState,
    CaptainTurn,
    ClaimRecord,
    SimulationEnvState,
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
from juvenal.workflow import (
    AnalysisConfig,
    AnalystSpec,
    ExploitSimSpec,
    Phase,
    ReporterSpec,
    VerifierSpec,
    Workflow,
    apply_vars,
)

# Canonical shipped location for static role subagent definitions. Native Claude
# Code discovery is mirrored via repo-root `.claude/agents/*.md` symlinks into here.
_PACKAGE_AGENTS_DIR = Path(__file__).resolve().parent.parent / "prompts" / "agents"

_CAPTAIN_EVENT_TYPES = frozenset(
    {
        "claim.verified",
        "claim.rejected",
        "claim.retry_scheduled",
        "target.no_findings",
        "target.blocked",
        "target.dependency_stranded",
        "target.exhausted",
        "directive.received",
        "captain.proposal_dropped",
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
        "target.dependency_stranded",
        "target.exhausted",
        "target.deferred",
        "directive.received",
        "directive.acknowledged",
        "captain.proposal_dropped",
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
    "   - Dependency-stranded target: DO re-enqueue, with `depends_on_claim_ids: []`. It was never "
    "dispatched — its dependency claim was rejected so the gate can never open — so nothing is "
    "known about the work itself.\n"
    "3. Enqueue at least 8 new targets pivoting to UNCOVERED SURFACE or following the "
    "variant-analysis rules above.\n"
    '4. Return `termination_state: "continue"`. Do not declare `complete` again until both '
    "floors are met AND `UNCOVERED SURFACE` is empty.\n"
)

# Per-backend, per-role default model. When the YAML does not specify a model
# (and no role-level override is set), this picks the right tier so users
# don't have to write model identifiers in every workflow. Captain gets opus
# 4.7 with the 1M-context beta because it carries the longest accumulated
# state; worker also runs opus 4.7 but on the default 200K context (no `[1m]`
# suffix) to keep input-token cost down per attempt. Verifier runs opus 4.6
# (200K) because the task — adversarial scrutiny of a single claim packet —
# rewards reasoning capacity over speed. Reporter stays on sonnet 4.6 for
# fast, low-cost report writing. Codex uses whatever the CLI defaults to —
# we don't pick a model on its behalf.
_DEFAULT_MODELS_BY_BACKEND_AND_ROLE: dict[str, dict[str, str | None]] = {
    "claude": {
        "captain": "claude-opus-4-7[1m]",
        "worker": "claude-opus-4-7",
        "verifier": "claude-opus-4-6",
        "reporter": "claude-sonnet-4-6",
        # Analyst defaults to opus 4.7 with the 1M-context beta because it
        # reads a lot (codebase, docs, web research) before producing the brief.
        "analyst": "claude-opus-4-7[1m]",
        # Exploit-sim roles. env_builder reads the codebase + docs to stand up a
        # runnable sandbox, so it gets the 1M-context model like the analyst.
        # simulator/attacker run opus 4.7 (they drive/observe a live instance);
        # exploit_judge runs opus 4.6 for adversarial categorization like verifiers.
        "env_builder": "claude-opus-4-7[1m]",
        "simulator": "claude-opus-4-7",
        "attacker": "claude-opus-4-7",
        "exploit_judge": "claude-opus-4-6",
    },
    "codex": {
        "captain": None,
        "worker": None,
        "verifier": None,
        "reporter": None,
        "analyst": None,
        "env_builder": None,
        "simulator": None,
        "attacker": None,
        "exploit_judge": None,
    },
}
# The SDK-backed Claude backend takes the same model strings (incl. the `[1m]`
# 1M-context suffix), so it inherits the subprocess backend's per-role defaults.
_DEFAULT_MODELS_BY_BACKEND_AND_ROLE["claude-sdk"] = _DEFAULT_MODELS_BY_BACKEND_AND_ROLE["claude"]


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


@dataclass
class _AnalystExecutionResult:
    agent_result: AgentResult
    brief: str
    error: str | None


@dataclass
class _EnvBuilderExecutionResult:
    agent_result: AgentResult
    brief: str
    artifact_path: str | None
    instantiate_script: str | None
    error: str | None


@dataclass
class _ExploitSimExecutionResult:
    claim_id: str
    category: Literal[
        "exploit_confirmed",
        "exploit_confirmed_nondefault",
        "exploit_unconfirmed",
        "sim_inconclusive",
        "sim_error",
    ]
    config_deltas: list[str]
    transcript_refs: list[str]
    exchange_rounds: int
    agent_results: list[AgentResult]
    error: str | None


_EXPLOIT_SIM_CATEGORIES = frozenset(
    {
        "exploit_confirmed",
        "exploit_confirmed_nondefault",
        "exploit_unconfirmed",
        "sim_inconclusive",
        "sim_error",
    }
)
_JUDGE_BEGIN = "EXPLOIT_JUDGE_JSON_BEGIN"
_JUDGE_END = "EXPLOIT_JUDGE_JSON_END"
_MAX_REPORTER_ATTEMPTS = 3
# After this many seconds since CREATION, a Claude session is considered too old
# to safely resume. The Anthropic CLI does not always error on stale --resume —
# sometimes it silently starts a fresh session that lacks the original system
# prompt, which makes the worker emit free-form text without the structured
# WORKER_JSON block. When we detect a stale parent session, we cold-restart
# with run_agent + the original system_prompt instead of resuming.
#
# This threshold needs to be MUCH LONGER than typical pause-and-resume gaps:
# a long-running analysis that started yesterday morning and paused/resumes a
# few hours later still has session IDs that are 12+ hours old, but those
# sessions are normally still resumable. Set high enough that we only flip
# to cold-restart for runs paused across days, where the symptom (silent
# fresh sessions returning unstructured output) has actually been observed.
_SESSION_STALENESS_THRESHOLD_SECONDS = 36 * 3600

# Substrings that indicate a resume was refused because the session no longer
# exists on the backend. Distinct from staleness, which is a time-based guess:
# these are the backend stating outright that the id is unusable, so no amount
# of waiting or retrying recovers it — only a cold restart does. Codex emits the
# rollout form (a run killed before codex flushed its rollout leaves a thread id
# that never materialized); the Claude CLI emits the conversation form.
_DEAD_SESSION_ERROR_SIGNATURES = (
    "no rollout found for thread id",
    "no conversation found with session id",
    "session not found",
)

# Substrings that indicate an Anthropic / Claude CLI rate-limit response. Used
# to distinguish errors worth a long backoff sleep ("wait it out") from errors
# where backoff would not help (parse failures, identity mismatches, etc.).
_RATE_LIMIT_ERROR_SIGNATURES = (
    "rate limit",
    "rate_limit",
    "monthly usage limit",
    "monthly limit",
    "out of extra usage",
    "you've hit your limit",
    "your limit · resets",
    "429",
)

# Substrings that indicate the account's agent quota is *spent*, not that a
# short-window throttle is in effect. Codex says "You've hit your usage limit …
# try again at <date>" where the date can be days out, so no backoff schedule
# can wait it out — the run stops and the user resumes after the reset. These
# must stay narrow enough not to swallow Anthropic's 5-hour-window messages,
# which the probe backoff in `_rate_limit_backoff` does recover from.
_QUOTA_EXHAUSTED_ERROR_SIGNATURES = (
    "hit your usage limit",
    "purchase more credits",
    "codex/settings/usage",
    "exceeded your current quota",
    "insufficient_quota",
)


def _error_text(error: str | AgentResult | None) -> str:
    """Raw text to match signatures against, from an error string or a result."""
    if error is None:
        return ""
    if isinstance(error, AgentResult):
        return f"{error.output or ''}\n{error.transcript or ''}"
    return error


def _is_quota_exhaustion(error: str | AgentResult | None) -> bool:
    text = _error_text(error).lower()
    return any(signature in text for signature in _QUOTA_EXHAUSTED_ERROR_SIGNATURES)


_DEFAULT_ANALYST_PROMPT = """You are the project's attack-surface analyst for a Juvenal bug-finding run.

Your job is to produce ONE structured project brief that the captain, workers, and verifiers \
will use as the source of truth for the project's trust model and attack surface throughout \
the rest of the analysis. You run exactly once at the start of the analysis. Be thorough but \
concise: cap your brief at roughly 8,000 words.

Investigate using whatever tools are available — Read / Grep / Glob across the repository at \
the path noted below, plus WebFetch / WebSearch for the project's documentation, security \
policy, threat model, prior CVEs, and public bug-bounty scope. Do NOT modify any files in \
the repo.

Repository root: {working_dir}

Mission context (the workflow this analysis is running under):

{mission}

Required brief structure (use these section headers verbatim):

# Project Brief

## Project identity
One paragraph: what this project is, what role it plays, who maintains it, the language(s), \
and any version pinned by the run.

## Trust model & threat model
Who is trusted (operators, signed peers, local users) vs. who is untrusted (network peers, \
arbitrary users, attacker-controlled inputs). Cite the project's own docs / SECURITY.md / \
threat-model docs where they exist; quote them. If the project explicitly documents an \
attacker model, say so.

## Attack surface — entry points
Concrete enumeration of externally reachable interfaces: network listeners (protocol, port, \
default exposure), CLI tools that take attacker-controlled input, file-format parsers, RPC \
endpoints, public APIs. Mark each as in-scope or out-of-scope where the project says so.

## Authentication & authorization boundaries
Where the project draws privilege lines, what credentials gate what, what an unauthenticated \
network peer can do vs. an authenticated one, etc.

## Privilege & sandbox boundaries
Process boundaries, syscall filters, container/namespace assumptions, plugin/extension \
loading, anything documented as a sandbox or escape-relevant boundary.

## Documented out-of-scope / by-design behaviors
Anything the project explicitly documents as not-a-bug, won't-fix, or by-design — including \
stated assumptions about input validation done elsewhere, trusted-input contracts, etc. \
Verifiers will use this to avoid filing design-critique reports.

## Known CVE classes & prior findings
A short list of the categories of issues this project has historically had (memory safety, \
parser confusion, type confusion, race conditions, etc.) and a couple of representative CVE \
identifiers if available. This helps the captain prioritize variant analysis.

## Bounty scope
If a public bug-bounty program is documented, summarize: which components/repos/branches/ \
versions are in scope, what is explicitly out of scope, what severity buckets exist. If \
none, say so.

## Quick-reference for verifiers
A bulleted set of tight rules verifiers can check at a glance, e.g.:
- "The XYZ daemon trusts its own admin socket but not its network listener."
- "Project explicitly documents that file format ABC is not parsed in untrusted contexts."
- "Bug class X is out of scope per <doc>."

End your output after the Quick-reference section. Do not append meta-commentary about your \
own process.
"""

# Exploit-sim role prompts. These are fallbacks used only when the workflow does
# not supply its own. They REQUIRE a real, runnable environment inside a sandbox
# and forbid pre-provisioning any credential that sits on a trust boundary — the
# whole point of the stage is to confirm reproduction on a realistic system, not
# to hand the attacker a shortcut past the boundary the bug is supposed to cross.
_DEFAULT_ENV_BUILDER_PROMPT = """You are the exploit-simulation environment builder for a Juvenal bug-bounty run.

Build a REAL, RUNNABLE instance of the target inside this sandbox — actually compile / \
install / configure it so a live process can be started, not a mock or a description. Use \
the project's own default configuration. Do NOT pre-install, pre-seed, or hand out any \
credential, token, key, or capability that lies on a trust boundary an attacker would have \
to cross (admin passwords, signed peer identities, auth cookies, privileged sockets). The \
simulation must reflect what an unprivileged/remote attacker actually faces.

Repository root: {working_dir}
Persist all artifacts under: {env_dir}

Mission context:
{mission}

Write a runnable `instantiate.sh` under {env_dir} that spins up a FRESH, isolated instance \
from the built artifact each time it is invoked (so per-claim runs never share state).

End your output with a machine-readable block:
ENV_JSON_BEGIN
{"artifact_path": "<path under env_dir>", "instantiate_script": "<path to instantiate.sh>"}
ENV_JSON_END
"""

_DEFAULT_SIMULATOR_PROMPT = """You are the exploit-simulation SIMULATOR for a Juvenal bug-bounty run \
(round {round} of {max_rounds}).

Instantiate / manage a fresh instance of the target from the prepared environment and report \
its observable behavior to the attacker. Run the REAL system; never fabricate output. Start \
from DEFAULT configuration. If the attacker requests a configuration change, you MAY grant it \
only if a real operator plausibly would — but you MUST log every granted change verbatim on \
its own line prefixed `CONFIG_DELTA: `. Never grant a change that simply hands over a \
trust-boundary credential.

Environment brief:
{env_brief}

Claim under test:
{claim_packet}

Attacker's last message: {attacker_last}

Report the instance's actual observed response.
"""

_DEFAULT_ATTACKER_PROMPT = """You are the exploit-simulation ATTACKER for a Juvenal bug-bounty run \
(round {round} of {max_rounds}).

Run the verified proof-of-concept for the claim below against the live instance the simulator \
manages. Use only capabilities a real attacker at the documented trust boundary would have. If \
you genuinely need a non-default configuration to reproduce, request it explicitly from the \
simulator (it will decide and log the delta) rather than assuming it.

Claim under test:
{claim_packet}

Simulator's last message: {simulator_last}

Report concretely whether the exploit reproduced and what you observed.
"""

_DEFAULT_EXPLOIT_JUDGE_PROMPT = """You are the exploit-simulation JUDGE for a Juvenal bug-bounty run.

Read the attacker<->simulator transcript and the granted config-delta log, then categorize the \
verified claim. You do NOT re-verify the bug (it already passed the verifier chain) and you \
NEVER reject it — you only record whether it reproduced live and under what configuration.

Categories:
- exploit_confirmed: reproduced on the DEFAULT configuration.
- exploit_confirmed_nondefault: reproduced only after granted non-default config changes.
- exploit_unconfirmed: valid bug, but it did not reproduce live in this run.
- sim_inconclusive: the environment/simulator could not exercise the claim.

Claim under test:
{claim_packet}

Config deltas granted (JSON list): {config_deltas}

Transcript:
{transcript}

End your output with:
EXPLOIT_JUDGE_JSON_BEGIN
{"category": "<one of the four>", "config_deltas": ["..."], "rationale": "one line"}
EXPLOIT_JUDGE_JSON_END
"""


def _empty_agent_result() -> AgentResult:
    return AgentResult(exit_code=1, output="", transcript="", duration=0.0, input_tokens=0, output_tokens=0)


def _parse_env_builder_brief(brief: str) -> tuple[str | None, str | None]:
    """Extract artifact_path / instantiate_script from the env-builder's ENV_JSON block."""
    payload = extract_json_block(brief, "ENV_JSON_BEGIN", "ENV_JSON_END")
    if not isinstance(payload, dict):
        return None, None
    artifact = payload.get("artifact_path")
    script = payload.get("instantiate_script")
    return (artifact if isinstance(artifact, str) else None, script if isinstance(script, str) else None)


def _parse_config_deltas(output: str) -> list[str]:
    """Collect every `CONFIG_DELTA: ...` line emitted during the dialogue."""
    deltas: list[str] = []
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("CONFIG_DELTA:"):
            value = stripped[len("CONFIG_DELTA:") :].strip()
            if value:
                deltas.append(value)
    return deltas


def _parse_exploit_judge_output(output: str) -> tuple[str, list[str], str | None]:
    """Return (category, config_deltas, error). Unparseable output degrades to
    sim_inconclusive — the stage is non-gating, so a judge parse failure must not
    strand the claim."""
    payload = extract_json_block(output, _JUDGE_BEGIN, _JUDGE_END)
    if not isinstance(payload, dict):
        return "sim_inconclusive", [], "exploit_judge output missing EXPLOIT_JUDGE_JSON block"
    category = payload.get("category")
    if category not in _EXPLOIT_SIM_CATEGORIES or category == "sim_error":
        return "sim_inconclusive", [], f"exploit_judge returned invalid category {category!r}"
    raw_deltas = payload.get("config_deltas", [])
    deltas = [d for d in raw_deltas if isinstance(d, str)] if isinstance(raw_deltas, list) else []
    return category, deltas, None


def _strip_agent_frontmatter(text: str) -> str:
    """Drop a leading Claude Code `---\\n…\\n---\\n` frontmatter block; return the body."""
    if text.startswith("---\n"):
        end = text.find("\n---\n", 4)
        if end != -1:
            return text[end + len("\n---\n") :].lstrip("\n")
    return text


def _load_agent_body(name: str, working_dir: Path | None = None) -> str | None:
    """Return the frontmatter-stripped body of subagent `{name}.md`, or None.

    A per-project `.claude/agents/{name}.md` under ``working_dir`` overrides the
    canonical package copy so a target repo can specialize a role.
    """
    candidates: list[Path] = []
    if working_dir is not None:
        candidates.append(Path(working_dir) / ".claude" / "agents" / f"{name}.md")
    candidates.append(_PACKAGE_AGENTS_DIR / f"{name}.md")
    for path in candidates:
        try:
            if not path.is_file():
                continue
            body = _strip_agent_frontmatter(path.read_text(encoding="utf-8")).strip()
        except OSError:
            continue
        if body:
            return body
    return None


# Shipped role subagents whose bodies are dual-emitted as Codex `.codex/agents/*.toml`
# definitions so a codex-backed worker/verifier can spawn them natively (verified
# `multi_agent stable true` on codex-cli 0.128.0; defs live in `.codex/agents/<name>.toml`
# with name/description/developer_instructions — the exact keys the installed binary parses).
_SHIPPED_AGENT_NAMES = (
    "code-survey",
    "attack-surface-verifier",
    "trust-model-verifier",
    "poc-verifier",
    "novelty-verifier",
    "exploit-sim-env-builder",
    "exploit-sim-simulator",
    "exploit-sim-attacker",
    "exploit-sim-judge",
)


def _backend_is_codex(name: str | None) -> bool:
    """True for the Codex subprocess/SDK backends (`codex`, `codex-sdk`)."""
    return name in ("codex", "codex-sdk")


def _parse_agent_frontmatter(text: str) -> dict[str, str]:
    """Return the `name`/`description` (etc.) scalars from a leading `---\\n…\\n---` block.

    Values are single-line in the shipped agent files; anything without a closing
    delimiter yields an empty mapping (matches `_strip_agent_frontmatter`).
    """
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}
    fields: dict[str, str] = {}
    for line in text[4:end].splitlines():
        key, sep, value = line.partition(":")
        if sep and key.strip():
            fields[key.strip()] = value.strip()
    return fields


def _toml_escape(value: str) -> str:
    r"""Escape a Python string for a TOML basic (double-quoted) string literal."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "").replace("\t", "\\t")


def _codex_agent_toml(name: str, description: str, developer_instructions: str) -> str:
    """Serialize a Codex subagent definition (`name`/`description`/`developer_instructions`).

    `developer_instructions` uses a TOML multi-line basic string so the role body
    survives verbatim; `name`/`description` are single-line basic strings.
    """
    body = developer_instructions.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
    return (
        f'name = "{_toml_escape(name)}"\n'
        f'description = "{_toml_escape(description)}"\n'
        f'developer_instructions = """\n{body}\n"""\n'
    )


def _agent_toml_from_shipped(name: str, working_dir: Path | None = None) -> str | None:
    """Build a Codex `.codex/agents/<name>.toml` from the shipped Claude agent `.md`.

    Single source of truth: reads the same `juvenal/prompts/agents/<name>.md` (or a
    per-project `.claude/agents/<name>.md` override) the Claude path uses, so the two
    formats never drift. Returns None when the source is missing/empty.
    """
    candidates: list[Path] = []
    if working_dir is not None:
        candidates.append(Path(working_dir) / ".claude" / "agents" / f"{name}.md")
    candidates.append(_PACKAGE_AGENTS_DIR / f"{name}.md")
    for path in candidates:
        try:
            if not path.is_file():
                continue
            raw = path.read_text(encoding="utf-8")
        except OSError:
            continue
        body = _strip_agent_frontmatter(raw).strip()
        if not body:
            continue
        fm = _parse_agent_frontmatter(raw)
        description = fm.get("description") or f"{name} role for the Juvenal bug-finding run."
        return _codex_agent_toml(fm.get("name") or name, description, body)
    return None


def write_codex_agent_definitions(working_dir: Path) -> list[Path]:
    """Materialize the shipped role subagents into `<working_dir>/.codex/agents/*.toml`.

    Mirrors the Claude `.claude/agents/*.md` path so a codex-backed role can spawn the
    same specialized roles natively. Best-effort: skips any agent that fails to render
    or write and returns the paths successfully written.
    """
    written: list[Path] = []
    agents_dir = Path(working_dir) / ".codex" / "agents"
    try:
        agents_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return written
    for name in _SHIPPED_AGENT_NAMES:
        toml_text = _agent_toml_from_shipped(name, working_dir)
        if not toml_text:
            continue
        target = agents_dir / f"{name}.toml"
        try:
            target.write_text(toml_text, encoding="utf-8")
        except OSError:
            continue
        written.append(target)
    return written


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
        pipeline_state: Any = None,
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
        # Parent pipeline state, used to pause this phase's active-runtime
        # accumulator across rate-limit sleeps so `juvenal status` Duration
        # reflects time actually running.
        self._pipeline_state: Any = pipeline_state

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
        self._analyst_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="juvenal-analyst")
        self._analyst_future: Future[_AnalystExecutionResult] | None = None
        # Exploit-sim (non-gating post-verification categorizer). env_builder runs
        # once in its own single-slot pool; per-claim exploit-sim attempts run
        # serially in a second single-slot pool so a fresh env instance is used
        # per claim without cross-claim state pollution.
        self._env_builder_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="juvenal-env-builder")
        self._env_builder_future: Future[_EnvBuilderExecutionResult] | None = None
        # The env-builder future can be drained from two threads (the main loop
        # and a per-claim exploit-sim worker waiting on it), so guard the
        # claim-and-clear so it drains exactly once.
        self._env_builder_lock = Lock()
        self._exploit_sim_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="juvenal-exploit-sim")
        self._exploit_sim_futures: dict[Future[_ExploitSimExecutionResult], str] = {}
        self._pending_exploit_sim_claim_ids: list[str] = []
        self._exploit_sim_attempts: dict[str, int] = {}
        # Stale-session warnings are debounced per-session so a target with N
        # retries doesn't emit N copies of the same warning.
        self._logged_stale_sessions: set[str] = set()
        # Sessions the backend has refused to resume. In-memory only: a process
        # restart re-learns each one at the cost of a single failed resume, and
        # persisting it would add a state field for a fact that only matters
        # while the owning attempt chain is live.
        self._dead_sessions: set[str] = set()
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
        # Wall-clock timestamp of the most recent observed Claude CLI 429.
        # When set, _rate_limit_backoff switches from exponential backoff to a
        # fixed probe cadence keyed off the typical 5h reset window.
        self._last_observed_rate_limit_at: float | None = None
        self._quota_exhausted = False
        # Set on shutdown (Ctrl-C, kill_active). Background threads in
        # _rate_limit_backoff use this to interrupt their sleep loop so the
        # process can exit promptly instead of waiting up to an hour for a
        # rate-limit timer to elapse.
        self._shutdown_event = Event()
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cached_input_tokens = 0
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
        # Per-spec scope: an explicit YAML `prompt` wins (user override); when it
        # is empty, fall back to the shipped `.claude/agents/{name}-verifier.md`
        # subagent body so the role prompt lives in one place. Both empty -> no scope.
        self._rendered_verifier_prompts: dict[str, str] = {}
        for spec in self._verifier_chain:
            source = spec.prompt or _load_agent_body(f"{spec.name}-verifier", self.working_dir) or ""
            self._rendered_verifier_prompts[spec.name] = apply_vars(source, self.workflow.vars) if source else ""

        self._reporter_spec: ReporterSpec | None = self.config.reporter
        self._rendered_reporter_prompt: str = ""
        if self._reporter_spec is not None and self._reporter_spec.prompt:
            self._rendered_reporter_prompt = apply_vars(self._reporter_spec.prompt, self.workflow.vars)

        self._analyst_spec: AnalystSpec | None = self.config.analyst if self.config.analyst is not None else None
        self._rendered_analyst_prompt: str = ""
        if self._analyst_spec is not None and self._analyst_spec.prompt:
            self._rendered_analyst_prompt = apply_vars(self._analyst_spec.prompt, self.workflow.vars)

        self._exploit_sim_spec: ExploitSimSpec | None = self.config.exploit_sim
        self._rendered_exploit_sim_prompts: dict[str, str] = {}
        if self._exploit_sim_spec is not None:
            # Effective prompt per role: explicit YAML `prompt` wins; when empty,
            # fall back to the shipped `.claude/agents/exploit-sim-{role}.md` subagent
            # body; still empty -> the embedded `_DEFAULT_*_PROMPT` at execution time.
            # Placeholders ({round}, {mission}, …) survive apply_vars and are filled by
            # the runner's `.replace(...)`.
            for role_key, agent_name, yaml_prompt in (
                ("env_builder", "exploit-sim-env-builder", self._exploit_sim_spec.env_builder.prompt),
                ("simulator", "exploit-sim-simulator", self._exploit_sim_spec.simulator.prompt),
                ("attacker", "exploit-sim-attacker", self._exploit_sim_spec.attacker.prompt),
                ("exploit_judge", "exploit-sim-judge", self._exploit_sim_spec.judge.prompt),
            ):
                source = yaml_prompt or _load_agent_body(agent_name, self.working_dir) or ""
                self._rendered_exploit_sim_prompts[role_key] = apply_vars(source, self.workflow.vars)

        self._rendered_worker_prompt: str = ""
        if self.config.worker_prompt:
            self._rendered_worker_prompt = apply_vars(self.config.worker_prompt, self.workflow.vars)

        # Persist the rendered mission text once, then reference it by path
        # from per-call verifier prompts instead of inlining ~6KB into every
        # call (5 verifiers × hundreds of claims). The captain still inlines
        # mission via its own prompt builder; verifiers Read this file on
        # demand if their per-spec scope is insufficient.
        rendered_mission = self.phase.render_prompt(failure_context=self.failure_context, vars=self.workflow.vars)
        self._mission_file = self.working_dir / ".juvenal" / f"{self.phase.id}-mission.md"
        self._mission_file.parent.mkdir(parents=True, exist_ok=True)
        self._mission_file.write_text(rendered_mission, encoding="utf-8")

    def run(self) -> PhaseResult:
        """Run the dynamic analysis loop to completion or deterministic failure."""

        if self.run_mode == "resume":
            self.state.normalize_for_resume(verifier_chain_length=len(self._verifier_chain))
        else:
            self.state = DynamicSessionState(self.state_file)
            self.state.save()

        self._rebuild_pending_claim_retries()
        self._rebuild_pending_reporter_claim_ids()
        self._rebuild_pending_exploit_sim_claim_ids()
        # Mirror the Claude `.claude/agents/*.md` role subagents into Codex's native
        # `.codex/agents/*.toml` format so a codex-backed role can spawn the same
        # specialized roles. Best-effort; a codex run without them still works.
        if self._uses_codex_backend():
            write_codex_agent_definitions(self.working_dir)
        self._maybe_start_analyst()
        self._maybe_start_env_builder()

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

            if not self._wait_for_analyst():
                return PhaseResult(success=False, failure_context="interrupted while waiting for analyst")

            while True:
                terminate, success, reason = self._should_terminate()
                if terminate:
                    if not success:
                        self.kill_active()
                    return PhaseResult(success=success, failure_context=reason if not success else "")

                made_progress = False
                made_progress |= self._drain_completed_futures()
                made_progress |= self._sweep_dead_dep_targets()
                made_progress |= self._schedule_verifiers()
                made_progress |= self._schedule_workers()
                made_progress |= self._schedule_exploit_sim()
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
            self._finalize_analyst_on_shutdown()
            self._finalize_env_builder_on_shutdown()
            if self._interaction_channel is not None:
                self._interaction_channel.stop()
            _flush_stdin_buffer()
            self._worker_executor.shutdown(wait=False, cancel_futures=True)
            self._verifier_executor.shutdown(wait=False, cancel_futures=True)
            self._reporter_executor.shutdown(wait=False, cancel_futures=True)
            self._analyst_executor.shutdown(wait=False, cancel_futures=True)
            self._env_builder_executor.shutdown(wait=False, cancel_futures=True)
            self._exploit_sim_executor.shutdown(wait=False, cancel_futures=True)

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

            if not self._wait_for_analyst():
                return PhaseResult(success=False, failure_context="interrupted while waiting for analyst")

            while True:
                terminate, success, reason = self._should_terminate()
                if terminate:
                    if not success:
                        self.kill_active()
                    return PhaseResult(success=success, failure_context=reason if not success else "")

                made_progress = False
                made_progress |= self._drain_completed_futures()
                made_progress |= self._sweep_dead_dep_targets()
                made_progress |= self._schedule_verifiers()
                made_progress |= self._schedule_workers()
                made_progress |= self._schedule_exploit_sim()
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
            self._finalize_analyst_on_shutdown()
            self._finalize_env_builder_on_shutdown()
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
            self._analyst_executor.shutdown(wait=False, cancel_futures=True)
            self._env_builder_executor.shutdown(wait=False, cancel_futures=True)
            self._exploit_sim_executor.shutdown(wait=False, cancel_futures=True)

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
            or delta.dependency_stranded_target_ids
            or delta.exhausted_target_ids
            or delta.pending_directive_ids
            or delta.dropped_proposals
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
        system_prompt, user_prompt = self._build_captain_prompt(summary_only=summary_only)
        backend = self._get_backend(self.config.captain_backend)
        session_id = self.state.captain.session_id
        display_callback = self._captain_chunk_callback()

        captain_model = _resolve_model(self.config.captain_backend, "captain", self.config.captain_model)
        if session_id:
            # Resume turns inherit the system prompt set at the original
            # run_agent call; only the per-turn delta ships via stdin.
            result = backend.resume_agent(
                session_id,
                user_prompt,
                working_dir=str(self.working_dir),
                display_callback=display_callback,
                timeout=self.phase.timeout,
                env=self._role_env("captain"),
                model=captain_model,
            )
        else:
            result = backend.run_agent(
                user_prompt,
                working_dir=str(self.working_dir),
                display_callback=display_callback,
                timeout=self.phase.timeout,
                env=self._role_env("captain"),
                model=captain_model,
                system_prompt=system_prompt or None,
            )

        if result.session_id:
            self.state.captain.session_id = result.session_id
            self.state.save()

        self._add_tokens(result)
        self._note_agent_result(result)
        if result.exit_code != 0:
            # Captain crash. Classify: only sleep on actual rate-limit signatures —
            # other captain crashes (e.g., parse errors mid-stream) won't recover
            # by waiting and shouldn't waste backoff time.
            self._note_quota_exhaustion(result)
            self._record_infrastructure_error(result)
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
            self._note_agent_result(result)
            if result.exit_code != 0:
                # Captain repair crash. Same classification as the main captain
                # path: only sleep on actual rate-limit signatures.
                self._note_quota_exhaustion(result)
                self._record_infrastructure_error(result)
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
            try:
                system_prompt, user_prompt = self._build_worker_prompt(target, attempt)
                future = self._worker_executor.submit(self._execute_worker_attempt, attempt, user_prompt, system_prompt)
            except Exception as exc:
                # Prompt-build / submit failed AFTER the attempt was persisted
                # as running. Without this revert, the attempt sits at
                # status="running" with no tracking future and its slot leaks
                # from the budget pool until --resume. Mark it failed so
                # _reconcile_orphaned_running_state isn't even needed for this
                # path on the same run, and so the target can re-queue.
                self._fail_orphan_attempt(target, attempt, f"scheduling failed: {exc}")
                continue
            self._worker_futures[future] = attempt.attempt_id
            scheduled = True

        # Process pending claim retries within remaining budget. Per-target
        # serialization: only one attempt may be in flight per target at a
        # time. Without this, two sibling rejected claims belonging to the
        # same target both dispatch concurrently, each call to
        # `_start_claim_retry_attempt` overwrites `target.active_attempt_id`,
        # and the loser's worker report is silently discarded by the
        # mismatch guard in `_apply_worker_result` — leaving the target
        # wedged at `status="running"` with `active_attempt_id=None`.
        if self._pending_claim_retries:
            available = self._available_worker_slots()
            kept: list[tuple[str, str]] = []
            consumed: set[str] = set()
            for target_id, claim_id in self._pending_claim_retries:
                target = self.state.targets.get(target_id)
                claim = self.state.claims.get(claim_id)
                if target is None or claim is None or claim.status != "rejected":
                    continue
                if self._is_target_ignored(target) or self._is_terminal_target(target):
                    continue
                if available <= 0 or target.active_attempt_id is not None or target_id in consumed:
                    kept.append((target_id, claim_id))
                    continue
                attempt = self._start_claim_retry_attempt(target, claim)
                try:
                    system_prompt, user_prompt = self._build_claim_retry_prompt(target, claim, attempt)
                    future = self._worker_executor.submit(
                        self._execute_worker_attempt, attempt, user_prompt, system_prompt
                    )
                except Exception as exc:
                    self._fail_orphan_attempt(target, attempt, f"scheduling failed: {exc}")
                    continue
                self._worker_futures[future] = attempt.attempt_id
                consumed.add(target_id)
                available -= 1
                scheduled = True
            self._pending_claim_retries = kept

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
            if self._is_terminal_target(target):
                # A target that has reached a terminal state does not get to
                # pull more work. Without this, a claim left mid-chain on a
                # target that later blocked would be re-scheduled here — and
                # this loop also flips the target back to "verifying", quietly
                # resurrecting a target the run had already finished with.
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
                session_id=str(uuid.uuid4()),
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
            try:
                system_prompt, user_prompt = self._build_verifier_prompt(target, claim, verification)
                future = self._verifier_executor.submit(
                    self._execute_verifier, verification, user_prompt, system_prompt
                )
            except Exception as exc:
                # Same orphan story as in `_schedule_workers`: revert
                # status="running" before the exception propagates so the slot
                # doesn't leak from the budget pool. Identical recovery to
                # ``normalize_for_resume``: bounce back to "pending" with
                # parent_session_id preserved for resume.
                if verification.session_id and not verification.parent_session_id:
                    verification.parent_session_id = verification.session_id
                verification.status = "pending"
                verification.started_at = None
                verification.completed_at = None
                verification.error = f"scheduling failed: {exc}"
                verification.disposition = None
                self.state.save()
                continue
            self._verifier_futures[future] = verification.verification_id
            scheduled = True
        return changed or scheduled

    def _fail_orphan_attempt(self, target: TargetRecord, attempt: WorkerAttempt, error: str) -> None:
        """Revert an attempt that was marked ``running`` but never got a future.

        Shared by `_schedule_workers` and the claim-retry submit path: when
        prompt-build or executor.submit raises after `_start_*_attempt` has
        already saved ``status="running"`` to disk, the slot would leak. This
        helper performs the same recovery as the worker-future-crash path in
        `_drain_completed_futures`, in-place.
        """
        attempt.status = "failed"
        attempt.error = error
        if attempt.completed_at is None:
            attempt.completed_at = time.time()
        if target.active_attempt_id == attempt.attempt_id:
            target.active_attempt_id = None
            target.error_retry_count += 1
            if target.error_retry_count > self.config.max_worker_retries:
                target.status = "blocked"
            else:
                target.status = "queued"
            target.updated_at = time.time()
        self.state.save()

    def _reconcile_orphaned_running_state(self) -> bool:
        """Reset any attempt/verification stuck at status="running" with no future.

        The shared-budget pool is sized off ``len(_worker_futures) + len(_verifier_futures)``,
        and the scheduling filters in step 1 of `_schedule_verifiers` and the
        ``queued_targets`` filter in `_schedule_workers` skip any record that's
        already running. So a record persisted as ``running`` in state but not
        present in our futures dict is invisible to both scheduler and drainer:
        the slot is free in the budget calc, but the scheduler cannot fill it
        because the underlying claim/target is wedged behind the orphan.

        How orphans arise: the scheduling code in
        `_schedule_workers`/`_schedule_verifiers` sets ``status="running"`` and
        saves state BEFORE building prompts and submitting the future. Any
        exception between save-state and dict-assignment (filesystem hiccup
        creating the scratch dir, prompt builder failure, executor at capacity,
        etc.) leaves a "running" record with no tracking future. The original
        run aborts on that exception, but on `--resume` the previous run's
        in-flight work was reset by ``normalize_for_resume`` — we need the same
        cleanup INSIDE a live run too, in case anything sneaks past.

        Detection is cheap (set membership against the two futures dicts) and
        safe to call every drain tick: a new attempt that was just submitted
        cannot be an orphan because we always add to the futures dict
        immediately after submit.
        """
        progressed = False
        live_attempt_ids = set(self._worker_futures.values())
        live_verification_ids = set(self._verifier_futures.values())

        for attempt in self.state.worker_attempts.values():
            if attempt.status != "running":
                continue
            if attempt.attempt_id in live_attempt_ids:
                continue
            attempt.status = "failed"
            if attempt.completed_at is None:
                attempt.completed_at = time.time()
            if not attempt.error:
                attempt.error = "orphaned-running-attempt-with-no-future"
            target = self.state.targets.get(attempt.target_id)
            if target is not None and target.active_attempt_id == attempt.attempt_id:
                target.active_attempt_id = None
                target.error_retry_count += 1
                if target.error_retry_count > self.config.max_worker_retries:
                    target.status = "blocked"
                else:
                    target.status = "queued"
                target.updated_at = time.time()
            progressed = True

        for verification in self.state.verifications.values():
            if verification.status != "running":
                continue
            if verification.verification_id in live_verification_ids:
                continue
            # Revert to "pending" so step 2 of `_schedule_verifiers` picks it up
            # again. Preserve session_id as parent_session_id so the next
            # attempt resumes the prior session if it had one. Identical
            # treatment to ``normalize_for_resume`` for interrupted verifiers.
            if verification.session_id and not verification.parent_session_id:
                verification.parent_session_id = verification.session_id
            verification.status = "pending"
            verification.started_at = None
            verification.completed_at = None
            verification.error = "orphaned-running-verification-with-no-future"
            verification.disposition = None
            progressed = True

        # Targets wedged at status="running" with no live attempt.
        # Distinct from the orphan-attempt loop above: that loop only catches
        # attempts persisted as "running"; this loop catches the inverse —
        # the target says it's running but no attempt is. Historical cause:
        # concurrent retries for the same target stomped active_attempt_id
        # and both reports were discarded by the mismatch guard. Recovery is
        # the same shape as the orphan-attempt path.
        now = time.time()
        for target in self.state.targets.values():
            if target.status != "running":
                continue
            aid = target.active_attempt_id
            if aid in live_attempt_ids:
                continue
            attempt = self.state.worker_attempts.get(aid) if aid else None
            if attempt is not None and attempt.status == "running":
                # Will be reconciled by the orphan-attempt loop above on this
                # same call (it runs first). Skip — the attempt path also
                # resets target.status, so we don't double-recover.
                continue
            target.active_attempt_id = None
            target.error_retry_count += 1
            if target.error_retry_count > self.config.max_worker_retries:
                target.status = "blocked"
                self.state.append_event(
                    "target.blocked",
                    target_id=target.target_id,
                    generation=target.active_generation,
                    blocker="orphaned-running-target-with-no-attempt",
                )
            else:
                target.status = "queued"
            target.updated_at = now
            progressed = True

        if progressed:
            self.state.save()
        return progressed

    def _drain_completed_futures(self) -> bool:
        progressed = False

        # Detect and recover slots leaked by attempts/verifications stuck at
        # status="running" with no tracking future. See the helper's docstring
        # for the failure mode this protects against.
        if self._reconcile_orphaned_running_state():
            progressed = True

        if self._drain_analyst_future():
            progressed = True

        if self._drain_env_builder_future():
            progressed = True

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
                self._record_infrastructure_error(attempt.error if attempt is not None else str(exc))
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

        for future, claim_id in list(self._exploit_sim_futures.items()):
            if not future.done():
                continue
            progressed = True
            self._exploit_sim_futures.pop(future, None)
            try:
                result = future.result()
                self._apply_exploit_sim_result(result)
            except Exception as exc:  # pragma: no cover - defensive, wrapper catches normally
                # Non-gating: an exploit-sim crash must never strand a verified
                # claim. Mark it sim_error and still hand it to the reporter.
                claim = self.state.claims.get(claim_id)
                if claim is not None:
                    claim.exploit_category = "sim_error"
                    self._enqueue_reporter(claim)
                    self.state.save()
                self._emit_analyst_message(f"[juvenal] exploit-sim for claim {claim_id} crashed: {exc}")

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
                and not any(claim.status == "verified" for claim in self.state.claims.values())
                and not (
                    delta.verified_claim_ids
                    or delta.rejected_claim_ids
                    or delta.no_findings_target_ids
                    or delta.blocked_target_ids
                    or delta.dependency_stranded_target_ids
                    or delta.exhausted_target_ids
                    or delta.pending_directive_ids
                    or delta.dropped_proposals
                )
            ):
                # Genuine retry-budget failure: every target reached terminal,
                # at least one was exhausted, AND no claim ever verified.
                # If even one claim verified, the run produced real output —
                # exhausted targets are normal noise on a real run, so the
                # all-terminal success path below should handle it.
                return True, False, "analysis exhausted retry budget across all targets"

            if (
                self.state.captain.turn_index > 0
                and not (
                    delta.verified_claim_ids
                    or delta.rejected_claim_ids
                    or delta.no_findings_target_ids
                    or delta.blocked_target_ids
                    or delta.dependency_stranded_target_ids
                    or delta.exhausted_target_ids
                    or delta.pending_directive_ids
                    or delta.dropped_proposals
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

        # taken_target_ids.json — every target_id ever registered (terminal
        # AND non-terminal). The captain proposes new targets via
        # `enqueue_targets[].target_id`, and `_normalize_captain_targets`
        # silently drops proposals that collide with any existing id. Without
        # this list the captain can't tell which ids are taken — terminal
        # targets are absent from frontier.json, so a captain that re-uses an
        # old id (e.g. `target-foo-t17` when an old `t17` is already
        # no_findings) will see its whole batch evaporate with no feedback.
        taken_ids = sorted(self.state.targets.keys())
        self._atomic_write(ctx / "taken_target_ids.json", json.dumps(taken_ids, indent=2))

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

    def _build_captain_prompt(self, *, summary_only: bool = False) -> tuple[str, str]:
        """Return (system_prompt, user_prompt) for the next captain call.

        ``system_prompt`` is non-empty only on the first turn (when the runner
        will call ``run_agent`` to start a new session). On subsequent turns
        the runner calls ``resume_agent``, which inherits the system prompt
        set at session creation; the returned system_prompt is empty in that
        case and only the dynamic per-turn payload ships via ``user_prompt``.
        """
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
            "dependency_stranded_targets": list(delta.dependency_stranded_target_ids),
            "exhausted_targets": list(delta.exhausted_target_ids),
            "frontier_counts": delta.frontier_counts,
            # If your previous turn proposed targets that didn't stick, they
            # show up here with the reason. Most common cause: target_id
            # collides with an already-terminal target (terminal targets
            # aren't in frontier.json, so you can't see them — generate
            # fresher ids, e.g. include a turn-number or hash suffix).
            "dropped_proposals": delta.dropped_proposals,
            # Why each target above ended, in the worker's own words. Read these
            # before enqueuing: a no_findings that closes a hypothesis with
            # evidence means the line is settled, not that it needs another
            # worker pointed at it.
            "target_outcome_notes": delta.target_outcome_notes,
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
            f"  - {ctx / 'taken_target_ids.json'} — every target_id already registered "
            "(terminal + non-terminal). When proposing new `enqueue_targets`, every "
            "`target_id` MUST be absent from this list — collisions are silently dropped.\n"
            "  These files are rewritten before every captain turn. Use Read / Grep to pull "
            "specific items when you need them — do not assume the prompt contains complete state.\n"
        )

        brief_block = self._project_brief_block(self.config.captain_backend)
        if is_first_turn:
            system_prompt = f"{self._captain_role_prompt}\n\nMission:\n{mission}"
            if brief_block:
                system_prompt = f"{system_prompt}\n\n{brief_block}"
            user_prompt = (
                f"Repository root: {self.working_dir}\n"
                f"Captain turn: 1\n"
                f"Mode: {mode_note}\n\n"
                f"{files_block}\n"
                "Pending user directives:\n"
                f"{json.dumps(pending_directives, indent=2)}\n"
            )
        else:
            system_prompt = ""
            # Captain turns 2+ use resume_agent, which inherits the system prompt
            # set on turn 1. Re-inject the ready brief into the user prompt so the
            # initialization evidence remains explicit on every turn.
            user_prompt_prefix = ""
            if brief_block:
                user_prompt_prefix = f"{brief_block}\n\n"
            user_prompt = (
                f"{user_prompt_prefix}"
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
            user_prompt = f"{nudge}\n\n{user_prompt}"
        if post_chat and not summary_only:
            user_prompt = (
                "## Resuming from free-form chat\n\n"
                "The user just had a free-form interactive conversation with you in their "
                "terminal. Acknowledge any directions they gave you in `message_to_user`, "
                "then RETURN to the structured analysis protocol — your next response must "
                "include exactly one CAPTAIN_JSON block as defined in the role prompt. Do "
                "not respond conversationally; the runner only consumes structured output "
                "going forward.\n\n"
            ) + user_prompt
        return system_prompt, user_prompt

    def _worker_system_prompt(self) -> str:
        """Static system prompt for the worker: framework role + workflow scope.

        When `worker_dynamic_workflow` is on (default), a backend-aware fan-out
        block turns the worker into a mini-captain of its single target — it
        spawns its own subagents to explore competing hypotheses in parallel and
        synthesizes, without changing its loop position or one-WORKER_JSON output.
        """
        base = self._worker_role_prompt
        if self._rendered_worker_prompt:
            base = f"{base}\n\n{self._rendered_worker_prompt}"
        if self.config.worker_dynamic_workflow:
            base = f"{base}\n\n{self._worker_fanout_block(self.config.worker_backend)}"
        brief_block = self._project_brief_block(self.config.worker_backend)
        if brief_block:
            base = f"{base}\n\n{brief_block}"
        return base

    def _scratch_dir_for_attempt(self, attempt: WorkerAttempt) -> Path:
        """Per-attempt scratch directory for worker PoC artifacts.

        Lives under .juvenal/scratch/ so it is hidden from the public output/
        tree. The reporter copies any artifacts from this dir into
        output/<bug-id>/ once verifiers pass.
        """
        return self.working_dir / ".juvenal" / "scratch" / attempt.attempt_id

    def _build_worker_prompt(self, target: TargetRecord, attempt: WorkerAttempt) -> tuple[str, str]:
        """Return (system_prompt, user_prompt) for an initial worker call."""
        scratch_dir = self._scratch_dir_for_attempt(attempt)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        try:
            scratch_rel = str(scratch_dir.relative_to(self.working_dir))
        except ValueError:
            scratch_rel = str(scratch_dir)
        task_packet = {
            "task_id": attempt.attempt_id,
            "target_id": target.target_id,
            "generation": attempt.generation,
            "title": target.title,
            "kind": target.kind,
            "priority": target.priority,
            "scope_paths": target.scope_paths,
            "scope_symbols": target.scope_symbols,
            "goal": target.goal or target.instructions,
            "instructions": target.instructions,
            "spawn_reason": target.spawn_reason,
            "allow_repo_tools": self.config.allow_repo_tools,
            "scratch_dir": scratch_rel,
        }
        user_prompt = (
            f"Repository root: `{self.working_dir}`\n\n"
            f"Scratch directory (PoC artifacts only — NEVER write under `output/`): `{scratch_rel}`\n\n"
            "Task packet:\n"
            f"```text\n{json.dumps(task_packet, indent=2)}\n```\n\n"
            "Verified dependencies:\n"
            f"```text\n{json.dumps(self._verified_dependency_payload(target), indent=2)}\n```\n\n"
            "Retry feedback or prior rejection context:\n"
            f"```text\n{json.dumps(self._retry_feedback_payload(target), indent=2)}\n```\n\n"
            "Code context pack:\n"
            f"```text\n{json.dumps(self._code_context_payload(target), indent=2)}\n```\n"
        )
        return self._worker_system_prompt(), user_prompt

    def _build_verifier_prompt(
        self, target: TargetRecord, claim: ClaimRecord, verification: VerificationRecord
    ) -> tuple[str, str]:
        """Return (system_prompt, user_prompt) for one verifier call.

        ``system_prompt`` carries the framework-level verifier role plus the
        per-spec workflow scope (both static across the call). ``user_prompt``
        carries the dynamic per-claim payload.
        """
        packet = asdict(claim_to_verifier_packet(claim))
        chain_length = len(self._verifier_chain)
        spec = self._verifier_chain[verification.verifier_index]
        rendered_scope = self._rendered_verifier_prompts.get(spec.name, "")

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
        system_prompt = self._verifier_role_prompt
        # Verifiers that opt in via `use_attack_surface_subagent: true` are a
        # literal materialization of the `.claude/agents/attack-surface.md`
        # subagent: load the subagent body and use it as the per-spec scope,
        # replacing the YAML prompt. The brief is already embedded inside the
        # subagent body so we skip the standard brief-block injection for
        # them. Falls back to the YAML scope if the subagent file is missing
        # (analyst failed / hasn't run).
        subagent_scope: str | None = None
        if spec.use_attack_surface_subagent:
            subagent_scope = self._load_subagent_scope_for_verifier(spec.backend)
        if subagent_scope is not None:
            system_prompt = f"{system_prompt}\n\n{subagent_scope}"
        else:
            if rendered_scope:
                system_prompt = f"{system_prompt}\n\n{rendered_scope}"
            brief_block = self._project_brief_block(spec.backend)
            if brief_block:
                system_prompt = f"{system_prompt}\n\n{brief_block}"
        try:
            mission_path = self._mission_file.relative_to(self.working_dir)
        except ValueError:
            mission_path = self._mission_file
        user_prompt = (
            f"Repository root: `{self.working_dir}`\n\n"
            "Chain context:\n"
            f"```text\n{json.dumps(chain_context, indent=2)}\n```\n\n"
            f"Mission file (read with the Read tool only if your per-spec scope is insufficient): "
            f"`{mission_path}`\n\n"
            "Target context:\n"
            f"```text\n{json.dumps(self._target_prompt_summary(target), indent=2)}\n```\n\n"
            "Verified dependencies:\n"
            f"```text\n{json.dumps(self._verified_dependency_payload(target), indent=2)}\n```\n\n"
            "Scrubbed claim packet:\n"
            f"```text\n{json.dumps(packet, indent=2)}\n```\n\n"
            "Code context pack:\n"
            f"```text\n{json.dumps(self._code_context_payload(target), indent=2)}\n```\n"
        )
        return system_prompt, user_prompt

    def _execute_worker_attempt(
        self,
        attempt: WorkerAttempt,
        prompt: str,
        system_prompt: str | None = None,
    ) -> _WorkerExecutionResult:
        backend = self._get_backend(self.config.worker_backend)
        worker_model = _resolve_model(self.config.worker_backend, "worker", self.config.worker_model)
        hooks_config = self._hooks_for_role("worker")

        def _assign(session_id: str) -> None:
            attempt.session_id = session_id

        record_session = self._session_id_recorder(_assign)
        parent_session_id = attempt.parent_session_id
        cold_restart = False
        if parent_session_id and parent_session_id in self._dead_sessions:
            parent_session_id = None
            cold_restart = True
        elif parent_session_id and self._session_is_stale(parent_session_id):
            # Parent session is too old to safely --resume (Claude session
            # expiration). Cold-restart with run_agent + system_prompt so the
            # worker has its full role + scope inherited from the start, and
            # the retry feedback ships as part of the user prompt. We also
            # drop the stale session_id below so the backend allocates a
            # fresh UUID — passing the stale UUID via `--session-id` makes
            # claude error with `Error: Session ID ... already in use`.
            if parent_session_id not in self._logged_stale_sessions:
                self._emit_analyst_message(
                    f"[juvenal] parent session {parent_session_id[:8]} is stale; "
                    "cold-restarting workers with system_prompt instead of --resume"
                )
                self._logged_stale_sessions.add(parent_session_id)
            parent_session_id = None
            cold_restart = True
        if parent_session_id:
            # Claim retry: resume the prior worker's session so its context
            # (codebase reading, build state, prior reasoning) carries over
            # and the rejection feedback arrives as a continuation rather
            # than a cold restart. The system prompt was set at the parent
            # session's run_agent call and is inherited; we do not re-apply
            # it here.
            result = backend.resume_agent(
                parent_session_id,
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("worker"),
                model=worker_model,
                hooks_config=hooks_config,
                on_session_id=record_session,
            )
        else:
            # On cold-restart, drop the (stale) attempt.session_id and let the
            # backend allocate a fresh UUID — passing the stale UUID via
            # `--session-id` makes claude error with `Error: Session ID …
            # already in use`. _apply_worker_result will write the fresh
            # session_id back onto the attempt record.
            effective_session_id = None if cold_restart else attempt.session_id
            result = backend.run_agent(
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("worker"),
                model=worker_model,
                system_prompt=system_prompt,
                session_id=effective_session_id,
                hooks_config=hooks_config,
                on_session_id=record_session,
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

    def _execute_verifier(
        self,
        verification: VerificationRecord,
        prompt: str,
        system_prompt: str | None = None,
    ) -> _VerifierExecutionResult:
        backend = self._get_backend(verification.backend)
        spec = self._verifier_chain[verification.verifier_index]
        hooks_config = self._hooks_for_role("verifier")

        def _assign(session_id: str) -> None:
            verification.session_id = session_id

        record_session = self._session_id_recorder(_assign)
        parent_session_id = verification.parent_session_id
        cold_restart = False
        if parent_session_id and parent_session_id in self._dead_sessions:
            parent_session_id = None
            cold_restart = True
        elif parent_session_id and self._session_is_stale(parent_session_id):
            if parent_session_id not in self._logged_stale_sessions:
                self._emit_analyst_message(
                    f"[juvenal] parent session {parent_session_id[:8]} is stale; "
                    "cold-restarting verifiers with system_prompt instead of --resume"
                )
                self._logged_stale_sessions.add(parent_session_id)
            parent_session_id = None
            cold_restart = True
        if parent_session_id:
            result = backend.resume_agent(
                parent_session_id,
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("verifier", verifier_name=verification.verifier_name),
                model=_resolve_model(spec.backend, "verifier", spec.model),
                hooks_config=hooks_config,
                on_session_id=record_session,
            )
        else:
            # See _execute_worker_attempt for why cold_restart drops session_id.
            effective_session_id = None if cold_restart else verification.session_id
            result = backend.run_agent(
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("verifier", verifier_name=verification.verifier_name),
                model=_resolve_model(spec.backend, "verifier", spec.model),
                system_prompt=system_prompt,
                session_id=effective_session_id,
                hooks_config=hooks_config,
                on_session_id=record_session,
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

    def _build_reporter_prompt(self, claim: ClaimRecord, target: TargetRecord) -> tuple[str, str]:
        """Return (system_prompt, user_prompt) for one reporter call.

        ``system_prompt`` carries the framework reporter role plus the workflow
        scope and the per-claim output directives (report_dir, bug_id). These
        are constant across the single call; the dynamic claim/artifact data
        ships via ``user_prompt``.
        """
        bug_id = self._bug_id_for_claim(claim)
        report_dir = self._report_dir_for_claim(claim)
        # Worker scratch dir for the attempt that produced this claim — the
        # reporter copies any PoC artifacts from there into report_dir.
        worker_attempt = self.state.worker_attempts.get(claim.attempt_id)
        if worker_attempt is not None:
            scratch_dir: Path | None = self._scratch_dir_for_attempt(worker_attempt)
        else:
            scratch_dir = None
        try:
            scratch_rel = str(scratch_dir.relative_to(self.working_dir)) if scratch_dir else ""
        except ValueError:
            scratch_rel = str(scratch_dir) if scratch_dir else ""
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

        scratch_block = (
            f"\nWorker scratch directory: `{scratch_rel}`\n"
            "  - The worker who produced this claim wrote any PoC artifacts here.\n"
            "  - List its contents and copy every file into the report directory before "
            "writing `report.md`. (Use `cp -a` or equivalent — preserve names.)\n"
            "  - The scratch directory may be empty if the worker emitted no artifacts; "
            "in that case synthesize a `poc` file from the claim packet (see below).\n"
            if scratch_rel
            else ""
        )
        system_prompt = (
            "You are the reporter agent for Juvenal's dynamic `analysis` phase.\n"
            "A claim has passed every verifier in the chain. You are the ONLY agent "
            "that writes anything under `output/` — workers and verifiers were told to "
            "stay out of that tree. Your job is to materialize the per-bug directory "
            "and write the durable, human-readable report there.\n\n"
            f"Repository root: `{self.working_dir}`\n"
            f"Report directory (create if missing): `{report_dir}`\n"
            f"Bug id: `{bug_id}`\n"
            f"{scratch_block}\n"
            "Required output:\n"
            f"- Create `{report_dir}` if it does not already exist.\n"
            f"- Copy any files from the worker scratch directory above into `{report_dir}`.\n"
            f"- Write `{report_dir}/report.md` containing:\n"
            "  - Title (one line)\n"
            "  - Severity (critical / high / medium / low) with one-sentence justification\n"
            "  - Primary location (file:line) and any secondary locations\n"
            "  - Description: what the bug is and why it matters\n"
            "  - Proof of Concept: the exact reproduction steps, input, or script. "
            "Include any sanitizer/crash output verbatim if present in the claim packet. "
            f"Reference any PoC files now in `{report_dir}/` by relative path.\n"
            "  - Impact: what an attacker can achieve\n"
            "  - Verifier consensus: a brief note that each verifier passed (poc, scope, novelty, etc.)\n"
            f"- If the scratch dir was empty, write a `poc` file (or `poc.<ext>`) "
            "capturing the trigger from the claim packet.\n"
            "- Overwriting an existing `report.md` is acceptable — this step is idempotent.\n\n"
            f"Do NOT write outside `{report_dir}` (the scratch dir is read-only to you — "
            "copy from it, do not edit it). Do NOT modify project source. After writing, "
            "exit cleanly. No structured-output block is expected from you — the runner "
            f"verifies success by checking that `{report_dir}/report.md` exists."
        )
        if self._rendered_reporter_prompt:
            system_prompt = f"{system_prompt}\n\n{self._rendered_reporter_prompt}"
        reporter_backend = self._reporter_spec.backend if self._reporter_spec else None
        brief_block = self._project_brief_block(reporter_backend)
        if brief_block:
            system_prompt = f"{system_prompt}\n\n{brief_block}"

        user_prompt = (
            "Claim packet:\n"
            f"```text\n{json.dumps(packet, indent=2)}\n```\n\n"
            "Worker artifact (reasoning, trace, commands):\n"
            f"```text\n{json.dumps(artifact_payload, indent=2, default=str)}\n```\n\n"
            "Target context:\n"
            f"```text\n{json.dumps(self._target_prompt_summary(target), indent=2)}\n```\n\n"
            "Verifier consensus (all of these PASSED):\n"
            f"```text\n{json.dumps(verifier_summaries, indent=2)}\n```\n\n"
            f"{self._exploitation_prompt_block(claim)}"
            "Code context pack:\n"
            f"```text\n{json.dumps(self._code_context_payload(target), indent=2)}\n```\n"
        )
        return system_prompt, user_prompt

    def _exploitation_prompt_block(self, claim: ClaimRecord) -> str:
        """Render the exploit-sim category into the reporter prompt so the report
        carries an 'Exploitation' line. Empty when exploit-sim is not configured."""
        if not self._exploit_sim_enabled():
            return ""
        descriptions = {
            "exploit_confirmed": "reproduced on the default configuration",
            "exploit_confirmed_nondefault": "reproduced only after granted non-default configuration changes",
            "exploit_unconfirmed": "valid but did not reproduce live in the simulation this run",
            "sim_inconclusive": "the simulation environment could not exercise the claim",
            "sim_error": "an infrastructure error occurred during simulation",
        }
        category = claim.exploit_category or "sim_inconclusive"
        detail = descriptions.get(category, "categorization unavailable")
        return (
            "Exploitation result (from the post-verification exploit-simulation stage):\n"
            f"```text\nexploit_category: {category}\nmeaning: {detail}\n```\n"
            'Include a one-line "Exploitation" entry in report.md stating this category.\n\n'
        )

    # --- Exploit-simulation (non-gating post-verification categorizer) ---------

    def _exploit_sim_enabled(self) -> bool:
        return self._exploit_sim_spec is not None and self._exploit_sim_spec.enabled

    def _enqueue_post_verification(self, claim: ClaimRecord) -> None:
        """Route a freshly-verified claim to the exploit-sim stage if enabled,
        otherwise straight to the reporter. Exploit-sim is NON-GATING: it never
        rejects the claim; on completion _apply_exploit_sim_result enqueues the
        reporter regardless of category."""
        if claim.reported_at is not None:
            return
        if self._exploit_sim_enabled() and not claim.exploit_sim_attempted:
            if (
                claim.claim_id not in self._pending_exploit_sim_claim_ids
                and claim.claim_id not in self._exploit_sim_futures.values()
            ):
                self._pending_exploit_sim_claim_ids.append(claim.claim_id)
            return
        self._enqueue_reporter(claim)

    def _enqueue_reporter(self, claim: ClaimRecord) -> None:
        if (
            self._reporter_spec is not None
            and claim.reported_at is None
            and claim.claim_id not in self._pending_reporter_claim_ids
            and claim.claim_id not in self._reporter_futures.values()
        ):
            self._pending_reporter_claim_ids.append(claim.claim_id)

    def _maybe_start_env_builder(self) -> None:
        """Submit the env-builder future at startup if exploit-sim is configured.

        NON-BLOCKING: unlike the analyst, workers/verifiers/captain proceed while
        the env builds. Terminal states (ready/failed) are sticky across resumes.
        """
        if not self._exploit_sim_enabled():
            return
        status = self.state.simulation_env.status
        if status in ("ready", "failed", "running"):
            return
        if self._env_builder_future is not None:
            return
        spec = self._exploit_sim_spec.env_builder
        now = time.time()
        self.state.simulation_env = SimulationEnvState(
            status="running",
            started_at=now,
            backend=spec.backend,
            model=_resolve_model(spec.backend, "env_builder", spec.model),
        )
        self.state.save()
        self._env_builder_future = self._env_builder_executor.submit(self._execute_env_builder)

    def _execute_env_builder(self) -> _EnvBuilderExecutionResult:
        spec = self._exploit_sim_spec.env_builder if self._exploit_sim_spec else None
        if spec is None:
            return _EnvBuilderExecutionResult(
                agent_result=_empty_agent_result(),
                brief="",
                artifact_path=None,
                instantiate_script=None,
                error="exploit_sim spec missing at execution time",
            )
        backend = self._get_backend(spec.backend)
        mission = self.phase.render_prompt(failure_context=self.failure_context, vars=self.workflow.vars)
        env_dir = self.working_dir / "output" / ".simulation-env"
        prompt = (
            (self._rendered_exploit_sim_prompts.get("env_builder") or _DEFAULT_ENV_BUILDER_PROMPT)
            .replace("{working_dir}", str(self.working_dir))
            .replace("{env_dir}", str(env_dir))
            .replace("{mission}", mission)
        )
        try:
            result = backend.run_agent(
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("env_builder"),
                model=_resolve_model(spec.backend, "env_builder", spec.model),
            )
        except Exception as exc:
            return _EnvBuilderExecutionResult(
                agent_result=_empty_agent_result(),
                brief="",
                artifact_path=None,
                instantiate_script=None,
                error=f"env_builder raised: {exc}",
            )
        if result.exit_code != 0:
            return _EnvBuilderExecutionResult(
                agent_result=result,
                brief="",
                artifact_path=None,
                instantiate_script=None,
                error=f"env_builder exited with code {result.exit_code}: {result.output[-2000:]}",
            )
        brief = result.output.strip()
        if not brief:
            return _EnvBuilderExecutionResult(
                agent_result=result,
                brief="",
                artifact_path=None,
                instantiate_script=None,
                error="env_builder returned empty output",
            )
        artifact_path, instantiate_script = _parse_env_builder_brief(brief)
        return _EnvBuilderExecutionResult(
            agent_result=result,
            brief=brief,
            artifact_path=artifact_path,
            instantiate_script=instantiate_script,
            error=None,
        )

    def _drain_env_builder_future(self) -> bool:
        with self._env_builder_lock:
            if self._env_builder_future is None or not self._env_builder_future.done():
                return False
            future = self._env_builder_future
            self._env_builder_future = None
        try:
            outcome = future.result()
        except Exception as exc:
            self._record_env_builder_failure(f"future crashed: {exc}")
            return True
        self._add_tokens(outcome.agent_result)
        if outcome.error is not None:
            self._record_env_builder_failure(outcome.error, agent_result=outcome.agent_result)
            return True
        self._record_env_builder_success(outcome)
        return True

    def _record_env_builder_success(self, outcome: _EnvBuilderExecutionResult) -> None:
        now = time.time()
        (self.working_dir / "output" / ".simulation-env").mkdir(parents=True, exist_ok=True)
        started = self.state.simulation_env.started_at or now
        self.state.simulation_env = SimulationEnvState(
            status="ready",
            brief=outcome.brief,
            artifact_path=outcome.artifact_path,
            instantiate_script=outcome.instantiate_script,
            error=None,
            started_at=started,
            completed_at=now,
            duration_seconds=max(0.0, now - started),
            input_tokens=outcome.agent_result.input_tokens,
            output_tokens=outcome.agent_result.output_tokens,
            session_id=outcome.agent_result.session_id,
            backend=self.state.simulation_env.backend,
            model=self.state.simulation_env.model,
        )
        self.state.save()
        self._emit_analyst_message(
            f"[juvenal] exploit-sim environment ready ({self.state.simulation_env.duration_seconds:.0f}s)"
        )

    def _record_env_builder_failure(self, error: str, *, agent_result: AgentResult | None = None) -> None:
        if self._note_quota_exhaustion(error) or self._note_quota_exhaustion(agent_result):
            # Sticky `failed` state, same as the analyst: don't spend it on a
            # refusal that never ran the builder.
            self.state.simulation_env = SimulationEnvState(
                status="pending",
                backend=self.state.simulation_env.backend,
                model=self.state.simulation_env.model,
            )
            self.state.save()
            return
        now = time.time()
        started = self.state.simulation_env.started_at or now
        self.state.simulation_env = SimulationEnvState(
            status="failed",
            brief=None,
            artifact_path=None,
            instantiate_script=None,
            error=error,
            started_at=started,
            completed_at=now,
            duration_seconds=max(0.0, now - started),
            input_tokens=agent_result.input_tokens if agent_result is not None else 0,
            output_tokens=agent_result.output_tokens if agent_result is not None else 0,
            session_id=agent_result.session_id if agent_result is not None else None,
            backend=self.state.simulation_env.backend,
            model=self.state.simulation_env.model,
        )
        self.state.save()

    def _finalize_env_builder_on_shutdown(self) -> None:
        if self._env_builder_future is None:
            return
        try:
            self._env_builder_future.result(timeout=10.0)
        except Exception:
            pass
        try:
            self._drain_env_builder_future()
        except Exception:
            pass

    def _wait_for_env_builder_ready(self) -> bool:
        """Block (per-claim) until env_builder reaches a terminal state. Returns
        True if the env is ready, False if it failed or the wait was interrupted."""
        if self.state.simulation_env.status in ("ready", "failed"):
            return self.state.simulation_env.status == "ready"
        while self._env_builder_future is not None and not self._env_builder_future.done():
            if self._shutdown_event.is_set() or self.state.control.stop_requested:
                return False
            time.sleep(_IDLE_SLEEP_SECONDS)
        if self._env_builder_future is not None:
            self._drain_env_builder_future()
        return self.state.simulation_env.status == "ready"

    def _schedule_exploit_sim(self) -> bool:
        """Submit exploit-sim attempts for verified-but-not-yet-attempted claims.

        NON-GATING: verified claims proceed to the reporter regardless of the
        exploit-sim outcome. Runs one attempt at a time so each claim gets a
        fresh env instance without cross-claim state pollution."""
        if not self._exploit_sim_enabled():
            return False
        if self._terminal_failure or self.state.control.stop_requested:
            return False
        if not self._pending_exploit_sim_claim_ids:
            return False
        available = 1 - len(self._exploit_sim_futures)
        if available <= 0:
            return False

        scheduled = False
        remaining: list[str] = []
        in_flight = set(self._exploit_sim_futures.values())
        for claim_id in self._pending_exploit_sim_claim_ids:
            if available <= 0 or claim_id in in_flight:
                remaining.append(claim_id)
                continue
            claim = self.state.claims.get(claim_id)
            if claim is None or claim.status != "verified" or claim.exploit_sim_attempted:
                continue
            target = self.state.targets.get(claim.target_id)
            if target is None:
                continue
            attempts = self._exploit_sim_attempts.get(claim_id, 0)
            if attempts >= self._exploit_sim_spec.max_attempts:
                continue
            exploit_sim_id = str(uuid.uuid4())
            claim.exploit_sim_id = exploit_sim_id
            claim.exploit_sim_attempted = True
            self.state.save()
            future = self._exploit_sim_executor.submit(self._execute_exploit_sim, claim, target, exploit_sim_id)
            self._exploit_sim_futures[future] = claim_id
            self._exploit_sim_attempts[claim_id] = attempts + 1
            available -= 1
            scheduled = True

        self._pending_exploit_sim_claim_ids = remaining
        return scheduled

    def _execute_exploit_sim(
        self,
        claim: ClaimRecord,
        target: TargetRecord,
        exploit_sim_id: str,
    ) -> _ExploitSimExecutionResult:
        """Env-builder (blocking wait) → runner-bounded attacker<->simulator loop
        → exploit_judge categorization. Any infra failure yields sim_error /
        sim_inconclusive with the claim STILL verified."""
        spec = self._exploit_sim_spec
        if spec is None:
            return _ExploitSimExecutionResult(
                claim_id=claim.claim_id,
                category="sim_inconclusive",
                config_deltas=[],
                transcript_refs=[],
                exchange_rounds=0,
                agent_results=[],
                error="exploit_sim spec missing",
            )

        if not self._wait_for_env_builder_ready():
            return _ExploitSimExecutionResult(
                claim_id=claim.claim_id,
                category="sim_inconclusive",
                config_deltas=[],
                transcript_refs=[],
                exchange_rounds=0,
                agent_results=[],
                error=f"simulation environment not ready (status={self.state.simulation_env.status})",
            )

        sim_dir = self.working_dir / "output" / ".simulation-env" / exploit_sim_id
        try:
            sim_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            return _ExploitSimExecutionResult(
                claim_id=claim.claim_id,
                category="sim_error",
                config_deltas=[],
                transcript_refs=[],
                exchange_rounds=0,
                agent_results=[],
                error=f"could not create sim workspace: {exc}",
            )

        agent_results: list[AgentResult] = []
        transcript_refs: list[str] = []
        config_deltas: list[str] = []
        transcript_lines: list[str] = []
        packet = json.dumps(asdict(claim_to_verifier_packet(claim)), indent=2)
        env_brief = self.state.simulation_env.brief or ""

        try:
            simulator_backend = self._get_backend(spec.simulator.backend)
            attacker_backend = self._get_backend(spec.attacker.backend)

            # Runner-owned bounded dialogue. The integer cap — NOT an LLM — decides
            # when the exchange stops.
            simulator_session: str | None = None
            attacker_session: str | None = None
            rounds_completed = 0
            for round_index in range(spec.max_exchange_rounds):
                sim_prompt = (
                    (self._rendered_exploit_sim_prompts.get("simulator") or _DEFAULT_SIMULATOR_PROMPT)
                    .replace("{working_dir}", str(self.working_dir))
                    .replace("{sim_dir}", str(sim_dir))
                    .replace("{env_brief}", env_brief)
                    .replace("{claim_packet}", packet)
                    .replace("{round}", str(round_index + 1))
                    .replace("{max_rounds}", str(spec.max_exchange_rounds))
                    .replace("{attacker_last}", transcript_lines[-1] if transcript_lines else "(none yet)")
                )
                sim_result = self._run_dialogue_turn(
                    simulator_backend, "simulator", sim_prompt, sim_dir, simulator_session, spec.simulator.model
                )
                agent_results.append(sim_result)
                simulator_session = sim_result.session_id or simulator_session
                transcript_lines.append(f"[simulator r{round_index + 1}] {sim_result.output.strip()}")
                config_deltas.extend(_parse_config_deltas(sim_result.output))

                att_prompt = (
                    (self._rendered_exploit_sim_prompts.get("attacker") or _DEFAULT_ATTACKER_PROMPT)
                    .replace("{working_dir}", str(self.working_dir))
                    .replace("{sim_dir}", str(sim_dir))
                    .replace("{claim_packet}", packet)
                    .replace("{round}", str(round_index + 1))
                    .replace("{max_rounds}", str(spec.max_exchange_rounds))
                    .replace("{simulator_last}", sim_result.output.strip())
                )
                att_result = self._run_dialogue_turn(
                    attacker_backend, "attacker", att_prompt, sim_dir, attacker_session, spec.attacker.model
                )
                agent_results.append(att_result)
                attacker_session = att_result.session_id or attacker_session
                transcript_lines.append(f"[attacker r{round_index + 1}] {att_result.output.strip()}")
                config_deltas.extend(_parse_config_deltas(att_result.output))
                rounds_completed = round_index + 1
        except Exception as exc:
            return _ExploitSimExecutionResult(
                claim_id=claim.claim_id,
                category="sim_error",
                config_deltas=config_deltas,
                transcript_refs=transcript_refs,
                exchange_rounds=0,
                agent_results=agent_results,
                error=f"attacker/simulator dialogue crashed: {exc}",
            )

        transcript_path = sim_dir / "dialogue.log"
        try:
            transcript_path.write_text("\n".join(transcript_lines), encoding="utf-8")
            transcript_refs.append(str(transcript_path))
        except Exception:
            pass

        # exploit_judge categorizes from the transcript + config-delta log.
        judge_prompt = (
            (self._rendered_exploit_sim_prompts.get("exploit_judge") or _DEFAULT_EXPLOIT_JUDGE_PROMPT)
            .replace("{working_dir}", str(self.working_dir))
            .replace("{claim_packet}", packet)
            .replace("{transcript}", "\n".join(transcript_lines))
            .replace("{config_deltas}", json.dumps(sorted(set(config_deltas))))
        )
        try:
            judge_backend = self._get_backend(spec.judge.backend)
            judge_result = judge_backend.run_agent(
                judge_prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("exploit_judge"),
                model=_resolve_model(spec.judge.backend, "exploit_judge", spec.judge.model),
            )
            agent_results.append(judge_result)
        except Exception as exc:
            return _ExploitSimExecutionResult(
                claim_id=claim.claim_id,
                category="sim_error",
                config_deltas=sorted(set(config_deltas)),
                transcript_refs=transcript_refs,
                exchange_rounds=rounds_completed,
                agent_results=agent_results,
                error=f"exploit_judge raised: {exc}",
            )
        if judge_result.exit_code != 0:
            return _ExploitSimExecutionResult(
                claim_id=claim.claim_id,
                category="sim_inconclusive",
                config_deltas=sorted(set(config_deltas)),
                transcript_refs=transcript_refs,
                exchange_rounds=rounds_completed,
                agent_results=agent_results,
                error=f"exploit_judge exited with code {judge_result.exit_code}",
            )
        category, judge_deltas, judge_error = _parse_exploit_judge_output(judge_result.output)
        merged_deltas = sorted(set(config_deltas) | set(judge_deltas))
        return _ExploitSimExecutionResult(
            claim_id=claim.claim_id,
            category=category,
            config_deltas=merged_deltas,
            transcript_refs=transcript_refs,
            exchange_rounds=rounds_completed,
            agent_results=agent_results,
            error=judge_error,
        )

    def _run_dialogue_turn(
        self,
        backend: Backend,
        role: str,
        prompt: str,
        sim_dir: Path,
        session_id: str | None,
        model: str | None,
    ) -> AgentResult:
        if session_id:
            return backend.resume_agent(
                session_id,
                prompt,
                working_dir=str(sim_dir),
                timeout=self.phase.timeout,
                env=self._role_env(role),
                model=_resolve_model(backend.name(), role, model),
            )
        return backend.run_agent(
            prompt,
            working_dir=str(sim_dir),
            timeout=self.phase.timeout,
            env=self._role_env(role),
            model=_resolve_model(backend.name(), role, model),
        )

    def _apply_exploit_sim_result(self, result: _ExploitSimExecutionResult) -> None:
        """Record the exploit-sim category on the claim, then enqueue the reporter.
        NEVER rejects a verified claim or touches claim.status / rejection_class."""
        for agent_result in result.agent_results:
            self._add_tokens(agent_result)
        claim = self.state.claims.get(result.claim_id)
        if claim is None:
            return
        claim.exploit_category = result.category
        self._enqueue_reporter(claim)
        self.state.save()

    def _rebuild_pending_exploit_sim_claim_ids(self) -> None:
        """On resume, enqueue verified claims not yet exploit-sim-attempted."""
        self._pending_exploit_sim_claim_ids = []
        if not self._exploit_sim_enabled():
            return
        in_flight = set(self._exploit_sim_futures.values())
        for claim in self.state.claims.values():
            if claim.status != "verified" or claim.reported_at is not None:
                continue
            if claim.exploit_sim_attempted or claim.claim_id in in_flight:
                continue
            self._pending_exploit_sim_claim_ids.append(claim.claim_id)

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
            # Pre-allocate the reporter session id so a Ctrl-C / rate-limit
            # crash mid-call leaves a recoverable id on the claim. If the id
            # is already set on entry, this scheduling call is either an
            # in-process retry or a resume after a prior killed run — both
            # need resume_agent to continue the existing session.
            if claim.reporter_session_id:
                is_retry = True
            else:
                claim.reporter_session_id = str(uuid.uuid4())
                self.state.save()
                is_retry = False
            system_prompt, user_prompt = self._build_reporter_prompt(claim, target)
            future = self._reporter_executor.submit(self._execute_reporter, claim, user_prompt, system_prompt, is_retry)
            self._reporter_futures[future] = claim_id
            self._reporter_attempts[claim_id] = attempts + 1
            available -= 1
            scheduled = True

        self._pending_reporter_claim_ids = remaining
        return scheduled

    def _execute_reporter(
        self,
        claim: ClaimRecord,
        prompt: str,
        system_prompt: str | None = None,
        is_retry: bool = False,
    ) -> _ReporterExecutionResult:
        spec_backend = self._reporter_spec.backend if self._reporter_spec else "claude"
        spec_model = self._reporter_spec.model if self._reporter_spec else None
        backend = self._get_backend(spec_backend)
        worker_attempt = self.state.worker_attempts.get(claim.attempt_id)
        scratch_dir = self._scratch_dir_for_attempt(worker_attempt) if worker_attempt is not None else None
        hooks_config = self._hooks_for_role("reporter", scratch_dir=scratch_dir)

        def _assign(session_id: str) -> None:
            claim.reporter_session_id = session_id

        record_session = self._session_id_recorder(_assign)
        if is_retry and claim.reporter_session_id in self._dead_sessions:
            # Re-key onto a fresh id so the cold start below does not hand the
            # backend a session id it has already rejected.
            claim.reporter_session_id = str(uuid.uuid4())
            is_retry = False
        if is_retry and claim.reporter_session_id:
            result = backend.resume_agent(
                claim.reporter_session_id,
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("reporter"),
                model=_resolve_model(spec_backend, "reporter", spec_model),
                hooks_config=hooks_config,
                on_session_id=record_session,
            )
        else:
            result = backend.run_agent(
                prompt,
                working_dir=str(self.working_dir),
                timeout=self.phase.timeout,
                env=self._role_env("reporter"),
                model=_resolve_model(spec_backend, "reporter", spec_model),
                system_prompt=system_prompt,
                session_id=claim.reporter_session_id,
                hooks_config=hooks_config,
                on_session_id=record_session,
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
        self._note_agent_result(result.agent_result)
        claim = self.state.claims.get(result.claim_id)
        if claim is None:
            return
        if result.error:
            # Leave reported_at unset; _schedule_reporters will retry up to _MAX_REPORTER_ATTEMPTS.
            attempts = self._reporter_attempts.get(claim.claim_id, 0)
            refused_before_running = self._note_quota_exhaustion(result.error) or self._note_dead_session(
                claim.reporter_session_id, result.error
            )
            if refused_before_running and attempts > 0:
                # Refund the attempt the refusal consumed; the next pass
                # cold-starts on a fresh session id.
                attempts -= 1
                self._reporter_attempts[claim.claim_id] = attempts
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
        self._note_agent_result(result.agent_result)
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
                if self._note_quota_exhaustion(result.error) or self._note_dead_session(
                    attempt.parent_session_id, result.error
                ):
                    # The backend refused before the worker ran — spent quota or
                    # a dead session — so this failure says nothing about the
                    # target. Re-queue for a cold start without spending budget
                    # the worker never got to use.
                    target.status = "queued"
                else:
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
            self._record_infrastructure_error(result.agent_result)
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
            self._record_infrastructure_error(attempt.error)
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
            self._record_infrastructure_error(attempt.error)
            return

        attempt.status = "completed"
        attempt.error = ""
        self._record_success()
        if target.active_generation != attempt.generation or target.active_attempt_id != attempt.attempt_id:
            # Stale completion — the target moved on to a different attempt
            # or generation. Do NOT touch `target.active_attempt_id`: it
            # belongs to the live successor, and clearing it here would
            # orphan that attempt (its mismatch path on completion would
            # then leave the target wedged at `status="running"` with
            # `active_attempt_id=None`).
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
            # Carry the worker's rationale, exactly as `target.blocked` carries
            # its blocker. A rigorous negative is a result the captain must be
            # able to act on; dropping it here made every no_findings look
            # alike and invited re-spawning work that was already settled.
            self.state.append_event(
                "target.no_findings",
                target_id=target.target_id,
                generation=attempt.generation,
                summary=report.summary,
            )
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
        self._note_agent_result(result.agent_result)
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
            self._enqueue_post_verification(claim)
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
            # The retry worker confirms the rejection: either it actively
            # found no evidence (no_findings) or it cannot proceed
            # (blocked). Either way the worker now agrees the claim was
            # false — running the same investigation 9 more times to
            # re-confirm wastes tokens. Burn the entire remaining budget
            # so _refresh_target_after_verification rolls the target up to
            # `exhausted` immediately, and the dead-dep sweep can cascade
            # any dependents to `blocked` on the next loop tick.
            if original_claim is not None:
                original_claim.retry_count = self.config.max_worker_retries
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
        # If a prior initial-worker attempt on this target+generation crashed
        # (rate-limit, Ctrl-C mid-stream, malformed output), inherit its session
        # so the new attempt resumes that conversation rather than cold-starting.
        # Only initial attempts are considered here — claim-retry attempts have
        # their own resume path in _start_claim_retry_attempt.
        parent_session_id: str | None = None
        prior = [
            a
            for a in self.state.worker_attempts.values()
            if a.target_id == target.target_id
            and a.generation == generation
            and a.retry_claim_id is None
            and a.status == "failed"
            and a.session_id
            and a.session_id not in self._dead_sessions
        ]
        if prior:
            most_recent = max(prior, key=lambda a: a.started_at or 0.0)
            parent_session_id = most_recent.session_id
        attempt = WorkerAttempt(
            attempt_id=self._next_attempt_id(target.target_id, generation),
            target_id=target.target_id,
            generation=generation,
            backend=self.config.worker_backend,
            session_id=str(uuid.uuid4()) if parent_session_id is None else parent_session_id,
            status="running",
            started_at=time.time(),
            completed_at=None,
            parent_session_id=parent_session_id,
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
        resumable = [a for a in existing if a.session_id not in self._dead_sessions]
        if resumable:
            most_recent = max(resumable, key=lambda a: a.started_at or 0.0)
            parent_session_id = most_recent.session_id
        if parent_session_id is None:
            origin = self.state.worker_attempts.get(claim.attempt_id)
            if origin is not None and origin.session_id not in self._dead_sessions:
                parent_session_id = origin.session_id
        attempt = WorkerAttempt(
            attempt_id=f"{target.target_id}-g{generation}-retry-{claim.claim_id}-{len(existing) + 1}",
            target_id=target.target_id,
            generation=generation,
            backend=self.config.worker_backend,
            session_id=parent_session_id if parent_session_id else str(uuid.uuid4()),
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

    def _build_claim_retry_prompt(
        self, target: TargetRecord, claim: ClaimRecord, attempt: WorkerAttempt
    ) -> tuple[str, str]:
        """Build a worker prompt scoped to re-investigating a single rejected claim.

        Returns (system_prompt, user_prompt). The system prompt is the same as
        for an initial worker call (framework role + workflow worker scope) so
        that fresh-session retries (no parent_session_id) still anchor the
        worker's identity. Resume-based retries inherit the system prompt set
        at the parent session and the executor will not re-apply it.
        """
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
        user_prompt = (
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
        return self._worker_system_prompt(), user_prompt

    def _normalize_captain_targets(self, turn: CaptainTurn) -> list[TargetRecord]:
        now = time.time()
        normalized: list[TargetRecord] = []
        seen_ids: set[str] = set()
        for proposal in turn.enqueue_targets:
            # Surface drops as events so the captain sees them on its next
            # delta and can self-correct (and so users on --interactive see
            # the reason in the dashboard). Without this, the captain
            # repeatedly proposes targets that get silently filtered and
            # never learns why — its frontier-context files only list
            # non-terminal targets, so it can't tell which ids are taken.
            if proposal.target_id in seen_ids:
                self.state.append_event(
                    "captain.proposal_dropped",
                    target_id=proposal.target_id,
                    reason="duplicate-in-this-batch",
                )
                continue
            if proposal.target_id in self.state.targets:
                existing_status = self.state.targets[proposal.target_id].status
                self.state.append_event(
                    "captain.proposal_dropped",
                    target_id=proposal.target_id,
                    reason=f"target-id-already-exists (status={existing_status})",
                )
                continue
            try:
                validate_target_scope(proposal.scope_paths, self.working_dir)
            except ValueError as exc:
                self.state.append_event(
                    "captain.proposal_dropped",
                    target_id=proposal.target_id,
                    reason=f"scope-invalid: {exc}",
                )
                continue
            missing_deps = [d for d in proposal.depends_on_claim_ids if d not in self.state.claims]
            if missing_deps:
                self.state.append_event(
                    "captain.proposal_dropped",
                    target_id=proposal.target_id,
                    reason=f"depends_on_claim_ids references unknown claim(s): {missing_deps}",
                )
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
                    goal=proposal.goal,
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
        if self._exploit_sim_futures or self._pending_exploit_sim_claim_ids:
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
            "goal": target.goal,
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
        quota_exhausted = self._note_quota_exhaustion(verification.error)
        dead_session = self._note_dead_session(verification.parent_session_id, verification.error)
        if not dead_session and not quota_exhausted:
            target.error_retry_count += 1
        if verification.verification_id in target.pending_verification_ids:
            target.pending_verification_ids.remove(verification.verification_id)
        if dead_session or quota_exhausted or target.error_retry_count <= self.config.max_worker_retries:
            # Resume the prior verifier session if it had time to register one
            # (i.e., the subprocess started). The verifier's `session_id` is
            # pre-allocated at VerificationRecord construction so even a
            # Ctrl-C-mid-stream crash leaves a usable id behind.
            resume_from = verification.session_id
            if resume_from in self._dead_sessions:
                resume_from = None
            new_verification = VerificationRecord(
                verification_id=self._next_verification_id(claim.claim_id),
                claim_id=claim.claim_id,
                target_id=claim.target_id,
                generation=verification.generation,
                backend=verification.backend,
                verifier_role=verification.verifier_role,
                session_id=resume_from if resume_from else str(uuid.uuid4()),
                status="pending",
                disposition=None,
                reason="",
                rejection_class=None,
                raw_output="",
                started_at=None,
                completed_at=None,
                verifier_name=verification.verifier_name,
                verifier_index=verification.verifier_index,
                parent_session_id=resume_from,
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
        self._record_infrastructure_error(verification.error)

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
        exploit_sim_on = self._exploit_sim_enabled()
        for claim in self.state.claims.values():
            if claim.status != "verified":
                continue
            if claim.reported_at is not None:
                continue
            if claim.claim_id in in_flight or claim.claim_id in queued:
                continue
            # When exploit-sim is enabled, a claim that has not yet been attempted
            # belongs in the exploit-sim queue first — the reporter runs only after
            # categorization. Attempted claims fall through to the reporter here.
            if exploit_sim_on and not claim.exploit_sim_attempted:
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

    def _record_infrastructure_error(self, error: str | AgentResult | None = None) -> None:
        """Track a worker/verifier infrastructure failure (crash, malformed output, non-zero exit).

        Does NOT count verifier rejections — those are normal operation.

        Only triggers ``_rate_limit_backoff`` when the recent errors actually look
        like rate limits (429, "monthly limit", "your limit · resets", etc.) or
        when an ``AgentResult`` carrying ``rate_limit_status == 429`` is provided.
        Other consecutive errors (parse failures, identity mismatches, output
        without a structured block) do NOT trigger long sleeps — backoff cannot
        recover those, and the per-target ``error_retry_count`` budget already
        contains runaway loops by marking targets ``blocked`` after enough
        retries.
        """
        self._consecutive_errors += 1
        if not self._error_looks_like_rate_limit(error):
            return
        if self._consecutive_errors >= self.config.max_consecutive_errors:
            self._rate_limit_backoff()

    def _error_looks_like_rate_limit(self, error: str | AgentResult | None) -> bool:
        if error is None:
            return False
        # Spent quota is checked first and never treated as a throttle: it can
        # carry a 429 or the word "limit" while being unrecoverable by waiting.
        if _is_quota_exhaustion(error):
            return False
        if isinstance(error, AgentResult) and error.rate_limit_status == 429:
            return True
        text = _error_text(error).lower()
        return any(sig in text for sig in _RATE_LIMIT_ERROR_SIGNATURES)

    def _note_quota_exhaustion(self, error: str | AgentResult | None) -> bool:
        """Record a backend refusal caused by spent account quota.

        Returns True for *every* quota error, not just the first, because each
        caller uses it to skip charging its retry budget: the agent never ran,
        so the failure says nothing about the work. The run stops instead of
        backing off — the reset can be days out — and `--resume` re-dispatches
        the untouched targets once the quota is back.
        """
        if not _is_quota_exhaustion(error):
            return False
        if not self._quota_exhausted:
            self._quota_exhausted = True
            detail = " ".join(_error_text(error).split())[-400:]
            self._terminal_failure = (
                f"backend quota exhausted, so no agent work can proceed: {detail} "
                "No retry budget was spent; state saved, --resume once the quota resets."
            )
            print(f"\n[juvenal] {self._terminal_failure}", flush=True)
        return True

    def _note_agent_result(self, agent_result: AgentResult | None) -> None:
        """Record observable signals from a finished agent result for downstream backoff decisions."""
        if agent_result is None:
            return
        if agent_result.rate_limit_status == 429 and not _is_quota_exhaustion(agent_result):
            self._last_observed_rate_limit_at = time.time()

    def _rate_limit_backoff(self) -> None:
        """Sleep until the agent CLI is available again, then resume.

        Two cadences. When the most recent agent result observed a Claude 429
        (the CLI surfaced an upstream rate limit), sleep on a fixed probe
        schedule keyed off the typical 5-hour Anthropic reset window — quick
        early checks, then hourly, with a guaranteed probe a few minutes after
        the 5h mark. Each probe is a one-shot `claude --print 'ok'` so the
        token cost is negligible.

        For generic crashes (no 429 observed) fall back to exponential
        backoff (60s → 1h doubling), since the cause may not be a rate limit
        and probing won't help.

        Cumulative sleep is capped at `max_total_backoff_seconds`; once
        exhausted the run gives up rather than waiting longer. The user can
        Ctrl+C at any time during the sleep — state is already saved so
        --resume picks up where it left off.
        """
        self.state.save()
        max_total = self.config.max_total_backoff_seconds
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

        observed = self._last_observed_rate_limit_at
        # Use the probe schedule when we saw a 429 within the cumulative cap;
        # otherwise stale signals shouldn't keep us probing forever.
        if observed is not None and (time.time() - observed) <= max_total:
            self._probe_backoff(observed_at=observed, remaining_budget=remaining_budget)
        else:
            self._exponential_backoff(remaining_budget=remaining_budget)

    def _exponential_backoff(self, *, remaining_budget: float) -> None:
        max_single = self.config.max_single_backoff_seconds
        delay = min(60 * (2**self._backoff_count), max_single)
        delay = min(delay, remaining_budget)
        self._backoff_count += 1
        minutes = delay / 60
        cumulative_minutes = (self._total_backoff_seconds + delay) / 60
        max_total = self.config.max_total_backoff_seconds
        print(
            f"\n[juvenal] {self._consecutive_errors} consecutive errors — "
            f"likely rate limit. Sleeping {minutes:.0f}m before retrying "
            f"(cumulative: {cumulative_minutes:.0f}m of {max_total / 60:.0f}m cap). "
            "State saved (Ctrl+C to exit, --resume to continue later).",
            flush=True,
        )
        self._pause_pipeline_active_timer()
        try:
            interrupted = self._sleep_with_shutdown(delay)
        finally:
            self._resume_pipeline_active_timer()
        if interrupted:
            self._total_backoff_seconds += delay
            return
        self._total_backoff_seconds += delay
        self._consecutive_errors = 0

    # Probe cadence keyed off the typical 5-hour Anthropic rate-limit reset.
    # Quick checks the first half hour, then hourly, then a guaranteed probe
    # 3 minutes past the 5h mark, then hourly indefinitely (capped by the
    # cumulative max_total_backoff_seconds budget).
    _RATE_LIMIT_PROBE_SCHEDULE: tuple[float, ...] = (
        300.0,  # 5 min
        600.0,  # 10 min
        1800.0,  # 30 min
        3600.0,  # 1 h
        7200.0,  # 2 h
        10800.0,  # 3 h
        14400.0,  # 4 h
        18000.0 + 180.0,  # 5h 3min — safety margin past typical reset
    )
    _RATE_LIMIT_PROBE_INTERVAL_AFTER_SCHEDULE: float = 3600.0  # then hourly

    def _probe_backoff(self, *, observed_at: float, remaining_budget: float) -> None:
        backend = self._claude_probe_backend()
        if backend is None:
            # No Claude backend in this configuration — fall back to exponential.
            self._exponential_backoff(remaining_budget=remaining_budget)
            return
        env = self._role_env("worker") if self.config.worker_backend == "claude" else self._role_env("captain")
        elapsed = time.time() - observed_at
        marks = list(self._RATE_LIMIT_PROBE_SCHEDULE)
        # Generate hourly marks past the schedule; loop emits one mark per probe.
        next_extra_mark = self._RATE_LIMIT_PROBE_SCHEDULE[-1] + self._RATE_LIMIT_PROBE_INTERVAL_AFTER_SCHEDULE
        print(
            f"\n[juvenal] {self._consecutive_errors} consecutive errors — Claude rate limit (HTTP 429) "
            f"observed at +{int(elapsed)}s. Probing on schedule keyed off the typical 5h reset. "
            "State saved (Ctrl+C to exit, --resume to continue later).",
            flush=True,
        )
        self._pause_pipeline_active_timer()
        try:
            while True:
                if marks:
                    next_mark = marks.pop(0)
                else:
                    next_mark = next_extra_mark
                    next_extra_mark += self._RATE_LIMIT_PROBE_INTERVAL_AFTER_SCHEDULE
                wait_until = observed_at + next_mark
                sleep_seconds = wait_until - time.time()
                if sleep_seconds > 0:
                    if sleep_seconds > remaining_budget:
                        sleep_seconds = remaining_budget
                    if self._sleep_with_shutdown(sleep_seconds):
                        self._total_backoff_seconds += sleep_seconds
                        return
                    remaining_budget -= sleep_seconds
                    self._total_backoff_seconds += sleep_seconds
                if remaining_budget <= 0:
                    return
                if self._shutdown_event.is_set():
                    return
                elapsed_minutes = (time.time() - observed_at) / 60
                print(
                    f"[juvenal] probing rate limit at +{elapsed_minutes:.0f}m...",
                    flush=True,
                )
                cleared = False
                try:
                    cleared = backend.probe_rate_limit(working_dir=str(self.working_dir), env=env)
                except Exception as exc:  # noqa: BLE001 - probe is best-effort
                    print(f"[juvenal] probe raised {type(exc).__name__}: {exc}; continuing schedule", flush=True)
                if cleared:
                    print(f"[juvenal] rate limit cleared at +{elapsed_minutes:.0f}m — resuming", flush=True)
                    self._consecutive_errors = 0
                    self._last_observed_rate_limit_at = None
                    return
        finally:
            self._resume_pipeline_active_timer()

    def _claude_probe_backend(self) -> Backend | None:
        """Return a Claude backend instance suitable for rate-limit probing, or None."""
        for backend_name in (self.config.worker_backend, self.config.captain_backend):
            if backend_name == "claude":
                backend = self._get_backend(backend_name)
                if hasattr(backend, "probe_rate_limit"):
                    return backend
        return None

    def _pause_pipeline_active_timer(self) -> None:
        if self._pipeline_state is None:
            return
        try:
            self._pipeline_state.pause_active(self.phase.id)
        except AttributeError:
            pass

    def _resume_pipeline_active_timer(self) -> None:
        if self._pipeline_state is None:
            return
        try:
            self._pipeline_state.resume_active(self.phase.id)
        except AttributeError:
            pass

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

    def _claim_mechanism_established(self, claim_id: str) -> bool:
        """True if a claim is verified, or was rejected only after a verifier passed it.

        A dependent target declared a dependency on a finding, not on a verdict. When a
        later verifier rejects on impact or scope, the mechanism an earlier verifier
        confirmed still exists and is still something to build on — so the dependent
        stays schedulable. A claim rejected by the first verifier established nothing.
        """
        claim = self.state.claims.get(claim_id)
        if claim is None:
            return False
        if claim.status == "verified":
            return True
        if claim.status != "rejected":
            return False
        return any(
            verification.claim_id == claim_id and verification.status == "passed"
            for verification in self.state.verifications.values()
        )

    def _dependencies_satisfied(self, target: TargetRecord) -> bool:
        def established_via_retries(claim_id: str, seen: set[str]) -> bool:
            if claim_id in seen:
                return False
            seen.add(claim_id)
            claim = self.state.claims.get(claim_id)
            if claim is None:
                return False
            if self._claim_mechanism_established(claim_id):
                return True
            return any(established_via_retries(rid, seen) for rid in claim.retry_claim_ids)

        return all(established_via_retries(dep_id, set()) for dep_id in target.depends_on_claim_ids)

    def _dep_claim_unverifiable(self, claim_id: str) -> bool:
        """Walk a dep claim's retry chain. Return True iff no claim in the
        chain is verified AND no path to verification remains: every claim
        in the chain is rejected with no retry budget, no live verification,
        and no descendant retry claim that's still alive.

        Used by `_sweep_dead_dep_targets` to short-circuit queued targets
        whose deps can never resolve, so they don't squat on the frontier
        forever waiting for a parent claim that will never verify.
        """
        pending_retry_keys = {(t, c) for t, c in self._pending_claim_retries}
        seen: set[str] = set()
        stack = [claim_id]
        while stack:
            cid = stack.pop()
            if cid in seen:
                continue
            seen.add(cid)
            claim = self.state.claims.get(cid)
            if claim is None:
                # Defensive: a missing dep is not a known dead-end. Don't
                # block targets on what might be a future claim.
                return False
            if self._claim_mechanism_established(cid):
                return False
            if claim.status in ("proposed", "verifying"):
                return False
            if claim.status == "rejected":
                # A claim is only retryable if its target isn't terminal —
                # `_rebuild_pending_claim_retries` filters out claims whose
                # target is in a terminal status, so retry_count alone is
                # not enough. Without this guard, dependents wedge waiting
                # on stranded mid-budget claims that can never re-dispatch
                # (target was blocked/exhausted/no_findings via some other
                # path while the claim still had budget).
                target = self.state.targets.get(claim.target_id)
                target_terminal = target is not None and target.status in _TERMINAL_TARGET_STATUSES
                if not target_terminal:
                    if claim.retry_count < self.config.max_worker_retries:
                        return False
                    if (claim.target_id, claim.claim_id) in pending_retry_keys:
                        return False
            for rid in claim.retry_claim_ids:
                stack.append(rid)
        return True

    def _sweep_dead_dep_targets(self) -> bool:
        """Mark queued targets blocked when at least one dep claim is
        terminally unverifiable (see `_dep_claim_unverifiable`).

        Without this sweep, captain-enqueued targets that pre-declared a
        dep on a future claim wedge in the frontier when the parent claim
        ultimately rejects with no successful retry — invisible to
        `_schedule_workers` (deps unsatisfied) and never resolved by any
        downstream event. The captain's frontier would grow without bound
        and the worker pool starves with no schedulable work.
        """
        progressed = False
        now = time.time()
        for target in self.state.targets.values():
            if target.status != "queued":
                continue
            if self._is_target_ignored(target):
                continue
            if not target.depends_on_claim_ids:
                continue
            dead = [dep_id for dep_id in target.depends_on_claim_ids if self._dep_claim_unverifiable(dep_id)]
            if not dead:
                continue
            target.status = "blocked"
            target.updated_at = now
            # Distinct from `target.blocked` because the remedy is the opposite
            # one. A blocked target needs a different approach and the captain
            # is told not to respawn it; this target's work was never attempted
            # and is recovered by re-enqueuing it with no dependency gate. Both
            # reach the captain as deltas, but only this one says "requeue me".
            self.state.append_event(
                "target.dependency_stranded",
                target_id=target.target_id,
                generation=target.active_generation,
                blocker=(
                    f"dependency claim(s) {', '.join(dead)} can never verify (rejected, retry budget "
                    f"exhausted), so this target was never dispatched. The work itself was not attempted "
                    f"and is not known to be infeasible."
                ),
                stranded_dependencies=list(dead),
                remedy="re-enqueue with depends_on_claim_ids: [] if the work is still worth doing",
            )
            progressed = True
        if progressed:
            self.state.save()
        return progressed

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

    def _maybe_start_analyst(self) -> None:
        """Submit the analyst future at startup if configured and not already done.

        Once the analyst reaches a terminal state (ready or failed) it is sticky —
        we do NOT auto-retry on subsequent resumes. The user can force a retry by
        editing the state file to set ``attack_surface.status`` back to ``pending``.
        """
        spec = self._analyst_spec
        if spec is None or not spec.enabled:
            return
        status = self.state.attack_surface.status
        if status in ("ready", "failed", "running"):
            return
        if self._analyst_future is not None:
            return
        now = time.time()
        self.state.attack_surface = AttackSurfaceState(
            status="running",
            started_at=now,
            backend=spec.backend,
            model=_resolve_model(spec.backend, "analyst", spec.model),
        )
        self.state.save()
        self._analyst_future = self._analyst_executor.submit(self._execute_analyst)

    def _wait_for_analyst(self) -> bool:
        """Block scheduling until the analyst's future drains.

        The user's contract: nothing else (workers, verifiers, captain, reporters)
        runs until the project brief is either ready or definitively failed. This
        prevents the historical failure mode where worker errors raced ahead and
        triggered phase termination before the analyst could finish.

        Returns False if the wait was interrupted by Ctrl-C / shutdown, True
        otherwise (including the no-analyst-future case).
        """
        if self._analyst_future is None:
            return True
        self._emit_analyst_message(
            "[juvenal] waiting for attack-surface analyst to finish before dispatching captain/workers/verifiers…"
        )
        while not self._analyst_future.done():
            if self._shutdown_event.is_set() or self.state.control.stop_requested:
                return False
            time.sleep(_IDLE_SLEEP_SECONDS)
        self._drain_analyst_future()
        return True

    def _finalize_analyst_on_shutdown(self) -> None:
        """Drain the analyst future once on shutdown so we record its result.

        Without this, an analyst that was running when the phase died would leave
        ``attack_surface.status == 'running'`` on disk. The next resume would be
        unable to distinguish 'crashed mid-flight' from 'still running' and would
        flip it to ``failed`` blindly. Draining here records ready/failed cleanly.
        """
        if self._analyst_future is None:
            return
        try:
            # Short bounded wait — kill_active() has already killed the analyst's
            # claude subprocess, so the future should resolve almost immediately.
            self._analyst_future.result(timeout=10.0)
        except Exception:
            pass
        try:
            self._drain_analyst_future()
        except Exception:
            pass

    def _execute_analyst(self) -> _AnalystExecutionResult:
        spec = self._analyst_spec
        if spec is None:
            return _AnalystExecutionResult(
                agent_result=AgentResult(
                    exit_code=1, output="", transcript="", duration=0.0, input_tokens=0, output_tokens=0
                ),
                brief="",
                error="analyst spec missing at execution time",
            )
        backend = self._get_backend(spec.backend)
        mission = self.phase.render_prompt(failure_context=self.failure_context, vars=self.workflow.vars)
        prompt_template = self._rendered_analyst_prompt or _DEFAULT_ANALYST_PROMPT
        prompt = prompt_template.replace("{working_dir}", str(self.working_dir)).replace("{mission}", mission)
        try:
            result = backend.run_agent(
                prompt,
                working_dir=str(self.working_dir),
                timeout=spec.max_duration_seconds,
                env=self._role_env("analyst"),
                model=_resolve_model(spec.backend, "analyst", spec.model),
            )
        except Exception as exc:
            return _AnalystExecutionResult(
                agent_result=AgentResult(
                    exit_code=1, output="", transcript="", duration=0.0, input_tokens=0, output_tokens=0
                ),
                brief="",
                error=f"analyst raised: {exc}",
            )
        if result.exit_code != 0:
            return _AnalystExecutionResult(
                agent_result=result,
                brief="",
                error=f"analyst exited with code {result.exit_code}: {result.output[-2000:]}",
            )
        brief = result.output.strip()
        if not brief:
            return _AnalystExecutionResult(agent_result=result, brief="", error="analyst returned empty output")
        return _AnalystExecutionResult(agent_result=result, brief=brief, error=None)

    def _drain_analyst_future(self) -> bool:
        if self._analyst_future is None or not self._analyst_future.done():
            return False
        future = self._analyst_future
        self._analyst_future = None
        try:
            outcome = future.result()
        except Exception as exc:
            self._record_analyst_failure(f"future crashed: {exc}")
            return True
        self._add_tokens(outcome.agent_result)
        if outcome.error is not None:
            self._record_analyst_failure(outcome.error, agent_result=outcome.agent_result)
            return True
        self._record_analyst_success(outcome)
        return True

    def _record_analyst_success(self, outcome: _AnalystExecutionResult) -> None:
        now = time.time()
        brief_path = self.working_dir / "output" / ".attack-surface-brief.md"
        brief_path.parent.mkdir(parents=True, exist_ok=True)
        brief_path.write_text(outcome.brief, encoding="utf-8")
        subagent_path = self._write_subagent_definition(outcome.brief)
        started = self.state.attack_surface.started_at or now
        self.state.attack_surface = AttackSurfaceState(
            status="ready",
            brief=outcome.brief,
            brief_path=str(brief_path),
            subagent_path=str(subagent_path) if subagent_path else None,
            error=None,
            started_at=started,
            completed_at=now,
            duration_seconds=max(0.0, now - started),
            input_tokens=outcome.agent_result.input_tokens,
            output_tokens=outcome.agent_result.output_tokens,
            session_id=outcome.agent_result.session_id,
            backend=self.state.attack_surface.backend,
            model=self.state.attack_surface.model,
        )
        self.state.save()
        message = (
            f"[juvenal] attack-surface analyst ready ({self.state.attack_surface.duration_seconds:.0f}s, "
            f"{outcome.agent_result.input_tokens} in / {outcome.agent_result.output_tokens} out tokens) "
            f"-> {brief_path}"
        )
        self._emit_analyst_message(message)
        if self._dashboard is not None:
            self._dashboard.render_event(kind="analyst.ready", text=str(brief_path))

    def _record_analyst_failure(self, error: str, *, agent_result: AgentResult | None = None) -> None:
        if self._note_quota_exhaustion(error) or self._note_quota_exhaustion(agent_result):
            # A `failed` analyst is sticky across resumes, so a quota refusal
            # must not spend it: the brief was never attempted. Park the state
            # back at `pending` and let the run stop — resume re-dispatches.
            self.state.attack_surface = AttackSurfaceState(
                status="pending",
                backend=self.state.attack_surface.backend,
                model=self.state.attack_surface.model,
            )
            self.state.save()
            return
        now = time.time()
        started = self.state.attack_surface.started_at or now
        input_tokens = agent_result.input_tokens if agent_result is not None else 0
        output_tokens = agent_result.output_tokens if agent_result is not None else 0
        session_id = agent_result.session_id if agent_result is not None else None
        self.state.attack_surface = AttackSurfaceState(
            status="failed",
            brief=None,
            brief_path=None,
            subagent_path=None,
            error=error,
            started_at=started,
            completed_at=now,
            duration_seconds=max(0.0, now - started),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            session_id=session_id,
            backend=self.state.attack_surface.backend,
            model=self.state.attack_surface.model,
        )
        self.state.save()
        message = f"[juvenal] [ANALYST ERROR] attack-surface analyst failed: {error}"
        self._emit_analyst_message(message)
        if self._dashboard is not None:
            self._dashboard.render_event(kind="analyst.failed", text=error)

    def _emit_analyst_message(self, message: str) -> None:
        # One print only. In chat / plain / parallel modes, `display.live_update`
        # falls through to its own `print(f"    {line}")` (display.py: when
        # there is no Rich Live attached) which produces a duplicated,
        # indented copy of every analyst-channel message — the cold-restart
        # warning is emitted twice as a result. The plain `print` here is
        # already the right output for every mode.
        try:
            print(message, flush=True)
        except Exception:
            pass

    def _load_subagent_scope_for_verifier(self, backend: str | None = None) -> str | None:
        """Return the shared runtime attack-surface body wrapped in verifier mode.

        Strips the Claude Code agent frontmatter (the leading ``---\\n…\\n---``
        block) so what's left is the system-prompt body the subagent itself
        would receive. Then prepends a short verifier-mode header so the LLM
        knows to apply the body's knowledge to a single claim and return a
        VERIFICATION_JSON block per the framework verifier role (the body on
        its own is analyst-mode and would just answer questions). The body
        already embeds the project brief, so the caller skips the standard
        brief-block injection for this verifier.

        The Claude and Codex definitions are emitted from the same in-memory body;
        the Claude Markdown file is used here as the readable materialization. The
        only backend-dependent bit is which definition path the framing cites.

        Falls back to None when the subagent file is missing (analyst failed
        / hasn't run); the caller then uses the YAML scope.
        """
        agent_file = self.working_dir / ".claude" / "agents" / "attack-surface.md"
        if not agent_file.is_file():
            return None
        try:
            content = agent_file.read_text(encoding="utf-8")
        except OSError:
            return None
        if content.startswith("---\n"):
            end = content.find("\n---\n", 4)
            if end != -1:
                content = content[end + len("\n---\n") :]
        body = content.lstrip()
        if not body:
            return None
        def_path = (
            ".codex/agents/attack-surface.toml" if _backend_is_codex(backend) else ".claude/agents/attack-surface.md"
        )
        verifier_framing = (
            "## Specialized scope: trust-model gate\n\n"
            "**You are this run's `attack-surface` subagent — the same role and same project "
            f"knowledge as the file at `{def_path}`. The body of that "
            "subagent definition is reproduced verbatim below, including the project brief.**\n\n"
            "You are operating in **verifier mode** for the trust-model gate (verifier 2 of 4). "
            "The previous verifier (`attack-surface`) already classified the bug class and "
            "filtered hardening / hypothetical / primitive-chain / correctness issues. Your one "
            "judgment: does the attacker's required starting position SIT INSIDE the trust "
            "boundary the project documents (or that its system class conventionally assumes)?\n\n"
            "- Memory-safety carve-out: a reachable OOB / UAF / double-free / type confusion / "
            "integer-overflow-into-corruption on production code passes this gate "
            "automatically, regardless of attacker role.\n"
            "- Pure-DoS carve-out: per the default scoping rule below, a reachable bug whose "
            "only effect is a crash / hang / heap-exhaustion / stuck-state-machine with no "
            "corruption primitive is a real bug but **out of scope for bug bounty** — REJECT "
            "with `precondition-not-met`.\n"
            "- Otherwise: compare attacker position to boundary using the brief. INSIDE the "
            "boundary → REJECT `within-trust-model`. ABOVE the boundary → REJECT "
            "`presupposes-attacker-position`. BELOW the boundary → check spec-authority "
            "citation per the body's rules and pass if it holds.\n\n"
            "Your `reason` MUST quote the boundary line from the brief, state the attacker's "
            "position, and (for non-memory-safety passes) include a verbatim spec / RFC / "
            "project-doc citation. Asymmetry-with-sibling-code is NOT a citation. Return your "
            "verdict as a `VERIFICATION_JSON_BEGIN` / `VERIFICATION_JSON_END` block per the "
            "framework verifier role at the top of this prompt — do NOT answer in analyst-mode "
            "free-form prose.\n\n"
            "What's out of scope for THIS verifier:\n"
            "- Bug class / hardening / design-critique smell test (`attack-surface` handled it).\n"
            "- PoC reproduction (`poc`).\n"
            "- Public novelty / already-known / in-run duplication (`novelty`).\n\n"
            "---\n\n"
            "## Subagent body (your trust-model knowledge — verbatim)\n\n"
        )
        return f"{verifier_framing}{body}"

    _ATTACK_SURFACE_DESCRIPTION = (
        "Project-specific trust-model and attack-surface analyst. Invoke this "
        "subagent before assuming anything about what is in/out of scope, what is "
        "attacker-reachable, or what the project considers by-design. The subagent has the "
        "full project brief in context and can do additional code/web research per question."
    )

    def _attack_surface_body(self, brief: str) -> str:
        """The runtime attack-surface subagent body (shared by the Claude `.md` and Codex `.toml`)."""
        return (
            "You are the project's attack-surface analyst for this Juvenal bug-finding run. "
            "The project brief below is your source of truth — quote and cite it when answering. "
            "If the question goes beyond the brief, use the repository and web-research tools "
            f"available in your backend over `{self.working_dir}` before "
            "answering. Keep answers focused and cite specific files, docs, or URLs whenever "
            "possible. Reply with a concise direct answer; do not dump the entire brief back.\n\n"
            "If the calling agent's question does not have a confident answer, say so plainly — "
            "do not fabricate scope rules or trust-model claims that are not in the brief.\n\n"
            "## Default scoping rule: pure-DoS is a bug, not a bug-bounty vulnerability\n\n"
            "A pure-DoS / crash-only finding (`the parser segfaults on malformed input`, "
            "`the daemon hangs on a crafted packet`) IS a real bug — it just is NOT a "
            "vulnerability for bug-bounty / VRP purposes. Unless the project brief below "
            "explicitly says otherwise, treat pure-DoS findings as **out of bug-bounty scope** "
            "and tell the calling agent to reject the claim on scope grounds. The bar for an "
            "in-scope memory-safety report is a corruption primitive that goes beyond a crash: "
            "UAF, OOB write into attacker-controlled state, type confusion exploitable for "
            "code execution, heap overflow, double-free, etc. Crashes that imply such "
            "corruption (e.g., an ASan write-of-N report, a UAF backtrace) remain in scope "
            "unless the brief excludes them. When a calling agent asks whether a crash-only "
            "finding is in scope, default to NO and direct them to the brief's bounty-scope "
            "section to confirm.\n\n"
            "<<PROJECT_BRIEF_BEGIN>>\n"
            f"{brief}\n"
            "<<PROJECT_BRIEF_END>>\n"
        )

    def _write_codex_attack_surface_subagent(self, brief: str) -> Path | None:
        """Emit the runtime attack-surface subagent as `.codex/agents/attack-surface.toml`.

        Mirrors the Claude `.md` write from the same body so a codex-backed role can spawn
        the analyst natively. Best-effort — a failure here never blocks the Claude write.
        """
        try:
            agents_dir = self.working_dir / ".codex" / "agents"
            agents_dir.mkdir(parents=True, exist_ok=True)
            toml_file = agents_dir / "attack-surface.toml"
            toml_file.write_text(
                _codex_agent_toml("attack-surface", self._ATTACK_SURFACE_DESCRIPTION, self._attack_surface_body(brief)),
                encoding="utf-8",
            )
            return toml_file
        except Exception as exc:
            self._emit_analyst_message(f"[juvenal] failed to write codex attack-surface subagent: {exc}")
            return None

    def _uses_codex_backend(self) -> bool:
        """True when any orchestrated role (captain/worker/verifier/reporter/analyst/exploit-sim) runs on Codex."""
        names: list[str | None] = [
            self.config.captain_backend,
            self.config.worker_backend,
            self.config.verifier_backend,
        ]
        names.extend(spec.backend for spec in self._verifier_chain)
        if self._reporter_spec is not None:
            names.append(self._reporter_spec.backend)
        if self._analyst_spec is not None:
            names.append(self._analyst_spec.backend)
        if self._exploit_sim_spec is not None:
            es = self._exploit_sim_spec
            names.extend([es.env_builder.backend, es.simulator.backend, es.attacker.backend, es.judge.backend])
        return any(_backend_is_codex(n) for n in names)

    def _write_subagent_definition(self, brief: str) -> Path | None:
        try:
            agents_dir = self.working_dir / ".claude" / "agents"
            agents_dir.mkdir(parents=True, exist_ok=True)
            agent_file = agents_dir / "attack-surface.md"
            content = (
                "---\n"
                "name: attack-surface\n"
                f"description: {self._ATTACK_SURFACE_DESCRIPTION}\n"
                "tools: Read, Grep, Glob, WebFetch, WebSearch\n"
                "---\n\n" + self._attack_surface_body(brief)
            )
            agent_file.write_text(content, encoding="utf-8")
            # Dual-emit the Codex definition when a codex-backed role may spawn it.
            if self._uses_codex_backend():
                self._write_codex_attack_surface_subagent(brief)
            return agent_file
        except Exception as exc:
            self._emit_analyst_message(f"[juvenal] failed to write attack-surface subagent: {exc}")
            return None

    def _subagent_invoke_phrase(self, backend: str | None) -> str:
        """Backend-specific instruction for reaching the `attack-surface` subagent.

        Claude spawns subagents via its native Agent tool; Codex spawns them natively
        from the `.codex/agents/attack-surface.toml` definition this runner emits. Only
        the invocation verb differs — the role and its project knowledge are identical.
        """
        if _backend_is_codex(backend):
            return (
                "**spawn the `attack-surface` subagent** (defined in "
                "`.codex/agents/attack-surface.toml`), wait for its result, then use it"
            )
        return "**invoke the `attack-surface` subagent via the Agent tool**"

    def _worker_fanout_block(self, backend: str | None) -> str:
        """Backend-aware 'fan out into your own subagents, then synthesize' guidance.

        Injected into the worker system prompt when `worker_dynamic_workflow` is on
        (default). The worker's loop position and one-WORKER_JSON output contract do
        NOT change — only its INTERNAL investigation method does: it spawns its own
        subagents to explore competing hypotheses/attack-angles in parallel, then
        folds their findings into the single structured result it already returns.
        Claude fans out via the native Agent tool; Codex spawns natively from its
        `.codex/agents/*.toml` definitions and degrades to a strong single pass when
        that mechanism is unavailable rather than faking parallelism.
        """
        header = (
            "## Investigate as a mini-captain: fan out, then synthesize\n\n"
            "You have ONE assigned target, but that target usually admits several "
            "competing hypotheses (different sink candidates, attack angles, guard-bypass "
            "theories, or PoC strategies). Instead of exploring them one at a time in your "
            "own context, delegate them to subagents that run in parallel, then SYNTHESIZE "
            "their evidence into your result. This does not change your job: you still "
            "return **exactly one WORKER_JSON block** with the same outcome "
            "(`claims` / `no_findings` / `blocked`) and the same claim shape. The fan-out is "
            "purely your internal method — the runner still treats you as a single worker on "
            "a single target.\n\n"
        )
        if _backend_is_codex(backend):
            mechanism = (
                "### How to fan out (Codex)\n\n"
                "If your Codex backend has native subagent spawning (multi-agent) enabled, "
                "spawn 2-4 parallel subagents — one per distinct hypothesis or attack angle — "
                "wait for all of them, then reconcile their findings. Reuse the "
                "`.codex/agents/*.toml` roles this run emits (e.g. `attack-surface`) where "
                "they fit. Keep the fan-out shallow (one layer): subagents investigate and "
                "report evidence; YOU alone decide the final claims and emit the single "
                "WORKER_JSON.\n\n"
                "If native spawning is unavailable in this environment, do NOT fake it and do "
                "NOT emit multiple WORKER_JSON blocks. Instead run a strong single pass: "
                "enumerate the competing hypotheses explicitly, work each to a conclusion "
                "yourself, and note in your `summary` that you investigated sequentially "
                "because subagent spawning was unavailable.\n\n"
            )
        else:
            mechanism = (
                "### How to fan out (Claude)\n\n"
                "Use the **Agent tool** to spawn 2-4 subagents in parallel, one per distinct "
                "hypothesis or attack angle. Each subagent runs in its own fresh context, so "
                "put every file path, error string, and code excerpt it needs directly in the "
                "Agent prompt, and ask it to return a compact evidence summary (what it "
                "confirmed/refuted, the exact sink/guard, any sanitizer output). Where a "
                "named role fits — e.g. the project-scoped `attack-surface` subagent, or the "
                "`.claude/agents/*` roles present in this repo — invoke it by name. Keep the "
                "fan-out shallow: subagents gather evidence; YOU alone reconcile it and emit "
                "the single WORKER_JSON. Spawn only as many as the target warrants — each "
                "subagent costs tokens and wall-clock.\n\n"
            )
        guardrail = (
            "### Fan-out guardrails (unchanged)\n\n"
            "- Keep all PoC artifacts in your assigned `scratch_dir`; the `output/` tree "
            "stays off-limits to you and every subagent you spawn (the runner denies writes "
            "there).\n"
            "- Stay inside your assigned target's scope — do not let a subagent sprawl into "
            "adjacent surfaces the captain owns.\n"
            "- One layer of fan-out is enough; do not build deep recursive agent trees.\n"
            "- The final synthesis is yours: emit exactly one WORKER_JSON, never one per "
            "subagent.\n"
        )
        return header + mechanism + guardrail

    @staticmethod
    def _current_project_brief(state: AttackSurfaceState) -> str:
        """Return the brief as it stands on disk, falling back to the state snapshot.

        Operators amend the brief file in place to correct scope that has moved since
        the analyst ran. Serving the snapshot instead makes those edits silently inert
        and keeps injecting superseded scope into every role for the life of the run.
        """
        if state.brief_path:
            try:
                on_disk = Path(state.brief_path).read_text(encoding="utf-8")
            except OSError:
                return state.brief or ""
            if on_disk.strip():
                return on_disk
        return state.brief or ""

    def _project_brief_block(self, backend: str | None = None) -> str:
        """Cacheable prefix injected into every captain/worker/verifier/reporter system prompt.

        ``backend`` tailors the subagent-invocation wording (Claude Agent tool vs. Codex
        native spawn); ``None`` keeps the Claude phrasing (default backend).
        """
        spec = self._analyst_spec
        if spec is None or not spec.enabled:
            return ""
        state = self.state.attack_surface
        invoke = self._subagent_invoke_phrase(backend)
        guidance = (
            "## Attack-surface subagent (always available)\n\n"
            "Before assuming anything about the project's trust model, attack surface, what is "
            "attacker-reachable, what is in/out of scope, or what the project considers by-design, "
            f"{invoke}. It is fast, has the full "
            "project brief in context, and can do additional code/web research per question. "
            "Treat its answers as authoritative for project-specific trust assumptions.\n"
        )
        if state.status == "ready" and state.brief:
            return (
                "## Project brief (attack-surface analyst output)\n\n"
                "The attack-surface analyst has produced the brief below. Treat it as the source "
                "of truth for the project's trust model and attack surface.\n\n"
                f"{self._current_project_brief(state)}\n\n"
                f"{guidance}"
            )
        if state.status == "running" or state.status == "pending":
            return (
                "## Project brief (attack-surface analyst)\n\n"
                "PROJECT_BRIEF: not ready yet — the attack-surface analyst is still running. "
                "Proceed with the information you have and consult the `attack-surface` subagent "
                "if it has finished by the time you reach a project-specific "
                "trust-model question. The subagent will respond `BRIEF_NOT_READY` until the "
                "analyst completes.\n"
            )
        if state.status == "failed":
            return (
                "## Project brief (attack-surface analyst)\n\n"
                f"PROJECT_BRIEF: unavailable (analyst failed: {state.error or 'unknown error'}). "
                "Proceed without the brief; the `attack-surface` subagent may still answer if "
                "it can use Read / Grep / WebFetch on the fly.\n"
            )
        return ""

    def _session_id_recorder(self, assign: Callable[[str], None]) -> Callable[[str], None]:
        """Persist a backend-issued session id the moment the backend reports it.

        Codex mints its thread id server-side, so before this the id existed
        only in the backend's return value — a run killed mid-turn left the
        record holding a juvenal-generated UUID that Codex had never issued, and
        every resume against it failed with "no rollout found for thread id".
        Writing through on the open event is what makes an interrupted Codex
        turn resumable the way a Claude one already was.
        """

        def _record(session_id: str) -> None:
            if not session_id:
                return
            assign(session_id)
            self.state.save()

        return _record

    def _note_dead_session(self, session_id: str | None, error: str | None) -> bool:
        """Record a session the backend refused to resume.

        Returns True only the first time a given session is found dead, which
        is what makes it safe for callers to skip charging their retry budget:
        the resume never reached the agent, and each id can produce at most one
        such refund before it is filtered out of every future resume decision.
        """
        if not session_id or not error:
            return False
        lowered = error.lower()
        if not any(signature in lowered for signature in _DEAD_SESSION_ERROR_SIGNATURES):
            return False
        if session_id in self._dead_sessions:
            return False
        self._dead_sessions.add(session_id)
        self._emit_analyst_message(
            f"[juvenal] session {session_id[:8]} no longer exists on the backend; cold-restarting instead of resuming"
        )
        return True

    def _session_is_stale(self, session_id: str) -> bool:
        """True if a Claude session has not been SUCCESSFULLY used inside the
        threshold window and is likely unrecoverable.

        We use last-successful-use time, not creation time. A long-running
        analysis re-uses the same captain/worker/verifier sessions across
        multi-day runs — keying off creation falsely flags healthy sessions
        once the run is older than the threshold (the user's symptom on
        attempt 18 of a multi-day phase).

        "Successful use" means the agent returned a parsed structured
        response on this session_id:
        - Worker attempts: ``status == "completed"`` (claims / no_findings
          / blocked all parse cleanly).
        - Verifications: ``disposition is not None`` (a clean PASS or
          REJECT — both prove the verifier session was alive enough to
          emit VERIFICATION_JSON).

        Filtering on success closes the original failure-loop concern:
        a session that's actually expired produces a stream of failed
        attempts whose ``completed_at`` would otherwise refresh the signal
        ("used recently" → don't cold-restart → fail again → "used
        recently" → ...). Failed attempts contribute nothing here, so the
        signal monotonically AGES until either a real success arrives
        (session is alive after all) or the threshold trips and we
        cold-restart.

        Falls back to creation time when there is no successful record
        yet — a brand-new session that's already failing is judged on
        age alone (matches the prior behavior on first use).
        """
        now = time.time()
        successful_use_times: list[float] = []
        for attempt in self.state.worker_attempts.values():
            if attempt.session_id != session_id:
                continue
            if attempt.status != "completed":
                continue
            if attempt.completed_at is not None:
                successful_use_times.append(attempt.completed_at)
        for verification in self.state.verifications.values():
            if verification.session_id != session_id:
                continue
            if verification.disposition is None:
                continue
            if verification.completed_at is not None:
                successful_use_times.append(verification.completed_at)

        if successful_use_times:
            return (now - max(successful_use_times)) > _SESSION_STALENESS_THRESHOLD_SECONDS

        # No successful use yet — fall back to creation time so a brand-new
        # session that's failing on the first try doesn't loop forever.
        creation_times: list[float] = []
        for attempt in self.state.worker_attempts.values():
            if attempt.session_id == session_id and not attempt.parent_session_id and attempt.started_at is not None:
                creation_times.append(attempt.started_at)
        for verification in self.state.verifications.values():
            if (
                verification.session_id == session_id
                and not verification.parent_session_id
                and verification.started_at is not None
            ):
                creation_times.append(verification.started_at)
        if not creation_times:
            return False  # No record at all — let the resume try.
        return (now - min(creation_times)) > _SESSION_STALENESS_THRESHOLD_SECONDS

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
        self.total_cached_input_tokens += result.cached_input_tokens

    def _role_env(self, role: str, *, verifier_name: str = "") -> dict[str, str] | None:
        env = dict(self.phase.env)
        env["JUVENAL_ANALYSIS_ROLE"] = role
        if role == "verifier" and verifier_name:
            env["JUVENAL_ANALYSIS_VERIFIER_NAME"] = verifier_name
        return env

    def _hooks_for_role(self, role: str, *, scratch_dir: Path | None = None) -> dict[str, Any] | None:
        """Per-role `--settings` fragment enforcing write guardrails (`//abs/**` deny globs)."""
        output_dir = self.working_dir / "output"
        if role in ("worker", "verifier"):
            # output/ is the reporter's tree. Verifier source writes stay open —
            # the poc verifier builds/runs harnesses in-tree, so a broader deny
            # would also block the sanitizer build it needs to reproduce a claim.
            deny = [f"Write(//{output_dir}/**)", f"Edit(//{output_dir}/**)"]
            return {"permissions": {"deny": deny}}
        if role == "reporter" and scratch_dir is not None:
            deny = [f"Write(//{scratch_dir}/**)", f"Edit(//{scratch_dir}/**)"]
            return {"permissions": {"deny": deny}}
        return None
