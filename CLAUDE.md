# Juvenal

> *Quis custodiet ipsos custodes?* — Who guards the agents?

A framework for orchestrating AI coding agents (Claude, Codex) through verified implementation phases. The core idea: the implementing agent and the checking agent are separate processes, so the implementer can't cheat by weakening tests.

## Quick Reference

```bash
# Install
pip install -e ".[dev]"

# Lint and format
ruff check juvenal/ tests/
ruff format juvenal/ tests/

# Unit tests (excludes E2E and skill tests)
pytest tests/ -x --ignore=tests/test_e2e_claude.py --ignore=tests/test_e2e_codex.py --ignore=tests/test_skill.py

# E2E tests (require API keys and CLI tools installed)
pytest tests/test_e2e_claude.py -x -v
pytest tests/test_e2e_codex.py -x -v
pytest tests/test_skill.py -x -v
```

## Architecture

The system uses a **non-agentic, deterministic execution loop**. All control flow decisions (retry, bounce, advance) are made programmatically — no LLM decides flow control.

### Core Modules

| Module | Purpose |
|--------|---------|
| `engine.py` | Main orchestration loop (`Engine.run()`). Executes phases sequentially or in parallel groups (flat or lane-based), including per-phase backend/model routing for implement/check phases. `BounceCounter` for thread-safe global bounce tracking in lanes. Global bounce counter (`max_bounces`) limits total bounces across all phases. Supports `--resume`, `--rewind N`, and `--rewind-to PHASE_ID` for resuming/rewinding pipeline state. |
| `workflow.py` | Workflow loading and `Phase`/`Workflow`/`ParallelGroup` dataclasses. Supports YAML, directory convention (including `parallel` directories for lane groups), and bare `.md` formats. Implement/check phases may override the workflow `backend` and select a `model`. `apply_vars()` handles `{{VAR}}` template substitution. In directory convention, extra `.md` files in a phase dir become check phases; command execution belongs inside agentic checker prompts rather than `.sh` phase discovery. |
| `backends.py` | Backend factory `create_backend(name)` plus the abstract `Backend` base class. Bare `claude`/`codex` prefer the installed in-process SDK (`ClaudeSDKBackend` / `CodexSDKBackend`) and fall back to `ClaudeBackend` / `CodexBackend`; `JUVENAL_BACKEND_NO_SDK=1` forces subprocess execution. Explicit `claude-sdk` / `codex-sdk` selectors support fail-loud environment flags. Manages subprocess invocation, SDK event mapping, and the shared `hooks_config` settings fragment. See `docs/backends/`. |
| `dynamic/` | Dynamic analysis engine package. `runner.py` owns captain/worker/verifier/exploit-sim/reporter orchestration (batch + chat modes), backend-aware subagent wiring, per-role write guardrails, and Codex `.codex/agents/*.toml` dual-emit; `protocol.py` parses structured outputs and directives, `state.py` persists child analysis state, `models.py` defines protocol/state dataclasses (including `ExploitSimRecord` and per-claim exploit fields), `interaction.py` collects user input on a background thread, and `chat_display.py` is the line-scrolling chat dashboard for `--interactive` runs (Rich `Live` was removed because it cannot share the cursor cleanly with line-buffered stdin reads). |
| `state.py` | Atomic JSON state persistence (`PipelineState`). Thread-safe (RLock). Writes to `.tmp`, fsyncs, then atomic renames. Supports resume, rewind, and scoped invalidation (for lane bounces). |
| `checkers.py` | Verdict parsing (`VERDICT: PASS` / `VERDICT: FAIL: reason`). |
| `display.py` | Rich TUI with rolling 15-line buffer. Thread-safe (Lock). Falls back to plain text with `--plain` or parallel mode. `pause()`/`resume()` for interactive terminal passthrough. |
| `cli.py` | CLI entry point. Commands: `run`, `plan`, `do`, `status`, `init`, `validate`. Run flags: `--resume`, `--rewind N`, `--rewind-to PHASE_ID`, `--phase`, `--backoff`, `--notify`, `-D VAR=VAL`, `--serialize`. `run`/`validate` surface `type: analysis` semantics, and `run --interactive` opens the analysis chat dashboard in addition to existing planning/interactive-phase flows. `status` exits 0 if pipeline fully completed, 1 otherwise. |
| `notifications.py` | Webhook notification support (`build_notification_payload`, `send_webhook`). |

### Execution Flow

1. `cli.py` parses args, dispatches to command handler
2. `workflow.py` loads and validates the workflow definition
3. `engine.py` iterates phases: implement/check/workflow/analysis → advance or bounce
4. `backends.py` spawns agent subprocesses (or drives the SDK in-process), streams JSON events
5. `dynamic/` handles captain → worker → verifier chain → exploit-sim → reporter orchestration for `analysis` phases
6. `checkers.py` parses verdicts from checker output
7. `state.py` persists workflow progress after each phase for resumability, while `dynamic/state.py` persists child analysis state

### Phase Types

- **implement** — agent executes a prompt to build/modify code. Supports `interactive: true` for terminal passthrough (Claude only, enabled with `--interactive`)
- **check** — separate agent verifies work, emits `VERDICT: PASS` or `VERDICT: FAIL: reason`
- **workflow** — sub-workflow: dynamic (LLM plans from `prompt`) or static (`workflow_file` / `workflow_dir`). Recursion depth capped by `max_depth`. Parent vars propagate to sub-workflows.
- **analysis** — dynamic analyst/captain/worker/verifier analysis. Uses nested `analysis:` config (`AnalysisConfig`) and persists child state to `.juvenal-state-<phase-id>-analysis.json`. When configured, the one-shot analyst is an initialization barrier: it reaches `ready` or definitively `failed` before captain/worker/verifier dispatch. The deterministic loop is then: captain enqueues targets → exactly one worker subagent per target/claim → the verifier chain → a non-gating exploit-sim stage → the reporter. `--interactive` opens a line-scrolling chat dashboard (`juvenal/dynamic/chat_display.py`) that prints captain output, worker/verifier events, and acknowledged directives to stdout as they happen, while the user types directives (`/focus`, `/ignore`, `/target`, `/ask`, `/now`, `/show captain`, `/chat`, `/summary`, `/stop`, `/wrap`, free-form notes) without the cursor fighting a Live redraw. By default workers and verifiers share a single `max_agents` budget with verifiers preempting workers (`shared_agent_budget: true`); set `shared_agent_budget: false` to fall back to legacy independent `max_workers` / `max_verifiers` pools.

  Additional invariants:
  - **Worker as its own captain** — `worker_dynamic_workflow: true` (default) lets each worker fan out into its own backend subagents (Claude Agent tool / Codex native spawn) to explore hypotheses before synthesizing. The worker keeps its exact loop position and one-`WORKER_JSON` output contract (`claims` / `no_findings` / `blocked`); only its internal investigation method changes. Codex degrades to a strong single pass when native spawning is unavailable — it never fakes fan-out. Set `worker_dynamic_workflow: false` for the legacy single-pass worker.
  - **Native subagents for both vendors** — the shipped role bodies in `juvenal/prompts/agents/*.md` are discovered natively by Claude Code via repo-root `.claude/agents/*.md` symlinks, and dual-emitted into `.codex/agents/*.toml` (via `write_codex_agent_definitions`) from the same source when a Codex-backed role runs. The runner swaps the "Agent tool" wording for Codex native-spawn wording per the effective role backend. See `docs/AGENTS.md`.
  - **Write guardrails** — `_hooks_for_role` emits per-role `--settings` deny globs (`Write`/`Edit`) applied through `hooks_config`: workers and verifiers cannot write under `output/` (the reporter's tree), and the reporter cannot write under a worker's `scratch_dir`. PoC artifacts stay under `.juvenal/scratch/`.
  - **Exploit-sim stage** — an optional non-gating post-verification stage (`analysis.exploit_sim`, `ExploitSimSpec`; env-builder → simulator → attacker → judge roles). It stands up a real runnable target instance and categorizes each verified claim without ever rejecting it. Categories: `exploit_confirmed`, `exploit_confirmed_nondefault`, `exploit_unconfirmed`, `sim_inconclusive`, `sim_error`. Any infra failure yields `sim_error`/`sim_inconclusive` with the claim still verified. `juvenal status` surfaces the per-claim category (e.g. `exploit: confirmed`).

### Template Variables

Prompts and check `run` commands support `{{VAR}}` placeholders. Variables are set via:
- **YAML `vars:` block** — workflow-level defaults
- **CLI `-D VAR=VAL`** — overrides YAML defaults (repeatable)
- **Includes** — included workflow vars are base defaults; including workflow overrides

Unrecognized `{{VAR}}` placeholders pass through unchanged. Applied at render time via `apply_vars()` in `workflow.py`.

Multi-value `-D VAR=VAL1 -D VAR=VAL2` duplicates phases referencing `{{VAR}}` into parallel lanes (cartesian product for multiple vars). `expand_multi_vars()` in `workflow.py` handles this.

## Code Conventions

- **Python 3.10+** with `from __future__ import annotations` for forward references
- **Type hints** throughout, modern union syntax (`X | None`)
- **Dataclasses** for all data structures (`Phase`, `Workflow`, `PhaseState`, `AgentResult`, etc.)
- **Snake case** for functions/variables, **PascalCase** for classes, **kebab-case** for phase IDs
- **Private methods** prefixed with `_`
- **Import order**: stdlib → third-party → local (enforced by ruff `I` rule)
- **Line length**: 120 characters (ruff enforced)
- **Ruff rules**: E (errors), F (pyflakes), W (warnings), I (import sorting)

## Testing

- Tests live in `tests/` using pytest
- `conftest.py` provides shared fixtures: `MockBackend`, `tmp_workflow`, `sample_yaml`, `bare_md`, `simple_workflow`
- `MockBackend` simulates agent responses — use it instead of hitting real APIs
- E2E tests (`test_e2e_claude.py`, `test_e2e_codex.py`) require API keys and are skipped in PRs (run on push to main only)
- `test_skill.py` exercises live skill integration; `test_agent_assets.py` checks
  shared Claude/Codex instruction and skill discovery without API calls
- CI runs: lint → unit → e2e (on push only)

## Versioning

When bumping the version, update it in both `pyproject.toml` and `.claude-plugin/plugin.json`.

## Shared Agent Guidance And Skills

- `CLAUDE.md` is the canonical repository instruction file. Root `AGENTS.md` is a
  symlink to it, so Claude Code and Codex read identical guidance. Do not replace
  the symlink with a divergent copy.
- `skills/juvenal/SKILL.md` is the canonical cross-agent skill and follows the
  portable Agent Skills format. `.claude/skills/juvenal` and
  `.agents/skills/juvenal` symlink to that directory for native Claude and Codex
  repository discovery. `juvenal install-skills` creates the corresponding
  user-scoped links under `~/.claude/skills` and `~/.agents/skills`, making the
  skill available outside this checkout. Keep `plugin/skills/juvenal/SKILL.md`
  byte-identical for standalone plugin packaging.
- Skill frontmatter must keep portable `name` and `description` fields. Put
  backend-specific invocation wording in the body only when both equivalents are
  stated (`/juvenal` for Claude, `$juvenal` or `/skills` for Codex).

## Dependencies

**Runtime**: `jinja2>=3.1` + `pyyaml>=6.0` (workflow parsing), `rich>=13.0` (terminal UI)
**Dev**: `pytest>=8.0`, `ruff>=0.4`
**SDK extras** (preferred automatically when installed; subprocess fallback remains available): `claude-sdk` (`claude-agent-sdk>=0.2`), `codex-sdk` (`openai-codex>=0.144`), `sdk` (both). See `docs/backends/`.
**External CLIs** (not pip-managed): `claude` (Anthropic CLI), `npx @openai/codex@latest` (OpenAI Codex)

## Project Layout

```
juvenal/
├── __init__.py          # __version__ derived from installed package metadata
├── backends.py          # SDK-first backend factory + subprocess fallbacks
├── checkers.py          # Verdict parsing helpers
├── cli.py               # CLI argument parsing and dispatch
├── display.py           # Rich TUI rendering
├── dynamic/             # Dynamic analysis engine package
│   ├── chat_display.py  # Rich Live chat dashboard (--interactive mode)
│   ├── interaction.py   # Background-thread non-blocking stdin reader
│   ├── models.py        # Captain/worker/verifier protocol and state dataclasses
│   ├── protocol.py      # Structured output and directive parsing
│   ├── runner.py        # DynamicAnalysisRunner orchestration loop (batch + chat)
│   └── state.py         # Analysis child-state persistence and resume normalization
├── engine.py            # Core execution loop
├── notifications.py     # Webhook notifications
├── state.py             # Atomic state persistence
├── workflow.py          # Workflow/Phase models and loading (AnalysisConfig, ExploitSimSpec)
├── prompts/             # Built-in checker role prompts (.md)
│   └── agents/          # Shipped role subagents (4 verifiers + 4 exploit-sim roles); repo-root .claude/agents/ symlinks here, and the runner dual-emits Codex .codex/agents/*.toml from the same bodies for codex-backed roles. See docs/AGENTS.md
├── templates/           # Workflow scaffolding templates
└── workflows/           # Built-in workflows and examples (plan.yaml, analysis-example.yaml, bug-bounty.yaml, pwn2own-smart-home.yaml)
docs/
├── AGENTS.md            # Native subagent (Claude + Codex) resolution and dual-emit
├── analysis-workflow.md # Analysis-phase author guide
└── backends/            # SDK backend integration status
    ├── claude-sdk-integration.md   # ClaudeSDKBackend implementation and fallback behavior
    └── codex-sdk-exploration.md    # CodexSDKBackend implementation and auth requirements
tests/
├── conftest.py          # Shared fixtures (MockBackend, etc.)
├── test_cli.py          # CLI argument parsing tests
├── test_engine.py       # Engine execution tests (largest test file)
├── test_state.py        # State persistence tests
├── test_workflow.py     # Workflow loading tests
├── test_e2e_claude.py   # E2E with Claude (needs ANTHROPIC_API_KEY)
├── test_e2e_codex.py    # E2E with Codex (needs OPENAI_API_KEY)
├── test_round2.py       # Includes, cost tracking, backoff, notifications tests
├── test_skill.py        # Claude Code skill tests
└── test_validation.py   # Workflow validation tests
skills/juvenal/SKILL.md  # Canonical Claude/Codex skill definition
.claude/skills/juvenal   # Symlink for Claude repository skill discovery
.agents/skills/juvenal   # Symlink for Codex repository skill discovery
```
