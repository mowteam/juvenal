---
name: juvenal
description: Create, validate, run, and troubleshoot verified AI-agent workflows with Juvenal. Use when a user asks about Juvenal YAML or directory workflows, implement/check/bounce phases, dynamic captain-worker-verifier analysis, Claude or Codex backends, workflow variables, resume/rewind, or Juvenal CLI commands. Do not trigger for generic agent design that does not use Juvenal.
---

# Juvenal — Verified AI Agent Workflows

You are helping the user create and manage Juvenal workflows. Juvenal orchestrates AI coding agents through alternating implementation and verification phases, preventing agents from cheating on success criteria.

For local use across repositories, `juvenal install-skills` installs this
canonical skill at both user discovery locations: `~/.claude/skills/juvenal`
for Claude Code and `~/.agents/skills/juvenal` for Codex. The command is
idempotent and refuses to replace an unrelated existing skill.

## What is Juvenal?

Juvenal is a framework where a deterministic Python runtime orchestrates AI coding agents (Claude or Codex) through verified phases. Each phase has:
1. An **implementation prompt** — tells the agent what to build
2. One or more **checks** — agentic verifiers that can review code and run commands when instructed

The key insight: the implementing agent and the checking agent are separate, so the implementer can't cheat by weakening tests.

## Workflow Formats

### 1. YAML (most expressive)

```yaml
name: "my-workflow"
backend: claude  # or "codex"
working_dir: "."
max_bounces: 999  # global bounce limit
backoff: 2.0      # exponential backoff between bounces (seconds)
max_backoff: 60.0 # cap on backoff delay
notify:
  - https://example.com/webhook  # webhook on completion/failure
vars:                             # template variable defaults
  ENV: staging
  TEST_DIR: tests/

include:
  - shared-phases.yaml  # merge phases from other workflows

phases:
  - id: setup
    backend: codex  # optional implement/check phase override
    model: gpt-5.6-sol
    prompt: "Set up the {{PROJECT}} project scaffolding in {{ENV}}."
    timeout: 300  # seconds
    env:
      NODE_ENV: development
    checks:
      - prompt: |
          Run `pytest tests/ -x` from the working directory and verify the result.
          Do not modify code while checking.
          Emit `VERDICT: FAIL: <reason>` on failure, otherwise emit `VERDICT: PASS`.
      - tester                    # built-in role shorthand
      - role: senior-engineer     # role as dict
      - prompt: "Review the code for security issues."  # inline prompt

  - id: implement
    prompt_file: phases/implement/prompt.md
    bounce_target: setup  # on failure, bounce back to setup
    checks:
      - prompt: "Run `make test` and emit `VERDICT: PASS` only if it succeeds."
      - role: senior-engineer

parallel_groups:
  # Lanes: concurrent mini-pipelines with per-lane bounce loops
  - lanes:
      - [feature-a, check-a]   # lane 1
      - [feature-b, check-b]   # lane 2

  # Legacy flat: run implement phases concurrently (no per-phase checking)
  - phases: [independent-x, independent-y]
```

### 2. Directory convention

```
my-workflow/
  phases/
    01-setup/
      prompt.md            # implement phase
      check.md             # check phase (auto-bounces to 01-setup)
    02-parallel/           # "parallel" in name → parallel lane group
      feature-a/           #   each subdir is a lane
        prompt.md          #     implement phase
        check.md           #     check phase (auto-bounces to implement)
      feature-b/
        prompt.md
        check.md
    03-check-final/        # "check-" prefix → standalone check phase
      prompt.md
```

In any phase directory, extra `.md` files (besides `prompt.md`) become check phases with `bounce_target` set to the implement phase. If a checker needs to run a command, write that command into the markdown prompt. Directories with `check-` prefix or `-check-` in the name are standalone check phases (only `prompt.md` is used).

Lanes can also use subdirectories: `02-parallel/a/01-implement/prompt.md`, `02-parallel/a/02-check-review/prompt.md`.

### 3. Bare .md files

A single `.md` file becomes a single implement phase:

```bash
juvenal run task.md
```

## Phase Types

| Type | Description |
|------|-------------|
| `implement` | Agent executes a prompt to build/modify code (default) |
| `check` | Separate agent verifies work, emits `VERDICT: PASS` or `VERDICT: FAIL: reason` |
| `workflow` | Sub-workflow: dynamic (from prompt) or static (from file/dir) |
| `analysis` | Dynamic captain/worker/verifier discovery loop (see Analysis Phases below) |

### Workflow Phases

```yaml
# Dynamic: LLM plans the sub-workflow from the prompt
- id: dynamic-feature
  type: workflow
  prompt: "Build a REST API with authentication and tests."
  max_depth: 2  # recursion depth limit (default: 3)

# Static: execute an existing workflow YAML or directory
- id: auth-module
  type: workflow
  workflow_file: auth/workflow.yaml

- id: frontend
  type: workflow
  workflow_dir: frontend/
```

Static sub-workflows skip the LLM planning step. Paths resolve relative to the declaring YAML file. Parent workflow `vars` propagate to sub-workflows. `workflow_file` and `workflow_dir` are mutually exclusive with each other.

### Analysis Phases

An `analysis` phase runs a deterministic dynamic-analysis loop (used by `juvenal/workflows/bug-bounty.yaml`). A long-lived captain maintains a frontier of targets, fresh workers investigate bounded targets, fresh verifiers independently accept/reject each claim, an optional non-gating exploit-sim stage categorizes verified claims, and a reporter writes them up. Child state persists to `.juvenal-state-<phase-id>-analysis.json`.

```yaml
- id: analyze-repo
  type: analysis
  prompt: "Analyze the repository for concrete security defects."
  analysis:
    captain_backend: claude          # claude | claude-sdk | codex | codex-sdk
    worker_backend: codex
    verifier_backend: claude
    shared_agent_budget: true        # workers+verifiers share max_agents; verifiers preempt
    max_agents: 12
    worker_dynamic_workflow: true    # each worker fans out into its own subagents, then synthesizes one WORKER_JSON
    allow_repo_tools: true
    verifiers: [...]                 # ordered verifier chain (VerifierSpec)
    exploit_sim:                     # optional, non-gating
      enabled: true
    reporter: {...}                  # ReporterSpec
```

Run with `-i` / `--interactive` for the chat dashboard (talk to the captain mid-run). `analysis` phases may not appear inside `parallel_groups`.

## Inline Checks

Checks are defined inline on implement phases. Each entry can be:

- **Bare string** — built-in role shorthand: `tester`, `architect`, `pm`, `senior-tester`, `senior-engineer`, `security-engineer`, `technical-writer`, `professor`, `grant-reviewer`
- **`role: NAME`** — agent checker with built-in role
- **`role: NAME` + `prompt: TEXT`** — built-in role plus extra checker instructions
- **`prompt: TEXT`** — agent checker with inline prompt
- **`prompt_file: PATH`** — agent checker with prompt from file

Checks can also carry `backend`, `model`, `timeout`, and `env`. Top-level
`implement` and `check` phases likewise accept optional `backend` and `model`
overrides. Model IDs pass through unchanged to the selected SDK or CLI backend.

## Bounce Targets

- **`bounce_target`** (singular, fixed): always bounces to this phase on failure
- **`bounce_targets`** (list, agent-guided): checker picks which phase to bounce to via `VERDICT: FAIL(target-id): reason`. Falls back to first in the list.

These are mutually exclusive.

```yaml
- id: review
  type: check
  bounce_targets:
    - design-experiments   # agent can bounce here
    - write-paper          # or here
```

## Parallel Groups

### Lanes (new)

Each lane is a mini-pipeline (implement + check) that runs its own internal bounce loop. All lanes run concurrently with a shared global bounce budget.

```yaml
parallel_groups:
  - lanes:
      - [feature-a, check-a]
      - [feature-b, check-b]
      - [feature-c, check-c]
```

### Legacy flat format

Run implement phases concurrently with no per-phase checking. A single failure aborts the group.

```yaml
parallel_groups:
  - phases: [a, b, c]
```

## Workflow Includes

Compose workflows from reusable pieces:

```yaml
# main.yaml
include:
  - shared/setup.yaml
  - shared/linting.yaml
phases:
  - id: feature
    prompt: "Build the feature."
```

Included phases, parallel groups, and other settings are merged. Circular includes are detected.

## Template Variables

Use `{{VAR}}` placeholders in prompts. Variables are resolved at runtime.

```yaml
vars:
  ENV: staging
  PROJECT: myapp

phases:
  - id: deploy
    prompt: "Deploy {{PROJECT}} to {{ENV}}."
    checks:
      - prompt: |
          Run `curl -f https://{{ENV}}.example.com/health` and verify the deployment health check.
          Emit `VERDICT: FAIL: <reason>` if it fails; otherwise emit `VERDICT: PASS`.
```

```bash
# Override defaults from CLI
juvenal run workflow.yaml -D ENV=prod -D PROJECT=api
```

- YAML `vars:` sets defaults; CLI `-D` overrides them
- Included workflows' vars merge (included = base, including = override)
- Unrecognized `{{VAR}}` passes through unchanged (safe for prompts containing literal `{{`)
- Multi-value: `-D T=a -D T=b` duplicates phases using `{{T}}` into parallel lanes (with checks grouped)
- `--dry-run` shows active variables

## CLI Commands

```bash
juvenal run <workflow> [--resume] [--rewind N] [--rewind-to PHASE_ID] [--phase X]
                       [--max-bounces N] [--backend claude|codex] [--dry-run]
                       [--backoff SECONDS] [--notify URL] [--working-dir DIR]
                       [--state-file PATH] [--checker SPEC] [--implementer ROLE]
                       [--clear-context-on-bounce] [-D VAR=VAL] [--serialize]
juvenal plan "goal" [-o output.yaml] [--backend claude|codex]
juvenal do "goal" [--backend claude|codex] [--max-bounces N] [-D VAR=VAL] [--serialize]
juvenal status [--state-file path]
juvenal init [directory] [--template name]
juvenal validate <workflow>
```

### Key flags

- **`--checker SPEC`**: Inject a checker on every implement phase. SPEC is `tester`, `tester:extra instructions`, or `prompt:TEXT`. Repeatable.
- **`--implementer ROLE`**: Prepend an implementer role prompt to every implement phase (e.g., `software-engineer`, `professor-writer`).
- **`--clear-context-on-bounce`**: Start a fresh agent session on bounce instead of resuming (default: resume session, preserving conversation context).
- **`-D VAR=VAL`**: Set a template variable. Use `{{VAR}}` in prompts. Repeatable. Overrides `vars:` defaults in YAML. Multiple values for the same key (`-D T=a -D T=b`) duplicate phases into parallel lanes.
- **`--serialize`**: Disable all parallelization (run parallel groups and lanes sequentially).
- **`--backoff SECONDS`**: Exponential backoff between bounces (base delay, doubles each bounce, capped at `--max-backoff` or workflow's `max_backoff`).
- **`--notify URL`**: Webhook URL for JSON notifications on completion/failure. Repeatable.
- **`--dry-run`**: Print execution plan, validation, and phase summary without running.

## Canned Workflows

### `research-paper` — Write a research paper

A 14-phase workflow for producing a research paper from an initial idea, with 6 agent roles: professor, postdoc, graduate researcher, research engineer, and two academic reviewers (one positive, one skeptical).

**Setup:** Create an `IDEA.md` in your working directory with the research idea, then run:

```bash
juvenal run $(python -c "from pathlib import Path; print(Path(__import__('juvenal').__file__).parent / 'workflows' / 'research-paper.yaml')") --backend claude
```

**Phases:**

| # | Phase | Agent | Type |
|---|-------|-------|------|
| 1 | `research-plan` | Professor | implement |
| 2 | `design-experiments` | Graduate Researcher | implement |
| 3 | `design-implementation` | Research Engineer | implement |
| 4 | `implement-project` | Research Engineer | implement |
| 5 | `ensure-tests` | Research Engineer | implement |
| 5b | `run-tests` | — | check |
| 6 | `implement-experiments` | Graduate Researcher | implement |
| 7 | `run-experiments` | Graduate Researcher | implement |
| 8 | `results-review` | Postdoc | check -> `design-experiments` |
| 9 | `design-paper` | Professor | implement |
| 10 | `write-paper` | Graduate Researcher | implement |
| 11 | `professor-review` | Professor | check -> `[design-experiments, write-paper]` |
| 12 | `reviewer-a-review` | Reviewer A (positive) | check -> `[design-experiments, write-paper]` |
| 13 | `reviewer-b-review` | Reviewer B (skeptical) | check -> `[design-experiments, write-paper]` |

**Artifacts produced:** `PLAN.md`, `DESIGN.md`, `IMPLEMENTATION.md`, `RESULTS.md`, `OUTLINE.md`, `PAPER.md`, `reviews/`

## Analysis Loop — Developer Map & Invariants

For work on the analysis engine (`bug-bounty.yaml` and friends), the source map is:

| Concern | Location |
|---------|----------|
| Captain/worker/verifier/exploit-sim/reporter orchestration | `juvenal/dynamic/runner.py` |
| Structured-output + directive parsing | `juvenal/dynamic/protocol.py` |
| Protocol/state dataclasses (incl. `ExploitSimRecord`, per-claim exploit fields) | `juvenal/dynamic/models.py` |
| Child-state persistence + resume normalization | `juvenal/dynamic/state.py` |
| `AnalysisConfig` / `ExploitSimSpec` parsing | `juvenal/workflow.py` |
| Per-claim status rendering | `juvenal/state.py` (`_format_claim_exploit_label`) |
| Shipped native subagent bodies | `juvenal/prompts/agents/*.md` (see `docs/AGENTS.md`) |
| Backend factory + SDK backends | `juvenal/backends.py`, `docs/backends/` |

Invariants that MUST hold when changing this loop:

- **The loop shape is fixed**: captain enqueues targets → exactly one worker subagent per target/claim → the verifier chain → non-gating exploit-sim → reporter. Deterministic control flow (retry/advance/terminate) is owned by the runner, never by an LLM.
- **Analyst is the initialization barrier**: when configured, the analyst runs once and must reach `ready` or definitively `failed` before the captain, workers, or verifiers dispatch. Its brief is injected into later roles and dual-emitted as Claude/Codex attack-surface subagent definitions.
- **Worker contract is fixed**: each worker keeps its loop position and emits exactly one `WORKER_JSON` (`claims` / `no_findings` / `blocked`). `worker_dynamic_workflow: true` only changes the worker's *internal* investigation method (it spawns its own backend-native subagents and synthesizes); it must never emit multiple `WORKER_JSON` blocks or move in the loop. If native spawning is unavailable, use a strong single pass rather than pretending fan-out occurred.
- **Exploit-sim is non-gating**: it categorizes verified claims (`exploit_confirmed`, `exploit_confirmed_nondefault`, `exploit_unconfirmed`, `sim_inconclusive`, `sim_error`) but never rejects one; infra failures yield `sim_error`/`sim_inconclusive` with the claim still verified.
- **Write guardrails**: workers/verifiers cannot write under `output/` (the reporter's tree); the reporter cannot write under a worker's `scratch_dir`. Enforced via per-role `--settings` deny globs (`_hooks_for_role`).
- **Backends stay portable**: bare `claude`/`codex` prefer installed SDK backends and fall back to subprocess `ClaudeBackend`/`CodexBackend`; `JUVENAL_BACKEND_NO_SDK=1` forces the fallback. Both paths must remain importable and functional. New `models.py` dataclass fields must have defaults.

## Your Task

When the user invokes the Juvenal skill (`/juvenal` in Claude Code or
`$juvenal`/`/skills` in Codex), help them by:

1. If they provide a goal, create a `workflow.yaml` file for that goal
2. If they ask to run something, invoke `juvenal run` via Bash
3. If they ask about canned workflows (e.g. "research paper"), explain the workflow and help them set it up
4. If they need help, explain the workflow format

Always create workflows that are specific, testable, and have meaningful checks.
