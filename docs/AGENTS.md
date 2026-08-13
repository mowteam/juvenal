# Documentation Guidance And Native Subagents

When editing files under `docs/`, keep Claude and Codex behavior described from
the same canonical role bodies. Do not document one backend as the default
semantic behavior when the runner supports both. Verify volatile CLI feature or
model claims before updating them.

Juvenal's static analysis role prompts are shipped as Claude Code subagent
definitions so they can be edited in one place, discovered natively by Claude
Code, and reused across workflows. Each has YAML frontmatter (`name`,
`description`, `tools`) followed by the role body.

## Where they live

- **Canonical source (loaded by the runner, packaged in the wheel):**
  `juvenal/prompts/agents/*.md`
- **Native Claude Code discovery (repo root):** `.claude/agents/*.md` — symlinks
  into the canonical files above, so there is a single editable copy.
- **Native Codex discovery (per target working dir):** `.codex/agents/*.toml` —
  materialized at run start from the same canonical `.md` bodies whenever any
  orchestrated role runs on a Codex backend. See "Codex parity" below.

## Codex parity

Codex has a real, first-class on-demand subagent mechanism (verified locally:
`codex features list` reports `multi_agent  stable  true` on codex-cli 0.128.0),
but its definitions use a different format than Claude Code: project-scoped
`.codex/agents/<name>.toml` files with `name` / `description` /
`developer_instructions` keys (these are the exact keys the installed Codex binary
parses). Codex spawns subagents **in-session, driven by the model** (`spawn_agent`
/ `wait_agent` collab primitives) — the runner does not, and cannot, call them
programmatically; delegation is triggered by the role prompt asking the model to
spawn a named subagent. Juvenal's deterministic loop is unchanged: the runner still
owns retry/advance/terminate and each role still runs as its own process.

To keep a single source of truth, the same shipped `.md` bodies are dual-emitted:

- `juvenal.dynamic.runner.write_codex_agent_definitions(working_dir)` writes every
  shipped role (`_SHIPPED_AGENT_NAMES`) into `<working_dir>/.codex/agents/*.toml`,
  serialized from the identical body the Claude path loads (the runner calls this at
  run start only when a Codex-backed role is present).
- The runtime attack-surface analyst subagent is dual-written: the Claude
  `.claude/agents/attack-surface.md` **and** the Codex
  `.codex/agents/attack-surface.toml` come from one `_attack_surface_body(brief)`.
- The brief block and trust-model verifier framing swap the Claude "invoke via the
  Agent tool" wording for Codex's "spawn the `attack-surface` subagent (defined in
  `.codex/agents/attack-surface.toml`)" wording based on the effective role backend.

**What is genuinely not possible today:** the runner cannot deterministically drive
Codex subagent fan-out/wait/collect the way Claude Code exposes a programmatic
`Task()` — Codex's native path is model-driven in-session. When strict, deterministic
out-of-process control is needed, the correct primitive is the out-of-process
`codex exec --json` / Codex SDK path (see `docs/backends/codex-sdk-exploration.md`),
not the in-session collab tools.

## Available subagents

| Subagent | Role in the bug-bounty verifier/exploit-sim chain |
|----------|---------------------------------------------------|
| `attack-surface-verifier` | Verifier 1 of 4 — semantic bug-class & attack-surface gate |
| `trust-model-verifier`    | Verifier 2 of 4 — trust-boundary & spec-authority gate |
| `poc-verifier`            | Verifier 3 of 4 — PoC reproduction gate |
| `novelty-verifier`        | Verifier 4 of 4 — novelty / already-known gate |
| `exploit-sim-env-builder` | Exploit-sim — builds a real runnable target instance |
| `exploit-sim-simulator`   | Exploit-sim — drives a fresh live instance |
| `exploit-sim-attacker`    | Exploit-sim — runs the verified PoC against the instance |
| `exploit-sim-judge`       | Exploit-sim — categorizes live reproduction (non-gating) |

The verifier subagents reference the `bug-report-reviewer` and
`design-critique-detector` skills when available.

## Analyst initialization ordering

When an analysis phase configures `analysis.analyst`, the runner launches it once
and blocks captain, worker, and verifier dispatch until the analyst reaches
`ready` or definitively `failed`. A successful brief is injected into every later
role and produces both runtime attack-surface definitions above. The exploit-sim
environment builder has its own executor and may initialize concurrently.

## Resolution order (fallback, additive & reversible)

The runner resolves each role's effective prompt in this order:

1. **Explicit config `prompt`** - a `prompt` set on the verifier / exploit-sim
   spec in the workflow YAML wins unless the verifier explicitly opts into
   `use_attack_surface_subagent: true` as described below.
2. **Per-project subagent** — `<working_dir>/.claude/agents/<name>.md` (lets a
   target repo specialize a role).
3. **Shipped subagent** — the packaged `juvenal/prompts/agents/<name>.md` body.
4. **Embedded default** — for exploit-sim roles, the `_DEFAULT_*_PROMPT`
   constants in `juvenal/dynamic/runner.py`; for verifiers, an empty scope
   (the framework verifier role still applies).

Because every layer falls back to the next, deleting the subagent files
silently reverts to the previously-embedded behavior — nothing else has to
change. Runtime placeholders (`{round}`, `{claim_packet}`, `{working_dir}`, …)
are preserved through loading and filled by the runner, and workflow `{{VAR}}`
templating still applies.

Note: the `trust-model` verifier additionally supports
`use_attack_surface_subagent: true`, which materializes the runtime-written
`.claude/agents/attack-surface.md` (the analyst's project brief) as its scope;
that path takes priority for that verifier and falls back to the resolution
order above when the analyst file is absent.

This option **replaces** the verifier's configured prompt; it does not append the
brief. Use it only when Juvenal's generic trust-model verifier semantics are
desired. A workflow with a specialized contest, policy, or impact verifier should
leave it false and rely on the normal project-brief injection, otherwise its
custom procedure is discarded.
