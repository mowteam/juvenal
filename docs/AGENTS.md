# Native subagents (AGENTS migration)

Juvenal's static analysis role prompts are shipped as Claude Code subagent
definitions so they can be edited in one place, discovered natively by Claude
Code, and reused across workflows. Each has YAML frontmatter (`name`,
`description`, `tools`) followed by the role body.

## Where they live

- **Canonical source (loaded by the runner, packaged in the wheel):**
  `juvenal/prompts/agents/*.md`
- **Native Claude Code discovery (repo root):** `.claude/agents/*.md` — symlinks
  into the canonical files above, so there is a single editable copy.

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

## Resolution order (fallback, additive & reversible)

The runner resolves each role's effective prompt in this order:

1. **Explicit config `prompt`** — a `prompt` set on the verifier / exploit-sim
   spec in the workflow YAML always wins (user override).
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
