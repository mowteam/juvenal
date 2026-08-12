# Claude Agent SDK backend — integration status

Developer notes for `ClaudeSDKBackend` (`juvenal/backends.py`). This is a
scaffolded backend: the class, factory case, model routing, and unit-test
contract are in place, but the SDK query loop (`ClaudeSDKBackend._drive_sdk`)
raises `NotImplementedError` until a human wires and E2E-verifies it.

## Package

- **Package name**: `claude-agent-sdk` (PyPI), import name `claude_agent_sdk`.
  Formerly published as `claude-code-sdk` (import name `claude_code_sdk`); the
  backend's `_load_claude_agent_sdk()` tries both import names.
- **Install**: `pip install claude-agent-sdk`
- The SDK drives the Claude Code CLI in-process (it shells out to the `claude`
  binary under the hood but manages the session and event stream from Python),
  so the `claude` CLI must still be on `PATH`.

> Availability note: verify presence against the pyenv juvenal runs under
> (`/home/mowteam/.pyenv/bin/python -c "import claude_agent_sdk"`). When the
> module is absent the backend must import cleanly and every unit test must
> still pass with the SDK missing — that guard is part of the contract.

## Contract the backend must satisfy (already enforced by the scaffold)

`ClaudeSDKBackend.run_agent` / `resume_agent` must return an `AgentResult`
identical in shape to `ClaudeBackend`:

- `exit_code`: 0 on success, non-zero on error.
- `output`: concatenated final assistant text.
- `transcript`: full event log.
- `session_id`: the caller-provided `session_id` on `run_agent` (pre-allocated
  UUID so it survives a crash before the first event), or the resumed id.
- `input_tokens` / `output_tokens`: summed across the turn.
- `rate_limit_status`: `429` when the SDK surfaces an upstream rate limit, else
  `None`.

Model strings pass through unchanged, **including the `[1m]` 1M-context suffix**
(e.g. `claude-opus-4-7[1m]`). The runner routes `claude-sdk` roles to the same
per-role defaults as `claude` (see `_DEFAULT_MODELS_BY_BACKEND_AND_ROLE` in
`juvenal/dynamic/runner.py`), so the SDK backend must accept those strings.

## Why this backend exists (session-expiration cold-restart gap)

Subprocess `claude --resume <sid>` fails silently after a session expires: the
CLI starts a fresh session **without** the original `--append-system-prompt-file`
system prompt, which breaks structured output. The runner currently counts that
as a worker crash (`_session_is_stale` in `runner.py` is a reactive age check).
The SDK keeps session state in-process, so a resume either works or raises —
eliminating the silent-fresh-session failure mode.

## Human follow-up (network + API key required — not runnable in unit tests)

1. Ensure `claude-agent-sdk` is installed into the juvenal pyenv.
2. Implement `ClaudeSDKBackend._drive_sdk` in `juvenal/backends.py`:
   - Build the SDK options object with `cwd=working_dir`, `model=` (pass the
     `[1m]` string through as-is — the CLI understands the suffix),
     `system_prompt=system_prompt` on `run_agent`, and `resume=session_id` /
     `fork_session=False` on `resume_agent`.
   - Apply `hooks_config` as CLI `settings` if the SDK exposes a settings hook
     (same `{"permissions": {"deny": [...]}}` fragment `ClaudeBackend` uses via
     `--settings`); otherwise document that guardrails ride via `_hooks_for_role`
     only on the subprocess path.
   - Drive the async query loop (the SDK is asyncio-based — wrap with
     `asyncio.run` since the runner calls backends synchronously from threads),
     enforce `timeout`, accumulate assistant text + tokens, map a 429 to
     `rate_limit_status`, and set `env` for the child.
   - Confirm the exact options/query API against the installed package version
     before relying on any specific field or kwarg name.
3. Verify E2E parity by running the existing suite against the SDK backend:
   ```
   pytest tests/test_e2e_claude.py -x -v
   ```
   and add a `backend="claude-sdk"` variant, confirming it produces the same
   `hello.txt` result as `backend="claude"`. Requires `ANTHROPIC_API_KEY` and the
   `claude` CLI.
4. Once verified, the SDK-guarded unit tests in `tests/test_backends_sdk.py`
   (skipped when the SDK is absent) exercise `run_agent` / `resume_agent` up to
   the not-yet-implemented drive loop.
