# Claude Agent SDK — availability and integration status

## Status (as of this branch)

The **Claude Agent SDK is NOT installed** in the pyenv used by juvenal
(`/home/mowteam/.pyenv/bin/python`, Python 3.12). Verified:

```
$ /home/mowteam/.pyenv/bin/python -c "import claude_agent_sdk"
ModuleNotFoundError: No module named 'claude_agent_sdk'
$ /home/mowteam/.pyenv/bin/python -c "import claude_code_sdk"
ModuleNotFoundError: No module named 'claude_code_sdk'
$ /home/mowteam/.pyenv/bin/python -c "import anthropic"
ModuleNotFoundError: No module named 'anthropic'
```

Because the SDK is absent, `ClaudeSDKBackend` is **scaffolded**: the class, the
`AgentResult` / `InteractiveResult` contract, the `claude-sdk` backend name, the
`create_backend("claude-sdk")` factory case, the `[1m]`-suffix-preserving model
routing, and the feature flag are all in place and unit-tested, but the actual
SDK query loop (`ClaudeSDKBackend._drive_sdk`) raises `NotImplementedError`. The
module imports cleanly and all existing tests pass with the SDK absent.

## Package

- **Package name**: `claude-agent-sdk` (PyPI). Import name `claude_agent_sdk`.
  Formerly published as `claude-code-sdk` (import name `claude_code_sdk`); the
  backend's `_load_claude_agent_sdk()` tries both import names.
- **Install**: `pip install claude-agent-sdk`
- The SDK drives the Claude Code CLI in-process (it shells out to the `claude`
  binary under the hood but manages the session and event stream from Python),
  so the `claude` CLI must still be on `PATH`.

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

## Human follow-up (network + API key required — not runnable here)

1. `pip install claude-agent-sdk` into `/home/mowteam/.pyenv`.
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
3. Verify E2E parity by running the existing suite against the SDK backend:
   ```
   /home/mowteam/.pyenv/bin/python -m pytest tests/test_e2e_claude.py -x -v
   ```
   and add a `backend="claude-sdk"` variant, confirming it produces the same
   `hello.txt` result as `backend="claude"`. Requires `ANTHROPIC_API_KEY` and the
   `claude` CLI.
4. Once verified, the SDK-guarded unit tests in `tests/test_backends_sdk.py`
   (skipped today) exercise `run_agent` / `resume_agent` end-to-end.
