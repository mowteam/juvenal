# Claude Agent SDK backend — integration status

Developer notes for `ClaudeSDKBackend` (`juvenal/backends.py`). The SDK query
loop (`ClaudeSDKBackend._drive_sdk`) is **implemented** against the installed
`claude-agent-sdk` (v0.2.137) and covered by unit tests that mock the SDK's
`query()` async stream (`tests/test_backends_sdk.py::TestClaudeSDKBackendDriveLoop`).
Bare `backend: claude` selects this SDK backend by default when the package is
installed and falls back to subprocess `ClaudeBackend` otherwise.

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
`juvenal/dynamic/runner.py`), so the SDK backend accepts those strings.

**Why the `[1m]` suffix passes through untouched:** the SDK forwards
`ClaudeAgentOptions.model` verbatim to `claude --model <value>`
(`claude_agent_sdk/_internal/transport/subprocess_cli.py` `_build_command`),
and the CLI parses the `[1m]` suffix to select the 1M context window. Stripping
it and translating to `betas=["context-1m-2025-08-07"]` would be wrong on two
counts: (1) it double-encodes what the CLI already understands, and (2) that
beta header was **retired 2026-04-30** — Opus 4.6/4.7/4.8 and Sonnet 4.6 carry
1M context natively at standard pricing with no beta header
(https://code.claude.com/docs/en/agent-sdk/python, https://platform.claude.com/docs/en/build-with-claude/context-windows).
So the backend does not set `betas`; it passes `model=` through exactly as the
subprocess `ClaudeBackend` passes `--model`.

## How the drive loop maps to the SDK (verified against v0.2.137)

- **Options** (`_build_options`): `cwd=working_dir`; `model=` passthrough;
  `system_prompt=` on first run; `env=` layered over `os.environ` minus
  `CLAUDECODE`; `permission_mode="bypassPermissions"` (parity with
  `--dangerously-skip-permissions`); `settings=json.dumps(hooks_config)` (the SDK
  accepts an inline JSON string for `--settings`, same fragment as the CLI path);
  `session_id=` on first run (pre-allocated so it survives a crash), `resume=` on
  resume.
- **Drain** (`_drain_query`): async iterate `query(prompt=..., options=...)`
  under `asyncio.run` (the runner calls backends synchronously from threads, so a
  fresh event loop per call is created and torn down); `asyncio.wait_for` enforces
  `timeout`. `AssistantMessage.content` `TextBlock`s become assistant output;
  `ThinkingBlock`/`ToolUseBlock` are display-only. `ResultMessage` supplies
  `session_id`, `usage["input_tokens"/"output_tokens"]`, `is_error`
  (`exit_code`), and `result` (final output text).
- **429**: `ResultMessage.api_error_status == 429`,
  `AssistantMessage.error == "rate_limit"`, or a `RateLimitEvent` all map to
  `AgentResult.rate_limit_status = 429`, matching the CLI's `api_error_status`
  path the runner uses for backoff cadence.

## Why this backend exists (session-expiration cold-restart gap)

Subprocess `claude --resume <sid>` fails silently after a session expires: the
CLI starts a fresh session **without** the original `--append-system-prompt-file`
system prompt, which breaks structured output. The runner currently counts that
as a worker crash (`_session_is_stale` in `runner.py` is a reactive age check).
The SDK keeps session state in-process, so a resume either works or raises —
eliminating the silent-fresh-session failure mode.

## Selecting the SDK backend

- Recommended: use `backend: claude`; the factory selects this backend when the
  SDK is installed and otherwise uses the subprocess fallback.
- Explicit preference: use `backend: claude-sdk` in YAML. This still falls back
  when the package is absent unless fail-loud mode is enabled.
- Force subprocess execution with `JUVENAL_BACKEND_NO_SDK=1`.
- Fail-loud (for the human verifying the SDK path): `JUVENAL_BACKEND_SDK=1`
  makes `create_backend("claude-sdk")` raise instead of falling back to the
  subprocess `ClaudeBackend` when the SDK is missing.

## E2E parity (network + `ANTHROPIC_API_KEY` required)

The unit tests mock the SDK; the true end-to-end parity check needs a live key:

1. Run a `backend="claude-sdk"` variant of the trivial workflow in
   `tests/test_e2e_claude.py` and confirm it produces the same `hello.txt`
   result as `backend="claude"`:
   ```
   pytest tests/test_e2e_claude.py -x -v
   ```
   Requires `ANTHROPIC_API_KEY` and the `claude` CLI on `PATH` (the SDK shells
   out to it).
Bare `backend: claude` exercises the same SDK path whenever the package is
installed; use `JUVENAL_BACKEND_NO_SDK=1` for explicit CLI parity testing.
