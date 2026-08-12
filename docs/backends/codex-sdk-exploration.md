# Codex Python SDK backend — integration status

Developer notes for `CodexSDKBackend` (`juvenal/backends.py`). The SDK thread
loop (`CodexSDKBackend._drive_codex_sdk`) is **implemented** against the official
`openai-codex` SDK (v0.144.4) and covered by unit tests that mock the SDK's
`Codex`/`Thread`/`TurnResult` surface
(`tests/test_backends.py::TestCodexSDKDriveLoop`). It is **opt-in** — the default
backend is still the subprocess `CodexBackend` (`npx @openai/codex@latest`).

## Package (verified from primary sources + local introspection)

- **Official SDK**: `openai-codex` (PyPI), import name `openai_codex`, v0.144.4.
  Install: `pip install openai-codex`. Provenance confirmed: its PyPI project
  URLs (Homepage/Repository) point at https://github.com/openai/codex, and it
  declares a runtime dependency on `openai-codex-cli-bin` (v0.144.4) which
  **bundles the pinned Codex binary** (`codex_cli_bin.bundled_codex_path()` ->
  `.../codex_cli_bin/bin/codex`). The SDK launches that binary as a persistent
  `codex app-server --listen stdio://` and speaks typed JSON-RPC over stdio
  (`openai_codex/client.py`: "Synchronous typed JSON-RPC client for
  `codex app-server` over stdio").
- **NOT used — third-party lookalike**: `openai-codex-sdk` (import
  `openai_codex_sdk`, PyPI maintainer 'tomasroda') is a different, unofficial
  package with **no** github.com/openai project URLs. It wraps
  `codex exec --experimental-json` (not an app-server) and vendors no binary.
  `_load_codex_sdk()` therefore imports **only** `openai_codex` and requires the
  `Codex` client class to be present, so the lookalike is never selected even
  though it happens to be installed alongside the official one.

Sources: `pip show openai-codex` (Requires: openai-codex-cli-bin; Project-URL ->
github.com/openai/codex); https://pypi.org/project/openai-codex/ ;
https://github.com/openai/codex/tree/main/sdk/python ;
https://github.com/openai/codex/blob/main/sdk/python/docs/api-reference.md ;
live introspection of openai_codex 0.144.4 in the juvenal pyenv.

## Why this backend exists (real subprocess pain points it targets)

- `npx @openai/codex@latest exec/resume` on **parallel** runs hits ENOTEMPTY /
  ETXTBSY races as concurrent npx invocations unpack the same cache.
- Transient `auth.json` 401s from the CLI path.
- npx install/unpack overhead on every subprocess spawn.

The SDK keeps one persistent `codex app-server` process and reuses Codex auth,
removing the per-call npx spawn that is the root of all three.

## How the drive loop maps to the SDK (verified against v0.144.4)

`CodexSDKBackend.run_agent` / `resume_agent` return an `AgentResult` identical in
shape to the subprocess `CodexBackend`.

- **Client**: `Codex(CodexConfig(cwd=working_dir, env=<os.environ + caller env>))`
  as a context the backend closes after each turn (stops the app-server).
- **Thread**: `thread_start(sandbox=Sandbox.full_access,
  approval_mode=ApprovalMode.auto_review, model=model)` on a fresh run, or
  `thread_resume(thread_id, sandbox=Sandbox.full_access, model=model)` on resume.
  `Sandbox.full_access` ("run without filesystem access restrictions") +
  `ApprovalMode.auto_review` is full autonomy with no approval prompts — parity
  with the CLI's `--dangerously-bypass-approvals-and-sandbox`.
- **Turn**: `thread.run(prompt)` (synchronous, blocking; a plain `str` is accepted
  and normalized to `TextInput`). Returns a `TurnResult`:
  `final_response` (assistant output), `status` (`TurnStatus`; `completed` -> exit
  0, otherwise non-zero), `usage.total.input_tokens` / `.output_tokens`, and
  `error.message`. `thread.id` is the Codex thread id used as `session_id`.
- **System prompt**: Codex has no separate slot, so `run_agent` folds
  `system_prompt` into the user message (`"{system_prompt}\n\n{prompt}"`);
  `hooks_config` is a no-op (no Claude-`--settings` equivalent).
- **Timeout**: `thread.run` has no timeout arg, so the turn runs on a helper
  thread and the caller joins with `timeout`; on expiry the client is closed
  (killing the app-server) and a non-zero timeout `AgentResult` is returned.
- **Overload/429**: an `openai_codex.ServerBusyError` /
  `RetryLimitExceededError` (detected via `openai_codex.is_retryable_error`) maps
  to `rate_limit_status = 429`, the Codex analogue of the CLI's 429 the runner
  uses for backoff cadence. Non-retryable errors (e.g. auth 401) map to a plain
  non-zero exit with `rate_limit_status = None`.

## Mechanism verified locally; final green turn needs Codex auth

A live smoke run in this environment launched the bundled app-server, started a
real thread (a genuine Codex thread id was assigned and surfaced as
`session_id`), drove the turn, and returned `exit_code=1` with the upstream
message when the account had no Codex auth configured — i.e. the whole
plumbing (app-server launch, thread lifecycle, `TurnResult` error mapping, thread
-id capture, 429-vs-plain-error classification) works; only the terminal
success turn needs valid Codex auth (`~/.codex/auth.json` from `codex login`, or
`OPENAI_API_KEY`).

## Selecting the SDK backend

- Per workflow: `backend: codex-sdk` in the YAML, or `--backend codex-sdk`.
- Fail-loud: `JUVENAL_BACKEND_CODEX_SDK=1` makes `create_backend("codex-sdk")`
  raise instead of falling back to the subprocess `CodexBackend` when the SDK is
  missing.

## E2E parity + one-line default flip (Codex auth required)

The unit tests mock the SDK; the true end-to-end check needs real auth:

1. Ensure Codex auth is present (`codex login`, or export `OPENAI_API_KEY`).
2. Add a `backend="codex-sdk"` variant of `test_trivial_workflow_codex` in
   `tests/test_e2e_codex.py` and confirm it produces the same `hello.txt` result
   as `backend="codex"`:
   ```
   pytest tests/test_e2e_codex.py -x -v
   ```
3. **Only after that passes**, flip a workflow's `backend: codex` to
   `backend: codex-sdk` (the one-line flip; `create_backend` dispatches by name,
   so there is no global default to change). Keep `codex` (subprocess) as the
   documented fallback.
