# OpenAI Codex Python SDK — availability and integration status

## Status (as of this branch)

An **embeddable Codex Python SDK does exist** (OpenAI shipped it in 2026), but it
is **NOT installed** in the pyenv used by juvenal (`/home/mowteam/.pyenv/bin/python`).
Verified:

```
$ /home/mowteam/.pyenv/bin/python -c "import openai_codex_sdk"
ModuleNotFoundError: No module named 'openai_codex_sdk'
$ /home/mowteam/.pyenv/bin/python -c "import openai_codex"
ModuleNotFoundError: No module named 'openai_codex'
```

Because the SDK is absent (and the exact Python API surface has a few undocumented
edges — see "Open questions" below), `CodexSDKBackend` is **scaffolded** the same
way `ClaudeSDKBackend` is: the class, the `AgentResult` / `InteractiveResult`
contract, the `codex-sdk` backend name, the `create_backend("codex-sdk")` factory
case with opt-in + subprocess fallback, the system-prompt folding, and the feature
flag are all in place and unit-tested, but the actual SDK thread loop
(`CodexSDKBackend._drive_codex_sdk`) raises `NotImplementedError`. The module
imports cleanly and all existing tests pass with the SDK absent.

## The SDK does exist — what it offers

- **Package**: `openai-codex-sdk` (PyPI). Import name `openai_codex_sdk`. Early
  builds were published under `openai-codex` (import `openai_codex`); the backend's
  `_load_codex_sdk()` tries both import names.
- **Install**: `pip install openai-codex-sdk` (requires Python 3.10+).
- **How it works**: the Python SDK drives a persistent local Codex **app-server over
  JSON-RPC** and **reuses existing Codex auth** — it does *not* `npx`-unpack per call.
  This is precisely what targets juvenal's Codex pain points (below).
- **Core API** (from the official docs):
  - `codex = Codex()` — construct the client.
  - `thread = codex.start_thread(...)` — create a new thread/session.
  - `thread = codex.resume_thread(thread_id)` — reconnect to a persisted session
    (threads persist in `~/.codex/sessions`, the same store the CLI's
    `codex exec resume <id>` uses).
  - `turn = await thread.run(prompt)` — run a turn; `turn.final_response` is the
    assistant text, `turn.items` the structured items.
  - `streamed = await thread.run_streamed(prompt)` then `async for event in
    streamed.events:` — structured events (`item.completed`, `turn.completed` with
    `event.usage`), matching the NDJSON event shapes `_process_codex_event` /
    `_extract_codex_tokens` already parse for the subprocess path.
  - Model + sandbox are thread options (docs show both `start_thread(model=...,
    sandbox=Sandbox.workspace_write)` and a `thread_start(...)` spelling across
    doc versions — confirm the exact name/kwargs against the installed package).
  - Auth: reuses existing Codex login; `Codex.login_with_auth_json()` /
    `login_with_device_code()` exist for explicit auth.

## Why this backend exists (Codex subprocess pain points it targets)

- `npx @openai/codex@latest exec/resume` on **parallel** runs hits ENOTEMPTY /
  ETXTBSY races as concurrent npx invocations unpack the same cache.
- Transient `auth.json` 401s from the CLI path.
- npx install/unpack overhead on every subprocess spawn.

The SDK's persistent JSON-RPC app-server (one process, reused auth) removes the
per-call npx spawn entirely, which is the root of all three.

## Contract the backend must satisfy (already enforced by the scaffold)

`CodexSDKBackend.run_agent` / `resume_agent` must return an `AgentResult` identical
in shape to `CodexBackend`:

- `exit_code`: 0 on success, non-zero on error.
- `output`: concatenated final assistant text (`turn.final_response`).
- `transcript`: full event log.
- `session_id`: the Codex **thread id** (Codex assigns its own; the externally
  chosen `session_id` on `run_agent` is ignored, matching subprocess `CodexBackend`).
  On `resume_agent` the caller's thread id is preserved if the SDK returns `None`.
- `input_tokens` / `output_tokens`: summed across the turn (from `event.usage`).

Codex has **no separate system-prompt slot**: `run_agent` folds `system_prompt`
into the user message (`"{system_prompt}\n\n{prompt}"`) before reaching the drive
seam, and `hooks_config` is a no-op (no Claude-`--settings` equivalent) — both
already handled in the scaffold.

## Open questions to resolve against the installed package

- Exact constructor/thread-start spelling: docs show both `start_thread(...)` and
  `thread_start(...)`, and both sync and `await codex.thread_start()` forms.
- How the thread id is read back off the thread object (for `session_id`).
- Exact `turn` / streamed-event field names for usage tokens and final text.
- Whether the SDK is asyncio-based (the run methods are `await`ed in docs), so the
  drive loop likely needs `asyncio.run(...)` since the runner calls backends
  synchronously from threads.

## Human follow-up (network + Codex auth required — not runnable here)

1. `pip install openai-codex-sdk` into `/home/mowteam/.pyenv`.
2. Implement `CodexSDKBackend._drive_codex_sdk` in `juvenal/backends.py`:
   - Construct `Codex()`; `start_thread(working_directory=working_dir,
     skip_git_repo_check=True, model=model, sandbox=...)` when `resume_thread_id`
     is `None`, else `resume_thread(resume_thread_id)`.
   - Drive `run_streamed(prompt)` (wrap the async generator with `asyncio.run`
     since the runner calls backends synchronously from threads), forwarding
     `item.completed` text to `display_callback`, accumulating assistant text and
     `turn.completed` usage tokens, and enforcing `timeout`.
   - Map failures/auth errors to a non-zero `exit_code`; set the resolved thread id
     as `session_id`.
   - Apply `env` for the child (Codex auth / API key) as the subprocess path does.
3. Verify E2E parity against the existing Codex suite:
   ```
   /home/mowteam/.pyenv/bin/python -m pytest tests/test_e2e_codex.py -x -v
   ```
   Add a `backend="codex-sdk"` variant of `test_trivial_workflow_codex` and confirm
   it produces the same `hello.txt` result as `backend="codex"`. Requires Codex auth
   (`OPENAI_API_KEY` / `~/.codex` login) and the SDK installed.
4. The offline unit tests in `tests/test_backends.py::TestCodexSDKBackend` already
   pin the name, signature parity, system-prompt folding, thread-id preservation,
   and the `AgentResult` contract via a stubbed drive; they keep passing once the
   real `_drive_codex_sdk` lands.
