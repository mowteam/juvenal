# Codex Python SDK backend — exploratory / UNVERIFIED

> **STATUS: EXPLORATORY. Do not treat any API surface in this file as ground
> truth.** Whether an official, embeddable OpenAI Codex Python SDK exists —
> and, if so, its package name, import name, and exact API — is being confirmed
> by a separate SDK-recon pass. Until that lands, every SDK-specific class,
> method, field, and kwarg named below is **UNVERIFIED**. Confirm against the
> actually-installed package before writing code that depends on it, and prefer
> the subprocess `CodexBackend` (which is real and works) for anything shipping.

`CodexSDKBackend` (`juvenal/backends.py`) is scaffolded the same way
`ClaudeSDKBackend` is: the class, the `AgentResult` / `InteractiveResult`
contract, the `codex-sdk` backend name, the `create_backend("codex-sdk")` factory
case (opt-in + subprocess fallback), the system-prompt folding, and the feature
flag are in place and unit-tested. The actual SDK drive loop
(`CodexSDKBackend._drive_codex_sdk`) raises `NotImplementedError`. The module
imports cleanly and the existing tests pass whether or not any SDK is present.

## What is verified vs. unverified

Verified locally (import probe only — NOT a guarantee of provenance or that
these are the official OpenAI packages):

- Running `import openai_codex_sdk` / `import openai_codex` in the juvenal pyenv
  may or may not succeed depending on the machine. Presence of an importable
  module named this way does **not** establish that it is an official,
  supported OpenAI Codex SDK. Re-probe in the target environment.

UNVERIFIED — do not assert these without recon confirmation:

- That an official embeddable Codex Python SDK exists or is published by OpenAI.
- The PyPI/package name and import name (`openai-codex-sdk` / `openai_codex_sdk`
  vs. `openai-codex` / `openai_codex` vs. something else).
- The constructor and thread lifecycle spelling (e.g. a `Codex()` client, a
  `start_thread` vs. `thread_start` method, sync vs. async forms).
- The turn / streamed-event object shapes and any field names used to read back
  final assistant text, token usage, or the thread id.
- Whether the SDK is asyncio-based (would dictate wrapping the drive loop in
  `asyncio.run` since the runner calls backends synchronously from threads).

Any real integration must first pin these against the installed package.

## Why this backend exists (real subprocess pain points it targets)

These are the concrete, reproducible problems with the current subprocess Codex
path — the motivation for an in-process backend, independent of whether an SDK
is available:

- `npx @openai/codex@latest exec/resume` on **parallel** runs hits ENOTEMPTY /
  ETXTBSY races as concurrent npx invocations unpack the same cache.
- Transient `auth.json` 401s from the CLI path.
- npx install/unpack overhead on every subprocess spawn.

An in-process backend that keeps one persistent Codex process and reuses auth
would remove the per-call npx spawn, which is the root of all three. Whether
such a backend rides an official SDK or a thinner IPC shim is an open question
for the recon pass.

## Contract the backend must satisfy (already enforced by the scaffold)

`CodexSDKBackend.run_agent` / `resume_agent` must return an `AgentResult`
identical in shape to the subprocess `CodexBackend`:

- `exit_code`: 0 on success, non-zero on error.
- `output`: concatenated final assistant text.
- `transcript`: full event log.
- `session_id`: the Codex thread/session id. Codex assigns its own, so the
  externally chosen `session_id` on `run_agent` is ignored (matching subprocess
  `CodexBackend`); on `resume_agent` the caller's id is preserved if the drive
  loop returns `None`.
- `input_tokens` / `output_tokens`: summed across the turn.

Codex has **no separate system-prompt slot**: `run_agent` folds `system_prompt`
into the user message (`"{system_prompt}\n\n{prompt}"`) before reaching the drive
seam, and `hooks_config` is a no-op (no Claude-`--settings` equivalent) — both
already handled in the scaffold.

## Human follow-up (network + Codex auth required — not runnable in unit tests)

1. Run the SDK-recon pass first: confirm whether an official Codex SDK exists,
   its package/import names, and its real API surface. Record the findings and
   replace the UNVERIFIED section above with confirmed facts (or delete this
   file if no SDK exists and the in-process path will use a different mechanism).
2. Only then implement `CodexSDKBackend._drive_codex_sdk` in
   `juvenal/backends.py`, against the confirmed API — constructing the client,
   starting or resuming a thread with `working_directory=working_dir` and the
   requested model/sandbox, driving the turn, forwarding streamed output to
   `display_callback`, accumulating assistant text + usage tokens, enforcing
   `timeout`, mapping auth/other failures to a non-zero `exit_code`, setting the
   resolved thread id as `session_id`, and applying `env` for the child.
3. Verify E2E parity against the existing Codex suite:
   ```
   pytest tests/test_e2e_codex.py -x -v
   ```
   Add a `backend="codex-sdk"` variant of `test_trivial_workflow_codex` and
   confirm it produces the same `hello.txt` result as `backend="codex"`.
   Requires Codex auth and the confirmed SDK installed.
4. The offline unit tests in `tests/test_backends.py::TestCodexSDKBackend` pin
   the name, signature parity, system-prompt folding, thread-id preservation,
   and the `AgentResult` contract via a stubbed drive; they keep passing once a
   real `_drive_codex_sdk` lands.
