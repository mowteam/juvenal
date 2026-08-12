"""Cross-vendor MIXED-RUN regression: the runner stays backend-agnostic.

Guards the core invariant that a single analysis phase can route different
roles to different backends (e.g. a claude captain + a codex worker) and that
swapping in the SDK backend classes changes nothing above ``backends.py`` — the
runner only ever sees the opaque ``Backend`` interface and the ``AgentResult`` /
``session_id`` contract.
"""

from __future__ import annotations

import inspect

from juvenal.backends import (
    AgentResult,
    Backend,
    ClaudeBackend,
    ClaudeSDKBackend,
    CodexBackend,
    CodexSDKBackend,
    create_backend,
)
from juvenal.display import Display
from juvenal.dynamic.models import VerificationRecord, WorkerAttempt
from juvenal.dynamic.runner import DynamicAnalysisRunner
from juvenal.workflow import AnalysisConfig, Phase, VerifierSpec, Workflow
from tests.conftest import MockBackend

# Every concrete backend the factory can hand back. SDK names fall back to their
# subprocess backend when the SDK isn't installed (the case on CI), so the
# concrete class list is deduped at resolution time, not here.
_ALL_BACKEND_NAMES = ("claude", "codex", "claude-sdk", "codex-sdk")


def _mixed_runner(tmp_path, config: AnalysisConfig, *, create_backend_impl, seed_names=()):
    """Runner wired to a per-name backend factory, without running the loop.

    ``seed_names`` pre-populates ``_backend_by_name`` via the factory so later
    ``_get_backend`` calls (after this helper's create_backend patch is gone) hit
    the cache instead of falling through to the real subprocess factory.
    """
    phase = Phase(id="analyze", type="analysis", prompt="x", analysis=config)
    workflow = Workflow(name="x", phases=[phase], working_dir=str(tmp_path))
    from unittest.mock import patch

    with patch("juvenal.dynamic.runner.create_backend", side_effect=create_backend_impl):
        runner = DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=tmp_path / "state.json",
            run_mode="fresh",
            display=Display(plain=True),
            interactive=False,
        )
        for name in seed_names:
            runner._backend_by_name[name] = create_backend_impl(name)
    return runner


# ---------------------------------------------------------------------------
# (1) Mixed per-role config routes each role to the RIGHT backend.
# ---------------------------------------------------------------------------


def test_get_backend_routes_each_role_to_its_own_backend(tmp_path):
    """A claude/codex mixed config resolves each role's backend name to a
    distinct concrete backend through the real create_backend/_get_backend."""
    config = AnalysisConfig(
        captain_backend="claude",
        worker_backend="codex",
        verifier_backend="claude",
    )
    # Use the REAL factory here: this is the actual routing seam.
    runner = _mixed_runner(tmp_path, config, create_backend_impl=create_backend)

    captain = runner._get_backend(config.captain_backend)
    worker = runner._get_backend(config.worker_backend)
    verifier = runner._get_backend(config.verifier_backend)

    assert isinstance(captain, ClaudeBackend)
    assert isinstance(worker, CodexBackend)
    assert isinstance(verifier, ClaudeBackend)

    # The runner never conflates two different vendors into one instance.
    assert captain is not worker
    assert captain.name() == "claude"
    assert worker.name() == "codex"

    # Same name resolves to the same cached instance (routing is stable, not
    # a fresh backend per call), and the captain/verifier share a backend since
    # they share a name.
    assert runner._get_backend("claude") is captain
    assert verifier is captain


def test_worker_and_verifier_dispatch_land_on_their_own_backend(tmp_path):
    """End-to-end proof through the runner: a worker dispatched on one backend
    and a verifier dispatched on a DIFFERENT backend each reach only their own
    backend instance. This is what makes a claude-captain/codex-worker run work
    without any vendor-specific branch in the runner."""
    backends_by_name: dict[str, MockBackend] = {}

    def factory(name: str) -> MockBackend:
        backends_by_name.setdefault(name, MockBackend())
        return backends_by_name[name]

    config = AnalysisConfig(
        captain_backend="claude",
        worker_backend="codex",
        verifier_backend="claude",
    )
    runner = _mixed_runner(tmp_path, config, create_backend_impl=factory, seed_names=("codex", "claude"))

    worker_backend = backends_by_name["codex"]
    verifier_backend = backends_by_name["claude"]
    worker_backend.add_role_response("worker", output="WORKER_JSON_BEGIN\n{}\nWORKER_JSON_END")
    verifier_backend.add_role_response("verifier", output="VERIFICATION_JSON_BEGIN\n{}\nVERIFICATION_JSON_END")

    attempt = WorkerAttempt(
        attempt_id="task-1",
        target_id="t1",
        generation=0,
        backend="codex",
        session_id="sess-w",
        status="queued",
        started_at=None,
        completed_at=None,
    )
    runner._execute_worker_attempt(attempt, "worker prompt", system_prompt="sys")

    verification = VerificationRecord(
        verification_id="v-1",
        claim_id="c-1",
        target_id="t1",
        generation=0,
        backend="claude",
        verifier_role="default",
        session_id="sess-v",
        status="pending",
        disposition=None,
        reason="",
        rejection_class=None,
        raw_output="",
        started_at=None,
        completed_at=None,
        verifier_name="default",
        verifier_index=0,
    )
    runner._execute_verifier(verification, "verifier prompt", system_prompt="sys")

    # The worker call landed on the codex backend only; the verifier on claude only.
    assert worker_backend.calls, "worker dispatch never reached the codex backend"
    assert verifier_backend.calls, "verifier dispatch never reached the claude backend"
    assert not verifier_backend.resume_calls  # fresh session, not a resume
    worker_roles = {role for role, _ in worker_backend.role_calls}
    verifier_roles = {role for role, _ in verifier_backend.role_calls}
    assert worker_roles == {"worker"}
    assert verifier_roles == {"verifier"}
    # No cross-contamination: the codex backend never saw a verifier call and
    # vice versa.
    assert "verifier" not in worker_roles
    assert "worker" not in verifier_roles


# ---------------------------------------------------------------------------
# (2) The AgentResult / session_id contract is honored identically per backend.
# ---------------------------------------------------------------------------


def test_run_agent_signature_identical_across_all_backends():
    """Every backend accepts the same run_agent kwargs, so the runner can call
    any of them the same way. If a backend dropped e.g. session_id or
    hooks_config, a mixed run would blow up at dispatch."""
    reference = list(inspect.signature(ClaudeBackend().run_agent).parameters)
    for backend in (CodexBackend(), ClaudeSDKBackend(), CodexSDKBackend(), MockBackend()):
        params = list(inspect.signature(backend.run_agent).parameters)
        assert params == reference, f"{backend.name()} run_agent signature drifted: {params} != {reference}"


def test_resume_agent_signature_identical_across_all_backends():
    reference = list(inspect.signature(ClaudeBackend().resume_agent).parameters)
    for backend in (CodexBackend(), ClaudeSDKBackend(), CodexSDKBackend(), MockBackend()):
        params = list(inspect.signature(backend.resume_agent).parameters)
        assert params == reference, f"{backend.name()} resume_agent signature drifted"


def test_agent_result_contract_fields_are_backend_independent():
    """AgentResult exposes exactly the fields the runner reads, and session_id
    is an opaque string the runner stores and echoes without interpreting."""
    from dataclasses import fields

    field_names = {f.name for f in fields(AgentResult)}
    assert field_names == {
        "exit_code",
        "output",
        "transcript",
        "duration",
        "input_tokens",
        "output_tokens",
        "session_id",
        "rate_limit_status",
    }

    # A Codex thread id and a Claude UUID are both just strings to the runner.
    for sid in ("thread-abc123", "6f1c2d34-0000-4000-8000-000000000000", None):
        result = AgentResult(exit_code=0, output="", transcript="", duration=0.1, session_id=sid)
        assert result.session_id == sid


def test_mock_backend_echoes_preallocated_session_id_like_claude(tmp_path):
    """The session_id contract: when the caller pre-allocates a session id and
    the backend didn't mint its own, the id is echoed back — the behavior the
    runner relies on to persist a session before the subprocess streams."""
    backend = MockBackend()
    backend.add_response(output="ok")
    result = backend.run_agent("p", str(tmp_path), session_id="pre-allocated")
    assert result.session_id == "pre-allocated"


# ---------------------------------------------------------------------------
# (3) Swapping in the SDK backend classes changes nothing above backends.py.
# ---------------------------------------------------------------------------


def test_sdk_backend_names_resolve_to_backend_without_runner_awareness(tmp_path):
    """A config naming the SDK backends resolves through the SAME _get_backend
    path to a Backend instance. When the SDK isn't installed the factory falls
    back to the subprocess backend; either way the runner sees only Backend."""
    config = AnalysisConfig(
        captain_backend="claude-sdk",
        worker_backend="codex-sdk",
        verifier_backend="claude-sdk",
    )
    runner = _mixed_runner(tmp_path, config, create_backend_impl=create_backend)

    captain = runner._get_backend(config.captain_backend)
    worker = runner._get_backend(config.worker_backend)

    assert isinstance(captain, Backend)
    assert isinstance(worker, Backend)
    # claude-sdk resolves to the SDK backend when available, else the subprocess
    # backend; both satisfy the interface. Same for codex-sdk.
    assert isinstance(captain, (ClaudeSDKBackend, ClaudeBackend))
    assert isinstance(worker, (CodexSDKBackend, CodexBackend))


def test_sdk_backends_swap_in_transparently_for_a_role(tmp_path):
    """Renaming a role's backend from 'codex' to 'codex-sdk' does not change how
    the runner dispatches: same routing, same call surface. Prove it by driving
    a worker dispatch with the SDK backend class mocked in for that name."""
    sdk_worker = MockBackend()
    sdk_worker.add_role_response("worker", output="WORKER_JSON_BEGIN\n{}\nWORKER_JSON_END")

    def factory(name: str) -> Backend:
        assert name == "codex-sdk"
        return sdk_worker

    config = AnalysisConfig(worker_backend="codex-sdk")
    runner = _mixed_runner(tmp_path, config, create_backend_impl=factory, seed_names=("codex-sdk",))

    attempt = WorkerAttempt(
        attempt_id="task-1",
        target_id="t1",
        generation=0,
        backend="codex-sdk",
        session_id="sess-w",
        status="queued",
        started_at=None,
        completed_at=None,
    )
    runner._execute_worker_attempt(attempt, "worker prompt", system_prompt="sys")

    assert sdk_worker.calls, "SDK backend was not reached for the worker role"
    assert [role for role, _ in sdk_worker.role_calls] == ["worker"]


def test_verifier_chain_can_mix_backends_per_verifier(tmp_path):
    """A single verifier chain can pin each verifier to a different backend
    (the record carries its own backend). The runner routes each to its own
    backend via verification.backend, not a phase-global name."""
    config = AnalysisConfig(
        verifiers=[
            VerifierSpec(name="static", backend="claude", prompt=""),
            VerifierSpec(name="poc", backend="codex", prompt=""),
        ]
    )
    runner = _mixed_runner(tmp_path, config, create_backend_impl=create_backend)

    static_backend = runner._get_backend(config.verifiers[0].backend)
    poc_backend = runner._get_backend(config.verifiers[1].backend)

    assert isinstance(static_backend, ClaudeBackend)
    assert isinstance(poc_backend, CodexBackend)
    assert static_backend is not poc_backend
