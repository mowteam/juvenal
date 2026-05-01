"""Shared fixtures for Juvenal tests."""

from __future__ import annotations

import shutil
from threading import Lock

import pytest

from juvenal.backends import AgentResult, Backend, InteractiveResult
from juvenal.workflow import Phase, Workflow, make_command_check_prompt


@pytest.fixture
def tmp_workflow(tmp_path):
    """Create a temporary workflow directory with phases.

    New convention:
    - Subdirectory with prompt.md and NO check- prefix -> implement
    - Subdirectory with prompt.md and check- prefix -> check
    """
    phases_dir = tmp_path / "phases"
    phases_dir.mkdir()

    # Phase 1: implement (setup)
    p1 = phases_dir / "01-setup"
    p1.mkdir()
    (p1 / "prompt.md").write_text("Set up the project.")

    # Phase 2: check
    p2 = phases_dir / "02-check-build"
    p2.mkdir()
    (p2 / "prompt.md").write_text("Run the project's verification command and emit VERDICT.")

    # Phase 3: implement (feature)
    p3 = phases_dir / "03-implement"
    p3.mkdir()
    (p3 / "prompt.md").write_text("Implement the feature.")

    # Phase 4: check (review)
    p4 = phases_dir / "04-check-review"
    p4.mkdir()
    (p4 / "prompt.md").write_text("Review the implementation.\nVERDICT: PASS or FAIL")

    return tmp_path


@pytest.fixture
def sample_yaml(tmp_path):
    """Create a sample workflow YAML file with flat phases."""
    yaml_content = """\
name: test-workflow
backend: claude
working_dir: "."
max_bounces: 3

phases:
  - id: setup
    prompt: "Set up the project scaffolding."
  - id: setup-check
    type: check
    prompt: "Run `echo ok` and emit `VERDICT: PASS` if it succeeds."
  - id: implement
    prompt: "Implement the feature."
    bounce_target: setup
  - id: implement-script
    type: check
    prompt: "Run `echo ok` and emit `VERDICT: PASS` if it succeeds."
  - id: implement-review
    type: check
    role: tester
"""
    yaml_path = tmp_path / "workflow.yaml"
    yaml_path.write_text(yaml_content)
    return yaml_path


@pytest.fixture
def bare_md(tmp_path):
    """Create a bare .md workflow file."""
    md_path = tmp_path / "task.md"
    md_path.write_text("Implement a hello world program.")
    return md_path


class MockBackend(Backend):
    """Mock backend for testing."""

    def __init__(self, responses: list[AgentResult] | None = None):
        super().__init__()
        self._responses = list(responses or [])
        self._role_responses: dict[str, list[AgentResult]] = {
            "captain": [],
            "worker": [],
            "verifier": [],
            "reporter": [],
        }
        self._role_side_effects: dict[str, list] = {}
        self._role_chunks: dict[str, list[list[str]]] = {}
        self._chunks: list[list[str]] = []
        self._interactive_responses: list[InteractiveResult] = []
        self._call_count = 0
        self._queue_lock = Lock()
        self.calls: list[str] = []
        self.resume_calls: list[tuple[str, str]] = []
        self.interactive_calls: list[str] = []
        self.role_calls: list[tuple[str | None, str]] = []
        # Each agent call records (role, model) for assertion in tests of model
        # selection. `model` is None when the runner falls back to the CLI default.
        self.model_calls: list[tuple[str | None, str | None]] = []
        # Records every (role, chunk_text) pair sent through a display_callback.
        self.chunk_calls: list[tuple[str | None, str]] = []
        # Each run_agent call records (role, system_prompt) so tests can assert
        # the system-prompt routing. resume_agent calls don't capture this
        # (system prompt is fixed at session creation).
        self.system_prompt_calls: list[tuple[str | None, str | None]] = []

    def add_role_chunks(self, role: str, chunks: list[str]) -> None:
        """Queue a list of streaming chunks delivered to the next display_callback
        call for `role`. FIFO per role, consumed once."""
        self._role_chunks.setdefault(role, []).append(list(chunks))

    def add_role_side_effect(self, role: str, side_effect) -> None:
        """Register a callable invoked the next time `role` is dispatched.

        Side effects are FIFO per role and consumed once. The callable receives
        `(prompt, env)`. Used by tests to simulate filesystem effects (e.g., a
        reporter creating its output file) at the moment the agent runs.
        """
        self._role_side_effects.setdefault(role, []).append(side_effect)

    def name(self) -> str:
        return "mock"

    def add_response(
        self,
        exit_code: int = 0,
        output: str = "",
        transcript: str = "",
        input_tokens: int = 0,
        output_tokens: int = 0,
        session_id: str | None = None,
        role: str | None = None,
    ):
        result = AgentResult(
            exit_code=exit_code,
            output=output,
            transcript=transcript,
            duration=0.1,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            session_id=session_id,
        )
        if role is None:
            self._responses.append(result)
            return
        if role not in self._role_responses and not role.startswith("verifier:"):
            raise ValueError(f"Unknown mock backend role: {role!r}")
        self._role_responses.setdefault(role, []).append(result)

    def add_role_response(
        self,
        role: str,
        *,
        exit_code: int = 0,
        output: str = "",
        transcript: str = "",
        input_tokens: int = 0,
        output_tokens: int = 0,
        session_id: str | None = None,
    ) -> None:
        self.add_response(
            exit_code=exit_code,
            output=output,
            transcript=transcript,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            session_id=session_id,
            role=role,
        )

    def _detect_role(self, prompt: str, env: dict[str, str] | None) -> str | None:
        if env is not None:
            role = env.get("JUVENAL_ANALYSIS_ROLE")
            if role == "verifier":
                verifier_name = env.get("JUVENAL_ANALYSIS_VERIFIER_NAME")
                if verifier_name:
                    keyed = f"verifier:{verifier_name}"
                    if self._role_responses.get(keyed):
                        return keyed
                return "verifier"
            if role in self._role_responses:
                return role
        if "You are the captain for Juvenal's dynamic `analysis` phase." in prompt:
            return "captain"
        if "You are a scoped analysis worker for Juvenal's dynamic `analysis` phase." in prompt:
            return "worker"
        if "You are an independent verifier for Juvenal's dynamic `analysis` phase." in prompt:
            return "verifier"
        if "You are the reporter agent for Juvenal's dynamic `analysis` phase." in prompt:
            return "reporter"
        return None

    def _next_result(self, role: str | None) -> AgentResult:
        with self._queue_lock:
            if role is not None and self._role_responses.get(role):
                self._call_count += 1
                return self._role_responses[role].pop(0)
            # Fallback for keyed verifier roles: if no per-verifier response is queued,
            # fall back to the generic "verifier" queue.
            if role is not None and role.startswith("verifier:") and self._role_responses.get("verifier"):
                self._call_count += 1
                return self._role_responses["verifier"].pop(0)
            if self._responses:
                self._call_count += 1
                return self._responses.pop(0)
        return AgentResult(exit_code=0, output="VERDICT: PASS", transcript="", duration=0.1)

    def _consume_side_effect(self, role: str | None, prompt: str, env: dict[str, str] | None) -> None:
        if role is None:
            return
        with self._queue_lock:
            queue = self._role_side_effects.get(role)
            if not queue:
                return
            side_effect = queue.pop(0)
        side_effect(prompt, env)

    def _emit_chunks(self, role: str | None, display_callback) -> None:
        if role is None or display_callback is None:
            return
        with self._queue_lock:
            queue = self._role_chunks.get(role)
            chunks = queue.pop(0) if queue else None
        if chunks is None:
            return
        for chunk in chunks:
            self.chunk_calls.append((role, chunk))
            try:
                display_callback(chunk)
            except Exception:
                pass

    def run_agent(
        self,
        prompt,
        working_dir,
        display_callback=None,
        timeout=None,
        env=None,
        model=None,
        system_prompt=None,
        session_id=None,
    ):
        role = self._detect_role(prompt, env)
        self.calls.append(prompt)
        self.role_calls.append((role, prompt))
        self.model_calls.append((role, model))
        self.system_prompt_calls.append((role, system_prompt))
        self._consume_side_effect(role, prompt, env)
        self._emit_chunks(role, display_callback)
        result = self._next_result(role)
        if session_id is not None and result.session_id is None:
            result.session_id = session_id
        return result

    def resume_agent(self, session_id, prompt, working_dir, display_callback=None, timeout=None, env=None, model=None):
        role = self._detect_role(prompt, env)
        self.resume_calls.append((session_id, prompt))
        self.role_calls.append((role, prompt))
        self.model_calls.append((role, model))
        self._consume_side_effect(role, prompt, env)
        self._emit_chunks(role, display_callback)
        return self._next_result(role)

    def add_interactive_response(self, exit_code: int = 0, session_id: str = "mock-session"):
        self._interactive_responses.append(InteractiveResult(session_id=session_id, exit_code=exit_code))

    def run_interactive(self, prompt, working_dir, env=None, model=None):
        self.interactive_calls.append(prompt)
        if self._interactive_responses:
            return self._interactive_responses.pop(0)
        return InteractiveResult(session_id="mock-session", exit_code=0)

    def resume_interactive(self, session_id, working_dir, env=None, model=None):
        self.interactive_calls.append(f"resume:{session_id}")
        if self._interactive_responses:
            return self._interactive_responses.pop(0)
        return InteractiveResult(session_id=session_id, exit_code=0)


@pytest.fixture
def mock_backend():
    return MockBackend()


@pytest.fixture
def simple_workflow():
    """A simple workflow with an implement phase and an agentic check."""
    return Workflow(
        name="test",
        phases=[
            Phase(id="setup", type="implement", prompt="Do the thing."),
            Phase(id="setup-check", type="check", prompt=make_command_check_prompt("exit 0")),
        ],
        backend="claude",
        max_bounces=3,
    )


def claude_available():
    """Check if Claude CLI is available."""
    return shutil.which("claude") is not None


def codex_available():
    """Check if Codex CLI is available."""
    return shutil.which("npx") is not None
