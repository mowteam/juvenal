"""Shared fixtures for Juvenal tests."""

from __future__ import annotations

import shutil

import pytest

from juvenal.backends import AgentResult, Backend
from juvenal.workflow import Checker, Phase, Workflow


@pytest.fixture
def tmp_workflow(tmp_path):
    """Create a temporary workflow directory with phases."""
    phases_dir = tmp_path / "phases"
    phases_dir.mkdir()

    # Phase 1: setup
    p1 = phases_dir / "01-setup"
    p1.mkdir()
    (p1 / "prompt.md").write_text("Set up the project.")
    (p1 / "check-build.sh").write_text("#!/bin/bash\nexit 0\n")
    (p1 / "check-build.sh").chmod(0o755)

    # Phase 2: implement
    p2 = phases_dir / "02-implement"
    p2.mkdir()
    (p2 / "prompt.md").write_text("Implement the feature.")
    (p2 / "check-tests.sh").write_text("#!/bin/bash\nexit 0\n")
    (p2 / "check-tests.sh").chmod(0o755)
    (p2 / "check-tests.md").write_text("Review tests.\nVERDICT: {script_output}")

    return tmp_path


@pytest.fixture
def sample_yaml(tmp_path):
    """Create a sample workflow YAML file."""
    yaml_content = """\
name: test-workflow
backend: claude
working_dir: "."
max_retries: 3

phases:
  - id: setup
    prompt: "Set up the project scaffolding."
    checkers:
      - type: script
        run: "echo ok"
  - id: implement
    prompt: "Implement the feature."
    checkers:
      - type: script
        run: "echo ok"
      - type: agent
        role: tester

bounce_targets:
  implement: setup
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
        self._responses = list(responses or [])
        self._call_count = 0
        self.calls: list[str] = []

    def name(self) -> str:
        return "mock"

    def add_response(self, exit_code: int = 0, output: str = "", transcript: str = ""):
        self._responses.append(AgentResult(exit_code=exit_code, output=output, transcript=transcript, duration=0.1))

    def run_agent(self, prompt, working_dir, display_callback=None):
        self.calls.append(prompt)
        if self._call_count < len(self._responses):
            result = self._responses[self._call_count]
        else:
            result = AgentResult(exit_code=0, output="VERDICT: PASS", transcript="", duration=0.1)
        self._call_count += 1
        return result


@pytest.fixture
def mock_backend():
    return MockBackend()


@pytest.fixture
def simple_workflow():
    """A simple workflow with one phase and one script checker."""
    return Workflow(
        name="test",
        phases=[
            Phase(
                id="setup",
                prompt="Do the thing.",
                checkers=[Checker(name="check", type="script", run="exit 0")],
            )
        ],
        backend="claude",
        max_retries=3,
    )


def claude_available():
    """Check if Claude CLI is available."""
    return shutil.which("claude") is not None


def codex_available():
    """Check if Codex CLI is available."""
    return shutil.which("npx") is not None
