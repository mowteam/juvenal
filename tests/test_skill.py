"""E2E test: Claude invokes the juvenal skill."""

import subprocess

import pytest

from tests.conftest import claude_available


@pytest.mark.skipif(not claude_available(), reason="Claude CLI not available")
def test_skill_invocation(tmp_path):
    """Test that Claude can invoke the juvenal skill to create a workflow."""
    result = subprocess.run(
        [
            "claude",
            "-p",
            "--dangerously-skip-permissions",
            "Use the /juvenal skill to create a simple workflow that creates a hello.txt file. "
            "Save the workflow as workflow.yaml in the current directory.",
        ],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        timeout=120,
    )
    # We just check it runs without crashing
    assert result.returncode == 0 or (tmp_path / "workflow.yaml").exists()
