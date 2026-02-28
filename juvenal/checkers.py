"""Checker execution — agent, script, and composite."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from juvenal.backends import Backend
    from juvenal.workflow import Checker


@dataclass
class CheckResult:
    """Result from running a checker."""

    passed: bool
    reason: str
    output: str


def run_checker(checker: Checker, backend: Backend, working_dir: str, display_callback=None) -> CheckResult:
    """Run a single checker and return the result."""
    if checker.type == "script":
        return _run_script_checker(checker, working_dir)
    elif checker.type == "agent":
        return _run_agent_checker(checker, backend, working_dir, display_callback)
    elif checker.type == "composite":
        return _run_composite_checker(checker, backend, working_dir, display_callback)
    else:
        raise ValueError(f"Unknown checker type: {checker.type!r}")


def _run_script_checker(checker: Checker, working_dir: str) -> CheckResult:
    """Run a script checker. Exit 0 = PASS, nonzero = FAIL."""
    try:
        result = subprocess.run(
            checker.run,
            shell=True,
            cwd=working_dir,
            capture_output=True,
            text=True,
            timeout=600,
        )
        output = result.stdout + result.stderr
        if result.returncode == 0:
            return CheckResult(passed=True, reason="", output=output)
        return CheckResult(
            passed=False,
            reason=f"Script exited with code {result.returncode}",
            output=output,
        )
    except subprocess.TimeoutExpired:
        return CheckResult(passed=False, reason="Script timed out after 600s", output="")


def _run_agent_checker(checker: Checker, backend: Backend, working_dir: str, display_callback=None) -> CheckResult:
    """Run an agent checker. Parses VERDICT from output."""
    prompt = checker.render_prompt()
    result = backend.run_agent(prompt, working_dir, display_callback)

    if result.exit_code != 0:
        return CheckResult(
            passed=False,
            reason=f"Checker agent crashed (exit {result.exit_code})",
            output=result.output,
        )

    passed, reason = parse_verdict(result.output)
    return CheckResult(passed=passed, reason=reason, output=result.output)


def _run_composite_checker(checker: Checker, backend: Backend, working_dir: str, display_callback=None) -> CheckResult:
    """Run composite checker: script first, then agent with script output."""
    # Run script
    try:
        script_result = subprocess.run(
            checker.run,
            shell=True,
            cwd=working_dir,
            capture_output=True,
            text=True,
            timeout=600,
        )
        script_output = script_result.stdout + script_result.stderr
    except subprocess.TimeoutExpired:
        return CheckResult(passed=False, reason="Composite script timed out after 600s", output="")

    # Run agent with script output injected
    prompt = checker.render_prompt(script_output=script_output)
    result = backend.run_agent(prompt, working_dir, display_callback)

    if result.exit_code != 0:
        return CheckResult(
            passed=False,
            reason=f"Composite checker agent crashed (exit {result.exit_code})",
            output=result.output,
        )

    passed, reason = parse_verdict(result.output)
    return CheckResult(passed=passed, reason=reason, output=result.output)


def parse_verdict(output: str) -> tuple[bool, str]:
    """Parse VERDICT from agent output, scanning backwards.

    Returns (passed, reason).
    """
    for line in reversed(output.splitlines()):
        line = line.strip()
        if line.startswith("VERDICT: PASS"):
            return True, ""
        if line.startswith("VERDICT: FAIL"):
            reason = line.split("VERDICT: FAIL:", 1)[-1].strip() if "FAIL:" in line else "unspecified"
            return False, reason
    return False, "checker did not emit a VERDICT line"
