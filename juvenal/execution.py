"""Shared execution result types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PhaseResult:
    """Result of executing a single phase."""

    success: bool
    bounce_target: str | None = None
    failure_context: str = ""


@dataclass
class PlanResult:
    """Result of planning a workflow from a goal description."""

    success: bool
    workflow_yaml_path: str | None = None
    temp_dir: str | None = None
    error: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
