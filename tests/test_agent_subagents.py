"""Tests for the `.claude/agents/*.md` subagent loading + fallback wiring.

Covers the additive AGENTS migration: verifier and exploit-sim role prompts are
sourced from shipped subagent files, with an explicit config `prompt` winning and
a per-project working-dir copy overriding the packaged one.
"""

from __future__ import annotations

from unittest.mock import patch

from juvenal.display import Display
from juvenal.dynamic.runner import (
    _PACKAGE_AGENTS_DIR,
    DynamicAnalysisRunner,
    _load_agent_body,
    _strip_agent_frontmatter,
)
from juvenal.workflow import (
    AnalysisConfig,
    AttackerSpec,
    EnvBuilderSpec,
    ExploitJudgeSpec,
    ExploitSimSpec,
    Phase,
    SimulatorSpec,
    VerifierSpec,
    Workflow,
    apply_vars,
)
from tests.conftest import MockBackend

SHIPPED_AGENTS = (
    "attack-surface-verifier",
    "trust-model-verifier",
    "poc-verifier",
    "novelty-verifier",
    "exploit-sim-env-builder",
    "exploit-sim-simulator",
    "exploit-sim-attacker",
    "exploit-sim-judge",
)


def _make_unstarted_runner(tmp_path, config: AnalysisConfig) -> DynamicAnalysisRunner:
    phase = Phase(id="analyze", type="analysis", prompt="x", analysis=config)
    workflow = Workflow(name="x", phases=[phase], working_dir=str(tmp_path))
    state_file = tmp_path / "analysis-state.json"
    backend = MockBackend()
    with patch("juvenal.dynamic.runner.create_backend", side_effect=lambda name: backend):
        return DynamicAnalysisRunner(
            phase=phase,
            workflow=workflow,
            state_file=state_file,
            run_mode="fresh",
            display=Display(plain=True),
            interactive=False,
        )


# --- module-level helpers --------------------------------------------------


def test_strip_frontmatter_wellformed():
    assert _strip_agent_frontmatter("---\nname: x\ntools: Read\n---\nbody line\n") == "body line\n"


def test_strip_frontmatter_missing_closing_delimiter_passes_through():
    # No closing `---`: nothing is stripped (guards against eating the body).
    text = "---\nname: x\nbody with no closing delimiter"
    assert _strip_agent_frontmatter(text) == text


def test_strip_frontmatter_no_frontmatter_passes_through():
    assert _strip_agent_frontmatter("just a body, no frontmatter") == "just a body, no frontmatter"


def test_all_shipped_agents_load_with_frontmatter_stripped():
    for name in SHIPPED_AGENTS:
        assert (_PACKAGE_AGENTS_DIR / f"{name}.md").is_file(), f"missing shipped agent {name}"
        body = _load_agent_body(name)
        assert body, f"empty body for {name}"
        assert not body.startswith("---"), f"frontmatter not stripped for {name}"


def test_load_agent_body_missing_returns_none():
    assert _load_agent_body("no-such-agent-xyz") is None


def test_load_agent_body_working_dir_overrides_package(tmp_path):
    agents = tmp_path / ".claude" / "agents"
    agents.mkdir(parents=True)
    (agents / "poc-verifier.md").write_text("---\nname: poc-verifier\n---\nPROJECT-LOCAL poc scope\n", encoding="utf-8")
    assert _load_agent_body("poc-verifier", tmp_path) == "PROJECT-LOCAL poc scope"
    # Without the working dir, the packaged copy (different text) is used.
    assert _load_agent_body("poc-verifier") != "PROJECT-LOCAL poc scope"


def test_load_agent_body_empty_working_dir_file_falls_back_to_package(tmp_path):
    agents = tmp_path / ".claude" / "agents"
    agents.mkdir(parents=True)
    # Frontmatter-only (empty body) working-dir file must not shadow the package.
    (agents / "novelty-verifier.md").write_text("---\nname: novelty-verifier\n---\n\n", encoding="utf-8")
    body = _load_agent_body("novelty-verifier", tmp_path)
    assert body == _load_agent_body("novelty-verifier")
    assert body


# --- verifier wiring -------------------------------------------------------


def test_verifier_empty_prompt_falls_back_to_shipped_subagent(tmp_path):
    """A verifier named after a shipped subagent with an EMPTY `prompt` picks up
    the subagent body as its scope."""
    config = AnalysisConfig(verifiers=[VerifierSpec(name="poc", backend="claude", prompt="")])
    runner = _make_unstarted_runner(tmp_path, config)
    scope = runner._rendered_verifier_prompts["poc"]
    assert scope == _load_agent_body("poc-verifier")
    assert "PoC reproduction" in scope


def test_verifier_explicit_prompt_wins_over_shipped_subagent(tmp_path):
    """An explicit `prompt` overrides the shipped subagent (user override path)."""
    config = AnalysisConfig(verifiers=[VerifierSpec(name="poc", backend="claude", prompt="CUSTOM inline poc scope")])
    runner = _make_unstarted_runner(tmp_path, config)
    scope = runner._rendered_verifier_prompts["poc"]
    assert scope == "CUSTOM inline poc scope"
    assert scope != _load_agent_body("poc-verifier")


def test_verifier_unknown_name_empty_prompt_stays_empty(tmp_path):
    """A verifier whose name has no shipped subagent and no prompt gets no scope."""
    config = AnalysisConfig(verifiers=[VerifierSpec(name="brand-new-role", backend="claude", prompt="")])
    runner = _make_unstarted_runner(tmp_path, config)
    assert runner._rendered_verifier_prompts["brand-new-role"] == ""


def test_verifier_working_dir_override_wins_over_package(tmp_path):
    agents = tmp_path / ".claude" / "agents"
    agents.mkdir(parents=True)
    (agents / "novelty-verifier.md").write_text(
        "---\nname: novelty-verifier\n---\nPROJECT-LOCAL novelty scope\n", encoding="utf-8"
    )
    config = AnalysisConfig(verifiers=[VerifierSpec(name="novelty", backend="claude", prompt="")])
    runner = _make_unstarted_runner(tmp_path, config)
    assert runner._rendered_verifier_prompts["novelty"] == "PROJECT-LOCAL novelty scope"


# --- exploit-sim wiring ----------------------------------------------------


def test_exploit_sim_empty_prompts_fall_back_to_shipped_subagents(tmp_path):
    config = AnalysisConfig(
        exploit_sim=ExploitSimSpec(
            env_builder=EnvBuilderSpec(prompt=""),
            simulator=SimulatorSpec(prompt=""),
            attacker=AttackerSpec(prompt=""),
            judge=ExploitJudgeSpec(prompt=""),
        )
    )
    runner = _make_unstarted_runner(tmp_path, config)

    def rendered(name):
        return apply_vars(_load_agent_body(name), runner.workflow.vars)

    assert runner._rendered_exploit_sim_prompts["env_builder"] == rendered("exploit-sim-env-builder")
    assert runner._rendered_exploit_sim_prompts["simulator"] == rendered("exploit-sim-simulator")
    assert runner._rendered_exploit_sim_prompts["attacker"] == rendered("exploit-sim-attacker")
    assert runner._rendered_exploit_sim_prompts["exploit_judge"] == rendered("exploit-sim-judge")


def test_exploit_sim_subagent_preserves_runner_placeholders(tmp_path):
    """The runner fills {round}/{claim_packet}/... via .replace(); the shipped
    bodies must keep those tokens intact after apply_vars."""
    config = AnalysisConfig(
        exploit_sim=ExploitSimSpec(
            simulator=SimulatorSpec(prompt=""),
            attacker=AttackerSpec(prompt=""),
            judge=ExploitJudgeSpec(prompt=""),
        )
    )
    runner = _make_unstarted_runner(tmp_path, config)
    sim = runner._rendered_exploit_sim_prompts["simulator"]
    assert "{round}" in sim and "{max_rounds}" in sim and "{claim_packet}" in sim
    judge = runner._rendered_exploit_sim_prompts["exploit_judge"]
    assert "{transcript}" in judge and "{config_deltas}" in judge


def test_exploit_sim_explicit_prompt_wins_over_shipped_subagent(tmp_path):
    config = AnalysisConfig(
        exploit_sim=ExploitSimSpec(
            env_builder=EnvBuilderSpec(prompt="CUSTOM env builder"),
            simulator=SimulatorSpec(prompt=""),
            attacker=AttackerSpec(prompt=""),
            judge=ExploitJudgeSpec(prompt=""),
        )
    )
    runner = _make_unstarted_runner(tmp_path, config)
    assert runner._rendered_exploit_sim_prompts["env_builder"] == "CUSTOM env builder"
    assert runner._rendered_exploit_sim_prompts["env_builder"] != _load_agent_body("exploit-sim-env-builder")
