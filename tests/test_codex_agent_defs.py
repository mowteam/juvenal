"""Tests for dual-emitting the shipped role subagents into Codex's `.codex/agents/*.toml`.

The Claude path ships role bodies as `.claude/agents/*.md`; a codex-backed worker/verifier
cannot consume those. These tests cover the additive Codex mirror: the same shipped bodies are
serialized to valid `.codex/agents/<name>.toml` (name/description/developer_instructions — the
keys the installed codex-cli parses), the runtime attack-surface subagent is dual-written, and
the brief/verifier framing swaps the Claude "Agent tool" wording for Codex native spawn wording.
"""

from __future__ import annotations

from unittest.mock import patch

import tomllib

from juvenal.display import Display
from juvenal.dynamic.models import AttackSurfaceState
from juvenal.dynamic.runner import (
    _SHIPPED_AGENT_NAMES,
    DynamicAnalysisRunner,
    _agent_toml_from_shipped,
    _backend_is_codex,
    _codex_agent_toml,
    _load_agent_body,
    _parse_agent_frontmatter,
    write_codex_agent_definitions,
)
from juvenal.workflow import (
    AnalysisConfig,
    AnalystSpec,
    Phase,
    VerifierSpec,
    Workflow,
)
from tests.conftest import MockBackend


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


# --- backend discriminator -------------------------------------------------


def test_backend_is_codex_discriminates():
    assert _backend_is_codex("codex")
    assert _backend_is_codex("codex-sdk")
    assert not _backend_is_codex("claude")
    assert not _backend_is_codex("claude-sdk")
    assert not _backend_is_codex(None)


# --- frontmatter parse + TOML serialization --------------------------------


def test_parse_agent_frontmatter_extracts_scalars():
    fm = _parse_agent_frontmatter("---\nname: poc-verifier\ndescription: Some desc\ntools: Read, Grep\n---\nbody\n")
    assert fm["name"] == "poc-verifier"
    assert fm["description"] == "Some desc"
    assert fm["tools"] == "Read, Grep"


def test_parse_agent_frontmatter_no_block_returns_empty():
    assert _parse_agent_frontmatter("no frontmatter here") == {}
    assert _parse_agent_frontmatter("---\nname: x\nunterminated") == {}


def test_codex_agent_toml_is_valid_and_roundtrips_tricky_content():
    tricky = 'body with """ triple, \\ backslash, " quote, and\ttab'
    text = _codex_agent_toml("n", 'desc " quote', tricky)
    parsed = tomllib.loads(text)
    assert parsed["name"] == "n"
    assert parsed["description"] == 'desc " quote'
    # Multi-line basic string trims the first newline and preserves the body verbatim.
    assert parsed["developer_instructions"].strip() == tricky


def test_all_shipped_agents_render_valid_codex_toml_with_verbatim_body():
    for name in _SHIPPED_AGENT_NAMES:
        text = _agent_toml_from_shipped(name)
        assert text is not None, f"missing shipped agent {name}"
        parsed = tomllib.loads(text)
        assert parsed["name"] and parsed["description"]
        # developer_instructions is the SAME body the Claude path loads.
        assert parsed["developer_instructions"].strip() == _load_agent_body(name).strip()


def test_agent_toml_from_shipped_missing_returns_none():
    assert _agent_toml_from_shipped("no-such-agent-xyz") is None


def test_agent_toml_from_shipped_working_dir_override_wins(tmp_path):
    agents = tmp_path / ".claude" / "agents"
    agents.mkdir(parents=True)
    (agents / "poc-verifier.md").write_text(
        "---\nname: poc-verifier\ndescription: LOCAL\n---\nPROJECT-LOCAL poc scope\n", encoding="utf-8"
    )
    parsed = tomllib.loads(_agent_toml_from_shipped("poc-verifier", tmp_path))
    assert parsed["description"] == "LOCAL"
    assert parsed["developer_instructions"].strip() == "PROJECT-LOCAL poc scope"


def test_exploit_sim_placeholders_survive_into_codex_toml():
    parsed = tomllib.loads(_agent_toml_from_shipped("exploit-sim-judge"))
    di = parsed["developer_instructions"]
    assert "{transcript}" in di and "{config_deltas}" in di


# --- directory materialization ---------------------------------------------


def test_write_codex_agent_definitions_writes_all_shipped(tmp_path):
    written = write_codex_agent_definitions(tmp_path)
    assert len(written) == len(_SHIPPED_AGENT_NAMES)
    agents_dir = tmp_path / ".codex" / "agents"
    for name in _SHIPPED_AGENT_NAMES:
        f = agents_dir / f"{name}.toml"
        assert f.is_file()
        assert tomllib.loads(f.read_text())["name"]


# --- runner run() gating ---------------------------------------------------


def test_run_emits_codex_defs_only_for_codex_backed_role(tmp_path):
    # A codex worker backend triggers materialization of the codex agent dir.
    config = AnalysisConfig(worker_backend="codex", min_captain_turns=0)
    runner = _make_unstarted_runner(tmp_path, config)
    assert runner._uses_codex_backend()
    with patch.object(runner, "_run_batch", return_value=None), patch.object(runner, "_maybe_start_analyst"):
        runner.run()
    assert (tmp_path / ".codex" / "agents" / "poc-verifier.toml").is_file()


def test_run_skips_codex_defs_for_pure_claude_run(tmp_path):
    config = AnalysisConfig(worker_backend="claude", captain_backend="claude", verifier_backend="claude")
    runner = _make_unstarted_runner(tmp_path, config)
    assert not runner._uses_codex_backend()
    with patch.object(runner, "_run_batch", return_value=None), patch.object(runner, "_maybe_start_analyst"):
        runner.run()
    assert not (tmp_path / ".codex").exists()


def test_uses_codex_backend_detects_verifier_spec_backend(tmp_path):
    config = AnalysisConfig(
        worker_backend="claude",
        captain_backend="claude",
        verifier_backend="claude",
        verifiers=[VerifierSpec(name="poc", backend="codex", prompt="x")],
    )
    runner = _make_unstarted_runner(tmp_path, config)
    assert runner._uses_codex_backend()


# --- backend-aware brief block + verifier framing --------------------------


def test_brief_block_uses_agent_tool_wording_for_claude(tmp_path):
    config = AnalysisConfig(analyst=AnalystSpec(prompt="x"))
    runner = _make_unstarted_runner(tmp_path, config)
    runner.state.attack_surface = AttackSurfaceState(status="ready", brief="BRIEF")
    block = runner._project_brief_block("claude")
    assert "Agent tool" in block
    assert ".codex/agents" not in block


def test_brief_block_uses_native_spawn_wording_for_codex(tmp_path):
    config = AnalysisConfig(analyst=AnalystSpec(prompt="x"))
    runner = _make_unstarted_runner(tmp_path, config)
    runner.state.attack_surface = AttackSurfaceState(status="ready", brief="BRIEF")
    block = runner._project_brief_block("codex")
    assert "Agent tool" not in block
    assert ".codex/agents/attack-surface.toml" in block
    assert "spawn the `attack-surface` subagent" in block


def test_verifier_scope_framing_cites_codex_path_for_codex_backend(tmp_path):
    config = AnalysisConfig(analyst=AnalystSpec(prompt="x"))
    runner = _make_unstarted_runner(tmp_path, config)
    # The trust-model verifier reads the runtime-written .claude/agents/attack-surface.md body.
    agents = tmp_path / ".claude" / "agents"
    agents.mkdir(parents=True)
    (agents / "attack-surface.md").write_text(
        "---\nname: attack-surface\n---\n\nRUNTIME-BRIEF-BODY\n", encoding="utf-8"
    )
    claude_scope = runner._load_subagent_scope_for_verifier("claude")
    codex_scope = runner._load_subagent_scope_for_verifier("codex")
    assert claude_scope is not None and codex_scope is not None
    assert "RUNTIME-BRIEF-BODY" in claude_scope and "RUNTIME-BRIEF-BODY" in codex_scope
    assert ".claude/agents/attack-surface.md" in claude_scope
    assert ".codex/agents/attack-surface.toml" in codex_scope


# --- runtime attack-surface subagent dual-write ----------------------------


def test_write_subagent_definition_dual_writes_codex_for_codex_run(tmp_path):
    config = AnalysisConfig(worker_backend="codex", analyst=AnalystSpec(prompt="x"))
    runner = _make_unstarted_runner(tmp_path, config)
    runner._write_subagent_definition("# Project Brief\n\nThe attacker is a joined peer.")
    md = tmp_path / ".claude" / "agents" / "attack-surface.md"
    toml_file = tmp_path / ".codex" / "agents" / "attack-surface.toml"
    assert md.is_file() and toml_file.is_file()
    parsed = tomllib.loads(toml_file.read_text())
    assert parsed["name"] == "attack-surface"
    # Both carry the same brief body.
    assert "joined peer" in parsed["developer_instructions"]
    assert "joined peer" in md.read_text()
    assert "PROJECT_BRIEF_BEGIN" in parsed["developer_instructions"]


def test_write_subagent_definition_skips_codex_for_pure_claude_run(tmp_path):
    config = AnalysisConfig(worker_backend="claude", captain_backend="claude", analyst=AnalystSpec(prompt="x"))
    runner = _make_unstarted_runner(tmp_path, config)
    runner._write_subagent_definition("brief")
    assert (tmp_path / ".claude" / "agents" / "attack-surface.md").is_file()
    assert not (tmp_path / ".codex" / "agents" / "attack-surface.toml").exists()
