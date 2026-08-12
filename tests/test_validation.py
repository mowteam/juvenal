"""Unit tests for workflow validation."""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from juvenal.cli import build_parser, cmd_validate
from juvenal.engine import Engine
from juvenal.workflow import (
    AnalysisConfig,
    ParallelGroup,
    Phase,
    Workflow,
    apply_vars,
    expand_multi_vars,
    load_workflow,
    make_command_check_prompt,
    validate_workflow,
)


class TestValidateWorkflow:
    def test_valid_workflow(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="setup", type="implement", prompt="Set up."),
                Phase(id="check", type="check", prompt=make_command_check_prompt("true")),
            ],
        )
        assert validate_workflow(wf) == []

    def test_duplicate_phase_ids(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="setup", type="implement", prompt="Set up."),
                Phase(id="setup", type="implement", prompt="Again."),
            ],
        )
        errors = validate_workflow(wf)
        assert any("Duplicate phase ID" in e for e in errors)

    def test_invalid_phase_type(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="setup", type="invalid", prompt="Set up."),
            ],
        )
        errors = validate_workflow(wf)
        assert any("invalid type" in e for e in errors)

    def test_invalid_bounce_target(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="setup", type="implement", prompt="Set up.", bounce_target="nonexistent"),
            ],
        )
        errors = validate_workflow(wf)
        assert any("bounce_target" in e and "nonexistent" in e for e in errors)

    def test_valid_bounce_target(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="setup", type="implement", prompt="Set up."),
                Phase(id="review", type="check", role="tester", bounce_target="setup"),
            ],
        )
        assert validate_workflow(wf) == []

    def test_implement_missing_prompt(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="setup", type="implement"),
            ],
        )
        errors = validate_workflow(wf)
        assert any("has no prompt" in e for e in errors)

    def test_check_missing_prompt_and_role(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="review", type="check"),
            ],
        )
        errors = validate_workflow(wf)
        assert any("no prompt or role" in e for e in errors)

    def test_check_with_role_is_valid(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="review", type="check", role="tester"),
            ],
        )
        assert validate_workflow(wf) == []

    def test_check_with_security_engineer_role_is_valid(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="review", type="check", role="security-engineer"),
            ],
        )
        assert validate_workflow(wf) == []

    def test_check_missing_all_verification_inputs(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="build", type="check"),
            ],
        )
        errors = validate_workflow(wf)
        assert any("no prompt or role" in e for e in errors)

    def test_invalid_role(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="review", type="check", role="invalid-role"),
            ],
        )
        errors = validate_workflow(wf)
        assert any("unknown role" in e for e in errors)

    def test_unknown_role_still_fails_after_security_engineer_added(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="review", type="check", role="security-reviewer"),
            ],
        )
        errors = validate_workflow(wf)
        assert any("unknown role" in e and "security-reviewer" in e for e in errors)

    def test_parallel_group_invalid_phase(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="setup", type="implement", prompt="Set up."),
            ],
            parallel_groups=[ParallelGroup(phases=["setup", "nonexistent"])],
        )
        errors = validate_workflow(wf)
        assert any("nonexistent" in e for e in errors)

    def test_parallel_group_valid(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="a", type="implement", prompt="A."),
                Phase(id="b", type="implement", prompt="B."),
            ],
            parallel_groups=[ParallelGroup(phases=["a", "b"])],
        )
        assert validate_workflow(wf) == []

    def test_multiple_errors(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="a", type="invalid"),
                Phase(id="a", type="check"),
                Phase(id="b", type="check"),
            ],
        )
        errors = validate_workflow(wf)
        assert len(errors) >= 3  # invalid type, duplicate ID, missing verification inputs

    def test_analysis_phase_with_defaults_is_valid(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="analyze", type="analysis", prompt="Analyze the repository."),
            ],
        )
        assert validate_workflow(wf) == []

    def test_analysis_phase_invalid_field_combinations(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="setup", type="implement", prompt="Set up."),
                Phase(
                    id="analyze",
                    type="analysis",
                    prompt="Analyze the repository.",
                    role="tester",
                    bounce_target="setup",
                    bounce_targets=["setup"],
                    workflow_file="/tmp/sub.yaml",
                    workflow_dir="/tmp/subdir",
                ),
            ],
        )
        errors = validate_workflow(wf)
        assert any("analysis phase must not have 'role'" in e for e in errors)
        assert any("analysis phase must not have 'bounce_target'" in e for e in errors)
        assert any("analysis phase must not have 'bounce_targets'" in e for e in errors)
        assert any("analysis phase must not have workflow_file or workflow_dir" in e for e in errors)

    def test_non_analysis_phase_rejects_analysis_config(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="build", type="implement", prompt="Build.", analysis=AnalysisConfig()),
            ],
        )
        errors = validate_workflow(wf)
        assert any("analysis config is only allowed on analysis phases" in e for e in errors)

    def test_analysis_phase_rejected_in_parallel_groups(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="analyze", type="analysis", prompt="Analyze."),
            ],
            parallel_groups=[ParallelGroup(phases=["analyze"])],
        )
        errors = validate_workflow(wf)
        assert any("analysis phase" in e and "parallel_groups" in e for e in errors)

    def test_analysis_phase_dry_run_rendering(self, tmp_path, capsys):
        workflow = Workflow(
            name="test",
            phases=[
                Phase(
                    id="analyze",
                    type="analysis",
                    prompt="Analyze {{TARGET}}.",
                    analysis=AnalysisConfig(max_workers=6, max_verifiers=9, interaction_timeout=1.5),
                ),
            ],
            vars={"TARGET": "repo"},
        )
        engine = Engine(workflow, state_file=str(tmp_path / "state.json"), dry_run=True, plain=True)

        assert engine.run() == 0
        captured = capsys.readouterr()
        assert "[analysis] analyze" in captured.out
        assert "Analyze repo." in captured.out
        assert "analysis:" in captured.out
        assert "captain: claude" in captured.out
        assert "worker:  claude" in captured.out
        assert "verifier_backend: claude" in captured.out
        assert "max_workers: 6" in captured.out
        assert "max_verifiers: 9" in captured.out
        assert "interaction_timeout: 1.5s" in captured.out

    def test_analysis_example_workflow_is_valid(self):
        workflow = load_workflow("juvenal/workflows/analysis-example.yaml")

        assert workflow.phases[0].type == "analysis"
        assert workflow.phases[1].type == "implement"
        assert validate_workflow(workflow) == []


SHIPPED_WORKFLOWS = sorted(p.name for p in Path("juvenal/workflows").glob("*.yaml"))


class TestShippedWorkflows:
    @pytest.mark.parametrize("name", SHIPPED_WORKFLOWS)
    def test_shipped_workflow_loads_and_validates(self, name):
        workflow = load_workflow(f"juvenal/workflows/{name}")

        assert workflow.phases, f"{name} declares no phases"
        assert validate_workflow(workflow) == []

    @pytest.mark.parametrize("name", SHIPPED_WORKFLOWS)
    def test_shipped_workflow_prompts_render_with_declared_vars(self, name):
        """Every templated prompt must render against the workflow's own vars.

        Guards the failure mode where a prompt references a var the workflow
        never declares — validate() catches phase prompts, but analysis role
        prompts (analyst / verifiers / reporter / exploit-sim) are rendered
        later, at dispatch, so a typo there surfaces mid-run instead of at
        load time.
        """
        workflow = load_workflow(f"juvenal/workflows/{name}")
        for phase in workflow.phases:
            config = phase.analysis
            if config is None:
                continue
            sources = [config.worker_prompt]
            if config.analyst is not None:
                sources.append(config.analyst.prompt)
            if config.reporter is not None:
                sources.append(config.reporter.prompt)
            sources.extend(spec.prompt for spec in config.verifiers)
            if config.exploit_sim is not None:
                sim = config.exploit_sim
                sources.extend([sim.env_builder.prompt, sim.simulator.prompt, sim.attacker.prompt, sim.judge.prompt])
            for source in sources:
                rendered = apply_vars(source, workflow.vars)
                # Undeclared vars are passed through as literal `{{name}}` rather
                # than raising, so a typo would otherwise ship silently — and the
                # leftover braces corrupt the runner's own `{placeholder}`
                # substitution downstream.
                assert "{{" not in rendered, f"{name}: unresolved Jinja passthrough in a role prompt"


class TestPwn2OwnSmartHomeWorkflow:
    """The Pwn2Own workflow leans on engine features whose contracts are easy to
    break silently: runtime `{placeholder}` substitution surviving Jinja
    rendering, and the ordered five-gate verifier chain."""

    @staticmethod
    def _analysis():
        workflow = load_workflow("juvenal/workflows/pwn2own-smart-home.yaml")
        phase = next(p for p in workflow.phases if p.type == "analysis")
        return workflow, phase.analysis

    def test_verifier_chain_order_and_preauth_subagent(self):
        _, config = self._analysis()

        assert [spec.name for spec in config.verifiers] == [
            "p2o-scope",
            "bug-class",
            "preauth-impact",
            "poc",
            "novelty",
        ]
        by_name = {spec.name: spec for spec in config.verifiers}
        assert by_name["preauth-impact"].use_attack_surface_subagent

    def test_recon_precedes_analysis_and_check_bounces_back(self):
        workflow, _ = self._analysis()

        ids = [phase.id for phase in workflow.phases]
        assert ids.index("device-recon") < ids.index("hunt-bugs")
        review = next(p for p in workflow.phases if p.id == "recap-review")
        assert review.type == "check"
        assert review.bounce_target == "device-recon"

    def test_exploit_sim_runtime_placeholders_survive_var_rendering(self):
        """Jinja renders these prompts before the runner substitutes its own
        single-brace placeholders. A stray `{{ }}` or a Jinja-swallowed brace
        would strip them and the exploit-sim roles would run blind."""
        workflow, config = self._analysis()
        sim = config.exploit_sim
        assert sim is not None and sim.enabled

        required = {
            sim.env_builder.prompt: ["{env_dir}"],
            sim.simulator.prompt: ["{round}", "{max_rounds}", "{env_brief}", "{claim_packet}", "{attacker_last}"],
            sim.attacker.prompt: ["{round}", "{max_rounds}", "{claim_packet}", "{simulator_last}"],
            sim.judge.prompt: ["{claim_packet}", "{config_deltas}", "{transcript}"],
        }
        for source, placeholders in required.items():
            rendered = apply_vars(source, workflow.vars)
            # `{{ x }}` for an undeclared `x` survives rendering as `{{x}}`, whose
            # inner substring would satisfy a naive `in` check while breaking the
            # runner's `.replace("{x}", ...)` into `{value}`. Reject the wrapper
            # form before checking for the placeholder itself.
            assert "{{" not in rendered, "placeholder was written as a Jinja expression"
            for placeholder in placeholders:
                assert placeholder in rendered, f"runner placeholder {placeholder} lost during rendering"
            assert workflow.vars["TARGET_DEVICE"] in rendered

    def test_analyst_runtime_placeholders_survive_var_rendering(self):
        workflow, config = self._analysis()

        rendered = apply_vars(config.analyst.prompt, workflow.vars)
        assert "{mission}" in rendered
        assert "{working_dir}" in rendered

    def test_continue_nudge_accepts_engine_format_fields(self):
        """The runner formats this template with exactly these fields; an
        unescaped brace elsewhere in the text would raise and silently fall
        back to the unformatted template."""
        _, config = self._analysis()

        rendered = config.continue_nudge.format(
            turns=1,
            terminal=2,
            min_captain_turns=config.min_captain_turns,
            min_terminal_targets=config.min_terminal_targets_before_complete,
            max_premature_completes=config.max_premature_completes,
            consecutive=1,
        )
        assert str(config.min_captain_turns) in rendered


class TestTimeoutField:
    def test_timeout_in_yaml(self, tmp_path):
        yaml_content = """\
name: test
phases:
  - id: build
    prompt: "Build it."
    timeout: 120
  - id: check
    type: check
    prompt: "Review the build and emit VERDICT."
    timeout: 30
"""
        yaml_path = tmp_path / "workflow.yaml"
        yaml_path.write_text(yaml_content)
        from juvenal.workflow import load_workflow

        wf = load_workflow(yaml_path)
        assert wf.phases[0].timeout == 120
        assert wf.phases[1].timeout == 30

    def test_timeout_default_none(self):
        phase = Phase(id="test", prompt="Test.")
        assert phase.timeout is None

    def test_timeout_in_validation(self):
        """Timeout field shouldn't cause validation errors."""
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="build", type="implement", prompt="Build.", timeout=60),
            ],
        )
        assert validate_workflow(wf) == []


class TestEnvField:
    def test_env_in_yaml(self, tmp_path):
        yaml_content = """\
name: test
phases:
  - id: build
    prompt: "Build it."
    env:
      NODE_ENV: production
      DEBUG: "true"
"""
        yaml_path = tmp_path / "workflow.yaml"
        yaml_path.write_text(yaml_content)
        from juvenal.workflow import load_workflow

        wf = load_workflow(yaml_path)
        assert wf.phases[0].env == {"NODE_ENV": "production", "DEBUG": "true"}

    def test_env_default_empty(self):
        phase = Phase(id="test", prompt="Test.")
        assert phase.env == {}

    def test_env_in_check_phase(self):
        """Check phases remain valid with env metadata."""
        phase = Phase(
            id="review",
            type="check",
            prompt=make_command_check_prompt("echo $TEST_VAR"),
            env={"TEST_VAR": "hello123"},
        )
        assert phase.env == {"TEST_VAR": "hello123"}


class TestWorkflowPhaseValidation:
    def test_workflow_phase_with_prompt_is_valid(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="dynamic", type="workflow", prompt="Build a REST API."),
            ],
        )
        assert validate_workflow(wf) == []

    def test_workflow_phase_missing_prompt(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="dynamic", type="workflow"),
            ],
        )
        errors = validate_workflow(wf)
        assert any("workflow phase needs prompt, workflow_file, or workflow_dir" in e for e in errors)

    def test_workflow_phase_with_role_is_invalid(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="dynamic", type="workflow", prompt="Do it.", role="tester"),
            ],
        )
        errors = validate_workflow(wf)
        assert any("must not have 'role'" in e for e in errors)

    def test_max_depth_less_than_1_is_invalid(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="dynamic", type="workflow", prompt="Do it.", max_depth=0),
            ],
        )
        errors = validate_workflow(wf)
        assert any("max_depth must be >= 1" in e for e in errors)

    def test_max_depth_negative_is_invalid(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="dynamic", type="workflow", prompt="Do it.", max_depth=-1),
            ],
        )
        errors = validate_workflow(wf)
        assert any("max_depth must be >= 1" in e for e in errors)

    def test_max_depth_valid(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="dynamic", type="workflow", prompt="Do it.", max_depth=2),
            ],
        )
        assert validate_workflow(wf) == []

    def test_max_depth_on_non_workflow_phase_invalid(self):
        """max_depth < 1 is invalid regardless of phase type."""
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="setup", type="implement", prompt="Do it.", max_depth=0),
            ],
        )
        errors = validate_workflow(wf)
        assert any("max_depth must be >= 1" in e for e in errors)

    def test_workflow_file_valid(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="sub", type="workflow", workflow_file="/some/path.yaml")],
        )
        assert validate_workflow(wf) == []

    def test_workflow_dir_valid(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="sub", type="workflow", workflow_dir="/some/dir")],
        )
        assert validate_workflow(wf) == []

    def test_workflow_file_and_dir_both_invalid(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="sub", type="workflow", workflow_file="a.yaml", workflow_dir="b/")],
        )
        errors = validate_workflow(wf)
        assert any("mutually exclusive" in e for e in errors)

    def test_workflow_file_on_non_workflow_invalid(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="Build.", workflow_file="sub.yaml")],
        )
        errors = validate_workflow(wf)
        assert any("only allowed on workflow phases" in e for e in errors)


class TestTemplateVarValidation:
    def test_undefined_var_in_prompt(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="Build {{PROJECT}}.")],
        )
        errors = validate_workflow(wf)
        assert any("PROJECT" in e and "no value defined" in e for e in errors)

    def test_undefined_var_in_jinja_control_block(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="{% if PROJECT %}Build it.{% endif %}")],
        )
        errors = validate_workflow(wf)
        assert any("PROJECT" in e and "no value defined" in e for e in errors)

    def test_undefined_var_in_check_prompt(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="build", type="implement", prompt="Build."),
                Phase(
                    id="test",
                    type="check",
                    prompt=make_command_check_prompt("pytest {{DIR}}"),
                    bounce_target="build",
                ),
            ],
        )
        errors = validate_workflow(wf)
        assert any("DIR" in e and "no value defined" in e for e in errors)

    def test_defined_var_passes(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="Build {{PROJECT}}.")],
            vars={"PROJECT": "myapp"},
        )
        errors = validate_workflow(wf)
        assert not any("no value defined" in e for e in errors)

    def test_multiple_undefined_vars(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="Deploy {{APP}} to {{ENV}}.")],
        )
        errors = validate_workflow(wf)
        undefined = [e for e in errors if "no value defined" in e]
        assert len(undefined) == 2

    def test_some_defined_some_not(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="Deploy {{APP}} to {{ENV}}.")],
            vars={"APP": "myservice"},
        )
        errors = validate_workflow(wf)
        undefined = [e for e in errors if "no value defined" in e]
        assert len(undefined) == 1
        assert "ENV" in undefined[0]

    def test_no_vars_no_placeholders_passes(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="Build it.")],
        )
        assert validate_workflow(wf) == []

    def test_duplicate_var_references_single_error(self):
        """Same undefined var referenced multiple times only produces one error per phase."""
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="{{X}} and {{X}} and {{X}}.")],
        )
        errors = validate_workflow(wf)
        undefined = [e for e in errors if "no value defined" in e]
        assert len(undefined) == 1

    def test_invalid_jinja_syntax_in_prompt(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="{{ PROJECT")],
        )
        errors = validate_workflow(wf)
        assert any("invalid Jinja2 prompt" in e for e in errors)

    def test_builtin_jinja_globals_are_not_treated_as_defined(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="{{ cycler }}")],
        )
        errors = validate_workflow(wf)
        assert any("{{cycler}}" in e and "no value defined" in e for e in errors)

    def test_default_filter_allows_undefined_var(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt='{{ missing|default("fallback") }}')],
        )
        assert validate_workflow(wf) == []

    def test_defined_test_allows_guarded_undefined_var(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="{% if missing is defined %}{{ missing }}{% endif %}")],
        )
        assert validate_workflow(wf) == []

    def test_short_circuit_defined_guard_allows_nested_access(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="build", type="implement", prompt="{% if missing is defined and missing.foo %}x{% endif %}")
            ],
        )
        assert validate_workflow(wf) == []

    def test_elif_branch_missing_var_is_still_validated(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="{% if ok %}A{% elif missing %}B{% endif %}")],
            vars={"ok": False},
        )
        errors = validate_workflow(wf)
        assert any("{{missing}}" in e and "no value defined" in e for e in errors)

    def test_unreachable_else_branch_missing_var_is_ignored(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="{% if ok %}A{% else %}{{ missing }}{% endif %}")],
            vars={"ok": True},
        )
        assert validate_workflow(wf) == []

    def test_unreachable_else_branch_missing_nested_var_is_ignored(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="{% if ok %}A{% else %}{{ missing.foo }}{% endif %}")],
            vars={"ok": True},
        )
        assert validate_workflow(wf) == []

    def test_unreachable_else_branch_missing_var_defers_to_render_error(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="build", type="implement", prompt="{% if ok %}A{% else %}{{ missing }}{% endif %} {{ 1 / 0 }}")
            ],
            vars={"ok": True},
        )
        errors = validate_workflow(wf)
        assert any("Jinja2 render error in prompt for phase 'build'" in e for e in errors)
        assert not any("{{missing}}" in e and "no value defined" in e for e in errors)

    def test_validate_workflow_reports_render_error(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="build", type="implement", prompt="{{ 1 / 0 }}")],
        )
        errors = validate_workflow(wf)
        assert any("Jinja2 render error in prompt for phase 'build'" in e for e in errors)

    def test_validate_workflow_reports_render_error_for_check_prompt_with_role(self):
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="review", type="check", role="tester", prompt="{{ 1 / 0 }}"),
            ],
        )
        errors = validate_workflow(wf)
        assert any("Jinja2 render error in checker prompt for phase 'review'" in e for e in errors)

    def test_expand_multi_vars_preserves_filtered_var_name_for_validation(self):
        wf = Workflow(
            name="test",
            phases=[Phase(id="deploy", type="implement", prompt="Deploy {{ app|title }} to {{ ENV }}.")],
            vars={"App": "svc"},
        )
        expanded = expand_multi_vars(wf, {"ENV": ["prod"]})
        errors = validate_workflow(expanded)
        assert any("{{app}}" in e and "no value defined" in e for e in errors)


class TestLaneValidation:
    def test_lane_phase_existence(self):
        """Lane phase IDs must exist in the workflow."""
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="a", type="implement", prompt="A."),
            ],
            parallel_groups=[ParallelGroup(lanes=[["a", "nonexistent"]])],
        )
        errors = validate_workflow(wf)
        assert any("nonexistent" in e for e in errors)

    def test_lane_bounce_target_containment(self):
        """Bounce targets in a lane must stay within that lane."""
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="a", type="implement", prompt="A."),
                Phase(id="check_a", type="check", role="tester", bounce_target="b"),
                Phase(id="b", type="implement", prompt="B."),
                Phase(id="check_b", type="check", role="tester", bounce_target="b"),
            ],
            parallel_groups=[ParallelGroup(lanes=[["a", "check_a"], ["b", "check_b"]])],
        )
        errors = validate_workflow(wf)
        assert any("outside its lane" in e for e in errors)

    def test_lane_empty(self):
        """Empty lanes are invalid."""
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="a", type="implement", prompt="A."),
            ],
            parallel_groups=[ParallelGroup(lanes=[["a"], []])],
        )
        errors = validate_workflow(wf)
        assert any("empty" in e for e in errors)

    def test_lane_duplicate_phase(self):
        """A phase cannot appear in multiple lanes."""
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="a", type="implement", prompt="A."),
                Phase(id="b", type="implement", prompt="B."),
            ],
            parallel_groups=[ParallelGroup(lanes=[["a", "b"], ["b"]])],
        )
        errors = validate_workflow(wf)
        assert any("multiple lanes" in e for e in errors)

    def test_lane_allows_workflow_type(self):
        """Lane groups may contain workflow phases."""
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="a", type="implement", prompt="A."),
                Phase(id="dyn", type="workflow", prompt="Dynamic."),
            ],
            parallel_groups=[ParallelGroup(lanes=[["a", "dyn"]])],
        )
        assert validate_workflow(wf) == []

    def test_expanded_workflow_phase_lane_group_is_valid(self):
        """Multi-var expansion must not manufacture an invalid lane group for workflow phases."""
        wf = Workflow(
            name="test",
            phases=[Phase(id="dyn", type="workflow", prompt="Plan {{ENV}}.")],
        )
        expanded = expand_multi_vars(wf, {"ENV": ["prod"]})
        assert validate_workflow(expanded) == []

    def test_valid_lane_group(self):
        """A valid lane group passes validation."""
        wf = Workflow(
            name="test",
            phases=[
                Phase(id="a", type="implement", prompt="A."),
                Phase(id="check_a", type="check", role="tester", bounce_target="a"),
                Phase(id="b", type="implement", prompt="B."),
                Phase(id="check_b", type="check", role="tester", bounce_target="b"),
            ],
            parallel_groups=[ParallelGroup(lanes=[["a", "check_a"], ["b", "check_b"]])],
        )
        assert validate_workflow(wf) == []


class TestValidateCLI:
    def test_validate_command_parsing(self):
        parser = build_parser()
        args = parser.parse_args(["validate", "workflow.yaml"])
        assert args.command == "validate"
        assert args.workflow == "workflow.yaml"

    def test_validate_accepts_run_flags(self):
        parser = build_parser()
        args = parser.parse_args(["validate", "workflow.yaml", "-D", "ENV=prod", "--checker", "tester"])
        assert args.defines == ["ENV=prod"]
        assert args.checker == ["tester"]

    def test_validate_valid_workflow(self, sample_yaml, capsys):
        parser = build_parser()
        args = parser.parse_args(["validate", str(sample_yaml)])
        args.plain = True
        result = cmd_validate(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "Validation: OK" in captured.out

    def test_validate_shows_execution_plan(self, sample_yaml, capsys):
        parser = build_parser()
        args = parser.parse_args(["validate", str(sample_yaml)])
        args.plain = True
        cmd_validate(args)
        captured = capsys.readouterr()
        assert "Execution plan:" in captured.out
        assert "Phase summary:" in captured.out

    def test_validate_surfaces_analysis_config(self, tmp_path, capsys):
        yaml_content = """\
name: analysis-docs
phases:
  - id: analyze
    type: analysis
    prompt: "Analyze {{TARGET}} for security findings."
    analysis:
      max_workers: 2
      max_verifiers: 3
      interaction_timeout: 1.5
"""
        yaml_path = tmp_path / "analysis.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path), "-D", "TARGET=src/"])
        args.plain = True

        assert cmd_validate(args) == 0
        captured = capsys.readouterr()
        assert "[analysis] analyze" in captured.out
        assert "captain: claude" in captured.out
        assert "worker:  claude" in captured.out
        assert "verifier_backend: claude" in captured.out
        assert "max_workers: 2" in captured.out
        assert "max_verifiers: 3" in captured.out
        assert "interaction_timeout: 1.5s" in captured.out

    def test_validate_undefined_template_var(self, tmp_path, capsys):
        yaml_content = """\
name: test
phases:
  - id: deploy
    prompt: "Deploy to {{ENV}} in {{REGION}}."
"""
        yaml_path = tmp_path / "bad.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path)])
        args.plain = True
        result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "ENV" in captured.out
        assert "REGION" in captured.out

    def test_validate_with_defines_resolves_vars(self, tmp_path, capsys):
        yaml_content = """\
name: test
phases:
  - id: deploy
    prompt: "Deploy to {{ENV}}."
"""
        yaml_path = tmp_path / "ok.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path), "-D", "ENV=prod"])
        args.plain = True
        result = cmd_validate(args)
        assert result == 0

    def test_validate_invalid_workflow(self, tmp_path, capsys):
        yaml_content = """\
name: bad
phases:
  - id: a
    type: invalid
    prompt: "whatever"
"""
        yaml_path = tmp_path / "bad.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path)])
        args.plain = True
        result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "error" in captured.out

    def test_validate_invalid_jinja_syntax_clean_error(self, tmp_path, capsys):
        """Invalid Jinja syntax prints a clean validation error, no traceback."""
        yaml_content = """\
name: bad
phases:
  - id: build
    prompt: "{{ PROJECT"
"""
        yaml_path = tmp_path / "bad-jinja.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path)])
        args.plain = True
        result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "invalid Jinja2 prompt" in captured.out
        assert "Traceback" not in captured.out

    def test_validate_jinja_render_error_clean_error(self, tmp_path, capsys):
        """Render-time Jinja errors print a clean validation error, no traceback."""
        yaml_content = """\
name: bad
phases:
  - id: build
    prompt: "{{ 1 / 0 }}"
"""
        yaml_path = tmp_path / "bad-jinja-runtime.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path)])
        args.plain = True
        result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "Jinja2 render error in prompt for phase 'build'" in captured.out
        assert "Traceback" not in captured.out

    def test_validate_check_prompt_with_role_jinja_render_error_clean_error(self, tmp_path, capsys):
        """Role-backed check prompts still surface render-time Jinja errors during validation."""
        yaml_content = """\
name: bad
phases:
  - id: review
    type: check
    role: tester
    prompt: "{{ 1 / 0 }}"
"""
        yaml_path = tmp_path / "bad-check-jinja-runtime.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path)])
        args.plain = True
        result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "Jinja2 render error in checker prompt for phase 'review'" in captured.out
        assert "Traceback" not in captured.out

    def test_validate_nested_lookup_missing_clean_error(self, tmp_path, capsys):
        """Missing nested lookups print a clean validation error, no traceback."""
        yaml_content = """\
name: bad
vars:
  config: {}
phases:
  - id: build
    prompt: "{{ config.env }}"
"""
        yaml_path = tmp_path / "bad-jinja-nested.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path)])
        args.plain = True
        result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "Jinja2 render error in prompt for phase 'build'" in captured.out
        assert "env" in captured.out
        assert "Traceback" not in captured.out

    def test_validate_unreachable_missing_with_render_error_reports_render_error(self, tmp_path, capsys):
        """A real render failure should win over undefined vars from unreachable branches."""
        yaml_content = """\
name: bad
vars:
  ok: true
phases:
  - id: build
    prompt: "{% if ok %}A{% else %}{{ missing }}{% endif %} {{ 1 / 0 }}"
"""
        yaml_path = tmp_path / "bad-jinja-mixed.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path)])
        args.plain = True
        result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "Jinja2 render error in prompt for phase 'build'" in captured.out
        assert "{{missing}}" not in captured.out
        assert "Traceback" not in captured.out

    def test_validate_unreachable_nested_missing_var_is_ignored(self, tmp_path, capsys):
        yaml_content = """\
name: ok
vars:
  ok: true
phases:
  - id: build
    prompt: "{% if ok %}A{% else %}{{ missing.foo }}{% endif %}"
"""
        yaml_path = tmp_path / "ok-jinja-dead-nested.yaml"
        yaml_path.write_text(yaml_content)
        parser = build_parser()
        args = parser.parse_args(["validate", str(yaml_path)])
        args.plain = True
        result = cmd_validate(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "Validation: OK" in captured.out
        assert "{{missing}}" not in captured.out
        assert "Traceback" not in captured.out

    def test_validate_missing_id_clean_error(self, tmp_path, capsys):
        """Missing phase ID prints a clean error, no stack trace."""
        yaml_content = """\
name: test
phases:
  - prompt: "no id here"
"""
        yaml_path = tmp_path / "bad.yaml"
        yaml_path.write_text(yaml_content)
        with pytest.raises(SystemExit) as exc_info:
            cmd_validate(
                argparse.Namespace(
                    workflow=str(yaml_path),
                    plain=True,
                    defines=[],
                    checker=[],
                    implementer=None,
                    backend="codex",
                    max_bounces=999,
                    working_dir=None,
                    backoff=None,
                    notify=[],
                )
            )
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "missing required 'id' field" in captured.out
        assert "Traceback" not in captured.out

    def test_validate_nonexistent_file_clean_error(self, capsys):
        """Nonexistent workflow file prints a clean error."""
        with pytest.raises(SystemExit) as exc_info:
            cmd_validate(
                argparse.Namespace(
                    workflow="/nonexistent/workflow.yaml",
                    plain=True,
                    defines=[],
                    checker=[],
                    implementer=None,
                    backend="codex",
                    max_bounces=999,
                    working_dir=None,
                    backoff=None,
                    notify=[],
                )
            )
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error:" in captured.out
