"""Unit tests for workflow loading."""

import pytest

from juvenal.workflow import load_workflow


class TestYAMLLoading:
    def test_load_basic_yaml(self, sample_yaml):
        wf = load_workflow(sample_yaml)
        assert wf.name == "test-workflow"
        assert wf.backend == "claude"
        assert wf.max_retries == 3
        assert len(wf.phases) == 2

    def test_yaml_phases(self, sample_yaml):
        wf = load_workflow(sample_yaml)
        assert wf.phases[0].id == "setup"
        assert wf.phases[0].prompt == "Set up the project scaffolding."
        assert len(wf.phases[0].checkers) == 1
        assert wf.phases[0].checkers[0].type == "script"

    def test_yaml_bounce_targets(self, sample_yaml):
        wf = load_workflow(sample_yaml)
        assert wf.bounce_targets == {"implement": "setup"}

    def test_yaml_checker_types(self, sample_yaml):
        wf = load_workflow(sample_yaml)
        impl = wf.phases[1]
        assert len(impl.checkers) == 2
        assert impl.checkers[0].type == "script"
        assert impl.checkers[1].type == "agent"
        assert impl.checkers[1].role == "tester"


class TestDirectoryLoading:
    def test_load_directory(self, tmp_workflow):
        wf = load_workflow(tmp_workflow)
        assert len(wf.phases) == 2
        assert wf.phases[0].id == "01-setup"
        assert wf.phases[1].id == "02-implement"

    def test_directory_checkers(self, tmp_workflow):
        wf = load_workflow(tmp_workflow)
        # Phase 1: only a .sh checker
        assert len(wf.phases[0].checkers) == 1
        assert wf.phases[0].checkers[0].type == "script"

        # Phase 2: paired .sh + .md = composite
        assert len(wf.phases[1].checkers) == 1
        assert wf.phases[1].checkers[0].type == "composite"

    def test_directory_prompts(self, tmp_workflow):
        wf = load_workflow(tmp_workflow)
        assert wf.phases[0].prompt == "Set up the project."


class TestBareFileLoading:
    def test_load_bare_md(self, bare_md):
        wf = load_workflow(bare_md)
        assert len(wf.phases) == 1
        assert wf.phases[0].id == "task"
        assert wf.phases[0].prompt == "Implement a hello world program."
        assert len(wf.phases[0].checkers) == 1
        assert wf.phases[0].checkers[0].type == "agent"
        assert wf.phases[0].checkers[0].role == "tester"


class TestPhaseRendering:
    def test_render_prompt_no_failure(self, sample_yaml):
        wf = load_workflow(sample_yaml)
        rendered = wf.phases[0].render_prompt()
        assert rendered == "Set up the project scaffolding."

    def test_render_prompt_with_failure(self, sample_yaml):
        wf = load_workflow(sample_yaml)
        rendered = wf.phases[0].render_prompt(failure_context="Tests failed")
        assert "IMPORTANT: A previous attempt failed verification" in rendered
        assert "Tests failed" in rendered


class TestErrors:
    def test_nonexistent_path(self):
        with pytest.raises(FileNotFoundError):
            load_workflow("/nonexistent/path")

    def test_unsupported_file_type(self, tmp_path):
        bad_file = tmp_path / "workflow.txt"
        bad_file.write_text("hello")
        with pytest.raises(ValueError, match="Unsupported file type"):
            load_workflow(bad_file)

    def test_yaml_with_string_content(self, tmp_path):
        """YAML that parses to a string instead of a dict should raise."""
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text("just a string\n")
        with pytest.raises(ValueError, match="expected a mapping"):
            load_workflow(bad_yaml)

    def test_yaml_with_list_content(self, tmp_path):
        """YAML that parses to a list instead of a dict should raise."""
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text("- item1\n- item2\n")
        with pytest.raises(ValueError, match="expected a mapping"):
            load_workflow(bad_yaml)

    def test_yaml_empty(self, tmp_path):
        """Empty YAML file should raise."""
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text("")
        with pytest.raises(ValueError, match="expected a mapping"):
            load_workflow(bad_yaml)
