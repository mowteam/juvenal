"""Unit tests for state persistence."""

import json
from pathlib import Path

from juvenal.state import PipelineState
from juvenal.workflow import Phase


class TestAtomicPersistence:
    def test_save_and_load(self, tmp_path):
        state_file = tmp_path / "state.json"
        state = PipelineState(state_file=state_file)
        state.set_attempt("setup", 1)
        state.mark_completed("setup")

        loaded = PipelineState.load(state_file)
        assert "setup" in loaded.phases
        assert loaded.phases["setup"].status == "completed"
        assert loaded.phases["setup"].attempt == 1

    def test_atomic_write_creates_no_tmp(self, tmp_path):
        state_file = tmp_path / "state.json"
        state = PipelineState(state_file=state_file)
        state.set_attempt("phase1", 1)

        # After save, there should be no .tmp file
        tmp_file = state_file.with_name(f"{state_file.name}.tmp")
        assert not tmp_file.exists()
        assert state_file.exists()

    def test_save_produces_valid_json(self, tmp_path):
        state_file = tmp_path / "state.json"
        state = PipelineState(state_file=state_file)
        state.set_attempt("setup", 1)
        state.log_step("setup", 1, "implement", "some output")

        data = json.loads(state_file.read_text())
        assert "phases" in data
        assert "setup" in data["phases"]
        assert data["phases"]["setup"]["attempt"] == 1


class TestResumeLogic:
    def test_resume_from_beginning(self, tmp_path):
        state = PipelineState(state_file=tmp_path / "state.json")
        phases = [Phase(id="a", prompt=""), Phase(id="b", prompt="")]
        assert state.get_resume_phase_index(phases) == 0

    def test_resume_after_first_completed(self, tmp_path):
        state = PipelineState(state_file=tmp_path / "state.json")
        state.set_attempt("a", 1)
        state.mark_completed("a")
        phases = [Phase(id="a", prompt=""), Phase(id="b", prompt="")]
        assert state.get_resume_phase_index(phases) == 1

    def test_resume_all_completed(self, tmp_path):
        state = PipelineState(state_file=tmp_path / "state.json")
        state.mark_completed("a")
        state.mark_completed("b")
        phases = [Phase(id="a", prompt=""), Phase(id="b", prompt="")]
        assert state.get_resume_phase_index(phases) == 2


class TestFailureContext:
    def test_set_and_get_failure_context(self, tmp_path):
        state = PipelineState(state_file=tmp_path / "state.json")
        state.set_failure_context("phase1", "tests failed", attempt=1)
        assert state.get_failure_context("phase1") == "tests failed"

    def test_failure_contexts_are_per_attempt(self, tmp_path):
        state = PipelineState(state_file=tmp_path / "state.json")
        state.set_failure_context("phase1", "first failure", attempt=1)
        state.set_failure_context("phase1", "second failure", attempt=2)
        ps = state.phases["phase1"]
        assert len(ps.failure_contexts) == 2
        assert ps.failure_contexts[0]["context"] == "first failure"
        assert ps.failure_contexts[0]["attempt"] == 1
        assert ps.failure_contexts[1]["context"] == "second failure"
        assert ps.failure_contexts[1]["attempt"] == 2
        # get_failure_context returns latest
        assert state.get_failure_context("phase1") == "second failure"

    def test_failure_contexts_persist_across_save_load(self, tmp_path):
        state = PipelineState(state_file=tmp_path / "state.json")
        state.set_failure_context("phase1", "fail one", attempt=1)
        state.set_failure_context("phase1", "fail two", attempt=2)
        loaded = PipelineState.load(tmp_path / "state.json")
        ps = loaded.phases["phase1"]
        assert len(ps.failure_contexts) == 2
        assert ps.failure_contexts[0]["context"] == "fail one"
        assert ps.failure_contexts[1]["context"] == "fail two"

    def test_get_nonexistent_phase(self, tmp_path):
        state = PipelineState(state_file=tmp_path / "state.json")
        assert state.get_failure_context("nonexistent") == ""

    def test_backwards_compat_scalar_failure_context(self, tmp_path):
        """Old state files with scalar failure_context are migrated on load."""
        import json

        state_file = tmp_path / "state.json"
        state_file.write_text(
            json.dumps(
                {
                    "started_at": None,
                    "completed_at": None,
                    "phases": {
                        "build": {
                            "status": "pending",
                            "attempt": 1,
                            "failure_context": "old failure",
                            "logs": [],
                        }
                    },
                }
            )
        )
        loaded = PipelineState.load(state_file)
        assert loaded.get_failure_context("build") == "old failure"
        assert len(loaded.phases["build"].failure_contexts) == 1


class TestInvalidation:
    def test_invalidate_from(self, tmp_path):
        state = PipelineState(state_file=tmp_path / "state.json")
        state.mark_completed("a")
        state.mark_completed("b")
        state.mark_completed("c")
        state.invalidate_from("b")

        assert state.phases["a"].status == "completed"
        assert state.phases["b"].status == "pending"
        assert state.phases["c"].status == "pending"

    def test_invalidate_preserves_attempt_count(self, tmp_path):
        """invalidate_from should not reset the attempt counter."""
        state = PipelineState(state_file=tmp_path / "state.json")
        state.set_attempt("a", 1)
        state.mark_completed("a")
        state.set_attempt("b", 2)
        state.mark_completed("b")
        state.invalidate_from("b")

        assert state.phases["a"].attempt == 1  # untouched
        assert state.phases["b"].attempt == 2  # preserved through invalidation


class TestBaselineSha:
    def test_baseline_sha_default_none(self, tmp_path):
        state = PipelineState(state_file=tmp_path / "state.json")
        ps = state._ensure_phase("setup")
        assert ps.baseline_sha is None

    def test_baseline_sha_persisted(self, tmp_path):
        state_file = tmp_path / "state.json"
        state = PipelineState(state_file=state_file)
        ps = state._ensure_phase("setup")
        ps.baseline_sha = "abc123"
        state.save()

        loaded = PipelineState.load(state_file)
        assert loaded.phases["setup"].baseline_sha == "abc123"

    def test_invalidate_preserves_baseline_sha(self, tmp_path):
        """invalidate_from should not reset baseline_sha."""
        state = PipelineState(state_file=tmp_path / "state.json")
        ps_a = state._ensure_phase("a")
        ps_a.baseline_sha = "sha-a"
        state.mark_completed("a")
        ps_b = state._ensure_phase("b")
        ps_b.baseline_sha = "sha-b"
        state.mark_completed("b")
        state.invalidate_from("b")

        assert state.phases["a"].baseline_sha == "sha-a"  # untouched
        assert state.phases["b"].baseline_sha == "sha-b"  # preserved through invalidation
        assert state.phases["b"].status == "pending"


class TestLoadEmpty:
    def test_load_nonexistent(self, tmp_path):
        state = PipelineState.load(tmp_path / "nonexistent.json")
        assert len(state.phases) == 0

    def test_load_none(self):
        state = PipelineState.load(None)
        assert state.state_file == Path(".juvenal-state.json")
