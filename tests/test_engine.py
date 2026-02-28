"""Unit tests for the execution engine with mocked backend."""

from juvenal.checkers import parse_verdict
from juvenal.engine import Engine
from juvenal.workflow import Checker, Phase, Workflow
from tests.conftest import MockBackend


class TestVerdictParsing:
    def test_pass(self):
        assert parse_verdict("some output\nVERDICT: PASS") == (True, "")

    def test_fail_with_reason(self):
        passed, reason = parse_verdict("output\nVERDICT: FAIL: tests broken")
        assert not passed
        assert reason == "tests broken"

    def test_fail_without_reason(self):
        passed, reason = parse_verdict("VERDICT: FAIL")
        assert not passed
        assert reason == "unspecified"

    def test_no_verdict(self):
        passed, reason = parse_verdict("no verdict here")
        assert not passed
        assert "did not emit a VERDICT" in reason

    def test_verdict_scan_backwards(self):
        """Should find the last VERDICT line."""
        output = "VERDICT: FAIL: old\nmore stuff\nVERDICT: PASS"
        assert parse_verdict(output) == (True, "")


class TestEngineWithMockedBackend:
    def _make_engine(self, workflow, backend, tmp_path, **kwargs):
        """Create an engine with injected mock backend."""
        engine = Engine(workflow, state_file=str(tmp_path / "state.json"), **kwargs)
        engine.backend = backend
        return engine

    def test_single_phase_pass(self, tmp_path):
        backend = MockBackend()
        backend.add_response(exit_code=0, output="done")  # implement
        workflow = Workflow(
            name="test",
            phases=[Phase(id="setup", prompt="Do it.", checkers=[Checker(name="check", type="script", run="true")])],
            max_retries=3,
        )
        engine = self._make_engine(workflow, backend, tmp_path)
        assert engine.run() == 0

    def test_implementation_crash_retries(self, tmp_path):
        backend = MockBackend()
        backend.add_response(exit_code=1, output="crash")  # attempt 1 crashes
        backend.add_response(exit_code=0, output="done")  # attempt 2 succeeds
        workflow = Workflow(
            name="test",
            phases=[Phase(id="setup", prompt="Do it.", checkers=[Checker(name="check", type="script", run="true")])],
            max_retries=3,
        )
        engine = self._make_engine(workflow, backend, tmp_path)
        assert engine.run() == 0

    def test_script_checker_failure_retries(self, tmp_path):
        backend = MockBackend()
        backend.add_response(exit_code=0, output="done")  # attempt 1 implement
        backend.add_response(exit_code=0, output="done")  # attempt 2 implement
        workflow = Workflow(
            name="test",
            phases=[
                Phase(
                    id="setup",
                    prompt="Do it.",
                    checkers=[Checker(name="check", type="script", run="false")],  # always fails
                )
            ],
            max_retries=2,
        )
        engine = self._make_engine(workflow, backend, tmp_path)
        assert engine.run() == 1  # exhausted

    def test_agent_checker_pass(self, tmp_path):
        backend = MockBackend()
        backend.add_response(exit_code=0, output="implemented")  # implement
        backend.add_response(exit_code=0, output="looks good\nVERDICT: PASS")  # checker
        workflow = Workflow(
            name="test",
            phases=[Phase(id="setup", prompt="Do it.", checkers=[Checker(name="tester", type="agent", role="tester")])],
            max_retries=3,
        )
        engine = self._make_engine(workflow, backend, tmp_path)
        assert engine.run() == 0

    def test_multi_phase(self, tmp_path):
        backend = MockBackend()
        # Phase 1: implement + check pass
        backend.add_response(exit_code=0, output="phase1 done")
        # Phase 2: implement + check pass
        backend.add_response(exit_code=0, output="phase2 done")
        workflow = Workflow(
            name="test",
            phases=[
                Phase(id="phase1", prompt="Do phase 1.", checkers=[Checker(name="c1", type="script", run="true")]),
                Phase(id="phase2", prompt="Do phase 2.", checkers=[Checker(name="c2", type="script", run="true")]),
            ],
            max_retries=3,
        )
        engine = self._make_engine(workflow, backend, tmp_path)
        assert engine.run() == 0

    def test_dry_run(self, tmp_path, capsys):
        workflow = Workflow(
            name="test",
            phases=[Phase(id="setup", prompt="Do the thing.", checkers=[Checker(name="c", type="script", run="true")])],
        )
        engine = self._make_engine(workflow, MockBackend(), tmp_path, dry_run=True)
        assert engine.run() == 0
        captured = capsys.readouterr()
        assert "test" in captured.out
        assert "setup" in captured.out
