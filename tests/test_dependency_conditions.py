"""Tests for ghanse/wkmigrate#44: Support Completed and Failed dependency conditions.

ADF supports four dependency conditions: Succeeded, Completed, Failed, Skipped.
Only Succeeded is currently handled — Completed and Failed cause _parse_dependency()
to return UnsupportedValue, which crashes the preparer with:
    AttributeError: 'UnsupportedValue' object has no attribute 'task_key'

These tests verify that pipelines with Completed and Failed dependency conditions
translate and prepare without crashing.

Fixture JSONs are under tests/resources/json/camel/dependency_conditions/.
"""

from __future__ import annotations

import os

import pytest

from wkmigrate.definition_stores.json_factory_definition_store import (
    JsonFactoryDefinitionStore,
)
from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    Pipeline,
)
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.preparers.preparer import prepare_workflow

_FIXTURE_DIR = os.path.join(
    os.path.dirname(__file__), "resources", "json", "camel", "dependency_conditions"
)


def _load(pipeline_name: str) -> Pipeline:
    store = JsonFactoryDefinitionStore(
        definition_dir=_FIXTURE_DIR,
        source_property_case="camel",
    )
    return store.load(pipeline_name)


# ============================================================================
# Completed condition
# ============================================================================


class TestCompletedCondition:
    """A task depending on another with condition 'Completed' should translate
    and prepare without crashing."""

    @pytest.fixture
    def pipeline(self) -> Pipeline:
        return _load("completed_condition")

    def test_pipeline_translates(self, pipeline: Pipeline) -> None:
        assert len(pipeline.tasks) == 2

    def test_dependency_is_not_unsupported(self, pipeline: Pipeline) -> None:
        cleanup = next(t for t in pipeline.tasks if t.name == "cleanup")
        assert cleanup.depends_on is not None
        assert len(cleanup.depends_on) == 1
        dep = cleanup.depends_on[0]
        assert not isinstance(dep, UnsupportedValue), (
            "Dependency with 'Completed' condition returned UnsupportedValue. "
            "See ghanse/wkmigrate#44."
        )
        assert dep.task_key == "run_process"

    def test_run_if_is_all_done(self, pipeline: Pipeline) -> None:
        """Completed condition should map to run_if=ALL_DONE in the prepared task."""
        prepared = prepare_workflow(pipeline)
        cleanup = next(t for t in prepared.tasks if t["task_key"] == "cleanup")
        assert cleanup.get("run_if") == "ALL_DONE", (
            f"Expected run_if='ALL_DONE' for Completed dependency, "
            f"got '{cleanup.get('run_if')}'. See ghanse/wkmigrate#44."
        )

    def test_preparer_does_not_crash(self, pipeline: Pipeline) -> None:
        prepared = prepare_workflow(pipeline)
        assert len(prepared.tasks) == 2
        task_keys = {t["task_key"] for t in prepared.tasks}
        assert "cleanup" in task_keys


# ============================================================================
# Failed condition
# ============================================================================


class TestFailedCondition:
    """A task depending on another with condition 'Failed' should translate
    and prepare without crashing."""

    @pytest.fixture
    def pipeline(self) -> Pipeline:
        return _load("failed_condition")

    def test_pipeline_translates(self, pipeline: Pipeline) -> None:
        assert len(pipeline.tasks) == 2

    def test_dependency_is_not_unsupported(self, pipeline: Pipeline) -> None:
        handler = next(t for t in pipeline.tasks if t.name == "error_handler")
        assert handler.depends_on is not None
        assert len(handler.depends_on) == 1
        dep = handler.depends_on[0]
        assert not isinstance(dep, UnsupportedValue), (
            "Dependency with 'Failed' condition returned UnsupportedValue. "
            "See ghanse/wkmigrate#44."
        )
        assert dep.task_key == "run_process"

    def test_run_if_is_at_least_one_failed(self, pipeline: Pipeline) -> None:
        """Failed condition should map to run_if=AT_LEAST_ONE_FAILED in the prepared task."""
        prepared = prepare_workflow(pipeline)
        handler = next(t for t in prepared.tasks if t["task_key"] == "error_handler")
        assert handler.get("run_if") == "AT_LEAST_ONE_FAILED", (
            f"Expected run_if='AT_LEAST_ONE_FAILED' for Failed dependency, "
            f"got '{handler.get('run_if')}'. See ghanse/wkmigrate#44."
        )

    def test_preparer_does_not_crash(self, pipeline: Pipeline) -> None:
        prepared = prepare_workflow(pipeline)
        assert len(prepared.tasks) == 2
        task_keys = {t["task_key"] for t in prepared.tasks}
        assert "error_handler" in task_keys


# ============================================================================
# Mixed: Succeeded + Completed + Failed in the same pipeline
# ============================================================================


class TestMixedConditions:
    """A pipeline using all three condition types should translate and prepare."""

    @pytest.fixture
    def pipeline(self) -> Pipeline:
        return _load("mixed_conditions")

    def test_pipeline_translates(self, pipeline: Pipeline) -> None:
        assert len(pipeline.tasks) == 4

    def test_no_unsupported_dependencies(self, pipeline: Pipeline) -> None:
        for task in pipeline.tasks:
            if task.depends_on:
                for dep in task.depends_on:
                    assert not isinstance(dep, UnsupportedValue), (
                        f"Task '{task.name}' has UnsupportedValue dependency. "
                        "See ghanse/wkmigrate#44."
                    )

    def test_run_if_values_per_condition(self, pipeline: Pipeline) -> None:
        """Each condition type should produce the correct run_if value."""
        prepared = prepare_workflow(pipeline)
        tasks = {t["task_key"]: t for t in prepared.tasks}

        assert tasks["run_process"].get("run_if") is None, "No deps → no run_if"
        assert tasks["log_result"].get("run_if") == "ALL_DONE", "Completed → ALL_DONE"
        assert tasks["error_handler"].get("run_if") == "AT_LEAST_ONE_FAILED", "Failed → AT_LEAST_ONE_FAILED"
        assert tasks["finalize"].get("run_if") is None, "Succeeded deps → no run_if"

    def test_preparer_does_not_crash(self, pipeline: Pipeline) -> None:
        prepared = prepare_workflow(pipeline)
        assert len(prepared.tasks) == 4
        task_keys = {t["task_key"] for t in prepared.tasks}
        assert {"run_process", "log_result", "error_handler", "finalize"} == task_keys


# ============================================================================
# Fix A: Completed condition inside IfCondition child activity
# ============================================================================


class TestCompletedInsideIfCondition:
    """A child activity inside an IfCondition branch that depends on a sibling
    with 'Completed' condition should not crash. The is_conditional_task path
    currently only allows TRUE/FALSE outcomes."""

    @pytest.fixture
    def pipeline(self) -> Pipeline:
        return _load("completed_inside_if_condition")

    def test_pipeline_translates(self, pipeline: Pipeline) -> None:
        assert len(pipeline.tasks) >= 1

    def test_no_unsupported_dependencies(self, pipeline: Pipeline) -> None:
        for task in pipeline.tasks:
            if task.depends_on:
                for dep in task.depends_on:
                    assert not isinstance(dep, UnsupportedValue), (
                        f"Task '{task.name}' has UnsupportedValue dependency "
                        f"('{dep.message}'). Completed/Failed conditions inside "
                        "IfCondition children should be accepted. "
                        "See ghanse/wkmigrate#44."
                    )

    def test_preparer_does_not_crash(self, pipeline: Pipeline) -> None:
        prepared = prepare_workflow(pipeline)
        assert len(prepared.tasks) >= 1


# ============================================================================
# Fix B: Multiple dependency conditions ['Succeeded', 'Failed']
# ============================================================================


class TestMultiConditionDependency:
    """A dependency with ['Succeeded', 'Failed'] is equivalent to 'Completed'
    and should be treated as run_if=ALL_DONE, not rejected."""

    @pytest.fixture
    def pipeline(self) -> Pipeline:
        return _load("multi_condition_dependency")

    def test_pipeline_translates(self, pipeline: Pipeline) -> None:
        assert len(pipeline.tasks) == 2

    def test_dependency_is_not_unsupported(self, pipeline: Pipeline) -> None:
        log_task = next(t for t in pipeline.tasks if t.name == "log_outcome")
        assert log_task.depends_on is not None
        assert len(log_task.depends_on) == 1
        dep = log_task.depends_on[0]
        assert not isinstance(dep, UnsupportedValue), (
            "Dependency with ['Succeeded', 'Failed'] returned UnsupportedValue. "
            "This combination is equivalent to 'Completed' and should be accepted. "
            "See ghanse/wkmigrate#44."
        )
        assert dep.task_key == "call_api"

    def test_run_if_is_all_done(self, pipeline: Pipeline) -> None:
        """['Succeeded', 'Failed'] is equivalent to Completed → ALL_DONE."""
        prepared = prepare_workflow(pipeline)
        log_task = next(t for t in prepared.tasks if t["task_key"] == "log_outcome")
        assert log_task.get("run_if") == "ALL_DONE", (
            f"Expected run_if='ALL_DONE' for ['Succeeded','Failed'] dependency, "
            f"got '{log_task.get('run_if')}'. See ghanse/wkmigrate#44."
        )

    def test_preparer_does_not_crash(self, pipeline: Pipeline) -> None:
        prepared = prepare_workflow(pipeline)
        assert len(prepared.tasks) == 2
