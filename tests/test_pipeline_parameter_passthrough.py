"""Tests for ghanse/wkmigrate#55: Pipeline parameter expressions in notebook base_parameters.

When an ADF DatabricksNotebook activity uses ``@pipeline().parameters.X`` in
``baseParameters``, the value should be translated to the Databricks job
parameter reference ``{{job.parameters.X}}``, not silently set to ``""``.

These tests assert the *correct* behaviour and are expected to FAIL until
the fix is implemented.
"""

from __future__ import annotations

import warnings

import pytest

from wkmigrate.translators.activity_translators.activity_translator import (
    translate_activity,
)
from wkmigrate.models.ir.pipeline import DatabricksNotebookActivity


# ============================================================================
# Unit tests: _parse_notebook_parameters via translate_activity
# ============================================================================


class TestPipelineParameterExpression:
    """@pipeline().parameters.X in baseParameters should map to
    {{job.parameters.X}}, not empty string."""

    def test_single_pipeline_parameter(self) -> None:
        activity = {
            "name": "notebook_with_param",
            "type": "DatabricksNotebook",
            "depends_on": [],
            "policy": {"timeout": "0.01:00:00"},
            "notebook_path": "/Workspace/notebooks/my_notebook",
            "base_parameters": {
                "my_param": {
                    "value": "@pipeline().parameters.myParam",
                    "type": "Expression",
                }
            },
        }
        result = translate_activity(activity)
        assert isinstance(result, DatabricksNotebookActivity)
        assert result.base_parameters is not None
        assert result.base_parameters["my_param"] == "{{job.parameters.myParam}}", (
            f"Expected pipeline parameter reference, got '{result.base_parameters['my_param']}'. "
            "See ghanse/wkmigrate#55."
        )

    def test_multiple_pipeline_parameters(self) -> None:
        activity = {
            "name": "notebook_multi_params",
            "type": "DatabricksNotebook",
            "depends_on": [],
            "policy": {"timeout": "0.01:00:00"},
            "notebook_path": "/Workspace/notebooks/multi_param_notebook",
            "base_parameters": {
                "table_name": {
                    "value": "@pipeline().parameters.tableName",
                    "type": "Expression",
                },
                "retention": {
                    "value": "@pipeline().parameters.retentionDays",
                    "type": "Expression",
                },
                "static_value": "hardcoded",
            },
        }
        result = translate_activity(activity)
        assert isinstance(result, DatabricksNotebookActivity)
        assert result.base_parameters is not None
        assert result.base_parameters["table_name"] == "{{job.parameters.tableName}}"
        assert result.base_parameters["retention"] == "{{job.parameters.retentionDays}}"
        assert result.base_parameters["static_value"] == "hardcoded"

    def test_pipeline_parameter_does_not_warn(self) -> None:
        """A resolvable pipeline parameter expression should not emit a warning."""
        activity = {
            "name": "notebook_no_warn",
            "type": "DatabricksNotebook",
            "depends_on": [],
            "policy": {"timeout": "0.01:00:00"},
            "notebook_path": "/Workspace/notebooks/no_warn",
            "base_parameters": {
                "param_a": {
                    "value": "@pipeline().parameters.paramA",
                    "type": "Expression",
                }
            },
        }
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            result = translate_activity(activity)
        assert result.base_parameters["param_a"] == "{{job.parameters.paramA}}"


# ============================================================================
# Edge cases
# ============================================================================


class TestNonPipelineParameterExpressions:
    """Expressions that are NOT @pipeline().parameters.X should still fall
    back gracefully (warning + empty string or UnsupportedValue)."""

    def test_concat_expression_still_warns(self) -> None:
        """@concat(...) is not resolvable — should still warn."""
        activity = {
            "name": "notebook_concat",
            "type": "DatabricksNotebook",
            "depends_on": [],
            "policy": {"timeout": "0.01:00:00"},
            "notebook_path": "/Workspace/notebooks/concat_nb",
            "base_parameters": {
                "path": {
                    "value": "@concat(pipeline().parameters.base, '/suffix')",
                    "type": "Expression",
                }
            },
        }
        with pytest.warns(UserWarning):
            result = translate_activity(activity)
        assert isinstance(result, DatabricksNotebookActivity)
        # concat is not resolvable, so empty string is acceptable for now
        assert result.base_parameters["path"] == ""

    def test_activity_output_expression_still_warns(self) -> None:
        """@activity('X').output.Y is not resolvable in base_parameters."""
        activity = {
            "name": "notebook_activity_ref",
            "type": "DatabricksNotebook",
            "depends_on": [],
            "policy": {"timeout": "0.01:00:00"},
            "notebook_path": "/Workspace/notebooks/activity_ref_nb",
            "base_parameters": {
                "result": {
                    "value": "@activity('upstream').output.value",
                    "type": "Expression",
                }
            },
        }
        with pytest.warns(UserWarning):
            result = translate_activity(activity)
        assert isinstance(result, DatabricksNotebookActivity)
        assert result.base_parameters["result"] == ""
