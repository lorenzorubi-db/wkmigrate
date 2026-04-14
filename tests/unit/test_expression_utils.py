"""Tests for the shared pipeline parameter expression utility."""

from __future__ import annotations

from wkmigrate.translators.expression_utils import resolve_pipeline_parameter_ref


def test_bare_pipeline_parameter() -> None:
    assert resolve_pipeline_parameter_ref("@pipeline().parameters.enable") == "{{job.parameters.enable}}"


def test_braced_pipeline_parameter() -> None:
    assert resolve_pipeline_parameter_ref("@{pipeline().parameters.environment}") == "{{job.parameters.environment}}"


def test_non_matching_returns_none() -> None:
    assert resolve_pipeline_parameter_ref("@equals('a', 'b')") is None


def test_empty_string_returns_none() -> None:
    assert resolve_pipeline_parameter_ref("") is None


def test_activity_output_returns_none() -> None:
    assert resolve_pipeline_parameter_ref("@activity('Lookup1').output.firstRow.id") is None
