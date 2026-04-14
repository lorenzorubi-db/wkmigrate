"""Shared utilities for resolving ADF pipeline parameter expressions."""

from __future__ import annotations

import re

_PIPELINE_PARAM_PATTERN = re.compile(r"^@\{?pipeline\(\)\.parameters\.(\w+)\}?$")


def resolve_pipeline_parameter_ref(raw: str) -> str | None:
    """Map ``@pipeline().parameters.X`` to ``{{job.parameters.X}}``.

    Args:
        raw: Raw ADF expression string.

    Returns:
        Databricks job parameter reference, or ``None`` if the input
        does not match the pipeline parameter pattern.
    """
    match = _PIPELINE_PARAM_PATTERN.match(raw)
    return f"{{{{job.parameters.{match.group(1)}}}}}" if match else None
