"""Deprecated: Use JsonDefinitionStore directly. This module will be removed in a future release."""

import warnings

from wkmigrate.definition_stores.json_definition_store import JsonDefinitionStore

warnings.warn(
    "The json_factory_definition_store module is deprecated. "
    "Use wkmigrate.definition_stores.json_definition_store.JsonDefinitionStore instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Backward-compatible alias
JsonFactoryDefinitionStore = JsonDefinitionStore

__all__ = ["JsonFactoryDefinitionStore"]
