"""Deprecated: Use JsonDefinitionStore directly. This module will be removed in a future release."""
import warnings
from wkmigrate.definition_stores.json_definition_store import JsonDefinitionStore


class JsonFactoryClient:
    """Deprecated. Use JsonDefinitionStore directly."""
    def __init__(self, **kwargs):
        warnings.warn(
            "JsonFactoryClient is deprecated. Use JsonDefinitionStore directly.",
            DeprecationWarning,
            stacklevel=2,
        )
