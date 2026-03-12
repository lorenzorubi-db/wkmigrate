"""Definition store that loads ADF pipeline definitions from local JSON files.

Uses JsonFactoryClient (same interface as FactoryClient) so the same base load logic
applies. No Azure credentials required. When the directory contains pipeline, dataset,
and linked-service JSON files, they are resolved like the API-backed store.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from wkmigrate.clients.json_factory_client import JsonFactoryClient
from wkmigrate.definition_stores.factory_definition_store import (
    BaseFactoryDefinitionStore,
    SourcePropertyCase,
)
from wkmigrate.utils import normalize_arm_pipeline


@dataclass(slots=True)
class JsonFactoryDefinitionStore(BaseFactoryDefinitionStore):
    """
    Definition store backed by a directory of JSON files (JsonFactoryClient).

    Attributes:
        definition_dir: Path to the directory containing JSON files.
        source_property_case: ``\"camel\"`` for portal/downloaded JSON (default);
            ``\"snake\"`` when files are already snake_case.
    """

    definition_dir: str | Path | None = None
    source_property_case: SourcePropertyCase = "camel"

    def __post_init__(self) -> None:
        BaseFactoryDefinitionStore.__post_init__(self)
        if self.definition_dir is None:
            raise ValueError("definition_dir must be provided")
        base = Path(self.definition_dir)
        if not base.is_dir():
            raise ValueError(f"definition_dir is not a directory: {base}")
        self.factory_client = JsonFactoryClient(definition_dir=base)
        if not self.factory_client.list_pipeline_names():
            raise ValueError(
                f"No pipeline JSON files found in definition_dir: {base}. "
                "Add one or more .json files containing pipeline definitions."
            )

    def list_pipeline_names(self) -> list[str]:
        """Return the names of all pipelines loaded from the store."""
        if self.factory_client is None:
            return []
        return self.factory_client.list_pipeline_names()

    def _normalize_pipeline_structure(self, pipeline: dict) -> dict:
        """Unwrap ARM shape and merge type_properties into activities."""
        return normalize_arm_pipeline(pipeline)
