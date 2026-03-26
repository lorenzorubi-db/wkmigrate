"""Definition store that loads ADF pipeline definitions from local JSON files.

Loads pipeline, trigger, dataset, and linked-service JSON files from
subdirectories of a user-specified source directory and exposes the same
interface as the API-backed store. No Azure credentials required.

Expected directory structure::

    source_directory/
    ├── pipelines/          # Pipeline JSON files
    ├── triggers/           # Trigger JSON files (optional)
    ├── datasets/           # Dataset JSON files (optional)
    └── linked_services/    # Linked-service JSON files (optional)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from wkmigrate.definition_stores.factory_definition_store import (
    BaseFactoryDefinitionStore,
)
from wkmigrate.enums.source_property_case import SourcePropertyCase
from wkmigrate.enums.workflow_source_type import WorkflowSourceType
from wkmigrate.utils import normalize_arm_pipeline, recursive_camel_to_snake


def _load_json_list(path: Path) -> list[dict]:
    """Load a JSON file as a list of dicts (or single dict wrapped in a list)."""
    with open(path, "rb") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _load_all_json_from_dir(directory: Path) -> list[dict]:
    """Load and merge all JSON files from a directory. Returns empty list if directory does not exist."""
    if not directory.is_dir():
        return []
    result: list[dict] = []
    for path in sorted(directory.glob("*.json")):
        result.extend(_load_json_list(path))
    return result


@dataclass(slots=True)
class JsonDefinitionStore(BaseFactoryDefinitionStore):
    """
    Definition store backed by a directory of JSON files.

    Loads pipeline, trigger, dataset, and linked-service definitions from
    subdirectories of ``source_directory``. All loaded data is normalized to
    snake_case at load time so that lookups work regardless of the original
    casing in the source files.

    Expected directory layout::

        source_directory/
        ├── pipelines/          # Required — pipeline definition JSON files
        ├── triggers/           # Optional — trigger definition JSON files
        ├── datasets/           # Optional — dataset definition JSON files
        └── linked_services/    # Optional — linked-service definition JSON files

    Attributes:
        source_directory: Path to the root directory containing the subdirectories.
        source_property_case: Property casing convention in the source files.
            Defaults to ``SourcePropertyCase.CAMEL`` (portal-exported JSON uses camelCase).
        source_system: The workflow source system type (default: ADF).
    """

    source_directory: str | Path | None = None
    source_property_case: SourcePropertyCase = SourcePropertyCase.CAMEL
    source_system: WorkflowSourceType = WorkflowSourceType.ADF

    _pipelines: list[dict] = field(init=False)
    _triggers: list[dict] = field(init=False)
    _datasets: list[dict] = field(init=False)
    _linked_services: list[dict] = field(init=False)

    def __post_init__(self) -> None:
        BaseFactoryDefinitionStore.__post_init__(self)
        if self.source_directory is None:
            raise ValueError("source_directory must be provided")
        base = Path(self.source_directory)
        if not base.is_dir():
            raise ValueError(f"source_directory is not a directory: {base}")

        self._pipelines = _load_all_json_from_dir(base / "pipelines")
        self._triggers = _load_all_json_from_dir(base / "triggers")
        self._datasets = _load_all_json_from_dir(base / "datasets")
        self._linked_services = _load_all_json_from_dir(base / "linked_services")

        self._pipelines = [recursive_camel_to_snake(p) for p in self._pipelines]
        self._triggers = [recursive_camel_to_snake(t) for t in self._triggers]
        self._datasets = [recursive_camel_to_snake(d) for d in self._datasets]
        self._linked_services = [recursive_camel_to_snake(ls) for ls in self._linked_services]

        if not self.list_pipelines():
            raise ValueError(
                f"No pipeline JSON files found in {base / 'pipelines'}. "
                "Add one or more .json files to the pipelines/ subdirectory."
            )

        self._factory_client = self  # type: ignore[assignment]

    def list_pipelines(self) -> list[str]:
        """Return the names of all loaded pipelines."""
        return [p.get("name") for p in self._pipelines if p.get("name")]

    def get_pipeline(self, pipeline_name: str) -> dict:
        """Return the pipeline dict for the given name. Raises ValueError if not found."""
        for p in self._pipelines:
            if p.get("name") == pipeline_name:
                return p
        raise ValueError(f'No pipeline found with name "{pipeline_name}"')

    def get_trigger(self, pipeline_name: str) -> dict | None:
        """Return the trigger for the given pipeline name, or None if none."""
        for trigger in self._triggers:
            properties = trigger.get("properties")
            if not isinstance(properties, dict):
                continue
            pipelines = properties.get("pipelines") or []
            for pl in pipelines:
                ref = pl.get("pipeline_reference") if isinstance(pl, dict) else None
                if not isinstance(ref, dict):
                    continue
                if ref.get("type") == "PipelineReference" and ref.get("reference_name") == pipeline_name:
                    return trigger
        return None

    def get_dataset(self, dataset_name: str) -> dict:
        """Return the dataset dict for the given name. Raises ValueError if not found."""
        for d in self._datasets:
            if d.get("name") == dataset_name:
                return d
        raise ValueError(f'No dataset found with name "{dataset_name}"')

    def get_linked_service(self, linked_service_name: str) -> dict:
        """Return the linked-service dict for the given name. Raises ValueError if not found."""
        for ls in self._linked_services:
            if ls.get("name") == linked_service_name:
                return ls
        raise ValueError(f'No linked service found with name "{linked_service_name}"')

    def _normalize_pipeline_structure(self, pipeline: dict) -> dict:
        """Unwrap ARM shape and merge type_properties into activities."""
        return normalize_arm_pipeline(pipeline)
