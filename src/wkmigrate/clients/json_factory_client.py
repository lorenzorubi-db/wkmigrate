"""Client that loads ADF pipeline, trigger, dataset, and linked-service definitions from local JSON files.

Uses the same interface as FactoryClient so definition stores can use either the Azure API
or a directory of JSON files. File naming convention:
- Pipelines: any *.json whose filename does not contain "trigger", "dataset", or "linked_service".
- Triggers: *trigger*.json (e.g. test_triggers.json)
- Datasets: *dataset*.json (e.g. test_datasets.json)
- Linked services: *linked_service*.json (e.g. test_linked_services.json)

Missing files are treated as empty lists. get_trigger returns None when no trigger is found.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


def _load_json_list(path: Path) -> list[dict]:
    """Load a JSON file as a list of dicts (or single dict wrapped in a list)."""
    with open(path, "rb") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


@dataclass(slots=True)
class JsonFactoryClient:
    """
    Client that reads pipeline, trigger, dataset, and linked-service definitions from a directory.
    Same interface as FactoryClient: get_pipeline, get_trigger, get_dataset, get_linked_service.
    """

    definition_dir: str | Path
    _pipelines: list[dict] = field(init=False)
    _triggers: list[dict] = field(init=False)
    _datasets: list[dict] = field(init=False)
    _linked_services: list[dict] = field(init=False)

    def __post_init__(self) -> None:
        base = Path(self.definition_dir)
        if not base.is_dir():
            raise ValueError(f"definition_dir is not a directory: {base}")

        pipeline_files = [
            p
            for p in sorted(base.glob("*.json"))
            if "trigger" not in p.name.lower()
            and "dataset" not in p.name.lower()
            and "linked_service" not in p.name.lower()
        ]
        trigger_files = sorted(base.glob("*trigger*.json"))
        dataset_files = sorted(base.glob("*dataset*.json"))
        linked_service_files = sorted(base.glob("*linked_service*.json"))

        self._pipelines = []
        for path in pipeline_files:
            self._pipelines.extend(_load_json_list(path))

        self._triggers = []
        for path in trigger_files:
            self._triggers.extend(_load_json_list(path))

        self._datasets = []
        for path in dataset_files:
            self._datasets.extend(_load_json_list(path))

        self._linked_services = []
        for path in linked_service_files:
            self._linked_services.extend(_load_json_list(path))

    def list_pipeline_names(self) -> list[str]:
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
