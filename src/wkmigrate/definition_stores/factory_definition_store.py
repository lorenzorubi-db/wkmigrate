"""This module defines a `FactoryDefinitionStore` class used to load pipeline definitions from Azure Data Factory.

``FactoryDefinitionStore`` connects to an ADF instance, loads pipeline JSON via
the ADF management client, and returns a translated internal representation with
embedded linked services and datasets. It is typically used as the source store
when migrating from ADF to Databricks Workflows.

Example:
    ```python
    from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore

    store = FactoryDefinitionStore(
        tenant_id=\"TENANT\",
        client_id=\"CLIENT_ID\",
        client_secret=\"SECRET\",
        subscription_id=\"SUBSCRIPTION\",
        resource_group_name=\"RESOURCE_GROUP\",
        factory_name=\"ADF_NAME\",
        source_property_case=\"snake\",  # or \"camel\" when source uses camelCase
    )
    pipeline_dict = store.load(\"my_pipeline\")
    ```
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Callable
from typing import Literal

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline
from wkmigrate.utils import recursive_camel_to_snake


SourcePropertyCase = Literal["camel", "snake"]


@dataclass(slots=True)
class FactoryDefinitionStore(DefinitionStore):
    """
    Definition store implementation backed by an Azure Data Factory instance.

    Attributes:
        tenant_id: Azure AD tenant identifier.
        client_id: Service principal application (client) ID.
        client_secret: Secret used to authenticate the client.
        subscription_id: Azure subscription identifier.
        resource_group_name: Resource group name for the factory.
        factory_name: Name of the Azure Data Factory instance.
        source_property_case: How property names are represented in the source.
            Use ``\"snake\"`` when the client returns snake_case (default; Azure Python SDK).
            Use ``\"camel\"`` when the source uses camelCase (e.g. raw REST/portal export).
            When ``\"camel\"\", payloads are normalized to snake_case at the boundary.
        factory_client: Concrete ``FactoryClient`` used to load pipelines and child resources. Automatically created using the provided credentials.
    """

    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    subscription_id: str | None = None
    resource_group_name: str | None = None
    factory_name: str | None = None
    source_property_case: SourcePropertyCase = "snake"
    factory_client: FactoryClient | None = field(init=False)
    _appenders: list[Callable[[dict], dict]] | None = field(init=False)
    _normalized_cache: dict[tuple[str, str], dict] = field(init=False)

    def __post_init__(self) -> None:
        """
        Validates configuration and initializes the Factory client.

        Raises:
            ValueError: If the tenant ID is not provided.
            ValueError: If the client ID is not provided.
            ValueError: If the client secret is not provided.
            ValueError: If the subscription ID is not provided.
            ValueError: If the resource group name is not provided.
            ValueError: If the factory name is not provided.
        """
        self._appenders = [self._append_datasets, self._append_linked_service]
        self._normalized_cache = {}
        if self.tenant_id is None:
            raise ValueError("A tenant_id must be provided when creating a FactoryDefinitionStore")
        if self.client_id is None:
            raise ValueError("A client_id must be provided when creating a FactoryDefinitionStore")
        if self.client_secret is None:
            raise ValueError("A client_secret must be provided when creating a FactoryDefinitionStore")
        if self.subscription_id is None:
            raise ValueError("subscription_id cannot be None")
        if self.resource_group_name is None:
            raise ValueError("resource_group_name cannot be None")
        if self.factory_name is None:
            raise ValueError("factory_name cannot be None")

        self.factory_client = FactoryClient(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            subscription_id=self.subscription_id,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
        )

    def _normalize_if_camel(self, d: dict, cache_key: tuple[str, str] | None = None) -> dict:
        """Normalize dict to snake_case when source_property_case is 'camel'; optionally cache by key."""
        if self.source_property_case != "camel":
            return d
        if cache_key is not None and cache_key in self._normalized_cache:
            return self._normalized_cache[cache_key]
        out = recursive_camel_to_snake(d)
        if cache_key is not None:
            self._normalized_cache[cache_key] = out
        return out

    def load(self, pipeline_name: str) -> Pipeline:
        """
        Returns an internal ``Pipeline`` representation of a Data Factory pipeline.

        Args:
            pipeline_name: Name of the pipeline to load as a ``str``.

        Returns:
            Pipeline definition decorated with linked resources as a ``Pipeline`` dataclass.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        if self.factory_client is None:
            raise ValueError("factory_client is not initialized")
        pipeline = self.factory_client.get_pipeline(pipeline_name)
        pipeline = self._normalize_if_camel(pipeline, cache_key=None)
        trigger = self.factory_client.get_trigger(pipeline_name)
        pipeline["trigger"] = self._normalize_if_camel(trigger, cache_key=None)
        activities = pipeline.get("activities")
        if activities is not None:
            pipeline["activities"] = [self._append_objects(activity) for activity in activities]
        else:
            pipeline["activities"] = []
        return translate_pipeline(pipeline)

    def _append_objects(self, activity: dict) -> dict:
        """
        Attaches datasets and linked services to an activity definition.

        Args:
            activity: Activity payload emitted by the factory service as a ``dict``.

        Returns:
            Activity definition with curated child objects as a ``dict``.
        """
        if self._appenders is None:
            return activity
        for appender in self._appenders:
            activity = appender(activity)
        return activity

    def _append_datasets(self, activity: dict) -> dict:
        """
        Populates referenced datasets for the provided activity.

        Args:
            activity: Activity definition containing dataset references as a ``dict``.

        Returns:
            Activity definition enriched with dataset metadata as a ``dict``.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        if self.factory_client is None:
            raise ValueError("factory_client is not initialized")
        if "inputs" in activity:
            datasets = activity.get("inputs")
            if datasets is not None:
                dataset_names = [
                    d.get("reference_name") for d in datasets if isinstance(d, dict)
                ]
                activity["input_dataset_definitions"] = [
                    self._normalize_if_camel(
                        self.factory_client.get_dataset(name), ("dataset", name)
                    )
                    for name in dataset_names
                    if name
                ]
        if "outputs" in activity:
            datasets = activity.get("outputs")
            if datasets is not None:
                dataset_names = [
                    d.get("reference_name") for d in datasets if isinstance(d, dict)
                ]
                activity["output_dataset_definitions"] = [
                    self._normalize_if_camel(
                        self.factory_client.get_dataset(name), ("dataset", name)
                    )
                    for name in dataset_names
                    if name
                ]
        return activity

    def _append_linked_service(self, activity: dict) -> dict:
        """
        Populates linked-service metadata for Databricks activities.

        Args:
            activity: Activity definition containing linked-service references as a ``dict``.

        Returns:
            Activity definition enriched with linked-service payloads as a ``dict``.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        if self.factory_client is None:
            raise ValueError("factory_client is not initialized")
        linked_service_reference = activity.get("linked_service_name")
        if linked_service_reference is not None:
            linked_service_name = linked_service_reference.get("reference_name")
            if linked_service_name:
                raw = self.factory_client.get_linked_service(linked_service_name)
                activity["linked_service_definition"] = self._normalize_if_camel(
                    raw, ("linked_service", linked_service_name)
                )

        if_false_activities = activity.get("if_false_activities")
        if if_false_activities is not None:
            activity["if_false_activities"] = [
                self._append_linked_service(if_false_activity) for if_false_activity in if_false_activities
            ]

        if_true_activities = activity.get("if_true_activities")
        if if_true_activities is not None:
            activity["if_true_activities"] = [
                self._append_linked_service(if_true_activity) for if_true_activity in if_true_activities
            ]

        activities = activity.get("activities")
        if activities is not None:
            activity["activities"] = [self._append_linked_service(activity) for activity in activities]
        return activity
