import pytest

from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore
from wkmigrate.models.ir.pipeline import Pipeline


class TestDefinitionStoreContracts:
    """Unit tests for ``FactoryDefinitionStore`` and ``WorkspaceDefinitionStore`` contracts."""

    def test_factory_definition_store_requires_mandatory_fields(self) -> None:
        """FactoryDefinitionStore should validate required configuration fields."""
        with pytest.raises(ValueError):
            FactoryDefinitionStore(  # type: ignore[call-arg]
                tenant_id=None,
                client_id=None,
                client_secret=None,
                subscription_id=None,
                resource_group_name=None,
                factory_name=None,
            )

    def test_workspace_definition_store_requires_auth_and_host(self) -> None:
        """WorkspaceDefinitionStore should validate authentication type and host name."""
        with pytest.raises(ValueError):
            WorkspaceDefinitionStore(  # type: ignore[call-arg]
                authentication_type="invalid",
                host_name=None,
            )

    def test_factory_definition_store_uses_definition_store_interface(
        self,
        mock_factory_client,
    ) -> None:
        """FactoryDefinitionStore should behave as a DefinitionStore when wired with a mock client."""
        assert mock_factory_client is not None

        store = FactoryDefinitionStore(
            tenant_id="TENANT_ID",
            client_id="CLIENT_ID",
            client_secret="SECRET",
            subscription_id="SUBSCRIPTION_ID",
            resource_group_name="RESOURCE_GROUP",
            factory_name="FACTORY_NAME",
        )

        assert isinstance(store, DefinitionStore)
        pipeline = store.load("TEST_PIPELINE_NAME")
        assert isinstance(pipeline, Pipeline)
        assert pipeline.name == "TEST_PIPELINE_NAME"

    def test_factory_definition_store_default_source_property_case_is_snake(
        self,
        mock_factory_client,
    ) -> None:
        """Default source_property_case is 'snake'; load works with snake_case payloads."""
        assert mock_factory_client is not None
        store = FactoryDefinitionStore(
            tenant_id="TENANT_ID",
            client_id="CLIENT_ID",
            client_secret="SECRET",
            subscription_id="SUBSCRIPTION_ID",
            resource_group_name="RESOURCE_GROUP",
            factory_name="FACTORY_NAME",
        )
        pipeline = store.load("TEST_PIPELINE_NAME")
        assert pipeline.name == "TEST_PIPELINE_NAME"

    def test_factory_definition_store_accepts_source_property_case_camel(
        self,
        mock_factory_client,
    ) -> None:
        """Store with source_property_case='camel' accepts options and load still works (snake payload stays snake)."""
        assert mock_factory_client is not None
        store = FactoryDefinitionStore(
            tenant_id="TENANT_ID",
            client_id="CLIENT_ID",
            client_secret="SECRET",
            subscription_id="SUBSCRIPTION_ID",
            resource_group_name="RESOURCE_GROUP",
            factory_name="FACTORY_NAME",
            source_property_case="camel",
        )
        pipeline = store.load("TEST_PIPELINE_NAME")
        assert pipeline.name == "TEST_PIPELINE_NAME"

    def test_factory_definition_store_camel_normalizes_to_snake(
        self,
        mock_factory_client,
        monkeypatch,
    ) -> None:
        """When source_property_case is 'camel', camelCase payload is normalized to snake_case for translation."""
        assert mock_factory_client is not None
        # Return a minimal camelCase pipeline from the mock so load() runs the normalizer
        camel_pipeline = {
            "name": "CAMEL_PIPELINE",
            "activities": [
                {
                    "name": "act1",
                    "type": "DatabricksNotebook",
                    "linkedServiceName": {"type": "LinkedServiceReference", "referenceName": "db_linkedservice_001"},
                    "notebookPath": "/test",
                }
            ],
        }
        camel_trigger = {"properties": {"recurrence": {"frequency": "Day", "interval": 1}}}
        original_get_pipeline = mock_factory_client.get_pipeline
        original_get_trigger = mock_factory_client.get_trigger

        def get_pipeline(name: str):
            if name == "CAMEL_PIPELINE":
                return dict(camel_pipeline)
            return original_get_pipeline(name)

        def get_trigger(name: str):
            if name == "CAMEL_PIPELINE":
                return dict(camel_trigger)
            return original_get_trigger(name)

        monkeypatch.setattr(mock_factory_client, "get_pipeline", get_pipeline)
        monkeypatch.setattr(mock_factory_client, "get_trigger", get_trigger)

        store = FactoryDefinitionStore(
            tenant_id="TENANT_ID",
            client_id="CLIENT_ID",
            client_secret="SECRET",
            subscription_id="SUBSCRIPTION_ID",
            resource_group_name="RESOURCE_GROUP",
            factory_name="FACTORY_NAME",
            source_property_case="camel",
        )
        pipeline = store.load("CAMEL_PIPELINE")
        assert pipeline.name == "CAMEL_PIPELINE"
        assert len(pipeline.tasks) >= 1

    def test_workspace_definition_store_uses_definition_store_interface(
        self,
        mock_workspace_client,
    ) -> None:
        """WorkspaceDefinitionStore should behave as a DefinitionStore when wired with a mock workspace client."""
        assert mock_workspace_client is not None

        store = WorkspaceDefinitionStore(
            authentication_type="pat",
            host_name="https://example.com",
            pat="DUMMY_TOKEN",
        )

        assert isinstance(store, DefinitionStore)
        # WorkspaceDefinitionStore no longer has load method - it is a sink for pipelines
        # Verify that to_job and to_asset_bundle methods are available
        assert hasattr(store, "to_job")
        assert hasattr(store, "to_asset_bundle")
