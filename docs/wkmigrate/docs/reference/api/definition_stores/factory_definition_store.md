---
sidebar_label: factory_definition_store
title: wkmigrate.definition_stores.factory_definition_store
---

This module defines a `FactoryDefinitionStore` class used to load pipeline definitions from Azure Data Factory.

``BaseFactoryDefinitionStore`` holds the shared load logic (fetch pipeline/trigger,
normalize, resolve datasets and linked services, translate). Subclasses provide
the client (e.g. FactoryClient for Azure API, JsonDefinitionStore for directory JSON).

``FactoryDefinitionStore`` connects to an ADF instance, loads pipeline JSON via
the ADF management client, and returns a translated internal representation with
embedded linked services and datasets. It is typically used as the source store
when migrating from ADF to Databricks Workflows.

**Example**:

    ```python
    from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore

    store = FactoryDefinitionStore(
        tenant_id="TENANT",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION",
        resource_group_name="RESOURCE_GROUP",
        factory_name="ADF_NAME",
        source_property_case="snake",  # or "camel" when source uses camelCase
    )
    pipeline_dict = store.load("my_pipeline")
    ```

## BaseFactoryDefinitionStore Objects

```python
@dataclass(slots=True)
class BaseFactoryDefinitionStore(DefinitionStore)
```

Base for definition stores that load ADF pipeline dicts, resolve datasets and
linked services, and translate to ``Pipeline``. Subclasses set ``factory_client``
(API or JSON) and optionally override ``_normalize_pipeline_structure``.

#### list\_pipelines

```python
def list_pipelines() -> list[str]
```

Returns the names of all pipelines available in the Data Factory.

**Returns**:

  Pipeline names as a ``list[str]``.
  

**Raises**:

- `ValueError` - If the factory client is not initialized.

#### load\_all

```python
def load_all(pipeline_names: list[str] | None = None) -> list[Pipeline]
```

Loads and translates multiple ADF pipelines.

When ``pipeline_names`` is ``None`` all pipelines in the factory are
loaded. Individual pipeline failures are logged and skipped so that
one broken pipeline does not prevent the rest from being translated.

**Arguments**:

- `pipeline_names` - Optional list of pipeline names to translate. When
  ``None``, every pipeline in the factory is included.
  

**Returns**:

  Translated ``Pipeline`` objects as a ``list[Pipeline]``.
  

**Raises**:

- `ValueError` - If the factory client is not initialized.

#### load

```python
def load(pipeline_name: str) -> Pipeline
```

Returns an internal ``Pipeline`` representation of a Data Factory pipeline.

**Arguments**:

- `pipeline_name` - Name of the pipeline to load as a ``str``.
  

**Returns**:

  Pipeline definition decorated with linked resources as a ``Pipeline`` dataclass.
  

**Raises**:

- `ValueError` - If the factory client is not initialized.

## FactoryDefinitionStore Objects

```python
@dataclass(slots=True)
class FactoryDefinitionStore(BaseFactoryDefinitionStore)
```

Definition store backed by an Azure Data Factory instance (management API).

**Attributes**:

- `tenant_id` - Azure AD tenant identifier.
- `client_id` - Service principal application (client) ID.
- `client_secret` - Secret used to authenticate the client.
- `subscription_id` - Azure subscription identifier.
- `resource_group_name` - Resource group name for the factory.
- `factory_name` - Name of the Azure Data Factory instance.
- `source_property_case` - ``"snake"`` when the client returns snake_case (default);
  ``"camel"`` when the source uses camelCase (normalized to snake_case at the boundary).

