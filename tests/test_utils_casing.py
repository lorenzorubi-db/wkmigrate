"""Unit tests for casing utilities (camel_to_snake, recursive_camel_to_snake, normalize_arm_pipeline)."""

import pytest

from wkmigrate.utils import (
    camel_to_snake,
    recursive_camel_to_snake,
    normalize_arm_pipeline,
)


class TestCamelToSnake:
    """Tests for camel_to_snake."""

    def test_simple_camel(self) -> None:
        assert camel_to_snake("linkedServiceName") == "linked_service_name"

    def test_pascal(self) -> None:
        assert camel_to_snake("ReferenceName") == "reference_name"

    def test_all_lower(self) -> None:
        assert camel_to_snake("activities") == "activities"

    def test_multiple_capitals(self) -> None:
        assert camel_to_snake("ifTrueActivities") == "if_true_activities"

    def test_single_word(self) -> None:
        assert camel_to_snake("name") == "name"


class TestRecursiveCamelToSnake:
    """Tests for recursive_camel_to_snake."""

    def test_nested_dict(self) -> None:
        obj = {"linkedServiceName": {"referenceName": "ls1"}}
        got = recursive_camel_to_snake(obj)
        assert got == {"linked_service_name": {"reference_name": "ls1"}}

    def test_list_of_dicts(self) -> None:
        obj = [{"referenceName": "a"}, {"referenceName": "b"}]
        got = recursive_camel_to_snake(obj)
        assert got == [{"reference_name": "a"}, {"reference_name": "b"}]

    def test_leaves_primitives_unchanged(self) -> None:
        obj = {"someKey": 1, "anotherKey": "value", "nested": {"k": True}}
        got = recursive_camel_to_snake(obj)
        assert got == {"some_key": 1, "another_key": "value", "nested": {"k": True}}

    def test_already_snake_unchanged(self) -> None:
        obj = {"reference_name": "x", "linked_service_name": "y"}
        got = recursive_camel_to_snake(obj)
        assert got == obj

    def test_empty_containers(self) -> None:
        assert recursive_camel_to_snake({}) == {}
        assert recursive_camel_to_snake([]) == []


class TestNormalizeArmPipeline:
    """Tests for normalize_arm_pipeline (ARM-shaped pipeline to flat)."""

    def test_flat_pipeline_unchanged(self) -> None:
        """Pipeline without 'properties' wrapper is returned as-is (aside from type_properties merge)."""
        pipeline = {"name": "p1", "activities": [{"name": "a1", "type": "Notebook"}]}
        got = normalize_arm_pipeline(pipeline)
        assert got["name"] == "p1"
        assert len(got["activities"]) == 1
        assert got["activities"][0]["name"] == "a1"

    def test_arm_wrapper_unwrapped(self) -> None:
        """Pipeline with top-level 'properties' is unwrapped."""
        pipeline = {
            "name": "p1",
            "properties": {
                "activities": [{"name": "a1", "type": "Notebook"}],
                "parameters": {},
            },
        }
        got = normalize_arm_pipeline(pipeline)
        assert got["name"] == "p1"
        assert got["activities"] == [{"name": "a1", "type": "Notebook"}]
        assert got["parameters"] == {}

    def test_type_properties_merged_into_activity(self) -> None:
        """Activity typeProperties / type_properties are merged into activity root."""
        pipeline = {
            "name": "p1",
            "activities": [
                {
                    "name": "a1",
                    "type": "DatabricksNotebook",
                    "type_properties": {"notebook_path": "/path", "base_parameters": {}},
                }
            ],
        }
        got = normalize_arm_pipeline(pipeline)
        act = got["activities"][0]
        assert act.get("notebook_path") == "/path"
        assert act.get("base_parameters") == {}
        assert "type_properties" not in act

    def test_list_annotations_converted_to_tags_dict(self) -> None:
        """ADF annotations (list of strings) should be converted to a tags dict."""
        pipeline = {
            "name": "p1",
            "properties": {
                "activities": [],
                "annotations": ["MyLabel", "AnotherLabel"],
            },
        }
        got = normalize_arm_pipeline(pipeline)
        assert isinstance(got["tags"], dict)
        assert got["tags"]["MyLabel"] == ""
        assert got["tags"]["AnotherLabel"] == ""

    def test_empty_annotations_gives_empty_tags(self) -> None:
        """Empty annotations list should produce empty tags dict."""
        pipeline = {
            "name": "p1",
            "properties": {
                "activities": [],
                "annotations": [],
            },
        }
        got = normalize_arm_pipeline(pipeline)
        assert isinstance(got["tags"], dict)
