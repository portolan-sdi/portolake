"""Integration tests for STAC metadata via on_post_add hook.

Verifies that after `portolan add`, the collection.json has correct
STAC extension metadata from the Iceberg backend.
"""

import json

import pytest

from tests.integration.conftest import (
    invoke_add,
    place_geojson_in_collection,
)


@pytest.mark.integration
def test_on_post_add_updates_collection_extensions(initialized_iceberg_catalog, runner):
    """After CLI add, collection.json should contain table:* extension fields."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "stac_test")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    collection_json = catalog_root / "stac_test" / "collection.json"
    coll = json.loads(collection_json.read_text())

    # on_post_add should have set table:columns from Iceberg schema
    assert "table:columns" in coll, f"Missing table:columns in {list(coll.keys())}"
    columns = coll["table:columns"]
    assert isinstance(columns, list)
    assert len(columns) > 0

    # Derived columns (geohash_*, bbox_*) should be excluded
    col_names = {c["name"] for c in columns}
    assert not any(n.startswith("geohash_") for n in col_names), (
        f"geohash column should be excluded from table:columns: {col_names}"
    )
    assert "bbox_xmin" not in col_names, "bbox columns should be excluded"


@pytest.mark.integration
def test_on_post_add_sets_row_count(initialized_iceberg_catalog, runner):
    """After CLI add, table:row_count should match the input data."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "rowcount_test")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    collection_json = catalog_root / "rowcount_test" / "collection.json"
    coll = json.loads(collection_json.read_text())

    # The test GeoJSON has 3 features
    row_count = coll.get("table:row_count")
    assert row_count is not None, "Missing table:row_count"
    assert row_count == 3, f"Expected 3 rows, got {row_count}"
