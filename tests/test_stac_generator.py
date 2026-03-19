"""Tests for STAC metadata generation from Iceberg tables (Phase 3 + 4).

Phase 3: table:* fields (STAC Table Extension) from Iceberg schema.
Phase 4: iceberg:* fields (STAC Iceberg Extension) from catalog/table state.
"""

import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _make_wkb_point(x: float, y: float) -> bytes:
    """Create a WKB point (little-endian)."""
    return struct.pack("<BIdd", 1, 1, x, y)


# --- Phase 3: STAC Table Extension fields ---


@pytest.mark.integration
def test_table_columns_from_iceberg_schema(iceberg_backend, iceberg_catalog, tmp_path):
    """table:columns should list all non-spatial columns with types."""
    table_data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["a", "b"], type=pa.string()),
            "value": pa.array([1.5, 2.5], type=pa.float64()),
        }
    )
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(path)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolake.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.buildings")
    metadata = generate_table_metadata(table)

    assert "table:columns" in metadata
    columns = metadata["table:columns"]
    col_names = [c["name"] for c in columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "value" in col_names

    # Each column should have name and type
    for col in columns:
        assert "name" in col
        assert "type" in col


@pytest.mark.integration
def test_table_columns_have_correct_types(iceberg_backend, iceberg_catalog, tmp_path):
    """table:columns types should map from Iceberg types."""
    table_data = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "score": pa.array([9.5], type=pa.float64()),
            "label": pa.array(["x"], type=pa.string()),
        }
    )
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="typed",
        assets={"data.parquet": str(path)},
        schema={"columns": [], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolake.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.typed")
    metadata = generate_table_metadata(table)

    col_map = {c["name"]: c["type"] for c in metadata["table:columns"]}
    assert col_map["id"] == "int64"
    assert col_map["score"] == "float64"
    assert col_map["label"] == "string"


@pytest.mark.integration
def test_table_row_count(iceberg_backend, iceberg_catalog, tmp_path):
    """table:row_count should reflect the number of rows in the current snapshot."""
    table_data = pa.table({"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="counted",
        assets={"data.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolake.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.counted")
    metadata = generate_table_metadata(table)

    assert metadata["table:row_count"] == 5


@pytest.mark.integration
def test_table_primary_geometry_detected(iceberg_backend, iceberg_catalog, tmp_path):
    """table:primary_geometry should be set when a geometry column exists."""
    wkb_values = [_make_wkb_point(2.35, 48.85), _make_wkb_point(-73.99, 40.75)]
    table_data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "geometry": pa.array(wkb_values, type=pa.binary()),
        }
    )
    path = tmp_path / "geo.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="geoplaces",
        assets={"geo.parquet": str(path)},
        schema={"columns": ["id", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolake.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.geoplaces")
    metadata = generate_table_metadata(table)

    assert metadata["table:primary_geometry"] == "geometry"


@pytest.mark.integration
def test_table_primary_geometry_none_without_geometry(iceberg_backend, iceberg_catalog, tmp_path):
    """table:primary_geometry should be None when no geometry column exists."""
    table_data = pa.table({"id": pa.array([1, 2], type=pa.int64())})
    path = tmp_path / "plain.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="plain",
        assets={"plain.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolake.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.plain")
    metadata = generate_table_metadata(table)

    assert metadata["table:primary_geometry"] is None


@pytest.mark.integration
def test_table_columns_exclude_spatial_derived(iceberg_backend, iceberg_catalog, tmp_path):
    """table:columns should exclude portolake-derived columns (geohash_*, bbox_*)."""
    wkb_values = [_make_wkb_point(2.35, 48.85)]
    table_data = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "geometry": pa.array(wkb_values, type=pa.binary()),
        }
    )
    path = tmp_path / "geo.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="filtered",
        assets={"geo.parquet": str(path)},
        schema={"columns": ["id", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolake.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.filtered")
    metadata = generate_table_metadata(table)

    col_names = [c["name"] for c in metadata["table:columns"]]
    assert not any(n.startswith("geohash_") for n in col_names)
    assert not any(n.startswith("bbox_") for n in col_names)


@pytest.mark.integration
def test_table_row_count_updates_after_publish(iceberg_backend, iceberg_catalog, tmp_path):
    """table:row_count should update after publishing more data."""
    t1 = pa.table({"id": pa.array([1, 2], type=pa.int64())})
    p1 = tmp_path / "v1.parquet"
    pq.write_table(t1, p1)

    iceberg_backend.publish(
        collection="growing",
        assets={"data.parquet": str(p1)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    t2 = pa.table({"id": pa.array([3, 4, 5], type=pa.int64())})
    p2 = tmp_path / "v2.parquet"
    pq.write_table(t2, p2)

    iceberg_backend.publish(
        collection="growing",
        assets={"data.parquet": str(p2)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v2",
    )

    from portolake.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.growing")
    metadata = generate_table_metadata(table)

    # Should have all rows from both publishes (append mode)
    assert metadata["table:row_count"] == 5
