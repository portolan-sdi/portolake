"""Tests for sdi_catalog module."""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolake import (
    create_stac_iso_record,
    create_stac_iso_schema,
    detect_parquet_type,
)


@pytest.mark.unit
class TestCreateStacIsoSchema:
    """Tests for create_stac_iso_schema()."""

    def test_returns_valid_schema(self):
        schema = create_stac_iso_schema()
        assert isinstance(schema, pa.Schema)
        assert len(schema) > 0

    def test_has_required_stac_fields(self):
        schema = create_stac_iso_schema()
        names = schema.names
        assert "id" in names
        assert "geometry" in names
        assert "bbox_west" in names
        assert "datetime" in names
        assert "assets" in names

    def test_has_iso_fields(self):
        schema = create_stac_iso_schema()
        names = schema.names
        assert "title" in names
        assert "abstract" in names
        assert "topic_category" in names
        assert "license" in names

    def test_id_is_not_nullable(self):
        schema = create_stac_iso_schema()
        id_field = schema.field("id")
        assert not id_field.nullable


@pytest.mark.unit
class TestCreateStacIsoRecord:
    """Tests for create_stac_iso_record()."""

    def test_minimal_record(self):
        record = create_stac_iso_record("item1", "Test Item")
        assert record["id"] == "item1"
        assert record["title"] == "Test Item"
        assert record["created_at"] is not None

    def test_with_stac_info(self):
        stac = {
            "bbox": [-180, -90, 180, 90],
            "datetime": "2024-01-01T00:00:00Z",
        }
        record = create_stac_iso_record("item1", "Test", stac_info=stac)
        assert record["bbox_west"] == -180
        assert record["bbox_north"] == 90
        assert record["datetime"] is not None

    def test_with_iso_info(self):
        iso = {
            "abstract": "A test dataset",
            "license": "CC-BY-4.0",
            "keywords": ["test", "data"],
        }
        record = create_stac_iso_record("item1", "Test", iso_info=iso)
        assert record["abstract"] == "A test dataset"
        assert record["license"] == "CC-BY-4.0"
        assert '"test"' in record["keywords"]


@pytest.mark.unit
class TestDetectParquetType:
    """Tests for detect_parquet_type()."""

    def test_geoparquet_detection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            table = pa.table({"geometry": [b"wkb"], "name": ["test"]})
            pq.write_table(table, path)
            assert detect_parquet_type(str(path)) == "geoparquet"

    def test_raquet_detection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            table = pa.table({"block": [0], "metadata": ["{}"], "data": [b""]})
            pq.write_table(table, path)
            assert detect_parquet_type(str(path)) == "raquet"

    def test_plain_parquet_defaults_to_geoparquet(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            table = pa.table({"id": [1], "value": [2.0]})
            pq.write_table(table, path)
            assert detect_parquet_type(str(path)) == "geoparquet"
