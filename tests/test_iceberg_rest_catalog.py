"""Tests for iceberg_rest_catalog module."""

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

from portolake import (
    IcebergTable,
    _arrow_schema_to_iceberg,
    create_catalog_config,
    create_load_table_response,
    create_namespace_detail,
    create_namespaces_list,
    create_tables_list,
    generate_static_catalog,
)


@pytest.mark.unit
class TestCreateCatalogConfig:
    """Tests for create_catalog_config()."""

    def test_default_prefix(self):
        config = create_catalog_config()
        assert config["overrides"]["prefix"] == "catalog"
        assert any("catalog" in ep for ep in config["endpoints"])

    def test_custom_prefix(self):
        config = create_catalog_config(prefix="myprefix")
        assert config["overrides"]["prefix"] == "myprefix"


@pytest.mark.unit
class TestCreateNamespacesList:
    """Tests for create_namespaces_list()."""

    def test_single_namespace(self):
        result = create_namespaces_list(["default"])
        assert result == {"namespaces": [["default"]]}

    def test_multiple_namespaces(self):
        result = create_namespaces_list(["ns1", "ns2"])
        assert result == {"namespaces": [["ns1"], ["ns2"]]}


@pytest.mark.unit
class TestCreateNamespaceDetail:
    """Tests for create_namespace_detail()."""

    def test_basic(self):
        result = create_namespace_detail("default")
        assert result == {"namespace": ["default"], "properties": {}}

    def test_with_properties(self):
        result = create_namespace_detail("test", {"title": "Test"})
        assert result["properties"] == {"title": "Test"}


@pytest.mark.unit
class TestCreateTablesList:
    """Tests for create_tables_list()."""

    def test_basic(self):
        result = create_tables_list(["table1", "table2"], "default")
        assert len(result["identifiers"]) == 2
        assert result["identifiers"][0] == {"namespace": ["default"], "name": "table1"}


@pytest.mark.unit
class TestCreateLoadTableResponse:
    """Tests for create_load_table_response()."""

    def test_basic(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        table = IcebergTable(
            name="test",
            parquet_path="data/test/test.parquet",
            schema=_arrow_schema_to_iceberg(schema),
            arrow_schema=schema,
            num_rows=10,
            file_size_bytes=512,
        )
        metadata = {"format-version": 2}
        result = create_load_table_response(table, metadata, "s3://bucket/metadata.json")
        assert result["metadata-location"] == "s3://bucket/metadata.json"
        assert result["metadata"] == metadata
        assert result["config"] == {}


@pytest.mark.unit
class TestGenerateStaticCatalog:
    """Tests for generate_static_catalog()."""

    def test_generates_catalog_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            schema = pa.schema([pa.field("id", pa.int64())])
            table = IcebergTable(
                name="test_table",
                parquet_path="data/test_table/test.parquet",
                schema=_arrow_schema_to_iceberg(schema),
                arrow_schema=schema,
                num_rows=10,
                file_size_bytes=512,
            )
            generate_static_catalog([table], tmpdir, data_base_url="https://example.com")

            # Check v1/config exists
            assert (Path(tmpdir) / "v1" / "config").exists()

            # Check namespace list
            assert (Path(tmpdir) / "v1" / "catalog" / "namespaces" / "__list__").exists()

            # Check table endpoint
            table_path = (
                Path(tmpdir) / "v1" / "catalog" / "namespaces" / "default" / "tables" / "test_table"
            )
            assert table_path.exists()

            # Verify JSON structure
            with open(table_path) as f:
                data = json.load(f)
            assert "metadata" in data
            assert "metadata-location" in data

    def test_multi_namespace(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            schema = pa.schema([pa.field("id", pa.int64())])
            t1 = IcebergTable(
                name="t1",
                parquet_path="data/t1/t1.parquet",
                schema=_arrow_schema_to_iceberg(schema),
                arrow_schema=schema,
                num_rows=5,
                file_size_bytes=256,
            )
            t2 = IcebergTable(
                name="t2",
                parquet_path="data/t2/t2.parquet",
                schema=_arrow_schema_to_iceberg(schema),
                arrow_schema=schema,
                num_rows=5,
                file_size_bytes=256,
            )
            generate_static_catalog(
                {"ns1": [t1], "ns2": [t2]},
                tmpdir,
                data_base_url="https://example.com",
            )

            # Both namespaces should have detail files
            assert (Path(tmpdir) / "v1" / "catalog" / "namespaces" / "ns1" / "__detail__").exists()
            assert (Path(tmpdir) / "v1" / "catalog" / "namespaces" / "ns2" / "__detail__").exists()
