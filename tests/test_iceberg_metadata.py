"""Tests for iceberg_metadata module."""

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolake import (
    IcebergTable,
    _arrow_schema_to_iceberg,
    add_iceberg_field_ids,
    create_name_mapping,
    create_table_metadata,
    generate_manifest_files,
    parquet_to_iceberg_table,
)


@pytest.mark.unit
class TestArrowSchemaToIceberg:
    """Tests for _arrow_schema_to_iceberg()."""

    def test_simple_types(self):
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
                pa.field("active", pa.bool_()),
            ]
        )
        result = _arrow_schema_to_iceberg(schema)
        assert result["type"] == "struct"
        assert result["schema-id"] == 0
        assert len(result["fields"]) == 4

        # Check type mappings
        types = {f["name"]: f["type"] for f in result["fields"]}
        assert types["id"] == "long"
        assert types["name"] == "string"
        assert types["value"] == "double"
        assert types["active"] == "boolean"

    def test_date_and_timestamp(self):
        schema = pa.schema(
            [
                pa.field("date_col", pa.date32()),
                pa.field("ts_col", pa.timestamp("us")),
            ]
        )
        result = _arrow_schema_to_iceberg(schema)
        types = {f["name"]: f["type"] for f in result["fields"]}
        assert types["date_col"] == "date"
        assert types["ts_col"] == "timestamp"

    def test_binary_type(self):
        schema = pa.schema([pa.field("data", pa.binary())])
        result = _arrow_schema_to_iceberg(schema)
        assert result["fields"][0]["type"] == "binary"

    def test_list_type(self):
        schema = pa.schema([pa.field("tags", pa.list_(pa.string()))])
        result = _arrow_schema_to_iceberg(schema)
        field_type = result["fields"][0]["type"]
        assert field_type["type"] == "list"
        assert field_type["element"] == "string"

    def test_struct_type(self):
        schema = pa.schema(
            [
                pa.field(
                    "address",
                    pa.struct(
                        [
                            pa.field("city", pa.string()),
                            pa.field("zip", pa.int32()),
                        ]
                    ),
                )
            ]
        )
        result = _arrow_schema_to_iceberg(schema)
        field_type = result["fields"][0]["type"]
        assert field_type["type"] == "struct"
        assert len(field_type["fields"]) == 2

    def test_field_ids_are_unique(self):
        schema = pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
            ]
        )
        result = _arrow_schema_to_iceberg(schema)
        ids = [f["id"] for f in result["fields"]]
        assert len(ids) == len(set(ids)), "Field IDs must be unique"

    def test_nullable_fields(self):
        schema = pa.schema(
            [
                pa.field("required_col", pa.string(), nullable=False),
                pa.field("optional_col", pa.string(), nullable=True),
            ]
        )
        result = _arrow_schema_to_iceberg(schema)
        fields = {f["name"]: f for f in result["fields"]}
        assert fields["required_col"]["required"] is True
        assert fields["optional_col"]["required"] is False


@pytest.mark.unit
class TestIcebergTable:
    """Tests for IcebergTable dataclass."""

    def test_creation(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        table = IcebergTable(
            name="test_table",
            parquet_path="data/test_table/test.parquet",
            schema={"type": "struct", "fields": []},
            arrow_schema=schema,
            num_rows=100,
            file_size_bytes=1024,
        )
        assert table.name == "test_table"
        assert table.num_rows == 100
        assert table.file_size_bytes == 1024


@pytest.mark.unit
class TestCreateNameMapping:
    """Tests for create_name_mapping()."""

    def test_simple_schema(self):
        schema = {
            "fields": [
                {"id": 1, "name": "col1", "type": "string"},
                {"id": 2, "name": "col2", "type": "long"},
            ]
        }
        result = json.loads(create_name_mapping(schema))
        assert len(result) == 2
        assert result[0] == {"field-id": 1, "names": ["col1"]}
        assert result[1] == {"field-id": 2, "names": ["col2"]}

    def test_nested_struct(self):
        schema = {
            "fields": [
                {
                    "id": 1,
                    "name": "address",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"id": 2, "name": "city", "type": "string"},
                        ],
                    },
                },
            ]
        }
        result = json.loads(create_name_mapping(schema))
        assert len(result) == 1
        assert result[0]["field-id"] == 1
        assert "fields" in result[0]
        assert result[0]["fields"][0]["field-id"] == 2


@pytest.mark.unit
class TestCreateTableMetadata:
    """Tests for create_table_metadata()."""

    def test_basic_metadata(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        iceberg_schema = _arrow_schema_to_iceberg(schema)
        table = IcebergTable(
            name="test",
            parquet_path="data/test/test.parquet",
            schema=iceberg_schema,
            arrow_schema=schema,
            num_rows=10,
            file_size_bytes=512,
        )
        metadata = create_table_metadata(table, "https://example.com")
        assert metadata["format-version"] == 2
        assert metadata["location"] == "https://example.com/data/test"
        assert metadata["current-snapshot-id"] == 1
        assert len(metadata["schemas"]) == 1
        assert len(metadata["snapshots"]) == 1

    def test_custom_table_path(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        iceberg_schema = _arrow_schema_to_iceberg(schema)
        table = IcebergTable(
            name="test",
            parquet_path="data/test/test.parquet",
            schema=iceberg_schema,
            arrow_schema=schema,
            num_rows=10,
            file_size_bytes=512,
        )
        metadata = create_table_metadata(table, "https://example.com", table_path="ns/test")
        assert metadata["location"] == "https://example.com/data/ns/test"


@pytest.mark.unit
class TestAddIcebergFieldIds:
    """Tests for add_iceberg_field_ids()."""

    def test_adds_field_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            pq.write_table(table, path)

            add_iceberg_field_ids(path)

            # Read back and check field IDs
            pf = pq.ParquetFile(path)
            for field in pf.schema_arrow:
                assert field.metadata is not None
                assert b"PARQUET:field_id" in field.metadata


@pytest.mark.unit
class TestParquetToIcebergTable:
    """Tests for parquet_to_iceberg_table()."""

    def test_basic_conversion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            table = pa.table({"id": [1, 2], "value": [1.0, 2.0]})
            pq.write_table(table, path)

            iceberg_table = parquet_to_iceberg_table(str(path))
            assert iceberg_table.name == "test"
            assert iceberg_table.num_rows == 2
            assert iceberg_table.file_size_bytes > 0
            assert iceberg_table.schema["type"] == "struct"

    def test_custom_table_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            table = pa.table({"id": [1]})
            pq.write_table(table, path)

            iceberg_table = parquet_to_iceberg_table(str(path), table_name="my_table")
            assert iceberg_table.name == "my_table"

    def test_name_sanitization(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "My Test-File.parquet"
            table = pa.table({"id": [1]})
            pq.write_table(table, path)

            iceberg_table = parquet_to_iceberg_table(str(path))
            assert iceberg_table.name == "my_test_file"


@pytest.mark.unit
class TestGenerateManifestFiles:
    """Tests for generate_manifest_files()."""

    def test_generates_avro_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            schema = pa.schema([pa.field("id", pa.int64())])
            table = IcebergTable(
                name="test",
                parquet_path="data/test/test.parquet",
                schema=_arrow_schema_to_iceberg(schema),
                arrow_schema=schema,
                num_rows=10,
                file_size_bytes=512,
            )
            metadata_dir = Path(tmpdir) / "metadata"
            result = generate_manifest_files(table, "https://example.com", metadata_dir, schema)

            # Check manifest files were created
            assert (metadata_dir / "snap-1-manifest.avro").exists()
            assert (metadata_dir / "snap-1-manifest-list.avro").exists()
            assert "snap-1-manifest-list.avro" in result
