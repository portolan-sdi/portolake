"""STAC metadata generation from Iceberg table state.

Layer 1 (Phase 3): STAC Table Extension fields (table:columns, table:row_count,
table:primary_geometry) extracted from Iceberg table schema and manifests.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)

if TYPE_CHECKING:
    from pyiceberg.table import Table

# Columns added by portolake spatial processing — exclude from table:columns
_DERIVED_PREFIXES = ("geohash_", "bbox_")

# Map Iceberg types to STAC Table Extension type strings
_TYPE_MAP: dict[type, str] = {
    LongType: "int64",
    IntegerType: "int32",
    DoubleType: "float64",
    FloatType: "float32",
    StringType: "string",
    BooleanType: "boolean",
    BinaryType: "binary",
    DateType: "date",
    TimestampType: "datetime",
    TimestamptzType: "datetime",
}


def _iceberg_type_to_str(iceberg_type: Any) -> str:
    """Convert an Iceberg type to a STAC-friendly type string."""
    return _TYPE_MAP.get(type(iceberg_type), str(iceberg_type))


def _is_derived_column(name: str) -> bool:
    """Check if a column name is a portolake-derived spatial column."""
    return any(name.startswith(prefix) for prefix in _DERIVED_PREFIXES)


def _detect_primary_geometry(field_names: list[str]) -> str | None:
    """Detect the primary geometry column name."""
    for name in ("geometry", "geom"):
        if name in field_names:
            return name
    return None


def generate_table_metadata(table: Table) -> dict[str, Any]:
    """Generate STAC Table Extension fields from an Iceberg table.

    Returns a dict with:
        - table:columns: list of {name, type} dicts
        - table:row_count: total rows in current snapshot
        - table:primary_geometry: geometry column name or None
    """
    schema = table.schema()

    # Build table:columns (excluding derived spatial columns)
    columns = []
    field_names = []
    for field in schema.fields:
        field_names.append(field.name)
        if _is_derived_column(field.name):
            continue
        columns.append(
            {
                "name": field.name,
                "type": _iceberg_type_to_str(field.field_type),
            }
        )

    # Get row count from scanning current snapshot
    row_count = len(table.scan().to_arrow())

    # Detect primary geometry
    primary_geometry = _detect_primary_geometry(field_names)

    return {
        "table:columns": columns,
        "table:row_count": row_count,
        "table:primary_geometry": primary_geometry,
    }
