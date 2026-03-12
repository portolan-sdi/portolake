"""Catalog configuration for Portolake.

Uses PyIceberg's load_catalog() for catalog-type-agnostic initialization.
Defaults to local SQL/SQLite. Users override via standard PyIceberg env vars:
  PYICEBERG_CATALOG__PORTOLAKE__TYPE=rest
  PYICEBERG_CATALOG__PORTOLAKE__URI=https://my-rest-catalog.example.com
  PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse

See: https://py.iceberg.apache.org/configuration/
"""

from __future__ import annotations

import os
from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog

CATALOG_NAME = "portolake"


def _default_properties() -> dict[str, str]:
    """Build default catalog properties (SQL/SQLite, local warehouse)."""
    cwd = os.getcwd()
    return {
        "type": "sql",
        "uri": f"sqlite:///{cwd}/.portolake/catalog.db",
        "warehouse": f"file:///{cwd}/.portolake/warehouse",
    }


def create_catalog() -> Catalog:
    """Create an Iceberg catalog using PyIceberg's load_catalog().

    Defaults to local SQL/SQLite. Users can override via standard PyIceberg
    environment variables (PYICEBERG_CATALOG__PORTOLAKE__*).
    """
    defaults = _default_properties()
    # Ensure the default catalog directory exists for SQLite
    uri = defaults["uri"]
    if uri.startswith("sqlite:///"):
        db_path = Path(uri.removeprefix("sqlite:///"))
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return load_catalog(CATALOG_NAME, **defaults)
