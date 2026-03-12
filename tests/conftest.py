"""Shared test fixtures for portolake tests."""

import pytest
from pyiceberg.catalog import load_catalog


@pytest.fixture
def iceberg_catalog(tmp_path):
    """Create a temporary Iceberg catalog backed by SQLite."""
    return load_catalog(
        "test",
        **{
            "type": "sql",
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse": f"file:///{tmp_path}/warehouse",
        },
    )


@pytest.fixture
def iceberg_backend(iceberg_catalog):
    """Create an IcebergBackend using a temporary SQLite catalog."""
    from portolake.backend import IcebergBackend

    return IcebergBackend(catalog=iceberg_catalog)
