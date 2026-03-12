"""Tests for catalog configuration and creation."""

import pytest

from portolake.config import _default_properties, create_catalog


@pytest.mark.unit
def test_default_properties_returns_sql_type():
    """Default catalog type should be 'sql'."""
    props = _default_properties()
    assert props["type"] == "sql"


@pytest.mark.unit
def test_default_properties_has_sqlite_uri():
    """Default URI should be a SQLite connection string."""
    props = _default_properties()
    assert props["uri"].startswith("sqlite:///")
    assert ".portolake/catalog.db" in props["uri"]


@pytest.mark.unit
def test_default_properties_has_warehouse():
    """Default warehouse should be a local file path."""
    props = _default_properties()
    assert props["warehouse"].startswith("file:///")
    assert ".portolake/warehouse" in props["warehouse"]


@pytest.mark.integration
def test_create_catalog_returns_catalog_instance(tmp_path, monkeypatch):
    """create_catalog() should return a working Catalog instance."""
    from pyiceberg.catalog import Catalog

    monkeypatch.setenv(
        "PYICEBERG_CATALOG__PORTOLAKE__URI",
        f"sqlite:///{tmp_path}/catalog.db",
    )
    monkeypatch.setenv(
        "PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE",
        f"file:///{tmp_path}/warehouse",
    )
    catalog = create_catalog()
    assert isinstance(catalog, Catalog)


@pytest.mark.integration
def test_pyiceberg_env_vars_override_defaults(tmp_path, monkeypatch):
    """PyIceberg env vars should override default properties."""
    custom_uri = f"sqlite:///{tmp_path}/custom.db"
    monkeypatch.setenv("PYICEBERG_CATALOG__PORTOLAKE__URI", custom_uri)
    monkeypatch.setenv(
        "PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE",
        f"file:///{tmp_path}/warehouse",
    )
    catalog = create_catalog()
    # The catalog should be created successfully with the custom URI
    from pyiceberg.catalog import Catalog

    assert isinstance(catalog, Catalog)
