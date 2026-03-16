"""Catalog configuration for Portolake.

Uses PyIceberg's load_catalog() for catalog-type-agnostic initialization.
Defaults to local SQL/SQLite. Users override via standard PyIceberg env vars:
  PYICEBERG_CATALOG__PORTOLAKE__TYPE=rest
  PYICEBERG_CATALOG__PORTOLAKE__URI=https://my-rest-catalog.example.com
  PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse

See: https://py.iceberg.apache.org/configuration/
"""

from __future__ import annotations

from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog

CATALOG_NAME = "portolake"


def _default_properties(catalog_root: Path | None = None) -> dict[str, str]:
    """Build default catalog properties (SQL/SQLite, local warehouse)."""
    root = str(catalog_root or Path.cwd())
    return {
        "type": "sql",
        "uri": f"sqlite:///{root}/.portolan/iceberg.db",
        "warehouse": f"file:///{root}/.portolan/warehouse",
    }


def _get_external_config() -> dict[str, str] | None:
    """Get PyIceberg config for this catalog (YAML or env vars), if any."""
    from pyiceberg.utils.config import Config

    config = Config()
    return config.get_catalog_config(CATALOG_NAME)


def create_catalog(catalog_root: Path | None = None) -> Catalog:
    """Create an Iceberg catalog using PyIceberg's load_catalog().

    If external configuration exists (YAML file or PYICEBERG_CATALOG__PORTOLAKE__*
    env vars), those settings take precedence. Local SQL/SQLite defaults are used
    as fallback for any properties not specified externally.
    """
    external = _get_external_config()
    if external and "type" in external and external["type"] != "sql":
        # Non-SQLite catalog (e.g., REST/BigLake) — use external config only
        return load_catalog(CATALOG_NAME)

    # SQLite or no external config — use defaults (external config, if any,
    # still overrides via PyIceberg's merge_config inside load_catalog)
    defaults = _default_properties(catalog_root)
    uri = defaults["uri"]
    if uri.startswith("sqlite:///"):
        db_path = Path(uri.removeprefix("sqlite:///"))
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return load_catalog(CATALOG_NAME, **defaults)
