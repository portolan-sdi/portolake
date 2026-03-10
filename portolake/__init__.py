"""Portolake: Lakehouse-grade versioning for Portolan catalogs.

This package provides enterprise-tier versioning for geospatial catalogs using
Apache Iceberg (for vector data) and Icechunk (for array/raster data).

It integrates with portolan-cli as a plugin backend, providing:
- ACID transactions for concurrent writes
- Native time travel and version branching
- Automated schema evolution detection
- Garbage collection and snapshot management

See: https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0015-two-tier-versioning-architecture.md
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    # These types are only needed for type checking, not at runtime.
    # Actual implementation will import from portolan_cli.backends.protocol
    pass

# Core Iceberg metadata & types
from portolake.iceberg_metadata import (
    IcebergTable,
    _arrow_schema_to_iceberg,
    _arrow_to_iceberg_schema,
    _arrow_type_to_iceberg,
    _arrow_type_to_pyiceberg,
    add_iceberg_field_ids,
    create_name_mapping,
    create_table_metadata,
    generate_manifest_files,
    parquet_to_iceberg_table,
)

# REST catalog generation
from portolake.iceberg_rest_catalog import (
    create_catalog_config,
    create_load_table_response,
    create_namespace_detail,
    create_namespaces_list,
    create_tables_list,
    generate_static_catalog,
)

# Namespace utilities
from portolake.namespace_utils import (
    arcgis_folder_to_namespace,
    build_namespace_tree,
    namespace_depth,
    namespace_parts,
    namespace_to_iceberg,
    validate_namespace,
)

# SDI catalog (STAC + ISO 19115)
from portolake.sdi_catalog import (
    create_items_table,
    create_stac_iso_record,
    create_stac_iso_schema,
    detect_parquet_type,
    extract_geoparquet_metadata,
    extract_parquet_metadata,
    extract_raquet_metadata,
    generate_sdi_catalog,
)


class IcebergBackend:
    """Enterprise versioning backend using Apache Iceberg + Icechunk.

    Implements the VersioningBackend protocol from portolan-cli.
    This class is discovered via the 'portolan.backends' entry point.

    Note: All methods currently raise NotImplementedError. Actual
    implementation is tracked in separate issues.

    Example usage (once implemented):
        from portolan_cli.backends import get_backend
        backend = get_backend("iceberg")
        version = backend.get_current_version("my-collection")
    """

    def get_current_version(self, _collection: str) -> Any:
        """Get the current (latest) version of a collection."""
        raise NotImplementedError("IcebergBackend.get_current_version not yet implemented")

    def list_versions(self, _collection: str) -> list[Any]:
        """List all versions of a collection, oldest first."""
        raise NotImplementedError("IcebergBackend.list_versions not yet implemented")

    def publish(
        self,
        _collection: str,
        _assets: dict[str, str],
        _schema: dict[str, Any],
        _breaking: bool,
        _message: str,
    ) -> Any:
        """Publish a new version of a collection."""
        raise NotImplementedError("IcebergBackend.publish not yet implemented")

    def rollback(self, _collection: str, _target_version: str) -> Any:
        """Rollback to a previous version."""
        raise NotImplementedError("IcebergBackend.rollback not yet implemented")

    def prune(self, _collection: str, _keep: int, _dry_run: bool) -> list[Any]:
        """Remove old versions, keeping the N most recent."""
        raise NotImplementedError("IcebergBackend.prune not yet implemented")

    def check_drift(self, _collection: str) -> dict[str, Any]:
        """Check for drift between local and remote state."""
        raise NotImplementedError("IcebergBackend.check_drift not yet implemented")


__all__ = [
    "__version__",
    "IcebergBackend",
    # iceberg_metadata
    "IcebergTable",
    "_arrow_schema_to_iceberg",
    "_arrow_to_iceberg_schema",
    "_arrow_type_to_iceberg",
    "_arrow_type_to_pyiceberg",
    "add_iceberg_field_ids",
    "create_name_mapping",
    "create_table_metadata",
    "generate_manifest_files",
    "parquet_to_iceberg_table",
    # iceberg_rest_catalog
    "create_catalog_config",
    "create_load_table_response",
    "create_namespace_detail",
    "create_namespaces_list",
    "create_tables_list",
    "generate_static_catalog",
    # sdi_catalog
    "create_items_table",
    "create_stac_iso_record",
    "create_stac_iso_schema",
    "detect_parquet_type",
    "extract_geoparquet_metadata",
    "extract_parquet_metadata",
    "extract_raquet_metadata",
    "generate_sdi_catalog",
    # namespace_utils
    "arcgis_folder_to_namespace",
    "build_namespace_tree",
    "namespace_depth",
    "namespace_parts",
    "namespace_to_iceberg",
    "validate_namespace",
]
