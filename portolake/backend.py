"""IcebergBackend: enterprise versioning backend using Apache Iceberg.

Implements the VersioningBackend protocol from portolan-cli, storing version
metadata in Iceberg snapshot summary properties.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint
from portolan_cli.versions import Asset, SchemaInfo, Version
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Transaction
from pyiceberg.table.update.snapshot import ExpireSnapshots
from pyiceberg.types import LongType, NestedField, StringType

from portolake.config import create_catalog
from portolake.versioning import (
    build_assets,
    compute_next_version,
    snapshot_to_version,
    version_to_snapshot_properties,
)

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

# Minimal Iceberg schema for the asset metadata table
ASSET_SCHEMA = IcebergSchema(
    NestedField(1, "asset_name", StringType(), required=True),
    NestedField(2, "href", StringType(), required=True),
    NestedField(3, "sha256", StringType(), required=True),
    NestedField(4, "size_bytes", LongType(), required=True),
)

NAMESPACE = "portolake"


class IcebergBackend:
    """Enterprise versioning backend using Apache Iceberg.

    Implements the VersioningBackend protocol from portolan-cli.
    Discovered via the 'portolan.backends' entry point.

    Configuration via PyIceberg env vars:
        PYICEBERG_CATALOG__PORTOLAKE__TYPE=sql (default)
        PYICEBERG_CATALOG__PORTOLAKE__URI=sqlite:///...
        PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=file:///...
    """

    def __init__(self, catalog: Catalog | None = None, catalog_root: Path | None = None) -> None:
        self._catalog: Catalog = catalog if catalog is not None else create_catalog(catalog_root)
        try:
            self._catalog.create_namespace(NAMESPACE)
        except Exception:
            pass  # Already exists

    def _validate_collection(self, collection: str) -> str:
        """Validate and sanitize collection name."""
        if not collection or not collection.strip():
            raise ValueError("Collection name cannot be empty")
        # Reject paths containing traversal components
        if ".." in collection or "/" in collection or "\\" in collection:
            raise ValueError(f"Invalid collection name: {collection!r}")
        safe = Path(collection).name
        if safe in ("", ".", ".."):
            raise ValueError(f"Invalid collection name: {collection!r}")
        return safe

    def _table_id(self, collection: str) -> str:
        return f"{NAMESPACE}.{self._validate_collection(collection)}"

    def _load_or_create_table(self, table_id: str) -> Table:
        """Load an existing table or create a new one."""
        try:
            return self._catalog.load_table(table_id)
        except NoSuchTableError:
            return self._catalog.create_table(table_id, schema=ASSET_SCHEMA)

    def _get_current_version_str(self, table: Table) -> str | None:
        """Extract portolake.version from the current snapshot, if any."""
        snap = table.current_snapshot()
        if snap is None or snap.summary is None:
            return None
        return snap.summary.additional_properties.get("portolake.version")

    def get_current_version(self, collection: str) -> Version:
        """Get the current (latest) version of a collection."""
        table_id = self._table_id(collection)
        try:
            table = self._catalog.load_table(table_id)
        except NoSuchTableError as exc:
            raise FileNotFoundError(f"No versions found for collection: {collection}") from exc
        snap = table.current_snapshot()
        if snap is None:
            raise FileNotFoundError(f"No versions found for collection: {collection}")
        return snapshot_to_version(snap)

    def list_versions(self, collection: str) -> list[Version]:
        """List all versions of a collection, oldest first."""
        table_id = self._table_id(collection)
        try:
            table = self._catalog.load_table(table_id)
        except NoSuchTableError:
            return []
        versions = []
        for snap in table.snapshots():
            if snap.summary and "portolake.version" in snap.summary.additional_properties:
                versions.append(snapshot_to_version(snap))
        return sorted(versions, key=lambda v: v.created)

    def publish(
        self,
        collection: str,
        assets: dict[str, str],
        schema: SchemaFingerprint,
        breaking: bool,
        message: str,
        removed: set[str] | None = None,
    ) -> Version:
        """Publish a new version of a collection."""
        table_id = self._table_id(collection)
        table = self._load_or_create_table(table_id)

        current_version = self._get_current_version_str(table)
        next_ver = compute_next_version(current_version, breaking)

        # Build new assets from input paths
        new_asset_objects, changes = build_assets(assets, collection=collection)

        # Merge with previous snapshot's assets if we have history
        merged_assets = {}
        snap = table.current_snapshot()
        if snap is not None and snap.summary is not None:
            prev_version = snapshot_to_version(snap)
            merged_assets.update(prev_version.assets)

        # Apply new assets (overwrite existing)
        merged_assets.update(new_asset_objects)

        # Remove requested assets
        if removed:
            for key in removed:
                merged_assets.pop(key, None)

        schema_info = SchemaInfo(
            type=schema.get("hash", "unknown"),
            fingerprint={
                "columns": schema.get("columns", []),
                "types": schema.get("types", {}),
            },
        )

        props = version_to_snapshot_properties(
            next_ver, breaking, message, merged_assets, schema_info, changes
        )

        arrow_table = _build_arrow_table(merged_assets)
        table.append(arrow_table, snapshot_properties=props)

        # Reload to get the committed snapshot
        table = self._catalog.load_table(table_id)
        return snapshot_to_version(table.current_snapshot())

    def rollback(self, collection: str, target_version: str) -> Version:
        """Rollback to a previous version.

        Creates a NEW version with the target version's assets,
        preserving full history.
        """
        table_id = self._table_id(collection)
        try:
            table = self._catalog.load_table(table_id)
        except NoSuchTableError as exc:
            raise FileNotFoundError(f"No versions found for collection: {collection}") from exc

        # Find the snapshot matching target_version
        target_snap = None
        for snap in table.snapshots():
            if (
                snap.summary
                and snap.summary.additional_properties.get("portolake.version") == target_version
            ):
                target_snap = snap
                break

        if target_snap is None:
            raise ValueError(f"Version {target_version} not found in collection: {collection}")

        # Extract the target version's data
        target_ver = snapshot_to_version(target_snap)

        # Compute next version (non-breaking rollback)
        current_version = self._get_current_version_str(table)
        next_ver = compute_next_version(current_version, breaking=False)

        rollback_message = f"Rollback to {target_version}"

        props = version_to_snapshot_properties(
            next_ver,
            breaking=False,
            message=rollback_message,
            assets=target_ver.assets,
            schema=target_ver.schema,
            changes=list(target_ver.assets.keys()),
        )

        # Write new snapshot with the target's asset data
        arrow_table = _build_arrow_table(target_ver.assets)
        table.append(arrow_table, snapshot_properties=props)

        table = self._catalog.load_table(table_id)
        return snapshot_to_version(table.current_snapshot())

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        """Remove old versions, keeping the N most recent."""
        table_id = self._table_id(collection)
        try:
            table = self._catalog.load_table(table_id)
        except NoSuchTableError:
            return []

        # Get all portolake versions sorted by timestamp
        versioned_snapshots = []
        for snap in table.snapshots():
            if snap.summary and "portolake.version" in snap.summary.additional_properties:
                versioned_snapshots.append(snap)
        versioned_snapshots.sort(key=lambda s: s.timestamp_ms)

        if len(versioned_snapshots) <= keep:
            return []

        # Identify snapshots to prune (oldest ones beyond keep)
        to_prune = versioned_snapshots[: len(versioned_snapshots) - keep]
        pruned_versions = [snapshot_to_version(s) for s in to_prune]

        if not dry_run:
            snapshot_ids = [s.snapshot_id for s in to_prune]
            expire = ExpireSnapshots(Transaction(table, autocommit=True))
            expire.by_ids(snapshot_ids).commit()

        return pruned_versions

    def check_drift(self, collection: str) -> DriftReport:
        """Check for drift between local and remote state.

        Currently a stub (same as JsonFileBackend).
        """
        table_id = self._table_id(collection)
        current = None
        try:
            table = self._catalog.load_table(table_id)
            current = self._get_current_version_str(table)
        except NoSuchTableError:
            pass

        return DriftReport(
            has_drift=False,
            local_version=current,
            remote_version=current,
            message="Drift detection pending sync implementation",
        )


def _build_arrow_table(assets: dict[str, Asset]) -> pa.Table:
    """Build a PyArrow table of asset metadata for Iceberg storage."""
    names = list(assets.keys())
    hrefs = [a.href for a in assets.values()]
    sha256s = [a.sha256 for a in assets.values()]
    sizes = [a.size_bytes for a in assets.values()]

    # Iceberg schema has required (non-nullable) fields, so Arrow arrays must match
    schema = pa.schema(
        [
            pa.field("asset_name", pa.string(), nullable=False),
            pa.field("href", pa.string(), nullable=False),
            pa.field("sha256", pa.string(), nullable=False),
            pa.field("size_bytes", pa.int64(), nullable=False),
        ]
    )
    return pa.table(
        {
            "asset_name": names,
            "href": hrefs,
            "sha256": sha256s,
            "size_bytes": sizes,
        },
        schema=schema,
    )
