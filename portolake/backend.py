"""IcebergBackend: enterprise versioning backend using Apache Iceberg.

Implements the VersioningBackend protocol from portolan-cli, storing actual
data in Iceberg tables and version metadata in snapshot summary properties.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint
from portolan_cli.versions import Asset, SchemaInfo, Version
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Transaction
from pyiceberg.table.update.snapshot import ExpireSnapshots

from portolake.config import create_catalog
from portolake.spatial import add_spatial_columns
from portolake.versioning import (
    build_assets,
    compute_next_version,
    snapshot_to_version,
    version_to_snapshot_properties,
)

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

NAMESPACE = "portolake"


class IcebergBackend:
    """Enterprise versioning backend using Apache Iceberg.

    Implements the VersioningBackend protocol from portolan-cli.
    Discovered via the 'portolan.backends' entry point.

    Data is stored natively in Iceberg tables (copy-on-write). Version
    metadata is stored in snapshot summary properties.
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
        if ".." in collection or "/" in collection or "\\" in collection:
            raise ValueError(f"Invalid collection name: {collection!r}")
        safe = Path(collection).name
        if safe in ("", ".", ".."):
            raise ValueError(f"Invalid collection name: {collection!r}")
        return safe

    def _table_id(self, collection: str) -> str:
        return f"{NAMESPACE}.{self._validate_collection(collection)}"

    def _load_or_create_table(self, table_id: str, arrow_schema: pa.Schema) -> Table:
        """Load an existing table or create one with the given schema."""
        try:
            table = self._catalog.load_table(table_id)
            # Schema evolution: add new columns if needed
            with table.update_schema() as update:
                update.union_by_name(table.schema().as_arrow())
                update.union_by_name(arrow_schema)
            return self._catalog.load_table(table_id)
        except NoSuchTableError:
            return self._catalog.create_table(table_id, schema=arrow_schema)

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
        """Publish a new version of a collection.

        Reads actual Parquet data from asset files and writes it into the
        Iceberg table. Version metadata is stored in snapshot properties.
        """
        table_id = self._table_id(collection)

        current_version = self._get_current_version_str_safe(table_id)
        next_ver = compute_next_version(current_version, breaking)

        # Build asset metadata (sha256, size, href)
        new_asset_objects, changes = build_assets(assets, collection=collection)

        # Merge with previous snapshot's assets if we have history
        merged_assets = self._get_merged_assets(table_id, new_asset_objects, removed)

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

        # Read actual Parquet data from asset files
        arrow_data = _read_parquet_assets(assets)

        if arrow_data is not None:
            table = self._load_or_create_table(table_id, arrow_data.schema)
            table.append(arrow_data, snapshot_properties=props)
        else:
            # No parquet data to ingest (e.g., only removals)
            table = self._load_or_create_table_from_existing(table_id)
            table.append(_empty_table(table.schema().as_arrow()), snapshot_properties=props)

        # Reload to get the committed snapshot
        table = self._catalog.load_table(table_id)
        return snapshot_to_version(table.current_snapshot())

    def _get_current_version_str_safe(self, table_id: str) -> str | None:
        """Get current version string, returning None if table doesn't exist."""
        try:
            table = self._catalog.load_table(table_id)
            return self._get_current_version_str(table)
        except NoSuchTableError:
            return None

    def _get_merged_assets(
        self,
        table_id: str,
        new_assets: dict[str, Asset],
        removed: set[str] | None,
    ) -> dict[str, Asset]:
        """Merge new assets with previous version's assets."""
        merged: dict[str, Asset] = {}
        try:
            table = self._catalog.load_table(table_id)
            snap = table.current_snapshot()
            if snap is not None and snap.summary is not None:
                prev_version = snapshot_to_version(snap)
                merged.update(prev_version.assets)
        except NoSuchTableError:
            pass

        merged.update(new_assets)

        if removed:
            for key in removed:
                merged.pop(key, None)

        return merged

    def _load_or_create_table_from_existing(self, table_id: str) -> Table:
        """Load an existing table (for operations that don't have new data)."""
        return self._catalog.load_table(table_id)

    def rollback(self, collection: str, target_version: str) -> Version:
        """Rollback to a previous version.

        Creates a NEW version with the target version's data,
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

        target_ver = snapshot_to_version(target_snap)

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

        # Read data from the target snapshot and write as new snapshot
        target_data = table.scan(snapshot_id=target_snap.snapshot_id).to_arrow()
        if len(target_data) > 0:
            table.append(target_data, snapshot_properties=props)
        else:
            table.append(_empty_table(table.schema().as_arrow()), snapshot_properties=props)

        table = self._catalog.load_table(table_id)
        return snapshot_to_version(table.current_snapshot())

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        """Remove old versions, keeping the N most recent."""
        table_id = self._table_id(collection)
        try:
            table = self._catalog.load_table(table_id)
        except NoSuchTableError:
            return []

        versioned_snapshots = []
        for snap in table.snapshots():
            if snap.summary and "portolake.version" in snap.summary.additional_properties:
                versioned_snapshots.append(snap)
        versioned_snapshots.sort(key=lambda s: s.timestamp_ms)

        if len(versioned_snapshots) <= keep:
            return []

        to_prune = versioned_snapshots[: len(versioned_snapshots) - keep]
        pruned_versions = [snapshot_to_version(s) for s in to_prune]

        if not dry_run:
            snapshot_ids = [s.snapshot_id for s in to_prune]
            expire = ExpireSnapshots(Transaction(table, autocommit=True))
            expire.by_ids(snapshot_ids).commit()

        return pruned_versions

    def check_drift(self, collection: str) -> DriftReport:
        """Check for drift between local and remote state."""
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


def _read_parquet_assets(assets: dict[str, str]) -> pa.Table | None:
    """Read Parquet data from asset file paths, concatenate, and add spatial columns."""
    tables = []
    for path_str in assets.values():
        path = Path(path_str)
        if path.exists() and path.suffix == ".parquet":
            try:
                tables.append(pq.read_table(path))
            except Exception:
                pass  # Not a valid parquet file, skip data ingestion

    if not tables:
        return None

    if len(tables) == 1:
        result = tables[0]
    else:
        result = pa.concat_tables(tables, promote_options="default")

    # Add spatial columns (geohash + bbox) if geometry is present
    return add_spatial_columns(result)


def _empty_table(arrow_schema: pa.Schema) -> pa.Table:
    """Create an empty PyArrow table with the given schema."""
    return pa.table(
        {field.name: pa.array([], type=field.type) for field in arrow_schema}, schema=arrow_schema
    )
