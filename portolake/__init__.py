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
        """Get the current (latest) version of a collection.

        Args:
            _collection: Collection identifier/path.

        Returns:
            The current Version object.

        Raises:
            NotImplementedError: Method not yet implemented.
        """
        raise NotImplementedError("IcebergBackend.get_current_version not yet implemented")

    def list_versions(self, _collection: str) -> list[Any]:
        """List all versions of a collection, oldest first.

        Args:
            _collection: Collection identifier/path.

        Returns:
            List of Version objects, ordered oldest to newest.

        Raises:
            NotImplementedError: Method not yet implemented.
        """
        raise NotImplementedError("IcebergBackend.list_versions not yet implemented")

    def publish(
        self,
        _collection: str,
        _assets: dict[str, str],
        _schema: dict[str, Any],
        _breaking: bool,
        _message: str,
    ) -> Any:
        """Publish a new version of a collection.

        Args:
            _collection: Collection identifier/path.
            _assets: Mapping of asset names to asset paths/URIs.
            _schema: Schema fingerprint for change detection.
            _breaking: Whether this is a breaking change.
            _message: Human-readable description of the change.

        Returns:
            The newly created Version object.

        Raises:
            NotImplementedError: Method not yet implemented.
        """
        raise NotImplementedError("IcebergBackend.publish not yet implemented")

    def rollback(self, _collection: str, _target_version: str) -> Any:
        """Rollback to a previous version.

        Creates a NEW version with the contents of the target version,
        preserving full history.

        Args:
            _collection: Collection identifier/path.
            _target_version: Semantic version string to roll back to.

        Returns:
            The newly created Version object (representing the rollback).

        Raises:
            NotImplementedError: Method not yet implemented.
        """
        raise NotImplementedError("IcebergBackend.rollback not yet implemented")

    def prune(self, _collection: str, _keep: int, _dry_run: bool) -> list[Any]:
        """Remove old versions, keeping the N most recent.

        Args:
            _collection: Collection identifier/path.
            _keep: Number of recent versions to keep.
            _dry_run: If True, don't delete, just report what would be deleted.

        Returns:
            List of Version objects that were (or would be) deleted.

        Raises:
            NotImplementedError: Method not yet implemented.
        """
        raise NotImplementedError("IcebergBackend.prune not yet implemented")

    def check_drift(self, _collection: str) -> dict[str, Any]:
        """Check for drift between local and remote state.

        Args:
            _collection: Collection identifier/path.

        Returns:
            DriftReport with drift status and details.

        Raises:
            NotImplementedError: Method not yet implemented.
        """
        raise NotImplementedError("IcebergBackend.check_drift not yet implemented")


__all__ = ["__version__", "IcebergBackend"]
