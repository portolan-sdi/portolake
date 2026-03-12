"""Portolake: Lakehouse-grade versioning for Portolan catalogs.

This package provides enterprise-tier versioning for geospatial catalogs using
Apache Iceberg for vector/tabular data (GeoParquet format).

It integrates with portolan-cli as a plugin backend, providing:
- ACID transactions for concurrent writes
- Version rollback and snapshot pruning
- Automated schema evolution detection

Future: Icechunk support for array/raster data (COG, NetCDF, Zarr) is planned
per ADR-0015.

See: https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0015-two-tier-versioning-architecture.md
"""

from __future__ import annotations

__version__ = "0.1.0"

from portolake.backend import IcebergBackend

__all__ = ["__version__", "IcebergBackend"]
