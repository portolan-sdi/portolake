"""Spatial utilities for geohash partitioning and bbox computation.

Adds geohash columns for Iceberg partition specs and per-row bounding box
columns for manifest-level min/max statistics.
"""

from __future__ import annotations

import pyarrow as pa
import pygeohash as pgh
import shapely


def detect_geohash_precision(row_count: int) -> int | None:
    """Determine geohash precision based on dataset size.

    Returns:
        None if row_count < 100K (no partitioning needed).
        3 (~150km cells) if row_count < 10M.
        4 (~20km cells) if row_count >= 10M.
    """
    if row_count < 100_000:
        return None
    if row_count < 10_000_000:
        return 3
    return 4


def _find_geometry_column(table: pa.Table) -> str | None:
    """Find the geometry column in a PyArrow table.

    Looks for columns named 'geometry' or 'geom' with binary type.
    """
    for name in ("geometry", "geom"):
        if name in table.column_names:
            col = table.column(name)
            if pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
                return name
    return None


def compute_geohash_column(table: pa.Table, precision: int = 4) -> pa.Table:
    """Add a geohash_{precision} column computed from geometry centroids.

    If the geometry column doesn't exist, returns the table unchanged.
    """
    col_name = _find_geometry_column(table)
    if col_name is None:
        return table

    wkb_array = table.column(col_name)
    geohashes = []
    for wkb_value in wkb_array:
        wkb_bytes = wkb_value.as_py()
        geom = shapely.from_wkb(wkb_bytes)
        centroid = shapely.centroid(geom)
        x, y = shapely.get_coordinates(centroid)[0]
        # pygeohash expects (latitude, longitude)
        geohashes.append(pgh.encode(y, x, precision=precision))

    geohash_col_name = f"geohash_{precision}"
    return table.append_column(geohash_col_name, pa.array(geohashes, type=pa.string()))


def compute_bbox_columns(table: pa.Table) -> pa.Table:
    """Add per-row bbox columns (xmin, ymin, xmax, ymax) from geometry bounds.

    If the geometry column doesn't exist, returns the table unchanged.
    """
    col_name = _find_geometry_column(table)
    if col_name is None:
        return table

    wkb_array = table.column(col_name)
    xmins, ymins, xmaxs, ymaxs = [], [], [], []

    for wkb_value in wkb_array:
        wkb_bytes = wkb_value.as_py()
        geom = shapely.from_wkb(wkb_bytes)
        bounds = shapely.bounds(geom)  # (xmin, ymin, xmax, ymax)
        xmins.append(bounds[0])
        ymins.append(bounds[1])
        xmaxs.append(bounds[2])
        ymaxs.append(bounds[3])

    table = table.append_column("bbox_xmin", pa.array(xmins, type=pa.float64()))
    table = table.append_column("bbox_ymin", pa.array(ymins, type=pa.float64()))
    table = table.append_column("bbox_xmax", pa.array(xmaxs, type=pa.float64()))
    table = table.append_column("bbox_ymax", pa.array(ymaxs, type=pa.float64()))

    return table


def add_spatial_columns(table: pa.Table, precision: int | None = None) -> pa.Table:
    """Add geohash and bbox columns if the table has geometry.

    If precision is None, auto-detect from row count.
    If the table has no geometry column, returns it unchanged.
    """
    if _find_geometry_column(table) is None:
        return table

    if precision is None:
        precision = detect_geohash_precision(len(table))

    # Always add bbox columns for manifest statistics
    table = compute_bbox_columns(table)

    # Add geohash column (use default precision 4 for small datasets,
    # detected precision for larger ones)
    geohash_precision = precision if precision is not None else 4
    table = compute_geohash_column(table, precision=geohash_precision)

    return table
