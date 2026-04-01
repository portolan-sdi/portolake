# API Reference

Portolake implements the `VersioningBackend` protocol from portolan-cli. Once installed, it is automatically discovered via the `portolan.backends` entry point.

Currently, Portolake provides the **IcebergBackend** for vector/tabular data. An Icechunk backend for array/raster data is planned per [ADR-0015](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0015-two-tier-versioning-architecture.md).

## CLI Usage

All versioning operations are available via `portolan version` commands when the backend is set to `iceberg`:

```bash
portolan config set backend iceberg

portolan version current demographics          # Show current version
portolan version list demographics             # List all versions
portolan version rollback demographics 1.0.0   # Rollback to v1.0.0
portolan version prune demographics --keep 5   # Prune old versions
portolan version prune demographics --keep 3 --dry-run  # Preview pruning
```

## Loading the Backend (Python API)

```python
from portolan_cli.backends import get_backend

# Load the Iceberg backend (requires portolake to be installed)
backend = get_backend("iceberg")
```

## IcebergBackend

::: portolake.backend.IcebergBackend
    options:
      show_source: false
      show_root_heading: true
      heading_level: 3
      members_order: source
      docstring_style: google

## Methods

### get_current_version

Get the current (latest) version of a collection.

```python
version = backend.get_current_version("demographics")
print(version.version)   # "2.1.0"
print(version.breaking)  # False
print(version.message)   # "Updated population data"
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `collection` | `str` | Collection identifier |

**Returns:** `Version` — the current version object.

**Raises:** `FileNotFoundError` if the collection has no versions.

---

### list_versions

List all versions of a collection, ordered oldest to newest.

```python
versions = backend.list_versions("demographics")
for v in versions:
    print(f"{v.version} ({v.created}): {v.message}")
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `collection` | `str` | Collection identifier |

**Returns:** `list[Version]` — versions ordered oldest first. Empty list if collection doesn't exist.

---

### publish

Publish a new version of a collection. Creates the underlying Iceberg table on first publish.

```python
version = backend.publish(
    collection="demographics",
    assets={"data.parquet": "/path/to/data.parquet"},
    schema={
        "columns": ["id", "geom", "population"],
        "types": {"id": "int64", "geom": "geometry", "population": "int64"},
        "hash": "abc123",
    },
    breaking=False,
    message="Updated population estimates",
)
print(version.version)  # "1.1.0" (minor bump)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `collection` | `str` | Collection identifier |
| `assets` | `dict[str, str]` | Mapping of asset names to file paths or URIs |
| `schema` | `SchemaFingerprint` | Schema fingerprint (`columns`, `types`, `hash`) |
| `breaking` | `bool` | Whether this is a breaking change |
| `message` | `str` | Human-readable description |

**Returns:** `Version` — the newly created version.

**Versioning rules:**

| Scenario | Version |
|----------|---------|
| First version (any collection) | `1.0.0` |
| Non-breaking change | Minor bump (`1.0.0` -> `1.1.0`) |
| Breaking change | Major bump (`1.2.3` -> `2.0.0`) |

---

### rollback

Roll back to a previous version. Uses Iceberg's native snapshot management to set the current snapshot pointer back to the target version. No data is copied — this is instant.

```python
# Roll back to version 1.0.0
rolled = backend.rollback("demographics", "1.0.0")
print(rolled.version)  # "1.0.0" (restored, not a new version)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `collection` | `str` | Collection identifier |
| `target_version` | `str` | Semantic version string to roll back to |

**Returns:** `Version` — the restored version.

**Raises:** `ValueError` if the target version doesn't exist.

---

### prune

Remove old versions, keeping the N most recent. Supports dry-run mode to preview what would be deleted.

```python
# Preview what would be pruned
prunable = backend.prune("demographics", keep=5, dry_run=True)
print(f"Would prune {len(prunable)} versions")

# Actually prune
pruned = backend.prune("demographics", keep=5, dry_run=False)
print(f"Pruned {len(pruned)} versions")
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `collection` | `str` | Collection identifier |
| `keep` | `int` | Number of most recent versions to keep |
| `dry_run` | `bool` | If `True`, report without deleting |

**Returns:** `list[Version]` — versions that were (or would be) deleted.

---

### check_drift

Check for drift between local and remote state. Currently a stub that reports no drift.

```python
report = backend.check_drift("demographics")
print(report["has_drift"])       # False
print(report["local_version"])   # "2.1.0"
print(report["message"])         # "Drift detection pending sync implementation"
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `collection` | `str` | Collection identifier |

**Returns:** `DriftReport` — dict with `has_drift`, `local_version`, `remote_version`, `message`.

## Data Types

These types are defined in `portolan_cli.versions` and used throughout the API:

### Version

| Field | Type | Description |
|-------|------|-------------|
| `version` | `str` | Semantic version string (e.g., `"1.2.0"`) |
| `created` | `datetime` | UTC timestamp |
| `breaking` | `bool` | Whether this is a breaking change |
| `assets` | `dict[str, Asset]` | Mapping of filename to Asset |
| `changes` | `list[str]` | Filenames that changed |
| `schema` | `SchemaInfo \| None` | Schema fingerprint |
| `message` | `str \| None` | Human-readable description |

### Asset

| Field | Type | Description |
|-------|------|-------------|
| `sha256` | `str` | SHA-256 checksum |
| `size_bytes` | `int` | File size in bytes |
| `href` | `str` | Path or URI to the asset |

### SchemaInfo

| Field | Type | Description |
|-------|------|-------------|
| `type` | `str` | Schema type identifier |
| `fingerprint` | `dict` | Type-specific schema fingerprint |

## How It Works

### Versioning: semver on top of Iceberg snapshots

Iceberg natively identifies versions by **snapshot ID** — a 64-bit integer (e.g., `4116805692239496156`). Portolake adds a **semantic versioning layer on top** by storing a semver string in each snapshot's summary properties.

Each Iceberg snapshot has a `summary` — a string key-value map for custom metadata. Portolake writes version info there:

```python
# What portolake stores in each snapshot's summary:
{
    "portolake.version": "1.1.0",
    "portolake.breaking": "false",
    "portolake.message": "Updated population data",
    "portolake.assets": '{"data.parquet": {"sha256": "...", "size_bytes": 4500, "href": "boundaries/data.parquet"}}',
    "portolake.schema": '{"type": "geoparquet", "fingerprint": {...}}',
    "portolake.changes": '["data.parquet"]'
}
```

This means version metadata is stored **inside Iceberg's own metadata** — in the `.metadata.json` and manifest files in the warehouse. There is no external `versions.json` or separate database. The version info travels with the Iceberg table.

You can inspect it directly with PyIceberg:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("portolake")
table = catalog.load_table("portolake.boundaries")

for snap in table.snapshots():
    props = snap.summary.additional_properties
    print(f"Snapshot {snap.snapshot_id}")                # Iceberg native ID
    print(f"  Version: {props['portolake.version']}")    # Portolake semver
    print(f"  Message: {props.get('portolake.message')}")
```

### Why semver instead of snapshot IDs?

The semver overlay exists because portolan-cli's `Version` dataclass uses semantic versioning across all backends. The file backend stores semver in `versions.json`; the Iceberg backend stores it in snapshot properties. This keeps the CLI consistent — `portolan version rollback boundaries 1.0.0` is more user-friendly than a 19-digit snapshot ID.

### Snapshot summary properties

| Property | Example | Description |
|----------|---------|-------------|
| `portolake.version` | `"1.2.0"` | Semantic version string |
| `portolake.breaking` | `"false"` | Whether this is a breaking change |
| `portolake.message` | `"Updated data"` | Human-readable description |
| `portolake.assets` | JSON string | Asset metadata (sha256, size, href) |
| `portolake.schema` | JSON string | Schema fingerprint |
| `portolake.changes` | JSON string | List of changed filenames |

### Collection-to-table mapping

Each collection maps to an Iceberg table under the `portolake` namespace: `portolake.<collection_name>`.

For example, `portolan add data.parquet --collection boundaries` creates an Iceberg table `portolake.boundaries`.
