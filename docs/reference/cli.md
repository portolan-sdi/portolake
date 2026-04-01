# CLI Commands

When Portolake is installed and the backend is set to `iceberg`, several portolan commands behave differently. This page documents all command behavior with the Iceberg backend and highlights differences from the default `file` backend.

## Setup

```bash
# Initialize a catalog with the Iceberg backend
portolan init my-catalog --backend iceberg
cd my-catalog

# Or switch an existing catalog
portolan config set backend iceberg
```

## Command reference

### portolan init

Creates a new catalog. With `--backend iceberg`, also creates the Iceberg catalog database and warehouse.

```bash
portolan init my-catalog --backend iceberg
portolan init my-catalog --backend iceberg --remote gs://my-bucket
```

| Backend | What's created |
|---------|---------------|
| `file` | `catalog.json`, `.portolan/config.yaml` |
| `iceberg` | `catalog.json`, `.portolan/config.yaml`, `.portolan/iceberg.db`, `.portolan/warehouse/` |

---

### portolan add

Adds geospatial data to a collection and records a new version.

```bash
portolan add boundaries/countries.geojson --collection boundaries
```

| Backend | Behavior |
|---------|----------|
| `file` | Converts file, writes locally, records version in `versions.json` |
| `iceberg` | Converts file, **ingests data into Iceberg table** (with spatial columns), records version as Iceberg snapshot, generates STAC metadata from Iceberg state |
| `iceberg` + remote | Same as above, plus **auto-uploads STAC metadata** (catalog.json, collection.json, item JSONs) to remote |

With the Iceberg backend, `add` also:

- Adds `geohash_{N}` and `bbox_*` spatial columns (vectorized shapely 2.0+)
- Creates an Iceberg partition spec on geohash for datasets >= 100K rows
- Evolves the schema automatically if new columns appear
- Updates `collection.json` with STAC Table Extension (`table:*`) and Iceberg Extension (`iceberg:*`) fields

---

### portolan push

Uploads catalog data to remote storage.

```bash
portolan push gs://my-bucket
```

| Backend | Behavior |
|---------|----------|
| `file` | Uploads assets + STAC metadata + `versions.json` to remote |
| `iceberg` | **Not needed** — `add` already uploads STAC metadata. Returns a message explaining this. |

---

### portolan pull

Downloads catalog data from remote storage.

```bash
portolan pull gs://my-bucket --collection boundaries
```

| Backend | Behavior |
|---------|----------|
| `file` | Downloads assets + STAC metadata + `versions.json` from remote |
| `iceberg` | Reads asset list from `backend.get_current_version()`, downloads each asset from `{remote}/{href}` to `{local_root}/{href}` |

---

### portolan list

Lists files in the catalog with tracking status.

```bash
portolan list
portolan list --collection boundaries
```

Behavior is the same for both backends — reads from local STAC catalog structure.

---

### portolan info

Shows information about files, collections, or the catalog.

```bash
portolan info
portolan info boundaries
```

Behavior is the same for both backends — reads from local STAC metadata.

---

### portolan check

Validates the local STAC catalog structure.

```bash
portolan check
```

Behavior is the same for both backends.

---

## Version commands (Iceberg only)

These commands are **only available with the Iceberg backend**. Using them with the `file` backend returns an error:

```
✗ 'portolan version list' requires the 'iceberg' backend. Current backend: 'file'
```

### portolan version current

Show the current version of a collection.

```bash
portolan version current boundaries
portolan version current boundaries --json
```

**Output:**
```
→ boundaries: 1.1.0  2026-03-31 00:58:50
    1 asset(s)
```

---

### portolan version list

List all versions of a collection, oldest first.

```bash
portolan version list boundaries
portolan version list boundaries --json
```

**Output:**
```
→ Versions for 'boundaries' (2 total):

→   1.0.0  2026-03-31 00:57:34
      ne_110m_admin_0_countries.parquet
→   1.1.0  2026-03-31 00:58:50
      ne_110m_admin_0_countries.parquet
```

---

### portolan version rollback

Rollback a collection to a previous version. Uses Iceberg's native snapshot management to set the current snapshot pointer — no data is copied, this is instant.

```bash
portolan version rollback boundaries 1.0.0
portolan version rollback boundaries 1.0.0 --json
```

**Output:**
```
✓ Rolled back 'boundaries' to version 1.0.0
```

After rollback, `portolan version current` shows the restored version.

---

### portolan version prune

Remove old versions, keeping the N most recent.

```bash
portolan version prune boundaries --keep 5          # Keep 5 most recent
portolan version prune boundaries --keep 3          # Keep 3 most recent
portolan version prune boundaries --keep 1 --dry-run  # Preview without deleting
```

**Dry-run output:**
```
→ [DRY RUN] Would prune 1 version(s), keeping 1:
    1.0.0  2026-03-31 00:57:34
```

---

## Backend behavior summary

| Command | `file` backend | `iceberg` backend |
|---------|---------------|-------------------|
| `init` | Local STAC catalog | + Iceberg catalog (`.portolan/iceberg.db`) |
| `add` | Writes locally, records in `versions.json` | Ingests into Iceberg table with spatial columns |
| `add` + remote | No auto-upload | Auto-uploads STAC metadata to remote |
| `push` | Uploads everything | Not needed (STAC uploaded on `add`) |
| `pull` | Downloads everything | Downloads assets from `{remote}/{href}` |
| `version current` | Not available | Shows current Iceberg snapshot version |
| `version list` | Not available | Lists all Iceberg snapshot versions |
| `version rollback` | Not available | Native Iceberg snapshot restore (instant) |
| `version prune` | Not available | Expires old Iceberg snapshots |
| `list` / `info` / `check` | Same | Same |

## JSON output

All version commands support `--json` for machine-readable output:

```bash
portolan version current boundaries --json
```

```json
{
  "success": true,
  "command": "version current",
  "data": {
    "collection": "boundaries",
    "version": "1.0.0",
    "created": "2026-03-31T00:57:34.637000+00:00",
    "breaking": false,
    "message": null,
    "assets": 1,
    "changes": ["ne_110m_admin_0_countries.parquet"]
  }
}
```
