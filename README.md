<div align="center">
  <img src="https://raw.githubusercontent.com/portolan-sdi/portolan-cli/main/docs/assets/images/cover.png" alt="Portolan" width="600"/>
</div>

<div align="center">

[![CI](https://github.com/portolan-sdi/portolake/actions/workflows/ci.yml/badge.svg)](https://github.com/portolan-sdi/portolake/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![PyPI version](https://badge.fury.io/py/portolake.svg)](https://badge.fury.io/py/portolake)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

</div>

---

<!-- --8<-- [start:intro] -->
Portolake is a plugin for [Portolan CLI](https://github.com/portolan-sdi/portolan-cli) that adds lakehouse-grade versioning to geospatial catalogs. It replaces the built-in file-based versioning with [Apache Iceberg](https://iceberg.apache.org/), giving you ACID transactions, concurrent writes, version rollback, and snapshot pruning.

Where Portolan's built-in backend stores versions in a JSON file (single-writer, no rollback), Portolake stores them as Iceberg snapshots — enabling multi-user access with full transactional guarantees.
<!-- --8<-- [end:intro] -->

<!-- --8<-- [start:quickstart] -->
## Quick Start

### CLI Usage

```bash
# Install portolake alongside portolan-cli
pip install portolake

# Set Iceberg as the default versioning backend
portolan config set backend iceberg

# Now all version commands use Iceberg
portolan version current demographics          # Show current version
portolan version list demographics             # List all versions
portolan version rollback demographics 1.0.0   # Rollback to a previous version
portolan version prune demographics --keep 5   # Prune old versions

# Or use --backend flag without changing config
portolan version --backend iceberg list demographics
```

### Python API

```python
from portolan_cli.backends import get_backend

backend = get_backend("iceberg")

# Publish a version
version = backend.publish(
    collection="demographics",
    assets={"data.parquet": "/path/to/data.parquet"},
    schema={"columns": ["id", "geom"], "types": {"id": "int64", "geom": "geometry"}, "hash": "abc123"},
    breaking=False,
    message="Initial data load",
)
print(f"Published: {version.version}")  # "1.0.0"

# List all versions
versions = backend.list_versions("demographics")

# Rollback to a previous version
rolled = backend.rollback("demographics", "1.0.0")

# Prune old versions (keep 5 most recent)
pruned = backend.prune("demographics", keep=5, dry_run=False)
```
<!-- --8<-- [end:quickstart] -->

<!-- --8<-- [start:features] -->
## What Portolake Adds

| Feature | Built-in (JsonFileBackend) | Portolake (IcebergBackend) |
|---------|---------------------------|---------------------------|
| Version storage | `versions.json` file | Iceberg snapshots |
| Concurrent writes | No (single-writer) | Yes (ACID transactions) |
| Rollback | Not supported | Creates new version from target |
| Prune | Not supported | Expire old snapshots |
| Time travel | No | Via Iceberg snapshot history |
| Schema evolution | Manual tracking | Automated detection |
| Catalog backends | Local files only | SQLite, PostgreSQL, REST, Glue, DynamoDB, Hive, BigQuery |
<!-- --8<-- [end:features] -->

<!-- --8<-- [start:installation] -->
## Installation

### With pip

```bash
pip install portolake
```

### With pipx (alongside portolan-cli)

```bash
pipx install portolan-cli
pipx inject portolan-cli portolake

# Set Iceberg as the default backend
portolan config set backend iceberg
```

### For Development

```bash
git clone https://github.com/portolan-sdi/portolake.git
cd portolake
uv sync --all-extras
uv run pre-commit install
```

### Developing with portolan-cli

To test portolake as a plugin alongside portolan-cli:

```bash
# From your portolan-cli directory
cd path/to/portolan-cli
uv pip install -e path/to/portolake

# Verify integration
uv run python -c "
from portolan_cli.backends import get_backend
backend = get_backend('iceberg')
print(f'Loaded: {backend.__class__.__name__}')
"
```

Editable mode (`-e`) means changes to portolake take effect immediately.
<!-- --8<-- [end:installation] -->

<!-- --8<-- [start:configuration] -->
## Configuration

Portolake defaults to a local SQLite catalog — zero configuration needed for getting started.

For production, configure the Iceberg catalog via standard [PyIceberg environment variables](https://py.iceberg.apache.org/configuration/):

```bash
# REST catalog (Tabular, Polaris, Nessie, etc.)
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=rest
export PYICEBERG_CATALOG__PORTOLAKE__URI=https://my-catalog.example.com

# AWS Glue
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=glue

# PostgreSQL-backed SQL catalog
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=sql
export PYICEBERG_CATALOG__PORTOLAKE__URI=postgresql://user:pass@host/db
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse
```

See the [Configuration Reference](docs/reference/configuration.md) for all options.
<!-- --8<-- [end:configuration] -->

## Roadmap

Portolake currently implements the **Apache Iceberg backend** for vector/tabular data. Planned:

- **[Icechunk](https://icechunk.io/) backend** for array/raster data (COG, NetCDF, Zarr via VirtualiZarr) — see [ADR-0015](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0015-two-tier-versioning-architecture.md)
- Full drift detection between local and remote state
- Automated schema evolution and breaking change detection

## Documentation

- [Configuration Reference](docs/reference/configuration.md)
- [API Reference](docs/reference/api.md)
- [Contributing Guide](docs/contributing.md)

## License

Apache 2.0 — see [LICENSE](LICENSE)
