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

Lakehouse-grade versioning plugin for [Portolan](https://github.com/portolan-sdi/portolan-cli).

Portolake provides ACID transactions, concurrent writes, and time travel for Portolan catalogs using Apache Iceberg (vector data) and Icechunk (raster data).

## Status

**Pre-release** — API and implementation in progress.

## Installation

### Recommended: pipx (for global use)

```bash
pipx install portolake
```

This installs `portolake` in an isolated environment while making the command globally available.

If you don't have pipx installed:
```bash
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```

### Alternative: pip

```bash
pip install portolake
```

**Note:** This installs into your global or user site-packages and may conflict with other packages.

### For Development

Use [uv](https://github.com/astral-sh/uv) for local development:

```bash
git clone https://github.com/portolan-sdi/portolake.git
cd portolake
uv sync --all-extras
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

See [Contributing Guide](docs/contributing.md) for full development setup.

## Documentation

- [Contributing Guide](docs/contributing.md)

## License

Apache 2.0 — see [LICENSE](LICENSE)
