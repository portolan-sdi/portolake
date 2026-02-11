# Portolake - Development Guide

## What is Portolake?

Portolake is a **plugin for [Portolan CLI](https://github.com/portolan-sdi/portolan-cli)** that provides lakehouse-grade versioning for geospatial catalogs. It's the enterprise-tier backend described in [ADR-0015](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0015-two-tier-versioning-architecture.md).

**Key concepts:**
- **Apache Iceberg** — ACID transactions for tabular/vector data (GeoParquet format)
- **Icechunk** — Time travel and versioning for array/raster data (COG, NetCDF, HDF, Zarr via VirtualiZarr)
- **Portolan** — The parent CLI that orchestrates format conversion and catalog management

**What Portolake adds over Portolan's built-in versioning:**
- ACID transactions enabling concurrent writes
- Native time travel and version branching
- Automated schema evolution detection
- Garbage collection and snapshot management
- Optimistic concurrency control

**Target users:** Organizations like Carto and HDX requiring multi-user access to geospatial catalogs.

## Guiding Principle

AI agents will write most of the code. Human review does not scale to match AI output volume. Therefore: every quality gate must be automated, every convention must be enforceable, and tests must be verified to actually test something.

## Quick Reference

| Resource | Location |
|----------|----------|
| Parent project | [portolan-cli](https://github.com/portolan-sdi/portolan-cli) |
| Architecture decisions | [portolan-cli ADRs](https://github.com/portolan-sdi/portolan-cli/tree/main/context/shared/adr) |
| Plugin architecture | [ADR-0003](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0003-plugin-architecture.md) |
| Two-tier versioning | [ADR-0015](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0015-two-tier-versioning-architecture.md) |
| Iceberg as plugin | [ADR-0004](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0004-iceberg-as-plugin.md) |

**Target Python version:** 3.11+ (matches pyarrow/iceberg requirements)

## Common Commands

```bash
# Environment setup
uv sync --all-extras                    # Install all dependencies
uv run pre-commit install               # Install git hooks

# Development
uv run pytest                           # Run tests
uv run pytest -m unit                   # Run only unit tests
uv run pytest --cov-report=html         # Coverage report
uv run ruff check .                     # Lint
uv run ruff format .                    # Format
uv run vulture portolake tests          # Dead code
uv run xenon --max-absolute=C portolake # Complexity

# Commits (use commitizen for conventional commits)
uv run cz commit                        # Interactive commit
uv run cz bump --dry-run                # Preview version bump
```

## Project Structure

```
portolake/
├── portolake/             # Source code
├── tests/                 # Test suite
└── .github/workflows/     # CI/CD pipelines
```

## Test-Driven Development (MANDATORY)

**YOU MUST USE TDD. NO EXCEPTIONS.** Unless the user explicitly says "skip tests":

1. **WRITE TESTS FIRST** — Before ANY implementation code
2. **RUN TESTS** — Verify they fail with `uv run pytest`
3. **IMPLEMENT** — Minimal code to pass tests
4. **RUN TESTS AGAIN** — Verify they pass
5. **ADD EDGE CASES** — Test error conditions

### Test Markers

```python
@pytest.mark.unit        # Fast, isolated, no I/O (< 100ms)
@pytest.mark.integration # Multi-component, may touch filesystem
@pytest.mark.network     # Requires network (mocked locally, live in nightly)
@pytest.mark.benchmark   # Performance measurement
@pytest.mark.slow        # Takes > 5 seconds
```

## CI Pipeline

| Tier | When | What |
|------|------|------|
| Tier 1 | Pre-commit | ruff, vulture, xenon, fast tests |
| Tier 2 | Every PR | lint, security, full tests, dead-code, build |

**All checks are strict** — no `continue-on-error`. Fix issues or they block.

## Code Quality

- **ruff** — Linting and formatting
- **vulture** — Dead code detection
- **xenon** — Complexity monitoring (max C function, B module, A average)
- **pip-audit** — Dependency vulnerabilities

## Git Workflow

### Branch Naming

```
feature/description    # New features
fix/description        # Bug fixes
docs/description       # Documentation
refactor/description   # Code restructuring
```

### Conventional Commits

Use `uv run cz commit` for interactive commit creation:

```
feat(scope): add new feature      # Minor version bump
fix(scope): fix bug               # Patch version bump
docs(scope): update documentation
refactor(scope): restructure code
test(scope): add tests
BREAKING CHANGE: ...              # Major version bump
```

### Merge Policy

**Squash-merge** all PRs to main. This ensures:
- Clean history (one commit per PR)
- PR title becomes the commit message (enforce conventional format)
- Commitizen can analyze commits cleanly for versioning

### Release Automation

Portolake uses a **tag-based release workflow**. See `.github/workflows/release.yml`.

**To release:**
1. Create a PR that runs `uv run cz bump --changelog`
2. Merge the bump PR
3. Release workflow detects the bump commit and creates tag + publishes

## Development Rules

- **ALL** new features require tests FIRST (TDD)
- **NO** new dependencies without discussion

## Key Dependencies

| Library | Purpose | Docs |
|---------|---------|------|
| [Apache Iceberg](https://iceberg.apache.org/) | Table format for vector data | [PyIceberg](https://py.iceberg.apache.org/) |
| [Icechunk](https://github.com/earth-mover/icechunk) | Zarr-compatible versioned storage | [Docs](https://icechunk.io/) |
| [geoparquet-io](https://github.com/geoparquet/geoparquet-io) | GeoParquet I/O | GitHub |

## Design Principles

| Principle | Meaning |
|-----------|---------|
| **Plugin, not standalone** | Portolake extends Portolan; it doesn't replace it |
| **Delegate to specialists** | Iceberg handles transactions, Icechunk handles arrays |
| **YAGNI** | No speculative features; complexity is expensive |
| **Concurrent-first** | Design for multi-user from the start |
