# Contributing to Portolake

Thank you for your interest in contributing to Portolake!

> **For AI agents and detailed development guidelines:** See `CLAUDE.md` in the project root. This document is for human contributors and covers workflow, not implementation details.

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/portolan-sdi/portolake.git
   cd portolake
   ```

2. **Install uv** (if not already installed)
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Install dependencies**
   ```bash
   uv sync --all-extras
   ```

4. **Install pre-commit hooks**
   ```bash
   uv run pre-commit install
   ```

5. **Verify setup**
   ```bash
   uv run pytest
   ```

## Making Changes

### Branch Naming

- Feature: `feature/description` (e.g., `feature/add-iceberg-support`)
- Bug fix: `fix/description` (e.g., `fix/transaction-rollback`)
- Documentation: `docs/description` (e.g., `docs/update-readme`)

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/) enforced by commitizen. See `CLAUDE.md` for the full format specification.

**Quick reference:**
```
feat(scope): add new feature
fix(scope): fix bug
docs(scope): update documentation
refactor(scope): restructure code
test(scope): add tests
```

### Pull Request Process

1. **Create a new branch** from `main`
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** — Write code, add tests, update docs

3. **Run pre-commit checks** (happens automatically on commit)
   ```bash
   uv run pre-commit run --all-files
   ```

4. **Run tests**
   ```bash
   uv run pytest
   ```

5. **Commit and push**
   ```bash
   git add .
   git commit -m "feat(scope): description"
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request** on GitHub
   - Fill in the PR template
   - Link any related issues
   - CI will run automatically

### What CI Checks

All PRs must pass these automated checks:

- **Linting & formatting** — ruff
- **Security scanning** — pip-audit
- **Tests** — pytest with coverage
- **Dead code detection** — vulture
- **Complexity limits** — xenon

## Testing

Tests use pytest with markers:

| Marker | Description |
|--------|-------------|
| `@pytest.mark.unit` | Fast, isolated, no I/O (< 100ms) |
| `@pytest.mark.integration` | Multi-component, may touch filesystem |
| `@pytest.mark.network` | Requires network (mocked locally) |
| `@pytest.mark.slow` | Takes > 5 seconds |

```bash
# All tests
uv run pytest

# Only unit tests
uv run pytest -m unit

# With coverage report
uv run pytest --cov=portolake --cov-report=html
```

### Test Fixtures

Shared fixtures in `tests/conftest.py`:

- `iceberg_catalog` — Temporary SQLite-backed Iceberg catalog in `tmp_path`
- `iceberg_backend` — `IcebergBackend` instance using the temporary catalog

```python
@pytest.mark.integration
def test_publish_creates_version(iceberg_backend, tmp_path):
    asset_file = tmp_path / "data.parquet"
    asset_file.write_bytes(b"test data")

    version = iceberg_backend.publish(
        collection="test",
        assets={"data.parquet": str(asset_file)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="test",
    )
    assert version.version == "1.0.0"
```

### Code Quality

```bash
# Lint and format
uv run ruff check . --fix
uv run ruff format .

# Dead code detection
uv run vulture portolake tests

# Complexity
uv run xenon --max-absolute=C portolake
```

## Project Structure

```
portolake/
├── portolake/
│   ├── __init__.py        # Package exports (IcebergBackend, __version__)
│   ├── backend.py         # IcebergBackend — all 6 protocol methods
│   ├── config.py          # Catalog configuration via load_catalog()
│   └── versioning.py      # Semver logic, snapshot/Version conversion
├── tests/
│   ├── conftest.py        # Shared fixtures (tmp catalog, backend)
│   ├── test_backend.py    # Integration tests for IcebergBackend
│   ├── test_config.py     # Config resolution tests
│   ├── test_entry_point.py # Plugin discovery tests
│   ├── test_import.py     # Basic import tests
│   └── test_versioning.py # Semver and conversion tests
├── docs/                  # Documentation (MkDocs)
└── pyproject.toml         # Dependencies and tool config
```

## Release Process

Portolake uses a **tag-based release workflow**:

1. **Accumulate changes** — Merge PRs to `main` as normal using conventional commits
2. **Prepare a release** — When ready to release, create a PR that bumps the version:
   ```bash
   uv run cz bump --changelog
   git push
   ```
3. **Merge the bump PR** — The release workflow detects the bump commit and:
   - Creates a git tag (e.g., `v0.2.0`)
   - Builds the package
   - Publishes to PyPI
   - Creates a GitHub Release

The version is determined by commitizen based on conventional commits since the last release:

| Commit type | Version bump |
|-------------|--------------|
| `feat:` | Minor (0.x.0) |
| `fix:` | Patch (0.0.x) |
| `BREAKING CHANGE:` | Major (x.0.0) |
| `docs:`, `refactor:`, `test:`, `chore:` | No release |

## Questions?

- **Bug reports / feature requests:** Open an issue
- **Questions:** Use GitHub Discussions
- **Check existing issues** before creating new ones

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help create a welcoming environment
- Report unacceptable behavior to maintainers

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
