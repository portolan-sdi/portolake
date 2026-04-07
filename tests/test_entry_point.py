"""Tests for portolake entry point registration and IcebergBackend discoverability.

These tests verify that portolake correctly registers as a portolan-cli backend
plugin via Python's entry_points mechanism.
"""

from importlib.metadata import entry_points

import pytest


@pytest.mark.unit
def test_backend_is_discoverable():
    """Verify portolake registers correctly as a portolan backend."""
    eps = entry_points(group="portolan.backends")
    names = [ep.name for ep in eps]
    assert "iceberg" in names, f"Expected 'iceberg' in {names}"


@pytest.mark.unit
def test_backend_loads():
    """Verify the backend class can be loaded from the entry point."""
    eps = entry_points(group="portolan.backends")
    iceberg_ep = next((ep for ep in eps if ep.name == "iceberg"), None)
    assert iceberg_ep is not None, "Entry point 'iceberg' not found"

    backend_class = iceberg_ep.load()
    assert backend_class.__name__ == "IcebergBackend"


@pytest.mark.unit
def test_iceberg_backend_importable_from_package():
    """Verify IcebergBackend can be imported directly from portolake."""
    from portolake import IcebergBackend

    assert IcebergBackend is not None
    assert IcebergBackend.__name__ == "IcebergBackend"


@pytest.mark.integration
def test_backend_isinstance_versioning_backend(iceberg_backend):
    """Verify the backend satisfies the VersioningBackend protocol."""
    from portolan_cli.backends.protocol import VersioningBackend

    assert isinstance(iceberg_backend, VersioningBackend)


@pytest.mark.integration
def test_backend_has_required_methods(iceberg_backend):
    """Verify the backend has all required VersioningBackend protocol methods."""
    required_methods = [
        "get_current_version",
        "list_versions",
        "publish",
        "rollback",
        "prune",
        "check_drift",
    ]
    for method_name in required_methods:
        assert hasattr(iceberg_backend, method_name), f"Missing method: {method_name}"
        assert callable(getattr(iceberg_backend, method_name)), f"Not callable: {method_name}"
