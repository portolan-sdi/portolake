"""Tests for portolake entry point registration and IcebergBackend discoverability.

These tests verify that portolake correctly registers as a portolan-cli backend
plugin via Python's entry_points mechanism.

See: https://github.com/portolan-sdi/portolake/issues/2
"""

from importlib.metadata import entry_points

import pytest


@pytest.mark.unit
def test_backend_is_discoverable():
    """Verify portolake registers correctly as a portolan backend.

    The entry point must be registered under the 'portolan.backends' group
    with the name 'iceberg'.
    """
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
def test_backend_instantiates():
    """Verify the backend class can be instantiated."""
    from portolake import IcebergBackend

    backend = IcebergBackend()
    assert backend is not None


@pytest.mark.unit
def test_backend_has_required_methods():
    """Verify the backend has all required VersioningBackend protocol methods."""
    from portolake import IcebergBackend

    backend = IcebergBackend()

    # All 6 protocol methods from VersioningBackend
    required_methods = [
        "get_current_version",
        "list_versions",
        "publish",
        "rollback",
        "prune",
        "check_drift",
    ]

    for method_name in required_methods:
        assert hasattr(backend, method_name), f"Missing method: {method_name}"
        assert callable(getattr(backend, method_name)), f"Not callable: {method_name}"


@pytest.mark.unit
def test_backend_methods_raise_not_implemented():
    """Verify stub methods raise NotImplementedError with descriptive messages."""
    from portolake import IcebergBackend

    backend = IcebergBackend()

    # Test each method raises NotImplementedError
    with pytest.raises(NotImplementedError, match="get_current_version"):
        backend.get_current_version("test-collection")

    with pytest.raises(NotImplementedError, match="list_versions"):
        backend.list_versions("test-collection")

    with pytest.raises(NotImplementedError, match="publish"):
        backend.publish("test-collection", {}, {}, False, "test")

    with pytest.raises(NotImplementedError, match="rollback"):
        backend.rollback("test-collection", "1.0.0")

    with pytest.raises(NotImplementedError, match="prune"):
        backend.prune("test-collection", 5, True)

    with pytest.raises(NotImplementedError, match="check_drift"):
        backend.check_drift("test-collection")


@pytest.mark.unit
def test_iceberg_backend_importable_from_package():
    """Verify IcebergBackend can be imported directly from portolake."""
    from portolake import IcebergBackend

    assert IcebergBackend is not None
    assert IcebergBackend.__name__ == "IcebergBackend"
