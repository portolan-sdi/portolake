"""Basic import tests to verify package structure."""

import pytest


@pytest.mark.unit
def test_import_portolake():
    """Verify the portolake package can be imported."""
    import portolake

    assert portolake.__version__ == "0.1.0"


@pytest.mark.unit
def test_import_dependencies():
    """Verify core dependencies are importable."""
    import click
    import pyarrow

    assert click is not None
    assert pyarrow is not None
