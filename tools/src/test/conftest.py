"""Test fixtures."""

import pytest

from pathlib import Path


@pytest.fixture
def testdata_path() -> Path:
    """Path to the root testdata directory."""
    return Path(__file__).parent.parent.parent.parent / "testdata"
