"""Fixture guard shared by the earnings research test suites (byte-identical copy
in every package's tests directory).

The parquet fixtures are subsets of vendor minute bars and are not shipped
with the exported sample; tests that need them skip instead of failing.
"""
from pathlib import Path

import pytest


def require(path: Path) -> Path:
    path = Path(path)
    if not path.exists():
        pytest.skip(f"fixture not present: {path.name} (vendor data, not shipped with the sample)")
    return path
