"""Pure-logic smoke test: the package imports and exposes a version (no Redis needed)."""

import ftq


def test_package_has_version() -> None:
    assert ftq.__version__ == "0.1.0"
