"""
Cross-implementation conftest.

Parametrizes tests to run against both the Python reference implementation
and (eventually) the Rust native implementation.  Until the PyO3 binding
exists, only the Python backend is active.

To add the Rust backend:

    1. Build with `maturin develop` in `bindings/python/`
    2. Add `"rust"` to `backends` below
    3. `MOOFILE_BACKEND=rust pytest tests-cross/`
"""

import os
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--backend",
        default="python",
        choices=("python", "rust", "both"),
        help="Which moofile backend to test",
    )


@pytest.fixture(params=["python"])
def backend(request):
    """Yields the backend name for parametrized tests."""
    return request.param


@pytest.fixture
def make_collection(backend, tmp_path):
    """Return a factory for creating collections in a temp dir."""
    if backend == "python":
        from moofile import Collection as PyCollection

        def _make(name="test.bson", **kwargs):
            path = tmp_path / name
            return PyCollection(str(path), **kwargs)

    elif backend == "rust":
        raise pytest.skip("Rust backend not built yet — build bindings/python first")

    else:
        raise ValueError(f"Unknown backend: {backend}")

    return _make
