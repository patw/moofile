"""
Cross-implementation conftest.

Runs every test against both implementations so divergences between them
show up as failures.

Important: the "python" backend must import ``moofile.collection`` directly.
Importing ``moofile.Collection`` resolves to the *Rust* adapter whenever the
native extension is loaded, which silently made this suite run one backend
under both labels — and let a batch of Python/Rust divergences ship
unnoticed.  Import each implementation explicitly.

The "rust" backend is skipped (not failed) when the extension isn't built,
so the suite still runs on a pure-Python checkout.
"""

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--backend",
        default="both",
        choices=("python", "rust", "both"),
        help="Which moofile backend(s) to test",
    )


def pytest_generate_tests(metafunc):
    """Parametrize `backend` from --backend (default: both)."""
    if "backend" not in metafunc.fixturenames:
        return
    selected = metafunc.config.getoption("--backend")
    params = ["python", "rust"] if selected == "both" else [selected]
    metafunc.parametrize("backend", params)


def _rust_collection():
    """Return the Rust-backed Collection, or None if unavailable."""
    import moofile

    if not getattr(moofile, "_NATIVE_LOADED", False):
        return None
    from moofile._rust_adapter import Collection as RustCollection

    return RustCollection


@pytest.fixture
def make_collection(backend, tmp_path):
    """Return a factory for creating collections in a temp dir."""
    if backend == "python":
        # Explicitly the pure-Python reference implementation.
        from moofile.collection import Collection as Impl
    elif backend == "rust":
        Impl = _rust_collection()
        if Impl is None:
            pytest.skip("native extension not built — run cargo build in bindings/python")
    else:
        raise ValueError(f"Unknown backend: {backend}")

    def _make(name="test.bson", **kwargs):
        path = tmp_path / name
        return Impl(str(path), **kwargs)

    return _make
