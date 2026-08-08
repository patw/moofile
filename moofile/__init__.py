"""
MooFile — lightweight embedded document store.

    from moofile import Collection, count, mean, sum

    with Collection("data.bson", indexes=["email", "age"]) as db:
        db.insert({"name": "alice", "email": "alice@example.com", "age": 30})

        results = (
            db.find({"age": {"$gt": 25}})
            .sort("age", descending=True)
            .to_list()
        )
"""

import os as _os
import warnings as _warnings

from .aggregation import collect, count, first, last, max, mean, min, sum
from .errors import (
    ConcurrentAccessError,
    DocumentNotFoundError,
    DuplicateKeyError,
    MooFileError,
    ReadOnlyError,
)

from importlib.metadata import PackageNotFoundError, version as _package_version

try:
    __version__ = _package_version("moofile")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"  # not installed — running from source

# --- Try the Rust native backend first ---
_NATIVE_LOADED = False
_NATIVE_IMPORT_ERROR: str | None = None
try:
    from moofile._native import NativeCollection as _NativeCollection  # type: ignore[import-untyped]
    from moofile._rust_adapter import Collection as _RustCollection

    _NATIVE_LOADED = True
except ImportError as _exc:
    _NATIVE_IMPORT_ERROR = str(_exc)

if _NATIVE_LOADED:
    # Patch the adapter with the native class
    import moofile._rust_adapter as _adapter

    _adapter._NativeCollection = _NativeCollection
    Collection = _RustCollection  # type: ignore[misc]
else:
    from .collection import Collection  # type: ignore[no-redef]

    # The fallback must announce itself.  It is the same import and the same
    # class name, but a different feature set — no autoembedding, no
    # semantic(), and 2-24x slower.  Silence is how a Python-only gap in
    # autoembedding survived several releases: nobody got an error, they got a
    # quietly less capable object.
    if _os.environ.get("MOOFILE_PURE_PYTHON") != "1":
        _warnings.warn(
            "moofile: the native extension could not be imported, falling back "
            f"to the pure-Python implementation ({_NATIVE_IMPORT_ERROR}). "
            "Autoembedding and semantic() are unavailable on this backend and "
            "reads/writes are several times slower. Install a native wheel, or "
            "build one with `maturin develop --release`. Set "
            "MOOFILE_PURE_PYTHON=1 to silence this warning.",
            RuntimeWarning,
            stacklevel=2,
        )

__all__ = [
    # Core
    "Collection",
    # Exceptions
    "MooFileError",
    "DuplicateKeyError",
    "DocumentNotFoundError",
    "ReadOnlyError",
    "ConcurrentAccessError",
    # Aggregation functions
    "count",
    "sum",
    "mean",
    "min",
    "max",
    "collect",
    "first",
    "last",
]
