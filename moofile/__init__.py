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

from .aggregation import collect, count, first, last, max, mean, min, sum
from .errors import (
    DocumentNotFoundError,
    DuplicateKeyError,
    MooFileError,
    ReadOnlyError,
)

__version__ = "0.3.4"

# --- Try the Rust native backend first ---
_NATIVE_LOADED = False
try:
    from moofile._native import NativeCollection as _NativeCollection  # type: ignore[import-untyped]
    from moofile._rust_adapter import Collection as _RustCollection

    _NATIVE_LOADED = True
except ImportError:
    pass

if _NATIVE_LOADED:
    # Patch the adapter with the native class
    import moofile._rust_adapter as _adapter

    _adapter._NativeCollection = _NativeCollection
    Collection = _RustCollection  # type: ignore[misc]
else:
    from .collection import Collection  # type: ignore[no-redef]

__all__ = [
    # Core
    "Collection",
    # Exceptions
    "MooFileError",
    "DuplicateKeyError",
    "DocumentNotFoundError",
    "ReadOnlyError",
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
