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
from .collection import Collection
from .errors import (
    DocumentNotFoundError,
    DuplicateKeyError,
    MooFileError,
    ReadOnlyError,
)

__version__ = "0.2.1"

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
