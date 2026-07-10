"""
Adapter: wraps the Rust NativeCollection to match the Python Collection API.

Used by moofile/__init__.py when the native extension is available.
"""

from typing import Optional

_NativeCollection = None


class Collection:
    """MooFile Collection backed by the Rust native engine."""

    def __init__(
        self,
        path: str,
        indexes=None,
        vector_indexes=None,
        text_indexes=None,
        readonly: bool = False,
        schema=None,
    ):
        if _NativeCollection is None:
            raise ImportError("Native moofile extension not loaded")
        self._native = _NativeCollection(
            path=path,
            indexes=list(indexes) if indexes else None,
            vector_indexes=dict(vector_indexes) if vector_indexes else None,
            text_indexes=list(text_indexes) if text_indexes else None,
            readonly=readonly,
        )

    # --- Bulk operations (single Rust round-trip) ---
    
    def insert_many(self, docs: list) -> int:
        return self._native.insert_many(docs)

    def insert(self, doc: dict) -> dict:
        # Single insert still needs to return the doc with _id
        return self._native.insert(doc)

    # --- Query ---

    def find(self, filter_dict=None):
        return _NativeQuery(self._native, filter_dict or {})

    def find_one(self, filter_dict=None):
        return self._native.find_one(filter_dict)

    def count(self, filter_dict=None) -> int:
        return self._native.count(filter_dict)

    def exists(self, filter_dict) -> bool:
        return self.find_one(filter_dict) is not None

    # --- Update ---

    def update_one(self, where, set=None, unset=None, inc=None):
        return self._native.update_one(where, set, unset, inc)

    def update_many(self, where, set=None, unset=None, inc=None):
        raise NotImplementedError("update_many not yet exposed in native binding")

    def replace_one(self, where, new_doc):
        raise NotImplementedError("replace_one not yet exposed in native binding")

    # --- Delete ---

    def delete_one(self, where) -> bool:
        return self._native.delete_one(where)

    def delete_many(self, where) -> int:
        return self._native.delete_many(where)

    # --- Utility ---

    def stats(self) -> dict:
        return self._native.stats()

    def compact(self):
        self._native.compact()

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class _NativeQuery:
    """Thin proxy that calls into the native find() in one shot."""

    def __init__(self, native, filter_dict):
        self._native = native
        self._filter = filter_dict
        self._sort_key = None
        self._sort_desc = False
        self._skip_n = 0
        self._limit_n = None

    def sort(self, field, descending=False):
        self._sort_key = field
        self._sort_desc = descending
        return self

    def skip(self, n):
        self._skip_n = n
        return self

    def limit(self, n):
        self._limit_n = n
        return self

    def to_list(self) -> list:
        # Single Rust round-trip — all docs returned at once
        results = self._native.find(self._filter)

        if self._sort_key is not None:
            results.sort(
                key=lambda d: (d.get(self._sort_key) is None, d.get(self._sort_key)),
                reverse=self._sort_desc,
            )

        if self._skip_n:
            results = results[self._skip_n:]

        if self._limit_n is not None:
            results = results[: self._limit_n]

        return results

    def first(self):
        results = self.to_list()
        return results[0] if results else None

    def count(self) -> int:
        return len(self.to_list())
