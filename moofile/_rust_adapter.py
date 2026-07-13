"""
Adapter: wraps the Rust NativeCollection to match the Python Collection API.

Used by moofile/__init__.py when the native extension is available.
"""

import bson as _bson
from collections import defaultdict
from typing import Optional

_NativeCollection = None


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

def _map_errors(e):
    """Re-raise a native RuntimeError/ValueError as the proper MooFile exception."""
    from .errors import (
        ConcurrentAccessError,
        DocumentNotFoundError,
        DuplicateKeyError,
        ReadOnlyError,
    )
    msg = str(e).lower()
    if "concurrent access" in msg:
        raise ConcurrentAccessError(str(e)) from e
    if "read-only" in msg:
        raise ReadOnlyError(str(e)) from e
    if "duplicate _id" in msg:
        raise DuplicateKeyError(str(e)) from e
    if "no document matches" in msg:
        raise DocumentNotFoundError(str(e)) from e
    raise e


# ---------------------------------------------------------------------------
# Compatibility shim for tests that access db._index_manager
# ---------------------------------------------------------------------------

class _IndexManagerShim:
    """Lightweight shim that exposes index configuration for tests
    that check db._index_manager._vector_fields etc."""

    def __init__(self, vector_fields: dict, text_fields: list):
        self._vector_fields = vector_fields
        self._text_fields = text_fields


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------

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
        durability: str = "os",
    ):
        if _NativeCollection is None:
            raise ImportError("Native moofile extension not loaded")
        try:
            self._native = _NativeCollection(
                path=path,
                indexes=list(indexes) if indexes else None,
                vector_indexes=dict(vector_indexes) if vector_indexes else None,
                text_indexes=list(text_indexes) if text_indexes else None,
                readonly=readonly,
                durability=durability,
            )
        except RuntimeError as e:
            _map_errors(e)

        self._path = path
        self._readonly = readonly
        self._storage = True  # compatibility: tests check db._storage is None after close

        # Build index manager compatibility shim
        try:
            _, vfields, tfields = self._native.index_config()
        except Exception:
            vfields = dict(vector_indexes) if vector_indexes else {}
            tfields = list(text_indexes) if text_indexes else []
        self._index_manager = _IndexManagerShim(vfields, tfields)

    # --- Insert ---

    def insert(self, doc: dict) -> dict:
        try:
            return self._native.insert(doc)
        except (RuntimeError, ValueError) as e:
            _map_errors(e)

    def insert_many(self, docs: list) -> list:
        try:
            raw_docs = self._native.insert_many(docs)
            return [_bson.BSON(raw).decode() for raw in raw_docs]
        except (RuntimeError, ValueError) as e:
            _map_errors(e)

    # --- Query ---

    def find(self, filter_dict=None):
        return _NativeQuery(self._native, filter_dict or {})

    def find_one(self, filter_dict=None):
        try:
            raw = self._native.find_one_raw(filter_dict)
            if raw is None:
                return None
            return _bson.BSON(raw).decode()
        except RuntimeError as e:
            _map_errors(e)

    def count(self, filter_dict=None) -> int:
        try:
            return self._native.count(filter_dict)
        except RuntimeError as e:
            _map_errors(e)

    def exists(self, filter_dict) -> bool:
        return self.find_one(filter_dict) is not None

    # --- Update ---

    def update_one(self, where, set=None, unset=None, inc=None):
        try:
            return self._native.update_one(where, set, unset, inc)
        except RuntimeError as e:
            _map_errors(e)

    def update_many(self, where, set=None, unset=None, inc=None):
        try:
            return self._native.update_many(where, set, unset, inc)
        except RuntimeError as e:
            _map_errors(e)

    def replace_one(self, where, new_doc):
        try:
            return self._native.replace_one(where, new_doc)
        except RuntimeError as e:
            _map_errors(e)

    # --- Delete ---

    def delete_one(self, where) -> bool:
        try:
            return self._native.delete_one(where)
        except RuntimeError as e:
            _map_errors(e)

    def delete_many(self, where) -> int:
        try:
            return self._native.delete_many(where)
        except RuntimeError as e:
            _map_errors(e)

    # --- Utility ---

    def stats(self) -> dict:
        try:
            return self._native.stats()
        except RuntimeError as e:
            _map_errors(e)

    def compact(self):
        try:
            self._native.compact()
        except RuntimeError as e:
            _map_errors(e)

    def sync(self):
        """Flush and fsync the data file, ensuring durability on disk."""
        try:
            self._native.sync()
        except RuntimeError as e:
            _map_errors(e)

    def batch(self):
        """Return a context manager for atomic batch writes."""
        return _NativeBatchContext(self._native)

    def close(self):
        try:
            self._native.save_cache()
        except Exception:
            pass  # cache is disposable — never fail on cache write
        try:
            self._native.close()
        except Exception:
            pass
        self._storage = None  # compatibility: tests check db._storage is None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# _NativeQuery — mirrors the pure-Python Query builder
# ---------------------------------------------------------------------------

class _NativeQuery:
    """Proxy that calls into the native find() and supports the full
    query chain: sort, skip, limit, group/agg, vector_search, text_search."""

    def __init__(self, native, filter_dict):
        self._native = native
        self._filter = filter_dict
        self._sort_key = None
        self._sort_desc = False
        self._skip_n = 0
        self._limit_n = None
        self._group_field = None
        self._agg_funcs = None

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

    def group(self, field):
        self._group_field = field
        return self

    def agg(self, *funcs):
        self._agg_funcs = list(funcs)
        return self

    def vector_search(self, field, query_vector, limit=10):
        return _NativeVectorQuery(
            self._native, self._filter, field, query_vector, limit
        )

    def text_search(self, field, query, limit=10):
        return _NativeTextQuery(
            self._native, self._filter, field, query, limit
        )

    def hybrid_search(self, text_field, vector_field, query_text, query_vector, limit=10):
        return _NativeHybridQuery(
            self._native, self._filter, text_field, vector_field, query_text, query_vector, limit
        )

    def to_list(self) -> list:
        try:
            raw_docs = self._native.find_raw(self._filter)
        except RuntimeError as e:
            _map_errors(e)
        results = [_bson.BSON(raw).decode() for raw in raw_docs]

        # Group + aggregate (done in Python, same as pure-Python impl)
        if self._group_field is not None:
            results = self._apply_group_agg(results)

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

    def _apply_group_agg(self, docs: list) -> list:
        groups: dict = defaultdict(list)
        for doc in docs:
            key = doc.get(self._group_field)
            groups[key].append(doc)

        result = []
        for key, group_docs in groups.items():
            row = {self._group_field: key}
            if self._agg_funcs:
                for func in self._agg_funcs:
                    row[func.output_name] = func.compute(group_docs)
            result.append(row)
        return result

    def first(self):
        results = self.to_list()
        return results[0] if results else None

    def count(self) -> int:
        if (
            self._group_field is None
            and self._sort_key is None
            and self._skip_n == 0
            and self._limit_n is None
        ):
            try:
                return self._native.count(self._filter)
            except RuntimeError as e:
                _map_errors(e)
        return len(self.to_list())


# ---------------------------------------------------------------------------
# _NativeVectorQuery
# ---------------------------------------------------------------------------

class _NativeVectorQuery:
    """Vector similarity search results from the native engine."""

    def __init__(self, native, pre_filter, field, query_vector, limit):
        self._native = native
        self._pre_filter = pre_filter
        self._field = field
        self._query_vector = query_vector
        self._limit = limit

    def to_list(self) -> list:
        try:
            raw_results = self._native.vector_search_raw(
                self._pre_filter if self._pre_filter else None,
                self._field,
                self._query_vector,
                self._limit if self._limit is not None else 10,
            )
        except RuntimeError as e:
            _map_errors(e)
        return [(_bson.BSON(raw).decode(), score) for raw, score in raw_results]

    def first(self):
        results = self.to_list()
        return results[0] if results else None


# ---------------------------------------------------------------------------
# _NativeTextQuery
# ---------------------------------------------------------------------------

class _NativeTextQuery:
    """BM25 text search results from the native engine."""

    def __init__(self, native, pre_filter, field, query, limit):
        self._native = native
        self._pre_filter = pre_filter
        self._field = field
        self._query = query
        self._limit = limit

    def to_list(self) -> list:
        try:
            raw_results = self._native.text_search_raw(
                self._pre_filter if self._pre_filter else None,
                self._field,
                self._query,
                self._limit if self._limit is not None else 10,
            )
        except RuntimeError as e:
            _map_errors(e)
        return [(_bson.BSON(raw).decode(), score) for raw, score in raw_results]

    def first(self):
        results = self.to_list()
        return results[0] if results else None


# ---------------------------------------------------------------------------
# _NativeHybridQuery
# ---------------------------------------------------------------------------

class _NativeHybridQuery:
    """Hybrid search (RRF) results from the native engine."""

    def __init__(self, native, pre_filter, text_field, vector_field, query_text, query_vector, limit):
        self._native = native
        self._pre_filter = pre_filter
        self._text_field = text_field
        self._vector_field = vector_field
        self._query_text = query_text
        self._query_vector = query_vector
        self._limit = limit

    def to_list(self) -> list:
        try:
            raw_results = self._native.hybrid_search_raw(
                self._pre_filter if self._pre_filter else None,
                self._text_field,
                self._vector_field,
                self._query_text,
                self._query_vector,
                self._limit if self._limit is not None else 10,
            )
        except RuntimeError as e:
            _map_errors(e)
        return [(_bson.BSON(raw).decode(), score) for raw, score in raw_results]

    def first(self):
        results = self.to_list()
        return results[0] if results else None


# ---------------------------------------------------------------------------
# _NativeBatchContext
# ---------------------------------------------------------------------------

class _NativeBatchContext:
    """Context manager for atomic batch writes, backed by the native engine."""

    def __init__(self, native):
        self._native = native

    def __enter__(self):
        try:
            self._native.batch_begin()
        except RuntimeError as e:
            _map_errors(e)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            try:
                self._native.batch_rollback()
            except RuntimeError as e:
                _map_errors(e)
        else:
            try:
                self._native.batch_commit()
            except RuntimeError as e:
                _map_errors(e)
        return False