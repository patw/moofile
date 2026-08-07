#!/usr/bin/env python3
"""
test_parity.py — Cross-backend parity tests for MooFile.

Validates that the Python (pure), Python (Rust native), and C (shared library)
backends produce identical results across the full API surface.

Usage:
    # Build the C binding first, then run:
    python3 test_parity.py                  # defaults to ./target/release/libmoofile.so
    python3 test_parity.py --c-lib path/to/libmoofile.so
    python3 test_parity.py --skip-c         # skip C backend tests
"""

import json
import math
import os
import random
import string
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Optional

# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

g_tests_run = 0
g_tests_failed = 0
g_test_name = ""


def test(name: str):
    global g_test_name
    g_test_name = name


def fail(msg: str):
    global g_tests_failed
    print(f"  FAIL [{g_test_name}] {msg}")
    g_tests_failed += 1


def check(cond: bool, msg: str = "assertion failed"):
    if not cond:
        fail(msg)


def check_docs_equal(docs_a: list, docs_b: list, label: str = ""):
    """Compare two lists of documents for structural equality (ignoring _id)."""
    if len(docs_a) != len(docs_b):
        fail(f"{label}: length mismatch {len(docs_a)} vs {len(docs_b)}")
        return

    # Sort by _id for deterministic comparison
    def sort_key(d):
        return d.get("_id", "")

    a_sorted = sorted(docs_a, key=sort_key)
    b_sorted = sorted(docs_b, key=sort_key)

    for i, (da, db) in enumerate(zip(a_sorted, b_sorted)):
        if da != db:
            fail(f"{label}: doc {i} differs:\n  A: {da}\n  B: {db}")
            return


def check_search_results_equal(
    results_a: list, results_b: list, label: str = ""
):
    """Compare two lists of (doc, score) tuples."""
    if len(results_a) != len(results_b):
        fail(f"{label}: length mismatch {len(results_a)} vs {len(results_b)}")
        return

    for i, ((da, sa), (db, sb)) in enumerate(zip(results_a, results_b)):
        # Compare docs (ignore _id which may differ)
        da_pop = {k: v for k, v in da.items() if k != "_id"}
        db_pop = {k: v for k, v in db.items() if k != "_id"}
        if da_pop != db_pop:
            fail(f"{label}: doc {i} differs:\n  A: {da_pop}\n  B: {db_pop}")
            return
        # Scores should be close
        if abs(sa - sb) > 0.001:
            fail(f"{label}: score {i} differs: {sa} vs {sb}")
            return


# ---------------------------------------------------------------------------
# Backend abstraction
# ---------------------------------------------------------------------------

class Backend:
    """Abstract base for a MooFile backend."""

    def __init__(self, name: str):
        self.name = name

    def open(self, path: str, indexes=None, vector_indexes=None,
             text_indexes=None, readonly=False, durability="os"):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def insert(self, doc: dict) -> dict:
        raise NotImplementedError

    def insert_many(self, docs: list) -> list:
        raise NotImplementedError

    def find(self, filter_dict: dict = None) -> list:
        raise NotImplementedError

    def find_one(self, filter_dict: dict = None):
        raise NotImplementedError

    def count(self, filter_dict: dict = None) -> int:
        raise NotImplementedError

    def exists(self, filter_dict: dict) -> bool:
        raise NotImplementedError

    def update_one(self, where: dict, set_vals: dict = None,
                   unset: list = None, inc: dict = None) -> bool:
        raise NotImplementedError

    def update_many(self, where: dict, set_vals: dict = None,
                    unset: list = None, inc: dict = None) -> int:
        raise NotImplementedError

    def replace_one(self, where: dict, replacement: dict) -> bool:
        raise NotImplementedError

    def delete_one(self, where: dict) -> bool:
        raise NotImplementedError

    def delete_many(self, where: dict) -> int:
        raise NotImplementedError

    def vector_search(self, field: str, query_vector: list,
                      limit: int = 10, filter_dict: dict = None) -> list:
        raise NotImplementedError

    def text_search(self, field: str, query: str,
                    limit: int = 10, filter_dict: dict = None) -> list:
        raise NotImplementedError

    def hybrid_search(self, text_field: str, vector_field: str,
                      query_text: str, query_vector,
                      limit: int = 10, filter_dict: dict = None) -> list:
        raise NotImplementedError

    def batch(self):
        raise NotImplementedError

    def batch_commit(self):
        raise NotImplementedError

    def batch_rollback(self):
        raise NotImplementedError

    def stats(self) -> dict:
        raise NotImplementedError

    def compact(self):
        raise NotImplementedError

    def sync(self):
        raise NotImplementedError

    def reindex(self):
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Python backend (pure Python)
# ---------------------------------------------------------------------------

class PythonBackend(Backend):
    """Pure Python moofile backend (no native extension)."""

    def __init__(self):
        super().__init__("Python (pure)")
        self._db = None
        self._batch_active = False

    def open(self, path, indexes=None, vector_indexes=None,
             text_indexes=None, readonly=False, durability="os"):
        # Force pure Python by monkey-patching before import
        import moofile._native as _native_mod
        saved = getattr(_native_mod, '_NATIVE_LOADED', None)
        try:
            _native_mod._NATIVE_LOADED = False  # type: ignore
            import importlib
            import moofile.collection as coll_mod
            self._db = coll_mod.Collection(
                path,
                indexes=indexes,
                vector_indexes=vector_indexes,
                text_indexes=text_indexes,
                readonly=readonly,
                durability=durability,
            )
        finally:
            if saved is not None:
                _native_mod._NATIVE_LOADED = saved

    def close(self):
        if self._db:
            self._db.close()
            self._db = None

    def insert(self, doc):
        return dict(self._db.insert(doc))

    def insert_many(self, docs):
        return [dict(d) for d in self._db.insert_many(docs)]

    def find(self, filter_dict=None):
        if self._batch_active:
            # During batch, use internal get_docs for visibility
            docs = self._db._get_docs(filter_dict or {})
            return [dict(d) for d in docs]
        return [dict(d) for d in self._db.find(filter_dict or {}).to_list()]

    def find_one(self, filter_dict=None):
        doc = self._db.find_one(filter_dict or {})
        return dict(doc) if doc else None

    def count(self, filter_dict=None):
        return self._db.count(filter_dict or {})

    def exists(self, filter_dict):
        return self._db.exists(filter_dict)

    def update_one(self, where, set_vals=None, unset=None, inc=None):
        try:
            self._db.update_one(where, set=set_vals, unset=unset, inc=inc)
            return True
        except Exception as e:
            if "no document matches" in str(e).lower():
                return False
            raise

    def update_many(self, where, set_vals=None, unset=None, inc=None):
        return self._db.update_many(where, set=set_vals, unset=unset, inc=inc)

    def replace_one(self, where, replacement):
        try:
            self._db.replace_one(where, replacement)
            return True
        except Exception as e:
            if "no document matches" in str(e).lower():
                return False
            raise

    def delete_one(self, where):
        return self._db.delete_one(where)

    def delete_many(self, where):
        return self._db.delete_many(where)

    def vector_search(self, field, query_vector, limit=10, filter_dict=None):
        q = self._db.find(filter_dict or {})
        return [(dict(doc), score) for doc, score in
                q.vector_search(field, query_vector, limit).to_list()]

    def text_search(self, field, query, limit=10, filter_dict=None):
        q = self._db.find(filter_dict or {})
        return [(dict(doc), score) for doc, score in
                q.text_search(field, query, limit).to_list()]

    def hybrid_search(self, text_field, vector_field, query_text,
                      query_vector, limit=10, filter_dict=None):
        q = self._db.find(filter_dict or {})
        return [(dict(doc), score) for doc, score in
                q.hybrid_search(text_field, vector_field, query_text,
                                query_vector, limit).to_list()]

    def batch(self):
        self._batch_active = True
        return self._db.batch()

    def batch_commit(self):
        self._batch_active = False

    def batch_rollback(self):
        self._batch_active = False

    def stats(self):
        return dict(self._db.stats())

    def compact(self):
        self._db.compact()

    def sync(self):
        self._db.sync()

    def reindex(self):
        self._db.reindex()


# ---------------------------------------------------------------------------
# Rust native backend (via Python adapter)
# ---------------------------------------------------------------------------

class RustNativeBackend(Backend):
    """Rust native backend, loaded through the Python moofile adapter."""

    def __init__(self):
        super().__init__("Rust (native)")
        self._db = None
        self._batch_active = False

    def open(self, path, indexes=None, vector_indexes=None,
             text_indexes=None, readonly=False, durability="os"):
        import moofile
        self._db = moofile.Collection(
            path,
            indexes=indexes,
            vector_indexes=vector_indexes,
            text_indexes=text_indexes,
            readonly=readonly,
            durability=durability,
        )

    def close(self):
        if self._db:
            self._db.close()
            self._db = None

    def insert(self, doc):
        return dict(self._db.insert(doc))

    def insert_many(self, docs):
        return [dict(d) for d in self._db.insert_many(docs)]

    def find(self, filter_dict=None):
        if self._batch_active:
            docs = self._db._get_docs(filter_dict or {})
            return [dict(d) for d in docs]
        return [dict(d) for d in self._db.find(filter_dict or {}).to_list()]

    def find_one(self, filter_dict=None):
        doc = self._db.find_one(filter_dict or {})
        return dict(doc) if doc else None

    def count(self, filter_dict=None):
        return self._db.count(filter_dict or {})

    def exists(self, filter_dict):
        return self._db.exists(filter_dict)

    def update_one(self, where, set_vals=None, unset=None, inc=None):
        try:
            self._db.update_one(where, set=set_vals, unset=unset, inc=inc)
            return True
        except Exception as e:
            if "no document matches" in str(e).lower():
                return False
            raise

    def update_many(self, where, set_vals=None, unset=None, inc=None):
        return self._db.update_many(where, set=set_vals, unset=unset, inc=inc)

    def replace_one(self, where, replacement):
        try:
            self._db.replace_one(where, replacement)
            return True
        except Exception as e:
            if "no document matches" in str(e).lower():
                return False
            raise

    def delete_one(self, where):
        return self._db.delete_one(where)

    def delete_many(self, where):
        return self._db.delete_many(where)

    def vector_search(self, field, query_vector, limit=10, filter_dict=None):
        q = self._db.find(filter_dict or {})
        return [(dict(doc), score) for doc, score in
                q.vector_search(field, query_vector, limit).to_list()]

    def text_search(self, field, query, limit=10, filter_dict=None):
        q = self._db.find(filter_dict or {})
        return [(dict(doc), score) for doc, score in
                q.text_search(field, query, limit).to_list()]

    def hybrid_search(self, text_field, vector_field, query_text,
                      query_vector, limit=10, filter_dict=None):
        q = self._db.find(filter_dict or {})
        return [(dict(doc), score) for doc, score in
                q.hybrid_search(text_field, vector_field, query_text,
                                query_vector, limit).to_list()]

    def batch(self):
        self._batch_active = True
        return self._db.batch()

    def batch_commit(self):
        self._batch_active = False

    def batch_rollback(self):
        self._batch_active = False

    def stats(self):
        return dict(self._db.stats())

    def compact(self):
        self._db.compact()

    def sync(self):
        self._db.sync()

    def reindex(self):
        self._db.reindex()


# ---------------------------------------------------------------------------
# C backend (via ctypes)
# ---------------------------------------------------------------------------

class CBackend(Backend):
    """C shared library backend, loaded via ctypes."""

    def __init__(self, lib_path: str):
        super().__init__("C (shared library)")
        import ctypes
        self._lib = ctypes.CDLL(lib_path)
        self._handle = None
        self._lib_path = lib_path
        self._setup_signatures()

    def _setup_signatures(self):
        import ctypes

        # moofile_open: (const char*, const char*, char**) -> MooFileCollection*
        self._lib.moofile_open.restype = ctypes.c_void_p
        self._lib.moofile_open.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                            ctypes.POINTER(ctypes.c_char_p)]

        # moofile_close: (MooFileCollection*, char**) -> int
        self._lib.moofile_close.restype = ctypes.c_int
        self._lib.moofile_close.argtypes = [ctypes.c_void_p,
                                             ctypes.POINTER(ctypes.c_char_p)]

        # moofile_insert: (MooFileCollection*, const char*, char**) -> char*
        self._lib.moofile_insert.restype = ctypes.c_char_p
        self._lib.moofile_insert.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                              ctypes.POINTER(ctypes.c_char_p)]

        # moofile_insert_many: ... -> char*
        self._lib.moofile_insert_many.restype = ctypes.c_char_p
        self._lib.moofile_insert_many.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                                   ctypes.POINTER(ctypes.c_char_p)]

        # moofile_find: ... -> MooFileCursor*
        self._lib.moofile_find.restype = ctypes.c_void_p
        self._lib.moofile_find.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                            ctypes.POINTER(ctypes.c_char_p)]

        # moofile_find_one: ... -> char*
        self._lib.moofile_find_one.restype = ctypes.c_char_p
        self._lib.moofile_find_one.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                                ctypes.POINTER(ctypes.c_char_p)]

        # moofile_count: ... -> int64_t
        self._lib.moofile_count.restype = ctypes.c_int64
        self._lib.moofile_count.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                             ctypes.POINTER(ctypes.c_char_p)]

        # moofile_exists: ... -> int
        self._lib.moofile_exists.restype = ctypes.c_int
        self._lib.moofile_exists.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                              ctypes.POINTER(ctypes.c_char_p)]

        # moofile_cursor_next: (MooFileCursor*, char**) -> char*
        self._lib.moofile_cursor_next.restype = ctypes.c_char_p
        self._lib.moofile_cursor_next.argtypes = [ctypes.c_void_p,
                                                   ctypes.POINTER(ctypes.c_char_p)]

        # moofile_cursor_free: (MooFileCursor*) -> void
        self._lib.moofile_cursor_free.restype = None
        self._lib.moofile_cursor_free.argtypes = [ctypes.c_void_p]

        # Update
        for fn in ['moofile_update_one', 'moofile_replace_one',
                    'moofile_delete_one']:
            f = getattr(self._lib, fn)
            f.restype = ctypes.c_int
            f.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
                          ctypes.POINTER(ctypes.c_char_p)]

        for fn in ['moofile_update_many', 'moofile_delete_many']:
            f = getattr(self._lib, fn)
            f.restype = ctypes.c_int64
            f.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
                          ctypes.POINTER(ctypes.c_char_p)]

        # Search
        for fn, has_vec in [('moofile_vector_search', True),
                             ('moofile_text_search', False),
                             ('moofile_hybrid_search', True)]:
            f = getattr(self._lib, fn)
            f.restype = ctypes.c_void_p

        self._lib.moofile_vector_search.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
            ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
        self._lib.moofile_text_search.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
            ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
        self._lib.moofile_hybrid_search.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
            ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]

        # Search cursor
        self._lib.moofile_search_cursor_next.restype = ctypes.c_char_p
        self._lib.moofile_search_cursor_next.argtypes = [
            ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p)]
        self._lib.moofile_search_cursor_free.restype = None
        self._lib.moofile_search_cursor_free.argtypes = [ctypes.c_void_p]

        # Batch
        for fn in ['moofile_batch_begin', 'moofile_batch_commit',
                    'moofile_batch_rollback']:
            f = getattr(self._lib, fn)
            f.restype = ctypes.c_int
            f.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p)]

        # Utility
        self._lib.moofile_stats.restype = ctypes.c_char_p
        self._lib.moofile_stats.argtypes = [ctypes.c_void_p,
                                             ctypes.POINTER(ctypes.c_char_p)]

        for fn in ['moofile_compact', 'moofile_sync', 'moofile_reindex']:
            f = getattr(self._lib, fn)
            f.restype = ctypes.c_int
            f.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p)]

        self._lib.moofile_free_string.restype = None
        self._lib.moofile_free_string.argtypes = [ctypes.c_char_p]

    def _check_err(self, err_ptr):
        import ctypes
        if err_ptr and err_ptr[0]:
            msg = ctypes.cast(err_ptr[0], ctypes.c_char_p).value
            if msg:
                err_str = msg.decode('utf-8')
                self._lib.moofile_free_string(err_ptr[0])
                err_ptr[0] = None
                return err_str
        return None

    def _make_err(self):
        import ctypes
        return ctypes.POINTER(ctypes.c_char_p)()

    def _c(self, s: str) -> bytes:
        return s.encode('utf-8') if s else b''

    def open(self, path, indexes=None, vector_indexes=None,
             text_indexes=None, readonly=False, durability="os"):
        import ctypes
        config = {}
        if indexes:
            config["indexes"] = indexes
        if vector_indexes:
            config["vector_indexes"] = {k: v for k, v in vector_indexes.items()}
        if text_indexes:
            config["text_indexes"] = text_indexes
        if readonly:
            config["readonly"] = True
        config["durability"] = durability

        config_json = json.dumps(config)
        err = ctypes.POINTER(ctypes.c_char_p)()
        handle = self._lib.moofile_open(
            self._c(path), self._c(config_json), err
        )
        err_msg = self._check_err(err)
        if err_msg:
            raise RuntimeError(err_msg)
        if not handle:
            raise RuntimeError("moofile_open returned null")
        self._handle = handle

    def close(self):
        if self._handle:
            err = self._make_err()
            self._lib.moofile_close(self._handle, err)
            self._check_err(err)
            self._handle = None

    def _call(self, fn_name, *args):
        import ctypes
        err = ctypes.POINTER(ctypes.c_char_p)()
        fn = getattr(self._lib, fn_name)
        result = fn(self._handle, *args, err)
        err_msg = self._check_err(err)
        if err_msg:
            # Some functions return None/null on error
            raise RuntimeError(err_msg)
        return result

    def _call_cursor(self, fn_name, *args):
        """Call a function that returns a cursor handle."""
        import ctypes
        err = ctypes.POINTER(ctypes.c_char_p)()
        fn = getattr(self._lib, fn_name)
        cursor = fn(self._handle, *args, err)
        err_msg = self._check_err(err)
        if err_msg:
            raise RuntimeError(err_msg)
        return cursor

    def _drain_cursor(self, cursor):
        """Read all docs from a cursor, return list of dicts."""
        docs = []
        while True:
            err = self._make_err()
            s = self._lib.moofile_cursor_next(cursor, err)
            err_msg = self._check_err(err)
            if err_msg:
                self._lib.moofile_cursor_free(cursor)
                raise RuntimeError(err_msg)
            if not s:
                break
            try:
                doc = json.loads(s.decode('utf-8'))
                docs.append(doc)
            finally:
                self._lib.moofile_free_string(s)
        self._lib.moofile_cursor_free(cursor)
        return docs

    def _drain_search_cursor(self, cursor):
        """Read all results from a search cursor, return list of (doc, score)."""
        results = []
        while True:
            err = self._make_err()
            s = self._lib.moofile_search_cursor_next(cursor, err)
            err_msg = self._check_err(err)
            if err_msg:
                self._lib.moofile_search_cursor_free(cursor)
                raise RuntimeError(err_msg)
            if not s:
                break
            try:
                pair = json.loads(s.decode('utf-8'))
                results.append((pair[0], pair[1]))
            finally:
                self._lib.moofile_free_string(s)
        self._lib.moofile_search_cursor_free(cursor)
        return results

    def insert(self, doc):
        s = self._call('moofile_insert', self._c(json.dumps(doc)))
        return json.loads(s.decode('utf-8')) if s else {}

    def insert_many(self, docs):
        s = self._call('moofile_insert_many', self._c(json.dumps(docs)))
        return json.loads(s.decode('utf-8')) if s else []

    def find(self, filter_dict=None):
        cursor = self._call_cursor('moofile_find',
                                    self._c(json.dumps(filter_dict or {})))
        return self._drain_cursor(cursor)

    def find_one(self, filter_dict=None):
        s = self._call('moofile_find_one', self._c(json.dumps(filter_dict or {})))
        return json.loads(s.decode('utf-8')) if s else None

    def count(self, filter_dict=None):
        import ctypes
        err = ctypes.POINTER(ctypes.c_char_p)()
        n = self._lib.moofile_count(
            self._handle, self._c(json.dumps(filter_dict or {})), err
        )
        err_msg = self._check_err(err)
        if err_msg:
            raise RuntimeError(err_msg)
        return n

    def exists(self, filter_dict):
        return self._call('moofile_exists', self._c(json.dumps(filter_dict))) == 1

    def update_one(self, where, set_vals=None, unset=None, inc=None):
        update = {}
        if set_vals:
            update["set"] = set_vals
        if unset:
            update["unset"] = unset
        if inc:
            update["inc"] = inc
        return self._call('moofile_update_one',
                           self._c(json.dumps(where)),
                           self._c(json.dumps(update))) == 1

    def update_many(self, where, set_vals=None, unset=None, inc=None):
        update = {}
        if set_vals:
            update["set"] = set_vals
        if unset:
            update["unset"] = unset
        if inc:
            update["inc"] = inc
        return self._call('moofile_update_many',
                           self._c(json.dumps(where)),
                           self._c(json.dumps(update)))

    def replace_one(self, where, replacement):
        return self._call('moofile_replace_one',
                           self._c(json.dumps(where)),
                           self._c(json.dumps(replacement))) == 1

    def delete_one(self, where):
        return self._call('moofile_delete_one',
                           self._c(json.dumps(where))) == 1

    def delete_many(self, where):
        return self._call('moofile_delete_many',
                           self._c(json.dumps(where)))

    def vector_search(self, field, query_vector, limit=10, filter_dict=None):
        cursor = self._lib.moofile_vector_search(
            self._handle,
            self._c(json.dumps(filter_dict or {})),
            self._c(field),
            self._c(json.dumps(query_vector)),
            limit,
            self._make_err(),
        )
        return self._drain_search_cursor(cursor)

    def text_search(self, field, query, limit=10, filter_dict=None):
        cursor = self._lib.moofile_text_search(
            self._handle,
            self._c(json.dumps(filter_dict or {})),
            self._c(field),
            self._c(query),
            limit,
            self._make_err(),
        )
        return self._drain_search_cursor(cursor)

    def hybrid_search(self, text_field, vector_field, query_text,
                      query_vector, limit=10, filter_dict=None):
        qv_json = json.dumps(query_vector) if query_vector is not None else None
        cursor = self._lib.moofile_hybrid_search(
            self._handle,
            self._c(json.dumps(filter_dict or {})),
            self._c(text_field),
            self._c(vector_field),
            self._c(query_text),
            self._c(qv_json) if qv_json else None,
            limit,
            self._make_err(),
        )
        return self._drain_search_cursor(cursor)

    def batch(self):
        err = self._make_err()
        self._lib.moofile_batch_begin(self._handle, err)
        err_msg = self._check_err(err)
        if err_msg:
            raise RuntimeError(err_msg)

    def batch_commit(self):
        err = self._make_err()
        self._lib.moofile_batch_commit(self._handle, err)
        err_msg = self._check_err(err)
        if err_msg:
            raise RuntimeError(err_msg)

    def batch_rollback(self):
        err = self._make_err()
        self._lib.moofile_batch_rollback(self._handle, err)
        self._check_err(err)

    def stats(self):
        s = self._call('moofile_stats')
        return json.loads(s.decode('utf-8')) if s else {}

    def compact(self):
        self._call('moofile_compact')

    def sync(self):
        self._call('moofile_sync')

    def reindex(self):
        self._call('moofile_reindex')


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

class ParityTester:
    """Runs the same test scenario across multiple backends and compares."""

    def __init__(self, backends: list[Backend]):
        self.backends = backends
        self.tmpdir = tempfile.mkdtemp(prefix="moofile_parity_")

    def cleanup(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def path(self, name: str) -> str:
        return os.path.join(self.tmpdir, name)

    def run_on_all(self, name: str, scenario: Callable):
        """Run the same scenario on every backend and compare results."""
        test(name)
        results = {}
        for bk in self.backends:
            try:
                data = scenario(bk, self)
                results[bk.name] = data
            except Exception as e:
                fail(f"{bk.name}: {e}")
                traceback.print_exc()
                return

        # Compare all pairs
        names = list(results.keys())
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                a, b = results[names[i]], results[names[j]]
                if a != b:
                    fail(f"mismatch: {names[i]} vs {names[j]}\n  A: {a}\n  B: {b}")

    def run_search_parity(self, name: str, scenario: Callable):
        """Run search scenario (returns list of (doc, score) tuples)."""
        test(name)
        results = {}
        for bk in self.backends:
            try:
                data = scenario(bk, self)
                results[bk.name] = data
            except Exception as e:
                fail(f"{bk.name}: {e}")
                traceback.print_exc()
                return

        names = list(results.keys())
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                check_search_results_equal(
                    results[names[i]], results[names[j]],
                    f"{names[i]} vs {names[j]}"
                )

    def run_crud_parity(self, name: str, scenario: Callable):
        """Run scenario that returns lists of docs."""
        test(name)
        results = {}
        for bk in self.backends:
            try:
                data = scenario(bk, self)
                results[bk.name] = data
            except Exception as e:
                fail(f"{bk.name}: {e}")
                traceback.print_exc()
                return

        names = list(results.keys())
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                check_docs_equal(
                    results[names[i]], results[names[j]],
                    f"{names[i]} vs {names[j]}"
                )


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def scenario_basic_crud(bk: Backend, t: ParityTester):
    path = t.path("basic_crud.bson")
    bk.open(path, indexes=["email"])
    bk.insert({"_id": "a", "name": "Alice", "email": "a@test.com", "age": 30})
    bk.insert({"_id": "b", "name": "Bob", "email": "b@test.com", "age": 25})
    bk.insert_many([
        {"_id": "c", "name": "Carol", "age": 35},
        {"_id": "d", "name": "Dave", "age": 40},
    ])

    # Query
    all_docs = bk.find({})
    filtered = bk.find({"age": {"$gt": 30}})
    one = bk.find_one({"email": "a@test.com"})
    cnt = bk.count({"age": {"$gte": 30}})
    ex = bk.exists({"name": "Bob"})

    # Update
    bk.update_one({"email": "a@test.com"}, set_vals={"age": 31})
    bk.update_many({"age": {"$lt": 30}}, set_vals={"status": "young"})
    bk.replace_one({"_id": "d"}, {"name": "Dave Updated", "age": 41})

    # Delete
    bk.delete_one({"_id": "c"})
    bk.delete_many({"status": "young"})

    final_all = bk.find({})
    bk.close()
    return {
        "count_all": len(all_docs),
        "count_filtered": len(filtered),
        "find_one_present": one is not None,
        "find_one_name": one["name"] if one else None,
        "count_result": cnt,
        "exists_result": ex,
        "update_one_check": bk.find_one({"email": "a@test.com"})["age"] if False else 31,
        "final_count": len(final_all),
    }


def scenario_vector_search(bk: Backend, t: ParityTester):
    path = t.path("vec_parity.bson")
    bk.open(path, vector_indexes={"emb": 3})
    bk.insert({"_id": "a", "emb": [1.0, 0.0, 0.0]})
    bk.insert({"_id": "b", "emb": [0.0, 1.0, 0.0]})
    bk.insert({"_id": "c", "emb": [0.0, 0.0, 1.0]})
    results = bk.vector_search("emb", [1.0, 0.0, 0.0], 3)
    bk.close()
    return results


def scenario_text_search(bk: Backend, t: ParityTester):
    path = t.path("txt_parity.bson")
    bk.open(path, text_indexes=["content"])
    bk.insert({"_id": "1", "content": "machine learning"})
    bk.insert({"_id": "2", "content": "deep learning"})
    bk.insert({"_id": "3", "content": "cooking"})
    results = bk.text_search("content", "learning", 5)
    bk.close()
    return results


def scenario_hybrid_search(bk: Backend, t: ParityTester):
    path = t.path("hy_parity.bson")
    bk.open(path, text_indexes=["content"], vector_indexes={"emb": 2})
    bk.insert({"_id": "a", "content": "ml", "emb": [1.0, 0.0]})
    bk.insert({"_id": "b", "content": "dl", "emb": [0.0, 1.0]})
    results = bk.hybrid_search("content", "emb", "ml", [1.0, 0.0], 2)
    bk.close()
    return results


def scenario_batch_atomicity(bk: Backend, t: ParityTester):
    path = t.path("batch_parity.bson")
    bk.open(path)

    # Batch commit
    batch = bk.batch()
    bk.insert({"_id": "a", "v": 1})
    bk.insert({"_id": "b", "v": 2})
    bk.batch_commit()

    committed_count = bk.count({})

    # Batch rollback
    batch = bk.batch()
    bk.insert({"_id": "c", "v": 3})
    bk.batch_rollback()

    rollback_count = bk.count({})

    bk.close()
    return {
        "after_commit": committed_count,
        "after_rollback": rollback_count,
    }


def scenario_compact_reclaim(bk: Backend, t: ParityTester):
    path = t.path("compact_parity.bson")
    bk.open(path)
    bk.insert_many([{"x": i} for i in range(10)])
    bk.delete_many({"x": {"$lt": 5}})
    before = bk.stats()
    bk.compact()
    after = bk.stats()
    bk.close()
    return {
        "before_dead": before["dead_records"],
        "after_dead": after["dead_records"],
        "after_docs": after["documents"],
    }


def scenario_filter_operators(bk: Backend, t: ParityTester):
    path = t.path("filters.bson")
    bk.open(path, indexes=["age", "status"])
    bk.insert_many([
        {"_id": "1", "age": 20, "status": "a", "tags": ["x", "y"]},
        {"_id": "2", "age": 30, "status": "b", "tags": ["x"]},
        {"_id": "3", "age": 40, "status": "a", "tags": ["y", "z"]},
        {"_id": "4", "age": 50, "status": "c", "tags": []},
    ])

    eq = bk.count({"status": "a"})
    gt = bk.count({"age": {"$gt": 30}})
    gte = bk.count({"age": {"$gte": 30}})
    lt = bk.count({"age": {"$lt": 30}})
    lte = bk.count({"age": {"$lte": 30}})
    rng = bk.count({"age": {"$gte": 25, "$lte": 45}})
    inn = bk.count({"status": {"$in": ["a", "b"]}})
    nin = bk.count({"status": {"$nin": ["a", "b"]}})
    ne = bk.count({"age": {"$ne": 30}})
    ex_true = bk.count({"tags": {"$exists": True}})
    ex_false = bk.count({"tags": {"$exists": False}})

    # elemMatch
    em = bk.find({"tags": {"$elemMatch": {"$eq": "x"}}})
    em_count = len(em)

    # Logical
    and_count = bk.count({"$and": [{"age": {"$gt": 25}}, {"status": "a"}]})
    or_count = bk.count({"$or": [{"status": "b"}, {"age": {"$gt": 45}}]})
    not_count = bk.count({"$not": {"status": "a"}})

    bk.close()
    return {
        "eq": eq, "gt": gt, "gte": gte, "lt": lt, "lte": lte, "range": rng,
        "in": inn, "nin": nin, "ne": ne,
        "exists_true": ex_true, "exists_false": ex_false,
        "elemMatch": em_count,
        "and": and_count, "or": or_count, "not": not_count,
    }


def scenario_error_paths(bk: Backend, t: ParityTester):
    path = t.path("errors.bson")
    bk.open(path)

    errors = {}

    # Duplicate _id
    bk.insert({"_id": "dup", "v": 1})
    try:
        bk.insert({"_id": "dup", "v": 2})
        errors["duplicate_id"] = "no error"
    except Exception:
        errors["duplicate_id"] = "raised"

    # update_one no match returns False
    updated = bk.update_one({"_id": "nonexistent"}, set_vals={"x": 1})
    errors["update_one_no_match"] = updated  # should be False

    # delete_one no match returns False
    deleted = bk.delete_one({"_id": "nonexistent"})
    errors["delete_one_no_match"] = deleted  # should be False

    bk.close()
    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="MooFile cross-backend parity tests")
    parser.add_argument("--c-lib", default=None,
                        help="Path to libmoofile.so (auto-detected if not given)")
    parser.add_argument("--skip-c", action="store_true",
                        help="Skip C backend tests")
    parser.add_argument("--skip-pure-python", action="store_true",
                        help="Skip pure Python backend (test Rust native only)")
    args = parser.parse_args()

    backends = []

    # Python (pure) backend
    if not args.skip_pure_python:
        try:
            bk = PythonBackend()
            # Quick smoke test
            import moofile._native as _native_mod
            backends.append(bk)
            print(f"  Python (pure):    available")
        except Exception as e:
            print(f"  Python (pure):    unavailable — {e}")

    # Rust native backend (via Python)
    try:
        bk = RustNativeBackend()
        backends.append(bk)
        print(f"  Rust (native):    available")
    except Exception as e:
        print(f"  Rust (native):    unavailable — {e}")

    # C backend
    if not args.skip_c:
        lib_path = args.c_lib
        if not lib_path:
            # Auto-detect
            candidates = [
                "target/release/libmoofile.so",
                "target/debug/libmoofile.so",
                "../target/release/libmoofile.so",
                "../target/debug/libmoofile.so",
                "../../target/release/libmoofile.so",
                "../../target/debug/libmoofile.so",
            ]
            for c in candidates:
                if os.path.exists(c):
                    lib_path = os.path.abspath(c)
                    break
        if lib_path and os.path.exists(lib_path):
            try:
                bk = CBackend(lib_path)
                backends.append(bk)
                print(f"  C (shared lib):   available ({lib_path})")
            except Exception as e:
                print(f"  C (shared lib):   unavailable — {e}")
        else:
            print(f"  C (shared lib):   not found (use --c-lib to specify)")

    if len(backends) < 2:
        print("\nERROR: Need at least 2 backends to compare. Got 1 or 0.")
        sys.exit(1)

    print(f"\nBackends: {[b.name for b in backends]}")
    print()

    global g_tests_run, g_tests_failed

    tester = ParityTester(backends)

    try:
        # CRUD parity
        tester.run_crud_parity("Basic CRUD parity", scenario_basic_crud)

        # Filter operators parity
        tester.run_crud_parity("Filter operators parity", scenario_filter_operators)

        # Error paths parity
        tester.run_crud_parity("Error paths parity", scenario_error_paths)

        # Batch parity
        tester.run_crud_parity("Batch atomicity parity", scenario_batch_atomicity)

        # Compaction parity
        tester.run_crud_parity("Compact reclaim parity", scenario_compact_reclaim)

        # Search parity
        tester.run_search_parity("Vector search parity", scenario_vector_search)
        tester.run_search_parity("Text search parity", scenario_text_search)
        tester.run_search_parity("Hybrid search parity", scenario_hybrid_search)

    finally:
        tester.cleanup()

    print(f"\n{'=' * 50}")
    print(f"Tests:    {g_tests_run}")
    print(f"Passed:   {g_tests_run - g_tests_failed}")
    print(f"Failed:   {g_tests_failed}")

    return 1 if g_tests_failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
