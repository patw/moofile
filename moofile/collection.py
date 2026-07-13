"""Main Collection class — the primary public interface to MooFile."""

import binascii
import json
import os
import pickle
import struct
import time
from datetime import datetime, timezone

from .errors import DocumentNotFoundError, DuplicateKeyError, ReadOnlyError
from .index import IndexManager
from .query import Query, matches
from .storage import (
    RECORD_LIVE,
    RECORD_REPLACEMENT,
    RECORD_TOMBSTONE,
    StorageEngine,
    compact,
    scan_file,
)


def _generate_id() -> str:
    """Generate a random 12-byte hex string for use as _id."""
    return binascii.hexlify(os.urandom(12)).decode()


class Collection:
    """
    An embedded, single-file document store.

    Usage::

        db = Collection("mydata.bson", indexes=["email", "age"])
        doc = db.insert({"name": "alice", "email": "alice@example.com"})
        results = db.find({"age": {"$gt": 25}}).sort("age").to_list()

    Use as a context manager for automatic close::

        with Collection("mydata.bson") as db:
            db.insert({"name": "bob"})
    """

    def __init__(
        self,
        path: str,
        indexes=None,
        vector_indexes=None,
        text_indexes=None,
        readonly: bool = False,
        schema=None,
        durability: str = "os",
    ) -> None:
        self._path = path
        self._readonly = readonly
        self._schema = schema  # informational only in v1; not enforced
        self._meta_path = path + ".meta"
        self._cache_path = path + ".cache"
        self._lock_path = path + ".lock"
        self._total_records: int = 0
        self._storage: StorageEngine | None = None
        self._loaded_from_cache: bool = False
        self._dirty: bool = False
        self._lock_fd = None
        self._batch: "BatchContext | None" = None

        declared = list(indexes or [])
        vector_fields = dict(vector_indexes or {})
        text_fields = list(text_indexes or [])

        # Acquire advisory lock to prevent silent corruption from
        # multi-process access.
        self._acquire_lock()

        if not readonly:
            # Create the data file if it does not exist
            if not os.path.exists(path):
                open(path, "wb").close()
            self._save_meta(declared, vector_fields, text_fields)

        loaded_indexes, loaded_vector_indexes, loaded_text_indexes = self._load_meta(
            declared, vector_fields, text_fields
        )
        self._storage = StorageEngine(path, readonly=readonly, durability=durability)
        self._index_manager = IndexManager(
            loaded_indexes, loaded_vector_indexes, loaded_text_indexes
        )
        self._load_from_file()

    # -----------------------------------------------------------------------
    # Insert
    # -----------------------------------------------------------------------

    def insert(self, doc: dict) -> dict:
        """
        Insert a single document.

        If _id is absent it is generated automatically.
        Returns the document with _id populated.
        Raises DuplicateKeyError if _id already exists.
        """
        self._require_write()
        if self._batch is not None:
            return self._batch.insert(doc)
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = _generate_id()
        if self._index_manager.get(doc["_id"]) is not None:
            raise DuplicateKeyError(f"Duplicate _id: {doc['_id']!r}")
        self._storage.append(RECORD_LIVE, doc)
        self._index_manager.add(doc)
        self._total_records += 1
        self._dirty = True
        return doc

    def insert_many(self, docs: list) -> list:
        """Insert multiple documents. Returns a list of inserted documents."""
        return [self.insert(doc) for doc in docs]

    # -----------------------------------------------------------------------
    # Update
    # -----------------------------------------------------------------------

    def update_one(
        self,
        where: dict,
        set: dict = None,
        unset: list = None,
        inc: dict = None,
    ) -> bool:
        """
        Update the first document matching *where*.

        Operators:
            set   – dict of field→value to set
            unset – list of field names to remove
            inc   – dict of field→delta to increment

        Returns True if a document was updated.
        Raises DocumentNotFoundError if no document matches.
        """
        self._require_write()
        if self._batch is not None:
            return self._batch.update_one(where, set, unset, inc)
        docs = self._get_docs(where)
        if not docs:
            raise DocumentNotFoundError(f"No document matches: {where!r}")
        old_doc = docs[0]
        new_doc = _apply_update(old_doc, set, unset, inc)
        self._storage.append(RECORD_REPLACEMENT, new_doc)
        self._index_manager.remove(old_doc["_id"])
        self._index_manager.add(new_doc)
        self._total_records += 1
        self._dirty = True
        return True

    def update_many(
        self,
        where: dict,
        set: dict = None,
        unset: list = None,
        inc: dict = None,
    ) -> int:
        """
        Update all documents matching *where*.

        Returns the count of updated documents.
        """
        self._require_write()
        if self._batch is not None:
            return self._batch.update_many(where, set, unset, inc)
        docs = self._get_docs(where)
        count = 0
        for old_doc in docs:
            new_doc = _apply_update(old_doc, set, unset, inc)
            self._storage.append(RECORD_REPLACEMENT, new_doc)
            self._index_manager.remove(old_doc["_id"])
            self._index_manager.add(new_doc)
            self._total_records += 1
            count += 1
        if count > 0:
            self._dirty = True
        return count

    def replace_one(self, where: dict, new_doc: dict) -> bool:
        """
        Replace the entire document matching *where* with *new_doc*.

        The original _id is preserved.
        Raises DocumentNotFoundError if no document matches.
        """
        self._require_write()
        if self._batch is not None:
            return self._batch.replace_one(where, new_doc)
        docs = self._get_docs(where)
        if not docs:
            raise DocumentNotFoundError(f"No document matches: {where!r}")
        old_doc = docs[0]
        replacement = dict(new_doc)
        replacement["_id"] = old_doc["_id"]
        self._storage.append(RECORD_REPLACEMENT, replacement)
        self._index_manager.remove(old_doc["_id"])
        self._index_manager.add(replacement)
        self._total_records += 1
        self._dirty = True
        return True

    # -----------------------------------------------------------------------
    # Delete
    # -----------------------------------------------------------------------

    def delete_one(self, where: dict) -> bool:
        """
        Delete the first document matching *where*.

        Returns True if a document was deleted, False if nothing matched.
        """
        self._require_write()
        if self._batch is not None:
            return self._batch.delete_one(where)
        docs = self._get_docs(where)
        if not docs:
            return False
        doc = docs[0]
        self._storage.append(RECORD_TOMBSTONE, {"_id": doc["_id"]})
        self._index_manager.remove(doc["_id"])
        self._total_records += 1
        self._dirty = True
        return True

    def delete_many(self, where: dict) -> int:
        """
        Delete all documents matching *where*.

        Returns the count of deleted documents.
        """
        self._require_write()
        if self._batch is not None:
            return self._batch.delete_many(where)
        docs = self._get_docs(where)
        count = 0
        for doc in docs:
            self._storage.append(RECORD_TOMBSTONE, {"_id": doc["_id"]})
            self._index_manager.remove(doc["_id"])
            self._total_records += 1
            count += 1
        if count > 0:
            self._dirty = True
        return count

    # -----------------------------------------------------------------------
    # Query
    # -----------------------------------------------------------------------

    def find(self, filter_dict: dict = None) -> Query:
        """Return a lazy Query object. No work is done until a terminal method is called."""
        return Query(self, filter_dict or {})

    def find_one(self, filter_dict: dict = None):
        """Return the first matching document, or None."""
        return self.find(filter_dict or {}).first()

    def count(self, filter_dict: dict = None) -> int:
        """Count documents matching *filter_dict* (all documents if omitted)."""
        return self._count_docs(filter_dict or {})

    def exists(self, filter_dict: dict) -> bool:
        """Return True if at least one document matches *filter_dict*."""
        return self.find_one(filter_dict) is not None

    # -----------------------------------------------------------------------
    # Utility
    # -----------------------------------------------------------------------

    def stats(self) -> dict:
        """
        Return database statistics::

            {
                "documents":       42150,
                "dead_records":    3201,
                "file_size_bytes": 8421000,
                "dead_ratio":      0.07,
            }
        """
        live = len(self._index_manager._documents)
        dead = self._total_records - live
        file_size = os.path.getsize(self._path) if os.path.exists(self._path) else 0
        ratio = dead / self._total_records if self._total_records > 0 else 0.0
        return {
            "documents": live,
            "dead_records": dead,
            "file_size_bytes": file_size,
            "dead_ratio": ratio,
        }

    def compact(self) -> None:
        """
        Rewrite the data file keeping only the latest live version of each document.

        Safe to interrupt — if it fails the original file is untouched.
        """
        self._require_write()
        live_docs = self._index_manager.all_docs()
        self._storage.close()
        try:
            compact(self._path, live_docs)
        finally:
            self._storage.reopen()
        # After compaction total_records == live document count
        self._total_records = len(live_docs)
        # The BSON file was rewritten — cache is definitely stale.
        self._delete_cache()
        self._dirty = True

    def sync(self) -> None:
        """
        Flush and fsync the data file, ensuring all buffered writes are
        durable on disk.

        With durability='os' (default) or durability='none', writes are
        only flushed to the OS page cache.  This method forces an fsync,
        making all prior writes durable across power loss.

        Useful for batched durability: insert many documents with the fast
        default durability, then call sync() once.
        """
        if self._storage is not None:
            self._storage.sync()

    def batch(self) -> "BatchContext":
        """
        Return a context manager for atomic batch writes.

        All write operations (insert, update, delete) performed within
        the ``with`` block are buffered and applied atomically on
        commit — a single storage append, a single flush/fsync, and
        all index mutations applied together.

        If an exception occurs within the ``with`` block, the batch is
        rolled back entirely: no records are appended and no indexes
        are mutated.

        Usage::

            with db.batch() as b:
                db.insert({"name": "alice"})
                db.update_one({"name": "bob"}, set={"status": "active"})
                db.delete_one({"name": "charlie"})
            # All three operations committed atomically here.

        Properties:
            - **Transactional visibility**: reads within the batch see
              the pre-batch state.  Buffered writes become visible only
              after commit.
            - **Batched I/O**: all records are appended in a single write
              with one flush/fsync.
            - **Rollback on exception**: if the ``with`` block raises,
              the batch is discarded.
            - **Crash semantics**: a crash mid-batch may commit a prefix
              of the batch (same as per-record semantics).
        """
        self._require_write()
        return BatchContext(self)

    def reindex(self) -> None:
        """Rebuild all in-memory indexes by re-scanning the data file."""
        self._loaded_from_cache = False
        self._dirty = True
        self._load_from_file()

    def close(self) -> None:
        """Close the collection, saving the cache if needed (Option B).

        - Loaded from cache, no writes → skip (cache still valid).
        - Rebuilt from scan, or writes occurred → write a fresh cache.
        """
        if self._storage is not None:
            # Close the storage handle FIRST so the data file's mtime
            # is settled before we capture it for the cache fingerprint.
            self._storage.close()
            self._storage = None

        if not self._loaded_from_cache or self._dirty:
            self._save_cache()

        self._release_lock()

    # --- Context manager protocol ---

    def __enter__(self) -> "Collection":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    # -----------------------------------------------------------------------
    # Internal helpers (used by Query)
    # -----------------------------------------------------------------------

    def _get_docs(self, filter_dict: dict) -> list:
        """Return all documents matching filter_dict, using indexes when possible."""
        if not filter_dict:
            return self._index_manager.all_docs()

        candidates = self._try_index(filter_dict)
        if candidates is not None:
            return [doc for doc in candidates if matches(doc, filter_dict)]

        # Full scan
        return [
            doc
            for doc in self._index_manager.all_docs()
            if matches(doc, filter_dict)
        ]

    def _count_docs(self, filter_dict: dict) -> int:
        if not filter_dict:
            return len(self._index_manager._documents)
        return len(self._get_docs(filter_dict))

    def _try_index(self, filter_dict: dict) -> list | None:
        """
        Attempt to use an index for the filter.

        Returns a (possibly over-broad) candidate list, or None if no index
        can be applied and a full scan is needed.
        """
        # Logical operators at the top level — can't use a single index
        for key in filter_dict:
            if key.startswith("$"):
                return None

        # Look for the first top-level field that is indexed
        for field, condition in filter_dict.items():
            if field not in self._index_manager._indexes:
                continue

            if not isinstance(condition, dict):
                # Implicit $eq — exact lookup
                return self._index_manager.get_by_field_exact(field, condition)

            op_keys = set(condition.keys())

            if "$eq" in op_keys:
                return self._index_manager.get_by_field_exact(field, condition["$eq"])

            range_ops = op_keys & {"$gt", "$gte", "$lt", "$lte"}
            if range_ops and not (op_keys - range_ops):
                # Only range operators present — use range scan
                min_val = max_val = None
                min_inc = max_inc = True
                for op, val in condition.items():
                    if op == "$gt":
                        min_val, min_inc = val, False
                    elif op == "$gte":
                        min_val, min_inc = val, True
                    elif op == "$lt":
                        max_val, max_inc = val, False
                    elif op == "$lte":
                        max_val, max_inc = val, True
                return self._index_manager.get_by_field_range(
                    field, min_val, max_val, min_inc, max_inc
                )

        return None  # no usable index found

    # -----------------------------------------------------------------------
    # File management helpers
    # -----------------------------------------------------------------------

    def _acquire_lock(self) -> None:
        """Acquire an advisory lock on a .lock file to detect concurrent access."""
        try:
            import fcntl
        except ImportError:
            return  # Windows — best-effort, no locking in pure-Python fallback

        try:
            self._lock_fd = open(self._lock_path, "a+")
            if self._readonly:
                fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            else:
                fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError):
            from .errors import ConcurrentAccessError
            raise ConcurrentAccessError(
                f"File is already open by another process: {self._lock_path}"
            )

    def _release_lock(self) -> None:
        """Release the advisory lock."""
        if self._lock_fd is not None:
            try:
                self._lock_fd.close()
            except OSError:
                pass
            self._lock_fd = None

    def _load_from_file(self) -> None:
        """Scan the BSON file and build in-memory indexes from scratch,
        or load from cache if valid."""
        self._index_manager.clear()
        self._total_records = 0

        if not os.path.exists(self._path):
            return

        # --- Try the disposable cache first ---
        if self._try_load_cache():
            self._loaded_from_cache = True
            return
        self._loaded_from_cache = False

        # --- Cache miss: rebuild from BSON scan ---
        records, truncate_to = scan_file(self._path)

        # Truncate partial trailing write if needed
        if truncate_to is not None and not self._readonly:
            self._storage.close()
            with open(self._path, "r+b") as f:
                f.truncate(truncate_to)
            self._storage.reopen()

        self._total_records = len(records)

        # Replay records; the last record for any _id wins
        for _offset, record_type, doc in records:
            _id = doc.get("_id")
            if _id is None:
                continue
            if record_type in (RECORD_LIVE, RECORD_REPLACEMENT):
                # Remove any previous version from the index
                if self._index_manager.get(_id) is not None:
                    self._index_manager.remove(_id)
                self._index_manager.add(doc)
            elif record_type == RECORD_TOMBSTONE:
                self._index_manager.remove(_id)

        # Rebuild vector indexes after loading all documents
        self._index_manager.rebuild_vector_indexes()

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def _file_fingerprint(self) -> tuple[int, float] | None:
        """Return (size, mtime_ns) for the data file, or None if unreadable."""
        try:
            stat = os.stat(self._path)
            return (stat.st_size, stat.st_mtime_ns)
        except OSError:
            return None

    def _try_load_cache(self) -> bool:
        """Try to load the index cache.  Returns True on hit, False on miss."""
        if not os.path.exists(self._cache_path):
            return False

        # Current data file fingerprint
        fp = self._file_fingerprint()
        if fp is None:
            return False
        actual_len, actual_mtime_ns = fp

        try:
            with open(self._cache_path, "rb") as f:
                cache = pickle.load(f)
        except Exception:
            return False  # corrupt or wrong format

        # Validate magic + version
        if cache.get("_magic") != b"MOOF" or cache.get("_version") != 1:
            return False

        # Validate data file fingerprint
        if cache.get("_data_file_length") != actual_len:
            return False
        if cache.get("_data_file_mtime_ns") != actual_mtime_ns:
            return False

        # Validate index configuration matches
        expected_regular = set(self._index_manager._fields)
        expected_vector = set(self._index_manager._vector_fields.items())
        expected_text = set(self._index_manager._text_fields)

        if set(cache.get("_regular_fields", [])) != expected_regular:
            return False
        if set(cache.get("_vector_fields", {}).items()) != expected_vector:
            return False
        if set(cache.get("_text_fields", [])) != expected_text:
            return False

        # --- Cache hit: reconstruct the IndexManager ---
        self._index_manager._documents = cache["_documents"]
        self._index_manager._indexes = cache["_indexes"]
        self._index_manager._vector_indexes = cache["_vector_indexes"]
        self._index_manager._text_indexes = cache["_text_indexes"]
        self._index_manager._vectors_dirty = False
        self._total_records = cache["_total_records"]
        return True

    def _save_cache(self) -> None:
        """Save the current index state to a cache file (best-effort)."""
        if self._readonly:
            return

        fp = self._file_fingerprint()
        if fp is None:
            return
        data_len, data_mtime_ns = fp

        cache = {
            "_magic": b"MOOF",
            "_version": 1,
            "_data_file_length": data_len,
            "_data_file_mtime_ns": data_mtime_ns,
            "_total_records": self._total_records,
            "_regular_fields": list(self._index_manager._fields),
            "_vector_fields": dict(self._index_manager._vector_fields),
            "_text_fields": list(self._index_manager._text_fields),
            "_documents": self._index_manager._documents,
            "_indexes": self._index_manager._indexes,
            "_vector_indexes": self._index_manager._vector_indexes,
            "_text_indexes": self._index_manager._text_indexes,
        }

        tmp_path = self._cache_path + ".tmp"
        try:
            with open(tmp_path, "wb") as f:
                pickle.dump(cache, f, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp_path, self._cache_path)
        except Exception:
            # Cache is disposable — never fail on write error.
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    def _delete_cache(self) -> None:
        """Delete the cache file if it exists."""
        try:
            os.remove(self._cache_path)
        except OSError:
            pass

    def _save_meta(self, indexes: list, vector_indexes: dict = None, text_indexes: list = None) -> None:
        """Persist (or update) the .meta file with the given index configurations."""
        existing: dict = {}
        if os.path.exists(self._meta_path):
            try:
                with open(self._meta_path) as f:
                    existing = json.load(f)
            except (json.JSONDecodeError, OSError):
                pass

        existing_indexes = existing.get("indexes", [])
        existing_vector_indexes = existing.get("vector_indexes", {})
        existing_text_indexes = existing.get("text_indexes", [])
        
        # Merge regular indexes, preserving order and removing duplicates
        merged_indexes = list(dict.fromkeys(existing_indexes + indexes))
        
        # Merge vector indexes
        merged_vector_indexes = {**existing_vector_indexes, **(vector_indexes or {})}
        
        # Merge text indexes
        merged_text_indexes = list(dict.fromkeys(existing_text_indexes + (text_indexes or [])))

        meta = {
            "version": 1,
            "indexes": merged_indexes,
            "vector_indexes": merged_vector_indexes,
            "text_indexes": merged_text_indexes,
            "created_at": existing.get(
                "created_at",
                datetime.now(timezone.utc).isoformat(),
            ),
        }
        with open(self._meta_path, "w") as f:
            json.dump(meta, f, indent=2)

    def _load_meta(self, declared: list, vector_indexes: dict, text_indexes: list) -> tuple:
        """Load persisted indexes from .meta and merge with declared indexes."""
        if os.path.exists(self._meta_path):
            try:
                with open(self._meta_path) as f:
                    meta = json.load(f)
                
                # Merge regular indexes
                persisted_indexes = meta.get("indexes", [])
                merged_indexes = list(dict.fromkeys(persisted_indexes + declared))
                
                # Merge vector indexes
                persisted_vector = meta.get("vector_indexes", {})
                merged_vector = {**persisted_vector, **vector_indexes}
                
                # Merge text indexes  
                persisted_text = meta.get("text_indexes", [])
                merged_text = list(dict.fromkeys(persisted_text + text_indexes))
                
                return merged_indexes, merged_vector, merged_text
                
            except (json.JSONDecodeError, OSError):
                pass
        return declared, vector_indexes, text_indexes

    def _require_write(self) -> None:
        if self._readonly:
            raise ReadOnlyError("Collection is open in read-only mode")


# ---------------------------------------------------------------------------
# Batch writes
# ---------------------------------------------------------------------------

class BatchContext:
    """
    Context manager for atomic batch writes.

    Created via ``db.batch()``.  See :meth:`Collection.batch` for usage.
    """

    def __init__(self, collection: "Collection") -> None:
        self._collection = collection
        # Buffered storage appends: [(record_type, doc), ...]
        self._records: list[tuple[int, dict]] = []
        # Buffered index mutations: [("add", doc) | ("remove", _id), ...]
        self._index_ops: list[tuple[str, ...]] = []
        # Working state overlay for validation: _id -> doc | None(deleted)
        self._overlay: dict = {}
        self._count: int = 0

    def __enter__(self) -> "BatchContext":
        self._collection._batch = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._collection._batch = None
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        return False  # don't suppress exceptions

    # --- Validation helpers ---

    def _get(self, _id):
        """Get a doc from the batch overlay or the live index."""
        if _id in self._overlay:
            return self._overlay[_id]  # None means deleted in this batch
        return self._collection._index_manager.get(_id)

    def _get_docs(self, filter_dict: dict) -> list:
        """Get docs matching filter from current view (pre-batch + batch changes)."""
        # Build current view: start with all live docs
        docs = {}
        for doc in self._collection._index_manager.all_docs():
            docs[doc["_id"]] = doc
        # Apply overlay
        for _id, doc in self._overlay.items():
            if doc is None:
                docs.pop(_id, None)
            else:
                docs[_id] = doc
        if not filter_dict:
            return list(docs.values())
        from .query import matches
        return [doc for doc in docs.values() if matches(doc, filter_dict)]

    # --- Buffered write operations ---

    def insert(self, doc: dict) -> dict:
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = _generate_id()
        if self._get(doc["_id"]) is not None:
            raise DuplicateKeyError(f"Duplicate _id: {doc['_id']!r}")
        self._records.append((RECORD_LIVE, doc))
        self._index_ops.append(("add", doc))
        self._overlay[doc["_id"]] = doc
        self._count += 1
        return doc

    def insert_many(self, docs: list) -> list:
        return [self.insert(doc) for doc in docs]

    def update_one(
        self,
        where: dict,
        set: dict = None,
        unset: list = None,
        inc: dict = None,
    ) -> bool:
        docs = self._get_docs(where)
        if not docs:
            raise DocumentNotFoundError(f"No document matches: {where!r}")
        old_doc = docs[0]
        new_doc = _apply_update(old_doc, set, unset, inc)
        self._records.append((RECORD_REPLACEMENT, new_doc))
        self._index_ops.append(("remove", old_doc["_id"]))
        self._index_ops.append(("add", new_doc))
        self._overlay[old_doc["_id"]] = new_doc
        self._count += 1
        return True

    def update_many(
        self,
        where: dict,
        set: dict = None,
        unset: list = None,
        inc: dict = None,
    ) -> int:
        docs = self._get_docs(where)
        count = 0
        for old_doc in docs:
            new_doc = _apply_update(old_doc, set, unset, inc)
            self._records.append((RECORD_REPLACEMENT, new_doc))
            self._index_ops.append(("remove", old_doc["_id"]))
            self._index_ops.append(("add", new_doc))
            self._overlay[old_doc["_id"]] = new_doc
            self._count += 1
            count += 1
        return count

    def replace_one(self, where: dict, new_doc: dict) -> bool:
        docs = self._get_docs(where)
        if not docs:
            raise DocumentNotFoundError(f"No document matches: {where!r}")
        old_doc = docs[0]
        replacement = dict(new_doc)
        replacement["_id"] = old_doc["_id"]
        self._records.append((RECORD_REPLACEMENT, replacement))
        self._index_ops.append(("remove", old_doc["_id"]))
        self._index_ops.append(("add", replacement))
        self._overlay[old_doc["_id"]] = replacement
        self._count += 1
        return True

    def delete_one(self, where: dict) -> bool:
        docs = self._get_docs(where)
        if not docs:
            return False
        doc = docs[0]
        self._records.append((RECORD_TOMBSTONE, {"_id": doc["_id"]}))
        self._index_ops.append(("remove", doc["_id"]))
        self._overlay[doc["_id"]] = None
        self._count += 1
        return True

    def delete_many(self, where: dict) -> int:
        docs = self._get_docs(where)
        count = 0
        for doc in docs:
            self._records.append((RECORD_TOMBSTONE, {"_id": doc["_id"]}))
            self._index_ops.append(("remove", doc["_id"]))
            self._overlay[doc["_id"]] = None
            self._count += 1
            count += 1
        return count

    # --- Commit / Rollback ---

    def commit(self) -> None:
        """Apply all buffered operations atomically."""
        if not self._records:
            return
        # Append all records in a single write with one flush
        self._collection._storage.append_batch(self._records)
        # Apply all index mutations in order
        for op in self._index_ops:
            if op[0] == "add":
                self._collection._index_manager.add(op[1])
            elif op[0] == "remove":
                self._collection._index_manager.remove(op[1])
        self._collection._total_records += self._count
        self._collection._dirty = True
        self._records.clear()
        self._index_ops.clear()
        self._overlay.clear()
        self._count = 0

    def rollback(self) -> None:
        """Discard all buffered operations."""
        self._records.clear()
        self._index_ops.clear()
        self._overlay.clear()
        self._count = 0


# ---------------------------------------------------------------------------
# Module-level helper
# ---------------------------------------------------------------------------

def _apply_update(
    doc: dict,
    set_dict: dict | None,
    unset_list: list | None,
    inc_dict: dict | None,
) -> dict:
    """Apply $set / $unset / $inc operators to a copy of *doc*."""
    new_doc = dict(doc)
    if set_dict:
        new_doc.update(set_dict)
    if unset_list:
        for field in unset_list:
            new_doc.pop(field, None)
    if inc_dict:
        for field, delta in inc_dict.items():
            new_doc[field] = new_doc.get(field, 0) + delta
    return new_doc
