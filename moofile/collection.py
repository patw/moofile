"""Main Collection class — the primary public interface to MooFile."""

import binascii
import json
import os
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
    ) -> None:
        self._path = path
        self._readonly = readonly
        self._schema = schema  # informational only in v1; not enforced
        self._meta_path = path + ".meta"
        self._total_records: int = 0
        self._storage: StorageEngine | None = None

        declared = list(indexes or [])
        vector_fields = dict(vector_indexes or {})
        text_fields = list(text_indexes or [])

        if not readonly:
            # Create the data file if it does not exist
            if not os.path.exists(path):
                open(path, "wb").close()
            self._save_meta(declared, vector_fields, text_fields)

        loaded_indexes, loaded_vector_indexes, loaded_text_indexes = self._load_meta(
            declared, vector_fields, text_fields
        )
        self._storage = StorageEngine(path, readonly=readonly)
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
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = _generate_id()
        if self._index_manager.get(doc["_id"]) is not None:
            raise DuplicateKeyError(f"Duplicate _id: {doc['_id']!r}")
        self._storage.append(RECORD_LIVE, doc)
        self._index_manager.add(doc)
        self._total_records += 1
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
        docs = self._get_docs(where)
        if not docs:
            raise DocumentNotFoundError(f"No document matches: {where!r}")
        old_doc = docs[0]
        new_doc = _apply_update(old_doc, set, unset, inc)
        self._storage.append(RECORD_REPLACEMENT, new_doc)
        self._index_manager.remove(old_doc["_id"])
        self._index_manager.add(new_doc)
        self._total_records += 1
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
        docs = self._get_docs(where)
        count = 0
        for old_doc in docs:
            new_doc = _apply_update(old_doc, set, unset, inc)
            self._storage.append(RECORD_REPLACEMENT, new_doc)
            self._index_manager.remove(old_doc["_id"])
            self._index_manager.add(new_doc)
            self._total_records += 1
            count += 1
        return count

    def replace_one(self, where: dict, new_doc: dict) -> bool:
        """
        Replace the entire document matching *where* with *new_doc*.

        The original _id is preserved.
        Raises DocumentNotFoundError if no document matches.
        """
        self._require_write()
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
        docs = self._get_docs(where)
        if not docs:
            return False
        doc = docs[0]
        self._storage.append(RECORD_TOMBSTONE, {"_id": doc["_id"]})
        self._index_manager.remove(doc["_id"])
        self._total_records += 1
        return True

    def delete_many(self, where: dict) -> int:
        """
        Delete all documents matching *where*.

        Returns the count of deleted documents.
        """
        self._require_write()
        docs = self._get_docs(where)
        count = 0
        for doc in docs:
            self._storage.append(RECORD_TOMBSTONE, {"_id": doc["_id"]})
            self._index_manager.remove(doc["_id"])
            self._total_records += 1
            count += 1
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

    def reindex(self) -> None:
        """Rebuild all in-memory indexes by re-scanning the data file."""
        self._load_from_file()

    def close(self) -> None:
        """Close the collection and release the file handle."""
        if self._storage is not None:
            self._storage.close()
            self._storage = None

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

    def _load_from_file(self) -> None:
        """Scan the BSON file and build in-memory indexes from scratch."""
        self._index_manager.clear()
        self._total_records = 0

        if not os.path.exists(self._path):
            return

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
