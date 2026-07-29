"""Multi-handle (SQLite-style) safety: one writer, many readers, no session lock.

Since v0.5.2 no lock is held for the lifetime of a handle, so several
processes can have the same file open.  That invalidated the assumption that a
handle's in-memory index describes the whole file.  Two things broke:

  * the index cache was stamped with the file's *current* length while
    describing only this handle's view, so it validated on the next open and
    permanently hid the other writer's records;
  * compact() rewrote the file from a stale in-memory snapshot, destroying the
    other writer's records outright.

Separate handles on the same path exercise exactly the same reconcile paths as
separate processes, without the flakiness of spawning them.
"""

import os

import pytest


def _ids(db, flt=None):
    return sorted(d["_id"] for d in db.find(flt or {}).to_list())


class TestVisibility:
    def test_reader_observes_writer_appends(self, make_collection):
        writer = make_collection("shared.bson")
        reader = make_collection("shared.bson")

        writer.insert({"_id": "a"})
        assert _ids(reader) == ["a"], "reader must see the writer's append"

        writer.insert({"_id": "b"})
        assert _ids(reader) == ["a", "b"]
        assert reader.count({}) == 2

        writer.close()
        reader.close()

    def test_writes_from_both_handles_converge(self, make_collection):
        a = make_collection("shared.bson")
        b = make_collection("shared.bson")

        a.insert({"_id": "from_a"})
        b.insert({"_id": "from_b"})

        assert _ids(a) == ["from_a", "from_b"]
        assert _ids(b) == ["from_a", "from_b"]
        a.close()
        b.close()

    def test_updates_and_deletes_propagate(self, make_collection):
        a = make_collection("shared.bson", indexes=["status"])
        b = make_collection("shared.bson", indexes=["status"])

        a.insert({"_id": "x", "status": "new"})
        assert b.find_one({"_id": "x"})["status"] == "new"

        a.update_one({"_id": "x"}, set={"status": "done"})
        assert b.find_one({"_id": "x"})["status"] == "done"
        assert b.count({"status": "new"}) == 0

        a.delete_one({"_id": "x"})
        assert b.find_one({"_id": "x"}) is None
        a.close()
        b.close()

    def test_duplicate_id_detected_across_handles(self, make_collection):
        from moofile.errors import DuplicateKeyError

        a = make_collection("shared.bson")
        b = make_collection("shared.bson")
        a.insert({"_id": "same"})
        with pytest.raises(DuplicateKeyError):
            b.insert({"_id": "same"})
        a.close()
        b.close()


class TestCacheSafety:
    def test_cache_does_not_hide_concurrent_writes(self, make_collection, tmp_path):
        """The last handle to close used to stamp its cache with the file's
        current size while describing only its own writes."""
        a = make_collection("shared.bson")
        b = make_collection("shared.bson")
        a.insert({"_id": "from_a"})
        b.insert({"_id": "from_b"})
        b.close()
        a.close()  # a closes last, writing the cache

        path = str(tmp_path / "shared.bson")
        reopened = make_collection("shared.bson")
        via_cache = _ids(reopened)
        reopened.close()

        if os.path.exists(path + ".cache"):
            os.remove(path + ".cache")
        rescanned = make_collection("shared.bson")
        via_scan = _ids(rescanned)
        rescanned.close()

        assert via_cache == via_scan == ["from_a", "from_b"]


class TestCompactSafety:
    def test_compact_preserves_other_handles_writes(self, make_collection, tmp_path):
        """compact() rewrites the file wholesale; it must reconcile first or it
        permanently destroys records it never saw."""
        a = make_collection("shared.bson")
        b = make_collection("shared.bson")
        a.insert({"_id": "from_a"})
        b.insert({"_id": "from_b"})
        b.close()

        a.compact()
        a.close()

        path = str(tmp_path / "shared.bson")
        for suffix in (".cache", ".meta"):
            if os.path.exists(path + suffix):
                os.remove(path + suffix)
        survivors = make_collection("shared.bson")
        assert _ids(survivors) == ["from_a", "from_b"]
        survivors.close()

    def test_handle_recovers_after_another_compacts(self, make_collection):
        """Compaction shrinks the file, so a stale handle must full-reload
        rather than try to replay a 'suffix' that no longer lines up."""
        a = make_collection("shared.bson")
        b = make_collection("shared.bson")
        for i in range(5):
            a.insert({"_id": str(i)})
        a.delete_many({"_id": {"$in": ["1", "3"]}})
        assert _ids(b) == ["0", "2", "4"]

        a.compact()

        assert _ids(b) == ["0", "2", "4"], "b must recover after the rewrite"
        b.insert({"_id": "9"})
        assert _ids(a) == ["0", "2", "4", "9"]
        a.close()
        b.close()


class TestBulkPathsUnderSharing:
    def test_insert_many_visible_to_other_handle(self, make_collection):
        a = make_collection("shared.bson")
        b = make_collection("shared.bson")
        a.insert_many([{"_id": str(i)} for i in range(20)])
        assert len(_ids(b)) == 20
        a.close()
        b.close()

    def test_batch_commit_visible_to_other_handle(self, make_collection):
        a = make_collection("shared.bson")
        b = make_collection("shared.bson")
        with a.batch():
            for i in range(10):
                a.insert({"_id": f"b{i}"})
        assert len(_ids(b)) == 10
        a.close()
        b.close()

    def test_interleaved_writes_from_both_handles(self, make_collection):
        a = make_collection("shared.bson")
        b = make_collection("shared.bson")
        for i in range(15):
            (a if i % 2 == 0 else b).insert({"_id": f"{i:02d}"})
        expected = [f"{i:02d}" for i in range(15)]
        assert _ids(a) == expected
        assert _ids(b) == expected
        a.close()
        b.close()
