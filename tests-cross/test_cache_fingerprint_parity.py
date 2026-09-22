"""
Cross-implementation cache-fingerprint parity.

The `.cache` sidecar is a disposable snapshot of the in-memory index, accepted
on open only if it still describes the data file.  "Still describes" has to
include the file's *identity*, not just its length and mtime: `compact()`
renames a freshly written file over the path, and rewriting the same live set
reproduces the same length — not by coincidence but by construction, since
that is exactly what compaction does when there is nothing dead to drop.

A cache that validates against a file it does not describe is served as truth,
so this is the one place where the cache stops being disposable.
"""

import os

import pytest


def _cache_path(path):
    return path + ".cache"


def _ident(path):
    st = os.stat(path)
    return (st.st_dev, st.st_ino)


class TestCacheIdentityIsPartOfTheFingerprint:
    def test_a_replaced_file_is_read_not_the_cached_index(
        self, make_collection, collection_impl, backend, tmp_path
    ):
        # Two collections whose files come out the same length: same _ids,
        # same field, values differing only in digits already present.
        db = make_collection("ident.bson", indexes=["v"])
        path = db._path
        db.insert_many([{"_id": str(i), "v": 10 + i} for i in range(5)])
        db.close()
        assert os.path.exists(_cache_path(path)), "close() should have written a cache"

        other = str(tmp_path / "replacement.bson")
        db2 = collection_impl(other, indexes=["v"])
        db2.insert_many([{"_id": str(i), "v": 90 + i} for i in range(5)])
        db2.close()
        os.remove(_cache_path(other))

        size_before = os.path.getsize(path)
        mtime_before = os.stat(path).st_mtime_ns
        ident_before = _ident(path)
        assert os.path.getsize(other) == size_before, (
            "the two files must be the same length for this test to mean anything"
        )

        # Replace by rename, the way compact() does, and put the mtime back —
        # so length and mtime both still match the cache's stamp and identity
        # is the only difference left.
        os.replace(other, path)
        os.utime(path, ns=(mtime_before, mtime_before))

        assert os.path.getsize(path) == size_before
        assert os.stat(path).st_mtime_ns == mtime_before
        assert _ident(path) != ident_before, "the rename must have changed the inode"

        db = collection_impl(path, indexes=["v"])
        assert db._loaded_from_cache is False, (
            "a cache stamped for a different inode must not be accepted"
        )
        # The observable consequence: serving the stale cache would return the
        # old file's values here.
        assert sorted(d["v"] for d in db.find({}).to_list()) == [90, 91, 92, 93, 94]
        db.close()

    def test_compact_invalidates_the_cache_it_supersedes(
        self, make_collection, collection_impl, backend
    ):
        # compact() with nothing dead to drop rewrites the same live set, so
        # the new file is the same length as the old one.
        db = make_collection("compactident.bson", indexes=["v"])
        path = db._path
        db.insert_many([{"_id": str(i), "v": i} for i in range(5)])
        db.compact()
        db.close()

        db = collection_impl(path, indexes=["v"])
        assert db.count() == 5
        assert [d["v"] for d in db.find({}).sort("v").to_list()] == [0, 1, 2, 3, 4]
        db.close()

    def test_cache_still_hits_when_nothing_changed(
        self, make_collection, collection_impl, backend
    ):
        # The identity check must not make the cache useless — the ordinary
        # open-close-open cycle still has to take the fast path.  A fingerprint
        # that never matches is invisible in results and costs every open a
        # full rescan, so this is the half of the contract worth guarding.
        db = make_collection("stillhits.bson", indexes=["v"])
        path = db._path
        db.insert_many([{"_id": str(i), "v": i} for i in range(5)])
        db.close()

        db = collection_impl(path, indexes=["v"])
        assert db._loaded_from_cache is True, "an unchanged file must still hit its cache"
        assert db.count() == 5
        db.close()
