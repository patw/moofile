"""Tests for atomic batch writes."""

import pytest
from moofile import Collection, DuplicateKeyError, DocumentNotFoundError

import tempfile
import os


@pytest.fixture
def temp_collection():
    """Create a temporary collection for batch testing."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name

    db = Collection(path, indexes=["status", "category"])

    yield db

    db.close()
    for suffix in ["", ".meta", ".cache", ".lock"]:
        p = path + suffix
        if os.path.exists(p):
            os.unlink(p)


# ---------------------------------------------------------------------------
# Basic commit / rollback
# ---------------------------------------------------------------------------

def test_batch_insert_commit(temp_collection):
    """Inserts within a batch are visible after commit."""
    db = temp_collection

    with db.batch() as b:
        db.insert({"_id": "a", "status": "active"})
        db.insert({"_id": "b", "status": "active"})
        db.insert({"_id": "c", "status": "inactive"})

    assert db.count({}) == 3
    assert db.find_one({"_id": "a"}) is not None
    assert db.find_one({"_id": "b"}) is not None
    assert db.find_one({"_id": "c"}) is not None


def test_batch_rollback_on_exception(temp_collection):
    """If the with block raises, nothing is committed."""
    db = temp_collection

    with pytest.raises(ValueError, match="oops"):
        with db.batch():
            db.insert({"_id": "a", "status": "active"})
            db.insert({"_id": "b", "status": "active"})
            raise ValueError("oops")

    assert db.count({}) == 0
    assert db.find_one({"_id": "a"}) is None
    assert db.find_one({"_id": "b"}) is None


def test_batch_empty_commit(temp_collection):
    """An empty batch commits cleanly as a no-op."""
    db = temp_collection

    with db.batch():
        pass

    assert db.count({}) == 0


# ---------------------------------------------------------------------------
# Mixed operations
# ---------------------------------------------------------------------------

def test_batch_mixed_operations(temp_collection):
    """Insert + update + delete in one batch commit atomically."""
    db = temp_collection

    # Seed initial data
    db.insert({"_id": "keep", "status": "active", "category": "x"})
    db.insert({"_id": "update_me", "status": "active", "category": "x"})
    db.insert({"_id": "delete_me", "status": "active", "category": "x"})

    with db.batch():
        db.insert({"_id": "new", "status": "active", "category": "y"})
        db.update_one({"_id": "update_me"}, set={"status": "inactive"})
        db.delete_one({"_id": "delete_me"})

    assert db.count({}) == 3  # keep + update_me + new
    assert db.find_one({"_id": "keep"}) is not None
    assert db.find_one({"_id": "new"}) is not None
    assert db.find_one({"_id": "delete_me"}) is None

    updated = db.find_one({"_id": "update_me"})
    assert updated["status"] == "inactive"


def test_batch_mixed_rollback(temp_collection):
    """Mixed operations are all rolled back on exception."""
    db = temp_collection

    db.insert({"_id": "keep", "status": "active"})
    db.insert({"_id": "delete_me", "status": "active"})

    original_count = db.count({})

    with pytest.raises(RuntimeError):
        with db.batch():
            db.insert({"_id": "new", "status": "active"})
            db.update_one({"_id": "keep"}, set={"status": "inactive"})
            db.delete_one({"_id": "delete_me"})
            raise RuntimeError("fail")

    # Everything should be as it was before the batch
    assert db.count({}) == original_count
    assert db.find_one({"_id": "keep"})["status"] == "active"
    assert db.find_one({"_id": "delete_me"}) is not None
    assert db.find_one({"_id": "new"}) is None


# ---------------------------------------------------------------------------
# Validation within batch
# ---------------------------------------------------------------------------

def test_batch_duplicate_id_detected(temp_collection):
    """Duplicate _id within a batch raises immediately."""
    db = temp_collection

    with pytest.raises(DuplicateKeyError):
        with db.batch():
            db.insert({"_id": "dup", "status": "active"})
            db.insert({"_id": "dup", "status": "inactive"})  # boom

    # Rollback: nothing committed
    assert db.count({}) == 0


def test_batch_duplicate_id_with_existing(temp_collection):
    """Duplicate _id against a pre-existing doc raises immediately."""
    db = temp_collection
    db.insert({"_id": "exists", "status": "active"})

    with pytest.raises(DuplicateKeyError):
        with db.batch():
            db.insert({"_id": "exists", "status": "inactive"})

    # Original doc untouched
    assert db.find_one({"_id": "exists"})["status"] == "active"


def test_batch_update_not_found_raises(temp_collection):
    """update_one on a non-existent doc raises within a batch."""
    db = temp_collection

    with pytest.raises(DocumentNotFoundError):
        with db.batch():
            db.update_one({"_id": "nope"}, set={"status": "active"})

    assert db.count({}) == 0


def test_batch_delete_not_found_returns_false(temp_collection):
    """delete_one on a non-existent doc returns False within a batch."""
    db = temp_collection

    with db.batch():
        result = db.delete_one({"_id": "nope"})
        assert result is False

    assert db.count({}) == 0


# ---------------------------------------------------------------------------
# Interleaved insert / update within a batch
# ---------------------------------------------------------------------------

def test_batch_update_pre_existing_doc(temp_collection):
    """Update on a doc that existed before the batch works correctly."""
    db = temp_collection
    db.insert({"_id": "pre", "status": "active", "category": "x"})

    with db.batch():
        db.update_one({"_id": "pre"}, set={"status": "inactive"})
        db.insert({"_id": "new", "status": "active", "category": "y"})

    assert db.find_one({"_id": "pre"})["status"] == "inactive"
    assert db.find_one({"_id": "new"}) is not None


def test_batch_update_many(temp_collection):
    """update_many within a batch updates all matching docs."""
    db = temp_collection
    db.insert_many([
        {"_id": "a", "status": "trial", "category": "x"},
        {"_id": "b", "status": "trial", "category": "x"},
        {"_id": "c", "status": "active", "category": "x"},
    ])

    with db.batch():
        count = db.update_many({"status": "trial"}, set={"status": "expired"})
        assert count == 2

    assert db.count({"status": "expired"}) == 2
    assert db.count({"status": "trial"}) == 0
    assert db.count({"status": "active"}) == 1


def test_batch_delete_many(temp_collection):
    """delete_many within a batch deletes all matching docs."""
    db = temp_collection
    db.insert_many([
        {"_id": "a", "status": "temp", "category": "x"},
        {"_id": "b", "status": "temp", "category": "x"},
        {"_id": "c", "status": "perm", "category": "x"},
    ])

    with db.batch():
        count = db.delete_many({"status": "temp"})
        assert count == 2

    assert db.count({}) == 1
    assert db.find_one({"_id": "c"}) is not None


def test_batch_replace_one(temp_collection):
    """replace_one within a batch replaces the document."""
    db = temp_collection
    db.insert({"_id": "rep", "status": "active", "category": "x", "old": True})

    with db.batch():
        db.replace_one({"_id": "rep"}, {"status": "inactive", "new": True})

    doc = db.find_one({"_id": "rep"})
    assert doc["status"] == "inactive"
    assert doc["new"] is True
    assert "old" not in doc


def test_batch_insert_many(temp_collection):
    """insert_many within a batch buffers all docs."""
    db = temp_collection

    with db.batch():
        db.insert_many([
            {"_id": f"doc{i}", "status": "active"} for i in range(100)
        ])

    assert db.count({}) == 100


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_batch_persistence_across_reopen(temp_collection, tmp_path):
    """Batched writes survive a close/reopen cycle."""
    path = str(tmp_path / "batch_persist.bson")

    with Collection(path, indexes=["status"]) as db:
        with db.batch():
            db.insert({"_id": "a", "status": "active"})
            db.insert({"_id": "b", "status": "active"})
            db.insert({"_id": "c", "status": "inactive"})

    with Collection(path, indexes=["status"]) as db:
        assert db.count({}) == 3
        assert db.find_one({"_id": "a"})["status"] == "active"

    # Clean up
    for suffix in ["", ".meta", ".cache", ".lock"]:
        p = path + suffix
        if os.path.exists(p):
            os.unlink(p)


def test_batch_rollback_persistence(temp_collection, tmp_path):
    """A rolled-back batch leaves no trace on disk."""
    path = str(tmp_path / "batch_rollback.bson")

    with Collection(path, indexes=["status"]) as db:
        db.insert({"_id": "pre", "status": "active"})

    with Collection(path, indexes=["status"]) as db:
        try:
            with db.batch():
                db.insert({"_id": "rolled", "status": "active"})
                raise RuntimeError("rollback!")
        except RuntimeError:
            pass

    with Collection(path, indexes=["status"]) as db:
        assert db.count({}) == 1
        assert db.find_one({"_id": "pre"}) is not None
        assert db.find_one({"_id": "rolled"}) is None

    for suffix in ["", ".meta", ".cache", ".lock"]:
        p = path + suffix
        if os.path.exists(p):
            os.unlink(p)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_batch_read_only_raises():
    """batch() on a read-only collection raises."""
    import tempfile
    path = tempfile.mktemp(suffix=".bson")
    try:
        with Collection(path) as db:
            db.insert({"_id": "a", "status": "active"})

        with Collection(path, readonly=True) as ro_db:
            from moofile import ReadOnlyError
            with pytest.raises(ReadOnlyError):
                with ro_db.batch():
                    pass
    finally:
        for suffix in ["", ".meta", ".cache", ".lock"]:
            p = path + suffix
            if os.path.exists(p):
                os.unlink(p)


def test_batch_stats_during_batch(temp_collection):
    """stats() during a batch reflects pre-batch state."""
    db = temp_collection
    db.insert({"_id": "pre", "status": "active"})

    pre_stats = db.stats()

    with db.batch() as b:
        db.insert({"_id": "a", "status": "active"})
        db.insert({"_id": "b", "status": "active"})
        # During the batch, stats should still show pre-batch state
        during_stats = db.stats()
        assert during_stats["documents"] == pre_stats["documents"]

    # After commit, stats should reflect the new state
    post_stats = db.stats()
    assert post_stats["documents"] == pre_stats["documents"] + 2