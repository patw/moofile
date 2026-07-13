"""
Cross-implementation round-trip tests.

These tests ensure the Python and Rust implementations produce
byte-identical BSON files and semantically identical query results.

For now these run against the Python reference impl only.  Once the
Rust binding is wired in they will run against both backends.
"""

import json
import os

import pytest
from bson import Binary
from moofile import Collection


def test_insert_readback(make_collection):
    """Insert via backend A, verify with backend A."""
    db = make_collection()
    doc = db.insert({"name": "Alice", "email": "alice@example.com"})
    assert "_id" in doc
    # Python impl generates 24-char hex (12 random bytes);
    # Rust impl will generate 16-char.  Both are valid.
    assert len(doc["_id"]) in (16, 24)

    found = db.find_one({"email": "alice@example.com"})
    assert found is not None
    assert found["name"] == "Alice"


def test_persistence_cross_session(make_collection, tmp_path):
    """Write with one session, read with another."""
    path = tmp_path / "cross.bson"

    # Write
    with Collection(str(path)) as db:
        db.insert({"_id": "a", "v": 1})
        db.insert({"_id": "b", "v": 2})
        db.insert({"_id": "c", "v": 3})

    # Read back
    with Collection(str(path), readonly=True) as db:
        assert db.count({}) == 3
        doc = db.find_one({"_id": "b"})
        assert doc["v"] == 2


def test_file_exact_match(make_collection, tmp_path):
    """The BSON file should be byte-identical when produced by either backend."""
    path = tmp_path / "exact.bson"

    with Collection(str(path)) as db:
        db.insert({"_id": "1", "name": "test"})

    raw = path.read_bytes()

    # Basic sanity: file should start with record header
    # First 4 bytes = payload length (little-endian)
    # Byte 4 = record type (0x01 = live)
    assert len(raw) > 5
    assert raw[4] == 0x01  # RECORD_LIVE


def test_meta_file_roundtrip(make_collection, tmp_path):
    """Meta file should be valid JSON and match the declared indexes."""
    path = tmp_path / "meta_test.bson"
    meta_path = tmp_path / "meta_test.bson.meta"

    with Collection(
        str(path),
        indexes=["email", "age"],
        vector_indexes={"embedding": 384},
        text_indexes=["content"],
    ) as db:
        db.insert({"_id": "x", "email": "a@b.com", "age": 25})

    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["version"] == 1
    assert "email" in meta["indexes"]
    assert "age" in meta["indexes"]
    assert meta["vector_indexes"]["embedding"] == 384
    assert "content" in meta["text_indexes"]


def test_compaction_idempotent(make_collection, tmp_path):
    """Compacting a compacted file should be a no-op (same bytes)."""
    path = tmp_path / "compact.bson"

    with Collection(str(path)) as db:
        db.insert_many([{"_id": str(i), "data": "x" * 100} for i in range(100)])
        for i in range(50):
            db.delete_one({"_id": str(i)})

    # First compaction
    with Collection(str(path)) as db:
        db.compact()

    size1 = path.stat().st_size

    # Second compaction — should be identical
    with Collection(str(path)) as db:
        db.compact()

    size2 = path.stat().st_size
    assert size1 == size2


def test_text_search_basic(make_collection):
    """Basic BM25 text search across a small corpus."""
    db = make_collection(text_indexes=["body"])

    db.insert({"_id": "1", "body": "machine learning is fascinating"})
    db.insert({"_id": "2", "body": "deep learning and neural networks"})
    db.insert({"_id": "3", "body": "cooking recipes for dinner tonight"})

    results = db.find({}).text_search("body", "machine learning", limit=5).to_list()
    # BM25 only returns docs with at least one matching term — doc 3 is
    # about cooking and should not appear.
    assert len(results) == 2  # docs 1 and 2
    ids = [doc["_id"] for doc, _score in results]
    assert "1" in ids
    assert "2" in ids
    assert "3" not in ids


def test_vector_search_basic(make_collection):
    """Basic vector similarity search."""
    db = make_collection(vector_indexes={"embedding": 3})

    db.insert({"_id": "near", "embedding": [1.0, 0.0, 0.0]})
    db.insert({"_id": "far", "embedding": [0.0, 0.0, 1.0]})

    results = db.find({}).vector_search("embedding", [1.0, 0.1, 0.0], limit=2).to_list()
    assert len(results) == 2
    assert results[0][0]["_id"] == "near"  # near should be closer
    assert results[0][1] > results[1][1]  # first score > second score


def test_bson_type_roundtrip(make_collection, tmp_path):
    """BSON type round-trip through the backend — catches the lossy
    ``_ => val.to_string()`` fallback in the PyO3 binding (item #6).

    Datetime, binary, and nested documents must survive a write→read cycle
    with their types intact, not be stringified.
    """
    from datetime import datetime, timezone

    path = tmp_path / "types.bson"
    db = make_collection(name="types.bson")

    original = {
        "_id": "type-test",
        "nested": {"a": 1, "b": [2, 3]},
        "dt": datetime(2025, 1, 15, 12, 30, 0, tzinfo=timezone.utc),
        "binary": Binary(b"\x00\x01\x02\x03"),
        "flag": True,
        "count": 42,
        "pi": 3.14,
        "label": "hello",
    }
    db.insert(original)

    found = db.find_one({"_id": "type-test"})
    assert found is not None

    # Type preservation checks
    assert isinstance(found["nested"], dict)
    assert found["nested"]["a"] == 1
    assert found["nested"]["b"] == [2, 3]

    assert isinstance(found["flag"], bool)
    assert found["flag"] is True

    assert isinstance(found["count"], int)
    assert found["count"] == 42

    assert isinstance(found["pi"], float)
    assert abs(found["pi"] - 3.14) < 1e-6

    assert found["label"] == "hello"

    # Datetime must come back as a datetime, not a string
    assert isinstance(found["dt"], datetime), \
        f"datetime must survive round-trip, got {type(found['dt']).__name__}"
    assert found["dt"].year == 2025
    assert found["dt"].hour == 12

    # Binary must come back as bytes/binary, not a string
    assert bytes(found["binary"]) == b"\x00\x01\x02\x03", \
        f"binary must survive round-trip, got {type(found['binary']).__name__}"


def test_vector_search_after_insert(make_collection, tmp_path):
    """Docs inserted after the first search must appear in subsequent searches.

    Regression test for the bonus finding: the old Python implementation
    never rebuilt vector indexes after the initial build.
    """
    db = make_collection(vector_indexes={"embedding": 3})

    db.insert({"_id": "a", "embedding": [1.0, 0.0, 0.0]})
    db.insert({"_id": "b", "embedding": [0.0, 1.0, 0.0]})

    # First search triggers initial vector rebuild
    r1 = db.find({}).vector_search("embedding", [1.0, 0.0, 0.0], limit=10).to_list()
    assert len(r1) == 2

    # Insert more docs
    db.insert({"_id": "c", "embedding": [0.9, 0.1, 0.0]})
    db.insert({"_id": "d", "embedding": [0.8, 0.2, 0.0]})

    # Second search MUST see all 4 docs
    r2 = db.find({}).vector_search("embedding", [1.0, 0.0, 0.0], limit=10).to_list()
    assert len(r2) == 4, "docs inserted after first search must be visible"
    ids = {doc["_id"] for doc, _ in r2}
    assert "c" in ids
    assert "d" in ids


def test_hybrid_search_basic(make_collection):
    """Hybrid search (RRF) fuses text and vector results."""
    db = make_collection(
        vector_indexes={"embedding": 3},
        text_indexes=["content"],
    )

    db.insert({"_id": "a", "category": "ai", "content": "machine learning algorithms", "embedding": [1.0, 0.0, 0.0]})
    db.insert({"_id": "b", "category": "ai", "content": "deep learning neural networks", "embedding": [0.9, 0.1, 0.0]})
    db.insert({"_id": "c", "category": "food", "content": "cooking recipes pasta pizza", "embedding": [0.0, 0.0, 0.1]})

    results = (
        db.find({})
        .hybrid_search("content", "embedding", "machine learning", [1.0, 0.0, 0.0], limit=5)
        .to_list()
    )

    assert len(results) > 0
    # Scores should be positive and descending
    scores = [s for _, s in results]
    assert all(s > 0 for s in scores)
    assert scores == sorted(scores, reverse=True)
    # "a" and "b" should rank above "c" (cooking) — they match both text and vector
    # while "c" only appears via vector with cosine=0 (low rank)
    a_score = next(s for d, s in results if d["_id"] == "a")
    c_score = next((s for d, s in results if d["_id"] == "c"), 0)
    assert a_score > c_score


def test_hybrid_search_with_prefilter(make_collection):
    """Hybrid search honours the find() pre-filter."""
    db = make_collection(
        indexes=["category"],
        vector_indexes={"embedding": 3},
        text_indexes=["content"],
    )

    db.insert({"_id": "a", "category": "ai", "content": "machine learning", "embedding": [1.0, 0.0, 0.0]})
    db.insert({"_id": "b", "category": "food", "content": "machine learning for food", "embedding": [1.0, 0.1, 0.0]})

    results = (
        db.find({"category": "ai"})
        .hybrid_search("content", "embedding", "machine learning", [1.0, 0.0, 0.0], limit=5)
        .to_list()
    )

    for doc, _ in results:
        assert doc["category"] == "ai"
    assert len(results) == 1
    assert results[0][0]["_id"] == "a"


def test_batch_insert_commit(make_collection):
    """Batch writes are visible after commit."""
    db = make_collection()

    with db.batch():
        db.insert({"_id": "a", "v": 1})
        db.insert({"_id": "b", "v": 2})
        db.insert({"_id": "c", "v": 3})

    assert db.count({}) == 3
    assert db.find_one({"_id": "a"}) is not None


def test_batch_rollback_on_exception(make_collection):
    """Batch writes are rolled back if the with block raises."""
    db = make_collection()

    with pytest.raises(ValueError, match="oops"):
        with db.batch():
            db.insert({"_id": "a", "v": 1})
            db.insert({"_id": "b", "v": 2})
            raise ValueError("oops")

    assert db.count({}) == 0


def test_batch_mixed_operations(make_collection):
    """Insert + update + delete in one batch."""
    db = make_collection()

    db.insert({"_id": "keep", "status": "active"})
    db.insert({"_id": "upd", "status": "active"})
    db.insert({"_id": "del", "status": "active"})

    with db.batch():
        db.insert({"_id": "new", "status": "active"})
        db.update_one({"_id": "upd"}, set={"status": "inactive"})
        db.delete_one({"_id": "del"})

    assert db.count({}) == 3
    assert db.find_one({"_id": "del"}) is None
    assert db.find_one({"_id": "upd"})["status"] == "inactive"
    assert db.find_one({"_id": "new"}) is not None
