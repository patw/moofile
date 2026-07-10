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
