"""Tests for vector similarity search functionality."""

import numpy as np
import pytest
from moofile import Collection
import tempfile
import os


@pytest.fixture
def temp_collection():
    """Create a temporary collection with vector indexes."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    # Create collection with vector index
    db = Collection(
        path, 
        vector_indexes={"embedding": 3}  # 3-dimensional vectors
    )
    
    yield db
    
    db.close()
    # Clean up files
    if os.path.exists(path):
        os.unlink(path)
    if os.path.exists(path + ".meta"):
        os.unlink(path + ".meta")


def test_vector_index_creation(temp_collection):
    """Test that vector indexes are created properly."""
    db = temp_collection
    
    # Insert documents with embeddings
    db.insert({
        "name": "doc1",
        "embedding": [1.0, 0.0, 0.0]
    })
    
    db.insert({
        "name": "doc2", 
        "embedding": [0.0, 1.0, 0.0]
    })
    
    db.insert({
        "name": "doc3",
        "embedding": [0.0, 0.0, 1.0]
    })
    
    # Verify documents are inserted
    assert db.count({}) == 3
    
    # Check that vector index exists
    assert "embedding" in db._index_manager._vector_fields
    assert db._index_manager._vector_fields["embedding"] == 3


def test_vector_search_cosine_similarity(temp_collection):
    """Test vector search with cosine similarity."""
    db = temp_collection
    
    # Insert test documents
    docs = [
        {"name": "doc1", "embedding": [1.0, 0.0, 0.0]},
        {"name": "doc2", "embedding": [0.0, 1.0, 0.0]},
        {"name": "doc3", "embedding": [0.0, 0.0, 1.0]},
        {"name": "doc4", "embedding": [0.7, 0.7, 0.0]},  # Similar to doc1 and doc2
    ]
    
    for doc in docs:
        db.insert(doc)
    
    # Search for documents similar to [1, 0, 0]
    query_vector = [1.0, 0.0, 0.0]
    results = db.find({}).vector_search("embedding", query_vector, limit=3).to_list()
    
    # Should return results sorted by similarity
    assert len(results) == 3
    
    # First result should be exact match (doc1)
    assert results[0][0]["name"] == "doc1"
    assert abs(results[0][1] - 1.0) < 1e-6  # Perfect similarity
    
    # Second result should be doc4 (has some similarity)
    assert results[1][0]["name"] == "doc4"
    assert results[1][1] > 0  # Positive similarity


def test_vector_search_with_filter(temp_collection):
    """Test vector search combined with pre-filtering."""
    db = temp_collection
    
    # Insert test documents with categories
    docs = [
        {"name": "doc1", "category": "A", "embedding": [1.0, 0.0, 0.0]},
        {"name": "doc2", "category": "B", "embedding": [1.0, 0.1, 0.0]},  # Similar to doc1
        {"name": "doc3", "category": "A", "embedding": [0.0, 1.0, 0.0]},
        {"name": "doc4", "category": "B", "embedding": [0.0, 0.0, 1.0]},
    ]
    
    for doc in docs:
        db.insert(doc)
    
    # Search for documents similar to [1, 0, 0] but only in category A
    query_vector = [1.0, 0.0, 0.0]
    results = db.find({"category": "A"}).vector_search("embedding", query_vector).to_list()
    
    # Should only return category A documents
    for doc, score in results:
        assert doc["category"] == "A"
    
    # First result should be doc1 (exact match)
    assert results[0][0]["name"] == "doc1"


def test_vector_search_invalid_vectors(temp_collection):
    """Test handling of invalid vector data."""
    db = temp_collection
    
    # Insert documents with various invalid embeddings
    db.insert({"name": "valid", "embedding": [1.0, 0.0, 0.0]})
    db.insert({"name": "wrong_dim", "embedding": [1.0, 0.0]})  # Wrong dimension
    db.insert({"name": "not_numeric", "embedding": ["a", "b", "c"]})  # Not numeric
    db.insert({"name": "missing_embedding"})  # No embedding field
    
    # Search should only find valid vectors
    query_vector = [1.0, 0.0, 0.0]
    results = db.find({}).vector_search("embedding", query_vector).to_list()
    
    # Should only return the valid document
    assert len(results) == 1
    assert results[0][0]["name"] == "valid"


def test_vector_search_empty_collection():
    """Test vector search on empty collection."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    db = Collection(path, vector_indexes={"embedding": 3})
    
    try:
        # Search empty collection
        results = db.find({}).vector_search("embedding", [1.0, 0.0, 0.0]).to_list()
        assert results == []
        
        # Test first() method
        result = db.find({}).vector_search("embedding", [1.0, 0.0, 0.0]).first()
        assert result is None
        
    finally:
        db.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")


def test_vector_meta_persistence():
    """Test that vector index configuration persists in .meta file."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    # Create collection with vector indexes
    db1 = Collection(path, vector_indexes={"embedding": 128, "image_vec": 512})
    db1.insert({"test": "data", "embedding": np.random.random(128).tolist()})
    db1.close()
    
    # Reopen without specifying vector indexes
    db2 = Collection(path)
    
    try:
        # Should remember vector index configuration
        assert "embedding" in db2._index_manager._vector_fields
        assert "image_vec" in db2._index_manager._vector_fields
        assert db2._index_manager._vector_fields["embedding"] == 128
        assert db2._index_manager._vector_fields["image_vec"] == 512
        
    finally:
        db2.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")


def test_vector_search_limit():
    """Test limit parameter in vector search."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    db = Collection(path, vector_indexes={"embedding": 2})
    
    try:
        # Insert multiple documents
        for i in range(10):
            db.insert({
                "name": f"doc{i}",
                "embedding": [i / 10.0, (10 - i) / 10.0]
            })
        
        # Search with limit
        results = db.find({}).vector_search("embedding", [1.0, 0.0], limit=3).to_list()
        assert len(results) <= 3
        
        # Search without limit should return more
        all_results = db.find({}).vector_search("embedding", [1.0, 0.0], limit=None).to_list()
        assert len(all_results) > len(results)
        
    finally:
        db.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")


# ---------------------------------------------------------------------------
# Regression tests for audit items #1–#4 and bonus finding
# ---------------------------------------------------------------------------

def test_vector_search_after_insert():
    """Docs inserted after the first search must be visible in subsequent searches.
    
    This is the bonus-finding regression test: the old Python implementation
    never rebuilt vector indexes after the initial build, so new docs were
    silently excluded from vector search results.
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    db = Collection(path, vector_indexes={"embedding": 3})
    
    try:
        db.insert({"_id": "a", "embedding": [1.0, 0.0, 0.0]})
        db.insert({"_id": "b", "embedding": [0.0, 1.0, 0.0]})

        # First search — triggers initial vector rebuild
        r1 = db.find({}).vector_search("embedding", [1.0, 0.0, 0.0], limit=10).to_list()
        assert len(r1) == 2

        # Insert more docs
        db.insert({"_id": "c", "embedding": [0.9, 0.1, 0.0]})
        db.insert({"_id": "d", "embedding": [0.8, 0.2, 0.0]})

        # Second search — MUST see all 4 docs
        r2 = db.find({}).vector_search("embedding", [1.0, 0.0, 0.0], limit=10).to_list()
        assert len(r2) == 4, "docs inserted after first search must be visible"
        ids = {doc["_id"] for doc, _ in r2}
        assert "c" in ids
        assert "d" in ids

    finally:
        db.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")


def test_vector_search_cosine_magnitude_invariant():
    """Cosine similarity must be magnitude-invariant (item #1: normalise at build)."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    db = Collection(path, vector_indexes={"embedding": 2})
    
    try:
        # Same direction, very different magnitudes
        db.insert({"_id": "big", "embedding": [10.0, 0.0]})
        db.insert({"_id": "orth", "embedding": [0.0, 10.0]})

        results = db.find({}).vector_search("embedding", [1.0, 0.0], limit=10).to_list()
        
        assert results[0][0]["_id"] == "big"
        assert abs(results[0][1] - 1.0) < 1e-5, "cosine=1.0 regardless of magnitude"
        assert abs(results[1][1] - 0.0) < 1e-5, "orthogonal → cosine=0.0"

    finally:
        db.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")


def test_vector_search_filtered_correctness():
    """Filtered vector search must only return allowed docs with correct scores (item #4)."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    db = Collection(path, vector_indexes={"embedding": 3}, indexes=["category"])
    
    try:
        db.insert({"_id": "a", "category": "x", "embedding": [1.0, 0.0, 0.0]})
        db.insert({"_id": "b", "category": "y", "embedding": [0.95, 0.05, 0.0]})
        db.insert({"_id": "c", "category": "x", "embedding": [0.5, 0.5, 0.0]})
        db.insert({"_id": "d", "category": "y", "embedding": [0.0, 0.0, 1.0]})

        # Filter to category "x" only — should return a and c, not b or d
        results = db.find({"category": "x"}).vector_search(
            "embedding", [1.0, 0.0, 0.0], limit=10
        ).to_list()
        
        assert len(results) == 2
        ids = {doc["_id"] for doc, _ in results}
        assert ids == {"a", "c"}, "filtered search must only return matching docs"
        
        # a should be closer to [1,0,0] than c
        assert results[0][0]["_id"] == "a"
        assert results[0][1] > results[1][1]

    finally:
        db.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")


def test_vector_search_topk_correctness():
    """Top-k must return the correct top-k elements in descending order (item #2)."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    db = Collection(path, vector_indexes={"embedding": 2})
    
    try:
        for i in range(100):
            f_val = i / 100.0
            db.insert({"_id": str(i), "embedding": [f_val, 1.0 - f_val]})
        
        results = db.find({}).vector_search("embedding", [1.0, 0.0], limit=5).to_list()
        
        assert len(results) == 5
        # Scores in descending order
        for i in range(4):
            assert results[i][1] >= results[i + 1][1], "scores must be descending"
        # Doc 99 has highest first component
        assert results[0][0]["_id"] == "99"

    finally:
        db.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")