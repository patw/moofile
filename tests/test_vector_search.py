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