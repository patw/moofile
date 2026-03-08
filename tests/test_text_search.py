"""Tests for BM25 text search functionality."""

import pytest
from moofile import Collection
import tempfile
import os


@pytest.fixture
def temp_collection():
    """Create a temporary collection with text indexes."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    # Create collection with text index
    db = Collection(path, text_indexes=["title", "content"])
    
    yield db
    
    db.close()
    # Clean up files
    if os.path.exists(path):
        os.unlink(path)
    if os.path.exists(path + ".meta"):
        os.unlink(path + ".meta")


def test_text_index_creation(temp_collection):
    """Test that text indexes are created properly."""
    db = temp_collection
    
    # Insert documents with text fields
    db.insert({
        "title": "Machine Learning Basics",
        "content": "Introduction to machine learning algorithms and techniques."
    })
    
    db.insert({
        "title": "Deep Learning Guide", 
        "content": "Advanced neural networks and deep learning methods."
    })
    
    # Verify documents are inserted
    assert db.count({}) == 2
    
    # Check that text indexes exist
    assert "title" in db._index_manager._text_fields
    assert "content" in db._index_manager._text_fields


def test_text_search_basic(temp_collection):
    """Test basic text search functionality."""
    db = temp_collection
    
    # Insert test documents
    docs = [
        {
            "title": "Machine Learning Introduction",
            "content": "Learn about supervised and unsupervised machine learning algorithms."
        },
        {
            "title": "Deep Learning Neural Networks", 
            "content": "Advanced techniques in deep learning and neural network architectures."
        },
        {
            "title": "Data Science Overview",
            "content": "Introduction to data science, statistics, and machine learning applications."
        },
        {
            "title": "Computer Vision",
            "content": "Image processing and computer vision with deep learning methods."
        }
    ]
    
    for doc in docs:
        db.insert(doc)
    
    # Search for "machine learning"
    results = db.find({}).text_search("content", "machine learning", limit=3).to_list()
    
    # Should return documents containing these terms
    assert len(results) > 0
    
    # Results should be sorted by relevance (BM25 score)
    scores = [score for doc, score in results]
    assert scores == sorted(scores, reverse=True)  # Descending order
    
    # Documents containing "machine learning" should be found
    # Note: BM25 scores can be negative for very common terms
    for doc, score in results:
        content = doc["content"].lower()
        assert "machine" in content or "learning" in content


def test_text_search_stemming(temp_collection):
    """Test that stemming works correctly."""
    db = temp_collection
    
    # Insert documents with different word forms
    docs = [
        {"title": "Running", "content": "I love running in the park."},
        {"title": "Runner", "content": "The runner finished the race quickly."},
        {"title": "Runs", "content": "She runs every morning for fitness."},
        {"title": "Cooking", "content": "I enjoy cooking Italian food."},
    ]
    
    for doc in docs:
        db.insert(doc)
    
    # Search for "run" - should match running, runs (but not runner due to stemming rules)
    results = db.find({}).text_search("content", "run").to_list()
    
    # Should find documents with stemmed forms of "run"
    found_titles = {doc["title"] for doc, score in results}
    assert "Running" in found_titles  # "running" stems to "run"
    assert "Runs" in found_titles     # "runs" stems to "run"
    # Note: "runner" stems to "runner", not "run", so it won't match
    assert "Cooking" not in found_titles


def test_text_search_with_filter(temp_collection):
    """Test text search combined with pre-filtering."""
    db = temp_collection
    
    # Insert documents with categories
    docs = [
        {
            "title": "Machine Learning", 
            "content": "Introduction to machine learning",
            "category": "AI"
        },
        {
            "title": "Machine Parts",
            "content": "Industrial machine components and parts", 
            "category": "Engineering"
        },
        {
            "title": "Learning Psychology",
            "content": "How humans learn and process information",
            "category": "Psychology"  
        },
        {
            "title": "AI Ethics",
            "content": "Machine learning ethics and responsible AI",
            "category": "AI"
        }
    ]
    
    for doc in docs:
        db.insert(doc)
    
    # Search for "machine" but only in AI category
    results = db.find({"category": "AI"}).text_search("content", "machine").to_list()
    
    # Should only return AI category documents
    for doc, score in results:
        assert doc["category"] == "AI"
    
    # Should find both AI documents that mention "machine"
    titles = {doc["title"] for doc, score in results}
    assert "Machine Learning" in titles
    assert "AI Ethics" in titles


def test_text_search_multiple_terms(temp_collection):
    """Test text search with multiple query terms."""
    db = temp_collection
    
    # Insert documents
    docs = [
        {
            "title": "Python Programming",
            "content": "Learn Python programming language fundamentals and advanced concepts."
        },
        {
            "title": "Java Development", 
            "content": "Java programming language for enterprise development applications."
        },
        {
            "title": "Programming Languages",
            "content": "Comparison of different programming languages like Python, Java, and C++."
        },
        {
            "title": "Web Development",
            "content": "Frontend and backend web development using modern frameworks."
        }
    ]
    
    for doc in docs:
        db.insert(doc)
    
    # Search for "Python programming"
    results = db.find({}).text_search("content", "Python programming").to_list()
    
    # Document with both terms should score highest
    assert len(results) > 0
    
    # First result should be most relevant
    best_doc = results[0][0]
    assert "Python" in best_doc["content"]
    assert "programming" in best_doc["content"]


def test_text_search_empty_query():
    """Test text search with empty or invalid queries."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    db = Collection(path, text_indexes=["content"])
    
    try:
        # Insert a document
        db.insert({"content": "Some test content here."})
        
        # Empty query
        results = db.find({}).text_search("content", "").to_list()
        assert results == []
        
        # Query with only short words (should be filtered out)
        results = db.find({}).text_search("content", "a to be").to_list()
        assert results == []
        
        # Query with only punctuation
        results = db.find({}).text_search("content", "!!! ??? ...").to_list()
        assert results == []
        
    finally:
        db.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")


def test_text_search_limit():
    """Test limit parameter in text search."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    db = Collection(path, text_indexes=["content"])
    
    try:
        # Insert multiple documents with common term
        for i in range(10):
            db.insert({
                "content": f"Document number {i} about machine learning and AI.",
                "number": i
            })
        
        # Search with limit
        results = db.find({}).text_search("content", "machine", limit=3).to_list()
        assert len(results) <= 3
        
        # Search with higher limit
        more_results = db.find({}).text_search("content", "machine", limit=7).to_list()
        assert len(more_results) >= len(results)
        
    finally:
        db.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")


def test_text_meta_persistence():
    """Test that text index configuration persists in .meta file."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name
    
    # Create collection with text indexes
    db1 = Collection(path, text_indexes=["title", "content", "description"])
    db1.insert({"title": "Test", "content": "Some content", "description": "A description"})
    db1.close()
    
    # Reopen without specifying text indexes
    db2 = Collection(path)
    
    try:
        # Should remember text index configuration
        assert "title" in db2._index_manager._text_fields
        assert "content" in db2._index_manager._text_fields
        assert "description" in db2._index_manager._text_fields
        
        # Search should work
        results = db2.find({}).text_search("content", "content").to_list()
        assert len(results) == 1
        
    finally:
        db2.close()
        if os.path.exists(path):
            os.unlink(path)
        if os.path.exists(path + ".meta"):
            os.unlink(path + ".meta")


def test_text_search_non_string_fields(temp_collection):
    """Test handling of non-string data in text-indexed fields."""
    db = temp_collection
    
    # Insert documents with various data types
    docs = [
        {"title": "String Title", "content": "Valid text content here."},
        {"title": 123, "content": "Content with numeric title."},
        {"title": "Another Title", "content": None},
        {"title": "Valid Title", "content": ["list", "of", "words"]},
        {"title": "Good Title", "content": {"nested": "object"}},
    ]
    
    for doc in docs:
        db.insert(doc)
    
    # Search should only find documents with valid string content
    results = db.find({}).text_search("content", "content").to_list()
    
    # Should only find documents with actual string content
    found_docs = [doc for doc, score in results]
    valid_titles = {doc["title"] for doc in found_docs}
    
    assert "String Title" in valid_titles
    assert 123 in valid_titles  # title is numeric but content is valid string
    # Others should not be found since content is not a string