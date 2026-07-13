"""Tests for hybrid search (Reciprocal Rank Fusion)."""

import pytest
from moofile import Collection

import tempfile
import os


@pytest.fixture
def temp_collection():
    """Create a temporary collection with both vector and text indexes."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bson") as f:
        path = f.name

    db = Collection(
        path,
        indexes=["category"],
        vector_indexes={"embedding": 3},
        text_indexes=["content"],
    )

    yield db

    db.close()
    for suffix in ["", ".meta", ".cache", ".lock"]:
        p = path + suffix
        if os.path.exists(p):
            os.unlink(p)


def _seed_corpus(db):
    """Insert a small corpus where text and vector relevance overlap partially."""
    docs = [
        {
            "_id": "ml_intro",
            "category": "ai",
            "content": "Introduction to machine learning algorithms and techniques",
            "embedding": [1.0, 0.0, 0.0],
        },
        {
            "_id": "ml_deep",
            "category": "ai",
            "content": "Deep learning neural networks for machine learning",
            "embedding": [0.9, 0.1, 0.0],
        },
        {
            "_id": "cv_paper",
            "category": "vision",
            "content": "Convolutional networks for computer vision image classification",
            "embedding": [0.1, 0.9, 0.0],
        },
        {
            "_id": "nlp_paper",
            "category": "nlp",
            "content": "Transformer models for natural language processing",
            "embedding": [0.0, 0.1, 0.9],
        },
        {
            "_id": "cooking",
            "category": "food",
            "content": "Italian cooking recipes pasta and pizza",
            "embedding": [0.0, 0.0, 0.1],
        },
    ]
    db.insert_many(docs)
    return docs


def test_hybrid_search_basic(temp_collection):
    """Hybrid search returns fused results from both rankers."""
    db = temp_collection
    _seed_corpus(db)

    results = (
        db.find({})
        .hybrid_search("content", "embedding", "machine learning", [1.0, 0.0, 0.0], limit=5)
        .to_list()
    )

    assert len(results) > 0
    # Results are (doc, rrf_score) tuples
    for doc, score in results:
        assert "_id" in doc
        assert isinstance(score, float)
        assert score > 0  # RRF scores are always positive

    # Scores should be in descending order
    scores = [s for _, s in results]
    assert scores == sorted(scores, reverse=True)


def test_hybrid_search_doc_in_both_ranks_higher(temp_collection):
    """A doc that appears in both text and vector results should rank higher
    than a doc that appears in only one."""
    db = temp_collection
    _seed_corpus(db)

    results = (
        db.find({})
        .hybrid_search("content", "embedding", "machine learning", [1.0, 0.0, 0.0], limit=10)
        .to_list()
    )

    # ml_intro and ml_deep both appear in both ranker lists:
    #   text:   ml_deep(rank 0), ml_intro(rank 1)
    #   vector: ml_intro(rank 0), ml_deep(rank 1)
    # Both get RRF = 1/61 + 1/62, so they tie for #1.
    # cv_paper appears only in vector (rank 2) → RRF = 1/63, much lower.
    top_two_ids = {results[0][0]["_id"], results[1][0]["_id"]}
    assert top_two_ids == {"ml_intro", "ml_deep"}
    # Both should have a higher score than cv_paper (which appears in only one list)
    cv_idx = next(i for i, (d, _) in enumerate(results) if d["_id"] == "cv_paper")
    assert results[0][1] > results[cv_idx][1]
    assert results[1][1] > results[cv_idx][1]


def test_hybrid_search_with_prefilter(temp_collection):
    """Hybrid search honours the find() pre-filter."""
    db = temp_collection
    _seed_corpus(db)

    results = (
        db.find({"category": "ai"})
        .hybrid_search("content", "embedding", "machine learning", [1.0, 0.0, 0.0], limit=5)
        .to_list()
    )

    # Only AI category docs should be returned
    for doc, _ in results:
        assert doc["category"] == "ai"

    # Should include ml_intro and ml_deep
    ids = {doc["_id"] for doc, _ in results}
    assert "ml_intro" in ids
    assert "ml_deep" in ids
    assert "cooking" not in ids


def test_hybrid_search_empty_text_results(temp_collection):
    """When text search returns nothing, hybrid degrades to vector ranking."""
    db = temp_collection
    _seed_corpus(db)

    # "xyzzy" won't match any text content
    results = (
        db.find({})
        .hybrid_search("content", "embedding", "xyzzy", [1.0, 0.0, 0.0], limit=3)
        .to_list()
    )

    # Should still return vector-only results
    assert len(results) > 0
    # ml_intro should be first (exact vector match)
    assert results[0][0]["_id"] == "ml_intro"


def test_hybrid_search_empty_vector_results(temp_collection):
    """When vector search returns nothing, hybrid degrades to text ranking."""
    db = temp_collection
    _seed_corpus(db)

    # Zero query vector → vector_search returns []
    results = (
        db.find({})
        .hybrid_search("content", "embedding", "machine learning", [0.0, 0.0, 0.0], limit=3)
        .to_list()
    )

    # Should still return text-only results
    assert len(results) > 0
    ids = {doc["_id"] for doc, _ in results}
    assert "ml_intro" in ids


def test_hybrid_search_both_empty(temp_collection):
    """When both rankers return nothing, hybrid returns empty list."""
    db = temp_collection
    _seed_corpus(db)

    results = (
        db.find({})
        .hybrid_search("content", "embedding", "xyzzy", [0.0, 0.0, 0.0], limit=3)
        .to_list()
    )

    assert results == []


def test_hybrid_search_limit(temp_collection):
    """Limit parameter is respected."""
    db = temp_collection
    _seed_corpus(db)

    results_all = (
        db.find({})
        .hybrid_search("content", "embedding", "learning", [0.5, 0.5, 0.0], limit=10)
        .to_list()
    )

    results_limited = (
        db.find({})
        .hybrid_search("content", "embedding", "learning", [0.5, 0.5, 0.0], limit=2)
        .to_list()
    )

    assert len(results_limited) <= 2
    # Limited results should be the prefix of full results
    if len(results_all) >= 2:
        assert results_limited[0][0]["_id"] == results_all[0][0]["_id"]


def test_hybrid_search_first(temp_collection):
    """first() returns the top result or None."""
    db = temp_collection
    _seed_corpus(db)

    result = (
        db.find({})
        .hybrid_search("content", "embedding", "machine learning", [1.0, 0.0, 0.0], limit=5)
        .first()
    )

    assert result is not None
    doc, score = result
    # ml_intro and ml_deep tie for #1 (both appear in both rankers)
    assert doc["_id"] in ("ml_intro", "ml_deep")

    # Empty search
    empty_result = (
        db.find({})
        .hybrid_search("content", "embedding", "xyzzy", [0.0, 0.0, 0.0], limit=5)
        .first()
    )
    assert empty_result is None


def test_hybrid_search_rrf_score_properties(temp_collection):
    """RRF scores have known mathematical properties."""
    db = temp_collection
    _seed_corpus(db)

    results = (
        db.find({})
        .hybrid_search("content", "embedding", "machine learning", [1.0, 0.0, 0.0], limit=10)
        .to_list()
    )

    # All RRF scores are positive (sum of 1/(k+rank+1) terms)
    for _, score in results:
        assert score > 0

    # The maximum possible RRF score with k=60 for a doc at rank 0 in both
    # lists is 2 * 1/61 ≈ 0.0328
    assert results[0][1] <= 2.0 / 61 + 1e-9

    # The minimum non-zero score is 1/(60 + pool) where pool = max(10*5, 50) = 50
    # So min is 1/(60+50) = 1/110 ≈ 0.0091
    # (But this depends on actual result count, so just check > 0)