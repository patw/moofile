"""Ranked search must be a total order.

Equal-scoring documents used to come back in HashMap iteration order on the
Rust side (randomised per process) and insertion order on the Python side, so
`text_search` returned different documents run-to-run, disagreed between
backends, and made pagination incoherent.  Both now break ties on _id.
"""

import random


def _tied_corpus(db, n=40):
    """Every document scores identically for the query below."""
    for i in range(n):
        db.insert({"_id": f"{i:03d}", "body": "machine learning is fascinating"})


def test_text_ranking_is_stable_across_reopen(make_collection, tmp_path):
    db = make_collection("t.bson", text_indexes=["body"])
    _tied_corpus(db)
    first = [d["_id"] for d, _ in db.find({}).text_search("body", "machine learning", 5).to_list()]
    db.close()

    db2 = make_collection("t.bson", text_indexes=["body"])
    second = [d["_id"] for d, _ in db2.find({}).text_search("body", "machine learning", 5).to_list()]
    db2.close()

    assert first == second
    assert first == sorted(first), "ties must break on _id ascending"


def test_text_ranking_is_stable_within_a_session(make_collection):
    db = make_collection(text_indexes=["body"])
    _tied_corpus(db)
    runs = [
        [d["_id"] for d, _ in db.find({}).text_search("body", "machine learning", 5).to_list()]
        for _ in range(5)
    ]
    assert all(r == runs[0] for r in runs)
    db.close()


def test_vector_ranking_is_stable_after_churn(make_collection):
    """Row order in the vector matrix shifts as documents are swap-removed."""
    dim = 4
    db = make_collection(vector_indexes={"emb": dim})
    for i in range(30):
        db.insert({"_id": f"{i:03d}", "emb": [1.0, 0.0, 0.0, 0.0]})  # all tied

    q = [1.0, 0.0, 0.0, 0.0]
    before = [d["_id"] for d, _ in db.find({}).vector_search("emb", q, 5).to_list()]
    db.delete_many({"_id": {"$in": [f"{i:03d}" for i in range(20, 30)]}})
    after = [d["_id"] for d, _ in db.find({}).vector_search("emb", q, 5).to_list()]

    assert before == sorted(before)
    assert after == sorted(after)
    assert before == after, "removing unrelated rows must not reshuffle the top-k"
    db.close()
