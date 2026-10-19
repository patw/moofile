"""BM25 scores must be positive, and more of a query term must never hurt.

Both backends used the unsmoothed IDF ``ln((N - df + 0.5) / (df + 0.5))``,
which goes negative once a term is in more than half the documents. Scores
are sorted descending, so a negative IDF inverted the ranking for common
terms: "redis redis redis" ranked *below* "redis cache broke", and the same
two documents swapped places depending on how many unrelated documents shared
the collection. Both now use the Lucene form ``ln(1 + (N - df + 0.5) / (df + 0.5))``.
"""

import math

import pytest

K1, B = 1.2, 0.75


def _ranked(db, query, field="body", limit=10):
    return [(d["i"], s) for d, s in db.find({}).text_search(field, query, limit).to_list()]


def _insert(db, bodies):
    for i, body in enumerate(bodies):
        db.insert({"_id": f"{i:03d}", "i": i, "body": body})


def test_a_common_term_still_ranks_more_occurrences_first(make_collection):
    db = make_collection(text_indexes=["body"])
    # "redis" is in 2 of 3 docs (df > N/2); both matches are three terms long,
    # so term frequency is the only difference between them.
    _insert(db, ["redis cache broke", "redis redis redis", "lunch at noon"])
    assert [i for i, _ in _ranked(db, "redis")] == [1, 0]
    db.close()


def test_unrelated_documents_do_not_reorder_matches(make_collection):
    db = make_collection(text_indexes=["body"])
    _insert(db, ["redis cache broke", "redis redis redis", "lunch at noon"])
    before = [i for i, _ in _ranked(db, "redis")]
    for j, body in enumerate(["coffee", "weather", "standup", "parking"]):
        db.insert({"_id": f"x{j}", "i": 100 + j, "body": body})
    after = [i for i, _ in _ranked(db, "redis")]
    assert before == after == [1, 0]
    db.close()


@pytest.mark.parametrize("bodies, query", [
    (["deploy redis cache", "deploy notes", "deploy again"], "deploy"),          # df == N
    (["deploy redis cache", "deploy notes", "deploy again"], "deploy redis"),
    (["machine learning is great", "deep learning and machine learning",
      "cooking recipes for dinner"], "machine learning"),                         # df 2/3
    (["alpha beta", "alpha gamma"], "alpha"),                                     # df == N == 2
])
def test_every_score_is_positive(make_collection, bodies, query):
    db = make_collection(text_indexes=["body"])
    _insert(db, bodies)
    ranked = _ranked(db, query)
    assert ranked, "every query above matches something"
    assert all(score > 0 for _, score in ranked), ranked
    db.close()


def test_matching_more_query_terms_ranks_higher(make_collection):
    db = make_collection(text_indexes=["body"])
    # "deploy" is in every document; only doc 0 also has "redis".
    _insert(db, ["deploy redis cache", "deploy notes here", "deploy again today"])
    assert _ranked(db, "deploy redis")[0][0] == 0
    db.close()


def test_score_matches_the_formula(make_collection):
    """Pins both backends to the same IDF and term-frequency formula, not
    merely the same order."""
    db = make_collection(text_indexes=["body"])
    bodies = ["redis cache broke", "redis redis redis", "lunch at noon"]
    _insert(db, bodies)

    n_docs, df = 3, 2
    avgdl = 3.0                       # every body is three terms
    idf = math.log(1 + (n_docs - df + 0.5) / (df + 0.5))

    def expected(tf, dl=3):
        return idf * (tf * (K1 + 1)) / (tf + K1 * (1 - B + B * dl / avgdl))

    scores = dict(_ranked(db, "redis"))
    assert scores[1] == pytest.approx(expected(3), rel=1e-5)
    assert scores[0] == pytest.approx(expected(1), rel=1e-5)
    db.close()
