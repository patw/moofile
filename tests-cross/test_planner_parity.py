"""The query planner must never change an answer, only how fast it arrives.

Each filter is run twice: as-is (planned) and wrapped in a one-element
``$and`` — a top-level operator that both planners refuse to index, so it is
always the full scan.  The scan is the oracle; any disagreement is a planner
bug.
"""

import pytest


def _ids(db, q):
    return sorted(d["_id"] for d in db.find(q).to_list())


def _scan_ids(db, q):
    return _ids(db, {"$and": [q]})


@pytest.fixture
def db(make_collection):
    db = make_collection(indexes=["age"])
    for i in range(10):
        db.insert({"_id": f"k{i}", "age": i})
    yield db
    db.close()


@pytest.mark.parametrize("q", [
    {"_id": "k3"},
    {"_id": "missing"},
    {"_id": {"$eq": "k3"}},
    {"_id": {"$in": ["k7", "k2", "missing", "k2"]}},
    {"_id": {"$in": []}},
    {"_id": {"$in": ["k1", 5]}},
    {"_id": {"$in": ["k1", "k4"], "$ne": "k4"}},
    {"_id": {"$gte": "k3", "$lt": "k6"}},
    {"_id": {"$gt": "k6", "$lt": "k2"}},
    {"_id": {"$gt": "k5", "$lt": "k5"}},
    {"_id": {"$gt": "k5", "$gte": "k3"}},
    {"_id": {"$ne": "k3"}},
    {"_id": 3},
    {"_id": "k3", "age": 3},
    {"_id": "k3", "age": 4},
    {"_id": {"$in": ["k1", "k2", "k3"]}, "age": {"$gte": 2}},
], ids=repr)
def test_id_filters_match_scan(db, q):
    assert _ids(db, q) == _scan_ids(db, q)


@pytest.mark.parametrize("q", [
    {"age": {"$gt": 5, "$lt": 2}},
    {"age": {"$gt": 5, "$lt": 5}},
    {"age": {"$gte": 5, "$lte": 5}},
    {"age": {"$gt": 5, "$gte": 3}},
    {"age": {"$lt": 4, "$lte": 6}},
], ids=repr)
def test_range_edge_cases_match_scan(db, q):
    """Inverted / empty ranges used to panic in Rust's BTreeMap::range, and
    `$gt`+`$gte` together used to claim an exact (inclusive) answer."""
    assert _ids(db, q) == _scan_ids(db, q)


def test_inverted_range_write_leaves_handle_usable(db):
    """The panic above, under the write lock, poisoned the collection."""
    assert db.delete_many({"age": {"$gt": 5, "$lt": 2}}) == 0
    assert db.update_many({"age": {"$gt": 5, "$lt": 5}}, set={"x": 1}) == 0
    assert db.count({}) == 10


def test_write_by_id(db):
    assert db.update_one({"_id": "k4"}, set={"x": 1})
    assert db.find_one({"_id": "k4"})["x"] == 1
    assert db.delete_many({"_id": {"$in": ["k1", "k2", "nope"]}}) == 2
    assert db.delete_one({"_id": {"$eq": "k4"}})
    assert db.count({"_id": "k4"}) == 0
    assert _ids(db, {}) == ["k0", "k3", "k5", "k6", "k7", "k8", "k9"]


@pytest.mark.parametrize("skip,limit", [
    (0, None), (0, 3), (2, 3), (0, 0), (8, 5), (20, 3), (-2, None), (0, -1),
])
@pytest.mark.parametrize("q", [{}, {"age": {"$gte": 3}}, {"_id": {"$in": ["k1", "k5", "k9"]}}], ids=repr)
def test_skip_limit_equal_slicing_the_full_result(db, q, skip, limit):
    """skip/limit run in the native core when there is no sort; they must
    still mean exactly a slice of the unsliced result."""
    full = [d["_id"] for d in db.find(q).to_list()]
    want = full[skip:] if limit is None else full[skip:][:limit]
    query = db.find(q).skip(skip)
    if limit is not None:
        query = query.limit(limit)
    assert [d["_id"] for d in query.to_list()] == want


def test_batch_writes_see_buffered_changes_the_index_does_not(db):
    """Inside a batch the committed docs are planned but the overlay is always
    a candidate: a buffered update can move a doc into an indexed range."""
    with db.batch():
        assert db.update_one({"_id": "k1"}, set={"age": 100})
        db.insert({"_id": "new", "age": 200})
        assert db.delete_one({"_id": "k2"})
        # k1 now matches only through the overlay; k2 is gone; `new` was
        # never committed.
        assert db.update_many({"age": {"$gte": 9}}, set={"hit": True}) == 3
        assert not db.delete_one({"_id": "k2"})
        assert db.delete_many({"_id": {"$in": ["k1", "k3", "k2"]}}) == 2
    assert _ids(db, {"hit": True}) == ["k9", "new"]
    assert _ids(db, {}) == ["k0", "k4", "k5", "k6", "k7", "k8", "k9", "new"]


def test_insert_many_is_all_or_nothing(make_collection):
    """A duplicate anywhere in the call -- including twice within it -- fails
    the call before anything is written."""
    from moofile.errors import DuplicateKeyError

    db = make_collection(indexes=["age"])
    db.insert({"_id": "x", "age": 1})
    for batch in ([{"_id": "a"}, {"_id": "x"}], [{"_id": "a"}, {"_id": "b"}, {"_id": "a"}]):
        with pytest.raises(DuplicateKeyError):
            db.insert_many(batch)
        assert _ids(db, {}) == ["x"]
    db.insert_many([{"_id": f"n{i}", "age": i} for i in range(5)])
    db.close()
    db = make_collection(indexes=["age"])
    assert _ids(db, {"age": {"$gte": 3}}) == ["n3", "n4"]
    db.close()


def test_text_index_follows_updates_and_deletes(make_collection):
    """Removal visits only the removed document's own terms now; nothing it
    indexed may linger, and nothing else may be disturbed."""
    db = make_collection(text_indexes=["body"])
    db.insert_many([
        {"_id": "a", "body": "zebra zebra crossing"},
        {"_id": "b", "body": "zebra stripes"},
        {"_id": "c", "body": "plain words"},
    ])
    db.update_one({"_id": "a"}, set={"body": "giraffe neck"})
    db.delete_one({"_id": "c"})

    def hits(q):
        return sorted(d["_id"] for d, _ in db.find().text_search("body", q, 10).to_list())

    assert hits("zebra") == ["b"]
    assert hits("crossing") == []
    assert hits("giraffe") == ["a"]
    assert hits("words") == []
    db.close()
    db = make_collection(text_indexes=["body"])
    assert sorted(d["_id"] for d, _ in db.find().text_search("body", "zebra", 10).to_list()) == ["b"]
    db.close()
