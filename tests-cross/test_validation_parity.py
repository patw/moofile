"""Cross-implementation parity for _id typing and filter validation.

Each of these covers a case where the two implementations disagreed.  They
run under both backends via the `make_collection` fixture, so a divergence
fails rather than hiding behind a single-backend run.
"""

import pytest

from moofile.errors import InvalidFilterError, InvalidIdError


# ---------------------------------------------------------------------------
# Range operators vs. missing fields
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("op,expected", [
    ("$gt", ["high"]),
    ("$gte", ["high"]),
    ("$lt", ["low"]),
    ("$lte", ["low"]),
])
def test_missing_field_excluded_from_range_ops(make_collection, op, expected):
    """Rust treated a missing field as greater than everything; Python's
    TypeError guard excluded it.  Both must now exclude it."""
    db = make_collection(indexes=["age"])
    db.insert({"_id": "high", "age": 30})
    db.insert({"_id": "low", "age": 5})
    db.insert({"_id": "absent"})

    got = sorted(d["_id"] for d in db.find({"age": {op: 10}}).to_list())
    assert got == expected
    db.close()


def test_indexed_and_unindexed_paths_agree(make_collection):
    """The index path and the full-scan path must return the same rows."""
    indexed = make_collection("indexed.bson", indexes=["age"])
    unindexed = make_collection("unindexed.bson")
    for db in (indexed, unindexed):
        db.insert({"_id": "1", "age": 30})
        db.insert({"_id": "2"})
        db.insert({"_id": "3", "age": 5})

    q = {"age": {"$gt": 10}}
    a = sorted(d["_id"] for d in indexed.find(q).to_list())
    b = sorted(d["_id"] for d in unindexed.find(q).to_list())
    assert a == b == ["1"]
    indexed.close()
    unindexed.close()


# ---------------------------------------------------------------------------
# _id typing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_id", [42, 1.5, ["a"], None, True])
def test_non_string_id_rejected_by_both(make_collection, bad_id):
    """Python used to accept these and write them to disk, where the Rust
    engine then silently dropped them on the next open — the two backends
    disagreed about what the file contained."""
    db = make_collection()
    with pytest.raises(InvalidIdError):
        db.insert({"_id": bad_id, "v": 1})
    db.close()


def test_string_id_still_accepted(make_collection):
    db = make_collection()
    db.insert({"_id": "a-string", "v": 1})
    assert db.find_one({"_id": "a-string"})["v"] == 1
    db.close()


def test_generated_ids_are_strings(make_collection):
    db = make_collection()
    doc = db.insert({"v": 1})
    assert isinstance(doc["_id"], str)
    db.close()


# ---------------------------------------------------------------------------
# Filter validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_filter", [
    {"$or": ["not a doc"]},
    {"$and": [1]},
    {"$or": "not an array"},
    {"$not": 5},
    {"$bogus": []},
    {"age": {"$bogus": 1}},
])
def test_malformed_filters_rejected_by_both(make_collection, bad_filter):
    """Rust panicked (poisoning the lock) or silently matched nothing;
    Python raised assorted AttributeError/ValueError.  Both now raise
    InvalidFilterError."""
    db = make_collection(indexes=["age"])
    db.insert({"_id": "a", "age": 1})
    with pytest.raises(InvalidFilterError):
        db.find(bad_filter).to_list()
    # Still usable — the Rust panic used to make every later call fail.
    assert db.count({}) == 1
    db.close()


@pytest.mark.parametrize("good_filter", [
    {},
    {"age": 30},
    {"age": {"$gt": 10, "$lte": 40}},
    {"$or": [{"age": 30}, {"age": 40}]},
    {"$not": {"age": 30}},
    {"nested": {"plain": "doc"}},
])
def test_well_formed_filters_accepted_by_both(make_collection, good_filter):
    db = make_collection(indexes=["age"])
    db.insert({"_id": "a", "age": 30})
    db.find(good_filter).to_list()  # must not raise
    db.close()
