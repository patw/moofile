"""Regression tests for _id typing and filter validation.

These cover three defects that shared a root cause: user-supplied input
reaching an unchecked code path.  Two of them panicked inside Rust while a
lock was held, which poisoned the lock and permanently bricked the
collection handle; the third silently returned wrong results.
"""

import pytest

from moofile import Collection
from moofile.errors import InvalidFilterError, InvalidIdError


@pytest.fixture
def col(tmp_path):
    with Collection(str(tmp_path / "test.bson"), indexes=["age"]) as db:
        yield db


# ---------------------------------------------------------------------------
# Range operators vs. missing fields
# ---------------------------------------------------------------------------

class TestMissingFieldComparisons:
    """A document without the field must not satisfy a range operator.

    Regression: `cmp_op` treated a missing field as greater than everything,
    so `{"age": {"$gt": N}}` matched every document lacking `age` — but only
    on the full-scan path.  The index path excluded them, so the same query
    returned different results depending on whether the field was indexed.
    """

    def test_missing_field_does_not_match_range_ops(self, col):
        col.insert({"_id": "has", "age": 30})
        col.insert({"_id": "missing"})

        for op in ("$gt", "$gte", "$lt", "$lte"):
            got = [d["_id"] for d in col.find({"age": {op: 10}}).to_list()]
            assert "missing" not in got, f"{op} matched a document with no 'age'"

    def test_indexed_and_unindexed_agree(self, tmp_path):
        """The same data and query must give the same answer either way."""
        docs = [{"_id": "1", "age": 30}, {"_id": "2"}, {"_id": "3", "age": 5}]
        results = {}
        for label, indexes in (("indexed", ["age"]), ("unindexed", [])):
            with Collection(str(tmp_path / f"{label}.bson"), indexes=indexes) as db:
                for d in docs:
                    db.insert(dict(d))
                results[label] = sorted(
                    x["_id"] for x in db.find({"age": {"$gt": 10}}).to_list()
                )

        assert results["indexed"] == results["unindexed"] == ["1"]


# ---------------------------------------------------------------------------
# _id typing
# ---------------------------------------------------------------------------

class TestInvalidId:
    @pytest.mark.parametrize("bad_id", [42, 1.5, ["a"], {"x": 1}, None, True])
    def test_non_string_id_rejected(self, col, bad_id):
        with pytest.raises(InvalidIdError):
            col.insert({"_id": bad_id, "v": 1})

    def test_collection_usable_after_rejection(self, col):
        """The old code panicked here and poisoned the lock, so every
        subsequent call — including reads — raised for the life of the handle."""
        with pytest.raises(InvalidIdError):
            col.insert({"_id": 42})

        col.insert({"_id": "fine", "v": 1})
        assert col.count({}) == 1
        assert col.find_one({"_id": "fine"})["v"] == 1

    def test_invalid_id_is_a_type_error(self, col):
        """InvalidIdError subclasses TypeError for ergonomics."""
        with pytest.raises(TypeError):
            col.insert({"_id": 42})

    def test_batch_survives_rejected_id(self, tmp_path):
        """A rejected insert must not discard already-buffered batch writes."""
        path = str(tmp_path / "batch.bson")
        with Collection(path) as db:
            with db.batch():
                db.insert({"_id": "a", "v": 1})
                db.insert({"_id": "b", "v": 2})
                with pytest.raises(InvalidIdError):
                    db.insert({"_id": 99})

        with Collection(path) as db:
            assert sorted(d["_id"] for d in db.find({}).to_list()) == ["a", "b"]


# ---------------------------------------------------------------------------
# Filter validation
# ---------------------------------------------------------------------------

class TestFilterValidation:
    @pytest.mark.parametrize("bad_filter", [
        {"$or": ["not a doc"]},
        {"$and": [1]},
        {"$or": "not an array"},
        {"$not": 5},
        {"$bogus": []},
        {"age": {"$bogus": 1}},
        {"$or": [{"age": {"$nope": 1}}]},
    ])
    def test_malformed_filters_rejected(self, col, bad_filter):
        with pytest.raises(InvalidFilterError):
            col.find(bad_filter).to_list()

    @pytest.mark.parametrize("good_filter", [
        {},
        {"age": 30},
        {"age": {"$gt": 10, "$lte": 40}},
        {"$or": [{"age": 30}, {"age": 40}]},
        {"$and": [{"$or": [{"age": 30}]}]},
        {"$not": {"age": 30}},
        {"nested": {"plain": "doc"}},
        {"tags": {"$elemMatch": {"$eq": "x"}}},
    ])
    def test_well_formed_filters_accepted(self, col, good_filter):
        col.find(good_filter).to_list()  # must not raise

    def test_collection_usable_after_bad_filter(self, col):
        col.insert({"_id": "a", "age": 1})
        with pytest.raises(InvalidFilterError):
            col.find({"$or": ["junk"]}).to_list()
        assert col.count({}) == 1

    def test_validation_applies_to_mutations(self, col):
        col.insert({"_id": "a", "age": 1})
        for call in (
            lambda: col.delete_many({"$or": [1]}),
            lambda: col.update_many({"$or": [1]}, set={"x": 1}),
            lambda: col.count({"age": {"$bogus": 1}}),
        ):
            with pytest.raises(InvalidFilterError):
                call()
        assert col.count({}) == 1

    def test_invalid_filter_is_a_value_error(self, col):
        with pytest.raises(ValueError):
            col.find({"$bogus": []}).to_list()
