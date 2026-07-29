"""Documents handed to callers must be isolated from the in-memory index.

Regression: the pure-Python implementation stored inserted dicts by reference
and returned those same objects from queries.  A caller mutating a result
mutated the index in place; because close() pickles the index into the .cache
file, the corruption then survived a reopen and disappeared again as soon as
the cache was invalidated, so reads disagreed with the durable BSON file.
"""

import pytest

from moofile import Collection

ORIGINAL = {
    "_id": "1",
    "status": "active",
    "tags": ["a", "b"],
    "nested": {"k": [1, 2], "deep": {"x": 1}},
}


@pytest.fixture
def col(tmp_path):
    with Collection(str(tmp_path / "test.bson"), indexes=["status"]) as db:
        db.insert(dict(ORIGINAL))
        db.insert({"_id": "2", "status": "active"})
        yield db


class TestReadIsolation:
    def test_mutating_result_does_not_corrupt_index(self, col):
        doc = col.find({"status": "active"}).first()
        doc["status"] = "HACKED"

        found = sorted(d["_id"] for d in col.find({"status": "active"}).to_list())
        assert found == ["1", "2"], "index must still find the document by its real value"

    def test_mutating_result_does_not_corrupt_document(self, col):
        doc = col.find_one({"_id": "1"})
        doc["status"] = "HACKED"
        doc["injected"] = True
        doc["tags"].append("MUTATED")
        doc["nested"]["k"].append(99)
        doc["nested"]["deep"]["x"] = 999

        assert col.find_one({"_id": "1"}) == ORIGINAL

    def test_separate_reads_return_separate_objects(self, col):
        a = col.find_one({"_id": "1"})
        b = col.find_one({"_id": "1"})
        assert a == b
        assert a is not b
        assert a["nested"] is not b["nested"]
        assert a["tags"] is not b["tags"]

    def test_to_list_results_are_isolated(self, col):
        for d in col.find({}).to_list():
            d["status"] = "HACKED"
        assert col.count({"status": "active"}) == 2

    def test_sorted_and_limited_results_are_isolated(self, col):
        for d in col.find({}).sort("_id").limit(1).to_list():
            d["tags"] = ["MUTATED"]
        assert col.find_one({"_id": "1"})["tags"] == ["a", "b"]


class TestWriteIsolation:
    def test_mutating_inserted_dict_does_not_reach_index(self, tmp_path):
        with Collection(str(tmp_path / "w.bson")) as db:
            src = {"_id": "x", "tags": ["orig"], "nested": {"k": [1]}}
            db.insert(src)
            src["tags"].append("post-insert")
            src["nested"]["k"].append(2)
            src["added"] = True

            assert db.find_one({"_id": "x"}) == {
                "_id": "x", "tags": ["orig"], "nested": {"k": [1]},
            }

    def test_mutating_replacement_dict_does_not_reach_index(self, tmp_path):
        with Collection(str(tmp_path / "r.bson")) as db:
            db.insert({"_id": "x", "v": 0})
            repl = {"v": 1, "tags": ["orig"]}
            db.replace_one({"_id": "x"}, repl)
            repl["tags"].append("post-replace")

            assert db.find_one({"_id": "x"})["tags"] == ["orig"]


class TestCachePersistence:
    def test_cache_and_bson_agree_after_caller_mutation(self, tmp_path):
        """The corruption used to be pickled into .cache and survive a reopen,
        then vanish when the cache was invalidated."""
        path = str(tmp_path / "c.bson")
        with Collection(path, indexes=["status"]) as db:
            db.insert(dict(ORIGINAL))
            doc = db.find_one({"_id": "1"})
            doc["status"] = "HACKED"
            doc["nested"]["k"].append(99)

        import os
        with Collection(path, indexes=["status"]) as db:
            from_cache = db.find_one({"_id": "1"})
        if os.path.exists(path + ".cache"):
            os.remove(path + ".cache")
        with Collection(path, indexes=["status"]) as db:
            from_bson = db.find_one({"_id": "1"})

        assert from_cache == from_bson == ORIGINAL


class TestIndexConsistencyUnderChurn:
    """Posting lists changed from lists to sets/ordered-dicts to kill an
    O(n) membership scan.  Verify the index stays exactly consistent."""

    def test_repeated_updates_leave_no_stale_entries(self, tmp_path):
        with Collection(str(tmp_path / "churn.bson"), indexes=["status"]) as db:
            for i in range(50):
                db.insert({"_id": str(i), "status": "a"})

            for cycle in range(5):
                target = "b" if cycle % 2 == 0 else "a"
                other = "a" if cycle % 2 == 0 else "b"
                db.update_many({"status": other}, set={"status": target})

                assert db.count({"status": target}) == 50
                assert db.count({"status": other}) == 0
                found = sorted(int(d["_id"]) for d in db.find({"status": target}).to_list())
                assert found == list(range(50)), "no duplicate or missing ids"

    def test_delete_removes_from_index(self, tmp_path):
        with Collection(str(tmp_path / "del.bson"), indexes=["status"]) as db:
            for i in range(20):
                db.insert({"_id": str(i), "status": "a"})
            db.delete_many({"_id": {"$in": [str(i) for i in range(0, 20, 2)]}})

            assert db.count({"status": "a"}) == 10
            assert sorted(int(d["_id"]) for d in db.find({"status": "a"}).to_list()) \
                == list(range(1, 20, 2))

    def test_reinserting_same_id_is_not_duplicated(self, tmp_path):
        with Collection(str(tmp_path / "re.bson"), indexes=["status"]) as db:
            db.insert({"_id": "x", "status": "a"})
            db.delete_one({"_id": "x"})
            db.insert({"_id": "x", "status": "a"})
            assert db.count({"status": "a"}) == 1
            assert len(db.find({"status": "a"}).to_list()) == 1
