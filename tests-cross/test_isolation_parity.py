"""Both backends must isolate returned documents from the index.

The Rust engine already cloned on read; the pure-Python reference returned
live references.  These run under both backends so the two cannot drift apart
again.
"""

ORIGINAL = {
    "_id": "1",
    "status": "active",
    "tags": ["a", "b"],
    "nested": {"k": [1, 2]},
}


def test_mutating_a_result_does_not_corrupt_the_index(make_collection):
    db = make_collection(indexes=["status"])
    db.insert(dict(ORIGINAL))
    db.insert({"_id": "2", "status": "active"})

    doc = db.find({"status": "active"}).first()
    doc["status"] = "HACKED"
    doc["tags"].append("MUTATED")
    doc["nested"]["k"].append(99)

    assert sorted(d["_id"] for d in db.find({"status": "active"}).to_list()) == ["1", "2"]
    assert db.find_one({"_id": "1"}) == ORIGINAL
    db.close()


def test_reads_return_independent_objects(make_collection):
    db = make_collection()
    db.insert(dict(ORIGINAL))
    a = db.find_one({"_id": "1"})
    b = db.find_one({"_id": "1"})
    assert a == b and a is not b
    assert a["nested"] is not b["nested"]
    db.close()


def test_mutating_inserted_dict_does_not_reach_index(make_collection):
    db = make_collection()
    src = {"_id": "x", "tags": ["orig"]}
    db.insert(src)
    src["tags"].append("post-insert")
    assert db.find_one({"_id": "x"})["tags"] == ["orig"]
    db.close()


def test_index_stays_consistent_under_churn(make_collection):
    db = make_collection(indexes=["status"])
    for i in range(30):
        db.insert({"_id": str(i), "status": "a"})
    for cycle in range(4):
        src, dst = ("a", "b") if cycle % 2 == 0 else ("b", "a")
        db.update_many({"status": src}, set={"status": dst})
        assert db.count({"status": dst}) == 30
        assert db.count({"status": src}) == 0
        assert sorted(int(d["_id"]) for d in db.find({"status": dst}).to_list()) \
            == list(range(30))
    db.close()
