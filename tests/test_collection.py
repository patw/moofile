"""Tests for Collection: CRUD, persistence, utility operations."""

import os
import pytest

from moofile import Collection, DuplicateKeyError, DocumentNotFoundError, ReadOnlyError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def col(tmp_path):
    """A fresh Collection for each test."""
    path = str(tmp_path / "test.bson")
    with Collection(path, indexes=["email", "age", "status"]) as db:
        yield db


@pytest.fixture
def col_path(tmp_path):
    """Return both path and a fresh Collection so we can reopen it."""
    path = str(tmp_path / "test.bson")
    return path


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

class TestInsert:
    def test_assigns_id_when_absent(self, col):
        doc = col.insert({"name": "alice"})
        assert "_id" in doc
        assert len(doc["_id"]) == 24  # 12 bytes → 24 hex chars

    def test_preserves_provided_id(self, col):
        doc = col.insert({"_id": "custom-id", "name": "bob"})
        assert doc["_id"] == "custom-id"

    def test_insert_returns_document(self, col):
        doc = col.insert({"x": 1})
        assert doc["x"] == 1

    def test_duplicate_id_raises(self, col):
        col.insert({"_id": "dup"})
        with pytest.raises(DuplicateKeyError):
            col.insert({"_id": "dup"})

    def test_insert_many(self, col):
        docs = col.insert_many([{"n": 1}, {"n": 2}, {"n": 3}])
        assert len(docs) == 3
        assert all("_id" in d for d in docs)

    def test_insert_many_returns_all(self, col):
        inserted = col.insert_many([{"_id": str(i)} for i in range(5)])
        assert [d["_id"] for d in inserted] == [str(i) for i in range(5)]


# ---------------------------------------------------------------------------
# Find
# ---------------------------------------------------------------------------

class TestFind:
    def _seed(self, col):
        col.insert_many([
            {"_id": "a1", "name": "alice", "age": 30, "status": "active"},
            {"_id": "a2", "name": "bob",   "age": 25, "status": "active"},
            {"_id": "a3", "name": "carol", "age": 40, "status": "inactive"},
            {"_id": "a4", "name": "dave",  "age": 22, "status": "trial"},
        ])

    def test_find_all_no_filter(self, col):
        self._seed(col)
        assert len(col.find().to_list()) == 4

    def test_find_exact_match(self, col):
        self._seed(col)
        results = col.find({"name": "alice"}).to_list()
        assert len(results) == 1
        assert results[0]["name"] == "alice"

    def test_find_indexed_exact(self, col):
        self._seed(col)
        results = col.find({"status": "active"}).to_list()
        assert len(results) == 2

    def test_find_one_returns_dict(self, col):
        self._seed(col)
        doc = col.find_one({"_id": "a1"})
        assert isinstance(doc, dict)
        assert doc["name"] == "alice"

    def test_find_one_no_match_returns_none(self, col):
        self._seed(col)
        assert col.find_one({"name": "nobody"}) is None

    def test_count(self, col):
        self._seed(col)
        assert col.count({"status": "active"}) == 2
        assert col.count() == 4

    def test_exists_true(self, col):
        self._seed(col)
        assert col.exists({"name": "alice"}) is True

    def test_exists_false(self, col):
        self._seed(col)
        assert col.exists({"name": "nobody"}) is False


# ---------------------------------------------------------------------------
# Query operators
# ---------------------------------------------------------------------------

class TestFilterOperators:
    def _seed(self, col):
        col.insert_many([
            {"_id": "1", "age": 10},
            {"_id": "2", "age": 20},
            {"_id": "3", "age": 30},
            {"_id": "4", "age": 40},
        ])

    def test_gt(self, col):
        self._seed(col)
        assert len(col.find({"age": {"$gt": 20}}).to_list()) == 2

    def test_gte(self, col):
        self._seed(col)
        assert len(col.find({"age": {"$gte": 20}}).to_list()) == 3

    def test_lt(self, col):
        self._seed(col)
        assert len(col.find({"age": {"$lt": 30}}).to_list()) == 2

    def test_lte(self, col):
        self._seed(col)
        assert len(col.find({"age": {"$lte": 30}}).to_list()) == 3

    def test_ne(self, col):
        self._seed(col)
        assert len(col.find({"age": {"$ne": 20}}).to_list()) == 3

    def test_in(self, col):
        self._seed(col)
        result = col.find({"age": {"$in": [10, 30]}}).to_list()
        assert len(result) == 2

    def test_nin(self, col):
        self._seed(col)
        result = col.find({"age": {"$nin": [10, 30]}}).to_list()
        assert len(result) == 2

    def test_eq_explicit(self, col):
        self._seed(col)
        result = col.find({"age": {"$eq": 20}}).to_list()
        assert len(result) == 1

    def test_and(self, col):
        col.insert_many([
            {"_id": "x", "age": 25, "status": "active"},
            {"_id": "y", "age": 25, "status": "inactive"},
            {"_id": "z", "age": 30, "status": "active"},
        ])
        result = col.find({"$and": [{"age": 25}, {"status": "active"}]}).to_list()
        assert len(result) == 1
        assert result[0]["_id"] == "x"

    def test_or(self, col):
        col.insert_many([
            {"_id": "p", "age": 10},
            {"_id": "q", "age": 20},
            {"_id": "r", "age": 30},
        ])
        result = col.find({"$or": [{"age": 10}, {"age": 30}]}).to_list()
        assert len(result) == 2

    def test_not(self, col):
        col.insert_many([
            {"_id": "s", "status": "active"},
            {"_id": "t", "status": "inactive"},
        ])
        result = col.find({"$not": {"status": "active"}}).to_list()
        assert len(result) == 1
        assert result[0]["_id"] == "t"

    def test_exists_true(self, col):
        col.insert_many([
            {"_id": "u", "email": "u@example.com"},
            {"_id": "v"},
        ])
        result = col.find({"email": {"$exists": True}}).to_list()
        assert len(result) == 1
        assert result[0]["_id"] == "u"

    def test_exists_false(self, col):
        col.insert_many([
            {"_id": "u2", "email": "u2@example.com"},
            {"_id": "v2"},
        ])
        result = col.find({"email": {"$exists": False}}).to_list()
        assert len(result) == 1
        assert result[0]["_id"] == "v2"

    def test_elem_match_objects(self, col):
        col.insert_many([
            {"_id": "e1", "items": [{"name": "pen", "qty": 5}, {"name": "book", "qty": 1}]},
            {"_id": "e2", "items": [{"name": "eraser", "qty": 10}]},
        ])
        result = col.find({"items": {"$elemMatch": {"name": "pen"}}}).to_list()
        assert len(result) == 1
        assert result[0]["_id"] == "e1"

    def test_elem_match_scalars(self, col):
        col.insert_many([
            {"_id": "s1", "scores": [80, 90, 70]},
            {"_id": "s2", "scores": [50, 60, 40]},
        ])
        result = col.find({"scores": {"$elemMatch": {"$gt": 85}}}).to_list()
        assert len(result) == 1
        assert result[0]["_id"] == "s1"

    def test_combined_range_on_indexed_field(self, col):
        self._seed(col)
        result = col.find({"age": {"$gte": 20, "$lt": 40}}).to_list()
        assert sorted(r["age"] for r in result) == [20, 30]


# ---------------------------------------------------------------------------
# Query chain
# ---------------------------------------------------------------------------

class TestQueryChain:
    def _seed(self, col):
        col.insert_many([
            {"_id": str(i), "age": i * 10, "name": f"user{i}"}
            for i in range(1, 6)
        ])

    def test_sort_asc(self, col):
        self._seed(col)
        ages = [d["age"] for d in col.find().sort("age").to_list()]
        assert ages == sorted(ages)

    def test_sort_desc(self, col):
        self._seed(col)
        ages = [d["age"] for d in col.find().sort("age", descending=True).to_list()]
        assert ages == sorted(ages, reverse=True)

    def test_limit(self, col):
        self._seed(col)
        result = col.find().limit(3).to_list()
        assert len(result) == 3

    def test_skip(self, col):
        self._seed(col)
        total = len(col.find().to_list())
        result = col.find().skip(2).to_list()
        assert len(result) == total - 2

    def test_skip_and_limit(self, col):
        self._seed(col)
        result = col.find().sort("age").skip(1).limit(2).to_list()
        assert len(result) == 2
        assert result[0]["age"] == 20

    def test_count_terminal(self, col):
        self._seed(col)
        assert col.find({"age": {"$gt": 20}}).count() == 3

    def test_first_terminal(self, col):
        self._seed(col)
        doc = col.find().sort("age").first()
        assert doc["age"] == 10

    def test_first_no_match_returns_none(self, col):
        self._seed(col)
        assert col.find({"age": 999}).first() is None

    def test_sort_with_none_values(self, col):
        col.insert_many([
            {"_id": "n1", "score": 5},
            {"_id": "n2"},              # missing field
            {"_id": "n3", "score": 3},
        ])
        result = col.find().sort("score").to_list()
        # Docs with None sort key should appear last
        assert result[-1]["_id"] == "n2"


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

class TestUpdate:
    def test_update_one_set(self, col):
        col.insert({"_id": "u1", "age": 30})
        col.update_one({"_id": "u1"}, set={"age": 31})
        assert col.find_one({"_id": "u1"})["age"] == 31

    def test_update_one_preserves_other_fields(self, col):
        col.insert({"_id": "u2", "name": "alice", "age": 30})
        col.update_one({"_id": "u2"}, set={"age": 31})
        doc = col.find_one({"_id": "u2"})
        assert doc["name"] == "alice"
        assert doc["age"] == 31

    def test_update_one_unset(self, col):
        col.insert({"_id": "u3", "name": "bob", "temp": "remove_me"})
        col.update_one({"_id": "u3"}, unset=["temp"])
        doc = col.find_one({"_id": "u3"})
        assert "temp" not in doc
        assert doc["name"] == "bob"

    def test_update_one_inc(self, col):
        col.insert({"_id": "u4", "count": 5})
        col.update_one({"_id": "u4"}, inc={"count": 3})
        assert col.find_one({"_id": "u4"})["count"] == 8

    def test_update_one_not_found_raises(self, col):
        with pytest.raises(DocumentNotFoundError):
            col.update_one({"_id": "nonexistent"}, set={"x": 1})

    def test_update_many(self, col):
        col.insert_many([
            {"_id": "m1", "status": "trial"},
            {"_id": "m2", "status": "trial"},
            {"_id": "m3", "status": "active"},
        ])
        updated = col.update_many({"status": "trial"}, set={"status": "expired"})
        assert updated == 2
        assert col.count({"status": "expired"}) == 2
        assert col.count({"status": "trial"}) == 0

    def test_replace_one(self, col):
        col.insert({"_id": "r1", "name": "old", "extra": True})
        col.replace_one({"_id": "r1"}, {"name": "new"})
        doc = col.find_one({"_id": "r1"})
        assert doc["name"] == "new"
        assert doc["_id"] == "r1"
        assert "extra" not in doc

    def test_replace_one_not_found_raises(self, col):
        with pytest.raises(DocumentNotFoundError):
            col.replace_one({"_id": "missing"}, {"name": "x"})


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

class TestDelete:
    def test_delete_one(self, col):
        col.insert({"_id": "d1", "name": "alice"})
        assert col.delete_one({"_id": "d1"}) is True
        assert col.find_one({"_id": "d1"}) is None

    def test_delete_one_returns_false_when_missing(self, col):
        assert col.delete_one({"_id": "nobody"}) is False

    def test_delete_many(self, col):
        col.insert_many([
            {"_id": "dm1", "status": "old"},
            {"_id": "dm2", "status": "old"},
            {"_id": "dm3", "status": "active"},
        ])
        count = col.delete_many({"status": "old"})
        assert count == 2
        assert col.count() == 1

    def test_delete_removes_from_index(self, col):
        col.insert({"_id": "di1", "email": "x@x.com"})
        col.delete_one({"_id": "di1"})
        assert col.find({"email": "x@x.com"}).count() == 0


# ---------------------------------------------------------------------------
# Stats and compaction
# ---------------------------------------------------------------------------

class TestStats:
    def test_stats_structure(self, col):
        col.insert({"_id": "s1"})
        s = col.stats()
        assert set(s.keys()) == {"documents", "dead_records", "file_size_bytes", "dead_ratio"}

    def test_dead_records_after_update(self, col):
        col.insert({"_id": "dc1", "v": 1})
        col.update_one({"_id": "dc1"}, set={"v": 2})
        s = col.stats()
        # Original record + replacement = 2 total, 1 live → 1 dead
        assert s["dead_records"] == 1
        assert s["documents"] == 1

    def test_compact_removes_dead_records(self, col):
        col.insert({"_id": "cp1", "v": 1})
        col.update_one({"_id": "cp1"}, set={"v": 2})
        col.update_one({"_id": "cp1"}, set={"v": 3})
        col.compact()
        s = col.stats()
        assert s["dead_records"] == 0
        assert s["documents"] == 1
        # Data is preserved
        assert col.find_one({"_id": "cp1"})["v"] == 3

    def test_compact_preserves_all_live_documents(self, col):
        col.insert_many([{"_id": str(i), "v": i} for i in range(20)])
        col.update_many({}, set={"updated": True})
        col.compact()
        assert col.count() == 20
        assert all(d.get("updated") for d in col.find().to_list())


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_data_survives_close_reopen(self, col_path):
        with Collection(col_path) as db:
            db.insert({"_id": "p1", "name": "persistent"})

        with Collection(col_path) as db:
            doc = db.find_one({"_id": "p1"})
            assert doc is not None
            assert doc["name"] == "persistent"

    def test_indexes_persist_across_reopens(self, col_path):
        with Collection(col_path, indexes=["email"]) as db:
            db.insert({"email": "a@b.com", "name": "alice"})

        with Collection(col_path, indexes=["email"]) as db:
            # Index should be rebuilt on reopen
            assert db.find_one({"email": "a@b.com"}) is not None

    def test_deletes_survive_reopen(self, col_path):
        with Collection(col_path) as db:
            db.insert({"_id": "del1", "name": "gone"})
            db.delete_one({"_id": "del1"})

        with Collection(col_path) as db:
            assert db.find_one({"_id": "del1"}) is None

    def test_updates_survive_reopen(self, col_path):
        with Collection(col_path) as db:
            db.insert({"_id": "upd1", "v": 1})
            db.update_one({"_id": "upd1"}, set={"v": 99})

        with Collection(col_path) as db:
            assert db.find_one({"_id": "upd1"})["v"] == 99


# ---------------------------------------------------------------------------
# Read-only mode
# ---------------------------------------------------------------------------

class TestReadOnly:
    def test_readonly_allows_reads(self, col_path):
        with Collection(col_path) as db:
            db.insert({"_id": "ro1", "name": "test"})

        with Collection(col_path, readonly=True) as db:
            assert db.find_one({"_id": "ro1"}) is not None

    def test_readonly_blocks_insert(self, col_path):
        with Collection(col_path) as db:
            pass
        with Collection(col_path, readonly=True) as db:
            with pytest.raises(ReadOnlyError):
                db.insert({"name": "x"})

    def test_readonly_blocks_update(self, col_path):
        with Collection(col_path) as db:
            db.insert({"_id": "ro2"})
        with Collection(col_path, readonly=True) as db:
            with pytest.raises(ReadOnlyError):
                db.update_one({"_id": "ro2"}, set={"x": 1})

    def test_readonly_blocks_delete(self, col_path):
        with Collection(col_path) as db:
            db.insert({"_id": "ro3"})
        with Collection(col_path, readonly=True) as db:
            with pytest.raises(ReadOnlyError):
                db.delete_one({"_id": "ro3"})

    def test_readonly_blocks_compact(self, col_path):
        with Collection(col_path) as db:
            pass
        with Collection(col_path, readonly=True) as db:
            with pytest.raises(ReadOnlyError):
                db.compact()


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    def test_context_manager_closes(self, col_path):
        with Collection(col_path) as db:
            db.insert({"_id": "cm1"})
        # Storage should be closed
        assert db._storage is None

    def test_context_manager_yields_self(self, col_path):
        with Collection(col_path) as db:
            assert isinstance(db, Collection)
