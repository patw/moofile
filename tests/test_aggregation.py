"""Tests for group/agg pipeline."""

import pytest
from moofile import Collection, count, sum, mean, min, max, collect, first, last


@pytest.fixture
def col(tmp_path):
    path = str(tmp_path / "agg.bson")
    with Collection(path) as db:
        db.insert_many([
            {"_id": "1", "city": "NYC",  "age": 30, "revenue": 100},
            {"_id": "2", "city": "NYC",  "age": 25, "revenue": 200},
            {"_id": "3", "city": "LA",   "age": 40, "revenue": 150},
            {"_id": "4", "city": "LA",   "age": 35, "revenue": 50},
            {"_id": "5", "city": "NYC",  "age": 20, "revenue": 300},
        ])
        yield db


class TestGroup:
    def test_group_count(self, col):
        result = col.find().group("city").agg(count()).to_list()
        by_city = {r["city"]: r["count"] for r in result}
        assert by_city["NYC"] == 3
        assert by_city["LA"] == 2

    def test_group_sum(self, col):
        result = col.find().group("city").agg(sum("revenue")).to_list()
        by_city = {r["city"]: r["sum_revenue"] for r in result}
        assert by_city["NYC"] == 600
        assert by_city["LA"] == 200

    def test_group_mean(self, col):
        result = col.find().group("city").agg(mean("age")).to_list()
        by_city = {r["city"]: r["mean_age"] for r in result}
        assert by_city["NYC"] == pytest.approx(25.0)
        assert by_city["LA"] == pytest.approx(37.5)

    def test_group_min(self, col):
        result = col.find().group("city").agg(min("age")).to_list()
        by_city = {r["city"]: r["min_age"] for r in result}
        assert by_city["NYC"] == 20
        assert by_city["LA"] == 35

    def test_group_max(self, col):
        result = col.find().group("city").agg(max("revenue")).to_list()
        by_city = {r["city"]: r["max_revenue"] for r in result}
        assert by_city["NYC"] == 300
        assert by_city["LA"] == 150

    def test_group_collect(self, col):
        result = col.find().group("city").agg(collect("age")).to_list()
        by_city = {r["city"]: sorted(r["collect_age"]) for r in result}
        assert by_city["NYC"] == [20, 25, 30]
        assert by_city["LA"] == [35, 40]

    def test_group_first(self, col):
        result = col.find().group("city").agg(first("age")).to_list()
        # first value encountered per group — just verify it's present
        for r in result:
            assert "first_age" in r
            assert isinstance(r["first_age"], int)

    def test_group_last(self, col):
        result = col.find().group("city").agg(last("revenue")).to_list()
        for r in result:
            assert "last_revenue" in r

    def test_multiple_agg_funcs(self, col):
        result = (
            col.find()
            .group("city")
            .agg(count(), sum("revenue"), mean("age"))
            .to_list()
        )
        for r in result:
            assert "count" in r
            assert "sum_revenue" in r
            assert "mean_age" in r

    def test_agg_with_sort(self, col):
        result = (
            col.find()
            .group("city")
            .agg(count())
            .sort("count", descending=True)
            .to_list()
        )
        counts = [r["count"] for r in result]
        assert counts == sorted(counts, reverse=True)

    def test_agg_with_limit(self, col):
        result = (
            col.find()
            .group("city")
            .agg(count())
            .sort("count", descending=True)
            .limit(1)
            .to_list()
        )
        assert len(result) == 1
        assert result[0]["city"] == "NYC"

    def test_group_with_filter(self, col):
        # Only NYC documents have age > 22
        result = (
            col.find({"age": {"$gt": 22}})
            .group("city")
            .agg(count())
            .to_list()
        )
        by_city = {r["city"]: r["count"] for r in result}
        assert by_city["NYC"] == 2   # ages 30 and 25
        assert by_city["LA"] == 2    # ages 40 and 35

    def test_group_preserves_key(self, col):
        result = col.find().group("city").agg(count()).to_list()
        for r in result:
            assert "city" in r
            assert r["city"] in ("NYC", "LA")

    def test_mean_with_missing_field(self, tmp_path):
        path = str(tmp_path / "mean_miss.bson")
        with Collection(path) as db:
            db.insert_many([
                {"_id": "x1", "grp": "a", "val": 10},
                {"_id": "x2", "grp": "a"},           # missing "val"
                {"_id": "x3", "grp": "a", "val": 20},
            ])
            result = db.find().group("grp").agg(mean("val")).first()
            # mean of [10, 20] = 15
            assert result["mean_val"] == pytest.approx(15.0)
