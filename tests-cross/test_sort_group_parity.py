"""Sort and group semantics, which the Rust core defines.

Both backends must agree with these exact answers.  Documents are inserted in
`_id` order so that ties (which keep scan order) come out the same under the
Python backend's insertion order and the Rust backend's `_id` order.
"""

import datetime

import pytest
from bson import ObjectId

from moofile import collect, count, first, last, max, mean, min, sum


MIXED = [
    ("a00", 3),
    ("a01", "b"),
    ("a02", None),
    ("a03", 2.5),
    ("a04", True),
    ("a05", [1, 2]),
    ("a06", {"x": 1}),
    ("a07", "B"),
    ("a08", datetime.datetime(2020, 1, 1)),
    ("a09", ObjectId("0123456789abcdef01234567")),
    ("a10", False),
    ("a11", b"\x01\x02"),
    ("a12", -7),
    ("a13", float("nan")),
]
# null/missing < numbers (NaN lowest) < strings < objects < arrays < binary
# < ObjectId < bool < date
ASC = ["a02", "a14", "a13", "a12", "a03", "a00", "a07", "a01", "a06", "a05",
       "a11", "a09", "a10", "a04", "a08"]


@pytest.fixture
def mixed(make_collection):
    db = make_collection()
    db.insert_many([{"_id": i, "v": v} for i, v in MIXED] + [{"_id": "a14"}])
    yield db
    db.close()


def test_sort_is_a_total_order_across_types(mixed):
    got = [d["_id"] for d in mixed.find().sort("v").to_list()]
    assert got == ASC


def test_descending_is_the_exact_reverse_keeping_ties_stable(mixed):
    got = [d["_id"] for d in mixed.find().sort("v", descending=True).to_list()]
    # a02 (null) and a14 (missing) tie, so they keep scan order.
    want = list(reversed(ASC))
    want[-2:] = ["a02", "a14"]
    assert got == want


def test_sort_then_skip_and_limit(mixed):
    got = [d["_id"] for d in mixed.find().sort("v").skip(3).limit(4).to_list()]
    assert got == ASC[3:7]


@pytest.fixture
def sales(make_collection):
    db = make_collection()
    db.insert_many([
        {"_id": "s0", "region": "east", "units": 3, "price": 1.5, "rep": "kim"},
        {"_id": "s1", "region": "west", "units": 4, "price": 2, "rep": "lee"},
        {"_id": "s2", "region": "east", "units": 5, "price": 2, "note": "x"},
        {"_id": "s3", "region": 1, "units": "n/a", "price": True},
        {"_id": "s4", "region": 1.0, "units": 2},
        {"_id": "s5", "region": True, "units": 1},
        {"_id": "s6", "units": 9},
    ])
    yield db
    db.close()


def test_group_semantics(sales):
    rows = (
        sales.find()
        .group("region")
        .agg(count(), sum("units"), sum("price"), mean("units"), min("units"),
             max("units"), collect("units"), first("rep"), last("rep"))
        .to_list()
    )
    by_region = {repr(r["region"]): r for r in rows}
    # 1 and 1.0 share a group (first-seen value kept); True does not.
    assert [repr(r["region"]) for r in rows] == ["'east'", "'west'", "1", "True", "None"]

    east = by_region["'east'"]
    assert east["count"] == 2
    assert east["sum_units"] == 8 and isinstance(east["sum_units"], int)
    assert east["sum_price"] == 3.5
    assert east["mean_units"] == 4.0
    assert (east["min_units"], east["max_units"]) == (3, 5)
    assert east["collect_units"] == [3, 5]
    # first/last read the group's first/last document, missing => None.
    assert (east["first_rep"], east["last_rep"]) == ("kim", None)

    ones = by_region["1"]
    assert ones["count"] == 2
    # Non-numbers (and bools) are skipped by sum/mean, not an error.
    assert ones["sum_units"] == 2 and ones["sum_price"] == 0
    assert ones["mean_units"] == 2.0
    # min/max use the sort order across types: numbers < strings.
    assert (ones["min_units"], ones["max_units"]) == (2, "n/a")

    assert by_region["None"]["sum_units"] == 9


def test_group_then_sort_skip_limit(sales):
    rows = (
        sales.find({"units": {"$gte": 1}})
        .group("region")
        .agg(sum("units"))
        .sort("sum_units", descending=True)
        .skip(1)
        .limit(2)
        .to_list()
    )
    assert [(r["region"], r["sum_units"]) for r in rows] == [("east", 8), ("west", 4)]


def test_hand_built_aggfunc_still_works(sales):
    """A custom AggFunc has no `kind`, so the native backend groups in
    Python -- with the same grouping and ordering as the core."""
    from moofile.aggregation import AggFunc

    reps = AggFunc("reps", "rep", lambda docs: sorted(d["rep"] for d in docs if "rep" in d))
    rows = sales.find().group("region").agg(reps).sort("region").to_list()
    assert [(repr(r["region"]), r["reps"]) for r in rows] == [
        ("None", []), ("1", []), ("'east'", ["kim"]), ("'west'", ["lee"]), ("True", []),
    ]
