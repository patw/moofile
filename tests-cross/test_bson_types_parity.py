"""Both backends must encode the full BSON type set identically.

The native binding used to convert Python values to BSON by hand, supporting
only None/bool/int/float/str/list/dict and raising TypeError on anything else —
so datetimes and Binary could not be stored at all through the Rust backend,
while the pure-Python one handled them fine.

It now delegates unknown types to pymongo's encoder, the same library the
pure-Python implementation uses, which makes the two byte-identical by
construction.

The subtle part is that several BSON types *subclass* a Python builtin
(Code < str, Int64 < int, Binary < bytes, SON < dict).  An extract()-based
type check silently downgrades them, so the fast paths must test exact types.
"""

import struct
from datetime import datetime, timezone
from decimal import Decimal

import bson
import pytest
from bson import Binary, Code, Decimal128, Int64, ObjectId, Regex, Timestamp

# Types that subclass a Python builtin — the ones a naive type check downgrades.
SUBCLASS_TRAPS = {
    "code": Code("function(){return 1}"),   # str
    "int64": Int64(2 ** 40),                # int
    "binary": Binary(b"\x00\x01\x02\x03"),  # bytes
}

ALL_TYPES = {
    "dt_aware": datetime(2025, 1, 15, 12, 30, 0, tzinfo=timezone.utc),
    "dt_naive": datetime(2024, 6, 1, 8, 0, 0),
    "binary_sub": Binary(b"\xde\xad", 128),
    "raw_bytes": b"\x01\x02",
    "oid": ObjectId("507f1f77bcf86cd799439011"),
    "dec": Decimal128(Decimal("3.14159")),
    "ts": Timestamp(1234567890, 1),
    "regex": Regex("^ab.*", "i"),
    "nested": {"a": [1, {"b": datetime(2020, 1, 1, tzinfo=timezone.utc)}]},
    "flag": True,
    "count": 42,
    "big": 2 ** 40,
    "pi": 3.14,
    "label": "hello",
    "nil": None,
    "arr": [1, "two", 3.0],
    **SUBCLASS_TRAPS,
}


def _first_payload(path):
    """Return the BSON payload of the first record in the file."""
    data = open(path, "rb").read()
    length, _rtype = struct.unpack("<IB", data[:5])
    return data[5 : 5 + length]


def test_full_type_set_round_trips(make_collection, tmp_path):
    db = make_collection("types.bson")
    db.insert({"_id": "t", **ALL_TYPES})
    db.close()

    # Reading back from disk (not from the in-memory copy) is the real check.
    db = make_collection("types.bson")
    got = db.find_one({"_id": "t"})
    db.close()

    expected = bson.decode(bson.encode({"_id": "t", **ALL_TYPES}))
    assert got == expected


def test_file_matches_what_pymongo_would_write(make_collection, tmp_path):
    """The on-disk bytes must equal pymongo's own encoding — this is what
    keeps the two implementations byte-compatible."""
    db = make_collection("types.bson")
    doc = {"_id": "t", **ALL_TYPES}
    db.insert(dict(doc))
    db.close()

    assert _first_payload(str(tmp_path / "types.bson")) == bson.encode(doc)


@pytest.mark.parametrize("field,value", sorted(SUBCLASS_TRAPS.items()))
def test_builtin_subclasses_keep_their_type(make_collection, field, value):
    """Code/Int64/Binary subclass str/int/bytes and were being downgraded."""
    db = make_collection("traps.bson")
    db.insert({"_id": "t", field: value})
    db.close()

    db = make_collection("traps.bson")
    got = db.find_one({"_id": "t"})[field]
    db.close()

    expected = bson.decode(bson.encode({"v": value}))["v"]
    assert type(got) is type(expected)
    assert got == expected


def test_insert_return_value_is_not_stringified(make_collection):
    """insert() built its return dict natively and stringified unknown types."""
    db = make_collection()
    returned = db.insert({"_id": "t", "dt": ALL_TYPES["dt_aware"], "b": ALL_TYPES["binary"]})
    assert not isinstance(returned["dt"], str)
    assert not isinstance(returned["b"], str)
    db.close()


def test_datetime_survives_query_filters(make_collection):
    """Filters go through the same conversion as documents."""
    when = datetime(2025, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
    later = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = make_collection(indexes=["ts"])
    db.insert({"_id": "a", "ts": when})
    db.insert({"_id": "b", "ts": later})

    assert db.count({"ts": when}) == 1
    assert [d["_id"] for d in db.find({"ts": {"$lt": later}}).to_list()] == ["a"]
    db.close()


def test_unsupported_type_raises_the_same_error(make_collection):
    """pymongo refuses a bare uuid.UUID; both backends must refuse it too,
    rather than one storing something lossy."""
    import uuid

    db = make_collection()
    with pytest.raises(ValueError):
        db.insert({"_id": "u", "u": uuid.uuid4()})
    db.close()
