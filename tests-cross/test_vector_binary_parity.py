"""Vector indexes accept BSON Binary subtype 9 as well as number arrays.

Subtype 9 is what autoembedding stores (4 bytes per dimension for FLOAT32,
1 for INT8, 1 bit for PACKED_BIT, against 13+ for an array of doubles); both
backends must index it identically.
"""

import pytest
from bson.binary import Binary, BinaryVectorDtype


def _search(db, q, limit=10):
    return [(d["_id"], round(s, 5)) for d, s in db.find().vector_search("v", q, limit).to_list()]


def test_float32_and_int8_vectors_rank_like_arrays(make_collection):
    rows = {"a": [1.0, 0.0, 0.0], "b": [0.6, 0.8, 0.0], "c": [0.0, 0.0, 1.0], "d": [-1.0, 0.5, 0.0]}
    arrays = make_collection("arrays.bson", vector_indexes={"v": 3})
    packed = make_collection("packed.bson", vector_indexes={"v": 3})
    for i, (k, v) in enumerate(rows.items()):
        arrays.insert({"_id": k, "v": v})
        dtype = BinaryVectorDtype.FLOAT32 if i % 2 else BinaryVectorDtype.INT8
        stored = v if dtype == BinaryVectorDtype.FLOAT32 else [int(x * 100) for x in v]
        packed.insert({"_id": k, "v": Binary.from_vector(stored, dtype)})
    q = [0.9, 0.1, 0.0]
    assert [i for i, _ in _search(packed, q)] == [i for i, _ in _search(arrays, q)]
    for (_, a), (_, b) in zip(_search(packed, q), _search(arrays, q)):
        assert a == pytest.approx(b, abs=1e-2)
    arrays.close()
    packed.close()


def test_packed_bits_are_plus_minus_one(make_collection):
    db = make_collection(vector_indexes={"v": 10})
    bits = [1, 0, 1, 1, 0, 0, 0, 0, 1, 0]
    db.insert({"_id": "x", "v": Binary.from_vector(_pack(bits), BinaryVectorDtype.PACKED_BIT, padding=6)})
    q = [1.0 if b else -1.0 for b in bits]
    [(doc_id, score)] = _search(db, q)
    assert doc_id == "x" and score == pytest.approx(1.0)
    db.close()


def test_wrong_width_and_malformed_vectors_are_not_indexed(make_collection):
    db = make_collection(vector_indexes={"v": 3})
    db.insert({"_id": "ok", "v": Binary.from_vector([1.0, 0.0, 0.0], BinaryVectorDtype.FLOAT32)})
    db.insert({"_id": "short", "v": Binary.from_vector([1.0, 0.0], BinaryVectorDtype.FLOAT32)})
    db.insert({"_id": "junk", "v": Binary(b"\x99\x00abc", 9)})
    db.insert({"_id": "strs", "v": [1.0, "0", 0.0]})
    db.insert({"_id": "bools", "v": [True, False, False]})
    assert [i for i, _ in _search(db, [1.0, 0.0, 0.0])] == ["ok"]
    db.close()


def test_survives_reopen(make_collection):
    db = make_collection(vector_indexes={"v": 3})
    db.insert({"_id": "a", "v": Binary.from_vector([0.0, 1.0, 0.0], BinaryVectorDtype.FLOAT32)})
    db.close()
    db = make_collection(vector_indexes={"v": 3})
    assert [i for i, _ in _search(db, [0.0, 1.0, 0.0])] == ["a"]
    v = db.find_one({"_id": "a"})["v"]
    assert isinstance(v, Binary) and v.subtype == 9
    assert v.as_vector().data == [0.0, 1.0, 0.0]
    db.close()


def _pack(bits):
    out = []
    for i in range(0, len(bits), 8):
        chunk = bits[i:i + 8] + [0] * (8 - len(bits[i:i + 8]))
        out.append(int("".join(map(str, chunk)), 2))
    return out
