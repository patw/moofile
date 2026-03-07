"""Tests for the storage engine: encoding, scanning, compaction, recovery."""

import os
import struct

import pytest
import bson

from moofile.storage import (
    RECORD_LIVE,
    RECORD_TOMBSTONE,
    RECORD_REPLACEMENT,
    encode_record,
    scan_file,
    compact,
    StorageEngine,
)
from moofile.errors import ReadOnlyError


# ---------------------------------------------------------------------------
# encode_record / scan_file round-trip
# ---------------------------------------------------------------------------

class TestEncodeRoundTrip:
    def test_encode_produces_bytes(self):
        data = encode_record(RECORD_LIVE, {"_id": "x", "v": 1})
        assert isinstance(data, bytes)
        assert len(data) > 5

    def test_header_format(self):
        doc = {"_id": "abc", "name": "test"}
        payload = bson.encode(doc)
        data = encode_record(RECORD_LIVE, doc)
        length, rtype = struct.unpack("<IB", data[:5])
        assert length == len(payload)
        assert rtype == RECORD_LIVE

    def test_scan_returns_correct_records(self, tmp_path):
        path = str(tmp_path / "test.bson")
        docs = [{"_id": str(i), "v": i} for i in range(5)]
        with open(path, "wb") as f:
            for doc in docs:
                f.write(encode_record(RECORD_LIVE, doc))

        records, truncate_to = scan_file(path)
        assert truncate_to is None
        assert len(records) == 5
        for i, (offset, rtype, doc) in enumerate(records):
            assert rtype == RECORD_LIVE
            assert doc["v"] == i

    def test_scan_preserves_record_types(self, tmp_path):
        path = str(tmp_path / "types.bson")
        with open(path, "wb") as f:
            f.write(encode_record(RECORD_LIVE, {"_id": "a"}))
            f.write(encode_record(RECORD_REPLACEMENT, {"_id": "a", "v": 2}))
            f.write(encode_record(RECORD_TOMBSTONE, {"_id": "a"}))

        records, _ = scan_file(path)
        assert records[0][1] == RECORD_LIVE
        assert records[1][1] == RECORD_REPLACEMENT
        assert records[2][1] == RECORD_TOMBSTONE

    def test_scan_empty_file(self, tmp_path):
        path = str(tmp_path / "empty.bson")
        open(path, "wb").close()
        records, truncate_to = scan_file(path)
        assert records == []
        assert truncate_to is None


# ---------------------------------------------------------------------------
# Partial write recovery
# ---------------------------------------------------------------------------

class TestPartialWriteRecovery:
    def test_truncates_partial_trailing_header(self, tmp_path):
        path = str(tmp_path / "partial.bson")
        with open(path, "wb") as f:
            f.write(encode_record(RECORD_LIVE, {"_id": "good"}))
            f.write(b"\x00\x00")  # incomplete header

        records, truncate_to = scan_file(path)
        assert len(records) == 1
        assert truncate_to is not None

    def test_truncates_partial_payload(self, tmp_path):
        path = str(tmp_path / "partial2.bson")
        good = encode_record(RECORD_LIVE, {"_id": "good"})
        bad_payload = bson.encode({"_id": "bad", "v": 99})
        bad_header = struct.pack("<IB", len(bad_payload), RECORD_LIVE)
        with open(path, "wb") as f:
            f.write(good)
            f.write(bad_header)
            f.write(bad_payload[:5])  # truncated payload

        records, truncate_to = scan_file(path)
        assert len(records) == 1
        assert records[0][2]["_id"] == "good"
        assert truncate_to is not None

    def test_collection_auto_truncates_on_open(self, tmp_path):
        from moofile import Collection

        path = str(tmp_path / "auto.bson")
        # Write a good record followed by garbage
        with open(path, "wb") as f:
            f.write(encode_record(RECORD_LIVE, {"_id": "ok", "v": 1}))
            f.write(b"\xff\xff\xff\xff\x01")  # large "length" header with no payload

        with Collection(path) as db:
            assert db.count() == 1
            assert db.find_one({"_id": "ok"}) is not None


# ---------------------------------------------------------------------------
# Compact function
# ---------------------------------------------------------------------------

class TestCompact:
    def test_compact_writes_only_live_docs(self, tmp_path):
        path = str(tmp_path / "compact.bson")
        live_docs = [{"_id": str(i), "v": i} for i in range(3)]
        compact(path, live_docs)

        records, _ = scan_file(path)
        assert len(records) == 3
        assert all(r[1] == RECORD_LIVE for r in records)

    def test_compact_tmp_removed_on_success(self, tmp_path):
        path = str(tmp_path / "compact2.bson")
        compact(path, [{"_id": "x"}])
        assert not os.path.exists(path + ".tmp")

    def test_compact_original_untouched_on_failure(self, tmp_path, monkeypatch):
        path = str(tmp_path / "safe.bson")
        # Write an original file
        with open(path, "wb") as f:
            f.write(encode_record(RECORD_LIVE, {"_id": "orig"}))
        original_content = open(path, "rb").read()

        # Patch encode_record inside the storage module to raise mid-compact
        import moofile.storage as storage_mod

        def _exploding_encode(record_type, doc):
            raise RuntimeError("simulated mid-compact failure")

        monkeypatch.setattr(storage_mod, "encode_record", _exploding_encode)

        with pytest.raises(RuntimeError):
            compact(path, [{"_id": "orig"}])

        # Original file should be unchanged
        assert open(path, "rb").read() == original_content
        assert not os.path.exists(path + ".tmp")


# ---------------------------------------------------------------------------
# StorageEngine
# ---------------------------------------------------------------------------

class TestStorageEngine:
    def test_append_and_scan(self, tmp_path):
        path = str(tmp_path / "engine.bson")
        engine = StorageEngine(path)
        engine.append(RECORD_LIVE, {"_id": "e1", "v": 1})
        engine.append(RECORD_LIVE, {"_id": "e2", "v": 2})
        engine.close()

        records, _ = scan_file(path)
        assert len(records) == 2

    def test_readonly_raises_on_append(self, tmp_path):
        path = str(tmp_path / "ro.bson")
        open(path, "wb").close()
        engine = StorageEngine(path, readonly=True)
        with pytest.raises(ReadOnlyError):
            engine.append(RECORD_LIVE, {"_id": "x"})
        engine.close()

    def test_reopen_allows_continued_writes(self, tmp_path):
        path = str(tmp_path / "reopen.bson")
        engine = StorageEngine(path)
        engine.append(RECORD_LIVE, {"_id": "r1"})
        engine.reopen()
        engine.append(RECORD_LIVE, {"_id": "r2"})
        engine.close()

        records, _ = scan_file(path)
        assert len(records) == 2
