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


# ---------------------------------------------------------------------------
# Zero-filled tails and repair
#
# A filesystem with delayed allocation can journal an inode's new size and
# then lose the data blocks, so an interrupted write leaves a file that is
# full length and reads back as zeros — not a short file.  That damage took a
# service down for nine days: the scanner decoded the zeros as a record,
# failed, and open() never reached its own truncate-the-tail path, so every
# restart failed identically forever.
# ---------------------------------------------------------------------------

from moofile.storage import RepairGap, RepairReport, repair_file  # noqa: E402
from moofile.errors import CorruptRecordError  # noqa: E402


def _write_records(path, n, start=0):
    """Write n records and return the file's length."""
    with open(path, "wb") as f:
        for i in range(start, start + n):
            f.write(encode_record(RECORD_LIVE, {"_id": str(i), "v": i}))
    return os.path.getsize(path)


class TestZeroFilledTail:
    def test_zero_tail_is_a_truncation_point(self, tmp_path):
        path = str(tmp_path / "zero_tail.bson")
        good_len = _write_records(path, 3)
        with open(path, "ab") as f:
            f.write(b"\x00" * 1814)

        records, truncate_to = scan_file(path)
        assert len(records) == 3
        assert truncate_to == good_len

    @pytest.mark.parametrize("zeros", [1, 2, 3, 4, 5, 6, 7, 8])
    def test_any_zero_tail_length_truncates(self, tmp_path, zeros):
        # 1..4 zero bytes hit EOF inside the header read; 5+ produce a
        # complete all-zero header.  Both must land on the same answer.
        path = str(tmp_path / "short_zero_tail.bson")
        good_len = _write_records(path, 2)
        with open(path, "ab") as f:
            f.write(b"\x00" * zeros)

        records, truncate_to = scan_file(path)
        assert len(records) == 2
        assert truncate_to == good_len

    def test_zero_run_starting_mid_record_is_also_a_tail(self, tmp_path):
        # The lost blocks need not start on a record boundary.  When they do
        # not, the final record's header survives and reads as plausible, so
        # the damage shows up as a payload that will not decode rather than as
        # an all-zero header.  Both are the same crash and both must heal.
        path = str(tmp_path / "mid_record_zeros.bson")
        _write_records(path, 4)
        records, _ = scan_file(path)
        last_offset = records[3][0]

        raw = bytearray(open(path, "rb").read())
        from_ = last_offset + 5 + 2      # keep the header, zero the payload
        raw[from_:] = b"\x00" * (len(raw) - from_)
        raw += b"\x00" * 1814
        open(path, "wb").write(bytes(raw))

        records, truncate_to = scan_file(path)
        assert len(records) == 3
        assert truncate_to == last_offset

    def test_all_zero_file_truncates_to_zero(self, tmp_path):
        path = str(tmp_path / "all_zero.bson")
        with open(path, "wb") as f:
            f.write(b"\x00" * 4096)

        records, truncate_to = scan_file(path)
        assert records == []
        assert truncate_to == 0

    def test_zero_hole_with_records_after_it_is_corruption(self, tmp_path):
        # Not a tail: truncating here would silently discard what follows.
        path = str(tmp_path / "zero_hole.bson")
        good_len = _write_records(path, 1)
        with open(path, "ab") as f:
            f.write(b"\x00" * 4096)
            for i in range(1, 5):
                f.write(encode_record(RECORD_LIVE, {"_id": str(i), "v": i}))

        with pytest.raises(CorruptRecordError) as exc:
            scan_file(path)
        assert str(good_len) in str(exc.value)

    def test_implausible_length_at_the_tail_truncates(self, tmp_path):
        path = str(tmp_path / "huge_tail.bson")
        good_len = _write_records(path, 1)
        with open(path, "ab") as f:
            f.write(b"\xCD" * 64)

        records, truncate_to = scan_file(path)
        assert len(records) == 1
        assert truncate_to == good_len

    def test_implausible_length_with_records_after_it_is_corruption(self, tmp_path):
        path = str(tmp_path / "huge_interior.bson")
        good_len = _write_records(path, 1)
        with open(path, "ab") as f:
            f.write(b"\xCD" * 64)
            for i in range(1, 5):
                f.write(encode_record(RECORD_LIVE, {"_id": str(i), "v": i}))

        with pytest.raises(CorruptRecordError):
            scan_file(path)

    def test_undecodable_payload_raises_corrupt_record(self, tmp_path):
        path = str(tmp_path / "bad_payload.bson")
        good_len = _write_records(path, 1)
        with open(path, "ab") as f:
            f.write(struct.pack("<IB", 64, RECORD_LIVE) + b"\xAB" * 64)
            f.write(encode_record(RECORD_LIVE, {"_id": "later"}))

        with pytest.raises(CorruptRecordError) as exc:
            scan_file(path)
        assert str(good_len) in str(exc.value)


class TestRepairFile:
    def test_drops_a_zero_tail_and_keeps_every_record(self, tmp_path):
        path = str(tmp_path / "repair_tail.bson")
        good_len = _write_records(path, 5)
        with open(path, "ab") as f:
            f.write(b"\x00" * 1814)

        report = repair_file(path)
        assert report.rewritten
        assert report.is_damaged
        assert report.records_kept == 5
        assert report.bytes_kept == good_len
        assert report.bytes_dropped == 1814
        assert report.gaps == [RepairGap(good_len, 1814, True)]

        records, truncate_to = scan_file(path)
        assert len(records) == 5
        assert truncate_to is None

    def test_resyncs_past_mid_file_damage(self, tmp_path):
        path = str(tmp_path / "repair_hole.bson")
        _write_records(path, 20)
        records, _ = scan_file(path)
        start, end = records[9][0], records[10][0]

        raw = bytearray(open(path, "rb").read())
        raw[start:end] = b"\xCD" * (end - start)
        open(path, "wb").write(bytes(raw))

        report = repair_file(path)
        assert report.records_kept == 19
        assert report.gaps == [RepairGap(start, end - start, False)]

        records, truncate_to = scan_file(path)
        assert truncate_to is None
        ids = [doc["_id"] for _o, _t, doc in records]
        assert "9" not in ids
        assert "8" in ids and "10" in ids

    def test_preserves_record_types_and_order(self, tmp_path):
        path = str(tmp_path / "repair_types.bson")
        with open(path, "wb") as f:
            f.write(encode_record(RECORD_LIVE, {"_id": "a", "v": 1}))
            f.write(encode_record(RECORD_LIVE, {"_id": "b", "v": 2}))
            f.write(encode_record(RECORD_REPLACEMENT, {"_id": "a", "v": 3}))
            f.write(encode_record(RECORD_TOMBSTONE, {"_id": "b"}))
            f.write(b"\x00" * 512)

        report = repair_file(path)
        assert report.records_kept == 4

        records, _ = scan_file(path)
        assert [t for _o, t, _d in records] == [
            RECORD_LIVE, RECORD_LIVE, RECORD_REPLACEMENT, RECORD_TOMBSTONE,
        ]

    def test_leaves_an_intact_file_untouched(self, tmp_path):
        path = str(tmp_path / "repair_clean.bson")
        _write_records(path, 4)
        before = open(path, "rb").read()
        mtime_before = os.stat(path).st_mtime_ns

        report = repair_file(path)
        assert not report.rewritten
        assert not report.is_damaged
        assert report.records_kept == 4
        assert open(path, "rb").read() == before
        assert os.stat(path).st_mtime_ns == mtime_before
        assert not os.path.exists(path + ".repair-tmp")

    def test_empty_file_is_a_no_op(self, tmp_path):
        path = str(tmp_path / "repair_empty.bson")
        open(path, "wb").close()
        report = repair_file(path)
        assert not report.rewritten
        assert report.records_kept == 0

    def test_missing_file_is_a_no_op(self, tmp_path):
        report = repair_file(str(tmp_path / "nope.bson"))
        assert isinstance(report, RepairReport)
        assert not report.rewritten

    def test_truncates_when_nothing_intact_follows(self, tmp_path):
        path = str(tmp_path / "repair_lost_tail.bson")
        kept = _write_records(path, 1)
        with open(path, "ab") as f:
            f.write(b"\x7F" * 4096)

        report = repair_file(path)
        assert report.records_kept == 1
        assert report.bytes_dropped == 4096
        assert report.gaps[0].to_end_of_file
        assert os.path.getsize(path) == kept


# ---------------------------------------------------------------------------
# Write-side size caps
#
# The reader refuses a record over MAX_DOCUMENT_SIZE, because from its side an
# over-cap length is indistinguishable from a corrupt one.  The writer has to
# refuse the same thing, or a caller can append a document that every later
# open rejects — a file broken by its own writer.
#
# MAX_BINARY_SIZE is a second, lower cap that belongs to the BSON layer: the
# Rust encoder will not write a binary value over 16 MiB.  pymongo will, so
# without checking it here this backend writes documents the other one cannot
# encode — and one that cannot be encoded is dropped from the index, so the
# record sits on disk and reads back as nothing at all.
# ---------------------------------------------------------------------------

from moofile.storage import (  # noqa: E402
    MAX_BINARY_SIZE,
    MAX_DOCUMENT_SIZE,
    encode_doc,
)
from moofile.errors import (  # noqa: E402
    BinaryFieldTooLargeError,
    DocumentTooLargeError,
)


class TestWriteSizeCaps:
    def test_encode_doc_accepts_up_to_the_document_cap(self):
        # A string, not a binary — binaries hit the lower cap first.
        doc = {"_id": "a", "blob": "x" * (MAX_DOCUMENT_SIZE // 2)}
        assert len(encode_doc(doc)) <= MAX_DOCUMENT_SIZE

    def test_encode_doc_rejects_over_the_document_cap(self):
        with pytest.raises(DocumentTooLargeError) as exc:
            encode_doc({"_id": "a", "blob": "x" * MAX_DOCUMENT_SIZE})
        assert str(MAX_DOCUMENT_SIZE) in str(exc.value)

    def test_encode_doc_accepts_a_binary_at_the_cap(self):
        doc = {"_id": "a", "blob": bson.Binary(b"\x07" * MAX_BINARY_SIZE)}
        assert len(encode_doc(doc)) > MAX_BINARY_SIZE

    def test_encode_doc_rejects_a_binary_over_the_cap(self):
        with pytest.raises(BinaryFieldTooLargeError) as exc:
            encode_doc({"_id": "a", "blob": bson.Binary(b"\x00" * (MAX_BINARY_SIZE + 1))})
        assert "'blob'" in str(exc.value)

    def test_oversized_binary_is_found_however_deeply_nested(self):
        big = bson.Binary(b"\x00" * (MAX_BINARY_SIZE + 1))

        with pytest.raises(BinaryFieldTooLargeError) as exc:
            encode_doc({"_id": "a", "outer": {"inner": big}})
        assert "'outer.inner'" in str(exc.value)

        with pytest.raises(BinaryFieldTooLargeError) as exc:
            encode_doc({"_id": "a", "items": ["ok", big]})
        assert "'items.1'" in str(exc.value)

    def test_append_rejects_without_writing(self, tmp_path):
        path = str(tmp_path / "oversize.bson")
        engine = StorageEngine(path)
        engine.append(RECORD_LIVE, {"_id": "small"})
        engine.close()
        good_len = os.path.getsize(path)

        engine = StorageEngine(path)
        with pytest.raises(DocumentTooLargeError):
            engine.append(RECORD_LIVE, {"_id": "big", "blob": "x" * MAX_DOCUMENT_SIZE})
        engine.close()

        assert os.path.getsize(path) == good_len
        records, truncate_to = scan_file(path)
        assert len(records) == 1
        assert truncate_to is None

    def test_append_batch_rejects_the_whole_batch(self, tmp_path):
        path = str(tmp_path / "oversize_batch.bson")
        engine = StorageEngine(path)
        with pytest.raises(DocumentTooLargeError):
            engine.append_batch([
                (RECORD_LIVE, {"_id": "a", "v": 1}),
                (RECORD_LIVE, {"_id": "b", "blob": "x" * MAX_DOCUMENT_SIZE}),
                (RECORD_LIVE, {"_id": "c", "v": 3}),
            ])
        engine.close()

        assert os.path.getsize(path) == 0, (
            "the small documents before the oversized one must not be written either"
        )
