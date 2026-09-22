"""
Cross-implementation recovery parity.

Both backends must classify a damaged file the same way, byte for byte.  The
distinction that matters is *tail damage* (an interrupted write, which is
self-healed on open) versus *interior damage* (a clobbered record with intact
data after it, which must be reported rather than silently truncated away).

The zero-filled-tail case is the one that took TheWatcher down for nine days:
a filesystem with delayed allocation journalled the file's new size and then
lost the data blocks, so the tail read back as 1,814 zero bytes rather than as
a short file.  The scanner decoded the zeros as a record, failed, and `open`
never reached its own truncate-the-tail path — so every restart failed
identically, ~20,400 times.
"""

import os
import struct

import pytest

from moofile.errors import CorruptRecordError
from moofile.storage import RECORD_LIVE, encode_record


def _seed(make_collection, n=50, name="test.bson"):
    """Create a collection with n documents, closed, with no cache left."""
    db = make_collection(name, indexes=["v"])
    db.insert_many([{"_id": str(i), "v": i} for i in range(n)])
    path = db._path
    db.close()
    for suffix in (".cache",):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    return path


def _reopen(collection_impl, path, **kwargs):
    return collection_impl(path, indexes=["v"], **kwargs)


class TestZeroFilledTailSelfHeals:
    def test_open_recovers_and_trims(self, make_collection, collection_impl, backend):
        path = _seed(make_collection)
        good_len = os.path.getsize(path)

        with open(path, "ab") as f:
            f.write(b"\x00" * 1814)

        db = _reopen(collection_impl, path)
        assert db.count() == 50
        assert os.path.getsize(path) == good_len
        db.close()

    def test_handle_is_writable_after_recovery(self, make_collection, collection_impl, backend):
        # The append fd has to be pointing past the trimmed tail, or the next
        # write lands in the hole and the file is broken again.
        path = _seed(make_collection, n=10)
        with open(path, "ab") as f:
            f.write(b"\x00" * 1814)

        db = _reopen(collection_impl, path)
        db.insert({"_id": "new", "v": 99})
        db.close()

        db = _reopen(collection_impl, path)
        assert db.count() == 11
        assert db.find_one({"_id": "new"})["v"] == 99
        db.close()

    @pytest.mark.parametrize("zeros", [1, 4, 5, 64, 4096])
    def test_every_tail_length_behaves_the_same(self, make_collection, collection_impl, backend, zeros):
        path = _seed(make_collection, n=5, name=f"z{zeros}.bson")
        good_len = os.path.getsize(path)
        with open(path, "ab") as f:
            f.write(b"\x00" * zeros)

        db = _reopen(collection_impl, path)
        assert db.count() == 5
        assert os.path.getsize(path) == good_len
        db.close()

    def test_zero_run_starting_mid_record_also_heals(self, make_collection, collection_impl, backend):
        # The lost blocks need not start on a record boundary.  When they do
        # not, the final record's header survives and reads as plausible, so
        # the damage shows up as a payload that will not decode rather than as
        # an all-zero header.  Both are the same crash and both must heal.
        from moofile.storage import scan_file

        path = _seed(make_collection, n=10, name="midrecord.bson")
        records, _ = scan_file(path)
        last_offset = records[9][0]

        raw = bytearray(open(path, "rb").read())
        from_ = last_offset + 5 + 2
        raw[from_:] = b"\x00" * (len(raw) - from_)
        raw += b"\x00" * 1814
        with open(path, "wb") as f:
            f.write(bytes(raw))

        db = _reopen(collection_impl, path)
        assert db.count() == 9, "the nine intact records survive"
        assert os.path.getsize(path) == last_offset
        db.close()

    def test_truncated_tail_still_self_heals(self, make_collection, collection_impl, backend):
        # The pre-existing case: a physically short final record.
        path = _seed(make_collection, n=10)
        good_len = os.path.getsize(path)
        with open(path, "ab") as f:
            f.write(encode_record(RECORD_LIVE, {"_id": "partial", "v": 1})[:7])

        db = _reopen(collection_impl, path)
        assert db.count() == 10
        assert os.path.getsize(path) == good_len
        db.close()


def _clobber_interior(path, record_index=9):
    """Overwrite one record's bytes in place; returns (offset, length)."""
    from moofile.storage import scan_file

    records, _ = scan_file(path)
    start = records[record_index][0]
    end = records[record_index + 1][0]
    raw = bytearray(open(path, "rb").read())
    raw[start:end] = b"\xCD" * (end - start)
    with open(path, "wb") as f:
        f.write(bytes(raw))
    if os.path.exists(path + ".cache"):
        os.remove(path + ".cache")
    return start, end - start


class TestInteriorDamageIsReported:
    def test_open_raises_rather_than_truncating(self, make_collection, collection_impl, backend):
        path = _seed(make_collection, n=20)
        offset, _length = _clobber_interior(path)
        size_before = os.path.getsize(path)

        with pytest.raises(CorruptRecordError) as exc:
            _reopen(collection_impl, path)
        assert str(offset) in str(exc.value)
        assert os.path.getsize(path) == size_before, "must not silently truncate"

    def test_repair_salvages_everything_else(self, make_collection, collection_impl, backend):
        path = _seed(make_collection, n=20)
        offset, length = _clobber_interior(path)

        report = collection_impl.repair(path)

        assert report.rewritten
        assert report.is_damaged
        assert report.records_kept == 19
        assert report.bytes_dropped == length
        assert len(report.gaps) == 1
        assert report.gaps[0].offset == offset
        assert report.gaps[0].length == length
        assert report.gaps[0].to_end_of_file is False

        db = _reopen(collection_impl, path)
        assert db.count() == 19
        assert db.find_one({"_id": "9"}) is None
        assert db.find_one({"_id": "8"})["v"] == 8
        assert db.find_one({"_id": "10"})["v"] == 10
        db.close()

    def test_repair_on_open_recovers_in_one_step(self, make_collection, collection_impl, backend):
        path = _seed(make_collection, n=20)
        _clobber_interior(path)

        db = _reopen(collection_impl, path, repair=True)
        assert db.count() == 19
        db.close()

        # Repaired in place — a plain open works now.
        db = _reopen(collection_impl, path)
        assert db.count() == 19
        db.close()

    def test_repair_of_an_intact_file_changes_nothing(self, make_collection, collection_impl, backend):
        path = _seed(make_collection, n=10)
        before = open(path, "rb").read()

        report = collection_impl.repair(path)

        assert not report.rewritten
        assert not report.is_damaged
        assert report.records_kept == 10
        assert report.gaps == []
        assert open(path, "rb").read() == before

    def test_repair_of_a_missing_file_is_a_no_op(self, make_collection, collection_impl, backend, tmp_path):
        report = collection_impl.repair(str(tmp_path / "does_not_exist.bson"))
        assert not report.rewritten
        assert report.records_kept == 0
        assert report.gaps == []


class TestRepairReportShapeMatches:
    """The two backends return the same type, not merely similar numbers."""

    def test_report_fields(self, make_collection, collection_impl, backend):
        path = _seed(make_collection, n=6)
        with open(path, "ab") as f:
            f.write(b"\x00" * 100)

        report = collection_impl.repair(path)

        assert report.records_kept == 6
        assert report.bytes_dropped == 100
        assert report.bytes_kept == os.path.getsize(path)
        assert report.rewritten is True
        assert report.is_damaged is True
        assert len(report.gaps) == 1
        gap = report.gaps[0]
        assert gap.to_end_of_file is True
        assert gap.offset == report.bytes_kept
        assert gap.length == 100
