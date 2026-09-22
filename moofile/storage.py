"""Append-only BSON file storage engine."""

import os
import struct

import bson

# Record type constants
RECORD_LIVE = 0x01         # live document
RECORD_TOMBSTONE = 0x02    # delete marker
RECORD_REPLACEMENT = 0x03  # update marker (new document version)

# File record header: [4 bytes: payload length (uint32 LE)] [1 byte: record type]
_HEADER_FMT = "<IB"
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)  # 5 bytes

#: Smallest possible BSON document: a 4-byte length prefix and the 0x00
#: terminator.  A record header claiming less than this cannot describe a
#: document, whatever else is going on.
_MIN_BSON_DOC = 5

#: Cap on a record's payload — a header claiming more than this is corrupt
#: rather than merely large, so the scanner refuses to honour it.  It is
#: therefore also the largest document that can be *written*: see _check_size.
MAX_DOCUMENT_SIZE = 100 * 1024 * 1024

#: Internal alias for the scan paths, which read as "payload" not "document".
_MAX_PAYLOAD = MAX_DOCUMENT_SIZE

#: Largest BSON binary value the Rust encoder will produce.  This is the bson
#: crate's cap, not MooFile's, and it is *lower* than MAX_DOCUMENT_SIZE —
#: pymongo has no equivalent, so without checking it here this backend would
#: write documents the other one cannot encode.
MAX_BINARY_SIZE = 16 * 1024 * 1024

#: How far past damage to look for the next intact record before concluding
#: the file simply ends there.  Damage from a filesystem losing blocks spans
#: an extent or two; a megabyte is far beyond that while keeping the search
#: bounded in both time and memory.
_RESYNC_WINDOW = 1024 * 1024

_RECORD_TYPES = (RECORD_LIVE, RECORD_TOMBSTONE, RECORD_REPLACEMENT)


def encode_record(record_type: int, doc: dict) -> bytes:
    """Encode a document into a file record."""
    payload = bson.encode(doc)
    header = struct.pack(_HEADER_FMT, len(payload), record_type)
    return header + payload


def scan_file(path: str) -> tuple:
    """
    Scan a BSON file from start to finish.

    Returns:
        (records, truncate_to) where records is a list of
        (offset, record_type, doc) tuples.  truncate_to is the byte
        offset of any partial trailing write (None if file is intact).
    """
    return scan_from(path, 0)


def scan_from(path: str, start: int = 0) -> tuple:
    """
    Scan a BSON file starting at byte offset *start*.

    The format is append-only, so records written by another process after
    this handle last read are always a contiguous suffix.  That lets a handle
    catch up on someone else's writes in O(new bytes) rather than re-reading
    and re-indexing the whole file.

    *start* must be a record boundary — pass an offset previously scanned to.

    Returns the same (records, truncate_to) pair as scan_file.
    """
    from .errors import CorruptRecordError

    records = []
    truncate_to = None

    with open(path, "rb") as f:
        file_len = os.fstat(f.fileno()).st_size
        if start:
            f.seek(start)
        while True:
            offset = f.tell()
            header_bytes = f.read(_HEADER_SIZE)
            if not header_bytes:
                break  # clean EOF
            if len(header_bytes) < _HEADER_SIZE:
                truncate_to = offset
                break
            length, record_type = struct.unpack(_HEADER_FMT, header_bytes)

            # Two ways a header can be unusable on its face: it claims a
            # payload too short to be a BSON document (in practice an all-zero
            # header, which is what an interrupted write leaves behind on a
            # filesystem with delayed allocation — the inode's new size reaches
            # the journal but the data blocks never reach the disk, so they
            # read back as zeros rather than as a short file), or it claims one
            # larger than any real record.
            #
            # Either is a truncation point if it is the file's tail, and
            # genuine corruption if intact records follow — see _damage_is_tail.
            if length < _MIN_BSON_DOC or length > _MAX_PAYLOAD:
                if _damage_is_tail(f, offset, file_len):
                    truncate_to = offset
                    break
                raise CorruptRecordError(
                    f"corrupt record at byte {offset}: "
                    f"implausible payload length {length} bytes"
                )

            payload = f.read(length)
            if len(payload) < length:
                truncate_to = offset
                break
            # A payload that will not decode gets the same tail-or-corruption
            # question as a bad header.  It has to: an interrupted write whose
            # lost blocks start partway through a record leaves a plausible
            # header over half-real, half-zero bytes, which lands here rather
            # than in the check above.  Only the record boundary the zeros
            # happened to fall on decides which of the two paths a given crash
            # takes, and both have to self-heal.
            try:
                doc = bson.decode(payload)
            except Exception as e:
                if _damage_is_tail(f, offset, file_len):
                    truncate_to = offset
                    break
                raise CorruptRecordError(
                    f"corrupt record at byte {offset}: BSON decode failed: {e}"
                ) from e
            records.append((offset, record_type, doc))

    return records, truncate_to


def _validate_record_at(buf: bytes, pos: int):
    """
    Parse a record at *pos* in *buf*, returning its total on-disk size, or
    None if there isn't a valid one there.

    Every check a genuine record must pass: a plausible length, a known record
    type, a payload that fits, a payload that decodes as BSON, and a string
    _id (a collection-wide invariant — the loader skips records without one).
    Together these make a false positive during resynchronisation unlikely
    enough to be worth the recovery.
    """
    header = buf[pos:pos + _HEADER_SIZE]
    if len(header) < _HEADER_SIZE:
        return None
    length, record_type = struct.unpack(_HEADER_FMT, header)
    if length < _MIN_BSON_DOC or length > _MAX_PAYLOAD:
        return None
    if record_type not in _RECORD_TYPES:
        return None
    payload = buf[pos + _HEADER_SIZE:pos + _HEADER_SIZE + length]
    if len(payload) < length:
        return None
    try:
        doc = bson.decode(payload)
    except Exception:
        return None
    if not isinstance(doc.get("_id"), str):
        return None
    return _HEADER_SIZE + length


def _resync(buf: bytes, from_pos: int, window_is_eof: bool):
    """
    Find the next record boundary at or after *from_pos* in *buf*.

    A single record that happens to decode is not enough — arbitrary bytes can
    occasionally satisfy _validate_record_at.  A candidate is accepted only if
    the record that follows it also validates, or if it ends exactly at the end
    of the file (*window_is_eof*), which is the same evidence one record later.
    """
    for pos in range(from_pos, len(buf)):
        size = _validate_record_at(buf, pos)
        if size is None:
            continue
        nxt = pos + size
        if nxt == len(buf):
            if window_is_eof:
                return pos
            continue
        if _validate_record_at(buf, nxt) is not None:
            return pos
    return None


def _damage_is_tail(f, offset: int, file_len: int) -> bool:
    """
    Decide whether unparseable bytes at *offset* are the file's tail or a
    damaged record with intact data after it.

    "Unparseable therefore tail" is not safe to assume: a single clobbered
    block mid-file would then silently truncate everything after it on the next
    open, and the append-only format carries no framing to resynchronise on.
    Looking ahead for an intact record is how the two are told apart — if one
    is found the damage is interior and must be reported (repair() can then
    salvage the rest); if none is found the file really does end here.

    Only ever called once a scan has already failed, so the window read costs
    nothing on the healthy path.
    """
    window_len = min(file_len - offset, _RESYNC_WINDOW)
    window_is_eof = (offset + window_len) == file_len
    f.seek(offset)
    window = f.read(window_len)
    return _resync(window, 1, window_is_eof) is None


class RepairGap:
    """A span of bytes that :func:`repair_file` could not parse and dropped."""

    __slots__ = ("offset", "length", "to_end_of_file")

    def __init__(self, offset: int, length: int, to_end_of_file: bool) -> None:
        #: Byte offset where the damage starts.
        self.offset = offset
        #: Number of bytes dropped.
        self.length = length
        #: True if the damage ran to the end of the file — i.e. this was a
        #: truncation rather than a skipped-over hole.
        self.to_end_of_file = to_end_of_file

    def __repr__(self) -> str:
        return (
            f"RepairGap(offset={self.offset}, length={self.length}, "
            f"to_end_of_file={self.to_end_of_file})"
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, RepairGap):
            return NotImplemented
        return (
            self.offset == other.offset
            and self.length == other.length
            and self.to_end_of_file == other.to_end_of_file
        )


class RepairReport:
    """What a :func:`repair_file` pass did."""

    __slots__ = ("records_kept", "bytes_kept", "bytes_dropped", "gaps", "rewritten")

    def __init__(
        self,
        records_kept: int = 0,
        bytes_kept: int = 0,
        bytes_dropped: int = 0,
        gaps=None,
        rewritten: bool = False,
    ) -> None:
        #: Records that decoded and were preserved.
        self.records_kept = records_kept
        #: Bytes of intact records preserved.
        self.bytes_kept = bytes_kept
        #: Bytes of unparseable data dropped.
        self.bytes_dropped = bytes_dropped
        #: Every damaged span, in file order.
        self.gaps = list(gaps or [])
        #: False when the file was already intact and was left untouched.
        self.rewritten = rewritten

    @property
    def is_damaged(self) -> bool:
        """Whether any damage was found."""
        return bool(self.gaps)

    def __repr__(self) -> str:
        return (
            f"RepairReport(records_kept={self.records_kept}, "
            f"bytes_kept={self.bytes_kept}, bytes_dropped={self.bytes_dropped}, "
            f"gaps={self.gaps!r}, rewritten={self.rewritten})"
        )


def repair_file(path: str) -> RepairReport:
    """
    Salvage a damaged BSON file in place.

    Walks the file, keeps every record that decodes, and drops the byte spans
    that do not — resynchronising past a damaged span where an intact record
    can be found after it, and truncating where one cannot.  Surviving records
    are copied verbatim, so the repaired file replays to exactly the state the
    intact part of the log describes: inserts, replacements and tombstones all
    keep their order and meaning.

    The rewrite goes to a .tmp file which is fsynced and renamed over the
    original, so an interrupted repair leaves the damaged file untouched rather
    than adding a second kind of damage to it.  A file with nothing wrong is
    not rewritten at all.
    """
    report = RepairReport()

    if not os.path.exists(path):
        return report

    tmp_path = path + ".repair-tmp"

    with open(path, "rb") as f:
        file_len = os.fstat(f.fileno()).st_size
        if file_len == 0:
            return report

        try:
            with open(tmp_path, "wb") as out:
                offset = 0
                while offset < file_len:
                    f.seek(offset)
                    header_bytes = f.read(_HEADER_SIZE)
                    intact = None

                    if len(header_bytes) == _HEADER_SIZE:
                        length, record_type = struct.unpack(_HEADER_FMT, header_bytes)
                        if (
                            _MIN_BSON_DOC <= length <= _MAX_PAYLOAD
                            and offset + _HEADER_SIZE + length <= file_len
                        ):
                            payload = f.read(length)
                            if len(payload) == length:
                                try:
                                    bson.decode(payload)
                                    intact = (record_type, payload)
                                except Exception:
                                    intact = None

                    if intact is not None:
                        record_type, payload = intact
                        size = _HEADER_SIZE + len(payload)
                        out.write(struct.pack(_HEADER_FMT, len(payload), record_type))
                        out.write(payload)
                        report.records_kept += 1
                        report.bytes_kept += size
                        offset += size
                        continue

                    # Damaged at `offset`.  Look ahead for the next intact record.
                    window_len = min(file_len - offset, _RESYNC_WINDOW)
                    window_is_eof = (offset + window_len) == file_len
                    f.seek(offset)
                    window = f.read(window_len)

                    rel = _resync(window, 1, window_is_eof)
                    if rel is None:
                        lost = file_len - offset
                        report.gaps.append(RepairGap(offset, lost, True))
                        report.bytes_dropped += lost
                        break
                    report.gaps.append(RepairGap(offset, rel, False))
                    report.bytes_dropped += rel
                    offset += rel

                out.flush()
                os.fsync(out.fileno())
        except Exception:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise

    # Nothing was wrong — leave the original alone rather than churning it.
    if not report.gaps:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        return report

    os.replace(tmp_path, path)
    # fsync parent directory so the rename is durable, as compact() does.
    dir_fd = os.open(os.path.dirname(path) or ".", os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)

    report.rewritten = True
    return report


def compact(path: str, live_docs: list) -> None:
    """
    Rewrite the BSON file keeping only live documents.
    Writes to a .tmp file first, then atomically renames so the original
    is untouched if the operation is interrupted.
    """
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "wb") as f:
            for doc in live_docs:
                f.write(encode_record(RECORD_LIVE, doc))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        # fsync parent directory so the rename is durable across power loss
        dir_fd = os.open(os.path.dirname(path) or ".", os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise


def _check_size(payload: bytes) -> None:
    """Reject a document the scanner could never read back.

    The reader caps a record's payload at MAX_DOCUMENT_SIZE so that a corrupt
    length field cannot trigger a wild allocation.  Nothing used to enforce the
    same cap on the way in, so a caller could append a document larger than
    that and produce a file every later open refuses — and refuses correctly,
    because from the reader's side an over-cap length is indistinguishable from
    corruption.  A rejected write is the better half of that trade.
    """
    if len(payload) > MAX_DOCUMENT_SIZE:
        from .errors import DocumentTooLargeError
        raise DocumentTooLargeError(
            f"document is {len(payload)} bytes, over the {MAX_DOCUMENT_SIZE}-byte "
            "limit — a record this large cannot be read back, so it is rejected "
            "rather than written to a file that would then fail to open"
        )


def _check_binary_fields(doc, payload_len: int) -> None:
    """Reject a document carrying a binary value the Rust encoder will not write.

    pymongo will happily encode a 20 MiB Binary; the bson crate will not.  A
    document that crosses that line is written by this backend, read back by
    it, and then silently *missing* under the Rust backend — which decodes the
    record, fails to re-encode it for the raw-document index, and drops it.

    Only runs when the encoded document is itself over the binary cap, since
    no binary inside a smaller document can exceed it.  So this costs nothing
    on the ordinary write path.
    """
    if payload_len <= MAX_BINARY_SIZE:
        return

    def walk(path, value):
        if isinstance(value, bytes) and len(value) > MAX_BINARY_SIZE:
            from .errors import BinaryFieldTooLargeError
            raise BinaryFieldTooLargeError(
                f"binary field '{path}' is {len(value)} bytes, over the "
                f"{MAX_BINARY_SIZE}-byte limit for a BSON binary value — a "
                "document containing it cannot be encoded, and would be "
                "silently absent from the collection"
            )
        if isinstance(value, dict):
            for k, v in value.items():
                walk(f"{path}.{k}", v)
        elif isinstance(value, (list, tuple)):
            for i, v in enumerate(value):
                walk(f"{path}.{i}", v)

    for k, v in doc.items():
        walk(k, v)


def encode_doc(doc: dict) -> bytes:
    """Encode a document for writing, rejecting what could not be read back."""
    payload = bson.encode(doc)
    _check_binary_fields(doc, len(payload))
    _check_size(payload)
    return payload


class StorageEngine:
    """Handles append-only writes to the BSON file."""

    def __init__(self, path: str, readonly: bool = False, durability: str = "os") -> None:
        self.path = path
        self.readonly = readonly
        self.durability = durability
        self._file = None
        self._open_file()

    def _open_file(self) -> None:
        if self.readonly:
            self._file = open(self.path, "rb")
        else:
            self._file = open(self.path, "ab")

    def append(self, record_type: int, doc: dict) -> None:
        from .errors import ReadOnlyError
        if self.readonly:
            raise ReadOnlyError("Collection is open in read-only mode")
        payload = encode_doc(doc)
        data = struct.pack(_HEADER_FMT, len(payload), record_type) + payload
        self._file.write(data)
        if self.durability == "os":
            self._file.flush()
        elif self.durability == "fsync":
            self._file.flush()
            os.fsync(self._file.fileno())
        # durability == "none": no flush at all

    def append_batch(self, records: list) -> None:
        """Append multiple records with a single flush/fsync.

        Args:
            records: list of (record_type, doc) tuples.
        """
        from .errors import ReadOnlyError
        if self.readonly:
            raise ReadOnlyError("Collection is open in read-only mode")
        if not records:
            return
        # Encode and size-check the whole batch before writing any of it, so
        # one oversized document fails the batch instead of half-applying it.
        encoded = []
        for rt, doc in records:
            encoded.append((rt, encode_doc(doc)))
        buf = b"".join(
            struct.pack(_HEADER_FMT, len(payload), rt) + payload
            for rt, payload in encoded
        )
        self._file.write(buf)
        if self.durability == "os":
            self._file.flush()
        elif self.durability == "fsync":
            self._file.flush()
            os.fsync(self._file.fileno())
        # durability == "none": no flush at all

    def sync(self) -> None:
        """Flush and fsync the data file, ensuring durability on disk."""
        if self._file is not None:
            self._file.flush()
            os.fsync(self._file.fileno())

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def reopen(self) -> None:
        self.close()
        self._open_file()
