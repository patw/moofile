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
    records = []
    truncate_to = None

    with open(path, "rb") as f:
        while True:
            offset = f.tell()
            header_bytes = f.read(_HEADER_SIZE)
            if not header_bytes:
                break  # clean EOF
            if len(header_bytes) < _HEADER_SIZE:
                truncate_to = offset
                break
            length, record_type = struct.unpack(_HEADER_FMT, header_bytes)
            payload = f.read(length)
            if len(payload) < length:
                truncate_to = offset
                break
            doc = bson.decode(payload)
            records.append((offset, record_type, doc))

    return records, truncate_to


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
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise


class StorageEngine:
    """Handles append-only writes to the BSON file."""

    def __init__(self, path: str, readonly: bool = False) -> None:
        self.path = path
        self.readonly = readonly
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
        data = encode_record(record_type, doc)
        self._file.write(data)
        self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def reopen(self) -> None:
        self.close()
        self._open_file()
