"""
Cross-implementation write-side size limits.

Two caps, both enforced on write so that neither backend can produce a file
the other cannot read:

* ``MAX_DOCUMENT_SIZE`` (100 MiB) is MooFile's own record cap.  The scanner
  refuses a longer record, because from its side an over-cap length field is
  indistinguishable from a corrupt one — so writing one produces a file that
  every later open rejects, and rejects correctly.

* ``MAX_BINARY_SIZE`` (16 MiB) belongs to the BSON layer: the Rust encoder
  will not write a binary value that large.  pymongo will.  A document over
  that line used to be accepted by the pure-Python backend, written, read
  back by it — and then be *silently missing* under the Rust backend, which
  decodes the record, fails to re-encode it for the raw-document index, and
  drops it.  On the Rust write side it was worse still: `bson::to_vec` was
  `expect`ed, so it panicked while holding the collection's write lock, which
  poisons it and bricks the handle.
"""

import os

import bson
import pytest

from moofile.errors import BinaryFieldTooLargeError, DocumentTooLargeError
from moofile.storage import MAX_BINARY_SIZE, MAX_DOCUMENT_SIZE


class TestBinaryFieldCap:
    def test_a_binary_at_the_cap_round_trips(self, make_collection, collection_impl, backend):
        db = make_collection("bin_ok.bson")
        path = db._path
        db.insert({"_id": "a", "blob": bson.Binary(b"\x07" * MAX_BINARY_SIZE)})
        db.close()
        os.remove(path + ".cache")

        db = collection_impl(path)
        assert db.count() == 1, "a document at the cap must still be visible"
        doc = db.find_one({"_id": "a"})
        assert len(doc["blob"]) == MAX_BINARY_SIZE
        db.close()

    def test_a_binary_over_the_cap_is_rejected(self, make_collection, backend):
        db = make_collection("bin_big.bson")
        path = db._path
        with pytest.raises(BinaryFieldTooLargeError):
            db.insert({"_id": "a", "blob": bson.Binary(b"\x00" * (MAX_BINARY_SIZE + 1))})
        db.close()

        assert os.path.getsize(path) == 0, "nothing should have reached the file"

    def test_the_handle_still_works_after_a_rejection(self, make_collection, backend):
        # On the Rust side this used to panic inside the write lock, poisoning
        # it — every later call on the handle failed too, permanently.
        db = make_collection("bin_after.bson")
        with pytest.raises(BinaryFieldTooLargeError):
            db.insert({"_id": "a", "blob": bson.Binary(b"\x00" * (MAX_BINARY_SIZE + 1))})

        db.insert({"_id": "b", "v": 1})
        assert db.count() == 1
        assert db.find_one({"_id": "b"})["v"] == 1
        db.close()

    def test_nested_and_arrayed_binaries_are_found(self, make_collection, backend):
        big = bson.Binary(b"\x00" * (MAX_BINARY_SIZE + 1))
        db = make_collection("bin_nested.bson")

        with pytest.raises(BinaryFieldTooLargeError):
            db.insert({"_id": "a", "outer": {"inner": big}})
        with pytest.raises(BinaryFieldTooLargeError):
            db.insert({"_id": "b", "items": ["ok", big]})

        assert db.count() == 0
        db.close()

    def test_rejection_also_applies_inside_a_batch(self, make_collection, backend):
        db = make_collection("bin_batch.bson")
        path = db._path
        with pytest.raises(BinaryFieldTooLargeError):
            with db.batch():
                db.insert({"_id": "a", "v": 1})
                db.insert({"_id": "b", "blob": bson.Binary(b"\x00" * (MAX_BINARY_SIZE + 1))})
        db.close()

        assert os.path.getsize(path) == 0, "a batch must not half-apply"


class TestDocumentCap:
    def test_a_document_over_the_record_cap_is_rejected(self, make_collection, backend):
        # A string, not a binary — binaries hit the lower cap first.
        db = make_collection("doc_big.bson")
        path = db._path
        with pytest.raises(DocumentTooLargeError):
            db.insert({"_id": "a", "blob": "x" * MAX_DOCUMENT_SIZE})
        db.close()

        assert os.path.getsize(path) == 0

    def test_a_large_but_legal_document_round_trips(self, make_collection, collection_impl, backend):
        # Comfortably over MAX_BINARY_SIZE, comfortably under the record cap:
        # the two limits are genuinely different and this must be accepted.
        big = "x" * (MAX_BINARY_SIZE * 2)
        db = make_collection("doc_ok.bson")
        path = db._path
        db.insert({"_id": "a", "blob": big})
        db.close()
        os.remove(path + ".cache")

        db = collection_impl(path)
        assert db.count() == 1
        assert len(db.find_one({"_id": "a"})["blob"]) == len(big)
        db.close()
