"""MooFile exception hierarchy."""


class MooFileError(Exception):
    """Base exception for all MooFile errors."""


class DuplicateKeyError(MooFileError):
    """Raised when inserting a document with a duplicate _id."""


class DocumentNotFoundError(MooFileError):
    """Raised when update_one or replace_one finds no matching document."""


class ReadOnlyError(MooFileError):
    """Raised when attempting a write operation on a read-only collection."""


class ConcurrentAccessError(MooFileError):
    """Raised when the database file is already open by another process."""


class InvalidIdError(MooFileError, TypeError):
    """Raised when a document's _id is not a string.

    _id is the key for every in-memory index and must be a BSON string.
    Both implementations enforce this: the Rust engine skips records whose
    _id is not a string when replaying the file, so a non-string _id would
    be written to disk and then silently vanish on the next open.

    Subclasses TypeError so that ``except TypeError`` still catches it.
    """


class InvalidFilterError(MooFileError, ValueError):
    """Raised when a query filter is malformed.

    For example a ``$or`` whose elements are not documents, or an unknown
    operator.  Subclasses ValueError so ``except ValueError`` still catches it.
    """


class CorruptRecordError(MooFileError):
    """Raised when a record in the data file cannot be decoded.

    Damage at the *tail* of the file is not this: an interrupted write leaves
    either a short record or an all-zero one, and both are recognised as a
    truncation point and trimmed on open.  This is raised only for damage with
    intact records after it, where trimming would silently discard them.

    Recover with :meth:`Collection.repair`, which salvages every record that
    still decodes, or open with ``repair=True`` to have that happen
    automatically.
    """


class DocumentTooLargeError(MooFileError, ValueError):
    """Raised when a document's encoded size exceeds MAX_DOCUMENT_SIZE.

    The cap exists in the reader so a corrupt length field cannot trigger a
    wild allocation.  Without the matching check on write, a caller could
    append a document that every later open refuses — a file broken by its own
    writer, which is worse than a rejected insert.

    Subclasses ValueError so ``except ValueError`` still catches it.
    """


class BinaryFieldTooLargeError(MooFileError, ValueError):
    """Raised when a binary field exceeds MAX_BINARY_SIZE.

    A lower limit than MAX_DOCUMENT_SIZE, and one that belongs to the BSON
    layer rather than to MooFile: the Rust encoder refuses to write a binary
    value over 16 MiB.  pymongo has no such cap, so without this check the
    pure-Python backend could write documents the Rust backend cannot encode
    — and a document that cannot be re-encoded is dropped from the index, so
    the record would sit on disk and read back as nothing at all.

    Subclasses ValueError so ``except ValueError`` still catches it.
    """
