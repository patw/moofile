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
