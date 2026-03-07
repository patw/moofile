"""MooFile exception hierarchy."""


class MooFileError(Exception):
    """Base exception for all MooFile errors."""


class DuplicateKeyError(MooFileError):
    """Raised when inserting a document with a duplicate _id."""


class DocumentNotFoundError(MooFileError):
    """Raised when update_one or replace_one finds no matching document."""


class ReadOnlyError(MooFileError):
    """Raised when attempting a write operation on a read-only collection."""
