"""In-memory index management using SortedDict."""

from sortedcontainers import SortedDict


class IndexManager:
    """
    Manages in-memory indexes for fast document lookup.

    Internal structure:
        _documents: {_id -> document_dict}
        _indexes:   {field -> SortedDict(value -> [list of _ids])}
    """

    def __init__(self, indexed_fields) -> None:
        self._fields = list(indexed_fields)
        self._indexes: dict[str, SortedDict] = {
            field: SortedDict() for field in self._fields
        }
        self._documents: dict = {}  # _id -> doc

    def add(self, doc: dict) -> None:
        """Add a document to the in-memory store and all indexes."""
        _id = doc["_id"]
        self._documents[_id] = doc
        for field, idx in self._indexes.items():
            val = doc.get(field)
            if val is None:
                continue
            if val not in idx:
                idx[val] = []
            if _id not in idx[val]:
                idx[val].append(_id)

    def remove(self, _id) -> None:
        """Remove a document from the store and all indexes."""
        doc = self._documents.pop(_id, None)
        if doc is None:
            return
        for field, idx in self._indexes.items():
            val = doc.get(field)
            if val is not None and val in idx:
                try:
                    idx[val].remove(_id)
                except ValueError:
                    pass
                if not idx[val]:
                    del idx[val]

    def get(self, _id):
        """Return the document for the given _id, or None."""
        return self._documents.get(_id)

    def all_docs(self) -> list:
        """Return all live documents as a list."""
        return list(self._documents.values())

    def get_by_field_exact(self, field: str, value) -> list | None:
        """
        Return list of docs where field == value using the index.
        Returns None if the field is not indexed.
        """
        idx = self._indexes.get(field)
        if idx is None:
            return None
        ids = idx.get(value, [])
        return [self._documents[i] for i in ids if i in self._documents]

    def get_by_field_range(
        self,
        field: str,
        min_val=None,
        max_val=None,
        min_inclusive: bool = True,
        max_inclusive: bool = True,
    ) -> list | None:
        """
        Return docs in a range using the index.
        Returns None if the field is not indexed or types are incomparable.
        """
        idx = self._indexes.get(field)
        if idx is None:
            return None
        try:
            keys = list(
                idx.irange(
                    minimum=min_val,
                    maximum=max_val,
                    inclusive=(min_inclusive, max_inclusive),
                )
            )
        except TypeError:
            return None  # incomparable types — fall back to full scan
        ids = []
        for k in keys:
            ids.extend(idx[k])
        return [self._documents[i] for i in ids if i in self._documents]

    def clear(self) -> None:
        """Clear all documents and indexes."""
        for idx in self._indexes.values():
            idx.clear()
        self._documents.clear()
