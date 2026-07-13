"""In-memory index management using SortedDict."""

import numpy as np
from sortedcontainers import SortedDict
from .text_search import TextIndex


class IndexManager:
    """
    Manages in-memory indexes for fast document lookup.

    Internal structure:
        _documents: {_id -> document_dict}
        _indexes:   {field -> SortedDict(value -> [list of _ids])}
        _vector_indexes: {field -> (embeddings_array, doc_ids_list)}
        _text_indexes: {field -> TextIndex}
    """

    def __init__(self, indexed_fields, vector_indexes=None, text_indexes=None) -> None:
        self._fields = list(indexed_fields)
        self._indexes: dict[str, SortedDict] = {
            field: SortedDict() for field in self._fields
        }
        self._documents: dict = {}  # _id -> doc
        
        # Vector indexes: field -> (embeddings_array, doc_ids_list)
        self._vector_fields = dict(vector_indexes or {})
        self._vector_indexes: dict[str, tuple] = {}
        self._vectors_dirty = True  # needs initial rebuild
        
        # Text indexes: field -> TextIndex
        self._text_fields = list(text_indexes or [])
        self._text_indexes: dict[str, TextIndex] = {
            field: TextIndex() for field in self._text_fields
        }

    def add(self, doc: dict) -> None:
        """Add a document to the in-memory store and all indexes."""
        _id = doc["_id"]
        self._documents[_id] = doc
        
        # Regular field indexes
        for field, idx in self._indexes.items():
            val = doc.get(field)
            if val is None:
                continue
            if val not in idx:
                idx[val] = []
            if _id not in idx[val]:
                idx[val].append(_id)
        
        # Text indexes
        for field in self._text_fields:
            text = doc.get(field)
            if text and isinstance(text, str):
                self._text_indexes[field].add_document(_id, text)
        
        # Vector indexes - mark dirty so they get rebuilt before next search
        if any(doc.get(f) is not None for f in self._vector_fields):
            self._vectors_dirty = True

    def remove(self, _id) -> None:
        """Remove a document from the store and all indexes."""
        doc = self._documents.pop(_id, None)
        if doc is None:
            return
            
        # Regular field indexes
        for field, idx in self._indexes.items():
            val = doc.get(field)
            if val is not None and val in idx:
                try:
                    idx[val].remove(_id)
                except ValueError:
                    pass
                if not idx[val]:
                    del idx[val]
        
        # Text indexes
        for field in self._text_fields:
            if field in self._text_indexes:
                self._text_indexes[field].remove_document(_id)
        
        # Vector indexes - mark dirty for rebuild
        self._vectors_dirty = True

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
        for text_idx in self._text_indexes.values():
            text_idx.clear()
        self._vector_indexes.clear()
        self._documents.clear()
        self._vectors_dirty = True
    
    def rebuild_vector_indexes(self) -> None:
        """Rebuild vector indexes from current documents."""
        for field, expected_dim in self._vector_fields.items():
            vectors = []
            doc_ids = []
            
            for _id, doc in self._documents.items():
                embedding = doc.get(field)
                if embedding is not None:
                    try:
                        vec = np.array(embedding, dtype=np.float32)
                        if vec.shape == (expected_dim,):
                            vectors.append(vec)
                            doc_ids.append(_id)
                    except (ValueError, TypeError):
                        continue  # Skip invalid vectors
            
            if vectors:
                embeddings = np.array(vectors)
                # Normalise rows at build time so cosine similarity becomes
                # a plain dot product at query time (item #1).
                norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
                norms[norms == 0] = 1.0  # avoid division by zero
                embeddings = embeddings / norms
                self._vector_indexes[field] = (embeddings, doc_ids)
            else:
                self._vector_indexes[field] = (np.empty((0, expected_dim), dtype=np.float32), [])
        
        self._vectors_dirty = False
    
    def vector_search(self, field: str, query_vector, limit: int = 10) -> list:
        """
        Perform cosine similarity search on a vector field.
        Returns list of (doc, score) tuples sorted by similarity descending.
        """
        # Rebuild if dirty or if field not in indexes or if indexes are empty
        if self._vectors_dirty or field not in self._vector_indexes or len(self._vector_indexes.get(field, ([], []))[0]) == 0:
            self.rebuild_vector_indexes()
        
        if field not in self._vector_indexes:
            return []
        
        embeddings, doc_ids = self._vector_indexes[field]
        if len(embeddings) == 0:
            return []
        
        # Convert query to numpy array
        query_vec = np.array(query_vector, dtype=np.float32)
        query_norm = np.linalg.norm(query_vec)
        if query_norm == 0:
            return []
        
        # Normalise query — rows are already normalised at build time,
        # so cosine similarity is just a dot product (item #1).
        q_normed = query_vec / query_norm
        similarities = embeddings @ q_normed
        
        # Bounded top-k using argpartition: O(n) instead of O(n log n) (item #2)
        n = len(similarities)
        if limit is not None and limit < n:
            if limit == 0:
                return []
            # argpartition: O(n) selection of top-k indices (unordered)
            top_indices = np.argpartition(similarities, n - limit)[n - limit:]
            # Sort just the top-k in descending order
            top_indices = top_indices[np.argsort(similarities[top_indices])[::-1]]
        else:
            top_indices = np.argsort(similarities)[::-1]
        
        results = []
        for idx in top_indices:
            doc_id = doc_ids[idx]
            doc = self._documents.get(doc_id)
            if doc:
                results.append((doc, float(similarities[idx])))
        
        return results

    def vector_search_filtered(self, field: str, query_vector, limit, allowed_ids: set) -> list:
        """
        Perform vector similarity search restricted to a set of allowed document IDs.
        Scores only the allowed documents instead of all then filtering (item #4).
        Returns list of (doc, score) tuples sorted by similarity descending.
        """
        # Rebuild if dirty or if field not in indexes or if indexes are empty
        if self._vectors_dirty or field not in self._vector_indexes or len(self._vector_indexes.get(field, ([], []))[0]) == 0:
            self.rebuild_vector_indexes()
        
        if field not in self._vector_indexes:
            return []
        
        embeddings, doc_ids = self._vector_indexes[field]
        if len(embeddings) == 0:
            return []
        
        query_vec = np.array(query_vector, dtype=np.float32)
        query_norm = np.linalg.norm(query_vec)
        if query_norm == 0:
            return []
        
        # Normalise query — rows are already normalised at build time
        q_normed = query_vec / query_norm
        
        # Filter to only allowed docs, then score only those (item #4)
        allowed_indices = [i for i, did in enumerate(doc_ids) if did in allowed_ids]
        if not allowed_indices:
            return []
        
        filtered_embeddings = embeddings[allowed_indices]
        similarities = filtered_embeddings @ q_normed
        
        # Bounded top-k (item #2)
        n = len(similarities)
        if limit is not None and limit < n:
            if limit == 0:
                return []
            top_local = np.argpartition(similarities, n - limit)[n - limit:]
            top_local = top_local[np.argsort(similarities[top_local])[::-1]]
        else:
            top_local = np.argsort(similarities)[::-1]
        
        results = []
        for local_idx in top_local:
            original_idx = allowed_indices[local_idx]
            doc_id = doc_ids[original_idx]
            doc = self._documents.get(doc_id)
            if doc:
                results.append((doc, float(similarities[local_idx])))
        
        return results
    
    def text_search(self, field: str, query: str, limit: int = 10) -> list:
        """
        Perform BM25 text search on a text field.
        Returns list of (doc, score) tuples sorted by relevance descending.
        """
        if field not in self._text_indexes:
            return []
        
        doc_scores = self._text_indexes[field].search(query, limit)
        
        results = []
        for doc_id, score in doc_scores:
            doc = self._documents.get(doc_id)
            if doc:
                results.append((doc, score))
        
        return results
