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
        
        # Vector indexes - defer rebuild until all docs loaded
        # Individual adds don't update vector arrays for efficiency

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
        
        # Vector indexes - defer rebuild until needed

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
                self._vector_indexes[field] = (embeddings, doc_ids)
            else:
                self._vector_indexes[field] = (np.empty((0, expected_dim), dtype=np.float32), [])
    
    def vector_search(self, field: str, query_vector, limit: int = 10) -> list:
        """
        Perform cosine similarity search on a vector field.
        Returns list of (doc, score) tuples sorted by similarity descending.
        """
        # Rebuild if field not in indexes or if indexes are empty
        if field not in self._vector_indexes or len(self._vector_indexes.get(field, ([], []))[0]) == 0:
            self.rebuild_vector_indexes()
        
        if field not in self._vector_indexes:
            return []
        
        embeddings, doc_ids = self._vector_indexes[field]
        if len(embeddings) == 0:
            return []
        
        # Convert query to numpy array
        query_vec = np.array(query_vector, dtype=np.float32)
        
        # Compute cosine similarities
        # similarities = dot(query, embeddings.T) / (||query|| * ||embeddings||)
        query_norm = np.linalg.norm(query_vec)
        if query_norm == 0:
            return []
        
        embedding_norms = np.linalg.norm(embeddings, axis=1)
        valid_mask = embedding_norms > 0
        
        if not np.any(valid_mask):
            return []
        
        similarities = np.zeros(len(embeddings))
        similarities[valid_mask] = np.dot(embeddings[valid_mask], query_vec) / (
            query_norm * embedding_norms[valid_mask]
        )
        
        # Get top-k results
        sorted_indices = np.argsort(similarities)[::-1]
        if limit is not None:
            top_indices = sorted_indices[:limit]
        else:
            top_indices = sorted_indices
        
        results = []
        for idx in top_indices:
            doc_id = doc_ids[idx]
            doc = self._documents.get(doc_id)
            if doc:
                results.append((doc, float(similarities[idx])))
        
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
