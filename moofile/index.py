"""In-memory index management using SortedDict."""

import numpy as np
from sortedcontainers import SortedDict
from .text_search import TextIndex

# BSON Binary subtype 9 (vector) dtypes -- mirrors core/src/vector.rs.
_DTYPE_INT8 = 0x03
_DTYPE_FLOAT32 = 0x27
_DTYPE_PACKED_BIT = 0x10


def vector_values(value):
    """A stored vector field as a float32 array, or None if it is not one.

    Accepts an array of numbers or a BSON Binary subtype 9 vector (what
    autoembedding writes).  Packed bits map to +/-1, as in the Rust core.
    """
    from bson.binary import Binary

    if isinstance(value, Binary) and value.subtype == 9:
        raw = bytes(value)
        if len(raw) < 2:
            return None
        dtype, padding, data = raw[0], raw[1], raw[2:]
        if dtype == _DTYPE_FLOAT32 and padding == 0 and len(data) % 4 == 0:
            return np.frombuffer(data, dtype="<f4").astype(np.float32)
        if dtype == _DTYPE_INT8 and padding == 0:
            return np.frombuffer(data, dtype=np.int8).astype(np.float32)
        if dtype == _DTYPE_PACKED_BIT and padding < 8 and (padding == 0 or data):
            bits = np.unpackbits(np.frombuffer(data, dtype=np.uint8))[: len(data) * 8 - padding]
            return bits.astype(np.float32) * 2 - 1
        return None
    # Numbers only, as in Rust: np.array would happily convert "2" or True.
    if not isinstance(value, list) or not all(
        isinstance(x, (int, float)) and not isinstance(x, bool) for x in value
    ):
        return None
    return np.array(value, dtype=np.float32)


class IndexManager:
    """
    Manages in-memory indexes for fast document lookup.

    Internal structure:
        _documents: {_id -> document_dict}
        _indexes:   {field -> SortedDict(value -> {_id: None})}

    Posting lists are dicts used as insertion-ordered sets: membership and
    removal are O(1) rather than the O(k) list scans they replaced, which
    made writes to a low-cardinality indexed field quadratic in the
    collection size.  A dict (not a set) preserves the insertion ordering
    that query results have always had.
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
                idx[val] = {}
            idx[val][_id] = None
        
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
                idx[val].pop(_id, None)
                if not idx[val]:
                    del idx[val]
        
        # Text indexes
        for field in self._text_fields:
            if field in self._text_indexes:
                text = doc.get(field)
                self._text_indexes[field].remove_document(
                    _id, text if isinstance(text, str) else None
                )
        
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
        ids = idx.get(value, {})
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
                    vec = vector_values(embedding)
                    if vec is not None and vec.shape == (expected_dim,):
                        vectors.append(vec)
                        doc_ids.append(_id)
            
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
        
        # Rank by score descending, breaking ties on doc_id ascending.
        # The tie-break makes the ranking a total order, so equal-scoring
        # documents come back in the same order here and in Rust, and do not
        # reshuffle as rows move within the matrix.  This is why argpartition
        # is not used: selecting the top-k by score alone picks an arbitrary
        # member of a tied group at the k-th boundary.
        n = len(similarities)
        if limit is not None and limit == 0:
            return []
        id_rank = np.argsort(np.argsort(np.array(doc_ids, dtype=object), kind="stable"))
        top_indices = np.lexsort((id_rank, -similarities))
        if limit is not None and limit < n:
            top_indices = top_indices[:limit]
        
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
        
        # Same total order as vector_search: score desc, then doc_id asc.
        n = len(similarities)
        if limit is not None and limit == 0:
            return []
        allowed_doc_ids = [doc_ids[i] for i in allowed_indices]
        id_rank = np.argsort(np.argsort(np.array(allowed_doc_ids, dtype=object), kind="stable"))
        top_local = np.lexsort((id_rank, -similarities))
        if limit is not None and limit < n:
            top_local = top_local[:limit]
        
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
