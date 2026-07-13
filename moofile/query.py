"""Query builder and filter evaluation."""

from .operators import apply_op


# ---------------------------------------------------------------------------
# Filter evaluation
# ---------------------------------------------------------------------------

def matches(doc: dict, filter_dict: dict) -> bool:
    """Return True if the document satisfies every condition in filter_dict."""
    for key, value in filter_dict.items():

        # --- Logical operators (top-level) ---
        if key == "$and":
            if not all(matches(doc, sub) for sub in value):
                return False
            continue
        if key == "$or":
            if not any(matches(doc, sub) for sub in value):
                return False
            continue
        if key == "$not":
            if matches(doc, value):
                return False
            continue

        # --- Field-level conditions ---
        field_value = doc.get(key)

        if isinstance(value, dict) and any(k.startswith("$") for k in value):
            # Operator expression: {"field": {"$gt": 5, ...}}
            for op, op_val in value.items():

                if op == "$exists":
                    if bool(op_val) != (key in doc):
                        return False

                elif op == "$elemMatch":
                    if not isinstance(field_value, list):
                        return False
                    if not any(_elem_matches(elem, op_val) for elem in field_value):
                        return False

                else:
                    if not apply_op(op, field_value, op_val):
                        return False
        else:
            # Implicit $eq
            if field_value != value:
                return False

    return True


def _elem_matches(elem, filter_dict: dict) -> bool:
    """Match a single array element against a filter (supports dicts and scalars)."""
    if isinstance(elem, dict):
        return matches(elem, filter_dict)
    # Scalar element: treat operator conditions as applying directly to the value
    for op, op_val in filter_dict.items():
        if op.startswith("$"):
            if not apply_op(op, elem, op_val):
                return False
        else:
            # key-based match doesn't apply to scalars
            return False
    return True


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

class Query:
    """
    Lazy query builder.  Results are not materialised until a terminal
    method (.to_list(), .first(), .count(), .to_df()) is called.
    """

    def __init__(self, collection, filter_dict: dict) -> None:
        self._collection = collection
        self._filter = filter_dict
        self._sort_key: str | None = None
        self._sort_desc: bool = False
        self._skip_n: int = 0
        self._limit_n: int | None = None
        self._group_field: str | None = None
        self._agg_funcs: list | None = None

    # --- Builder methods (each returns a new Query) ---

    def sort(self, field: str, descending: bool = False) -> "Query":
        """Sort results by field."""
        q = self._clone()
        q._sort_key = field
        q._sort_desc = descending
        return q

    def skip(self, n: int) -> "Query":
        """Skip the first n results."""
        q = self._clone()
        q._skip_n = n
        return q

    def limit(self, n: int) -> "Query":
        """Return at most n results."""
        q = self._clone()
        q._limit_n = n
        return q

    def group(self, field: str) -> "Query":
        """Group results by field before aggregation."""
        q = self._clone()
        q._group_field = field
        return q

    def agg(self, *funcs) -> "Query":
        """Apply aggregation functions to each group."""
        q = self._clone()
        q._agg_funcs = list(funcs)
        return q
    
    def vector_search(self, field: str, query_vector, limit: int = 10) -> "VectorQuery":
        """
        Perform vector similarity search on a field.
        Returns a VectorQuery that yields (doc, score) tuples.
        """
        return VectorQuery(self._collection, field, query_vector, limit, self._filter)
    
    def text_search(self, field: str, query: str, limit: int = 10) -> "TextQuery":
        """
        Perform BM25 text search on a field.
        Returns a TextQuery that yields (doc, score) tuples.
        """
        return TextQuery(self._collection, field, query, limit, self._filter)

    def hybrid_search(
        self,
        text_field: str,
        vector_field: str,
        query_text: str,
        query_vector,
        limit: int = 10,
    ) -> "HybridQuery":
        """
        Perform hybrid search combining BM25 text search and vector
        similarity using Reciprocal Rank Fusion (RRF).

        Returns a HybridQuery that yields (doc, rrf_score) tuples.
        """
        return HybridQuery(
            self._collection,
            text_field,
            vector_field,
            query_text,
            query_vector,
            limit,
            self._filter,
        )

    # --- Terminal methods ---

    def to_list(self) -> list:
        """Materialise results as a list of dicts."""
        return self._execute()

    def first(self):
        """Return the first matching document, or None."""
        results = self._execute()
        return results[0] if results else None

    def count(self) -> int:
        """Return the number of matching documents."""
        # Fast path: skip execution pipeline when no transformations
        if (
            self._group_field is None
            and self._sort_key is None
            and self._skip_n == 0
            and self._limit_n is None
        ):
            return self._collection._count_docs(self._filter)
        return len(self._execute())

    def to_df(self):
        """Return results as a pandas DataFrame (pandas must be installed)."""
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError(
                "pandas is required for .to_df().  Install it with: pip install pandas"
            ) from exc
        return pd.DataFrame(self._execute())

    # --- Internal helpers ---

    def _clone(self) -> "Query":
        q = Query(self._collection, self._filter)
        q._sort_key = self._sort_key
        q._sort_desc = self._sort_desc
        q._skip_n = self._skip_n
        q._limit_n = self._limit_n
        q._group_field = self._group_field
        q._agg_funcs = self._agg_funcs
        return q

    def _execute(self) -> list:
        """Run the full query pipeline and return results."""
        # 1. Filter
        docs = self._collection._get_docs(self._filter)

        # 2. Group + aggregate
        if self._group_field is not None:
            docs = self._apply_group_agg(docs)

        # 3. Sort
        if self._sort_key is not None:
            docs = sorted(
                docs,
                key=lambda d: (d.get(self._sort_key) is None, d.get(self._sort_key)),
                reverse=self._sort_desc,
            )

        # 4. Skip
        if self._skip_n:
            docs = docs[self._skip_n :]

        # 5. Limit
        if self._limit_n is not None:
            docs = docs[: self._limit_n]

        return docs

    def _apply_group_agg(self, docs: list) -> list:
        from collections import defaultdict

        groups: dict = defaultdict(list)
        for doc in docs:
            key = doc.get(self._group_field)
            groups[key].append(doc)

        result = []
        for key, group_docs in groups.items():
            row = {self._group_field: key}
            if self._agg_funcs:
                for func in self._agg_funcs:
                    row[func.output_name] = func.compute(group_docs)
            result.append(row)
        return result


class VectorQuery:
    """
    Query results from vector similarity search.
    Returns (document, similarity_score) tuples.
    """
    
    def __init__(self, collection, field: str, query_vector, limit: int, pre_filter: dict):
        self._collection = collection
        self._field = field
        self._query_vector = query_vector
        self._limit = limit
        self._pre_filter = pre_filter
    
    def to_list(self) -> list:
        """Return list of (doc, score) tuples sorted by similarity descending."""
        # Apply pre-filter if any (non-empty filter dict)
        if self._pre_filter and self._pre_filter != {}:
            # Get documents that match the filter first, then score only those
            # (item #4: avoids scoring all docs then filtering)
            filtered_docs = self._collection._get_docs(self._pre_filter)
            allowed_ids = {doc["_id"] for doc in filtered_docs}
            return self._collection._index_manager.vector_search_filtered(
                self._field, self._query_vector, self._limit, allowed_ids
            )
        else:
            return self._collection._index_manager.vector_search(
                self._field, self._query_vector, self._limit
            )
    
    def first(self):
        """Return the best match as (doc, score) tuple or None."""
        results = self.to_list()
        return results[0] if results else None


class TextQuery:
    """
    Query results from BM25 text search.
    Returns (document, relevance_score) tuples.
    """
    
    def __init__(self, collection, field: str, query: str, limit: int, pre_filter: dict):
        self._collection = collection
        self._field = field
        self._query = query
        self._limit = limit
        self._pre_filter = pre_filter
    
    def to_list(self) -> list:
        """Return list of (doc, score) tuples sorted by relevance descending."""
        # Apply pre-filter if any (non-empty filter dict)
        if self._pre_filter and self._pre_filter != {}:
            # Get documents that match the filter first
            filtered_docs = self._collection._get_docs(self._pre_filter)
            # Get text search results
            all_results = self._collection._index_manager.text_search(
                self._field, self._query, limit=None
            )
            # Filter results to only include pre-filtered docs
            filtered_doc_ids = {doc["_id"] for doc in filtered_docs}
            results = [(doc, score) for doc, score in all_results 
                      if doc["_id"] in filtered_doc_ids]
            return results[:self._limit]
        else:
            return self._collection._index_manager.text_search(
                self._field, self._query, self._limit
            )
    
    def first(self):
        """Return the best match as (doc, score) tuple or None."""
        results = self.to_list()
        return results[0] if results else None


class HybridQuery:
    """
    Hybrid search results using Reciprocal Rank Fusion (RRF).

    Combines BM25 text search and vector cosine similarity by fusing
    their rank positions rather than their raw scores.  RRF is
    score-scale-agnostic — it works even though BM25 scores are
    unbounded (and can be negative) while cosine similarity is in
    [-1, 1].

    Returns (document, rrf_score) tuples sorted by fused rank descending.
    """

    #: RRF constant — the standard value from the original literature.
    #: Smaller values weight top ranks more heavily; 60 is the
    #: canonical default.
    _RRF_K = 60

    def __init__(
        self,
        collection,
        text_field: str,
        vector_field: str,
        query_text: str,
        query_vector,
        limit: int,
        pre_filter: dict,
    ):
        self._collection = collection
        self._text_field = text_field
        self._vector_field = vector_field
        self._query_text = query_text
        self._query_vector = query_vector
        self._limit = limit
        self._pre_filter = pre_filter

    def to_list(self) -> list:
        """Return list of (doc, rrf_score) tuples sorted by fused rank descending."""
        # Pull a wider candidate pool from each ranker so RRF has
        # enough overlap to fuse meaningfully.
        pool = max(self._limit * 5, 50)

        text_results = TextQuery(
            self._collection, self._text_field, self._query_text, pool, self._pre_filter
        ).to_list()
        vec_results = VectorQuery(
            self._collection, self._vector_field, self._query_vector, pool, self._pre_filter
        ).to_list()

        # RRF fusion: score(d) = Σ 1/(k + rank + 1)
        k = self._RRF_K
        scores: dict = {}
        docs: dict = {}

        for rank, (doc, _) in enumerate(text_results):
            did = doc["_id"]
            scores[did] = scores.get(did, 0.0) + 1.0 / (k + rank + 1)
            docs[did] = doc

        for rank, (doc, _) in enumerate(vec_results):
            did = doc["_id"]
            scores[did] = scores.get(did, 0.0) + 1.0 / (k + rank + 1)
            if did not in docs:
                docs[did] = doc

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        if self._limit is not None:
            ranked = ranked[: self._limit]

        return [(docs[did], score) for did, score in ranked]

    def first(self):
        """Return the top result as (doc, rrf_score) tuple or None."""
        results = self.to_list()
        return results[0] if results else None
