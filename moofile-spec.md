# MooFile — Specification v0.2.0

> A lightweight, embedded, single-file document store with vector similarity search, BM25 text search, and a developer-friendly query API.  
> No server. No infrastructure. Just a file and a library.

---

## Motivation

SQLite is magical because it is a library, not a server. You get persistence, indexing, and querying with zero infrastructure. The tradeoff is SQL — a relational query language that does not map naturally to how most application code thinks about data.

JSON files are the other extreme. Perfect ergonomics, zero infrastructure, but no indexes. Once your dataset grows past a few thousand documents, every query is a full scan.

MongoDB's query language (MQL) is closer to how developers naturally think — queries look like the documents they describe. But MongoDB requires a server, which is heavy for local or embedded use cases. Vector databases like Pinecone provide semantic search but require infrastructure and often sacrifice document flexibility.

MooFile sits in the gap:

- **Embedded** like SQLite — a library, no daemon, no network
- **Document-oriented** like MongoDB — JSON-shaped data, flexible schema
- **Vector search** like modern vector databases — cosine similarity, semantic search
- **Text search** like Elasticsearch — BM25 ranking, full-text indexing
- **Developer-friendly API** — method chains, not operator dicts
- **Single-file portability** — your database is a file you can copy, version control, or email

Target dataset size: **megabytes to single-digit gigabytes**.  
If you need horizontal scaling, distributed search, or sub-millisecond vector lookups, use specialized infrastructure.

---

## Non-Goals

- No network interface or server mode — ever
- No replication or clustering
- No multi-process concurrent writes
- No `$lookup` / joins — denormalize your data
- No SQL compatibility
- No full MongoDB MQL parity — implement the 80%, skip the 20% nobody uses
- No HNSW, IVF, or advanced vector indexing — brute-force is sufficient for target scale
- No advanced NLP — BM25 + Porter stemming covers most use cases
- No persistent indexes — correctness over startup performance

---

## File Layout

A MooFile database is two files:

```
mydata.bson       ← append-only document store, source of truth
mydata.bson.meta  ← index configuration (regular, vector, and text indexes)
```

Indexes are **never persisted**. They are rebuilt in memory on every open by scanning the BSON file. This includes regular field indexes, vector indexes for similarity search, and inverted text indexes for BM25 ranking. The BSON file is always the source of truth. If the meta file is lost or corrupt, delete it and reopen — it will be rebuilt.

### Why No Persistent Indexes?

- Eliminates crash recovery complexity entirely
- Eliminates WAL complexity entirely  
- Simplifies the codebase dramatically
- For the target dataset size (MBs to low GBs), rebuild on open is fast enough
- Correctness is guaranteed — indexes can never be out of sync with data
- Applies to all index types: regular field indexes, vector arrays, and inverted text indexes
- Vector rebuilds are O(n) but acceptable for datasets under ~100K documents
- Text index rebuilds include tokenization and stemming but complete in seconds

### BSON File Format

The data file is **append-only**. Documents are never updated or deleted in place.

Each entry in the file is a fixed-header record followed by a BSON payload:

```
[4 bytes: record length] [1 byte: record type] [BSON payload]
```

Record types:
- `0x01` — live document
- `0x02` — tombstone (delete marker)
- `0x03` — replacement (update marker, contains new document)

On open, the file is scanned once from start to finish. The last record for any given `_id` wins. Tombstones remove a document from the in-memory index.

### Meta File Format

A small JSON file — human readable by design:

```json
{
  "version": 1,
  "indexes": ["email", "age", "status"],
  "vector_indexes": {"embedding": 384, "profile_vec": 128},
  "text_indexes": ["title", "content", "description"],
  "created_at": "2025-01-01T00:00:00Z"
}
```

Vector indexes specify field name and vector dimensionality. Text indexes list fields that support BM25 full-text search.

---

## Storage Engine

### Append-Only Writes

```
insert  → append type=0x01 record to BSON file
update  → append type=0x03 record (new document version)
delete  → append type=0x02 tombstone record
```

The file only ever grows. Old versions of documents remain in the file as dead bytes until compaction.

### Compaction

Compaction rewrites the BSON file keeping only the latest live version of each document.

```python
db.compact()  # explicit, never automatic
```

Rules:
- Never runs automatically — the developer decides when
- Writes to a `.tmp` file first, then atomically renames
- Safe to interrupt — if it fails, original file is untouched
- Recommended when dead space exceeds ~30% of file size (check via `db.stats()`)

### In-Memory Indexes

On open, MooFile scans the BSON file and builds three types of in-memory indexes:

```python
# internal structure (simplified)
self._indexes = {
    "email": SortedDict(),   # value → [list of _ids]
    "age":   SortedDict(),   # value → [list of _ids]
}
self._vector_indexes = {
    "embedding": np.array(),  # [n_docs × vector_dim] matrix
    "embedding_ids": [],     # [_id1, _id2, ...] parallel to matrix rows
}
self._text_indexes = {
    "content": {
        "terms": {},          # term → {_id: tf_count, ...}
        "doc_lengths": {},    # _id → token_count
        "idf": {},            # term → inverse_document_frequency
    }
}
self._documents = {}         # _id → (file_offset, document_dict)
```

**Regular indexes:** Use `sortedcontainers.SortedDict` for range queries  
**Vector indexes:** Dense numpy arrays for efficient cosine similarity  
**Text indexes:** Inverted indexes with term frequencies for BM25 scoring

All index updates are synchronous — after every insert/update/delete, all relevant indexes are updated immediately before returning to the caller.

### No WAL

WAL is explicitly excluded. The tradeoff:

- A crash mid-write may corrupt the final record in the BSON file
- On open, scan to the last complete record and truncate any partial trailing write
- All prior records are intact — you lose at most the last in-flight write

This is acceptable for the target use case. Implementing a WAL adds significant complexity for marginal benefit at this scale.

---

## Document Identity

Every document has an `_id` field. Rules:

- If not provided on insert, MooFile generates a random 12-byte hex string (no ObjectId dependency)
- `_id` is always indexed automatically, regardless of declared indexes
- `_id` must be unique — inserting a duplicate `_id` raises `DuplicateKeyError`
- The developer can provide their own `_id` value of any hashable type

---

## Python API

### Opening a Collection

```python
from moofile import Collection

# open or create with multiple index types
db = Collection(
    "mydata.bson", 
    indexes=["email", "age", "status"],           # regular field indexes
    vector_indexes={"embedding": 384},             # vector similarity search
    text_indexes=["title", "content"]             # BM25 text search
)

# read-only mode
db = Collection("mydata.bson", readonly=True)

# minimal setup (indexes optional)
db = Collection("mydata.bson")
```

On open:
1. Create files if they do not exist
2. Scan BSON file, build all in-memory indexes (regular, vector, text)
3. Ready

Index build time scales with document count: ~1-2 seconds for 10K documents, including vector arrays and text tokenization.

### Insert

```python
# basic insert
doc = db.insert({"name": "alice", "age": 30, "email": "alice@example.com"})
# returns the document with _id populated

# insert with vector and text data
doc = db.insert({
    "title": "Machine Learning Basics",
    "content": "Introduction to supervised and unsupervised learning...",
    "embedding": [0.1, 0.2, 0.3, ...],  # 384-dimensional vector
    "category": "AI"
})

docs = db.insert_many([{...}, {...}, {...}])
# returns list of inserted documents
```

Vectors and text content are automatically indexed according to the declared `vector_indexes` and `text_indexes` configuration.

### Traditional Query

```python
# return all matching documents as a list of dicts
db.find({"age": {"$gt": 25}})

# return first match or None
db.find_one({"email": "alice@example.com"})

# count matches without materializing documents
db.count({"status": "active"})

# check existence
db.exists({"email": "alice@example.com"})
```

### Vector Similarity Search

```python
# find semantically similar documents
query_vector = [0.15, 0.25, 0.35, ...]
results = db.find({}).vector_search("embedding", query_vector, limit=5).to_list()
# returns: [(doc1, similarity1), (doc2, similarity2), ...]

# combine with filters - search only within specific categories
results = db.find({"category": "AI"}).vector_search("embedding", query_vector).to_list()
```

### BM25 Text Search

```python
# keyword search with relevance ranking
results = db.find({}).text_search("content", "machine learning", limit=5).to_list()
# returns: [(doc1, relevance1), (doc2, relevance2), ...]

# search multiple fields
title_results = db.find({}).text_search("title", "neural networks").to_list()
content_results = db.find({}).text_search("content", "neural networks").to_list()

# combine with filters
results = db.find({"year": {"$gte": 2020}}).text_search("content", "transformers").to_list()
```

### Method Chain API

`.find()` returns a lazy `Query` object. Vector and text search return specialized query objects. Results are not materialized until a terminal method is called.

```python
# traditional queries
db.find({"status": "active"})
  .sort("age", descending=True)
  .skip(20)
  .limit(10)
  .to_list()

# vector similarity search with chaining
db.find({"category": "research"})
  .vector_search("embedding", query_vector, limit=20)
  .to_list()  # returns [(doc, similarity), ...]

# text search with chaining  
db.find({"published": True})
  .text_search("content", "machine learning", limit=10)
  .to_list()  # returns [(doc, relevance), ...]

# terminal methods
.to_list()       # → list of dicts (or list of (doc, score) tuples for search)
.to_df()         # → pandas DataFrame (pandas optional dependency)
.first()         # → first dict or (doc, score) tuple
.count()         # → int
```

### Update

```python
# update first match
db.update_one(
    where={"email": "alice@example.com"},
    set={"age": 31}
)

# update all matches
db.update_many(
    where={"status": "trial"},
    set={"status": "expired"}
)

# replace entire document (preserves _id)
db.replace_one({"_id": "abc123"}, {"name": "alice", "age": 32})
```

Update operators: `set`, `unset`, `inc`. That covers 95% of real usage.

When updating documents with vector or text fields, the relevant indexes are automatically updated.

### Delete

```python
db.delete_one({"_id": "abc123"})
db.delete_many({"status": "archived"})
```

### Aggregation

Aggregation is expressed as a method chain, not a pipeline dict. This is intentional — it reads as a sentence.

```python
db.find({"status": "active"})
  .group("city")
  .agg(
      count(),
      mean("age"),
      sum("revenue"),
      min("created_at"),
      max("created_at"),
  )
  .sort("count", descending=True)
  .limit(10)
  .to_list()
```

#### Aggregation Functions (v1)

| Function | Description |
|---|---|
| `count()` | number of documents in group |
| `sum(field)` | sum of field values |
| `mean(field)` | average of field values |
| `min(field)` | minimum field value |
| `max(field)` | maximum field value |
| `collect(field)` | list of all values in group |
| `first(field)` | first value encountered |
| `last(field)` | last value encountered |

No `$lookup`. No `$facet`. No `$bucket` in v1.

### Filter Operators

#### Comparison

| Operator | Meaning |
|---|---|
| `$eq` | equal (default, implicit) |
| `$ne` | not equal |
| `$gt` | greater than |
| `$gte` | greater than or equal |
| `$lt` | less than |
| `$lte` | less than or equal |
| `$in` | value is in list |
| `$nin` | value is not in list |

#### Logical

| Operator | Meaning |
|---|---|
| `$and` | all conditions must match |
| `$or` | any condition must match |
| `$not` | inverts a condition |

#### Element

| Operator | Meaning |
|---|---|
| `$exists` | field exists (or does not exist) |

#### Array

| Operator | Meaning |
|---|---|
| `$elemMatch` | at least one array element matches |

That is the complete filter surface for traditional queries. No `$regex`, no `$where`, no geospatial.

Text search uses dedicated `.text_search()` methods rather than `$text` operators, providing more explicit control over BM25 ranking.

### Utility

```python
# database stats
db.stats()
# → {"documents": 42150, "dead_records": 3201, "file_size_bytes": 8421000, "dead_ratio": 0.07}

# compact the file
db.compact()

# rebuild indexes from scratch (useful after manual file manipulation)
db.reindex()

# close explicitly (also called on __del__ and context manager exit)
db.close()

# context manager
with Collection("mydata.bson", indexes=["email"]) as db:
    db.insert({"email": "bob@example.com"})
```

---

## Query Execution

### Index Usage

**Traditional queries:** When a filter references an indexed field, MooFile uses the sorted index. Otherwise it falls back to a full document scan. No query planner — simple rule: if the top-level filter key matches an index, use it.

**Vector search:** Always O(n) brute-force cosine similarity across all documents (after applying filters). Uses numpy for efficient vector operations.

**Text search:** Uses inverted indexes with BM25 scoring. Includes tokenization, Porter stemming, and term frequency analysis.

```python
# regular indexed queries — fast
db.find({"email": "alice@example.com"})
db.find({"age": {"$gt": 25}})

# vector similarity — O(n) but optimized
db.find().vector_search("embedding", query_vec)  # numpy cosine similarity

# text search — inverted index lookup + BM25 scoring
db.find().text_search("content", "machine learning")  # stemmed token matching

# not indexed — full document scan
db.find({"name": "alice"})
db.find({"address.city": "Toronto"})  # nested fields not indexed
```

Nested field indexing is explicitly out of scope. Index top-level fields only.

### Execution Order

For traditional chained queries:

```
filter → group + agg → sort → skip → limit → project
```

For search queries:

```
filter → vector_search/text_search → limit (implicit in search) → materialize
```

Filtering always runs first. For search queries, filters are applied before similarity computation to reduce the search space.

---

## Schema

MooFile is schema-free by design. Any document can be inserted regardless of shape. The `schema` parameter is optional and informational only — it is never enforced:

```python
db = Collection("mydata.bson",
    indexes=["email", "age"],
    schema={"email": str, "age": int, "name": str}  # optional hints only
)
```

Schema hints may be used in future versions for index type optimization. In v1 they are ignored by the engine and exist purely for developer documentation purposes.

---

## Dependencies

MooFile has grown beyond minimal dependencies to support search capabilities:

| Dependency | Purpose | Required |
|---|---|---|
| `pymongo>=4.0` | BSON encode/decode | Yes |
| `sortedcontainers>=2.0` | In-memory sorted indexes | Yes |
| `numpy>=1.20` | Vector operations, cosine similarity | Yes |
| `snowballstemmer>=2.0` | Porter stemming for text search | Yes |
| `pandas>=1.0` | `.to_df()` terminal method | Optional |

The addition of numpy and stemming increases the footprint but enables semantic and text search without external services. No database drivers, async frameworks, or deep learning dependencies.

---

## Error Handling

```python
from moofile import (
    Collection,
    DuplicateKeyError,    # _id conflict on insert
    DocumentNotFoundError, # update_one / replace_one with no match
    MooFileError,         # base exception class
)
```

All errors are subclasses of `MooFileError`. No silent failures.

---

## Thread Safety

Single-threaded only. One `Collection` instance per process per file. Concurrent reads from multiple threads are safe. Concurrent writes are not protected and will corrupt the file.

This applies to all operations: traditional queries, vector search, text search, and index updates. If you need multi-threaded writes, serialize them with a lock at the application layer. MooFile will not do this for you.

---

## Implementation Notes

### Simplicity Over Performance

When in doubt, choose the simpler implementation. MooFile's primary value is being a readable codebase that a developer can understand. Examples:

- **Vector search:** Brute-force cosine similarity, not HNSW or IVF indexing
- **Text search:** Simple BM25 + Porter stemming, not transformer embeddings
- **Storage:** Append-only files, not complex page-based storage
- **Indexes:** Full rebuilds on open, not incremental maintenance

If a performance optimization requires significant complexity, it likely violates the embedded-database design philosophy.

### Current Code Structure (v0.2.0)

```
moofile/
  __init__.py         exports, version
  collection.py       main Collection class, open/close, scan
  storage.py          BSON file append, read, compaction
  index.py            in-memory index build and update (regular + vector + text)
  query.py            Query builders: Query, VectorQuery, TextQuery
  operators.py        $gt, $lt, $in, etc. — one function each
  aggregation.py      group/agg execution  
  text_search.py      BM25 implementation, Porter stemming, inverted indexes
  errors.py           exception classes
  cli/                command-line tools (moosh, moo2json, moo2mongo, moo2sqlite)
    __init__.py
    moosh.py
    moo2json.py
    moo2mongo.py
    moo2sqlite.py

~ 1800-2000 lines total (excluding tests)
```

Scope has grown significantly beyond the original v1 plan, but the additions (search capabilities and CLI tools) provide substantial practical value.

### Recommended Libraries

- Use `pymongo.bson` — do not implement BSON encoding
- Use `sortedcontainers.SortedDict` — do not implement a B-tree
- Use `numpy` for vector operations — do not implement matrix math
- Use `snowballstemmer` for text processing — do not implement NLP algorithms
- Use `os.replace()` for atomic file rename during compaction
- Use built-in `json` for meta file format — human readable

### What Not to Build

- Custom BSON parser
- Custom B-tree  
- WAL
- Query optimizer / planner
- Nested field indexing
- `$lookup` / joins
- Network interface
- Async API
- Advanced vector indexing (HNSW, IVF)
- Deep learning embeddings (use external models)
- Complex NLP beyond stemming
- Persistent indexes
- Multi-process coordination

---

## Future Directions

### Implemented in v0.2.0
- ✅ CLI tools: `moosh`, `moo2json`, `moo2mongo`, `moo2sqlite`
- ✅ Vector similarity search with cosine distance
- ✅ BM25 text search with Porter stemming

### Potential Future Features
- Persistent mmap'd index files for faster startup on very large datasets
- Advanced vector search algorithms (HNSW) for > 100K document performance
- Nested field indexes (`"address.city"` indexing)
- `$unwind` for array flattening in aggregation pipelines
- Optional schema enforcement mode
- Watch/change streams for local reactivity
- C core with Python/JS/Go bindings for performance
- Approximate nearest neighbor search with configurable trade-offs
- Multi-field text search with weighted scoring

### Explicitly Not Planned
- Network interfaces or server modes
- Distributed or replicated deployments  
- Multi-process write coordination
- Advanced NLP beyond stemming
- Integration with specific ML frameworks

---

## Name

**MooFile**. It is a cow. Cows are not fast or scalable but they are reliable, friendly, and everyone likes them. This is that.
