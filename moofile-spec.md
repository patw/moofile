# MooFile — Specification v0.1

> A lightweight, embedded, single-file document store with a developer-friendly query API.  
> No server. No infrastructure. Just a file and a library.

---

## Motivation

SQLite is magical because it is a library, not a server. You get persistence, indexing, and querying with zero infrastructure. The tradeoff is SQL — a relational query language that does not map naturally to how most application code thinks about data.

JSON files are the other extreme. Perfect ergonomics, zero infrastructure, but no indexes. Once your dataset grows past a few thousand documents, every query is a full scan.

MongoDB's query language (MQL) is closer to how developers naturally think — queries look like the documents they describe. But MongoDB requires a server, which is heavy for local or embedded use cases.

MooFile sits in the gap:

- **Embedded** like SQLite — a library, no daemon, no network
- **Document-oriented** like MongoDB — JSON-shaped data, flexible schema
- **Developer-friendly API** — method chains, not operator dicts
- **Single-file portability** — your database is a file you can copy, version control, or email

Target dataset size: **megabytes to single-digit gigabytes**.  
If you need replication, sharding, or multi-process writes, use real MongoDB.

---

## Non-Goals

- No network interface or server mode — ever
- No replication or clustering
- No multi-process concurrent writes
- No `$lookup` / joins in v1 — denormalize your data
- No SQL compatibility
- No full MongoDB MQL parity — implement the 80%, skip the 20% nobody uses

---

## File Layout

A MooFile database is two files:

```
mydata.bson       ← append-only document store, source of truth
mydata.bson.meta  ← index configuration (which fields are indexed)
```

Indexes are **never persisted**. They are rebuilt in memory on every open by scanning the BSON file. The BSON file is always the source of truth. If the index file is lost or corrupt, delete it and reopen — it will be rebuilt.

### Why No Persistent Indexes?

- Eliminates crash recovery complexity entirely
- Eliminates WAL complexity entirely  
- Simplifies the codebase dramatically
- For the target dataset size (MBs to low GBs), rebuild on open is fast enough
- Correctness is guaranteed — the index can never be out of sync with the data

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
  "created_at": "2025-01-01T00:00:00Z"
}
```

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

On open, MooFile scans the BSON file and builds in-memory indexes using Python's `sortedcontainers.SortedDict`. Do not implement a custom B-tree.

```python
# internal structure (simplified)
self._indexes = {
    "email": SortedDict(),   # value → [list of _ids]
    "age":   SortedDict(),   # value → [list of _ids]
}
self._documents = {}         # _id → (file_offset, document_dict)
```

Index updates on write are synchronous — after every insert/update/delete, the in-memory index is updated immediately before returning to the caller.

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

# open or create
db = Collection("mydata.bson", indexes=["email", "age", "status"])

# read-only mode
db = Collection("mydata.bson", readonly=True)
```

On open:
1. Create files if they do not exist
2. Scan BSON file, build in-memory indexes
3. Ready

### Insert

```python
doc = db.insert({"name": "alice", "age": 30, "email": "alice@example.com"})
# returns the document with _id populated

docs = db.insert_many([{...}, {...}, {...}])
# returns list of inserted documents
```

### Query

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

### Method Chain API

`.find()` returns a lazy `Query` object. Results are not materialized until a terminal method is called.

```python
db.find({"status": "active"})
  .sort("age", descending=True)
  .skip(20)
  .limit(10)
  .to_list()

# terminal methods
.to_list()       # → list of dicts
.to_df()         # → pandas DataFrame (pandas optional dependency)
.first()         # → first dict or None
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

Update operators in v1: `$set`, `$unset`, `$inc`. That covers 95% of real usage.

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

That is the complete v1 filter surface. No `$regex`, no `$text`, no `$where`, no geospatial.

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

When a filter references an indexed field, MooFile uses the index. Otherwise it falls back to a full document scan. No query planner — simple rule: if the top-level filter key matches an index, use it.

```python
# indexed — fast
db.find({"email": "alice@example.com"})
db.find({"age": {"$gt": 25}})

# not indexed — full scan
db.find({"name": "alice"})
db.find({"address.city": "Toronto"})  # nested fields not indexed in v1
```

Nested field indexing is explicitly out of scope for v1. Index top-level fields only.

### Execution Order

For a chained query:

```
filter → unwind (if any) → group + agg → sort → skip → limit → project
```

Filtering always runs first. Limit and skip always run last.

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

MooFile targets a minimal dependency footprint:

| Dependency | Purpose | Required |
|---|---|---|
| `bson` | BSON encode/decode | Yes |
| `sortedcontainers` | In-memory sorted index | Yes |
| `pandas` | `.to_df()` terminal method | Optional |

No database drivers. No async frameworks. No C extensions beyond what `bson` brings.

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

Single-threaded only in v1. One `Collection` instance per process per file. Concurrent reads from multiple threads are safe. Concurrent writes are not protected and will corrupt the file.

If you need multi-threaded writes, serialize them with a lock at the application layer. MooFile will not do this for you.

---

## Implementation Notes

### Simplicity Over Performance

When in doubt, choose the simpler implementation. MooFile's primary value is being a small, readable codebase that a developer can understand in under an hour. If a performance optimization requires more than ~30 lines of non-obvious code, skip it.

### Approximate Code Budget (v1)

```
moofile/
  __init__.py         exports
  collection.py       main Collection class, open/close, scan
  storage.py          BSON file append, read, compaction
  index.py            in-memory index build and update
  query.py            Query builder, filter evaluation
  operators.py        $gt, $lt, $in, etc. — one function each
  aggregation.py      group/agg execution
  errors.py           exception classes

~ 800-1200 lines total
```

If the codebase exceeds ~1500 lines before tests, scope has crept.

### Recommended Libraries

- Use `bson` from PyMongo — do not implement BSON encoding
- Use `sortedcontainers.SortedDict` — do not implement a B-tree
- Use Python's `mmap` module for the optional future persistent index path
- Use `os.replace()` for atomic file rename during compaction

### What Not to Build in v1

- Custom BSON parser
- Custom B-tree
- WAL
- Query optimizer / planner
- Nested field indexing
- `$lookup`
- Network interface
- Async API
- CLI tool (nice to have, not v1)

---

## Future Directions (Not v1)

- Persistent mmap'd index file for faster open on large datasets
- CLI inspection tool: `moofile inspect mydata.bson`
- C core with Python/JS/Go bindings
- Optional schema enforcement mode
- Nested field indexes
- `$unwind` for array flattening
- Watch/change stream for local reactivity

---

## Name

**MooFile**. It is a cow. Cows are not fast or scalable but they are reliable, friendly, and everyone likes them. This is that.
