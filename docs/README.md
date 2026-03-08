# MooFile

> A lightweight, embedded, single-file document store with a developer-friendly query API.
> No server. No infrastructure. Just a file and a library.

```python
from moofile import Collection, count, mean

with Collection("mydata.bson", indexes=["email", "age"]) as db:
    db.insert({"name": "Alice", "email": "alice@example.com", "age": 30})

    result = (
        db.find({"age": {"$gt": 25}})
        .sort("age", descending=True)
        .limit(10)
        .to_list()
    )
```

---

## Why MooFile?

| | SQLite | JSON file | MongoDB | **MooFile** |
|---|---|---|---|---|
| No server | ✓ | ✓ | ✗ | **✓** |
| Document-oriented | ✗ | ✓ | ✓ | **✓** |
| Indexes | ✓ | ✗ | ✓ | **✓** |
| Developer-friendly API | ✗ (SQL) | ✓ (raw Python) | ✓ | **✓** |
| Single-file portability | ✓ | ✓ | ✗ | **✓** |

MooFile is the right tool when you want MongoDB-style ergonomics without running a server: local tooling, embedded applications, tests, small datasets, single-process services.

**Target dataset size:** megabytes to single-digit gigabytes.

---

## Installation

```bash
pip install moofile
# or, with pandas support for .to_df():
pip install "moofile[pandas]"
```

**Dependencies:**  `pymongo` (for BSON encoding) and `sortedcontainers`.

---

## Quick Start

```python
from moofile import Collection

# Open or create a collection.  Indexes are declared here.
db = Collection("users.bson", indexes=["email", "status"])

# Insert
alice = db.insert({"name": "Alice", "email": "alice@example.com", "age": 30, "status": "active"})
print(alice["_id"])   # auto-generated 24-char hex string

db.insert_many([
    {"name": "Bob",   "email": "bob@example.com",  "age": 22, "status": "trial"},
    {"name": "Carol", "email": "carol@example.com", "age": 40, "status": "active"},
])

# Query
active = db.find({"status": "active"}).to_list()
young  = db.find({"age": {"$lt": 30}}).sort("age").to_list()
one    = db.find_one({"email": "alice@example.com"})

# Update
db.update_one({"email": "alice@example.com"}, set={"age": 31})
db.update_many({"status": "trial"}, set={"status": "expired"})

# Delete
db.delete_one({"email": "carol@example.com"})
db.delete_many({"status": "expired"})

# Always close when done (or use a context manager)
db.close()
```

---

## Key Differences & Gotchas

MooFile's API looks like MongoDB but has important differences that can trip up coding agents:

- **Updates use Python keyword args**: `update_one(filter, set={...}, inc={...})` NOT `update_one(filter, {"$set": {...}})`
- **update_one/replace_one are strict**: Raise `DocumentNotFoundError` if no match (MongoDB silently no-ops)
- **delete_one returns bool**: Returns `True`/`False`, NOT a result object like MongoDB
- **Vector/text search return tuples**: `[(doc, score), ...]` NOT plain document lists like `find()`
- **Single-threaded only**: No concurrent write safety — serialize writes at application layer
- **No nested field indexes**: Only top-level fields can be indexed (no `"user.name"` paths)
- **No joins or $lookup**: No cross-document references or aggregation pipelines
- **No async API**: All operations are synchronous
- **Filters vs updates**: Filters use MongoDB-style `{"field": {"$gt": 5}}` but updates use kwargs `set={"field": value}`
- **Explicit compaction**: Dead records accumulate until you call `db.compact()`

---

## File Layout

A MooFile database is two files:

```
users.bson        ← append-only document store, source of truth
users.bson.meta   ← index configuration (JSON, human-readable)
```

The `.meta` file is a small JSON file:

```json
{
  "version": 1,
  "indexes": ["email", "status"],
  "created_at": "2025-01-01T00:00:00+00:00"
}
```

Indexes are **never persisted** — they are rebuilt in memory on every open by scanning the BSON file.  If the `.meta` file is lost, delete it and reopen; the data is always safe in the `.bson` file.

---

## All Imports

```python
from moofile import (
    Collection,
    count, sum, mean, min, max, collect, first, last,
    MooFileError, DuplicateKeyError, DocumentNotFoundError, ReadOnlyError,
)
```

---

## API Reference

### Return Types Quick Reference

| Method | Returns |
|---|---|
| `find().to_list()` | `list[dict]` |
| `find().first()` | `dict \| None` |
| `find_one()` | `dict \| None` |
| `find().count()` | `int` (0 if no matches) |
| `count()` | `int` (0 if no matches) |
| `exists()` | `bool` |
| `vector_search().to_list()` | `list[tuple[dict, float]]` |
| `text_search().to_list()` | `list[tuple[dict, float]]` |
| `insert()` | `dict` (with _id populated) |
| `insert_many()` | `list[dict]` |
| `update_one()` | `bool` (always True, raises DocumentNotFoundError if no match) |
| `update_many()` | `int` (count of updated docs) |
| `replace_one()` | `bool` (always True, raises DocumentNotFoundError if no match) |
| `delete_one()` | `bool` |
| `delete_many()` | `int` |

### Opening a Collection

```python
db = Collection(
    path,                        # path to the .bson file (created if absent)
    indexes=[],                  # list of top-level field names to index
    vector_indexes={},           # dict: field -> vector_dimension
    text_indexes=[],             # list of field names for full-text search
    readonly=False,              # True to prevent all writes
    schema=None,                 # optional hints, ignored in v1
)
```

Use as a context manager for automatic cleanup:

```python
with Collection("data.bson", 
                indexes=["email"], 
                vector_indexes={"embedding": 384},
                text_indexes=["title", "content"]) as db:
    db.insert({
        "email": "bob@example.com",
        "title": "Machine Learning Guide",
        "content": "Introduction to ML algorithms",
        "embedding": [0.1, 0.2, ...]  # 384-dimensional vector
    })
```

---

### Insert

```python
doc  = db.insert({"name": "alice", "age": 30})
# → dict with _id populated

docs = db.insert_many([{...}, {...}])
# → list of dicts with _id populated
```

- If `_id` is absent, a random 24-char hex string is generated.
- Providing a custom `_id` of any hashable type is allowed.
- `DuplicateKeyError` is raised if `_id` already exists.

### _id Behavior

- **Auto-generated type**: 24-character hex string (e.g., `"507f1f77bcf86cd799439011"`)
- **Custom _id**: Any hashable type allowed (`str`, `int`, `tuple`, etc.) 
- **Always present**: `_id` is populated on all returned documents after insert
- **Uniqueness**: Enforced at insert time — duplicates raise `DuplicateKeyError`
- **Preserved**: `_id` cannot be changed by updates, always preserved during `replace_one()`

---

### Find

```python
# Return all matching documents as a list
db.find({"status": "active"}).to_list()

# Return first match or None
db.find_one({"email": "alice@example.com"})

# Count without materialising documents
db.count({"status": "active"})

# Existence check
db.exists({"email": "alice@example.com"})
```

`.find()` returns a lazy `Query` object.  No work is done until a terminal method is called.

### Empty/Edge Case Behavior

- **find() with no matches**: `to_list()` → `[]`, `first()` → `None`, `count()` → `0`
- **find_one() with no matches**: → `None`
- **count()/exists() with no matches**: → `0` / `False`
- **update_many() with no matches**: → `0` (count of updated docs, not an error)
- **group().agg() with no documents**: → `[]` (empty list, no group rows created)

---

### Query Chains

```python
results = (
    db.find({"status": "active"})
    .sort("age", descending=True)
    .skip(20)
    .limit(10)
    .to_list()
)
```

**Builder methods** (each returns a new `Query`):

| Method | Description |
|---|---|
| `.sort(field, descending=False)` | Sort by field |
| `.skip(n)` | Skip the first n results |
| `.limit(n)` | Return at most n results |
| `.group(field)` | Group results by field |
| `.agg(*funcs)` | Apply aggregation functions to each group |

**Terminal methods** (trigger execution):

| Method | Returns |
|---|---|
| `.to_list()` | `list[dict]` |
| `.first()` | `dict` or `None` |
| `.count()` | `int` |
| `.to_df()` | `pandas.DataFrame` (requires pandas) |

---

### Filter Operators

#### Comparison

```python
{"age": 30}                        # implicit $eq
{"age": {"$eq": 30}}               # explicit $eq
{"age": {"$ne": 30}}               # not equal
{"age": {"$gt": 25}}               # greater than
{"age": {"$gte": 25}}              # greater than or equal
{"age": {"$lt": 40}}               # less than
{"age": {"$lte": 40}}              # less than or equal
{"age": {"$gte": 25, "$lt": 40}}   # range
{"status": {"$in":  ["active", "trial"]}}
{"status": {"$nin": ["expired", "archived"]}}
```

#### Logical

```python
{"$and": [{"age": {"$gt": 25}}, {"status": "active"}]}
{"$or":  [{"status": "active"}, {"status": "trial"}]}
{"$not": {"status": "archived"}}
```

#### Element

```python
{"email": {"$exists": True}}    # field must be present
{"email": {"$exists": False}}   # field must be absent
```

#### Array

```python
# At least one element of 'tags' equals "vip"
{"tags": {"$elemMatch": {"$eq": "vip"}}}

# At least one element of 'scores' is > 90
{"scores": {"$elemMatch": {"$gt": 90}}}

# At least one element of 'items' matches a sub-document filter
{"items": {"$elemMatch": {"product": "xyz", "qty": {"$gt": 5}}}}
```

---

### Update

```python
# Update first match — raises DocumentNotFoundError if no match
db.update_one(
    where={"email": "alice@example.com"},
    set={"age": 31},           # $set: set field values
    unset=["temp_field"],      # $unset: remove fields
    inc={"login_count": 1},    # $inc: increment numeric fields
)

# Update all matches — returns count of updated documents
n = db.update_many(
    where={"status": "trial"},
    set={"status": "expired"},
)

# Replace entire document — preserves _id, raises DocumentNotFoundError if no match
db.replace_one({"_id": "abc123"}, {"name": "Alice", "age": 32})
```

---

### Delete

```python
# Delete first match — returns True if deleted, False if nothing matched
db.delete_one({"_id": "abc123"})

# Delete all matches — returns count
n = db.delete_many({"status": "archived"})
```

---

### Aggregation

Group documents and compute aggregate statistics:

```python
from moofile import count, sum, mean, min, max, collect, first, last

results = (
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
)
```

**Aggregation functions:**

| Function | Output field | Description |
|---|---|---|
| `count()` | `"count"` | Number of documents in group |
| `sum("field")` | `"sum_field"` | Sum of field values |
| `mean("field")` | `"mean_field"` | Arithmetic mean of field values |
| `min("field")` | `"min_field"` | Minimum field value |
| `max("field")` | `"max_field"` | Maximum field value |
| `collect("field")` | `"collect_field"` | List of all values |
| `first("field")` | `"first_field"` | First value encountered |
| `last("field")` | `"last_field"` | Last value encountered |

Documents where the aggregated field is absent are excluded from the computation (but still counted by `count()`).

---

### Vector Search

Vector similarity search using cosine similarity. Requires numpy.

```python
# Setup collection with vector index
db = Collection("docs.bson", vector_indexes={"embedding": 384})

# Insert documents with vector embeddings
db.insert({
    "title": "Machine Learning",
    "content": "Introduction to ML algorithms",
    "embedding": [0.1, 0.2, 0.3, ...]  # 384-dimensional vector
})

# Perform vector search
query_vector = [0.15, 0.25, 0.35, ...]  # Your query embedding
results = db.find({}).vector_search("embedding", query_vector, limit=10).to_list()

# Results are (document, similarity_score) tuples
for doc, score in results:
    print(f"{doc['title']}: {score:.3f}")

# Combine with filters
results = (
    db.find({"category": "AI"})
    .vector_search("embedding", query_vector, limit=5)
    .to_list()
)
```

**Vector search features:**
- Uses cosine similarity (values from -1 to 1, higher is more similar)
- Brute-force search rebuilds vector arrays on collection open
- Invalid vectors (wrong dimension, non-numeric) are ignored
- Pre-filtering with `.find()` conditions is supported

---

### Text Search

BM25 full-text search with Porter stemming. Requires snowballstemmer.

```python
# Setup collection with text index
db = Collection("docs.bson", text_indexes=["title", "content"])

# Insert documents with text content
db.insert({
    "title": "Machine Learning Introduction",
    "content": "Learn about supervised and unsupervised learning algorithms.",
    "category": "AI"
})

# Perform text search
results = db.find({}).text_search("content", "machine learning", limit=10).to_list()

# Results are (document, relevance_score) tuples
for doc, score in results:
    print(f"{doc['title']}: {score:.3f}")

# Combine with filters
results = (
    db.find({"category": "AI"})
    .text_search("content", "neural networks", limit=5)
    .to_list()
)

# Search specific fields
title_results = db.find({}).text_search("title", "introduction").to_list()
```

**Text search features:**
- BM25 scoring algorithm with stemming (higher scores = more relevant)
- Porter stemming handles word variations ("running" matches "run", "runs")
- Tokenizes on word boundaries, ignores punctuation
- Pre-filtering with `.find()` conditions is supported
- Only processes string fields (non-strings are ignored)

---

### Utility

```python
# Database statistics
s = db.stats()
# → {
#     "documents":       42150,
#     "dead_records":    3201,
#     "file_size_bytes": 8421000,
#     "dead_ratio":      0.07,
# }

# Compact the file (remove dead records)
db.compact()

# Rebuild indexes from scratch (useful after manual file manipulation)
db.reindex()

# Explicit close
db.close()
```

**When to compact:** when `dead_ratio` exceeds ~0.30 (30 %).  Compaction is always explicit — MooFile never compacts automatically.

---

### Error Handling

```python
from moofile import (
    MooFileError,           # base exception
    DuplicateKeyError,      # _id conflict on insert
    DocumentNotFoundError,  # update_one / replace_one with no match
    ReadOnlyError,          # write attempted on read-only collection
)
```

All MooFile exceptions are subclasses of `MooFileError`.

---

## Index Usage

MooFile uses an index automatically when a filter's top-level field is indexed:

```python
db = Collection("data.bson", 
                indexes=["email", "age"],
                vector_indexes={"embedding": 384},
                text_indexes=["content"])

# Regular field indexes — O(log n) lookup
db.find({"email": "alice@example.com"})
db.find({"age": {"$gt": 25}})

# Vector search — O(n) cosine similarity
db.find({}).vector_search("embedding", query_vector)

# Text search — BM25 scoring 
db.find({}).text_search("content", "machine learning")

# Full scan — 'name' is not indexed
db.find({"name": "Alice"})
```

Index rules:
- **Regular indexes**: Only top-level fields (no nested paths in v1)
- **Vector indexes**: Brute-force cosine similarity on all vectors
- **Text indexes**: BM25 scoring with Porter stemming
- `_id` is always available for fast lookup regardless of declared indexes
- All indexes are rebuilt in memory on every open
- Declaring additional indexes is cheap — just reopen the collection

---

## How It Works

The `.bson` file is **append-only**. Every insert, update, and delete appends a new record — nothing is ever modified in place.

```
[4 bytes: payload length] [1 byte: record type] [BSON payload]
```

Record types:
- `0x01` live document
- `0x02` tombstone (delete marker)
- `0x03` replacement (update marker)

On open, MooFile scans the file once from start to finish.  The last record for any given `_id` wins.  In-memory indexes are built from the live document set.

If the file is truncated mid-write (crash during a write), MooFile detects and removes the incomplete trailing record on the next open.  You lose at most the last in-flight write; all prior records are safe.

---

## Thread Safety

Single-threaded only.  Concurrent reads are safe.  Concurrent writes are not protected — serialise writes at the application layer if needed.

---

## Non-Goals (v1)

- No server or network interface
- No replication or clustering
- No multi-process concurrent writes
- No `$lookup` / joins
- No nested field indexes
- No async API

---

## CLI Tools

Four command-line tools are installed with the package.

### moosh — interactive shell

Opens a `.bson` collection and starts a Python REPL with `db` pre-bound to the `Collection` and all aggregation helpers in scope. Good for quick inspection, one-off queries, or data fixes.

```
moosh [--indexes FIELDS] [--readonly] <collection.bson>
```

| Flag | Description |
|---|---|
| `--indexes FIELDS` | Comma-separated fields to index (e.g. `email,age`) |
| `--readonly` | Open the collection read-only |

```bash
moosh users.bson
moosh users.bson --indexes email,age --readonly
```

Inside the shell:

```python
>>> db.find({"age": {"$gt": 25}}).sort("age").to_list()
>>> db.insert({"name": "Dave", "email": "dave@example.com"})
>>> db.find().group("status").agg(count(), mean("age")).to_list()
>>> exit()
```

Available names: `db`, `count`, `sum`, `mean`, `min`, `max`, `collect`, `first`, `last`, and the exception classes (`MooFileError`, `DuplicateKeyError`, `DocumentNotFoundError`, `ReadOnlyError`).

---

### moo2json

Export/import between a `.bson` collection and a JSON file (array format) or NDJSON stream.

```
moo2json [--import] [--indexes FIELDS] [--quiet] <src> <dst>
```

| Flag | Description |
|---|---|
| *(no flag)* | Export: `<collection.bson>` → `<output.json>` (use `-` for stdout) |
| `--import` | Import: `<input.json>` → `<collection.bson>` (use `-` for stdin) |
| `--indexes FIELDS` | Comma-separated fields to index on import (e.g. `email,age`) |
| `--quiet` | Suppress progress output |

```bash
# Export all documents to a JSON file
moo2json users.bson users.json

# Export to stdout (pipe-friendly)
moo2json users.bson -

# Import from a JSON array or NDJSON file
moo2json --import users.json users.bson --indexes email,age

# Import from stdin (e.g. from another process)
cat users.json | moo2json --import - users.bson
```

---

### moo2mongo

Export/import between a `.bson` collection and a MongoDB collection.

```
moo2mongo [--import] --uri <uri> --collection <name> [--drop] [--indexes FIELDS] [--quiet] <collection.bson>
```

| Flag | Description |
|---|---|
| *(no flag)* | Export: MooFile → MongoDB |
| `--import` | Import: MongoDB → MooFile |
| `--uri` | MongoDB connection URI (must include database name, e.g. `mongodb://localhost/mydb`) |
| `--collection` | MongoDB collection name |
| `--drop` | Drop target MongoDB collection before exporting |
| `--indexes FIELDS` | Comma-separated fields to index on import (MooFile side) |
| `--quiet` | Suppress progress output |

```bash
# Export to MongoDB
moo2mongo users.bson --uri mongodb://localhost/mydb --collection users

# Export with drop (replace existing data)
moo2mongo users.bson --uri mongodb://localhost/mydb --collection users --drop

# Import from MongoDB
moo2mongo --import users.bson --uri mongodb://localhost/mydb --collection users --indexes email
```

---

### moo2sqlite

Export/import between a `.bson` collection and a SQLite database table.

Nested documents and arrays are flattened to JSON strings in SQLite; they are restored automatically on import.

```
moo2sqlite [--import] [--table <name>] [--drop] [--indexes FIELDS] [--quiet] <src> <dst>
```

| Flag | Description |
|---|---|
| *(no flag)* | Export: `<collection.bson>` → `<database.sqlite>` |
| `--import` | Import: `<database.sqlite>` → `<collection.bson>` |
| `--table` | SQLite table name (default: derived from `.bson` filename stem) |
| `--drop` | Drop existing table before export |
| `--indexes FIELDS` | Comma-separated fields to index on import (MooFile side) |
| `--quiet` | Suppress progress output |

```bash
# Export to SQLite (table name: "users", derived from "users.bson")
moo2sqlite users.bson users.db

# Export to a named table, replacing existing data
moo2sqlite users.bson users.db --table people --drop

# Import from SQLite
moo2sqlite --import users.db users.bson --table people --indexes email,age
```

All columns are stored as `TEXT`. The `_id` field becomes the `TEXT PRIMARY KEY`.

---

## Examples

See the [`examples/`](../examples/) directory:

| File | Description |
|---|---|
| `basic_crud.py` | Insert, find, update, delete — the complete CRUD tour |
| `contacts_app.py` | A realistic contacts manager with filtering and updates |
| `analytics.py` | Sales analytics with `group().agg()` pipeline |
| `event_log.py` | Structured event log with time-based purging and compaction |
| `import_export.py` | CLI tools in action: JSON, SQLite, and MongoDB round-trips |

### End-to-End Examples

#### 1. Filter + Vector Search

```python
from moofile import Collection
import numpy as np

with Collection("docs.bson", vector_indexes={"embedding": 384}) as db:
    # Pre-filter by category, then find similar documents by embedding
    query_vector = np.random.randn(384).tolist()
    
    results = (
        db.find({"category": "AI", "published": True})
        .vector_search("embedding", query_vector, limit=5)
        .to_list()
    )
    
    # Unpack tuples: each result is (document, similarity_score)
    for doc, similarity in results:
        print(f"{doc['title']}: {similarity:.3f}")
```

#### 2. Full Analytics Pipeline  

```python
from moofile import Collection, count, mean, sum, max

with Collection("sales.bson", indexes=["region", "date"]) as db:
    # Complex analytics: filter → group → aggregate → sort → limit
    monthly_stats = (
        db.find({
            "date": {"$gte": "2024-01-01"}, 
            "status": "completed"
        })
        .group("region")
        .agg(
            count(),                  # Total transactions
            sum("amount"),           # Revenue per region
            mean("amount"),          # Average order value  
            max("date")              # Latest transaction
        )
        .sort("sum_amount", descending=True)
        .limit(10)
        .to_list()
    )
    
    for row in monthly_stats:
        print(f"Region: {row['region']}")
        print(f"  Revenue: ${row['sum_amount']:,.2f}")
        print(f"  Transactions: {row['count']}")
        print(f"  Avg Order: ${row['mean_amount']:.2f}")
```

#### 3. Data Lifecycle Management

```python
from moofile import Collection, DocumentNotFoundError

with Collection("users.bson", indexes=["email", "last_login"]) as db:
    # Insert new user
    user = db.insert({
        "email": "alice@example.com",
        "name": "Alice Smith", 
        "credits": 100,
        "last_login": "2024-01-15"
    })
    user_id = user["_id"]  # Capture the auto-generated ID
    
    # Update user activity  
    try:
        db.update_one(
            {"_id": user_id}, 
            set={"last_login": "2024-01-20"},
            inc={"credits": -25}
        )
        print("User updated successfully")
    except DocumentNotFoundError:
        print("User not found!")
    
    # Find updated user
    updated_user = db.find_one({"email": "alice@example.com"})
    if updated_user:
        print(f"Credits remaining: {updated_user['credits']}")
        
    # Archive inactive users
    archived_count = db.update_many(
        {"last_login": {"$lt": "2024-01-01"}},
        set={"status": "archived"}
    )
    print(f"Archived {archived_count} inactive users")
```
