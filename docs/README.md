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

## API Reference

### Opening a Collection

```python
db = Collection(
    path,              # path to the .bson file (created if absent)
    indexes=[],        # list of top-level field names to index
    readonly=False,    # True to prevent all writes
    schema=None,       # optional hints, ignored in v1
)
```

Use as a context manager for automatic cleanup:

```python
with Collection("data.bson", indexes=["email"]) as db:
    db.insert({"email": "bob@example.com"})
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
db = Collection("data.bson", indexes=["email", "age"])

# Uses the 'email' index — O(log n) lookup
db.find({"email": "alice@example.com"})

# Uses the 'age' index — O(log n) range scan
db.find({"age": {"$gt": 25}})

# Full scan — 'name' is not indexed
db.find({"name": "Alice"})
```

Index rules:
- Only **top-level** fields can be indexed (no nested paths in v1).
- `_id` is always available for fast lookup regardless of declared indexes.
- Indexes are rebuilt in memory on every open.
- Declaring additional indexes is cheap — add them to the `indexes=` parameter and reopen.

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

## Examples

See the [`examples/`](../examples/) directory:

| File | Description |
|---|---|
| `basic_crud.py` | Insert, find, update, delete — the complete CRUD tour |
| `contacts_app.py` | A realistic contacts manager with filtering and updates |
| `analytics.py` | Sales analytics with `group().agg()` pipeline |
| `event_log.py` | Structured event log with time-based purging and compaction |
