# MooFile

![MooFile](images/moofile-banner.png)

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

**Dependencies:** `pymongo` (for BSON encoding) and `sortedcontainers`.

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

## Full Documentation

See [`docs/README.md`](docs/README.md) for the complete API reference, including:

- Filter operators (`$gt`, `$lt`, `$in`, `$and`, `$or`, `$elemMatch`, ...)
- Query chains (`.sort()`, `.skip()`, `.limit()`, `.group()`, `.agg()`)
- Aggregation functions (`count`, `sum`, `mean`, `min`, `max`, `collect`, `first`, `last`)
- Update operators (`set`, `unset`, `inc`)
- Index usage and performance notes
- File format internals

---

## Examples

See the [`examples/`](examples/) directory:

| File | Description |
|---|---|
| `basic_crud.py` | Insert, find, update, delete — the complete CRUD tour |
| `contacts_app.py` | A realistic contacts manager with filtering and updates |
| `analytics.py` | Sales analytics with `group().agg()` pipeline |
| `event_log.py` | Structured event log with time-based purging and compaction |

---

## License

MIT — see [LICENSE](LICENSE).
