# MooFile

![MooFile](images/moofile-banner.png)

> A lightweight, embedded, single-file document store with a developer-friendly query API.
> No server. No infrastructure. Just a file and a library.

```python
from moofile import Collection, count, mean

with Collection("mydata.bson", 
                indexes=["email", "age"],
                vector_indexes={"embedding": 384},
                text_indexes=["content"]) as db:
    
    db.insert({
        "name": "Alice", 
        "email": "alice@example.com", 
        "age": 30,
        "content": "Machine learning and data science expert",
        "embedding": [0.1, 0.2, ...]  # 384-dimensional vector
    })

    # Traditional query
    results = db.find({"age": {"$gt": 25}}).sort("age").to_list()
    
    # Vector similarity search
    query_vector = [0.15, 0.25, ...]
    similar_docs = db.find({}).vector_search("embedding", query_vector, limit=5).to_list()
    
    # Text search
    text_results = db.find({}).text_search("content", "machine learning", limit=10).to_list()
```

---

## Why MooFile?

| | SQLite | JSON file | MongoDB | **MooFile** |
|---|---|---|---|---|
| No server | ✓ | ✓ | ✗ | **✓** |
| Document-oriented | ✗ | ✓ | ✓ | **✓** |
| Indexes | ✓ | ✗ | ✓ | **✓** |
| Vector search | ✗ | ✗ | ✓ (Atlas) | **✓** |
| Text search | ✓ (FTS) | ✗ | ✓ | **✓** |
| Developer-friendly API | ✗ (SQL) | ✓ (raw Python) | ✓ | **✓** |
| Single-file portability | ✓ | ✓ | ✗ | **✓** |

MooFile is the right tool when you want MongoDB-style ergonomics with vector and text search without running a server: local tooling, embedded applications, tests, small datasets, single-process services.

**Target dataset size:** megabytes to single-digit gigabytes.  
**New in v0.2:** Vector similarity search and BM25 text search with lightweight dependencies.

---

## Installation

```bash
pip install moofile
# or, with pandas support for .to_df():
pip install "moofile[pandas]"
```

**Dependencies:** `pymongo` (BSON encoding), `sortedcontainers` (indexes), `numpy` (vector search), `snowballstemmer` (text search).

---

## Quick Start

```python
from moofile import Collection

# Open or create a collection with multiple index types
db = Collection("users.bson", 
                indexes=["email", "status"],           # Regular field indexes
                text_indexes=["bio"],                   # Full-text search
                vector_indexes={"profile_vec": 128})    # Vector similarity

# Insert
alice = db.insert({"name": "Alice", "email": "alice@example.com", "age": 30, "status": "active"})
print(alice["_id"])   # auto-generated 24-char hex string

db.insert_many([
    {
        "name": "Bob", "email": "bob@example.com", "age": 22, "status": "trial",
        "bio": "Software engineer interested in machine learning",
        "profile_vec": [0.1, 0.2, ...] # 128-dimensional vector
    },
    {
        "name": "Carol", "email": "carol@example.com", "age": 40, "status": "active",
        "bio": "Data scientist with expertise in deep learning",
        "profile_vec": [0.3, 0.4, ...] # 128-dimensional vector  
    },
])

# Query
active = db.find({"status": "active"}).to_list()
young  = db.find({"age": {"$lt": 30}}).sort("age").to_list()
one    = db.find_one({"email": "alice@example.com"})

# Vector search - find similar profiles
query_vector = [0.15, 0.25, ...]  # Your query vector
similar_profiles = db.find({}).vector_search("profile_vec", query_vector, limit=3).to_list()
for doc, similarity in similar_profiles:
    print(f"{doc['name']}: {similarity:.3f}")

# Text search - find people by bio content  
ml_experts = db.find({}).text_search("bio", "machine learning", limit=5).to_list()
for doc, relevance in ml_experts:
    print(f"{doc['name']}: {relevance:.3f}")

# Combined search - vector search within filtered results
active_similar = db.find({"status": "active"}).vector_search("profile_vec", query_vector).to_list()

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

## CLI Tools

Four command-line utilities are installed alongside the package:

| Tool | Description |
|---|---|
| `moosh` | Interactive Python shell with the collection pre-loaded as `db` |
| `moo2json` | Export/import between a `.bson` collection and a JSON file |
| `moo2mongo` | Export/import between a `.bson` collection and a MongoDB collection |
| `moo2sqlite` | Export/import between a `.bson` collection and a SQLite database |

### moosh — interactive shell

```bash
moosh users.bson
moosh users.bson --indexes email,age
moosh users.bson --readonly
```

Drops into a Python REPL with `db` bound to the open collection and all aggregation helpers (`count`, `sum`, `mean`, ...) in scope.

All tools default to **export** (MooFile → target) and switch to **import** with `--import`.

### moo2json

```bash
# Export to JSON file
moo2json users.bson users.json

# Export to stdout (pipe-friendly)
moo2json users.bson -

# Import from JSON (array or NDJSON), indexing on email
moo2json --import users.json users.bson --indexes email
```

### moo2mongo

```bash
# Export to MongoDB
moo2mongo users.bson --uri mongodb://localhost/mydb --collection users

# Import from MongoDB
moo2mongo --import users.bson --uri mongodb://localhost/mydb --collection users
```

### moo2sqlite

```bash
# Export to SQLite (table name derived from filename: "users")
moo2sqlite users.bson users.db

# Export to a specific table, dropping it first
moo2sqlite users.bson users.db --table people --drop

# Import from SQLite
moo2sqlite --import users.db users.bson --table people --indexes email
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
| `import_export.py` | Demonstration of CLI tools with round-trip import/export testing |
| `document_search.py` | Vector similarity and BM25 text search for semantic document retrieval |

---

## License

MIT — see [LICENSE](LICENSE).
