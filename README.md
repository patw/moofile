# MooFile

![MooFile](images/moofile-banner.png)

> A lightweight, embedded, single-file document store with a developer-friendly query API.  
> No server. No infrastructure. Just a file and a library.  
> **🦀 Rust core available — 2-24× faster than pure Python.**

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
        "embedding": [0.1, 0.2, ...]
    })

    # Traditional query
    results = db.find({"age": {"$gt": 25}}).sort("age").to_list()
    
    # Vector similarity search
    similar = db.find({}).vector_search("embedding", query_vector, limit=5).to_list()
    
    # BM25 text search
    text = db.find({}).text_search("content", "machine learning", limit=10).to_list()
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
| Developer API | ✗ (SQL) | ✓ (raw) | ✓ | **✓** |
| Single-file portable | ✓ | ✓ | ✗ | **✓** |
| **Rust core available** | ✗ | ✗ | ✗ | **✓ (v0.3+)** |

**Target dataset size:** megabytes to single-digit gigabytes.

---

## Installation

```bash
pip install moofile
```

This installs the pure-Python version which works everywhere. See [Native install](#native-install-rust-core) below for the Rust-powered version.

---

## Quick Start

```python
from moofile import Collection

db = Collection("users.bson", 
                indexes=["email", "status"],
                text_indexes=["bio"],
                vector_indexes={"profile_vec": 128})

# Insert
alice = db.insert({"name": "Alice", "email": "a@ex.com", "age": 30, "status": "active"})
db.insert_many([...])

# Query
active = db.find({"status": "active"}).to_list()
young  = db.find({"age": {"$lt": 30}}).sort("age").to_list()
one    = db.find_one({"email": "alice@example.com"})

# Vector search
similar = db.find({}).vector_search("profile_vec", query_vector, limit=3).to_list()
for doc, score in similar:
    print(f"{doc['name']}: {score:.3f}")

# Text search
results = db.find({}).text_search("bio", "machine learning", limit=5).to_list()

# Update & Delete
db.update_one({"email": "a@ex.com"}, set={"age": 31})
db.update_many({"status": "trial"}, set={"status": "expired"})
db.delete_one({"email": "c@ex.com"})
db.delete_many({"status": "expired"})
```

---

## Native Install (Rust Core)

When the Rust native extension is installed, `import moofile` transparently uses it — same API, 2-24× faster.

### From source (requires Rust)

```bash
# Install Rust: https://rustup.rs
curl --proto '=https' --tls v1.2 -sSf https://sh.rustup.rs | sh

# Build and install with native extension
pip install maturin
cd moofile
maturin develop --release
```

### Prebuilt wheels

Coming soon — GitHub Actions CI will build platform wheels for:
| Platform | Architectures |
|---|---|
| Linux | x86_64 (manylinux) |
| macOS | x86_64, ARM64 (Apple Silicon) |
| Windows | x86_64 |

In the meantime, `pip install moofile` always works (pure Python fallback).

---

## CLI Tools

| Tool | Description |
|---|---|
| `moosh` | Interactive Python shell with `db` pre-bound |
| `moo2json` | Export/import to/from JSON |
| `moo2mongo` | Export/import to/from MongoDB |
| `moo2sqlite` | Export/import to/from SQLite |

```bash
moosh users.bson --indexes email,age
moo2json users.bson users.json
moo2json --import users.json users.bson --indexes email
moo2mongo users.bson --uri mongodb://localhost/mydb --collection users
moo2sqlite users.bson users.db --table people
```

---

## Full Documentation

- **[Specification](moofile-spec.md)** — file format, architecture, design decisions
- **[API Reference](docs/README.md)** — complete Python API, filter operators, aggregation
- **[bench_native.py](bench_native.py)** — Python vs Rust head-to-head benchmark

---

## Development

```bash
# Pure-Python tests (always run)
pytest tests/ -v

# Cross-implementation tests
pytest tests-cross/ -v

# Rust core tests
cd core && cargo test

# Rust benchmark
cd core && cargo run --example bench --release

# Python vs Rust benchmark
PYTHONPATH=. python bench_native.py
```

### Project layout

```
moofile/
├── core/                    # Rust engine (cargo build)
│   ├── src/{lib,storage,index,query,text,cache,errors}.rs
│   └── examples/bench.rs    # Pure-Rust benchmark
├── bindings/python/         # PyO3 binding (maturin build)
│   └── src/lib.rs
├── moofile/                 # Python package
│   ├── __init__.py          # Auto-detects Rust, falls back to Python
│   ├── _rust_adapter.py     # Adapts NativeCollection → Collection API
│   ├── collection.py        # Pure-Python reference implementation
│   ├── query.py, index.py, storage.py, ...
│   └── cli/                 # moosh, moo2json, moo2mongo, moo2sqlite
├── tests/                   # Python test suite
├── tests-cross/             # Cross-implementation validation
└── pyproject.toml
```

---

## License

MIT — see [LICENSE](LICENSE).
