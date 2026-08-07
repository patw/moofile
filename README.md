# MooFile

![MooFile](images/moofile-banner.png)

> A lightweight, embedded, single-file document store with a developer-friendly query API.  
> No server. No infrastructure. Just a file and a library.  
> **🦀 Rust core available — 2-24× faster than pure Python.**  
> **🧠 On-device autoembedding — local embedding models for semantic search.**  
> **🔀 Multi-process friendly — a background worker and a web app can share one file.**

```python
from moofile import Collection, count, mean

with Collection("mydata.bson", 
                indexes=["email", "age"],
                vector_indexes={"embedding": 1024},
                text_indexes=["content"],
                auto_embed={
                    "content": {
                        "model": "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",
                        "target": "embedding",
                        "precision": "int8",
                    },
                }) as db:
    
    # Insert — auto-embeds content into embedding (int8, 1KB/doc)
    db.insert({
        "name": "Alice", 
        "email": "alice@example.com", 
        "age": 30,
        "content": "Machine learning and data science expert",
    })

    # Traditional query
    results = db.find({"age": {"$gt": 25}}).sort("age").to_list()
    
    # Vector similarity search (raw vector)
    similar = db.find({}).vector_search("embedding", query_vector, limit=5).to_list()
    
    # Semantic search — auto-embeds query text
    similar = db.find({}).semantic("content", "data science", limit=5).to_list()
    
    # BM25 text search
    text = db.find({}).text_search("content", "machine learning", limit=10).to_list()
    
    # Hybrid search — auto-embeds query vector from query text
    results = db.find({}).hybrid_search("content", "content", "data science", None, 10).to_list()
```

---

## Why MooFile?

| | SQLite | JSON file | MongoDB | **MooFile** |
|---|---|---|---|---|
| No server | ✓ | ✓ | ✗ | **✓** |
| Document-oriented | ✗ | ✓ | ✓ | **✓** |
| Indexes | ✓ | ✗ | ✓ | **✓** |
| Vector search | ✗ | ✗ | ✓ (Atlas) | **✓** |
| On-device autoembedding | ✗ | ✗ | ✗ | **✓** |
| Text search | ✓ (FTS) | ✗ | ✓ | **✓** |
| Developer API | ✗ (SQL) | ✓ (raw) | ✓ | **✓** |
| Single-file portable | ✓ | ✓ | ✗ | **✓** |
| Multi-process safe | ✓ | ✗ | ✓ | **✓ (v0.5.2+)** |
| **Rust core available** | ✗ | ✗ | ✗ | **✓ (v0.3+)** |

**Target dataset size:** megabytes to single-digit gigabytes.

---

## Sharing a file between processes

Like SQLite, several processes can keep the same file open — the usual setup
being a long-running worker that writes and a web app that reads:

```python
# worker.py — writes events forever
with Collection("app.bson", indexes=["kind"]) as db:
    for event in stream:
        db.insert({"kind": "event", **event})

# web.py — reads them, and writes the occasional setting
with Collection("app.bson", indexes=["kind"]) as db:
    recent = db.find({"kind": "event"}).sort("_id", descending=True).limit(50).to_list()
    db.insert({"kind": "config", "theme": "dark"})
```

Readers pick up new writes automatically, writes are serialized so nothing is
lost or interleaved, and duplicate `_id`s are caught across processes. Best
suited to one writer with many readers — writes take a brief exclusive lock, so
many simultaneous writers will queue.

---

## Installation

```bash
pip install moofile
```

This installs the pure-Python version which works everywhere. See [Native install](#native-install-rust-core) below for the Rust-powered version.

---

## Quick Start

```python
from datetime import datetime, timezone
from bson import Binary

from moofile import Collection

db = Collection("users.bson", 
                indexes=["email", "status"],
                text_indexes=["bio"],
                vector_indexes={"profile_vec": 128})

# Insert — any BSON type: datetimes, binary, ObjectId, Decimal128, nested docs
alice = db.insert({"name": "Alice", "email": "a@ex.com", "age": 30, "status": "active",
                   "joined": datetime(2025, 1, 15, tzinfo=timezone.utc),
                   "avatar": Binary(b"...")})
db.insert_many([...])

# Query — ranges work on dates too
active = db.find({"status": "active"}).to_list()
young  = db.find({"age": {"$lt": 30}}).sort("age").to_list()
recent = db.find({"joined": {"$gte": datetime(2025, 1, 1, tzinfo=timezone.utc)}}).to_list()
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

### With Autoembedding

```python
from moofile import Collection

# Autoembedding: text in "abstract" is automatically embedded into
# "embedding" on insert, using a local GGUF model (downloaded on first use).
db = Collection("papers.bson",
    indexes=["year", "category"],
    vector_indexes={"embedding": 1024},
    auto_embed={
        "abstract": {
            "model": "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",
            "target": "embedding",
            "dims": 1024,
            "precision": "int8",
        },
    })

# Insert — auto-embeds abstract → embedding (1 KB, int8 quantized)
db.insert({"title": "Quantum ML", "abstract": "Quantum computing for ML...", "year": 2025})

# Semantic search — query text is auto-embedded using the same model
results = db.find({"year": 2025}).semantic("abstract", "quantum algorithms", 5).to_list()
for doc, score in results:
    print(f"{doc['title']}: {score:.3f}")

# Hybrid search — auto-embeds query_text for the vector leg
results = db.find({}).hybrid_search("abstract", "abstract", "quantum", None, 10).to_list()
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
- **[Language Bindings](bindings/README.md)** — C, C++, Node.js, Go, Java, C#
- **[Building & Testing](BUILDING.md)** — toolchain setup for every language
- **[bench_native.py](bench_native.py)** — Python vs Rust head-to-head benchmark

---

## Language Bindings

MooFile is implemented in **Rust** with a **Python** binding (via PyO3). A **C shared library** (`libmoofile.so`) exposes the full API via `extern "C"` functions, and all other languages consume that:

| Language | Approach | Directory | Tests |
|----------|----------|-----------|:-----:|
| **Python** | PyO3 native (or pure-Python fallback) | `bindings/python/` | 307 |
| **C** | `extern "C"` from Rust core | `bindings/c/` | 73 |
| **C++** | RAII wrapper over C API | `bindings/c/include/moofile.hpp` | 42 |
| **Node.js** | `koffi` FFI (pure JS, no native compile) | `bindings/node/` | 22 |
| **Go** | cgo + C header | `bindings/go/` | 22 |
| **Java** | Foreign Function & Memory API (JDK 22+) | `bindings/java/` | 30 |
| **C#** | P/Invoke + `DllImport` | `bindings/csharp/` | 30 |

Plus 8 cross-backend parity scenarios comparing pure-Python, PyO3 and C.

Every binding passes documents as **JSON strings** across the FFI boundary. The autoembedding feature (local GGUF embedding models) works transparently in all languages — the model loading and inference happen entirely inside the Rust core.

See [`bindings/README.md`](bindings/README.md) for build instructions, usage examples, and test results for each language.

---

## Development

Setting up a machine from scratch — including the toolchains for all seven
language bindings — is covered in **[BUILDING.md](BUILDING.md)**. Once
installed, `./scripts/test-all.sh` runs every suite and prints a summary,
skipping any language whose toolchain is absent.

```bash
# Unit tests (PYTHONPATH=. so you test this checkout, not an installed copy)
PYTHONPATH=. pytest tests/ -v

# Cross-implementation tests — runs both backends
PYTHONPATH=. pytest tests-cross/ -v

# Rust core tests
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
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
│   ├── src/{lib,storage,index,query,text,cache,embed,errors}.rs
│   └── examples/bench.rs    # Pure-Rust benchmark
├── bindings/                # Language bindings — see bindings/README.md
│   ├── python/              # PyO3 binding (maturin build)
│   ├── c/                   # C ABI (cdylib) + C++ header-only wrapper
│   ├── node/                # Node.js via koffi
│   ├── go/                  # Go via cgo
│   ├── java/                # Java via the Foreign Function & Memory API
│   └── csharp/              # C# via P/Invoke
├── moofile/                 # Python package
│   ├── __init__.py          # Auto-detects Rust, falls back to Python
│   ├── _rust_adapter.py     # Adapts Rust NativeCollection → Python API
│   ├── collection.py        # Pure-Python reference implementation
│   ├── query.py, index.py, storage.py, ...
│   └── cli/                 # moosh, moo2json, moo2mongo, moo2sqlite
├── tests/                   # Python test suite
├── tests-cross/             # Cross-implementation validation
├── docs/README.md           # Full Python API reference
├── moofile-spec.md          # File format & architecture spec
└── pyproject.toml           # Python package config
```

### Other languages

Beyond Python, MooFile ships bindings for **C, C++, Node.js, Go, Java and
C#**, all layered on one C ABI (`bindings/c`). They share the same file
format, query language and semantics.

```bash
cargo build -p moofile-c --release   # build the shared library first
```

See [bindings/README.md](bindings/README.md) for per-language setup, usage,
and the ABI contract (error conventions, ownership rules, no-match
semantics).

---

## License

MIT — see [LICENSE](LICENSE).
