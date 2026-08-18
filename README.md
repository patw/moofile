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
                text_indexes=["content"]) as db:
    
    db.insert({
        "name": "Alice", 
        "email": "alice@example.com", 
        "age": 30,
        "content": "Machine learning and data science expert",
        "embedding": embedding_vector,   # 1024 floats, from your embedding model
    })

    # Traditional query
    results = db.find({"age": {"$gt": 25}}).sort("age").to_list()
    
    # Vector similarity search
    similar = db.find({}).vector_search("embedding", query_vector, limit=5).to_list()
    
    # BM25 text search
    text = db.find({}).text_search("content", "machine learning", limit=10).to_list()
    
    # Hybrid search — BM25 + cosine, fused with Reciprocal Rank Fusion
    results = db.find({}).hybrid_search("content", "embedding",
                                        "data science", query_vector, 10).to_list()
```

> Prefer not to manage embeddings yourself? [Autoembedding](#autoembedding) runs a local
> ONNX model on-device (~4 ms/embed), filling in the vector on insert and embedding your
> query text at search time. It needs the Rust core (`pip install moofile` ships it for most platforms).

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

On Linux (x86_64/ARM64), macOS (Apple Silicon) and Windows (x86_64) this installs the
Rust-powered wheel. Elsewhere it installs the pure-Python fallback, which warns at
import. See [Native install](#native-install-rust-core) below.

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

### Autoembedding

MooFile runs **voyage-4-nano** on-device through ONNX Runtime, so text is embedded on
insert and query text is embedded at search time — no external embedding API.
A short sentence embeds in **~35 ms** (or ~6 ms each when batched).

```python
from moofile import Collection

db = Collection("papers.bson",
    indexes=["year", "category"],
    vector_indexes={"embedding": 2048},
    auto_embed={
        "abstract": {                             # source text field
            "target": "embedding",                # target vector field
            "dims": 2048,
            "max_length": 1024,                   # tokenizer cap (default; raise only for long docs)
            "batch_size": 32,                      # document-count cap per inference pass
            "max_batch_tokens": 8192,              # padded-token budget; bounds peak RAM
            "precision": "int8",                  # f32 | int8 | uint8 | binary
            # voyage is asymmetric: queries get an instruction, documents do not.
            "query_prefix": "Represent the query for retrieving supporting documents: ",
            "doc_prefix": "",
        },
    })

# Insert — auto-embeds abstract → embedding
db.insert({"title": "Quantum ML", "abstract": "Quantum computing for ML...", "year": 2025})

# Semantic search — the query text is embedded with the same model
for doc, score in db.find({"year": 2025}).semantic("abstract", "quantum algorithms", 5).to_list():
    print(f"{doc['title']}: {score:.3f}")

# Hybrid search — pass None for the query vector and the vector leg auto-embeds
db.find({}).hybrid_search("abstract", "embedding", "quantum", None, 10).to_list()
```

**Requires the Rust core.** The pure-Python fallback cannot run a model — it
raises `NotImplementedError` from both `auto_embed` and `semantic()`. Everything else
works there unchanged.

The same config block is accepted verbatim by every other binding, as JSON:

```jsonc
{
  "vector_indexes": {"embedding": 2048},
  "auto_embed": {
    "abstract": {"target": "embedding", "dims": 2048, "max_length": 1024,
                 "batch_size": 32, "max_batch_tokens": 8192, "precision": "int8"}
  }
}
```

#### Model selection

The model is **voyage-4-nano** — a 180M+160M-param, 2048-dim, 32k-context
model trained for Matryoshka truncation, so `dims` may be set to 2048, 1024,
512 or 256 (smaller is fine, larger is rejected at open). There is no `model`
key to set: it is omitted in modern configs and defaults to voyage-4-nano.
For offline/deployment use you may set `"model"` to a path to a local directory
containing `model_quantized.onnx` (+ its `.onnx_data`) and `tokenizer.json`;
any other value is rejected at open with a clear message.

`max_length` caps how many tokens of a document are embedded (default **1024**).
That's deliberate: the ONNX export materializes a full `[1, 16, T, T]`
attention mask, so memory grows as 16·T²·4 bytes — 1024 tokens is 67 MB and
~0.7 s, but 32k would need ~64 GB. Raise it only for whole-document cases on a
box that can afford it.

For bulk `insert_many()` and `reembed()`, `batch_size` is a document-count cap
(default **32**) and `max_batch_tokens` is a padded-token budget (default
**8192**). MooFile sorts texts longest-first and honors both limits, so short
texts still batch efficiently while long texts cannot turn a nominal 32-document
batch into an out-of-memory inference pass. The default budget holds
voyage-4-nano inference to roughly 2.5 GiB; lower it on memory-constrained
machines.

The model is downloaded from HuggingFace on first use (~422 MB int8 export) and
cached in `~/.cache/moofile/models/`; later opens load from disk in ~250 ms.
Autoembedding is on by default; building with `--no-default-features` drops the
embedding runner and a statically linked ONNX Runtime (~38 MB → ~2.8 MB), after
which `auto_embed` and semantic search return a clear "not available" error and
everything else works unchanged.

#### Changing the embedding model

Vectors of different widths cannot be compared, so switching models invalidates
every stored vector. MooFile detects this at open, logs a warning, and
**disables** the affected vector index — searching it raises
`VectorIndexDisabled` rather than silently ranking against whichever documents
happen to match:

```
vector index 'embedding' is disabled: it expects 2048-dim vectors, but the
configured model and/or the 4213 stored document(s) are 1024-dim. Call
reembed() to rewrite them at 2048, or restore the 1024-dim model.
```

Recover by re-embedding the collection, which rewrites the stored vectors,
retargets the index and clears the flag:

```python
n = db.reembed("abstract")     # source field, not the vector field
```

This is never done implicitly on open: it is a whole-collection write that can
take minutes, and if the model change was a typo, doing it automatically would
destroy the old vectors before anyone noticed. Embedding is batched, so it runs
several times faster per document than re-inserting.

---

## Native Install (Rust Core)

When the Rust native extension is installed, `import moofile` transparently uses it — same API, 2-24× faster.

### From source (requires Rust)

```bash
# Install Rust: https://rustup.rs
curl --proto '=https' --tls v1.2 -sSf https://sh.rustup.rs | sh

# Build and install the native extension — from the REPO ROOT.
# The root pyproject.toml is what points maturin at bindings/python and adds
# the moofile/ package; running maturin inside bindings/python builds a wheel
# containing only the compiled module, with no Python package in it.
pip install maturin
maturin develop --release      # or: maturin build --release
```

### Prebuilt wheels

GitHub Actions builds one **abi3** wheel per platform on tag push — a single wheel
that works on every CPython from 3.10 up, rather than one per minor version:

| Platform | Architecture | Python |
|---|---|---|
| Linux (manylinux 2_28) | x86_64 | 3.10+ |
| Linux (manylinux 2_28) | aarch64 / ARM64 | 3.10+ |
| macOS | ARM64 (Apple Silicon) | 3.10+ |
| Windows | x86_64 | 3.10+ |

Anything else — musl/Alpine, Intel macOS — gets the pure-Python wheel, which has no
autoembedding and is several times slower. That fallback emits a `RuntimeWarning` at
import naming the reason; set `MOOFILE_PURE_PYTHON=1` to silence it if you are on it
deliberately.

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
| **C++** | RAII wrapper over C API | `bindings/c/include/moofile.hpp` | 43 |
| **Node.js** | `koffi` FFI (pure JS, no native compile) | `bindings/node/` | 22 |
| **Go** | cgo + C header | `bindings/go/` | 23 |
| **Java** | Foreign Function & Memory API (JDK 22+) | `bindings/java/` | 31 |
| **C#** | P/Invoke + `DllImport` | `bindings/csharp/` | 32 |

Plus 8 cross-backend parity scenarios comparing pure-Python, PyO3 and C.

Every binding passes documents as **JSON strings** across the FFI boundary. The autoembedding feature (local ONNX embedding models) works in every binding — model loading and inference happen entirely inside the Rust core, so the `auto_embed` config block is identical in all of them. The one exception is the pure-Python fallback, which cannot run a model at all.

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
