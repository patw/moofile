# MooFile Language Bindings

Every binding except Python calls the same C shared library
(`libmoofile.so` / `.dylib` / `.dll`) built from `bindings/c`. Documents and
configuration cross the FFI boundary as **JSON strings**.

| Language | Mechanism | Runtime requirement |
|---|---|---|
| Python | PyO3 (in-process) | — (pure-Python fallback ships too) |
| C | `extern "C"` | — |
| C++ | header-only RAII wrapper | C++17, nlohmann/json |
| Node.js | [koffi](https://koffi.dev) | Node 18+ |
| Go | cgo | Go 1.21+, a C toolchain |
| Java | Foreign Function &amp; Memory API | **JDK 22+** |
| C# | P/Invoke | .NET 8+ |

## Prerequisites

**[BUILDING.md](../BUILDING.md)** has the full toolchain setup — a single
`apt install` for Ubuntu, plus notes for Fedora, Arch, macOS and Windows.

Build the C library first — everything but the Python binding needs it:

```bash
cargo build -p moofile-c --release
# → target/release/libmoofile.so
```

Autoembedding is on by default. To build without it (dropping `llama-gguf`
and ~300 transitive crates, ~8.3 MB → ~2.8 MB), add `--no-default-features`;
`auto_embed` and `semantic()` then fail with a clear error while everything
else works normally.

Each binding locates that file automatically by walking up from its own
directory. Override with the `MOOFILE_LIB` environment variable (or, for
Java, `-Dmoofile.library.path=…`).

---

## The C ABI contract

All bindings inherit these rules; `include/moofile.h` is the authority.

**Errors.** Every function takes a trailing `char** err_out`. On failure it is
set to an allocated message and the function returns `NULL` (pointer),
`-1` (int), or `-1` (int64_t). A `NULL` return with `*err_out == NULL` is
*not* an error — it means "no result": an exhausted cursor, or
`moofile_find_one()` with no match.

**Ownership.** Every `char*` the library returns is yours and must be released
with `moofile_free_string()` — documents from cursors and error messages
alike. Cursors are released with `moofile_cursor_free()` /
`moofile_search_cursor_free()`, collections with `moofile_close()`. Strings
passed *in* are borrowed for the call and never retained.

**Missing documents.** `update_one` and `replace_one` **fail** when nothing
matches (`"no document matches filter"`), mirroring the Rust and Python APIs,
which raise `DocumentNotFound`. `update_many`, `delete_one` and `delete_many`
do not — they return 0. Bindings surface the first group as an exception and
the second as a plain result.

**Statistics.** `dead_ratio` is the number to threshold on when deciding
whether to compact. Note that one delete produces **two** dead records (the
superseded original plus a tombstone) and one update produces one.

**Threading.** A collection handle may be shared between threads; the Rust
core guards it. Cursors are not thread-safe.

---

## Two traps when writing a binding

Both of these have shipped here before, and neither announces itself: one
corrupts the heap, the other makes every failure look like a success. If you
are adding a language or debugging an existing one, check these first.

### 1. Never let the FFI layer marshal a returned `char*` to a native string

Most FFI layers offer a convenience type that turns `char*` into a native
string — ctypes `c_char_p`, ffi-napi `'string'`, P/Invoke's default `string`
return, JNA's `String`. **Do not use them for anything MooFile returns.**

They copy the bytes and discard the pointer. The pointer is the only thing
that can be passed to `moofile_free_string()`, so you get a choice of two
bugs: leak every document you ever read, or — worse — pass the *host
runtime's* buffer to `free()`. The latter is what
`free(): invalid size` looks like from Python:

```python
# WRONG — ctypes copies to bytes and drops the pointer
lib.moofile_find_one.restype = ctypes.c_char_p
s = lib.moofile_find_one(handle, b"{}", err)
lib.moofile_free_string(s)     # frees Python's buffer. Aborts.

# RIGHT — keep the pointer, read through it, free the original
lib.moofile_find_one.restype = ctypes.c_void_p
ptr = lib.moofile_find_one(handle, b"{}", err)
value = ctypes.cast(ptr, ctypes.c_char_p).value
lib.moofile_free_string(ptr)
```

Declare the return as an opaque pointer, read the string through it, then free
that same pointer. Every binding here has a `take_string`-style helper doing
exactly this — `takeString` (Node, Go, Java), `TakeString` (C#), `exec` (C++).

### 2. The `char** err_out` slot must be real, and passed by reference

The C layer skips writing the message entirely when `err_out` is NULL:

```rust
unsafe fn set_error(err_out: *mut *mut i8, msg: &str) {
    if !err_out.is_null() { /* ... */ }   // NULL slot → message dropped
}
```

So passing a null pointer where a `char**` is expected does not merely lose
the message — it disables error reporting for that call. The function still
returns its failure code, but a binding that decides "no message, therefore no
error" will report success on every failure. The parity harness did this for
months and its C backend appeared to pass while executing almost nothing.

Allocate a real slot, initialise it to NULL, pass its address, and check it
after every call:

```python
err = ctypes.c_void_p()                      # a real slot...
lib.moofile_count(handle, b"{}", ctypes.byref(err))   # ...passed by reference
if err.value:
    raise MooFileError(read_and_free(err.value))
```

A corollary: check the error slot even when the return value looks fine. The
two carry different information — a `NULL` return with an empty slot means
"no result", not "failure".

---

## Query builder

`moofile_find_ex()` exposes the same chain the Rust and Python APIs have —
sort, skip, limit, group, agg. Stages apply in the order
**filter → group/agg → sort → skip → limit**.

Aggregation functions are `count` (no field), `sum`, `mean` (alias `avg`),
`min`, `max`, `collect`, `first`, `last`. Output fields are named `count`,
`sum_<field>`, `mean_<field>`, and so on.

An unrecognised option key or function name is an **error**, not something
quietly ignored — a typo like `limt` cannot silently return the whole
collection.

```jsonc
{
  "sort":  {"field": "age", "desc": true},   // or just "age" for ascending
  "skip":  10,
  "limit": 5,
  "group": "department",
  "agg":   [{"func": "count"}, {"func": "sum", "field": "amount"}]
}
```

---

## C

| File | Description |
|------|-------------|
| `include/moofile.h` | C API header — 30 public functions |
| `include/moofile.hpp` | C++17 RAII wrapper |
| `src/lib.rs` | Rust `extern "C"` implementation |
| `tests/test_c_api.c` | 73 C API tests |
| `tests/test_cxx_api.cpp` | 42 C++ wrapper tests |
| `tests/test_parity.py` | Python ↔ Rust ↔ C parity |

```c
#include "moofile.h"

char* err = NULL;
MooFileCollection* db = moofile_open("data.bson", "{\"indexes\":[\"email\"]}", &err);

char* doc = moofile_insert(db, "{\"name\":\"Alice\",\"age\":30}", &err);
moofile_free_string(doc);

/* Top 10 oldest, newest first */
MooFileCursor* cur = moofile_find_ex(db, "{}",
    "{\"sort\":{\"field\":\"age\",\"desc\":true},\"limit\":10}", &err);
char* row;
while ((row = moofile_cursor_next(cur, &err)) != NULL) {
    puts(row);
    moofile_free_string(row);
}
moofile_cursor_free(cur);
moofile_close(db, &err);
```

**Build & test:**
```bash
cd bindings/c/tests
./run_tests.sh --release
```

Needs gcc/g++ with C11 and C++17, and cmake ≥ 3.16. nlohmann/json is
downloaded automatically if it is not already installed.

---

## C++

The wrapper is header-only and RAII throughout: cursors and collections free
themselves, and every error becomes a `moofile::error` exception.

```cpp
#include "moofile.hpp"

moofile::Collection db("data.bson",
    moofile::Config{}.index("email").vector_index("embedding", 384));

db.insert({{"name", "Alice"}, {"age", 30}});

for (auto doc : db.find({{"age", {{"$gt", 25}}}}).to_vector()) {
    std::cout << doc.dump() << "\n";
}

// Sorting, paging, aggregation
auto oldest = db.find(json::object(),
    moofile::FindOptions().sort("age", true).limit(10)).to_vector();

auto by_dept = db.find(json::object(),
    moofile::FindOptions().group("dept").count().sum("pay")).to_vector();

// Atomic batch — commits on scope exit, rolls back if an exception escapes
{
    moofile::Collection::Batch batch(db);
    db.insert({{"_id", "a"}});
    batch.commit();
}
```

---

## Node.js

| File | Description |
|------|-------------|
| `moofile.js` | Binding via koffi |
| `test.js` | 22 tests |
| `example.js` | Runnable examples |

koffi rather than `ffi-napi`: `ffi-napi` needs a node-gyp build and no longer
compiles against the N-API headers shipped with Node 18+.

```js
const { Collection } = require('./moofile');

const db = new Collection('data.bson', {
    indexes: ['email'],
    vector_indexes: { embedding: 384 },
    text_indexes: ['content'],
});

db.insert({ name: 'Alice', email: 'a@test.com', age: 30 });

// Cursors are iterable and free themselves once exhausted
for (const doc of db.find({ age: { $gte: 30 } })) console.log(doc);

// Sorting, paging, aggregation
db.find({}, { sort: 'age', desc: true, limit: 10 }).toArray();
db.find({}, { group: 'dept', agg: ['count', { func: 'sum', field: 'pay' }] }).toArray();

db.batch(() => {
    db.insert({ _id: 'a' });
    db.insert({ _id: 'b' });
});

db.close();
```

**Install & test:**
```bash
cd bindings/node
npm install
node test.js
node example.js
```

---

## Go

| File | Description |
|------|-------------|
| `moofile/collection.go` | Go package via cgo |
| `moofile/collection_test.go` | 22 tests |
| `example/main.go` | Runnable example |

The cgo directives point at the in-repo `target/release` and bake an rpath, so
binaries find `libmoofile` without `LD_LIBRARY_PATH`. Override with
`CGO_CFLAGS` / `CGO_LDFLAGS` when vendoring elsewhere.

```go
import "github.com/patw/moofile-go/moofile"

db, err := moofile.Open("data.bson", &moofile.Config{
    Indexes: []string{"email"},
})
defer db.Close()

doc, _ := db.Insert(map[string]any{"name": "Alice", "age": 30})

// nil filter matches everything; nil options skips the query builder
results, _ := db.Find(map[string]any{"age": map[string]any{"$gt": 25}}, nil)

oldest, _ := db.Find(nil, &moofile.FindOptions{Sort: "age", Desc: true, Limit: 10})

byDept, _ := db.Find(nil, &moofile.FindOptions{
    Group: "dept",
    Agg:   []moofile.Agg{moofile.Count(), moofile.Sum("pay")},
})

err = db.Batch(func() error {
    db.Insert(map[string]any{"_id": "a"})
    return nil // returning an error rolls back
})
```

**Build & test:**
```bash
cd bindings/go
CGO_ENABLED=1 go test ./moofile/
CGO_ENABLED=1 go run ./example/
```

---

## Java

| File | Description |
|------|-------------|
| `src/main/java/com/moofile/Collection.java` | Public API |
| `src/main/java/com/moofile/Document.java` | Document type |
| `src/main/java/com/moofile/Config.java` | Config + AutoEmbedConfig |
| `src/main/java/com/moofile/FindOptions.java` | Query builder |
| `src/main/java/com/moofile/Json.java` | Dependency-free JSON reader/writer |
| `src/main/java/com/moofile/Native.java` | Panama FFI layer |
| `src/main/java/com/moofile/Example.java` | Runnable examples |
| `src/test/java/com/moofile/CollectionTest.java` | 30 tests |

**Requires JDK 22+.** The binding uses the JDK's Foreign Function &amp; Memory
API, so it needs no third-party jars and no Maven or Gradle — a JDK and the
shared library are the whole toolchain.

```java
import com.moofile.*;

try (Collection db = Collection.open("data.bson",
        Config.create().index("email"))) {

    db.insert(Document.of("name", "Alice", "email", "a@example.com", "age", 30));

    for (Document d : db.find(Document.of("age", Document.of("$gt", 25)))) {
        System.out.println(d);
    }

    List<Document> oldest = db.find(null,
        FindOptions.create().sort("age", true).limit(10));

    List<Document> byDept = db.find(null,
        FindOptions.create().group("dept").count().sum("pay"));

    db.batch(() -> {          // rolls back if anything is thrown
        db.insert(Document.of("_id", "a"));
        db.insert(Document.of("_id", "b"));
    });
}
```

**Build & test:**
```bash
cd bindings/java
./build.sh test      # compile, then run the test suite
./build.sh example   # compile, then run the examples
```

Or by hand:
```bash
javac -d build/classes src/main/java/com/moofile/*.java
java --enable-native-access=ALL-UNNAMED -cp build/classes com.moofile.Example
```

---

## C#

| File | Description |
|------|-------------|
| `Moofile/Collection.cs` | Public API |
| `Moofile/Document.cs` | Document, SearchResult, MooFileException |
| `Moofile/Config.cs` | Config, AutoEmbedConfig, FindOptions |
| `Moofile/Native.cs` | P/Invoke layer + library resolver |
| `Moofile.Tests/` | 30 tests |
| `Moofile.Example/` | Runnable examples |

Documents hold plain CLR values (`string`, `long`, `double`, `bool`, `List`,
nested `Document`) rather than `JsonElement`, so `doc["age"]` compares and
casts the way you would expect.

```csharp
using Moofile;

using var db = Collection.Open("data.bson", new Config {
    Indexes = new[] { "email" },
});

db.Insert(Document.Of("name", "Alice", "email", "a@example.com", "age", 30));

foreach (var doc in db.Find(Document.Of("age", Document.Of("$gt", 25))))
    Console.WriteLine(doc);

var oldest = db.Find(null, FindOptions.Create().Sort("age", desc: true).Limit(10));

var byDept = db.Find(null, FindOptions.Create().Group("dept").Count().Sum("pay"));

db.Batch(() => {             // rolls back if anything is thrown
    db.Insert(Document.Of("_id", "a"));
    db.Insert(Document.Of("_id", "b"));
});
```

**Build & test:**
```bash
cd bindings/csharp
dotnet build Moofile
dotnet run --project Moofile.Tests
dotnet run --project Moofile.Example
```

The projects target `net10.0`; retarget in the `.csproj` files for .NET 8 or 9
(nothing used here is newer than .NET 8).

---

## Autoembedding

All bindings support on-device embedding via the `auto_embed` config option.
Model loading and inference happen entirely inside the Rust core.

The config shape is identical everywhere:

```json
{
  "vector_indexes": {"embedding": 1024},
  "auto_embed": {
    "content": {
      "model": "hf:user/repo:filename.gguf",
      "target": "embedding",
      "dims": 1024,
      "precision": "int8",
      "normalize": true,
      "query_prefix": "Represent the query: ",
      "doc_prefix": "Represent the document: "
    }
  }
}
```

With that in place, inserting a document populates `embedding` automatically,
and `semantic()` embeds the query text for you:

```js
// Node.js
const db = new Collection('semantic.bson', {
    vector_indexes: { embedding: 1024 },
    auto_embed: {
        content: {
            model: 'hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf',
            target: 'embedding',
            dims: 1024,
            precision: 'int8',
        },
    },
});
db.insert({ content: 'Machine learning is fascinating' });
const hits = db.semantic('content', 'deep learning', 5).toArray();
```

`semantic()` requires the Rust engine. The pure-Python fallback cannot run
embedding models and raises `NotImplementedError` explaining as much; use
`vector_search()` with a pre-computed embedding there.

---

## Test matrix

Every suite below runs against a freshly built `libmoofile`.

| Language | Tests | Command |
|----------|:-----:|---------|
| Python (both backends) | 307 | `PYTHONPATH=. pytest tests/ tests-cross/` |
| Rust core | 79 | `cargo test` |
| C | 73 | `cd bindings/c/tests && ./run_tests.sh --release` |
| C++ | 42 | (same command) |
| Cross-backend parity | 8 | (same command) |
| Node.js | 22 | `cd bindings/node && node test.js` |
| Go | 22 | `cd bindings/go && go test ./moofile/` |
| Java | 30 | `cd bindings/java && ./build.sh test` |
| C# | 30 | `cd bindings/csharp && dotnet run --project Moofile.Tests` |

`run_tests.sh` now runs `test_parity.py` as its third stage. That script
cross-checks the pure-Python, PyO3 and C backends against each other over the
full API surface, driving the C library through ctypes. It does **not**
exercise the Node, Go, Java or C# bindings, which have their own suites above.

Each binding's suite covers the same ground: lifecycle, insert, the filter
operators, the query builder, updates and deletes (including the differing
no-match contracts), vector and text search, batch commit and rollback, stats
and compaction, and the document/JSON round trip.

Every binding also ships a runnable example covering the same six topics —
CRUD, sorting and aggregation, vector search, text search, atomic batches, and
autoembedding:

```bash
node bindings/node/example.js
cd bindings/go && go run ./example/
cd bindings/java && ./build.sh example
cd bindings/csharp && dotnet run --project Moofile.Example
```
