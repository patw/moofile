# MooFile Language Bindings

All bindings call the same C shared library (`libmoofile.so` / `.dylib` / `.dll`)
compiled from `bindings/c`.  Every operation passes documents as **JSON strings**
across the FFI boundary.

## Prerequisites

Build the C library first:

```bash
cargo build -p moofile-c --release
# → target/release/libmoofile.so
```

Each binding directory points at that `.so` by default.

---

## C

| File | Description |
|------|-------------|
| `include/moofile.h` | C API header — 28 public functions |
| `include/moofile.hpp` | C++ RAII wrapper |
| `src/lib.rs` | Rust `extern "C"` implementation |
| `tests/test_c_api.c` | 62 C API tests |
| `tests/test_cxx_api.cpp` | 39 C++ wrapper tests |
| `tests/test_parity.py` | Cross-backend parity (Python ↔ Rust ↔ C) |

**Build & test:**
```bash
cd bindings/c
cargo build --release
cd tests
./run_tests.sh --release
```

---

## Node.js

| File | Description |
|------|-------------|
| `moofile.js` | Binding via `ffi-napi` |
| `test.js` | 18 test groups |
| `example.js` | Usage examples |
| `package.json` | NPM metadata |

**Usage:**
```js
const { Collection } = require('./moofile');

const db = new Collection('data.bson', {
    indexes: ['email'],
    vector_indexes: { embedding: 384 },
    text_indexes: ['content'],
});

db.insert({ name: 'Alice', email: 'a@test.com' });
console.log(db.find({ email: 'a@test.com' }));
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
| `moofile/collection_test.go` | 13 test functions |
| `example/main.go` | Usage example |
| `go.mod` | Module definition |

**Usage:**
```go
import "github.com/patw/moofile-go/moofile"

db, err := moofile.Open("data.bson", &moofile.Config{
    Indexes: []string{"email"},
})
doc, err := db.Insert(map[string]any{"name": "Alice", "age": 30})
results, _ := db.Find(map[string]any{"age": map[string]any{"$gt": 25}})
db.Close()
```

**Build & test:**
```bash
cd bindings/go
CGO_ENABLED=1 go build ./moofile/
CGO_ENABLED=1 go test ./moofile/
```

---

## Java

| File | Description |
|------|-------------|
| `src/main/java/com/moofile/Collection.java` | JNR-FFI binding |
| `src/main/java/com/moofile/Config.java` | Config + AutoEmbedConfig |
| `src/main/java/com/moofile/Document.java` | JSON document wrapper |

**Usage:**
```java
import com.moofile.*;

var db = Collection.open("data.bson",
    Config.create().index("email"));
db.insert(new Document().put("name", "Alice"));
var results = db.find(new Document().put("age", new Document().put("$gt", 25)));
db.close();
```

**Build:**
```bash
cd bindings/java
javac src/main/java/com/moofile/*.java
java -cp src/main/java -Djava.library.path=../target/release com.moofile.Main
```

Requires [JNR-FFI](https://github.com/jnr/jnr-ffi) on the classpath.

---

## C#

| File | Description |
|------|-------------|
| `Moofile/Collection.cs` | P/Invoke binding + Document, Config, SearchResult |
| `Moofile/Moofile.csproj` | .NET 8 project |

**Usage:**
```csharp
using Moofile;

using var db = Collection.Open("data.bson", new Config {
    Indexes = new[] { "email" },
    Durability = "os",
});

db.Insert(new Document { ["name"] = "Alice" });
var results = db.Find(new Document { ["age"] = new Document { ["$gt"] = 25 } });
```

**Build & test:**
```bash
cd bindings/csharp/Moofile
dotnet build
```

---

## Autoembedding

All bindings support on-device embedding via the `auto_embed` config option.
The model loading and inference happen entirely inside the Rust core —
no language-specific code needed.

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
        }
    }
});
db.insert({ content: 'Machine learning is fascinating' });
// doc.embedding is auto-generated

const results = db.semantic('content', 'deep learning', 5);
```

The config JSON format is identical across all languages:

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

---

## Test matrix

| Language | Unit tests | Cross-backend parity |
|----------|:----------:|:--------------------:|
| C | 62 | ✅ `test_parity.py` |
| C++ | 39 | ✅ `test_parity.py` |
| Node.js | 18 groups | via `test_parity.py` |
| Go | 13 functions | via `test_parity.py` |
| Java | — | via `test_parity.py` |
| C# | — | via `test_parity.py` |

The Python `test_parity.py` script can drive any binding by loading
`libmoofile.so` via ctypes and comparing results against the Rust native
and pure Python backends.  Add `--c-lib path/to/libmoofile.so` to test
a freshly built library.
