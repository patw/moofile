# MooFile

Lightweight embedded document store — MongoDB-style queries, vector search and
BM25 text search over a single file. No server, no infrastructure.

```bash
dotnet add package MooFile
```

Native libraries for linux (x64, arm64), macOS (Intel, Apple Silicon) and
Windows (x64) ship inside the package. The SDK copies the right one to your
output directory automatically — there is no build step and nothing to install.

## Usage

```csharp
using Moofile;

using var db = Collection.Open("data.bson", new Config {
    Indexes = new[] { "email" },
    VectorIndexes = new Dictionary<string, int> { ["embedding"] = 384 },
    TextIndexes = new[] { "content" },
});

db.Insert(Document.Of("name", "Alice", "email", "alice@example.com", "age", 30));
db.InsertMany(new[] {
    Document.Of("name", "Bob", "age", 25),
    Document.Of("name", "Carol", "age", 35),
});

// Filters
db.FindOne(Document.Of("email", "alice@example.com"));
db.Find(Document.Of("age", Document.Of("$gte", 30)));

// Sorting, paging, aggregation
db.Find(null, FindOptions.Create().Sort("age", desc: true).Limit(10));
db.Find(null, FindOptions.Create().Group("region").Count().Sum("amount"));

// Search
db.VectorSearch("embedding", queryVector, 5);      // → List<SearchResult>
db.TextSearch("content", "machine learning", 5);

// Atomic writes — rolls back if the delegate throws
db.Batch(() => {
    db.Insert(Document.Of("_id", "a", "amount", 100));
    db.Insert(Document.Of("_id", "b", "amount", -50));
});
```

## Things worth knowing

**Documents hold plain CLR values** — `string`, `long`, `double`, `bool`,
`List<object?>`, nested `Document` — not `JsonElement`. So `doc["age"]`
compares and casts the way you would expect.

**`UpdateOne` and `ReplaceOne` throw `MooFileException` when nothing matches.**
This mirrors the Rust and Python APIs, which raise `DocumentNotFound`.
`UpdateMany`, `DeleteOne` and `DeleteMany` do not — they return `0`/`false`.
Call `Exists` first when a miss is expected.

**`_id` is always a string**, assigned on insert if you do not supply one.

**Query stages apply in the order** filter → group/agg → sort → skip → limit.
An unrecognised aggregation name is an error rather than being ignored.
Aggregation output fields are named `count`, `sum_<field>`, `mean_<field>`,
and so on.

**`Stats().GetDouble("dead_ratio")`** is what to threshold on before calling
`Compact()`. Note that one delete produces *two* dead records — the superseded
original plus a tombstone.

**Thread safety.** A `Collection` may be shared between threads. Dispose it
when done, or use `using`.

## Semantic search

With an `AutoEmbed` source field configured, documents are embedded on insert
using a local GGUF model, and query text is embedded for you:

```csharp
using var db = Collection.Open("semantic.bson", new Config {
    VectorIndexes = new Dictionary<string, int> { ["embedding"] = 1024 },
    AutoEmbed = new Dictionary<string, AutoEmbedConfig> {
        ["content"] = new AutoEmbedConfig {
            Model = "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",
            Target = "embedding",
            Dims = 1024,
            Precision = "int8",
        },
    },
});

db.Insert(Document.Of("content", "Machine learning is fascinating"));
db.Semantic("content", "deep learning", 5);
```

The model downloads on first use and is cached.

## Using a different library build

Set `MOOFILE_LIB` to a `libmoofile` path to override the bundled binary:

```bash
MOOFILE_LIB=/path/to/libmoofile.so dotnet run
```

## Links

- [Repository and full documentation](https://github.com/patw/moofile)
- [All language bindings](https://github.com/patw/moofile/tree/main/bindings)

MIT licensed.
