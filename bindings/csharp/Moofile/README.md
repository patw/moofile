# MooFile

Lightweight embedded document store — MongoDB-style queries, vector search and
BM25 text search over a single file. No server, no infrastructure.

```bash
dotnet add package MooFile
```

Native libraries for linux (x64, arm64), macOS (Apple Silicon) and Windows
(x64) ship inside the package. The SDK copies the right one to your
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

// Strongly typed filters select fields with C# property expressions. They work
// with the document-oriented Collection API and return List<Document>.
// The selected property name must match the stored field exactly in Phase 1.
var adults = Builders<Person>.Filter.Gte(person => person.age, 30);
var hasBirthday = Builders<Person>.Filter.Ne(person => person.birthday, null);
var activeAdults = Builders<Person>.Filter.And(
    adults,
    Builders<Person>.Filter.Eq(person => person.status, "active"));
db.Find(activeAdults);
db.Count(activeAdults);

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

**Typed filters are an optional convenience layer.** `Builders<T>.Filter`
turns direct top-level property selectors (for example,
`person => person.Age`) into MooFile's existing MongoDB-style filters. It is
not a general LINQ provider: it does not translate arbitrary predicates, and
this Phase 1 API still reads and writes `Document` values rather than POCOs.
Raw `Document` filters remain available for dynamic field names.

## Typed filter builder

Use `Builders<T>.Filter` when your document shape is known at compile time.
`T` is a field-selection type in this Phase 1 API; it does **not** cause
`Collection` to serialize or return POCOs yet. Its selected public property
name must exactly match the stored MooFile field name.

```csharp
sealed class Person
{
    public int age { get; init; }
    public string? status { get; init; }
    public DateTime? birthday { get; init; }
    public List<Tag> tags { get; init; } = new();
}

sealed class Tag
{
    public string? label { get; init; }
}

var ageFilter = Builders<Person>.Filter.Gte(person => person.age, 18);
var activeFilter = Builders<Person>.Filter.Eq(person => person.status, "active");
var eligible = Builders<Person>.Filter.And(ageFilter, activeFilter);

// Document-oriented results, with a type-safe field selection in the filter.
List<Document> people = db.Find(eligible);
Document? first = db.FindOne(eligible);
long count = db.Count(eligible);
bool any = db.Exists(eligible);
```

The builder supports MooFile's complete current filter surface:

```csharp
var filter = Builders<Person>.Filter;

filter.Eq(person => person.status, "active");
filter.Ne(person => person.birthday, null);
filter.Gt(person => person.age, 21);
filter.Gte(person => person.age, 21);
filter.Lt(person => person.age, 65);
filter.Lte(person => person.age, 65);
filter.In(person => person.status, new[] { "active", "trial" });
filter.Nin(person => person.status, new[] { "archived", "deleted" });
filter.Exists(person => person.birthday);
filter.And(/* filters */);
filter.Or(/* filters */);
filter.Not(filter.Eq(person => person.status, "archived"));
filter.ElemMatch(person => person.tags,
    Builders<Tag>.Filter.Eq(tag => tag.label, "vip"));
```

Typed filters can also be used with `UpdateOne`, `UpdateMany`, `ReplaceOne`,
`DeleteOne`, `DeleteMany`, and as pre-filters for `VectorSearch`, `TextSearch`,
`HybridSearch`, and `Semantic`.

Selectors must be direct, top-level properties such as `person => person.age`.
Nested or computed selectors such as `person => person.Address.City` and
`person => person.Name.Length` are rejected. Use a raw `Document` filter for
dynamic field names or any future operation not exposed by the typed builder.

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
using a local ONNX model, and query text is embedded for you:

```csharp
using var db = Collection.Open("semantic.bson", new Config {
    VectorIndexes = new Dictionary<string, int> { ["embedding"] = 384 },
    AutoEmbed = new Dictionary<string, AutoEmbedConfig> {
        ["content"] = new AutoEmbedConfig {
            Model = "BAAI/bge-small-en-v1.5",
            Target = "embedding",
            Dims = 384,
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
