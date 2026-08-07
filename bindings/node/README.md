# moofile

Lightweight embedded document store — MongoDB-style queries, vector search and
BM25 text search over a single file. No server, no infrastructure.

```bash
npm install moofile
```

Prebuilt native libraries for linux (x64, arm64), macOS (Intel, Apple Silicon)
and Windows (x64) ship inside the package. There is no compile step and no
`postinstall` download.

## Usage

```js
const { Collection } = require('moofile');

const db = new Collection('data.bson', {
    indexes: ['email'],
    vector_indexes: { embedding: 384 },
    text_indexes: ['content'],
});

db.insert({ name: 'Alice', email: 'alice@example.com', age: 30 });
db.insertMany([
    { name: 'Bob', email: 'bob@example.com', age: 25 },
    { name: 'Carol', email: 'carol@example.com', age: 35 },
]);

// Filters
db.findOne({ email: 'alice@example.com' });
db.find({ age: { $gte: 30 } }).toArray();

// Cursors are iterable and free themselves
for (const doc of db.find({ age: { $lt: 40 } })) console.log(doc.name);

// Sorting, paging, aggregation
db.find({}, { sort: 'age', desc: true, limit: 10 }).toArray();
db.find({}, {
    group: 'region',
    agg: ['count', { func: 'sum', field: 'amount' }],
}).toArray();

// Search
db.vectorSearch('embedding', queryVector, 5).toArray();  // → [{ doc, score }]
db.textSearch('content', 'machine learning', 5).toArray();

// Atomic writes — rolls back if the callback throws
db.batch(() => {
    db.insert({ _id: 'a', amount: 100 });
    db.insert({ _id: 'b', amount: -50 });
});

db.close();
```

TypeScript definitions are included.

## Things worth knowing

**`updateOne` and `replaceOne` throw when nothing matches.** This mirrors the
Rust and Python APIs, which raise `DocumentNotFound`. `updateMany`,
`deleteOne` and `deleteMany` do not — they return `0`/`false`. Call
`exists()` first when a miss is expected.

**`_id` is always a string**, assigned on insert if you do not supply one.

**Query stages apply in the order** filter → group/agg → sort → skip → limit.
An unrecognised option key is an error rather than being ignored, so a typo
like `limt` cannot silently return the whole collection.

**Aggregation output fields** are named `count`, `sum_<field>`,
`mean_<field>`, and so on.

**`stats().dead_ratio`** is what to threshold on before calling `compact()`.
Note that one delete produces *two* dead records — the superseded original
plus a tombstone.

## Semantic search

With an `auto_embed` source field configured, documents are embedded on insert
using a local GGUF model, and query text is embedded for you:

```js
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
db.semantic('content', 'deep learning', 5).toArray();
```

The model downloads on first use and is cached.

## Using a different library build

Set `MOOFILE_LIB` to a `libmoofile` path to override the bundled binary — a
locally built one, or a slim build without embedding support:

```bash
MOOFILE_LIB=/path/to/libmoofile.so node app.js
```

## Links

- [Repository and full documentation](https://github.com/patw/moofile)
- [All language bindings](https://github.com/patw/moofile/tree/main/bindings)

MIT licensed.
