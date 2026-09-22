# Changelog

## v1.2.5 (2026-09-22)

Recovery fixes, from the TheWatcher incident of 2026-09-18: `granular.bson`
grew a 1,814-byte all-zero tail when miniserv died mid-write, and the service
then failed to start **20,427 times over three days and nineteen hours** — a
16-second restart loop that ran from the start of the boot to the end of it,
never once serving a request. Remediation was a one-line `truncate`; the point
of these changes is that no human should have had to type it.

### An all-zero tail was read as a corrupt record, not as a partial write

The scanner had two "this is an interrupted write, truncate here" escape
hatches and both were triggered by hitting physical EOF — a short header or a
short payload. But an interrupted write does not always leave a *short* file.
On a filesystem with delayed allocation (ext4 in its default `data=ordered`),
the inode's new size can reach the journal while the data blocks never reach
the disk, so the tail reads back at full length as **zeros**.

Zeros are not short. The scanner read a complete 5-byte header out of the zero
region — `payload_len = 0`, `record_type = 0` — and `read_exact` on a
zero-length buffer succeeds, so it fell through to `bson::from_slice(&[])`,
which fails with `"document too short"`. That arm was a hard
`Err(CorruptRecord)`, returned by `?` from inside the very scan whose return
value `load_from_file` needed in order to truncate the tail. The self-healing
code was three lines further down and unreachable:

```rust
let truncate_to = storage::scan_from_streaming(path, 0, ...)?;  // bails here
if let Some(at) = truncate_to { storage::truncate(path, at)?; } // never runs
```

Deterministic on-disk state, deterministic failure, forever. A zero tail of
one to four bytes self-healed correctly; five or more did not.

A header claiming a payload below the 5-byte minimum BSON document is now a
truncation point rather than something to decode. A concurrent writer mid-
append cannot produce one — an interrupted `write()` leaves the file short,
which is the pre-existing EOF path — and `catch_up` still stops at such an
offset without truncating, since there another writer may genuinely be mid-
append.

### Symmetrically: mid-file damage was silently truncating the file on open

Found while writing the regression test for the above. A clobbered block in
the middle of the file usually reads as an implausible payload length, and the
rule for that was "if the claimed length runs past EOF, treat it as a partial
write" — i.e. truncate. For a genuine tail that is right. For damage with
intact records *after* it, it silently discarded every one of them, on open,
with no error and no log. Worse than the bug above, and it had been there
longer.

Neither case can be decided from the header alone, because the format carries
no magic and no checksum to resynchronise on. Both now consult
`damage_is_tail()`, which looks ahead (bounded, 1 MiB) for a record that
validates *and* is followed by another that validates. Found means the damage
is interior — raise `CorruptRecord` and let `repair()` handle it. Not found
means the file really does end there — truncate, losing nothing readable.

### New: `Collection::repair()`, and a `repair` flag on open

There was no way to salvage a damaged file at all: `reindex()` and `compact()`
are methods on an open collection, and the file that needs them is the one
`open()` refuses. So `repair` takes a path, not a handle.

It keeps every record that still decodes and drops the byte spans that do not,
resynchronising past damage where an intact record follows it and truncating
where none does. Surviving records are copied verbatim and in order, so the
repaired log replays to exactly the state its intact part describes — inserts,
replacements and tombstones all keep their meaning. The rewrite is staged
through a temp file, fsynced, and renamed under the same exclusive lock writes
take, so an interrupted repair leaves the damaged file untouched and other
processes holding the file open see the inode change and reload. An intact
file is not rewritten at all.

The returned report says what was lost — `records_kept`, `bytes_kept`,
`bytes_dropped`, `rewritten`, and every `gap` with its offset, length and
whether it ran to EOF — so a caller can log or alert on data loss instead of
discovering it later.

Surfaced in all seven bindings, with a `repair` open flag that runs it on
demand for unattended services that would rather lose a damaged span than not
start:

| | salvage | open flag |
|---|---|---|
| Rust | `Collection::repair(path)` | `.repair()` |
| Python | `Collection.repair(path)` | `repair=True` |
| C | `moofile_repair(path, &err)` | `"repair": true` |
| C++ | `moofile::repair(path)` | `Config::set_repair()` |
| Node | `moofile.repair(path)` | `{ repair: true }` |
| Go | `moofile.Repair(path)` | `Config.Repair` |
| Java | `Collection.repair(path)` | `Config.repair(true)` |
| C# | `Collection.Repair(path)` | `Config.Repair` |

Python also gains `CorruptRecordError`; the pure-Python scanner previously let
pymongo's raw decode exception escape, so the two backends did not even raise
the same type.

### The index cache's fingerprint could not see the file being replaced

The `.cache` sidecar is accepted on open if it still describes the data file,
which was checked as (length, mtime). Neither sees a `compact()`: it renames a
freshly written file over the path, and rewriting the same live set reproduces
the same length — not by coincidence but by construction, since that is exactly
what compaction does when there is nothing dead to drop. Two handles, or a
handle and a crash, can land on the same mtime nanosecond too.

The result would be a cache validating against a file it does not describe, and
being served as truth — the one case where the cache stops being disposable.
`catch_up()` had carried an inode check since v0.5.2 for exactly this reason;
the cache had not.

The fingerprint now includes the data file's inode (Rust) / `(st_dev, st_ino)`
(Python), stamped from the same basis as the length and mtime — the file state
the index actually describes, not the file's current state. Cache versions
bumped (Rust 3 → 4, Python 2 → 3), so existing caches are rebuilt once.

### Writes that produced a file the reader could not read

Nothing checked, on the way in, that a document was one the reader would give
back. Three ways that went wrong, all silent or worse:

**A document over the 100 MiB record cap.** The scanner refuses a longer record
— correctly, because from its side an over-cap length field is
indistinguishable from a corrupt one. So a single oversized insert produced a
file that every later cold open rejected. It did not even fail immediately:
`close()` writes an index cache, so the collection kept opening from cache and
only detonated once the cache was invalidated.

**A single binary value over 16 MiB.** This is the `bson` crate's own cap, far
below the record cap, and crossing it failed in three directions at once:

* `bson::to_vec` returns an error there, and every write path called it as
  `.expect("BSON serialisation is infallible for Document")`. It is not
  infallible. The panic fired while holding the collection's `RwLock`, which
  poisons it — so a single 17 MiB insert permanently bricked the handle, the
  exact failure the "matches() never panics" invariant exists to prevent.
* `IndexManager::add` did `let Ok(raw) = RawDocumentBuf::try_from(&doc) else
  { return }` — it dropped, without a word, any document it could not
  re-encode. A record written by the Python backend (pymongo has no such cap)
  was therefore *on disk and invisible*: the file scanned clean, and `count()`
  returned 0.
* The two implementations disagreed about what was writable at all, so the
  pure-Python backend could produce files whose documents the Rust backend
  silently could not see.

Both caps are now enforced on write in both implementations, raising
`DocumentTooLarge` / `DocumentTooLargeError` and `BinaryFieldTooLarge` /
`BinaryFieldTooLargeError`. The binary check walks the document (including
nested documents and arrays, naming the offending path) which is O(fields), and
only runs ahead of an encode of the same document which is O(bytes) — so it
costs nothing measurable. The batch path validates on the way into the buffer
rather than at commit, so the error names the insert that caused it.

`bson::to_vec` is no longer `expect`ed anywhere on a write path; failures
propagate as `BsonEncode`. `IndexManager::add` still skips a document it cannot
re-encode — erroring there would fail a whole open for one bad record — but now
logs a warning instead of saying nothing.

**Also fixed in passing:** the PyO3 bridge encoded every returned document with
`bson::to_vec(doc).unwrap_or_default()`, handing Python an *empty buffer* when
encoding failed. The adapter then decoded it and raised pymongo's "not enough
data for a BSON document", naming neither the document nor the cause. It now
propagates a real error.

New: `MAX_DOCUMENT_SIZE` and `MAX_BINARY_SIZE` are public in both
implementations.

## v1.2.4 (2026-09-14)

Three more fixes from the same investigation as v1.2.3, benchmarked separately
before landing (see each entry). Where v1.2.3 was an emergency stop-gap that
halved *open-time peak* RSS, the third change below removes the steady-state
cost of holding documents in memory — together they are the complete fix for
the downstream app (TheWatcher) whose systemd unit was holding gigabytes of
RSS against a ~170 MB data file.

### `try_index` always used the first indexed filter field, regardless of selectivity

`try_index` walked a multi-field filter document and returned candidates
from whichever indexed field it hit *first*, in the filter's insertion
order — with no regard for how selective that field's condition actually
was. A filter like `{metric, timestamp_ms: {$gte, $lte}}` (thewatcher's
rollup queries the vast majority of hits this) always resolved on `metric`
(written first), even though it's the low-cardinality field: every document
that ever had that metric became a "candidate," each then walked through a
full-document `query::matches` check to apply the timestamp range — turning
a query that should touch a handful of documents in a tight time window
into a scan of everything sharing that metric across the collection's whole
history.

`try_index` now evaluates every indexed field present in the filter and
keeps whichever yields the smallest candidate set. Same `IndexResult`
contract (`get_matching`/`count_matching` still re-verify the full filter
via `query::matches` against whichever candidates come back), so this is a
pure performance change — no behavior/result difference, confirmed by the
full test suite passing unmodified.

Benchmarked against thewatcher's actual rollup query
(`{metric, timestamp_ms: {$gte, $lte}}`, 1-hour bucket) on a real 638k-doc
production collection: **47–146ms → 8–30ms per call (4–5x)**, metric-
dependent on how many documents share that metric. A simulated 24-bucket
catch-up (e.g. after a day of service downtime) for the highest-cardinality
metric went **3519ms → 748ms**.

### `Query::to_list` deep-cloned every match before applying `sort`/`limit`

`to_list()` fetched matches as `Arc<Document>` (cheap) from the index, then
immediately deep-cloned all of them into owned `Document`s *before* sorting
and truncating to the requested `limit`. A `.sort(...).limit(1)` query —
e.g. the resume-point lookup every rollup/downstream consumer doing
incremental sync tends to run (`find({field}).sort(...).limit(1)`) — paid
to clone the *entire* matching set just to keep one document.

`to_list()` now keeps documents as `Arc<Document>` through sort/skip/limit
(sorting and truncating references, not data) and only deep-clones the
survivors at the very end. The one exception is `.group()`/aggregation,
which reads every input document's fields regardless of any later limit —
that path still clones up front since there's no deferral to be had, then
sorts/skips/limits the synthesized aggregate output same as before.

Benchmarked against the resume-point lookup pattern on a collection sized
to represent a year of hourly-rollup retention (70k docs): **14.7–69.5ms →
2.1–22.2ms per call (3–7x)**, more pronounced on the smaller/more common
case (5.2k docs, today's actual size): **1.5–7.6ms → 0.12–1.2ms (6–12x)**.

### Documents were stored fully decoded — ~10x their on-wire size, for the life of the collection

The big one from the same investigation. `IndexManager.documents` held
`Arc<bson::Document>` — every live document, decoded, forever, no paging
(moofile's design is deliberately all-in-memory; this isn't a paging
proposal). The problem: `bson::Bson` is a 112-byte enum sized to its
largest variant (`JavaScriptCodeWithScope { code: String, scope: Document }`
— 24 + 88 bytes, confirmed via `size_of`), so a `bool` field costs the same
112 bytes as a decimal128, plus a heap-allocated key per field on top via
`Document`'s `IndexMap`. Measured: a 298-byte-on-wire, 12-field document
cost **2978 bytes in memory — 10.0x**. Against a real 638k-doc production
collection this meant ~1.9 GB retained for ~180 MB of actual data.

Documents are now stored as `Arc<bson::raw::RawDocumentBuf>` — the raw BSON
bytes, wrapped, already part of the `bson` crate we depend on (no new
dependency). `RawBsonRef`, the value type reading a raw field returns,
borrows into the bytes (`&str` not `String`, etc.) — measured `size_of`
40 bytes vs `Bson`'s 112, and reading a field allocates nothing. Same
document, same measurement: **491 bytes — 1.65x wire, a 6.1x reduction**.
Verified end-to-end against thewatcher's real 638k-doc collection, not just
the synthetic probe: **RSS after open 1965 MB → 418 MB** (a 4.7x reduction
on top of what v1.2.3 already bought; ~8.9x off the original, pre-any-of-
this-work baseline).

- `IndexManager::add`/`remove` now read only the handful of configured
  regular/text/vector fields out of the raw bytes via `RawDocument::get()`
  — never a full decode just to index a document. That lookup is
  O(fields-in-document) (an unindexed byte scan — raw BSON has no built-in
  field index) versus `Document::get()`'s O(1) `IndexMap` hash lookup;
  measured within noise of each other for realistic field counts (73.8ns
  vs 74.6ns/field, 12-field doc, 3 fields looked up) — it degrades for
  documents with many more fields than are actually indexed.
- A `Document` is materialized (`RawDocumentBuf::to_document()`) only at
  the point a document is about to leave the index for a caller — `get()`,
  the survivors of `to_list()`'s sort/skip/limit, an updated/deleted
  document's old content. Existence checks (duplicate-key checks on the
  insert hot path in particular) go through a new `contains()` — no decode,
  no clone, just a `BTreeMap` key lookup.
- `insert()`'s hot path now encodes a document exactly once — the same
  bytes are written to disk and wrapped as the raw index entry — instead of
  encoding for disk (`storage.append`) and separately deep-cloning the
  whole `Document` for the index. Measured: insert throughput went from
  98.6k/sec to 106-107k/sec (a side effect, not the point of this change,
  but a real one).
- `compact()` no longer decodes anything either: each live document's raw
  bytes are already exactly the bytes it needs to write, so it went from
  clone-every-live-document-then-re-encode to a straight byte copy. This
  incidentally closes a gap flagged when v1.2.3 shipped — `compact()`'s
  `all_docs()` clone was the one transient-spike path the v1.2.3
  `malloc_trim` fix didn't cover, since there was nothing to trim there
  once there's no clone left to make.
- The disposable index cache (`cache.rs`) already stored documents as raw
  BSON bytes on disk (bincode can't handle `Document`'s `deserialize_any`)
  and decoded them back into `Document` on every cache-hit load, purely to
  satisfy the old in-memory type. That decode is gone — cache load now just
  wraps the bytes.
- `Query::to_list`'s sort step needed one adjustment to avoid a regression:
  a naive `sort_by` that reads the sort key from each side's raw bytes
  *inside the comparator* pays that O(fields) scan on every one of the
  O(n log n) comparisons, not once per document. Fixed with a decorate-
  sort-undecorate — extract each document's sort key once into a side
  vector, sort using the cached values. Measured before/after this specific
  fix on the `last_timestamp`-shaped query below: network 6.63ms → 911μs.

**What this doesn't cover**: `query::matches()` (~1000 lines of filter
evaluation) still takes a decoded `&Document`. Candidates that reach it
(the post-index-selection, pre-final-result set) are decoded individually
right before the check — safe, correct, and already a small set thanks to
the `try_index` selectivity fix above, but a real, measured cost on queries
whose winning index is a broad range with a secondary field to check
against `query::matches` (see benchmarks below). A raw-native fast path for
flat, non-nested filters (thewatcher's entire actual query surface) would
close that gap; deliberately out of scope here — proposed as a follow-up,
not attempted alongside a storage-representation change this size.

**Benchmarked** (real 638k-doc production data = "peak"; synthetic 46k-doc
/2d-retention + 70k-doc/365d-retention data = "steady", matching where
retention tuning is taking that collection):

| query | peak (638k) before→after | steady (46k/70k) before→after |
|---|---|---|
| `last_timestamp` (single-field `Exact` match, no decode needed) | 123μs–1.22ms → 185–911μs (flat to faster) | 2.2–21.6ms → 2.2–13.8ms (faster) |
| bucket query (`{metric, timestamp_ms: $gte/$lte}`, 1h window) | 8.3–31.0ms → 8.4–30.0ms (flat) | 422μs–951μs → 1.3–2.1ms (**slower, 2-3x**) |
| 24h sorted query (`{metric, timestamp_ms: $gte/$lte}` over a day, then sort) | 62.1ms → 81.1ms (**slower, 31%**) | 36.1ms → 60.7ms (**slower, 68%**) |
| `insert()` | 10.1μs/doc → 9.4μs/doc (faster) | 10.2μs/doc → 9.3μs/doc (faster) |

The two "slower" rows are the `get_matching` `Candidates`-path decode cost
described above — real, and worth naming plainly rather than only reporting
the wins. In absolute terms every number here is sub-100ms; the *aggregate*
5-metric granular→hourly rollup step (`last_timestamp` + bucket query,
what actually runs every 5 minutes) is a net wash-to-improvement at both
scales once you sum it (steady: 42.1ms → 36.8ms combined) — the bucket-
query regression is more than paid for by the `last_timestamp` win on the
same workload. The 24h-sorted-query row is the one place this trades real
latency for the memory win with nothing offsetting it on the same query;
it's a large-candidate-set, decode-then-sort shape thewatcher's dashboard
does exercise (the "Last 24 hours" charts) but only on user-facing HTTP
requests, not the recurring background rollup.

Cold-open *time* was not re-measured for this change — the table above covers
queries and inserts, not a collection open — and the load path now re-encodes
each record into the raw form the index stores it in (~280 ns/doc for a
12-field/263-byte document, ≈0.2 s across 638k records) where it previously
moved the decoded document straight into the index. Expect a small open-time
increase alongside the much larger RSS reduction above.

All existing tests pass unmodified: 90 unit + 13 `api_guard` + 1 doc-test with
default features, 84 unit + 13 `api_guard` under `--no-default-features` (the
difference is the six tests gated on the `embed` feature), plus every suite in
`scripts/test-all.sh` — Rust (both feature configurations), Python
`tests/`+`tests-cross/` (319 passed, 7 skipped), C 73, C++ 43, parity 8,
Node 23, Go, Java 32, C# 33. Cross-checked independently of the test
suite: ran the pre- and post-change binaries against byte-identical copies
of the same real 638k-doc collection and diffed `/api/history` responses
across every metric, resolution, and filter combination thewatcher's API
supports — byte-identical output.

## v1.2.3 (2026-09-14)

### Opening a large collection peaked at roughly double its steady-state memory

Found while chasing a downstream app (TheWatcher) whose systemd unit was
consuming gigabytes of RSS against a ~170 MB data file. moofile keeps every
live document decoded in RAM (`documents: BTreeMap<String, Arc<Document>>`
with no paging), so that's expected to scale with document count — but
*opening* a collection cost noticeably more than the index it built.

- **`scan_file`/`scan_from` buffered the whole file before replaying it.**
  Both returned a `Vec<Record>` holding every decoded record — live, dead,
  and tombstones — and only after the full scan finished did the caller loop
  over it to build the index. For the whole scan's duration, every document
  existed twice: once in the scan `Vec`, once in the index. Measured against
  a 638k-live/767k-total-record collection, this alone doubled peak RSS
  during open (3811 MB peak vs. ~1900 MB actually retained). Load, catch-up,
  full-reload, and reindex now stream through a new `scan_from_streaming`,
  applying each record to the index as it's decoded and dropping it
  immediately after — no second copy. `apply_record` also now takes the
  record by value and moves its document into the index instead of cloning
  it, cutting one more full-document allocation per record during replay.
  `scan_file`/`scan_from` are kept as thin `Vec`-collecting wrappers around
  the streaming core for callers that want a batch.
- **glibc never returned the scan's freed arena to the OS.** Even after the
  transient buffers above are dropped, `malloc_trim` is required to hand
  freed heap back to the kernel — without it, RSS sits at its load-time peak
  for the life of the process. `Collection::open` now calls `malloc_trim(0)`
  once, after the cache load or BSON scan completes. Linux+glibc only
  (`cfg(target_os = "linux", target_env = "gnu")`); a no-op elsewhere.

Together these brought RSS after open on the collection above from 3735 MB
down to 1965 MB, with no change to what ends up in the index — same
document counts, same query results (`cargo test` — including the existing
`scan_*` unit tests, unmodified — passes unchanged).

## v1.2.2 (2026-08-18)

### Text analyzer: digits are indexed, arrays are no longer skipped (re-index)

Two silent defects in the inverted index, both found by an accuracy audit of a
439-document corpus.

- **Digits were discarded.** The tokenizer matched `[a-zA-Z]+` and dropped
  single characters, so no numeric token ever reached the index or a query.
  `14900K` and `595.71.05` returned *zero* results against a corpus that
  contained both; `moofile v1.2.0` and `moofile v0.3.0` produced byte-identical
  queries and therefore identical rankings. The pattern is now
  `[a-zA-Z0-9]+(?:[._-][a-zA-Z0-9]+)*`, which also holds compound identifiers
  (`llama.cpp`, `x86-64-v3`) together; a compound additionally emits its parts,
  so `llama` still finds `llama.cpp`. Porter stemming is now applied only to
  purely alphabetic terms — it mangled identifiers to no benefit.
- **Array fields were never indexed.** The add path matched `Bson::String`
  only, so declaring a text index on an array field built an empty index in
  silence and every search against it returned nothing. Arrays are now squashed
  into one space-joined string and indexed like any other field (as Lucene does
  for multi-valued fields); nested arrays flatten, and numeric elements are
  stringified so they stay searchable.

Measured on the audit corpus, the previously-broken query classes go from no
results to the correct document at rank 1, while ordinary prose queries are
unchanged (Recall@5 0.880 → 0.900, NDCG@5 0.871 → 0.875 on a 25-query set).

**This changes index contents, not the cache layout**, so `CACHE_VERSION` is
bumped 2 → 3: a stale cache would otherwise load happily and keep serving the
old token set. Existing collections rebuild their index on next open. No
document data is touched and no migration is required.

### Embedding memory is now bounded by a token budget, not a document count

Re-embedding a few hundred ordinary documents could OOM a 16 GB machine.
`batch_size` defaulted to 32 documents, and peak inference memory tracks
`batch x padded_sequence_length` — so the peak depended entirely on how long
the caller's documents happened to be. Measured on voyage-4-nano, the model
weights are ~170 MiB while a batch of 32 x 1024 tokens peaks at **~9.2 GiB**
(scaling close to linearly: ~285 MiB per document at full sequence length).

- New `max_batch_tokens` (default **8192**) caps `batch x padded_len`, holding
  inference to roughly 2.5 GiB regardless of document length. `batch_size`
  remains as a count cap and still applies, so short documents keep batching
  widely and lose no throughput.
- Batches are now assembled **longest-first**. The tokenizer pads to the
  longest member, so one long document in an otherwise short batch previously
  inflated every row to its width.
- **`batch_size` and `max_batch_tokens` are now settable from the bindings.**
  Both were reachable only from Rust, which left Python and C callers with no
  way to cap a re-embed at all.


## v1.2.1 (2026-08-16)

### Windows CI/CD fix: drop tokenizers default features (esaxx-rs CRT mismatch)

The v1.2.0 release workflows both failed on `windows-x86_64` at link time:
`link.exe` aborted with LNK2038 / exit code 1169 because `esaxx-rs` (pulled in
by `tokenizers`'s default `esaxx_fast` feature) hardcodes `static_crt(true)`
(/MT) while ONNX Runtime's prebuilt binaries are /MD. `v4nano-embed` now
depends on `tokenizers` with `default-features = false, features = ["onig"]` —
the same configuration `fastembed` used — so the static-CRT C++ build is never
compiled. No other platform or runtime behavior changed.

## v1.2.0 (2026-08-15)

### Autoembedding now runs voyage-4-nano via a dedicated crate (breaking)

fastembed is gone. It was pulled in to run *any* registry model, but the only
model we ever wanted was voyage-4-nano — so the generic registry was replaced
with [`v4nano-embed`](v4nano-embed/), a single-purpose ONNX runner extracted
from fastembed's glue (~200 lines), depending on `ort` + `tokenizers` directly.

- The default (and only built-in) model is now **`voyage-4-nano`**
  (`onnx-community/voyage-4-nano-ONNX` int8 export): 180M+160M params, 2048
  dims, 32k context, MRL-truncatable to 1024/512/256. `model` may also be a
  path to a local directory holding `model_quantized.onnx` + `tokenizer.json`.
- **`model` is now optional** — modern configs omit it and get voyage-4-nano.
  An empty string (some bindings serialize unset fields as `""`) also means
  the default. `hf:` GGUF URIs and fastembed registry ids are rejected with
  guidance to use `voyage-4-nano`.
- **`max_length`** (new config field, default **1024**) caps tokenizer
  truncation per source field. The ONNX export materializes a full
  `[1, 16, T, T]` attention mask (16·T²·4 bytes), so 1024 is cheap (67 MB,
  ~0.7 s) while 32k needs ~64 GB and fails with a clean embedding error — raise
  it only for whole-document embedding on hardware that can afford it.
- The previous GGUF backend (~7 s/embed) is long gone; this replaces the
  fastembed backend (~4 ms/embed, 384 dims) with voyage-4-nano
  (~35 ms single, ~6 ms batched, 2048 dims).
- `v4nano-embed` is a workspace member and carries the Apache-2.0 license for
  the code adapted from fastembed; the rest of moofile stays MIT.
- `--no-default-features` still drops embedding entirely (~38 MB → ~2.8 MB
  `libmoofile`).

## v1.1.1 (2026-08-15)

### Wheel build fix: manylinux 2_17 → 2_28 (std::regex ABI segfault)

The Linux wheels were built in the 2014-era `manylinux_2_17` container (CentOS 7,
GCC 4.9 headers → `GLIBCXX <= 3.4.19`). `std::regex` has no stable ABI in
libstdc++, and ONNX Runtime's `DeviceDiscovery::GetPciBusId` uses it — so on hosts
with a modern libstdc++ (e.g. Ubuntu 22.04+, libstdc++ 6.0.3x) opening a
collection with `auto_embed` segfaulted inside `regex_traits::transform`.

- Wheels are now built with `manylinux_2_28` (AlmaLinux 8, glibc 2.28, GCC 12,
  `GLIBCXX 3.4.30`) — still covers every supported distro, but the compiled
  `std::regex` code is compatible with current libstdc++ runtimes.
- Docs updated (README, moofile-spec, workflow comments).

## v1.1.0 (2026-08-14)

### Autoembedding now runs ONNX models via fastembed (breaking)

The `llama-gguf` embedding backend is gone, replaced by
[`fastembed`](https://crates.io/crates/fastembed) (ONNX Runtime + HuggingFace
tokenizers). The default model is **`BAAI/bge-small-en-v1.5`** (33M params,
384 dims).

This was not a tuning problem. `llama-gguf` 0.14's `EmbeddingExtractor` ran a
full forward pass **per token** and used the vocabulary *logits* as a stand-in
for the hidden state — slicing the first `hidden_dim` values off a ~152k-wide
distribution. The stored vectors were not sentence embeddings, and each one
cost ~7 seconds.

| | before | after |
|---|---|---|
| embed, short sentence | ~7 000 ms | **~4 ms** |
| semantic search | ~6 500 ms | **~4 ms** |
| model load (warm) | ~500 ms | **~186 ms** |
| stored vector | 1024 dims | 384 dims |
| `libmoofile.so` | 8.3 MB | **38.5 MB** |

The size increase is the statically linked ONNX Runtime and is not avoidable
while embedding is compiled in; `--no-default-features` still drops the whole
feature (down to ~2.8 MB) and `auto_embed` then returns `EmbedDisabled`.
Prebuilt ONNX Runtime binaries exist for all four supported targets
(linux x86-64/aarch64, macOS arm64, Windows x86-64).

**Migrating.** `model` now names a fastembed registry entry rather than a GGUF
file. Three spellings resolve, case-insensitively: the canonical HuggingFace id
(`BAAI/bge-small-en-v1.5`), the exact registry `model_code`
(`Xenova/bge-small-en-v1.5`), or the bare name (`bge-small-en-v1.5`). The old
`hf:repo:file.gguf` syntax is rejected with a message pointing at the
replacement rather than a bare "unknown model". Local model paths are **not**
currently supported — they only ever pointed at `.gguf` files, which no longer
load at all, but the "bring your own model" escape hatch is a follow-up.

Config defaults moved with the model: `dims` 1024 → 384, `query_prefix` to
BGE's instruction, `doc_prefix` to empty (BGE is asymmetric), `batch_size`
1 → 32.

### Changing the embedding model no longer corrupts search silently

Vectors of different widths cannot be compared, and the vector index quietly
skips any vector whose length is not the declared dimension. That is right for
one malformed document and badly wrong for "the embedding model changed":
the entire collection dropped out of the index and every search returned
nothing, with no signal anywhere.

- At open, each autoembedded vector field is checked against both the model's
  output width and the widths actually stored. A mismatch logs a warning and
  **disables** that index; searching it raises `VectorIndexDisabled` naming the
  expected width, the found width and the affected document count.
- New `reembed(source_field)` rewrites every stored vector at the new width,
  retargets the index and its `.meta` entry, and clears the flag. Embedding is
  batched, so it is several times faster per document than re-inserting.
- Re-embedding is never implicit on `open()`. It is a whole-collection write
  that can take minutes, it would turn a read-only handle into a writer, and a
  typo in the model id would destroy the old vectors before anyone noticed.

Note that `merge_meta` keeps a vector index's existing width and ignores a
re-declaration, so simply changing `vector_indexes={...}` on an existing
database does not widen or narrow the index — `reembed()` is what updates it.

### Bulk insert embeds in batches

`insert_many` embedded one document per ONNX pass. It now collects the batch's
texts, embeds them `batch_size` at a time, and inserts with the vectors already
attached — **4.9× faster** per document (2.94 ms → 0.61 ms on a 64-document
batch), with byte-identical output to the per-document path.

A duplicate `_id` already present in the collection is now detected before any
embedding work, so a bad id in a 10 000-document batch no longer costs 10 000
forward passes before erroring. Duplicates *within* a batch are still caught
per document, by the running index.

Documents inserted inside a `batch()` still embed one at a time, since those
are buffered individually.

### `reembed()` in every binding

New across all seven bindings, following the C ABI as usual:

| Language | Signature |
|---|---|
| C | `int64_t moofile_reembed(MooFileCollection*, const char*, char**)` |
| C++ | `int64_t reembed(const std::string&)` |
| Python | `db.reembed(source_field) -> int` |
| Node.js | `db.reembed(sourceField) -> number` |
| Go | `db.Reembed(sourceField string) (int64, error)` |
| Java | `long reembed(String sourceField)` |
| C# | `long Reembed(string sourceField)` |

The pure-Python backend raises `NotImplementedError`, matching how it already
refuses `auto_embed` — a missing attribute would read as a version mismatch
rather than a missing capability.

Every binding's `auto_embed` example and default `dims` moved to bge-small/384.
The documented `vector_indexes` widths moved with them: left at 1024 they would
have tripped the new dimension guard.

### The Python test suites were never running against the Rust backend

`moofile/_native` was a **tracked symlink** to `bindings/python/_native/`, a
directory package. Directories win over extension modules in Python's import
order, so that package permanently shadowed `moofile/_native.cpython-*.so` and
`import moofile` always fell through to the pure-Python implementation — in
this checkout, on every machine, for anyone running from source.

Both Python suites still reported PASS, because both backends pass most of the
same tests. What they were not doing was exercising the Rust backend at all.
With the shadow removed, two genuine failures surfaced immediately in
`tests-cross/test_autoembed_parity.py` — assertions still expecting the GGUF
error text.

The symlink was added incidentally in an unrelated caching commit and was never
the intended layout. The `__init__.py` inside its target explains the fallback
it causes, which suggests someone hit this, documented the symptom, and left
the cause in place. Refreshing the `.so` inside the symlink target — the
previous advice in CLAUDE.md — cannot work, since that `__init__.py`
deliberately imports nothing.

Check which backend you have with
`python -c "import moofile; print(moofile._NATIVE_LOADED)"`.

### Repo hygiene

- Deleted `core/Cargo.lock` and `bindings/python/Cargo.lock`. Cargo only reads
  the workspace root lockfile, so these two sat tracked and untouched at
  **0.4.1** since July — stale enough to predate autoembedding entirely, and
  misleading to anyone who opened them. Subdirectory lockfiles are now
  gitignored (`Cargo.lock` with `!/Cargo.lock`) so they cannot drift back.

### Other

- Embedding engines are keyed on the configured model id instead of a resolved
  filesystem path. The old key meant re-resolving the model URI on **every
  insert** — for `hf:` models, a filesystem cache probe per document. Invisible
  behind a 7-second embed; not invisible at 4 ms.
- `semantic()` no longer holds the collection's read lock across the embedding
  forward pass; it clones the config and engine handle and releases the lock
  first, as the hybrid path already did.
- `EmbeddingEngine::dims()` reports the model's real output width from
  fastembed's registry. It previously returned a hardcoded `1024` with a
  "will be refined" comment.
- Truncation, normalization and quantization were duplicated across the insert
  and query paths; both now share one `finalize_embedding` helper.

## v1.0.4 (2026-08-08)

### Build fixes

- Fixed `CString` pointer type mismatch in the C binding (`*mut i8` → `*mut u8` cast)

- **`Collection(..., auto_embed={...})` now works from Python.** The PyO3 constructor
  never grew the parameter, so on-device embedding was reachable from every binding
  *except* the one most users are on. It accepted exactly the keys the C ABI parses,
  so an `auto_embed` block is now portable verbatim between Python and the other
  bindings. `model_cache_dir` is exposed alongside it.
- Malformed config is rejected with a message naming the offender — an unknown key,
  an unknown `precision`, a missing `model`, or a non-dict value. Silently ignoring a
  misspelled `precision` would have left vectors at f32 and quadrupled stored size.
- Autoembedding failures (missing model file, no config for a source field, a build
  without the `embed` feature) now raise `MooFileError` from the Python adapter
  instead of a bare `RuntimeError`.
- The pure-Python backend accepts `auto_embed` and raises `NotImplementedError`,
  rather than `TypeError`, so a portable config block no longer looks like a typo.

### The pure-Python fallback announces itself

- Falling back to the pure-Python implementation now emits a `RuntimeWarning` naming
  the underlying import error. Same import, same class name, different feature set —
  the silence is how the autoembedding gap above survived several releases.
  `MOOFILE_PURE_PYTHON=1` silences it; `moofile._NATIVE_IMPORT_ERROR` holds the reason.
- Fixed `Collection.__del__` raising `AttributeError` during GC when the constructor
  failed before assigning its attributes, which buried the real construction error.

### Wheels: abi3, Linux ARM, and a working pure-Python floor

- **Wheels are now `abi3` (`abi3-py310`)** — one wheel per platform covers every
  CPython from 3.10 up. Previously each wheel was pinned to a single minor version
  and only the versions CI happened to build could install. On everything else pip
  did not fail, it resolved *backwards*: `pip install moofile` on macOS 3.12 or
  Windows 3.11 silently installed **0.2.1**, the last release from before the Rust
  core existed. Five of fifteen common platform/version combinations were getting
  1.0.2; nine were getting 0.2.1.
- **Linux ARM64 wheels are built again**, natively on `ubuntu-24.04-arm` rather than
  cross-compiled — cross-builds are what failed here before, and the embedding engine
  pulls a C/C++ toolchain through `llama-gguf`. This covers Docker on Apple Silicon
  and ARM servers.
- **The pure-Python wheel actually ships again.** Its CI job wrote the setuptools
  build backend to `pyproject.toml.bak` and never moved it into place, so it built
  through maturin and produced a native wheel that the publish step then deleted; no
  `py3-none-any` wheel has reached PyPI since 0.3.1. Replaced with
  `scripts/build_pure_wheel.py`, which swaps the backend, builds, and restores.
- **The release refuses to publish an incomplete artifact set.** A missing wheel does
  not break a release, it quietly narrows coverage — which is how the above went
  unnoticed. CI also asserts each wheel is abi3, installs it, and re-checks it on a
  newer interpreter than it was built against.
- Corrected the build instructions: `maturin build` must run from the repo root. Run
  inside `bindings/python/` it produces a wheel containing the compiled module and no
  `moofile/` package at all.

### Native libraries: Linux ARM64

- **`libmoofile` is built for `linux-aarch64`**, natively on `ubuntu-24.04-arm`, and
  ships in the release archives, the npm package (`native/linux-arm64/`) and the
  NuGet package (`runtimes/linux-arm64/`). C, C++, Node, Go, Java and C# all reach
  ARM Linux now — Docker on Apple Silicon and ARM servers included.
- The Node binding's `SUPPORTED_PLATFORMS` list advertised `darwin-x64` and
  `linux-arm64`, neither of which shipped. That list exists to turn an unsupported
  platform into a clear message instead of a dlopen error, so listing a platform that
  is not there produced exactly the confusing failure it is meant to prevent. It now
  matches what CI stages, and both packaging jobs fail if a binary is missing.

### Docs

- Corrected every claim that autoembedding works "transparently in all languages", and
  the platform tables: the native-library archives and the npm/NuGet packages carry
  Linux x64, macOS ARM64 and Windows x64 only, not the Intel macOS and Linux ARM builds
  they advertised. (Python wheels do now cover Linux ARM again — see above.)
- Added `llms.txt`, a compressed full-engine reference for coding agents.

## v1.0.2 (2026-08-07)

### Python native binding fixes

- Repaired PyO3/Python adapter parity for BSON-rich documents, including datetimes, binary values, ObjectIds, Code, and Decimal128.
- Validate document IDs and filters in the Python adapter before crossing the native boundary, preventing malformed inputs from poisoning a native collection lock.
- Restored consistent missing-field range-filter behavior for native full scans and indexed queries.

## v1.0.1 (2026-08-07)

### Minor refinements to language bindings

- **C#**: Added optional `Builders<T>.Filter` property-expression filters for the document-oriented API, including typed overloads across CRUD and search pre-filters.
- **Java**: Added MongoDB-style static `Filters` factories, a versioned binding JAR release artifact, and consumer guidance for `javac`, Maven/Gradle local dependencies, and fat-JAR deployments.
- **Go**: Added `Document`, `Filter`, `Update`, and `SearchOptions` conveniences with named update/search methods that avoid positional `nil` arguments.
- **C++**: Cursors now work in range-for loops; `Batch::commit()` now commits immediately and batch cleanup safely rolls back uncommitted work.
- **Node/TypeScript**: Removed the unsupported `$regex` filter operator from declarations.
- **Release tooling and docs**: Native release re-runs now skip already-published npm versions; Java JARs are included in release checksums; stale test counts and examples were refreshed.

## v1.2.0

## v1.0.0 (2026-08-07)

Moofile finally hits 1.0 with proper multi language bindings. These all work on my machine, but are in a 1.0 state and may have rapid fixes in the coming weeks. We've finally escaped Python world and can Moo everywhere!

- Stable release of the shared Rust core and Python, C, C++, Go, Node.js, and C# bindings.
- GitHub Actions publish Python wheels to PyPI, native libraries as release artifacts, and packages to npm and NuGet.
- Java remains available as a buildable binding and release artifact, but is not published to a package registry yet.


### Language bindings

- **Node.js, Go, Java and C# bindings are now functional.** As first written
  none of them could execute: the Node binding required `ffi-napi` (which no
  longer builds on Node 18+), the Java binding declared JNI natives against
  symbols `libmoofile` does not export, and the C# binding did not compile and
  applied its error check to the result pointer rather than the error pointer.
- **Node.js ported from `ffi-napi` to [koffi](https://koffi.dev)** — prebuilt,
  no node-gyp. Returned strings are now freed, the library handle is cached
  across collections, and cursors are iterable and self-freeing.
- **Java rewritten on the Foreign Function & Memory API** (JDK 22+). No
  third-party jars and no Maven or Gradle; `build.sh` is the whole toolchain.
  Adds a correct JSON parser — the previous comma-splitting one corrupted any
  document containing a vector, a nested object, or a comma inside a string.
- **C# rewritten** on a working P/Invoke layer with UTF-8 marshalling and a
  `NativeLibrary` resolver. Documents now hold plain CLR values instead of
  `JsonElement`, so `doc["age"]` compares as expected. Targets `net10.0`.
- **Go: `auto_embed` now reaches the core.** `AutoEmbedConfig` fields lacked
  JSON tags, so they serialised as `Model`/`Target` while the C layer reads
  `model`/`target` — semantic search could never be configured from Go. Also
  fixes a `C.CString` leak on every call and adds `-L`/rpath link flags.
- **C++ wrapper now compiles.** `insert`, `insert_many` and `stats` referenced
  an undeclared `err`, so the header was unusable; missing `<optional>` and
  `<cstdint>` includes added. `db.count({})` no longer trips over nlohmann
  decaying `{}` to null.

### C ABI

- **New `moofile_find_ex()`** exposes the full query builder — sort, skip,
  limit, group, agg — which had no C entry point, leaving every non-Python
  language unable to sort or paginate. Surfaced in all six bindings.
  Unrecognised option keys and aggregation names are errors, so a typo cannot
  silently return the whole collection.
- **Documented the ABI contract** in `moofile.h`: error conventions, string
  and cursor ownership, no-match semantics, and `dead_records` accounting.
- **Corrected the header's no-match documentation.** `update_one` and
  `replace_one` fail when nothing matches (matching Rust and Python, which
  raise `DocumentNotFound`); the header claimed they return 0.

### Build

- **The `embed` feature is now real.** `bindings/c/Cargo.toml` referenced
  `moofile-core/embed`, a feature that never existed — the line was a hard
  build error, and `llama-gguf` was an unconditional dependency, so
  autoembedding was always compiled in with no way to opt out. `moofile-core`
  now has a genuine `embed` feature, **on by default** and forwarded by both
  bindings. `--no-default-features` drops `llama-gguf` and ~300 transitive
  crates (libmoofile ~8.3 MB → ~2.8 MB); the config types remain and any
  attempt to embed returns the new `MooFileError::EmbedDisabled`. Only
  `core/src/embed.rs` needed `cfg` attributes: `EmbeddingEngine` becomes an
  uninhabited stub, so the rest of the crate stays gate-free.
- **`BUILDING.md`** — toolchain setup for all seven languages: one
  `apt install` for Ubuntu, plus Fedora, Arch, Alpine, macOS and best-effort
  Windows notes, version floors, and troubleshooting.
- **`scripts/test-all.sh`** — runs every suite and prints a summary, skipping
  languages whose toolchain is absent rather than failing.

### Core

- **Fixed `group()` in the Rust backend.** Group keys were stringified via
  `Bson::to_string()`, so a group on a string field produced `"\"eng\""` and a
  group on an integer produced text — diverging from the Python backend for
  every type. Keys now keep their original BSON value, and first-seen ordering
  matches Python.
- **`semantic()` exposed in the PyO3 binding**, which lacked it even though
  the core and C ABI both had it. The pure-Python backend raises
  `NotImplementedError` explaining that autoembedding needs the Rust engine.

### Tests

- Test suites for every binding: C 73, C++ 42, parity 8, Node 22, Go 22,
  Java 30, C# 30 — plus runnable examples for Node, Go, Java and C#.
- **`run_tests.sh` now runs the cross-backend parity suite**, and the C/C++
  suites build again (they were failing on missing includes and an
  nlohmann download path that did not match the `#include`).
- **Fixed the parity harness**, which was aborting with `free(): invalid size`:
  it declared C string returns as `c_char_p`, so ctypes discarded the real
  pointer and handed Python's own buffer to `moofile_free_string`. Its error
  slot was also a NULL `char**`, which disabled error reporting entirely, and
  its Python/Rust backends never actually opened a batch — making their
  rollback a silent no-op.

## v0.6.0 (2026-07-28)

- **Datetime parity**: BSON datetime round-tripping fixed in Rust backend (naive ↔ timezone-aware handled consistently)
- **Normalised BSON values**: Documents are now round-tripped through BSON encode/decode on write, ensuring reads always return the same Python types regardless of pickle cache validity (`Binary` → `bytes`, tz-aware → naive `datetime`)
- **`_id` enforcement**: Both backends now reject non-string `_id` at write time with `InvalidIdError` (Rust backend silently skipped them on replay, causing silent data loss)
- **Filter BSON normalisation**: Query filter values are now BSON-normalised before matching, fixing type mismatch bugs (e.g. tz-aware datetime in query vs naive datetime on disk)
- **`_check_id`**: Shared validation ensures Rust and Python reject the same documents
- **API guard tests**: Cross-backend parity tests for BSON types, filter operators, and edge cases
- **`update_one`/`replace_one`**: Now raise `DocumentNotFoundError` (was silently no-oping in some paths)

## v0.5.2 (2026-07-14)

- **Multi-process locking redesigned**: No lock held during normal operation — exclusive `flock` only acquired briefly during writes. Multiple processes (e.g. web UI + bot runners) can now open the same BSON file simultaneously for reads
- **`_catch_up` reconciliation**: Long-lived readers now detect and replay another process's suffix appends without a full reload
- **Compact safety**: Compaction re-acquires the exclusive lock for the full rewrite, preventing interleaved appends from other processes

## v0.5.1 (2026-07-13)

- **Fix `hybrid_search` type mismatch**: Python/Rust backends now return identical (doc, score) tuple shapes
- **Dead code removal**: Cleaned up vestigial pure-Python paths no longer reachable with the native backend active
- **API guard tests**: Added cross-implementation compatibility tests in `tests-cross/`

## v0.5.0 (2026-07-13)

- **Autoembedding**: Local GGUF embedding models via `auto_embed` parameter — no external API needed
- **`.semantic()`**: New query method for autoembedding-based semantic search
- **Model URIs**: `hf:user/repo:filename.gguf` (HuggingFace Hub auto-download), local file paths, absolute paths
- **Precision options**: `f32`, `int8`, `uint8`, `binary` with QAT-trained models
- **Hybrid + autoembed**: Pass `None` for `query_vector` in `hybrid_search()` to auto-embed the query text
- **Multiple auto-embed sources**: Different source fields can target different vector fields with different models/precisions
- **MRL truncation**: Multi-vector-dimension support via `dims` parameter

## v0.4.1 (2026-07-13)

- **Docs updated**: Coding-agent-friendly documentation with full API reference, gotchas, and edge cases

## v0.4.0 (2026-07-13)

- **Index caching**: Disposable `.bson.cache` file for sub-second cold opens — pickled snapshot of in-memory indexes validated against data file fingerprint (size + mtime + config)
- **Cache invalidation**: Automatically invalidated on data file change, version mismatch, or config change — safe to delete at any time
- **`reindex()`**: New method to rebuild indexes from scratch
- **Durability modes**: `"none"` (no flush), `"os"` (flush to page cache, default), `"fsync"` (sync to disk)
- **`sync()`**: Force an fsync for batched durability patterns
- **Compact fsync**: Compaction now fsyncs both the replacement file and its parent directory
- **`ReadOnlyError`**: Write attempts on read-only collections now raise a proper exception
- **`InvalidFilterError`**: Structured error for malformed query filters

## v0.3.5 (2026-07-11)

- **Python 3.13 & 3.14 wheels**: Linux CI now builds and uploads wheels for the latest Python versions

## v0.3.4 (2026-07-10)

- **GitHub Releases**: Automated release creation on tag push

## v0.3.3 (2026-07-10)

- **Wheel stripping**: Strip `linux_x86_64` wheels before PyPI upload to reduce package size

## v0.3.2 (2026-07-10)

- **Manylinux tag fix**: Corrected platform tag for PyPI compatibility

## v0.3.1 (2026-07-10)

- **macOS re-added**: Restored macos-latest to the build matrix after temporary drop
- **CI hardening**: Fixed various build matrix issues

## v0.3.0 (2026-07-09)

- **Rust native backend**: 20× performance improvement on key operations via PyO3 + maturin
- **Dual-engine architecture**: Auto-selects Rust native extension when available, falls back to pure Python
- **`_rust_adapter.py`**: Wraps `NativeCollection` to match the Python `Collection` API, enabling transparent backend switching
- **Cross-platform wheels**: CI builds for Linux (manylinux), macOS, and Windows via maturin
- **`core/` crate**: Standalone Rust library with BSON storage engine, query engine, BM25 text search, and vector search
- **`bindings/python/`**: Python bindings crate using PyO3
- **Vector search optimisations**: Cosine similarity via normalised dot product, numpy-free in Rust path

## v0.2.1 (2026-03-08)

- **Complete pyproject.toml**: Added metadata for PyPI publication (classifiers, keywords, license)

## v0.2.0 (2026-03-08)

- **BM25 text search**: Full-text search with Porter stemming via `text_indexes=["field"]` and `.text_search()`
- **Vector similarity search**: Cosine similarity via `.vector_search()` with numpy backend
- **Hybrid RRF search**: Reciprocal Rank Fusion combining BM25 + vector scores via `.hybrid_search()`
- **Document search example**: `examples/document_search.py` — end-to-end search with all three modes
- **`TextIndex` class**: In-memory inverted index with BM25 scoring, Porter stemming, and pickle support
- **`IndexManager` enhancements**: Vector index arrays, text index management, `rebuild_vector_indexes()`
- **`VectorQuery`, `TextQuery`, `HybridQuery`**: Dedicated result classes returning `[(doc, score)]` tuples
- **`$elemMatch` filter operator**: Array element matching for nested document conditions
- **`$exists` filter operator**: Check field presence/absence
- **Aggregation pipeline**: `group().agg()` with `count`, `sum`, `mean`, `min`, `max`, `collect`, `first`, `last`

## v0.1.0 (2026-03-06)

- **Initial release**: "Something I needed for a home project"
- **Core CRUD**: `insert`, `find`, `find_one`, `update_one`, `update_many`, `replace_one`, `delete_one`, `delete_many`
- **MongoDB-style query filters**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$and`, `$or`, `$not`
- **Sorted indexes**: O(log n) field lookups via `sortedcontainers.SortedDict`
- **Append-only BSON storage**: Record types `LIVE` (0x01), `TOMBSTONE` (0x02), `REPLACEMENT` (0x03)
- **Compaction**: `compact()` rewrites live documents to a fresh file, atomically renamed
- **Stats**: `stats()` returns document count, dead records, file size, and dead ratio
- **CLI tools**: `moosh` (interactive REPL), `moo2json` (BSON ↔ JSON), `moo2mongo` (BSON ↔ MongoDB), `moo2sqlite` (BSON ↔ SQLite)
- **Context manager**: `with Collection(...) as db:` for auto-close
- **Examples**: `basic_crud.py`, `contacts_app.py`, `analytics.py`, `event_log.py`
