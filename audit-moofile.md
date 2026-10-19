# moofile audit: what tripped me up building on it

**Context.** This comes from building pengyplexity's Discord channel-history
search (`pengyplexity/discordbot/history.py`) on moofile's BM25 text index. That
work ran against the installed **1.2.2** (`~/.local/lib/python3.14/site-packages`).
Every finding below was then re-checked against this checkout, **HEAD `7e986a6`
(pyproject 1.2.5)**, with `PYTHONPATH=.`. Both backends were tested: native
(`moofile._NATIVE_LOADED == True`) and pure Python (`moofile.collection.Collection`
imported directly, as `tests-cross/conftest.py` does).

Findings are sorted by how much they matter. Each one says whether it is a
**bug**, a **footgun** (works as designed, but the design invites the mistake),
or a **docs** gap. Several things I first took for bugs turned out to be
documented in `llms.txt`. I worked from `inspect` and experiments rather than
reading the docs first, and the audit says so where that applies.

| # | Severity | Kind | Finding |
|---|---|---|---|
| 1 | **High** | Bug — **fixed** | BM25 IDF goes negative for terms in more than half the documents, which inverts the ranking |
| 2 | **High** | Bug | Pure-Python backend crashes inserting a list/dict value into an indexed field (native accepts it) |
| 3 | Medium | Footgun | `sort("f", 1)` sorts descending; `-1` does too |
| 4 | Low | Footgun | `sort()` before `text_search()` is silently ignored; after it, `AttributeError` |
| 5 | Low | Docs | Equality doesn't match array elements (Mongo does); missing from "differences from MongoDB" |
| 6 | Low | Footgun | `update_one` raises on no match, and there's no upsert, so callers must find-then-write |
| 7 | Low | Docs | BM25 ties come back in `_id` order, which is effectively random relative to insertion |
| 8 | Low | Docs | Unsorted `find()` order differs between backends |
| 9 | Low | Ergonomics | Datetimes read back naive (documented), so every consumer re-attaches UTC |
| 10 | Low | Hygiene | Stale `moofile.egg-info` makes `moofile.__version__` say 1.0.2 in this checkout |
| 11 | Medium | Bug | Pure-Python tokenizer drops digits, so `v1.2.0` / `14900K` are unsearchable there but not on native |

---

## 1. BM25 IDF goes negative for common terms, which inverts ranking (High, bug)

> **Status: fixed in the working tree, not yet released.** Both backends now
> use `ln(1 + (N - df + 0.5) / (df + 0.5))`. `add_and_search` requires the
> correct order, there is a new Rust test for a term in every document, and
> `tests-cross/test_bm25_scoring_parity.py` covers both backends. Verified on
> 2026-10-19 with a rebuilt native extension and `libmoofile.so`: `cargo test`
> (core, 119), `tests/` (210), `tests-cross/` on both backends (204, 7 skipped),
> and the C / C++ / parity suites (78 / 47 / 8) all pass. pengyplexity's suite
> also passes against the fixed build.

Both backends use the unsmoothed Robertson–Spärck Jones IDF:

- `core/src/text.rs:172`: `((n_docs - df + 0.5) / (df + 0.5)).ln()`
- `moofile/text_search.py:133`: `math.log((self._doc_count - df + 0.5) / (df + 0.5))`

When `df > N/2` the ratio drops below 1 and the IDF is **negative**. The final
score is `Σ idf × tf_component`, sorted descending. So for a common term,
every extra occurrence, and every bit of BM25 length normalisation that should
help a document, pushes the document *down*. The ranking runs backwards
exactly where a term is common.

**Repro (identical on both backends):**

```python
c = Collection(path, text_indexes=["content"])
c.insert_many([{"i": 0, "content": "redis cache broke"},
               {"i": 1, "content": "redis redis redis"},
               {"i": 2, "content": "lunch at noon"}])
c.find({}).text_search("content", "redis", limit=5).to_list()
# [(i=0, -0.511), (i=1, -0.803)]   <- the doc that is *all* "redis" ranks last
```

Add three unrelated docs so "redis" falls to df 2/6, and the order flips to
the correct one: `[(i=1, 0.834), (i=0, 0.488)]`. **The same two documents rank
in opposite orders depending on how many unrelated documents share the
collection.**

Other symptoms:
- A term in every document (`df == N`) still has a negative IDF. Searching for
  it alone ranks the shortest, most on-topic documents *below* longer ones,
  because length normalisation makes their `tf_component` larger, and that
  larger component multiplies a negative IDF.
- A stopword-ish term at exactly `df == N/2` scores `0.0`, so it matches but
  carries no weight.

**Why it matters in practice.** The corpora where this bites are the normal
ones for an embedded store:
- **Small collections.** Pengyplexity's per-user memory store often holds a
  handful of documents, so most query terms are in half of them.
- **Topical collections.** A Discord channel named `#deploys` has "deploy" in
  most messages.
- **Pre-filtered searches.** `find(filter).text_search(...)` narrows to a few
  candidates. IDF is computed over the whole index, though, so this one is
  less direct.

It also leaks into anything built on the scores:
- **`hybrid_search`.** RRF fuses *ranks*, so an inverted lexical rank goes
  straight into the fused result.
- **Score arithmetic downstream.** Pengyplexity's `core/memory.py` does
  `score * 1.5` to boost title matches, which makes a negative title score
  *worse*. It also normalises lexical scores as `lex / max_lex` to get a
  0–1 "relative" signal, and with all-negative scores that clamps nearly
  every hit to 1.0. Both were written assuming BM25 scores are positive.

**It's known, but filed under the wrong problem.**
- `docs/README.md:549` says BM25 scores "can even be negative (when a term
  appears in >50% of documents)". That's offered as a reason to fuse with RRF,
  not as a ranking defect, but RRF doesn't fix an inverted rank.
- `tests/test_text_search.py:89` has the same note.
- The Rust unit test `text.rs::add_and_search` (around line 328) accepts
  either order for two ML documents ("Either "1" or "2" is valid"). "learning"
  is in 2 of its 3 docs, so its IDF is negative and the doc with the extra
  "learning" loses. The test was loosened around the symptom.

**Suggested fix.** Use the Lucene/BM25+ form, which is always positive and
keeps the familiar shape:

```
idf = ln(1 + (N - df + 0.5) / (df + 0.5))
```

Change it in both backends at once, and add a cross-backend test (in
`tests-cross/`) asserting:
- more occurrences of a query term never lowers a document's rank, all else
  equal, including when the term is in every document;
- all scores are `> 0`.

Then tighten `add_and_search` to require doc "2" first. Note in the changelog
that scores change scale, since anyone thresholding on raw BM25 values (rare,
but possible) will notice.

## 2. Pure-Python backend crashes on list/dict values in indexed fields (High, bug)

```python
from moofile.collection import Collection          # the pure-Python backend
Collection(path, indexes=["tags"]).insert({"tags": ["a"]})
# TypeError: cannot use 'list' as a dict key (unhashable type: 'list')
#   moofile/index.py:53  `if val not in idx:`
Collection(path, indexes=["meta"]).insert({"meta": {"k": 1}})
# TypeError: cannot use 'dict' as a dict key (unhashable type: 'dict')
```

The native backend accepts both. The Python implementation is the documented
fallback whenever the extension fails to load (`moofile/__init__.py` warns and
falls back), so the same program works on one machine and throws a bare
`TypeError`, not a `MooFileError`, on another. This happens on *insert*,
before any query is involved. A schema that puts tags in an indexed field
can't be written at all without the native build.

**Suggested fix.** Decide on the semantics and make both backends match:

- **Multikey** (Mongo): index each element of a list, which makes
  `{"tags": "a"}` hit the index. This would also resolve #5.
- **Whole-value**: index a canonical, hashable encoding of the list/dict
  (e.g. its BSON bytes or a frozen tuple form).

Either way, add a `tests-cross/` case for inserting and finding documents with
list- and dict-valued indexed fields. This probably slipped through because
the cross suite indexes only scalar fields.

## 3. `sort("f", 1)` sorts descending (Medium, footgun)

The signature is `Query.sort(field, descending=False)`, and it's documented
that way (`llms.txt:99`, `docs/README.md:278`). But the project pitches
"MongoDB-style ergonomics", and Mongo spells direction as `1` / `-1`. Both
are truthy:

```python
c.find({}).sort("n", 1).to_list()    # [3, 2, 1, 0]  <- descending
c.find({}).sort("n", -1).to_list()   # [3, 2, 1, 0]
```

I wrote `sort("message_id", 1)` for "oldest first" and got the newest. Nothing
complained. Only a test asserting the oldest record's date caught it.

**Suggested fix.** Reject anything that isn't a `bool` (`TypeError: sort()
takes descending=True/False, not 1`). Alternatively, accept `1`/`-1` with
Mongo's meaning. Either is better than silently treating `1` as descending.
Do it in `query.py` and the native adapter alike.

## 4. `sort()` and `text_search()` don't compose, silently in one direction (Low, footgun)

- `find(f).sort("i", descending=True).text_search(...)`: the sort is
  **dropped without a word**. Results come back in BM25 order on both
  backends.
- `find(f).text_search(...).sort("i")`: `AttributeError: 'TextQuery' object
  has no attribute 'sort'` (`'_NativeTextQuery'` on native).

`llms.txt` says search entry points "replace the terminal set", which
explains the second case if you read closely. But the first case is easy to
write and impossible to notice. The same probably holds for `.skip()` and
`.limit()` placed before a search, which I didn't test.

**Suggested fix.** Raise `InvalidQueryError` (or similar) when `text_search` /
`vector_search` / `hybrid_search` / `semantic` is called on a query that
already has sort/skip/limit/group set. Alternatively, apply the sort to the
search results. Either way, give `TextQuery` a helpful `__getattr__` error
instead of the bare `AttributeError`.

## 5. Equality doesn't match array elements (Low, docs)

```python
c.insert_many([{"tags": ["a", "b"]}, {"tags": "a"}])
c.find({"tags": "a"})                       # -> only {"tags": "a"}
c.find({"tags": {"$in": ["a"]}})            # -> only {"tags": "a"}
c.find({"tags": {"$elemMatch": {"$eq": "a"}}})   # -> only {"tags": ["a","b"]}
```

In MongoDB the first two match both documents. `$elemMatch` is documented
(`llms.txt:133`), so this works as designed. But `docs/README.md:93-95`
has a short "differences from MongoDB" list (update kwargs, strict
`update_one`, `delete_one` returns bool), and this belongs on it. It's the
difference most likely to produce silently wrong results, because nothing
errors. With no `$regex` either (documented, `llms.txt:136`), case-insensitive
matching means storing a normalised copy of the field. I ended up storing
`author_handle_key` / `author_name_key` as scalar lowercased fields, because
a list of name variants couldn't be matched by equality.

**Suggested fix.** Add it to the README's differences list. If #2 goes
multikey, consider Mongo's array semantics for `$eq`/`$in` at the same time.

## 6. `update_one` raises on no match, and there's no upsert (Low, footgun)

`update_one` and `replace_one` raise `DocumentNotFoundError` on no match,
while `update_many`, `delete_one` and `delete_many` return 0 / `False`. This
is documented clearly (`llms.txt:78-81`, `docs/README.md:94`, C ABI
`llms.txt:387`). I didn't read that first and was caught out.

The friction is less the strictness than the missing alternative. There's no
`upsert=True`, so every "insert or update" becomes `find_one` + `insert` /
`update_one`. That's two lookups, plus a `try/except DocumentNotFoundError`
wherever a miss is expected. Pengyplexity's `ConversationMap._set` and the new
`HistoryIndex.edit` both carry this boilerplate.

**Suggested fix.** Add `upsert=True` to `update_one`, or a separate
`upsert_one(where, set=...)`. Optionally add `missing_ok=True` for the
"update if present" case. The strict default can stay.

## 7. BM25 ties are ordered by `_id` (Low, docs)

The Rust core breaks score ties on `doc_id` (`text.rs`, comment above
`scored.sort_by`), and so does Python (`text_search.py:151`, key
`(-score, doc_id)`). That makes the order deterministic for a given
collection. But `_id`s aren't insertion-ordered, so across fresh collections
holding the same documents the order of a tie changes. In 8 fresh collections
with identical inserts, two tied documents came back in both orders. A test
asserting `[:2] == [a, b]` passed alone and failed in the full suite.

Ties are common because of #1 and because single-character tokens aren't
indexed. Five documents that differed only in the number of `x` words all
scored exactly `-2.398`.

**Suggested fix.** Either break ties on insertion sequence, which is what a
user expects, or document that ties are ordered by `_id`.

## 8. Unsorted `find()` order differs between backends (Low, docs)

```python
c.insert_many([{"i": i} for i in range(5)])
[d["i"] for d in c.find({}).to_list()]
# python: [0, 1, 2, 3, 4]     native: [1, 2, 3, 4, 0]
```

No order is promised, and that's fine. But the Python backend happens to
return insertion order, which makes it easy to depend on by accident and then
break on native. This briefly misled me: I indexed `find({})[1]` on native,
got the wrong document, and nearly reported a datetime bug that didn't exist.

**Suggested fix.** Document that `find()` without `.sort()` has no defined
order. Or make native return insertion order too, if that's cheap.

## 9. Datetimes read back naive (Low, ergonomics; documented)

Aware datetimes are stored as UTC and read back **naive** (documented,
`llms.txt:471`). Filters accept aware values and compare instants correctly,
and a `-05:00` value is converted, not truncated. Verified on both backends.

The cost falls on consumers. Every read site needs
`.replace(tzinfo=timezone.utc)` before comparing with `datetime.now(timezone.utc)`,
or it gets a `TypeError` mixing naive and aware. Pengyplexity has this in at
least four places (`api.py`, `core/apikeys.py`, `discordbot/conversations.py`,
`discordbot/history.py`), each with its own "moofile hands datetimes back
naive" comment.

**Suggested fix.** Add an opt-in `Collection(..., tz_aware=True)` that returns
aware UTC datetimes.

## 10. Stale `moofile.egg-info` in the checkout (Low, hygiene)

With `PYTHONPATH=.`, `moofile.__version__` reports **1.0.2**. That comes from
`moofile.egg-info/PKG-INFO` at the repo root, while `pyproject.toml` says
1.2.5. It's harmless at runtime, but it's the same family of confusion
`CLAUDE.md` already warns about with site-packages vs. checkout. If someone
uses `__version__` to work out which code they're running, it lies. Delete
the egg-info (and `.gitignore` it if it isn't already), or fall back to
reading `pyproject.toml` when running from source.

Related: pengyplexity requires `moofile>=1.2.1` and was running 1.2.2 from
user site-packages. None of the findings above are fixed in 1.2.5, so a
version bump alone changes nothing.

## 11. Pure-Python tokenizer drops digits (Medium, bug)

The Rust tokenizer (`core/src/text.rs`, `tokenize_and_stem`) keeps digits and
holds compound identifiers together. Its comment explains why: an earlier
`[a-zA-Z]+` "made version strings, driver revisions and part numbers
unsearchable". The Python backend still uses exactly that pattern
(`moofile/text_search.py`, `_tokenize_and_stem`:
`re.findall(r'\b[a-zA-Z]+\b', ...)`).

```python
c.insert({"content": "upgraded to v1.2.0 on the 14900K"})
# text_search for "v1.2.0" / "14900K" / "upgraded"
# python: 0 / 0 / 1        native: 1 / 1 / 1
```

The same query finds a document on one backend and not the other. Document
lengths differ too, so even shared terms score differently once a document
contains digits.

**Suggested fix.** Port the Rust pattern and its split-on-separator behaviour
to Python. Add a `tests-cross/` case with version strings and part numbers.
The existing cross-backend scoring tests only pass because their corpora are
all letters.

---

## What worked well

These all behaved exactly as I expected, on both backends:
- `$in` on scalars, combined range filters on aware datetimes, and
  `find(prefilter).text_search(...)`.
- Text re-indexing on `update_one` (old terms gone, new ones found) and on
  delete.
- Persistence across close/reopen, and `insert_many` / `delete_many({"$in": ...})`.

The error for a malformed filter (`InvalidFilterError: unknown operator
'$regex' on field 'tags'`) is a model of what #3 and #4 should do: fail loudly,
naming the problem.

## Probe scripts

The repros above are condensed from scripts that ran each case against both
backends in a fresh temp collection. All of them are short enough to paste
into a `tests-cross/` test as-is. The ones I'd add first are #1 (rank
monotonicity in tf, scores > 0) and #2 (list/dict values in indexed fields).
