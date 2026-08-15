/**
 * MooFile — Node.js binding.
 *
 * Calls the C shared library (libmoofile.so / .dylib / .dll) through koffi.
 * All documents cross the FFI boundary as JSON strings.
 *
 * koffi rather than ffi-napi: ffi-napi needs a node-gyp build and no longer
 * compiles against the N-API headers shipped with Node 18+.  koffi is
 * prebuilt and needs no toolchain.
 *
 * Usage:
 *   const { Collection } = require('./moofile');
 *   const db = new Collection('data.bson', { indexes: ['email'] });
 *   db.insert({ name: 'Alice', email: 'a@test.com' });
 *   console.log(db.find({ email: 'a@test.com' }).toArray());
 *   db.close();
 */

'use strict';

const fs = require('fs');
const path = require('path');
const koffi = require('koffi');

// ---------------------------------------------------------------------------
// Library loading
// ---------------------------------------------------------------------------

/**
 * Opaque handle types.  Declaring them keeps koffi from coercing the pointers
 * to plain numbers, so a handle from one collection cannot be passed to
 * another type's function by accident.
 */
const MooFileCollection = koffi.opaque('MooFileCollection');
const MooFileCursor = koffi.opaque('MooFileCursor');
const MooFileSearchCursor = koffi.opaque('MooFileSearchCursor');

/**
 * Returned `char*` buffers are owned by us and must be released with
 * moofile_free_string, so they are declared as raw pointers rather than
 * koffi's `str` (which would copy and then leak the original).  Error
 * out-params are `char**`, modelled as a one-element array of pointers.
 */
const ErrOut = koffi.out(koffi.pointer('void*'));

let cachedLib = null;
let cachedLibPath = null;

/**
 * Locate libmoofile.
 *
 * Order: MOOFILE_LIB, then the library bundled in the published package, then
 * the in-repo cargo output directories (so a git checkout works without an
 * install step), then the platform loader's own search path.
 */
function defaultLibPath() {
    if (process.env.MOOFILE_LIB) return process.env.MOOFILE_LIB;

    const ext = process.platform === 'darwin' ? '.dylib'
        : process.platform === 'win32' ? '.dll'
        : '.so';
    const stem = process.platform === 'win32' ? 'moofile' : 'libmoofile';
    const name = stem + ext;

    const roots = [
        // Published package: native/<platform>-<arch>/ mirrors process.platform
        // and process.arch, so the lookup is exact rather than a search.
        path.join(__dirname, 'native', `${process.platform}-${process.arch}`),
        path.join(__dirname, '..', '..', 'target', 'release'),
        path.join(__dirname, '..', '..', 'target', 'debug'),
        __dirname,
    ];
    for (const root of roots) {
        const candidate = path.join(root, name);
        if (fs.existsSync(candidate)) return candidate;
    }
    // Fall through to the platform loader's own search path.
    return name;
}

/**
 * Platform/arch combinations the published package carries binaries for.
 *
 * Keep this in step with the staging step in
 * `.github/workflows/release-libs.yml` — it exists to turn an unsupported
 * platform into a clear message instead of a dlopen error, so listing one that
 * does not ship (this claimed `darwin-x64`, and `linux-arm64` before it was
 * built) produces exactly the confusing failure it is meant to prevent.
 */
const SUPPORTED_PLATFORMS = [
    'linux-x64', 'linux-arm64',
    'darwin-arm64',
    'win32-x64',
];

/**
 * Load and bind the shared library.  Cached: repeated `new Collection()` calls
 * must not re-bind ~30 symbols each time.
 */
function loadLibrary(libPath) {
    const resolved = libPath || defaultLibPath();
    if (cachedLib && cachedLibPath === resolved) return cachedLib;

    let lib;
    try {
        lib = koffi.load(resolved);
    } catch (e) {
        // An unsupported platform gives a confusing dlopen error otherwise,
        // so say plainly that no binary was shipped for it.
        const current = `${process.platform}-${process.arch}`;
        const unsupported = !SUPPORTED_PLATFORMS.includes(current)
            ? `\nNo prebuilt binary ships for ${current}. ` +
              `Supported: ${SUPPORTED_PLATFORMS.join(', ')}.`
            : '';
        throw new MooFileError(
            `failed to load ${resolved}: ${e.message}${unsupported}\n` +
            'Build it with: cargo build -p moofile-c --release\n' +
            'Or point MOOFILE_LIB at an existing library.'
        );
    }

    const api = {
        // Lifecycle
        open: lib.func('MooFileCollection* moofile_open(const char*, const char*, _Out_ void**)'),
        close: lib.func('int moofile_close(MooFileCollection*, _Out_ void**)'),

        // Insert
        insert: lib.func('void* moofile_insert(MooFileCollection*, const char*, _Out_ void**)'),
        insertMany: lib.func('void* moofile_insert_many(MooFileCollection*, const char*, _Out_ void**)'),

        // Query
        find: lib.func('MooFileCursor* moofile_find(MooFileCollection*, const char*, _Out_ void**)'),
        findEx: lib.func('MooFileCursor* moofile_find_ex(MooFileCollection*, const char*, const char*, _Out_ void**)'),
        findOne: lib.func('void* moofile_find_one(MooFileCollection*, const char*, _Out_ void**)'),
        count: lib.func('int64_t moofile_count(MooFileCollection*, const char*, _Out_ void**)'),
        exists: lib.func('int moofile_exists(MooFileCollection*, const char*, _Out_ void**)'),

        // Cursor
        cursorNext: lib.func('void* moofile_cursor_next(MooFileCursor*, _Out_ void**)'),
        cursorFree: lib.func('void moofile_cursor_free(MooFileCursor*)'),

        // Update
        updateOne: lib.func('int moofile_update_one(MooFileCollection*, const char*, const char*, _Out_ void**)'),
        updateMany: lib.func('int64_t moofile_update_many(MooFileCollection*, const char*, const char*, _Out_ void**)'),
        replaceOne: lib.func('int moofile_replace_one(MooFileCollection*, const char*, const char*, _Out_ void**)'),

        // Delete
        deleteOne: lib.func('int moofile_delete_one(MooFileCollection*, const char*, _Out_ void**)'),
        deleteMany: lib.func('int64_t moofile_delete_many(MooFileCollection*, const char*, _Out_ void**)'),

        // Search
        vectorSearch: lib.func('MooFileSearchCursor* moofile_vector_search(MooFileCollection*, const char*, const char*, const char*, int, _Out_ void**)'),
        textSearch: lib.func('MooFileSearchCursor* moofile_text_search(MooFileCollection*, const char*, const char*, const char*, int, _Out_ void**)'),
        hybridSearch: lib.func('MooFileSearchCursor* moofile_hybrid_search(MooFileCollection*, const char*, const char*, const char*, const char*, const char*, int, _Out_ void**)'),
        semanticSearch: lib.func('MooFileSearchCursor* moofile_semantic_search(MooFileCollection*, const char*, const char*, const char*, int, _Out_ void**)'),

        // Search cursor
        searchCursorNext: lib.func('void* moofile_search_cursor_next(MooFileSearchCursor*, _Out_ void**)'),
        searchCursorFree: lib.func('void moofile_search_cursor_free(MooFileSearchCursor*)'),

        // Batch
        batchBegin: lib.func('int moofile_batch_begin(MooFileCollection*, _Out_ void**)'),
        batchCommit: lib.func('int moofile_batch_commit(MooFileCollection*, _Out_ void**)'),
        batchRollback: lib.func('int moofile_batch_rollback(MooFileCollection*, _Out_ void**)'),

        // Utility
        stats: lib.func('void* moofile_stats(MooFileCollection*, _Out_ void**)'),
        compact: lib.func('int moofile_compact(MooFileCollection*, _Out_ void**)'),
        sync: lib.func('int moofile_sync(MooFileCollection*, _Out_ void**)'),
        reindex: lib.func('int moofile_reindex(MooFileCollection*, _Out_ void**)'),
        reembed: lib.func('int64_t moofile_reembed(MooFileCollection*, const char*, _Out_ void**)'),

        // Memory
        freeString: lib.func('void moofile_free_string(void*)'),
    };

    cachedLib = api;
    cachedLibPath = resolved;
    return api;
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

class MooFileError extends Error {
    constructor(msg) {
        super(msg);
        this.name = 'MooFileError';
    }
}

/** Allocate the `char**` out-param every call needs. */
function makeErrOut() {
    return [null];
}

/**
 * Throw if the C call set an error message, freeing it either way.
 * The library sets `*err_out` to NULL on entry, so a non-null slot always
 * means a real failure.
 */
function checkError(api, errOut) {
    const ptr = errOut[0];
    if (ptr === null || koffi.address(ptr) === 0n) return;
    const msg = koffi.decode(ptr, 'char', -1);
    api.freeString(ptr);
    throw new MooFileError(msg);
}

/**
 * Decode an owned `char*` result and release it.  Returns null for a NULL
 * pointer, which the C API uses for "no result" (not an error).
 */
function takeString(api, ptr) {
    if (ptr === null || koffi.address(ptr) === 0n) return null;
    const s = koffi.decode(ptr, 'char', -1);
    api.freeString(ptr);
    return s;
}

// ---------------------------------------------------------------------------
// Cursors
// ---------------------------------------------------------------------------

/** Iterator over query results.  Iterable, and auto-freed once exhausted. */
class Cursor {
    constructor(ptr, api) {
        this.ptr = ptr;
        this.api = api;
    }

    /** Next document, or null when exhausted. */
    next() {
        if (!this.ptr) return null;
        const errOut = makeErrOut();
        const raw = this.api.cursorNext(this.ptr, errOut);
        checkError(this.api, errOut);
        const s = takeString(this.api, raw);
        if (s === null) {
            // Exhausted — release the cursor eagerly rather than waiting for
            // an explicit close() the caller may never make.
            this.close();
            return null;
        }
        return JSON.parse(s);
    }

    /** Collect all remaining documents. */
    toArray() {
        const docs = [];
        let doc;
        while ((doc = this.next()) !== null) docs.push(doc);
        return docs;
    }

    [Symbol.iterator]() {
        return {
            next: () => {
                const value = this.next();
                return value === null ? { done: true, value: undefined }
                                      : { done: false, value };
            },
        };
    }

    /** Release the cursor.  Safe to call more than once. */
    close() {
        if (this.ptr) {
            this.api.cursorFree(this.ptr);
            this.ptr = null;
        }
    }
}

/** Iterator over search results — `{ doc, score }` pairs. */
class SearchCursor {
    constructor(ptr, api) {
        this.ptr = ptr;
        this.api = api;
    }

    next() {
        if (!this.ptr) return null;
        const errOut = makeErrOut();
        const raw = this.api.searchCursorNext(this.ptr, errOut);
        checkError(this.api, errOut);
        const s = takeString(this.api, raw);
        if (s === null) {
            this.close();
            return null;
        }
        const [doc, score] = JSON.parse(s);
        return { doc, score };
    }

    toArray() {
        const results = [];
        let item;
        while ((item = this.next()) !== null) results.push(item);
        return results;
    }

    [Symbol.iterator]() {
        return {
            next: () => {
                const value = this.next();
                return value === null ? { done: true, value: undefined }
                                      : { done: false, value };
            },
        };
    }

    close() {
        if (this.ptr) {
            this.api.searchCursorFree(this.ptr);
            this.ptr = null;
        }
    }
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/**
 * Build the options blob for moofile_find_ex from a plain object.
 *
 * Accepts `{ sort, desc, skip, limit, group, agg }`, where `agg` is an array
 * of `{ func, field }` (or the shorthand string `'count'`).
 */
const FIND_OPTION_KEYS = new Set(['sort', 'desc', 'skip', 'limit', 'group', 'agg']);

function buildFindOptions(opts) {
    // Reject typos here rather than silently dropping them — an ignored
    // `limt: 10` would quietly return the whole collection.
    for (const key of Object.keys(opts)) {
        if (!FIND_OPTION_KEYS.has(key)) {
            throw new MooFileError(
                `unknown find option '${key}' (expected one of: ` +
                `${[...FIND_OPTION_KEYS].join(', ')})`
            );
        }
    }

    const out = {};
    if (opts.sort !== undefined && opts.sort !== null) {
        out.sort = { field: opts.sort, desc: !!opts.desc };
    }
    if (opts.skip !== undefined && opts.skip !== null) out.skip = opts.skip;
    if (opts.limit !== undefined && opts.limit !== null) out.limit = opts.limit;
    if (opts.group !== undefined && opts.group !== null) out.group = opts.group;
    if (opts.agg !== undefined && opts.agg !== null) {
        out.agg = opts.agg.map(a => (typeof a === 'string' ? { func: a } : a));
    }
    return out;
}

class Collection {
    /**
     * @param {string} path Path to the .bson file (created if absent).
     * @param {object} config indexes, vector_indexes, text_indexes, readonly,
     *   durability, auto_embed, model_cache_dir, and `libPath` to override
     *   shared-library discovery.
     */
    constructor(filePath, config = {}) {
        this.api = loadLibrary(config.libPath);

        // Undefined keys drop out of JSON.stringify, so the Rust side sees
        // only what was actually set and applies its own defaults.
        const configJson = JSON.stringify({
            indexes: config.indexes,
            vector_indexes: config.vector_indexes,
            text_indexes: config.text_indexes,
            readonly: config.readonly,
            durability: config.durability,
            auto_embed: config.auto_embed,
            model_cache_dir: config.model_cache_dir,
        });

        const errOut = makeErrOut();
        const handle = this.api.open(filePath, configJson, errOut);
        checkError(this.api, errOut);
        if (!handle) throw new MooFileError(`failed to open collection: ${filePath}`);

        this.handle = handle;
        this.path = filePath;
    }

    /** @private Guard against use after close(). */
    _handle() {
        if (!this.handle) throw new MooFileError('collection is closed');
        return this.handle;
    }

    /** @private Call a function whose result needs no string handling. */
    _call(fn, ...args) {
        const errOut = makeErrOut();
        const result = fn(this._handle(), ...args, errOut);
        checkError(this.api, errOut);
        return result;
    }

    /** @private Call a function returning an owned char*. */
    _callStr(fn, ...args) {
        const errOut = makeErrOut();
        const raw = fn(this._handle(), ...args, errOut);
        checkError(this.api, errOut);
        return takeString(this.api, raw);
    }

    /** @private Call a function returning a cursor handle. */
    _callCursor(fn, ...args) {
        const errOut = makeErrOut();
        const ptr = fn(this._handle(), ...args, errOut);
        checkError(this.api, errOut);
        if (!ptr) throw new MooFileError('expected a cursor but got NULL');
        return ptr;
    }

    // ----------------------------------------------------------
    // Insert
    // ----------------------------------------------------------

    /** Insert one document; returns it with `_id` populated. */
    insert(doc) {
        return JSON.parse(this._callStr(this.api.insert, JSON.stringify(doc)));
    }

    /** Insert many documents; returns them with `_id`s populated. */
    insertMany(docs) {
        return JSON.parse(this._callStr(this.api.insertMany, JSON.stringify(docs)));
    }

    // ----------------------------------------------------------
    // Query
    // ----------------------------------------------------------

    /**
     * Find documents matching a filter.  Returns a Cursor.
     *
     * With `options`, applies the query builder — sort, skip, limit, group,
     * agg — in the order filter → group/agg → sort → skip → limit:
     *
     *   db.find({ active: true }, { sort: 'age', desc: true, limit: 10 })
     *   db.find({}, { group: 'dept', agg: ['count', { func: 'sum', field: 'pay' }] })
     */
    find(filter = {}, options = null) {
        if (options) {
            const ptr = this._callCursor(
                this.api.findEx,
                JSON.stringify(filter),
                JSON.stringify(buildFindOptions(options))
            );
            return new Cursor(ptr, this.api);
        }
        return new Cursor(this._callCursor(this.api.find, JSON.stringify(filter)), this.api);
    }

    /** First matching document, or null. */
    findOne(filter = {}) {
        const s = this._callStr(this.api.findOne, JSON.stringify(filter));
        return s === null ? null : JSON.parse(s);
    }

    /** Number of matching documents. */
    count(filter = {}) {
        return Number(this._call(this.api.count, JSON.stringify(filter)));
    }

    /** True if at least one document matches. */
    exists(filter) {
        return this._call(this.api.exists, JSON.stringify(filter)) === 1;
    }

    // ----------------------------------------------------------
    // Update
    // ----------------------------------------------------------

    /** @private Assemble the {set, unset, inc} update blob. */
    static _update(setValues, unsetFields, incValues) {
        const update = {};
        if (setValues && Object.keys(setValues).length > 0) update.set = setValues;
        if (unsetFields && unsetFields.length > 0) update.unset = unsetFields;
        if (incValues && Object.keys(incValues).length > 0) update.inc = incValues;
        return JSON.stringify(update);
    }

    /**
     * Update the first matching document.  Returns true.
     *
     * Throws MooFileError if nothing matches — the same contract as the Rust
     * and Python APIs.  Use exists() first when a miss is expected.
     */
    updateOne(where, setValues = {}, unsetFields = [], incValues = {}) {
        return this._call(
            this.api.updateOne,
            JSON.stringify(where),
            Collection._update(setValues, unsetFields, incValues)
        ) === 1;
    }

    /** Update all matching documents.  Returns the count; 0 if none matched. */
    updateMany(where, setValues = {}, unsetFields = [], incValues = {}) {
        return Number(this._call(
            this.api.updateMany,
            JSON.stringify(where),
            Collection._update(setValues, unsetFields, incValues)
        ));
    }

    /**
     * Replace the first matching document, keeping its `_id`.  Returns true.
     * Throws MooFileError if nothing matches.
     */
    replaceOne(where, replacement) {
        return this._call(
            this.api.replaceOne,
            JSON.stringify(where),
            JSON.stringify(replacement)
        ) === 1;
    }

    // ----------------------------------------------------------
    // Delete
    // ----------------------------------------------------------

    /** Delete the first matching document.  False if nothing matched. */
    deleteOne(where) {
        return this._call(this.api.deleteOne, JSON.stringify(where)) === 1;
    }

    /** Delete all matching documents.  Returns the count. */
    deleteMany(where) {
        return Number(this._call(this.api.deleteMany, JSON.stringify(where)));
    }

    // ----------------------------------------------------------
    // Search
    // ----------------------------------------------------------

    /** Cosine-similarity search over a vector field. */
    vectorSearch(field, queryVector, limit = 10, filter = {}) {
        const ptr = this._callCursor(
            this.api.vectorSearch,
            JSON.stringify(filter), field, JSON.stringify(queryVector), limit
        );
        return new SearchCursor(ptr, this.api);
    }

    /** BM25 full-text search over a text field. */
    textSearch(field, query, limit = 10, filter = {}) {
        const ptr = this._callCursor(
            this.api.textSearch,
            JSON.stringify(filter), field, query, limit
        );
        return new SearchCursor(ptr, this.api);
    }

    /**
     * Hybrid BM25 + vector search fused with Reciprocal Rank Fusion.
     * Pass `queryVector = null` to auto-embed `queryText`.
     */
    hybridSearch(textField, vectorField, queryText, queryVector = null, limit = 10, filter = {}) {
        const qv = queryVector ? JSON.stringify(queryVector) : null;
        const ptr = this._callCursor(
            this.api.hybridSearch,
            JSON.stringify(filter), textField, vectorField, queryText, qv, limit
        );
        return new SearchCursor(ptr, this.api);
    }

    /**
     * Semantic search — auto-embeds `queryText` with the model configured for
     * `sourceField` via auto_embed.
     */
    semantic(sourceField, queryText, limit = 10, filter = {}) {
        const ptr = this._callCursor(
            this.api.semanticSearch,
            JSON.stringify(filter), sourceField, queryText, limit
        );
        return new SearchCursor(ptr, this.api);
    }

    // ----------------------------------------------------------
    // Batch
    // ----------------------------------------------------------

    batchBegin() { this._call(this.api.batchBegin); }
    batchCommit() { this._call(this.api.batchCommit); }
    batchRollback() { this._call(this.api.batchRollback); }

    /** Run `fn` atomically.  Rolls back if it throws, then rethrows. */
    batch(fn) {
        this.batchBegin();
        try {
            const result = fn();
            this.batchCommit();
            return result;
        } catch (e) {
            // Roll back on the way out, but never let a rollback failure mask
            // the error that actually caused it.
            try { this.batchRollback(); } catch (_) { /* ignore */ }
            throw e;
        }
    }

    // ----------------------------------------------------------
    // Utility
    // ----------------------------------------------------------

    /** Collection statistics: documents, dead_records, file_size_bytes, dead_ratio. */
    stats() {
        return JSON.parse(this._callStr(this.api.stats));
    }

    /** Rewrite the file, reclaiming space from dead records. */
    compact() { this._call(this.api.compact); }

    /** Flush and fsync. */
    sync() { this._call(this.api.sync); }

    /** Rebuild all in-memory indexes. */
    reindex() { this._call(this.api.reindex); }

    /**
     * Re-embed every document carrying `sourceField`, rewriting its vector
     * field at the model's current width.  Returns the number rewritten.
     *
     * The recovery path after changing the embedding model: a collection
     * whose stored vectors no longer match its index has that index
     * disabled, and searching it throws.  `sourceField` is the text field
     * configured under `auto_embed`, not the vector field it writes to.
     */
    reembed(sourceField) {
        return Number(this._call(this.api.reembed, sourceField));
    }

    /** Close the collection.  Safe to call more than once. */
    close() {
        if (this.handle) {
            const errOut = makeErrOut();
            this.api.close(this.handle, errOut);
            this.handle = null;
            checkError(this.api, errOut);
        }
    }
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

/** Convenience wrapper: `moofile.open(path, config)`. */
function open(filePath, config = {}) {
    return new Collection(filePath, config);
}

module.exports = { Collection, Cursor, SearchCursor, MooFileError, open };
