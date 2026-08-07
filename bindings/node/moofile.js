/**
 * MooFile — Node.js binding via ffi-napi.
 *
 * Calls the C shared library (libmoofile.so / .dylib / .dll) directly.
 * All documents are passed as JSON strings across the FFI boundary.
 *
 * Usage:
 *   const moo = require('./moofile');
 *   const db = moo.open('data.bson', { indexes: ['email'] });
 *   db.insert({ name: 'Alice', email: 'a@test.com' });
 *   console.log(db.find({ email: 'a@test.com' }));
 *   db.close();
 */

const ffi = require('ffi-napi');
const ref = require('ref-napi');
const Struct = require('ref-struct-napi');

// ---------------------------------------------------------------------------
// Load the shared library
// ---------------------------------------------------------------------------

function loadLibrary(libPath) {
    const lib = ffi.Library(libPath, {
        // Lifecycle
        'moofile_open': ['pointer', ['string', 'string', 'pointer']],
        'moofile_close': ['int', ['pointer', 'pointer']],

        // Insert
        'moofile_insert': ['string', ['pointer', 'string', 'pointer']],
        'moofile_insert_many': ['string', ['pointer', 'string', 'pointer']],

        // Query
        'moofile_find': ['pointer', ['pointer', 'string', 'pointer']],
        'moofile_find_one': ['string', ['pointer', 'string', 'pointer']],
        'moofile_count': ['int64', ['pointer', 'string', 'pointer']],
        'moofile_exists': ['int', ['pointer', 'string', 'pointer']],

        // Cursor
        'moofile_cursor_next': ['string', ['pointer', 'pointer']],
        'moofile_cursor_free': ['void', ['pointer']],

        // Update
        'moofile_update_one': ['int', ['pointer', 'string', 'string', 'pointer']],
        'moofile_update_many': ['int64', ['pointer', 'string', 'string', 'pointer']],
        'moofile_replace_one': ['int', ['pointer', 'string', 'string', 'pointer']],

        // Delete
        'moofile_delete_one': ['int', ['pointer', 'string', 'pointer']],
        'moofile_delete_many': ['int64', ['pointer', 'string', 'pointer']],

        // Vector search
        'moofile_vector_search': ['pointer', ['pointer', 'string', 'string', 'string', 'int', 'pointer']],

        // Text search
        'moofile_text_search': ['pointer', ['pointer', 'string', 'string', 'string', 'int', 'pointer']],

        // Hybrid search
        'moofile_hybrid_search': ['pointer', ['pointer', 'string', 'string', 'string', 'string', 'string', 'int', 'pointer']],

        // Semantic search
        'moofile_semantic_search': ['pointer', ['pointer', 'string', 'string', 'string', 'int', 'pointer']],

        // Search cursor
        'moofile_search_cursor_next': ['string', ['pointer', 'pointer']],
        'moofile_search_cursor_free': ['void', ['pointer']],

        // Batch
        'moofile_batch_begin': ['int', ['pointer', 'pointer']],
        'moofile_batch_commit': ['int', ['pointer', 'pointer']],
        'moofile_batch_rollback': ['int', ['pointer', 'pointer']],

        // Utility
        'moofile_stats': ['string', ['pointer', 'pointer']],
        'moofile_compact': ['int', ['pointer', 'pointer']],
        'moofile_sync': ['int', ['pointer', 'pointer']],
        'moofile_reindex': ['int', ['pointer', 'pointer']],

        // Memory
        'moofile_free_string': ['void', ['string']],
    });
    return lib;
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

function checkError(errPtr, lib) {
    if (errPtr && !errPtr.isNull() && errPtr.deref()) {
        const msg = errPtr.deref().readCString();
        lib.moofile_free_string(errPtr.deref());
        errPtr.writePointer(ref.NULL);
        throw new MooFileError(msg);
    }
}

function makeErrPtr() {
    const buf = Buffer.alloc(ref.sizeof.pointer);
    ref.writePointer(buf, 0, ref.NULL);
    return buf;
}

// ---------------------------------------------------------------------------
// SearchCursor
// ---------------------------------------------------------------------------

class SearchCursor {
    constructor(ptr, lib) {
        this.ptr = ptr;
        this.lib = lib;
    }

    next() {
        const errPtr = makeErrPtr();
        const s = this.lib.moofile_search_cursor_next(this.ptr, errPtr);
        checkError(errPtr, this.lib);
        if (s === null) return null;
        const pair = JSON.parse(s);
        return { doc: pair[0], score: pair[1] };
    }

    toArray() {
        const results = [];
        let item;
        while ((item = this.next()) !== null) {
            results.push(item);
        }
        return results;
    }

    close() {
        if (this.ptr) {
            this.lib.moofile_search_cursor_free(this.ptr);
            this.ptr = null;
        }
    }
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

class Cursor {
    constructor(ptr, lib) {
        this.ptr = ptr;
        this.lib = lib;
    }

    next() {
        const errPtr = makeErrPtr();
        const s = this.lib.moofile_cursor_next(this.ptr, errPtr);
        checkError(errPtr, this.lib);
        if (s === null) return null;
        return JSON.parse(s);
    }

    toArray() {
        const docs = [];
        let doc;
        while ((doc = this.next()) !== null) {
            docs.push(doc);
        }
        return docs;
    }

    close() {
        if (this.ptr) {
            this.lib.moofile_cursor_free(this.ptr);
            this.ptr = null;
        }
    }
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

class Collection {
    constructor(path, config = {}) {
        const libPath = config.libPath || Collection._defaultLibPath();
        this.lib = loadLibrary(libPath);

        const configJson = JSON.stringify({
            indexes: config.indexes,
            vector_indexes: config.vector_indexes,
            text_indexes: config.text_indexes,
            readonly: config.readonly,
            durability: config.durability || 'os',
            auto_embed: config.auto_embed,
            model_cache_dir: config.model_cache_dir,
        });

        const errPtr = makeErrPtr();
        const handle = this.lib.moofile_open(path, configJson, errPtr);
        checkError(errPtr, this.lib);
        if (ref.isNull(handle)) {
            throw new MooFileError('moofile_open returned null');
        }
        this.handle = handle;
        this.path = path;
    }

    /** @private Auto-detect the shared library path. */
    static _defaultLibPath() {
        const candidates = [
            '../target/release/libmoofile.so',
            '../target/debug/libmoofile.so',
            '../../target/release/libmoofile.so',
            '../../target/debug/libmoofile.so',
            './libmoofile.so',
        ];
        const fs = require('fs');
        for (const c of candidates) {
            try {
                if (fs.statSync(c).isFile()) return require('path').resolve(c);
            } catch (_) {}
        }
        return '../target/release/libmoofile.so'; // best guess
    }

    /** Call a method, check for error, return result. */
    _call(method, ...args) {
        const errPtr = makeErrPtr();
        const result = method(this.handle, ...args, errPtr);
        checkError(errPtr, this.lib);
        return result;
    }

    /** Call a method that returns a handle pointer. */
    _callPtr(method, ...args) {
        const errPtr = makeErrPtr();
        const ptr = method(this.handle, ...args, errPtr);
        checkError(errPtr, this.lib);
        if (ref.isNull(ptr)) throw new MooFileError('null pointer returned');
        return ptr;
    }

    // ----------------------------------------------------------
    // Insert
    // ----------------------------------------------------------

    insert(doc) {
        const result = this._call(this.lib.moofile_insert, JSON.stringify(doc));
        return result ? JSON.parse(result) : doc;
    }

    insertMany(docs) {
        const result = this._call(this.lib.moofile_insert_many, JSON.stringify(docs));
        return result ? JSON.parse(result) : docs;
    }

    // ----------------------------------------------------------
    // Query
    // ----------------------------------------------------------

    find(filter = {}) {
        const ptr = this._callPtr(this.lib.moofile_find, JSON.stringify(filter));
        return new Cursor(ptr, this.lib);
    }

    findOne(filter = {}) {
        const result = this._call(this.lib.moofile_find_one, JSON.stringify(filter));
        return result ? JSON.parse(result) : null;
    }

    count(filter = {}) {
        return Number(this._call(this.lib.moofile_count, JSON.stringify(filter)));
    }

    exists(filter) {
        return this._call(this.lib.moofile_exists, JSON.stringify(filter)) === 1;
    }

    // ----------------------------------------------------------
    // Update
    // ----------------------------------------------------------

    updateOne(where, setValues = {}, unsetFields = [], incValues = {}) {
        const update = {};
        if (Object.keys(setValues).length > 0) update.set = setValues;
        if (unsetFields.length > 0) update.unset = unsetFields;
        if (Object.keys(incValues).length > 0) update.inc = incValues;
        return this._call(this.lib.moofile_update_one, JSON.stringify(where), JSON.stringify(update)) === 1;
    }

    updateMany(where, setValues = {}, unsetFields = [], incValues = {}) {
        const update = {};
        if (Object.keys(setValues).length > 0) update.set = setValues;
        if (unsetFields.length > 0) update.unset = unsetFields;
        if (Object.keys(incValues).length > 0) update.inc = incValues;
        return Number(this._call(this.lib.moofile_update_many, JSON.stringify(where), JSON.stringify(update)));
    }

    replaceOne(where, replacement) {
        return this._call(this.lib.moofile_replace_one, JSON.stringify(where), JSON.stringify(replacement)) === 1;
    }

    // ----------------------------------------------------------
    // Delete
    // ----------------------------------------------------------

    deleteOne(where) {
        return this._call(this.lib.moofile_delete_one, JSON.stringify(where)) === 1;
    }

    deleteMany(where) {
        return Number(this._call(this.lib.moofile_delete_many, JSON.stringify(where)));
    }

    // ----------------------------------------------------------
    // Search
    // ----------------------------------------------------------

    vectorSearch(field, queryVector, limit = 10, filter = {}) {
        const ptr = this._callPtr(
            this.lib.moofile_vector_search,
            JSON.stringify(filter), field, JSON.stringify(queryVector), limit
        );
        return new SearchCursor(ptr, this.lib);
    }

    textSearch(field, query, limit = 10, filter = {}) {
        const ptr = this._callPtr(
            this.lib.moofile_text_search,
            JSON.stringify(filter), field, query, limit
        );
        return new SearchCursor(ptr, this.lib);
    }

    hybridSearch(textField, vectorField, queryText, queryVector = null, limit = 10, filter = {}) {
        const qv = queryVector ? JSON.stringify(queryVector) : null;
        const ptr = this._callPtr(
            this.lib.moofile_hybrid_search,
            JSON.stringify(filter), textField, vectorField, queryText, qv, limit
        );
        return new SearchCursor(ptr, this.lib);
    }

    semantic(sourceField, queryText, limit = 10, filter = {}) {
        const ptr = this._callPtr(
            this.lib.moofile_semantic_search,
            JSON.stringify(filter), sourceField, queryText, limit
        );
        return new SearchCursor(ptr, this.lib);
    }

    // ----------------------------------------------------------
    // Batch
    // ----------------------------------------------------------

    batchBegin() {
        this._call(this.lib.moofile_batch_begin);
    }

    batchCommit() {
        this._call(this.lib.moofile_batch_commit);
    }

    batchRollback() {
        this._call(this.lib.moofile_batch_rollback);
    }

    /** Run operations atomically.  Rolls back on exception. */
    batch(fn) {
        this.batchBegin();
        try {
            fn();
            this.batchCommit();
        } catch (e) {
            this.batchRollback();
            throw e;
        }
    }

    // ----------------------------------------------------------
    // Utility
    // ----------------------------------------------------------

    stats() {
        const result = this._call(this.lib.moofile_stats);
        return result ? JSON.parse(result) : {};
    }

    compact() {
        this._call(this.lib.moofile_compact);
    }

    sync() {
        this._call(this.lib.moofile_sync);
    }

    reindex() {
        this._call(this.lib.moofile_reindex);
    }

    close() {
        if (this.handle) {
            this._call(this.lib.moofile_close);
            this.handle = null;
        }
    }
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = { Collection, Cursor, SearchCursor, MooFileError };
