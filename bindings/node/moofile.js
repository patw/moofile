/**
 * MooFile — Node.js binding via koffi (modern C FFI).
 *
 * Usage:
 *   const moo = require('./moofile');
 *   const db = moo.open('data.bson', { indexes: ['email'] });
 *   db.insert({ name: 'Alice', email: 'a@test.com' });
 *   console.log(db.find({ email: 'a@test.com' }));
 *   db.close();
 */

const koffi = require('koffi');
const path = require('path');
const fs = require('fs');

// ---------------------------------------------------------------------------
// Library singleton — registers opaque types once globally
// ---------------------------------------------------------------------------

let _lib = null;
let _libPath = null;
let _typesRegistered = false;

function registerTypes() {
    if (_typesRegistered) return;
    koffi.pointer('MooFileCollection', koffi.opaque());
    koffi.pointer('MooFileCursor', koffi.opaque());
    koffi.pointer('MooFileSearchCursor', koffi.opaque());
    _typesRegistered = true;
}

function getLibrary(libPath) {
    if (_lib && _libPath === libPath) return _lib;

    registerTypes();
    const lib = koffi.load(libPath);

    // All functions - store as method references on lib
    lib._open       = lib.func('MooFileCollection* moofile_open(const char* path, const char* config_json, void* err_out)');
    lib._close      = lib.func('int moofile_close(MooFileCollection* handle, void* err_out)');
    lib._insert     = lib.func('char* moofile_insert(MooFileCollection* handle, const char* doc_json, void* err_out)');
    lib._insertMany = lib.func('char* moofile_insert_many(MooFileCollection* handle, const char* docs_json, void* err_out)');
    lib._find       = lib.func('MooFileCursor* moofile_find(MooFileCollection* handle, const char* filter_json, void* err_out)');
    lib._findOne    = lib.func('char* moofile_find_one(MooFileCollection* handle, const char* filter_json, void* err_out)');
    lib._count      = lib.func('int64_t moofile_count(MooFileCollection* handle, const char* filter_json, void* err_out)');
    lib._exists     = lib.func('int moofile_exists(MooFileCollection* handle, const char* filter_json, void* err_out)');
    lib._cursorNext = lib.func('char* moofile_cursor_next(MooFileCursor* cursor, void* err_out)');
    lib._cursorFree = lib.func('void moofile_cursor_free(MooFileCursor* cursor)');
    lib._updateOne  = lib.func('int moofile_update_one(MooFileCollection* handle, const char* where_json, const char* update_json, void* err_out)');
    lib._updateMany = lib.func('int64_t moofile_update_many(MooFileCollection* handle, const char* where_json, const char* update_json, void* err_out)');
    lib._replaceOne = lib.func('int moofile_replace_one(MooFileCollection* handle, const char* where_json, const char* replacement_json, void* err_out)');
    lib._deleteOne  = lib.func('int moofile_delete_one(MooFileCollection* handle, const char* where_json, void* err_out)');
    lib._deleteMany = lib.func('int64_t moofile_delete_many(MooFileCollection* handle, const char* where_json, void* err_out)');
    lib._vecSearch  = lib.func('MooFileSearchCursor* moofile_vector_search(MooFileCollection* handle, const char* filter_json, const char* field, const char* query_vector_json, int limit, void* err_out)');
    lib._txtSearch  = lib.func('MooFileSearchCursor* moofile_text_search(MooFileCollection* handle, const char* filter_json, const char* field, const char* query, int limit, void* err_out)');
    lib._hybSearch  = lib.func('MooFileSearchCursor* moofile_hybrid_search(MooFileCollection* handle, const char* filter_json, const char* text_field, const char* vector_field, const char* query_text, const char* query_vector_json, int limit, void* err_out)');
    lib._semSearch  = lib.func('MooFileSearchCursor* moofile_semantic_search(MooFileCollection* handle, const char* filter_json, const char* source_field, const char* query_text, int limit, void* err_out)');
    lib._searchNext = lib.func('char* moofile_search_cursor_next(MooFileSearchCursor* cursor, void* err_out)');
    lib._searchFree = lib.func('void moofile_search_cursor_free(MooFileSearchCursor* cursor)');
    lib._batchBegin = lib.func('int moofile_batch_begin(MooFileCollection* handle, void* err_out)');
    lib._batchCommit = lib.func('int moofile_batch_commit(MooFileCollection* handle, void* err_out)');
    lib._batchRollback = lib.func('int moofile_batch_rollback(MooFileCollection* handle, void* err_out)');
    lib._stats      = lib.func('char* moofile_stats(MooFileCollection* handle, void* err_out)');
    lib._compact    = lib.func('int moofile_compact(MooFileCollection* handle, void* err_out)');
    lib._sync       = lib.func('int moofile_sync(MooFileCollection* handle, void* err_out)');
    lib._reindex    = lib.func('int moofile_reindex(MooFileCollection* handle, void* err_out)');
    lib._freeString = lib.func('void moofile_free_string(char* s)');

    _lib = lib;
    _libPath = libPath;
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

// ---------------------------------------------------------------------------
// SearchCursor
// ---------------------------------------------------------------------------

class SearchCursor {
    constructor(ptr, lib) {
        this.ptr = ptr;
        this.lib = lib;
    }

    next() {
        if (!this.ptr) return null;
        const s = this.lib._searchNext(this.ptr, null);
        if (s === null) return null;
        const pair = JSON.parse(s);
        return { doc: pair[0], score: pair[1] };
    }

    toArray() {
        const results = [];
        let item;
        while ((item = this.next()) !== null) results.push(item);
        return results;
    }

    close() {
        if (this.ptr) { this.lib._searchFree(this.ptr); this.ptr = null; }
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
        if (!this.ptr) return null;
        const s = this.lib._cursorNext(this.ptr, null);
        if (s === null) return null;
        return JSON.parse(s);
    }

    toArray() {
        const docs = [];
        let doc;
        while ((doc = this.next()) !== null) docs.push(doc);
        return docs;
    }

    close() {
        if (this.ptr) { this.lib._cursorFree(this.ptr); this.ptr = null; }
    }
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

class Collection {
    constructor(path, config = {}) {
        const libPath = config.libPath || Collection._defaultLibPath();
        this.lib = getLibrary(libPath);

        const configJson = JSON.stringify({
            indexes: config.indexes,
            vector_indexes: config.vector_indexes,
            text_indexes: config.text_indexes,
            readonly: config.readonly,
            durability: config.durability || 'os',
            auto_embed: config.auto_embed,
            model_cache_dir: config.model_cache_dir,
        });

        const handle = this.lib._open(path, configJson, null);
        if (handle === null) throw new MooFileError('moofile_open returned null');
        this.handle = handle;
    }

    static _defaultLibPath() {
        const candidates = [
            '../target/release/libmoofile.so',
            '../target/debug/libmoofile.so',
            '../../target/release/libmoofile.so',
            '../../target/debug/libmoofile.so',
            './libmoofile.so',
        ];
        for (const c of candidates) {
            try {
                const p = path.resolve(__dirname, c);
                if (fs.statSync(p).isFile()) return p;
            } catch (_) {}
        }
        return path.resolve(__dirname, '../target/release/libmoofile.so');
    }

    _call(fn, ...args) { return fn(this.handle, ...args, null); }

    // ----------------------------------------------------------
    // Insert
    // ----------------------------------------------------------

    insert(doc) {
        const result = this._call(this.lib._insert, JSON.stringify(doc));
        if (result === null) throw new MooFileError('moofile_insert failed (check err_out)');
        return JSON.parse(result);
    }

    insertMany(docs) {
        const result = this._call(this.lib._insertMany, JSON.stringify(docs));
        if (result === null) throw new MooFileError('moofile_insert_many failed');
        return JSON.parse(result);
    }

    // ----------------------------------------------------------
    // Query
    // ----------------------------------------------------------

    find(filter = {}) {
        const ptr = this._call(this.lib._find, JSON.stringify(filter));
        if (!ptr) return [];
        return new Cursor(ptr, this.lib);
    }

    findOne(filter = {}) {
        const result = this._call(this.lib._findOne, JSON.stringify(filter));
        return result ? JSON.parse(result) : null;
    }

    count(filter = {}) {
        return Number(this._call(this.lib._count, JSON.stringify(filter)));
    }

    exists(filter) {
        return this._call(this.lib._exists, JSON.stringify(filter)) === 1;
    }

    // ----------------------------------------------------------
    // Update
    // ----------------------------------------------------------

    updateOne(where, setValues = {}, unsetFields = [], incValues = {}) {
        const update = {};
        if (Object.keys(setValues).length > 0) update.set = setValues;
        if (unsetFields.length > 0) update.unset = unsetFields;
        if (Object.keys(incValues).length > 0) update.inc = incValues;
        return this._call(this.lib._updateOne, JSON.stringify(where), JSON.stringify(update)) === 1;
    }

    updateMany(where, setValues = {}, unsetFields = [], incValues = {}) {
        const update = {};
        if (Object.keys(setValues).length > 0) update.set = setValues;
        if (unsetFields.length > 0) update.unset = unsetFields;
        if (Object.keys(incValues).length > 0) update.inc = incValues;
        return Number(this._call(this.lib._updateMany, JSON.stringify(where), JSON.stringify(update)));
    }

    replaceOne(where, replacement) {
        return this._call(this.lib._replaceOne, JSON.stringify(where), JSON.stringify(replacement)) === 1;
    }

    // ----------------------------------------------------------
    // Delete
    // ----------------------------------------------------------

    deleteOne(where) { return this._call(this.lib._deleteOne, JSON.stringify(where)) === 1; }
    deleteMany(where) { return Number(this._call(this.lib._deleteMany, JSON.stringify(where))); }

    // ----------------------------------------------------------
    // Search
    // ----------------------------------------------------------

    vectorSearch(field, queryVector, limit = 10, filter = {}) {
        const ptr = this._call(this.lib._vecSearch, JSON.stringify(filter), field, JSON.stringify(queryVector), limit);
        return new SearchCursor(ptr, this.lib);
    }

    textSearch(field, query, limit = 10, filter = {}) {
        const ptr = this._call(this.lib._txtSearch, JSON.stringify(filter), field, query, limit);
        return new SearchCursor(ptr, this.lib);
    }

    hybridSearch(textField, vectorField, queryText, queryVector = null, limit = 10, filter = {}) {
        const qv = queryVector ? JSON.stringify(queryVector) : null;
        const ptr = this._call(this.lib._hybSearch, JSON.stringify(filter), textField, vectorField, queryText, qv, limit);
        return new SearchCursor(ptr, this.lib);
    }

    semantic(sourceField, queryText, limit = 10, filter = {}) {
        const ptr = this._call(this.lib._semSearch, JSON.stringify(filter), sourceField, queryText, limit);
        return new SearchCursor(ptr, this.lib);
    }

    // ----------------------------------------------------------
    // Batch
    // ----------------------------------------------------------

    batchBegin() { this._call(this.lib._batchBegin); }
    batchCommit() { this._call(this.lib._batchCommit); }
    batchRollback() { this._call(this.lib._batchRollback); }

    batch(fn) {
        this.batchBegin();
        try { fn(); this.batchCommit(); }
        catch (e) { this.batchRollback(); throw e; }
    }

    // ----------------------------------------------------------
    // Utility
    // ----------------------------------------------------------

    stats() {
        const result = this._call(this.lib._stats);
        return result ? JSON.parse(result) : {};
    }

    compact()  { this._call(this.lib._compact); }
    sync()     { this._call(this.lib._sync); }
    reindex()  { this._call(this.lib._reindex); }

    close() {
        if (this.handle) { this._call(this.lib._close); this.handle = null; }
    }
}

module.exports = { Collection, Cursor, SearchCursor, MooFileError };
