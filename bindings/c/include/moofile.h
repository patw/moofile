#ifndef MOOFILE_H
#define MOOFILE_H

/* ---------------------------------------------------------------------------
 * ABI contract
 * ---------------------------------------------------------------------------
 *
 * Error convention
 *   Every function takes a trailing `char** err_out`, which may be NULL if the
 *   caller does not want the message.  On entry it is set to NULL; on failure
 *   it is set to an allocated message the caller must release with
 *   moofile_free_string().  Failure is signalled by the return value:
 *     - pointer-returning functions return NULL
 *     - int-returning functions return -1
 *     - int64_t-returning functions return -1
 *   A NULL return with `*err_out == NULL` is not an error — it means "no
 *   result" (an exhausted cursor, or moofile_find_one() with no match).
 *
 * Ownership
 *   Every `char*` returned by this library is owned by the caller and must be
 *   released with moofile_free_string().  That includes documents from
 *   cursors and every error message.  Cursors are released with
 *   moofile_cursor_free() / moofile_search_cursor_free(), and collections with
 *   moofile_close().  Strings passed *in* are borrowed for the duration of the
 *   call and are never retained.
 *
 * Missing-document semantics
 *   The single-document mutators moofile_update_one() and
 *   moofile_replace_one() fail with -1 and the message "no document matches
 *   filter" when nothing matches, mirroring the Rust and Python APIs, which
 *   raise DocumentNotFound.  The bulk and delete operations do not:
 *   moofile_update_many(), moofile_delete_one() and moofile_delete_many()
 *   return 0 on a miss, which is likewise how Rust and Python behave.
 *
 * Thread safety
 *   A collection handle may be shared between threads; the Rust core guards it
 *   with a lock.  Cursors are *not* thread-safe — do not iterate one cursor
 *   from two threads at once.
 * --------------------------------------------------------------------------- */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

/* ---------------------------------------------------------------------------
 * Opaque handle types
 * --------------------------------------------------------------------------- */

/** Handle to an open MooFile collection.  Created by moofile_open(). */
typedef struct MooFileCollection MooFileCollection;

/** Cursor for iterating over query results.  Created by moofile_find(). */
typedef struct MooFileCursor MooFileCursor;

/** Cursor for iterating over search results (vector/text/hybrid). */
typedef struct MooFileSearchCursor MooFileSearchCursor;

/* ---------------------------------------------------------------------------
 * Collection lifecycle
 * --------------------------------------------------------------------------- */

/**
 * Open a MooFile collection.
 *
 * @param path        Path to the .bson file (created if absent).
 * @param config_json JSON configuration string (or NULL for defaults):
 *   {
 *     "indexes": ["field1", "field2"],
 *     "vector_indexes": {"embedding": 2048},
 *     "text_indexes": ["content"],
 *     "readonly": false,
 *     "durability": "os",   // "none", "os" (default), "fsync"
 *     "auto_embed": {        // on-device embedding (v0.5.0+)
 *       "content": {
 *         "target": "embedding",                   // target vector field
 *         "dims": 2048,                            // embedding dimensions (2048/1024/512/256)
 *         "max_length": 1024,                       // tokenizer truncation cap (default 1024, max 32768)
 *         "precision": "int8",                     // "f32", "int8", "uint8", "binary"
 *         "normalize": true,
 *         "query_prefix": "Represent the query for retrieving supporting documents: ",
 *         "doc_prefix": ""
 *         // "model" is optional: omit it to use the built-in voyage-4-nano,
 *         // or set it to a local model directory (model_quantized.onnx + tokenizer.json).
 *       }
 *     },
 *     "model_cache_dir": "/path/to/cache"  // default: ~/.cache/moofile/models/
 *   }
 * @param err_out     Optional pointer to receive an error message string
 *                    (must be freed with moofile_free_string).
 * @return A collection handle, or NULL on error.
 */
MooFileCollection* moofile_open(const char* path, const char* config_json, char** err_out);

/**
 * Close a collection and free all associated resources.
 *
 * @param handle   The collection handle (must not be NULL).
 * @param err_out  Optional error output.
 * @return 0 on success, -1 on error.
 */
int moofile_close(MooFileCollection* handle, char** err_out);

/* ---------------------------------------------------------------------------
 * Insert
 * --------------------------------------------------------------------------- */

/**
 * Insert a single document.
 *
 * @param handle   The collection handle.
 * @param doc_json JSON object string representing the document.
 * @param err_out  Optional error output.
 * @return A JSON string of the inserted document (with _id populated),
 *         or NULL on error.  Must be freed with moofile_free_string().
 */
char* moofile_insert(MooFileCollection* handle, const char* doc_json, char** err_out);

/**
 * Insert multiple documents.
 *
 * @param handle    The collection handle.
 * @param docs_json JSON array of objects string.
 * @param err_out   Optional error output.
 * @return A JSON array of inserted documents, or NULL on error.
 *         Must be freed with moofile_free_string().
 */
char* moofile_insert_many(MooFileCollection* handle, const char* docs_json, char** err_out);

/* ---------------------------------------------------------------------------
 * Query
 * --------------------------------------------------------------------------- */

/**
 * Find documents matching a filter.
 *
 * @param handle     The collection handle.
 * @param filter_json JSON object filter (use "{}" for all documents).
 * @param err_out    Optional error output.
 * @return A cursor, or NULL on error.  Must be freed with moofile_cursor_free().
 */
MooFileCursor* moofile_find(MooFileCollection* handle, const char* filter_json, char** err_out);

/**
 * Find documents with the full query builder — sort, skip, limit, group, agg.
 *
 * @param handle      The collection handle.
 * @param filter_json JSON object filter (use "{}" for all documents).
 * @param options_json JSON object (or NULL / "{}" for none):
 *   {
 *     "sort":  "age",                       // ascending, shorthand
 *     "sort":  {"field": "age", "desc": true},
 *     "skip":  10,
 *     "limit": 5,
 *     "group": "department",                 // group before sort/skip/limit
 *     "agg":   [{"func": "count"},
 *               {"func": "sum",  "field": "amount"},
 *               {"func": "mean", "field": "score"}]
 *   }
 *   Aggregation functions: "count" (no field), "sum", "mean" (alias "avg"),
 *   "min", "max", "collect", "first", "last".  Output fields are named
 *   "count", "sum_<field>", "mean_<field>", and so on.  An unrecognised
 *   option key or function name is an error rather than being ignored, so a
 *   typo cannot silently return the whole collection.
 *
 *   Stages apply in this order: filter → group/agg → sort → skip → limit.
 * @param err_out     Optional error output.
 * @return A cursor, or NULL on error.  Must be freed with moofile_cursor_free().
 */
MooFileCursor* moofile_find_ex(MooFileCollection* handle, const char* filter_json,
                                const char* options_json, char** err_out);

/**
 * Find the first document matching a filter.
 *
 * @param handle     The collection handle.
 * @param filter_json JSON object filter (use "{}" for all documents).
 * @param err_out    Optional error output.
 * @return A JSON string of the document, or NULL if not found or on error.
 *         Must be freed with moofile_free_string().
 */
char* moofile_find_one(MooFileCollection* handle, const char* filter_json, char** err_out);

/**
 * Count documents matching a filter.
 *
 * @param handle     The collection handle.
 * @param filter_json JSON object filter (use "{}" for all documents).
 * @param err_out    Optional error output.
 * @return The number of matching documents, or -1 on error.
 */
int64_t moofile_count(MooFileCollection* handle, const char* filter_json, char** err_out);

/**
 * Check if at least one document matches a filter.
 *
 * @param handle     The collection handle.
 * @param filter_json JSON object filter.
 * @param err_out    Optional error output.
 * @return 1 if exists, 0 if not, -1 on error.
 */
int moofile_exists(MooFileCollection* handle, const char* filter_json, char** err_out);

/* ---------------------------------------------------------------------------
 * Cursor iteration
 * --------------------------------------------------------------------------- */

/**
 * Get the next document from a query cursor.
 *
 * @param cursor   The cursor.
 * @param err_out  Optional error output.
 * @return A JSON string of the next document, or NULL when exhausted or on error.
 *         Must be freed with moofile_free_string().
 */
char* moofile_cursor_next(MooFileCursor* cursor, char** err_out);

/**
 * Free a query cursor.
 *
 * @param cursor  The cursor to free (may be NULL).
 */
void moofile_cursor_free(MooFileCursor* cursor);

/* ---------------------------------------------------------------------------
 * Update
 * --------------------------------------------------------------------------- */

/**
 * Update the first document matching a filter.
 *
 * @param handle     The collection handle.
 * @param where_json JSON object filter to match documents.
 * @param update_json JSON object with optional "set", "unset", "inc" fields:
 *   {
 *     "set": {"field": value},
 *     "unset": ["field1"],
 *     "inc": {"counter": 1}
 *   }
 * @param err_out    Optional error output.
 * @return 1 if a document was updated, or -1 on error.  Matching nothing *is*
 *         an error here ("no document matches filter"), mirroring the Rust and
 *         Python APIs — use moofile_exists() first if a miss is expected.
 */
int moofile_update_one(MooFileCollection* handle, const char* where_json,
                        const char* update_json, char** err_out);

/**
 * Update all documents matching a filter.
 *
 * @return The number of documents updated, or -1 on error.  Unlike
 *         moofile_update_one(), matching nothing is *not* an error here — it
 *         returns 0, matching the Rust and Python bulk-update APIs.
 */
int64_t moofile_update_many(MooFileCollection* handle, const char* where_json,
                             const char* update_json, char** err_out);

/**
 * Replace the first document matching a filter.  The original _id is kept.
 *
 * @param replacement_json JSON object of the replacement document.
 * @return 1 if replaced, or -1 on error.  As with moofile_update_one(),
 *         matching nothing is an error.
 */
int moofile_replace_one(MooFileCollection* handle, const char* where_json,
                         const char* replacement_json, char** err_out);

/* ---------------------------------------------------------------------------
 * Delete
 * --------------------------------------------------------------------------- */

/**
 * Delete the first document matching a filter.
 *
 * @return 1 if a document was deleted, 0 if nothing matched, -1 on error.
 */
int moofile_delete_one(MooFileCollection* handle, const char* where_json, char** err_out);

/**
 * Delete all documents matching a filter.
 *
 * @return The number of documents deleted, or -1 on error.
 */
int64_t moofile_delete_many(MooFileCollection* handle, const char* where_json, char** err_out);

/* ---------------------------------------------------------------------------
 * Vector search
 * --------------------------------------------------------------------------- */

/**
 * Perform vector similarity search (cosine similarity).
 *
 * @param field             The vector field name.
 * @param query_vector_json JSON array of floats (e.g. "[0.1, 0.2, 0.3]").
 * @param limit             Max results (use 0 for default of 10).
 * @return A search cursor with (doc_json, score) pairs,
 *         or NULL on error.  Free with moofile_search_cursor_free().
 */
MooFileSearchCursor* moofile_vector_search(MooFileCollection* handle, const char* filter_json,
                                            const char* field, const char* query_vector_json,
                                            int limit, char** err_out);

/* ---------------------------------------------------------------------------
 * Text search
 * --------------------------------------------------------------------------- */

/**
 * Perform BM25 full-text search.
 *
 * @param field  The text field name.
 * @param query  The search query text.
 * @param limit  Max results (use 0 for default of 10).
 * @return A search cursor with (doc_json, score) pairs.
 */
MooFileSearchCursor* moofile_text_search(MooFileCollection* handle, const char* filter_json,
                                          const char* field, const char* query,
                                          int limit, char** err_out);

/* ---------------------------------------------------------------------------
 * Hybrid search (RRF)
 * --------------------------------------------------------------------------- */

/**
 * Perform hybrid search combining BM25 text search and vector similarity
 * using Reciprocal Rank Fusion (RRF).
 *
 * @param text_field        The text field name for BM25.
 * @param vector_field      The vector field name for cosine similarity.
 * @param query_text        The text query for BM25.
 * @param query_vector_json JSON array of floats (or NULL to auto-embed from query_text).
 * @param limit             Max results (use 0 for default of 10).
 * @return A search cursor with (doc_json, rrf_score) pairs.
 */
MooFileSearchCursor* moofile_hybrid_search(MooFileCollection* handle, const char* filter_json,
                                            const char* text_field, const char* vector_field,
                                            const char* query_text, const char* query_vector_json,
                                            int limit, char** err_out);

/* ---------------------------------------------------------------------------
 * Semantic search (autoembedding)
 * --------------------------------------------------------------------------- */

/**
 * Perform semantic search — auto-embeds the query text using the configured
 * embedding model and returns vector similarity results.
 *
 * The `source_field` must have been configured with `auto_embed` at collection
 * open time.  The query text is automatically prefixed with the configured
 * `query_prefix` before embedding.
 *
 * @param source_field The text field name configured in auto_embed.
 * @param query_text   The search query text (auto-embedded).
 * @param limit        Max results (use 0 for default of 10).
 * @return A search cursor with (doc_json, score) pairs.
 */
MooFileSearchCursor* moofile_semantic_search(MooFileCollection* handle, const char* filter_json,
                                              const char* source_field, const char* query_text,
                                              int limit, char** err_out);

/* ---------------------------------------------------------------------------
 * Search cursor iteration
 * --------------------------------------------------------------------------- */

/**
 * Get the next result from a search cursor.
 *
 * @return A JSON string "[doc_json, score]" or NULL when exhausted.
 *         Must be freed with moofile_free_string().
 */
char* moofile_search_cursor_next(MooFileSearchCursor* cursor, char** err_out);

/**
 * Free a search cursor.
 *
 * @param cursor  The cursor to free (may be NULL).
 */
void moofile_search_cursor_free(MooFileSearchCursor* cursor);

/* ---------------------------------------------------------------------------
 * Batch writes
 * --------------------------------------------------------------------------- */

/**
 * Begin an atomic batch write context.
 * All subsequent writes are buffered until moofile_batch_commit().
 */
int moofile_batch_begin(MooFileCollection* handle, char** err_out);

/**
 * Commit the current batch — all buffered writes are applied atomically.
 */
int moofile_batch_commit(MooFileCollection* handle, char** err_out);

/**
 * Rollback the current batch — all buffered writes are discarded.
 */
int moofile_batch_rollback(MooFileCollection* handle, char** err_out);

/* ---------------------------------------------------------------------------
 * Utility
 * --------------------------------------------------------------------------- */

/**
 * Get collection statistics as a JSON string:
 *   {"documents": 1, "dead_records": 2, "file_size_bytes": 146, "dead_ratio": 0.66}
 *
 * `documents` is the live document count.  `dead_records` counts every record
 * on disk that is no longer live — note that deleting one document adds *two*
 * (the superseded original plus the tombstone), and updating one adds one.
 * `dead_ratio` is dead_records / total records, the figure to threshold on
 * when deciding whether to call moofile_compact().
 *
 * Must be freed with moofile_free_string().
 */
char* moofile_stats(MooFileCollection* handle, char** err_out);

/**
 * Compact the data file, reclaiming space from dead records.
 */
int moofile_compact(MooFileCollection* handle, char** err_out);

/**
 * Flush and fsync the data file.
 */
int moofile_sync(MooFileCollection* handle, char** err_out);

/**
 * Rebuild all in-memory indexes from scratch.
 */
int moofile_reindex(MooFileCollection* handle, char** err_out);

/**
 * Re-embed every document carrying `source_field`, rewriting its configured
 * vector field at the embedding model's current width.
 *
 * This is the recovery path after changing the embedding model: vectors of
 * different widths cannot be compared, so a collection opened with a model
 * whose output width no longer matches its vector index has that index
 * disabled, and searching it fails with an error naming both widths.
 * Re-embedding rewrites the stored vectors, retargets the index and clears
 * the flag.  It is never implicit — it rewrites the whole collection.
 *
 * `source_field` is the *text* field configured under `auto_embed`, not the
 * vector field it writes to.
 *
 * Returns the number of documents rewritten, or -1 on error.
 */
int64_t moofile_reembed(MooFileCollection* handle, const char* source_field,
                        char** err_out);

/* ---------------------------------------------------------------------------
 * Memory management
 * --------------------------------------------------------------------------- */

/**
 * Free a string returned by any MooFile function.
 * Safe to call with NULL.
 */
void moofile_free_string(char* s);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* MOOFILE_H */
