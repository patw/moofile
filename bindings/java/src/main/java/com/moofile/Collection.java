package com.moofile;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.moofile.Native.C_INT;
import static com.moofile.Native.C_LONG;

/**
 * A handle to an open MooFile collection.
 *
 * <p>Requires JDK 22 or newer (it uses the Foreign Function &amp; Memory API)
 * and the {@code libmoofile} shared library:
 *
 * <pre>{@code
 * cargo build -p moofile-c --release
 * }</pre>
 *
 * <pre>{@code
 * try (Collection db = Collection.open("data.bson",
 *         Config.create().index("email"))) {
 *
 *     db.insert(Document.of("name", "Alice", "email", "a@example.com", "age", 30));
 *
 *     for (Document d : db.find(Document.of("age", Document.of("$gt", 25)))) {
 *         System.out.println(d);
 *     }
 *
 *     List<Document> oldest = db.find(null,
 *         FindOptions.create().sort("age", true).limit(10));
 * }
 * }</pre>
 *
 * <p>Thread-safe: every method synchronises on the handle, on top of the
 * cross-process locking the Rust core already does.
 */
public class Collection implements AutoCloseable {

    private MemorySegment handle;
    private final String path;

    private Collection(MemorySegment handle, String path) {
        this.handle = handle;
        this.path = path;
    }

    // ------------------------------------------------------------------
    // Open / close
    // ------------------------------------------------------------------

    /** Open a collection with the default configuration. */
    public static Collection open(String path) {
        return open(path, null);
    }

    /** Open a collection, creating the file if it does not exist. */
    public static Collection open(String path, Config config) {
        if (path == null) throw new MooFileException("path must not be null");
        String configJson = config == null ? "{}" : config.toJson();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment h = (MemorySegment) Native.OPEN.invokeExact(
                Native.cString(arena, path),
                Native.cString(arena, configJson),
                err);
            Native.checkError(err);
            if (h.equals(MemorySegment.NULL)) {
                throw new MooFileException("failed to open collection: " + path);
            }
            return new Collection(h, path);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /** The file this collection was opened from. */
    public String path() { return path; }

    /** Close the collection. Idempotent, so try-with-resources is safe. */
    @Override
    public synchronized void close() {
        if (handle == null) return;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            int unused = (int) Native.CLOSE.invokeExact(handle, err);
            handle = null;
            Native.checkError(err);
        } catch (Throwable t) {
            handle = null;
            throw Native.rethrow(t);
        }
    }

    /** @throws MooFileException if the collection has been closed */
    private MemorySegment handle() {
        if (handle == null) throw new MooFileException("collection is closed");
        return handle;
    }

    // ------------------------------------------------------------------
    // Insert
    // ------------------------------------------------------------------

    /** Insert one document; returns it with {@code _id} populated. */
    public synchronized Document insert(Document doc) {
        if (doc == null) throw new MooFileException("document must not be null");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment result = (MemorySegment) Native.INSERT.invokeExact(
                handle(), Native.cString(arena, doc.toJson()), err);
            Native.checkError(err);
            String json = Native.takeString(result);
            if (json == null) throw new MooFileException("insert returned no document");
            return Document.parse(json);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /** Insert several documents; returns them with {@code _id}s populated. */
    public synchronized List<Document> insertMany(List<Document> docs) {
        if (docs == null) throw new MooFileException("documents must not be null");
        List<Object> raw = new ArrayList<>(docs.size());
        for (Document d : docs) raw.add(d.asMap());

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment result = (MemorySegment) Native.INSERT_MANY.invokeExact(
                handle(), Native.cString(arena, Json.write(raw)), err);
            Native.checkError(err);
            String json = Native.takeString(result);
            if (json == null) throw new MooFileException("insertMany returned no documents");
            return toDocuments(Json.parseArray(json));
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Document> toDocuments(List<Object> values) {
        List<Document> out = new ArrayList<>(values.size());
        for (Object v : values) {
            if (v instanceof Map) out.add(new Document((Map<String, Object>) v));
            else throw new MooFileException("expected an object in result array, got " + v);
        }
        return out;
    }

    // ------------------------------------------------------------------
    // Query
    // ------------------------------------------------------------------

    /** Find every document matching {@code filter}; null matches everything. */
    public List<Document> find(Document filter) {
        return find(filter, null);
    }

    /**
     * Find with the query builder — sort, skip, limit, group, agg.
     *
     * @param filter  match filter, or null for everything
     * @param options query-builder stages, or null for none
     */
    public synchronized List<Document> find(Document filter, FindOptions options) {
        String filterJson = filter == null ? "{}" : filter.toJson();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment cursor;
            if (options == null || options.isEmpty()) {
                cursor = (MemorySegment) Native.FIND.invokeExact(
                    handle(), Native.cString(arena, filterJson), err);
            } else {
                cursor = (MemorySegment) Native.FIND_EX.invokeExact(
                    handle(),
                    Native.cString(arena, filterJson),
                    Native.cString(arena, options.toJson()),
                    err);
            }
            Native.checkError(err);
            if (cursor.equals(MemorySegment.NULL)) {
                throw new MooFileException("find returned a null cursor");
            }
            return drainCursor(cursor);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /** Consume a document cursor, freeing it even if decoding fails. */
    private static List<Document> drainCursor(MemorySegment cursor) throws Throwable {
        List<Document> docs = new ArrayList<>();
        try (Arena arena = Arena.ofConfined()) {
            while (true) {
                MemorySegment err = Native.errOut(arena);
                MemorySegment raw = (MemorySegment) Native.CURSOR_NEXT.invokeExact(cursor, err);
                Native.checkError(err);
                String json = Native.takeString(raw);
                if (json == null) break;
                docs.add(Document.parse(json));
            }
        } finally {
            Native.CURSOR_FREE.invokeExact(cursor);
        }
        return docs;
    }

    /** The first matching document, or null if there is none. */
    public synchronized Document findOne(Document filter) {
        String filterJson = filter == null ? "{}" : filter.toJson();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment result = (MemorySegment) Native.FIND_ONE.invokeExact(
                handle(), Native.cString(arena, filterJson), err);
            Native.checkError(err);
            String json = Native.takeString(result);
            return json == null ? null : Document.parse(json);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /** Number of documents matching {@code filter}; null counts everything. */
    public synchronized long count(Document filter) {
        String filterJson = filter == null ? "{}" : filter.toJson();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            long n = (long) Native.COUNT.invokeExact(
                handle(), Native.cString(arena, filterJson), err);
            Native.checkError(err);
            return n;
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /** Count every document. */
    public long count() { return count(null); }

    /** True if at least one document matches. */
    public synchronized boolean exists(Document filter) {
        String filterJson = filter == null ? "{}" : filter.toJson();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            int r = (int) Native.EXISTS.invokeExact(
                handle(), Native.cString(arena, filterJson), err);
            Native.checkError(err);
            return r == 1;
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    // ------------------------------------------------------------------
    // Update
    // ------------------------------------------------------------------

    /** Assemble the {set, unset, inc} blob the C layer expects. */
    private static String updateJson(Document set, List<String> unset, Document inc) {
        Map<String, Object> update = new java.util.LinkedHashMap<>();
        if (set != null && !set.isEmpty())     update.put("set", set.asMap());
        if (unset != null && !unset.isEmpty()) update.put("unset", unset);
        if (inc != null && !inc.isEmpty())     update.put("inc", inc.asMap());
        return Json.write(update);
    }

    /** Update the first matching document with {@code $set} values. */
    public boolean updateOne(Document where, Document set) {
        return updateOne(where, set, null, null);
    }

    /**
     * Update the first matching document.
     *
     * @throws MooFileException if nothing matches — the same contract as the
     *         Rust and Python APIs. Call {@link #exists} first when a miss is
     *         expected.
     */
    public synchronized boolean updateOne(Document where, Document set,
                                          List<String> unset, Document inc) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            int r = (int) Native.UPDATE_ONE.invokeExact(
                handle(),
                Native.cString(arena, where == null ? "{}" : where.toJson()),
                Native.cString(arena, updateJson(set, unset, inc)),
                err);
            Native.checkError(err);
            return r == 1;
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /** Update every matching document with {@code $set} values. */
    public long updateMany(Document where, Document set) {
        return updateMany(where, set, null, null);
    }

    /**
     * Update every matching document and return the count. Unlike
     * {@link #updateOne}, matching nothing is not an error — it returns 0.
     */
    public synchronized long updateMany(Document where, Document set,
                                        List<String> unset, Document inc) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            long n = (long) Native.UPDATE_MANY.invokeExact(
                handle(),
                Native.cString(arena, where == null ? "{}" : where.toJson()),
                Native.cString(arena, updateJson(set, unset, inc)),
                err);
            Native.checkError(err);
            return n;
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /**
     * Replace the first matching document, keeping its {@code _id}.
     *
     * @throws MooFileException if nothing matches
     */
    public synchronized boolean replaceOne(Document where, Document replacement) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            int r = (int) Native.REPLACE_ONE.invokeExact(
                handle(),
                Native.cString(arena, where == null ? "{}" : where.toJson()),
                Native.cString(arena, replacement == null ? "{}" : replacement.toJson()),
                err);
            Native.checkError(err);
            return r == 1;
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    // ------------------------------------------------------------------
    // Delete
    // ------------------------------------------------------------------

    /**
     * Delete the first matching document. Returns false when nothing
     * matched — unlike {@link #updateOne}, that is not an error.
     */
    public synchronized boolean deleteOne(Document where) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            int r = (int) Native.DELETE_ONE.invokeExact(
                handle(),
                Native.cString(arena, where == null ? "{}" : where.toJson()),
                err);
            Native.checkError(err);
            return r == 1;
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /** Delete every matching document and return the count. */
    public synchronized long deleteMany(Document where) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            long n = (long) Native.DELETE_MANY.invokeExact(
                handle(),
                Native.cString(arena, where == null ? "{}" : where.toJson()),
                err);
            Native.checkError(err);
            return n;
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    // ------------------------------------------------------------------
    // Search
    // ------------------------------------------------------------------

    /** A document paired with its similarity or relevance score. */
    public static final class SearchResult {
        private final Document doc;
        private final double score;

        SearchResult(Document doc, double score) {
            this.doc = doc;
            this.score = score;
        }

        public Document doc()  { return doc; }
        public double score()  { return score; }

        @Override
        public String toString() { return score + ": " + doc; }
    }

    /** Cosine-similarity search over a vector field. */
    public List<SearchResult> vectorSearch(String field, List<Double> queryVector, int limit) {
        return vectorSearch(field, queryVector, limit, null);
    }

    /** Cosine-similarity search, restricted to documents matching a filter. */
    public synchronized List<SearchResult> vectorSearch(String field, List<Double> queryVector,
                                                        int limit, Document filter) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment cursor = (MemorySegment) Native.VECTOR_SEARCH.invokeExact(
                handle(),
                Native.cString(arena, filter == null ? "{}" : filter.toJson()),
                Native.cString(arena, field),
                Native.cString(arena, Json.write(queryVector)),
                limit,
                err);
            Native.checkError(err);
            return drainSearchCursor(cursor);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /** BM25 full-text search over a text field. */
    public List<SearchResult> textSearch(String field, String query, int limit) {
        return textSearch(field, query, limit, null);
    }

    /** BM25 search, restricted to documents matching a filter. */
    public synchronized List<SearchResult> textSearch(String field, String query,
                                                      int limit, Document filter) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment cursor = (MemorySegment) Native.TEXT_SEARCH.invokeExact(
                handle(),
                Native.cString(arena, filter == null ? "{}" : filter.toJson()),
                Native.cString(arena, field),
                Native.cString(arena, query),
                limit,
                err);
            Native.checkError(err);
            return drainSearchCursor(cursor);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /**
     * Hybrid BM25 + vector search fused with Reciprocal Rank Fusion.
     * Pass a null {@code queryVector} to auto-embed {@code queryText}.
     */
    public synchronized List<SearchResult> hybridSearch(String textField, String vectorField,
                                                        String queryText, List<Double> queryVector,
                                                        int limit, Document filter) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment vec = queryVector == null
                ? MemorySegment.NULL
                : Native.cString(arena, Json.write(queryVector));
            MemorySegment cursor = (MemorySegment) Native.HYBRID_SEARCH.invokeExact(
                handle(),
                Native.cString(arena, filter == null ? "{}" : filter.toJson()),
                Native.cString(arena, textField),
                Native.cString(arena, vectorField),
                Native.cString(arena, queryText),
                vec,
                limit,
                err);
            Native.checkError(err);
            return drainSearchCursor(cursor);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /**
     * Semantic search — auto-embeds {@code queryText} with the model
     * configured for {@code sourceField} via
     * {@link Config#autoEmbed(String, Config.AutoEmbedConfig)}.
     */
    public List<SearchResult> semantic(String sourceField, String queryText, int limit) {
        return semantic(sourceField, queryText, limit, null);
    }

    /** Semantic search, restricted to documents matching a filter. */
    public synchronized List<SearchResult> semantic(String sourceField, String queryText,
                                                    int limit, Document filter) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment cursor = (MemorySegment) Native.SEMANTIC_SEARCH.invokeExact(
                handle(),
                Native.cString(arena, filter == null ? "{}" : filter.toJson()),
                Native.cString(arena, sourceField),
                Native.cString(arena, queryText),
                limit,
                err);
            Native.checkError(err);
            return drainSearchCursor(cursor);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /**
     * Consume a search cursor, freeing it even if decoding fails.
     *
     * <p>Each entry is the JSON array {@code [doc, score]}, decoded with the
     * real parser — splitting on the first comma breaks on any document
     * holding a vector or a comma inside a string.
     */
    private static List<SearchResult> drainSearchCursor(MemorySegment cursor) throws Throwable {
        if (cursor.equals(MemorySegment.NULL)) return Collections.emptyList();

        List<SearchResult> results = new ArrayList<>();
        try (Arena arena = Arena.ofConfined()) {
            while (true) {
                MemorySegment err = Native.errOut(arena);
                MemorySegment raw =
                    (MemorySegment) Native.SEARCH_CURSOR_NEXT.invokeExact(cursor, err);
                Native.checkError(err);
                String json = Native.takeString(raw);
                if (json == null) break;

                List<Object> pair = Json.parseArray(json);
                if (pair.size() < 2) {
                    throw new MooFileException("malformed search result: " + json);
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> doc = (Map<String, Object>) pair.get(0);
                double score = ((Number) pair.get(1)).doubleValue();
                results.add(new SearchResult(new Document(doc), score));
            }
        } finally {
            Native.SEARCH_CURSOR_FREE.invokeExact(cursor);
        }
        return results;
    }

    // ------------------------------------------------------------------
    // Batch
    // ------------------------------------------------------------------

    /** Begin a batch. Prefer {@link #batch(Runnable)}, which cannot leak one. */
    public synchronized void batchBegin() {
        callVoid(Native.BATCH_BEGIN);
    }

    /** Apply the buffered writes atomically. */
    public synchronized void batchCommit() {
        callVoid(Native.BATCH_COMMIT);
    }

    /** Discard the buffered writes. */
    public synchronized void batchRollback() {
        callVoid(Native.BATCH_ROLLBACK);
    }

    /**
     * Run {@code body} inside an atomic batch: committed if it returns
     * normally, rolled back if it throws anything (including an Error).
     *
     * <p>A rollback failure never masks the original exception.
     */
    public void batch(Runnable body) {
        batchBegin();
        boolean committed = false;
        try {
            body.run();
            batchCommit();
            committed = true;
        } finally {
            if (!committed) {
                try {
                    batchRollback();
                } catch (RuntimeException suppressed) {
                    // Losing the rollback error is better than hiding why the
                    // batch failed in the first place.
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------

    /**
     * Collection statistics: {@code documents}, {@code dead_records},
     * {@code file_size_bytes}, {@code dead_ratio}.
     *
     * <p>One delete produces two dead records (the superseded original plus a
     * tombstone), so use {@code dead_ratio} to decide when to {@link #compact}.
     */
    public synchronized Document stats() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            MemorySegment result = (MemorySegment) Native.STATS.invokeExact(handle(), err);
            Native.checkError(err);
            String json = Native.takeString(result);
            return json == null ? new Document() : Document.parse(json);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }

    /** Rewrite the file, reclaiming space from dead records. */
    public synchronized void compact() { callVoid(Native.COMPACT); }

    /** Flush and fsync the data file. */
    public synchronized void sync() { callVoid(Native.SYNC); }

    /** Rebuild every in-memory index from the data file. */
    public synchronized void reindex() { callVoid(Native.REINDEX); }

    /** Invoke a {@code int f(handle, char**)} function, checking the error slot. */
    private void callVoid(java.lang.invoke.MethodHandle fn) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errOut(arena);
            int unused = (int) fn.invokeExact(handle(), err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw Native.rethrow(t);
        }
    }
}
