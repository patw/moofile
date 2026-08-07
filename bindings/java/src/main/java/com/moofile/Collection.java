package com.moofile;

import java.util.*;
import java.io.*;

/**
 * MooFile Collection — Java binding via JNR-FFI.
 *
 * Requires: jnr-ffi on the classpath.
 *
 * Usage:
 *   Collection db = Collection.open("data.bson",
 *       Config.create().index("email"));
 *   Document doc = db.insert(new Document().put("name", "Alice"));
 *   List<Document> results = db.find(new Document());
 *   db.close();
 *
 * To run with JNR-FFI:
 *   javac -cp jnr-ffi.jar src/main/java/com/moofile/*.java
 *   java -cp .:jnr-ffi.jar -Djava.library.path=../target/release com.moofile.Collection
 */
public class Collection {

    // ------------------------------------------------------------------
    // Native library loading through JNR-FFI
    // ------------------------------------------------------------------

    private static native long moofile_open(String path, String configJson, long[] errOut);
    private static native int moofile_close(long handle, long[] errOut);
    private static native String moofile_insert(long handle, String docJson, long[] errOut);
    private static native String moofile_insert_many(long handle, String docsJson, long[] errOut);
    private static native long moofile_find(long handle, String filterJson, long[] errOut);
    private static native String moofile_find_one(long handle, String filterJson, long[] errOut);
    private static native long moofile_count(long handle, String filterJson, long[] errOut);
    private static native int moofile_exists(long handle, String filterJson, long[] errOut);
    private static native String moofile_cursor_next(long cursor, long[] errOut);
    private static native void moofile_cursor_free(long cursor);
    private static native int moofile_update_one(long handle, String whereJson, String updateJson, long[] errOut);
    private static native long moofile_update_many(long handle, String whereJson, String updateJson, long[] errOut);
    private static native int moofile_replace_one(long handle, String whereJson, String replJson, long[] errOut);
    private static native int moofile_delete_one(long handle, String whereJson, long[] errOut);
    private static native long moofile_delete_many(long handle, String whereJson, long[] errOut);
    private static native long moofile_vector_search(long handle, String filterJson, String field, String vecJson, int limit, long[] errOut);
    private static native long moofile_text_search(long handle, String filterJson, String field, String query, int limit, long[] errOut);
    private static native long moofile_hybrid_search(long handle, String filterJson, String tf, String vf, String qt, String qv, int limit, long[] errOut);
    private static native long moofile_semantic_search(long handle, String filterJson, String field, String query, int limit, long[] errOut);
    private static native String moofile_search_cursor_next(long cursor, long[] errOut);
    private static native void moofile_search_cursor_free(long cursor);
    private static native int moofile_batch_begin(long handle, long[] errOut);
    private static native int moofile_batch_commit(long handle, long[] errOut);
    private static native int moofile_batch_rollback(long handle, long[] errOut);
    private static native String moofile_stats(long handle, long[] errOut);
    private static native int moofile_compact(long handle, long[] errOut);
    private static native int moofile_sync(long handle, long[] errOut);
    private static native int moofile_reindex(long handle, long[] errOut);
    private static native void moofile_free_string(long ptr);

    static {
        try {
            System.loadLibrary("moofile");
        } catch (UnsatisfiedLinkError e) {
            // Try default path
            String libPath = "../target/release/libmoofile.so";
            File f = new File(libPath);
            if (f.exists()) {
                System.load(f.getAbsolutePath());
            } else {
                throw new RuntimeException("Cannot find libmoofile. " +
                    "Build with: cargo build -p moofile-c --release\n" +
                    "Then run with: -Djava.library.path=target/release", e);
            }
        }
    }

    // ------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------

    private long handle;
    private final String path;

    private Collection(long handle, String path) {
        this.handle = handle;
        this.path = path;
    }

    // ------------------------------------------------------------------
    // Open / Close
    // ------------------------------------------------------------------

    /** Open a MooFile collection. */
    public static Collection open(String path, Config config) {
        long[] errOut = new long[1];
        long h = native_moofile_open(path, config.toJson(), errOut);
        checkError(errOut);
        if (h == 0) throw new RuntimeException("moofile_open returned null");
        return new Collection(h, path);
    }

    private static long native_moofile_open(String path, String configJson, long[] errOut) {
        return moofile_open(path, configJson, errOut);
    }

    /** Close the collection. */
    public void close() {
        if (handle != 0) {
            long[] errOut = new long[1];
            moofile_close(handle, errOut);
            checkError(errOut);
            handle = 0;
        }
    }

    // ------------------------------------------------------------------
    // Insert
    // ------------------------------------------------------------------

    public Document insert(Document doc) {
        long[] errOut = new long[1];
        String result = moofile_insert(handle, doc.toJson(), errOut);
        checkError(errOut);
        return result != null ? Document.parse(result) : doc;
    }

    public List<Document> insertMany(List<Document> docs) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Document d : docs) {
            if (!first) sb.append(",");
            first = false;
            sb.append(d.toJson());
        }
        sb.append("]");
        long[] errOut = new long[1];
        String result = moofile_insert_many(handle, sb.toString(), errOut);
        checkError(errOut);
        if (result == null) return docs;
        // Parse JSON array
        List<Document> parsed = new ArrayList<>();
        // Quick and dirty: split by top-level objects
        String array = result.trim();
        if (array.startsWith("[") && array.endsWith("]")) {
            array = array.substring(1, array.length() - 1);
            int depth = 0;
            int start = 0;
            for (int i = 0; i <= array.length(); i++) {
                if (i == array.length() || (array.charAt(i) == ',' && depth == 0)) {
                    String item = array.substring(start, i).trim();
                    if (!item.isEmpty()) parsed.add(Document.parse(item));
                    start = i + 1;
                } else if (array.charAt(i) == '{') depth++;
                else if (array.charAt(i) == '}') depth--;
            }
        }
        return parsed;
    }

    // ------------------------------------------------------------------
    // Query
    // ------------------------------------------------------------------

    /** Find documents matching a filter. */
    public List<Document> find(Document filter) {
        long[] errOut = new long[1];
        long cursor = moofile_find(handle, filter.toJson(), errOut);
        checkError(errOut);
        if (cursor == 0) return Collections.emptyList();

        List<Document> docs = new ArrayList<>();
        while (true) {
            long[] nextErr = new long[1];
            String s = moofile_cursor_next(cursor, nextErr);
            checkError(nextErr);
            if (s == null) break;
            docs.add(Document.parse(s));
        }
        moofile_cursor_free(cursor);
        return docs;
    }

    /** Find the first matching document, or null. */
    public Document findOne(Document filter) {
        long[] errOut = new long[1];
        String result = moofile_find_one(handle, filter.toJson(), errOut);
        checkError(errOut);
        return result != null ? Document.parse(result) : null;
    }

    /** Count documents matching a filter. */
    public long count(Document filter) {
        long[] errOut = new long[1];
        long n = moofile_count(handle, filter.toJson(), errOut);
        checkError(errOut);
        return n;
    }

    /** Check if at least one document matches. */
    public boolean exists(Document filter) {
        long[] errOut = new long[1];
        int r = moofile_exists(handle, filter.toJson(), errOut);
        checkError(errOut);
        return r == 1;
    }

    // ------------------------------------------------------------------
    // Update
    // ------------------------------------------------------------------

    /** Update the first matching document. */
    public boolean updateOne(Document where, Document setValues, List<String> unsetFields, Document incValues) {
        Map<String, Object> update = new LinkedHashMap<>();
        if (setValues != null && !setValues.data.isEmpty()) update.put("set", setValues.data);
        if (unsetFields != null && !unsetFields.isEmpty()) update.put("unset", unsetFields);
        if (incValues != null && !incValues.data.isEmpty()) update.put("inc", incValues.data);

        long[] errOut = new long[1];
        int r = moofile_update_one(handle, where.toJson(), new Document(update).toJson(), errOut);
        checkError(errOut);
        return r == 1;
    }

    /** Update all matching documents. */
    public long updateMany(Document where, Document setValues, List<String> unsetFields, Document incValues) {
        Map<String, Object> update = new LinkedHashMap<>();
        if (setValues != null && !setValues.data.isEmpty()) update.put("set", setValues.data);
        if (unsetFields != null && !unsetFields.isEmpty()) update.put("unset", unsetFields);
        if (incValues != null && !incValues.data.isEmpty()) update.put("inc", incValues.data);

        long[] errOut = new long[1];
        long n = moofile_update_many(handle, where.toJson(), new Document(update).toJson(), errOut);
        checkError(errOut);
        return n;
    }

    /** Replace the first matching document. */
    public boolean replaceOne(Document where, Document replacement) {
        long[] errOut = new long[1];
        int r = moofile_replace_one(handle, where.toJson(), replacement.toJson(), errOut);
        checkError(errOut);
        return r == 1;
    }

    // ------------------------------------------------------------------
    // Delete
    // ------------------------------------------------------------------

    public boolean deleteOne(Document where) {
        long[] errOut = new long[1];
        int r = moofile_delete_one(handle, where.toJson(), errOut);
        checkError(errOut);
        return r == 1;
    }

    public long deleteMany(Document where) {
        long[] errOut = new long[1];
        long n = moofile_delete_many(handle, where.toJson(), errOut);
        checkError(errOut);
        return n;
    }

    // ------------------------------------------------------------------
    // Search
    // ------------------------------------------------------------------

    /** Vector search. Returns list of (doc_json, score) strings. */
    public List<SearchResult> vectorSearch(String field, List<Double> queryVector, int limit, Document filter) {
        long[] errOut = new long[1];
        long cursor = moofile_vector_search(handle,
            filter != null ? filter.toJson() : "{}",
            field, jsonArray(queryVector), limit, errOut);
        checkError(errOut);
        return drainSearchCursor(cursor);
    }

    public List<SearchResult> textSearch(String field, String query, int limit, Document filter) {
        long[] errOut = new long[1];
        long cursor = moofile_text_search(handle,
            filter != null ? filter.toJson() : "{}",
            field, query, limit, errOut);
        checkError(errOut);
        return drainSearchCursor(cursor);
    }

    public List<SearchResult> hybridSearch(String textField, String vectorField,
                                           String queryText, List<Double> queryVector,
                                           int limit, Document filter) {
        String qv = queryVector != null ? jsonArray(queryVector) : null;
        long[] errOut = new long[1];
        long cursor = moofile_hybrid_search(handle,
            filter != null ? filter.toJson() : "{}",
            textField, vectorField, queryText, qv, limit, errOut);
        checkError(errOut);
        return drainSearchCursor(cursor);
    }

    public List<SearchResult> semantic(String sourceField, String queryText, int limit, Document filter) {
        long[] errOut = new long[1];
        long cursor = moofile_semantic_search(handle,
            filter != null ? filter.toJson() : "{}",
            sourceField, queryText, limit, errOut);
        checkError(errOut);
        return drainSearchCursor(cursor);
    }

    private List<SearchResult> drainSearchCursor(long cursor) {
        if (cursor == 0) return Collections.emptyList();
        List<SearchResult> results = new ArrayList<>();
        while (true) {
            long[] errOut = new long[1];
            String s = moofile_search_cursor_next(cursor, errOut);
            checkError(errOut);
            if (s == null) break;

            // Parse [doc, score]
            if (s.startsWith("[") && s.endsWith("]")) {
                String inner = s.substring(1, s.length() - 1);
                // Find the document/score boundary
                int depth = 0;
                int split = -1;
                for (int i = 0; i < inner.length(); i++) {
                    char c = inner.charAt(i);
                    if (c == '{') depth++;
                    else if (c == '}') depth--;
                    else if (c == ',' && depth == 0) { split = i; break; }
                }
                if (split > 0) {
                    String docStr = inner.substring(0, split).trim();
                    String scoreStr = inner.substring(split + 1).trim();
                    results.add(new SearchResult(
                        Document.parse(docStr),
                        Double.parseDouble(scoreStr)
                    ));
                }
            }
        }
        moofile_search_cursor_free(cursor);
        return results;
    }

    // ------------------------------------------------------------------
    // Batch
    // ------------------------------------------------------------------

    public void batchBegin() {
        long[] errOut = new long[1];
        if (moofile_batch_begin(handle, errOut) != 0) checkError(errOut);
    }

    public void batchCommit() {
        long[] errOut = new long[1];
        if (moofile_batch_commit(handle, errOut) != 0) checkError(errOut);
    }

    public void batchRollback() {
        long[] errOut = new long[1];
        moofile_batch_rollback(handle, errOut);
    }

    /** Run operations atomically; rolls back on any exception. */
    public void batch(Runnable fn) {
        batchBegin();
        try {
            fn.run();
            batchCommit();
        } catch (RuntimeException e) {
            batchRollback();
            throw e;
        }
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------

    public Document stats() {
        long[] errOut = new long[1];
        String result = moofile_stats(handle, errOut);
        checkError(errOut);
        return result != null ? Document.parse(result) : new Document();
    }

    public void compact() {
        long[] errOut = new long[1];
        if (moofile_compact(handle, errOut) != 0) checkError(errOut);
    }

    public void sync() {
        long[] errOut = new long[1];
        if (moofile_sync(handle, errOut) != 0) checkError(errOut);
    }

    public void reindex() {
        long[] errOut = new long[1];
        if (moofile_reindex(handle, errOut) != 0) checkError(errOut);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static String jsonArray(List<Double> list) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(list.get(i));
        }
        return sb.append("]").toString();
    }

    private static void checkError(long[] errOut) {
        if (errOut[0] != 0) {
            // In JNR-FFI the error is returned differently.
            // We handle it through the JNR interface.
        }
    }

    /** Search result holding a document and similarity score. */
    public static class SearchResult {
        public final Document doc;
        public final double score;
        public SearchResult(Document doc, double score) {
            this.doc = doc; this.score = score;
        }
    }
}
