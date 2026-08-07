using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Moofile;

/// <summary>
/// A handle to an open MooFile collection.
/// </summary>
/// <remarks>
/// Requires the libmoofile shared library:
/// <code>cargo build -p moofile-c --release</code>
///
/// <code>
/// using var db = Collection.Open("data.bson", new Config {
///     Indexes = new[] { "email" },
/// });
///
/// db.Insert(Document.Of("name", "Alice", "email", "a@example.com", "age", 30));
///
/// foreach (var doc in db.Find(Document.Of("age", Document.Of("$gt", 25))))
///     Console.WriteLine(doc);
///
/// var oldest = db.Find(null, FindOptions.Create().Sort("age", desc: true).Limit(10));
/// </code>
///
/// Thread-safe: every method locks the handle, on top of the cross-process
/// locking the Rust core already does.
/// </remarks>
public sealed class Collection : IDisposable
{
    private IntPtr _handle;
    private readonly string _path;
    private readonly object _lock = new();

    private Collection(IntPtr handle, string path)
    {
        _handle = handle;
        _path = path;
    }

    // -----------------------------------------------------------------
    // Open / dispose
    // -----------------------------------------------------------------

    /// <summary>Open a collection, creating the file if it does not exist.</summary>
    public static Collection Open(string path, Config? config = null)
    {
        ArgumentNullException.ThrowIfNull(path);
        Native.EnsureLoaded();

        var handle = Native.moofile_open(path, config?.ToJson() ?? "{}", out var err);
        Native.ThrowIfError(err);
        if (handle == IntPtr.Zero)
            throw new MooFileException($"failed to open collection: {path}");

        return new Collection(handle, path);
    }

    /// <summary>The file this collection was opened from.</summary>
    public string Path => _path;

    /// <summary>Close the collection. Idempotent.</summary>
    public void Dispose()
    {
        lock (_lock)
        {
            if (_handle == IntPtr.Zero) return;
            var handle = _handle;
            _handle = IntPtr.Zero;
            Native.moofile_close(handle, out var err);
            Native.ThrowIfError(err);
        }
        GC.SuppressFinalize(this);
    }

    /// <summary>Backstop for callers who forget Dispose.</summary>
    ~Collection()
    {
        if (_handle == IntPtr.Zero) return;
        // Don't surface errors from the finalizer thread — nobody can act on them.
        Native.moofile_close(_handle, out var err);
        if (err != IntPtr.Zero) Native.moofile_free_string(err);
        _handle = IntPtr.Zero;
    }

    private IntPtr Handle =>
        _handle != IntPtr.Zero ? _handle : throw new ObjectDisposedException(nameof(Collection));

    // -----------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------

    /// <summary>Insert one document; returns it with <c>_id</c> populated.</summary>
    public Document Insert(Document doc)
    {
        ArgumentNullException.ThrowIfNull(doc);
        lock (_lock)
        {
            var raw = Native.moofile_insert(Handle, doc.ToJson(), out var err);
            Native.ThrowIfError(err);
            var json = Native.TakeString(raw)
                ?? throw new MooFileException("insert returned no document");
            return Document.Parse(json);
        }
    }

    /// <summary>Insert several documents; returns them with <c>_id</c>s populated.</summary>
    public List<Document> InsertMany(IEnumerable<Document> docs)
    {
        ArgumentNullException.ThrowIfNull(docs);

        var arr = new JsonArray();
        foreach (var d in docs) arr.Add(Document.ToNode(d));

        lock (_lock)
        {
            var raw = Native.moofile_insert_many(Handle, arr.ToJsonString(), out var err);
            Native.ThrowIfError(err);
            var json = Native.TakeString(raw)
                ?? throw new MooFileException("insertMany returned no documents");
            return ParseDocumentArray(json);
        }
    }

    private static List<Document> ParseDocumentArray(string json)
    {
        if (JsonNode.Parse(json) is not JsonArray arr)
            throw new MooFileException("expected a JSON array of documents");

        var docs = new List<Document>(arr.Count);
        foreach (var node in arr)
        {
            if (Document.FromNode(node) is Document d) docs.Add(d);
            else throw new MooFileException("expected an object in the result array");
        }
        return docs;
    }

    // -----------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------

    /// <summary>
    /// Find documents matching a filter, optionally sorted, paged, or grouped.
    /// </summary>
    /// <param name="filter">Match filter, or null for everything.</param>
    /// <param name="options">Query-builder stages, or null for none.</param>
    public List<Document> Find(Document? filter = null, FindOptions? options = null)
    {
        var filterJson = filter?.ToJson() ?? "{}";

        lock (_lock)
        {
            IntPtr cursor;
            IntPtr err;
            if (options is null || options.IsEmpty)
            {
                cursor = Native.moofile_find(Handle, filterJson, out err);
            }
            else
            {
                cursor = Native.moofile_find_ex(Handle, filterJson, options.ToJson(), out err);
            }
            Native.ThrowIfError(err);
            if (cursor == IntPtr.Zero)
                throw new MooFileException("find returned a null cursor");

            return DrainCursor(cursor);
        }
    }

    /// <summary>Find documents using a strongly typed property-based filter.</summary>
    public List<Document> Find<TDocument>(FilterDefinition<TDocument> filter, FindOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return Find(filter.ToDocument(), options);
    }

    /// <summary>Consume a cursor, freeing it even if decoding throws.</summary>
    private static List<Document> DrainCursor(IntPtr cursor)
    {
        var docs = new List<Document>();
        try
        {
            while (true)
            {
                var raw = Native.moofile_cursor_next(cursor, out var err);
                Native.ThrowIfError(err);
                var json = Native.TakeString(raw);
                if (json is null) break;
                docs.Add(Document.Parse(json));
            }
        }
        finally
        {
            Native.moofile_cursor_free(cursor);
        }
        return docs;
    }

    /// <summary>The first matching document, or null if there is none.</summary>
    public Document? FindOne(Document? filter = null)
    {
        lock (_lock)
        {
            var raw = Native.moofile_find_one(Handle, filter?.ToJson() ?? "{}", out var err);
            Native.ThrowIfError(err);
            var json = Native.TakeString(raw);
            return json is null ? null : Document.Parse(json);
        }
    }

    /// <summary>Find the first document using a strongly typed property-based filter.</summary>
    public Document? FindOne<TDocument>(FilterDefinition<TDocument> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return FindOne(filter.ToDocument());
    }

    /// <summary>Number of documents matching the filter; null counts everything.</summary>
    public long Count(Document? filter = null)
    {
        lock (_lock)
        {
            var n = Native.moofile_count(Handle, filter?.ToJson() ?? "{}", out var err);
            Native.ThrowIfError(err);
            return n;
        }
    }

    /// <summary>Count documents using a strongly typed property-based filter.</summary>
    public long Count<TDocument>(FilterDefinition<TDocument> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return Count(filter.ToDocument());
    }

    /// <summary>True if at least one document matches.</summary>
    public bool Exists(Document filter)
    {
        lock (_lock)
        {
            var r = Native.moofile_exists(Handle, filter?.ToJson() ?? "{}", out var err);
            Native.ThrowIfError(err);
            return r == 1;
        }
    }

    /// <summary>Check whether a strongly typed property-based filter has a match.</summary>
    public bool Exists<TDocument>(FilterDefinition<TDocument> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return Exists(filter.ToDocument());
    }

    // -----------------------------------------------------------------
    // Update
    // -----------------------------------------------------------------

    /// <summary>Assemble the {set, unset, inc} blob the C layer expects.</summary>
    private static string BuildUpdate(Document? set, IEnumerable<string>? unset, Document? inc)
    {
        var update = new JsonObject();
        if (set is { Count: > 0 }) update["set"] = Document.ToNode(set);
        if (unset is not null)
        {
            var arr = new JsonArray();
            foreach (var f in unset) arr.Add(f);
            if (arr.Count > 0) update["unset"] = arr;
        }
        if (inc is { Count: > 0 }) update["inc"] = Document.ToNode(inc);
        return update.ToJsonString();
    }

    /// <summary>
    /// Update the first matching document.
    /// </summary>
    /// <exception cref="MooFileException">
    /// Thrown when nothing matches — the same contract as the Rust and Python
    /// APIs. Call <see cref="Exists"/> first when a miss is expected.
    /// </exception>
    public bool UpdateOne(Document where, Document? set = null,
                          IEnumerable<string>? unset = null, Document? inc = null)
    {
        lock (_lock)
        {
            var r = Native.moofile_update_one(Handle, where?.ToJson() ?? "{}",
                BuildUpdate(set, unset, inc), out var err);
            Native.ThrowIfError(err);
            return r == 1;
        }
    }

    /// <summary>Update the first document matching a strongly typed filter.</summary>
    public bool UpdateOne<TDocument>(FilterDefinition<TDocument> where, Document? set = null,
                                     IEnumerable<string>? unset = null, Document? inc = null)
    {
        ArgumentNullException.ThrowIfNull(where);
        return UpdateOne(where.ToDocument(), set, unset, inc);
    }

    /// <summary>
    /// Update every matching document and return the count. Unlike
    /// <see cref="UpdateOne"/>, matching nothing is not an error — it returns 0.
    /// </summary>
    public long UpdateMany(Document where, Document? set = null,
                           IEnumerable<string>? unset = null, Document? inc = null)
    {
        lock (_lock)
        {
            var n = Native.moofile_update_many(Handle, where?.ToJson() ?? "{}",
                BuildUpdate(set, unset, inc), out var err);
            Native.ThrowIfError(err);
            return n;
        }
    }

    /// <summary>Update every document matching a strongly typed filter.</summary>
    public long UpdateMany<TDocument>(FilterDefinition<TDocument> where, Document? set = null,
                                      IEnumerable<string>? unset = null, Document? inc = null)
    {
        ArgumentNullException.ThrowIfNull(where);
        return UpdateMany(where.ToDocument(), set, unset, inc);
    }

    /// <summary>
    /// Replace the first matching document, keeping its <c>_id</c>.
    /// </summary>
    /// <exception cref="MooFileException">Thrown when nothing matches.</exception>
    public bool ReplaceOne(Document where, Document replacement)
    {
        lock (_lock)
        {
            var r = Native.moofile_replace_one(Handle, where?.ToJson() ?? "{}",
                replacement?.ToJson() ?? "{}", out var err);
            Native.ThrowIfError(err);
            return r == 1;
        }
    }

    /// <summary>Replace the first document matching a strongly typed filter.</summary>
    public bool ReplaceOne<TDocument>(FilterDefinition<TDocument> where, Document replacement)
    {
        ArgumentNullException.ThrowIfNull(where);
        return ReplaceOne(where.ToDocument(), replacement);
    }

    // -----------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------

    /// <summary>
    /// Delete the first matching document. Returns false when nothing
    /// matched — unlike <see cref="UpdateOne"/>, that is not an error.
    /// </summary>
    public bool DeleteOne(Document where)
    {
        lock (_lock)
        {
            var r = Native.moofile_delete_one(Handle, where?.ToJson() ?? "{}", out var err);
            Native.ThrowIfError(err);
            return r == 1;
        }
    }

    /// <summary>Delete the first document matching a strongly typed filter.</summary>
    public bool DeleteOne<TDocument>(FilterDefinition<TDocument> where)
    {
        ArgumentNullException.ThrowIfNull(where);
        return DeleteOne(where.ToDocument());
    }

    /// <summary>Delete every matching document and return the count.</summary>
    public long DeleteMany(Document where)
    {
        lock (_lock)
        {
            var n = Native.moofile_delete_many(Handle, where?.ToJson() ?? "{}", out var err);
            Native.ThrowIfError(err);
            return n;
        }
    }

    /// <summary>Delete every document matching a strongly typed filter.</summary>
    public long DeleteMany<TDocument>(FilterDefinition<TDocument> where)
    {
        ArgumentNullException.ThrowIfNull(where);
        return DeleteMany(where.ToDocument());
    }

    // -----------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------

    private static string VectorJson(IEnumerable<double> v)
    {
        var arr = new JsonArray();
        foreach (var x in v) arr.Add(x);
        return arr.ToJsonString();
    }

    /// <summary>Cosine-similarity search over a vector field.</summary>
    public List<SearchResult> VectorSearch(string field, IEnumerable<double> queryVector,
                                           int limit = 10, Document? filter = null)
    {
        lock (_lock)
        {
            var cursor = Native.moofile_vector_search(Handle, filter?.ToJson() ?? "{}",
                field, VectorJson(queryVector), limit, out var err);
            Native.ThrowIfError(err);
            return DrainSearchCursor(cursor);
        }
    }

    /// <summary>Vector search with a strongly typed pre-filter.</summary>
    public List<SearchResult> VectorSearch<TDocument>(string field, IEnumerable<double> queryVector,
                                                      FilterDefinition<TDocument> filter, int limit = 10)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return VectorSearch(field, queryVector, limit, filter.ToDocument());
    }

    /// <summary>BM25 full-text search over a text field.</summary>
    public List<SearchResult> TextSearch(string field, string query,
                                         int limit = 10, Document? filter = null)
    {
        lock (_lock)
        {
            var cursor = Native.moofile_text_search(Handle, filter?.ToJson() ?? "{}",
                field, query, limit, out var err);
            Native.ThrowIfError(err);
            return DrainSearchCursor(cursor);
        }
    }

    /// <summary>Text search with a strongly typed pre-filter.</summary>
    public List<SearchResult> TextSearch<TDocument>(string field, string query,
                                                    FilterDefinition<TDocument> filter, int limit = 10)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return TextSearch(field, query, limit, filter.ToDocument());
    }

    /// <summary>
    /// Hybrid BM25 + vector search fused with Reciprocal Rank Fusion.
    /// Pass a null <paramref name="queryVector"/> to auto-embed the query text.
    /// </summary>
    public List<SearchResult> HybridSearch(string textField, string vectorField,
                                           string queryText, IEnumerable<double>? queryVector = null,
                                           int limit = 10, Document? filter = null)
    {
        lock (_lock)
        {
            var cursor = Native.moofile_hybrid_search(Handle, filter?.ToJson() ?? "{}",
                textField, vectorField, queryText,
                queryVector is null ? null : VectorJson(queryVector),
                limit, out var err);
            Native.ThrowIfError(err);
            return DrainSearchCursor(cursor);
        }
    }

    /// <summary>Hybrid search with a strongly typed pre-filter.</summary>
    public List<SearchResult> HybridSearch<TDocument>(string textField, string vectorField,
        string queryText, FilterDefinition<TDocument> filter, IEnumerable<double>? queryVector = null,
        int limit = 10)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return HybridSearch(textField, vectorField, queryText, queryVector, limit, filter.ToDocument());
    }

    /// <summary>
    /// Semantic search — auto-embeds <paramref name="queryText"/> with the
    /// model configured for <paramref name="sourceField"/> via
    /// <see cref="Config.AutoEmbed"/>.
    /// </summary>
    public List<SearchResult> Semantic(string sourceField, string queryText,
                                       int limit = 10, Document? filter = null)
    {
        lock (_lock)
        {
            var cursor = Native.moofile_semantic_search(Handle, filter?.ToJson() ?? "{}",
                sourceField, queryText, limit, out var err);
            Native.ThrowIfError(err);
            return DrainSearchCursor(cursor);
        }
    }

    /// <summary>Semantic search with a strongly typed pre-filter.</summary>
    public List<SearchResult> Semantic<TDocument>(string sourceField, string queryText,
                                                  FilterDefinition<TDocument> filter, int limit = 10)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return Semantic(sourceField, queryText, limit, filter.ToDocument());
    }

    /// <summary>
    /// Consume a search cursor, freeing it even if decoding throws.
    /// </summary>
    /// <remarks>
    /// Each entry is the JSON array <c>[doc, score]</c>, decoded with a real
    /// parser — splitting on the first comma breaks on any document holding a
    /// vector or a comma inside a string.
    /// </remarks>
    private static List<SearchResult> DrainSearchCursor(IntPtr cursor)
    {
        if (cursor == IntPtr.Zero)
            throw new MooFileException("search returned a null cursor");

        var results = new List<SearchResult>();
        try
        {
            while (true)
            {
                var raw = Native.moofile_search_cursor_next(cursor, out var err);
                Native.ThrowIfError(err);
                var json = Native.TakeString(raw);
                if (json is null) break;

                if (JsonNode.Parse(json) is not JsonArray pair || pair.Count < 2)
                    throw new MooFileException($"malformed search result: {json}");

                if (Document.FromNode(pair[0]) is not Document doc)
                    throw new MooFileException("search result is not a document");

                results.Add(new SearchResult
                {
                    Doc = doc,
                    Score = Convert.ToDouble(Document.FromNode(pair[1])),
                });
            }
        }
        finally
        {
            Native.moofile_search_cursor_free(cursor);
        }
        return results;
    }

    // -----------------------------------------------------------------
    // Batch
    // -----------------------------------------------------------------

    /// <summary>Begin a batch. Prefer <see cref="Batch(Action)"/>.</summary>
    public void BatchBegin()
    {
        lock (_lock)
        {
            Native.moofile_batch_begin(Handle, out var err);
            Native.ThrowIfError(err);
        }
    }

    /// <summary>Apply the buffered writes atomically.</summary>
    public void BatchCommit()
    {
        lock (_lock)
        {
            Native.moofile_batch_commit(Handle, out var err);
            Native.ThrowIfError(err);
        }
    }

    /// <summary>Discard the buffered writes.</summary>
    public void BatchRollback()
    {
        lock (_lock)
        {
            Native.moofile_batch_rollback(Handle, out var err);
            Native.ThrowIfError(err);
        }
    }

    /// <summary>
    /// Run <paramref name="body"/> inside an atomic batch: committed if it
    /// returns normally, rolled back if it throws. A rollback failure never
    /// masks the original exception.
    /// </summary>
    public void Batch(Action body)
    {
        ArgumentNullException.ThrowIfNull(body);
        BatchBegin();
        var committed = false;
        try
        {
            body();
            BatchCommit();
            committed = true;
        }
        finally
        {
            if (!committed)
            {
                try { BatchRollback(); }
                catch (MooFileException) { /* keep the original exception */ }
            }
        }
    }

    // -----------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------

    /// <summary>
    /// Collection statistics: <c>documents</c>, <c>dead_records</c>,
    /// <c>file_size_bytes</c>, <c>dead_ratio</c>.
    /// </summary>
    /// <remarks>
    /// One delete produces two dead records (the superseded original plus a
    /// tombstone), so use <c>dead_ratio</c> to decide when to
    /// <see cref="Compact"/>.
    /// </remarks>
    public Document Stats()
    {
        lock (_lock)
        {
            var raw = Native.moofile_stats(Handle, out var err);
            Native.ThrowIfError(err);
            var json = Native.TakeString(raw);
            return json is null ? new Document() : Document.Parse(json);
        }
    }

    /// <summary>Rewrite the file, reclaiming space from dead records.</summary>
    public void Compact()
    {
        lock (_lock)
        {
            Native.moofile_compact(Handle, out var err);
            Native.ThrowIfError(err);
        }
    }

    /// <summary>Flush and fsync the data file.</summary>
    public void Sync()
    {
        lock (_lock)
        {
            Native.moofile_sync(Handle, out var err);
            Native.ThrowIfError(err);
        }
    }

    /// <summary>Rebuild every in-memory index from the data file.</summary>
    public void Reindex()
    {
        lock (_lock)
        {
            Native.moofile_reindex(Handle, out var err);
            Native.ThrowIfError(err);
        }
    }
}
