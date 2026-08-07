using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Moofile;

/// <summary>
/// MooFile — lightweight embedded document store.
///
/// C# binding using P/Invoke to call libmoofile.so / moofile.dll.
/// All documents are passed as JSON strings across the FFI boundary.
///
/// Usage:
///   using Moofile;
///   using var db = Collection.Open("data.bson", new Config {
///       Indexes = new[] { "email" }
///   });
///   db.Insert(new Document { ["name"] = "Alice" });
///   var results = db.Find(new Document { ["age"] = new Document { ["$gt"] = 25 } });
/// </summary>

// ---------------------------------------------------------------------------
// Native methods (P/Invoke)
// ---------------------------------------------------------------------------

internal static class Native
{
    private const string LibName = "moofile";

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_open(string path, string configJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_close(IntPtr handle, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_insert(IntPtr handle, string docJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_insert_many(IntPtr handle, string docsJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_find(IntPtr handle, string filterJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_find_one(IntPtr handle, string filterJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long moofile_count(IntPtr handle, string filterJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_exists(IntPtr handle, string filterJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_cursor_next(IntPtr cursor, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void moofile_cursor_free(IntPtr cursor);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_update_one(IntPtr handle, string whereJson, string updateJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long moofile_update_many(IntPtr handle, string whereJson, string updateJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_replace_one(IntPtr handle, string whereJson, string replJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_delete_one(IntPtr handle, string whereJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long moofile_delete_many(IntPtr handle, string whereJson, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_vector_search(IntPtr handle, string filterJson, string field, string vecJson, int limit, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_text_search(IntPtr handle, string filterJson, string field, string query, int limit, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_hybrid_search(IntPtr handle, string filterJson, string tf, string vf, string qt, string qv, int limit, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_semantic_search(IntPtr handle, string filterJson, string field, string query, int limit, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_search_cursor_next(IntPtr cursor, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void moofile_search_cursor_free(IntPtr cursor);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_batch_begin(IntPtr handle, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_batch_commit(IntPtr handle, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_batch_rollback(IntPtr handle, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_stats(IntPtr handle, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_compact(IntPtr handle, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_sync(IntPtr handle, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_reindex(IntPtr handle, IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void moofile_free_string(IntPtr s);
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

public class AutoEmbedConfig
{
    public string Model { get; set; } = "";
    public string Target { get; set; } = "";
    public int Dims { get; set; } = 1024;
    public string Precision { get; set; } = "int8";
    public bool Normalize { get; set; } = true;
    public string QueryPrefix { get; set; } = "Represent the query for retrieving supporting documents: ";
    public string DocPrefix { get; set; } = "Represent the document for retrieval: ";
}

public class Config
{
    public string[]? Indexes { get; set; }
    public Dictionary<string, int>? VectorIndexes { get; set; }
    public string[]? TextIndexes { get; set; }
    public Dictionary<string, AutoEmbedConfig>? AutoEmbed { get; set; }
    public bool Readonly { get; set; }
    public string Durability { get; set; } = "os";
    public string? ModelCacheDir { get; set; }

    internal string ToJson()
    {
        var obj = new Dictionary<string, object>();
        if (Indexes is { Length: > 0 }) obj["indexes"] = Indexes;
        if (VectorIndexes is { Count: > 0 }) obj["vector_indexes"] = VectorIndexes;
        if (TextIndexes is { Length: > 0 }) obj["text_indexes"] = TextIndexes;
        if (AutoEmbed is { Count: > 0 })
        {
            var ae = new Dictionary<string, object>();
            foreach (var (k, v) in AutoEmbed)
            {
                ae[k] = new Dictionary<string, object>
                {
                    ["model"] = v.Model, ["target"] = v.Target,
                    ["dims"] = v.Dims, ["precision"] = v.Precision,
                    ["normalize"] = v.Normalize,
                    ["query_prefix"] = v.QueryPrefix, ["doc_prefix"] = v.DocPrefix,
                };
            }
            obj["auto_embed"] = ae;
        }
        if (Readonly) obj["readonly"] = true;
        obj["durability"] = Durability;
        if (ModelCacheDir != null) obj["model_cache_dir"] = ModelCacheDir;
        return JsonSerializer.Serialize(obj);
    }
}

// ---------------------------------------------------------------------------
// Document
// ---------------------------------------------------------------------------

public class Document : Dictionary<string, object?>
{
    public Document() { }
    public Document(IDictionary<string, object?> d) : base(d) { }

    public string ToJson() => JsonSerializer.Serialize(this);
    public static Document Parse(string json) => JsonSerializer.Deserialize<Document>(json) ?? new();
}

// ---------------------------------------------------------------------------
// SearchResult
// ---------------------------------------------------------------------------

public class SearchResult
{
    public Document Doc { get; init; } = new();
    public double Score { get; init; }
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

public class Collection : IDisposable
{
    private IntPtr _handle;
    private bool _disposed;

    private static string FindLibrary()
    {
        var candidates = new[]
        {
            "../target/release/libmoofile.so",
            "../target/debug/libmoofile.so",
            "../../target/release/libmoofile.so",
            "./libmoofile.so",
        };
        foreach (var c in candidates)
        {
            var full = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, c));
            if (File.Exists(full)) return full;
        }
        return "libmoofile"; // fallback to system path
    }

    static Collection()
    {
        var lib = FindLibrary();
        if (lib != "libmoofile" && File.Exists(lib))
        {
            NativeLibrary.Load(lib);
        }
    }

    public static Collection Open(string path, Config? config = null)
    {
        var cfgJson = config?.ToJson() ?? "{}";
        var handle = Native.moofile_open(path, cfgJson, IntPtr.Zero);
        if (handle == IntPtr.Zero)
            throw new InvalidOperationException("moofile_open failed");
        return new Collection(handle);
    }

    private Collection(IntPtr handle) { _handle = handle; }

    public void Dispose()
    {
        if (!_disposed && _handle != IntPtr.Zero)
        {
            Native.moofile_close(_handle, IntPtr.Zero);
            _handle = IntPtr.Zero;
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }

    ~Collection() => Dispose();

    private string? ReadStr(IntPtr p)
    {
        if (p == IntPtr.Zero) return null;
        var s = Marshal.PtrToStringUTF8(p);
        Native.moofile_free_string(p);
        return s;
    }

    private List<Document> DrainCursor(IntPtr cursor)
    {
        var docs = new List<Document>();
        while (true)
        {
            var s = Native.moofile_cursor_next(cursor, IntPtr.Zero);
            if (s == IntPtr.Zero) break;
            var json = Marshal.PtrToStringUTF8(s);
            Native.moofile_free_string(s);
            if (json != null) docs.Add(Document.Parse(json));
        }
        Native.moofile_cursor_free(cursor);
        return docs;
    }

    private List<SearchResult> DrainSearch(IntPtr cursor)
    {
        var results = new List<SearchResult>();
        while (true)
        {
            var s = Native.moofile_search_cursor_next(cursor, IntPtr.Zero);
            if (s == IntPtr.Zero) break;
            var json = Marshal.PtrToStringUTF8(s);
            Native.moofile_free_string(s);
            if (json == null || json.Length < 2) continue;

            // Parse [doc, score]
            var inner = json.Substring(1, json.Length - 2);
            var split = FindTopLevelComma(inner);
            if (split >= 0)
            {
                var docStr = inner.Substring(0, split).Trim();
                var scoreStr = inner.Substring(split + 1).Trim();
                results.Add(new SearchResult
                {
                    Doc = Document.Parse(docStr),
                    Score = double.Parse(scoreStr)
                });
            }
        }
        Native.moofile_search_cursor_free(cursor);
        return results;
    }

    private static int FindTopLevelComma(string s)
    {
        int depth = 0;
        for (int i = 0; i < s.Length; i++)
        {
            if (s[i] == '{') depth++;
            else if (s[i] == '}') depth--;
            else if (s[i] == ',' && depth == 0) return i;
        }
        return -1;
    }

    // ----------------------------------------------------------
    // Insert
    // ----------------------------------------------------------

    public Document Insert(Document doc)
    {
        var result = Native.moofile_insert(_handle, doc.ToJson(), IntPtr.Zero);
        var json = ReadStr(result);
        return json != null ? Document.Parse(json) : doc;
    }

    public List<Document> InsertMany(List<Document> docs)
    {
        var json = "[" + string.Join(",", docs.ConvertAll(d => d.ToJson())) + "]";
        var result = Native.moofile_insert_many(_handle, json, IntPtr.Zero);
        var s = ReadStr(result);
        if (s == null) return docs;
        return JsonSerializer.Deserialize<List<Dictionary<string, object?>>>(s)
            ?.ConvertAll(d => new Document(d)) ?? docs;
    }

    // ----------------------------------------------------------
    // Query
    // ----------------------------------------------------------

    public List<Document> Find(Document? filter = null)
    {
        var f = filter?.ToJson() ?? "{}";
        var cursor = Native.moofile_find(_handle, f, IntPtr.Zero);
        return cursor != IntPtr.Zero ? DrainCursor(cursor) : new();
    }

    public Document? FindOne(Document? filter = null)
    {
        var f = filter?.ToJson() ?? "{}";
        var result = Native.moofile_find_one(_handle, f, IntPtr.Zero);
        var json = ReadStr(result);
        return json != null ? Document.Parse(json) : null;
    }

    public long Count(Document? filter = null)
    {
        var f = filter?.ToJson() ?? "{}";
        return Native.moofile_count(_handle, f, IntPtr.Zero);
    }

    public bool Exists(Document filter)
    {
        return Native.moofile_exists(_handle, filter.ToJson(), IntPtr.Zero) == 1;
    }

    // ----------------------------------------------------------
    // Update
    // ----------------------------------------------------------

    public bool UpdateOne(Document where, Document? setValues = null,
                          List<string>? unsetFields = null, Document? incValues = null)
    {
        var update = new Document();
        if (setValues is { Count: > 0 }) update["set"] = setValues;
        if (unsetFields is { Count: > 0 }) update["unset"] = unsetFields;
        if (incValues is { Count: > 0 }) update["inc"] = incValues;
        return Native.moofile_update_one(_handle, where.ToJson(), update.ToJson(), IntPtr.Zero) == 1;
    }

    public long UpdateMany(Document where, Document? setValues = null,
                           List<string>? unsetFields = null, Document? incValues = null)
    {
        var update = new Document();
        if (setValues is { Count: > 0 }) update["set"] = setValues;
        if (unsetFields is { Count: > 0 }) update["unset"] = unsetFields;
        if (incValues is { Count: > 0 }) update["inc"] = incValues;
        return Native.moofile_update_many(_handle, where.ToJson(), update.ToJson(), IntPtr.Zero);
    }

    public bool ReplaceOne(Document where, Document replacement)
    {
        return Native.moofile_replace_one(_handle, where.ToJson(), replacement.ToJson(), IntPtr.Zero) == 1;
    }

    // ----------------------------------------------------------
    // Delete
    // ----------------------------------------------------------

    public bool DeleteOne(Document where)
    {
        return Native.moofile_delete_one(_handle, where.ToJson(), IntPtr.Zero) == 1;
    }

    public long DeleteMany(Document where)
    {
        return Native.moofile_delete_many(_handle, where.ToJson(), IntPtr.Zero);
    }

    // ----------------------------------------------------------
    // Search
    // ----------------------------------------------------------

    public List<SearchResult> VectorSearch(string field, List<double> queryVector,
                                           int limit = 10, Document? filter = null)
    {
        var f = filter?.ToJson() ?? "{}";
        var vec = JsonSerializer.Serialize(queryVector);
        var cursor = Native.moofile_vector_search(_handle, f, field, vec, limit, IntPtr.Zero);
        return DrainSearch(cursor);
    }

    public List<SearchResult> TextSearch(string field, string query,
                                         int limit = 10, Document? filter = null)
    {
        var f = filter?.ToJson() ?? "{}";
        var cursor = Native.moofile_text_search(_handle, f, field, query, limit, IntPtr.Zero);
        return DrainSearch(cursor);
    }

    public List<SearchResult> HybridSearch(string textField, string vectorField,
                                           string queryText, List<double>? queryVector = null,
                                           int limit = 10, Document? filter = null)
    {
        var f = filter?.ToJson() ?? "{}";
        var qv = queryVector != null ? JsonSerializer.Serialize(queryVector) : null;
        var cursor = Native.moofile_hybrid_search(_handle, f, textField, vectorField,
            queryText, qv!, limit, IntPtr.Zero);
        return DrainSearch(cursor);
    }

    public List<SearchResult> Semantic(string sourceField, string queryText,
                                       int limit = 10, Document? filter = null)
    {
        var f = filter?.ToJson() ?? "{}";
        var cursor = Native.moofile_semantic_search(_handle, f, sourceField, queryText, limit, IntPtr.Zero);
        return DrainSearch(cursor);
    }

    // ----------------------------------------------------------
    // Batch
    // ----------------------------------------------------------

    public void BatchBegin() => Native.moofile_batch_begin(_handle, IntPtr.Zero);
    public void BatchCommit() => Native.moofile_batch_commit(_handle, IntPtr.Zero);
    public void BatchRollback() => Native.moofile_batch_rollback(_handle, IntPtr.Zero);

    public void Batch(Action fn)
    {
        BatchBegin();
        try { fn(); BatchCommit(); }
        catch { BatchRollback(); throw; }
    }

    // ----------------------------------------------------------
    // Utility
    // ----------------------------------------------------------

    public Document Stats()
    {
        var result = Native.moofile_stats(_handle, IntPtr.Zero);
        var json = ReadStr(result);
        return json != null ? Document.Parse(json) : new();
    }

    public void Compact()  { Native.moofile_compact(_handle, IntPtr.Zero); }
    public void Sync()     { Native.moofile_sync(_handle, IntPtr.Zero); }
    public void Reindex()  { Native.moofile_reindex(_handle, IntPtr.Zero); }
}
