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
///
/// Usage:
///   using Moofile;
///
///   var db = Collection.Open("data.bson", new Config {
///       Indexes = new[] { "email" }
///   });
///
///   db.Insert(new Document { ["name"] = "Alice" });
///   var results = db.Find(new Document { ["age"] = new Document { ["$gt"] = 25 } });
///   db.Close();
/// </summary>

// ---------------------------------------------------------------------------
// Native methods (P/Invoke)
// ---------------------------------------------------------------------------

internal static class Native
{
    private const string LibName = "moofile";

    // Lifecycle
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_open(string path, string configJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_close(IntPtr handle, out IntPtr errOut);

    // Insert
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_insert(IntPtr handle, string docJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_insert_many(IntPtr handle, string docsJson, out IntPtr errOut);

    // Query
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_find(IntPtr handle, string filterJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_find_one(IntPtr handle, string filterJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long moofile_count(IntPtr handle, string filterJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_exists(IntPtr handle, string filterJson, out IntPtr errOut);

    // Cursor
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_cursor_next(IntPtr cursor, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void moofile_cursor_free(IntPtr cursor);

    // Update
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_update_one(IntPtr handle, string whereJson, string updateJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long moofile_update_many(IntPtr handle, string whereJson, string updateJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_replace_one(IntPtr handle, string whereJson, string replJson, out IntPtr errOut);

    // Delete
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_delete_one(IntPtr handle, string whereJson, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long moofile_delete_many(IntPtr handle, string whereJson, out IntPtr errOut);

    // Search
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_vector_search(IntPtr handle, string filterJson, string field, string vecJson, int limit, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_text_search(IntPtr handle, string filterJson, string field, string query, int limit, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_hybrid_search(IntPtr handle, string filterJson, string tf, string vf, string qt, string qv, int limit, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_semantic_search(IntPtr handle, string filterJson, string field, string query, int limit, out IntPtr errOut);

    // Search cursor
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_search_cursor_next(IntPtr cursor, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void moofile_search_cursor_free(IntPtr cursor);

    // Batch
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_batch_begin(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_batch_commit(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_batch_rollback(IntPtr handle, out IntPtr errOut);

    // Utility
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr moofile_stats(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_compact(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_sync(IntPtr handle, out IntPtr errOut);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int moofile_reindex(IntPtr handle, out IntPtr errOut);

    // Memory
    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void moofile_free_string(IntPtr s);

    // Library load helper
    internal static void EnsureLoaded()
    {
        // P/Invoke resolves automatically through NuGet runtimes/ folder.
        // If not found, try the default build path.
        try
        {
            // Force load check
            moofile_open("", "{}", out _);
        }
        catch (DllNotFoundException)
        {
            var candidates = new[]
            {
                "../target/release/libmoofile.so",
                "../target/debug/libmoofile.so",
                "../../target/release/libmoofile.so",
            };
            foreach (var c in candidates)
            {
                var full = Path.GetFullPath(c);
                if (File.Exists(full))
                {
                    NativeLibrary.Load(full);
                    return;
                }
            }
            throw;
        }
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// <summary>Configuration for a single auto-embedding source field.</summary>
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

/// <summary>Configuration for opening a MooFile collection.</summary>
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
                    ["model"] = v.Model,
                    ["target"] = v.Target,
                    ["dims"] = v.Dims,
                    ["precision"] = v.Precision,
                    ["normalize"] = v.Normalize,
                    ["query_prefix"] = v.QueryPrefix,
                    ["doc_prefix"] = v.DocPrefix,
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
// Document (JSON wrapper)
// ---------------------------------------------------------------------------

/// <summary>A BSON document, exposed as a dictionary with JSON serialization.</summary>
public class Document : Dictionary<string, object?>
{
    public Document() { }
    public Document(IDictionary<string, object?> d) : base(d) { }

    public string ToJson() => JsonSerializer.Serialize(this, JsonOptions);
    public static Document Parse(string json) => JsonSerializer.Deserialize<Document>(json, JsonOptions) ?? new();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
    };
}

// ---------------------------------------------------------------------------
// SearchResult
// ---------------------------------------------------------------------------

/// <summary>A search result containing a document and a similarity score.</summary>
public class SearchResult
{
    public Document Doc { get; init; } = new();
    public double Score { get; init; }
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/// <summary>
/// RAII wrapper for a MooFile collection handle.  Implements IDisposable.
/// </summary>
public class Collection : IDisposable
{
    private IntPtr _handle;
    private readonly string _path;
    private bool _disposed;

    static Collection()
    {
        Native.EnsureLoaded();
    }

    /// <summary>Open a MooFile collection.</summary>
    public static Collection Open(string path, Config? config = null)
    {
        var cfgJson = config?.ToJson() ?? "{}";
        var handle = Native.moofile_open(path, cfgJson, out var err);
        CheckError(err);
        if (handle == IntPtr.Zero)
            throw new InvalidOperationException("moofile_open returned null");
        return new Collection(handle, path);
    }

    private Collection(IntPtr handle, string path)
    {
        _handle = handle;
        _path = path;
    }

    public void Dispose()
    {
        if (!_disposed && _handle != IntPtr.Zero)
        {
            Native.moofile_close(_handle, out _);
            _handle = IntPtr.Zero;
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }

    ~Collection() => Dispose();

    // ----------------------------------------------------------
    // Insert
    // ----------------------------------------------------------

    public Document Insert(Document doc)
    {
        var result = CallStr(h => Native.moofile_insert(h, doc.ToJson(), out _));
        return result != null ? Document.Parse(result) : doc;
    }

    public List<Document> InsertMany(List<Document> docs)
    {
        var json = "[" + string.Join(",", docs.ConvertAll(d => d.ToJson())) + "]";
        var result = CallStr(h => Native.moofile_insert_many(h, json, out _));
        if (result == null) return docs;
        return Document.Parse(result).Select(kvp => new Document { ["value"] = kvp.Value }).ToList()
            ?? docs;
    }

    // ----------------------------------------------------------
    // Query
    // ----------------------------------------------------------

    public List<Document> Find(Document? filter = null)
    {
        var cursor = CallPtr(h => Native.moofile_find(h, (filter ?? new()).ToJson(), out _));
        if (cursor == IntPtr.Zero) return new();

        var docs = new List<Document>();
        while (true)
        {
            var s = CallStrPtr(cursor, Native.moofile_cursor_next);
            if (s == null) break;
            docs.Add(Document.Parse(s));
        }
        Native.moofile_cursor_free(cursor);
        return docs;
    }

    public Document? FindOne(Document? filter = null)
    {
        var result = CallStr(h => Native.moofile_find_one(h, (filter ?? new()).ToJson(), out _));
        return result != null ? Document.Parse(result) : null;
    }

    public long Count(Document? filter = null)
    {
        return CallLong(h => Native.moofile_count(h, (filter ?? new()).ToJson(), out _));
    }

    public bool Exists(Document filter)
    {
        return CallInt(h => Native.moofile_exists(h, filter.ToJson(), out _)) == 1;
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
        return CallInt(h => Native.moofile_update_one(h, where.ToJson(), update.ToJson(), out _)) == 1;
    }

    public long UpdateMany(Document where, Document? setValues = null,
                           List<string>? unsetFields = null, Document? incValues = null)
    {
        var update = new Document();
        if (setValues is { Count: > 0 }) update["set"] = setValues;
        if (unsetFields is { Count: > 0 }) update["unset"] = unsetFields;
        if (incValues is { Count: > 0 }) update["inc"] = incValues;
        return CallLong(h => Native.moofile_update_many(h, where.ToJson(), update.ToJson(), out _));
    }

    public bool ReplaceOne(Document where, Document replacement)
    {
        return CallInt(h => Native.moofile_replace_one(h, where.ToJson(), replacement.ToJson(), out _)) == 1;
    }

    // ----------------------------------------------------------
    // Delete
    // ----------------------------------------------------------

    public bool DeleteOne(Document where)
    {
        return CallInt(h => Native.moofile_delete_one(h, where.ToJson(), out _)) == 1;
    }

    public long DeleteMany(Document where)
    {
        return CallLong(h => Native.moofile_delete_many(h, where.ToJson(), out _));
    }

    // ----------------------------------------------------------
    // Search
    // ----------------------------------------------------------

    public List<SearchResult> VectorSearch(string field, List<double> queryVector,
                                           int limit = 10, Document? filter = null)
    {
        var vecJson = JsonSerializer.Serialize(queryVector);
        var cursor = CallPtr(h => Native.moofile_vector_search(h,
            (filter ?? new()).ToJson(), field, vecJson, limit, out _));
        return DrainSearchCursor(cursor);
    }

    public List<SearchResult> TextSearch(string field, string query,
                                         int limit = 10, Document? filter = null)
    {
        var cursor = CallPtr(h => Native.moofile_text_search(h,
            (filter ?? new()).ToJson(), field, query, limit, out _));
        return DrainSearchCursor(cursor);
    }

    public List<SearchResult> HybridSearch(string textField, string vectorField,
                                           string queryText, List<double>? queryVector = null,
                                           int limit = 10, Document? filter = null)
    {
        var qvJson = queryVector != null ? JsonSerializer.Serialize(queryVector) : null;
        var cursor = CallPtr(h => Native.moofile_hybrid_search(h,
            (filter ?? new()).ToJson(), textField, vectorField, queryText, qvJson, limit, out _));
        return DrainSearchCursor(cursor);
    }

    public List<SearchResult> Semantic(string sourceField, string queryText,
                                       int limit = 10, Document? filter = null)
    {
        var cursor = CallPtr(h => Native.moofile_semantic_search(h,
            (filter ?? new()).ToJson(), sourceField, queryText, limit, out _));
        return DrainSearchCursor(cursor);
    }

    private List<SearchResult> DrainSearchCursor(IntPtr cursor)
    {
        if (cursor == IntPtr.Zero) return new();
        var results = new List<SearchResult>();
        while (true)
        {
            var s = CallStrPtr(cursor, Native.moofile_search_cursor_next);
            if (s == null) break;

            // Parse [doc, score]
            if (s.Length > 2 && s[0] == '[' && s[^1] == ']')
            {
                var inner = s[1..^1];
                var split = FindTopLevelComma(inner);
                if (split >= 0)
                {
                    var docStr = inner[..split].Trim();
                    var scoreStr = inner[(split + 1)..].Trim();
                    results.Add(new SearchResult
                    {
                        Doc = Document.Parse(docStr),
                        Score = double.Parse(scoreStr)
                    });
                }
            }
        }
        Native.moofile_search_cursor_free(cursor);
        return results;
    }

    // ----------------------------------------------------------
    // Batch
    // ----------------------------------------------------------

    public void BatchBegin()
    {
        CheckError(Native.moofile_batch_begin(_handle, out _));
    }

    public void BatchCommit()
    {
        CheckError(Native.moofile_batch_commit(_handle, out _));
    }

    public void BatchRollback()
    {
        Native.moofile_batch_rollback(_handle, out _);
    }

    /// <summary>Execute actions atomically; rolls back on exception.</summary>
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
        var result = CallStr(h => Native.moofile_stats(h, out _));
        return result != null ? Document.Parse(result) : new();
    }

    public void Compact()
    {
        CheckError(Native.moofile_compact(_handle, out _));
    }

    public void Sync()
    {
        CheckError(Native.moofile_sync(_handle, out _));
    }

    public void Reindex()
    {
        CheckError(Native.moofile_reindex(_handle, out _));
    }

    // ----------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------

    private static void CheckError(int code)
    {
        if (code != 0) throw new InvalidOperationException($"MooFile error code: {code}");
    }

    private static void CheckError(IntPtr errPtr)
    {
        if (errPtr != IntPtr.Zero)
        {
            var msg = Marshal.PtrToStringUTF8(errPtr) ?? "unknown error";
            Native.moofile_free_string(errPtr);
            throw new InvalidOperationException(msg);
        }
    }

    private string? CallStr(Func<IntPtr, IntPtr> fn)
    {
        if (_handle == IntPtr.Zero) throw new ObjectDisposedException(nameof(Collection));
        var result = fn(_handle);
        CheckError(result);
        if (result == IntPtr.Zero) return null;
        var s = Marshal.PtrToStringUTF8(result);
        Native.moofile_free_string(result);
        return s;
    }

    private IntPtr CallPtr(Func<IntPtr, IntPtr> fn)
    {
        if (_handle == IntPtr.Zero) throw new ObjectDisposedException(nameof(Collection));
        var result = fn(_handle);
        CheckError(result);
        return result;
    }

    private long CallLong(Func<IntPtr, long> fn)
    {
        if (_handle == IntPtr.Zero) throw new ObjectDisposedException(nameof(Collection));
        return fn(_handle);
    }

    private int CallInt(Func<IntPtr, int> fn)
    {
        if (_handle == IntPtr.Zero) throw new ObjectDisposedException(nameof(Collection));
        return fn(_handle);
    }

    private static string? CallStrPtr(IntPtr cursor, Func<IntPtr, IntPtr, IntPtr> fn)
    {
        if (cursor == IntPtr.Zero) return null;
        var result = fn(cursor, out _);
        if (result == IntPtr.Zero) return null;
        var s = Marshal.PtrToStringUTF8(result);
        Native.moofile_free_string(result);
        return s;
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
}
