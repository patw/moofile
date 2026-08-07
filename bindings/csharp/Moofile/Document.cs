using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Moofile;

/// <summary>Raised for every MooFile failure.</summary>
public class MooFileException : Exception
{
    public MooFileException(string message) : base(message) { }
    public MooFileException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// A MooFile document — an insertion-ordered map of field names to values.
/// </summary>
/// <remarks>
/// Values read back from the database are plain CLR types (string, long,
/// double, bool, null, <see cref="List{T}"/>, nested <see cref="Document"/>)
/// rather than <c>JsonElement</c>, so <c>doc["age"]</c> compares and casts the
/// way you would expect.
/// </remarks>
public class Document : IEnumerable<KeyValuePair<string, object?>>
{
    private readonly Dictionary<string, object?> _data = new();
    private readonly List<string> _order = new();

    public Document() { }

    public Document(IDictionary<string, object?> source)
    {
        foreach (var kvp in source) this[kvp.Key] = kvp.Value;
    }

    /// <summary>Build a document from alternating key/value arguments.</summary>
    public static Document Of(params object?[] keyValuePairs)
    {
        if (keyValuePairs.Length % 2 != 0)
            throw new ArgumentException("expected alternating key/value arguments");

        var d = new Document();
        for (int i = 0; i < keyValuePairs.Length; i += 2)
        {
            d[Convert.ToString(keyValuePairs[i]) ?? ""] = keyValuePairs[i + 1];
        }
        return d;
    }

    public object? this[string key]
    {
        get => _data.TryGetValue(key, out var v) ? v : null;
        set
        {
            if (!_data.ContainsKey(key)) _order.Add(key);
            _data[key] = value;
        }
    }

    /// <summary>Set a field and return this, for chaining.</summary>
    public Document Set(string key, object? value)
    {
        this[key] = value;
        return this;
    }

    public bool ContainsKey(string key) => _data.ContainsKey(key);

    public bool Remove(string key)
    {
        _order.Remove(key);
        return _data.Remove(key);
    }

    public int Count => _data.Count;
    public IReadOnlyList<string> Keys => _order;

    /// <summary>The document's <c>_id</c>, or null before insertion.</summary>
    public string? Id => GetString("_id");

    public string? GetString(string key) => this[key]?.ToString();

    public long GetLong(string key) => this[key] switch
    {
        long l => l,
        int i => i,
        double d => (long)d,
        _ => throw new MooFileException($"field '{key}' is not a number: {this[key]}"),
    };

    public double GetDouble(string key) => this[key] switch
    {
        double d => d,
        long l => l,
        int i => i,
        _ => throw new MooFileException($"field '{key}' is not a number: {this[key]}"),
    };

    public bool GetBoolean(string key) => this[key] is true;

    public List<object?>? GetList(string key) => this[key] as List<object?>;

    public Document? GetDocument(string key) => this[key] as Document;

    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
    {
        foreach (var key in _order) yield return new(key, _data[key]);
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // -----------------------------------------------------------------
    // JSON
    // -----------------------------------------------------------------

    public string ToJson()
    {
        var node = ToNode(this);
        return node?.ToJsonString() ?? "{}";
    }

    /// <summary>Convert any supported value into a JsonNode tree.</summary>
    internal static JsonNode? ToNode(object? value)
    {
        switch (value)
        {
            case null:
                return null;
            case Document doc:
            {
                var obj = new JsonObject();
                foreach (var (key, v) in doc) obj[key] = ToNode(v);
                return obj;
            }
            case IDictionary<string, object?> map:
            {
                var obj = new JsonObject();
                foreach (var kvp in map) obj[kvp.Key] = ToNode(kvp.Value);
                return obj;
            }
            case string s:
                return JsonValue.Create(s);
            case bool b:
                return JsonValue.Create(b);
            case int or long or short or byte:
                return JsonValue.Create(Convert.ToInt64(value));
            case float or double or decimal:
                return JsonValue.Create(Convert.ToDouble(value));
            case JsonNode node:
                return node.DeepClone();
            case IEnumerable seq:
            {
                var arr = new JsonArray();
                foreach (var item in seq) arr.Add(ToNode(item));
                return arr;
            }
            default:
                return JsonValue.Create(value.ToString());
        }
    }

    /// <summary>Parse a JSON object into a document.</summary>
    public static Document Parse(string json)
    {
        JsonNode? node;
        try
        {
            node = JsonNode.Parse(json);
        }
        catch (JsonException e)
        {
            throw new MooFileException($"invalid JSON: {e.Message}", e);
        }

        if (node is not JsonObject obj)
            throw new MooFileException("expected a JSON object");

        return (Document)FromNode(obj)!;
    }

    /// <summary>
    /// Convert a JsonNode tree into plain CLR values. Numbers become long
    /// when they are integral and double otherwise, so scores stay doubles
    /// while counts stay integers.
    /// </summary>
    internal static object? FromNode(JsonNode? node)
    {
        switch (node)
        {
            case null:
                return null;
            case JsonObject obj:
            {
                var doc = new Document();
                foreach (var kvp in obj) doc[kvp.Key] = FromNode(kvp.Value);
                return doc;
            }
            case JsonArray arr:
            {
                var list = new List<object?>(arr.Count);
                foreach (var item in arr) list.Add(FromNode(item));
                return list;
            }
            case JsonValue val:
            {
                if (val.TryGetValue<bool>(out var b)) return b;
                if (val.TryGetValue<string>(out var s)) return s;
                if (val.TryGetValue<long>(out var l)) return l;
                if (val.TryGetValue<double>(out var d)) return d;
                return val.ToJsonString();
            }
            default:
                return null;
        }
    }

    public override string ToString() => ToJson();
}

/// <summary>A document paired with its similarity or relevance score.</summary>
public class SearchResult
{
    public Document Doc { get; init; } = new();
    public double Score { get; init; }

    public override string ToString() => $"{Score}: {Doc}";
}
