using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Moofile;

/// <summary>Auto-embedding configuration for one source text field.</summary>
public class AutoEmbedConfig
{
    /// <summary>GGUF model URI, e.g. "hf:user/repo:file.gguf". Required.</summary>
    public string Model { get; set; } = "";

    /// <summary>Vector field the embedding is written to. Required.</summary>
    public string Target { get; set; } = "";

    public int Dims { get; set; } = 1024;

    /// <summary>"f32", "int8", "uint8", or "binary".</summary>
    public string Precision { get; set; } = "int8";

    public bool Normalize { get; set; } = true;

    public string QueryPrefix { get; set; } =
        "Represent the query for retrieving supporting documents: ";

    public string DocPrefix { get; set; } =
        "Represent the document for retrieval: ";

    internal JsonObject ToNode() => new()
    {
        ["model"] = Model,
        ["target"] = Target,
        ["dims"] = Dims,
        ["precision"] = Precision,
        ["normalize"] = Normalize,
        ["query_prefix"] = QueryPrefix,
        ["doc_prefix"] = DocPrefix,
    };
}

/// <summary>Configuration for opening a MooFile collection.</summary>
public class Config
{
    /// <summary>Fields to build a B-tree index over.</summary>
    public string[]? Indexes { get; set; }

    /// <summary>Vector fields and their dimensions.</summary>
    public Dictionary<string, int>? VectorIndexes { get; set; }

    /// <summary>Fields to build a BM25 text index over.</summary>
    public string[]? TextIndexes { get; set; }

    /// <summary>Source text fields to embed on insert, keyed by field name.</summary>
    public Dictionary<string, AutoEmbedConfig>? AutoEmbed { get; set; }

    public bool Readonly { get; set; }

    /// <summary>"none", "os" (default), or "fsync".</summary>
    public string Durability { get; set; } = "os";

    /// <summary>Where downloaded models are cached. Defaults to ~/.cache/moofile/models/.</summary>
    public string? ModelCacheDir { get; set; }

    internal string ToJson()
    {
        var obj = new JsonObject();

        if (Indexes is { Length: > 0 })
        {
            var arr = new JsonArray();
            foreach (var i in Indexes) arr.Add(i);
            obj["indexes"] = arr;
        }

        if (VectorIndexes is { Count: > 0 })
        {
            var vi = new JsonObject();
            foreach (var (field, dims) in VectorIndexes) vi[field] = dims;
            obj["vector_indexes"] = vi;
        }

        if (TextIndexes is { Length: > 0 })
        {
            var arr = new JsonArray();
            foreach (var t in TextIndexes) arr.Add(t);
            obj["text_indexes"] = arr;
        }

        if (AutoEmbed is { Count: > 0 })
        {
            var ae = new JsonObject();
            foreach (var (field, cfg) in AutoEmbed) ae[field] = cfg.ToNode();
            obj["auto_embed"] = ae;
        }

        if (Readonly) obj["readonly"] = true;
        obj["durability"] = Durability;
        if (ModelCacheDir != null) obj["model_cache_dir"] = ModelCacheDir;

        return obj.ToJsonString();
    }
}

/// <summary>
/// Query-builder options for <see cref="Collection.Find(Document?, FindOptions?)"/>.
/// </summary>
/// <remarks>
/// Stages apply in the order: filter → group/agg → sort → skip → limit.
/// <code>
/// db.Find(Document.Of("active", true),
///         FindOptions.Create().Sort("age", desc: true).Limit(10));
///
/// db.Find(null, FindOptions.Create()
///     .Group("dept").Count().Sum("pay").Sort("dept"));
/// </code>
/// </remarks>
public class FindOptions
{
    private string? _sortField;
    private bool _sortDesc;
    private int _skip;
    private int _limit = -1;
    private string? _group;
    private readonly List<(string Func, string? Field)> _aggs = new();

    public static FindOptions Create() => new();

    /// <summary>Sort by a field, descending when <paramref name="desc"/> is true.</summary>
    public FindOptions Sort(string field, bool desc = false)
    {
        _sortField = field;
        _sortDesc = desc;
        return this;
    }

    public FindOptions Skip(int n)  { _skip = n; return this; }
    public FindOptions Limit(int n) { _limit = n; return this; }

    /// <summary>Group by a field; combine with the aggregations below.</summary>
    public FindOptions Group(string field) { _group = field; return this; }

    /// <summary>Documents per group; the output field is <c>count</c>.</summary>
    public FindOptions Count() => Agg("count", null);

    /// <summary>Sum per group; the output field is <c>sum_&lt;field&gt;</c>.</summary>
    public FindOptions Sum(string field) => Agg("sum", field);

    /// <summary>Average per group; the output field is <c>mean_&lt;field&gt;</c>.</summary>
    public FindOptions Mean(string field) => Agg("mean", field);

    /// <summary>Minimum per group; the output field is <c>min_&lt;field&gt;</c>.</summary>
    public FindOptions Min(string field) => Agg("min", field);

    /// <summary>Maximum per group; the output field is <c>max_&lt;field&gt;</c>.</summary>
    public FindOptions Max(string field) => Agg("max", field);

    /// <summary>All values per group; the output field is <c>collect_&lt;field&gt;</c>.</summary>
    public FindOptions Collect(string field) => Agg("collect", field);

    /// <summary>First value per group; the output field is <c>first_&lt;field&gt;</c>.</summary>
    public FindOptions First(string field) => Agg("first", field);

    /// <summary>Last value per group; the output field is <c>last_&lt;field&gt;</c>.</summary>
    public FindOptions Last(string field) => Agg("last", field);

    /// <summary>
    /// Add an aggregation by name. Prefer the named methods; this exists for
    /// functions added to the core after this binding was written.
    /// </summary>
    public FindOptions Agg(string func, string? field)
    {
        _aggs.Add((func, field));
        return this;
    }

    /// <summary>True when nothing is set, so Find can take the plain path.</summary>
    internal bool IsEmpty =>
        _sortField is null && _skip == 0 && _limit < 0 && _group is null && _aggs.Count == 0;

    internal string ToJson()
    {
        var obj = new JsonObject();

        if (_sortField is not null)
        {
            obj["sort"] = new JsonObject { ["field"] = _sortField, ["desc"] = _sortDesc };
        }
        if (_skip > 0)   obj["skip"] = _skip;
        if (_limit >= 0) obj["limit"] = _limit;
        if (_group is not null) obj["group"] = _group;

        if (_aggs.Count > 0)
        {
            var arr = new JsonArray();
            foreach (var (func, field) in _aggs)
            {
                var entry = new JsonObject { ["func"] = func };
                if (field is not null) entry["field"] = field;
                arr.Add(entry);
            }
            obj["agg"] = arr;
        }

        return obj.ToJsonString();
    }
}
