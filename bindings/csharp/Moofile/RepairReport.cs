using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Moofile;

/// <summary>A span of bytes a repair could not parse and dropped.</summary>
/// <param name="Offset">Byte offset where the damage starts.</param>
/// <param name="Length">Number of bytes dropped.</param>
/// <param name="ToEndOfFile">
/// Whether the damage ran to the end of the file — i.e. this was a truncation
/// rather than a skipped-over hole.
/// </param>
public readonly record struct RepairGap(long Offset, long Length, bool ToEndOfFile);

/// <summary>What a <see cref="Collection.Repair(string)"/> pass did.</summary>
/// <remarks>
/// A repair keeps every record that still decodes and drops the byte spans
/// that do not — resynchronising past damage where an intact record follows it
/// and truncating where none does.  <see cref="Gaps"/> lists what was dropped,
/// so a caller can log or alert on data loss rather than discover it later.
/// </remarks>
public sealed class RepairReport
{
    /// <summary>Records that decoded and were preserved.</summary>
    public long RecordsKept { get; init; }

    /// <summary>Bytes of intact records preserved.</summary>
    public long BytesKept { get; init; }

    /// <summary>Bytes of unparseable data dropped.</summary>
    public long BytesDropped { get; init; }

    /// <summary>False when the file was already intact and was left untouched.</summary>
    public bool Rewritten { get; init; }

    /// <summary>Every damaged span, in file order.</summary>
    public IReadOnlyList<RepairGap> Gaps { get; init; } = Array.Empty<RepairGap>();

    /// <summary>Whether any damage was found.</summary>
    public bool IsDamaged => Gaps.Count > 0;

    /// <summary>Parse the JSON report the C layer returns.</summary>
    internal static RepairReport FromJson(string json)
    {
        var node = JsonNode.Parse(json)?.AsObject()
            ?? throw new MooFileException("repair returned a malformed report");

        var gaps = new List<RepairGap>();
        if (node["gaps"] is JsonArray arr)
        {
            foreach (var g in arr)
            {
                if (g is not JsonObject o) continue;
                gaps.Add(new RepairGap(
                    o["offset"]?.GetValue<long>() ?? 0,
                    o["length"]?.GetValue<long>() ?? 0,
                    o["to_end_of_file"]?.GetValue<bool>() ?? false));
            }
        }

        return new RepairReport
        {
            RecordsKept = node["records_kept"]?.GetValue<long>() ?? 0,
            BytesKept = node["bytes_kept"]?.GetValue<long>() ?? 0,
            BytesDropped = node["bytes_dropped"]?.GetValue<long>() ?? 0,
            Rewritten = node["rewritten"]?.GetValue<bool>() ?? false,
            Gaps = gaps,
        };
    }
}
