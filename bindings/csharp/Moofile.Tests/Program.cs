using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Moofile;

namespace Moofile.Tests;

/// <summary>
/// MooFile C# binding test suite.
///
/// Plain console-driven rather than xunit so it runs with nothing but the
/// .NET SDK: <c>dotnet run --project Moofile.Tests</c>.
/// </summary>
public static class Program
{
    private static int _testsRun;
    private static int _testsFailed;
    private static string _currentTest = "";
    private static string _tempDir = "";

    private sealed class Tag
    {
        public string? Label { get; init; }
    }

    private sealed class VectorRecord
    {
        // Field names are the selected property names in Phase 1.
        public string? kind { get; init; }
    }

    private sealed class Person
    {
        public string? Name { get; init; }
        public int Age { get; init; }
        public DateTime? Birthday { get; init; }
        public string? Status { get; init; }
        public List<Tag> Tags { get; init; } = new();
    }

    // -----------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------

    private static void Test(string name)
    {
        _currentTest = name;
        _testsRun++;
    }

    private static void Check(bool cond, string msg)
    {
        if (!cond)
        {
            Console.Error.WriteLine($"  FAIL [{_currentTest}] {msg}");
            _testsFailed++;
        }
    }

    private static void CheckEquals(object? actual, object? expected, string msg)
    {
        if (!Equals(actual, expected))
        {
            Console.Error.WriteLine(
                $"  FAIL [{_currentTest}] {msg}: expected {expected}, got {actual}");
            _testsFailed++;
        }
    }

    private static void CheckThrows<T>(Action action, string msg) where T : Exception
    {
        try
        {
            action();
            Console.Error.WriteLine($"  FAIL [{_currentTest}] {msg}: no exception thrown");
            _testsFailed++;
        }
        catch (T)
        {
            // expected
        }
    }

    private static string Path(string name) => System.IO.Path.Combine(_tempDir, name);

    private static List<string?> Ids(List<Document> docs) => docs.Select(d => d.Id).ToList();

    /// <summary>Four documents across two departments, out of age order.</summary>
    private static Collection Sortable(string file)
    {
        var db = Collection.Open(Path(file));
        db.InsertMany(new[]
        {
            Document.Of("_id", "a", "age", 30, "dept", "eng", "pay", 100),
            Document.Of("_id", "b", "age", 20, "dept", "eng", "pay", 200),
            Document.Of("_id", "c", "age", 50, "dept", "ops", "pay", 300),
            Document.Of("_id", "d", "age", 40, "dept", "ops", "pay", 400),
        });
        return db;
    }

    // -----------------------------------------------------------------
    // Document / JSON
    // -----------------------------------------------------------------

    private static void TestDocumentRoundTrip()
    {
        Test("document round-trips nested structures");
        var doc = Document.Of(
            "s", "hi", "i", 42, "f", 3.5, "b", true,
            "arr", new List<object?> { 1L, 2L, 3L },
            "obj", Document.Of("k", "v"));

        var parsed = Document.Parse(doc.ToJson());
        CheckEquals(parsed.GetString("s"), "hi", "string");
        CheckEquals(parsed.GetLong("i"), 42L, "integer");
        CheckEquals(parsed.GetDouble("f"), 3.5, "real");
        Check(parsed.GetBoolean("b"), "boolean");
        CheckEquals(parsed.GetList("arr")?.Count, 3, "array length");
        CheckEquals(parsed.GetDocument("obj")?.GetString("k"), "v", "nested object");
    }

    private static void TestDocumentValuesArePlainTypes()
    {
        // Deserialising to object would leave JsonElement values behind,
        // making doc["age"] == 30 quietly false.
        Test("document values are plain CLR types, not JsonElement");
        var parsed = Document.Parse("{\"n\":30,\"f\":1.5,\"s\":\"x\",\"b\":true}");
        Check(parsed["n"] is long, $"integer should be long, got {parsed["n"]?.GetType()}");
        Check(parsed["f"] is double, $"real should be double, got {parsed["f"]?.GetType()}");
        Check(parsed["s"] is string, $"string should be string, got {parsed["s"]?.GetType()}");
        Check(parsed["b"] is bool, $"bool should be bool, got {parsed["b"]?.GetType()}");
    }

    private static void TestDocumentEscapes()
    {
        Test("document handles escapes, commas and unicode");
        var original = Document.Of(
            "quote", "she said \"hi\"",
            "comma", "a,b,c",
            "brace", "{not: json}",
            "backslash", @"C:\path\to",
            "newline", "line1\nline2",
            "unicode", "café ☃");

        var parsed = Document.Parse(original.ToJson());
        foreach (var key in original.Keys)
        {
            CheckEquals(parsed.GetString(key), original.GetString(key), $"field {key}");
        }
    }

    private static void TestDocumentRejectsGarbage()
    {
        Test("document rejects malformed JSON");
        CheckThrows<MooFileException>(() => Document.Parse("{not json"), "malformed JSON");
    }

    // -----------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------

    private static void TestOpenClose()
    {
        Test("open and dispose");
        using var db = Collection.Open(Path("open.bson"));
        CheckEquals(db.Count(), 0L, "new collection is empty");
    }

    private static void TestDisposeIsIdempotent()
    {
        Test("dispose is idempotent and blocks further use");
        var db = Collection.Open(Path("idem.bson"));
        db.Dispose();
        db.Dispose();
        CheckThrows<ObjectDisposedException>(() => db.Count(), "use after dispose");
    }

    private static void TestOpenWithConfig()
    {
        Test("open with indexes");
        using var db = Collection.Open(Path("cfg.bson"), new Config
        {
            Indexes = new[] { "email" },
            VectorIndexes = new Dictionary<string, int> { ["emb"] = 3 },
            TextIndexes = new[] { "content" },
        });
        CheckEquals(db.Count(), 0L, "configured collection is empty");
    }

    private static void TestPersistence()
    {
        Test("data survives a reopen");
        using (var db = Collection.Open(Path("persist.bson")))
        {
            db.Insert(Document.Of("_id", "keep", "v", 1));
        }
        using (var db = Collection.Open(Path("persist.bson")))
        {
            CheckEquals(db.Count(), 1L, "count after reopen");
            CheckEquals(db.FindOne(Document.Of("_id", "keep"))?.GetLong("v"), 1L, "value");
        }
    }

    // -----------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------

    private static void TestInsert()
    {
        Test("insert assigns an _id");
        using var db = Collection.Open(Path("ins.bson"));
        var d = db.Insert(Document.Of("name", "Alice"));
        Check(!string.IsNullOrEmpty(d.Id), "_id populated");
        CheckEquals(d.GetString("name"), "Alice", "field preserved");
    }

    private static void TestInsertCustomIdAndDuplicate()
    {
        Test("custom _id is kept and duplicates rejected");
        using var db = Collection.Open(Path("insid.bson"));
        var d = db.Insert(Document.Of("_id", "custom", "v", 1));
        CheckEquals(d.Id, "custom", "_id");
        CheckThrows<MooFileException>(
            () => db.Insert(Document.Of("_id", "custom")), "duplicate _id");
    }

    private static void TestInsertMany()
    {
        Test("insertMany returns every document");
        using var db = Collection.Open(Path("insmany.bson"));
        var docs = db.InsertMany(new[]
        {
            Document.Of("n", 1), Document.Of("n", 2), Document.Of("n", 3),
        });
        CheckEquals(docs.Count, 3, "returned count");
        CheckEquals(db.Count(), 3L, "stored count");
        Check(docs.All(d => d.Id != null), "each has an _id");
    }

    private static void TestInsertVectorSurvivesRoundTrip()
    {
        // Arrays and embedded commas were the exact cases the old
        // comma-splitting parser mangled.
        Test("array fields survive the round trip");
        using var db = Collection.Open(Path("insvec.bson"));
        db.Insert(Document.Of("_id", "v",
            "emb", new List<object?> { 0.1, 0.2, 0.3 }, "note", "a,b"));

        var d = db.FindOne(Document.Of("_id", "v"));
        CheckEquals(d?.GetList("emb")?.Count, 3, "vector length");
        CheckEquals(d?.GetString("note"), "a,b", "comma-bearing string");
    }

    // -----------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------

    private static void TestFindFilters()
    {
        Test("find with comparison operators");
        using var db = Sortable("find.bson");
        CheckEquals(db.Find().Count, 4, "match everything");
        CheckEquals(db.Find(Document.Of("dept", "eng")).Count, 2, "equality");
        CheckEquals(db.Find(Document.Of("age", Document.Of("$gt", 30))).Count, 2, "$gt");
        CheckEquals(db.Find(Document.Of("age", Document.Of("$gte", 30, "$lte", 45))).Count,
            2, "range");
    }

    private static void TestFindOneCountExists()
    {
        Test("findOne, count and exists");
        using var db = Sortable("fone.bson");
        CheckEquals(db.FindOne(Document.Of("_id", "a"))?.GetLong("age"), 30L, "findOne");
        Check(db.FindOne(Document.Of("_id", "zz")) is null, "findOne miss returns null");
        CheckEquals(db.Count(Document.Of("dept", "ops")), 2L, "filtered count");
        Check(db.Exists(Document.Of("_id", "b")), "exists hit");
        Check(!db.Exists(Document.Of("_id", "zz")), "exists miss");
    }

    private static void TestTypedFilters()
    {
        Test("typed filters render supported operators and work across CRUD");
        using var db = Collection.Open(Path("typed-filters.bson"));
        db.InsertMany(new[]
        {
            Document.Of("_id", "a", "Name", "Alice", "Age", 30, "Birthday", null,
                "Status", "active", "Tags", new[] { Document.Of("Label", "vip"), Document.Of("Label", "beta") }),
            Document.Of("_id", "b", "Name", "Bob", "Age", 20,
                "Status", "trial", "Tags", new[] { Document.Of("Label", "new") }),
            Document.Of("_id", "c", "Name", "Carol", "Age", 40,
                "Status", "archived", "Tags", new[] { Document.Of("Label", "vip") }),
        });

        var adults = Builders<Person>.Filter.Gte(person => person.Age, 30);
        var hasBirthday = Builders<Person>.Filter.Ne(person => person.Birthday, null);
        var activeAdults = Builders<Person>.Filter.And(
            adults,
            Builders<Person>.Filter.Eq(person => person.Status, "active"));
        var vip = Builders<Person>.Filter.ElemMatch(
            person => person.Tags,
            Builders<Tag>.Filter.Eq(tag => tag.Label, "vip"));

        CheckEquals(adults.ToDocument().ToJson(), "{\"Age\":{\"$gte\":30}}", "renders $gte");
        CheckEquals(hasBirthday.ToDocument().ToJson(), "{\"Birthday\":{\"$ne\":null}}", "renders null $ne");
        CheckEquals(db.Count(adults), 2L, "typed count");
        Check(db.Exists(activeAdults), "typed exists");
        CheckEquals(db.Find(activeAdults)[0].Id, "a", "typed find");
        CheckEquals(db.Find(vip).Count, 2, "typed elemMatch");
        CheckEquals(db.Find(Builders<Person>.Filter.In(person => person.Status,
            new[] { "active", "trial" })).Count, 2, "typed $in");
        CheckEquals(db.Find(Builders<Person>.Filter.Or(
            Builders<Person>.Filter.Eq(person => person.Name, "Alice"),
            Builders<Person>.Filter.Eq(person => person.Name, "Bob"))).Count, 2, "typed $or");
        CheckEquals(db.Find(Builders<Person>.Filter.Not(
            Builders<Person>.Filter.Eq(person => person.Status, "archived"))).Count, 2, "typed $not");

        Check(db.UpdateOne(adults, set: Document.Of("Status", "reviewed")), "typed update");
        CheckEquals(db.DeleteMany(Builders<Person>.Filter.Lt(person => person.Age, 25)),
            1L, "typed delete");
    }

    private static void TestTypedFiltersRejectUnsupportedSelectors()
    {
        Test("typed filters reject nested and computed selectors");
        CheckThrows<ArgumentException>(
            () => Builders<Person>.Filter.Eq(person => person.Name!.Length, 5),
            "computed selector");
    }

    private static void TestFindOptionsSort()
    {
        Test("find sorts ascending and descending");
        using var db = Sortable("fsort.bson");
        CheckEquals(string.Join(",", Ids(db.Find(null, FindOptions.Create().Sort("age")))),
            "b,a,d,c", "ascending");
        CheckEquals(string.Join(",", Ids(db.Find(null, FindOptions.Create().Sort("age", true)))),
            "c,d,a,b", "descending");
    }

    private static void TestFindOptionsSkipLimit()
    {
        Test("find paginates with skip and limit");
        using var db = Sortable("fpage.bson");
        CheckEquals(
            string.Join(",", Ids(db.Find(null, FindOptions.Create().Sort("age").Skip(1).Limit(2)))),
            "a,d", "skip then limit");
        CheckEquals(
            string.Join(",", Ids(db.Find(Document.Of("dept", "ops"),
                FindOptions.Create().Sort("age").Limit(1)))),
            "d", "filter with sort and limit");
    }

    private static void TestFindOptionsGroupAgg()
    {
        Test("find groups and aggregates");
        using var db = Sortable("fgroup.bson");
        var rows = db.Find(null, FindOptions.Create()
            .Group("dept").Count().Sum("pay").Mean("pay").Sort("dept"));

        CheckEquals(rows.Count, 2, "group count");
        // The group key keeps its original type — a plain string, not a
        // quoted one, matching the Python backend.
        CheckEquals(rows[0].GetString("dept"), "eng", "group key");
        CheckEquals(rows[0].GetLong("count"), 2L, "eng count");
        CheckEquals(rows[0].GetDouble("sum_pay"), 300.0, "eng sum");
        CheckEquals(rows[0].GetDouble("mean_pay"), 150.0, "eng mean");
        CheckEquals(rows[1].GetDouble("sum_pay"), 700.0, "ops sum");
    }

    private static void TestFindOptionsRejectsBadAgg()
    {
        Test("find rejects an unknown agg function");
        using var db = Sortable("fbadagg.bson");
        CheckThrows<MooFileException>(
            () => db.Find(null, FindOptions.Create().Group("dept").Agg("median", "pay")),
            "unknown agg function");
    }

    // -----------------------------------------------------------------
    // Update / delete
    // -----------------------------------------------------------------

    private static void TestUpdateOne()
    {
        Test("updateOne applies set, unset and inc");
        using var db = Collection.Open(Path("up.bson"));
        db.Insert(Document.Of("_id", "a", "age", 30, "city", "NYC"));

        db.UpdateOne(Document.Of("_id", "a"), Document.Of("age", 31));
        CheckEquals(db.FindOne(Document.Of("_id", "a"))?.GetLong("age"), 31L, "set");

        db.UpdateOne(Document.Of("_id", "a"), inc: Document.Of("age", 5));
        CheckEquals(db.FindOne(Document.Of("_id", "a"))?.GetLong("age"), 36L, "inc");

        db.UpdateOne(Document.Of("_id", "a"), unset: new[] { "city" });
        Check(!db.FindOne(Document.Of("_id", "a"))!.ContainsKey("city"), "unset");
    }

    private static void TestUpdateNoMatchContract()
    {
        Test("updateOne throws on a miss, updateMany reports zero");
        using var db = Collection.Open(Path("upnm.bson"));
        db.Insert(Document.Of("_id", "a", "v", 1));

        CheckThrows<MooFileException>(
            () => db.UpdateOne(Document.Of("_id", "zz"), Document.Of("v", 2)),
            "updateOne on a miss");

        CheckEquals(db.UpdateMany(Document.Of("_id", "zz"), Document.Of("v", 2)),
            0L, "updateMany on a miss");
    }

    private static void TestUpdateManyAndReplace()
    {
        Test("updateMany and replaceOne");
        using var db = Collection.Open(Path("upm.bson"));
        db.InsertMany(new[]
        {
            Document.Of("_id", "a", "s", "old"),
            Document.Of("_id", "b", "s", "old"),
            Document.Of("_id", "c", "s", "new"),
        });

        CheckEquals(db.UpdateMany(Document.Of("s", "old"), Document.Of("s", "fresh")),
            2L, "updateMany count");

        db.ReplaceOne(Document.Of("_id", "c"), Document.Of("replaced", true));
        var c = db.FindOne(Document.Of("_id", "c"))!;
        Check(c.GetBoolean("replaced"), "replacement applied");
        Check(!c.ContainsKey("s"), "old fields dropped");
        CheckEquals(c.Id, "c", "_id preserved");
    }

    private static void TestDelete()
    {
        Test("deleteOne and deleteMany");
        using var db = Collection.Open(Path("del.bson"));
        db.InsertMany(new[]
        {
            Document.Of("_id", "a"), Document.Of("_id", "b"), Document.Of("_id", "c"),
        });

        Check(db.DeleteOne(Document.Of("_id", "a")), "deleteOne hit");
        CheckEquals(db.Count(), 2L, "count after delete");
        // Unlike UpdateOne, a delete miss is not an error
        Check(!db.DeleteOne(Document.Of("_id", "zz")), "deleteOne miss returns false");
        CheckEquals(db.DeleteMany(Document.Of("_id", Document.Of("$ne", "b"))),
            1L, "deleteMany count");
    }

    // -----------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------

    private static void TestVectorSearch()
    {
        Test("vector search ranks by cosine similarity");
        using var db = Collection.Open(Path("vec.bson"), new Config
        {
            VectorIndexes = new Dictionary<string, int> { ["emb"] = 3 },
        });
        db.InsertMany(new[]
        {
            Document.Of("_id", "a", "emb", new List<object?> { 1.0, 0.0, 0.0 }),
            Document.Of("_id", "b", "emb", new List<object?> { 0.5, 0.5, 0.0 }),
            Document.Of("_id", "c", "emb", new List<object?> { 0.0, 0.0, 1.0 }),
        });

        var hits = db.VectorSearch("emb", new[] { 1.0, 0.0, 0.0 }, 3);
        CheckEquals(hits.Count, 3, "result count");
        CheckEquals(hits[0].Doc.Id, "a", "nearest first");
        Check(hits[0].Score > hits[1].Score, "scores descend");
    }

    private static void TestTextSearch()
    {
        Test("text search ranks by BM25");
        using var db = Collection.Open(Path("txt.bson"), new Config
        {
            TextIndexes = new[] { "content" },
        });
        db.InsertMany(new[]
        {
            Document.Of("_id", "1", "content", "machine learning is fascinating"),
            Document.Of("_id", "2", "content", "deep learning only"),
            Document.Of("_id", "3", "content", "cooking"),
        });

        var hits = db.TextSearch("content", "machine learning", 5);
        Check(hits.Count >= 1, "at least one hit");
        CheckEquals(hits[0].Doc.Id, "1", "best match first");
    }

    private static void TestSearchWithFilter()
    {
        Test("vector search honours a pre-filter");
        using var db = Collection.Open(Path("vecf.bson"), new Config
        {
            VectorIndexes = new Dictionary<string, int> { ["emb"] = 3 },
        });
        db.InsertMany(new[]
        {
            Document.Of("_id", "a", "kind", "x", "emb", new List<object?> { 1.0, 0.0, 0.0 }),
            Document.Of("_id", "b", "kind", "y", "emb", new List<object?> { 0.9, 0.1, 0.0 }),
        });

        var hits = db.VectorSearch("emb", new[] { 1.0, 0.0, 0.0 }, 5, Document.Of("kind", "y"));
        CheckEquals(hits.Count, 1, "filtered result count");
        CheckEquals(hits[0].Doc.Id, "b", "filtered result");

        var typedHits = db.VectorSearch("emb", new[] { 1.0, 0.0, 0.0 },
            Builders<VectorRecord>.Filter.Eq(record => record.kind, "y"), 5);
        CheckEquals(typedHits.Count, 1, "typed filtered result count");
        CheckEquals(typedHits[0].Doc.Id, "b", "typed filtered result");
    }

    // -----------------------------------------------------------------
    // Batch and utility
    // -----------------------------------------------------------------

    private static void TestBatchCommit()
    {
        Test("batch commits atomically");
        using var db = Collection.Open(Path("batch.bson"));
        db.Batch(() =>
        {
            db.Insert(Document.Of("_id", "a", "v", 1));
            db.Insert(Document.Of("_id", "b", "v", 2));
        });
        CheckEquals(db.Count(), 2L, "documents after commit");
    }

    private static void TestBatchRollback()
    {
        Test("batch rolls back on exception");
        using var db = Collection.Open(Path("batchrb.bson"));
        CheckThrows<InvalidOperationException>(() => db.Batch(() =>
        {
            db.Insert(Document.Of("_id", "a", "v", 1));
            // The original exception must survive the rollback
            throw new InvalidOperationException("simulated failure");
        }), "batch body exception propagates");

        CheckEquals(db.Count(), 0L, "documents after rollback");
    }

    private static void TestStatsAndCompact()
    {
        Test("stats and compact");
        using var db = Collection.Open(Path("stats.bson"));
        db.InsertMany(new[] { Document.Of("x", 1), Document.Of("x", 2) });
        db.DeleteOne(Document.Of("x", 1));

        var before = db.Stats();
        CheckEquals(before.GetLong("documents"), 1L, "live documents");
        // One delete leaves two dead records: the original plus a tombstone
        CheckEquals(before.GetLong("dead_records"), 2L, "dead records");

        db.Compact();
        var after = db.Stats();
        CheckEquals(after.GetLong("dead_records"), 0L, "dead records after compact");
        CheckEquals(after.GetLong("documents"), 1L, "documents after compact");
    }

    /// <summary>
    /// Reembed must reach the core and surface its error, rather than quietly
    /// returning 0, when the named field has no auto_embed config. Exercises the
    /// FFI round trip -- including error marshalling -- without a model download.
    /// </summary>
    private static void TestReembedWithoutConfig()
    {
        Test("reembed surfaces the core error for an unconfigured field");
        using var db = Collection.Open(Path("reembed.bson"));
        db.Insert(Document.Of("summary", "hello"));

        var threw = false;
        try
        {
            db.Reembed("summary");
        }
        catch (MooFileException e)
        {
            threw = true;
            Check(e.Message.Contains("autoembed", StringComparison.OrdinalIgnoreCase),
                $"error should name the missing autoembed config: {e.Message}");
        }
        Check(threw, "Reembed on an unconfigured field must throw, not return 0");
    }

    private static void TestSyncAndReindex()
    {
        Test("sync and reindex");
        using var db = Collection.Open(Path("sync.bson"), new Config
        {
            Indexes = new[] { "k" },
        });
        db.Insert(Document.Of("k", "v"));
        db.Sync();
        db.Reindex();
        CheckEquals(db.Count(Document.Of("k", "v")), 1L, "index still resolves");
    }

    private static void TestReadonlyRejectsWrites()
    {
        Test("readonly collections reject writes");
        using (var db = Collection.Open(Path("ro.bson")))
        {
            db.Insert(Document.Of("_id", "a"));
        }
        using (var db = Collection.Open(Path("ro.bson"), new Config { Readonly = true }))
        {
            CheckEquals(db.Count(), 1L, "reads still work");
            CheckThrows<MooFileException>(
                () => db.Insert(Document.Of("_id", "b")), "readonly write");
        }
    }

    // -----------------------------------------------------------------
    // Main
    // -----------------------------------------------------------------

    public static int Main()
    {
        _tempDir = Directory.CreateTempSubdirectory("moofile-csharp-test-").FullName;

        Console.WriteLine("MooFile C# Test Suite");
        Console.WriteLine("=====================\n");

        var tests = new Action[]
        {
            TestDocumentRoundTrip,
            TestDocumentValuesArePlainTypes,
            TestDocumentEscapes,
            TestDocumentRejectsGarbage,
            TestOpenClose,
            TestDisposeIsIdempotent,
            TestOpenWithConfig,
            TestPersistence,
            TestInsert,
            TestInsertCustomIdAndDuplicate,
            TestInsertMany,
            TestInsertVectorSurvivesRoundTrip,
            TestFindFilters,
            TestFindOneCountExists,
            TestTypedFilters,
            TestTypedFiltersRejectUnsupportedSelectors,
            TestFindOptionsSort,
            TestFindOptionsSkipLimit,
            TestFindOptionsGroupAgg,
            TestFindOptionsRejectsBadAgg,
            TestUpdateOne,
            TestUpdateNoMatchContract,
            TestUpdateManyAndReplace,
            TestDelete,
            TestVectorSearch,
            TestTextSearch,
            TestSearchWithFilter,
            TestBatchCommit,
            TestBatchRollback,
            TestStatsAndCompact,
            TestReembedWithoutConfig,
            TestSyncAndReindex,
            TestReadonlyRejectsWrites,
        };

        foreach (var t in tests)
        {
            try
            {
                t();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"  FAIL [{_currentTest}] exception: {e}");
                _testsFailed++;
            }
        }

        Console.WriteLine("\n====================");
        Console.WriteLine($"Tests:   {_testsRun}");
        Console.WriteLine($"Passed:  {_testsRun - _testsFailed}");
        Console.WriteLine($"Failed:  {_testsFailed}");

        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }

        return _testsFailed > 0 ? 1 : 0;
    }
}
