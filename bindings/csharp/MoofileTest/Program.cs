using System;
using System.Collections.Generic;
using System.IO;
using Moofile;

class Test
{
    static int testsRun = 0, testsFailed = 0;

    static void TestCase(string name, Action fn)
    {
        testsRun++;
        try { fn(); Console.WriteLine($"  PASS [{name}]"); }
        catch (Exception e) { Console.WriteLine($"  FAIL [{name}] {e.Message}"); testsFailed++; }
    }

    static void Assert(bool cond, string msg = "assertion failed")
    {
        if (!cond) throw new Exception(msg);
    }

    static void Main()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "moofile-cs-test-" + Guid.NewGuid());
        Directory.CreateDirectory(tmpDir);

        try
        {
            Console.WriteLine("MooFile C# Test Suite\n");

            TestCase("open default", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "default.bson"));
                Assert(db.Count() == 0);
            });

            TestCase("insert and find", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "crud.bson"));
                var doc = db.Insert(new Document { ["name"] = "Alice", ["age"] = 30 });
                Assert(doc.ContainsKey("_id"));
                var found = db.FindOne(new Document { ["name"] = "Alice" });
                Assert(found != null);
            });

            TestCase("insert many", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "many.bson"));
                db.InsertMany(new List<Document> {
                    new() { ["x"] = 1 }, new() { ["x"] = 2 }, new() { ["x"] = 3 }
                });
                Assert(db.Count() == 3);
            });

            TestCase("duplicate rejected", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "dup.bson"));
                db.Insert(new Document { ["_id"] = "a", ["v"] = 1 });
                try { db.Insert(new Document { ["_id"] = "a", ["v"] = 2 }); Assert(false); }
                catch { }
            });

            TestCase("find with comparison", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "cmp.bson"));
                db.InsertMany(new List<Document> {
                    new() { ["age"] = 20 }, new() { ["age"] = 30 }, new() { ["age"] = 40 }
                });
                Assert(db.Find(new Document { ["age"] = new Document { ["$gt"] = 25 } }).Count == 2);
            });

            TestCase("update one", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "up.bson"));
                db.Insert(new Document { ["_id"] = "a", ["age"] = 30 });
                Assert(db.UpdateOne(new Document { ["_id"] = "a" }, new Document { ["age"] = 31 }));
                var doc = db.FindOne(new Document { ["_id"] = "a" });
                var age = doc!["age"]!.ToString();
                Assert(age == "31" || age == "31.0", $"expected age 31, got {age}");
            });

            TestCase("delete one", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "del.bson"));
                db.Insert(new Document { ["_id"] = "a" });
                Assert(db.DeleteOne(new Document { ["_id"] = "a" }));
                Assert(db.Count() == 0);
            });

            TestCase("vector search", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "vec.bson"),
                    new Config { VectorIndexes = new() { ["emb"] = 3 } });
                db.InsertMany(new List<Document> {
                    new() { ["_id"] = "a", ["emb"] = new List<double> { 1, 0, 0 } },
                    new() { ["_id"] = "b", ["emb"] = new List<double> { 0.5, 0.5, 0 } },
                });
                var results = db.VectorSearch("emb", new List<double> { 1, 0, 0 }, 3);
                Assert(results.Count == 2);
                Assert(results[0].Doc["_id"]!.ToString() == "a");
            });

            TestCase("text search", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "txt.bson"),
                    new Config { TextIndexes = new[] { "content" } });
                db.InsertMany(new List<Document> {
                    new() { ["_id"] = "1", ["content"] = "machine learning" },
                    new() { ["_id"] = "2", ["content"] = "cooking" },
                });
                Assert(db.TextSearch("content", "learning", 5).Count == 1);
            });

            TestCase("batch commit", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "batch.bson"));
                db.Batch(() => { db.Insert(new Document { ["_id"] = "a" }); });
                Assert(db.Count() == 1);
            });

            TestCase("compact reclaims space", () =>
            {
                using var db = Collection.Open(Path.Combine(tmpDir, "cp.bson"));
                db.InsertMany(new List<Document> { new() { ["x"] = 1 }, new() { ["x"] = 2 } });
                db.DeleteOne(new Document { ["x"] = 1 });
                db.Compact();
                var stats = db.Stats();
                Assert(stats["dead_records"]!.ToString() == "0");
            });

            TestCase("persistence across reopen", () =>
            {
                var p = Path.Combine(tmpDir, "persist.bson");
                { using var db = Collection.Open(p); db.Insert(new Document { ["x"] = 1 }); }
                { using var db = Collection.Open(p); Assert(db.Count() == 1); }
            });

            Console.WriteLine($"\n====================");
            Console.WriteLine($"Tests:   {testsRun}");
            Console.WriteLine($"Passed:  {testsRun - testsFailed}");
            Console.WriteLine($"Failed:  {testsFailed}");
        }
        finally { Directory.Delete(tmpDir, true); }
        Environment.Exit(testsFailed > 0 ? 1 : 0);
    }
}
