using System;
using System.Collections.Generic;
using System.IO;
using Moofile;

var tmpDir = Path.Combine(Path.GetTempPath(), "moofile-example-" + Guid.NewGuid());
Directory.CreateDirectory(tmpDir);

Console.WriteLine("=== MooFile C# Examples ===\n");

try
{
    // 1. Basic CRUD
    {
        using var db = Collection.Open(Path.Combine(tmpDir, "contacts.bson"));

        var alice = db.Insert(new Document { ["name"] = "Alice", ["email"] = "alice@example.com", ["age"] = 30 });
        Console.WriteLine($"1. Inserted: {alice["name"]} (_id: {alice["_id"]})");

        db.InsertMany(new List<Document> {
            new() { ["name"] = "Bob", ["email"] = "bob@example.com", ["age"] = 25 },
            new() { ["name"] = "Carol", ["email"] = "carol@example.com", ["age"] = 35 },
        });
        Console.WriteLine($"   Total contacts: {db.Count()}");

        var found = db.FindOne(new Document { ["email"] = "alice@example.com" });
        Console.WriteLine($"2. Found: {found!["name"]}, age {found!["age"]}");

        db.UpdateOne(new Document { ["email"] = "alice@example.com" }, new Document { ["age"] = 31 });
        Console.WriteLine($"3. Updated age: {db.FindOne(new Document { ["email"] = "alice@example.com" })?["age"]}");

        var over30 = db.Find(new Document { ["age"] = new Document { ["$gte"] = 30 } });
        Console.WriteLine($"4. Contacts over 30: {over30.Count}");

        db.DeleteOne(new Document { ["email"] = "bob@example.com" });
        Console.WriteLine($"5. After delete, total: {db.Count()}");
    }

    // 2. Vector Search
    {
        using var db = Collection.Open(Path.Combine(tmpDir, "vectors.bson"),
            new Config { VectorIndexes = new() { ["embedding"] = 3 } });

        db.InsertMany(new List<Document> {
            new() { ["_id"] = "doc1", ["title"] = "ML Guide", ["embedding"] = new List<double> { 1, 0, 0 } },
            new() { ["_id"] = "doc2", ["title"] = "Deep Learning", ["embedding"] = new List<double> { 0.5, 0.5, 0 } },
            new() { ["_id"] = "doc3", ["title"] = "Cooking", ["embedding"] = new List<double> { 0, 0, 1 } },
        });

        var results = db.VectorSearch("embedding", new List<double> { 1, 0, 0 }, 3);
        Console.WriteLine("\n6. Vector Search Results:");
        foreach (var r in results)
            Console.WriteLine($"   {r.Doc["title"]}: similarity = {r.Score:F4}");
    }

    // 3. Text Search
    {
        using var db = Collection.Open(Path.Combine(tmpDir, "text.bson"),
            new Config { TextIndexes = new[] { "content" } });

        db.InsertMany(new List<Document> {
            new() { ["_id"] = "1", ["content"] = "Machine learning is transforming AI" },
            new() { ["_id"] = "2", ["content"] = "Deep neural networks for ML" },
            new() { ["_id"] = "3", ["content"] = "Cooking recipes" },
        });

        var results = db.TextSearch("content", "machine learning", 5);
        Console.WriteLine("\n7. Text Search Results:");
        foreach (var r in results)
            Console.WriteLine($"   [{r.Doc["_id"]}] {r.Doc["content"]}: score = {r.Score:F4}");
    }

    // 4. Batch
    {
        using var db = Collection.Open(Path.Combine(tmpDir, "batch.bson"));
        db.Batch(() => {
            db.Insert(new Document { ["_id"] = "a", ["amount"] = 100 });
            db.Insert(new Document { ["_id"] = "b", ["amount"] = -50 });
        });
        Console.WriteLine($"\n8. Batch: {db.Count()} transactions");
    }
}
finally { Directory.Delete(tmpDir, true); }

Console.WriteLine("\n=== Done ===");
