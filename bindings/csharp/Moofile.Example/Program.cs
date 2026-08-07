using System;
using System.Collections.Generic;
using System.IO;
using Moofile;

namespace Moofile.Example;

/// <summary>
/// MooFile C# usage examples.
///
/// Run: <c>dotnet run --project Moofile.Example</c>
/// </summary>
public static class Program
{
    public static void Main()
    {
        var dir = Directory.CreateTempSubdirectory("moofile-csharp-example-").FullName;
        Console.WriteLine("=== MooFile C# Examples ===\n");

        BasicCrud(dir);
        SortingAndAggregation(dir);
        VectorSearch(dir);
        TextSearch(dir);
        BatchWrites(dir);
        AutoEmbedding();

        Console.WriteLine("\n=== Done ===");
        try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
    }

    private static string Path(string dir, string name) =>
        System.IO.Path.Combine(dir, name);

    // -----------------------------------------------------------------
    // 1. Basic CRUD
    // -----------------------------------------------------------------
    private static void BasicCrud(string dir)
    {
        using var db = Collection.Open(Path(dir, "contacts.bson"), new Config
        {
            Indexes = new[] { "email" },
        });

        var alice = db.Insert(Document.Of(
            "name", "Alice", "email", "alice@example.com", "age", 30));
        Console.WriteLine($"1. Inserted: {alice.GetString("name")} (_id: {alice.Id})");

        db.InsertMany(new[]
        {
            Document.Of("name", "Bob", "email", "bob@example.com", "age", 25),
            Document.Of("name", "Carol", "email", "carol@example.com", "age", 35),
        });
        Console.WriteLine($"   Total contacts: {db.Count()}");

        var found = db.FindOne(Document.Of("email", "alice@example.com"))!;
        Console.WriteLine($"2. Found: {found.GetString("name")}, age {found.GetLong("age")}");

        db.UpdateOne(Document.Of("email", "alice@example.com"), Document.Of("age", 31));
        var updated = db.FindOne(Document.Of("email", "alice@example.com"))!;
        Console.WriteLine($"3. Updated age: {updated.GetLong("age")}");

        var over30 = db.Find(Document.Of("age", Document.Of("$gte", 30)));
        Console.WriteLine($"4. Contacts 30 or older: {over30.Count}");

        db.DeleteOne(Document.Of("email", "bob@example.com"));
        Console.WriteLine($"5. After delete, total: {db.Count()}");
    }

    // -----------------------------------------------------------------
    // 2. Sorting, paging, aggregation
    // -----------------------------------------------------------------
    private static void SortingAndAggregation(string dir)
    {
        using var db = Collection.Open(Path(dir, "sales.bson"));

        db.InsertMany(new[]
        {
            Document.Of("rep", "Alice", "region", "east", "amount", 100),
            Document.Of("rep", "Bob",   "region", "east", "amount", 250),
            Document.Of("rep", "Carol", "region", "west", "amount", 175),
            Document.Of("rep", "Dan",   "region", "west", "amount", 300),
            Document.Of("rep", "Erin",  "region", "west", "amount", 125),
        });

        Console.WriteLine("\n6. Top 3 sales:");
        foreach (var s in db.Find(null, FindOptions.Create().Sort("amount", desc: true).Limit(3)))
        {
            Console.WriteLine($"   {s.GetString("rep")}: {s.GetLong("amount")}");
        }

        var page2 = db.Find(null, FindOptions.Create().Sort("rep").Skip(2).Limit(2));
        Console.WriteLine($"   Page 2 (by name): " +
            $"{page2[0].GetString("rep")}, {page2[1].GetString("rep")}");

        Console.WriteLine("   Totals by region:");
        foreach (var r in db.Find(null, FindOptions.Create()
                     .Group("region").Count().Sum("amount").Mean("amount").Sort("region")))
        {
            Console.WriteLine($"     {r.GetString("region")}: {r.GetLong("count")} deals" +
                $", sum {r.GetDouble("sum_amount")}, avg {r.GetDouble("mean_amount")}");
        }
    }

    // -----------------------------------------------------------------
    // 3. Vector search
    // -----------------------------------------------------------------
    private static void VectorSearch(string dir)
    {
        using var db = Collection.Open(Path(dir, "vectors.bson"), new Config
        {
            VectorIndexes = new Dictionary<string, int> { ["embedding"] = 3 },
        });

        db.InsertMany(new[]
        {
            Document.Of("_id", "doc1", "title", "ML Guide",
                        "embedding", new List<object?> { 1.0, 0.0, 0.0 }),
            Document.Of("_id", "doc2", "title", "Deep Learning",
                        "embedding", new List<object?> { 0.5, 0.5, 0.0 }),
            Document.Of("_id", "doc3", "title", "Cooking",
                        "embedding", new List<object?> { 0.0, 0.0, 1.0 }),
        });

        Console.WriteLine("\n7. Vector search:");
        foreach (var r in db.VectorSearch("embedding", new[] { 1.0, 0.0, 0.0 }, 3))
        {
            Console.WriteLine($"   {r.Doc.GetString("title")}: similarity = {r.Score:F4}");
        }
    }

    // -----------------------------------------------------------------
    // 4. Text search
    // -----------------------------------------------------------------
    private static void TextSearch(string dir)
    {
        using var db = Collection.Open(Path(dir, "text.bson"), new Config
        {
            TextIndexes = new[] { "content" },
        });

        db.InsertMany(new[]
        {
            Document.Of("_id", "1", "content", "Machine learning is transforming AI"),
            Document.Of("_id", "2", "content", "Deep neural networks for ML"),
            Document.Of("_id", "3", "content", "Cooking recipes for dinner"),
        });

        Console.WriteLine("\n8. Text search:");
        foreach (var r in db.TextSearch("content", "machine learning", 5))
        {
            Console.WriteLine($"   [{r.Doc.Id}] score = {r.Score:F4}");
        }
    }

    // -----------------------------------------------------------------
    // 5. Atomic batch writes
    // -----------------------------------------------------------------
    private static void BatchWrites(string dir)
    {
        using var db = Collection.Open(Path(dir, "batch.bson"));

        db.Batch(() =>
        {
            db.Insert(Document.Of("_id", "a", "type", "transaction", "amount", 100));
            db.Insert(Document.Of("_id", "b", "type", "transaction", "amount", -50));
        });
        Console.WriteLine($"\n9. Batch: {db.Count(Document.Of("type", "transaction"))} " +
            "transactions committed atomically");

        // Anything thrown inside the batch rolls the whole thing back
        try
        {
            db.Batch(() =>
            {
                db.Insert(Document.Of("_id", "c", "type", "transaction"));
                throw new InvalidOperationException("simulated failure");
            });
        }
        catch (InvalidOperationException)
        {
            Console.WriteLine("   After a failed batch, still " +
                $"{db.Count(Document.Of("type", "transaction"))} transactions");
        }
    }

    // -----------------------------------------------------------------
    // 6. Autoembedding
    // -----------------------------------------------------------------
    private static void AutoEmbedding()
    {
        Console.WriteLine("\n10. Autoembedding (skipped — requires a GGUF model file)");
        Console.WriteLine("   var db = Collection.Open(\"semantic.bson\", new Config {");
        Console.WriteLine("       VectorIndexes = new() { [\"embedding\"] = 1024 },");
        Console.WriteLine("       AutoEmbed = new() {");
        Console.WriteLine("           [\"content\"] = new AutoEmbedConfig {");
        Console.WriteLine("               Model = \"hf:user/repo:model.gguf\",");
        Console.WriteLine("               Target = \"embedding\", Dims = 1024,");
        Console.WriteLine("           },");
        Console.WriteLine("       },");
        Console.WriteLine("   });");
        Console.WriteLine("   db.Insert(Document.Of(\"content\", \"Machine learning\"));");
        Console.WriteLine("   // embedding is generated on insert");
        Console.WriteLine("   db.Semantic(\"content\", \"deep learning\", 5);");
    }
}
