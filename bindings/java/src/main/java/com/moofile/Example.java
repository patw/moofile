package com.moofile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * MooFile Java usage examples.
 *
 * <p>Run with build.sh, or directly:
 * <pre>{@code
 * javac -d build/classes src/main/java/com/moofile/*.java
 * java --enable-native-access=ALL-UNNAMED -cp build/classes com.moofile.Example
 * }</pre>
 */
public class Example {

    public static void main(String[] args) throws IOException {
        Path dir = Files.createTempDirectory("moofile-java-example-");
        System.out.println("=== MooFile Java Examples ===\n");

        basicCrud(dir);
        sortingAndAggregation(dir);
        vectorSearch(dir);
        textSearch(dir);
        batchWrites(dir);
        autoEmbedding();

        System.out.println("\n=== Done ===");
        deleteRecursively(dir);
    }

    // -----------------------------------------------------------------
    // 1. Basic CRUD
    // -----------------------------------------------------------------
    private static void basicCrud(Path dir) {
        try (Collection db = Collection.open(dir.resolve("contacts.bson").toString(),
                Config.create().index("email"))) {

            Document alice = db.insert(Document.of(
                "name", "Alice", "email", "alice@example.com", "age", 30));
            System.out.println("1. Inserted: " + alice.getString("name")
                + " (_id: " + alice.id() + ")");

            db.insertMany(List.of(
                Document.of("name", "Bob", "email", "bob@example.com", "age", 25, "status", "trial"),
                Document.of("name", "Carol", "email", "carol@example.com", "age", 35, "status", "active")));
            System.out.println("   Total contacts: " + db.count());

            Document found = db.findOne(Document.of("email", "alice@example.com"));
            System.out.println("2. Found: " + found.getString("name")
                + ", age " + found.getLong("age"));

            db.updateOne(Document.of("email", "alice@example.com"), Document.of("age", 31));
            System.out.println("3. Updated age: "
                + db.findOne(Document.of("email", "alice@example.com")).getLong("age"));

            List<Document> over30 = db.find(Document.of("age", Document.of("$gte", 30)));
            System.out.println("4. Contacts 30 or older: " + over30.size());

            // Java uses static filter factories, like the MongoDB Java driver.
            // import static com.moofile.Filters.*; makes these calls shorter.
            Document typedOver30 = Filters.gte("age", 30);
            Document typedAlice = Filters.eq("email", "alice@example.com");
            Document activeAdults = Filters.and(
                typedOver30,
                Filters.eq("status", "active"));
            System.out.println("   Filter factory: " + db.count(typedOver30)
                + " contacts 30 or older; Alice exists: " + db.exists(typedAlice));

            // Factory filters work with updates and deletes too.
            db.updateMany(activeAdults, Document.of("status", "reviewed"));

            db.deleteOne(Document.of("email", "bob@example.com"));
            System.out.println("5. After delete, total: " + db.count());
        }
    }

    // -----------------------------------------------------------------
    // 2. Sorting, paging, aggregation
    // -----------------------------------------------------------------
    private static void sortingAndAggregation(Path dir) {
        try (Collection db = Collection.open(dir.resolve("sales.bson").toString())) {

            db.insertMany(List.of(
                Document.of("rep", "Alice", "region", "east", "amount", 100),
                Document.of("rep", "Bob",   "region", "east", "amount", 250),
                Document.of("rep", "Carol", "region", "west", "amount", 175),
                Document.of("rep", "Dan",   "region", "west", "amount", 300),
                Document.of("rep", "Erin",  "region", "west", "amount", 125)));

            System.out.println("\n6. Top 3 sales:");
            for (Document s : db.find(null,
                    FindOptions.create().sort("amount", true).limit(3))) {
                System.out.println("   " + s.getString("rep") + ": " + s.getLong("amount"));
            }

            List<Document> page2 = db.find(null,
                FindOptions.create().sort("rep").skip(2).limit(2));
            System.out.println("   Page 2 (by name): "
                + page2.get(0).getString("rep") + ", " + page2.get(1).getString("rep"));

            System.out.println("   Totals by region:");
            for (Document r : db.find(null, FindOptions.create()
                    .group("region").count().sum("amount").mean("amount").sort("region"))) {
                System.out.println("     " + r.getString("region")
                    + ": " + r.getLong("count") + " deals"
                    + ", sum " + r.getDouble("sum_amount")
                    + ", avg " + r.getDouble("mean_amount"));
            }
        }
    }

    // -----------------------------------------------------------------
    // 3. Vector search
    // -----------------------------------------------------------------
    private static void vectorSearch(Path dir) {
        try (Collection db = Collection.open(dir.resolve("vectors.bson").toString(),
                Config.create().vectorIndex("embedding", 3))) {

            db.insertMany(List.of(
                Document.of("_id", "doc1", "title", "ML Guide",
                            "embedding", List.of(1.0, 0.0, 0.0)),
                Document.of("_id", "doc2", "title", "Deep Learning",
                            "embedding", List.of(0.5, 0.5, 0.0)),
                Document.of("_id", "doc3", "title", "Cooking",
                            "embedding", List.of(0.0, 0.0, 1.0))));

            System.out.println("\n7. Vector search:");
            for (Collection.SearchResult r :
                    db.vectorSearch("embedding", List.of(1.0, 0.0, 0.0), 3)) {
                System.out.printf("   %s: similarity = %.4f%n",
                    r.doc().getString("title"), r.score());
            }
        }
    }

    // -----------------------------------------------------------------
    // 4. Text search
    // -----------------------------------------------------------------
    private static void textSearch(Path dir) {
        try (Collection db = Collection.open(dir.resolve("text.bson").toString(),
                Config.create().textIndex("content"))) {

            db.insertMany(List.of(
                Document.of("_id", "1", "content", "Machine learning is transforming AI"),
                Document.of("_id", "2", "content", "Deep neural networks for ML"),
                Document.of("_id", "3", "content", "Cooking recipes for dinner")));

            System.out.println("\n8. Text search:");
            for (Collection.SearchResult r : db.textSearch("content", "machine learning", 5)) {
                System.out.printf("   [%s] score = %.4f%n", r.doc().id(), r.score());
            }
        }
    }

    // -----------------------------------------------------------------
    // 5. Atomic batch writes
    // -----------------------------------------------------------------
    private static void batchWrites(Path dir) {
        try (Collection db = Collection.open(dir.resolve("batch.bson").toString())) {

            db.batch(() -> {
                db.insert(Document.of("_id", "a", "type", "transaction", "amount", 100));
                db.insert(Document.of("_id", "b", "type", "transaction", "amount", -50));
            });
            System.out.println("\n9. Batch: " + db.count(Document.of("type", "transaction"))
                + " transactions committed atomically");

            // Anything thrown inside the batch rolls the whole thing back
            try {
                db.batch(() -> {
                    db.insert(Document.of("_id", "c", "type", "transaction"));
                    throw new IllegalStateException("simulated failure");
                });
            } catch (IllegalStateException expected) {
                System.out.println("   After a failed batch, still "
                    + db.count(Document.of("type", "transaction")) + " transactions");
            }
        }
    }

    // -----------------------------------------------------------------
    // 6. Autoembedding
    // -----------------------------------------------------------------
    private static void autoEmbedding() {
        System.out.println("\n10. Autoembedding (skipped — requires a GGUF model file)");
        System.out.println("   Config cfg = Config.create()");
        System.out.println("       .vectorIndex(\"embedding\", 1024)");
        System.out.println("       .autoEmbed(\"content\", Config.AutoEmbedConfig");
        System.out.println("           .of(\"hf:user/repo:model.gguf\", \"embedding\")");
        System.out.println("           .dims(1024).precision(\"int8\"));");
        System.out.println("   db.insert(Document.of(\"content\", \"Machine learning\"));");
        System.out.println("   // embedding is generated on insert");
        System.out.println("   db.semantic(\"content\", \"deep learning\", 5);");
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) { }
            });
        }
    }
}
