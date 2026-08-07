package com.moofile;

import java.util.*;

/**
 * MooFile Java binding usage example.
 *
 * Run with JNR-FFI on the classpath:
 *   javac -cp jnr-ffi.jar src/main/java/com/moofile/*.java
 *   java -cp .:jnr-ffi.jar -Djava.library.path=../target/release com.moofile.Example
 */
public class Example {
    public static void main(String[] args) throws Exception {
        String tmpDir = System.getProperty("java.io.tmpdir") + "/moofile-example-" + UUID.randomUUID();
        new java.io.File(tmpDir).mkdirs();

        try {
            System.out.println("=== MooFile Java Examples ===\n");

            // 1. Basic CRUD
            {
                Config cfg = Config.create().index("email");
                Collection db = Collection.open(tmpDir + "/contacts.bson", cfg);

                Document alice = db.insert(new Document()
                    .put("name", "Alice")
                    .put("email", "alice@example.com")
                    .put("age", 30));
                System.out.println("1. Inserted: " + alice.get("name") + " (_id: " + alice.get("_id") + ")");

                List<Document> many = new ArrayList<>();
                many.add(new Document().put("name", "Bob").put("email", "bob@example.com").put("age", 25));
                many.add(new Document().put("name", "Carol").put("email", "carol@example.com").put("age", 35));
                db.insertMany(many);
                System.out.println("   Total: " + db.count(new Document()));

                Document found = db.findOne(new Document().put("email", "alice@example.com"));
                System.out.println("2. Found: " + found.get("name") + ", age " + found.get("age"));

                db.updateOne(new Document().put("email", "alice@example.com"),
                    new Document().put("age", 31), null, null);
                System.out.println("3. Updated age: " +
                    db.findOne(new Document().put("email", "alice@example.com")).get("age"));

                List<Document> over30 = db.find(new Document().put("age",
                    new Document().put("$gte", 30)));
                System.out.println("4. Over 30: " + over30.size());

                db.deleteOne(new Document().put("email", "bob@example.com"));
                System.out.println("5. After delete: " + db.count(new Document()));

                db.close();
            }

            // 2. Vector Search
            {
                Config cfg = Config.create().vectorIndex("embedding", 3);
                Collection db = Collection.open(tmpDir + "/vectors.bson", cfg);

                List<Document> docs = new ArrayList<>();
                docs.add(new Document().put("_id", "a").put("title", "ML Guide")
                    .put("embedding", Arrays.asList(1.0, 0.0, 0.0)));
                docs.add(new Document().put("_id", "b").put("title", "Deep Learning")
                    .put("embedding", Arrays.asList(0.5, 0.5, 0.0)));
                docs.add(new Document().put("_id", "c").put("title", "Cooking")
                    .put("embedding", Arrays.asList(0.0, 0.0, 1.0)));
                db.insertMany(docs);

                List<Collection.SearchResult> results = db.vectorSearch(
                    "embedding", Arrays.asList(1.0, 0.0, 0.0), 3, null);
                System.out.println("\n6. Vector Search:");
                for (Collection.SearchResult r : results) {
                    System.out.println("   " + r.doc.get("title") + ": score=" + String.format("%.4f", r.score));
                }

                db.close();
            }

            // 3. Text Search
            {
                Config cfg = Config.create().textIndex("content");
                Collection db = Collection.open(tmpDir + "/text.bson", cfg);

                List<Document> docs = new ArrayList<>();
                docs.add(new Document().put("_id", "1").put("content", "Machine learning is fascinating"));
                docs.add(new Document().put("_id", "2").put("content", "Deep learning only"));
                docs.add(new Document().put("_id", "3").put("content", "Cooking"));
                db.insertMany(docs);

                List<Collection.SearchResult> results = db.textSearch("content", "machine learning", 5, null);
                System.out.println("\n7. Text Search:");
                for (Collection.SearchResult r : results) {
                    System.out.println("   [" + r.doc.get("_id") + "] score=" + String.format("%.4f", r.score));
                }

                db.close();
            }

            // 4. Batch
            {
                Collection db = Collection.open(tmpDir + "/batch.bson", Config.create());
                db.batch(() -> {
                    db.insert(new Document().put("_id", "a").put("amount", 100));
                    db.insert(new Document().put("_id", "b").put("amount", -50));
                });
                System.out.println("\n8. Batch: " + db.count(new Document()) + " transactions");
                db.close();
            }

        } finally {
            deleteDir(new java.io.File(tmpDir));
        }

        System.out.println("\n=== Done ===");
    }

    static void deleteDir(java.io.File dir) {
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File f : files) {
                if (f.isDirectory()) deleteDir(f);
                else f.delete();
            }
        }
        dir.delete();
    }
}
