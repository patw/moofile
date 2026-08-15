package com.moofile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * MooFile Java binding test suite.
 *
 * <p>Plain {@code main}-driven rather than JUnit so it runs with nothing but a
 * JDK — see build.sh.
 */
public class CollectionTest {

    private static int testsRun;
    private static int testsFailed;
    private static String currentTest = "";
    private static Path tempDir;

    // -----------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------

    private static void test(String name) {
        currentTest = name;
        testsRun++;
    }

    private static void check(boolean cond, String msg) {
        if (!cond) {
            System.err.println("  FAIL [" + currentTest + "] " + msg);
            testsFailed++;
        }
    }

    private static void checkEquals(Object actual, Object expected, String msg) {
        if (actual == null ? expected != null : !actual.equals(expected)) {
            System.err.println("  FAIL [" + currentTest + "] " + msg
                + ": expected " + expected + ", got " + actual);
            testsFailed++;
        }
    }

    private static String path(String name) {
        return tempDir.resolve(name).toString();
    }

    private static List<String> names(List<Document> docs) {
        List<String> out = new ArrayList<>();
        for (Document d : docs) out.add(d.getString("_id"));
        return out;
    }

    /** Four documents across two departments, out of age order. */
    private static Collection sortable(String file) {
        Collection db = Collection.open(path(file));
        db.insertMany(List.of(
            Document.of("_id", "a", "age", 30, "dept", "eng", "pay", 100),
            Document.of("_id", "b", "age", 20, "dept", "eng", "pay", 200),
            Document.of("_id", "c", "age", 50, "dept", "ops", "pay", 300),
            Document.of("_id", "d", "age", 40, "dept", "ops", "pay", 400)));
        return db;
    }

    // -----------------------------------------------------------------
    // JSON layer
    // -----------------------------------------------------------------

    private static void testJsonRoundTrip() {
        test("JSON round-trips nested structures");
        String json = "{\"s\":\"hi\",\"i\":42,\"f\":3.5,\"b\":true,\"n\":null,"
            + "\"arr\":[1,2,3],\"obj\":{\"k\":\"v\"}}";
        Document d = Document.parse(json);
        checkEquals(d.getString("s"), "hi", "string");
        checkEquals(d.getLong("i"), 42L, "integer");
        checkEquals(d.getDouble("f"), 3.5, "real");
        check(d.getBoolean("b"), "boolean");
        check(d.get("n") == null, "null");
        checkEquals(d.getList("arr").size(), 3, "array length");
        checkEquals(d.getDocument("obj").getString("k"), "v", "nested object");
    }

    private static void testJsonEscapes() {
        // Splitting JSON on commas — the shortcut this binding used to take —
        // corrupts every one of these.
        test("JSON handles escapes, commas and exponents");
        Document original = Document.of(
            "quote", "she said \"hi\"",
            "comma", "a,b,c",
            "brace", "{not: json}",
            "backslash", "C:\\path\\to",
            "newline", "line1\nline2",
            "tab", "a\tb",
            "unicode", "caf\u00e9 \u2603");
        Document parsed = Document.parse(original.toJson());
        for (String key : original.keySet()) {
            checkEquals(parsed.getString(key), original.getString(key), "field " + key);
        }

        Document exp = Document.parse("{\"small\":1e-5,\"big\":1.5E10,\"neg\":-42}");
        checkEquals(exp.getDouble("small"), 1e-5, "exponent");
        checkEquals(exp.getDouble("big"), 1.5e10, "capital exponent");
        checkEquals(exp.getLong("neg"), -42L, "negative integer");
    }

    private static void testJsonRejectsGarbage() {
        test("JSON rejects malformed input");
        try {
            Json.parse("{not json");
            check(false, "expected a SyntaxException");
        } catch (Json.SyntaxException expected) {
            // correct
        }
    }

    // -----------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------

    private static void testOpenClose() {
        test("open and close");
        try (Collection db = Collection.open(path("open.bson"))) {
            checkEquals(db.count(), 0L, "new collection is empty");
        }
    }

    private static void testCloseIsIdempotent() {
        test("close is idempotent");
        Collection db = Collection.open(path("idem.bson"));
        db.close();
        db.close();
        try {
            db.count();
            check(false, "expected use-after-close to throw");
        } catch (MooFileException expected) {
            // correct
        }
    }

    private static void testOpenWithConfig() {
        test("open with indexes");
        try (Collection db = Collection.open(path("cfg.bson"),
                Config.create().index("email").vectorIndex("emb", 3).textIndex("content"))) {
            checkEquals(db.count(), 0L, "configured collection is empty");
        }
    }

    private static void testPersistence() {
        test("data survives a reopen");
        try (Collection db = Collection.open(path("persist.bson"))) {
            db.insert(Document.of("_id", "keep", "v", 1));
        }
        try (Collection db = Collection.open(path("persist.bson"))) {
            checkEquals(db.count(), 1L, "document count after reopen");
            checkEquals(db.findOne(Document.of("_id", "keep")).getLong("v"), 1L, "value");
        }
    }

    // -----------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------

    private static void testInsert() {
        test("insert assigns an _id");
        try (Collection db = Collection.open(path("ins.bson"))) {
            Document d = db.insert(Document.of("name", "Alice"));
            check(d.id() != null && !d.id().isEmpty(), "_id populated");
            checkEquals(d.getString("name"), "Alice", "field preserved");
        }
    }

    private static void testInsertCustomId() {
        test("insert keeps a custom _id");
        try (Collection db = Collection.open(path("insid.bson"))) {
            Document d = db.insert(Document.of("_id", "custom", "v", 1));
            checkEquals(d.id(), "custom", "_id");
        }
    }

    private static void testInsertDuplicateRejected() {
        test("duplicate _id is rejected");
        try (Collection db = Collection.open(path("insdup.bson"))) {
            db.insert(Document.of("_id", "dup"));
            try {
                db.insert(Document.of("_id", "dup"));
                check(false, "expected a duplicate-key error");
            } catch (MooFileException expected) {
                // correct
            }
        }
    }

    private static void testInsertMany() {
        test("insertMany returns every document");
        try (Collection db = Collection.open(path("insmany.bson"))) {
            List<Document> docs = db.insertMany(List.of(
                Document.of("n", 1), Document.of("n", 2), Document.of("n", 3)));
            checkEquals(docs.size(), 3, "returned count");
            checkEquals(db.count(), 3L, "stored count");
            for (Document d : docs) check(d.id() != null, "each has an _id");
        }
    }

    private static void testInsertVectorSurvivesRoundTrip() {
        // A document holding an array was the exact case the old comma-splitting
        // parser mangled.
        test("array fields survive the round trip");
        try (Collection db = Collection.open(path("insvec.bson"))) {
            db.insert(Document.of("_id", "v", "emb", List.of(0.1, 0.2, 0.3), "note", "a,b"));
            Document d = db.findOne(Document.of("_id", "v"));
            checkEquals(d.getList("emb").size(), 3, "vector length");
            checkEquals(d.getString("note"), "a,b", "comma-bearing string");
        }
    }

    // -----------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------

    private static void testFindFilters() {
        test("find with comparison and logical operators");
        try (Collection db = sortable("find.bson")) {
            checkEquals(db.find(null).size(), 4, "match everything");
            checkEquals(db.find(Document.of("dept", "eng")).size(), 2, "equality");
            checkEquals(db.find(Document.of("age", Document.of("$gt", 30))).size(), 2, "$gt");
            checkEquals(db.find(Document.of("age", Document.of("$gte", 30, "$lte", 45))).size(),
                2, "range");

            Document or = new Document();
            or.put("$or", List.of(
                Document.of("_id", "a").asMap(),
                Document.of("_id", "c").asMap()));
            checkEquals(db.find(or).size(), 2, "$or");
        }
    }

    private static void testFindOneAndCountAndExists() {
        test("findOne, count and exists");
        try (Collection db = sortable("fone.bson")) {
            checkEquals(db.findOne(Document.of("_id", "a")).getLong("age"), 30L, "findOne");
            check(db.findOne(Document.of("_id", "zz")) == null, "findOne miss returns null");
            checkEquals(db.count(Document.of("dept", "ops")), 2L, "filtered count");
            check(db.exists(Document.of("_id", "b")), "exists hit");
            check(!db.exists(Document.of("_id", "zz")), "exists miss");
        }
    }

    private static void testFiltersFactory() {
        test("Filters factory builds supported filters for every document API");
        try (Collection db = Collection.open(path("filters.bson"))) {
            db.insertMany(List.of(
                Document.of("_id", "a", "age", 30, "status", "active",
                    "birthday", null, "tags", List.of(Document.of("label", "vip"))),
                Document.of("_id", "b", "age", 20, "status", "trial",
                    "tags", List.of(Document.of("label", "new"))),
                Document.of("_id", "c", "age", 40, "status", "archived",
                    "tags", List.of(Document.of("label", "vip")))));

            Document adults = Filters.gte("age", 30);
            Document activeAdults = Filters.and(adults, Filters.eq("status", "active"));
            Document vip = Filters.elemMatch("tags", Filters.eq("label", "vip"));

            checkEquals(adults.toJson(), "{\"age\":{\"$gte\":30}}", "renders $gte");
            checkEquals(Filters.ne("birthday", null).toJson(),
                "{\"birthday\":{\"$ne\":null}}", "renders null $ne");
            checkEquals(db.count(adults), 2L, "factory count");
            check(db.exists(activeAdults), "factory exists");
            checkEquals(db.findOne(activeAdults).id(), "a", "factory findOne");
            checkEquals(db.find(vip).size(), 2, "factory elemMatch");
            checkEquals(db.find(Filters.in("status", List.of("active", "trial"))).size(),
                2, "factory $in");
            checkEquals(db.find(Filters.or(Filters.eq("_id", "a"), Filters.eq("_id", "b"))).size(),
                2, "factory $or");
            checkEquals(db.find(Filters.not(Filters.eq("status", "archived"))).size(),
                2, "factory $not");

            check(db.updateOne(activeAdults, Document.of("status", "reviewed")), "factory update");
            checkEquals(db.deleteMany(Filters.lt("age", 25)), 1L, "factory delete");
        }
    }

    private static void testFindOptionsSort() {
        test("find sorts ascending and descending");
        try (Collection db = sortable("fsort.bson")) {
            checkEquals(names(db.find(null, FindOptions.create().sort("age"))),
                Arrays.asList("b", "a", "d", "c"), "ascending");
            checkEquals(names(db.find(null, FindOptions.create().sort("age", true))),
                Arrays.asList("c", "d", "a", "b"), "descending");
        }
    }

    private static void testFindOptionsSkipLimit() {
        test("find paginates with skip and limit");
        try (Collection db = sortable("fpage.bson")) {
            checkEquals(names(db.find(null, FindOptions.create().sort("age").skip(1).limit(2))),
                Arrays.asList("a", "d"), "skip then limit");
            checkEquals(names(db.find(Document.of("dept", "ops"),
                    FindOptions.create().sort("age").limit(1))),
                Arrays.asList("d"), "filter with sort and limit");
        }
    }

    private static void testFindOptionsGroupAgg() {
        test("find groups and aggregates");
        try (Collection db = sortable("fgroup.bson")) {
            List<Document> rows = db.find(null, FindOptions.create()
                .group("dept").count().sum("pay").mean("pay").sort("dept"));

            checkEquals(rows.size(), 2, "group count");
            // The group key keeps its original type — a plain string, not a
            // quoted one, matching the Python backend.
            checkEquals(rows.get(0).getString("dept"), "eng", "group key");
            checkEquals(rows.get(0).getLong("count"), 2L, "eng count");
            checkEquals(rows.get(0).getDouble("sum_pay"), 300.0, "eng sum");
            checkEquals(rows.get(0).getDouble("mean_pay"), 150.0, "eng mean");
            checkEquals(rows.get(1).getDouble("sum_pay"), 700.0, "ops sum");
        }
    }

    private static void testFindOptionsRejectsBadAgg() {
        test("find rejects an unknown agg function");
        try (Collection db = sortable("fbadagg.bson")) {
            try {
                db.find(null, FindOptions.create().group("dept").agg("median", "pay"));
                check(false, "expected an unknown agg function to throw");
            } catch (MooFileException e) {
                check(e.getMessage().contains("unknown agg func"),
                    "error should name the problem, got: " + e.getMessage());
            }
        }
    }

    // -----------------------------------------------------------------
    // Update / delete
    // -----------------------------------------------------------------

    private static void testUpdateOne() {
        test("updateOne applies set, unset and inc");
        try (Collection db = Collection.open(path("up.bson"))) {
            db.insert(Document.of("_id", "a", "age", 30, "city", "NYC"));

            db.updateOne(Document.of("_id", "a"), Document.of("age", 31));
            checkEquals(db.findOne(Document.of("_id", "a")).getLong("age"), 31L, "set");

            db.updateOne(Document.of("_id", "a"), null, null, Document.of("age", 5));
            checkEquals(db.findOne(Document.of("_id", "a")).getLong("age"), 36L, "inc");

            db.updateOne(Document.of("_id", "a"), null, List.of("city"), null);
            check(!db.findOne(Document.of("_id", "a")).containsKey("city"), "unset");
        }
    }

    private static void testUpdateNoMatchContract() {
        test("updateOne throws on a miss, updateMany reports zero");
        try (Collection db = Collection.open(path("upnm.bson"))) {
            db.insert(Document.of("_id", "a", "v", 1));
            try {
                db.updateOne(Document.of("_id", "zz"), Document.of("v", 2));
                check(false, "expected updateOne to throw");
            } catch (MooFileException e) {
                check(e.getMessage().contains("no document matches"),
                    "message should explain the miss, got: " + e.getMessage());
            }
            checkEquals(db.updateMany(Document.of("_id", "zz"), Document.of("v", 2)),
                0L, "updateMany on a miss");
        }
    }

    private static void testUpdateManyAndReplace() {
        test("updateMany and replaceOne");
        try (Collection db = Collection.open(path("upm.bson"))) {
            db.insertMany(List.of(
                Document.of("_id", "a", "s", "old"),
                Document.of("_id", "b", "s", "old"),
                Document.of("_id", "c", "s", "new")));

            checkEquals(db.updateMany(Document.of("s", "old"), Document.of("s", "fresh")),
                2L, "updateMany count");
            checkEquals(db.count(Document.of("s", "fresh")), 2L, "updated documents");

            db.replaceOne(Document.of("_id", "c"), Document.of("replaced", true));
            Document c = db.findOne(Document.of("_id", "c"));
            check(c.getBoolean("replaced"), "replacement applied");
            check(!c.containsKey("s"), "old fields dropped");
            checkEquals(c.id(), "c", "_id preserved");
        }
    }

    private static void testDelete() {
        test("deleteOne and deleteMany");
        try (Collection db = Collection.open(path("del.bson"))) {
            db.insertMany(List.of(
                Document.of("_id", "a"), Document.of("_id", "b"), Document.of("_id", "c")));

            check(db.deleteOne(Document.of("_id", "a")), "deleteOne hit");
            checkEquals(db.count(), 2L, "count after delete");
            // Unlike updateOne, a delete miss is not an error
            check(!db.deleteOne(Document.of("_id", "zz")), "deleteOne miss returns false");
            checkEquals(db.deleteMany(Document.of("_id", Document.of("$ne", "b"))),
                1L, "deleteMany count");
        }
    }

    // -----------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------

    private static void testVectorSearch() {
        test("vector search ranks by cosine similarity");
        try (Collection db = Collection.open(path("vec.bson"),
                Config.create().vectorIndex("emb", 3))) {
            db.insertMany(List.of(
                Document.of("_id", "a", "emb", List.of(1.0, 0.0, 0.0)),
                Document.of("_id", "b", "emb", List.of(0.5, 0.5, 0.0)),
                Document.of("_id", "c", "emb", List.of(0.0, 0.0, 1.0))));

            List<Collection.SearchResult> hits =
                db.vectorSearch("emb", List.of(1.0, 0.0, 0.0), 3);
            checkEquals(hits.size(), 3, "result count");
            checkEquals(hits.get(0).doc().id(), "a", "nearest first");
            check(hits.get(0).score() > hits.get(1).score(), "scores descend");
        }
    }

    private static void testTextSearch() {
        test("text search ranks by BM25");
        try (Collection db = Collection.open(path("txt.bson"),
                Config.create().textIndex("content"))) {
            db.insertMany(List.of(
                Document.of("_id", "1", "content", "machine learning is fascinating"),
                Document.of("_id", "2", "content", "deep learning only"),
                Document.of("_id", "3", "content", "cooking")));

            List<Collection.SearchResult> hits = db.textSearch("content", "machine learning", 5);
            check(hits.size() >= 1, "at least one hit");
            checkEquals(hits.get(0).doc().id(), "1", "best match first");
        }
    }

    private static void testSearchWithFilter() {
        test("vector search honours a pre-filter");
        try (Collection db = Collection.open(path("vecf.bson"),
                Config.create().vectorIndex("emb", 3))) {
            db.insertMany(List.of(
                Document.of("_id", "a", "kind", "x", "emb", List.of(1.0, 0.0, 0.0)),
                Document.of("_id", "b", "kind", "y", "emb", List.of(0.9, 0.1, 0.0))));

            List<Collection.SearchResult> hits = db.vectorSearch(
                "emb", List.of(1.0, 0.0, 0.0), 5, Document.of("kind", "y"));
            checkEquals(hits.size(), 1, "filtered result count");
            checkEquals(hits.get(0).doc().id(), "b", "filtered result");
        }
    }

    // -----------------------------------------------------------------
    // Batch and utility
    // -----------------------------------------------------------------

    private static void testBatchCommit() {
        test("batch commits atomically");
        try (Collection db = Collection.open(path("batch.bson"))) {
            db.batch(() -> {
                db.insert(Document.of("_id", "a", "v", 1));
                db.insert(Document.of("_id", "b", "v", 2));
            });
            checkEquals(db.count(), 2L, "documents after commit");
        }
    }

    private static void testBatchRollback() {
        test("batch rolls back on exception");
        try (Collection db = Collection.open(path("batchrb.bson"))) {
            try {
                db.batch(() -> {
                    db.insert(Document.of("_id", "a", "v", 1));
                    throw new IllegalStateException("simulated failure");
                });
                check(false, "expected the exception to propagate");
            } catch (IllegalStateException expected) {
                // correct — and the original exception, not a rollback error
            }
            checkEquals(db.count(), 0L, "documents after rollback");
        }
    }

    private static void testStatsAndCompact() {
        test("stats and compact");
        try (Collection db = Collection.open(path("stats.bson"))) {
            db.insertMany(List.of(Document.of("x", 1), Document.of("x", 2)));
            db.deleteOne(Document.of("x", 1));

            Document before = db.stats();
            checkEquals(before.getLong("documents"), 1L, "live documents");
            // One delete leaves two dead records: the original plus a tombstone
            checkEquals(before.getLong("dead_records"), 2L, "dead records");

            db.compact();
            Document after = db.stats();
            checkEquals(after.getLong("dead_records"), 0L, "dead records after compact");
            checkEquals(after.getLong("documents"), 1L, "documents after compact");
        }
    }

    /**
     * reembed must reach the core and surface its error, rather than quietly
     * returning 0, when the named field has no auto_embed config. Exercises the
     * FFI round trip -- including error marshalling -- without a model download.
     */
    private static void testReembedWithoutConfig() {
        test("reembed surfaces the core error for an unconfigured field");
        try (Collection db = Collection.open(path("reembed.bson"))) {
            db.insert(Document.of("summary", "hello"));
            boolean threw = false;
            try {
                db.reembed("summary");
            } catch (MooFileException e) {
                threw = true;
                check(e.getMessage().toLowerCase().contains("autoembed"),
                    "error should name the missing autoembed config: " + e.getMessage());
            }
            check(threw, "reembed on an unconfigured field must throw, not return 0");
        }
    }

    private static void testSyncAndReindex() {
        test("sync and reindex");
        try (Collection db = Collection.open(path("sync.bson"),
                Config.create().index("k"))) {
            db.insert(Document.of("k", "v"));
            db.sync();
            db.reindex();
            checkEquals(db.count(Document.of("k", "v")), 1L, "index still resolves");
        }
    }

    private static void testReadonlyRejectsWrites() {
        test("readonly collections reject writes");
        try (Collection db = Collection.open(path("ro.bson"))) {
            db.insert(Document.of("_id", "a"));
        }
        try (Collection db = Collection.open(path("ro.bson"),
                Config.create().readonly(true))) {
            checkEquals(db.count(), 1L, "reads still work");
            try {
                db.insert(Document.of("_id", "b"));
                check(false, "expected a readonly write to throw");
            } catch (MooFileException expected) {
                // correct
            }
        }
    }

    // -----------------------------------------------------------------
    // Main
    // -----------------------------------------------------------------

    public static void main(String[] args) throws IOException {
        tempDir = Files.createTempDirectory("moofile-java-test-");

        System.out.println("MooFile Java Test Suite");
        System.out.println("=======================\n");

        Runnable[] tests = {
            CollectionTest::testJsonRoundTrip,
            CollectionTest::testJsonEscapes,
            CollectionTest::testJsonRejectsGarbage,
            CollectionTest::testOpenClose,
            CollectionTest::testCloseIsIdempotent,
            CollectionTest::testOpenWithConfig,
            CollectionTest::testPersistence,
            CollectionTest::testInsert,
            CollectionTest::testInsertCustomId,
            CollectionTest::testInsertDuplicateRejected,
            CollectionTest::testInsertMany,
            CollectionTest::testInsertVectorSurvivesRoundTrip,
            CollectionTest::testFindFilters,
            CollectionTest::testFindOneAndCountAndExists,
            CollectionTest::testFiltersFactory,
            CollectionTest::testFindOptionsSort,
            CollectionTest::testFindOptionsSkipLimit,
            CollectionTest::testFindOptionsGroupAgg,
            CollectionTest::testFindOptionsRejectsBadAgg,
            CollectionTest::testUpdateOne,
            CollectionTest::testUpdateNoMatchContract,
            CollectionTest::testUpdateManyAndReplace,
            CollectionTest::testDelete,
            CollectionTest::testVectorSearch,
            CollectionTest::testTextSearch,
            CollectionTest::testSearchWithFilter,
            CollectionTest::testBatchCommit,
            CollectionTest::testBatchRollback,
            CollectionTest::testStatsAndCompact,
            CollectionTest::testReembedWithoutConfig,
            CollectionTest::testSyncAndReindex,
            CollectionTest::testReadonlyRejectsWrites,
        };

        for (Runnable t : tests) {
            try {
                t.run();
            } catch (Throwable e) {
                System.err.println("  FAIL [" + currentTest + "] exception: " + e);
                e.printStackTrace();
                testsFailed++;
            }
        }

        System.out.println("\n====================");
        System.out.println("Tests:   " + testsRun);
        System.out.println("Passed:  " + (testsRun - testsFailed));
        System.out.println("Failed:  " + testsFailed);

        deleteRecursively(tempDir);
        System.exit(testsFailed > 0 ? 1 : 0);
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (var walk = Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) { }
            });
        }
    }
}
