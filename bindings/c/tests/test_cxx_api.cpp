/**
 * test_cxx_api.cpp — Comprehensive test suite for the MooFile C++ wrapper.
 *
 * Tests RAII resource management, exception safety, and idiomatic C++ usage
 * of the moofile::Collection class.
 *
 * Build:
 *   g++ -std=c++17 -Wall -Wextra -o test_cxx_api test_cxx_api.cpp \
 *       -L.. -lmoofile -I../include
 */

#include "moofile.hpp"

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

using json = nlohmann::json;

/* ---------------------------------------------------------------------------
 * Test infrastructure
 * --------------------------------------------------------------------------- */

static int  g_tests_run   = 0;
static int  g_tests_failed = 0;
static char g_test_name[256];
static char g_temp_dir[256];

#define TEST(name)   do { snprintf(g_test_name, sizeof(g_test_name), "%s", name); g_tests_run++; } while(0)
#define FAIL(msg)    do { std::cerr << "  FAIL [" << g_test_name << "] line " << __LINE__ << ": " << msg << std::endl; g_tests_failed++; } while(0)
#define ASSERT(cond) do { if (!(cond)) { FAIL(#cond); return; } } while(0)

static void setup_temp_dir() {
    snprintf(g_temp_dir, sizeof(g_temp_dir), "/tmp/moofile_cxx_test_%d_%d", getpid(), rand());
    mkdir(g_temp_dir, 0755);
}

static void cleanup_temp_dir() {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", g_temp_dir);
    system(cmd);
}

static std::string make_path(const std::string& name) {
    return std::string(g_temp_dir) + "/" + name;
}

/* ---------------------------------------------------------------------------
 * Tests
 * --------------------------------------------------------------------------- */

static void test_open_default() {
    TEST("open with default config");
    try {
        moofile::Collection db(make_path("default.bson"));
        ASSERT(db.count({}) == 0);
    } catch (const moofile::error& e) {
        FAIL(e.what());
    }
}

static void test_open_with_config() {
    TEST("open with indexes/vector/text config");
    try {
        moofile::Config cfg;
        cfg.index("email").index("name")
           .vector_index("emb", 3)
           .text_index("content");
        moofile::Collection db(make_path("configured.bson"), cfg);
        ASSERT(db.count({}) == 0);
    } catch (const moofile::error& e) {
        FAIL(e.what());
    }
}

static void test_open_readonly() {
    TEST("open readonly rejects writes");
    try {
        moofile::Collection db(make_path("ro_cxx.bson"));
        db.insert({{"x", 1}});
    } catch (...) { FAIL("write on normal collection failed"); }

    try {
        moofile::Config cfg;
        cfg.set_readonly(true);
        moofile::Collection db(make_path("ro_cxx.bson"), cfg);
        db.insert({{"x", 2}});
        FAIL("readonly insert should throw");
    } catch (const moofile::error&) {
        /* expected */
    }
}

static void test_raii_close_on_destruction() {
    TEST("RAII closes collection on destruction");
    /* Just create and destroy — no crash means the file handle was freed */
    {
        moofile::Collection db(make_path("raii.bson"));
        db.insert({{"name", "Alice"}});
    }
    /* Reopen to prove data persisted */
    {
        moofile::Collection db(make_path("raii.bson"));
        ASSERT(db.count({}) == 1);
    }
}

static void test_move_constructor() {
    TEST("move constructor transfers ownership");
    moofile::Collection db1(make_path("move.bson"));
    db1.insert({{"_id", "a"}, {"v", 1}});

    moofile::Collection db2(std::move(db1));
    /* db1 is now invalid, db2 owns the handle */
    ASSERT(db2.count({}) == 1);
    ASSERT(db2.exists({{"_id", "a"}}));
}

static void test_insert_returns_doc() {
    TEST("insert returns document with _id");
    moofile::Collection db(make_path("ins_ret.bson"));
    auto doc = db.insert({{"name", "Alice"}, {"email", "a@test.com"}});
    ASSERT(doc.contains("_id"));
    ASSERT(doc["name"] == "Alice");
}

static void test_insert_duplicate_throws() {
    TEST("insert duplicate _id throws error");
    moofile::Collection db(make_path("ins_dup.bson"));
    db.insert({{"_id", "x"}, {"v", 1}});
    try {
        db.insert({{"_id", "x"}, {"v", 2}});
        FAIL("duplicate insert should throw");
    } catch (const moofile::error&) { /* expected */ }
}

static void test_insert_many() {
    TEST("insert_many returns array of docs");
    moofile::Collection db(make_path("ins_many.bson"));
    auto docs = db.insert_many({{{ "n", 1 }}, {{ "n", 2 }}, {{ "n", 3 }}});
    ASSERT(docs.is_array());
    ASSERT(docs.size() == 3);
}

static void test_find_all() {
    TEST("find returns all documents");
    moofile::Collection db(make_path("f_all.bson"));
    db.insert_many({{{ "x", 1 }}, {{ "x", 2 }}, {{ "x", 3 }}});
    auto docs = db.find({}).to_vector();
    ASSERT(docs.size() == 3);
}

static void test_find_filtered() {
    TEST("find with filter");
    moofile::Collection db(make_path("f_filt.bson"), moofile::Config{}.index("name"));
    db.insert_many({{{"name", "Alice"}, {"age", 30}}, {{"name", "Bob"}, {"age", 25}}});
    auto docs = db.find({{"name", "Alice"}}).to_vector();
    ASSERT(docs.size() == 1);
    ASSERT(docs[0]["name"] == "Alice");
}

static void test_find_comparison() {
    TEST("find with comparison operators");
    moofile::Collection db(make_path("f_cmp.bson"));
    db.insert_many({{{"age", 20}}, {{"age", 30}}, {{"age", 40}}});

    auto docs = db.find({{"age", {{"$gt", 25}}}}).to_vector();
    ASSERT(docs.size() == 2);

    docs = db.find({{"age", {{"$lte", 30}}}}).to_vector();
    ASSERT(docs.size() == 2);
}

static void test_find_logical() {
    TEST("find with $and / $or / $not");
    moofile::Collection db(make_path("f_log.bson"));
    db.insert_many({{{"s", "a"}, {"v", 1}}, {{"s", "b"}, {"v", 2}}, {{"s", "a"}, {"v", 3}}});

    /* Built explicitly: nested brace-init of an array-of-objects is
     * ambiguous in nlohmann and silently adds a level of nesting. */
    json and_filter = json::object();
    and_filter["$and"] = json::array({
        json{{"s", "a"}},
        json{{"v", json{{"$gt", 2}}}},
    });
    auto docs = db.find(and_filter).to_vector();
    ASSERT(docs.size() == 1); /* s=a and v=3 */

    json or_filter = json::object();
    or_filter["$or"] = json::array({
        json{{"s", "b"}},
        json{{"v", 1}},
    });
    docs = db.find(or_filter).to_vector();
    ASSERT(docs.size() == 2); /* {b,2} and {a,1} */

    json not_filter = json::object();
    not_filter["$not"] = json{{"s", "a"}};
    docs = db.find(not_filter).to_vector();
    ASSERT(docs.size() == 1); /* {b,2} */
}

/* ---------------------------------------------------------------------------
 * FindOptions — sort / skip / limit / group / agg
 * --------------------------------------------------------------------------- */

/* Four documents across two departments, deliberately not in age order. */
static moofile::Collection make_sortable(const std::string& name) {
    moofile::Collection db(make_path(name));
    db.insert_many(json::array({
        json{{"_id", "a"}, {"age", 30}, {"dept", "eng"}, {"pay", 100}},
        json{{"_id", "b"}, {"age", 20}, {"dept", "eng"}, {"pay", 200}},
        json{{"_id", "c"}, {"age", 50}, {"dept", "ops"}, {"pay", 300}},
        json{{"_id", "d"}, {"age", 40}, {"dept", "ops"}, {"pay", 400}},
    }));
    return db;
}

static void test_find_options_sort() {
    TEST("FindOptions sorts ascending and descending");
    auto db = make_sortable("cxx_fo_sort.bson");

    auto asc = db.find(json::object(), moofile::FindOptions().sort("age")).to_vector();
    ASSERT(asc.size() == 4);
    ASSERT(asc[0]["_id"] == "b");
    ASSERT(asc[3]["_id"] == "c");

    auto desc = db.find(json::object(),
                        moofile::FindOptions().sort("age", true)).to_vector();
    ASSERT(desc[0]["_id"] == "c");
    ASSERT(desc[3]["_id"] == "b");
}

static void test_find_options_skip_limit() {
    TEST("FindOptions paginates with skip and limit");
    auto db = make_sortable("cxx_fo_page.bson");

    auto page = db.find(json::object(),
                        moofile::FindOptions().sort("age").skip(1).limit(2)).to_vector();
    ASSERT(page.size() == 2);
    ASSERT(page[0]["_id"] == "a"); /* b,a,d,c → skip 1, take 2 */
    ASSERT(page[1]["_id"] == "d");
}

static void test_find_options_with_filter() {
    TEST("FindOptions combines with a filter");
    auto db = make_sortable("cxx_fo_filt.bson");

    auto docs = db.find(json{{"dept", "ops"}},
                        moofile::FindOptions().sort("age").limit(1)).to_vector();
    ASSERT(docs.size() == 1);
    ASSERT(docs[0]["_id"] == "d");
}

static void test_find_options_group_agg() {
    TEST("FindOptions groups and aggregates");
    auto db = make_sortable("cxx_fo_group.bson");

    auto rows = db.find(json::object(),
                        moofile::FindOptions()
                            .group("dept")
                            .count()
                            .sum("pay")
                            .sort("dept")).to_vector();
    ASSERT(rows.size() == 2);
    /* The group key keeps its original type — a JSON string, not a quoted one. */
    ASSERT(rows[0]["dept"] == "eng");
    ASSERT(rows[0]["count"] == 2);
    ASSERT(rows[0]["sum_pay"] == 300);
    ASSERT(rows[1]["dept"] == "ops");
    ASSERT(rows[1]["sum_pay"] == 700);
}

static void test_find_options_rejects_bad_agg() {
    TEST("FindOptions surfaces an unknown agg function as an error");
    auto db = make_sortable("cxx_fo_badagg.bson");
    try {
        db.find(json::object(),
                moofile::FindOptions().group("dept").agg("median", "pay")).to_vector();
        FAIL("expected an unknown agg function to throw");
    } catch (const moofile::error& e) {
        ASSERT(std::string(e.what()).find("unknown agg func") != std::string::npos);
    }
}

static void test_find_one() {
    TEST("find_one returns optional document");
    moofile::Collection db(make_path("fo.bson"));
    db.insert({{"name", "Alice"}});

    auto doc = db.find_one({{"name", "Alice"}});
    ASSERT(doc.has_value());

    auto none = db.find_one({{"name", "Nobody"}});
    ASSERT(!none.has_value());
}

static void test_count() {
    TEST("count returns correct numbers");
    moofile::Collection db(make_path("cnt.bson"));
    ASSERT(db.count({}) == 0);
    db.insert({{"x", 1}});
    ASSERT(db.count({}) == 1);
    ASSERT(db.count({{"x", 1}}) == 1);
    ASSERT(db.count({{"x", 99}}) == 0);
}

static void test_exists() {
    TEST("exists returns correct boolean");
    moofile::Collection db(make_path("ex.bson"));
    ASSERT(!db.exists({{"x", 1}}));
    db.insert({{"x", 1}});
    ASSERT(db.exists({{"x", 1}}));
}

static void test_update_one() {
    TEST("update_one with $set");
    moofile::Collection db(make_path("up1.bson"));
    db.insert({{"_id", "a"}, {"name", "Alice"}, {"age", 30}});

    bool ok = db.update_one({{"_id", "a"}}, {{"age", 31}, {"city", "NYC"}});
    ASSERT(ok);

    auto doc = db.find_one({{"_id", "a"}});
    ASSERT(doc->at("age") == 31);
    ASSERT(doc->at("city") == "NYC");
}

static void test_update_one_no_match() {
    /* Matching nothing throws, as in Rust and Python — not a silent no-op. */
    TEST("update_one with no match throws");
    moofile::Collection db(make_path("up1nm.bson"));
    try {
        db.update_one({{"x", 99}}, {{"x", 1}});
        FAIL("expected update_one to throw when nothing matches");
    } catch (const moofile::error& e) {
        ASSERT(std::string(e.what()).find("no document matches") != std::string::npos);
    }
}

static void test_update_many() {
    TEST("update_many returns count");
    moofile::Collection db(make_path("up_m.bson"));
    db.insert_many({{{"s", "old"}}, {{"s", "old"}}, {{"s", "new"}}});

    auto n = db.update_many({{"s", "old"}}, {{"s", "updated"}});
    ASSERT(n == 2);
    ASSERT(db.count({{"s", "updated"}}) == 2);
}

static void test_replace_one() {
    TEST("replace_one preserves _id");
    moofile::Collection db(make_path("rep1.bson"));
    db.insert({{"_id", "a"}, {"old", "data"}});

    bool ok = db.replace_one({{"_id", "a"}}, {{"new", "data"}});
    ASSERT(ok);

    auto doc = db.find_one({{"_id", "a"}});
    ASSERT(doc->contains("new"));
    ASSERT(!doc->contains("old"));
    ASSERT((*doc)["_id"] == "a");
}

static void test_delete_one() {
    TEST("delete_one removes matching doc");
    moofile::Collection db(make_path("del1.bson"));
    db.insert({{"_id", "a"}});
    ASSERT(db.delete_one({{"_id", "a"}}));
    ASSERT(!db.exists({{"_id", "a"}}));
}

static void test_delete_many() {
    TEST("delete_many removes all matching");
    moofile::Collection db(make_path("del_m.bson"));
    db.insert_many({{{"tag", "x"}}, {{"tag", "x"}}, {{"tag", "y"}}});
    ASSERT(db.delete_many({{"tag", "x"}}) == 2);
    ASSERT(db.count({}) == 1);
}

static void test_vector_search() {
    TEST("vector_search returns (doc, score) pairs");
    moofile::Config cfg;
    cfg.vector_index("emb", 3);
    moofile::Collection db(make_path("vec.bson"), cfg);

    db.insert({{"_id", "near"}, {"emb", {1.0f, 0.0f, 0.0f}}});
    db.insert({{"_id", "far"}, {"emb", {0.0f, 0.0f, 1.0f}}});

    auto results = db.vector_search("emb", {1.0f, 0.0f, 0.0f}, 5).to_vector();
    ASSERT(results.size() == 2);
    ASSERT(results[0].first["_id"] == "near");
    ASSERT(results[0].second > results[1].second);
}

static void test_vector_search_with_filter() {
    TEST("vector_search with pre-filter");
    moofile::Config cfg;
    cfg.index("cat").vector_index("emb", 2);
    moofile::Collection db(make_path("vec_filt_cxx.bson"), cfg);

    db.insert({{"_id", "a"}, {"cat", "x"}, {"emb", {1.0f, 0.0f}}});
    db.insert({{"_id", "b"}, {"cat", "y"}, {"emb", {0.0f, 1.0f}}});

    auto results = db.vector_search("emb", {1.0f, 0.0f}, 5, {{"cat", "x"}}).to_vector();
    ASSERT(results.size() == 1);
    ASSERT(results[0].first["_id"] == "a");
}

static void test_text_search() {
    TEST("text_search returns (doc, score) pairs");
    moofile::Config cfg;
    cfg.text_index("content");
    moofile::Collection db(make_path("txt.bson"), cfg);

    db.insert({{"_id", "1"}, {"content", "machine learning"}});
    db.insert({{"_id", "2"}, {"content", "cooking"}});

    auto results = db.text_search("content", "learning", 5).to_vector();
    ASSERT(results.size() == 1);
    ASSERT(results[0].first["_id"] == "1");
}

static void test_hybrid_search() {
    TEST("hybrid_search fuses both rankers");
    moofile::Config cfg;
    cfg.text_index("content").vector_index("emb", 3);
    moofile::Collection db(make_path("hy.bson"), cfg);

    db.insert({{"_id", "a"}, {"content", "machine learning"}, {"emb", {1.0f, 0.0f, 0.0f}}});
    db.insert({{"_id", "b"}, {"content", "cooking"}, {"emb", {0.0f, 1.0f, 0.0f}}});

    auto results = db.hybrid_search("content", "emb", "learning", {1.0f, 0.0f, 0.0f}, 5).to_vector();
    ASSERT(results.size() == 2);
    /* "a" matches both rankers, should be first */
    ASSERT(results[0].first["_id"] == "a");
}

static void test_batch_commit() {
    TEST("batch commit applies writes atomically");
    moofile::Collection db(make_path("batch_c.bson"));

    {
        moofile::Collection::Batch batch(db);
        db.insert({{"_id", "a"}, {"v", 1}});
        db.insert({{"_id", "b"}, {"v", 2}});
        batch.commit();
    }

    ASSERT(db.count({}) == 2);
}

static void test_batch_rollback() {
    TEST("batch rollback discards writes");
    moofile::Collection db(make_path("batch_r.bson"));

    {
        moofile::Collection::Batch batch(db);
        db.insert({{"_id", "a"}, {"v", 1}});
        /* No commit — destructor rolls back */
    }

    ASSERT(db.count({}) == 0);
}

static void test_batch_exception_rollback() {
    TEST("exception in batch triggers automatic rollback");
    moofile::Collection db(make_path("batch_ex.bson"));

    try {
        moofile::Collection::Batch batch(db);
        db.insert({{"_id", "a"}, {"v", 1}});
        throw std::runtime_error("simulated failure");
    } catch (...) {
        /* destructor rolls back */
    }

    ASSERT(db.count({}) == 0);
}

static void test_stats() {
    TEST("stats returns valid JSON");
    moofile::Collection db(make_path("st.bson"));
    db.insert({{"x", 1}});
    auto s = db.stats();
    ASSERT(s.contains("documents"));
    ASSERT(s.contains("file_size_bytes"));
    ASSERT(s.contains("dead_ratio"));
}

static void test_compact() {
    TEST("compact reclaims space");
    moofile::Collection db(make_path("cp.bson"));
    db.insert_many({{{"x", 1}}, {{"x", 2}}});
    db.delete_one({{"x", 1}});
    
    auto before = db.stats();
    ASSERT(before["dead_records"] >= 1);
    
    db.compact();
    
    auto after = db.stats();
    ASSERT(after["dead_records"] == 0);
    ASSERT(after["documents"] == 1);
}

static void test_sync() {
    TEST("sync does not throw");
    moofile::Collection db(make_path("sy.bson"));
    db.insert({{"x", 1}});
    db.sync();
    ASSERT(db.count({}) == 1);
}

static void test_reindex() {
    TEST("reindex preserves data");
    moofile::Config cfg;
    cfg.index("email");
    moofile::Collection db(make_path("ri.bson"), cfg);
    db.insert({{"email", "a@test.com"}, {"v", 1}});
    db.reindex();
    ASSERT(db.count({}) == 1);
    ASSERT(db.exists({{"email", "a@test.com"}}));
}

static void test_error_on_readonly_write() {
    TEST("write on readonly throws moofile::error");
    try {
        moofile::Config cfg;
        cfg.set_readonly(true);
        moofile::Collection db(make_path("ro_err.bson"), cfg);
        db.insert({{"x", 1}});
        FAIL("should throw");
    } catch (const moofile::error&) { /* expected */ }
}

static void test_error_on_duplicate() {
    TEST("duplicate _id throws moofile::error");
    moofile::Collection db(make_path("dup_err.bson"));
    db.insert({{"_id", "k"}, {"v", 1}});
    try {
        db.insert({{"_id", "k"}, {"v", 2}});
        FAIL("should throw");
    } catch (const moofile::error&) { /* expected */ }
}

static void test_empty_cursor() {
    TEST("find on empty collection returns empty vector");
    moofile::Collection db(make_path("empty_c.bson"));
    auto docs = db.find({}).to_vector();
    ASSERT(docs.empty());
}

static void test_range_check_edge_cases() {
    TEST("negative values and zeros work correctly");
    moofile::Collection db(make_path("edge.bson"));
    db.insert({{"_id", "a"}, {"val", -10}});
    db.insert({{"_id", "b"}, {"val", 0}});
    db.insert({{"_id", "c"}, {"val", 10}});

    ASSERT(db.count({{"val", {{"$lt", 0}}}}) == 1); /* -10 */
    ASSERT(db.count({{"val", {{"$gte", 0}}}}) == 2); /* 0, 10 */
    ASSERT(db.count({{"val", {{"$eq", 0}}}}) == 1);
}

static void test_nested_documents() {
    TEST("nested JSON objects round-trip correctly");
    moofile::Collection db(make_path("nested.bson"));
    db.insert({{"_id", "n"}, {"meta", {{"key", "val"}, {"count", 3}}}});

    auto doc = db.find_one({{"_id", "n"}});
    ASSERT(doc.has_value());
    ASSERT((*doc)["meta"]["key"] == "val");
    ASSERT((*doc)["meta"]["count"] == 3);
}

/* ---------------------------------------------------------------------------
 * Main
 * --------------------------------------------------------------------------- */

int main() {
    srand((unsigned)time(nullptr));
    setup_temp_dir();

    std::cout << "MooFile C++ Wrapper Test Suite\n";
    std::cout << "==============================\n\n";

    /* Lifecycle & RAII */
    test_open_default();
    test_open_with_config();
    test_open_readonly();
    test_raii_close_on_destruction();
    test_move_constructor();

    /* Insert */
    test_insert_returns_doc();
    test_insert_duplicate_throws();
    test_insert_many();

    /* Query */
    test_find_all();
    test_find_filtered();
    test_find_comparison();
    test_find_logical();
    test_find_options_sort();
    test_find_options_skip_limit();
    test_find_options_with_filter();
    test_find_options_group_agg();
    test_find_options_rejects_bad_agg();
    test_find_one();
    test_count();
    test_exists();

    /* Update */
    test_update_one();
    test_update_one_no_match();
    test_update_many();
    test_replace_one();

    /* Delete */
    test_delete_one();
    test_delete_many();

    /* Search */
    test_vector_search();
    test_vector_search_with_filter();
    test_text_search();
    test_hybrid_search();

    /* Batch */
    test_batch_commit();
    test_batch_rollback();
    test_batch_exception_rollback();

    /* Utility */
    test_stats();
    test_compact();
    test_sync();
    test_reindex();

    /* Error handling */
    test_error_on_readonly_write();
    test_error_on_duplicate();

    /* Edge cases */
    test_empty_cursor();
    test_range_check_edge_cases();
    test_nested_documents();

    cleanup_temp_dir();

    std::cout << "\n======================\n";
    std::cout << "Tests:    " << g_tests_run << "\n";
    std::cout << "Passed:   " << (g_tests_run - g_tests_failed) << "\n";
    std::cout << "Failed:   " << g_tests_failed << "\n";

    return g_tests_failed > 0 ? 1 : 0;
}
