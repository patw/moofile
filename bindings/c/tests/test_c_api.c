/**
 * test_c_api.c — Comprehensive test suite for the MooFile C API.
 *
 * Tests every public function in moofile.h across normal operation,
 * edge cases, error paths, and memory safety.
 *
 * Build: gcc -std=c11 -Wall -Wextra -o test_c_api test_c_api.c -L.. -lmoofile -I../include
 */

#include "moofile.h"

#include <assert.h>
#include <math.h>
#include <setjmp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ---------------------------------------------------------------------------
 * Test infrastructure
 * --------------------------------------------------------------------------- */

static int  g_tests_run   = 0;
static int  g_tests_failed = 0;
static char g_test_name[256];

#define TEST(name)   do { snprintf(g_test_name, sizeof(g_test_name), "%s", name); g_tests_run++; } while(0)
#define FAIL(msg)    do { fprintf(stderr, "  FAIL [%s] line %d: %s\n", g_test_name, __LINE__, msg); g_tests_failed++; } while(0)
#define ASSERT(cond) do { if (!(cond)) { FAIL(#cond); return; } } while(0)
#define ASSERT_STREQ(a,b) do { if (strcmp((a),(b)) != 0) { FAIL("expected '" a "' == '" b "'"); return; } } while(0)

/* Temp directory helper */
static char temp_dir[256];

static void setup_temp_dir(void) {
    snprintf(temp_dir, sizeof(temp_dir), "/tmp/moofile_test_%d_%d", getpid(), rand());
    mkdir(temp_dir, 0755);
}

static void cleanup_temp_dir(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", temp_dir);
    system(cmd);
}

static char* make_path(const char* name) {
    static char buf[512];
    snprintf(buf, sizeof(buf), "%s/%s", temp_dir, name);
    return buf;
}

/* Helper: check that a JSON string contains a key with a value */
static int json_has_key(const char* json_str, const char* key) {
    if (!json_str) return 0;
    char search[128];
    snprintf(search, sizeof(search), "\"%s\"", key);
    return strstr(json_str, search) != NULL;
}

/* Helper: extract an _id from a JSON result */
static char* extract_id(const char* json_str) {
    /* Find "_id":"..." */
    const char* p = strstr(json_str, "\"_id\":\"");
    if (!p) return NULL;
    p += 7; /* skip past "_id":" */
    const char* end = strchr(p, '"');
    if (!end) return NULL;
    size_t len = end - p;
    char* id = malloc(len + 1);
    memcpy(id, p, len);
    id[len] = '\0';
    return id;
}

/* ---------------------------------------------------------------------------
 * Lifecycle tests
 * --------------------------------------------------------------------------- */

static void test_open_default(void) {
    TEST("open with default config");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("default.bson"), NULL, &err);
    ASSERT(err == NULL);
    ASSERT(db != NULL);
    ASSERT(moofile_close(db, &err) == 0);
    ASSERT(err == NULL);
}

static void test_open_with_indexes(void) {
    TEST("open with indexes config");
    char* err = NULL;
    const char* config = "{\"indexes\":[\"email\",\"name\"],\"vector_indexes\":{\"emb\":3},\"text_indexes\":[\"content\"]}";
    MooFileCollection* db = moofile_open(make_path("indexed.bson"), config, &err);
    ASSERT(err == NULL);
    ASSERT(db != NULL);
    moofile_close(db, &err);
}

static void test_open_readonly(void) {
    TEST("open readonly rejects writes");
    char* err = NULL;
    /* Create writable first */
    MooFileCollection* db = moofile_open(make_path("ro.bson"), NULL, &err);
    ASSERT(db != NULL);
    moofile_close(db, &err);
    
    /* Open readonly */
    db = moofile_open(make_path("ro.bson"), "{\"readonly\":true}", &err);
    ASSERT(db != NULL);
    
    char* result = moofile_insert(db, "{\"x\":1}", &err);
    ASSERT(result == NULL);
    ASSERT(err != NULL);
    ASSERT(strstr(err, "read-only") != NULL);
    moofile_free_string(err);
    err = NULL;
    
    moofile_close(db, &err);
}

static void test_open_invalid_durability(void) {
    TEST("open with invalid durability returns error");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("bad_dur.bson"), "{\"durability\":\"bogus\"}", &err);
    ASSERT(db == NULL);
    ASSERT(err != NULL);
    moofile_free_string(err);
}

static void test_open_null_path(void) {
    TEST("open with null path returns error");
    char* err = NULL;
    MooFileCollection* db = moofile_open(NULL, NULL, &err);
    ASSERT(db == NULL);
    ASSERT(err != NULL);
    moofile_free_string(err);
}

static void test_close_null_handle(void) {
    TEST("close null handle returns error");
    char* err = NULL;
    ASSERT(moofile_close(NULL, &err) != 0);
    ASSERT(err != NULL);
    moofile_free_string(err);
}

static void test_persistence_across_opens(void) {
    TEST("data persists across close/reopen");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("persist.bson"), NULL, &err);
    ASSERT(db != NULL);
    moofile_insert(db, "{\"name\":\"Alice\"}", &err);
    ASSERT(err == NULL);
    moofile_close(db, &err);
    
    /* Reopen and count */
    db = moofile_open(make_path("persist.bson"), NULL, &err);
    ASSERT(db != NULL);
    int64_t n = moofile_count(db, "{}", &err);
    ASSERT(n == 1);
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Insert tests
 * --------------------------------------------------------------------------- */

static void test_insert_auto_id(void) {
    TEST("insert auto-generates _id");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("insert_auto.bson"), "{\"indexes\":[\"email\"]}", &err);
    ASSERT(db != NULL);
    
    char* result = moofile_insert(db, "{\"name\":\"Alice\",\"email\":\"a@test.com\"}", &err);
    ASSERT(err == NULL);
    ASSERT(result != NULL);
    ASSERT(json_has_key(result, "_id"));
    ASSERT(json_has_key(result, "name"));
    ASSERT(json_has_key(result, "email"));
    moofile_free_string(result);
    
    moofile_close(db, &err);
}

static void test_insert_custom_id(void) {
    TEST("insert with custom _id");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("insert_custom.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    char* result = moofile_insert(db, "{\"_id\":\"mykey\",\"v\":42}", &err);
    ASSERT(err == NULL);
    ASSERT(result != NULL);
    ASSERT(strstr(result, "\"_id\":\"mykey\"") != NULL);
    moofile_free_string(result);
    
    moofile_close(db, &err);
}

static void test_insert_duplicate_id(void) {
    TEST("insert duplicate _id returns error");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("dup.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    char* r1 = moofile_insert(db, "{\"_id\":\"same\",\"v\":1}", &err);
    ASSERT(r1 != NULL);
    moofile_free_string(r1);
    err = NULL;
    
    char* r2 = moofile_insert(db, "{\"_id\":\"same\",\"v\":2}", &err);
    ASSERT(r2 == NULL);
    ASSERT(err != NULL);
    ASSERT(strstr(err, "duplicate") != NULL || strstr(err, "Duplicate") != NULL);
    moofile_free_string(err);
    
    moofile_close(db, &err);
}

static void test_insert_many(void) {
    TEST("insert_many returns array of docs");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("insert_many.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    char* result = moofile_insert_many(db, "[{\"a\":1},{\"a\":2},{\"a\":3}]", &err);
    ASSERT(err == NULL);
    ASSERT(result != NULL);
    /* Should be a JSON array of 3 objects */
    ASSERT(result[0] == '[');
    moofile_free_string(result);
    
    ASSERT(moofile_count(db, "{}", &err) == 3);
    
    moofile_close(db, &err);
}

static void test_insert_many_invalid_json(void) {
    TEST("insert_many with invalid JSON returns error");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("insert_many_bad.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    char* result = moofile_insert_many(db, "not json array", &err);
    ASSERT(result == NULL);
    ASSERT(err != NULL);
    moofile_free_string(err);
    
    moofile_close(db, &err);
}

static void test_insert_non_string_id(void) {
    TEST("insert with non-string _id returns error");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("bad_id.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    /* Integer _id should fail */
    char* result = moofile_insert(db, "{\"_id\":42,\"v\":1}", &err);
    ASSERT(result == NULL);
    ASSERT(err != NULL);
    moofile_free_string(err);
    
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Query tests
 * --------------------------------------------------------------------------- */

static void test_find_all(void) {
    TEST("find with empty filter returns all docs");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("find_all.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"x\":1},{\"x\":2},{\"x\":3}]", &err);
    ASSERT(err == NULL);
    
    MooFileCursor* cur = moofile_find(db, "{}", &err);
    ASSERT(err == NULL);
    ASSERT(cur != NULL);
    
    int count = 0;
    char* doc;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) {
        ASSERT(err == NULL);
        count++;
        moofile_free_string(doc);
    }
    ASSERT(count == 3);
    
    moofile_cursor_free(cur);
    moofile_close(db, &err);
}

static void test_find_with_filter(void) {
    TEST("find with equality filter");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("find_eq.bson"), "{\"indexes\":[\"name\"]}", &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25},{\"name\":\"Alice\",\"age\":35}]", &err);
    ASSERT(err == NULL);
    
    MooFileCursor* cur = moofile_find(db, "{\"name\":\"Alice\"}", &err);
    ASSERT(cur != NULL);
    
    int count = 0;
    char* doc;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) {
        ASSERT(strstr(doc, "\"name\":\"Alice\"") != NULL);
        count++;
        moofile_free_string(doc);
    }
    ASSERT(count == 2);
    
    moofile_cursor_free(cur);
    moofile_close(db, &err);
}

static void test_find_comparison_ops(void) {
    TEST("find with comparison operators");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("find_cmp.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"age\":20},{\"age\":30},{\"age\":40},{\"age\":50}]", &err);
    ASSERT(err == NULL);
    
    /* $gt */
    MooFileCursor* cur = moofile_find(db, "{\"age\":{\"$gt\":30}}", &err);
    ASSERT(cur != NULL);
    int count = 0;
    char* doc;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 2); /* 40, 50 */
    moofile_cursor_free(cur);
    
    /* $gte */
    cur = moofile_find(db, "{\"age\":{\"$gte\":40}}", &err);
    count = 0;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 2); /* 40, 50 */
    moofile_cursor_free(cur);
    
    /* $lt */
    cur = moofile_find(db, "{\"age\":{\"$lt\":30}}", &err);
    count = 0;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 1); /* 20 */
    moofile_cursor_free(cur);
    
    /* $lte */
    cur = moofile_find(db, "{\"age\":{\"$lte\":30}}", &err);
    count = 0;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 2); /* 20, 30 */
    moofile_cursor_free(cur);
    
    /* Range */
    cur = moofile_find(db, "{\"age\":{\"$gte\":25,\"$lte\":45}}", &err);
    count = 0;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 3); /* 30, 40, ... 25 <= age <= 45 */
    moofile_cursor_free(cur);
    
    moofile_close(db, &err);
}

static void test_find_in_nin(void) {
    TEST("find with $in / $nin operators");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("find_in.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"color\":\"red\"},{\"color\":\"blue\"},{\"color\":\"green\"}]", &err);
    ASSERT(err == NULL);
    
    MooFileCursor* cur = moofile_find(db, "{\"color\":{\"$in\":[\"red\",\"blue\"]}}", &err);
    int count = 0;
    char* doc;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 2);
    moofile_cursor_free(cur);
    
    cur = moofile_find(db, "{\"color\":{\"$nin\":[\"red\",\"blue\"]}}", &err);
    count = 0;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 1); /* only green */
    moofile_cursor_free(cur);
    
    moofile_close(db, &err);
}

static void test_find_logical_ops(void) {
    TEST("find with $and / $or / $not operators");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("find_logic.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"status\":\"active\",\"age\":30},{\"status\":\"inactive\",\"age\":25},{\"status\":\"active\",\"age\":20}]", &err);
    
    /* $and */
    MooFileCursor* cur = moofile_find(db, "{\"$and\":[{\"status\":\"active\"},{\"age\":{\"$gt\":25}}]}", &err);
    int count = 0;
    char* doc;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 1);
    moofile_cursor_free(cur);
    
    /* $or */
    cur = moofile_find(db, "{\"$or\":[{\"status\":\"inactive\"},{\"age\":{\"$lt\":25}}]}", &err);
    count = 0;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 2);
    moofile_cursor_free(cur);
    
    /* $not */
    cur = moofile_find(db, "{\"$not\":{\"status\":\"active\"}}", &err);
    count = 0;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 1);
    moofile_cursor_free(cur);
    
    moofile_close(db, &err);
}

static void test_find_exists(void) {
    TEST("find with $exists operator");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("find_exists.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"a\":1,\"b\":2},{\"a\":3},{\"b\":4}]", &err);
    
    MooFileCursor* cur = moofile_find(db, "{\"b\":{\"$exists\":true}}", &err);
    int count = 0;
    char* doc;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 2);
    moofile_cursor_free(cur);
    
    cur = moofile_find(db, "{\"b\":{\"$exists\":false}}", &err);
    count = 0;
    while ((doc = moofile_cursor_next(cur, &err)) != NULL) { count++; moofile_free_string(doc); }
    ASSERT(count == 1);
    moofile_cursor_free(cur);
    
    moofile_close(db, &err);
}

static void test_find_no_matches(void) {
    TEST("find with no matches returns empty cursor");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("find_empty.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"x\":1}", &err);
    
    MooFileCursor* cur = moofile_find(db, "{\"x\":99}", &err);
    ASSERT(cur != NULL);
    char* doc = moofile_cursor_next(cur, &err);
    ASSERT(doc == NULL);
    moofile_cursor_free(cur);
    
    moofile_close(db, &err);
}

static void test_find_one_found(void) {
    TEST("find_one returns matching doc");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("fo_found.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"name\":\"Alice\",\"age\":30}", &err);
    
    char* result = moofile_find_one(db, "{\"name\":\"Alice\"}", &err);
    ASSERT(result != NULL);
    ASSERT(err == NULL);
    ASSERT(json_has_key(result, "name"));
    moofile_free_string(result);
    
    moofile_close(db, &err);
}

static void test_find_one_not_found(void) {
    TEST("find_one returns NULL when no match");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("fo_miss.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    char* result = moofile_find_one(db, "{\"name\":\"Nobody\"}", &err);
    ASSERT(result == NULL);
    ASSERT(err == NULL);
    
    moofile_close(db, &err);
}

static void test_count_all(void) {
    TEST("count with empty filter counts all");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("count_all.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    ASSERT(moofile_count(db, "{}", &err) == 0);
    moofile_insert(db, "{\"x\":1}", &err);
    ASSERT(moofile_count(db, "{}", &err) == 1);
    moofile_insert_many(db, "[{\"x\":2},{\"x\":3}]", &err);
    ASSERT(moofile_count(db, "{}", &err) == 3);
    
    moofile_close(db, &err);
}

static void test_count_filtered(void) {
    TEST("count with filter");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("count_filt.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"status\":\"a\"},{\"status\":\"b\"},{\"status\":\"a\"}]", &err);
    ASSERT(moofile_count(db, "{\"status\":\"a\"}", &err) == 2);
    ASSERT(moofile_count(db, "{\"status\":\"b\"}", &err) == 1);
    ASSERT(moofile_count(db, "{\"status\":\"c\"}", &err) == 0);
    
    moofile_close(db, &err);
}

static void test_exists_true(void) {
    TEST("exists returns 1 when doc found");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("ex_true.bson"), NULL, &err);
    ASSERT(db != NULL);
    moofile_insert(db, "{\"k\":\"v\"}", &err);
    ASSERT(moofile_exists(db, "{\"k\":\"v\"}", &err) == 1);
    moofile_close(db, &err);
}

static void test_exists_false(void) {
    TEST("exists returns 0 when doc not found");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("ex_false.bson"), NULL, &err);
    ASSERT(db != NULL);
    ASSERT(moofile_exists(db, "{\"k\":\"v\"}", &err) == 0);
    moofile_close(db, &err);
}

static void test_cursor_exhaustion(void) {
    TEST("cursor returns NULL after all docs consumed");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("cursor_ex.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"n\":1}", &err);
    
    MooFileCursor* cur = moofile_find(db, "{}", &err);
    char* d1 = moofile_cursor_next(cur, &err);
    ASSERT(d1 != NULL);
    moofile_free_string(d1);
    
    char* d2 = moofile_cursor_next(cur, &err);
    ASSERT(d2 == NULL); /* exhausted */
    
    /* Further calls also return NULL */
    char* d3 = moofile_cursor_next(cur, &err);
    ASSERT(d3 == NULL);
    
    moofile_cursor_free(cur);
    moofile_close(db, &err);
}

static void test_cursor_free_null(void) {
    TEST("cursor_free with NULL handle does not crash");
    moofile_cursor_free(NULL);
    /* If we get here, test passes */
    TEST("cursor_free null — no crash");
}

/* ---------------------------------------------------------------------------
 * Update tests
 * --------------------------------------------------------------------------- */

static void test_update_one_set(void) {
    TEST("update_one with $set");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("up_set.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"a\",\"name\":\"Alice\",\"age\":30}", &err);
    
    int r = moofile_update_one(db, "{\"_id\":\"a\"}", "{\"set\":{\"age\":31,\"city\":\"NYC\"}}", &err);
    ASSERT(err == NULL);
    ASSERT(r == 1);
    
    char* doc = moofile_find_one(db, "{\"_id\":\"a\"}", &err);
    ASSERT(strstr(doc, "\"age\":31") != NULL);
    ASSERT(strstr(doc, "\"city\":\"NYC\"") != NULL);
    ASSERT(strstr(doc, "\"name\":\"Alice\"") != NULL);
    moofile_free_string(doc);
    
    moofile_close(db, &err);
}

static void test_update_one_unset(void) {
    TEST("update_one with $unset");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("up_unset.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"a\",\"name\":\"Alice\",\"temp\":\"remove_me\"}", &err);
    
    int r = moofile_update_one(db, "{\"_id\":\"a\"}", "{\"unset\":[\"temp\"]}", &err);
    ASSERT(r == 1);
    
    char* doc = moofile_find_one(db, "{\"_id\":\"a\"}", &err);
    ASSERT(strstr(doc, "\"name\":\"Alice\"") != NULL);
    ASSERT(strstr(doc, "\"temp\"") == NULL); /* removed */
    moofile_free_string(doc);
    
    moofile_close(db, &err);
}

static void test_update_one_inc(void) {
    TEST("update_one with $inc");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("up_inc.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"a\",\"score\":10}", &err);
    
    int r = moofile_update_one(db, "{\"_id\":\"a\"}", "{\"inc\":{\"score\":5}}", &err);
    ASSERT(r == 1);
    
    char* doc = moofile_find_one(db, "{\"_id\":\"a\"}", &err);
    ASSERT(strstr(doc, "\"score\":15") != NULL);
    moofile_free_string(doc);
    
    moofile_close(db, &err);
}

static void test_update_one_no_match(void) {
    TEST("update_one with no match returns 0");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("up_nomatch.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    int r = moofile_update_one(db, "{\"x\":99}", "{\"set\":{\"x\":1}}", &err);
    ASSERT(r == 0);
    ASSERT(err == NULL);
    
    moofile_close(db, &err);
}

static void test_update_many(void) {
    TEST("update_many updates multiple docs");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("up_many.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"status\":\"old\",\"n\":1},{\"status\":\"old\",\"n\":2},{\"status\":\"new\",\"n\":3}]", &err);
    
    int64_t n = moofile_update_many(db, "{\"status\":\"old\"}", "{\"set\":{\"status\":\"updated\"}}", &err);
    ASSERT(err == NULL);
    ASSERT(n == 2);
    ASSERT(moofile_count(db, "{\"status\":\"updated\"}", &err) == 2);
    
    moofile_close(db, &err);
}

static void test_update_many_no_match(void) {
    TEST("update_many with no match returns 0");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("up_many_none.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    int64_t n = moofile_update_many(db, "{\"x\":99}", "{\"set\":{\"x\":1}}", &err);
    ASSERT(n == 0);
    ASSERT(err == NULL);
    
    moofile_close(db, &err);
}

static void test_replace_one(void) {
    TEST("replace_one replaces entire document preserving _id");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("rep_one.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"a\",\"old\":\"data\"}", &err);
    
    int r = moofile_replace_one(db, "{\"_id\":\"a\"}", "{\"new\":\"data\"}", &err);
    ASSERT(r == 1);
    
    char* doc = moofile_find_one(db, "{\"_id\":\"a\"}", &err);
    ASSERT(strstr(doc, "\"_id\":\"a\"") != NULL);
    ASSERT(strstr(doc, "\"new\":\"data\"") != NULL);
    ASSERT(strstr(doc, "\"old\"") == NULL); /* replaced */
    moofile_free_string(doc);
    
    moofile_close(db, &err);
}

static void test_replace_one_no_match(void) {
    TEST("replace_one with no match returns 0");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("rep_nomatch.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    int r = moofile_replace_one(db, "{\"_id\":\"nonexistent\"}", "{\"v\":1}", &err);
    ASSERT(r == 0);
    ASSERT(err == NULL);
    
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Delete tests
 * --------------------------------------------------------------------------- */

static void test_delete_one(void) {
    TEST("delete_one removes matching doc");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("del_one.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"a\",\"v\":1}", &err);
    moofile_insert(db, "{\"_id\":\"b\",\"v\":2}", &err);
    
    int r = moofile_delete_one(db, "{\"_id\":\"a\"}", &err);
    ASSERT(r == 1);
    ASSERT(moofile_count(db, "{}", &err) == 1);
    ASSERT(moofile_exists(db, "{\"_id\":\"a\"}", &err) == 0);
    
    moofile_close(db, &err);
}

static void test_delete_one_no_match(void) {
    TEST("delete_one with no match returns 0");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("del_none.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    int r = moofile_delete_one(db, "{\"x\":99}", &err);
    ASSERT(r == 0);
    ASSERT(err == NULL);
    
    moofile_close(db, &err);
}

static void test_delete_many(void) {
    TEST("delete_many removes all matching docs");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("del_many.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"tag\":\"x\"},{\"tag\":\"x\"},{\"tag\":\"y\"}]", &err);
    
    int64_t n = moofile_delete_many(db, "{\"tag\":\"x\"}", &err);
    ASSERT(n == 2);
    ASSERT(moofile_count(db, "{}", &err) == 1);
    
    moofile_close(db, &err);
}

static void test_delete_many_no_match(void) {
    TEST("delete_many with no match returns 0");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("del_many_none.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    int64_t n = moofile_delete_many(db, "{\"x\":99}", &err);
    ASSERT(n == 0);
    
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Vector search tests
 * --------------------------------------------------------------------------- */

static void test_vector_search_basic(void) {
    TEST("vector search returns (doc, score) ordered by similarity");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("vec_basic.bson"), "{\"vector_indexes\":{\"emb\":3}}", &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"near\",\"emb\":[1.0,0.0,0.0]}", &err);
    moofile_insert(db, "{\"_id\":\"mid\",\"emb\":[0.5,0.5,0.0]}", &err);
    moofile_insert(db, "{\"_id\":\"far\",\"emb\":[0.0,0.0,1.0]}", &err);
    
    MooFileSearchCursor* cur = moofile_vector_search(db, "{}", "emb", "[1.0,0.0,0.0]", 3, &err);
    ASSERT(cur != NULL);
    ASSERT(err == NULL);
    
    /* First result should be "near" (highest cosine sim to [1,0,0]) */
    char* first = moofile_search_cursor_next(cur, &err);
    ASSERT(first != NULL);
    ASSERT(strstr(first, "\"near\"") != NULL);
    moofile_free_string(first);
    
    /* Should get 3 results total */
    int count = 1;
    while (moofile_search_cursor_next(cur, &err) != NULL) count++;
    ASSERT(count == 3);
    
    moofile_search_cursor_free(cur);
    moofile_close(db, &err);
}

static void test_vector_search_with_filter(void) {
    TEST("vector search with pre-filter");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("vec_filt.bson"), "{\"vector_indexes\":{\"emb\":2},\"indexes\":[\"cat\"]}", &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"a\",\"cat\":\"x\",\"emb\":[1.0,0.0]}", &err);
    moofile_insert(db, "{\"_id\":\"b\",\"cat\":\"y\",\"emb\":[0.0,1.0]}", &err);
    moofile_insert(db, "{\"_id\":\"c\",\"cat\":\"x\",\"emb\":[0.9,0.1]}", &err);
    
    MooFileSearchCursor* cur = moofile_vector_search(db, "{\"cat\":\"x\"}", "emb", "[1.0,0.0]", 5, &err);
    ASSERT(cur != NULL);
    
    int count = 0;
    char* result;
    while ((result = moofile_search_cursor_next(cur, &err)) != NULL) {
        count++;
        ASSERT(strstr(result, "\"cat\":\"x\"") != NULL);
        moofile_free_string(result);
    }
    ASSERT(count == 2); /* only docs with cat=x */
    
    moofile_search_cursor_free(cur);
    moofile_close(db, &err);
}

static void test_vector_search_empty(void) {
    TEST("vector search with no matching docs returns empty cursor");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("vec_empty.bson"), "{\"vector_indexes\":{\"emb\":2}}", &err);
    ASSERT(db != NULL);
    
    MooFileSearchCursor* cur = moofile_vector_search(db, "{}", "emb", "[1.0,0.0]", 5, &err);
    ASSERT(cur != NULL);
    ASSERT(moofile_search_cursor_next(cur, &err) == NULL);
    
    moofile_search_cursor_free(cur);
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Text search tests
 * --------------------------------------------------------------------------- */

static void test_text_search_basic(void) {
    TEST("text search returns (doc, score) ordered by relevance");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("txt_basic.bson"), "{\"text_indexes\":[\"content\"]}", &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"1\",\"content\":\"machine learning is fascinating\"}", &err);
    moofile_insert(db, "{\"_id\":\"2\",\"content\":\"deep learning and neural networks\"}", &err);
    moofile_insert(db, "{\"_id\":\"3\",\"content\":\"cooking recipes\"}", &err);
    
    MooFileSearchCursor* cur = moofile_text_search(db, "{}", "content", "machine learning", 5, &err);
    ASSERT(cur != NULL);
    ASSERT(err == NULL);
    
    /* Should match docs 1 and 2 (both have "learning"), not 3 */
    int count = 0;
    char* result;
    while ((result = moofile_search_cursor_next(cur, &err)) != NULL) {
        count++;
        moofile_free_string(result);
    }
    ASSERT(count == 2);
    
    moofile_search_cursor_free(cur);
    moofile_close(db, &err);
}

static void test_text_search_with_filter(void) {
    TEST("text search with pre-filter");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("txt_filt.bson"), "{\"text_indexes\":[\"body\"],\"indexes\":[\"lang\"]}", &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"1\",\"lang\":\"en\",\"body\":\"machine learning\"}", &err);
    moofile_insert(db, "{\"_id\":\"2\",\"lang\":\"fr\",\"body\":\"apprentissage automatique\"}", &err);
    moofile_insert(db, "{\"_id\":\"3\",\"lang\":\"en\",\"body\":\"cooking\"}", &err);
    
    MooFileSearchCursor* cur = moofile_text_search(db, "{\"lang\":\"en\"}", "body", "learning", 5, &err);
    int count = 0;
    char* result;
    while ((result = moofile_search_cursor_next(cur, &err)) != NULL) {
        count++;
        ASSERT(strstr(result, "\"lang\":\"en\"") != NULL);
        moofile_free_string(result);
    }
    ASSERT(count == 1); /* only doc 1 matches both filter and search */
    
    moofile_search_cursor_free(cur);
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Hybrid search tests
 * --------------------------------------------------------------------------- */

static void test_hybrid_search_basic(void) {
    TEST("hybrid search fuses text + vector results");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("hybrid_basic.bson"),
        "{\"text_indexes\":[\"content\"],\"vector_indexes\":{\"emb\":3}}", &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"_id\":\"a\",\"content\":\"machine learning\",\"emb\":[1.0,0.0,0.0]}", &err);
    moofile_insert(db, "{\"_id\":\"b\",\"content\":\"deep learning\",\"emb\":[0.0,1.0,0.0]}", &err);
    moofile_insert(db, "{\"_id\":\"c\",\"content\":\"cooking\",\"emb\":[0.0,0.0,1.0]}", &err);
    
    MooFileSearchCursor* cur = moofile_hybrid_search(db, "{}", "content", "emb", "learning", "[1.0,0.0,0.0]", 3, &err);
    ASSERT(cur != NULL);
    ASSERT(err == NULL);
    
    int count = 0;
    char* result;
    while ((result = moofile_search_cursor_next(cur, &err)) != NULL) {
        count++;
        moofile_free_string(result);
    }
    ASSERT(count == 3); /* all docs should appear in at least one ranker */
    
    moofile_search_cursor_free(cur);
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Batch tests
 * --------------------------------------------------------------------------- */

static void test_batch_commit(void) {
    TEST("batch commit applies all writes atomically");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("batch_ok.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_batch_begin(db, &err);
    ASSERT(err == NULL);
    
    moofile_insert(db, "{\"_id\":\"a\",\"v\":1}", &err);
    ASSERT(err == NULL);
    moofile_insert(db, "{\"_id\":\"b\",\"v\":2}", &err);
    ASSERT(err == NULL);
    moofile_update_one(db, "{\"_id\":\"a\"}", "{\"set\":{\"v\":10}}", &err);
    ASSERT(err == NULL);
    
    /* Before commit, nothing visible */
    ASSERT(moofile_count(db, "{}", &err) == 0);
    
    moofile_batch_commit(db, &err);
    ASSERT(err == NULL);
    
    /* After commit, all visible */
    ASSERT(moofile_count(db, "{}", &err) == 2);
    char* doc = moofile_find_one(db, "{\"_id\":\"a\"}", &err);
    ASSERT(strstr(doc, "\"v\":10") != NULL);
    moofile_free_string(doc);
    
    moofile_close(db, &err);
}

static void test_batch_rollback(void) {
    TEST("batch rollback discards all buffered writes");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("batch_roll.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_batch_begin(db, &err);
    moofile_insert(db, "{\"_id\":\"a\",\"v\":1}", &err);
    moofile_batch_rollback(db, &err);
    
    ASSERT(moofile_count(db, "{}", &err) == 0);
    ASSERT(moofile_exists(db, "{\"_id\":\"a\"}", &err) == 0);
    
    moofile_close(db, &err);
}

static void test_batch_double_begin(void) {
    TEST("batch double begin returns error");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("batch_dbl.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    ASSERT(moofile_batch_begin(db, &err) == 0);
    ASSERT(moofile_batch_begin(db, &err) != 0); /* should fail */
    ASSERT(err != NULL);
    moofile_free_string(err);
    err = NULL;
    
    moofile_batch_rollback(db, &err); /* clean up */
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Utility tests
 * --------------------------------------------------------------------------- */

static void test_stats(void) {
    TEST("stats returns document counts and file info");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("stats.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"x\":1},{\"x\":2}]", &err);
    
    char* stats = moofile_stats(db, &err);
    ASSERT(stats != NULL);
    ASSERT(strstr(stats, "\"documents\":2") != NULL);
    ASSERT(strstr(stats, "\"file_size_bytes\"") != NULL);
    moofile_free_string(stats);
    
    moofile_close(db, &err);
}

static void test_compact(void) {
    TEST("compact reclaims space from deleted docs");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("compact.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert_many(db, "[{\"x\":1},{\"x\":2}]", &err);
    moofile_delete_one(db, "{\"x\":1}", &err);
    
    char* before = moofile_stats(db, &err);
    ASSERT(strstr(before, "\"dead_records\":1") != NULL);
    moofile_free_string(before);
    
    ASSERT(moofile_compact(db, &err) == 0);
    
    char* after = moofile_stats(db, &err);
    ASSERT(strstr(after, "\"dead_records\":0") != NULL);
    ASSERT(strstr(after, "\"documents\":1") != NULL);
    moofile_free_string(after);
    
    moofile_close(db, &err);
}

static void test_sync(void) {
    TEST("sync returns success");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("sync.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"x\":1}", &err);
    ASSERT(moofile_sync(db, &err) == 0);
    ASSERT(err == NULL);
    
    moofile_close(db, &err);
}

static void test_reindex(void) {
    TEST("reindex rebuilds indexes without data loss");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("reindex.bson"), "{\"indexes\":[\"email\"]}", &err);
    ASSERT(db != NULL);
    
    moofile_insert(db, "{\"email\":\"a@test.com\",\"v\":1}", &err);
    moofile_insert(db, "{\"email\":\"b@test.com\",\"v\":2}", &err);
    
    ASSERT(moofile_reindex(db, &err) == 0);
    ASSERT(err == NULL);
    ASSERT(moofile_count(db, "{}", &err) == 2);
    ASSERT(moofile_exists(db, "{\"email\":\"a@test.com\"}", &err) == 1);
    
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Error handling tests
 * --------------------------------------------------------------------------- */

static void test_error_null_handle_on_all_ops(void) {
    TEST("all functions reject null handle with error");
    char* err = NULL;
    
    ASSERT(moofile_insert(NULL, "{}", &err) == NULL);     ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_insert_many(NULL, "[]", &err) == NULL); ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_find(NULL, "{}", &err) == NULL);        ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_find_one(NULL, "{}", &err) == NULL);    ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_count(NULL, "{}", &err) == -1);         ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_exists(NULL, "{}", &err) == -1);        ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_update_one(NULL, "{}", "{}", &err) != 0); ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_update_many(NULL, "{}", "{}", &err) == -1); ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_replace_one(NULL, "{}", "{}", &err) != 0);  ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_delete_one(NULL, "{}", &err) != 0);    ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_delete_many(NULL, "{}", &err) == -1);  ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_vector_search(NULL, "{}", "f", "[1]", 5, &err) == NULL); ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_text_search(NULL, "{}", "f", "q", 5, &err) == NULL);    ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_hybrid_search(NULL, "{}", "tf", "vf", "q", "[1]", 5, &err) == NULL); ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_batch_begin(NULL, &err) != 0);           ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_batch_commit(NULL, &err) != 0);          ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_batch_rollback(NULL, &err) != 0);        ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_compact(NULL, &err) != 0);               ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_sync(NULL, &err) != 0);                  ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_reindex(NULL, &err) != 0);               ASSERT(err != NULL); moofile_free_string(err); err = NULL;
    ASSERT(moofile_stats(NULL, &err) == NULL);              ASSERT(err != NULL); moofile_free_string(err); err = NULL;
}

static void test_error_bad_json_filter(void) {
    TEST("invalid filter JSON returns error");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("bad_json.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    MooFileCursor* cur = moofile_find(db, "not valid json", &err);
    ASSERT(cur == NULL);
    ASSERT(err != NULL);
    moofile_free_string(err);
    
    moofile_close(db, &err);
}

static void test_error_free_string_null(void) {
    TEST("free_string with NULL does not crash");
    moofile_free_string(NULL);
    TEST("free_string null — no crash");
}

static void test_error_search_cursor_free_null(void) {
    TEST("search_cursor_free with NULL does not crash");
    moofile_search_cursor_free(NULL);
    TEST("search_cursor_free null — no crash");
}

/* ---------------------------------------------------------------------------
 * Round-trip type preservation tests
 * --------------------------------------------------------------------------- */

static void test_roundtrip_types(void) {
    TEST("BSON types round-trip through C API");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("roundtrip.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    /* Insert with various types */
    char* result = moofile_insert(db,
        "{\"_id\":\"types\",\"s\":\"hello\",\"i\":42,\"f\":3.14,\"b\":true,\"n\":null,\"arr\":[1,2,3]}", &err);
    ASSERT(err == NULL);
    moofile_free_string(result);
    
    /* Read back */
    char* doc = moofile_find_one(db, "{\"_id\":\"types\"}", &err);
    ASSERT(doc != NULL);
    ASSERT(strstr(doc, "\"s\":\"hello\"") != NULL);
    ASSERT(strstr(doc, "\"i\":42") != NULL);
    ASSERT(strstr(doc, "\"f\":3.14") != NULL);
    ASSERT(strstr(doc, "\"b\":true") != NULL);
    ASSERT(strstr(doc, "\"n\":null") != NULL);
    ASSERT(strstr(doc, "\"arr\":[1,2,3]") != NULL);
    moofile_free_string(doc);
    
    moofile_close(db, &err);
}

static void test_empty_collection_ops(void) {
    TEST("operations on empty collection work correctly");
    char* err = NULL;
    MooFileCollection* db = moofile_open(make_path("empty_coll.bson"), NULL, &err);
    ASSERT(db != NULL);
    
    ASSERT(moofile_count(db, "{}", &err) == 0);
    ASSERT(moofile_exists(db, "{\"x\":1}", &err) == 0);
    
    MooFileCursor* cur = moofile_find(db, "{}", &err);
    ASSERT(cur != NULL);
    ASSERT(moofile_cursor_next(cur, &err) == NULL);
    moofile_cursor_free(cur);
    
    ASSERT(moofile_find_one(db, "{}", &err) == NULL);
    
    moofile_close(db, &err);
}

/* ---------------------------------------------------------------------------
 * Main
 * --------------------------------------------------------------------------- */

int main(void) {
    srand((unsigned)time(NULL));
    setup_temp_dir();
    
    printf("MooFile C API Test Suite\n");
    printf("=======================\n\n");
    
    /* Lifecycle */
    test_open_default();
    test_open_with_indexes();
    test_open_readonly();
    test_open_invalid_durability();
    test_open_null_path();
    test_close_null_handle();
    test_persistence_across_opens();
    
    /* Insert */
    test_insert_auto_id();
    test_insert_custom_id();
    test_insert_duplicate_id();
    test_insert_many();
    test_insert_many_invalid_json();
    test_insert_non_string_id();
    
    /* Query */
    test_find_all();
    test_find_with_filter();
    test_find_comparison_ops();
    test_find_in_nin();
    test_find_logical_ops();
    test_find_exists();
    test_find_no_matches();
    test_find_one_found();
    test_find_one_not_found();
    test_count_all();
    test_count_filtered();
    test_exists_true();
    test_exists_false();
    test_cursor_exhaustion();
    test_cursor_free_null();
    
    /* Update */
    test_update_one_set();
    test_update_one_unset();
    test_update_one_inc();
    test_update_one_no_match();
    test_update_many();
    test_update_many_no_match();
    test_replace_one();
    test_replace_one_no_match();
    
    /* Delete */
    test_delete_one();
    test_delete_one_no_match();
    test_delete_many();
    test_delete_many_no_match();
    
    /* Vector search */
    test_vector_search_basic();
    test_vector_search_with_filter();
    test_vector_search_empty();
    
    /* Text search */
    test_text_search_basic();
    test_text_search_with_filter();
    
    /* Hybrid search */
    test_hybrid_search_basic();
    
    /* Batch */
    test_batch_commit();
    test_batch_rollback();
    test_batch_double_begin();
    
    /* Utility */
    test_stats();
    test_compact();
    test_sync();
    test_reindex();
    
    /* Error handling */
    test_error_null_handle_on_all_ops();
    test_error_bad_json_filter();
    test_error_free_string_null();
    test_error_search_cursor_free_null();
    
    /* Round-trip */
    test_roundtrip_types();
    test_empty_collection_ops();
    
    /* Cleanup */
    cleanup_temp_dir();
    
    printf("\n====================\n");
    printf("Tests:    %d\n", g_tests_run);
    printf("Passed:   %d\n", g_tests_run - g_tests_failed);
    printf("Failed:   %d\n", g_tests_failed);
    
    return g_tests_failed > 0 ? 1 : 0;
}
