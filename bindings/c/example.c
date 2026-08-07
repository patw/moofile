/**
 * example.c — MooFile C API usage examples.
 *
 * Build: gcc -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -o example example.c \\
 *       -L../target/release -lmoofile -I../include
 * Run:   LD_LIBRARY_PATH=../target/release ./example
 */

#define _POSIX_C_SOURCE 200809L
#include "moofile.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main() {
    char tmp_dir[] = "/tmp/moofile-example-XXXXXX";
    mkdtemp(tmp_dir);

    char path[512];

    printf("=== MooFile C Examples ===\n\n");

    // 1. Basic CRUD
    {
        snprintf(path, sizeof(path), "%s/contacts.bson", tmp_dir);
        char* err = NULL;
        MooFileCollection* db = moofile_open(path,
            "{\"indexes\":[\"email\"]}", &err);
        if (!db) { printf("Error: %s\n", err); return 1; }

        // Insert
        char* doc = moofile_insert(db,
            "{\"name\":\"Alice\",\"email\":\"alice@example.com\",\"age\":30}", &err);
        printf("1. Inserted: Alice (_id: %s)\n", strstr(doc, "\"_id\":\"") ? "..." : "?");
        moofile_free_string(doc);

        // Insert many
        moofile_insert_many(db,
            "[{\"name\":\"Bob\",\"email\":\"bob@example.com\",\"age\":25},"
            "{\"name\":\"Carol\",\"email\":\"carol@example.com\",\"age\":35}]", &err);
        printf("   Total: %ld\n", moofile_count(db, "{}", &err));

        // Find one
        doc = moofile_find_one(db, "{\"email\":\"alice@example.com\"}", &err);
        printf("2. Found: %s\n", strstr(doc, "\"name\":\"Alice\"") ? "Alice, age 30" : "?");
        moofile_free_string(doc);

        // Update
        moofile_update_one(db, "{\"email\":\"alice@example.com\"}",
            "{\"set\":{\"age\":31}}", &err);
        doc = moofile_find_one(db, "{\"email\":\"alice@example.com\"}", &err);
        printf("3. Updated age: %s\n", strstr(doc, "\"age\":31") ? "31" : "?");
        moofile_free_string(doc);

        // Filter
        MooFileCursor* cur = moofile_find(db, "{\"age\":{\"$gte\":30}}", &err);
        int count = 0;
        while (moofile_cursor_next(cur, &err)) count++;
        moofile_cursor_free(cur);
        printf("4. Over 30: %d\n", count);

        // Delete
        moofile_delete_one(db, "{\"email\":\"bob@example.com\"}", &err);
        printf("5. After delete: %ld\n", moofile_count(db, "{}", &err));

        moofile_close(db, &err);
    }

    // 2. Sorting, paging, and aggregation
    {
        snprintf(path, sizeof(path), "%s/sales.bson", tmp_dir);
        char* err = NULL;
        MooFileCollection* db = moofile_open(path, NULL, &err);
        if (!db) { printf("Error: %s\n", err); return 1; }

        moofile_insert_many(db,
            "[{\"rep\":\"Alice\",\"region\":\"east\",\"amount\":100},"
            " {\"rep\":\"Bob\",\"region\":\"east\",\"amount\":250},"
            " {\"rep\":\"Carol\",\"region\":\"west\",\"amount\":175},"
            " {\"rep\":\"Dan\",\"region\":\"west\",\"amount\":300},"
            " {\"rep\":\"Erin\",\"region\":\"west\",\"amount\":125}]", &err);

        // Sort descending, take the top 3
        printf("\n6. Top 3 sales:\n");
        MooFileCursor* cur = moofile_find_ex(db, "{}",
            "{\"sort\":{\"field\":\"amount\",\"desc\":true},\"limit\":3}", &err);
        char* row;
        while ((row = moofile_cursor_next(cur, &err)) != NULL) {
            printf("   %s\n", row);
            moofile_free_string(row);
        }
        moofile_cursor_free(cur);

        // Group and aggregate
        printf("   Totals by region:\n");
        cur = moofile_find_ex(db, "{}",
            "{\"group\":\"region\","
            " \"agg\":[{\"func\":\"count\"},"
            "         {\"func\":\"sum\",\"field\":\"amount\"}],"
            " \"sort\":\"region\"}", &err);
        while ((row = moofile_cursor_next(cur, &err)) != NULL) {
            printf("     %s\n", row);
            moofile_free_string(row);
        }
        moofile_cursor_free(cur);

        moofile_close(db, &err);
    }

    // 3. Vector Search
    {
        snprintf(path, sizeof(path), "%s/vectors.bson", tmp_dir);
        char* err = NULL;
        MooFileCollection* db = moofile_open(path,
            "{\"vector_indexes\":{\"embedding\":3}}", &err);

        moofile_insert_many(db,
            "[{\"_id\":\"a\",\"title\":\"ML Guide\",\"embedding\":[1.0,0.0,0.0]},"
            "{\"_id\":\"b\",\"title\":\"Deep Learning\",\"embedding\":[0.5,0.5,0.0]},"
            "{\"_id\":\"c\",\"title\":\"Cooking\",\"embedding\":[0.0,0.0,1.0]}]", &err);

        MooFileSearchCursor* sc = moofile_vector_search(db, "{}", "embedding",
            "[1.0,0.0,0.0]", 3, &err);
        char* result;
        printf("\n7. Vector Search:\n");
        while ((result = moofile_search_cursor_next(sc, &err)) != NULL) {
            // result is [doc_json, score]
            printf("   %s\n", result);
            moofile_free_string(result);
        }
        moofile_search_cursor_free(sc);
        moofile_close(db, &err);
    }

    // 4. Text Search
    {
        snprintf(path, sizeof(path), "%s/text.bson", tmp_dir);
        char* err = NULL;
        MooFileCollection* db = moofile_open(path,
            "{\"text_indexes\":[\"content\"]}", &err);

        moofile_insert_many(db,
            "[{\"_id\":\"1\",\"content\":\"Machine learning is transforming AI\"},"
            "{\"_id\":\"2\",\"content\":\"Deep neural networks for ML\"},"
            "{\"_id\":\"3\",\"content\":\"Cooking recipes\"}]", &err);

        MooFileSearchCursor* sc = moofile_text_search(db, "{}", "content",
            "machine learning", 5, &err);
        printf("\n8. Text Search:\n");
        char* result;
        while ((result = moofile_search_cursor_next(sc, &err)) != NULL) {
            printf("   %s\n", result);
            moofile_free_string(result);
        }
        moofile_search_cursor_free(sc);
        moofile_close(db, &err);
    }

    // 5. Batch
    {
        snprintf(path, sizeof(path), "%s/batch.bson", tmp_dir);
        char* err = NULL;
        MooFileCollection* db = moofile_open(path, NULL, &err);

        moofile_batch_begin(db, &err);
        moofile_insert(db, "{\"_id\":\"a\",\"amount\":100}", &err);
        moofile_insert(db, "{\"_id\":\"b\",\"amount\":-50}", &err);
        moofile_batch_commit(db, &err);

        printf("\n9. Batch: %ld transactions\n", moofile_count(db, "{}", &err));
        moofile_close(db, &err);
    }

    // Cleanup
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", tmp_dir);
    system(cmd);

    printf("\n=== Done ===\n");
    return 0;
}
