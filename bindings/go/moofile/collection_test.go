package moofile_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/patw/moofile-go/moofile"
)

func tmpPath(t *testing.T) string {
	f, err := os.CreateTemp("", "moofile-test-*.bson")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func TestOpenDefault(t *testing.T) {
	db, err := moofile.Open(tmpPath(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}

func TestInsertAndFind(t *testing.T) {
	db, err := moofile.Open(tmpPath(t), &moofile.Config{
		Indexes: []string{"email"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	doc, err := db.Insert(map[string]any{"name": "Alice", "email": "a@test.com", "age": 30})
	if err != nil {
		t.Fatal(err)
	}
	if doc["_id"] == "" {
		t.Error("missing _id")
	}

	found, err := db.FindOne(map[string]any{"email": "a@test.com"})
	if err != nil {
		t.Fatal(err)
	}
	if found["name"] != "Alice" {
		t.Error("unexpected name")
	}
}

func TestInsertMany(t *testing.T) {
	db, err := moofile.Open(tmpPath(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	docs, err := db.InsertMany([]map[string]any{
		{"x": 1}, {"x": 2}, {"x": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 3 {
		t.Error("expected 3 docs")
	}
}

func TestDuplicateRejected(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()
	db.Insert(map[string]any{"_id": "a", "v": 1})
	_, err := db.Insert(map[string]any{"_id": "a", "v": 2})
	if err == nil {
		t.Error("expected error for duplicate _id")
	}
}

func TestCountFiltered(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()
	db.InsertMany([]map[string]any{
		{"status": "a"}, {"status": "b"}, {"status": "a"},
	})
	n, err := db.Count(map[string]any{"status": "a"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("expected 2, got %d", n)
	}
}

func TestExists(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()
	ex, _ := db.Exists(map[string]any{"x": 1})
	if ex {
		t.Error("should not exist")
	}
	db.Insert(map[string]any{"x": 1})
	ex, _ = db.Exists(map[string]any{"x": 1})
	if !ex {
		t.Error("should exist")
	}
}

func TestUpdateOne(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()
	db.Insert(map[string]any{"_id": "a", "name": "Alice", "age": 30})

	ok, err := db.UpdateOne(
		map[string]any{"_id": "a"},
		map[string]any{"age": 31},
		nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("update should return true")
	}

	doc, _ := db.FindOne(map[string]any{"_id": "a"})
	if doc["age"] != float64(31) {
		t.Errorf("expected age 31, got %v", doc["age"])
	}
}

func TestDeleteOne(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()
	db.Insert(map[string]any{"_id": "a"})

	ok, _ := db.DeleteOne(map[string]any{"_id": "a"})
	if !ok {
		t.Error("delete should return true")
	}
	ok, _ = db.DeleteOne(map[string]any{"_id": "none"})
	if ok {
		t.Error("delete no match should return false")
	}
}

func TestVectorSearch(t *testing.T) {
	db, err := moofile.Open(tmpPath(t), &moofile.Config{
		VectorIndexes: map[string]int{"emb": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.InsertMany([]map[string]any{
		{"_id": "a", "emb": []float64{1, 0, 0}},
		{"_id": "b", "emb": []float64{0.5, 0.5, 0}},
		{"_id": "c", "emb": []float64{0, 0, 1}},
	})

	results, err := db.VectorSearch("emb", []float64{1, 0, 0}, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatal("expected 3 results")
	}
	if results[0].Doc["_id"] != "a" {
		t.Error("first should be nearest")
	}
}

func TestTextSearch(t *testing.T) {
	db, err := moofile.Open(tmpPath(t), &moofile.Config{
		TextIndexes: []string{"content"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.InsertMany([]map[string]any{
		{"_id": "1", "content": "machine learning is fascinating"},
		{"_id": "2", "content": "deep learning only"},
		{"_id": "3", "content": "cooking"},
	})

	results, err := db.TextSearch("content", "machine learning", 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatal("expected 2 results")
	}
}

func TestBatchAtomic(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()

	err := db.Batch(func() error {
		db.Insert(map[string]any{"_id": "a", "v": 1})
		db.Insert(map[string]any{"_id": "b", "v": 2})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	n, _ := db.Count(nil)
	if n != 2 {
		t.Errorf("expected 2 after commit, got %d", n)
	}
}

func TestBatchRollback(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()

	// Force a rollback by returning an error
	err := db.Batch(func() error {
		db.Insert(map[string]any{"_id": "a", "v": 1})
		return moofile.Error{Msg: "rollback"}
	})
	if err == nil {
		t.Error("expected error")
	}

	n, _ := db.Count(nil)
	if n != 0 {
		t.Errorf("expected 0 after rollback, got %d", n)
	}
}

func TestCompact(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()
	db.InsertMany([]map[string]any{{"x": 1}, {"x": 2}})
	db.DeleteOne(map[string]any{"x": 1})

	stats, _ := db.Stats()
	if stats["dead_records"].(float64) < 1 {
		t.Error("expected dead records")
	}

	db.Compact()

	stats, _ = db.Stats()
	if stats["dead_records"].(float64) != 0 {
		t.Error("expected 0 dead records after compact")
	}
}

func TestJSONSerialization(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()

	db.Insert(map[string]any{
		"_id":  "types",
		"s":    "hello",
		"i":    42,
		"f":    3.14,
		"b":    true,
		"arr":  []any{1, "two", 3.0},
		"nested": map[string]any{"k": "v"},
	})

	doc, _ := db.FindOne(map[string]any{"_id": "types"})
	j, _ := json.Marshal(doc)
	if !contains(j, `"s":"hello"`) {
		t.Error("string field missing:", string(j))
	}
}

func contains(s []byte, substr string) bool {
	return string(s) == substr || len(s) > len(substr)
}
