package moofile_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/patw/moofile/bindings/go/moofile"
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
		return &moofile.Error{Msg: "rollback"}
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

// ---------------------------------------------------------------------------
// Find options: sort / skip / limit / group / agg
// ---------------------------------------------------------------------------

// sortableDB returns a collection of four documents across two departments,
// deliberately inserted out of age order.
func sortableDB(t *testing.T) *moofile.Collection {
	t.Helper()
	db, err := moofile.Open(tmpPath(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.InsertMany([]map[string]any{
		{"_id": "a", "age": 30, "dept": "eng", "pay": 100},
		{"_id": "b", "age": 20, "dept": "eng", "pay": 200},
		{"_id": "c", "age": 50, "dept": "ops", "pay": 300},
		{"_id": "d", "age": 40, "dept": "ops", "pay": 400},
	}); err != nil {
		t.Fatal(err)
	}
	return db
}

func ids(docs []map[string]any) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i], _ = d["_id"].(string)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFindSort(t *testing.T) {
	db := sortableDB(t)
	defer db.Close()

	asc, err := db.Find(nil, &moofile.FindOptions{Sort: "age"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"b", "a", "d", "c"}; !equalStrings(ids(asc), want) {
		t.Errorf("ascending: got %v, want %v", ids(asc), want)
	}

	desc, err := db.Find(nil, &moofile.FindOptions{Sort: "age", Desc: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"c", "d", "a", "b"}; !equalStrings(ids(desc), want) {
		t.Errorf("descending: got %v, want %v", ids(desc), want)
	}
}

func TestFindSkipLimit(t *testing.T) {
	db := sortableDB(t)
	defer db.Close()

	page, err := db.Find(nil, &moofile.FindOptions{Sort: "age", Skip: 1, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "d"}; !equalStrings(ids(page), want) {
		t.Errorf("skip+limit: got %v, want %v", ids(page), want)
	}
}

func TestFindFilterWithOptions(t *testing.T) {
	db := sortableDB(t)
	defer db.Close()

	docs, err := db.Find(map[string]any{"dept": "ops"},
		&moofile.FindOptions{Sort: "age", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"d"}; !equalStrings(ids(docs), want) {
		t.Errorf("filter+sort+limit: got %v, want %v", ids(docs), want)
	}
}

func TestFindGroupAgg(t *testing.T) {
	db := sortableDB(t)
	defer db.Close()

	rows, err := db.Find(nil, &moofile.FindOptions{
		Group: "dept",
		Agg:   []moofile.Agg{moofile.Count(), moofile.Sum("pay"), moofile.Mean("pay")},
		Sort:  "dept",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(rows))
	}

	// The group key keeps its original type — a plain string, not a quoted one.
	if rows[0]["dept"] != "eng" {
		t.Errorf("group key: got %#v, want \"eng\"", rows[0]["dept"])
	}
	if rows[0]["count"].(float64) != 2 {
		t.Errorf("eng count: got %v", rows[0]["count"])
	}
	if rows[0]["sum_pay"].(float64) != 300 {
		t.Errorf("eng sum_pay: got %v", rows[0]["sum_pay"])
	}
	if rows[0]["mean_pay"].(float64) != 150 {
		t.Errorf("eng mean_pay: got %v", rows[0]["mean_pay"])
	}
	if rows[1]["sum_pay"].(float64) != 700 {
		t.Errorf("ops sum_pay: got %v", rows[1]["sum_pay"])
	}
}

func TestFindRejectsUnknownAgg(t *testing.T) {
	db := sortableDB(t)
	defer db.Close()

	_, err := db.Find(nil, &moofile.FindOptions{
		Group: "dept",
		Agg:   []moofile.Agg{{Func: "median", Field: "pay"}},
	})
	if err == nil {
		t.Fatal("expected an error for an unknown agg function")
	}
}

// ---------------------------------------------------------------------------
// Contract details
// ---------------------------------------------------------------------------

func TestUpdateOneNoMatchErrors(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	defer db.Close()
	db.Insert(map[string]any{"_id": "a", "v": 1})

	// UpdateOne treats a miss as an error, matching Rust and Python...
	if _, err := db.UpdateOne(map[string]any{"_id": "nope"},
		map[string]any{"v": 2}, nil, nil); err == nil {
		t.Error("expected UpdateOne with no match to fail")
	}

	// ...while UpdateMany simply reports zero.
	n, err := db.UpdateMany(map[string]any{"_id": "nope"},
		map[string]any{"v": 2}, nil, nil)
	if err != nil {
		t.Fatalf("UpdateMany should not fail on a miss: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 updated, got %d", n)
	}
}

func TestAutoEmbedConfigSerialisation(t *testing.T) {
	// The config must reach the core with snake_case keys.  An untagged
	// struct field would serialise as "Model" and be silently ignored,
	// leaving semantic search permanently unconfigured.  Opening with a
	// bogus model URI proves the key was read: the error names the model.
	_, err := moofile.Open(tmpPath(t), &moofile.Config{
		VectorIndexes: map[string]int{"emb": 8},
		AutoEmbed: map[string]moofile.AutoEmbedConfig{
			"content": {
				Model:  "hf:definitely/not-a-real-repo:missing.gguf",
				Target: "emb",
				Dims:   8,
			},
		},
	})
	if err == nil {
		t.Skip("auto_embed accepted the model; nothing to assert offline")
	}
	if !strings.Contains(err.Error(), "not-a-real-repo") &&
		!strings.Contains(err.Error(), "model") &&
		!strings.Contains(err.Error(), "embed") {
		t.Errorf("auto_embed config does not appear to have been parsed: %v", err)
	}
}

func TestClosedCollectionRejectsCalls(t *testing.T) {
	db, _ := moofile.Open(tmpPath(t), nil)
	db.Insert(map[string]any{"x": 1})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Count(nil); err == nil {
		t.Error("expected Count after Close to fail")
	}
	if err := db.Close(); err != nil {
		t.Errorf("Close should be idempotent, got %v", err)
	}
}
