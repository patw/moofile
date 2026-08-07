package main

import (
	"fmt"
	"os"

	"github.com/patw/moofile-go/moofile"
)

func main() {
	tmpDir, _ := os.MkdirTemp("", "moofile-example-*")
	defer os.RemoveAll(tmpDir)

	fmt.Println("=== MooFile Go Examples ===\n")

	// -------------------------------------------------------------------
	// 1. Basic CRUD
	// -------------------------------------------------------------------
	func() {
		db, err := moofile.Open(tmpDir+"/contacts.bson", &moofile.Config{
			Indexes: []string{"email"},
		})
		if err != nil {
			panic(err)
		}
		defer db.Close()

		alice, _ := db.Insert(map[string]any{
			"name": "Alice", "email": "alice@example.com", "age": 30,
		})
		fmt.Printf("1. Inserted: %v (_id: %v)\n", alice["name"], alice["_id"])

		db.InsertMany([]map[string]any{
			{"name": "Bob", "email": "bob@example.com", "age": 25},
			{"name": "Carol", "email": "carol@example.com", "age": 35},
		})

		fmt.Printf("   Total: %d\n", must(db.Count(nil)))

		found, _ := db.FindOne(map[string]any{"email": "alice@example.com"})
		fmt.Printf("2. Found: %v, age %v\n", found["name"], found["age"])

		db.UpdateOne(
			map[string]any{"email": "alice@example.com"},
			map[string]any{"age": 31}, nil, nil,
		)
		updated, _ := db.FindOne(map[string]any{"email": "alice@example.com"})
		fmt.Printf("3. Updated age: %v\n", updated["age"])

		over30, _ := db.Find(map[string]any{"age": map[string]any{"$gte": 30}})
		fmt.Printf("4. Over 30: %d\n", len(over30))

		db.DeleteOne(map[string]any{"email": "bob@example.com"})
		fmt.Printf("5. After delete: %d\n", must(db.Count(nil)))
	}()

	// -------------------------------------------------------------------
	// 2. Vector Search
	// -------------------------------------------------------------------
	func() {
		db, _ := moofile.Open(tmpDir+"/vectors.bson", &moofile.Config{
			VectorIndexes: map[string]int{"embedding": 3},
		})
		defer db.Close()

		db.InsertMany([]map[string]any{
			{"_id": "a", "title": "ML Guide", "embedding": []float64{1, 0, 0}},
			{"_id": "b", "title": "Deep Learning", "embedding": []float64{0.5, 0.5, 0}},
			{"_id": "c", "title": "Cooking", "embedding": []float64{0, 0, 1}},
		})

		results, _ := db.VectorSearch("embedding", []float64{1, 0, 0}, 3, nil)
		fmt.Println("\n6. Vector Search:")
		for _, r := range results {
			fmt.Printf("   %v: score=%.4f\n", r.Doc["title"], r.Score)
		}
	}()

	// -------------------------------------------------------------------
	// 3. Text Search
	// -------------------------------------------------------------------
	func() {
		db, _ := moofile.Open(tmpDir+"/text.bson", &moofile.Config{
			TextIndexes: []string{"content"},
		})
		defer db.Close()

		db.InsertMany([]map[string]any{
			{"_id": "1", "content": "Machine learning is transforming AI"},
			{"_id": "2", "content": "Deep neural networks for ML"},
			{"_id": "3", "content": "Cooking recipes"},
		})

		results, _ := db.TextSearch("content", "machine learning", 5, nil)
		fmt.Println("\n7. Text Search:")
		for _, r := range results {
			fmt.Printf("   [%v] score=%.4f\n", r.Doc["_id"], r.Score)
		}
	}()

	// -------------------------------------------------------------------
	// 4. Batch Atomic Write
	// -------------------------------------------------------------------
	func() {
		db, _ := moofile.Open(tmpDir+"/batch.bson", nil)
		defer db.Close()

		db.Batch(func() error {
			db.Insert(map[string]any{"_id": "a", "amount": 100})
			db.Insert(map[string]any{"_id": "b", "amount": -50})
			return nil
		})

		fmt.Printf("\n8. Batch: %d transactions\n", must(db.Count(nil)))
	}()

	// -------------------------------------------------------------------
	// 5. Autoembedding (requires GGUF model)
	// -------------------------------------------------------------------
	fmt.Println("\n9. Autoembedding requires a GGUF model file.")
	fmt.Println("   See moofile.h documentation for auto_embed config.")

	fmt.Println("\n=== Done ===")
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
