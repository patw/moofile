// Package moofile provides a Go binding for the MooFile embedded document store.
//
// It calls the C shared library (libmoofile.so) via cgo.
// Documents and configuration are passed as JSON strings.
//
// Usage:
//
//     import "github.com/patw/moofile-go/moofile"
//
//     db, err := moofile.Open("data.bson", &moofile.Config{
//         Indexes: []string{"email"},
//     })
//     if err != nil { log.Fatal(err) }
//     defer db.Close()
//
//     doc, err := db.Insert(map[string]any{"name": "Alice", "age": 30})
//     results, err := db.Find(map[string]any{"age": map[string]any{"$gt": 25}})
//
// All errors returned from the C library are surfaced as *moofile.Error.

package moofile

/*
#cgo LDFLAGS: -lmoofile -lm
#cgo CFLAGS: -I${SRCDIR}/../../../bindings/c/include
#include "moofile.h"
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// Error represents a MooFile error returned from the C library.
type Error struct {
	Msg string
}

func (e *Error) Error() string { return e.Msg }

func newError(errPtr **C.char) error {
	if errPtr == nil || *errPtr == nil {
		return nil
	}
	msg := C.GoString(*errPtr)
	C.moofile_free_string(*errPtr)
	return &Error{Msg: msg}
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// AutoEmbedConfig configures on-device embedding for a source text field.
type AutoEmbedConfig struct {
	Model       string `json:"model"`
	Target      string `json:"target"`
	Dims        int    `json:"dims,omitempty"`
	Precision   string `json:"precision,omitempty"`
	Normalize   *bool  `json:"normalize,omitempty"`
	QueryPrefix string `json:"query_prefix,omitempty"`
	DocPrefix   string `json:"doc_prefix,omitempty"`
}

// Config configures a MooFile collection on open.
type Config struct {
	Indexes       []string                   `json:"indexes,omitempty"`
	VectorIndexes map[string]int             `json:"vector_indexes,omitempty"`
	TextIndexes   []string                   `json:"text_indexes,omitempty"`
	AutoEmbed     map[string]AutoEmbedConfig `json:"auto_embed,omitempty"`
	Readonly      bool                       `json:"readonly,omitempty"`
	Durability    string                     `json:"durability,omitempty"`
	ModelCacheDir string                     `json:"model_cache_dir,omitempty"`
}

func (c *Config) toJSON() string {
	if c == nil {
		return "{}"
	}
	b, _ := json.Marshal(c)
	return string(b)
}

// ---------------------------------------------------------------------------
// SearchResult
// ---------------------------------------------------------------------------

// SearchResult holds a (document, score) pair from a search cursor.
type SearchResult struct {
	Doc   map[string]any
	Score float64
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

// Collection is a handle to an open MooFile database.
type Collection struct {
	mu     sync.Mutex
	handle *C.MooFileCollection
}

// Open opens a MooFile collection. The file is created if it does not exist.
func Open(path string, config *Config) (*Collection, error) {
	configJSON := config.toJSON()
	cPath := C.CString(path)
	cConfig := C.CString(configJSON)
	defer C.free(unsafe.Pointer(cPath))
	defer C.free(unsafe.Pointer(cConfig))

	var errPtr *C.char
	handle := C.moofile_open(cPath, cConfig, &errPtr)
	if err := newError(&errPtr); err != nil {
		return nil, err
	}
	if handle == nil {
		return nil, &Error{Msg: "moofile_open returned null"}
	}

	return &Collection{handle: handle}, nil
}

// Close closes the collection and frees native resources.
func (c *Collection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil
	}
	var errPtr *C.char
	C.moofile_close(c.handle, &errPtr)
	err := newError(&errPtr)
	c.handle = nil
	return err
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

// ccall1 calls a C function that takes (handle, *) and returns int.
func (c *Collection) ccall1(fn func(*C.MooFileCollection, **C.char) C.int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return &Error{Msg: "collection is closed"}
	}
	var errPtr *C.char
	fn(c.handle, &errPtr)
	return newError(&errPtr)
}

// ccallStr calls a C function that returns a C string.
func (c *Collection) ccallStr(fn func(*C.MooFileCollection, **C.char) *C.char) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return "", &Error{Msg: "collection is closed"}
	}
	var errPtr *C.char
	result := fn(c.handle, &errPtr)
	if err := newError(&errPtr); err != nil {
		return "", err
	}
	if result == nil {
		return "", nil
	}
	s := C.GoString(result)
	C.moofile_free_string(result)
	return s, nil
}

// ccallCur calls a find-like function and returns parsed documents.
func (c *Collection) ccallCur(fn func(*C.MooFileCollection, **C.char) *C.MooFileCursor) ([]map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, &Error{Msg: "collection is closed"}
	}
	var errPtr *C.char
	cursor := fn(c.handle, &errPtr)
	if err := newError(&errPtr); err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, nil
	}
	defer C.moofile_cursor_free(cursor)

	var docs []map[string]any
	for {
		var nextErr *C.char
		s := C.moofile_cursor_next(cursor, &nextErr)
		if err := newError(&nextErr); err != nil {
			return nil, err
		}
		if s == nil {
			break
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(C.GoString(s)), &doc); err != nil {
			C.moofile_free_string(s)
			return nil, err
		}
		C.moofile_free_string(s)
		docs = append(docs, doc)
	}
	return docs, nil
}

// ccallSearch calls a search function and returns (doc, score) pairs.
func (c *Collection) ccallSearch(fn func(*C.MooFileCollection, **C.char) *C.MooFileSearchCursor) ([]SearchResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, &Error{Msg: "collection is closed"}
	}
	var errPtr *C.char
	cursor := fn(c.handle, &errPtr)
	if err := newError(&errPtr); err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, nil
	}
	defer C.moofile_search_cursor_free(cursor)

	var results []SearchResult
	for {
		var nextErr *C.char
		s := C.moofile_search_cursor_next(cursor, &nextErr)
		if err := newError(&nextErr); err != nil {
			return nil, err
		}
		if s == nil {
			break
		}
		var pair []json.RawMessage
		if err := json.Unmarshal([]byte(C.GoString(s)), &pair); err != nil {
			C.moofile_free_string(s)
			return nil, err
		}
		C.moofile_free_string(s)
		if len(pair) >= 2 {
			var doc map[string]any
			var score float64
			json.Unmarshal(pair[0], &doc)
			json.Unmarshal(pair[1], &score)
			results = append(results, SearchResult{Doc: doc, Score: score})
		}
	}
	return results, nil
}

// -----------------------------------------------------------------------
// Insert
// -----------------------------------------------------------------------

func (c *Collection) Insert(doc map[string]any) (map[string]any, error) {
	b, _ := json.Marshal(doc)
	s, err := c.ccallStr(func(h *C.MooFileCollection, e **C.char) *C.char {
		return C.moofile_insert(h, C.CString(string(b)), e)
	})
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Collection) InsertMany(docs []map[string]any) ([]map[string]any, error) {
	b, _ := json.Marshal(docs)
	s, err := c.ccallStr(func(h *C.MooFileCollection, e **C.char) *C.char {
		return C.moofile_insert_many(h, C.CString(string(b)), e)
	})
	if err != nil {
		return nil, err
	}
	var result []map[string]any
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return nil, err
	}
	return result, nil
}

// -----------------------------------------------------------------------
// Query
// -----------------------------------------------------------------------

func (c *Collection) Find(filter map[string]any) ([]map[string]any, error) {
	return c.ccallCur(func(h *C.MooFileCollection, e **C.char) *C.MooFileCursor {
		return C.moofile_find(h, C.CString(string(jsonOrEmpty(filter))), e)
	})
}

func (c *Collection) FindOne(filter map[string]any) (map[string]any, error) {
	s, err := c.ccallStr(func(h *C.MooFileCollection, e **C.char) *C.char {
		return C.moofile_find_one(h, C.CString(string(jsonOrEmpty(filter))), e)
	})
	if err != nil || s == "" {
		return nil, err
	}
	var doc map[string]any
	json.Unmarshal([]byte(s), &doc)
	return doc, nil
}

func (c *Collection) Count(filter map[string]any) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errPtr *C.char
	n := int64(C.moofile_count(c.handle, C.CString(string(jsonOrEmpty(filter))), &errPtr))
	return n, newError(&errPtr)
}

func (c *Collection) Exists(filter map[string]any) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errPtr *C.char
	r := C.moofile_exists(c.handle, C.CString(string(jsonOrEmpty(filter))), &errPtr)
	return r == 1, newError(&errPtr)
}

// -----------------------------------------------------------------------
// Update
// -----------------------------------------------------------------------

func (c *Collection) UpdateOne(where, setValues map[string]any, unsetFields []string, incValues map[string]any) (bool, error) {
	update := make(map[string]any)
	if len(setValues) > 0 {
		update["set"] = setValues
	}
	if len(unsetFields) > 0 {
		update["unset"] = unsetFields
	}
	if len(incValues) > 0 {
		update["inc"] = incValues
	}
	bWhere, _ := json.Marshal(where)
	bUpdate, _ := json.Marshal(update)

	c.mu.Lock()
	defer c.mu.Unlock()
	var errPtr *C.char
	r := C.moofile_update_one(c.handle, C.CString(string(bWhere)), C.CString(string(bUpdate)), &errPtr)
	return r == 1, newError(&errPtr)
}

func (c *Collection) UpdateMany(where, setValues map[string]any, unsetFields []string, incValues map[string]any) (int64, error) {
	update := make(map[string]any)
	if len(setValues) > 0 {
		update["set"] = setValues
	}
	if len(unsetFields) > 0 {
		update["unset"] = unsetFields
	}
	if len(incValues) > 0 {
		update["inc"] = incValues
	}
	bWhere, _ := json.Marshal(where)
	bUpdate, _ := json.Marshal(update)

	c.mu.Lock()
	defer c.mu.Unlock()
	var errPtr *C.char
	n := int64(C.moofile_update_many(c.handle, C.CString(string(bWhere)), C.CString(string(bUpdate)), &errPtr))
	return n, newError(&errPtr)
}

func (c *Collection) ReplaceOne(where, replacement map[string]any) (bool, error) {
	bWhere, _ := json.Marshal(where)
	bRepl, _ := json.Marshal(replacement)
	c.mu.Lock()
	defer c.mu.Unlock()
	var errPtr *C.char
	r := C.moofile_replace_one(c.handle, C.CString(string(bWhere)), C.CString(string(bRepl)), &errPtr)
	return r == 1, newError(&errPtr)
}

// -----------------------------------------------------------------------
// Delete
// -----------------------------------------------------------------------

func (c *Collection) DeleteOne(where map[string]any) (bool, error) {
	b, _ := json.Marshal(where)
	c.mu.Lock()
	defer c.mu.Unlock()
	var errPtr *C.char
	r := C.moofile_delete_one(c.handle, C.CString(string(b)), &errPtr)
	return r == 1, newError(&errPtr)
}

func (c *Collection) DeleteMany(where map[string]any) (int64, error) {
	b, _ := json.Marshal(where)
	c.mu.Lock()
	defer c.mu.Unlock()
	var errPtr *C.char
	n := int64(C.moofile_delete_many(c.handle, C.CString(string(b)), &errPtr))
	return n, newError(&errPtr)
}

// -----------------------------------------------------------------------
// Search
// -----------------------------------------------------------------------

func (c *Collection) VectorSearch(field string, queryVector []float64, limit int, filter map[string]any) ([]SearchResult, error) {
	bFilter := jsonOrEmpty(filter)
	return c.ccallSearch(func(h *C.MooFileCollection, e **C.char) *C.MooFileSearchCursor {
		return C.moofile_vector_search(h,
			C.CString(string(bFilter)), C.CString(field), C.CString(jsonString(queryVector)),
			C.int(limit), e)
	})
}

func (c *Collection) TextSearch(field, query string, limit int, filter map[string]any) ([]SearchResult, error) {
	bFilter := jsonOrEmpty(filter)
	return c.ccallSearch(func(h *C.MooFileCollection, e **C.char) *C.MooFileSearchCursor {
		return C.moofile_text_search(h, C.CString(string(bFilter)), C.CString(field), C.CString(query), C.int(limit), e)
	})
}

func (c *Collection) HybridSearch(textField, vectorField, queryText string, queryVector []float64, limit int, filter map[string]any) ([]SearchResult, error) {
	bFilter := jsonOrEmpty(filter)
	var qvStr string
	if queryVector != nil {
		qvStr = jsonString(queryVector)
	}
	return c.ccallSearch(func(h *C.MooFileCollection, e **C.char) *C.MooFileSearchCursor {
		var qvPtr *C.char
		if qvStr != "" {
			qvPtr = C.CString(qvStr)
		}
		return C.moofile_hybrid_search(h,
			C.CString(string(bFilter)), C.CString(textField), C.CString(vectorField),
			C.CString(queryText), qvPtr, C.int(limit), e)
	})
}

func (c *Collection) Semantic(sourceField, queryText string, limit int, filter map[string]any) ([]SearchResult, error) {
	bFilter := jsonOrEmpty(filter)
	return c.ccallSearch(func(h *C.MooFileCollection, e **C.char) *C.MooFileSearchCursor {
		return C.moofile_semantic_search(h,
			C.CString(string(bFilter)), C.CString(sourceField), C.CString(queryText),
			C.int(limit), e)
	})
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// -----------------------------------------------------------------------
// Batch
// -----------------------------------------------------------------------

// Batch runs fn inside an atomic batch context.
func (c *Collection) Batch(fn func() error) error {
	c.mu.Lock()
	var errPtr *C.char
	if int(C.moofile_batch_begin(c.handle, &errPtr)) != 0 {
		c.mu.Unlock()
		return newError(&errPtr)
	}
	c.mu.Unlock()

	var batchErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				batchErr = fmt.Errorf("panic: %v", r)
			}
		}()
		batchErr = fn()
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if batchErr != nil {
		C.moofile_batch_rollback(c.handle, &errPtr)
		return batchErr
	}
	if int(C.moofile_batch_commit(c.handle, &errPtr)) != 0 {
		return newError(&errPtr)
	}
	return nil
}

// -----------------------------------------------------------------------
// Utility
// -----------------------------------------------------------------------

func (c *Collection) Stats() (map[string]any, error) {
	s, err := c.ccallStr(func(h *C.MooFileCollection, e **C.char) *C.char {
		return C.moofile_stats(h, e)
	})
	if err != nil {
		return nil, err
	}
	var stats map[string]any
	json.Unmarshal([]byte(s), &stats)
	return stats, nil
}

func (c *Collection) Compact() error {
	return c.ccall1(func(h *C.MooFileCollection, e **C.char) C.int {
		return C.moofile_compact(h, e)
	})
}

func (c *Collection) Sync() error {
	return c.ccall1(func(h *C.MooFileCollection, e **C.char) C.int {
		return C.moofile_sync(h, e)
	})
}

func (c *Collection) Reindex() error {
	return c.ccall1(func(h *C.MooFileCollection, e **C.char) C.int {
		return C.moofile_reindex(h, e)
	})
}

func jsonOrEmpty(m map[string]any) []byte {
	if m == nil {
		return []byte("{}")
	}
	b, _ := json.Marshal(m)
	return b
}
