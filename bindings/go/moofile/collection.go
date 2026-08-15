// Package moofile provides a Go binding for the MooFile embedded document store.
//
// It calls the C shared library (libmoofile.so) via cgo.  Documents and
// configuration cross the FFI boundary as JSON strings.
//
// Usage:
//
//	import "github.com/patw/moofile/bindings/go/moofile"
//
//	db, err := moofile.Open("data.bson", &moofile.Config{
//	    Indexes: []string{"email"},
//	})
//	if err != nil { log.Fatal(err) }
//	defer db.Close()
//
//	doc, err := db.Insert(map[string]any{"name": "Alice", "age": 30})
//	if err != nil { log.Fatal(err) }
//
//	results, err := db.Find(map[string]any{"age": map[string]any{"$gt": 25}}, nil)
//
// Build requirements: the shared library must exist before `go build` runs:
//
//	cargo build -p moofile-c --release
//
// The cgo directives below point at the in-repo target/release directory and
// bake an rpath so the binary finds libmoofile without LD_LIBRARY_PATH.  When
// vendoring this package elsewhere, override with CGO_CFLAGS / CGO_LDFLAGS.
package moofile

/*
#cgo CFLAGS: -I${SRCDIR}/../../c/include
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -lmoofile -lm -Wl,-rpath,${SRCDIR}/../../../target/release
#include "moofile.h"
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// Document is a JSON-shaped MooFile document. It is also used for filters.
type Document map[string]any

// Filter is a Document interpreted as a MongoDB-style match expression.
type Filter = Document

// Error represents a MooFile error returned from the C library.
type Error struct {
	Msg string
}

func (e *Error) Error() string { return e.Msg }

// newError converts a C error string into a Go error, freeing the C string.
// Returns nil when errPtr is NULL (the success case).
func newError(errPtr *C.char) error {
	if errPtr == nil {
		return nil
	}
	msg := C.GoString(errPtr)
	C.moofile_free_string(errPtr)
	return &Error{Msg: msg}
}

// ErrClosed is returned by every method once Close has been called.
var ErrClosed = &Error{Msg: "collection is closed"}

// ---------------------------------------------------------------------------
// C string helper
// ---------------------------------------------------------------------------

// cstrings collects C strings allocated for one call so they can all be freed
// together.  C.CString mallocs; without this every call leaks its arguments.
type cstrings struct {
	ptrs []*C.char
}

// new allocates a NUL-terminated copy of s, owned by the arena.
func (a *cstrings) new(s string) *C.char {
	p := C.CString(s)
	a.ptrs = append(a.ptrs, p)
	return p
}

// newJSON marshals v and allocates the result. Marshalling errors are
// reported rather than silently producing "null".
func (a *cstrings) newJSON(v any) (*C.char, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("moofile: cannot marshal argument: %w", err)
	}
	return a.new(string(b)), nil
}

// newDoc allocates the JSON form of a document, filter, or update map.
//
// A nil map must become `{}`, not `null`: a nil map boxed into `any` is not
// an untyped nil, so json.Marshal would emit "null" and the C layer would
// reject it with "expected JSON object at top level" — which is exactly what
// `db.Count(nil)` should mean, namely "no filter".
func (a *cstrings) newDoc(m map[string]any) (*C.char, error) {
	if m == nil {
		return a.new("{}"), nil
	}
	return a.newJSON(m)
}

// free releases every string in the arena.  Always call via defer.
func (a *cstrings) free() {
	for _, p := range a.ptrs {
		C.free(unsafe.Pointer(p))
	}
	a.ptrs = nil
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// AutoEmbedConfig configures on-device embedding for a source text field.
//
// The JSON tags matter: the C layer reads lower-case snake_case keys, so a
// field without a tag would be serialised as "Model" and silently ignored.
type AutoEmbedConfig struct {
	Model       string `json:"model"`               // fastembed model id, e.g. "BAAI/bge-small-en-v1.5"
	Target      string `json:"target"`              // Target vector field name
	Dims        int    `json:"dims,omitempty"`      // Embedding dimensions (default 384)
	Precision   string `json:"precision,omitempty"` // "f32", "int8", "uint8", "binary"
	Normalize   *bool  `json:"normalize,omitempty"` // L2-normalize (default true)
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
	Durability    string                     `json:"durability,omitempty"` // "none", "os" (default), "fsync"
	ModelCacheDir string                     `json:"model_cache_dir,omitempty"`
}

func (c *Config) toJSON() (string, error) {
	if c == nil {
		return "{}", nil
	}
	b, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("moofile: cannot marshal config: %w", err)
	}
	return string(b), nil
}

// ---------------------------------------------------------------------------
// Find options
// ---------------------------------------------------------------------------

// Agg is a single aggregation over a group.  Func is one of "count", "sum",
// "mean" (alias "avg"), "min", "max", "collect", "first", "last".  Every
// function except "count" requires a Field.
//
// Output fields are named "count", "sum_<field>", "mean_<field>", and so on.
type Agg struct {
	Func  string `json:"func"`
	Field string `json:"field,omitempty"`
}

// Count aggregates the number of documents per group.
func Count() Agg { return Agg{Func: "count"} }

// Sum totals a numeric field per group.
func Sum(field string) Agg { return Agg{Func: "sum", Field: field} }

// Mean averages a numeric field per group.
func Mean(field string) Agg { return Agg{Func: "mean", Field: field} }

// Min takes the smallest value of a field per group.
func Min(field string) Agg { return Agg{Func: "min", Field: field} }

// Max takes the largest value of a field per group.
func Max(field string) Agg { return Agg{Func: "max", Field: field} }

// Collect gathers every value of a field per group into an array.
func Collect(field string) Agg { return Agg{Func: "collect", Field: field} }

// First takes the first value of a field per group.
func First(field string) Agg { return Agg{Func: "first", Field: field} }

// Last takes the last value of a field per group.
func Last(field string) Agg { return Agg{Func: "last", Field: field} }

// sortSpec is the wire form of the sort option.
type sortSpec struct {
	Field string `json:"field"`
	Desc  bool   `json:"desc"`
}

// FindOptions carries the query-builder stages for Find.
//
// Stages apply in the order: filter → group/agg → sort → skip → limit.
//
//	db.Find(nil, &moofile.FindOptions{Sort: "age", Desc: true, Limit: 10})
//	db.Find(nil, &moofile.FindOptions{
//	    Group: "dept",
//	    Agg:   []moofile.Agg{moofile.Count(), moofile.Sum("pay")},
//	})
type FindOptions struct {
	Sort  string // Field to sort by; empty means unsorted
	Desc  bool   // Sort descending
	Skip  int    // Skip the first N results
	Limit int    // Return at most N results; 0 means no limit
	Group string // Group by this field
	Agg   []Agg  // Aggregations to compute per group
}

func (o *FindOptions) toJSON() (string, error) {
	if o == nil {
		return "{}", nil
	}
	m := make(map[string]any)
	if o.Sort != "" {
		m["sort"] = sortSpec{Field: o.Sort, Desc: o.Desc}
	}
	if o.Skip > 0 {
		m["skip"] = o.Skip
	}
	if o.Limit > 0 {
		m["limit"] = o.Limit
	}
	if o.Group != "" {
		m["group"] = o.Group
	}
	if len(o.Agg) > 0 {
		m["agg"] = o.Agg
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("moofile: cannot marshal find options: %w", err)
	}
	return string(b), nil
}

// ---------------------------------------------------------------------------
// SearchResult
// ---------------------------------------------------------------------------

// SearchResult holds a (document, score) pair from a search.
type SearchResult struct {
	Doc   map[string]any
	Score float64
}

// Update describes the supported MooFile update operations. Use it with
// UpdateOneWith or UpdateManyWith to avoid positional nil arguments.
type Update struct {
	Set   Document
	Unset []string
	Inc   Document
}

// SearchOptions controls a search and its optional MongoDB-style pre-filter.
type SearchOptions struct {
	Limit  int
	Filter Filter
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

// Collection is a handle to an open MooFile database.  It is safe for
// concurrent use: every method serialises on an internal mutex, on top of the
// locking the Rust core already does across processes.
type Collection struct {
	mu     sync.Mutex
	handle *C.MooFileCollection
	path   string
}

// Open opens a MooFile collection, creating the file if it does not exist.
// Pass nil for config to accept the defaults.
func Open(path string, config *Config) (*Collection, error) {
	configJSON, err := config.toJSON()
	if err != nil {
		return nil, err
	}

	var arena cstrings
	defer arena.free()
	cPath := arena.new(path)
	cConfig := arena.new(configJSON)

	var errPtr *C.char
	handle := C.moofile_open(cPath, cConfig, &errPtr)
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	if handle == nil {
		return nil, &Error{Msg: "moofile_open returned null"}
	}

	c := &Collection{handle: handle, path: path}
	// Backstop for callers who forget Close; Close remains the documented way.
	runtime.SetFinalizer(c, func(c *Collection) { _ = c.Close() })
	return c, nil
}

// Path returns the file this collection was opened from.
func (c *Collection) Path() string { return c.path }

// Close closes the collection and releases native resources.  Idempotent.
func (c *Collection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil
	}
	var errPtr *C.char
	C.moofile_close(c.handle, &errPtr)
	c.handle = nil
	runtime.SetFinalizer(c, nil)
	return newError(errPtr)
}

// -----------------------------------------------------------------------
// Insert
// -----------------------------------------------------------------------

// Insert adds one document and returns it with _id populated.
func (c *Collection) Insert(doc map[string]any) (map[string]any, error) {
	var arena cstrings
	defer arena.free()
	cDoc, err := arena.newDoc(doc)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, ErrClosed
	}

	var errPtr *C.char
	result := C.moofile_insert(c.handle, cDoc, &errPtr)
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	s := takeString(result)

	var out map[string]any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("moofile: cannot decode inserted document: %w", err)
	}
	return out, nil
}

// InsertMany adds several documents and returns them with _ids populated.
func (c *Collection) InsertMany(docs []map[string]any) ([]map[string]any, error) {
	var arena cstrings
	defer arena.free()
	cDocs, err := arena.newJSON(docs)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, ErrClosed
	}

	var errPtr *C.char
	result := C.moofile_insert_many(c.handle, cDocs, &errPtr)
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	s := takeString(result)

	var out []map[string]any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("moofile: cannot decode inserted documents: %w", err)
	}
	return out, nil
}

// takeString copies an owned C string into Go and frees it.
func takeString(p *C.char) string {
	if p == nil {
		return ""
	}
	s := C.GoString(p)
	C.moofile_free_string(p)
	return s
}

// -----------------------------------------------------------------------
// Query
// -----------------------------------------------------------------------

// Find returns every document matching filter.  Pass nil for filter to match
// everything, and nil for options to skip sorting/paging/aggregation.
func (c *Collection) Find(filter map[string]any, options *FindOptions) ([]map[string]any, error) {
	optsJSON, err := options.toJSON()
	if err != nil {
		return nil, err
	}

	var arena cstrings
	defer arena.free()
	cFilter, err := arena.newDoc(filter)
	if err != nil {
		return nil, err
	}
	cOpts := arena.new(optsJSON)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, ErrClosed
	}

	var errPtr *C.char
	var cursor *C.MooFileCursor
	if options == nil {
		cursor = C.moofile_find(c.handle, cFilter, &errPtr)
	} else {
		cursor = C.moofile_find_ex(c.handle, cFilter, cOpts, &errPtr)
	}
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, &Error{Msg: "find returned a null cursor"}
	}
	defer C.moofile_cursor_free(cursor)

	docs := []map[string]any{}
	for {
		var nextErr *C.char
		raw := C.moofile_cursor_next(cursor, &nextErr)
		if err := newError(nextErr); err != nil {
			return nil, err
		}
		if raw == nil {
			break
		}
		s := takeString(raw)
		var doc map[string]any
		if err := json.Unmarshal([]byte(s), &doc); err != nil {
			return nil, fmt.Errorf("moofile: cannot decode document: %w", err)
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// FindOne returns the first matching document, or nil if there is none.
func (c *Collection) FindOne(filter map[string]any) (map[string]any, error) {
	var arena cstrings
	defer arena.free()
	cFilter, err := arena.newDoc(filter)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, ErrClosed
	}

	var errPtr *C.char
	raw := C.moofile_find_one(c.handle, cFilter, &errPtr)
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil // no match — not an error
	}
	s := takeString(raw)

	var doc map[string]any
	if err := json.Unmarshal([]byte(s), &doc); err != nil {
		return nil, fmt.Errorf("moofile: cannot decode document: %w", err)
	}
	return doc, nil
}

// Count returns the number of documents matching filter.
func (c *Collection) Count(filter map[string]any) (int64, error) {
	var arena cstrings
	defer arena.free()
	cFilter, err := arena.newDoc(filter)
	if err != nil {
		return 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return 0, ErrClosed
	}

	var errPtr *C.char
	n := int64(C.moofile_count(c.handle, cFilter, &errPtr))
	if err := newError(errPtr); err != nil {
		return 0, err
	}
	return n, nil
}

// Exists reports whether at least one document matches filter.
func (c *Collection) Exists(filter map[string]any) (bool, error) {
	var arena cstrings
	defer arena.free()
	cFilter, err := arena.newDoc(filter)
	if err != nil {
		return false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return false, ErrClosed
	}

	var errPtr *C.char
	r := C.moofile_exists(c.handle, cFilter, &errPtr)
	if err := newError(errPtr); err != nil {
		return false, err
	}
	return r == 1, nil
}

// -----------------------------------------------------------------------
// Update
// -----------------------------------------------------------------------

// buildUpdate assembles the {set, unset, inc} blob the C layer expects.
func buildUpdate(setValues map[string]any, unsetFields []string, incValues map[string]any) map[string]any {
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
	return update
}

// UpdateOne updates the first document matching where.
//
// Matching nothing is an error ("no document matches filter"), mirroring the
// Rust and Python APIs.  Use Exists first when a miss is expected.
func (c *Collection) UpdateOne(where, setValues map[string]any, unsetFields []string, incValues map[string]any) (bool, error) {
	var arena cstrings
	defer arena.free()
	cWhere, err := arena.newDoc(where)
	if err != nil {
		return false, err
	}
	cUpdate, err := arena.newDoc(buildUpdate(setValues, unsetFields, incValues))
	if err != nil {
		return false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return false, ErrClosed
	}

	var errPtr *C.char
	r := C.moofile_update_one(c.handle, cWhere, cUpdate, &errPtr)
	if err := newError(errPtr); err != nil {
		return false, err
	}
	return r == 1, nil
}

// UpdateMany updates every document matching where and returns the count.
// Unlike UpdateOne, matching nothing is not an error — it returns 0.
func (c *Collection) UpdateMany(where, setValues map[string]any, unsetFields []string, incValues map[string]any) (int64, error) {
	var arena cstrings
	defer arena.free()
	cWhere, err := arena.newDoc(where)
	if err != nil {
		return 0, err
	}
	cUpdate, err := arena.newDoc(buildUpdate(setValues, unsetFields, incValues))
	if err != nil {
		return 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return 0, ErrClosed
	}

	var errPtr *C.char
	n := int64(C.moofile_update_many(c.handle, cWhere, cUpdate, &errPtr))
	if err := newError(errPtr); err != nil {
		return 0, err
	}
	return n, nil
}

// UpdateOneWith updates the first matching document using a named Update.
// It is equivalent to UpdateOne(where, update.Set, update.Unset, update.Inc).
func (c *Collection) UpdateOneWith(where Filter, update Update) (bool, error) {
	return c.UpdateOne(where, update.Set, update.Unset, update.Inc)
}

// UpdateManyWith updates every matching document using a named Update.
// It is equivalent to UpdateMany(where, update.Set, update.Unset, update.Inc).
func (c *Collection) UpdateManyWith(where Filter, update Update) (int64, error) {
	return c.UpdateMany(where, update.Set, update.Unset, update.Inc)
}

// ReplaceOne replaces the first document matching where, keeping its _id.
// Matching nothing is an error, as with UpdateOne.
func (c *Collection) ReplaceOne(where, replacement map[string]any) (bool, error) {
	var arena cstrings
	defer arena.free()
	cWhere, err := arena.newDoc(where)
	if err != nil {
		return false, err
	}
	cRepl, err := arena.newDoc(replacement)
	if err != nil {
		return false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return false, ErrClosed
	}

	var errPtr *C.char
	r := C.moofile_replace_one(c.handle, cWhere, cRepl, &errPtr)
	if err := newError(errPtr); err != nil {
		return false, err
	}
	return r == 1, nil
}

// -----------------------------------------------------------------------
// Delete
// -----------------------------------------------------------------------

// DeleteOne removes the first document matching where.  Returns false when
// nothing matched — unlike UpdateOne, that is not an error.
func (c *Collection) DeleteOne(where map[string]any) (bool, error) {
	var arena cstrings
	defer arena.free()
	cWhere, err := arena.newDoc(where)
	if err != nil {
		return false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return false, ErrClosed
	}

	var errPtr *C.char
	r := C.moofile_delete_one(c.handle, cWhere, &errPtr)
	if err := newError(errPtr); err != nil {
		return false, err
	}
	return r == 1, nil
}

// DeleteMany removes every document matching where and returns the count.
func (c *Collection) DeleteMany(where map[string]any) (int64, error) {
	var arena cstrings
	defer arena.free()
	cWhere, err := arena.newDoc(where)
	if err != nil {
		return 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return 0, ErrClosed
	}

	var errPtr *C.char
	n := int64(C.moofile_delete_many(c.handle, cWhere, &errPtr))
	if err := newError(errPtr); err != nil {
		return 0, err
	}
	return n, nil
}

// -----------------------------------------------------------------------
// Search
// -----------------------------------------------------------------------

// drainSearchCursor consumes a search cursor into results, freeing it.
// The caller must hold c.mu.
func drainSearchCursor(cursor *C.MooFileSearchCursor) ([]SearchResult, error) {
	if cursor == nil {
		return nil, &Error{Msg: "search returned a null cursor"}
	}
	defer C.moofile_search_cursor_free(cursor)

	results := []SearchResult{}
	for {
		var nextErr *C.char
		raw := C.moofile_search_cursor_next(cursor, &nextErr)
		if err := newError(nextErr); err != nil {
			return nil, err
		}
		if raw == nil {
			break
		}
		s := takeString(raw)

		var pair []json.RawMessage
		if err := json.Unmarshal([]byte(s), &pair); err != nil {
			return nil, fmt.Errorf("moofile: cannot decode search result: %w", err)
		}
		if len(pair) < 2 {
			return nil, &Error{Msg: "malformed search result: expected [doc, score]"}
		}
		var doc map[string]any
		if err := json.Unmarshal(pair[0], &doc); err != nil {
			return nil, fmt.Errorf("moofile: cannot decode search document: %w", err)
		}
		var score float64
		if err := json.Unmarshal(pair[1], &score); err != nil {
			return nil, fmt.Errorf("moofile: cannot decode search score: %w", err)
		}
		results = append(results, SearchResult{Doc: doc, Score: score})
	}
	return results, nil
}

// VectorSearch ranks documents by cosine similarity against queryVector.
// Pass limit <= 0 for the default of 10.
func (c *Collection) VectorSearch(field string, queryVector []float64, limit int, filter map[string]any) ([]SearchResult, error) {
	var arena cstrings
	defer arena.free()
	cFilter, err := arena.newDoc(filter)
	if err != nil {
		return nil, err
	}
	cVec, err := arena.newJSON(queryVector)
	if err != nil {
		return nil, err
	}
	cField := arena.new(field)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, ErrClosed
	}

	var errPtr *C.char
	cursor := C.moofile_vector_search(c.handle, cFilter, cField, cVec, C.int(limit), &errPtr)
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	return drainSearchCursor(cursor)
}

// TextSearch ranks documents by BM25 relevance to query.
func (c *Collection) TextSearch(field, query string, limit int, filter map[string]any) ([]SearchResult, error) {
	var arena cstrings
	defer arena.free()
	cFilter, err := arena.newDoc(filter)
	if err != nil {
		return nil, err
	}
	cField := arena.new(field)
	cQuery := arena.new(query)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, ErrClosed
	}

	var errPtr *C.char
	cursor := C.moofile_text_search(c.handle, cFilter, cField, cQuery, C.int(limit), &errPtr)
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	return drainSearchCursor(cursor)
}

// VectorSearchWithOptions ranks documents by cosine similarity using named options.
func (c *Collection) VectorSearchWithOptions(field string, queryVector []float64, options SearchOptions) ([]SearchResult, error) {
	return c.VectorSearch(field, queryVector, options.Limit, options.Filter)
}

// TextSearchWithOptions ranks documents by BM25 relevance using named options.
func (c *Collection) TextSearchWithOptions(field, query string, options SearchOptions) ([]SearchResult, error) {
	return c.TextSearch(field, query, options.Limit, options.Filter)
}

// HybridSearch fuses BM25 and vector rankings with Reciprocal Rank Fusion.
// Pass a nil queryVector to auto-embed queryText.
func (c *Collection) HybridSearch(textField, vectorField, queryText string, queryVector []float64, limit int, filter map[string]any) ([]SearchResult, error) {
	var arena cstrings
	defer arena.free()
	cFilter, err := arena.newDoc(filter)
	if err != nil {
		return nil, err
	}
	cTextField := arena.new(textField)
	cVecField := arena.new(vectorField)
	cQueryText := arena.new(queryText)

	// A nil pointer tells the C layer to auto-embed queryText instead.
	var cVec *C.char
	if queryVector != nil {
		cVec, err = arena.newJSON(queryVector)
		if err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, ErrClosed
	}

	var errPtr *C.char
	cursor := C.moofile_hybrid_search(c.handle, cFilter, cTextField, cVecField,
		cQueryText, cVec, C.int(limit), &errPtr)
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	return drainSearchCursor(cursor)
}

// HybridSearchWithOptions fuses BM25 and vector rankings using named options.
func (c *Collection) HybridSearchWithOptions(textField, vectorField, queryText string, queryVector []float64, options SearchOptions) ([]SearchResult, error) {
	return c.HybridSearch(textField, vectorField, queryText, queryVector, options.Limit, options.Filter)
}

// Semantic auto-embeds queryText with the model configured for sourceField
// via Config.AutoEmbed, then runs a vector search.
func (c *Collection) Semantic(sourceField, queryText string, limit int, filter map[string]any) ([]SearchResult, error) {
	var arena cstrings
	defer arena.free()
	cFilter, err := arena.newDoc(filter)
	if err != nil {
		return nil, err
	}
	cField := arena.new(sourceField)
	cQuery := arena.new(queryText)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, ErrClosed
	}

	var errPtr *C.char
	cursor := C.moofile_semantic_search(c.handle, cFilter, cField, cQuery, C.int(limit), &errPtr)
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	return drainSearchCursor(cursor)
}

// -----------------------------------------------------------------------
// Batch
// -----------------------------------------------------------------------

// SemanticWithOptions auto-embeds queryText and searches using named options.
func (c *Collection) SemanticWithOptions(sourceField, queryText string, options SearchOptions) ([]SearchResult, error) {
	return c.Semantic(sourceField, queryText, options.Limit, options.Filter)
}

// BatchBegin starts an atomic batch.  Prefer Batch, which cannot leak an
// open batch.
func (c *Collection) BatchBegin() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return ErrClosed
	}
	var errPtr *C.char
	C.moofile_batch_begin(c.handle, &errPtr)
	return newError(errPtr)
}

// BatchCommit applies the buffered writes atomically.
func (c *Collection) BatchCommit() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return ErrClosed
	}
	var errPtr *C.char
	C.moofile_batch_commit(c.handle, &errPtr)
	return newError(errPtr)
}

// BatchRollback discards the buffered writes.
func (c *Collection) BatchRollback() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return ErrClosed
	}
	var errPtr *C.char
	C.moofile_batch_rollback(c.handle, &errPtr)
	return newError(errPtr)
}

// Batch runs fn inside an atomic batch.  The batch is committed if fn returns
// nil, and rolled back if it returns an error or panics.  A panic is
// re-raised after the rollback.
//
// fn must call methods on the same Collection; it runs without the mutex held
// so those calls can take it themselves.
func (c *Collection) Batch(fn func() error) (err error) {
	if err := c.BatchBegin(); err != nil {
		return err
	}

	panicked := true
	defer func() {
		if panicked {
			// Roll back, then let the panic continue unwinding.
			_ = c.BatchRollback()
		}
	}()

	batchErr := fn()
	panicked = false

	if batchErr != nil {
		if rbErr := c.BatchRollback(); rbErr != nil {
			return fmt.Errorf("%w (rollback also failed: %v)", batchErr, rbErr)
		}
		return batchErr
	}
	return c.BatchCommit()
}

// -----------------------------------------------------------------------
// Utility
// -----------------------------------------------------------------------

// Stats reports documents, dead_records, file_size_bytes and dead_ratio.
//
// Note that one delete produces two dead records (the superseded original
// plus a tombstone); use dead_ratio to decide when to Compact.
func (c *Collection) Stats() (map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return nil, ErrClosed
	}

	var errPtr *C.char
	raw := C.moofile_stats(c.handle, &errPtr)
	if err := newError(errPtr); err != nil {
		return nil, err
	}
	s := takeString(raw)

	var stats map[string]any
	if err := json.Unmarshal([]byte(s), &stats); err != nil {
		return nil, fmt.Errorf("moofile: cannot decode stats: %w", err)
	}
	return stats, nil
}

// Compact rewrites the file, reclaiming space from dead records.
func (c *Collection) Compact() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return ErrClosed
	}
	var errPtr *C.char
	C.moofile_compact(c.handle, &errPtr)
	return newError(errPtr)
}

// Sync flushes and fsyncs the data file.
func (c *Collection) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return ErrClosed
	}
	var errPtr *C.char
	C.moofile_sync(c.handle, &errPtr)
	return newError(errPtr)
}

// Reindex rebuilds every in-memory index from the data file.
func (c *Collection) Reindex() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return ErrClosed
	}
	var errPtr *C.char
	C.moofile_reindex(c.handle, &errPtr)
	return newError(errPtr)
}

// Reembed re-embeds every document carrying sourceField, rewriting its
// configured vector field at the embedding model's current width, and returns
// the number of documents rewritten.
//
// This is the recovery path after changing the embedding model: vectors of
// different widths cannot be compared, so a collection whose stored vectors no
// longer match its vector index has that index disabled, and searching it
// returns an error naming both widths. Reembed rewrites the vectors, retargets
// the index and clears the flag. It is never implicit — it rewrites the whole
// collection.
//
// sourceField is the text field configured under auto_embed, not the vector
// field it writes to.
func (c *Collection) Reembed(sourceField string) (int64, error) {
	var arena cstrings
	defer arena.free()
	cField := arena.new(sourceField)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return 0, ErrClosed
	}

	var errPtr *C.char
	n := int64(C.moofile_reembed(c.handle, cField, &errPtr))
	if err := newError(errPtr); err != nil {
		return 0, err
	}
	return n, nil
}
