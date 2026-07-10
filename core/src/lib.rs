//! **MooFile** — lightweight embedded document store.
//!
//! ```no_run
//! use moofile::Collection;
//! use bson::doc;
//!
//! let db = Collection::builder("mydata.bson")
//!     .index("email")
//!     .index("age")
//!     .vector_index("embedding", 384)
//!     .text_index("content")
//!     .open()
//!     .unwrap();
//!
//! db.insert(doc! { "name": "Alice", "email": "alice@example.com", "age": 30 })
//!     .unwrap();
//!
//! let results = db.find(doc! { "age": { "$gt": 25 } })
//!     .unwrap()
//!     .sort("age", true)
//!     .limit(10)
//!     .to_list()
//!     .unwrap();
//! ```
//!
//! # Architecture
//!
//! - **Storage**: append-only BSON file, never modified in place.
//! - **Indexes**: rebuilt in memory on every open (regular B-Tree, vector, text).
//! - **Query**: lazy builder pattern — no work until a terminal method is called.

pub mod errors;
pub mod storage;

mod index;
mod query;
mod text;

pub use errors::MooFileError;
pub use query::{AggFunc, Query, TextQuery, VectorQuery};

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use bson::{doc, Bson, Document};

use crate::index::IndexManager;
use crate::storage::{StorageEngine, RECORD_LIVE, RECORD_REPLACEMENT, RECORD_TOMBSTONE};

// ---------------------------------------------------------------------------
// CollectionBuilder
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct CollectionBuilder {
    path: PathBuf,
    indexes: Vec<String>,
    vector_indexes: Vec<(String, usize)>,
    text_indexes: Vec<String>,
    readonly: bool,
}

impl CollectionBuilder {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            indexes: Vec::new(),
            vector_indexes: Vec::new(),
            text_indexes: Vec::new(),
            readonly: false,
        }
    }

    pub fn index(mut self, field: impl Into<String>) -> Self {
        self.indexes.push(field.into());
        self
    }

    pub fn indexes(mut self, fields: &[&str]) -> Self {
        for f in fields {
            self.indexes.push(f.to_string());
        }
        self
    }

    pub fn vector_index(mut self, field: impl Into<String>, dim: usize) -> Self {
        self.vector_indexes.push((field.into(), dim));
        self
    }

    pub fn text_index(mut self, field: impl Into<String>) -> Self {
        self.text_indexes.push(field.into());
        self
    }

    pub fn readonly(mut self) -> Self {
        self.readonly = true;
        self
    }

    pub fn open(self) -> Result<Collection, MooFileError> {
        Collection::open_inner(
            &self.path,
            &self.indexes,
            &self.vector_indexes,
            &self.text_indexes,
            self.readonly,
        )
    }
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Collection {
    inner: Arc<RwLock<CollectionInner>>,
}

#[derive(Debug)]
struct CollectionInner {
    path: PathBuf,
    readonly: bool,
    storage: StorageEngine,
    index_manager: IndexManager,
    total_records: u64,
    closed: bool,
}

impl Collection {
    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    pub fn builder(path: impl Into<PathBuf>) -> CollectionBuilder {
        CollectionBuilder::new(path)
    }

    pub fn open(
        path: impl Into<PathBuf>,
        indexes: &[&str],
        vector_indexes: &[(&str, usize)],
        text_indexes: &[&str],
    ) -> Result<Self, MooFileError> {
        let mut b = CollectionBuilder::new(path).indexes(indexes);
        for (f, d) in vector_indexes {
            b = b.vector_index(*f, *d);
        }
        for f in text_indexes {
            b = b.text_index(*f);
        }
        b.open()
    }

    fn open_inner(
        path: &Path,
        indexes: &[String],
        vector_indexes: &[(String, usize)],
        text_indexes: &[String],
        readonly: bool,
    ) -> Result<Self, MooFileError> {
        let meta_path = path.with_extension("bson.meta");

        if !readonly && !path.exists() {
            fs::write(path, &[]).map_err(|e| crate::errors::io_err(path, e))?;
        }

        let (merged_indexes, merged_vector, merged_text) =
            if let Ok(meta) = load_meta(&meta_path) {
                merge_meta(meta, indexes, vector_indexes, text_indexes)
            } else {
                (
                    indexes.to_vec(),
                    vector_indexes.to_vec(),
                    text_indexes.to_vec(),
                )
            };

        if !readonly {
            save_meta(&meta_path, &merged_indexes, &merged_vector, &merged_text)?;
        }

        let mut storage = StorageEngine::open(path, readonly)?;
        let mut index_manager =
            IndexManager::new(&merged_indexes, &merged_vector, &merged_text);

        let total_records =
            load_from_file(path, readonly, &mut storage, &mut index_manager)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(CollectionInner {
                path: path.to_path_buf(),
                readonly,
                storage,
                index_manager,
                total_records,
                closed: false,
            })),
        })
    }

    // ------------------------------------------------------------------
    // Insert
    // ------------------------------------------------------------------

    pub fn insert(&self, mut doc: Document) -> Result<Document, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        if !doc.contains_key("_id") {
            doc.insert("_id", generate_id());
        }

        let _id = doc.get_str("_id").unwrap().to_string();
        if inner.index_manager.get(&_id).is_some() {
            return Err(MooFileError::DuplicateKey(_id));
        }

        inner.storage.append(RECORD_LIVE, &doc)?;
        inner.index_manager.add(doc.clone());
        inner.total_records += 1;
        Ok(doc)
    }

    pub fn insert_many(&self, docs: Vec<Document>) -> Result<Vec<Document>, MooFileError> {
        docs.into_iter().map(|d| self.insert(d)).collect()
    }

    // ------------------------------------------------------------------
    // Update
    // ------------------------------------------------------------------

    /// Update the first document matching `where_clause`.
    ///
    /// Supports `$set`, `$unset`, `$inc` operators.  Returns `Ok(true)`
    /// if a document was updated, or [`MooFileError::DocumentNotFound`]
    /// if no document matched.
    pub fn update_one(
        &self,
        where_clause: Document,
        set: Option<Document>,
        unset: Option<Vec<String>>,
        inc: Option<Document>,
    ) -> Result<bool, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        let docs_arc = inner.index_manager.get_matching(&where_clause); let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        if docs.is_empty() {
            return Err(MooFileError::DocumentNotFound);
        }
        let old_doc = &docs[0];
        let old_id = old_doc.get_str("_id").unwrap().to_string();

        let new_doc = apply_update(old_doc, set.as_ref(), unset.as_ref(), inc.as_ref());

        inner.storage.append(RECORD_REPLACEMENT, &new_doc)?;
        inner.index_manager.remove(&old_id);
        inner.index_manager.add(new_doc);
        inner.total_records += 1;
        Ok(true)
    }

    /// Update all documents matching `where_clause`.  Returns the count
    /// of updated documents.
    pub fn update_many(
        &self,
        where_clause: Document,
        set: Option<Document>,
        unset: Option<Vec<String>>,
        inc: Option<Document>,
    ) -> Result<usize, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        let docs_arc = inner.index_manager.get_matching(&where_clause); let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        let mut count = 0;

        for old_doc in &docs {
            let old_id = old_doc.get_str("_id").unwrap().to_string();
            let new_doc = apply_update(old_doc, set.as_ref(), unset.as_ref(), inc.as_ref());

            inner.storage.append(RECORD_REPLACEMENT, &new_doc)?;
            inner.index_manager.remove(&old_id);
            inner.index_manager.add(new_doc);
            inner.total_records += 1;
            count += 1;
        }

        if count > 0 {
        }
        Ok(count)
    }

    /// Replace the entire document matching `where_clause`.  The original
    /// `_id` is preserved.
    pub fn replace_one(
        &self,
        where_clause: Document,
        replacement: Document,
    ) -> Result<bool, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        let docs_arc = inner.index_manager.get_matching(&where_clause); let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        if docs.is_empty() {
            return Err(MooFileError::DocumentNotFound);
        }
        let old_doc = &docs[0];
        let old_id = old_doc.get_str("_id").unwrap().to_string();

        let mut new_doc = replacement;
        new_doc.insert("_id", old_id.clone());

        inner.storage.append(RECORD_REPLACEMENT, &new_doc)?;
        inner.index_manager.remove(&old_id);
        inner.index_manager.add(new_doc);
        inner.total_records += 1;
        Ok(true)
    }

    // ------------------------------------------------------------------
    // Delete
    // ------------------------------------------------------------------

    /// Delete the first document matching `where_clause`.
    /// Returns `Ok(true)` if a document was deleted, `Ok(false)` if none matched.
    pub fn delete_one(&self, where_clause: Document) -> Result<bool, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        let docs_arc = inner.index_manager.get_matching(&where_clause); let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        if docs.is_empty() {
            return Ok(false);
        }
        let _id = docs[0].get_str("_id").unwrap().to_string();

        inner
            .storage
            .append(RECORD_TOMBSTONE, &doc! { "_id": &_id })?;
        inner.index_manager.remove(&_id);
        inner.total_records += 1;
        Ok(true)
    }

    /// Delete all documents matching `where_clause`.  Returns the count
    /// of deleted documents.
    pub fn delete_many(&self, where_clause: Document) -> Result<usize, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        let docs_arc = inner.index_manager.get_matching(&where_clause); let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        let mut count = 0;

        for doc in &docs {
            let _id = doc.get_str("_id").unwrap().to_string();
            inner
                .storage
                .append(RECORD_TOMBSTONE, &doc! { "_id": &_id })?;
            inner.index_manager.remove(&_id);
            inner.total_records += 1;
            count += 1;
        }

        if count > 0 {
        }
        Ok(count)
    }

    // ------------------------------------------------------------------
    // Query
    // ------------------------------------------------------------------

    pub fn find(&self, filter: Document) -> Result<Query, MooFileError> {
        let inner = self.inner.read().expect("lock poisoned");
        inner.require_open()?;
        Ok(Query::new(Arc::clone(&self.inner), filter))
    }

    pub fn find_one(&self, filter: Document) -> Result<Option<Document>, MooFileError> {
        self.find(filter)?.first()
    }

    pub fn count(&self, filter: Document) -> Result<usize, MooFileError> {
        let inner = self.inner.read().expect("lock poisoned");
        inner.require_open()?;
        Ok(inner.index_manager.count_matching(&filter))
    }

    pub fn exists(&self, filter: Document) -> Result<bool, MooFileError> {
        Ok(self.find_one(filter)?.is_some())
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------

    pub fn stats(&self) -> Result<CollectionStats, MooFileError> {
        let inner = self.inner.read().expect("lock poisoned");
        inner.require_open()?;

        let live = inner.index_manager.doc_count() as u64;
        let dead = inner.total_records - live;
        let file_size = fs::metadata(&inner.path).map(|m| m.len()).unwrap_or(0);
        let dead_ratio = if inner.total_records > 0 {
            dead as f64 / inner.total_records as f64
        } else {
            0.0
        };

        Ok(CollectionStats {
            documents: live,
            dead_records: dead,
            file_size_bytes: file_size,
            dead_ratio,
        })
    }

    /// Ensure vector indexes are rebuilt if stale.
    pub fn ensure_vectors_fresh(&self) -> Result<(), MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_open()?;
        inner.index_manager.ensure_vectors_fresh();
        Ok(())
    }

    pub fn compact(&self) -> Result<(), MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        let live_docs = inner.index_manager.all_docs();
        inner.storage.close();
        let result = storage::compact(&inner.path, &live_docs);
        inner.storage.reopen()?;

        if result.is_ok() {
            inner.total_records = live_docs.len() as u64;
        }

        result
    }

    /// Rebuild all in-memory indexes by re-scanning the BSON file.
    /// Useful after manual file manipulation.
    pub fn reindex(&self) -> Result<(), MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        let path = inner.path.clone();
        let readonly = inner.readonly;

        inner.index_manager.clear();

        let (records, truncate_to) = storage::scan_file(&path)?;
        if let Some(at) = truncate_to {
            if !readonly {
                inner.storage.close();
                storage::truncate(&path, at)?;
                inner.storage.reopen()?;
            }
        }

        let total = records.len() as u64;
        for record in &records {
            let _id = match record.doc.get("_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => continue,
            };
            match record.record_type {
                RECORD_LIVE | RECORD_REPLACEMENT => {
                    if inner.index_manager.get(&_id).is_some() {
                        inner.index_manager.remove(&_id);
                    }
                    inner.index_manager.add(record.doc.clone());
                }
                RECORD_TOMBSTONE => {
                    inner.index_manager.remove(&_id);
                }
                _ => {}
            }
        }

        inner.total_records = total;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CollectionStats {
    pub documents: u64,
    pub dead_records: u64,
    pub file_size_bytes: u64,
    pub dead_ratio: f64,
}

// ---------------------------------------------------------------------------
// Inner helpers
// ---------------------------------------------------------------------------

impl CollectionInner {
    fn require_write(&self) -> Result<(), MooFileError> {
        if self.readonly {
            return Err(MooFileError::ReadOnly);
        }
        self.require_open()
    }

    fn require_open(&self) -> Result<(), MooFileError> {
        if self.closed {
            return Err(MooFileError::ReadOnly);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Update operators
// ---------------------------------------------------------------------------

fn apply_update(
    doc: &Document,
    set: Option<&Document>,
    unset: Option<&Vec<String>>,
    inc: Option<&Document>,
) -> Document {
    let mut new_doc = doc.clone();

    if let Some(set_dict) = set {
        for (k, v) in set_dict {
            new_doc.insert(k, v.clone());
        }
    }

    if let Some(fields) = unset {
        for field in fields {
            new_doc.remove(field);
        }
    }

    if let Some(inc_dict) = inc {
        for (field, delta) in inc_dict {
            let current = new_doc.get(field).and_then(|v| match v {
                Bson::Int32(i) => Some(*i as f64),
                Bson::Int64(i) => Some(*i as f64),
                Bson::Double(f) => Some(*f),
                _ => None,
            }).unwrap_or(0.0);

            let delta_val = match delta {
                Bson::Int32(i) => *i as f64,
                Bson::Int64(i) => *i as f64,
                Bson::Double(f) => *f,
                _ => 0.0,
            };

            new_doc.insert(field, current + delta_val);
        }
    }

    new_doc
}

// ---------------------------------------------------------------------------
// Meta file
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Debug, Default)]
struct MetaFile {
    #[serde(default)]
    indexes: Vec<String>,
    #[serde(default)]
    vector_indexes: Vec<(String, usize)>,
    #[serde(default)]
    text_indexes: Vec<String>,
}

fn load_meta(path: &Path) -> Result<MetaFile, MooFileError> {
    if !path.exists() {
        return Ok(MetaFile::default());
    }
    let raw = fs::read_to_string(path).map_err(|e| MooFileError::MetaCorrupt(e.to_string()))?;
    serde_json::from_str(&raw).map_err(|e| MooFileError::MetaCorrupt(e.to_string()))
}

fn save_meta(
    path: &Path,
    indexes: &[String],
    vector_indexes: &[(String, usize)],
    text_indexes: &[String],
) -> Result<(), MooFileError> {
    let meta = serde_json::json!({
        "version": 1,
        "indexes": indexes,
        "vector_indexes": vector_indexes.iter().map(|(k, v)| (k.clone(), v)).collect::<std::collections::HashMap<_, _>>(),
        "text_indexes": text_indexes,
    });
    let raw = serde_json::to_string_pretty(&meta).unwrap();
    fs::write(path, &raw).map_err(|e| crate::errors::io_err(path, e))
}

fn merge_meta(
    existing: MetaFile,
    declared_indexes: &[String],
    declared_vector: &[(String, usize)],
    declared_text: &[String],
) -> (Vec<String>, Vec<(String, usize)>, Vec<String>) {
    let mut indexes = existing.indexes;
    for i in declared_indexes {
        if !indexes.contains(i) {
            indexes.push(i.clone());
        }
    }

    let mut vector = existing.vector_indexes;
    for (field, dim) in declared_vector {
        if !vector.iter().any(|(f, _)| f == field) {
            vector.push((field.clone(), *dim));
        }
    }

    let mut text = existing.text_indexes;
    for t in declared_text {
        if !text.contains(t) {
            text.push(t.clone());
        }
    }

    (indexes, vector, text)
}

// ---------------------------------------------------------------------------
// BSON file loader
// ---------------------------------------------------------------------------

fn load_from_file(
    path: &Path,
    readonly: bool,
    storage: &mut StorageEngine,
    index_manager: &mut IndexManager,
) -> Result<u64, MooFileError> {
    index_manager.clear();

    if !path.exists() {
        return Ok(0);
    }

    let (records, truncate_to) = storage::scan_file(path)?;

    if let Some(at) = truncate_to {
        if !readonly {
            storage.close();
            storage::truncate(path, at)?;
            storage.reopen()?;
        }
    }

    let total = records.len() as u64;

    for record in &records {
        let _id = match record.doc.get("_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => continue,
        };

        match record.record_type {
            RECORD_LIVE | RECORD_REPLACEMENT => {
                if index_manager.get(&_id).is_some() {
                    index_manager.remove(&_id);
                }
                index_manager.add(record.doc.clone());
            }
            RECORD_TOMBSTONE => {
                index_manager.remove(&_id);
            }
            _ => {}
        }
    }

    index_manager.rebuild_vector_indexes();

    Ok(total)
}

// ---------------------------------------------------------------------------
// _id generation
// ---------------------------------------------------------------------------

fn generate_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut buf = [0u8; 8];
    getrandom::fill(&mut buf).unwrap_or_else(|_| {
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        buf = (ns as u64).to_le_bytes();
    });

    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let raw = u64::from_le_bytes(buf) ^ counter;
    format!("{raw:016x}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use tempfile::TempDir;

    fn setup() -> (TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bson");
        (dir, path)
    }

    // --- Insert & Query ---

    #[test]
    fn insert_and_find() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).index("email").open().unwrap();

        let doc = db
            .insert(doc! { "name": "Alice", "email": "a@example.com" })
            .unwrap();
        assert!(doc.contains_key("_id"));

        let found = db.find_one(doc! { "email": "a@example.com" }).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn duplicate_key_rejected() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();
        let _doc = db.insert(doc! { "_id": "same", "v": 1 }).unwrap();
        let err = db.insert(doc! { "_id": "same", "v": 2 }).unwrap_err();
        assert!(matches!(err, MooFileError::DuplicateKey(_)));
    }

    #[test]
    fn readonly_rejects_writes() {
        let (_dir, path) = setup();
        {
            let db = Collection::builder(&path).open().unwrap();
            db.insert(doc! { "x": 1 }).unwrap();
        }
        let db = Collection::builder(&path).readonly().open().unwrap();
        let err = db.insert(doc! { "x": 2 }).unwrap_err();
        assert!(matches!(err, MooFileError::ReadOnly));
    }

    #[test]
    fn find_empty_collection() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();
        let results = db.find(doc! {}).unwrap().to_list().unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn count_and_exists() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        assert_eq!(db.count(doc! {}).unwrap(), 0);
        assert!(!db.exists(doc! { "name": "Alice" }).unwrap());

        db.insert(doc! { "name": "Alice" }).unwrap();
        db.insert(doc! { "name": "Bob" }).unwrap();

        assert_eq!(db.count(doc! {}).unwrap(), 2);
        assert!(db.exists(doc! { "name": "Alice" }).unwrap());
        assert!(!db.exists(doc! { "name": "Eve" }).unwrap());
    }

    // --- Update ---

    #[test]
    fn update_one_set() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert(doc! { "_id": "a", "name": "Alice", "age": 30 })
            .unwrap();

        let ok = db
            .update_one(
                doc! { "_id": "a" },
                Some(doc! { "age": 31, "city": "NYC" }),
                None,
                None,
            )
            .unwrap();
        assert!(ok);

        let doc = db.find_one(doc! { "_id": "a" }).unwrap().unwrap();
        assert_eq!(doc.get_i32("age").unwrap(), 31);
        assert_eq!(doc.get_str("city").unwrap(), "NYC");
        // name should survive
        assert_eq!(doc.get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn update_one_unset() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert(doc! { "_id": "x", "name": "Bob", "tmp": "remove-me" })
            .unwrap();

        db.update_one(
            doc! { "_id": "x" },
            None,
            Some(vec!["tmp".into()]),
            None,
        )
        .unwrap();

        let doc = db.find_one(doc! { "_id": "x" }).unwrap().unwrap();
        assert!(doc.get("tmp").is_none());
        assert!(doc.get("name").is_some());
    }

    #[test]
    fn update_one_inc() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert(doc! { "_id": "counter", "value": 10 })
            .unwrap();

        db.update_one(
            doc! { "_id": "counter" },
            None,
            None,
            Some(doc! { "value": 5 }),
        )
        .unwrap();

        let doc = db.find_one(doc! { "_id": "counter" }).unwrap().unwrap();
        assert!((doc.get_f64("value").unwrap() - 15.0).abs() < 0.01);
    }

    #[test]
    fn update_one_not_found() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        let err = db
            .update_one(doc! { "nope": true }, Some(doc! { "x": 1 }), None, None)
            .unwrap_err();
        assert!(matches!(err, MooFileError::DocumentNotFound));
    }

    #[test]
    fn update_many() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert_many(vec![
            doc! { "status": "trial", "n": 1 },
            doc! { "status": "trial", "n": 2 },
            doc! { "status": "active", "n": 3 },
        ])
        .unwrap();

        let count = db
            .update_many(
                doc! { "status": "trial" },
                Some(doc! { "status": "expired" }),
                None,
                None,
            )
            .unwrap();
        assert_eq!(count, 2);

        assert_eq!(db.count(doc! { "status": "expired" }).unwrap(), 2);
        assert_eq!(db.count(doc! { "status": "active" }).unwrap(), 1);
    }

    #[test]
    fn replace_one() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert(doc! { "_id": "r", "old": true }).unwrap();

        db.replace_one(doc! { "_id": "r" }, doc! { "new": true })
            .unwrap();

        let doc = db.find_one(doc! { "_id": "r" }).unwrap().unwrap();
        assert!(doc.get("old").is_none());
        assert!(doc.get("new").is_some());
        assert_eq!(doc.get_str("_id").unwrap(), "r");
    }

    #[test]
    fn replace_one_not_found() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        let err = db
            .replace_one(doc! { "nope": 1 }, doc! { "x": 1 })
            .unwrap_err();
        assert!(matches!(err, MooFileError::DocumentNotFound));
    }

    // --- Delete ---

    #[test]
    fn delete_one() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert(doc! { "_id": "del", "x": 1 }).unwrap();
        assert_eq!(db.count(doc! {}).unwrap(), 1);

        let ok = db.delete_one(doc! { "_id": "del" }).unwrap();
        assert!(ok);
        assert_eq!(db.count(doc! {}).unwrap(), 0);
    }

    #[test]
    fn delete_one_not_found() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        let ok = db.delete_one(doc! { "nope": true }).unwrap();
        assert!(!ok);
    }

    #[test]
    fn delete_many() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert_many(vec![
            doc! { "flag": true },
            doc! { "flag": true },
            doc! { "flag": false },
        ])
        .unwrap();

        let count = db.delete_many(doc! { "flag": true }).unwrap();
        assert_eq!(count, 2);
        assert_eq!(db.count(doc! {}).unwrap(), 1);
    }

    // --- Index-Accelerated Queries ---

    #[test]
    fn indexed_eq_query() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).index("email").open().unwrap();

        db.insert_many(vec![
            doc! { "email": "a@x.com", "v": 1 },
            doc! { "email": "b@x.com", "v": 2 },
            doc! { "email": "a@x.com", "v": 3 },
        ])
        .unwrap();

        let results = db
            .find(doc! { "email": "a@x.com" })
            .unwrap()
            .to_list()
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn indexed_range_query() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).index("age").open().unwrap();

        db.insert_many(vec![
            doc! { "age": 18 },
            doc! { "age": 25 },
            doc! { "age": 30 },
            doc! { "age": 45 },
            doc! { "age": 60 },
        ])
        .unwrap();

        let results = db
            .find(doc! { "age": { "$gte": 25, "$lt": 50 } })
            .unwrap()
            .to_list()
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn unindexed_field_full_scan() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).index("email").open().unwrap();

        db.insert_many(vec![
            doc! { "email": "a@x.com", "name": "Alice" },
            doc! { "email": "b@x.com", "name": "Bob" },
            doc! { "email": "c@x.com", "name": "Alice" },
        ])
        .unwrap();

        // "name" is not indexed — should still work via full scan
        let results = db
            .find(doc! { "name": "Alice" })
            .unwrap()
            .to_list()
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    // --- Persistence & Compaction ---

    #[test]
    fn persistence_across_opens() {
        let (_dir, path) = setup();

        {
            let db = Collection::builder(&path).index("name").open().unwrap();
            db.insert(doc! { "name": "Alice", "age": 30 }).unwrap();
            db.insert(doc! { "name": "Bob", "age": 25 }).unwrap();
        }

        {
            let db = Collection::builder(&path).index("name").open().unwrap();
            assert_eq!(db.count(doc! {}).unwrap(), 2);
            let alice = db.find_one(doc! { "name": "Alice" }).unwrap().unwrap();
            assert_eq!(alice.get_i32("age").unwrap(), 30);
        }
    }

    #[test]
    fn stats_reflects_inserts() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert_many(vec![doc! { "x": 1 }, doc! { "x": 2 }, doc! { "x": 3 }])
            .unwrap();

        let s = db.stats().unwrap();
        assert_eq!(s.documents, 3);
        assert_eq!(s.dead_records, 0);
        assert!(s.file_size_bytes > 0);
    }

    #[test]
    fn stats_reflects_deletes() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert_many(vec![doc! { "x": 1 }, doc! { "x": 2 }, doc! { "x": 3 }])
            .unwrap();
        db.delete_many(doc! { "x": { "$lt": 3 } }).unwrap();

        let s = db.stats().unwrap();
        assert_eq!(s.documents, 1);
        // 3 inserts + 2 tombstones = 5 total records; 1 live → 4 dead
        assert_eq!(s.dead_records, 4);
    }

    #[test]
    fn meta_file_persists_index_config() {
        let (_dir, path) = setup();

        {
            Collection::builder(&path)
                .index("email")
                .vector_index("embedding", 128)
                .text_index("content")
                .open()
                .unwrap();
        }

        {
            let db = Collection::builder(&path).open().unwrap();
            db.insert(doc! { "content": "hello world" }).unwrap();
        }
    }

    // --- Vector & Text Search ---

    #[test]
    fn vector_search_returns_ordered() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path)
            .vector_index("embedding", 3)
            .open()
            .unwrap();

        db.insert(doc! { "_id": "near", "embedding": [1.0, 0.0, 0.0] })
            .unwrap();
        db.insert(doc! { "_id": "far", "embedding": [0.0, 0.0, 1.0] })
            .unwrap();

        let results = db
            .find(doc! {})
            .unwrap()
            .vector_search("embedding", vec![1.0, 0.0, 0.0], 2)
            .to_list()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.get_str("_id").unwrap(), "near");
        assert!(results[0].1 > results[1].1);
    }

    #[test]
    fn vector_search_with_prefilter() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path)
            .index("category")
            .vector_index("embedding", 3)
            .open()
            .unwrap();

        db.insert(doc! { "_id": "a", "category": "ai", "embedding": [1.0, 0.0, 0.0] })
            .unwrap();
        db.insert(doc! { "_id": "b", "category": "food", "embedding": [1.0, 0.1, 0.0] })
            .unwrap();

        // Filter to only "food" category
        let results = db
            .find(doc! { "category": "food" })
            .unwrap()
            .vector_search("embedding", vec![1.0, 0.0, 0.0], 5)
            .to_list()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.get_str("_id").unwrap(), "b");
    }

    #[test]
    fn text_search_basic() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path)
            .text_index("body")
            .open()
            .unwrap();

        db.insert(doc! { "_id": "1", "body": "machine learning is fascinating" })
            .unwrap();
        db.insert(doc! { "_id": "2", "body": "deep learning and neural networks" })
            .unwrap();
        db.insert(doc! { "_id": "3", "body": "cooking recipes for dinner" })
            .unwrap();

        let results = db
            .find(doc! {})
            .unwrap()
            .text_search("body", "machine learning", 5)
            .to_list()
            .unwrap();

        assert_eq!(results.len(), 2); // only 1 and 2 have matching terms
        let ids: Vec<&str> = results.iter().map(|(d, _)| d.get_str("_id").unwrap()).collect();
        assert!(ids.contains(&"1"));
        assert!(ids.contains(&"2"));
        assert!(!ids.contains(&"3"));
    }

    #[test]
    fn text_search_with_filter() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path)
            .index("tag")
            .text_index("body")
            .open()
            .unwrap();

        db.insert(doc! { "_id": "a", "tag": "pub", "body": "deep learning advances" })
            .unwrap();
        db.insert(doc! { "_id": "b", "tag": "priv", "body": "deep learning for enterprise" })
            .unwrap();

        let results = db
            .find(doc! { "tag": "priv" })
            .unwrap()
            .text_search("body", "deep learning", 5)
            .to_list()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.get_str("_id").unwrap(), "b");
    }

    // --- Edge cases ---

    #[test]
    fn sort_and_limit() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert_many(vec![
            doc! { "score": 10 },
            doc! { "score": 30 },
            doc! { "score": 20 },
        ])
        .unwrap();

        let results = db
            .find(doc! {})
            .unwrap()
            .sort("score", true)
            .limit(2)
            .to_list()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_i32("score").unwrap(), 30);
        assert_eq!(results[1].get_i32("score").unwrap(), 20);
    }

    #[test]
    fn skip() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert_many(vec![
            doc! { "v": 1 },
            doc! { "v": 2 },
            doc! { "v": 3 },
        ])
        .unwrap();

        let results = db
            .find(doc! {})
            .unwrap()
            .sort("v", false)
            .skip(1)
            .to_list()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_i32("v").unwrap(), 2);
        assert_eq!(results[1].get_i32("v").unwrap(), 3);
    }

    #[test]
    fn update_reflected_in_index() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).index("email").open().unwrap();

        db.insert(doc! { "_id": "u", "email": "old@x.com" })
            .unwrap();

        db.update_one(
            doc! { "_id": "u" },
            Some(doc! { "email": "new@x.com" }),
            None,
            None,
        )
        .unwrap();

        // Old value should not match
        assert!(db
            .find_one(doc! { "email": "old@x.com" })
            .unwrap()
            .is_none());
        // New value should match via index
        let doc = db.find_one(doc! { "email": "new@x.com" }).unwrap().unwrap();
        assert_eq!(doc.get_str("_id").unwrap(), "u");
    }

    #[test]
    fn delete_reflected_in_index() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).index("status").open().unwrap();

        db.insert(doc! { "_id": "d", "status": "active" })
            .unwrap();
        db.delete_one(doc! { "_id": "d" }).unwrap();

        let results = db
            .find(doc! { "status": "active" })
            .unwrap()
            .to_list()
            .unwrap();
        assert!(results.is_empty());
    }
}
