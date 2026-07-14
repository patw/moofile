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
//!     .auto_embed("content", moofile::AutoEmbedConfig {
//!         model: "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf".into(),
//!         target_field: "embedding".into(),
//!         dims: 1024,
//!         precision: moofile::EmbeddingPrecision::Int8,
//!         ..Default::default()
//!     })
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
//! - **Autoembed**: on-device embedding via `llama-gguf`, quantified storage.

pub mod embed;
pub mod errors;
pub mod storage;

mod cache;
mod index;
mod query;
mod text;

pub use embed::{AutoEmbedConfig, EmbeddingPrecision, ModelUri};
pub use errors::MooFileError;
pub use query::{AggFunc, HybridQuery, Query, TextQuery, VectorQuery};
pub use storage::Durability;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use bson::{doc, Bson, Document};

use crate::embed::EmbeddingEngine;
use crate::index::IndexManager;
use crate::storage::{StorageEngine, RECORD_LIVE, RECORD_REPLACEMENT, RECORD_TOMBSTONE};

/// Default cache directory for auto-downloaded models.
pub(crate) fn default_model_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("moofile")
        .join("models")
}

// ---------------------------------------------------------------------------
// Batch buffer
// ---------------------------------------------------------------------------

/// Buffered index mutation for batch writes.
#[derive(Debug)]
enum BatchIndexOp {
    Add(Document),
    Remove(String),
}

/// Buffer for atomic batch writes.
#[derive(Debug, Default)]
struct BatchBuffer {
    records: Vec<(u8, Document)>,
    index_ops: Vec<BatchIndexOp>,
    overlay: BTreeMap<String, Option<Document>>,
    count: u64,
}

// ---------------------------------------------------------------------------
// CollectionBuilder
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct CollectionBuilder {
    path: PathBuf,
    indexes: Vec<String>,
    vector_indexes: Vec<(String, usize)>,
    text_indexes: Vec<String>,
    auto_embeds: Vec<(String, AutoEmbedConfig)>,
    model_cache_dir: Option<PathBuf>,
    readonly: bool,
    durability: Durability,
}

impl CollectionBuilder {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            indexes: Vec::new(),
            vector_indexes: Vec::new(),
            text_indexes: Vec::new(),
            auto_embeds: Vec::new(),
            model_cache_dir: None,
            readonly: false,
            durability: Durability::Os,
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

    /// Configure auto-embedding for a source text field.
    ///
    /// When a document is inserted/updated with `source_field`, the text is
    /// embedded using the configured model and the result is stored in
    /// `config.target_field`.
    pub fn auto_embed(mut self, source_field: impl Into<String>, config: AutoEmbedConfig) -> Self {
        self.auto_embeds.push((source_field.into(), config));
        self
    }

    /// Set a custom model cache directory (default: `~/.cache/moofile/models/`).
    pub fn model_cache_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.model_cache_dir = Some(path.into());
        self
    }

    pub fn readonly(mut self) -> Self {
        self.readonly = true;
        self
    }

    pub fn durability(mut self, d: Durability) -> Self {
        self.durability = d;
        self
    }

    pub fn open(self) -> Result<Collection, MooFileError> {
        Collection::open_inner(
            &self.path,
            &self.indexes,
            &self.vector_indexes,
            &self.text_indexes,
            self.auto_embeds,
            self.model_cache_dir.unwrap_or_else(default_model_cache_dir),
            self.readonly,
            self.durability,
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
    loaded_from_cache: bool,
    dirty: bool,
    _lock_file: Option<fs::File>,
    batch: Option<BatchBuffer>,
    /// Auto-embed configuration: source_field → config
    auto_embeds: BTreeMap<String, AutoEmbedConfig>,
    /// Resolved embedding engines (loaded model for each unique model path)
    embedding_engines: BTreeMap<String, EmbeddingEngine>,
}

impl Collection {
    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    pub fn builder(path: impl Into<PathBuf>) -> CollectionBuilder {
        CollectionBuilder::new(path)
    }

    pub fn path(&self) -> PathBuf {
        let inner = self.inner.read().expect("lock poisoned");
        inner.path.clone()
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
        auto_embeds: Vec<(String, AutoEmbedConfig)>,
        model_cache_dir: PathBuf,
        readonly: bool,
        durability: Durability,
    ) -> Result<Self, MooFileError> {
        let meta_path = path.with_extension("bson.meta");

        // --- Advisory file lock ---
        let lock_path = {
            let mut s = path.as_os_str().to_owned();
            s.push(".lock");
            PathBuf::from(s)
        };
        let _lock_file = {
            use fs4::fs_std::FileExt;
            let lf = fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&lock_path)
                .map_err(|e| crate::errors::io_err(&lock_path, e))?;
            if readonly {
                lf.try_lock_shared()
                    .map_err(|_| MooFileError::ConcurrentAccess(lock_path.clone()))?;
            } else {
                lf.try_lock_exclusive()
                    .map_err(|_| MooFileError::ConcurrentAccess(lock_path.clone()))?;
            }
            Some(lf)
        };

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

        let mut storage = StorageEngine::open(path, readonly, durability)?;

        // --- Try disposable cache ---
        let (index_manager, total_records, loaded_from_cache) =
            match cache::try_load_cache(path, &merged_indexes, &merged_vector, &merged_text) {
                cache::CacheLoad::Hit {
                    index_manager,
                    total_records,
                } => {
                    log::debug!("moofile: cache hit — skipping BSON scan");
                    (index_manager, total_records, true)
                }
                cache::CacheLoad::Miss => {
                    log::debug!("moofile: cache miss — rebuilding from BSON scan");
                    let mut im = IndexManager::new(&merged_indexes, &merged_vector, &merged_text);
                    let total = load_from_file(path, readonly, &mut storage, &mut im)?;
                    (im, total, false)
                }
            };

        // --- Load embedding engines ---
        let auto_embeds_map: BTreeMap<String, AutoEmbedConfig> = auto_embeds.into_iter().collect();
        let mut embedding_engines: BTreeMap<String, EmbeddingEngine> = BTreeMap::new();

        for (source_field, config) in &auto_embeds_map {
            // Resolve model URI to local path (downloading if needed)
            let model_uri = ModelUri::parse(&config.model);
            let local_path = model_uri.resolve(&model_cache_dir)?;

            // Only load each unique model path once
            let model_key = local_path.to_string_lossy().into_owned();
            if !embedding_engines.contains_key(&model_key) {
                let engine = EmbeddingEngine::load(&local_path)?;
                embedding_engines.insert(model_key, engine);
            }

            // Validate dims match
            log::info!(
                "moofile: autoembed configured: '{}' → '{}' ({} dim, {})",
                source_field,
                config.target_field,
                config.dims,
                config.precision,
            );
        }

        Ok(Self {
            inner: Arc::new(RwLock::new(CollectionInner {
                path: path.to_path_buf(),
                readonly,
                storage,
                index_manager,
                total_records,
                closed: false,
                loaded_from_cache,
                dirty: false,
                _lock_file,
                batch: None,
                auto_embeds: auto_embeds_map,
                embedding_engines,
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

        // --- Batch path ---
        if inner.batch.is_some() {
            let exists = {
                let batch = inner.batch.as_ref().unwrap();
                match batch.overlay.get(&_id) {
                    Some(Some(_)) => true,
                    Some(None) => false,
                    None => inner.index_manager.get(&_id).is_some(),
                }
            };
            if exists {
                return Err(MooFileError::DuplicateKey(_id));
            }

            // Auto-embed before buffering
            let doc = inner.apply_auto_embed(doc)?;

            let batch = inner.batch.as_mut().unwrap();
            batch.records.push((RECORD_LIVE, doc.clone()));
            batch.index_ops.push(BatchIndexOp::Add(doc.clone()));
            batch.overlay.insert(_id, Some(doc.clone()));
            batch.count += 1;
            return Ok(doc);
        }

        // --- Normal path ---
        if inner.index_manager.get(&_id).is_some() {
            return Err(MooFileError::DuplicateKey(_id));
        }

        // Auto-embed before storing
        let doc = inner.apply_auto_embed(doc)?;

        inner.storage.append(RECORD_LIVE, &doc)?;
        inner.index_manager.add(doc.clone());
        inner.total_records += 1;
        inner.dirty = true;
        Ok(doc)
    }

    pub fn insert_many(&self, docs: Vec<Document>) -> Result<Vec<Document>, MooFileError> {
        docs.into_iter().map(|d| self.insert(d)).collect()
    }

    // ------------------------------------------------------------------
    // Update
    // ------------------------------------------------------------------

    pub fn update_one(
        &self,
        where_clause: Document,
        set: Option<Document>,
        unset: Option<Vec<String>>,
        inc: Option<Document>,
    ) -> Result<bool, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        if inner.batch.is_some() {
            let docs = batch_get_matching(&inner, &where_clause);
            if docs.is_empty() {
                return Err(MooFileError::DocumentNotFound);
            }
            let old_doc = docs[0].clone();
            let old_id = old_doc.get_str("_id").unwrap().to_string();
            let mut new_doc = apply_update(&old_doc, set.as_ref(), unset.as_ref(), inc.as_ref());
            new_doc = inner.apply_auto_embed(new_doc)?;
            let batch = inner.batch.as_mut().unwrap();
            batch.records.push((RECORD_REPLACEMENT, new_doc.clone()));
            batch.index_ops.push(BatchIndexOp::Remove(old_id.clone()));
            batch.index_ops.push(BatchIndexOp::Add(new_doc.clone()));
            batch.overlay.insert(old_id, Some(new_doc));
            batch.count += 1;
            return Ok(true);
        }

        let docs_arc = inner.index_manager.get_matching(&where_clause);
        let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        if docs.is_empty() {
            return Err(MooFileError::DocumentNotFound);
        }
        let old_doc = &docs[0];
        let old_id = old_doc.get_str("_id").unwrap().to_string();

        let mut new_doc = apply_update(old_doc, set.as_ref(), unset.as_ref(), inc.as_ref());
        new_doc = inner.apply_auto_embed(new_doc)?;

        inner.storage.append(RECORD_REPLACEMENT, &new_doc)?;
        inner.index_manager.remove(&old_id);
        inner.index_manager.add(new_doc);
        inner.total_records += 1;
        inner.dirty = true;
        Ok(true)
    }

    pub fn update_many(
        &self,
        where_clause: Document,
        set: Option<Document>,
        unset: Option<Vec<String>>,
        inc: Option<Document>,
    ) -> Result<usize, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        if inner.batch.is_some() {
            let docs = batch_get_matching(&inner, &where_clause);
            let mut count = 0;
            for old_doc in &docs {
                let old_id = old_doc.get_str("_id").unwrap().to_string();
                let mut new_doc = apply_update(old_doc, set.as_ref(), unset.as_ref(), inc.as_ref());
                new_doc = inner.apply_auto_embed(new_doc)?;
                let batch = inner.batch.as_mut().unwrap();
                batch.records.push((RECORD_REPLACEMENT, new_doc.clone()));
                batch.index_ops.push(BatchIndexOp::Remove(old_id.clone()));
                batch.index_ops.push(BatchIndexOp::Add(new_doc.clone()));
                batch.overlay.insert(old_id, Some(new_doc));
                batch.count += 1;
                count += 1;
            }
            return Ok(count);
        }

        let docs_arc = inner.index_manager.get_matching(&where_clause);
        let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        let mut count = 0;

        for old_doc in &docs {
            let old_id = old_doc.get_str("_id").unwrap().to_string();
            let mut new_doc = apply_update(old_doc, set.as_ref(), unset.as_ref(), inc.as_ref());
            new_doc = inner.apply_auto_embed(new_doc)?;

            inner.storage.append(RECORD_REPLACEMENT, &new_doc)?;
            inner.index_manager.remove(&old_id);
            inner.index_manager.add(new_doc);
            inner.total_records += 1;
            count += 1;
        }

        if count > 0 {
            inner.dirty = true;
        }
        Ok(count)
    }

    pub fn replace_one(
        &self,
        where_clause: Document,
        replacement: Document,
    ) -> Result<bool, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        if inner.batch.is_some() {
            let docs = batch_get_matching(&inner, &where_clause);
            if docs.is_empty() {
                return Err(MooFileError::DocumentNotFound);
            }
            let old_doc = &docs[0];
            let old_id = old_doc.get_str("_id").unwrap().to_string();
            let mut new_doc = replacement;
            new_doc.insert("_id", old_id.clone());
            new_doc = inner.apply_auto_embed(new_doc)?;
            let batch = inner.batch.as_mut().unwrap();
            batch.records.push((RECORD_REPLACEMENT, new_doc.clone()));
            batch.index_ops.push(BatchIndexOp::Remove(old_id.clone()));
            batch.index_ops.push(BatchIndexOp::Add(new_doc.clone()));
            batch.overlay.insert(old_id, Some(new_doc));
            batch.count += 1;
            return Ok(true);
        }

        let docs_arc = inner.index_manager.get_matching(&where_clause);
        let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        if docs.is_empty() {
            return Err(MooFileError::DocumentNotFound);
        }
        let old_doc = &docs[0];
        let old_id = old_doc.get_str("_id").unwrap().to_string();

        let mut new_doc = replacement;
        new_doc.insert("_id", old_id.clone());
        new_doc = inner.apply_auto_embed(new_doc)?;

        inner.storage.append(RECORD_REPLACEMENT, &new_doc)?;
        inner.index_manager.remove(&old_id);
        inner.index_manager.add(new_doc);
        inner.total_records += 1;
        inner.dirty = true;
        Ok(true)
    }

    // ------------------------------------------------------------------
    // Delete
    // ------------------------------------------------------------------

    pub fn delete_one(&self, where_clause: Document) -> Result<bool, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        if inner.batch.is_some() {
            let docs = batch_get_matching(&inner, &where_clause);
            if docs.is_empty() {
                return Ok(false);
            }
            let _id = docs[0].get_str("_id").unwrap().to_string();
            let batch = inner.batch.as_mut().unwrap();
            batch.records.push((RECORD_TOMBSTONE, doc! { "_id": &_id }));
            batch.index_ops.push(BatchIndexOp::Remove(_id.clone()));
            batch.overlay.insert(_id, None);
            batch.count += 1;
            return Ok(true);
        }

        let docs_arc = inner.index_manager.get_matching(&where_clause);
        let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        if docs.is_empty() {
            return Ok(false);
        }
        let _id = docs[0].get_str("_id").unwrap().to_string();

        inner.storage.append(RECORD_TOMBSTONE, &doc! { "_id": &_id })?;
        inner.index_manager.remove(&_id);
        inner.total_records += 1;
        inner.dirty = true;
        Ok(true)
    }

    pub fn delete_many(&self, where_clause: Document) -> Result<usize, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        if inner.batch.is_some() {
            let docs = batch_get_matching(&inner, &where_clause);
            let mut count = 0;
            for doc in &docs {
                let _id = doc.get_str("_id").unwrap().to_string();
                let batch = inner.batch.as_mut().unwrap();
                batch.records.push((RECORD_TOMBSTONE, doc! { "_id": &_id }));
                batch.index_ops.push(BatchIndexOp::Remove(_id.clone()));
                batch.overlay.insert(_id, None);
                batch.count += 1;
                count += 1;
            }
            return Ok(count);
        }

        let docs_arc = inner.index_manager.get_matching(&where_clause);
        let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
        let mut count = 0;

        for doc in &docs {
            let _id = doc.get_str("_id").unwrap().to_string();
            inner.storage.append(RECORD_TOMBSTONE, &doc! { "_id": &_id })?;
            inner.index_manager.remove(&_id);
            inner.total_records += 1;
            count += 1;
        }

        if count > 0 {
            inner.dirty = true;
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

    pub fn ensure_vectors_fresh(&self) -> Result<(), MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_open()?;
        inner.index_manager.ensure_vectors_fresh();
        Ok(())
    }

    pub fn sync(&self) -> Result<(), MooFileError> {
        let inner = self.inner.write().expect("lock poisoned");
        inner.require_open()?;
        inner.storage.sync()
    }

    // ------------------------------------------------------------------
    // Batch
    // ------------------------------------------------------------------

    pub fn batch_begin(&self) -> Result<(), MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;
        if inner.batch.is_some() {
            return Err(MooFileError::BatchAlreadyActive);
        }
        inner.batch = Some(BatchBuffer::default());
        Ok(())
    }

    pub fn batch_commit(&self) -> Result<(), MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        if let Some(batch) = inner.batch.take() {
            if !batch.records.is_empty() {
                let refs: Vec<(u8, &Document)> =
                    batch.records.iter().map(|(rt, d)| (*rt, d)).collect();
                inner.storage.append_batch(&refs)?;
            }
            for op in batch.index_ops {
                match op {
                    BatchIndexOp::Add(doc) => inner.index_manager.add(doc),
                    BatchIndexOp::Remove(id) => {
                        inner.index_manager.remove(&id);
                    }
                }
            }
            inner.total_records += batch.count;
            inner.dirty = true;
        }
        Ok(())
    }

    pub fn batch_rollback(&self) -> Result<(), MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.batch = None;
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
            cache::delete_cache(&inner.path);
            inner.dirty = true;
        }

        result
    }

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
        inner.loaded_from_cache = false;
        inner.dirty = true;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Cache
    // ------------------------------------------------------------------

    pub fn save_cache(&self) -> Result<(), MooFileError> {
        let inner = self.inner.write().expect("lock poisoned");
        inner.require_open()?;
        cache::save_cache(&inner.path, &inner.index_manager, inner.total_records)
    }

    pub fn close(&self) -> Result<(), MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        if inner.closed {
            return Ok(());
        }

        inner.storage.close();

        if !inner.loaded_from_cache || inner.dirty {
            if !inner.readonly {
                if let Err(e) = cache::save_cache(
                    &inner.path,
                    &inner.index_manager,
                    inner.total_records,
                ) {
                    log::warn!("moofile: failed to save cache: {e}");
                }
            }
        }

        inner._lock_file = None;
        inner.closed = true;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Auto-embed helper on CollectionInner
// ---------------------------------------------------------------------------

impl CollectionInner {
    /// If the document has any auto-embedded source fields, embed them
    /// and populate the target fields.
    fn apply_auto_embed(&self, mut doc: Document) -> Result<Document, MooFileError> {
        for (source_field, config) in &self.auto_embeds {
            // Only embed if the source field actually exists in the document
            if let Some(bson::Bson::String(text)) = doc.get(source_field).cloned() {
                // Look up the engine by model path
                let model_uri = ModelUri::parse(&config.model);
                let cache_dir = default_model_cache_dir();
                let local_path = model_uri.resolve(&cache_dir)?;
                let model_key = local_path.to_string_lossy().into_owned();

                let engine = self.embedding_engines.get(&model_key)
                    .ok_or_else(|| MooFileError::NoAutoEmbed(source_field.clone()))?;

                // Prefix and embed
                let prefixed = format!("{}{}", config.doc_prefix, text);
                let raw_emb = engine.embed(&prefixed)?;

                // Truncate to requested dims (MRL support)
                let emb: Vec<f32> = if raw_emb.len() > config.dims {
                    raw_emb[..config.dims].to_vec()
                } else {
                    raw_emb
                };

                // Normalize if requested
                let emb = if config.normalize {
                    let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
                    if norm > 0.0 {
                        emb.iter().map(|x| x / norm).collect()
                    } else {
                        emb
                    }
                } else {
                    emb
                };

                // Quantize and store as BSON array of f64 (matching existing format)
                let quantized = crate::embed::quantize(&emb, config.precision);
                let dequantized = crate::embed::dequantize(&quantized, config.precision, config.dims);
                let bson_array: Vec<Bson> = dequantized.iter().map(|&v| Bson::Double(v as f64)).collect();

                doc.insert(&config.target_field, Bson::Array(bson_array));
            }
        }
        Ok(doc)
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
    vector_indexes: std::collections::HashMap<String, usize>,
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

    let mut vector: Vec<(String, usize)> = existing
        .vector_indexes
        .into_iter()
        .collect();
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
// Batch helper
// ---------------------------------------------------------------------------

fn batch_get_matching(inner: &CollectionInner, filter: &Document) -> Vec<Document> {
    let batch = match inner.batch.as_ref() {
        Some(b) => b,
        None => return Vec::new(),
    };

    let mut view: Vec<Document> = Vec::new();

    for (id, doc) in &inner.index_manager.documents {
        match batch.overlay.get(id) {
            Some(Some(replacement)) => view.push(replacement.clone()),
            Some(None) => {}
            None => view.push(doc.as_ref().clone()),
        }
    }

    for (id, opt) in &batch.overlay {
        if opt.is_some() && !inner.index_manager.documents.contains_key(id) {
            view.push(opt.as_ref().unwrap().clone());
        }
    }

    view.into_iter().filter(|d| crate::query::matches(d, filter)).collect()
}

// ---------------------------------------------------------------------------
// _id generation
// ---------------------------------------------------------------------------

fn generate_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut buf = [0u8; 12];
    getrandom::fill(&mut buf[..8]).unwrap_or_else(|_| {
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        buf[..8].copy_from_slice(&(ns as u64).to_le_bytes());
    });
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    buf[8..].copy_from_slice(&counter.to_le_bytes()[..4]);

    hex::encode(buf)
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

    #[test]
    fn update_one_set() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert(doc! { "_id": "a", "name": "Alice", "age": 30 }).unwrap();

        let ok = db.update_one(doc! { "_id": "a" }, Some(doc! { "age": 31, "city": "NYC" }), None, None).unwrap();
        assert!(ok);

        let doc = db.find_one(doc! { "_id": "a" }).unwrap().unwrap();
        assert_eq!(doc.get_i32("age").unwrap(), 31);
        assert_eq!(doc.get_str("city").unwrap(), "NYC");
        assert_eq!(doc.get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn update_many() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.insert_many(vec![
            doc! { "status": "trial", "n": 1 },
            doc! { "status": "trial", "n": 2 },
            doc! { "status": "active", "n": 3 },
        ]).unwrap();

        let count = db.update_many(doc! { "status": "trial" }, Some(doc! { "status": "expired" }), None, None).unwrap();
        assert_eq!(count, 2);
        assert_eq!(db.count(doc! { "status": "expired" }).unwrap(), 2);
    }

    #[test]
    fn vector_search_returns_ordered() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path)
            .vector_index("embedding", 3)
            .open()
            .unwrap();

        db.insert(doc! { "_id": "near", "embedding": [1.0, 0.0, 0.0] }).unwrap();
        db.insert(doc! { "_id": "far", "embedding": [0.0, 0.0, 1.0] }).unwrap();

        let results = db.find(doc! {}).unwrap()
            .vector_search("embedding", vec![1.0, 0.0, 0.0], 2).to_list().unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.get_str("_id").unwrap(), "near");
    }

    #[test]
    fn text_search_basic() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).text_index("body").open().unwrap();

        db.insert(doc! { "_id": "1", "body": "machine learning is fascinating" }).unwrap();
        db.insert(doc! { "_id": "2", "body": "deep learning and neural networks" }).unwrap();
        db.insert(doc! { "_id": "3", "body": "cooking recipes for dinner" }).unwrap();

        let results = db.find(doc! {}).unwrap()
            .text_search("body", "machine learning", 5).to_list().unwrap();

        assert_eq!(results.len(), 2);
    }

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
        }
    }

    #[test]
    fn batch_mixed_operations() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).index("status").open().unwrap();

        db.insert(doc! { "_id": "keep", "status": "active" }).unwrap();
        db.insert(doc! { "_id": "update_me", "status": "active" }).unwrap();
        db.insert(doc! { "_id": "delete_me", "status": "active" }).unwrap();

        db.batch_begin().unwrap();
        db.insert(doc! { "_id": "new", "status": "active" }).unwrap();
        db.update_one(doc! { "_id": "update_me" }, Some(doc! { "status": "inactive" }), None, None).unwrap();
        db.delete_one(doc! { "_id": "delete_me" }).unwrap();
        db.batch_commit().unwrap();

        assert_eq!(db.count(doc! {}).unwrap(), 3);
        assert!(db.find_one(doc! { "_id": "delete_me" }).unwrap().is_none());
    }
}
