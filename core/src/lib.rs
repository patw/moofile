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
    /// Advisory lock file handle — used to serialize concurrent multi-process
    /// writes. No lock is held during normal operation; an exclusive lock is
    /// acquired only briefly during write operations via `with_write_lock`.
    lock_file: Option<fs::File>,
    /// Byte offset of the data file that this handle's in-memory index has
    /// replayed up to, plus the mtime observed at that point.
    ///
    /// Another process can append to the file at any time (see
    /// `with_write_lock`).  The format is append-only, so their records are
    /// always a suffix and this handle can catch up by scanning from
    /// `known_len` — see `catch_up`.  Every operation that trusts the
    /// in-memory index must catch up first, and the index cache must be
    /// stamped with *this* offset rather than the file's current length,
    /// or it will claim to describe records it never saw.
    known_len: u64,
    known_mtime_ns: u64,
    /// Identity of the file this handle's storage fd refers to.  compact()
    /// renames a fresh file over the path, so any other handle keeps writing
    /// into the now-unlinked old inode and loses everything it appends.
    /// Detecting the swap forces a reload *and* a storage reopen.
    known_ino: u64,
    /// Active batch buffer, if a batch is in progress.
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

// --- Advisory file lock for serializing concurrent multi-process writes.
        //     No lock is held here — this allows multiple processes to open
        //     the same file simultaneously. An exclusive lock is acquired
        //     only briefly during write operations (see `with_write_lock`). ---
        let lock_path = {
            let mut s = path.as_os_str().to_owned();
            s.push(".lock");
            PathBuf::from(s)
        };
        let lock_file = {
            // Open the lock file with create+read+write so it can be
            // created if it doesn't exist yet.  No lock is acquired here.
            match fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&lock_path)
            {
                Ok(lf) => Some(lf),
                Err(_) => None, // best-effort — writes won't be serialized
            }
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

        // The index we just built (from cache or scan) describes the file as
        // it stands right now; record that as our sync point.
        let (known_len, known_mtime_ns, known_ino) = fs::metadata(path)
            .ok()
            .map(|m| {
                let mtime = m
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                #[cfg(unix)]
                let ino = {
                    use std::os::unix::fs::MetadataExt;
                    m.ino()
                };
                #[cfg(not(unix))]
                let ino = 0u64;
                (m.len(), mtime, ino)
            })
            .unwrap_or((0, 0, 0));

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
                lock_file,
                known_len,
                known_mtime_ns,
                known_ino,
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

        // `_id` is the key for every in-memory index, so it must be a string.
        // This is checked before any mutation: returning an error unwinds
        // cleanly, whereas the previous `get_str(..).unwrap()` panicked while
        // holding the write guard, poisoning the lock and permanently
        // bricking the collection handle (and discarding any open batch).
        let _id = match doc.get("_id") {
            Some(Bson::String(s)) => s.clone(),
            Some(other) => {
                return Err(MooFileError::InvalidId(format!(
                    "{:?}",
                    other.element_type()
                )))
            }
            None => unreachable!("_id was just inserted if absent"),
        };

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
        // The duplicate check must happen under the same lock hold as the
        // append, and after catch_up, or a concurrent writer's record for this
        // _id is invisible and we write a second one for the same key.
        let doc = inner.with_write_lock(|inner| insert_locked(inner, doc, &_id))?;
        inner.dirty = true;
        Ok(doc)
    }

    /// Insert many documents under a *single* file-lock hold.
    ///
    /// Going through `insert()` per document meant one flock/unflock pair and
    /// one reconciliation per document, which dominated bulk loads.
    pub fn insert_many(&self, docs: Vec<Document>) -> Result<Vec<Document>, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;

        // With a batch open the per-document path buffers instead of writing,
        // so keep using it.
        if inner.batch.is_some() {
            drop(inner);
            return docs.into_iter().map(|d| self.insert(d)).collect();
        }

        let prepared: Vec<(Document, String)> = docs
            .into_iter()
            .map(|mut doc| {
                if !doc.contains_key("_id") {
                    doc.insert("_id", generate_id());
                }
                match doc.get("_id") {
                    Some(Bson::String(s)) => {
                        let id = s.clone();
                        Ok((doc, id))
                    }
                    Some(other) => Err(MooFileError::InvalidId(format!(
                        "{:?}",
                        other.element_type()
                    ))),
                    None => unreachable!("_id was just inserted if absent"),
                }
            })
            .collect::<Result<_, _>>()?;

        let out = inner.with_write_lock(|inner| {
            let mut out = Vec::with_capacity(prepared.len());
            for (doc, id) in prepared {
                out.push(insert_locked(inner, doc, &id)?);
            }
            Ok(out)
        })?;
        inner.dirty = true;
        Ok(out)
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
        query::validate_filter(&where_clause)?;

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

        // Match and write under a single lock hold, so the document we picked
        // is still the one we update.
        inner.with_write_lock(|inner| {
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
            Ok(())
        })?;
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
        query::validate_filter(&where_clause)?;

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

        // One lock hold for the whole operation: taking it per document meant
        // N flock/unflock pairs and N reconciliations, and let another writer
        // interleave in the middle of a bulk update.
        let count = inner.with_write_lock(|inner| {
            let docs_arc = inner.index_manager.get_matching(&where_clause);
            let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
            let mut count = 0;

            for old_doc in &docs {
                let old_id = old_doc.get_str("_id").unwrap().to_string();
                let mut new_doc =
                    apply_update(old_doc, set.as_ref(), unset.as_ref(), inc.as_ref());
                new_doc = inner.apply_auto_embed(new_doc)?;

                inner.storage.append(RECORD_REPLACEMENT, &new_doc)?;
                inner.index_manager.remove(&old_id);
                inner.index_manager.add(new_doc);
                inner.total_records += 1;
                count += 1;
            }
            Ok(count)
        })?;

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
        query::validate_filter(&where_clause)?;

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

        inner.with_write_lock(|inner| {
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
            Ok(())
        })?;
        inner.dirty = true;
        Ok(true)
    }

    // ------------------------------------------------------------------
    // Delete
    // ------------------------------------------------------------------

    pub fn delete_one(&self, where_clause: Document) -> Result<bool, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;
        query::validate_filter(&where_clause)?;

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

        let deleted = inner.with_write_lock(|inner| {
            let docs_arc = inner.index_manager.get_matching(&where_clause);
            let docs: Vec<Document> = docs_arc.iter().map(|d| d.as_ref().clone()).collect();
            if docs.is_empty() {
                return Ok(false);
            }
            let _id = docs[0].get_str("_id").unwrap().to_string();

            inner.storage.append(RECORD_TOMBSTONE, &doc! { "_id": &_id })?;
            inner.index_manager.remove(&_id);
            inner.total_records += 1;
            Ok(true)
        })?;
        if deleted {
            inner.dirty = true;
        }
        Ok(deleted)
    }

    pub fn delete_many(&self, where_clause: Document) -> Result<usize, MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_write()?;
        query::validate_filter(&where_clause)?;

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

        // One lock hold for the whole delete, as in update_many.
        let count = inner.with_write_lock(|inner| {
            let docs_arc = inner.index_manager.get_matching(&where_clause);
            let ids: Vec<String> = docs_arc
                .iter()
                .filter_map(|d| d.get_str("_id").ok().map(String::from))
                .collect();
            let mut count = 0;
            for _id in &ids {
                inner.storage.append(RECORD_TOMBSTONE, &doc! { "_id": _id })?;
                inner.index_manager.remove(_id);
                inner.total_records += 1;
                count += 1;
            }
            Ok(count)
        })?;

        if count > 0 {
            inner.dirty = true;
        }
        Ok(count)
    }

    // ------------------------------------------------------------------
    // Query
    // ------------------------------------------------------------------

    /// Reconcile the in-memory index with the file before a read.
    ///
    /// Another process may have appended since we last looked; without this a
    /// long-lived reader would never observe a writer's records.  Costs one
    /// `stat` when nothing has changed.
    pub(crate) fn refresh(&self) -> Result<(), MooFileError> {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.require_open()?;
        inner.catch_up()
    }

    pub fn find(&self, filter: Document) -> Result<Query, MooFileError> {
        query::validate_filter(&filter)?;
        self.refresh()?;
        let inner = self.inner.read().expect("lock poisoned");
        inner.require_open()?;
        Ok(Query::new(Arc::clone(&self.inner), filter))
    }

    pub fn find_one(&self, filter: Document) -> Result<Option<Document>, MooFileError> {
        self.find(filter)?.first()
    }

    pub fn count(&self, filter: Document) -> Result<usize, MooFileError> {
        query::validate_filter(&filter)?;
        self.refresh()?;
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
                inner.with_write_lock(|inner| inner.storage.append_batch(&refs))?;
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

        inner.storage.close();
        // The live set must be computed *after* catch_up (which with_write_lock
        // runs under the exclusive lock), or compaction rewrites the file from
        // a stale snapshot and permanently destroys another process's records.
        let result = inner.with_write_lock(|inner| {
            let live_docs = inner.index_manager.all_docs();
            storage::compact(&inner.path, &live_docs)?;
            inner.total_records = live_docs.len() as u64;
            Ok(())
        });
        inner.storage.reopen()?;

        if result.is_ok() {
            cache::delete_cache(&inner.path);
            inner.dirty = true;
            inner.mark_synced();
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
        cache::save_cache(
            &inner.path,
            &inner.index_manager,
            inner.total_records,
            (inner.known_len, inner.known_mtime_ns),
        )
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
                    (inner.known_len, inner.known_mtime_ns),
                ) {
                    log::warn!("moofile: failed to save cache: {e}");
                }
            }
        }

inner.lock_file = None; // drop lock file handle
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

    /// Acquire an exclusive advisory lock on the lock file, run the
    /// closure, then release the lock.  Uses blocking mode so the
    /// write waits for any other process currently writing.  Since no
    /// process holds a lock during normal operation, this only blocks
    /// briefly during concurrent writes.
    /// Current (length, mtime_ns, inode) of the data file, if it can be stat'd.
    /// The inode is 0 on platforms that don't expose one; there we fall back
    /// to detecting a replacement by the file shrinking.
    fn file_state(&self) -> Option<(u64, u64, u64)> {
        let meta = fs::metadata(&self.path).ok()?;
        let mtime = meta
            .modified()
            .ok()?
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        #[cfg(unix)]
        let ino = {
            use std::os::unix::fs::MetadataExt;
            meta.ino()
        };
        #[cfg(not(unix))]
        let ino = 0u64;
        Some((meta.len(), mtime, ino))
    }

    /// Reconcile the in-memory index with the data file.
    ///
    /// Three cases:
    ///   * unchanged  — one `stat`, no work (the overwhelmingly common path)
    ///   * grew       — another writer appended; replay just the new suffix
    ///   * shrank     — the file was compacted out from under us; full reload
    ///
    /// A partially written record at the tail is *not* truncated here: with
    /// multiple processes it most likely means another writer is mid-append,
    /// and truncating would destroy their record.  We simply stop before it
    /// and pick it up on the next catch-up.
    fn catch_up(&mut self) -> Result<(), MooFileError> {
        if self.closed || self.batch.is_some() {
            // Mid-batch the index deliberately reflects pre-batch state.
            return Ok(());
        }
        let (len, mtime, ino) = match self.file_state() {
            Some(s) => s,
            None => return Ok(()),
        };
        // A different inode means the file was replaced (compaction) — our
        // storage fd points at a dead inode and must be reopened.
        if ino != self.known_ino || len < self.known_len {
            return self.full_reload();
        }
        if len == self.known_len && mtime == self.known_mtime_ns {
            return Ok(());
        }

        let (records, partial) = storage::scan_from(&self.path, self.known_len)?;
        for record in &records {
            apply_record(&mut self.index_manager, record);
        }
        self.total_records += records.len() as u64;
        // Resume from the start of any partial record next time.
        self.known_len = partial.unwrap_or(len);
        self.known_mtime_ns = mtime;
        self.known_ino = ino;
        Ok(())
    }

    /// Rebuild the whole index from the file (used when it was rewritten).
    fn full_reload(&mut self) -> Result<(), MooFileError> {
        // Repoint the storage handle at the current file: after a compaction
        // our fd refers to the unlinked old inode, so appends would be lost.
        self.storage.reopen()?;
        self.index_manager.clear();
        let (records, partial) = storage::scan_file(&self.path)?;
        for record in &records {
            apply_record(&mut self.index_manager, record);
        }
        self.index_manager.rebuild_vector_indexes();
        self.total_records = records.len() as u64;
        let (len, mtime, ino) = self.file_state().unwrap_or((0, 0, 0));
        self.known_len = partial.unwrap_or(len);
        self.known_mtime_ns = mtime;
        self.known_ino = ino;
        self.loaded_from_cache = false;
        Ok(())
    }

    /// Record that this handle is in sync with the file as it stands now.
    /// Called after our own writes, which we have already applied in memory.
    fn mark_synced(&mut self) {
        if let Some((len, mtime, ino)) = self.file_state() {
            self.known_len = len;
            self.known_mtime_ns = mtime;
            self.known_ino = ino;
        }
    }

    fn with_write_lock<F, R>(&mut self, f: F) -> Result<R, MooFileError>
    where
        F: FnOnce(&mut Self) -> Result<R, MooFileError>,
    {
        use fs4::fs_std::FileExt;

        let locked = if let Some(ref lf) = self.lock_file {
            lf.lock_exclusive().is_ok()
        } else {
            false
        };

        // Pick up anyone else's writes before appending our own, so our index
        // (and the cache we derive from it) reflects the whole file.
        let result = self.catch_up().and_then(|()| f(self));
        if result.is_ok() {
            self.mark_synced();
        }

        if locked {
            if let Some(ref lf) = self.lock_file {
                let _ = lf.unlock();
            }
        }

        result
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

/// Insert one document. The caller must already hold the exclusive file lock
/// and have reconciled the index (see `with_write_lock`).
fn insert_locked(
    inner: &mut CollectionInner,
    doc: Document,
    _id: &str,
) -> Result<Document, MooFileError> {
    if inner.index_manager.get(_id).is_some() {
        return Err(MooFileError::DuplicateKey(_id.to_string()));
    }
    let doc = inner.apply_auto_embed(doc)?;
    inner.storage.append(RECORD_LIVE, &doc)?;
    inner.index_manager.add(doc.clone());
    inner.total_records += 1;
    Ok(doc)
}

/// Apply one scanned record to the index (last write wins per `_id`).
fn apply_record(index_manager: &mut IndexManager, record: &storage::Record) {
    let _id = match record.doc.get("_id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => return,
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

#[cfg(test)]
mod id_validation_tests {
    use super::*;
    use bson::doc;

    fn setup() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bson");
        (dir, path)
    }

    #[test]
    fn non_string_id_is_rejected_not_panicked() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        for bad in [doc! { "_id": 42 }, doc! { "_id": 1.5 }, doc! { "_id": [1, 2] }] {
            assert!(matches!(db.insert(bad), Err(MooFileError::InvalidId(_))));
        }

        // The collection must remain fully usable: the old code panicked
        // while holding the write guard, poisoning the lock forever.
        db.insert(doc! { "_id": "ok", "v": 1 }).unwrap();
        assert_eq!(db.count(doc! {}).unwrap(), 1);
    }

    #[test]
    fn rejected_id_inside_batch_does_not_lose_buffered_writes() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();

        db.batch_begin().unwrap();
        db.insert(doc! { "_id": "a", "v": 1 }).unwrap();
        db.insert(doc! { "_id": "b", "v": 2 }).unwrap();
        assert!(db.insert(doc! { "_id": 99 }).is_err());
        // The batch survives the rejected insert and still commits.
        db.batch_commit().unwrap();

        assert_eq!(db.count(doc! {}).unwrap(), 2);
    }

    #[test]
    fn malformed_filter_is_rejected_at_the_api_boundary() {
        let (_dir, path) = setup();
        let db = Collection::builder(&path).open().unwrap();
        db.insert(doc! { "_id": "a", "v": 1 }).unwrap();

        assert!(matches!(
            db.find(doc! { "$or": ["junk"] }),
            Err(MooFileError::InvalidFilter(_))
        ));
        assert!(matches!(
            db.count(doc! { "v": { "$bogus": 1 } }),
            Err(MooFileError::InvalidFilter(_))
        ));
        assert!(matches!(
            db.delete_many(doc! { "$or": [1] }),
            Err(MooFileError::InvalidFilter(_))
        ));

        // Still usable afterwards.
        assert_eq!(db.count(doc! {}).unwrap(), 1);
    }
}
