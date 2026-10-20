/// In-memory index management.
///
/// Documents are stored as `Arc<RawDocumentBuf>` — raw, undecoded BSON
/// bytes behind a cheap reference-counted pointer. A decoded `bson::Document`
/// is ~10x its on-wire size (`Bson` is a 112-byte enum sized to its largest
/// variant, `Document` wraps an `IndexMap` with its own per-entry overhead
/// on top) — that cost is only worth paying for a document a caller is
/// actually about to read. Indexing extracts just the handful of configured
/// fields via `RawDocument::get()` (no decode); a full `Document` is
/// materialized via `to_document()` only at the point a document is about
/// to leave the index for a caller.
///
/// Every document lives in a numbered slot — its [`Row`] — and the indexes
/// refer to documents by row, not by `_id`.  They used to hold `String` ids:
/// each regular-index entry carried its own heap copy of the id (~170 B per
/// entry all told) and every text posting another (~1.2 KB per document for
/// one text field), so the indexes outweighed the documents several times
/// over.  A row is 4 bytes.  `by_id` is the one place an id is stored.
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use bson::raw::{RawBsonRef, RawDocumentBuf};
use bson::Document;
use rayon::prelude::*;

use crate::text::TextIndex;

/// Threshold for switching from sequential to parallel scoring.
/// Below this, rayon's thread-pool overhead isn't worth it.
const PARALLEL_THRESHOLD: usize = 4096;

/// A document's slot number.  Slots freed by a delete are reused, so a row
/// identifies a document only while that document is live.
pub(crate) type Row = u32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub(crate) enum Value {
    Null, Bool(bool), I32(i32), I64(i64), Double(OrderedFloat), String(String),
    /// Milliseconds since the epoch.  Datetimes must be indexable or an
    /// indexed timestamp field yields `IndexResult::Exact` over an empty set
    /// and the query returns nothing.
    DateTime(i64),
    ObjectId([u8; 12]),
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub(crate) struct OrderedFloat(f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool { self.0.total_cmp(&other.0) == std::cmp::Ordering::Equal }
}
impl Eq for OrderedFloat {}
impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.0.total_cmp(&other.0) }
}

#[derive(Debug)]
pub(crate) enum IndexResult {
    Exact(Vec<Row>),
    Candidates(Vec<Row>),
}

impl IndexResult {
    pub(crate) fn rows(&self) -> &[Row] {
        match self { IndexResult::Exact(r) | IndexResult::Candidates(r) => r }
    }
}

/// The rows holding one value of one indexed field.
///
/// Most values of a selective field (an email, a timestamp) belong to a
/// single document, and a `BTreeSet` allocates a node even for one element;
/// `One` keeps those inline.  `Many` always holds at least two rows — it is
/// demoted back to `One` when a removal leaves a single row — so removal is
/// O(log k) however low the field's cardinality.
#[derive(Debug, Clone)]
pub(crate) enum Postings {
    One(Row),
    Many(BTreeSet<Row>),
}

impl Postings {
    pub(crate) fn from_rows(rows: Vec<Row>) -> Option<Self> {
        match rows.len() {
            0 => None,
            1 => Some(Postings::One(rows[0])),
            _ => Some(Postings::Many(rows.into_iter().collect())),
        }
    }

    fn insert(&mut self, row: Row) {
        match self {
            Postings::One(r) if *r == row => {}
            Postings::One(r) => *self = Postings::Many([*r, row].into_iter().collect()),
            Postings::Many(set) => { set.insert(row); }
        }
    }

    /// Remove `row`; true when nothing is left.
    fn remove(&mut self, row: Row) -> bool {
        match self {
            Postings::One(r) => *r == row,
            Postings::Many(set) => {
                set.remove(&row);
                if set.len() == 1 {
                    *self = Postings::One(*set.iter().next().unwrap());
                }
                false
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self { Postings::One(_) => 1, Postings::Many(s) => s.len() }
    }

    pub(crate) fn extend_into(&self, out: &mut Vec<Row>) {
        match self {
            Postings::One(r) => out.push(*r),
            Postings::Many(s) => out.extend(s.iter().copied()),
        }
    }
}

/// One vector index: a dense matrix of L2-normalised rows.
#[derive(Debug, Clone, Default)]
pub(crate) struct VectorMatrix {
    /// Matrix position → document row.
    pub(crate) rows: Vec<Row>,
    /// `rows.len() × dim` floats, row-major, each row normalised.
    pub(crate) data: Vec<f32>,
    pub(crate) dim: usize,
    /// Document row → matrix position (`ABSENT` if the row has none).
    /// Removing a document used to find its matrix row with a linear search
    /// over every id, which made each update or delete of a vector-bearing
    /// document O(n).
    pos: Vec<u32>,
}

const ABSENT: u32 = u32::MAX;

impl VectorMatrix {
    fn new(dim: usize) -> Self { Self { dim, ..Default::default() } }

    pub(crate) fn from_parts(rows: Vec<Row>, data: Vec<f32>, dim: usize) -> Self {
        let mut m = Self { rows, data, dim, pos: Vec::new() };
        for (i, &row) in m.rows.iter().enumerate() {
            set_pos(&mut m.pos, row, i as u32);
        }
        m
    }

    pub(crate) fn len(&self) -> usize { self.rows.len() }

    fn push(&mut self, row: Row, normalized: &[f32]) {
        set_pos(&mut self.pos, row, self.rows.len() as u32);
        self.rows.push(row);
        self.data.extend_from_slice(normalized);
    }

    /// swap_remove keeps rows, data and pos in lockstep: the trailing row
    /// moves into the hole, then the matrix is truncated.
    fn remove(&mut self, row: Row) {
        let Some(&p) = self.pos.get(row as usize) else { return };
        if p == ABSENT { return; }
        let (p, last, dim) = (p as usize, self.rows.len() - 1, self.dim);
        if p != last {
            let moved = self.rows[last];
            self.rows[p] = moved;
            self.pos[moved as usize] = p as u32;
            let (head, tail) = self.data.split_at_mut(last * dim);
            head[p * dim..(p + 1) * dim].copy_from_slice(&tail[..dim]);
        }
        self.rows.pop();
        self.data.truncate(last * dim);
        self.pos[row as usize] = ABSENT;
    }
}

fn set_pos(pos: &mut Vec<u32>, row: Row, p: u32) {
    let i = row as usize;
    if pos.len() <= i { pos.resize(i + 1, ABSENT); }
    pos[i] = p;
}

fn normalized(vec: Vec<f32>) -> Vec<f32> {
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 { vec.iter().map(|x| x / norm).collect() } else { vec } // zero vector — cosine is 0 regardless
}

pub(crate) struct IndexManager {
    /// `_id` → row.  Ordered, so full scans and `_id` ranges run in `_id`
    /// order, as they always have.
    by_id: BTreeMap<String, Row>,
    /// Row → document; `None` is a free slot.
    docs: Vec<Option<Arc<RawDocumentBuf>>>,
    /// Free slots, reused last-freed-first.
    free: Vec<Row>,
    pub(crate) regular: BTreeMap<String, BTreeMap<Value, Postings>>,
    pub(crate) regular_fields: Vec<String>,
    pub(crate) vector_fields: Vec<(String, usize)>,
    pub(crate) vector_data: BTreeMap<String, VectorMatrix>,
    pub(crate) text_indexes: BTreeMap<String, TextIndex>,
    pub(crate) text_fields: Vec<String>,
    pub(crate) vectors_stale: bool,
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field("ndocs", &self.by_id.len())
            .finish()
    }
}

impl IndexManager {
    pub fn new(regular_fields: &[String], vector_fields: &[(String, usize)], text_fields: &[String]) -> Self {
        let mut regular = BTreeMap::new();
        for f in regular_fields { regular.insert(f.clone(), BTreeMap::new()); }
        let mut text_indexes = BTreeMap::new();
        for f in text_fields { text_indexes.insert(f.clone(), TextIndex::new()); }
        Self {
            by_id: BTreeMap::new(), docs: Vec::new(), free: Vec::new(),
            regular, regular_fields: regular_fields.to_vec(),
            vector_fields: vector_fields.to_vec(), vector_data: BTreeMap::new(),
            text_indexes, text_fields: text_fields.to_vec(),
            vectors_stale: true,
        }
    }

    /// Reconstruct an IndexManager from a cache snapshot.
    ///
    /// All indexes are pre-built — no tokenisation, no vector normalisation
    /// needed.  `docs` must be the snapshot's row → document assignment
    /// exactly; `None` if it is inconsistent (a document without a string
    /// `_id`, or two rows with the same one), which the caller treats as a
    /// cache miss.
    ///
    /// The field lists (`regular_fields`, `vector_fields`, `text_fields`)
    /// are passed explicitly so that configured-but-empty indexes are
    /// preserved (e.g. a vector index on a field that no document has yet).
    pub(crate) fn from_cache(
        regular: BTreeMap<String, BTreeMap<Value, Postings>>,
        regular_fields: Vec<String>,
        vector_fields: Vec<(String, usize)>,
        vector_data: BTreeMap<String, VectorMatrix>,
        text_fields: Vec<String>,
        text_indexes: BTreeMap<String, TextIndex>,
        docs: Vec<Option<Arc<RawDocumentBuf>>>,
    ) -> Option<Self> {
        let mut by_id = BTreeMap::new();
        let mut free = Vec::new();
        for (row, slot) in docs.iter().enumerate() {
            match slot {
                Some(doc) => {
                    let id = doc.get_str("_id").ok()?.to_string();
                    if by_id.insert(id, row as Row).is_some() { return None; }
                }
                None => free.push(row as Row),
            }
        }
        free.reverse();
        Some(Self {
            by_id, docs, free,
            regular, regular_fields,
            vector_fields, vector_data,
            text_indexes, text_fields,
            vectors_stale: false,
        })
    }

    /// Row → `_id` for every slot, `None` for free ones — what the cache
    /// stores instead of the documents themselves.
    pub(crate) fn row_ids(&self) -> Vec<Option<String>> {
        self.docs.iter()
            .map(|slot| slot.as_ref().and_then(|d| d.get_str("_id").ok()).map(String::from))
            .collect()
    }

    pub(crate) fn id_of(&self, row: Row) -> Option<&str> {
        self.doc_at(row)?.get_str("_id").ok()
    }

    fn doc_at(&self, row: Row) -> Option<&Arc<RawDocumentBuf>> {
        self.docs.get(row as usize)?.as_ref()
    }

    /// Index a document, taking ownership of an already-decoded `Document`.
    /// Encodes once internally to build the raw storage representation.
    /// Prefer `add_raw` on hot paths that already have the encoded bytes in
    /// hand (insert, load/replay) — this exists for callers (tests, the
    /// batch-commit path, cache replay) that only have a `Document`.
    pub fn add(&mut self, doc: Document) {
        let raw = match RawDocumentBuf::try_from(&doc) {
            Ok(raw) => raw,
            Err(e) => {
                // Skipping keeps the load total — returning an error here
                // would make one unencodable record fail the whole open — but
                // it must not be silent: the document is on disk and reads
                // back as nothing, which is the same shape of bug as a
                // non-string `_id`.  In practice this is a binary value over
                // `MAX_BINARY_SIZE`, which both implementations now reject at
                // insert, so reaching this means a file written by something
                // that did not.
                log::warn!(
                    "moofile: document {} cannot be re-encoded and is absent \
                     from the index: {e}",
                    doc.get_str("_id").unwrap_or("<no _id>"),
                );
                return;
            }
        };
        self.add_raw(raw);
    }

    /// Index a document from its already-encoded BSON bytes — no decode, no
    /// re-encode. `insert()` builds these bytes once (to write to disk) and
    /// hands them straight here; only the handful of configured
    /// regular/text/vector fields are read out of them, via
    /// `RawDocument::get()`, which — unlike `bson::Document::get()`'s O(1)
    /// `IndexMap` lookup — is an O(fields-in-document) scan of the raw bytes.
    /// For the small, flat documents moofile is typically used for this is
    /// within noise of the decoded lookup (measured: 73.8ns vs 74.6ns/field
    /// on a 12-field doc); it degrades for documents with many more fields
    /// than are actually indexed.
    pub fn add_raw(&mut self, raw: RawDocumentBuf) {
        // get_str returns Result<&str, ValueAccessError> — a missing/wrong-
        // typed "_id" is an Err here, not an Ok(None); treat any error as
        // "skip", mirroring the decoded path's `.unwrap_or("")` behavior.
        let Ok(id_ref) = raw.get_str("_id") else { return };
        if id_ref.is_empty() { return; }
        let _id = id_ref.to_string();
        // A second add for a live id replaces it; its old index entries must
        // go first or they would point at the new document's row.
        if self.by_id.contains_key(&_id) { self.remove(&_id); }

        let row = match self.free.pop() {
            Some(r) => r,
            None => { self.docs.push(None); (self.docs.len() - 1) as Row }
        };
        for field in &self.regular_fields {
            if let Ok(Some(val)) = raw.get(field) {
                if let Some(key) = raw_bson_to_value(val) {
                    let map = self.regular.entry(field.clone()).or_default();
                    match map.get_mut(&key) {
                        Some(p) => p.insert(row),
                        None => { map.insert(key, Postings::One(row)); }
                    }
                }
            }
        }
        for field in &self.text_fields {
            if let Ok(Some(val)) = raw.get(field) {
                if let Some(text) = raw_text_index_value(val) {
                    if let Some(ti) = self.text_indexes.get_mut(field) {
                        ti.add_document(row, &text);
                    }
                }
            }
        }
        // Incremental vector update: if vectors are not stale, append the
        // new row to the existing matrix instead of triggering a full O(n·d)
        // rebuild on the next search.
        if !self.vectors_stale {
            for (field, dim) in &self.vector_fields {
                if let Some(vec) = raw.get(field).ok().flatten().and_then(crate::vector::raw_vector) {
                    if vec.len() == *dim {
                        self.vector_data.entry(field.clone())
                            .or_insert_with(|| VectorMatrix::new(*dim))
                            .push(row, &normalized(vec));
                    }
                }
            }
        }
        self.docs[row as usize] = Some(Arc::new(raw));
        self.by_id.insert(_id, row);
    }

    /// Removes a document and returns its decoded content. Callers that
    /// only need the `_id` (most of them) never trigger this decode — see
    /// `contains`.
    pub fn remove(&mut self, _id: &str) -> Option<Document> {
        let row = self.by_id.remove(_id)?;
        let raw = Arc::unwrap_or_clone(self.docs[row as usize].take()?);
        self.free.push(row);
        for field in &self.regular_fields {
            if let Ok(Some(val)) = raw.get(field) {
                if let Some(key) = raw_bson_to_value(val) {
                    if let Some(map) = self.regular.get_mut(field) {
                        if map.get_mut(&key).is_some_and(|p| p.remove(row)) {
                            map.remove(&key);
                        }
                    }
                }
            }
        }
        for field in &self.text_fields {
            if let Some(ti) = self.text_indexes.get_mut(field) {
                // The text that was indexed is still in the document, so its
                // terms can be visited directly; this used to walk every term
                // in the vocabulary on each update or delete.
                let text = raw.get(field).ok().flatten().and_then(raw_text_index_value);
                ti.remove_document(row, text.as_deref());
            }
        }
        // Drop this document's row from each vector matrix in place.  Marking
        // the whole matrix stale instead would force a full O(n·d) renormalise
        // on the next search.
        if !self.vectors_stale {
            for m in self.vector_data.values_mut() { m.remove(row); }
        }
        raw.to_document().ok()
    }

    /// Existence check with no decode and no Arc clone — prefer this over
    /// `get(...).is_some()` (duplicate-key checks on the insert hot path in
    /// particular).
    pub fn contains(&self, _id: &str) -> bool {
        self.by_id.contains_key(_id)
    }

    pub fn get(&self, _id: &str) -> Option<Document> {
        self.get_raw(_id)?.to_document().ok()
    }

    pub(crate) fn get_raw(&self, _id: &str) -> Option<&Arc<RawDocumentBuf>> {
        self.doc_at(*self.by_id.get(_id)?)
    }

    /// Every live document with its `_id`, in `_id` order.
    pub(crate) fn iter_docs(&self) -> impl Iterator<Item = (&str, &Arc<RawDocumentBuf>)> {
        self.by_id.iter().filter_map(|(id, &row)| Some((id.as_str(), self.doc_at(row)?)))
    }

    /// No internal caller left after `compact()`/`Query::to_list()` moved to
    /// `all_docs_arc()` (decode-on-demand); kept as a convenience API for a
    /// full-collection decode.
    #[allow(dead_code)]
    pub fn all_docs(&self) -> Vec<Document> {
        self.iter_docs().filter_map(|(_, d)| d.to_document().ok()).collect()
    }

    /// As `all_docs`, but without decoding each document — cheap `Arc`
    /// clones of the raw bytes only. Used by `compact()` (which never needs
    /// a `Document` at all — see `storage::compact_raw`).
    pub fn all_docs_arc(&self) -> Vec<Arc<RawDocumentBuf>> {
        self.iter_docs().map(|(_, d)| d.clone()).collect()
    }

    pub fn doc_count(&self) -> usize { self.by_id.len() }

    pub fn count_matching(&self, filter: &Document) -> usize {
        if filter.is_empty() { return self.by_id.len(); }
        match self.try_index(filter) {
            Some(IndexResult::Exact(rows)) => rows.len(),
            _ => self.matching_rows_first(filter, usize::MAX).len(),
        }
    }

    /// Returns matches as `Arc<RawDocumentBuf>` — un-decoded. The `Exact`
    /// path (index alone proves the match) never decodes at all; the
    /// `Candidates`/`None` paths decode each candidate transiently to
    /// evaluate `query::matches`, but return the original `Arc`, not the
    /// decoded copy — callers that only need `_id` (most of them: delete,
    /// vector/text prefilter) never pay for a second decode, and callers
    /// that need full content decode exactly once, at the point they
    /// actually consume it.
    pub fn get_matching(&self, filter: &Document) -> Vec<Arc<RawDocumentBuf>> {
        self.get_matching_first(filter, usize::MAX)
    }

    /// As `get_matching`, but stops after `n` matches — the first `n` in the
    /// order `get_matching` would return them.  An unsorted `limit` only ever
    /// needs a prefix, and without this a scan decoded (and an index lookup
    /// fetched) every match just to keep ten of them.
    pub fn get_matching_first(&self, filter: &Document, n: usize) -> Vec<Arc<RawDocumentBuf>> {
        self.matching_rows_first(filter, n).into_iter()
            .filter_map(|row| self.doc_at(row).cloned())
            .collect()
    }

    /// The rows matching `filter`, for callers that need membership rather
    /// than documents (search pre-filters).
    pub(crate) fn matching_rows(&self, filter: &Document) -> Vec<Row> {
        self.matching_rows_first(filter, usize::MAX)
    }

    fn matching_rows_first(&self, filter: &Document, n: usize) -> Vec<Row> {
        let is_match = |row: &Row| self.doc_at(*row)
            .is_some_and(|raw| raw.to_document().is_ok_and(|d| crate::query::matches(&d, filter)));
        if filter.is_empty() { return self.by_id.values().take(n).copied().collect(); }
        match self.try_index(filter) {
            Some(IndexResult::Exact(rows)) => rows.into_iter().take(n).collect(),
            Some(IndexResult::Candidates(rows)) => rows.into_iter().filter(is_match).take(n).collect(),
            None => self.by_id.values().copied().filter(is_match).take(n).collect(),
        }
    }

    pub fn clear(&mut self) {
        for map in self.regular.values_mut() { map.clear(); }
        for ti in self.text_indexes.values_mut() { ti.clear(); }
        self.vector_data.clear();
        self.by_id.clear();
        self.docs.clear();
        self.free.clear();
        self.vectors_stale = true;
    }

    // --- Index lookup ---

    pub fn try_index(&self, filter: &Document) -> Option<IndexResult> {
        for key in filter.keys() { if key.starts_with('$') { return None; } }
        // `_id` needs no declared index: `by_id` is keyed by it.  Without
        // this branch every find/update/delete by `_id` was a full scan —
        // ~1.1 s per call at 1M documents.
        let by_id = filter.get("_id").and_then(|c| self.lookup_id_rows(c));
        if filter.len() == 1 {
            if let Some((rows, exact)) = by_id {
                return Some(if exact { IndexResult::Exact(rows) } else { IndexResult::Candidates(rows) });
            }
            let (field, condition) = filter.iter().next().unwrap();
            if self.regular.contains_key(field) {
                return self.try_index_single_field(field, condition);
            }
        }
        // Multiple indexed fields can appear in the same filter — evaluate
        // every one of them and keep whichever yields the smallest
        // candidate set, rather than always taking the first indexed field
        // in filter-document insertion order. Insertion order has no
        // relationship to selectivity: e.g. `{metric, timestamp_ms: {$gte,
        // $lte}}` used to always resolve on `metric` (written first) even
        // when it's the low-cardinality field, turning a query that should
        // touch a handful of documents in a tight time window into a scan
        // of every document that ever had that metric.
        let mut best: Option<Vec<Row>> = by_id.map(|(rows, _)| rows);
        for (field, condition) in filter.iter() {
            if !self.regular.contains_key(field) { continue; }
            if let Some(rows) = self.lookup_rows(field, condition) {
                if best.as_ref().map_or(true, |b| rows.len() < b.len()) {
                    best = Some(rows);
                }
            }
        }
        best.map(IndexResult::Candidates)
    }

    fn try_index_single_field(&self, field: &str, condition: &bson::Bson) -> Option<IndexResult> {
        match condition {
            c if !is_operator_doc(c) => {
                Some(IndexResult::Exact(self.lookup_exact_rows(field, c)?))
            }
            bson::Bson::Document(ops) => {
                if ops.len() == 1 {
                    if let Some(eq_val) = ops.get("$eq") {
                        return Some(IndexResult::Exact(self.lookup_exact_rows(field, eq_val)?));
                    }
                }
                let is_pure_range = ops.keys().all(|k| matches!(k.as_str(), "$gt"|"$gte"|"$lt"|"$lte"));
                if is_pure_range {
                    let rows = self.lookup_range_rows(field,
                        ops.get("$gt").or(ops.get("$gte")), ops.contains_key("$gte"),
                        ops.get("$lt").or(ops.get("$lte")), ops.contains_key("$lte"))?;
                    // With both `$gt` and `$gte` (or `$lt` and `$lte`) the
                    // lookup takes one value with the other's inclusivity — a
                    // superset, so it is only safe as candidates.
                    let doubled = (ops.contains_key("$gt") && ops.contains_key("$gte"))
                        || (ops.contains_key("$lt") && ops.contains_key("$lte"));
                    return Some(if doubled { IndexResult::Candidates(rows) } else { IndexResult::Exact(rows) });
                }
                if let Some(eq_val) = ops.get("$eq") {
                    return Some(IndexResult::Candidates(self.lookup_exact_rows(field, eq_val)?));
                }
                None
            }
            _ => None,
        }
    }

    fn lookup_rows(&self, field: &str, condition: &bson::Bson) -> Option<Vec<Row>> {
        match condition {
            c if !is_operator_doc(c) => self.lookup_exact_rows(field, c),
            bson::Bson::Document(ops) => {
                if let Some(eq_val) = ops.get("$eq") { return self.lookup_exact_rows(field, eq_val); }
                let is_range = ops.keys().all(|k| matches!(k.as_str(), "$gt"|"$gte"|"$lt"|"$lte"));
                if is_range && !ops.is_empty() {
                    self.lookup_range_rows(field,
                        ops.get("$gt").or(ops.get("$gte")), ops.contains_key("$gte"),
                        ops.get("$lt").or(ops.get("$lte")), ops.contains_key("$lte"))
                } else { None }
            }
            _ => None,
        }
    }

    pub fn lookup_exact_rows(&self, field: &str, value: &bson::Bson) -> Option<Vec<Row>> {
        let key = bson_to_value(value)?;
        let mut rows = Vec::new();
        if let Some(p) = self.regular.get(field)?.get(&key) { p.extend_into(&mut rows); }
        Some(rows)
    }

    pub fn lookup_range_rows(&self, field: &str, min_val: Option<&bson::Bson>, min_inclusive: bool,
                             max_val: Option<&bson::Bson>, max_inclusive: bool) -> Option<Vec<Row>> {
        let idx = self.regular.get(field)?;
        let min = match min_val.and_then(bson_to_value) {
            Some(k) if min_inclusive => Bound::Included(k), Some(k) => Bound::Excluded(k), None => Bound::Unbounded,
        };
        let max = match max_val.and_then(bson_to_value) {
            Some(k) if max_inclusive => Bound::Included(k), Some(k) => Bound::Excluded(k), None => Bound::Unbounded,
        };
        let mut rows = Vec::new();
        if range_is_empty(&min, &max) { return Some(rows); }
        for (_, p) in idx.range((min, max)) { p.extend_into(&mut rows); }
        Some(rows)
    }

    /// Plan an `_id` condition against `by_id` directly.  Returns the
    /// matching rows in `_id` order (the order a full scan yields) and
    /// whether they are the exact answer for that condition.
    ///
    /// Only string operands are planned: `_id` is always a string
    /// (invariant 1), so anything else falls back to the scan, which is
    /// slow but cannot be wrong.
    fn lookup_id_rows(&self, condition: &bson::Bson) -> Option<(Vec<Row>, bool)> {
        let one = |id: &str| -> Vec<Row> { self.by_id.get(id).copied().into_iter().collect() };
        match condition {
            bson::Bson::String(id) => Some((one(id), true)),
            c if !is_operator_doc(c) => None,
            bson::Bson::Document(ops) => {
                let exact = ops.len() == 1;
                if let Some(eq_val) = ops.get("$eq") {
                    return Some((one(eq_val.as_str()?), exact));
                }
                if let Some(bson::Bson::Array(arr)) = ops.get("$in") {
                    let wanted: BTreeSet<&str> = arr.iter().map(|v| v.as_str()).collect::<Option<_>>()?;
                    let rows = wanted.into_iter().filter_map(|id| self.by_id.get(id).copied()).collect();
                    return Some((rows, exact));
                }
                let is_pure_range = ops.keys().all(|k| matches!(k.as_str(), "$gt"|"$gte"|"$lt"|"$lte"));
                if !is_pure_range { return None; }
                let bound = |inc: &str, exc: &str| -> Option<Bound<String>> {
                    Some(match (ops.get(inc), ops.get(exc)) {
                        (Some(v), None) => Bound::Included(v.as_str()?.to_string()),
                        (None, Some(v)) => Bound::Excluded(v.as_str()?.to_string()),
                        (None, None) => Bound::Unbounded,
                        // `$gt` and `$gte` together: rare, leave it to the scan.
                        (Some(_), Some(_)) => return None,
                    })
                };
                let (min, max) = (bound("$gte", "$gt")?, bound("$lte", "$lt")?);
                if range_is_empty(&min, &max) { return Some((Vec::new(), true)); }
                Some((self.by_id.range((min, max)).map(|(_, &row)| row).collect(), true))
            }
            _ => None,
        }
    }

    // --- Vector ---

    pub fn rebuild_vector_indexes(&mut self) {
        for (field, dim) in &self.vector_fields {
            let mut m = VectorMatrix::new(*dim);
            // `_id` order, so a rebuilt matrix is laid out deterministically.
            for &row in self.by_id.values() {
                let Some(doc) = self.doc_at(row) else { continue };
                if let Some(vec) = doc.get(field).ok().flatten().and_then(crate::vector::raw_vector) {
                    // Normalise at build time so cosine similarity collapses
                    // to a plain dot product at query time.
                    if vec.len() == *dim { m.push(row, &normalized(vec)); }
                }
            }
            self.vector_data.insert(field.clone(), m);
        }
        self.vectors_stale = false;
    }

    pub fn ensure_vectors_fresh(&mut self) { if self.vectors_stale { self.rebuild_vector_indexes(); } }

    pub fn vector_search(&self, field: &str, query: &[f32], limit: usize) -> Vec<(Document, f32)> {
        self.vector_search_inner(field, query, limit, None)
    }

    pub fn vector_search_filtered(&self, field: &str, query: &[f32], limit: usize,
                                  allowed: &HashSet<Row>) -> Vec<(Document, f32)> {
        self.vector_search_inner(field, query, limit, Some(allowed))
    }

    fn vector_search_inner(&self, field: &str, query: &[f32], limit: usize,
                           allowed: Option<&HashSet<Row>>) -> Vec<(Document, f32)> {
        let Some(m) = self.vector_data.get(field) else { return Vec::new() };
        let (n, dim) = (m.len(), m.dim);
        if n == 0 || dim == 0 || limit == 0 { return Vec::new(); }

        // Normalise query once — rows are already normalised at build time,
        // so cosine similarity is just a dot product.
        let qn: f32 = query.iter().map(|x| x*x).sum::<f32>().sqrt();
        if qn == 0.0 { return Vec::new(); }
        let q_norm: Vec<f32> = query.iter().map(|x| x / qn).collect();

        // Score only allowed rows when pre-filtered — no dot products for
        // documents that would be dropped anyway.  Parallel for large n.
        let score = |i: usize| -> Option<(Row, f32)> {
            let row = m.rows[i];
            if allowed.is_some_and(|a| !a.contains(&row)) { return None; }
            let v = &m.data[i*dim..(i+1)*dim];
            Some((row, v.iter().zip(&q_norm).map(|(a,b)| a*b).sum()))
        };
        let scored: Vec<(Row, f32)> = if n > PARALLEL_THRESHOLD {
            (0..n).into_par_iter().filter_map(score).collect()
        } else {
            (0..n).filter_map(score).collect()
        };
        self.decode_top(scored, limit)
    }

    pub fn text_search(&self, field: &str, query: &str, limit: usize) -> Vec<(Document, f32)> {
        let Some(ti) = self.text_indexes.get(field) else { return Vec::new() };
        self.decode_top(ti.scores(query), limit)
    }

    pub fn text_search_filtered(&self, field: &str, query: &str, limit: usize,
                                allowed: &HashSet<Row>) -> Vec<(Document, f32)> {
        let Some(ti) = self.text_indexes.get(field) else { return Vec::new() };
        let mut scored = ti.scores(query);
        scored.retain(|(row, _)| allowed.contains(row));
        self.decode_top(scored, limit)
    }

    /// The `limit` best-scoring rows, best first, decoded.
    ///
    /// Ties break on `_id` so the ranking is a total order: rows are reused
    /// and matrix positions shift as documents are swap-removed, and text
    /// scores come out of a HashMap, so without it equal-scoring documents
    /// came back in an order that changed over time and between processes.
    /// Only the cut-off's ties need their ids read: select the top `limit`
    /// by score alone, keep everything tied with the last of them, then
    /// order that small set fully.
    fn decode_top(&self, mut scored: Vec<(Row, f32)>, limit: usize) -> Vec<(Document, f32)> {
        if limit == 0 || scored.is_empty() { return Vec::new(); }
        if limit < scored.len() {
            scored.select_nth_unstable_by(limit - 1, |a, b| b.1.total_cmp(&a.1));
            let cutoff = scored[limit - 1].1;
            scored.retain(|e| e.1.total_cmp(&cutoff).is_ge());
        }
        let id = |row: Row| self.id_of(row).unwrap_or("");
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| id(a.0).cmp(id(b.0))));
        scored.truncate(limit);
        scored.into_iter()
            .filter_map(|(row, s)| Some((self.doc_at(row)?.to_document().ok()?, s)))
            .collect()
    }
}

/// `BTreeMap::range` panics on an inverted range, or on `(x, x)` with both
/// ends excluded.  A filter like `{age: {$gt: 5, $lt: 2}}` is legal and
/// simply matches nothing — but a panic there, under a write lock in
/// `update_many`/`delete_many`, would poison the collection (invariant 4).
fn range_is_empty<K: Ord>(min: &Bound<K>, max: &Bound<K>) -> bool {
    match (min, max) {
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a >= b,
        _ => false,
    }
}

fn is_operator_doc(val: &bson::Bson) -> bool {
    matches!(val, bson::Bson::Document(d) if d.keys().any(|k| k.starts_with('$')))
}

/// Flatten a raw BSON value into the text an inverted index should see.
/// Mirror of the old `&bson::Bson`-based `text_index_value`, operating on
/// `RawBsonRef` (borrowed, no allocation until a `String` is actually built)
/// so `add_raw`/`remove` never need to decode a document just to index its
/// text fields.
///
/// Strings index as themselves.  Arrays are squashed into one space-joined
/// string and indexed like any other field, which is what Lucene does for
/// multi-valued fields -- previously an array field matched
/// `Bson::String` and was skipped, so declaring a text index on `tags` built
/// an empty index in silence and every search against it returned nothing.
/// Nested arrays flatten recursively; non-textual scalars are stringified so
/// numeric tags stay searchable.
fn raw_text_index_value(val: RawBsonRef) -> Option<String> {
    match val {
        RawBsonRef::String(s) => Some(s.to_string()),
        RawBsonRef::Array(arr) => {
            let parts: Vec<String> = arr.into_iter()
                .filter_map(|v| raw_text_index_value(v.ok()?))
                .collect();
            if parts.is_empty() { None } else { Some(parts.join(" ")) }
        }
        RawBsonRef::Int32(i) => Some(i.to_string()),
        RawBsonRef::Int64(i) => Some(i.to_string()),
        RawBsonRef::Double(f) => Some(f.to_string()),
        RawBsonRef::Boolean(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Mirror of `raw_bson_to_value` for already-decoded `&bson::Bson` values —
/// still used by filter-side lookups (`lookup_exact_ids`/`lookup_range_ids`
/// take a query filter's `&Bson`, not a stored document's raw bytes).
fn bson_to_value(v: &bson::Bson) -> Option<Value> {
    match v {
        bson::Bson::Null => Some(Value::Null), bson::Bson::Boolean(b) => Some(Value::Bool(*b)),
        bson::Bson::Int32(i) => Some(Value::I32(*i)), bson::Bson::Int64(i) => Some(Value::I64(*i)),
        bson::Bson::Double(f) => Some(Value::Double(OrderedFloat(*f))),
        bson::Bson::String(s) => Some(Value::String(s.clone())),
        bson::Bson::DateTime(d) => Some(Value::DateTime(d.timestamp_millis())),
        bson::Bson::ObjectId(o) => Some(Value::ObjectId(o.bytes())),
        _ => None,
    }
}

/// As `bson_to_value`, for a value borrowed straight out of a stored
/// document's raw BSON bytes — used by `add_raw`/`remove` so indexing never
/// needs a full `Document` decode.
fn raw_bson_to_value(v: RawBsonRef) -> Option<Value> {
    match v {
        RawBsonRef::Null => Some(Value::Null), RawBsonRef::Boolean(b) => Some(Value::Bool(b)),
        RawBsonRef::Int32(i) => Some(Value::I32(i)), RawBsonRef::Int64(i) => Some(Value::I64(i)),
        RawBsonRef::Double(f) => Some(Value::Double(OrderedFloat(f))),
        RawBsonRef::String(s) => Some(Value::String(s.to_string())),
        RawBsonRef::DateTime(d) => Some(Value::DateTime(d.timestamp_millis())),
        RawBsonRef::ObjectId(o) => Some(Value::ObjectId(o.bytes())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*; use bson::doc;
    fn m() -> IndexManager {
        IndexManager::new(&["email".into(),"age".into()], &[("embedding".into(),3)], &["content".into()])
    }
    fn mt() -> IndexManager {
        IndexManager::new(&[], &[], &["tags".into(), "content".into()])
    }

    #[test]
    fn array_fields_are_text_indexed() {
        // Declaring a text index on an array field used to build an empty
        // index in silence: the add path matched Bson::String only.
        let mut im = mt();
        im.add(doc!{"_id":"a","tags":["gaming","grim-dawn","ritualist"]});
        im.add(doc!{"_id":"b","tags":["kubernetes","helm"]});

        assert_eq!(im.text_search("tags","gaming",5).len(), 1);
        assert_eq!(im.text_search("tags","gaming",5)[0].0.get_str("_id").unwrap(), "a");
        assert_eq!(im.text_search("tags","helm",5)[0].0.get_str("_id").unwrap(), "b");
        // Multi-valued fields squash to one string, so any element matches.
        assert_eq!(im.text_search("tags","ritualist",5)[0].0.get_str("_id").unwrap(), "a");
    }

    #[test]
    fn array_text_index_survives_removal() {
        let mut im = mt();
        im.add(doc!{"_id":"a","tags":["alpha","beta"]});
        im.add(doc!{"_id":"b","tags":["alpha"]});
        assert_eq!(im.text_search("tags","alpha",5).len(), 2);
        im.remove("a");
        assert_eq!(im.text_search("tags","alpha",5).len(), 1);
    }

    #[test]
    fn non_string_scalars_index_as_text() {
        let mut im = mt();
        im.add(doc!{"_id":"a","tags":["release", 2026i32]});
        // Numeric elements are stringified, so they stay searchable.
        assert_eq!(im.text_search("tags","2026",5).len(), 1);
    }

    /// The `_id`s of some rows, in the order given.
    fn names(im: &IndexManager, rows: &[Row]) -> Vec<String> { rows.iter().map(|&r| im.id_of(r).unwrap().to_string()).collect() }
    fn row_set(im: &IndexManager, ids: &[&str]) -> HashSet<Row> { ids.iter().map(|id| im.by_id[*id]).collect() }
    #[test] fn exact_lookup() {
        let mut im = m(); im.add(doc!{"_id":"a","email":"a@b.com","age":30}); im.add(doc!{"_id":"b","email":"c@d.com","age":25}); im.add(doc!{"_id":"c","email":"a@b.com","age":30});
        assert_eq!(names(&im, &im.lookup_exact_rows("email",&bson::Bson::String("a@b.com".into())).unwrap()), ["a","c"]);
        assert_eq!(im.lookup_exact_rows("email",&bson::Bson::String("nope".into())), Some(vec![]));
    }
    #[test] fn range_lookup() {
        let mut im = m(); im.add(doc!{"_id":"a","age":20}); im.add(doc!{"_id":"b","age":30}); im.add(doc!{"_id":"c","age":40});
        assert_eq!(names(&im, &im.lookup_range_rows("age",Some(&bson::Bson::Int32(25)),true,Some(&bson::Bson::Int32(35)),true).unwrap()), ["b"]);
    }
    #[test] fn try_exact_eq() { let mut im=m(); im.add(doc!{"_id":"x","email":"x@y.com"}); let r=im.try_index(&doc!{"email":"x@y.com"}).unwrap(); assert!(matches!(&r, IndexResult::Exact(_))); assert_eq!(names(&im, r.rows()), ["x"]); }
    #[test] fn try_exact_range() { let mut im=m(); for i in 0..10{im.add(doc!{"_id":i.to_string(),"age":i*10});} let r=im.try_index(&doc!{"age":{"$gt":25,"$lt":55}}).unwrap(); assert!(matches!(&r, IndexResult::Exact(_))); let mut v=names(&im, r.rows());v.sort();assert_eq!(v,vec!["3","4","5"]);}
    #[test] fn try_candidates_multi() { let mut im=m(); im.add(doc!{"_id":"a","email":"x@y.com","status":"active"}); let r=im.try_index(&doc!{"email":"x@y.com","status":"active"}).unwrap(); assert!(matches!(&r, IndexResult::Candidates(_))); }
    #[test] fn try_fallback() { let mut im=m(); im.add(doc!{"_id":"a","email":"x@y.com"}); assert!(im.try_index(&doc!{"$or":[{"email":"x@y.com"},{"age":99}]}).is_none()); }
    fn im_ids() -> IndexManager { let mut im=m(); for i in 0..10 { im.add(doc!{"_id":format!("k{i}"),"age":i}); } im }
    #[test] fn try_id_eq_is_exact_without_index() {
        let im=im_ids();
        for q in [doc!{"_id":"k3"}, doc!{"_id":{"$eq":"k3"}}] { let r=im.try_index(&q).unwrap(); assert!(matches!(r, IndexResult::Exact(_))); assert_eq!(names(&im, r.rows()), ["k3"]); }
        assert!(im.try_index(&doc!{"_id":"nope"}).unwrap().rows().is_empty());
    }
    #[test] fn try_id_in_sorted_deduped() {
        let im=im_ids(); let r=im.try_index(&doc!{"_id":{"$in":["k7","k2","nope","k2"]}}).unwrap();
        assert!(matches!(r, IndexResult::Exact(_))); assert_eq!(names(&im, r.rows()), ["k2","k7"]);
        assert!(im.try_index(&doc!{"_id":{"$in":["k1",5]}}).is_none());
    }
    #[test] fn try_id_range() {
        let im=im_ids();
        assert_eq!(names(&im, im.try_index(&doc!{"_id":{"$gte":"k3","$lt":"k6"}}).unwrap().rows()), ["k3","k4","k5"]);
        assert!(im.try_index(&doc!{"_id":{"$gt":"k6","$lt":"k2"}}).unwrap().rows().is_empty());
        assert!(im.try_index(&doc!{"_id":{"$gt":"k6","$gte":"k2"}}).is_none());
    }
    #[test] fn try_id_with_other_field_is_candidates() {
        let im=im_ids(); let r=im.try_index(&doc!{"_id":"k3","age":4}).unwrap();
        assert!(matches!(r, IndexResult::Candidates(_))); assert!(im.get_matching(&doc!{"_id":"k3","age":4}).is_empty());
    }
    #[test] fn inverted_or_empty_range_does_not_panic() {
        let im=im_ids();
        assert!(im.get_matching(&doc!{"age":{"$gt":5,"$lt":2}}).is_empty());
        assert!(im.get_matching(&doc!{"age":{"$gt":5,"$lt":5}}).is_empty());
        assert_eq!(im.get_matching(&doc!{"age":{"$gte":5,"$lte":5}}).len(), 1);
    }
    #[test] fn doubled_bound_is_not_exact() {
        let im=im_ids(); let q=doc!{"age":{"$gt":5,"$gte":3}};
        assert!(matches!(im.try_index(&q).unwrap(), IndexResult::Candidates(_)));
        assert_eq!(im.get_matching(&q).len(), 4);
    }
    /// A freed slot is reused by the next insert; nothing indexed under the
    /// old occupant may resolve to the new one.
    #[test] fn reused_rows_carry_no_stale_entries() {
        let mut im = m();
        im.add(doc!{"_id":"old","email":"old@x","age":1,"content":"zebra crossing","embedding":[1.0,0.0,0.0]});
        im.add(doc!{"_id":"keep","email":"keep@x","age":2,"content":"plain words","embedding":[0.0,1.0,0.0]});
        im.rebuild_vector_indexes();
        let old_row = im.by_id["old"];
        im.remove("old");
        im.add(doc!{"_id":"new","email":"new@x","age":3,"content":"giraffe","embedding":[0.0,0.0,1.0]});
        assert_eq!(im.by_id["new"], old_row, "the freed row is reused");

        assert!(im.get_matching(&doc!{"email":"old@x"}).is_empty());
        assert!(im.get_matching(&doc!{"age":1}).is_empty());
        assert!(im.text_search("content","zebra",5).is_empty());
        assert_eq!(im.text_search("content","giraffe",5)[0].0.get_str("_id").unwrap(), "new");
        let top = im.vector_search("embedding",&[0.0,0.0,1.0],1);
        assert_eq!(top[0].0.get_str("_id").unwrap(), "new");
        assert_eq!(im.vector_data["embedding"].len(), 2);
    }
    #[test] fn postings_demote_and_drop() {
        let mut im = m();
        for id in ["a","b","c"] { im.add(doc!{"_id":id,"age":7}); }
        assert!(matches!(im.regular["age"].values().next(), Some(Postings::Many(s)) if s.len() == 3));
        im.remove("a"); im.remove("b");
        assert!(matches!(im.regular["age"].values().next(), Some(Postings::One(_))));
        im.remove("c");
        assert!(im.regular["age"].is_empty());
    }
    #[test] fn ties_rank_by_id_whatever_the_row_order() {
        let mut im = m();
        // Rows are assigned z, y, x; identical text gives identical scores.
        for id in ["z","y","x","w"] { im.add(doc!{"_id":id,"content":"same words here"}); }
        im.remove("w");
        let got: Vec<String> = im.text_search("content","same",2).iter().map(|(d,_)| d.get_str("_id").unwrap().to_string()).collect();
        assert_eq!(got, ["x","y"]);
    }
    #[test] fn matching_exact() { let mut im=m(); for i in 0..10{im.add(doc!{"_id":i.to_string(),"age":i*10});} assert_eq!(im.get_matching(&doc!{"age":{"$gte":30,"$lte":50}}).len(),3); }
    #[test] fn vec_basic() { let mut im=m(); im.add(doc!{"_id":"near","embedding":[1.0,0.0,0.0]}); im.add(doc!{"_id":"far","embedding":[0.0,0.0,1.0]}); im.add(doc!{"_id":"mid","embedding":[0.7,0.0,0.7]}); im.rebuild_vector_indexes(); let r=im.vector_search("embedding",&[1.0,0.0,0.0],10); assert_eq!(r.len(),3); assert_eq!(r[0].0.get_str("_id").unwrap(),"near"); assert!(r[0].1>=r[1].1); }
    #[test] fn vec_limit() { let mut im=m(); for i in 0..5{im.add(doc!{"_id":i.to_string(),"embedding":[i as f64,0.0,0.0]});} im.rebuild_vector_indexes(); assert_eq!(im.vector_search("embedding",&[0.0,1.0,0.0],2).len(),2); }
    #[test] fn vec_wrong_dim() { let mut im=m(); im.add(doc!{"_id":"good","embedding":[1.0,0.0,0.0]}); im.add(doc!{"_id":"bad","embedding":[1.0,0.0]}); im.rebuild_vector_indexes(); assert_eq!(im.vector_search("embedding",&[1.0,0.0,0.0],10).len(),1); }

    // --- Tests for items #1–#4: incremental updates, normalised scores, filtered search, top-k ---

    #[test]
    fn vec_incremental_add_visible() {
        // After rebuild, new add() calls must be visible in search without
        // requiring another rebuild (item #3).
        let mut im = m();
        im.add(doc!{"_id":"a","embedding":[1.0,0.0,0.0]});
        im.add(doc!{"_id":"b","embedding":[0.0,1.0,0.0]});
        im.rebuild_vector_indexes();
        // Simulate a RAG-style interleaved insert/search workload
        im.add(doc!{"_id":"c","embedding":[0.9,0.1,0.0]});
        let r = im.vector_search("embedding",&[1.0,0.0,0.0],10);
        assert_eq!(r.len(),3);
        let ids: Vec<_> = r.iter().map(|(d,_)| d.get_str("_id").unwrap()).collect();
        assert!(ids.contains(&"c"), "incrementally added doc must be visible in search");
    }

    #[test]
    fn vec_incremental_after_delete_rebuild() {
        // Delete sets stale → next search rebuilds → subsequent adds are incremental again
        let mut im = m();
        im.add(doc!{"_id":"a","embedding":[1.0,0.0,0.0]});
        im.add(doc!{"_id":"b","embedding":[0.0,1.0,0.0]});
        im.rebuild_vector_indexes();
        im.remove("a"); // sets stale
        im.ensure_vectors_fresh(); // rebuilds → stale=false
        im.add(doc!{"_id":"c","embedding":[1.0,0.0,0.0]}); // incremental
        let r = im.vector_search("embedding",&[1.0,0.0,0.0],10);
        assert_eq!(r.len(),2); // b and c (a was deleted)
        let ids: Vec<_> = r.iter().map(|(d,_)| d.get_str("_id").unwrap()).collect();
        assert!(!ids.contains(&"a"));
        assert!(ids.contains(&"c"));
    }

    #[test]
    fn vec_normalised_scores_cosine() {
        // Vectors with different magnitudes but same direction should have
        // cosine similarity 1.0 (item #1: normalise at build time).
        let mut im = m();
        im.add(doc!{"_id":"big","embedding":[3.0,0.0,0.0]});
        im.add(doc!{"_id":"orth","embedding":[0.0,5.0,0.0]});
        im.rebuild_vector_indexes();
        let r = im.vector_search("embedding",&[1.0,0.0,0.0],10);
        assert_eq!(r.len(),2);
        assert!((r[0].1 - 1.0).abs() < 1e-5, "same-direction vector should have cosine=1.0");
        assert_eq!(r[0].0.get_str("_id").unwrap(),"big");
        assert!((r[1].1 - 0.0).abs() < 1e-5, "orthogonal vector should have cosine=0.0");
    }

    #[test]
    fn vec_filtered_scores_only_allowed() {
        // Filtered search must only return allowed docs with correct scores (item #4).
        let mut im = m();
        im.add(doc!{"_id":"a","embedding":[1.0,0.0,0.0]});
        im.add(doc!{"_id":"b","embedding":[0.9,0.1,0.0]});
        im.add(doc!{"_id":"c","embedding":[0.8,0.2,0.0]});
        im.rebuild_vector_indexes();
        let allowed = row_set(&im, &["a", "c"]);
        let r = im.vector_search_filtered("embedding",&[1.0,0.0,0.0],10,&allowed);
        assert_eq!(r.len(),2);
        let ids: Vec<_> = r.iter().map(|(d,_)| d.get_str("_id").unwrap()).collect();
        assert!(ids.contains(&"a"));
        assert!(ids.contains(&"c"));
        assert!(!ids.contains(&"b"), "filtered-out doc must not appear");
        // a is closer to [1,0,0] than c
        assert!(r[0].1 >= r[1].1);
    }

    #[test]
    fn vec_filtered_empty_allowed() {
        let mut im = m();
        im.add(doc!{"_id":"a","embedding":[1.0,0.0,0.0]});
        im.rebuild_vector_indexes();
        let allowed = HashSet::new();
        let r = im.vector_search_filtered("embedding",&[1.0,0.0,0.0],10,&allowed);
        assert!(r.is_empty());
    }

    #[test]
    fn vec_topk_correctness_large() {
        // Verify top-k returns the correct top-k elements in order (item #2).
        let mut im = m();
        for i in 0..200 {
            let f = i as f64 / 200.0;
            im.add(doc!{"_id":i.to_string(),"embedding":[f,1.0-f,0.0]});
        }
        im.rebuild_vector_indexes();
        let r = im.vector_search("embedding",&[1.0,0.0,0.0],5);
        assert_eq!(r.len(),5);
        // Descending order
        for i in 0..4 { assert!(r[i].1 >= r[i+1].1, "scores must be descending"); }
        // Doc 199 has the highest first component (0.995)
        assert_eq!(r[0].0.get_str("_id").unwrap(), "199");
    }

    #[test]
    fn vec_limit_zero() {
        let mut im = m();
        im.add(doc!{"_id":"a","embedding":[1.0,0.0,0.0]});
        im.rebuild_vector_indexes();
        assert!(im.vector_search("embedding",&[1.0,0.0,0.0],0).is_empty());
        let allowed = row_set(&im, &["a"]);
        assert!(im.vector_search_filtered("embedding",&[1.0,0.0,0.0],0,&allowed).is_empty());
    }
}

#[cfg(test)]
mod incremental_vector_tests {
    use super::*;
    use bson::doc;

    fn im() -> IndexManager {
        IndexManager::new(&[], &[("embedding".into(), 3)], &[])
    }

    /// Search results after in-place removal must equal a full rebuild.
    fn assert_matches_rebuild(im: &mut IndexManager, query: &[f32]) {
        let incremental = im.vector_search("embedding", query, 100);
        im.rebuild_vector_indexes();
        let rebuilt = im.vector_search("embedding", query, 100);

        let ids = |r: &Vec<(Document, f32)>| -> Vec<(String, String)> {
            let mut v: Vec<_> = r
                .iter()
                .map(|(d, s)| (d.get_str("_id").unwrap().to_string(), format!("{s:.5}")))
                .collect();
            v.sort();
            v
        };
        assert_eq!(ids(&incremental), ids(&rebuilt));
    }

    #[test]
    fn remove_keeps_ids_and_matrix_in_lockstep() {
        let mut m = im();
        for i in 0..6 {
            let f = i as f64;
            m.add(doc! {"_id": i.to_string(), "embedding": [f, 1.0, 0.0]});
        }
        m.rebuild_vector_indexes();

        // Remove from the middle, the front, and the end.
        m.remove("3");
        m.remove("0");
        m.remove("5");

        let vm = m.vector_data.get("embedding").unwrap();
        assert_eq!(vm.rows.len(), 3);
        assert_eq!(vm.data.len(), vm.rows.len() * vm.dim);
        let mut got: Vec<_> = vm.rows.iter().map(|&r| m.id_of(r).unwrap()).collect();
        got.sort();
        assert_eq!(got, vec!["1", "2", "4"]);

        assert_matches_rebuild(&mut m, &[1.0, 1.0, 0.0]);
    }

    #[test]
    fn remove_last_remaining_row() {
        let mut m = im();
        m.add(doc! {"_id": "only", "embedding": [1.0, 0.0, 0.0]});
        m.rebuild_vector_indexes();
        m.remove("only");

        let vm = m.vector_data.get("embedding").unwrap();
        assert!(vm.rows.is_empty());
        assert!(vm.data.is_empty());
        assert!(m.vector_search("embedding", &[1.0, 0.0, 0.0], 10).is_empty());
    }

    #[test]
    fn interleaved_add_remove_stays_consistent() {
        let mut m = im();
        for i in 0..10 {
            m.add(doc! {"_id": i.to_string(), "embedding": [i as f64, 1.0, 0.0]});
        }
        m.rebuild_vector_indexes();

        for i in 0..10 {
            if i % 2 == 0 {
                m.remove(&i.to_string());
            }
            m.add(doc! {"_id": format!("n{i}"), "embedding": [1.0, i as f64, 0.0]});
        }

        let vm = m.vector_data.get("embedding").unwrap();
        assert_eq!(vm.data.len(), vm.rows.len() * vm.dim);
        assert_eq!(vm.rows.len(), m.doc_count());
        assert_matches_rebuild(&mut m, &[1.0, 1.0, 0.0]);
    }

    #[test]
    fn update_pattern_remove_then_add() {
        // update_one() is remove() + add(); the row must be replaced, not duplicated.
        let mut m = im();
        m.add(doc! {"_id": "a", "embedding": [1.0, 0.0, 0.0]});
        m.add(doc! {"_id": "b", "embedding": [0.0, 1.0, 0.0]});
        m.rebuild_vector_indexes();

        m.remove("a");
        m.add(doc! {"_id": "a", "embedding": [0.0, 0.0, 1.0]});

        assert_eq!(m.vector_data["embedding"].rows.len(), 2, "row should be replaced, not duplicated");

        let r = m.vector_search("embedding", &[0.0, 0.0, 1.0], 1);
        assert_eq!(r[0].0.get_str("_id").unwrap(), "a");
        assert!((r[0].1 - 1.0).abs() < 1e-5, "must use the updated vector");
    }

    #[test]
    fn removing_doc_with_no_vector_is_harmless() {
        let mut m = im();
        m.add(doc! {"_id": "vec", "embedding": [1.0, 0.0, 0.0]});
        m.add(doc! {"_id": "novec", "other": 1});
        m.add(doc! {"_id": "baddim", "embedding": [1.0, 0.0]});
        m.rebuild_vector_indexes();

        m.remove("novec");
        m.remove("baddim");

        let vm = m.vector_data.get("embedding").unwrap();
        assert_eq!(vm.rows.iter().map(|&r| m.id_of(r).unwrap()).collect::<Vec<_>>(), ["vec"]);
        assert_eq!(vm.data.len(), vm.dim);
    }
}
