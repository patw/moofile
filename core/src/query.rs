/// Lazy query builder and filter evaluation.
///
/// [`Query`] is the main builder — chain `.sort()`, `.skip()`, `.limit()`,
/// then materialise with `.to_list()`, `.first()`, or `.count()`.
///
/// Auto-embedding methods:
/// - `.semantic()` — auto-embeds query text for semantic search
/// - `.hybrid_search()` — now accepts `None` for query_vector to auto-embed

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use bson::{Bson, Document};

use crate::embed::{self, ModelUri};
use crate::CollectionInner;
use crate::MooFileError;

// ---------------------------------------------------------------------------
// Filter evaluation
// ---------------------------------------------------------------------------

/// Return `true` if `doc` satisfies every condition in `filter`.
pub fn matches(doc: &Document, filter: &Document) -> bool {
    for (key, value) in filter.iter() {
        match key.as_str() {
            "$and" => {
                if let Some(Bson::Array(subs)) = filter.get(key) {
                    if !subs.iter().all(|sub| matches(doc, sub.as_document().unwrap())) {
                        return false;
                    }
                    continue;
                }
                return false;
            }
            "$or" => {
                if let Some(Bson::Array(subs)) = filter.get(key) {
                    if !subs.iter().any(|sub| matches(doc, sub.as_document().unwrap())) {
                        return false;
                    }
                    continue;
                }
                return false;
            }
            "$not" => {
                if let Some(sub) = value.as_document() {
                    if matches(doc, sub) {
                        return false;
                    }
                    continue;
                }
                return false;
            }
            _ => {
                let field_val = doc.get(key);
                if !eval_field_condition(field_val, value) {
                    return false;
                }
            }
        }
    }
    true
}

fn eval_field_condition(field_val: Option<&Bson>, condition: &Bson) -> bool {
    match condition {
        c if !is_operator_doc(c) => field_val == Some(c),
        Bson::Document(ops) => {
            for (op, op_val) in ops {
                match op.as_str() {
                    "$eq" => { if field_val != Some(op_val) { return false; } }
                    "$ne" => { if field_val == Some(op_val) { return false; } }
                    "$gt" => { if !cmp_op(field_val, op_val, std::cmp::Ordering::Greater) { return false; } }
                    "$gte" => {
                        if !cmp_op(field_val, op_val, std::cmp::Ordering::Greater)
                            && field_val != Some(op_val) { return false; }
                    }
                    "$lt" => { if !cmp_op(field_val, op_val, std::cmp::Ordering::Less) { return false; } }
                    "$lte" => {
                        if !cmp_op(field_val, op_val, std::cmp::Ordering::Less)
                            && field_val != Some(op_val) { return false; }
                    }
                    "$in" => match op_val {
                        Bson::Array(arr) => { if !arr.contains(&field_val.unwrap_or(&Bson::Null)) { return false; } }
                        _ => return false,
                    },
                    "$nin" => match op_val {
                        Bson::Array(arr) => { if arr.contains(&field_val.unwrap_or(&Bson::Null)) { return false; } }
                        _ => return false,
                    },
                    "$exists" => {
                        let should_exist = op_val.as_bool().unwrap_or(false);
                        if should_exist != (field_val.is_some() && field_val != Some(&Bson::Null)) { return false; }
                    }
                    "$elemMatch" => {
                        let sub_filter = match op_val.as_document() {
                            Some(d) => d, None => return false,
                        };
                        let arr = match field_val {
                            Some(Bson::Array(arr)) => arr, _ => return false,
                        };
                        if !arr.iter().any(|elem| elem_matches(elem, sub_filter)) { return false; }
                    }
                    _ => return false,
                }
            }
            true
        }
        _ => false,
    }
}

fn is_operator_doc(val: &Bson) -> bool {
    match val {
        Bson::Document(d) => d.keys().any(|k| k.starts_with('$')),
        _ => false,
    }
}

fn elem_matches(elem: &Bson, filter: &Document) -> bool {
    match elem {
        Bson::Document(doc) => matches(doc, filter),
        _ => eval_field_condition(Some(elem), &Bson::Document(filter.clone())),
    }
}

fn cmp_op(a: Option<&Bson>, b: &Bson, target: std::cmp::Ordering) -> bool {
    let a = match a {
        Some(v) => v,
        None => return target == std::cmp::Ordering::Greater,
    };
    match bson_cmp(a, b) {
        Some(ord) => ord == target,
        None => false,
    }
}

fn bson_cmp(a: &Bson, b: &Bson) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Bson::Int32(a), Bson::Int32(b)) => Some(a.cmp(b)),
        (Bson::Int64(a), Bson::Int64(b)) => Some(a.cmp(b)),
        (Bson::Double(a), Bson::Double(b)) => a.partial_cmp(b),
        (Bson::Int32(a), Bson::Double(b)) => (*a as f64).partial_cmp(b),
        (Bson::Double(a), Bson::Int32(b)) => a.partial_cmp(&(*b as f64)),
        (Bson::Int64(a), Bson::Double(b)) => (*a as f64).partial_cmp(b),
        (Bson::Double(a), Bson::Int64(b)) => a.partial_cmp(&(*b as f64)),
        (Bson::Int32(a), Bson::Int64(b)) => (*a as i64).cmp(b).into(),
        (Bson::Int64(a), Bson::Int32(b)) => a.cmp(&(*b as i64)).into(),
        (Bson::String(a), Bson::String(b)) => Some(a.cmp(b)),
        (Bson::Boolean(a), Bson::Boolean(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Query builder
// ---------------------------------------------------------------------------

/// Lazy query chain.
#[derive(Debug, Clone)]
pub struct Query {
    pub(crate) inner: Arc<RwLock<CollectionInner>>,
    pub(crate) filter: Document,
    pub(crate) sort_key: Option<String>,
    pub(crate) sort_desc: bool,
    pub(crate) skip_n: usize,
    pub(crate) limit_n: Option<usize>,
    pub(crate) group_field: Option<String>,
    pub(crate) agg_funcs: Vec<AggFunc>,
}

impl Query {
    pub(crate) fn new(inner: Arc<RwLock<CollectionInner>>, filter: Document) -> Self {
        Self {
            inner,
            filter,
            sort_key: None,
            sort_desc: false,
            skip_n: 0,
            limit_n: None,
            group_field: None,
            agg_funcs: Vec::new(),
        }
    }

    pub fn sort(mut self, field: impl Into<String>, descending: bool) -> Self {
        self.sort_key = Some(field.into());
        self.sort_desc = descending;
        self
    }

    pub fn skip(mut self, n: usize) -> Self {
        self.skip_n = n;
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit_n = Some(n);
        self
    }

    pub fn group(mut self, field: impl Into<String>) -> Self {
        self.group_field = Some(field.into());
        self
    }

    pub fn agg(mut self, funcs: Vec<AggFunc>) -> Self {
        self.agg_funcs = funcs;
        self
    }

    /// Switch to vector similarity search with a raw query vector.
    pub fn vector_search(
        self,
        field: impl Into<String>,
        query_vector: Vec<f32>,
        limit: usize,
    ) -> VectorQuery {
        VectorQuery {
            inner: self.inner,
            field: field.into(),
            query_vector: query_vector.into(),
            limit,
            pre_filter: self.filter,
        }
    }

    /// Switch to semantic search — auto-embeds the query text.
    ///
    /// The `source_field` must have been configured with `auto_embed` at
    /// collection open time.  The query text is automatically prefixed
    /// with the configured `query_prefix` and embedded using the model.
    ///
    /// Returns a `VectorQuery` targeting the associated vector field.
    pub fn semantic(
        self,
        source_field: impl Into<String>,
        query_text: impl Into<String>,
        limit: usize,
    ) -> Result<VectorQuery, MooFileError> {
        let source_field = source_field.into();
        let query_text = query_text.into();

        // Read the autoembed config to know which model and field to use
        let (target_field, query_vector) = {
            let inner = self.inner.read().expect("lock poisoned");

            let config = inner.auto_embeds.get(&source_field)
                .ok_or_else(|| MooFileError::NoAutoEmbed(source_field.clone()))?;

            // Resolve model path
            let model_uri = ModelUri::parse(&config.model);
            let cache_dir = crate::default_model_cache_dir();
            let local_path = model_uri.resolve(&cache_dir)?;
            let model_key = local_path.to_string_lossy().into_owned();

            let engine = inner.embedding_engines.get(&model_key)
                .ok_or_else(|| MooFileError::NoAutoEmbed(source_field.clone()))?;

            // Prefix and embed
            let prefixed = format!("{}{}", config.query_prefix, query_text);
            let raw_emb = engine.embed(&prefixed)?;

            // Truncate / normalize
            let emb: Vec<f32> = if raw_emb.len() > config.dims {
                raw_emb[..config.dims].to_vec()
            } else {
                raw_emb
            };

            let emb = if config.normalize {
                let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm > 0.0 { emb.iter().map(|x| x / norm).collect() } else { emb }
            } else {
                emb
            };

            // Quantize and dequantize to match stored format, then convert to f32
            let quantized = embed::quantize(&emb, config.precision);
            let dequantized = embed::dequantize(&quantized, config.precision, config.dims);

            (config.target_field.clone(), dequantized)
        };

        Ok(VectorQuery {
            inner: self.inner,
            field: target_field,
            query_vector: EmbeddingOrVector::Vector(query_vector),
            limit,
            pre_filter: self.filter,
        })
    }

    /// Switch to BM25 text search.
    pub fn text_search(
        self,
        field: impl Into<String>,
        query: impl Into<String>,
        limit: usize,
    ) -> TextQuery {
        TextQuery {
            inner: self.inner,
            field: field.into(),
            query: query.into(),
            limit,
            pre_filter: self.filter,
        }
    }

    /// Switch to hybrid search (Reciprocal Rank Fusion of BM25 + vector).
    ///
    /// `query_vector` can be `None` to auto-embed from `query_text`.
    /// If `vector_field` matches an autoembed source, it's resolved to
    /// the actual vector field automatically.
    pub fn hybrid_search(
        self,
        text_field: impl Into<String>,
        vector_field: impl Into<String>,
        query_text: impl Into<String>,
        query_vector: Option<Vec<f32>>,
        limit: usize,
    ) -> HybridQuery {
        HybridQuery {
            inner: self.inner,
            text_field: text_field.into(),
            vector_field: vector_field.into(),
            query_text: query_text.into(),
            query_vector,
            limit,
            pre_filter: self.filter,
        }
    }

    // -----------------------------------------------------------
    // Terminal methods
    // -----------------------------------------------------------

    pub fn to_list(self) -> Result<Vec<Document>, MooFileError> {
        let inner = self.inner.read().expect("lock poisoned");
        inner.require_open()?;

        let mut docs: Vec<Document> = if self.filter.is_empty() {
            inner.index_manager.all_docs()
        } else {
            inner.index_manager.get_matching(&self.filter)
                .iter().map(|d| d.as_ref().clone()).collect()
        };

        if let Some(ref field) = self.group_field {
            docs = apply_group_agg(&docs, field, &self.agg_funcs);
        }

        if let Some(ref key) = self.sort_key {
            docs.sort_by(|a, b| {
                let va = a.get(key);
                let vb = b.get(key);
                let ord = bson_cmp(va.unwrap_or(&Bson::Null), vb.unwrap_or(&Bson::Null))
                    .unwrap_or(std::cmp::Ordering::Equal);
                if self.sort_desc { ord.reverse() } else { ord }
            });
        }

        if self.skip_n > 0 {
            docs = docs.into_iter().skip(self.skip_n).collect();
        }

        if let Some(n) = self.limit_n {
            docs.truncate(n);
        }

        Ok(docs)
    }

    pub fn first(self) -> Result<Option<Document>, MooFileError> {
        let mut q = self;
        q.limit_n = Some(1);
        Ok(q.to_list()?.into_iter().next())
    }

    pub fn count(self) -> Result<usize, MooFileError> {
        if self.group_field.is_none() && self.sort_key.is_none() && self.skip_n == 0 {
            let inner = self.inner.read().expect("lock poisoned");
            return Ok(inner.index_manager.count_matching(&self.filter));
        }
        Ok(self.to_list()?.len())
    }
}

// ---------------------------------------------------------------------------
// EmbeddingOrVector: either a raw vector or an auto-embedded query
// ---------------------------------------------------------------------------

/// A query vector that may be a pre-computed f32 vector or a text to auto-embed.
#[derive(Debug, Clone)]
pub(crate) enum EmbeddingOrVector {
    /// Raw f32 vector — use as-is
    Vector(Vec<f32>),
}

impl From<Vec<f32>> for EmbeddingOrVector {
    fn from(v: Vec<f32>) -> Self {
        EmbeddingOrVector::Vector(v)
    }
}

// ---------------------------------------------------------------------------
// VectorQuery
// ---------------------------------------------------------------------------

/// Results of a vector similarity search.  Returns `(doc, score)` tuples.
#[derive(Debug, Clone)]
pub struct VectorQuery {
    pub(crate) inner: Arc<RwLock<CollectionInner>>,
    pub(crate) field: String,
    pub(crate) query_vector: EmbeddingOrVector,
    pub(crate) limit: usize,
    pub(crate) pre_filter: Document,
}

impl VectorQuery {
    fn resolve_vector(&self) -> Result<Vec<f32>, MooFileError> {
        match &self.query_vector {
            EmbeddingOrVector::Vector(v) => Ok(v.clone()),
        }
    }

    /// Return `(doc, score)` pairs sorted by similarity descending.
    pub fn to_list(self) -> Result<Vec<(Document, f32)>, MooFileError> {
        let query_vector = self.resolve_vector()?;

        {
            let mut inner = self.inner.write().expect("lock poisoned");
            inner.require_open()?;
            inner.index_manager.ensure_vectors_fresh();
        }

        let inner = self.inner.read().expect("lock poisoned");
        inner.require_open()?;

        if self.pre_filter.is_empty() {
            Ok(inner.index_manager.vector_search(
                &self.field,
                &query_vector,
                self.limit,
            ))
        } else {
            let matching_docs = inner.index_manager.get_matching(&self.pre_filter);
            let allowed_ids: HashSet<String> = matching_docs
                .iter()
                .filter_map(|d| d.get_str("_id").ok().map(String::from))
                .collect();

            Ok(inner.index_manager.vector_search_filtered(
                &self.field,
                &query_vector,
                self.limit,
                &allowed_ids,
            ))
        }
    }

    pub fn first(self) -> Result<Option<(Document, f32)>, MooFileError> {
        Ok(self.to_list()?.into_iter().next())
    }
}

// ---------------------------------------------------------------------------
// TextQuery
// ---------------------------------------------------------------------------

/// Results of a BM25 text search.  Returns `(doc, score)` tuples.
#[derive(Debug, Clone)]
pub struct TextQuery {
    inner: Arc<RwLock<CollectionInner>>,
    field: String,
    query: String,
    limit: usize,
    pre_filter: Document,
}

impl TextQuery {
    pub fn to_list(self) -> Result<Vec<(Document, f32)>, MooFileError> {
        let inner = self.inner.read().expect("lock poisoned");
        inner.require_open()?;

        if self.pre_filter.is_empty() {
            return Ok(inner.index_manager.text_search(&self.field, &self.query, self.limit));
        }

        let matching_docs = inner.index_manager.get_matching(&self.pre_filter);
        let allowed_ids: HashSet<String> = matching_docs
            .iter()
            .filter_map(|d| d.get_str("_id").ok().map(String::from))
            .collect();

        let all_results = inner.index_manager.text_search(&self.field, &self.query, usize::MAX);

        Ok(all_results
            .into_iter()
            .filter(|(doc, _)| {
                doc.get_str("_id")
                    .map(|id| allowed_ids.contains(id))
                    .unwrap_or(false)
            })
            .take(self.limit)
            .collect())
    }

    pub fn first(self) -> Result<Option<(Document, f32)>, MooFileError> {
        Ok(self.to_list()?.into_iter().next())
    }
}

// ---------------------------------------------------------------------------
// HybridQuery (Reciprocal Rank Fusion)
// ---------------------------------------------------------------------------

const RRF_K: f32 = 60.0;

/// Hybrid search results using Reciprocal Rank Fusion (RRF).
#[derive(Debug, Clone)]
pub struct HybridQuery {
    inner: Arc<RwLock<CollectionInner>>,
    text_field: String,
    vector_field: String,
    query_text: String,
    query_vector: Option<Vec<f32>>,
    limit: usize,
    pre_filter: Document,
}

impl HybridQuery {
    /// Resolve the vector field name (handling autoembed source fields)
    /// and produce the query vector (auto-embedding if needed).
    fn resolve(&self) -> Result<(String, Vec<f32>), MooFileError> {
        let inner = self.inner.read().expect("lock poisoned");

        // Step 1: Resolve the vector field name.
        // If `vector_field` is a known autoembed source, use its target.
        let (actual_field, actual_vector) = if let Some(config) = inner.auto_embeds.get(&self.vector_field) {
            // Source field name given (e.g., "content") → resolve to target (e.g., "embedding")
            let target = config.target_field.clone();
            (target, Some(config.clone()))
        } else {
            // Might be a raw vector field name (e.g., "embedding").
            // Check if ANY autoembed source maps to it.
            let config = inner.auto_embeds.values()
                .find(|cfg| cfg.target_field == self.vector_field)
                .cloned();
            (self.vector_field.clone(), config)
        };

        // Step 2: Produce the query vector
        let query_vector = match &self.query_vector {
            Some(v) => v.clone(),
            None => {
                // Auto-embed from query_text
                let config = actual_vector
                    .ok_or_else(|| MooFileError::NoAutoEmbed(
                        format!("no autoembed config for '{}' and no raw vector provided", self.vector_field)
                    ))?;

                // Find the engine
                let model_uri = ModelUri::parse(&config.model);
                let cache_dir = crate::default_model_cache_dir();
                let local_path = model_uri.resolve(&cache_dir)?;
                let model_key = local_path.to_string_lossy().into_owned();

                let engine = inner.embedding_engines.get(&model_key)
                    .ok_or_else(|| MooFileError::EmbeddingError(
                        format!("embedding engine not loaded for model '{}'", config.model)
                    ))?
                    .clone(); // clone the Arc before dropping the lock

                // Drop the lock before embedding (could be slow)
                drop(inner);

                let prefixed = format!("{}{}", config.query_prefix, self.query_text);
                let raw_emb = engine.embed(&prefixed)?;

                // Truncate/normalize
                let emb: Vec<f32> = if raw_emb.len() > config.dims {
                    raw_emb[..config.dims].to_vec()
                } else {
                    raw_emb
                };

                let emb = if config.normalize {
                    let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
                    if norm > 0.0 { emb.iter().map(|x| x / norm).collect() } else { emb }
                } else {
                    emb
                };

                emb
            }
        };

        Ok((actual_field, query_vector))
    }

    /// Return `(doc, rrf_score)` pairs sorted by fused rank descending.
    pub fn to_list(self) -> Result<Vec<(Document, f32)>, MooFileError> {
        let pool = (self.limit * 5).max(50);

        // Resolve vector field + query vector
        let (vec_field, query_vector) = self.resolve()?;

        // Get text search results
        let text_results = TextQuery {
            inner: Arc::clone(&self.inner),
            field: self.text_field.clone(),
            query: self.query_text.clone(),
            limit: pool,
            pre_filter: self.pre_filter.clone(),
        }.to_list()?;

        // Ensure vector indexes are fresh
        {
            let mut inner = self.inner.write().expect("lock poisoned");
            inner.require_open()?;
            inner.index_manager.ensure_vectors_fresh();
        }

        let vec_results = VectorQuery {
            inner: Arc::clone(&self.inner),
            field: vec_field,
            query_vector: EmbeddingOrVector::Vector(query_vector),
            limit: pool,
            pre_filter: self.pre_filter.clone(),
        }.to_list()?;

        // RRF fusion: score(d) = Σ 1/(k + rank + 1)
        let mut scores: std::collections::HashMap<String, f32> = std::collections::HashMap::new();
        let mut docs: std::collections::HashMap<String, Document> = std::collections::HashMap::new();

        for (rank, (doc, _)) in text_results.iter().enumerate() {
            let id = doc.get_str("_id").unwrap_or("").to_string();
            if id.is_empty() { continue; }
            *scores.entry(id.clone()).or_insert(0.0) += 1.0 / (RRF_K + rank as f32 + 1.0);
            docs.insert(id, doc.clone());
        }

        for (rank, (doc, _)) in vec_results.iter().enumerate() {
            let id = doc.get_str("_id").unwrap_or("").to_string();
            if id.is_empty() { continue; }
            *scores.entry(id.clone()).or_insert(0.0) += 1.0 / (RRF_K + rank as f32 + 1.0);
            docs.entry(id.clone()).or_insert_with(|| doc.clone());
        }

        let mut ranked: Vec<(String, f32)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
        ranked.truncate(self.limit);

        Ok(ranked
            .into_iter()
            .filter_map(|(id, score)| docs.get(&id).map(|doc| (doc.clone(), score)))
            .collect())
    }

    pub fn first(self) -> Result<Option<(Document, f32)>, MooFileError> {
        Ok(self.to_list()?.into_iter().next())
    }
}

// ---------------------------------------------------------------------------
// Aggregation functions
// ---------------------------------------------------------------------------

/// Aggregation function descriptor.
#[derive(Debug, Clone)]
pub enum AggFunc {
    Count,
    Sum(String),
    Mean(String),
    Min(String),
    Max(String),
    Collect(String),
    First(String),
    Last(String),
}

impl AggFunc {
    pub fn output_name(&self) -> String {
        match self {
            AggFunc::Count => "count".into(),
            AggFunc::Sum(f) => format!("sum_{f}"),
            AggFunc::Mean(f) => format!("mean_{f}"),
            AggFunc::Min(f) => format!("min_{f}"),
            AggFunc::Max(f) => format!("max_{f}"),
            AggFunc::Collect(f) => format!("collect_{f}"),
            AggFunc::First(f) => format!("first_{f}"),
            AggFunc::Last(f) => format!("last_{f}"),
        }
    }

    fn compute(&self, docs: &[Document]) -> Bson {
        match self {
            AggFunc::Count => Bson::Int32(docs.len() as i32),
            AggFunc::Sum(field) => {
                let total: f64 = docs.iter()
                    .filter_map(|d| d.get(field))
                    .filter_map(|v| bson_number(v))
                    .sum();
                Bson::Double(total)
            }
            AggFunc::Mean(field) => {
                let vals: Vec<f64> = docs.iter()
                    .filter_map(|d| d.get(field))
                    .filter_map(|v| bson_number(v))
                    .collect();
                if vals.is_empty() { Bson::Null } else { Bson::Double(vals.iter().sum::<f64>() / vals.len() as f64) }
            }
            AggFunc::Min(field) => docs.iter()
                .filter_map(|d| d.get(field).cloned())
                .min_by(|a, b| bson_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or(Bson::Null),
            AggFunc::Max(field) => docs.iter()
                .filter_map(|d| d.get(field).cloned())
                .max_by(|a, b| bson_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or(Bson::Null),
            AggFunc::Collect(field) => Bson::Array(
                docs.iter().filter_map(|d| d.get(field).cloned()).collect(),
            ),
            AggFunc::First(field) => docs.first()
                .and_then(|d| d.get(field).cloned())
                .unwrap_or(Bson::Null),
            AggFunc::Last(field) => docs.last()
                .and_then(|d| d.get(field).cloned())
                .unwrap_or(Bson::Null),
        }
    }
}

fn bson_number(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        _ => None,
    }
}

fn apply_group_agg(
    docs: &[Document],
    group_field: &str,
    agg_funcs: &[AggFunc],
) -> Vec<Document> {
    let mut groups: std::collections::BTreeMap<String, Vec<Document>> = std::collections::BTreeMap::new();

    for doc in docs {
        let key = doc.get(group_field).map(|v| v.to_string()).unwrap_or_default();
        groups.entry(key).or_default().push(doc.clone());
    }

    let mut result = Vec::new();
    for (key, group_docs) in groups {
        let mut row = Document::new();
        row.insert(group_field, key);
        for func in agg_funcs {
            row.insert(func.output_name(), func.compute(&group_docs));
        }
        result.push(row);
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn implicit_eq() {
        let doc = doc! { "name": "Alice", "age": 30 };
        assert!(matches(&doc, &doc! { "name": "Alice" }));
        assert!(!matches(&doc, &doc! { "name": "Bob" }));
    }

    #[test]
    fn comparison_ops() {
        let doc = doc! { "age": 30 };
        assert!(matches(&doc, &doc! { "age": { "$gt": 25 } }));
        assert!(matches(&doc, &doc! { "age": { "$lt": 35 } }));
        assert!(matches(&doc, &doc! { "age": { "$gte": 30 } }));
        assert!(!matches(&doc, &doc! { "age": { "$gt": 30 } }));
    }

    #[test]
    fn logical_and_or() {
        let doc = doc! { "age": 30, "status": "active" };
        assert!(matches(&doc, &doc! { "$and": [ { "age": { "$gt": 20 } }, { "status": "active" } ] }));
        assert!(matches(&doc, &doc! { "$or": [ { "status": "inactive" }, { "age": 30 } ] }));
        assert!(!matches(&doc, &doc! { "$or": [ { "status": "inactive" }, { "age": 99 } ] }));
    }

    #[test]
    fn nin_operator() {
        let doc = doc! { "color": "red" };
        assert!(matches(&doc, &doc! { "color": { "$nin": ["blue", "green"] } }));
        assert!(!matches(&doc, &doc! { "color": { "$nin": ["red", "green"] } }));
    }

    #[test]
    fn exists_operator() {
        let doc = doc! { "name": "Alice" };
        assert!(matches(&doc, &doc! { "name": { "$exists": true } }));
        assert!(!matches(&doc, &doc! { "age": { "$exists": true } }));
        assert!(matches(&doc, &doc! { "age": { "$exists": false } }));
    }
}
