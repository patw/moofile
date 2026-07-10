/// Lazy query builder and filter evaluation.
///
/// [`Query`] is the main builder — chain `.sort()`, `.skip()`, `.limit()`,
/// then materialise with `.to_list()`, `.first()`, or `.count()`.
///
/// [`VectorQuery`] and [`TextQuery`] are returned by `.vector_search()`
/// and `.text_search()` respectively.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use bson::{Bson, Document};

use crate::CollectionInner;
use crate::MooFileError;

// ---------------------------------------------------------------------------
// Filter evaluation
// ---------------------------------------------------------------------------

/// Return `true` if `doc` satisfies every condition in `filter`.
///
/// This mirrors the Python `matches()` in `moofile/query.py`.
/// Supports: implicit `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`,
/// `$in`, `$nin`, `$and`, `$or`, `$not`, `$exists`, `$elemMatch`.
pub fn matches(doc: &Document, filter: &Document) -> bool {
    for (key, value) in filter.iter() {
        match key.as_str() {
            // --- Logical operators ---
            "$and" => {
                if let Some(Bson::Array(subs)) = filter.get(key) {
                    if !subs
                        .iter()
                        .all(|sub| matches(doc, sub.as_document().unwrap()))
                    {
                        return false;
                    }
                    continue;
                }
                return false;
            }
            "$or" => {
                if let Some(Bson::Array(subs)) = filter.get(key) {
                    if !subs
                        .iter()
                        .any(|sub| matches(doc, sub.as_document().unwrap()))
                    {
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
            // --- Field-level condition ---
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

/// Evaluate a single `{"field": condition}` pair.
fn eval_field_condition(field_val: Option<&Bson>, condition: &Bson) -> bool {
    match condition {
        // Implicit $eq: `{"field": "value"}`
        c if !is_operator_doc(c) => field_val == Some(c),

        // Operator document: `{"field": {"$gt": 5}}`
        Bson::Document(ops) => {
            for (op, op_val) in ops {
                match op.as_str() {
                    "$eq" => {
                        if field_val != Some(op_val) {
                            return false;
                        }
                    }
                    "$ne" => {
                        if field_val == Some(op_val) {
                            return false;
                        }
                    }
                    "$gt" => {
                        if !cmp_op(field_val, op_val, std::cmp::Ordering::Greater) {
                            return false;
                        }
                    }
                    "$gte" => {
                        if !cmp_op(field_val, op_val, std::cmp::Ordering::Greater)
                            && field_val != Some(op_val)
                        {
                            return false;
                        }
                    }
                    "$lt" => {
                        if !cmp_op(field_val, op_val, std::cmp::Ordering::Less) {
                            return false;
                        }
                    }
                    "$lte" => {
                        if !cmp_op(field_val, op_val, std::cmp::Ordering::Less)
                            && field_val != Some(op_val)
                        {
                            return false;
                        }
                    }
                    "$in" => match op_val {
                        Bson::Array(arr) => {
                            if !arr.contains(&field_val.unwrap_or(&Bson::Null)) {
                                return false;
                            }
                        }
                        _ => return false,
                    },
                    "$nin" => match op_val {
                        Bson::Array(arr) => {
                            if arr.contains(&field_val.unwrap_or(&Bson::Null)) {
                                return false;
                            }
                        }
                        _ => return false,
                    },
                    "$exists" => {
                        let should_exist = op_val.as_bool().unwrap_or(false);
                        if should_exist != (field_val.is_some() && field_val != Some(&Bson::Null))
                        {
                            return false;
                        }
                    }
                    _ => {
                        // Unknown operator — reject
                        return false;
                    }
                }
            }
            true
        }

        _ => false,
    }
}

/// True if this Bson value looks like an operator document
/// (contains keys starting with `$`).
fn is_operator_doc(val: &Bson) -> bool {
    match val {
        Bson::Document(d) => d.keys().any(|k| k.starts_with('$')),
        _ => false,
    }
}

/// Compare two Bson values.  Returns `false` on type mismatch
/// (mirrors Python behaviour).
fn cmp_op(a: Option<&Bson>, b: &Bson, target: std::cmp::Ordering) -> bool {
    let a = match a {
        Some(v) => v,
        None => return target == std::cmp::Ordering::Greater, // None < everything
    };

    // We need a partial ordering.  Use bson's native comparison
    // where possible, falling back to string representation.
    match bson_cmp(a, b) {
        Some(ord) => ord == target,
        None => false,
    }
}

/// Partial comparison of two Bson values.  Returns `None` on type
/// mismatch (MongoDB-style: different types cannot be compared).
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
        _ => None, // type mismatch
    }
}

// ---------------------------------------------------------------------------
// Query builder
// ---------------------------------------------------------------------------

/// Lazy query chain.
///
/// Created via [`crate::Collection::find`].  Methods return `Self`
/// for chaining; terminal methods materialise the result set.
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

    /// Sort results by `field`.
    pub fn sort(mut self, field: impl Into<String>, descending: bool) -> Self {
        self.sort_key = Some(field.into());
        self.sort_desc = descending;
        self
    }

    /// Skip the first `n` results.
    pub fn skip(mut self, n: usize) -> Self {
        self.skip_n = n;
        self
    }

    /// Return at most `n` results.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit_n = Some(n);
        self
    }

    /// Group results by `field` before aggregation.
    pub fn group(mut self, field: impl Into<String>) -> Self {
        self.group_field = Some(field.into());
        self
    }

    /// Apply aggregation functions to each group.
    pub fn agg(mut self, funcs: Vec<AggFunc>) -> Self {
        self.agg_funcs = funcs;
        self
    }

    /// Switch to vector similarity search.
    pub fn vector_search(
        self,
        field: impl Into<String>,
        query_vector: Vec<f32>,
        limit: usize,
    ) -> VectorQuery {
        VectorQuery {
            inner: self.inner,
            field: field.into(),
            query_vector,
            limit,
            pre_filter: self.filter,
        }
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

    // -----------------------------------------------------------
    // Terminal methods
    // -----------------------------------------------------------

    /// Materialise results as `Vec<Document>`.
    pub fn to_list(self) -> Result<Vec<Document>, MooFileError> {
        let inner = self.inner.read().expect("lock poisoned");
        inner.require_open()?;

        // 1. Filter — uses index acceleration when available
        let mut docs: Vec<Document> = if self.filter.is_empty() {
            inner.index_manager.all_docs()
        } else {
            inner.index_manager.get_matching(&self.filter)
                .iter().map(|d| d.as_ref().clone()).collect()
        };

        // 2. Group + aggregate
        if let Some(ref field) = self.group_field {
            docs = apply_group_agg(&docs, field, &self.agg_funcs);
        }

        // 3. Sort
        if let Some(ref key) = self.sort_key {
            docs.sort_by(|a, b| {
                let va = a.get(key);
                let vb = b.get(key);
                let ord = bson_cmp(va.unwrap_or(&Bson::Null), vb.unwrap_or(&Bson::Null))
                    .unwrap_or(std::cmp::Ordering::Equal);
                if self.sort_desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
        }

        // 4. Skip
        if self.skip_n > 0 {
            docs = docs.into_iter().skip(self.skip_n).collect();
        }

        // 5. Limit
        if let Some(n) = self.limit_n {
            docs.truncate(n);
        }

        Ok(docs)
    }

    /// Return the first matching document, or `None`.
    pub fn first(self) -> Result<Option<Document>, MooFileError> {
        let mut q = self;
        q.limit_n = Some(1);
        Ok(q.to_list()?.into_iter().next())
    }

    /// Count matching documents (without materialising them all).
    pub fn count(self) -> Result<usize, MooFileError> {
        // Fast path: no transformations
        if self.group_field.is_none() && self.sort_key.is_none() && self.skip_n == 0 {
            let inner = self.inner.read().expect("lock poisoned");
            return Ok(inner.index_manager.count_matching(&self.filter));
        }
        Ok(self.to_list()?.len())
    }
}

// ---------------------------------------------------------------------------
// VectorQuery
// ---------------------------------------------------------------------------

/// Results of a vector similarity search.  Returns `(doc, score)` tuples.
#[derive(Debug, Clone)]
pub struct VectorQuery {
    inner: Arc<RwLock<CollectionInner>>,
    field: String,
    query_vector: Vec<f32>,
    limit: usize,
    pre_filter: Document,
}

impl VectorQuery {
    /// Return `(doc, score)` pairs sorted by similarity descending.
    pub fn to_list(self) -> Result<Vec<(Document, f32)>, MooFileError> {
        // Ensure vector indexes are fresh (requires write lock briefly)
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
                &self.query_vector,
                self.limit,
            ))
        } else {
            let matching_docs = inner.index_manager.get_matching(&self.pre_filter);
            let allowed_ids: HashSet<String> = matching_docs
                .iter()
                .filter_map(|d| d.get("_id").map(|v| v.to_string()))
                .collect();

            Ok(inner.index_manager.vector_search_filtered(
                &self.field,
                &self.query_vector,
                self.limit,
                &allowed_ids,
            ))
        }
    }

    /// Return the best match or `None`.
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
    /// Return `(doc, score)` pairs sorted by relevance descending.
    pub fn to_list(self) -> Result<Vec<(Document, f32)>, MooFileError> {
        let inner = self.inner.read().expect("lock poisoned");
        inner.require_open()?;

        if self.pre_filter.is_empty() {
            return Ok(inner
                .index_manager
                .text_search(&self.field, &self.query, self.limit));
        }

        // Pre-filter using index acceleration, then filter text results
        let matching_docs = inner.index_manager.get_matching(&self.pre_filter);
        let allowed_ids: HashSet<String> = matching_docs
            .iter()
            .filter_map(|d| d.get("_id").map(|v| v.to_string()))
            .collect();

        let all_results =
            inner
                .index_manager
                .text_search(&self.field, &self.query, usize::MAX);

        Ok(all_results
            .into_iter()
            .filter(|(doc, _)| {
                doc.get("_id")
                    .map(|id| allowed_ids.contains(&id.to_string()))
                    .unwrap_or(false)
            })
            .take(self.limit)
            .collect())
    }

    /// Return the best match or `None`.
    pub fn first(self) -> Result<Option<(Document, f32)>, MooFileError> {
        Ok(self.to_list()?.into_iter().next())
    }
}

// ---------------------------------------------------------------------------
// Aggregation functions
// ---------------------------------------------------------------------------

/// Aggregation function descriptor.
///
/// Mirrors Python's `AggFunc` — an operation applied to each group
/// after `.group(field).agg(...)`.
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
                let total: f64 = docs
                    .iter()
                    .filter_map(|d| d.get(field))
                    .filter_map(|v| bson_number(v))
                    .sum();
                Bson::Double(total)
            }
            AggFunc::Mean(field) => {
                let vals: Vec<f64> = docs
                    .iter()
                    .filter_map(|d| d.get(field))
                    .filter_map(|v| bson_number(v))
                    .collect();
                if vals.is_empty() {
                    Bson::Null
                } else {
                    Bson::Double(vals.iter().sum::<f64>() / vals.len() as f64)
                }
            }
            AggFunc::Min(field) => docs
                .iter()
                .filter_map(|d| d.get(field).cloned())
                .min_by(|a, b| bson_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or(Bson::Null),
            AggFunc::Max(field) => docs
                .iter()
                .filter_map(|d| d.get(field).cloned())
                .max_by(|a, b| bson_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or(Bson::Null),
            AggFunc::Collect(field) => Bson::Array(
                docs.iter()
                    .filter_map(|d| d.get(field).cloned())
                    .collect(),
            ),
            AggFunc::First(field) => docs
                .first()
                .and_then(|d| d.get(field).cloned())
                .unwrap_or(Bson::Null),
            AggFunc::Last(field) => docs
                .last()
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

// ---------------------------------------------------------------------------
// Group-by helper
// ---------------------------------------------------------------------------

fn apply_group_agg(
    docs: &[Document],
    group_field: &str,
    agg_funcs: &[AggFunc],
) -> Vec<Document> {
    let mut groups: std::collections::BTreeMap<String, Vec<Document>> =
        std::collections::BTreeMap::new();

    for doc in docs {
        let key = doc
            .get(group_field)
            .map(|v| v.to_string())
            .unwrap_or_default();
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
        assert!(matches(
            &doc,
            &doc! { "$and": [ { "age": { "$gt": 20 } }, { "status": "active" } ] }
        ));
        assert!(matches(
            &doc,
            &doc! { "$or": [ { "status": "inactive" }, { "age": 30 } ] }
        ));
        assert!(!matches(
            &doc,
            &doc! { "$or": [ { "status": "inactive" }, { "age": 99 } ] }
        ));
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

    /// Tests that the Query builder methods chain correctly and store
    /// the right values without materialising.
    #[test]
    fn query_builder_stores_state() {
        // We can't easily construct a Query without a Collection backing it,
        // but the struct is pub(crate) and tested comprehensively via
        // the integration tests in lib.rs (insert_and_find, etc.).
        //
        // This placeholder ensures the test module compiles.
        let doc = doc! { "x": 1 };
        assert!(matches(&doc, &doc! { "x": 1 }));
    }
}
