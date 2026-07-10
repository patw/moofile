/// In-memory index management.
///
/// Documents are stored as `Arc<Document>` — cheap reference-counted
/// clones instead of deep-copying 128-dim vectors on every query.
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use bson::Document;

use crate::text::TextIndex;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Value {
    Null, Bool(bool), I32(i32), I64(i64), Double(OrderedFloat), String(String),
}

#[derive(Debug, Clone, Copy)]
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
    Exact(Vec<String>),
    Candidates(Vec<String>),
}

impl IndexResult {
    pub fn ids(&self) -> &[String] {
        match self { IndexResult::Exact(ids) | IndexResult::Candidates(ids) => ids.as_slice() }
    }
    pub fn is_exact(&self) -> bool { matches!(self, IndexResult::Exact(_)) }
}

pub(crate) struct IndexManager {
    regular: BTreeMap<String, BTreeMap<Value, Vec<String>>>,
    regular_fields: Vec<String>,
    vector_fields: Vec<(String, usize)>,
    vector_data: BTreeMap<String, (Vec<String>, Vec<f32>, usize)>,
    text_indexes: BTreeMap<String, TextIndex>,
    text_fields: Vec<String>,
    documents: BTreeMap<String, Arc<Document>>,
    vectors_stale: bool,
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field("ndocs", &self.documents.len())
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
            regular, regular_fields: regular_fields.to_vec(),
            vector_fields: vector_fields.to_vec(), vector_data: BTreeMap::new(),
            text_indexes, text_fields: text_fields.to_vec(),
            documents: BTreeMap::new(), vectors_stale: true,
        }
    }

    pub fn add(&mut self, doc: Document) {
        let _id = doc.get_str("_id").unwrap_or("").to_string();
        if _id.is_empty() { return; }
        for field in &self.regular_fields {
            if let Some(val) = doc.get(field) {
                if let Some(key) = bson_to_value(val) {
                    self.regular.entry(field.clone()).or_default().entry(key).or_default().push(_id.clone());
                }
            }
        }
        for field in &self.text_fields {
            if let Some(bson::Bson::String(text)) = doc.get(field) {
                if let Some(ti) = self.text_indexes.get_mut(field) {
                    ti.add_document(_id.clone(), text);
                }
            }
        }
        self.documents.insert(_id, Arc::new(doc));
        self.vectors_stale = true;
    }

    pub fn remove(&mut self, _id: &str) -> Option<Document> {
        let doc = Arc::unwrap_or_clone(self.documents.remove(_id)?);
        for field in &self.regular_fields {
            if let Some(val) = doc.get(field) {
                if let Some(key) = bson_to_value(val) {
                    if let Some(map) = self.regular.get_mut(field) {
                        if let Some(ids) = map.get_mut(&key) {
                            ids.retain(|id| id != _id);
                            if ids.is_empty() { map.remove(&key); }
                        }
                    }
                }
            }
        }
        for field in &self.text_fields {
            if let Some(ti) = self.text_indexes.get_mut(field) { ti.remove_document(_id); }
        }
        self.vectors_stale = true;
        Some(doc)
    }

    pub fn get(&self, _id: &str) -> Option<Arc<Document>> {
        self.documents.get(_id).cloned()
    }

    pub fn all_docs(&self) -> Vec<Document> {
        self.documents.values().map(|d| d.as_ref().clone()).collect()
    }

    pub fn doc_count(&self) -> usize { self.documents.len() }

    pub fn count_matching(&self, filter: &Document) -> usize {
        if filter.is_empty() { return self.documents.len(); }
        match self.try_index(filter) {
            Some(IndexResult::Exact(ids)) => ids.len(),
            Some(IndexResult::Candidates(ids)) => ids.iter()
                .filter_map(|id| self.documents.get(id))
                .filter(|d| crate::query::matches(d.as_ref(), filter))
                .count(),
            None => self.documents.values()
                .filter(|d| crate::query::matches(d.as_ref(), filter)).count(),
        }
    }

    pub fn get_matching(&self, filter: &Document) -> Vec<Arc<Document>> {
        if filter.is_empty() { return self.documents.values().cloned().collect(); }
        match self.try_index(filter) {
            Some(IndexResult::Exact(ids)) => ids.iter()
                .filter_map(|id| self.documents.get(id).cloned()).collect(),
            Some(IndexResult::Candidates(ids)) => ids.iter()
                .filter_map(|id| self.documents.get(id))
                .filter(|d| crate::query::matches(d.as_ref(), filter))
                .cloned().collect(),
            None => self.documents.values()
                .filter(|d| crate::query::matches(d.as_ref(), filter))
                .cloned().collect(),
        }
    }

    pub fn clear(&mut self) {
        for map in self.regular.values_mut() { map.clear(); }
        for ti in self.text_indexes.values_mut() { ti.clear(); }
        self.vector_data.clear();
        self.documents.clear();
        self.vectors_stale = true;
    }

    // --- Index lookup ---

    pub fn try_index(&self, filter: &Document) -> Option<IndexResult> {
        for key in filter.keys() { if key.starts_with('$') { return None; } }
        if filter.len() == 1 {
            let (field, condition) = filter.iter().next().unwrap();
            if self.regular.contains_key(field) {
                return self.try_index_single_field(field, condition);
            }
        }
        for (field, condition) in filter.iter() {
            if !self.regular.contains_key(field) { continue; }
            if let Some(ids) = self.lookup_ids(field, condition) {
                return Some(IndexResult::Candidates(ids));
            }
        }
        None
    }

    fn try_index_single_field(&self, field: &str, condition: &bson::Bson) -> Option<IndexResult> {
        match condition {
            c if !is_operator_doc(c) => {
                Some(IndexResult::Exact(self.lookup_exact_ids(field, c)?))
            }
            bson::Bson::Document(ops) => {
                if ops.len() == 1 {
                    if let Some(eq_val) = ops.get("$eq") {
                        return Some(IndexResult::Exact(self.lookup_exact_ids(field, eq_val)?));
                    }
                }
                let is_pure_range = ops.keys().all(|k| matches!(k.as_str(), "$gt"|"$gte"|"$lt"|"$lte"));
                if is_pure_range {
                    let ids = self.lookup_range_ids(field,
                        ops.get("$gt").or(ops.get("$gte")), ops.contains_key("$gte"),
                        ops.get("$lt").or(ops.get("$lte")), ops.contains_key("$lte"))?;
                    return Some(IndexResult::Exact(ids));
                }
                if let Some(eq_val) = ops.get("$eq") {
                    return Some(IndexResult::Candidates(self.lookup_exact_ids(field, eq_val)?));
                }
                None
            }
            _ => None,
        }
    }

    fn lookup_ids(&self, field: &str, condition: &bson::Bson) -> Option<Vec<String>> {
        match condition {
            c if !is_operator_doc(c) => self.lookup_exact_ids(field, c),
            bson::Bson::Document(ops) => {
                if let Some(eq_val) = ops.get("$eq") { return self.lookup_exact_ids(field, eq_val); }
                let is_range = ops.keys().all(|k| matches!(k.as_str(), "$gt"|"$gte"|"$lt"|"$lte"));
                if is_range && !ops.is_empty() {
                    self.lookup_range_ids(field,
                        ops.get("$gt").or(ops.get("$gte")), ops.contains_key("$gte"),
                        ops.get("$lt").or(ops.get("$lte")), ops.contains_key("$lte"))
                } else { None }
            }
            _ => None,
        }
    }

    pub fn lookup_exact_ids(&self, field: &str, value: &bson::Bson) -> Option<Vec<String>> {
        let key = bson_to_value(value)?;
        Some(self.regular.get(field)?.get(&key).cloned().unwrap_or_default())
    }

    pub fn lookup_range_ids(&self, field: &str, min_val: Option<&bson::Bson>, min_inclusive: bool,
                            max_val: Option<&bson::Bson>, max_inclusive: bool) -> Option<Vec<String>> {
        let idx = self.regular.get(field)?;
        let min = match min_val.and_then(bson_to_value) {
            Some(k) if min_inclusive => Bound::Included(k), Some(k) => Bound::Excluded(k), None => Bound::Unbounded,
        };
        let max = match max_val.and_then(bson_to_value) {
            Some(k) if max_inclusive => Bound::Included(k), Some(k) => Bound::Excluded(k), None => Bound::Unbounded,
        };
        let mut ids = Vec::new();
        for (_, key_ids) in idx.range((min, max)) { ids.extend(key_ids.iter().cloned()); }
        Some(ids)
    }

    // --- Vector ---

    pub fn rebuild_vector_indexes(&mut self) {
        for (field, dim) in &self.vector_fields {
            let mut ids = Vec::new(); let mut data = Vec::new();
            for (_id, doc) in &self.documents {
                if let Some(bson::Bson::Array(arr)) = doc.get(field) {
                    let vec: Vec<f32> = arr.iter().filter_map(|v| match v {
                        bson::Bson::Double(f) => Some(*f as f32),
                        bson::Bson::Int32(i) => Some(*i as f32),
                        bson::Bson::Int64(i) => Some(*i as f32),
                        _ => None,
                    }).collect();
                    if vec.len() == *dim { ids.push(_id.clone()); data.extend_from_slice(&vec); }
                }
            }
            self.vector_data.insert(field.clone(), (ids, data, *dim));
        }
        self.vectors_stale = false;
    }

    pub fn ensure_vectors_fresh(&mut self) { if self.vectors_stale { self.rebuild_vector_indexes(); } }

    pub fn vector_search(&self, field: &str, query: &[f32], limit: usize) -> Vec<(Document, f32)> {
        let (ids, data, dim) = match self.vector_data.get(field) { Some(vd) => vd, None => return Vec::new() };
        let n = ids.len(); if n == 0 || *dim == 0 { return Vec::new(); }
        let qn = query.iter().map(|x| x*x).sum::<f32>().sqrt();
        if qn == 0.0 { return Vec::new(); }
        let mut scored: Vec<(usize, f32)> = (0..n).map(|i| {
            let row = &data[i*dim..(i+1)*dim];
            let dot: f32 = row.iter().zip(query).map(|(a,b)| a*b).sum();
            let rn = row.iter().map(|x| x*x).sum::<f32>().sqrt();
            (i, if rn > 0.0 { dot/(qn*rn) } else { 0.0 })
        }).collect();
        scored.sort_by(|a,b| b.1.total_cmp(&a.1)); scored.truncate(limit);
        scored.into_iter().filter_map(|(i, score)|
            self.documents.get(&ids[i]).map(|doc| (doc.as_ref().clone(), score))
        ).collect()
    }

    pub fn vector_search_filtered(&self, field: &str, query: &[f32], limit: usize,
                                   allowed_ids: &std::collections::HashSet<String>) -> Vec<(Document, f32)> {
        let mut all = self.vector_search(field, query, usize::MAX);
        all.retain(|(doc, _)| doc.get("_id").map(|id| allowed_ids.contains(&id.to_string())).unwrap_or(false));
        all.truncate(limit); all
    }

    pub fn text_search(&self, field: &str, query: &str, limit: usize) -> Vec<(Document, f32)> {
        let ti = match self.text_indexes.get(field) { Some(ti) => ti, None => return Vec::new() };
        ti.search(query, limit).into_iter().filter_map(|(id, score)|
            self.documents.get(&id).map(|doc| (doc.as_ref().clone(), score))
        ).collect()
    }
}

fn is_operator_doc(val: &bson::Bson) -> bool {
    matches!(val, bson::Bson::Document(d) if d.keys().any(|k| k.starts_with('$')))
}

fn bson_to_value(v: &bson::Bson) -> Option<Value> {
    match v {
        bson::Bson::Null => Some(Value::Null), bson::Bson::Boolean(b) => Some(Value::Bool(*b)),
        bson::Bson::Int32(i) => Some(Value::I32(*i)), bson::Bson::Int64(i) => Some(Value::I64(*i)),
        bson::Bson::Double(f) => Some(Value::Double(OrderedFloat(*f))),
        bson::Bson::String(s) => Some(Value::String(s.clone())), _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*; use bson::doc;
    fn m() -> IndexManager {
        IndexManager::new(&["email".into(),"age".into()], &[("embedding".into(),3)], &["content".into()])
    }
    #[test] fn exact_lookup() {
        let mut im = m(); im.add(doc!{"_id":"a","email":"a@b.com","age":30}); im.add(doc!{"_id":"b","email":"c@d.com","age":25}); im.add(doc!{"_id":"c","email":"a@b.com","age":30});
        assert_eq!(im.lookup_exact_ids("email",&bson::Bson::String("a@b.com".into())), Some(vec!["a".into(),"c".into()]));
        assert_eq!(im.lookup_exact_ids("email",&bson::Bson::String("nope".into())), Some(vec![]));
    }
    #[test] fn range_lookup() {
        let mut im = m(); im.add(doc!{"_id":"a","age":20}); im.add(doc!{"_id":"b","age":30}); im.add(doc!{"_id":"c","age":40});
        assert_eq!(im.lookup_range_ids("age",Some(&bson::Bson::Int32(25)),true,Some(&bson::Bson::Int32(35)),true).unwrap(), vec!["b"]);
    }
    #[test] fn try_exact_eq() { let mut im=m(); im.add(doc!{"_id":"x","email":"x@y.com"}); let r=im.try_index(&doc!{"email":"x@y.com"}).unwrap(); assert!(r.is_exact()); assert_eq!(r.ids(),&["x"]); }
    #[test] fn try_exact_range() { let mut im=m(); for i in 0..10{im.add(doc!{"_id":i.to_string(),"age":i*10});} let r=im.try_index(&doc!{"age":{"$gt":25,"$lt":55}}).unwrap(); assert!(r.is_exact()); let mut v:Vec<_>=r.ids().to_vec();v.sort();assert_eq!(v,vec!["3","4","5"]);}
    #[test] fn try_candidates_multi() { let mut im=m(); im.add(doc!{"_id":"a","email":"x@y.com","status":"active"}); let r=im.try_index(&doc!{"email":"x@y.com","status":"active"}).unwrap(); assert!(!r.is_exact()); }
    #[test] fn try_fallback() { let mut im=m(); im.add(doc!{"_id":"a","email":"x@y.com"}); assert!(im.try_index(&doc!{"$or":[{"email":"x@y.com"},{"age":99}]}).is_none()); }
    #[test] fn matching_exact() { let mut im=m(); for i in 0..10{im.add(doc!{"_id":i.to_string(),"age":i*10});} assert_eq!(im.get_matching(&doc!{"age":{"$gte":30,"$lte":50}}).len(),3); }
    #[test] fn vec_basic() { let mut im=m(); im.add(doc!{"_id":"near","embedding":[1.0,0.0,0.0]}); im.add(doc!{"_id":"far","embedding":[0.0,0.0,1.0]}); im.add(doc!{"_id":"mid","embedding":[0.7,0.0,0.7]}); im.rebuild_vector_indexes(); let r=im.vector_search("embedding",&[1.0,0.0,0.0],10); assert_eq!(r.len(),3); assert_eq!(r[0].0.get_str("_id").unwrap(),"near"); assert!(r[0].1>=r[1].1); }
    #[test] fn vec_limit() { let mut im=m(); for i in 0..5{im.add(doc!{"_id":i.to_string(),"embedding":[i as f64,0.0,0.0]});} im.rebuild_vector_indexes(); assert_eq!(im.vector_search("embedding",&[0.0,1.0,0.0],2).len(),2); }
    #[test] fn vec_wrong_dim() { let mut im=m(); im.add(doc!{"_id":"good","embedding":[1.0,0.0,0.0]}); im.add(doc!{"_id":"bad","embedding":[1.0,0.0]}); im.rebuild_vector_indexes(); assert_eq!(im.vector_search("embedding",&[1.0,0.0,0.0],10).len(),1); }
}
