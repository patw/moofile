//! Compile-time API guard tests.
//!
//! These tests exercise the exact method signatures that the Python bindings
//! (`moofile-py`) depend on.  If a core API changes in a way that would break
//! the bindings (e.g. a parameter type change, a new required argument, or
//! `Option` wrapping), these tests fail to **compile** — catching the mismatch
//! before it reaches CI.
//!
//! This is the class of bug that caused the 2026-07-13 CI failure:
//! `hybrid_search()` changed `query_vector` from `Vec<f32>` to
//! `Option<Vec<f32>>` but the Python bindings were not updated.

use bson::doc;
use moofile::Collection;
use tempfile::NamedTempFile;

/// Helper: open a temp collection with vector + text indexes, insert seed docs.
fn setup() -> (Collection, NamedTempFile) {
    let tmp = NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_string_lossy().to_string();

    let db = Collection::builder(&path)
        .index("category")
        .vector_index("embedding", 3)
        .text_index("content")
        .open()
        .expect("open");

    // Seed with a small corpus
    let docs = vec![
        doc! { "_id": "ml",    "category": "ai",  "content": "machine learning algorithms",            "embedding": [1.0, 0.0, 0.0] },
        doc! { "_id": "dl",    "category": "ai",  "content": "deep learning neural networks",           "embedding": [0.9, 0.1, 0.0] },
        doc! { "_id": "cv",    "category": "vis", "content": "computer vision image classification",    "embedding": [0.1, 0.9, 0.0] },
        doc! { "_id": "cook",  "category": "food","content": "italian cooking recipes pasta and pizza", "embedding": [0.0, 0.0, 0.1] },
    ];
    for d in docs {
        db.insert(d).expect("insert");
    }

    (db, tmp)
}

// ── Vector search ──────────────────────────────────────────────────────────

#[test]
fn guard_vector_search_signature() {
    let (db, _tmp) = setup();

    // Exact call pattern used by _NativeVectorQuery::to_list()
    let results = db
        .find(doc! {})
        .unwrap()
        .vector_search("embedding", vec![1.0_f32, 0.0, 0.0], 10)
        .to_list()
        .unwrap();

    assert_eq!(results.len(), 4);
    // Nearest to [1,0,0] should be "ml"
    assert_eq!(results[0].0.get_str("_id").unwrap(), "ml");
    assert!(results[0].1 > 0.9); // cosine ~1.0
}

#[test]
fn guard_vector_search_with_prefilter() {
    let (db, _tmp) = setup();

    let results = db
        .find(doc! { "category": "ai" })
        .unwrap()
        .vector_search("embedding", vec![1.0_f32, 0.0, 0.0], 10)
        .to_list()
        .unwrap();

    assert_eq!(results.len(), 2);
    let ids: Vec<&str> = results.iter().map(|(d, _)| d.get_str("_id").unwrap()).collect();
    assert!(ids.contains(&"ml"));
    assert!(ids.contains(&"dl"));
}

// ── Text search ────────────────────────────────────────────────────────────

#[test]
fn guard_text_search_signature() {
    let (db, _tmp) = setup();

    // Exact call pattern used by _NativeTextQuery::to_list()
    let results = db
        .find(doc! {})
        .unwrap()
        .text_search("content", "machine learning", 10)
        .to_list()
        .unwrap();

    assert!(!results.is_empty());
    let ids: Vec<&str> = results.iter().map(|(d, _)| d.get_str("_id").unwrap()).collect();
    assert!(ids.contains(&"ml"));
}

#[test]
fn guard_text_search_with_prefilter() {
    let (db, _tmp) = setup();

    let results = db
        .find(doc! { "category": "ai" })
        .unwrap()
        .text_search("content", "learning", 10)
        .to_list()
        .unwrap();

    assert_eq!(results.len(), 2);
    for (doc, _) in &results {
        assert_eq!(doc.get_str("category").unwrap(), "ai");
    }
}

// ── Hybrid search (RRF) ────────────────────────────────────────────────────

#[test]
fn guard_hybrid_search_with_vector_signature() {
    // This is THE test that would have caught the CI failure.
    // It calls hybrid_search() with Some(vec) — the exact pattern
    // the Python bindings use in hybrid_search_raw().
    let (db, _tmp) = setup();

    let results = db
        .find(doc! {})
        .unwrap()
        .hybrid_search(
            "content",
            "embedding",
            "machine learning",
            Some(vec![1.0_f32, 0.0, 0.0]),
            10,
        )
        .to_list()
        .unwrap();

    assert!(!results.is_empty());
    // "ml" matches both text + vector → top rank
    assert_eq!(results[0].0.get_str("_id").unwrap(), "ml");
    // RRF scores are always positive
    assert!(results[0].1 > 0.0);
}

#[test]
fn guard_hybrid_search_none_vector_signature() {
    // Also verify the None path compiles (auto-embedding fallback).
    // Without autoembed config this fails at runtime with NoAutoEmbed,
    // which is expected — we only guard compilation of the None variant here.
    let (db, _tmp) = setup();

    let _result = db
        .find(doc! {})
        .unwrap()
        .hybrid_search(
            "content",
            "embedding",
            "machine learning",
            None::<Vec<f32>>,
            10,
        )
        .to_list();

    // Runtime behavior: NoAutoEmbed is expected. Compilation is all we need.
}

#[test]
fn guard_hybrid_search_with_prefilter() {
    let (db, _tmp) = setup();

    let results = db
        .find(doc! { "category": "ai" })
        .unwrap()
        .hybrid_search(
            "content",
            "embedding",
            "machine learning",
            Some(vec![1.0_f32, 0.0, 0.0]),
            10,
        )
        .to_list()
        .unwrap();

    // Only AI docs
    for (doc, _) in &results {
        assert_eq!(doc.get_str("category").unwrap(), "ai");
    }
}

// ── Additional binding-used API surface ────────────────────────────────────

#[test]
fn guard_find_to_list_signature() {
    let (db, _tmp) = setup();

    // Used by _NativeQuery::to_list() → native.find_raw()
    let results = db.find(doc! {}).unwrap().to_list().unwrap();
    assert_eq!(results.len(), 4);
}

#[test]
fn guard_find_first_signature() {
    let (db, _tmp) = setup();

    let result = db.find(doc! { "_id": "ml" }).unwrap().first().unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().get_str("_id").unwrap(), "ml");
}

#[test]
fn guard_find_count_signature() {
    let (db, _tmp) = setup();

    let n = db.find(doc! { "category": "ai" }).unwrap().count().unwrap();
    assert_eq!(n, 2);
}

#[test]
fn guard_vector_search_first_signature() {
    let (db, _tmp) = setup();

    let result = db
        .find(doc! {})
        .unwrap()
        .vector_search("embedding", vec![1.0_f32, 0.0, 0.0], 10)
        .first()
        .unwrap();

    assert!(result.is_some());
    assert_eq!(result.unwrap().0.get_str("_id").unwrap(), "ml");
}

#[test]
fn guard_text_search_first_signature() {
    let (db, _tmp) = setup();

    let result = db
        .find(doc! {})
        .unwrap()
        .text_search("content", "machine", 10)
        .first()
        .unwrap();

    assert!(result.is_some());
}

#[test]
fn guard_hybrid_search_first_signature() {
    let (db, _tmp) = setup();

    let result = db
        .find(doc! {})
        .unwrap()
        .hybrid_search(
            "content",
            "embedding",
            "machine learning",
            Some(vec![1.0_f32, 0.0, 0.0]),
            10,
        )
        .first()
        .unwrap();

    assert!(result.is_some());
}
