use bson::doc;
use moofile::{AutoEmbedConfig, Collection, EmbeddingPrecision};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("bench.moo");

    let cfg = AutoEmbedConfig {
        target_field: "summary_embedding".into(),
        dims: 2048,
        precision: EmbeddingPrecision::Int8,
        ..Default::default()
    };
    println!("model = {}", cfg.model);

    let t = Instant::now();
    let c = Collection::builder(&path)
        .vector_index("summary_embedding", 2048)
        .auto_embed("summary", cfg)
        .open()?;
    println!("open (model load): {:?}", t.elapsed());

    let texts = [
        "MooFile is a single-file embedded document store.",
        "Rust is a systems programming language.",
        "Cats sleep for most of the day.",
        "BM25 ranks documents by term frequency.",
        "ONNX Runtime executes neural network graphs.",
    ];

    let t = Instant::now();
    for (i, s) in texts.iter().enumerate() {
        c.insert(doc! { "_id": format!("d{i}"), "summary": *s })?;
    }
    println!("insert+embed: {:?} total, {:?}/ea", t.elapsed(), t.elapsed() / texts.len() as u32);

    let stored = c.find(doc! {"_id": "d0"})?.to_list()?;
    let v = stored[0].get_array("summary_embedding")?;
    println!("stored vector dims: {}", v.len());

    // --- insert_many: batched embedding, and identical results ---
    let bulk: Vec<bson::Document> = (0..64)
        .map(|i| doc! { "_id": format!("b{i}"), "summary": format!("bulk document {i} about storage engines") })
        .collect();
    let t = Instant::now();
    let inserted = c.insert_many(bulk.clone())?;
    let batched = t.elapsed();
    println!("insert_many(64): {:?} total, {:?}/ea", batched, batched / 64);

    // Same texts one at a time, for comparison and to check equality.
    let dir2 = tempfile::tempdir()?;
    let c2 = Collection::builder(dir2.path().join("b.moo"))
        .vector_index("summary_embedding", 2048)
        .auto_embed("summary", AutoEmbedConfig {
            target_field: "summary_embedding".into(),
            dims: 2048,
            precision: EmbeddingPrecision::Int8,
            ..Default::default()
        })
        .open()?;
    let t = Instant::now();
    for d in &bulk { c2.insert(d.clone())?; }
    let single = t.elapsed();
    println!("64x insert():   {:?} total, {:?}/ea  ({:.1}x)",
             single, single / 64, single.as_secs_f64() / batched.as_secs_f64());

    let mut mismatches = 0;
    for d in &inserted {
        let id = d.get_str("_id")?;
        let other = c2.find(doc! {"_id": id})?.to_list()?;
        if d.get_array("summary_embedding")? != other[0].get_array("summary_embedding")? {
            mismatches += 1;
        }
    }
    println!("batched vs per-doc embeddings differing: {mismatches}/64");

    let t = Instant::now();
    let hits = c.find(doc! {})?.semantic("summary", "how do embedded databases store data?", 3)?.to_list()?;
    println!("semantic search: {:?}", t.elapsed());
    for (h, score) in &hits {
        println!("  {:.4}  {} — {}", score, h.get_str("_id")?, h.get_str("summary")?);
    }
    Ok(())
}
