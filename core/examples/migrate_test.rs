use bson::doc;
use moofile::{AutoEmbedConfig, Collection, EmbeddingPrecision, MooFileError};

fn cfg(dims: usize) -> AutoEmbedConfig {
    AutoEmbedConfig {
        target_field: "emb".into(),
        dims,
        precision: EmbeddingPrecision::Int8,
        ..Default::default()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("m.moo");

    // --- Simulate the old 1024-dim database: hand-written wide vectors. ---
    {
        let c = Collection::builder(&path).vector_index("emb", 1024).open()?;
        for i in 0..3 {
            c.insert(doc! {
                "_id": format!("d{i}"),
                "summary": format!("document number {i} about embedded databases"),
                "emb": vec![0.1f64; 1024],
            })?;
        }
        c.close()?;
    }
    println!("seeded 3 docs with 1024-dim vectors");

    // --- Reopen with the new 384-dim model configured. ---
    let c = Collection::builder(&path)
        .vector_index("emb", 384)
        .auto_embed("summary", cfg(384))
        .open()?;

    let probe = c
        .find(doc! {})?
        .semantic("summary", "embedded database", 3)
        .and_then(|q| q.to_list());
    match probe {
        Err(MooFileError::VectorIndexDisabled { field, expected, found, count }) => {
            println!("GUARD FIRED: '{field}' expects {expected}, got {found} x{count}");
        }
        Err(e) => println!("unexpected error: {e}"),
        Ok(hits) => println!("NOT GUARDED — returned {} hits", hits.len()),
    }

    // --- Recover. ---
    let n = c.reembed("summary")?;
    println!("reembed rewrote {n} docs");

    let hits = c.find(doc! {})?.semantic("summary", "embedded database", 3)?.to_list()?;
    println!("after reembed: {} hits", hits.len());
    for (h, s) in &hits {
        println!("  {:.4}  {}", s, h.get_str("_id")?);
    }
    let v = c.find(doc! {"_id": "d0"})?.to_list()?[0].get_array("emb")?.len();
    println!("stored dims now: {v}");

    // --- Reopen once more: the guard must stay quiet. ---
    c.close()?;
    let c = Collection::builder(&path)
        .vector_index("emb", 384)
        .auto_embed("summary", cfg(384))
        .open()?;
    let hits = c.find(doc! {})?.semantic("summary", "embedded database", 3)?.to_list()?;
    println!("after reopen: {} hits (guard quiet)", hits.len());
    Ok(())
}
