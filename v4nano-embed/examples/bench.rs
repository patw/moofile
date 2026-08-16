//! Sanity + benchmark for `v4nano-embed`. Mirrors the earlier fastembed test
//! so the numbers are directly comparable.

use std::path::PathBuf;
use std::time::Instant;

use v4nano_embed::{V4Nano, DIM};

const QUERY_PREFIX: &str = "Represent the query for retrieving supporting documents: ";

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/home/beholder/voyage4nano-test/models"));

    let t = Instant::now();
    let mut m = V4Nano::load(dir.join("model_quantized.onnx"), dir.join("tokenizer.json"), 32768, None)?;
    println!("v4nano-embed load (int8, {DIM}d): {:?}", t.elapsed());

    // Correctness: model-card "Red Planet" example.
    let docs = vec![
        "Venus is often called Earth's twin because of its similar size and proximity.".to_string(),
        "Mars, known for its reddish appearance, is often referred to as the Red Planet.".to_string(),
        "Jupiter, the largest planet in our solar system, has a prominent red spot.".to_string(),
        "Saturn, famous for its rings, is sometimes mistaken for the Red Planet.".to_string(),
    ];
    let query = format!("{QUERY_PREFIX}Which planet is known as the Red Planet?");
    let q = &m.embed(&[query])?[0];
    let d = m.embed(&docs)?;
    println!("embedding dims: {}", q.len());

    let mut scored: Vec<(f32, &str)> = docs
        .iter()
        .zip(d.iter())
        .map(|(doc, v)| (cosine(q, v), doc.as_str()))
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    println!("full 2048d ranking:");
    for (s, doc) in &scored {
        println!("  {:.4}  {}", s, doc);
    }
    println!("=> top hit is Mars doc: {}", scored[0].1.contains("Mars"));

    // Warm up.
    for _ in 0..3 {
        m.embed(&["warmup sentence about nothing in particular".to_string()])?;
    }

    let short = "The quick brown fox jumps over the lazy dog.".to_string();
    let n = 100usize;
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let _ = m.embed(&[short.clone()])?;
        times.push(t.elapsed());
    }
    times.sort();
    let mean = times.iter().map(|d| d.as_secs_f64() * 1e3).sum::<f64>() / n as f64;
    println!("\nsingle-embed latency ({} iters):", n);
    println!("  mean {:.2} ms   p50 {:.2} ms   p99 {:.2} ms",
        mean, times[n / 2].as_secs_f64() * 1e3, times[(n as f64 * 0.99) as usize].as_secs_f64() * 1e3);

    let pool: Vec<String> = (0..512)
        .map(|i| format!("This is benchmark sentence number {i} about retrieval and storage."))
        .collect();
    println!("\nbatched throughput ({} cores):", std::thread::available_parallelism()?.get());
    for &bs in &[1usize, 4, 8, 16, 32, 64, 128, 256] {
        let batch: Vec<String> = pool.iter().take(bs).cloned().collect();
        let mut best = std::time::Duration::from_secs(1000);
        for _ in 0..3 {
            let t = Instant::now();
            let out = m.embed(&batch)?;
            debug_assert_eq!(out.len(), bs);
            best = best.min(t.elapsed());
        }
        let ms = best.as_secs_f64() * 1e3;
        println!("  batch {:>3}: {:8.2} ms total   {:7.3} ms/text   {:7.0} texts/s",
            bs, ms, ms / bs as f64, bs as f64 / (ms / 1e3));
    }
    Ok(())
}
