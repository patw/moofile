//! Pure-Rust benchmark — no Python, no PyO3 overhead.
//!
//! Run:  cargo run --example bench --release
//!
//! Compares against the Python numbers from bench_native.py.

use std::time::Instant;

use bson::doc;
use moofile::Collection;

const N_DOCS: usize = 10_000;
const N_LOOKUPS: usize = 2_000;
const N_RANGES: usize = 1_000;
const N_SCANS: usize = 200;
const N_UPDATES: usize = 500;
const N_DELETES: usize = 200;
const N_VECTOR: usize = 50;
const N_TEXT: usize = 50;
const VECTOR_DIM: usize = 128;

const STATUSES: &[&str] = &["active", "inactive", "trial", "expired"];
const CITIES: &[&str] = &["NYC", "LA", "Chicago", "Houston", "Phoenix", "Austin"];
const WORDS: &[&str] = &[
    "machine", "learning", "data", "science", "neural", "network", "deep",
    "algorithm", "python", "database", "analytics", "system", "cloud", "server",
];

fn rand_usize(max: usize) -> usize {
    // Simple LCG — fine for a benchmark
    static mut STATE: u64 = 42;
    unsafe {
        STATE = STATE.wrapping_mul(6364136223846793005).wrapping_add(1);
        (STATE as usize) % max
    }
}

fn make_doc(i: usize) -> bson::Document {
    let email = format!("user{i:06}@example.com");
    let age = 18 + (rand_usize(63)) as i32;
    let status = STATUSES[rand_usize(STATUSES.len())];
    let city = CITIES[rand_usize(CITIES.len())];
    let score = (rand_usize(10000) as f64) / 100.0;
    let name: String = (0..8).map(|_| (b'a' + rand_usize(26) as u8) as char).collect();

    let content_len = 5 + rand_usize(11);
    let content: String = (0..content_len)
        .map(|_| WORDS[rand_usize(WORDS.len())])
        .collect::<Vec<_>>()
        .join(" ");

    let embedding: Vec<bson::Bson> = (0..VECTOR_DIM)
        .map(|_| bson::Bson::Double(rand_usize(200) as f64 / 100.0 - 1.0))
        .collect();

    doc! {
        "_id": i.to_string(),
        "email": email,
        "age": age,
        "status": status,
        "city": city,
        "score": score,
        "name": name,
        "content": content,
        "embedding": bson::Bson::Array(embedding),
    }
}

fn main() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("rust_bench.bson");

    println!("\n{:->72}", "");
    println!("  MooFile Pure-Rust Benchmark — {N_DOCS} docs, vec={VECTOR_DIM}d");
    println!("{:->72}", "");
    println!("  {:<40} {:>10}", "Operation", "Time");
    println!("  {:-<52}", "");

    // --- Generate data ---
    let docs: Vec<_> = (0..N_DOCS).map(make_doc).collect();

    // --- Insert ---
    let t0 = Instant::now();
    let db = Collection::builder(&path)
        .index("email")
        .index("age")
        .index("status")
        .index("city")
        .vector_index("embedding", VECTOR_DIM)
        .text_index("content")
        .open()
        .unwrap();
    for doc in &docs {
        db.insert(doc.clone()).unwrap();
    }
    let insert_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", format!("insert ({N_DOCS})"), insert_elapsed.as_secs_f64() * 1000.0);

    // --- Count ---
    let t0 = Instant::now();
    let n = db.count(doc! {}).unwrap();
    assert_eq!(n, N_DOCS);
    let count_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", "count all", count_elapsed.as_secs_f64() * 1000.0);

    // --- Indexed exact lookup ---
    let emails: Vec<String> = (0..N_LOOKUPS)
        .map(|_| format!("user{:06}@example.com", rand_usize(N_DOCS)))
        .collect();
    let t0 = Instant::now();
    for email in &emails {
        db.find_one(doc! { "email": email }).unwrap();
    }
    let lookup_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", format!("find_one indexed ({N_LOOKUPS}x)"), lookup_elapsed.as_secs_f64() * 1000.0);

    // --- Indexed range ---
    let t0 = Instant::now();
    for _ in 0..N_RANGES {
        let age = 20 + rand_usize(51) as i32;
        db.find(doc! { "age": { "$gte": age, "$lt": age + 10 } })
            .unwrap()
            .to_list()
            .unwrap();
    }
    let range_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", format!("range find ({N_RANGES}x)"), range_elapsed.as_secs_f64() * 1000.0);

    // --- Full scan ---
    let names: Vec<String> = (0..N_SCANS)
        .map(|_| {
            let i = rand_usize(N_DOCS);
            docs[i].get_str("name").unwrap().to_string()
        })
        .collect();
    let t0 = Instant::now();
    for name in &names {
        db.find(doc! { "name": name }).unwrap().to_list().unwrap();
    }
    let scan_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", format!("full scan ({N_SCANS}x)"), scan_elapsed.as_secs_f64() * 1000.0);

    // --- Update ---
    let t0 = Instant::now();
    for _ in 0..N_UPDATES {
        let uid = rand_usize(N_DOCS).to_string();
        let _ = db.update_one(
            doc! { "_id": &uid },
            Some(doc! { "score": 0.0 }),
            None,
            None,
        );
    }
    let update_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", format!("update ({N_UPDATES}x)"), update_elapsed.as_secs_f64() * 1000.0);

    // --- Delete ---
    let t0 = Instant::now();
    for _ in 0..N_DELETES {
        let did = rand_usize(N_DOCS / 2).to_string();
        db.delete_one(doc! { "_id": &did }).unwrap();
    }
    let delete_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", format!("delete ({N_DELETES}x)"), delete_elapsed.as_secs_f64() * 1000.0);

    // --- Vector search ---
    // Vectors are rebuilt lazily; ensure they're fresh before searching
    db.ensure_vectors_fresh().unwrap();
    let t0 = Instant::now();
    for _ in 0..N_VECTOR {
        let qv: Vec<f32> = (0..VECTOR_DIM)
            .map(|_| rand_usize(200) as f32 / 100.0 - 1.0)
            .collect();
        db.find(doc! {})
            .unwrap()
            .vector_search("embedding", qv, 10)
            .to_list()
            .unwrap();
    }
    let vector_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", format!("vector_search ({N_VECTOR}x)"), vector_elapsed.as_secs_f64() * 1000.0);

    // --- Text search ---
    let t0 = Instant::now();
    for _ in 0..N_TEXT {
        let q = WORDS[rand_usize(WORDS.len())];
        db.find(doc! {})
            .unwrap()
            .text_search("content", q, 5)
            .to_list()
            .unwrap();
    }
    let text_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", format!("text_search ({N_TEXT}x)"), text_elapsed.as_secs_f64() * 1000.0);

    // --- Compact ---
    let t0 = Instant::now();
    db.compact().unwrap();
    let compact_elapsed = t0.elapsed();
    println!("  {:<40} {:>7.1} ms", "compact", compact_elapsed.as_secs_f64() * 1000.0);

    // --- Summary ---
    let total = insert_elapsed
        + count_elapsed
        + lookup_elapsed
        + range_elapsed
        + scan_elapsed
        + update_elapsed
        + delete_elapsed
        + vector_elapsed
        + text_elapsed
        + compact_elapsed;

    println!("  {:-<52}", "");
    println!("  {:<40} {:>7.0} ms", "TOTAL (pure Rust)", total.as_secs_f64() * 1000.0);
    println!();
}
