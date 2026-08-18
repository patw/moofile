//! Peak-RSS probe: how does embedding memory scale with batch size and
//! sequence length?  Run one (batch, seq) pair per process so VmHWM is clean.
//!
//!   cargo run --release -p v4nano-embed --example memprobe -- <batch> <seq>

use std::fs;

fn hwm_mib() -> f64 {
    for line in fs::read_to_string("/proc/self/status").unwrap().lines() {
        if let Some(v) = line.strip_prefix("VmHWM:") {
            let kb: f64 = v.trim().trim_end_matches(" kB").trim().parse().unwrap();
            return kb / 1024.0;
        }
    }
    0.0
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let batch: usize = a[1].parse().unwrap();
    let seq: usize = a[2].parse().unwrap();

    let base = std::env::var("MODEL_DIR").unwrap();
    let model = format!("{base}/onnx/model_quantized.onnx");
    let tok = format!("{base}/tokenizer.json");

    let after_load;
    let mut m = v4nano_embed::V4Nano::load(&model, &tok, seq, None).unwrap();
    after_load = hwm_mib();

    // "word " is ~1 token, so this lands close to `seq` tokens per text and
    // every text is the same length -> BatchLongest pads to exactly seq.
    let text = "word ".repeat(seq * 2);
    let texts: Vec<String> = (0..batch).map(|_| text.clone()).collect();

    let v = m.embed(&texts).unwrap();
    println!(
        "batch={batch:3} seq={seq:5}  after_load={after_load:8.0} MiB  peak={:8.0} MiB  delta={:8.0} MiB  vecs={}",
        hwm_mib(),
        hwm_mib() - after_load,
        v.len()
    );
}
