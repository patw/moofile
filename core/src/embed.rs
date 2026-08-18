/// Auto-embedding engine — runs `voyage-4-nano` through ONNX Runtime.
///
/// This module provides:
/// - [`EmbeddingEngine`]: loads and runs the embedding model
/// - [`AutoEmbedConfig`]: configuration per source text field
/// - [`EmbeddingPrecision`]: how to quantize the output vectors
/// - Quantization/helper functions for int8/uint8/binary
///
/// ## The `embed` feature
///
/// Everything that actually *runs* a model lives behind the `embed` feature,
/// which is on by default.  Building with `--no-default-features` drops the
/// `v4nano-embed` dependency (plus the statically linked ONNX Runtime and the
/// HuggingFace downloader) along with model loading and downloads.
///
/// The configuration types — [`AutoEmbedConfig`], [`EmbeddingPrecision`] —
/// and the quantisation helpers are always compiled, so the rest of the crate
/// needs no `cfg` attributes: only [`EmbeddingEngine`] changes shape, becoming
/// a stub whose constructor returns [`MooFileError::EmbedDisabled`].

use std::path::Path;
#[cfg(feature = "embed")]
use std::path::PathBuf;
#[cfg(feature = "embed")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "embed")]
use v4nano_embed::V4Nano;

use crate::MooFileError;

// ---------------------------------------------------------------------------
// Embedding precision
// ---------------------------------------------------------------------------

/// How to quantize embedding vectors for storage and search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EmbeddingPrecision {
    /// 32-bit floating point (4 bytes per dim)
    F32,
    /// Signed 8-bit integer (1 byte per dim), symmetric quantization
    Int8,
    /// Unsigned 8-bit integer (1 byte per dim), min-max quantization
    Uint8,
    /// Binary packing (1 bit per dim → 128 bytes for 1024 dims)
    Binary,
}

impl std::fmt::Display for EmbeddingPrecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmbeddingPrecision::F32 => write!(f, "f32"),
            EmbeddingPrecision::Int8 => write!(f, "int8"),
            EmbeddingPrecision::Uint8 => write!(f, "uint8"),
            EmbeddingPrecision::Binary => write!(f, "binary"),
        }
    }
}

// ---------------------------------------------------------------------------
// Model resolution
// ---------------------------------------------------------------------------

/// Output width of voyage-4-nano in its native (non-MRL) form.  A configured
/// `dims` below this is deliberate (MRL truncation); above it is an error.
pub const MODEL_DIMS: usize = 2048;

/// The files a local model directory must contain.  The quantised export keeps
/// its weights in an external `.onnx_data` file next to the graph.
#[cfg(feature = "embed")]
pub(crate) const MODEL_FILE: &str = "model_quantized.onnx";
#[cfg(feature = "embed")]
pub(crate) const MODEL_DATA_FILE: &str = "model_quantized.onnx_data";
#[cfg(feature = "embed")]
pub(crate) const TOKENIZER_FILE: &str = "tokenizer.json";

/// The HuggingFace repo the built-in model is fetched from.
#[cfg(feature = "embed")]
pub(crate) const HF_REPO: &str = "onnx-community/voyage-4-nano-ONNX";

/// The model's hard ceiling (its trained context).  `max_length` above this is
/// clamped here: the ONNX export materializes a full [1, 16, T, T] attention
/// mask, so going past 32k would only spend ~64 GB on a mask the model can't
/// use anyway.
#[cfg(feature = "embed")]
pub(crate) const MODEL_MAX_LENGTH: usize = 32768;

/// One of the two things a `model` spec can resolve to.
#[cfg(feature = "embed")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedModel {
    /// The built-in voyage-4-nano, fetched from HuggingFace into the cache dir.
    Voyage4Nano,
    /// A directory on disk holding `model_quantized.onnx` (+ data) and `tokenizer.json`.
    Local(PathBuf),
}

/// Resolve a configured `model` string.
///
/// Accepted, case-insensitively:
/// - the built-in id `"voyage-4-nano"` (plus a few aliases), and
/// - a path (leading `.` or `/`, or any existing path) to a local model dir.
///
/// The GGUF-era `hf:` syntax is rejected with migration guidance.
#[cfg(feature = "embed")]
pub(crate) fn resolve_model(spec: &str) -> Result<ResolvedModel, MooFileError> {
    let normalized = spec.trim().to_ascii_lowercase();
    // An empty `model` (some bindings serialize an unset string field as "")
    // means "use the default".
    if normalized.is_empty() {
        return Ok(ResolvedModel::Voyage4Nano);
    }
    const ALIASES: [&str; 4] = [
        "voyage-4-nano",
        "voyageai/voyage-4-nano",
        "voyage-4-nano-onnx",
        "onnx-community/voyage-4-nano-onnx",
    ];
    if ALIASES.contains(&normalized.as_str()) {
        return Ok(ResolvedModel::Voyage4Nano);
    }

    if let Some(rest) = spec.strip_prefix("hf:") {
        return Err(MooFileError::EmbeddingError(format!(
            "the 'hf:' GGUF model syntax was removed in moofile 1.1 — \
             autoembedding now runs voyage-4-nano through ONNX Runtime. \
             Use 'voyage-4-nano' (got 'hf:{rest}')."
        )));
    }

    if spec.starts_with('.') || spec.starts_with('/') || Path::new(spec).exists() {
        return Ok(ResolvedModel::Local(PathBuf::from(spec)));
    }

    Err(MooFileError::EmbeddingError(format!(
        "unknown embedding model '{spec}'. Only 'voyage-4-nano' is supported — \
         omit 'model' to use it, or pass a path to a local model directory \
         containing '{MODEL_FILE}' and '{TOKENIZER_FILE}'."
    )))
}

// ---------------------------------------------------------------------------
// AutoEmbed config
// ---------------------------------------------------------------------------

/// Configuration for a single auto-embedding source field.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AutoEmbedConfig {
    /// The embedding model id.  `voyage-4-nano` is built in; anything else
    /// must be a path to a local model directory.
    pub model: String,
    /// The target vector field name (default: inferred or configured)
    pub target_field: String,
    /// Embedding dimension (MRL truncation; defaults to the model's 2048)
    pub dims: usize,
    /// How to quantize the stored vectors
    pub precision: EmbeddingPrecision,
    /// Whether to L2-normalize the output
    pub normalize: bool,
    /// Prompt prefix for query-side embedding
    pub query_prefix: String,
    /// Prompt prefix for document-side embedding
    pub doc_prefix: String,
    /// Tokenizer truncation cap.  Default 1024: cheap (67 MB attention mask,
    /// ~0.7 s on CPU) and right for retrieval-sized chunks.  Long-document
    /// embedding is quadratic in memory (16·T²·4 B for the attention mask), so
    /// raise this only for the rare whole-document case.
    pub max_length: usize,
    /// Maximum batch size for embedding (1 = one at a time).  This is a cap
    /// on document *count*; `max_batch_tokens` usually binds first.
    pub batch_size: usize,
    /// Memory budget for one forward pass, in padded token slots
    /// (`batch x padded_sequence_length`).
    ///
    /// Peak RSS during inference is dominated by this product, not by the
    /// model weights: measured on voyage-4-nano the weights are ~170 MiB
    /// while a batch of 32 x 1024 tokens peaks at ~9.2 GiB.  Batching purely
    /// by document count made that peak depend on how long the caller's
    /// documents happened to be, which is how a re-embed of a few hundred
    /// ordinary documents could OOM a 16 GB machine.  Budgeting by token
    /// slots instead bounds the peak no matter what the corpus looks like:
    /// long documents get small batches, short ones get large batches.
    ///
    /// The default of 8192 slots holds inference to roughly 2.5 GiB.
    pub max_batch_tokens: usize,
}

/// The default model: voyage-4-nano, 180M + 160M params, 2048 dims, 32k
/// context, frontier retrieval quality.
pub const DEFAULT_MODEL: &str = "voyage-4-nano";

/// voyage-4-nano is asymmetric: queries carry an instruction prefix, documents
/// do not.
pub const DEFAULT_QUERY_PREFIX: &str =
    "Represent the query for retrieving supporting documents: ";

impl Default for AutoEmbedConfig {
    fn default() -> Self {
        Self {
            model: DEFAULT_MODEL.into(),
            target_field: String::new(),
            dims: MODEL_DIMS,
            precision: EmbeddingPrecision::F32,
            normalize: true,
            query_prefix: DEFAULT_QUERY_PREFIX.into(),
            doc_prefix: String::new(),
            max_length: 1024,
            batch_size: 32,
            max_batch_tokens: 8192,
        }
    }
}

// ---------------------------------------------------------------------------
// Embedding engine
// ---------------------------------------------------------------------------

/// Wraps a [`V4Nano`] ONNX session for embedding text.
///
/// Cloning is cheap (an `Arc` bump) and shares the underlying session.
///
/// The `Mutex` is forced by `V4Nano::embed` taking `&mut self` (ONNX Runtime
/// sessions are not `Sync`).  It costs nothing on the write path, which already
/// holds the collection's write lock and the file lock, and on the read path
/// ONNX Runtime's intra-op threading already saturates the available cores for
/// a single embed — so concurrent sessions would mostly contend anyway.
#[cfg(feature = "embed")]
#[derive(Clone)]
pub struct EmbeddingEngine {
    inner: Arc<Mutex<V4Nano>>,
    dims: usize,
}

/// Stub used when the `embed` feature is off.
///
/// Keeping the type (rather than removing it) is what lets the rest of the
/// crate stay free of `cfg` attributes: `Collection` can still hold a map of
/// engines, it just can never construct one.  The uninhabited body makes that
/// a compile-time guarantee rather than a convention.
#[cfg(not(feature = "embed"))]
#[derive(Clone)]
pub struct EmbeddingEngine {
    _never: std::convert::Infallible,
}

impl std::fmt::Debug for EmbeddingEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingEngine").finish()
    }
}

#[cfg(not(feature = "embed"))]
impl EmbeddingEngine {
    /// Always fails — this build has no embedding engine compiled in.
    pub fn load(_model: &str, _max_length: usize, _cache_dir: &Path) -> Result<Self, MooFileError> {
        Err(MooFileError::EmbedDisabled)
    }

    /// Unreachable: no value of this type can exist without the feature.
    pub fn embed(&self, _text: &str) -> Result<Vec<f32>, MooFileError> {
        match self._never {}
    }

    /// Unreachable: no value of this type can exist without the feature.
    pub fn embed_batch(&self, _texts: Vec<String>) -> Result<Vec<Vec<f32>>, MooFileError> {
        match self._never {}
    }

    /// Unreachable: no value of this type can exist without the feature.
    pub fn dims(&self) -> usize {
        match self._never {}
    }
}

#[cfg(feature = "embed")]
impl EmbeddingEngine {
    /// Load the embedding model, downloading it into `cache_dir` on first use.
    ///
    /// `cache_dir` is where the ONNX weights and tokenizer are cached; the
    /// download happens once and every later open reads from disk.
    ///
    /// `max_length` caps tokenizer truncation (clamped to
    /// 1..=`MODEL_MAX_LENGTH`); keep it small — the export's attention mask
    /// costs 16·T²·4 bytes.
    pub fn load(model: &str, max_length: usize, cache_dir: &Path) -> Result<Self, MooFileError> {
        let resolved = resolve_model(model)?;

        let (model_path, tokenizer_path) = match resolved {
            ResolvedModel::Voyage4Nano => ensure_model_files(cache_dir)?,
            ResolvedModel::Local(dir) => {
                let model_path = dir.join(MODEL_FILE);
                let tokenizer_path = dir.join(TOKENIZER_FILE);
                for (path, name) in [(&model_path, MODEL_FILE), (&tokenizer_path, TOKENIZER_FILE)] {
                    if !path.exists() {
                        return Err(MooFileError::EmbeddingError(format!(
                            "local model directory '{}' has no '{name}'",
                            dir.display()
                        )));
                    }
                }
                (model_path, tokenizer_path)
            }
        };

        debug_assert_eq!(
            v4nano_embed::DIM,
            MODEL_DIMS,
            "moofile expects the model's output width"
        );
        let engine = V4Nano::load(&model_path, &tokenizer_path, max_length.clamp(1, MODEL_MAX_LENGTH), None)
            .map_err(|e| MooFileError::EmbeddingError(format!("failed to load model '{model}': {e}")))?;

        log::info!("moofile: loaded embedding model '{model}' ({MODEL_DIMS} dim)");
        Ok(Self {
            inner: Arc::new(Mutex::new(engine)),
            dims: MODEL_DIMS,
        })
    }

    /// Generate an embedding vector for a single text string.
    pub fn embed(&self, text: &str) -> Result<Vec<f32>, MooFileError> {
        let mut out = self.embed_batch(vec![text.to_string()])?;
        if out.is_empty() {
            return Err(MooFileError::EmbeddingError(
                "model returned no embedding".into(),
            ));
        }
        Ok(out.swap_remove(0))
    }

    /// Embed several texts in one ONNX pass.
    ///
    /// Measurably cheaper per text than looping over [`Self::embed`] — the
    /// per-call overhead dominates for short inputs.
    pub fn embed_batch(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, MooFileError> {
        // A panic inside the model would poison this lock and brick the
        // collection, so recover the guard rather than propagating it.
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .embed(&texts)
            .map_err(|e| MooFileError::EmbeddingError(format!("embedding failed: {e}")))
    }

    /// The model's output dimension.
    pub fn dims(&self) -> usize {
        self.dims
    }
}

/// Fetch the ONNX export + tokenizer into `cache_dir` (via hf-hub) and return
/// the paths to the model graph and tokenizer.
#[cfg(feature = "embed")]
fn ensure_model_files(cache_dir: &Path) -> Result<(PathBuf, PathBuf), MooFileError> {
    use hf_hub::api::sync::ApiBuilder;

    let api = ApiBuilder::new()
        .with_cache_dir(cache_dir.to_path_buf())
        .with_progress(true)
        .build()
        .map_err(|e| MooFileError::EmbeddingError(format!("hf hub init failed: {e}")))?;
    let repo = api.model(HF_REPO.to_string());

    let model_path = repo
        .get(&format!("onnx/{MODEL_FILE}"))
        .map_err(|e| MooFileError::EmbeddingError(format!("failed to fetch {MODEL_FILE}: {e}")))?;
    // The graph references its weights via `model_quantized.onnx_data` relative
    // to its own path, so fetching it into the same snapshot dir is enough for
    // ONNX Runtime to find the external data.
    repo.get(&format!("onnx/{MODEL_DATA_FILE}"))
        .map_err(|e| MooFileError::EmbeddingError(format!("failed to fetch {MODEL_DATA_FILE}: {e}")))?;
    let tokenizer_path = repo
        .get(TOKENIZER_FILE)
        .map_err(|e| MooFileError::EmbeddingError(format!("failed to fetch {TOKENIZER_FILE}: {e}")))?;

    Ok((model_path, tokenizer_path))
}

// ---------------------------------------------------------------------------
// Quantization helpers
// ---------------------------------------------------------------------------

/// Group texts into batches that respect both the count cap and the token
/// budget, longest first.
///
/// Returns batches of indices into `texts`.  Two things matter here:
///
/// * **Sorting by length.** The tokenizer pads to the longest member of a
///   batch, so one long document in an otherwise short batch inflates every
///   row to its width.  Sorting groups similar lengths together, which cuts
///   both wasted compute and wasted memory.
/// * **Budgeting by `batch x padded_len`.** That product is what inference
///   memory tracks.  Because the batch is built longest-first, the first
///   member's length *is* the padded width, so the budget can be enforced
///   exactly rather than guessed.
pub(crate) fn plan_batches(
    texts: &[String],
    config: &AutoEmbedConfig,
) -> Vec<Vec<usize>> {
    let count_cap = config.batch_size.max(1);
    let token_budget = config.max_batch_tokens.max(1);

    // Token count is estimated from bytes rather than by running the
    // tokenizer: this only decides batch shape, and over-estimating is the
    // safe direction (smaller batches).  3 bytes/token is deliberately
    // pessimistic -- technical prose with identifiers and punctuation
    // tokenizes far worse than the ~4 bytes/token of ordinary English.
    let est = |i: usize| (texts[i].len() / 3 + 2).clamp(1, config.max_length.max(1));

    let mut order: Vec<usize> = (0..texts.len()).collect();
    order.sort_by(|&a, &b| est(b).cmp(&est(a)).then_with(|| a.cmp(&b)));

    let mut batches: Vec<Vec<usize>> = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut width = 0usize;

    for i in order {
        // Longest-first means the running width can only be set by the first
        // member, so it never grows once the batch has started.
        let w = if current.is_empty() { est(i) } else { width };
        let fits = current.len() < count_cap && (current.len() + 1) * w <= token_budget;
        if !current.is_empty() && !fits {
            batches.push(std::mem::take(&mut current));
            width = est(i);
        } else if current.is_empty() {
            width = w;
        }
        current.push(i);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

/// Quantize an f32 embedding vector to the specified precision.
pub fn quantize(emb: &[f32], precision: EmbeddingPrecision) -> Vec<u8> {
    match precision {
        EmbeddingPrecision::F32 => {
            let mut bytes = Vec::with_capacity(emb.len() * 4);
            for &v in emb {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            bytes
        }
        EmbeddingPrecision::Int8 => {
            let max_abs = emb.iter().map(|x| x.abs()).fold(0.0_f32, f32::max);
            if max_abs == 0.0 {
                return vec![0u8; emb.len()];
            }
            let scale = 127.0 / max_abs;
            emb.iter()
                .map(|x| (x * scale).round().clamp(-128.0, 127.0) as i8 as u8)
                .collect()
        }
        EmbeddingPrecision::Uint8 => {
            let min = emb.iter().fold(f32::MAX, |a, &b| a.min(b));
            let max = emb.iter().fold(f32::MIN, |a, &b| a.max(b));
            let range = max - min;
            if range == 0.0 {
                return vec![128u8; emb.len()];
            }
            let scale = 255.0 / range;
            emb.iter()
                .map(|x| ((x - min) * scale).round().clamp(0.0, 255.0) as u8)
                .collect()
        }
        EmbeddingPrecision::Binary => {
            let n = emb.len();
            let byte_len = (n + 7) / 8;
            let mut bits = vec![0u8; byte_len];
            for (i, &val) in emb.iter().enumerate() {
                if val >= 0.0 {
                    bits[i / 8] |= 1 << (i % 8);
                }
            }
            bits
        }
    }
}

/// Dequantize a precision-encoded embedding back to f32 (for search).
pub fn dequantize(bytes: &[u8], precision: EmbeddingPrecision, dims: usize) -> Vec<f32> {
    match precision {
        EmbeddingPrecision::F32 => {
            bytes.chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect()
        }
        EmbeddingPrecision::Int8 => {
            bytes.iter()
                .map(|&b| {
                    let v = b as i8 as f32;
                    v / 127.0 // de-scale
                })
                .collect()
        }
        EmbeddingPrecision::Uint8 => {
            bytes.iter()
                .map(|&b| (b as f32 - 128.0) / 128.0)
                .collect()
        }
        EmbeddingPrecision::Binary => {
            let mut vec = Vec::with_capacity(dims);
            for i in 0..dims {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                let val = if byte_idx < bytes.len() && (bytes[byte_idx] & (1 << bit_idx)) != 0 {
                    1.0
                } else {
                    -1.0
                };
                vec.push(val);
            }
            vec
        }
    }
}

/// Compute cosine similarity for precision-encoded embeddings.
pub fn cosine_similarity_quantized(
    a: &[u8],
    b: &[u8],
    precision: EmbeddingPrecision,
    dims: usize,
) -> f32 {
    match precision {
        EmbeddingPrecision::F32 | EmbeddingPrecision::Int8 | EmbeddingPrecision::Uint8 => {
            let a_f32 = dequantize(a, precision, dims);
            let b_f32 = dequantize(b, precision, dims);
            let dot: f32 = a_f32.iter().zip(&b_f32).map(|(x, y)| x * y).sum();
            let norm_a: f32 = a_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b: f32 = b_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                0.0
            } else {
                dot / (norm_a * norm_b)
            }
        }
        EmbeddingPrecision::Binary => {
            // Binary cosine: 1 - 2 * popcount(a XOR b) / n
            let total_bits = dims as f32;
            if total_bits == 0.0 {
                return 0.0;
            }
            let diff_bits: u32 = a.iter().zip(b.iter())
                .map(|(x, y)| (x ^ y).count_ones())
                .sum();
            1.0 - 2.0 * diff_bits as f32 / total_bits
        }
    }
}

/// Get the byte storage size for one embedding at the given precision.
pub fn storage_size(dims: usize, precision: EmbeddingPrecision) -> usize {
    match precision {
        EmbeddingPrecision::F32 => dims * 4,
        EmbeddingPrecision::Int8 | EmbeddingPrecision::Uint8 => dims,
        EmbeddingPrecision::Binary => (dims + 7) / 8,
    }
}

#[cfg(all(test, feature = "embed"))]
mod resolve_tests {
    use super::*;

    /// The default id and every alias land on the built-in model.
    #[test]
    fn default_model_and_aliases_resolve() {
        assert_eq!(resolve_model(DEFAULT_MODEL).unwrap(), ResolvedModel::Voyage4Nano);
        assert_eq!(
            resolve_model("voyageai/voyage-4-nano").unwrap(),
            ResolvedModel::Voyage4Nano
        );
        assert_eq!(
            resolve_model("onnx-community/voyage-4-nano-ONNX").unwrap(),
            ResolvedModel::Voyage4Nano
        );
        // Case-insensitive.
        assert_eq!(
            resolve_model("VOYAGE-4-NANO").unwrap(),
            ResolvedModel::Voyage4Nano
        );
    }

    /// Path-shaped specs are local model directories, not registry lookups.
    #[test]
    fn local_paths_resolve_to_a_directory() {
        assert_eq!(
            resolve_model("/models/voyage-4-nano").unwrap(),
            ResolvedModel::Local(PathBuf::from("/models/voyage-4-nano"))
        );
        assert_eq!(
            resolve_model("./models/my-model").unwrap(),
            ResolvedModel::Local(PathBuf::from("./models/my-model"))
        );
    }

    /// An empty `model` (bindings serialize unset strings as "") means default.
    #[test]
    fn empty_model_resolves_to_default() {
        assert_eq!(resolve_model("").unwrap(), ResolvedModel::Voyage4Nano);
        assert_eq!(resolve_model("   ").unwrap(), ResolvedModel::Voyage4Nano);
    }

    /// The old GGUF syntax must fail with migration guidance, not a bare
    /// "unknown model" — it is what every pre-1.1 config contains.
    #[test]
    fn gguf_uri_reports_the_migration() {
        let err = resolve_model("hf:jsonMartin/voyage-4-nano-gguf:q8_0.gguf")
            .unwrap_err()
            .to_string();
        assert!(err.contains("'hf:'"), "no migration guidance: {err}");
        assert!(err.contains("voyage-4-nano"), "no replacement suggested: {err}");
    }

    /// A non-voyage registry id is rejected, pointing at the supported model.
    #[test]
    fn unknown_model_is_rejected_with_the_replacement() {
        let err = resolve_model("bge-small-en-v1.5").unwrap_err().to_string();
        assert!(err.contains("unknown embedding model"), "unhelpful error: {err}");
        assert!(err.contains("voyage-4-nano"), "no replacement suggested: {err}");
    }

    /// The engine advertises the model's native width.
    #[test]
    fn model_dims_are_2048() {
        assert_eq!(MODEL_DIMS, 2048);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod batch_tests {
    use super::*;

    fn cfg(batch_size: usize, max_batch_tokens: usize) -> AutoEmbedConfig {
        AutoEmbedConfig { batch_size, max_batch_tokens, ..Default::default() }
    }

    fn texts(byte_lens: &[usize]) -> Vec<String> {
        byte_lens.iter().map(|&n| "x".repeat(n)).collect()
    }

    /// Every index appears exactly once, or a re-embed would silently skip or
    /// double-write documents.
    #[test]
    fn batches_partition_the_input() {
        let t = texts(&[10, 3000, 50, 900, 12, 4000, 77, 2100]);
        let mut seen: Vec<usize> = plan_batches(&t, &cfg(32, 8192)).concat();
        seen.sort();
        assert_eq!(seen, (0..t.len()).collect::<Vec<_>>());
    }

    /// The whole point: batch x padded width stays under budget, so peak
    /// inference memory does not depend on how long the corpus happens to be.
    #[test]
    fn token_budget_is_respected() {
        let budget = 4096;
        // 3 KB of text ~= 1024 estimated tokens, the max_length cap.
        let t = texts(&[3000; 40]);
        let batches = plan_batches(&t, &cfg(32, budget));
        for b in &batches {
            assert!(b.len() * 1024 <= budget, "batch of {} exceeds budget", b.len());
        }
        // 1024-token documents: 4 per batch at this budget, not 32.
        assert!(batches.iter().all(|b| b.len() <= 4));
    }

    /// Short documents must still batch widely -- the budget should cost
    /// throughput only where memory actually demands it.
    #[test]
    fn short_texts_still_fill_the_count_cap() {
        let t = texts(&[30; 64]); // ~12 estimated tokens each
        let batches = plan_batches(&t, &cfg(32, 8192));
        assert_eq!(batches[0].len(), 32, "short docs should hit the count cap");
    }

    /// One long document must not drag a batch of short ones up to its width.
    #[test]
    fn long_and_short_are_not_mixed() {
        let mut lens = vec![20usize; 30];
        lens.push(3000);
        let t = texts(&lens);
        let batches = plan_batches(&t, &cfg(32, 8192));
        let long_batch = batches.iter().find(|b| b.contains(&30)).unwrap();
        assert!(long_batch.len() <= 8, "long doc batched with {} others", long_batch.len() - 1);
    }

    #[test]
    fn empty_input_yields_no_batches() {
        assert!(plan_batches(&[], &cfg(32, 8192)).is_empty());
    }

    /// Degenerate settings must not produce a zero-sized batch and spin.
    #[test]
    fn tiny_budget_still_makes_progress() {
        let t = texts(&[100_000; 3]);
        let batches = plan_batches(&t, &cfg(32, 1));
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|b| b.len() == 1));
    }
}
