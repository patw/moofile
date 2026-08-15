/// Auto-embedding engine — wraps `fastembed` for on-device ONNX embedding.
///
/// This module provides:
/// - [`EmbeddingEngine`]: loads and runs an ONNX embedding model
/// - [`AutoEmbedConfig`]: configuration per source text field
/// - [`EmbeddingPrecision`]: how to quantize the output vectors
/// - Quantization/helper functions for int8/uint8/binary
///
/// ## The `embed` feature
///
/// Everything that actually *runs* a model lives behind the `embed` feature,
/// which is on by default.  Building with `--no-default-features` drops the
/// `fastembed` dependency (a ~129-crate tree plus a statically linked ONNX
/// Runtime) along with model loading and HuggingFace downloads.
///
/// The configuration types — [`AutoEmbedConfig`], [`EmbeddingPrecision`] —
/// and the quantisation helpers are always compiled, so the rest of the crate
/// needs no `cfg` attributes: only [`EmbeddingEngine`] changes shape, becoming
/// a stub whose constructor returns [`MooFileError::EmbedDisabled`].

use std::path::Path;
#[cfg(feature = "embed")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "embed")]
use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};

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

/// Resolve a configured model string to a `fastembed` registry entry.
///
/// Three spellings are accepted, tried in this order:
///
/// 1. The exact `model_code` from fastembed's registry, e.g.
///    `"Qdrant/bge-small-en-v1.5-onnx-Q"`.  Unambiguous, and the only way to
///    name a quantised variant that shares a basename with its parent.
/// 2. The Rust variant name, e.g. `"BGESmallENV15"` (what fastembed's own
///    `FromStr` accepts).
/// 3. The basename after the last `/`, e.g. `"bge-small-en-v1.5"` — which is
///    what makes the natural `"BAAI/bge-small-en-v1.5"` work even though
///    fastembed serves that model from the `Xenova/` mirror.
///
/// All three are case-insensitive.  A basename can be ambiguous (both
/// `NomicEmbedTextV15` and `NomicEmbedTextV15Q` are served from
/// `nomic-ai/nomic-embed-text-v1.5`), so ties are broken by sorting the
/// variant names and taking the first — which deterministically prefers the
/// unquantised model.  Name the `model_code` explicitly to get the other one.
#[cfg(feature = "embed")]
pub(crate) fn resolve_model(spec: &str) -> Result<EmbeddingModel, MooFileError> {
    // A path-shaped spec is a local model, which is not wired up yet.  Catch
    // it here so it fails with an explanation rather than "unknown model".
    if spec.starts_with('.') || spec.starts_with('/') || Path::new(spec).exists() {
        return Err(MooFileError::EmbeddingError(format!(
            "local model paths are not supported yet: '{spec}'. \
             Use a fastembed registry model such as 'BAAI/bge-small-en-v1.5'."
        )));
    }
    if let Some(rest) = spec.strip_prefix("hf:") {
        return Err(MooFileError::EmbeddingError(format!(
            "the 'hf:' GGUF model syntax was removed in moofile 1.1 — \
             autoembedding now runs ONNX models through fastembed. \
             Use a registry id such as 'BAAI/bge-small-en-v1.5' \
             (got 'hf:{rest}')."
        )));
    }

    let models = TextEmbedding::list_supported_models();
    let wanted = basename(spec);

    // Every strategy can match more than one variant — `model_code` included,
    // since the quantised models are often served from the same repo as their
    // parent.  Registry order is not guaranteed, so always break ties by
    // sorting on the variant name, which puts `NomicEmbedTextV15` ahead of
    // `NomicEmbedTextV15Q` and so prefers the unquantised model.
    let pick = |f: &dyn Fn(&fastembed::ModelInfo<EmbeddingModel>) -> bool| {
        let mut matches: Vec<_> = models.iter().filter(|m| f(m)).collect();
        matches.sort_by_key(|m| format!("{:?}", m.model));
        matches.first().map(|m| m.model.clone())
    };

    if let Some(m) = pick(&|m| m.model_code.eq_ignore_ascii_case(spec)) {
        return Ok(m);
    }
    if let Some(m) = pick(&|m| format!("{:?}", m.model).eq_ignore_ascii_case(spec)) {
        return Ok(m);
    }
    if let Some(m) = pick(&|m| basename(&m.model_code).eq_ignore_ascii_case(wanted)) {
        return Ok(m);
    }

    // Nothing matched — suggest the closest few by shared prefix so the error
    // is actionable rather than a wall of 40 model names.
    let mut suggestions: Vec<String> = models
        .iter()
        .filter(|m| {
            let b = basename(&m.model_code).to_ascii_lowercase();
            let w = wanted.to_ascii_lowercase();
            b.contains(&w) || w.contains(&b)
        })
        .map(|m| m.model_code.clone())
        .collect();
    suggestions.sort();
    suggestions.truncate(5);

    Err(MooFileError::EmbeddingError(if suggestions.is_empty() {
        format!(
            "unknown embedding model '{spec}'. \
             See fastembed's model registry; 'BAAI/bge-small-en-v1.5' is the default."
        )
    } else {
        format!(
            "unknown embedding model '{spec}'. Did you mean one of: {}?",
            suggestions.join(", ")
        )
    }))
}

/// The portion of a model id after the last `/`.
#[cfg(feature = "embed")]
fn basename(s: &str) -> &str {
    s.rsplit('/').next().unwrap_or(s)
}

// ---------------------------------------------------------------------------
// AutoEmbed config
// ---------------------------------------------------------------------------

/// Configuration for a single auto-embedding source field.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AutoEmbedConfig {
    /// The embedding model id (e.g., `BAAI/bge-small-en-v1.5`)
    pub model: String,
    /// The target vector field name (default: inferred or configured)
    pub target_field: String,
    /// Embedding dimension (for MRL truncation, defaults to model's hidden_size)
    pub dims: usize,
    /// How to quantize the stored vectors
    pub precision: EmbeddingPrecision,
    /// Whether to L2-normalize the output
    pub normalize: bool,
    /// Prompt prefix for query-side embedding
    pub query_prefix: String,
    /// Prompt prefix for document-side embedding
    pub doc_prefix: String,
    /// Maximum batch size for embedding (1 = one at a time)
    pub batch_size: usize,
}

/// The default model: 33M params, 384 dims, ~61 MTEB.  Also fastembed's own
/// default, so the happy path needs no model configuration at all.
pub const DEFAULT_MODEL: &str = "BAAI/bge-small-en-v1.5";

/// BGE is asymmetric: queries carry an instruction prefix, documents do not.
pub const DEFAULT_QUERY_PREFIX: &str =
    "Represent this sentence for searching relevant passages: ";

impl Default for AutoEmbedConfig {
    fn default() -> Self {
        Self {
            model: DEFAULT_MODEL.into(),
            target_field: String::new(),
            dims: 384,
            precision: EmbeddingPrecision::F32,
            normalize: true,
            query_prefix: DEFAULT_QUERY_PREFIX.into(),
            doc_prefix: String::new(),
            batch_size: 32,
        }
    }
}

// ---------------------------------------------------------------------------
// Embedding engine
// ---------------------------------------------------------------------------

/// Wraps a `fastembed` ONNX session for embedding text.
///
/// Cloning is cheap (an `Arc` bump) and shares the underlying session.
///
/// The `Mutex` is forced by fastembed: `TextEmbedding::embed` takes
/// `&mut self`.  It costs nothing on the write path, which already holds the
/// collection's write lock and the file lock, and on the read path ONNX
/// Runtime's intra-op threading already saturates the available cores for a
/// single embed — so concurrent sessions would mostly contend anyway.
#[cfg(feature = "embed")]
#[derive(Clone)]
pub struct EmbeddingEngine {
    inner: Arc<Mutex<TextEmbedding>>,
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
    pub fn load(_model: &str, _cache_dir: &Path) -> Result<Self, MooFileError> {
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
    /// Load an embedding model by registry id, downloading it if needed.
    ///
    /// `cache_dir` is where the ONNX weights and tokenizer are cached; the
    /// download happens once and every later open reads from disk.
    pub fn load(model: &str, cache_dir: &Path) -> Result<Self, MooFileError> {
        let resolved = resolve_model(model)?;

        // The registry knows each model's output width, so dims are exact
        // without paying for a throwaway embed at open time.
        let dims = TextEmbedding::get_model_info(&resolved)
            .map_err(|e| MooFileError::EmbeddingError(format!("no model info for {model}: {e}")))?
            .dim;

        log::info!("moofile: loading embedding model {model} ({resolved:?}, {dims} dim)");

        let options = TextInitOptions::new(resolved)
            .with_cache_dir(cache_dir.to_path_buf())
            .with_show_download_progress(true);

        let engine = TextEmbedding::try_new(options)
            .map_err(|e| MooFileError::EmbeddingError(format!("failed to load {model}: {e}")))?;

        log::info!("moofile: embedding model loaded successfully");
        Ok(Self {
            inner: Arc::new(Mutex::new(engine)),
            dims,
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
            .embed(texts, None)
            .map_err(|e| MooFileError::EmbeddingError(format!("embedding failed: {e}")))
    }

    /// The model's output dimension, from fastembed's registry.
    pub fn dims(&self) -> usize {
        self.dims
    }
}

// ---------------------------------------------------------------------------
// Quantization helpers
// ---------------------------------------------------------------------------

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

    /// The three accepted spellings all land on the same registry entry.
    #[test]
    fn all_spellings_resolve_to_the_same_model() {
        let canonical = resolve_model("BAAI/bge-small-en-v1.5").unwrap();
        // The mirror fastembed actually serves it from.
        assert_eq!(resolve_model("Xenova/bge-small-en-v1.5").unwrap(), canonical);
        // The Rust variant name.
        assert_eq!(resolve_model("BGESmallENV15").unwrap(), canonical);
        // Bare basename, and case-insensitively.
        assert_eq!(resolve_model("bge-small-en-v1.5").unwrap(), canonical);
        assert_eq!(resolve_model("BGE-Small-EN-V1.5").unwrap(), canonical);
    }

    #[test]
    fn default_model_resolves_and_is_384_dim() {
        let m = resolve_model(DEFAULT_MODEL).unwrap();
        assert_eq!(TextEmbedding::get_model_info(&m).unwrap().dim, 384);
    }

    /// An exact `model_code` must win over the basename rule, otherwise the
    /// quantised variants would be unreachable.
    #[test]
    fn exact_model_code_selects_the_quantised_variant() {
        let q = resolve_model("Qdrant/bge-small-en-v1.5-onnx-Q").unwrap();
        assert_ne!(q, resolve_model("BAAI/bge-small-en-v1.5").unwrap());
    }

    /// Two variants share the `nomic-ai/nomic-embed-text-v1.5` code, so the
    /// basename is ambiguous.  It must still resolve, deterministically, to
    /// the unquantised one.
    #[test]
    fn ambiguous_basename_prefers_unquantised() {
        let m = resolve_model("nomic-embed-text-v1.5").unwrap();
        let name = format!("{m:?}");
        assert!(!name.ends_with('Q'), "picked quantised variant: {name}");
        // Stable across calls.
        assert_eq!(resolve_model("nomic-ai/nomic-embed-text-v1.5").unwrap(), m);
    }

    /// The old GGUF syntax must fail with migration guidance, not a bare
    /// "unknown model" — it is what every pre-1.1 config contains.
    #[test]
    fn gguf_uri_reports_the_migration() {
        let err = resolve_model("hf:jsonMartin/voyage-4-nano-gguf:q8_0.gguf")
            .unwrap_err()
            .to_string();
        assert!(err.contains("fastembed"), "unhelpful error: {err}");
        assert!(err.contains("bge-small"), "no replacement suggested: {err}");
    }

    #[test]
    fn local_path_is_rejected_with_an_explanation() {
        let err = resolve_model("/models/thing.onnx").unwrap_err().to_string();
        assert!(err.contains("local model paths"), "unhelpful error: {err}");
    }

    #[test]
    fn near_miss_suggests_candidates() {
        let err = resolve_model("bge-small-en").unwrap_err().to_string();
        assert!(err.contains("Did you mean"), "no suggestions: {err}");
        assert!(err.contains("bge-small-en-v1.5"), "wrong suggestions: {err}");
    }
}
