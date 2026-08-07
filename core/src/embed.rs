/// Auto-embedding engine — wraps `llama-gguf` for on-device embedding.
///
/// This module provides:
/// - [`EmbeddingEngine`]: loads and runs a GGUF embedding model
/// - [`AutoEmbedConfig`]: configuration per source text field
/// - [`EmbeddingPrecision`]: how to quantize the output vectors
/// - [`ModelUri`]: parsing HuggingFace hub identifiers
/// - Quantization/helper functions for int8/uint8/binary
///
/// ## The `embed` feature
///
/// Everything that actually *runs* a model lives behind the `embed` feature,
/// which is on by default.  Building with `--no-default-features` drops the
/// `llama-gguf` dependency (a ~300-crate tree) along with model loading and
/// HuggingFace downloads.
///
/// The configuration types — [`AutoEmbedConfig`], [`EmbeddingPrecision`],
/// [`ModelUri`] — and the quantisation helpers are always compiled, so the
/// rest of the crate needs no `cfg` attributes: only [`EmbeddingEngine`]
/// changes shape, becoming a stub whose constructor returns
/// [`MooFileError::EmbedDisabled`].

use std::path::{Path, PathBuf};
#[cfg(feature = "embed")]
use std::sync::Arc;

#[cfg(feature = "embed")]
use llama_gguf::engine::{Engine, EngineConfig};
#[cfg(feature = "embed")]
use llama_gguf::huggingface::HfClient;

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
// Model URI
// ---------------------------------------------------------------------------

/// A model identifier, either a local path or a HuggingFace hub reference.
#[derive(Debug, Clone)]
pub enum ModelUri {
    /// Local file path on disk
    Local(PathBuf),
    /// HuggingFace hub: (repo_id, optional filename)
    HuggingFace(String, Option<String>),
}

impl ModelUri {
    /// Parse a model URI string.
    ///
    /// Supported formats:
    /// - `"hf:user/repo:filename.gguf"` — HF hub with specific file
    /// - `"hf:user/repo"` — HF hub, default file
    /// - `"/path/to/model.gguf"` — local absolute path
    /// - `"./relative/path.gguf"` — local relative path
    /// - `"model.gguf"` — local relative path
    pub fn parse(uri: &str) -> Self {
        if let Some(hf_rest) = uri.strip_prefix("hf:") {
            let (repo_id, filename) = match hf_rest.split_once(':') {
                Some((repo, file)) => (repo.to_string(), Some(file.to_string())),
                None => (hf_rest.to_string(), None),
            };
            ModelUri::HuggingFace(repo_id, filename)
        } else {
            ModelUri::Local(PathBuf::from(uri))
        }
    }

    /// Resolve to a local file path, downloading from HuggingFace if needed.
    pub fn resolve(&self, _cache_dir: &Path) -> Result<PathBuf, MooFileError> {
        match self {
            ModelUri::Local(path) => {
                if !path.exists() {
                    return Err(MooFileError::ModelNotFound(path.to_path_buf()));
                }
                Ok(path.clone())
            }
            // Downloading needs the HuggingFace client from llama-gguf.
            // A local path still resolves without the feature, so a
            // pre-fetched model can be pointed at directly.
            #[cfg(not(feature = "embed"))]
            ModelUri::HuggingFace(_, _) => Err(MooFileError::EmbedDisabled),

            #[cfg(feature = "embed")]
            ModelUri::HuggingFace(repo_id, filename) => {
                let client = HfClient::default();

                let target_file = match filename {
                    Some(f) => f.clone(),
                    None => {
                        let files = client.list_gguf_files(repo_id)
                            .map_err(|e| MooFileError::DownloadError(
                                format!("failed to list files: {e}")
                            ))?;
                        files.into_iter()
                            .next()
                            .ok_or_else(|| MooFileError::DownloadError(
                                "no .gguf files in repo".into()
                            ))?
                            .filename().to_string()
                    }
                };

                // If already cached, return path directly
                if client.is_cached(repo_id, &target_file) {
                    return Ok(client.get_cached_path(repo_id, &target_file));
                }

                // Download with progress bar
                log::info!("moofile: downloading {repo_id}/{target_file}");
                let path = client.download_file(repo_id, &target_file, true)
                    .map_err(|e| MooFileError::DownloadError(format!("download failed: {e}")))?;

                Ok(path)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AutoEmbed config
// ---------------------------------------------------------------------------

/// Configuration for a single auto-embedding source field.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AutoEmbedConfig {
    /// The embedding model URI (e.g., `hf:user/repo:file.gguf`)
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

impl Default for AutoEmbedConfig {
    fn default() -> Self {
        Self {
            model: String::new(),
            target_field: String::new(),
            dims: 1024,
            precision: EmbeddingPrecision::F32,
            normalize: true,
            query_prefix: "Represent the query for retrieving supporting documents: ".into(),
            doc_prefix: "Represent the document for retrieval: ".into(),
            batch_size: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// Embedding engine
// ---------------------------------------------------------------------------

/// Wraps a `llama-gguf` engine for embedding text.
///
/// This is `Send + Sync` so it can be shared across threads via `Arc`.
#[cfg(feature = "embed")]
#[derive(Clone)]
pub struct EmbeddingEngine {
    inner: Arc<Engine>,
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
    pub fn load(_model_path: &Path) -> Result<Self, MooFileError> {
        Err(MooFileError::EmbedDisabled)
    }

    /// Unreachable: no value of this type can exist without the feature.
    pub fn embed(&self, _text: &str) -> Result<Vec<f32>, MooFileError> {
        match self._never {}
    }

    /// Unreachable: no value of this type can exist without the feature.
    pub fn dims(&self) -> usize {
        match self._never {}
    }
}

#[cfg(feature = "embed")]
impl EmbeddingEngine {
    /// Load an embedding model from a GGUF file.
    pub fn load(model_path: &Path) -> Result<Self, MooFileError> {
        log::info!("moofile: loading embedding model from {}", model_path.display());

        let config = EngineConfig {
            model_path: model_path.to_string_lossy().into_owned(),
            use_gpu: false,
            ..Default::default()
        };

        let engine = Engine::load(config)
            .map_err(|e| MooFileError::EmbeddingError(format!("failed to load model: {e}")))?;

        log::info!("moofile: embedding model loaded successfully");
        Ok(Self {
            inner: Arc::new(engine),
        })
    }

    /// Generate an embedding vector for a single text string.
    pub fn embed(&self, text: &str) -> Result<Vec<f32>, MooFileError> {
        self.inner.embed(text)
            .map_err(|e| MooFileError::EmbeddingError(format!("embedding failed: {e}")))
    }

    /// Get the output dimension of the model.
    pub fn dims(&self) -> usize {
        // We don't have direct access to model_config from the Engine API,
        // but the embedding output size is known from the first embed call.
        // For now, default to asking the engine.
        1024 // default; will be refined
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
