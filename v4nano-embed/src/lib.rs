//! `v4nano-embed` — a deliberately narrow ONNX runner for **one** model:
//! `voyageai/voyage-4-nano` (via the `onnx-community/voyage-4-nano-ONNX`
//! int8 export).
//!
//! This crate exists because moofile's autoembedding engine (fastembed) only
//! knows the models compiled into its registry, and adding a model there means
//! forking fastembed. Instead we extracted the ~100 lines of glue fastembed
//! uses to run an ONNX text-embedding model and hardcoded the rest.
//!
//! There is no registry, no downloader, no batching strategy selection — just
//! "load this model, mean-pool its `last_hidden_state`, L2-normalize". The
//! tokenizer glue and pooling/normalization math are adapted from
//! [`fastembed`](https://github.com/Anush008/fastembed-rs) (Apache-2.0).
//!
//! # Usage
//! ```no_run
//! use v4nano_embed::V4Nano;
//! let mut m = V4Nano::load("model_quantized.onnx", "tokenizer.json", 32768, None)?;
//! let v = m.embed(&["hello world".to_string()])?;
//! assert_eq!(v[0].len(), v4nano_embed::DIM);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::path::Path;

use ort::session::builder::GraphOptimizationLevel;
use ort::session::Session;
use ort::value::Tensor;
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

/// Embedding dimensionality. voyage-4-nano's default (and the ONNX export's)
/// output width. MRL lets callers truncate to 1024/512/256 themselves.
pub const DIM: usize = 2048;

/// Pad token id for the Qwen3 tokenizer this model uses (`<|endoftext|>`).
/// The onnx-community export's `config.json` has `pad_token_id: null`, so we
/// hardcode the correct value instead of trusting the (absent) config.
const PAD_ID: u32 = 151643;
const PAD_TOKEN: &str = "<|endoftext|>";

/// The only ONNX output we care about. The export also emits a fused
/// `pooler_output`, but mean-pooling `last_hidden_state` ourselves matches the
/// official model card exactly.
const OUTPUT_NAME: &str = "last_hidden_state";

/// A loaded voyage-4-nano embedding model.
pub struct V4Nano {
    session: Session,
    tokenizer: Tokenizer,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tokenizer error: {0}")]
    Tokenizer(#[from] tokenizers::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("onnx runtime error: {0}")]
    Ort(String),
    #[error("model has no '{OUTPUT_NAME}' output")]
    MissingOutput,
    #[error("empty input batch")]
    EmptyBatch,
}

impl From<ort::Error> for Error {
    fn from(e: ort::Error) -> Self {
        Error::Ort(e.to_string())
    }
}

/// Helper so `?` works on the generic `ort::Error<Builder>` variants too.
fn ort_err(e: impl std::fmt::Display) -> Error {
    Error::Ort(e.to_string())
}

impl V4Nano {
    /// Load the model from an ONNX file and a `tokenizer.json`.
    ///
    /// `max_length` caps tokenizer truncation (pass 32768 for the model's full
    /// context). `intra_threads` maps to ONNX Runtime's intra-op thread count;
    /// `None` = all cores.
    pub fn load(
        model_path: impl AsRef<Path>,
        tokenizer_path: impl AsRef<Path>,
        max_length: usize,
        intra_threads: Option<usize>,
    ) -> Result<Self, Error> {
        let threads = match intra_threads {
            Some(n) => n,
            None => std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
        };

        let session = Session::builder()
            .map_err(ort_err)?
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(ort_err)?
            .with_intra_threads(threads)
            .map_err(ort_err)?
            .commit_from_file(model_path)
            .map_err(ort_err)?;

        let mut tokenizer = Tokenizer::from_file(tokenizer_path)?;
        // Batch-longest padding with the correct Qwen3 pad token. Left/right
        // doesn't matter for mean pooling, but the id must be real.
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            pad_token: PAD_TOKEN.into(),
            pad_id: PAD_ID,
            ..Default::default()
        }));
        tokenizer.with_truncation(Some(TruncationParams {
            max_length,
            ..Default::default()
        }))?;

        Ok(Self { session, tokenizer })
    }

    /// Embed a batch of texts. Returns one 2048-dim L2-normalized vector per
    /// input, in order. Takes `&mut self` because ONNX Runtime sessions are
    /// not `Sync`.
    pub fn embed(&mut self, texts: &[String]) -> Result<Vec<Vec<f32>>, Error> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let encodings = self
            .tokenizer
            .encode_batch(texts.iter().map(|s| s.as_str()).collect(), true)?;

        let batch = encodings.len();
        let seq = encodings[0].len();

        let mut ids = vec![0i64; batch * seq];
        let mut mask = vec![0i64; batch * seq];
        for (b, enc) in encodings.iter().enumerate() {
            let eids = enc.get_ids();
            let emask = enc.get_attention_mask();
            for t in 0..seq {
                ids[b * seq + t] = eids[t] as i64;
                mask[b * seq + t] = emask[t] as i64;
            }
        }

        let input_ids = Tensor::from_array(([batch, seq], ids))?;
        let attention_mask = Tensor::from_array(([batch, seq], mask.clone()))?;

        let outputs = self.session.run(ort::inputs![
            "input_ids" => input_ids,
            "attention_mask" => attention_mask,
        ])?;

        let lh = outputs.get(OUTPUT_NAME).ok_or(Error::MissingOutput)?;
        let (shape, data) = lh.try_extract_tensor::<f32>()?;

        let b = shape[0] as usize;
        let t = shape[1] as usize;
        let h = shape[2] as usize;
        debug_assert_eq!(h, DIM);

        // Mean pool over real tokens, then L2-normalize.
        let mut out = vec![0f32; b * h];
        for bi in 0..b {
            let mut count = 0f32;
            for ti in 0..t {
                if mask[bi * t + ti] != 0 {
                    for hi in 0..h {
                        out[bi * h + hi] += data[(bi * t + ti) * h + hi];
                    }
                    count += 1.0;
                }
            }
            if count == 0.0 {
                count = 1.0;
            }
            let mut norm = 0f32;
            for hi in 0..h {
                let v = out[bi * h + hi] / count;
                out[bi * h + hi] = v;
                norm += v * v;
            }
            norm = norm.sqrt().max(1e-12);
            for hi in 0..h {
                out[bi * h + hi] /= norm;
            }
        }

        Ok((0..b).map(|bi| out[bi * h..(bi + 1) * h].to_vec()).collect())
    }
}
