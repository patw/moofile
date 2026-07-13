/// MooFile error types.
///
/// All errors are variants of [`MooFileError`].  There is no
/// `Result` alias — callers should use `Result<T, MooFileError>`.
use std::path::PathBuf;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum MooFileError {
    /// Inserted a document whose `_id` already exists.
    #[error("duplicate _id: {0}")]
    DuplicateKey(String),

    /// `update_one` / `replace_one` found no matching document.
    #[error("no document matches filter")]
    DocumentNotFound,

    /// Attempted a write on a read-only collection.
    #[error("collection is open in read-only mode")]
    ReadOnly,

    /// Underlying I/O error (includes path for context).
    #[error("I/O error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Corrupt or truncated record in the BSON file.
    #[error("corrupt record at byte {offset}: {reason}")]
    CorruptRecord { offset: u64, reason: String },

    /// BSON deserialisation failure.
    #[error("BSON decode error: {0}")]
    BsonDecode(#[from] bson::de::Error),

    /// BSON serialisation failure.
    #[error("BSON encode error: {0}")]
    BsonEncode(#[from] bson::ser::Error),

    /// Meta-file parse failure — if the .meta file is corrupt, delete it
    /// and re-open; the indexes will be rebuilt.
    #[error("meta file corrupt: {0}")]
    MetaCorrupt(String),

    /// Cache file error (serialisation/deserialisation failure).
    /// Non-fatal — the cache is disposable and will be rebuilt.
    #[error("cache error: {0}")]
    CacheError(String),

    /// Another process has the database file open — concurrent multi-process
    /// access is not supported and would silently corrupt the file.
    #[error("concurrent access detected — file is locked by another process: {0}")]
    ConcurrentAccess(PathBuf),

    /// Attempted to start a batch while another batch is already active.
    #[error("a batch is already active — nested batches are not supported")]
    BatchAlreadyActive,
}

/// Convenience: wrap a `std::io::Error` alongside the path that caused it.
pub(crate) fn io_err(path: impl Into<PathBuf>, source: std::io::Error) -> MooFileError {
    MooFileError::Io {
        path: path.into(),
        source,
    }
}
