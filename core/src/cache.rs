//! Disposable index snapshot cache.
//!
//! On open, if the cache matches the data file exactly (length + mtime),
//! load the pre-built indexes directly — skipping the BSON scan, decode,
//! tokenisation, stemming, and vector normalisation that a cold rebuild
//! requires.
//!
//! On any mismatch (file modified, cache corrupt, version change, index
//! configuration change) the cache is silently ignored and the normal
//! rebuild runs.
//!
//! The cache is **never** a source of truth.  It is safe to delete at any
//! time.  It can never produce incorrect results because any mismatch
//! triggers a full rebuild.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use bson::Document;

use crate::index::{IndexManager, Value};
use crate::text::TextIndex;

/// Cache format version — bump when the serialised layout changes.
/// Old caches with a different version are silently rejected.
const CACHE_VERSION: u32 = 1;

/// Magic bytes at the start of every cache file, so we can quickly reject
/// non-cache files (e.g. a pickle written by the Python implementation).
const CACHE_MAGIC: [u8; 4] = *b"MOOF";

/// The serialised snapshot of the entire in-memory index state.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct CacheFile {
    /// Magic bytes — must equal `CACHE_MAGIC`.
    magic: [u8; 4],
    /// Cache format version — must equal `CACHE_VERSION`.
    version: u32,
    /// Data file byte length at the time the cache was written.
    data_file_length: u64,
    /// Data file mtime (nanoseconds since UNIX_EPOCH) when cache was written.
    data_file_mtime_ns: u64,
    /// Total record count (live + dead) from the BSON scan.
    total_records: u64,
    /// Index configuration that was used to build this cache.
    regular_fields: Vec<String>,
    vector_fields: Vec<(String, usize)>,
    text_fields: Vec<String>,
    /// The live documents (final, replayed — last-wins already applied).
    /// Stored as raw BSON bytes because `bson::Document` uses
    /// `deserialize_any` which bincode 1.x does not support.
    documents: Vec<(String, Vec<u8>)>,
    /// Regular BTreeMap indexes: field → (value → [_ids]).
    regular: Vec<(String, Vec<(Value, Vec<String>)>)>,
    /// Vector data: field → (ids, flat normalised matrix, dim).
    vector_data: Vec<(String, Vec<String>, Vec<f32>, usize)>,
    /// Text indexes: field → TextIndex.
    text_indexes: Vec<(String, TextIndex)>,
}

/// Return the cache file path for a given data file path.
fn cache_path(data_path: &Path) -> PathBuf {
    // mydata.bson → mydata.bson.cache
    let mut p = data_path.as_os_str().to_owned();
    p.push(".cache");
    PathBuf::from(p)
}

/// Get (length, mtime_ns) for a file.  Returns `None` if metadata can't
/// be read (e.g. file doesn't exist).
fn file_fingerprint(path: &Path) -> Option<(u64, u64)> {
    let meta = fs::metadata(path).ok()?;
    let len = meta.len();
    let mtime_ns = meta
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    Some((len, mtime_ns))
}

/// Result of a cache load attempt.
pub(crate) enum CacheLoad {
    /// Cache hit — the IndexManager has been fully reconstructed.
    Hit { index_manager: IndexManager, total_records: u64 },
    /// Cache miss — caller must do a full rebuild from the BSON file.
    Miss,
}

/// Try to load a cache file for the given data file.
///
/// Returns `CacheLoad::Hit` only if **every** validation check passes:
///   1. Cache file exists and can be read
///   2. Magic bytes match
///   3. Version matches
///   4. Data file length matches
///   5. Data file mtime matches
///   6. Index configuration matches the expected fields
///
/// On any failure, returns `CacheLoad::Miss` — the caller rebuilds normally.
pub(crate) fn try_load_cache(
    data_path: &Path,
    expected_regular: &[String],
    expected_vector: &[(String, usize)],
    expected_text: &[String],
) -> CacheLoad {
    let cache_path = cache_path(data_path);

    // 1. File must exist
    if !cache_path.exists() {
        return CacheLoad::Miss;
    }

    // 2. Read the entire file
    let mut buf = Vec::new();
    {
        let mut f = match fs::File::open(&cache_path) {
            Ok(f) => f,
            Err(_) => return CacheLoad::Miss,
        };
        // Sanity: reject absurdly large cache files
        let size = f.metadata().map(|m| m.len()).unwrap_or(0);
        if size > 16 * 1024 * 1024 * 1024 {
            return CacheLoad::Miss; // >16 GB — something is wrong
        }
        if f.read_to_end(&mut buf).is_err() {
            return CacheLoad::Miss;
        }
    }

    // 3. Deserialize
    let cache: CacheFile = match bincode::deserialize(&buf) {
        Ok(c) => c,
        Err(_) => return CacheLoad::Miss, // corrupt or wrong format (e.g. Python pickle)
    };

    // 4. Validate magic + version
    if cache.magic != CACHE_MAGIC || cache.version != CACHE_VERSION {
        return CacheLoad::Miss;
    }

    // 5. Validate data file fingerprint (length + mtime)
    let (actual_len, actual_mtime) = match file_fingerprint(data_path) {
        Some(fp) => fp,
        None => return CacheLoad::Miss,
    };
    if cache.data_file_length != actual_len || cache.data_file_mtime_ns != actual_mtime {
        return CacheLoad::Miss;
    }

    // 6. Validate index configuration matches what the caller expects
    if !fields_match(&cache.regular_fields, expected_regular)
        || !vector_fields_match(&cache.vector_fields, expected_vector)
        || !fields_match(&cache.text_fields, expected_text)
    {
        return CacheLoad::Miss;
    }

    // --- All checks passed: reconstruct the IndexManager ---
    // Decode documents from raw BSON bytes.
    let documents: std::collections::BTreeMap<String, Arc<Document>> = cache
        .documents
        .into_iter()
        .filter_map(|(k, bytes)| {
            bson::from_slice::<Document>(&bytes).ok().map(|d| (k, Arc::new(d)))
        })
        .collect();

    let regular: std::collections::BTreeMap<String, std::collections::BTreeMap<Value, Vec<String>>> =
        cache
            .regular
            .into_iter()
            .map(|(field, entries)| {
                (
                    field,
                    entries.into_iter().collect::<std::collections::BTreeMap<_, _>>(),
                )
            })
            .collect();

    let vector_data: std::collections::BTreeMap<String, (Vec<String>, Vec<f32>, usize)> =
        cache
            .vector_data
            .into_iter()
            .map(|(field, ids, data, dim)| (field, (ids, data, dim)))
            .collect();

    let text_indexes: std::collections::BTreeMap<String, TextIndex> =
        cache.text_indexes.into_iter().collect();

    let index_manager = IndexManager::from_cache(
        regular,
        cache.regular_fields,
        cache.vector_fields,
        vector_data,
        cache.text_fields,
        text_indexes,
        documents,
    );

    CacheLoad::Hit {
        index_manager,
        total_records: cache.total_records,
    }
}

/// Save a cache file for the given data file.
///
/// Writes to a `.tmp` file first, then atomically renames — safe to
/// interrupt (a partial cache file is simply rejected on next open).
pub(crate) fn save_cache(
    data_path: &Path,
    index_manager: &IndexManager,
    total_records: u64,
) -> Result<(), MooFileError> {
    let cache_path = cache_path(data_path);
    let tmp_path = {
        let mut p = cache_path.clone();
        p.set_extension("cache.tmp");
        p
    };

    let (data_len, data_mtime_ns) = file_fingerprint(data_path).unwrap_or((0, 0));

    // Extract the field lists directly from the IndexManager (not derived
    // from data) so configured-but-empty indexes are preserved.
    let regular_fields = index_manager.regular_fields.clone();
    let vector_fields = index_manager.vector_fields.clone();
    let text_fields = index_manager.text_fields.clone();

    // Flatten BTreeMaps into Vecs for bincode (slightly more compact).
    // Documents are stored as raw BSON bytes (bincode can't handle
    // bson::Document's deserialize_any).
    let documents: Vec<(String, Vec<u8>)> = index_manager
        .documents
        .iter()
        .map(|(k, v)| {
            (k.clone(), bson::to_vec(v.as_ref()).unwrap_or_default())
        })
        .collect();

    let regular: Vec<(String, Vec<(Value, Vec<String>)>)> = index_manager
        .regular
        .iter()
        .map(|(field, map)| {
            (field.clone(), map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        })
        .collect();

    let vector_data: Vec<(String, Vec<String>, Vec<f32>, usize)> = index_manager
        .vector_data
        .iter()
        .map(|(field, (ids, data, dim))| (field.clone(), ids.clone(), data.clone(), *dim))
        .collect();

    let text_indexes: Vec<(String, TextIndex)> = index_manager
        .text_indexes
        .iter()
        .map(|(field, ti)| (field.clone(), ti.clone()))
        .collect();

    let cache = CacheFile {
        magic: CACHE_MAGIC,
        version: CACHE_VERSION,
        data_file_length: data_len,
        data_file_mtime_ns: data_mtime_ns,
        total_records,
        regular_fields,
        vector_fields,
        text_fields,
        documents,
        regular,
        vector_data,
        text_indexes,
    };

    let bytes = bincode::serialize(&cache)
        .map_err(|e| MooFileError::CacheError(format!("serialise: {e}")))?;

    let mut f = fs::File::create(&tmp_path)
        .map_err(|e| crate::errors::io_err(&tmp_path, e))?;
    f.write_all(&bytes)
        .map_err(|e| crate::errors::io_err(&tmp_path, e))?;
    f.flush()
        .map_err(|e| crate::errors::io_err(&tmp_path, e))?;
    drop(f);

    fs::rename(&tmp_path, &cache_path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        crate::errors::io_err(&cache_path, e)
    })?;

    Ok(())
}

/// Delete the cache file if it exists.  Called on compaction (the file is
/// rewritten, so the cache is definitely stale).
pub(crate) fn delete_cache(data_path: &Path) {
    let p = cache_path(data_path);
    let _ = fs::remove_file(&p);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn fields_match(a: &[String], b: &[String]) -> bool {
    let mut a_sorted: Vec<&str> = a.iter().map(|s| s.as_str()).collect();
    let mut b_sorted: Vec<&str> = b.iter().map(|s| s.as_str()).collect();
    a_sorted.sort();
    b_sorted.sort();
    a_sorted == b_sorted
}

fn vector_fields_match(a: &[(String, usize)], b: &[(String, usize)]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_sorted = a.to_vec();
    let mut b_sorted = b.to_vec();
    a_sorted.sort();
    b_sorted.sort();
    a_sorted == b_sorted
}

// ---------------------------------------------------------------------------
// Re-export error
// ---------------------------------------------------------------------------

use crate::errors::MooFileError;

// We need a CacheError variant.  Add it to the error enum in errors.rs.
// If it's not there yet, this will cause a compile error that prompts
// the developer to add it.

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::IndexManager;
    use bson::doc;
    use tempfile::TempDir;

    #[test]
    fn cache_miss_when_no_file() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("test.bson");
        fs::write(&data_path, b"").unwrap();

        match try_load_cache(&data_path, &[], &[], &[]) {
            CacheLoad::Miss => {} // expected
            CacheLoad::Hit { .. } => panic!("should be a miss when no cache file exists"),
        }
    }

    #[test]
    fn cache_roundtrip() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("test.bson");

        // Write a small BSON file
        let doc1 = doc! { "_id": "a", "email": "a@x.com", "age": 30, "content": "hello world" };
        let doc2 = doc! { "_id": "b", "email": "b@x.com", "age": 25, "content": "goodbye world" };
        let mut buf = Vec::new();
        buf.extend(crate::storage::encode_record(crate::storage::RECORD_LIVE, &doc1));
        buf.extend(crate::storage::encode_record(crate::storage::RECORD_LIVE, &doc2));
        fs::write(&data_path, &buf).unwrap();

        // Build an IndexManager as if we just scanned the file
        let mut im = IndexManager::new(
            &["email".into(), "age".into()],
            &[("embedding".into(), 3)],
            &["content".into()],
        );
        im.add(doc1.clone());
        im.add(doc2.clone());
        im.rebuild_vector_indexes();

        // Save the cache
        save_cache(&data_path, &im, 2).unwrap();

        // Load the cache — should hit
        match try_load_cache(
            &data_path,
            &["email".into(), "age".into()],
            &[("embedding".into(), 3)],
            &["content".into()],
        ) {
            CacheLoad::Hit {
                index_manager,
                total_records,
            } => {
                assert_eq!(total_records, 2);
                assert_eq!(index_manager.doc_count(), 2);
                assert!(index_manager.get("a").is_some());
                assert!(index_manager.get("b").is_some());
                // Verify regular index works
                let results = index_manager.lookup_exact_ids(
                    "email",
                    &bson::Bson::String("a@x.com".into()),
                );
                assert_eq!(results, Some(vec!["a".into()]));
            }
            CacheLoad::Miss => panic!("cache should hit after save"),
        }
    }

    #[test]
    fn cache_miss_on_file_modification() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("test.bson");

        let doc1 = doc! { "_id": "a", "email": "a@x.com" };
        let mut buf = Vec::new();
        buf.extend(crate::storage::encode_record(crate::storage::RECORD_LIVE, &doc1));
        fs::write(&data_path, &buf).unwrap();

        let mut im = IndexManager::new(&["email".into()], &[], &[]);
        im.add(doc1);
        save_cache(&data_path, &im, 1).unwrap();

        // Append to the file (simulating a write)
        let doc2 = doc! { "_id": "b", "email": "b@x.com" };
        let extra = crate::storage::encode_record(crate::storage::RECORD_LIVE, &doc2);
        let mut f = fs::OpenOptions::new().append(true).open(&data_path).unwrap();
        use std::io::Write;
        f.write_all(&extra).unwrap();
        drop(f);

        // File length changed → cache should miss
        match try_load_cache(&data_path, &["email".into()], &[], &[]) {
            CacheLoad::Miss => {} // expected
            CacheLoad::Hit { .. } => panic!("cache should miss after file modification"),
        }
    }

    #[test]
    fn cache_miss_on_index_config_change() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("test.bson");

        let doc1 = doc! { "_id": "a", "email": "a@x.com", "age": 30 };
        let mut buf = Vec::new();
        buf.extend(crate::storage::encode_record(crate::storage::RECORD_LIVE, &doc1));
        fs::write(&data_path, &buf).unwrap();

        let mut im = IndexManager::new(&["email".into()], &[], &[]);
        im.add(doc1);
        save_cache(&data_path, &im, 1).unwrap();

        // Try to load with a different index config (added "age")
        match try_load_cache(
            &data_path,
            &["email".into(), "age".into()],
            &[],
            &[],
        ) {
            CacheLoad::Miss => {} // expected
            CacheLoad::Hit { .. } => panic!("cache should miss when index config differs"),
        }
    }

    #[test]
    fn delete_cache_is_safe_when_missing() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("nonexistent.bson");
        delete_cache(&data_path); // should not panic
    }

    #[test]
    fn cache_rejects_non_cache_file() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("test.bson");
        let cache_p = cache_path(&data_path);

        fs::write(&data_path, b"some data").unwrap();
        // Write garbage as cache file
        fs::write(&cache_p, b"this is not a bincode cache file").unwrap();

        match try_load_cache(&data_path, &[], &[], &[]) {
            CacheLoad::Miss => {} // expected
            CacheLoad::Hit { .. } => panic!("should reject non-cache file"),
        }
    }
}