//! Disposable index snapshot cache.
//!
//! On open, if the cache matches the data file exactly (inode + length +
//! mtime), load the pre-built indexes directly — skipping the BSON decode,
//! tokenisation, stemming, and vector normalisation that a cold rebuild
//! requires.
//!
//! The snapshot holds the indexes and the row → `_id` assignment, **not the
//! documents**: those are already in the data file, and a hit reads them
//! back with one sequential raw pass (`storage::read_live_payloads`).
//! Embedding a second copy of every document made the cache 2.7× the size of
//! the data it described, and most of a cached open was spent deserialising
//! it.
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

use crate::index::{IndexManager, Postings, Row, Value, VectorMatrix};
use crate::text::TextIndex;

/// Cache format version — bump when the serialised layout **or the meaning of
/// its contents** changes.  Old caches with a different version are silently
/// rejected and rebuilt from the documents.
///   1 -> 2: `index::Value` gained DateTime/ObjectId variants, which changes
///           the bincode discriminants of every indexed value.
///   2 -> 3: the text analyzer changed — digits are no longer discarded and
///           array fields are indexed instead of skipped.  The layout is
///           identical, so a stale cache would load happily and keep serving
///           the old token set; only the version bump forces the rebuild that
///           makes the new analyzer take effect.
///   3 -> 4: the fingerprint gained the data file's inode.
///   4 -> 5: indexes refer to documents by row; documents are no longer
///           stored (they are read back from the data file).
const CACHE_VERSION: u32 = 5;

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
    /// Data file inode when the cache was written.  Length and mtime alone
    /// cannot see a `compact()`: it renames a freshly written file over the
    /// path, and rewriting the same live set at the same moment reproduces
    /// the same length — not by coincidence but by construction, since that
    /// is exactly what compaction does when there is nothing dead to drop.
    /// The inode is the only part of the fingerprint that always changes when
    /// the file is replaced.
    ///
    /// **0 means "this platform did not supply one"**, which off Unix is
    /// always: `std::os::windows::fs::MetadataExt` has no stable accessor for
    /// the NTFS file index, so a Windows build stamps and compares 0 and the
    /// check is inert — the same limitation `catch_up` has carried since
    /// v0.5.2, where a replaced file is caught only if it also shrank.  The
    /// pure-Python backend *is* live there, because `os.stat` populates
    /// `st_ino` on Windows.
    ///
    /// A strict `!=` is still the right comparison across that asymmetry: a
    /// known value meeting an unknown one is a miss, and a miss only ever
    /// costs a rebuild.
    data_file_ino: u64,
    /// Total record count (live + dead) from the BSON scan.
    total_records: u64,
    /// Index configuration that was used to build this cache.
    regular_fields: Vec<String>,
    vector_fields: Vec<(String, usize)>,
    text_fields: Vec<String>,
    /// Row → `_id` (`None` for a free slot).  The documents themselves are
    /// read back from the data file on load.
    rows: Vec<Option<String>>,
    /// Regular BTreeMap indexes: field → (value → [rows]).
    regular: Vec<(String, Vec<(Value, Vec<Row>)>)>,
    /// Vector data: field → (rows, flat normalised matrix, dim).
    vector_data: Vec<(String, Vec<Row>, Vec<f32>, usize)>,
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

/// Get (length, mtime_ns, inode) for a file.  Returns `None` if metadata
/// can't be read (e.g. file doesn't exist).
///
/// The inode is 0 where the platform does not supply one — see
/// `CacheFile::data_file_ino`.
fn file_fingerprint(path: &Path) -> Option<(u64, u64, u64)> {
    let meta = fs::metadata(path).ok()?;
    let len = meta.len();
    let mtime_ns = meta
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    #[cfg(unix)]
    let ino = {
        use std::os::unix::fs::MetadataExt;
        meta.ino()
    };
    #[cfg(not(unix))]
    let ino = 0u64;
    Some((len, mtime_ns, ino))
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
///   6. Data file inode matches
///   7. Index configuration matches the expected fields
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

    // 5. Validate data file fingerprint (length + mtime + inode)
    let (actual_len, actual_mtime, actual_ino) = match file_fingerprint(data_path) {
        Some(fp) => fp,
        None => return CacheLoad::Miss,
    };
    if cache.data_file_length != actual_len
        || cache.data_file_mtime_ns != actual_mtime
        || cache.data_file_ino != actual_ino
    {
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
    // Documents come back from the data file, which the fingerprint has
    // just confirmed is byte-for-byte the one this snapshot describes.  The
    // row map must account for every live document exactly; anything else
    // is a miss and a normal rebuild.
    let Some(mut live) = crate::storage::read_live_payloads(data_path, cache.data_file_length) else {
        return CacheLoad::Miss;
    };
    let mut docs = Vec::with_capacity(cache.rows.len());
    for slot in cache.rows {
        docs.push(match slot {
            None => None,
            Some(id) => match live.remove(&id).map(bson::raw::RawDocumentBuf::from_bytes) {
                Some(Ok(raw)) => Some(Arc::new(raw)),
                _ => return CacheLoad::Miss,
            },
        });
    }
    if !live.is_empty() {
        return CacheLoad::Miss;
    }

    let regular: std::collections::BTreeMap<String, std::collections::BTreeMap<Value, Postings>> = cache
        .regular
        .into_iter()
        .map(|(field, entries)| {
            (
                field,
                entries
                    .into_iter()
                    .filter_map(|(v, rows)| Some((v, Postings::from_rows(rows)?)))
                    .collect::<std::collections::BTreeMap<_, _>>(),
            )
        })
        .collect();

    let vector_data: std::collections::BTreeMap<String, VectorMatrix> = cache
        .vector_data
        .into_iter()
        .map(|(field, rows, data, dim)| (field, VectorMatrix::from_parts(rows, data, dim)))
        .collect();

    let text_indexes: std::collections::BTreeMap<String, TextIndex> =
        cache.text_indexes.into_iter().collect();

    let Some(index_manager) = IndexManager::from_cache(
        regular,
        cache.regular_fields,
        cache.vector_fields,
        vector_data,
        cache.text_fields,
        text_indexes,
        docs,
    ) else {
        return CacheLoad::Miss;
    };

    CacheLoad::Hit {
        index_manager,
        total_records: cache.total_records,
    }
}

/// Save a cache file for the given data file.
///
/// Writes to a `.tmp` file first, then atomically renames — safe to
/// interrupt (a partial cache file is simply rejected on next open).
/// `basis` is the (length, mtime_ns, inode) of the data file that
/// `index_manager` actually describes — **not** the file's current state.
/// With multiple processes those differ: another writer may have appended, or
/// compacted, since this handle last read.  Stamping the current state would
/// produce a cache that passes validation while silently omitting the other
/// writer's records; stamping the basis makes the next open see a mismatch
/// and rescan instead.
pub(crate) fn save_cache(
    data_path: &Path,
    index_manager: &IndexManager,
    total_records: u64,
    basis: (u64, u64, u64),
) -> Result<(), MooFileError> {
    let cache_path = cache_path(data_path);
    let tmp_path = {
        let mut p = cache_path.clone();
        p.set_extension("cache.tmp");
        p
    };

    let (data_len, data_mtime_ns, data_ino) = basis;

    // Extract the field lists directly from the IndexManager (not derived
    // from data) so configured-but-empty indexes are preserved.
    let regular_fields = index_manager.regular_fields.clone();
    let vector_fields = index_manager.vector_fields.clone();
    let text_fields = index_manager.text_fields.clone();

    // Flatten BTreeMaps into Vecs for bincode (slightly more compact).
    let rows = index_manager.row_ids();

    let regular: Vec<(String, Vec<(Value, Vec<Row>)>)> = index_manager
        .regular
        .iter()
        .map(|(field, map)| {
            (
                field.clone(),
                map.iter()
                    .map(|(k, p)| {
                        let mut rows = Vec::with_capacity(p.len());
                        p.extend_into(&mut rows);
                        (k.clone(), rows)
                    })
                    .collect(),
            )
        })
        .collect();

    let vector_data: Vec<(String, Vec<Row>, Vec<f32>, usize)> = index_manager
        .vector_data
        .iter()
        .map(|(field, m)| (field.clone(), m.rows.clone(), m.data.clone(), m.dim))
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
        data_file_ino: data_ino,
        total_records,
        regular_fields,
        vector_fields,
        text_fields,
        rows,
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

    /// The inode is the only part of the fingerprint that always changes when
    /// `compact()` renames a new file over the path.  Length matches by
    /// construction when compaction has nothing dead to drop, and mtime can
    /// match too — here both are forced to match so the inode is the only
    /// thing left to catch it.
    #[test]
    fn cache_miss_when_the_file_was_replaced_at_the_same_length_and_mtime() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("test.bson");

        let original = doc! { "_id": "a", "v": 1 };
        let replacement = doc! { "_id": "b", "v": 2 };
        let encode = |d: &bson::Document| {
            crate::storage::encode_record(crate::storage::RECORD_LIVE, d)
        };
        let original_bytes = encode(&original);
        let replacement_bytes = encode(&replacement);
        assert_eq!(
            original_bytes.len(),
            replacement_bytes.len(),
            "the two documents must encode to the same length for this test to mean anything"
        );

        fs::write(&data_path, &original_bytes).unwrap();
        let fp = file_fingerprint(&data_path).unwrap();

        let mut im = IndexManager::new(&["v".into()], &[], &[]);
        im.add(original.clone());
        save_cache(&data_path, &im, 1, fp).unwrap();
        assert!(
            matches!(try_load_cache(&data_path, &["v".into()], &[], &[]), CacheLoad::Hit { .. }),
            "the cache must hit before the file is replaced"
        );

        // Replace via rename, the way compact() does — new inode, same length.
        let tmp = dir.path().join("test.bson.tmp");
        fs::write(&tmp, &replacement_bytes).unwrap();
        fs::rename(&tmp, &data_path).unwrap();

        // Force the mtime back so length and mtime both match the stamp, and
        // the inode is the only difference left.
        let mtime = std::time::UNIX_EPOCH + std::time::Duration::from_nanos(fp.1);
        let f = fs::OpenOptions::new().write(true).open(&data_path).unwrap();
        f.set_times(fs::FileTimes::new().set_modified(mtime)).unwrap();
        drop(f);

        let after = file_fingerprint(&data_path).unwrap();
        assert_eq!(after.0, fp.0, "length must match for the test to be meaningful");
        assert_eq!(after.1, fp.1, "mtime must match for the test to be meaningful");
        assert_ne!(after.2, fp.2, "the rename must have changed the inode");

        match try_load_cache(&data_path, &["v".into()], &[], &[]) {
            CacheLoad::Miss => {} // expected — the inode caught it
            CacheLoad::Hit { .. } => {
                panic!("a replaced file must not serve the old file's cached index")
            }
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
        save_cache(&data_path, &im, 2, file_fingerprint(&data_path).unwrap()).unwrap();

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
                let rows = index_manager
                    .lookup_exact_rows("email", &bson::Bson::String("a@x.com".into()))
                    .unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(index_manager.id_of(rows[0]), Some("a"));
            }
            CacheLoad::Miss => panic!("cache should hit after save"),
        }
    }

    /// Free slots and replaced documents must round-trip: the cache keeps
    /// rows, and reads each row's current document back from the log.
    #[test]
    fn cache_roundtrip_with_free_slots_and_replacements() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("test.bson");
        let a1 = doc! { "_id": "a", "age": 1 };
        let b = doc! { "_id": "b", "age": 2 };
        let a2 = doc! { "_id": "a", "age": 3 };
        let c = doc! { "_id": "c", "age": 4 };
        let mut buf = Vec::new();
        use crate::storage::{encode_record, RECORD_LIVE, RECORD_REPLACEMENT, RECORD_TOMBSTONE};
        buf.extend(encode_record(RECORD_LIVE, &a1));
        buf.extend(encode_record(RECORD_LIVE, &b));
        buf.extend(encode_record(RECORD_REPLACEMENT, &a2));
        buf.extend(encode_record(RECORD_TOMBSTONE, &doc! { "_id": "b" }));
        buf.extend(encode_record(RECORD_LIVE, &c));
        buf.extend(encode_record(RECORD_TOMBSTONE, &doc! { "_id": "c" }));
        fs::write(&data_path, &buf).unwrap();

        let mut im = IndexManager::new(&["age".into()], &[], &[]);
        im.add(a1);
        im.add(b);
        im.remove("a");
        im.add(a2);
        im.remove("b");
        im.add(c);
        im.remove("c");
        save_cache(&data_path, &im, 6, file_fingerprint(&data_path).unwrap()).unwrap();

        match try_load_cache(&data_path, &["age".into()], &[], &[]) {
            CacheLoad::Hit { index_manager, .. } => {
                assert_eq!(index_manager.doc_count(), 1);
                assert_eq!(index_manager.get("a").unwrap().get_i32("age").unwrap(), 3);
                let rows = index_manager.lookup_exact_rows("age", &bson::Bson::Int32(3)).unwrap();
                assert_eq!(index_manager.id_of(rows[0]), Some("a"));
                assert!(index_manager.lookup_exact_rows("age", &bson::Bson::Int32(1)).unwrap().is_empty());
            }
            CacheLoad::Miss => panic!("cache should hit"),
        }
    }

    /// A snapshot whose rows disagree with the log's live set is a miss,
    /// never a partial load.
    #[test]
    fn cache_miss_when_rows_and_log_disagree() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("test.bson");
        let mut buf = Vec::new();
        buf.extend(crate::storage::encode_record(crate::storage::RECORD_LIVE, &doc! { "_id": "a" }));
        buf.extend(crate::storage::encode_record(crate::storage::RECORD_LIVE, &doc! { "_id": "b" }));
        fs::write(&data_path, &buf).unwrap();

        let mut im = IndexManager::new(&[], &[], &[]);
        im.add(doc! { "_id": "a" });
        save_cache(&data_path, &im, 2, file_fingerprint(&data_path).unwrap()).unwrap();
        assert!(matches!(try_load_cache(&data_path, &[], &[], &[]), CacheLoad::Miss));
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
        save_cache(&data_path, &im, 1, file_fingerprint(&data_path).unwrap()).unwrap();

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
        save_cache(&data_path, &im, 1, file_fingerprint(&data_path).unwrap()).unwrap();

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