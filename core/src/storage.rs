/// Append-only BSON file storage engine.
///
/// # File format
///
/// Every record is:
///
/// ```text
/// [4 bytes LE u32: payload length] [1 byte: record type] [BSON payload]
/// ```
///
/// Record types:
///   - `0x01` — live document
///   - `0x02` — tombstone (delete marker)
///   - `0x03` — replacement (updated version of an existing document)
///
/// The file is append-only — documents are never modified in place.
/// Old versions persist as dead bytes until [`compact`] is called.
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use bson::Document;

use crate::errors::{self, MooFileError};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// A live (inserted) document record.
pub(crate) const RECORD_LIVE: u8 = 0x01;
/// A tombstone — marks a document as deleted.
pub(crate) const RECORD_TOMBSTONE: u8 = 0x02;
/// A replacement — an updated version of an existing document.
pub(crate) const RECORD_REPLACEMENT: u8 = 0x03;

/// Header layout: 4-byte LE u32 length + 1-byte type = 5 bytes.
const HEADER_SIZE: usize = 5;

// ---------------------------------------------------------------------------
// Raw record encoding / decoding
// ---------------------------------------------------------------------------

/// A decoded record from the BSON file.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct Record {
    /// Byte offset of this record's header in the file.
    pub offset: u64,
    /// The record type (0x01, 0x02, 0x03).
    pub record_type: u8,
    /// The decoded BSON document.
    pub doc: Document,
}

/// Encode a document and record type into the on-disk wire format.
pub(crate) fn encode_record(record_type: u8, doc: &Document) -> Vec<u8> {
    let payload = bson::to_vec(doc).expect("BSON serialisation is infallible for Document");
    let len = payload.len() as u32;
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.push(record_type);
    buf.extend_from_slice(&payload);
    buf
}

/// Scan a BSON file from start to finish.
///
/// Returns every complete record found and, if the file ends with a partial
/// write, the byte offset where truncation should occur.
pub(crate) fn scan_file(path: &Path) -> Result<(Vec<Record>, Option<u64>), MooFileError> {
    let mut f = File::open(path).map_err(|e| errors::io_err(path, e))?;
    let file_len = f
        .metadata()
        .map_err(|e| errors::io_err(path, e))?
        .len();

    let mut records = Vec::new();
    let mut buf = [0u8; HEADER_SIZE];

    // If the file is empty, return cleanly.
    if file_len == 0 {
        return Ok((records, None));
    }

    loop {
        let offset = f
            .stream_position()
            .map_err(|e| errors::io_err(path, e))?;

        // Read header
        match f.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Partial header at end of file — truncate here.
                return Ok((records, Some(offset)));
            }
            Err(e) => return Err(errors::io_err(path, e)),
        }

        let payload_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let record_type = buf[4];

        // Sanity check: don't allocate gigabytes based on corrupt length
        if payload_len > 100 * 1024 * 1024 {
            return Err(MooFileError::CorruptRecord {
                offset,
                reason: format!("implausible payload length {payload_len} bytes"),
            });
        }

        // Read payload
        let mut payload = vec![0u8; payload_len];
        match f.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Partial payload — truncate here.
                return Ok((records, Some(offset)));
            }
            Err(e) => return Err(errors::io_err(path, e)),
        }

        let doc =
            bson::from_slice(&payload).map_err(|e| MooFileError::CorruptRecord {
                offset,
                reason: format!("BSON decode failed: {e}"),
            })?;

        records.push(Record {
            offset,
            record_type,
            doc,
        });

        // Guard against infinite loops on truncated files
        if offset + HEADER_SIZE as u64 + payload_len as u64 >= file_len {
            break;
        }
    }

    Ok((records, None))
}

/// Rewrite the BSON file keeping only `live_docs`.
///
/// Writes to a `.tmp` file first, then atomically renames — if the process
/// is interrupted the original file is untouched.
pub(crate) fn compact(path: &Path, live_docs: &[Document]) -> Result<(), MooFileError> {
    let tmp_path = path.with_extension("bson.tmp");

    let mut f = File::create(&tmp_path).map_err(|e| errors::io_err(&tmp_path, e))?;

    for doc in live_docs {
        let record = encode_record(RECORD_LIVE, doc);
        f.write_all(&record)
            .map_err(|e| errors::io_err(&tmp_path, e))?;
    }

    f.flush().map_err(|e| errors::io_err(&tmp_path, e))?;
    drop(f);

    fs::rename(&tmp_path, path).map_err(|e| {
        // Best-effort cleanup of the temp file on failure.
        let _ = fs::remove_file(&tmp_path);
        errors::io_err(path, e)
    })
}

/// Truncate a file at the given byte offset.
pub(crate) fn truncate(path: &Path, at: u64) -> Result<(), MooFileError> {
    let f = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|e| errors::io_err(path, e))?;
    f.set_len(at).map_err(|e| errors::io_err(path, e))
}

// ---------------------------------------------------------------------------
// StorageEngine — manages an open file handle for appending
// ---------------------------------------------------------------------------

/// Manages an append-only file handle for writing records.
#[derive(Debug)]
pub(crate) struct StorageEngine {
    path: PathBuf,
    readonly: bool,
    file: Option<File>,
}

impl StorageEngine {
    /// Open (or create) the BSON data file.
    pub fn open(path: &Path, readonly: bool) -> Result<Self, MooFileError> {
        if readonly {
            let file = File::open(path).map_err(|e| errors::io_err(path, e))?;
            Ok(Self {
                path: path.to_path_buf(),
                readonly,
                file: Some(file),
            })
        } else {
            // Create the file if it doesn't exist.
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| errors::io_err(path, e))?;
            Ok(Self {
                path: path.to_path_buf(),
                readonly: false,
                file: Some(file),
            })
        }
    }

    /// Append a record to the file and flush.
    pub fn append(&mut self, record_type: u8, doc: &Document) -> Result<(), MooFileError> {
        if self.readonly {
            return Err(MooFileError::ReadOnly);
        }

        let data = encode_record(record_type, doc);
        let f = self.file.as_mut().expect("StorageEngine: file handle missing");
        f.write_all(&data)
            .map_err(|e| errors::io_err(&self.path, e))?;
        f.flush().map_err(|e| errors::io_err(&self.path, e))?;
        Ok(())
    }

    /// Close the file handle.
    pub fn close(&mut self) {
        self.file = None;
    }

    /// Re-open the file handle (used after compaction replaces the
    /// underlying file).
    pub fn reopen(&mut self) -> Result<(), MooFileError> {
        self.close();
        if self.readonly {
            let f = File::open(&self.path).map_err(|e| errors::io_err(&self.path, e))?;
            self.file = Some(f);
        } else {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(|e| errors::io_err(&self.path, e))?;
            self.file = Some(f);
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use std::io::Write;
    use tempfile::TempDir;

    fn setup_dir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn encode_decode_roundtrip() {
        let doc = doc! { "_id": "abc", "name": "Alice", "age": 30 };
        let encoded = encode_record(RECORD_LIVE, &doc);

        // Header is 5 bytes
        assert!(encoded.len() > HEADER_SIZE);

        let len = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(encoded[4], RECORD_LIVE);
        assert_eq!(len as usize, encoded.len() - HEADER_SIZE);

        // Decode
        let decoded: Document = bson::from_slice(&encoded[HEADER_SIZE..]).unwrap();
        assert_eq!(decoded.get_str("_id").unwrap(), "abc");
        assert_eq!(decoded.get_str("name").unwrap(), "Alice");
        assert_eq!(decoded.get_i32("age").unwrap(), 30);
    }

    #[test]
    fn scan_empty_file() {
        let dir = setup_dir();
        let path = dir.path().join("empty.bson");
        File::create(&path).unwrap();

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert!(records.is_empty());
        assert!(truncate_to.is_none());
    }

    #[test]
    fn scan_single_record() {
        let dir = setup_dir();
        let path = dir.path().join("one.bson");

        let doc = doc! { "_id": "1", "x": 42 };
        let record = encode_record(RECORD_LIVE, &doc);
        std::fs::write(&path, &record).unwrap();

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert!(truncate_to.is_none());
        assert_eq!(records[0].record_type, RECORD_LIVE);
        assert_eq!(records[0].doc.get_str("_id").unwrap(), "1");
        assert_eq!(records[0].doc.get_i32("x").unwrap(), 42);
    }

    #[test]
    fn scan_multiple_records_last_wins() {
        let dir = setup_dir();
        let path = dir.path().join("multi.bson");

        let doc1 = doc! { "_id": "a", "v": 1 };
        let doc2 = doc! { "_id": "b", "v": 2 };
        let doc3 = doc! { "_id": "a", "v": 3 }; // overwrite

        let mut f = File::create(&path).unwrap();
        f.write_all(&encode_record(RECORD_LIVE, &doc1)).unwrap();
        f.write_all(&encode_record(RECORD_LIVE, &doc2)).unwrap();
        f.write_all(&encode_record(RECORD_REPLACEMENT, &doc3)).unwrap();
        f.write_all(&encode_record(RECORD_TOMBSTONE, &doc! {"_id": "b"})).unwrap();
        drop(f);

        let (records, _) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 4);

        // Replay logic (from collection) would see:
        // a→v=1, b→v=2, a→v=3 (overwrite), b→deleted
        // Final: only "a" with v=3 survives.
        // scan_file itself doesn't deduplicate — that's the caller's job.
    }

    #[test]
    fn scan_truncated_header() {
        let dir = setup_dir();
        let path = dir.path().join("trunc.bson");

        let doc = doc! { "_id": "x" };
        let full = encode_record(RECORD_LIVE, &doc);
        // Write only 2 bytes of a second record header
        let mut data = full.clone();
        data.push(0xAB);
        data.push(0xCD);
        std::fs::write(&path, &data).unwrap();

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(truncate_to, Some(full.len() as u64));
    }

    #[test]
    fn scan_truncated_payload() {
        let dir = setup_dir();
        let path = dir.path().join("trunc_payload.bson");

        let doc = doc! { "_id": "x" };
        let full = encode_record(RECORD_LIVE, &doc);
        let mut corrupted = full.clone();
        // Add a header claiming 9999 bytes but only 10 bytes of payload
        corrupted.extend_from_slice(&9999u32.to_le_bytes());
        corrupted.push(RECORD_LIVE);
        corrupted.extend_from_slice(&[0u8; 10]); // short payload
        std::fs::write(&path, &corrupted).unwrap();

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 1);
        // Should detect truncation at the start of the bogus record
        assert_eq!(truncate_to, Some(full.len() as u64));
    }

    #[test]
    fn compact_preserves_only_live() {
        let dir = setup_dir();
        let path = dir.path().join("compact_test.bson");

        // Write some records
        let doc_a = doc! { "_id": "a", "v": 1 };
        let doc_b = doc! { "_id": "b", "v": 2 };

        let mut f = File::create(&path).unwrap();
        f.write_all(&encode_record(RECORD_LIVE, &doc_a)).unwrap();
        f.write_all(&encode_record(RECORD_LIVE, &doc_b)).unwrap();
        drop(f);

        // Compact keeping only doc_a
        let live = vec![doc_a.clone()];
        compact(&path, &live).unwrap();

        let (records, _) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].doc.get_str("_id").unwrap(), "a");
        assert_eq!(records[0].record_type, RECORD_LIVE);
    }

    #[test]
    fn storage_engine_append_and_reopen() {
        let dir = setup_dir();
        let path = dir.path().join("engine.bson");

        let mut engine = StorageEngine::open(&path, false).unwrap();
        let doc = doc! { "_id": "1", "hello": "world" };
        engine.append(RECORD_LIVE, &doc).unwrap();

        // Close and reopen
        engine.close();
        engine.reopen().unwrap();

        // Append another
        let doc2 = doc! { "_id": "2" };
        engine.append(RECORD_LIVE, &doc2).unwrap();
        engine.close();

        // Verify both records on disk
        let (records, _) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn readonly_engine_rejects_writes() {
        let dir = setup_dir();
        let path = dir.path().join("ro.bson");
        File::create(&path).unwrap();

        let mut engine = StorageEngine::open(&path, true).unwrap();
        let result = engine.append(RECORD_LIVE, &doc! {"_id": "x"});
        assert!(result.is_err());
        match result.unwrap_err() {
            MooFileError::ReadOnly => {}
            other => panic!("expected ReadOnly, got {other:?}"),
        }
    }
}
