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

use bson::{Bson, Document};

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

/// Smallest possible BSON document: a 4-byte length prefix and the 0x00
/// terminator.  A record header claiming less than this cannot describe a
/// document, whatever else is going on.
const MIN_BSON_DOC: usize = 5;

/// Cap on a record's payload — a header claiming more than this is corrupt
/// rather than merely large, so the scanner refuses to honour it.
///
/// It is therefore also the largest document that can be *written*: see
/// [`StorageEngine::append_bytes`].
pub const MAX_DOCUMENT_SIZE: usize = 100 * 1024 * 1024;

/// Internal alias kept for the scan paths, which read as "payload" rather
/// than "document".
const MAX_PAYLOAD: usize = MAX_DOCUMENT_SIZE;

/// Largest BSON binary value the encoder will produce.
///
/// This is the `bson` crate's own cap, not moofile's, and it is *lower* than
/// [`MAX_DOCUMENT_SIZE`] — a document carrying a bigger binary field fails to
/// encode even though the document as a whole would fit in a record.
pub const MAX_BINARY_SIZE: usize = 16 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Durability
// ---------------------------------------------------------------------------

/// Write durability level.
///
/// - [`Durability::None`] — no flush at all; data sits in the userspace
///   buffer.  Fastest, but a process crash can lose buffered writes.
/// - [`Durability::Os`] — flush to the OS page cache (the default).
///   Survives process crashes but **not** power loss.
/// - [`Durability::Fsync`] — `sync_all()` after every write.  Durable
///   across power loss, but significantly slower for per-document inserts.
#[derive(Clone, Copy, Debug)]
pub enum Durability {
    None,
    Os,
    Fsync,
}

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
///
/// # Panics
///
/// If `doc` cannot be BSON-encoded — in practice, a binary value over
/// [`MAX_BINARY_SIZE`].  Production write paths go through [`encode_doc`],
/// which returns an error instead; this is for tests, whose inputs have
/// already round-tripped through the file.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn encode_record(record_type: u8, doc: &Document) -> Vec<u8> {
    let payload = bson::to_vec(doc).expect("document came from the file, so it re-encodes");
    encode_record_bytes(record_type, &payload)
}

/// As `encode_record`, but for a payload that's already encoded — the
/// insert hot path builds the BSON bytes once (to write to disk) and reuses
/// the same bytes to build the in-memory raw-document index entry, instead
/// of encoding twice.
pub(crate) fn encode_record_bytes(record_type: u8, payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.push(record_type);
    buf.extend_from_slice(payload);
    buf
}

/// Scan a BSON file from start to finish.
///
/// Returns every complete record found and, if the file ends with a partial
/// write, the byte offset where truncation should occur.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn scan_file(path: &Path) -> Result<(Vec<Record>, Option<u64>), MooFileError> {
    scan_from(path, 0)
}

/// Scan a BSON file starting at `start` bytes, collecting every record into
/// a `Vec`.
///
/// The file format is append-only, so records written by another process
/// after this handle last read are always a contiguous suffix.  That lets a
/// handle catch up on someone else's writes in O(new bytes) instead of
/// re-reading and re-indexing the whole file.
///
/// `start` must be a record boundary — pass an offset this handle has
/// previously scanned up to.
///
/// This is a thin `Vec`-collecting wrapper around [`scan_from_streaming`]
/// kept for tests and callers that genuinely want the whole batch at once.
/// Hot paths that replay records straight into an index (open, catch-up,
/// reindex) should call `scan_from_streaming` directly — collecting into a
/// `Vec` first means every record briefly exists twice: once in the `Vec`,
/// once in the index it's replayed into.  On a large collection that
/// doubles the peak memory of a load for no benefit.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn scan_from(
    path: &Path,
    start: u64,
) -> Result<(Vec<Record>, Option<u64>), MooFileError> {
    let mut records = Vec::new();
    let truncate_to = scan_from_streaming(path, start, |record| records.push(record))?;
    Ok((records, truncate_to))
}

/// Decide whether unparseable bytes at `offset` are the file's tail or a
/// damaged record with intact data after it.
///
/// The scanner cannot simply assume "unparseable ⇒ tail".  Doing so means a
/// single clobbered block mid-file silently truncates everything after it on
/// the next open — the append-only format gives no framing to resynchronise
/// on, so there is no cheap way to tell the two apart from the header alone.
/// Looking ahead for an intact record is that way: if one is found the damage
/// is interior and must be reported (`repair` can then salvage the rest), and
/// if none is found the file really does end here and truncating loses
/// nothing that was readable anyway.
///
/// Only ever called once a scan has already failed, so the window read costs
/// nothing on the healthy path.
fn damage_is_tail(
    f: &mut File,
    path: &Path,
    offset: u64,
    file_len: u64,
) -> Result<bool, MooFileError> {
    let window_len = ((file_len - offset) as usize).min(RESYNC_WINDOW);
    let window_is_eof = (offset + window_len as u64) == file_len;

    let mut window = vec![0u8; window_len];
    f.seek(std::io::SeekFrom::Start(offset))
        .map_err(|e| errors::io_err(path, e))?;
    f.read_exact(&mut window)
        .map_err(|e| errors::io_err(path, e))?;

    Ok(resync(&window, 1, window_is_eof).is_none())
}

/// Scan a BSON file starting at `start` bytes, invoking `on_record` for each
/// complete record as it's decoded instead of buffering them.
///
/// Returns the byte offset to truncate at if the file ends with a partial
/// write, or `None` if it ends cleanly. See [`scan_from`] for the semantics
/// of `start`.
pub(crate) fn scan_from_streaming<F>(
    path: &Path,
    start: u64,
    mut on_record: F,
) -> Result<Option<u64>, MooFileError>
where
    F: FnMut(Record),
{
    let mut f = File::open(path).map_err(|e| errors::io_err(path, e))?;
    let file_len = f
        .metadata()
        .map_err(|e| errors::io_err(path, e))?
        .len();

    let mut buf = [0u8; HEADER_SIZE];

    // Nothing new (or an empty file) — return cleanly.
    if file_len == 0 || start >= file_len {
        return Ok(None);
    }

    if start > 0 {
        f.seek(std::io::SeekFrom::Start(start))
            .map_err(|e| errors::io_err(path, e))?;
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
                return Ok(Some(offset));
            }
            Err(e) => return Err(errors::io_err(path, e)),
        }

        let payload_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let record_type = buf[4];

        // Two ways a header can be unusable on its face:
        //
        //   * it claims a payload too short to be a BSON document (the
        //     minimum is 5 bytes).  In practice this is an all-zero header,
        //     and the way a file grows an all-zero region is an interrupted
        //     write on a filesystem with delayed allocation: the inode's new
        //     size reaches the journal but the data blocks never reach the
        //     disk, so they read back as zeros rather than as a short file.
        //     This used to fall through to the decode below and raise
        //     `CorruptRecord` from every entry point — including `open`,
        //     which could then never reach its own truncation path and so
        //     failed identically on every restart, forever.
        //
        //   * it claims a payload larger than any real record, so large that
        //     honouring it would mean a wild allocation.
        //
        // Either is a truncation point *if it is the tail*, and genuine
        // corruption if intact records follow it — see `damage_is_tail`.
        if payload_len < MIN_BSON_DOC || payload_len > MAX_PAYLOAD {
            if damage_is_tail(&mut f, path, offset, file_len)? {
                return Ok(Some(offset));
            }
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
                return Ok(Some(offset));
            }
            Err(e) => return Err(errors::io_err(path, e)),
        }

        // A payload that will not decode gets the same tail-or-corruption
        // question as a bad header.  It has to: an interrupted write whose
        // lost blocks start partway through a record leaves a plausible
        // header over half-real, half-zero bytes, which lands here rather
        // than in the check above.  Only the record boundary the zeros
        // happened to fall on decides which of the two paths a given crash
        // takes, and both have to self-heal.
        let doc = match bson::from_slice(&payload) {
            Ok(doc) => doc,
            Err(e) => {
                if damage_is_tail(&mut f, path, offset, file_len)? {
                    return Ok(Some(offset));
                }
                return Err(MooFileError::CorruptRecord {
                    offset,
                    reason: format!("BSON decode failed: {e}"),
                });
            }
        };

        // Compute before `on_record` consumes the record.
        let at_end = offset + HEADER_SIZE as u64 + payload_len as u64 >= file_len;

        on_record(Record {
            offset,
            record_type,
            doc,
        });

        // Guard against infinite loops on truncated files
        if at_end {
            break;
        }
    }

    Ok(None)
}

/// Rewrite the BSON file keeping only `live_docs`.
///
/// Writes to a `.tmp` file first, then atomically renames — if the process
/// is interrupted the original file is untouched.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn compact(path: &Path, live_docs: &[Document]) -> Result<(), MooFileError> {
    let encoded: Vec<Vec<u8>> = live_docs
        .iter()
        .map(|d| bson::to_vec(d).expect("document came from the file, so it re-encodes"))
        .collect();
    compact_raw_bytes(path, encoded.iter().map(|v| v.as_slice()))
}

/// As `compact`, but for documents whose encoded bytes are already in hand
/// (the raw-document index stores exactly these bytes) — no decode, no
/// re-encode, just a straight byte copy into the new file.
pub(crate) fn compact_raw<'a>(
    path: &Path,
    live_docs: impl Iterator<Item = &'a [u8]>,
) -> Result<(), MooFileError> {
    compact_raw_bytes(path, live_docs)
}

fn compact_raw_bytes<'a>(
    path: &Path,
    live_docs: impl Iterator<Item = &'a [u8]>,
) -> Result<(), MooFileError> {
    let tmp_path = path.with_extension("bson.tmp");

    let mut f = File::create(&tmp_path).map_err(|e| errors::io_err(&tmp_path, e))?;

    for payload in live_docs {
        let record = encode_record_bytes(RECORD_LIVE, payload);
        f.write_all(&record)
            .map_err(|e| errors::io_err(&tmp_path, e))?;
    }

    f.flush().map_err(|e| errors::io_err(&tmp_path, e))?;
    // fsync the tmp file so its contents are durable on disk before the
    // rename.  Without this, a power loss after the rename could leave
    // the new file pointing to unallocated/zeroed blocks.
    f.sync_all().map_err(|e| errors::io_err(&tmp_path, e))?;
    drop(f);

    fs::rename(&tmp_path, path).map_err(|e| {
        // Best-effort cleanup of the temp file on failure.
        let _ = fs::remove_file(&tmp_path);
        errors::io_err(path, e)
    })?;

    // fsync the parent directory so the rename (a directory entry update)
    // is durable across power loss.  Without this, a power loss could
    // result in the old file being gone but the new name not yet visible.
    let parent = path.parent().unwrap_or(Path::new("."));
    if let Ok(parent_file) = File::open(parent) {
        let _ = parent_file.sync_all();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Repair
// ---------------------------------------------------------------------------

/// How far past a damaged record `repair` will look for the next intact one
/// before giving up and treating the rest of the file as lost.
///
/// Damage that comes from a filesystem losing blocks spans an extent or two;
/// a megabyte is far beyond that while keeping the resynchronisation search
/// bounded in both time and memory.
const RESYNC_WINDOW: usize = 1024 * 1024;

/// A span of bytes that [`repair_file`] could not parse and dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairGap {
    /// Byte offset where the damage starts.
    pub offset: u64,
    /// Number of bytes dropped.
    pub length: u64,
    /// `true` if the damage ran to the end of the file — i.e. this was a
    /// truncation rather than a skipped-over hole.
    pub to_end_of_file: bool,
}

/// What a [`repair_file`] pass did.
#[derive(Debug, Clone, Default)]
pub struct RepairReport {
    /// Records that decoded and were preserved.
    pub records_kept: u64,
    /// Bytes of intact records preserved.
    pub bytes_kept: u64,
    /// Bytes of unparseable data dropped.
    pub bytes_dropped: u64,
    /// Every damaged span, in file order.
    pub gaps: Vec<RepairGap>,
    /// `false` when the file was already intact and was left untouched.
    pub rewritten: bool,
}

impl RepairReport {
    /// Whether any damage was found.
    pub fn is_damaged(&self) -> bool {
        !self.gaps.is_empty()
    }
}

/// Parse a record at `pos` in `buf`, returning its total on-disk size.
///
/// Every check a genuine record must pass: a plausible length, a known record
/// type, a payload that fits, a payload that decodes as BSON, and a string
/// `_id` (a collection-wide invariant — the loader skips records without one).
/// Together these make a false positive during resynchronisation unlikely
/// enough to be worth the recovery.
fn validate_record_at(buf: &[u8], pos: usize) -> Option<usize> {
    let header = buf.get(pos..pos + HEADER_SIZE)?;
    let payload_len =
        u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let record_type = header[4];

    if !(MIN_BSON_DOC..=MAX_PAYLOAD).contains(&payload_len) {
        return None;
    }
    if !matches!(
        record_type,
        RECORD_LIVE | RECORD_TOMBSTONE | RECORD_REPLACEMENT
    ) {
        return None;
    }

    let payload = buf.get(pos + HEADER_SIZE..pos + HEADER_SIZE + payload_len)?;
    let doc: Document = bson::from_slice(payload).ok()?;
    doc.get("_id")?.as_str()?;

    Some(HEADER_SIZE + payload_len)
}

/// Find the next record boundary at or after `from` in `buf`.
///
/// A single record that happens to decode is not enough — arbitrary bytes can
/// occasionally satisfy `validate_record_at`.  A candidate is accepted only if
/// the record that follows it also validates, or if it ends exactly at the end
/// of the file (`window_is_eof`), which is the same evidence one record later.
fn resync(buf: &[u8], from: usize, window_is_eof: bool) -> Option<usize> {
    for pos in from..buf.len() {
        let Some(size) = validate_record_at(buf, pos) else {
            continue;
        };
        let next = pos + size;
        if next == buf.len() {
            if window_is_eof {
                return Some(pos);
            }
            continue;
        }
        if validate_record_at(buf, next).is_some() {
            return Some(pos);
        }
    }
    None
}

/// Salvage a damaged BSON file in place.
///
/// Walks the file, keeps every record that decodes, and drops the byte spans
/// that do not — resynchronising past a damaged span where an intact record
/// can be found after it, and truncating where one cannot.  Surviving records
/// are copied verbatim, so the repaired file replays to exactly the state the
/// intact part of the log describes: inserts, replacements and tombstones all
/// keep their order and meaning.
///
/// The rewrite goes to a `.tmp` file which is fsynced and renamed over the
/// original, so an interrupted repair leaves the damaged file untouched rather
/// than adding a second kind of damage to it.  A file with nothing wrong is
/// not rewritten at all.
pub(crate) fn repair_file(path: &Path) -> Result<RepairReport, MooFileError> {
    let mut report = RepairReport::default();

    let mut f = File::open(path).map_err(|e| errors::io_err(path, e))?;
    let file_len = f
        .metadata()
        .map_err(|e| errors::io_err(path, e))?
        .len();
    if file_len == 0 {
        return Ok(report);
    }

    let tmp_path = path.with_extension("bson.repair-tmp");
    let mut out = File::create(&tmp_path).map_err(|e| errors::io_err(&tmp_path, e))?;

    // Wrap the rest so a failure mid-way can clean the temp file up.
    let result = (|| -> Result<(), MooFileError> {
        let mut offset = 0u64;
        let mut header = [0u8; HEADER_SIZE];

        while offset < file_len {
            f.seek(std::io::SeekFrom::Start(offset))
                .map_err(|e| errors::io_err(path, e))?;

            // A record is intact if the header is whole, the payload fits and
            // the payload decodes.  Anything else drops into the resync below.
            let intact = match f.read_exact(&mut header) {
                Ok(()) => {
                    let payload_len =
                        u32::from_le_bytes([header[0], header[1], header[2], header[3]])
                            as usize;
                    let record_type = header[4];
                    if !(MIN_BSON_DOC..=MAX_PAYLOAD).contains(&payload_len)
                        || offset + (HEADER_SIZE + payload_len) as u64 > file_len
                    {
                        None
                    } else {
                        let mut payload = vec![0u8; payload_len];
                        match f.read_exact(&mut payload) {
                            Ok(()) => match bson::from_slice::<Document>(&payload) {
                                Ok(_) => Some((record_type, payload)),
                                Err(_) => None,
                            },
                            Err(_) => None,
                        }
                    }
                }
                Err(_) => None,
            };

            if let Some((record_type, payload)) = intact {
                let size = (HEADER_SIZE + payload.len()) as u64;
                out.write_all(&encode_record_bytes(record_type, &payload))
                    .map_err(|e| errors::io_err(&tmp_path, e))?;
                report.records_kept += 1;
                report.bytes_kept += size;
                offset += size;
                continue;
            }

            // Damaged at `offset`.  Look ahead for the next intact record.
            let window_len = ((file_len - offset) as usize).min(RESYNC_WINDOW);
            let window_is_eof = (offset + window_len as u64) == file_len;
            let mut window = vec![0u8; window_len];
            f.seek(std::io::SeekFrom::Start(offset))
                .map_err(|e| errors::io_err(path, e))?;
            f.read_exact(&mut window)
                .map_err(|e| errors::io_err(path, e))?;

            match resync(&window, 1, window_is_eof) {
                Some(rel) => {
                    report.gaps.push(RepairGap {
                        offset,
                        length: rel as u64,
                        to_end_of_file: false,
                    });
                    report.bytes_dropped += rel as u64;
                    offset += rel as u64;
                }
                None => {
                    let lost = file_len - offset;
                    report.gaps.push(RepairGap {
                        offset,
                        length: lost,
                        to_end_of_file: true,
                    });
                    report.bytes_dropped += lost;
                    break;
                }
            }
        }

        out.flush().map_err(|e| errors::io_err(&tmp_path, e))?;
        out.sync_all().map_err(|e| errors::io_err(&tmp_path, e))?;
        Ok(())
    })();

    drop(out);

    if let Err(e) = result {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    // Nothing was wrong — leave the original alone rather than churning it.
    if report.gaps.is_empty() {
        let _ = fs::remove_file(&tmp_path);
        return Ok(report);
    }

    fs::rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        errors::io_err(path, e)
    })?;

    // Make the rename durable — the same reason `compact` does it.
    let parent = path.parent().unwrap_or(Path::new("."));
    if let Ok(parent_file) = File::open(parent) {
        let _ = parent_file.sync_all();
    }

    report.rewritten = true;
    Ok(report)
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
    durability: Durability,
    file: Option<File>,
}

/// Reject a payload the scanner could never read back.
///
/// The reader caps a record's payload at [`MAX_DOCUMENT_SIZE`] so that a
/// corrupt length field cannot trigger a wild allocation.  Nothing used to
/// enforce the same cap on the way in, so a caller could append a document
/// larger than that and produce a file that every later `open` refuses — and
/// refuses correctly, because from the reader's side an over-cap length is
/// indistinguishable from corruption.  A rejected write is the better half of
/// that trade.
fn check_size(payload: &[u8]) -> Result<(), MooFileError> {
    if payload.len() > MAX_DOCUMENT_SIZE {
        return Err(MooFileError::DocumentTooLarge {
            size: payload.len(),
            max: MAX_DOCUMENT_SIZE,
        });
    }
    Ok(())
}

/// Reject a document carrying a binary value the BSON encoder will not write.
///
/// [`MAX_BINARY_SIZE`] is well below [`MAX_DOCUMENT_SIZE`], and going over it
/// used to fail in three different silent ways at once: `bson::to_vec`
/// returns an error that the write path `expect`ed — panicking while holding
/// the collection's write lock, which poisons it and bricks the handle — and
/// on the read side the same document decodes but then cannot be re-encoded
/// for the raw-document index, so it is dropped and the record reads back as
/// nothing at all.  The pure-Python backend has no such cap, so it could
/// write files whose documents the Rust backend silently could not see.
///
/// The walk is O(fields), and only runs ahead of a full BSON encode of the
/// same document, which is O(bytes) — so it costs nothing measurable.
fn check_binary_fields(doc: &Document) -> Result<(), MooFileError> {
    fn walk(path: &str, value: &Bson) -> Result<(), MooFileError> {
        match value {
            Bson::Binary(b) if b.bytes.len() > MAX_BINARY_SIZE => {
                Err(MooFileError::BinaryFieldTooLarge {
                    field: path.to_string(),
                    size: b.bytes.len(),
                    max: MAX_BINARY_SIZE,
                })
            }
            Bson::Document(d) => {
                for (k, v) in d {
                    walk(&format!("{path}.{k}"), v)?;
                }
                Ok(())
            }
            Bson::Array(a) => {
                for (i, v) in a.iter().enumerate() {
                    walk(&format!("{path}.{i}"), v)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    for (k, v) in doc {
        walk(k, v)?;
    }
    Ok(())
}

/// Check a document is storable without encoding it.
///
/// The batch path buffers documents in memory and only encodes them at
/// commit, but a caller who hands `insert()` an unstorable document should
/// hear about it from that call rather than from a commit several statements
/// later — and, before this existed, what they actually got was a confusing
/// BSON decode error from the adapter, because the *return* value could not
/// be encoded either.  O(fields), so it is free to run eagerly.
pub(crate) fn validate_doc(doc: &Document) -> Result<(), MooFileError> {
    check_binary_fields(doc)
}

/// Encode a document for writing, rejecting anything that could not be read
/// back as it was written.
///
/// The `expect` this replaced ("BSON serialisation is infallible for
/// Document") was not true: `bson::to_vec` fails on an over-cap binary value,
/// and it was being called while holding the collection's write lock.
pub(crate) fn encode_doc(doc: &Document) -> Result<Vec<u8>, MooFileError> {
    check_binary_fields(doc)?;
    let payload = bson::to_vec(doc)?;
    check_size(&payload)?;
    Ok(payload)
}

impl StorageEngine {
    /// Open (or create) the BSON data file.
    pub fn open(path: &Path, readonly: bool, durability: Durability) -> Result<Self, MooFileError> {
        if readonly {
            let file = File::open(path).map_err(|e| errors::io_err(path, e))?;
            Ok(Self {
                path: path.to_path_buf(),
                readonly,
                durability,
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
                durability,
                file: Some(file),
            })
        }
    }

    /// Append a record to the file and flush.
    pub fn append(&mut self, record_type: u8, doc: &Document) -> Result<(), MooFileError> {
        let payload = encode_doc(doc)?;
        self.append_bytes(record_type, &payload)
    }

    /// As `append`, but for a payload that's already encoded — see
    /// `encode_record_bytes`.
    pub fn append_bytes(&mut self, record_type: u8, payload: &[u8]) -> Result<(), MooFileError> {
        if self.readonly {
            return Err(MooFileError::ReadOnly);
        }
        check_size(payload)?;

        let data = encode_record_bytes(record_type, payload);
        let f = self.file.as_mut().expect("StorageEngine: file handle missing");
        f.write_all(&data)
            .map_err(|e| errors::io_err(&self.path, e))?;
        match self.durability {
            Durability::None => {}
            Durability::Os => {
                f.flush().map_err(|e| errors::io_err(&self.path, e))?;
            }
            Durability::Fsync => {
                f.sync_all().map_err(|e| errors::io_err(&self.path, e))?;
            }
        }
        Ok(())
    }

    /// Append multiple records with a single flush/fsync.
    ///
    /// Used by the batch context to commit all buffered records in one
    /// I/O operation regardless of the durability mode.
    pub fn append_batch(&mut self, records: &[(u8, &Document)]) -> Result<(), MooFileError> {
        if self.readonly {
            return Err(MooFileError::ReadOnly);
        }
        if records.is_empty() {
            return Ok(());
        }

        // Encode and size-check the whole batch before writing any of it, so
        // one oversized document fails the batch instead of half-applying it.
        let mut buf = Vec::new();
        let mut encoded = Vec::with_capacity(records.len());
        for (rt, doc) in records {
            encoded.push((*rt, encode_doc(doc)?));
        }
        for (rt, payload) in &encoded {
            buf.extend_from_slice(&encode_record_bytes(*rt, payload));
        }
        let f = self.file.as_mut().expect("StorageEngine: file handle missing");
        f.write_all(&buf)
            .map_err(|e| errors::io_err(&self.path, e))?;
        match self.durability {
            Durability::None => {}
            Durability::Os => {
                f.flush().map_err(|e| errors::io_err(&self.path, e))?;
            }
            Durability::Fsync => {
                f.sync_all().map_err(|e| errors::io_err(&self.path, e))?;
            }
        }
        Ok(())
    }

    /// Flush and fsync the file, ensuring all buffered writes are durable
    /// on disk.  Useful with [`Durability::Os`] or [`Durability::None`] to
    /// batch durability: insert many documents, then call `sync()` once.
    pub fn sync(&self) -> Result<(), MooFileError> {
        if let Some(f) = &self.file {
            f.sync_all().map_err(|e| errors::io_err(&self.path, e))?;
        }
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

    // -----------------------------------------------------------------
    // Zero-filled tails
    //
    // A filesystem with delayed allocation can journal an inode's new size
    // and then lose the data blocks, so an interrupted write leaves a file
    // that is full length and reads back as zeros — not a short file.  That
    // is real-world damage (it took a service down for nine days), and the
    // scanner has to classify it as a truncation point rather than decode
    // the zeros as a record and fail.
    // -----------------------------------------------------------------

    /// Build `n` records followed by `zeros` bytes of lost tail.
    fn file_with_zero_tail(path: &Path, n: usize, zeros: usize) -> u64 {
        let mut f = File::create(path).unwrap();
        let mut good_len = 0u64;
        for i in 0..n {
            let rec = encode_record(RECORD_LIVE, &doc! { "_id": i.to_string(), "v": i as i64 });
            f.write_all(&rec).unwrap();
            good_len += rec.len() as u64;
        }
        f.write_all(&vec![0u8; zeros]).unwrap();
        good_len
    }

    #[test]
    fn scan_zero_filled_tail_is_a_truncation_point() {
        let dir = setup_dir();
        let path = dir.path().join("zero_tail.bson");
        let good_len = file_with_zero_tail(&path, 3, 1814);

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 3, "records before the zeros must survive");
        assert_eq!(truncate_to, Some(good_len));
    }

    #[test]
    fn scan_zero_tail_shorter_than_a_header_still_truncates() {
        // 1..4 zero bytes hit EOF inside the header read; 5+ produce a
        // complete all-zero header.  Both must land on the same answer.
        for zeros in 1..=8 {
            let dir = setup_dir();
            let path = dir.path().join("short_zero_tail.bson");
            let good_len = file_with_zero_tail(&path, 2, zeros);

            let (records, truncate_to) = scan_file(&path).unwrap();
            assert_eq!(records.len(), 2, "zeros={zeros}");
            assert_eq!(truncate_to, Some(good_len), "zeros={zeros}");
        }
    }

    #[test]
    fn zero_run_starting_mid_record_is_also_a_tail() {
        // The lost blocks need not start on a record boundary.  When they do
        // not, the final record's header survives and reads as plausible, so
        // the damage shows up as a payload that will not decode rather than
        // as an all-zero header.  Both are the same crash and both must heal.
        let dir = setup_dir();
        let path = dir.path().join("mid_record_zeros.bson");
        file_with_zero_tail(&path, 4, 0);

        let (records, _) = scan_file(&path).unwrap();
        let last_offset = records[3].offset;

        // Keep the last record's header, zero its payload and past EOF.
        let mut bytes = std::fs::read(&path).unwrap();
        let from = (last_offset as usize) + HEADER_SIZE + 2;
        bytes[from..].fill(0);
        bytes.extend(std::iter::repeat(0u8).take(1814));
        std::fs::write(&path, &bytes).unwrap();

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 3, "the three intact records survive");
        assert_eq!(truncate_to, Some(last_offset));
    }

    #[test]
    fn scan_zero_only_file_truncates_to_zero() {
        let dir = setup_dir();
        let path = dir.path().join("all_zero.bson");
        std::fs::write(&path, vec![0u8; 4096]).unwrap();

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert!(records.is_empty());
        assert_eq!(truncate_to, Some(0));
    }

    #[test]
    fn scan_still_rejects_genuine_mid_file_corruption() {
        // A plausible header over undecodable bytes is not a tail and must
        // not be silently swallowed by the zero-tail rule.
        let dir = setup_dir();
        let path = dir.path().join("garbage.bson");

        let good = encode_record(RECORD_LIVE, &doc! { "_id": "a" });
        let mut data = good.clone();
        data.extend_from_slice(&64u32.to_le_bytes());
        data.push(RECORD_LIVE);
        data.extend_from_slice(&[0xAB; 64]);
        data.extend_from_slice(&encode_record(RECORD_LIVE, &doc! { "_id": "b" }));
        std::fs::write(&path, &data).unwrap();

        match scan_file(&path) {
            Err(MooFileError::CorruptRecord { offset, .. }) => {
                assert_eq!(offset, good.len() as u64);
            }
            other => panic!("expected CorruptRecord, got {other:?}"),
        }
    }

    #[test]
    fn implausible_length_at_the_tail_truncates() {
        // A header whose length field was itself caught half-written: the
        // file genuinely ends here, so this is a tail.
        let dir = setup_dir();
        let path = dir.path().join("huge_tail.bson");

        let good = encode_record(RECORD_LIVE, &doc! { "_id": "a" });
        let mut data = good.clone();
        data.extend_from_slice(&[0xCD; 64]);
        std::fs::write(&path, &data).unwrap();

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(truncate_to, Some(good.len() as u64));
    }

    #[test]
    fn implausible_length_with_records_after_it_is_corruption() {
        // The same bytes, but with intact records following.  Truncating here
        // would silently discard them, which is what used to happen.
        let dir = setup_dir();
        let path = dir.path().join("huge_interior.bson");

        let good = encode_record(RECORD_LIVE, &doc! { "_id": "a" });
        let mut data = good.clone();
        data.extend_from_slice(&[0xCD; 64]);
        for i in 0..4 {
            data.extend_from_slice(&encode_record(
                RECORD_LIVE,
                &doc! { "_id": format!("later{i}"), "v": i as i64 },
            ));
        }
        std::fs::write(&path, &data).unwrap();

        match scan_file(&path) {
            Err(MooFileError::CorruptRecord { offset, .. }) => {
                assert_eq!(offset, good.len() as u64);
            }
            other => panic!("expected CorruptRecord, got {other:?}"),
        }

        // ...and repair recovers everything except the clobbered span.
        let report = repair_file(&path).unwrap();
        assert_eq!(report.records_kept, 5);
        assert_eq!(report.bytes_dropped, 64);
        assert!(!report.gaps[0].to_end_of_file);
    }

    #[test]
    fn zero_hole_with_records_after_it_is_corruption_not_a_tail() {
        let dir = setup_dir();
        let path = dir.path().join("zero_interior.bson");

        let good = encode_record(RECORD_LIVE, &doc! { "_id": "a" });
        let mut data = good.clone();
        data.extend_from_slice(&[0u8; 4096]);
        for i in 0..4 {
            data.extend_from_slice(&encode_record(
                RECORD_LIVE,
                &doc! { "_id": format!("later{i}"), "v": i as i64 },
            ));
        }
        std::fs::write(&path, &data).unwrap();

        match scan_file(&path) {
            Err(MooFileError::CorruptRecord { offset, .. }) => {
                assert_eq!(offset, good.len() as u64);
            }
            other => panic!("expected CorruptRecord, got {other:?}"),
        }

        let report = repair_file(&path).unwrap();
        assert_eq!(report.records_kept, 5);
        assert_eq!(report.bytes_dropped, 4096);
    }

    // -----------------------------------------------------------------
    // repair_file
    // -----------------------------------------------------------------

    #[test]
    fn repair_drops_a_zero_tail_and_keeps_every_record() {
        let dir = setup_dir();
        let path = dir.path().join("repair_tail.bson");
        let good_len = file_with_zero_tail(&path, 5, 1814);

        let report = repair_file(&path).unwrap();
        assert!(report.rewritten);
        assert_eq!(report.records_kept, 5);
        assert_eq!(report.bytes_kept, good_len);
        assert_eq!(report.bytes_dropped, 1814);
        assert_eq!(report.gaps.len(), 1);
        assert!(report.gaps[0].to_end_of_file);
        assert_eq!(report.gaps[0].offset, good_len);

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 5);
        assert!(truncate_to.is_none());
    }

    #[test]
    fn repair_resyncs_past_mid_file_damage() {
        let dir = setup_dir();
        let path = dir.path().join("repair_hole.bson");

        // 20 records, with one record's bytes overwritten by garbage — the
        // shape a lost block in the middle of the file leaves behind.
        let mut data = Vec::new();
        let mut damaged_at = 0usize;
        let mut damaged_len = 0usize;
        for i in 0..20 {
            let rec = encode_record(RECORD_LIVE, &doc! { "_id": i.to_string(), "v": i as i64 });
            if i == 9 {
                damaged_at = data.len();
                damaged_len = rec.len();
                data.extend(std::iter::repeat(0xCDu8).take(rec.len()));
            } else {
                data.extend_from_slice(&rec);
            }
        }
        std::fs::write(&path, &data).unwrap();

        let report = repair_file(&path).unwrap();
        assert!(report.rewritten);
        assert_eq!(report.records_kept, 19, "only the damaged record is lost");
        assert_eq!(report.gaps.len(), 1);
        assert!(!report.gaps[0].to_end_of_file, "it resynced, not truncated");
        assert_eq!(report.gaps[0].offset, damaged_at as u64);
        assert_eq!(report.gaps[0].length, damaged_len as u64);

        let (records, truncate_to) = scan_file(&path).unwrap();
        assert!(truncate_to.is_none());
        let ids: Vec<&str> = records.iter().map(|r| r.doc.get_str("_id").unwrap()).collect();
        assert_eq!(ids.len(), 19);
        assert!(!ids.contains(&"9"));
        assert!(ids.contains(&"8") && ids.contains(&"10"));
    }

    #[test]
    fn repair_preserves_tombstones_and_replacements_in_order() {
        let dir = setup_dir();
        let path = dir.path().join("repair_types.bson");

        let mut data = Vec::new();
        data.extend_from_slice(&encode_record(RECORD_LIVE, &doc! { "_id": "a", "v": 1 }));
        data.extend_from_slice(&encode_record(RECORD_LIVE, &doc! { "_id": "b", "v": 2 }));
        data.extend_from_slice(&encode_record(RECORD_REPLACEMENT, &doc! { "_id": "a", "v": 3 }));
        data.extend_from_slice(&encode_record(RECORD_TOMBSTONE, &doc! { "_id": "b" }));
        data.extend_from_slice(&[0u8; 512]);
        std::fs::write(&path, &data).unwrap();

        let report = repair_file(&path).unwrap();
        assert_eq!(report.records_kept, 4);

        let (records, _) = scan_file(&path).unwrap();
        let types: Vec<u8> = records.iter().map(|r| r.record_type).collect();
        assert_eq!(
            types,
            vec![RECORD_LIVE, RECORD_LIVE, RECORD_REPLACEMENT, RECORD_TOMBSTONE]
        );
    }

    #[test]
    fn repair_leaves_an_intact_file_untouched() {
        let dir = setup_dir();
        let path = dir.path().join("repair_clean.bson");
        file_with_zero_tail(&path, 4, 0);

        let before = std::fs::read(&path).unwrap();
        let mtime_before = std::fs::metadata(&path).unwrap().modified().unwrap();

        let report = repair_file(&path).unwrap();
        assert!(!report.rewritten);
        assert!(!report.is_damaged());
        assert_eq!(report.records_kept, 4);

        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), mtime_before);
        assert!(!path.with_extension("bson.repair-tmp").exists());
    }

    #[test]
    fn repair_of_an_empty_file_is_a_no_op() {
        let dir = setup_dir();
        let path = dir.path().join("repair_empty.bson");
        File::create(&path).unwrap();

        let report = repair_file(&path).unwrap();
        assert!(!report.rewritten);
        assert_eq!(report.records_kept, 0);
    }

    #[test]
    fn repair_truncates_when_nothing_intact_follows_the_damage() {
        let dir = setup_dir();
        let path = dir.path().join("repair_lost_tail.bson");

        let mut data = Vec::new();
        data.extend_from_slice(&encode_record(RECORD_LIVE, &doc! { "_id": "a" }));
        let kept = data.len();
        data.extend(std::iter::repeat(0x7Fu8).take(4096));
        std::fs::write(&path, &data).unwrap();

        let report = repair_file(&path).unwrap();
        assert_eq!(report.records_kept, 1);
        assert_eq!(report.bytes_kept, kept as u64);
        assert_eq!(report.bytes_dropped, 4096);
        assert!(report.gaps[0].to_end_of_file);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), kept as u64);
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

        let mut engine = StorageEngine::open(&path, false, Durability::Os).unwrap();
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

    // -----------------------------------------------------------------
    // Write-side size cap
    //
    // The reader refuses a record over MAX_DOCUMENT_SIZE, because from its
    // side an over-cap length is indistinguishable from a corrupt one.  The
    // writer has to refuse the same thing, or a caller can append a document
    // that every later open rejects — a file broken by its own writer.
    // -----------------------------------------------------------------

    #[test]
    fn check_size_accepts_up_to_the_cap() {
        assert!(check_size(&[]).is_ok());
        assert!(check_size(&vec![0u8; 1024]).is_ok());
        assert!(check_size(&vec![0u8; MAX_DOCUMENT_SIZE]).is_ok());
    }

    #[test]
    fn check_size_rejects_over_the_cap() {
        match check_size(&vec![0u8; MAX_DOCUMENT_SIZE + 1]) {
            Err(MooFileError::DocumentTooLarge { size, max }) => {
                assert_eq!(size, MAX_DOCUMENT_SIZE + 1);
                assert_eq!(max, MAX_DOCUMENT_SIZE);
            }
            other => panic!("expected DocumentTooLarge, got {other:?}"),
        }
    }

    /// A document over the record cap, built from a string — binaries hit the
    /// lower `MAX_BINARY_SIZE` first and are covered separately below.
    fn oversized_doc() -> Document {
        doc! { "_id": "big", "blob": "x".repeat(MAX_DOCUMENT_SIZE) }
    }

    fn oversized_binary_doc() -> Document {
        doc! {
            "_id": "bin",
            "blob": bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![0u8; MAX_BINARY_SIZE + 1],
            },
        }
    }

    #[test]
    fn append_rejects_an_oversized_document_without_writing() {
        let dir = setup_dir();
        let path = dir.path().join("oversize.bson");

        let mut engine = StorageEngine::open(&path, false, Durability::Os).unwrap();
        engine.append(RECORD_LIVE, &doc! { "_id": "small" }).unwrap();
        let good_len = std::fs::metadata(&path).unwrap().len();

        assert!(matches!(
            engine.append(RECORD_LIVE, &oversized_doc()),
            Err(MooFileError::DocumentTooLarge { .. })
        ));
        engine.close();

        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            good_len,
            "a rejected append must not have written anything"
        );
        let (records, truncate_to) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert!(truncate_to.is_none());
    }

    #[test]
    fn append_batch_rejects_the_whole_batch_not_half_of_it() {
        let dir = setup_dir();
        let path = dir.path().join("oversize_batch.bson");

        let mut engine = StorageEngine::open(&path, false, Durability::Os).unwrap();
        let small = doc! { "_id": "a", "v": 1 };
        let big = oversized_doc();
        let last = doc! { "_id": "c", "v": 3 };

        assert!(matches!(
            engine.append_batch(&[
                (RECORD_LIVE, &small),
                (RECORD_LIVE, &big),
                (RECORD_LIVE, &last),
            ]),
            Err(MooFileError::DocumentTooLarge { .. })
        ));
        engine.close();

        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            0,
            "the small documents before the oversized one must not be written either"
        );
    }

    /// The `bson` crate refuses to encode a binary value over 16 MiB.  That
    /// used to surface as a panic from `bson::to_vec(..).expect(..)` on the
    /// write path — while holding the collection's write lock, which poisons
    /// it and bricks the handle for good.
    #[test]
    fn append_rejects_an_oversized_binary_field_instead_of_panicking() {
        let dir = setup_dir();
        let path = dir.path().join("oversize_binary.bson");

        let mut engine = StorageEngine::open(&path, false, Durability::Os).unwrap();
        match engine.append(RECORD_LIVE, &oversized_binary_doc()) {
            Err(MooFileError::BinaryFieldTooLarge { field, size, max }) => {
                assert_eq!(field, "blob");
                assert_eq!(size, MAX_BINARY_SIZE + 1);
                assert_eq!(max, MAX_BINARY_SIZE);
            }
            other => panic!("expected BinaryFieldTooLarge, got {other:?}"),
        }
        engine.close();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn oversized_binary_is_found_however_deeply_it_is_nested() {
        let big = bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![0u8; MAX_BINARY_SIZE + 1],
        };

        let nested = doc! { "_id": "a", "outer": { "inner": big.clone() } };
        match check_binary_fields(&nested) {
            Err(MooFileError::BinaryFieldTooLarge { field, .. }) => {
                assert_eq!(field, "outer.inner");
            }
            other => panic!("expected BinaryFieldTooLarge, got {other:?}"),
        }

        let in_array = doc! { "_id": "a", "items": [ "ok", big ] };
        match check_binary_fields(&in_array) {
            Err(MooFileError::BinaryFieldTooLarge { field, .. }) => {
                assert_eq!(field, "items.1");
            }
            other => panic!("expected BinaryFieldTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn a_binary_at_the_cap_is_accepted_and_round_trips() {
        let dir = setup_dir();
        let path = dir.path().join("binary_at_cap.bson");

        let at_cap = doc! {
            "_id": "bin",
            "blob": bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![7u8; MAX_BINARY_SIZE],
            },
        };
        let mut engine = StorageEngine::open(&path, false, Durability::Os).unwrap();
        engine.append(RECORD_LIVE, &at_cap).unwrap();
        engine.close();

        let (records, _) = scan_file(&path).unwrap();
        assert_eq!(records.len(), 1);
        // And it survives the re-encode the index does, which is the step
        // that used to drop it on the floor.
        assert!(bson::raw::RawDocumentBuf::try_from(&records[0].doc).is_ok());
    }

    #[test]
    fn readonly_engine_rejects_writes() {
        let dir = setup_dir();
        let path = dir.path().join("ro.bson");
        File::create(&path).unwrap();

        let mut engine = StorageEngine::open(&path, true, Durability::Os).unwrap();
        let result = engine.append(RECORD_LIVE, &doc! {"_id": "x"});
        assert!(result.is_err());
        match result.unwrap_err() {
            MooFileError::ReadOnly => {}
            other => panic!("expected ReadOnly, got {other:?}"),
        }
    }
}
