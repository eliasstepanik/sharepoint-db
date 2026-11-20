//! The synced folder: filesystem access behind a trait, plus the segment codec.
//!
//! A *segment* is the unit of publication: a run of one node's frames, written
//! once into `segments/` and never modified afterwards. Other nodes read it,
//! verify it as a whole and merge its frames into their own store.
//!
//! Wire format (all integers little-endian):
//!
//! ```text
//! segment := "SDBSEG01" | db_id[16] | node_id[8] | first_seq u64 | count u32 | frame * count
//! frame   := body_len u32 | seq u64 | lamport u64 | kind u8 | prev_hash[32] | payload | hash[32]
//! ```
//!
//! A segment is valid only as a WHOLE: the magic and `db_id` must match, the
//! frame `seq`s must be contiguous starting at `first_seq`, every frame hash must
//! verify, the internal `prev_hash` links must chain, and the file must end
//! exactly after the last frame. Anything else means the file is ignored.
//!
//! All folder access goes through [`FolderFs`] so tests can inject hostile
//! behaviour (partial reads, sharing violations, …). The real implementation
//! retries transient errors, because a folder synced by OneDrive/SharePoint
//! regularly refuses a file for a moment while it is being uploaded or
//! downloaded.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::frame::{DbId, Frame, NodeId};

/// Magic at the start of every segment file.
#[doc(hidden)]
pub const SEGMENT_MAGIC: &[u8; 8] = b"SDBSEG01";

/// Whether `bytes` is a well-formed segment for `db_id`, verified as a whole.
///
/// Exposed for the tests: it is the same check the ingest path runs, so a test
/// can assert that a corrupted segment is rejected without going through a
/// whole sync.
#[doc(hidden)]
pub fn segment_valid(bytes: &[u8], db_id: &DbId) -> bool {
    decode_segment(bytes, db_id).is_ok()
}

/// Length of the segment header: magic + db_id + node_id + first_seq + count.
const HEADER_LEN: usize = 8 + 16 + 8 + 8 + 4;

/// The smallest a frame can be: `body_len` plus the fixed body prefix plus the
/// trailing hash, with an empty payload.
const MIN_FRAME_LEN: usize = 4 + 8 + 8 + 1 + 32 + 32;

/// The directory (inside the synced folder) that holds the segments.
pub(crate) const SEGMENTS_DIR: &str = "segments";
// ---------------------------------------------------------------------------
// Folder filesystem access
// ---------------------------------------------------------------------------

/// Access to the synced folder, behind a trait so tests can inject hostile
/// behaviour (a file that disappears between listing and reading, a rename that
/// fails with a sharing violation, …).
///
/// This is an injection seam, not a stable public surface: the methods mirror
/// the handful of operations a sync needs and nothing else. Nothing here may
/// modify a file that already exists — the folder is write-once.
pub trait FolderFs: Send + Sync {
    /// The names (not paths) of the entries in `dir`.
    ///
    /// A missing directory yields an empty list: a folder that has not been
    /// created yet simply holds nothing.
    fn list(&self, dir: &Path) -> Result<Vec<String>>;

    /// Reads a whole file.
    fn read(&self, path: &Path) -> Result<Vec<u8>>;

    /// Writes `bytes` to `path` atomically: a `*.tmp` file in the same
    /// directory, fsynced, then renamed onto `path`.
    ///
    /// The rename is the publication point: a reader either sees nothing or the
    /// complete file. `path` must not exist yet.
    fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<()>;

    /// Creates `dir` and its parents if missing.
    fn create_dir_all(&self, dir: &Path) -> Result<()>;

    /// True when `path` exists.
    ///
    /// The folder is write-once, so a publish must never rename over a name that
    /// is already taken; this is the check that keeps it honest.
    fn exists(&self, path: &Path) -> Result<bool>;

    /// Deletes `path`.
    ///
    /// Deleting is legal only for the compaction prune (see [`crate::checkpoint`]):
    /// a segment whose frames are all below the checkpoint boundary may be
    /// removed to bound the folder, and superseded checkpoint files are garbage-
    /// collected too (the best and the previous are kept, see
    /// [`crate::checkpoint::CHECKPOINTS_KEPT`]). So "write-once" describes the
    /// *creation* discipline — a file is published by an atomic rename and never
    /// modified in place — not that files live forever: segments and checkpoints
    /// are both deleted by prune. Prune is best-effort, so an implementation that
    /// refuses the delete is not a correctness problem.
    fn delete(&self, path: &Path) -> Result<()>;
}

/// The real filesystem, with retries for the transient errors a sync client
/// causes.
///
/// While OneDrive/SharePoint uploads or downloads a file it briefly refuses
/// other access to it: `PermissionDenied` and `WouldBlock` on Unix, and on
/// Windows the sharing violations `ERROR_SHARING_VIOLATION` (32) and
/// `ERROR_LOCK_VIOLATION` (33). Those are retried five times with a backoff that
/// starts at 25 ms and doubles. Everything else — and a failure that outlives
/// the retries — is returned to the caller.
pub struct RealFs;

/// How many times a transient failure is retried after the first attempt.
const RETRIES: u32 = 5;
/// The first backoff; it doubles on every retry.
const FIRST_BACKOFF: Duration = Duration::from_millis(25);

/// True for the errors that are worth retrying.
fn is_transient(err: &std::io::Error) -> bool {
    match err.kind() {
        std::io::ErrorKind::PermissionDenied
        | std::io::ErrorKind::WouldBlock
        | std::io::ErrorKind::Interrupted => true,
        // Windows sharing violations. On Unix `raw_os_error` values 32/33 are
        // EPIPE/EDOM, which are never produced by these calls, so matching the
        // code directly is safe and keeps the Windows case honest.
        _ => matches!(err.raw_os_error(), Some(32) | Some(33)),
    }
}

/// Runs `f`, retrying transient I/O errors with a doubling backoff.
///
/// Exposed for the tests only: a hostile [`FolderFs`] wraps it so the retry and
/// backoff policy it exercises is the one the real filesystem uses.
#[doc(hidden)]
pub fn with_retry<T, F>(mut f: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut backoff = FIRST_BACKOFF;
    for attempt in 0..=RETRIES {
        match f() {
            Ok(value) => return Ok(value),
            Err(Error::Io(err)) if attempt < RETRIES && is_transient(&err) => {
                std::thread::sleep(backoff);
                backoff *= 2;
            }
            Err(err) => return Err(err),
        }
    }
    // Unreachable: the loop returns on the last attempt.
    Err(Error::Io(std::io::Error::other("retry loop exited")))
}

impl FolderFs for RealFs {
    fn list(&self, dir: &Path) -> Result<Vec<String>> {
        with_retry(|| {
            let entries = match std::fs::read_dir(dir) {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(err) => return Err(Error::Io(err)),
            };
            let mut out = Vec::new();
            for entry in entries {
                let entry = entry?;
                if entry.path().is_file() {
                    out.push(entry.file_name().to_string_lossy().into_owned());
                }
            }
            Ok(out)
        })
    }

    fn read(&self, path: &Path) -> Result<Vec<u8>> {
        with_retry(|| Ok(std::fs::read(path)?))
    }

    fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        with_retry(|| {
            let tmp = tmp_path(path);
            // A temp file left behind by a crashed publish would block the rename
            // forever, so it is cleared first. Only one process can publish for a
            // given database (the `LOCK` file), and a temp name carries its node's
            // id, so no other writer can be using it.
            let _ = std::fs::remove_file(&tmp);
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)?;
            // A half-written temp file must never be renamed onto a real name:
            // write, fsync, then rename.
            let written = file.write_all(bytes).and_then(|()| file.sync_all());
            drop(file);
            written?;
            std::fs::rename(&tmp, path)?;
            Ok(())
        })
    }

    fn create_dir_all(&self, dir: &Path) -> Result<()> {
        with_retry(|| Ok(std::fs::create_dir_all(dir)?))
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        with_retry(|| Ok(path.exists()))
    }

    fn delete(&self, path: &Path) -> Result<()> {
        with_retry(|| match std::fs::remove_file(path) {
            // A file that is already gone is exactly what a delete wanted.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            other => Ok(other?),
        })
    }
}

/// Suffix of the temporary files a publish writes before renaming.
const TMP_SUFFIX: &str = ".tmp";

/// The `*.tmp` name a publish uses before renaming onto `path`.
fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    name.push_str(TMP_SUFFIX);
    path.with_file_name(name)
}

/// True for a file name a sync should read as a segment.
pub(crate) fn is_segment_name(name: &str) -> bool {
    name.ends_with(".seg") && !name.ends_with(TMP_SUFFIX)
}

/// A decoded segment: one node's frames, in `seq` order.
#[derive(Debug)]
pub struct Segment {
    /// The node that produced it.
    pub node_id: NodeId,
    /// The frames, contiguous in `seq` (verified by [`decode_segment`]).
    pub frames: Vec<Frame>,
}

impl Segment {
    /// The `seq` of the last frame.
    pub fn last_seq(&self) -> u64 {
        self.frames
            .last()
            .expect("a segment always has at least one frame")
            .seq
    }
}

// ---------------------------------------------------------------------------
// File names
// ---------------------------------------------------------------------------

/// The canonical name of the segment holding `first_seq..=last_seq` of `node`'s
/// log: `<node16hex>-<first_seq:010>-<last_seq:010>.seg`.
pub(crate) fn segment_name(node_id: &NodeId, first_seq: u64, last_seq: u64) -> String {
    format!(
        "{}-{first_seq:010}-{last_seq:010}.seg",
        crate::util::hex(node_id)
    )
}

/// Parses a segment file name.
///
/// Returns `None` for anything that is not a segment name (a `.tmp` file, a
/// conflicted copy, a name with the wrong shape). Names carry no authority: the
/// file's *content* decides what it is, and every `.seg` file is read and
/// verified regardless of its name. The parsed range is used only to cross-check
/// the content (a file whose frames are not the range its name announces is
/// corrupt) and to decide what to republish.
pub(crate) fn parse_segment_name(name: &str) -> Option<(NodeId, u64, u64)> {
    let stem = name.strip_suffix(".seg")?;
    if stem.ends_with(TMP_SUFFIX) {
        return None;
    }
    let mut parts = stem.rsplitn(3, '-');
    let last = parts.next()?.parse::<u64>().ok()?;
    let first = parts.next()?.parse::<u64>().ok()?;
    let node_hex = parts.next()?;
    let bytes = crate::util::hex_decode(node_hex)?;
    let node_id = <[u8; 8]>::try_from(bytes).ok()?;
    if first == 0 || first > last {
        return None;
    }
    Some((node_id, first, last))
}

/// True for names a sync client writes when it cannot merge a file
/// ("foo (conflicted copy 2024-01-01).seg", case-insensitive).
pub(crate) fn is_conflicted_copy(name: &str) -> bool {
    name.to_lowercase().contains("(conflicted copy")
}

// ---------------------------------------------------------------------------
// Segment codec
// ---------------------------------------------------------------------------

/// Encodes `frames` (contiguous, starting at `frames[0].seq`) as a segment.
///
/// Exposed because the format is public: a tool that publishes frames it got
/// from somewhere else (a backup, an import) has to be able to produce exactly
/// the bytes `sync` would have written.
///
/// Fails with [`Error::CorruptSegment`] on an empty slice: a segment always
/// carries at least one frame (its `first_seq` comes from the first frame), so
/// there is nothing to encode.
pub fn encode_segment(db_id: &DbId, node_id: &NodeId, frames: &[Frame]) -> Result<Vec<u8>> {
    let Some(first) = frames.first() else {
        return Err(Error::CorruptSegment(
            "segment with no frames cannot be encoded".into(),
        ));
    };
    let mut out = Vec::new();
    out.extend_from_slice(SEGMENT_MAGIC);
    out.extend_from_slice(db_id);
    out.extend_from_slice(node_id);
    out.extend_from_slice(&first.seq.to_le_bytes());
    out.extend_from_slice(&(frames.len() as u32).to_le_bytes());
    for frame in frames {
        out.extend_from_slice(&frame.encode(db_id, node_id));
    }
    Ok(out)
}

/// Decodes a segment, verifying it as a whole.
///
/// Checks the magic, the `db_id`, contiguity of the `seq`s, every frame hash and
/// the internal `prev_hash` chain, and that the file ends exactly after the last
/// frame. `prev_hash` of the first frame is *not* checked here: whether the
/// segment continues the node's chain in *this* store is the caller's business
/// (a segment from a node the store has never seen legitimately starts with a
/// non-zero `prev_hash`).
pub fn decode_segment(bytes: &[u8], expected_db_id: &DbId) -> Result<Segment> {
    if bytes.len() < HEADER_LEN {
        return Err(Error::CorruptSegment("segment too short for header".into()));
    }
    if &bytes[..8] != SEGMENT_MAGIC {
        return Err(Error::CorruptSegment("bad segment magic".into()));
    }
    let db_id = <[u8; 16]>::try_from(&bytes[8..24]).expect("length checked above");
    if &db_id != expected_db_id {
        return Err(Error::CorruptSegment(
            "segment belongs to another database".into(),
        ));
    }
    let node_id = <[u8; 8]>::try_from(&bytes[24..32]).expect("length checked above");
    let first_seq = u64::from_le_bytes([
        bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
    ]);
    let count = u32::from_le_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]) as usize;
    if count == 0 {
        return Err(Error::CorruptSegment("segment with no frames".into()));
    }
    if first_seq == 0 {
        return Err(Error::CorruptSegment(
            "segment first_seq must be 1 or greater".into(),
        ));
    }
    // A frame is at least `body_len` + the fixed body prefix + its hash, so a
    // count that cannot fit in the file is corrupt. Checking before allocating
    // keeps a corrupt header from asking for a huge vector.
    if count > (bytes.len() - HEADER_LEN) / MIN_FRAME_LEN {
        return Err(Error::CorruptSegment(
            "segment frame count larger than the file".into(),
        ));
    }
    // The last frame's seq is `first_seq + count - 1`; a crafted header can push
    // it past `u64::MAX`, which the per-frame `checked_add` below would catch,
    // but rejecting it here keeps the whole range provably in range.
    if first_seq.checked_add(count as u64 - 1).is_none() {
        return Err(Error::CorruptSegment(
            "segment seq range overflows u64".into(),
        ));
    }

    let mut rest = &bytes[HEADER_LEN..];
    let mut frames: Vec<Frame> = Vec::with_capacity(count);
    for i in 0..count {
        // A frame is self-delimiting through its `body_len`; a truncated tail
        // fails inside `Frame::decode_prefix` (length mismatch).
        let (frame, used) = Frame::decode_prefix(rest, &db_id, &node_id)
            .map_err(|e| Error::CorruptSegment(format!("segment frame {i}: {e}")))?;
        rest = &rest[used..];
        // `checked_add`, so a crafted `first_seq` near `u64::MAX` cannot wrap
        // into a seq that happens to match a frame's.
        let want = first_seq
            .checked_add(i as u64)
            .ok_or_else(|| Error::CorruptSegment(format!("segment seq overflow at frame {i}")))?;
        if frame.seq != want {
            return Err(Error::CorruptSegment(format!(
                "segment seq not contiguous: expected {want}, found {}",
                frame.seq
            )));
        }
        if let Some(prev) = frames.last() {
            if frame.prev_hash != prev.hash {
                return Err(Error::CorruptSegment(format!(
                    "segment chain broken at seq {}",
                    frame.seq
                )));
            }
        }
        frames.push(frame);
    }
    if !rest.is_empty() {
        return Err(Error::CorruptSegment(format!(
            "segment has {} trailing bytes after the last frame",
            rest.len()
        )));
    }

    Ok(Segment { node_id, frames })
}
