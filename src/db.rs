//! The durable single-node database: [`ShareDb`].
//!
//! A `ShareDb` lives in a synced folder (the *database path*) plus a local
//! *state directory* (one per database per machine, by default under the
//! machine's local data directory — never inside the synced folder). The synced
//! folder holds only the write-once `MANIFEST` and (from step 3) the published
//! segments; all mutable state lives in the state directory:
//!
//! * `LOCK` — a single-process guard.
//! * `store.db` — every frame this node has ever seen (see [`crate::store`]).
//! * `data.db` — the user's tables plus `__sharedb_*` bookkeeping; disposable
//!   and rebuilt from `store.db` whenever its digest disagrees with the store.
//!
//! The commit protocol never touches the synced folder. A commit writes the
//! captured changeset into `data.db` first and then appends the frame to
//! `store.db`; the store write is the durability point. A commit that fails
//! after the `data.db` commit is repaired immediately by rebuilding `data.db`
//! from the store, so the data file can never run ahead of the frame log.

use std::collections::HashMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, OptionalExtension};

use crate::error::{Error, Result};
use crate::folder::{self, FolderFs, RealFs, Segment};
use crate::frame::{DbId, Frame, FrameKind, FrameRef, NodeId};
use crate::materialize::{
    self, changeset, digest, dump, ensure_meta, replay, validate_schema, Dump, RollbackOnDrop,
};
use crate::util::{hex as hex_encode, hex_decode};
use crate::{checkpoint, store};

/// A fault-injection plan used by the tests. Not part of the public surface.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPlan {
    /// Return an error after `data.db` has committed but before the frame is
    /// inserted into `store.db`. Models a crash in the window between the two
    /// writes: the change is acknowledged nowhere and must vanish on reopen.
    AfterDataCommitBeforeFrameInsert,
    /// Like [`FaultPlan::AfterDataCommitBeforeFrameInsert`], but the repair that
    /// follows fails as well. Models a state directory that cannot be written
    /// (read-only, full disk): the handle must be marked failed instead of
    /// carrying on with a `data.db` the store cannot account for.
    AfterDataCommitAndRepairFails,
    /// Every read of the store's [`checkpoint::Base`] made from inside the handle
    /// fails ([`ShareDb::read_base`]); store writes still work. Models a `store.db`
    /// whose read fails: the read that decides whether this node is bootstrapped
    /// must not be read as "there is no Base", or [`ShareDb::refresh_chain`] drops
    /// the lamport floor to 0 and the node commits below the cut (F2).
    StoreBaseReadFails,
    /// [`FaultPlan::AfterDataCommitBeforeFrameInsert`] and
    /// [`FaultPlan::StoreBaseReadFails`] together: the store write fails *and* the
    /// `Base` read fails, which is the state a busy or failing `store.db` really
    /// leaves — the read can fail exactly where the write just did.
    /// [`ShareDb::repair_to_store`] must then fail closed, not store-only-rebuild.
    AfterDataCommitAndStoreBaseReadFails,
    /// Like [`FaultPlan::AfterDataCommitBeforeFrameInsert`], but the removal of a
    /// bootstrapped node's diverged `data.db` fails as well (a Windows file lock,
    /// an antivirus holding the file open). The phantom row is then still on disk,
    /// so the repair marks the file invalid in `store.db` instead.
    BaseRepairRemoveFails,
}

/// Options for [`ShareDb::open`].
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// Create the database if the folder does not exist yet.
    pub create: bool,
    /// Where to keep the local state (`LOCK`, `store.db`, `data.db`).
    ///
    /// Defaults to a machine-local directory under the local data directory
    /// (`$XDG_DATA_HOME` or `~/.local/share` on Unix, `%LOCALAPPDATA%` on
    /// Windows), keyed by the database id — never inside the synced folder.
    pub state_dir: Option<PathBuf>,
    /// How many store frames may accumulate beyond the newest checkpoint before
    /// `sync` writes a new one.
    ///
    /// A checkpoint is what lets the folder's segments be pruned (see
    /// [`crate::checkpoint`]), so this is the knob that trades folder size and
    /// checkpoint work against how often a node must sync. Default `64`.
    pub checkpoint_threshold: usize,
    /// How many segments above the checkpoint boundary must survive a prune.
    ///
    /// This is the retention window: a node that syncs at least once per
    /// `retention_segments` of cluster activity always finds the frames it is
    /// missing in the folder. `0` prunes as aggressively as the boundary allows.
    /// Default `8`.
    pub retention_segments: usize,
    /// Test-only fault injection.
    #[doc(hidden)]
    pub fault: Option<FaultPlan>,
}

/// The default `checkpoint_threshold` (see [`OpenOptions`]).
pub const DEFAULT_CHECKPOINT_THRESHOLD: usize = 64;
/// The default `retention_segments` (see [`OpenOptions`]).
pub const DEFAULT_RETENTION_SEGMENTS: usize = 8;

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions {
            create: false,
            state_dir: None,
            checkpoint_threshold: DEFAULT_CHECKPOINT_THRESHOLD,
            retention_segments: DEFAULT_RETENTION_SEGMENTS,
            fault: None,
        }
    }
}

impl OpenOptions {
    /// Options that create the database if it is missing.
    pub fn create() -> Self {
        OpenOptions {
            create: true,
            ..Default::default()
        }
    }
}

/// The outcome of [`ShareDb::migrate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateOutcome {
    /// The migration ran and was recorded as a frame.
    Applied,
    /// This exact migration was already applied on this node.
    AlreadyApplied,
}

/// How [`ShareDb::sync`] brought `data.db` in line with the frame store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MaterializeKind {
    /// Nothing new was ingested, so `data.db` was left alone.
    #[default]
    UpToDate,
    /// The new frames sorted after everything already applied: they were
    /// replayed onto the existing `data.db`.
    Fast,
    /// A frame sorted before frames already applied (or `data.db` could not be
    /// trusted): `data.db` was rebuilt from scratch by replaying every frame.
    Rebuilt,
}

/// The outcome of [`ShareDb::sync`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    /// Frames published to the synced folder this call.
    pub published: usize,
    /// Frames read from the synced folder and appended to the local store.
    pub ingested: usize,
    /// How `data.db` was brought in line with the store.
    pub materialize: MaterializeKind,
    /// Frames found in the folder that this store already had.
    pub skipped: usize,
    /// Segment files that could not be read or did not verify; they were ignored.
    pub ignored_files: Vec<PathBuf>,
    /// `(node, seq)` where a node's chain is missing: `seq` is the first seq this
    /// node could not continue from.
    pub gaps: Vec<(NodeId, u64)>,
    /// `(node, seq)` where the folder holds a frame that disagrees with the one
    /// this store recorded at that position.
    pub forks: Vec<(NodeId, u64)>,
    /// Publishing failed (the folder refused a write); the local data is intact
    /// and the frames will be published on a later sync.
    pub publish_pending: bool,
    /// Frames below a checkpoint's cut boundary that this node could **not**
    /// apply, because the checkpoint's version vector does not cover them and
    /// this node's store is behind that cut.
    ///
    /// This is the documented loss case (see the module docs of
    /// [`crate::checkpoint`]): a node that stayed offline past the retention
    /// window and lost its local `store.db`. It is reported here rather than
    /// silently claimed as applied; recovery needs that node's own store, which
    /// is never pruned. `0` is the normal value.
    ///
    /// For a bootstrapped node held `materialize_pending` this count is carried
    /// forward across syncs (the max of previous and current), never reset to
    /// `0` while the node is still pending.
    pub dropped_below_boundary: usize,
    /// A bootstrapped node could not re-bootstrap from any dominating, gate-
    /// passing checkpoint, so it kept its last-good `data.db` instead of a
    /// store-only rebuild (which would drop the state below the cut). Reads
    /// still work off that file; the node retries a re-bootstrap on every sync
    /// and clears this flag once a covering checkpoint appears (see [`crate::db`]
    /// R2).
    pub materialize_pending: bool,
}

/// A snapshot of this node's state, returned by [`ShareDb::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// This node's id.
    pub node_id: NodeId,
    /// The highest `seq` this node has written.
    pub own_seq: u64,
    /// Frames recorded locally but not yet published to the synced folder.
    pub unpublished: usize,
    /// `(node, highest seq seen)` for every peer this node has frames from.
    pub peers: Vec<(NodeId, u64)>,
    /// Segment files the sync client could not merge and renamed with
    /// `(conflicted copy …)`. Reported only: their content is handled later.
    pub conflicted_copies: Vec<PathBuf>,
    /// A bootstrapped node that could not re-bootstrap from a dominating, gate-
    /// passing checkpoint keeps its last-good `data.db` and stays pending (see
    /// [`SyncReport::materialize_pending`]). Reads still work off that file.
    pub materialize_pending: bool,
    /// The dropped-below-boundary count carried on the handle: for a pending
    /// bootstrapped node this is the count carried forward across syncs (never
    /// reset to `0` while pending), so an open that could not re-bootstrap still
    /// reports it. `0` otherwise.
    pub dropped_below_boundary: usize,
}

/// A handle to a durable single-node `sharepoint-db` database.
pub struct ShareDb {
    folder: PathBuf,
    state_dir: PathBuf,
    db_id: DbId,
    node_id: NodeId,
    store: Connection,
    data: Connection,
    schema_version: u32,
    applied_count: u64,
    /// SHA-256 over the hashes of the frames already recorded as applied (the
    /// running value behind `applied_digest`), kept so a commit never has to
    /// re-hash the whole store.
    applied_hasher: sha2::Sha256,
    /// The digest matching `applied_count` (same value, materialised).
    applied_digest: [u8; 32],
    /// This node's chain position, kept in memory: highest own `seq` and the
    /// hash of that frame (zeros when the node has no frames).
    own_seq: u64,
    own_prev_hash: [u8; 32],
    /// The highest lamport timestamp this node has seen.
    lamport_max: u64,
    /// Set once a commit left the handle in a state we cannot trust; every
    /// later call fails instead of building on diverged data.
    failed: Option<String>,
    /// A bootstrapped node that could not re-bootstrap (no dominating, gate-
    /// passing checkpoint) keeps its last-good `data.db` and stays pending: it
    /// retries a re-bootstrap on every sync. Reads still work off the last-good
    /// file while pending (see [`crate::db`] R2).
    materialize_pending: bool,
    /// The dropped-below-boundary count carried forward while pending: the max
    /// seen across syncs, never reset to `0` while the node is still pending.
    dropped_below_boundary: usize,
    /// Raised while a user closure runs so the authorizer installed on `data`
    /// denies `BEGIN`/`COMMIT`/`ROLLBACK`, savepoints and `ATTACH`.
    txn_guard: Arc<AtomicBool>,
    fault: Option<FaultPlan>,
    /// See [`OpenOptions::checkpoint_threshold`].
    checkpoint_threshold: usize,
    /// See [`OpenOptions::retention_segments`].
    retention_segments: usize,
    /// Access to the synced folder (segments). Only [`ShareDb::sync`] and
    /// [`ShareDb::status`] touch it.
    fs: Arc<dyn FolderFs>,
    // Held for the lifetime of the handle so the OS lock is released on drop.
    _lock: FileLock,
}

/// Raises the authorizer flag for the duration of a user closure.
struct GuardTxn(Arc<AtomicBool>);

impl GuardTxn {
    fn new(flag: &Arc<AtomicBool>) -> Self {
        flag.store(true, Ordering::Relaxed);
        Self(flag.clone())
    }
}

impl Drop for GuardTxn {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for ShareDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareDb")
            .field("folder", &self.folder)
            .field("state_dir", &self.state_dir)
            .field("node_id", &hex_encode(&self.node_id))
            .field("schema_version", &self.schema_version)
            .field("applied_count", &self.applied_count)
            .finish()
    }
}

/// A single-process advisory lock held for the lifetime of a [`ShareDb`].
///
/// The lock is a real OS advisory lock: `File::try_lock` (stable since Rust
/// 1.89, `flock(LOCK_EX | LOCK_NB)` on Unix, `LockFileEx` on Windows) taken on
/// the `LOCK` file. The kernel releases it when the owning process exits, so a
/// crashed owner never blocks a later open and two processes can never hold it
/// at the same time — on any platform. The file also carries the owning pid,
/// for diagnostics only; there is no pid-liveness heuristic and therefore no
/// lock-reclaim race and no pid-reuse hazard.
///
/// (The earlier implementation probed `/proc/<pid>`, which never exists on
/// macOS or Windows, so a live owner's `LOCK` was deleted there. That path is
/// gone.)
struct FileLock {
    /// The locked file; dropping it releases the OS lock.
    file: fs::File,
}

impl FileLock {
    /// Open `state_dir/LOCK` (creating it if needed) and take the exclusive OS
    /// lock. A live holder makes `try_lock` fail; a crashed holder released its
    /// lock when the process died, so the file is simply re-locked.
    fn acquire(state_dir: &Path) -> Result<FileLock> {
        let path = state_dir.join("LOCK");
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        match file.try_lock() {
            Ok(()) => {}
            // Only "someone else holds it" means busy. Any other failure (the
            // platform refusing the lock, an I/O error) is an I/O problem, not
            // contention, and must not be reported as `Busy`.
            Err(e @ std::fs::TryLockError::WouldBlock) => {
                return Err(Error::Busy(format!(
                    "another process holds {} ({e})",
                    path.display()
                )));
            }
            Err(e) => {
                return Err(Error::Io(std::io::Error::from(e)));
            }
        }
        file.set_len(0)?;
        let _ = writeln!(file, "{}", std::process::id());
        Ok(FileLock { file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // Releasing the file releases the OS lock. The file is deliberately
        // *not* deleted: unlinking a lock that is still held (or that another
        // process has just taken) is the classic flock+unlink race — A unlocks,
        // B locks the old inode, A unlinks the path, C creates and locks a new
        // inode, and B and C both believe they hold "the" lock. Leaving the
        // file behind costs one small file in the state directory.
        let _ = self.file.unlock();
    }
}

const MANIFEST_NAME: &str = "MANIFEST";
const FORMAT_LINE: &str = "sharedb-format=1";

/// Reads and parses a `MANIFEST`, returning its `db_id`.
///
/// The MANIFEST lives in the synced folder, so it is untrusted input: a
/// truncated or hand-edited file must be reported as [`Error::Manifest`], never
/// panic. A `db_id` that is not exactly 32 hex characters is rejected here.
fn read_manifest(path: &Path) -> Result<DbId> {
    let text = fs::read_to_string(path)
        .map_err(|e| Error::Manifest(format!("cannot read {}: {e}", path.display())))?;
    let mut format_seen = false;
    let mut db_id: Option<DbId> = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == FORMAT_LINE {
            format_seen = true;
        } else if let Some(hex) = line.strip_prefix("db_id=") {
            // A `db_id` is exactly 16 bytes, i.e. 32 hex chars. `hex_decode`
            // accepts any even-length hex, so the length is checked separately:
            // a truncated upload (`db_id=abcd`) or an over-long value is a
            // manifest error, never a panic.
            const DB_ID_HEX_LEN: usize = 32;
            let bytes = hex_decode(hex)
                .ok_or_else(|| Error::Manifest(format!("db_id is not 32 hex chars: {hex}")))?;
            let db_id_bytes = <[u8; 16]>::try_from(bytes).map_err(|_| {
                Error::Manifest(format!(
                    "db_id is not {DB_ID_HEX_LEN} hex chars (decoded {} bytes): {hex}",
                    hex.len() / 2
                ))
            })?;
            db_id = Some(db_id_bytes);
        } else {
            return Err(Error::Manifest(format!("unknown manifest line: {line}")));
        }
    }
    if !format_seen {
        return Err(Error::Manifest("missing sharedb-format line".into()));
    }
    db_id.ok_or_else(|| Error::Manifest("missing db_id line".into()))
}

/// Writes `contents` to `path` atomically: a `*.tmp` file in the same directory,
/// fsynced, then renamed onto `path` (the same discipline as
/// [`crate::folder::RealFs::write_atomic`]).
///
/// A half-written file is never visible under the final name, so a sync client
/// can never upload a truncated MANIFEST.
fn write_atomic(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default()
    ));
    // A temp file left behind by a crashed write would block the rename forever.
    let _ = fs::remove_file(&tmp);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)?;
    let written = file.write_all(contents).and_then(|()| file.sync_all());
    drop(file);
    written?;
    fs::rename(&tmp, path)
}

/// Creates a fresh `MANIFEST` (write-once) and returns its `db_id`.
fn create_manifest(path: &Path) -> Result<DbId> {
    // 16 random bytes rendered as 32 hex chars, from two 8-byte draws.
    let mut hex = materialize::new_id();
    hex.push_str(&materialize::new_id());
    let bytes = hex_decode(&hex).expect("new_id always yields hex");
    let db_id = <[u8; 16]>::try_from(bytes).expect("32 hex chars yield 16 bytes");
    let contents = format!("{FORMAT_LINE}\ndb_id={hex}\n");
    // `create_new` on the temp name keeps two racing creators from both
    // publishing; the fsynced temp + rename means a half-written manifest is
    // never visible to another process or a sync client.
    write_atomic(path, contents.as_bytes())
        .map_err(|e| Error::Manifest(format!("cannot write {}: {e}", path.display())))?;
    Ok(db_id)
}

/// Resolves the state directory and refuses to place it inside the sync root.
///
/// With no explicit `state_dir`, a machine-local directory keyed by the
/// database id is used (see [`default_state_root`]) — deliberately outside the
/// synced folder, which must hold only the manifest and segments.
fn resolve_state_dir(folder: &Path, requested: Option<PathBuf>, db_id: &DbId) -> Result<PathBuf> {
    let state_dir = match requested {
        Some(dir) => dir,
        None => default_state_root().join(hex_encode(db_id)),
    };
    let state_dir = absolutize(&state_dir)?;
    let sync_root = absolutize(folder)?;
    if state_dir.starts_with(&sync_root) {
        return Err(Error::StateDirInsideSyncRoot);
    }
    Ok(state_dir)
}

/// The machine-local root for per-database state directories.
///
/// `$XDG_DATA_HOME/sharepoint-db`, or `$HOME/.local/share/sharepoint-db` when
/// `XDG_DATA_HOME` is unset (Unix); `%LOCALAPPDATA%\sharepoint-db` on Windows.
/// Implemented by hand so no new dependency is needed. When no environment
/// variable is usable, the current directory is the last resort.
fn default_state_root() -> PathBuf {
    #[cfg(windows)]
    {
        if let Some(dir) = std::env::var_os("LOCALAPPDATA") {
            if !dir.is_empty() {
                return PathBuf::from(dir).join("sharepoint-db");
            }
        }
    }
    #[cfg(not(windows))]
    {
        if let Some(dir) = std::env::var_os("XDG_DATA_HOME") {
            if !dir.is_empty() {
                return PathBuf::from(dir).join("sharepoint-db");
            }
        }
        if let Some(home) = std::env::var_os("HOME") {
            if !home.is_empty() {
                return PathBuf::from(home)
                    .join(".local")
                    .join("share")
                    .join("sharepoint-db");
            }
        }
    }
    PathBuf::from(".sharepoint-db")
}

/// Best-effort absolute path with symlinks resolved as far as the path exists,
/// so that a state directory nested under the sync root is detected even
/// through `..` and even when it (or its parents) do not exist yet.
fn absolutize(path: &Path) -> Result<PathBuf> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    // Canonicalize the nearest existing ancestor, then append the remainder.
    let mut ancestor = abs.as_path();
    let mut remainder: Vec<&std::ffi::OsStr> = Vec::new();
    loop {
        match ancestor.canonicalize() {
            Ok(canon) => {
                let mut out = canon;
                for part in remainder.iter().rev() {
                    out.push(part);
                }
                return Ok(out);
            }
            Err(_) => match (ancestor.parent(), ancestor.file_name()) {
                (Some(parent), Some(name)) => {
                    remainder.push(name);
                    ancestor = parent;
                }
                _ => return Ok(abs),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// data.db bookkeeping helpers (all in `__sharedb_meta`, never captured).
// ---------------------------------------------------------------------------

fn data_meta_get(conn: &Connection, key: &str) -> Result<Option<String>> {
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM __sharedb_meta WHERE key = ?1",
            [key],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(value)
}

fn data_meta_set(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO __sharedb_meta (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

fn read_applied(conn: &Connection) -> Result<Option<(u64, [u8; 32])>> {
    let count = data_meta_get(conn, "applied_count")?;
    let digest_hex = data_meta_get(conn, "applied_digest")?;
    let (Some(count), Some(digest_hex)) = (count, digest_hex) else {
        return Ok(None);
    };
    let count: u64 = count
        .parse()
        .map_err(|_| Error::Store("bad applied_count in data.db".into()))?;
    let bytes = hex_decode(&digest_hex)
        .ok_or_else(|| Error::Store("bad applied_digest hex in data.db".into()))?;
    let digest = <[u8; 32]>::try_from(bytes)
        .map_err(|_| Error::Store("bad applied_digest length".into()))?;
    Ok(Some((count, digest)))
}

fn write_applied(conn: &Connection, count: u64, digest: &[u8; 32]) -> Result<()> {
    data_meta_set(conn, "applied_count", &count.to_string())?;
    data_meta_set(conn, "applied_digest", &hex_encode(digest))?;
    Ok(())
}

/// `store.db` meta key marking this node's `data.db` as unusable.
///
/// Written by [`ShareDb::repair_to_store`] when the diverged `data.db` of a
/// bootstrapped node could not be removed (a Windows file lock, an antivirus
/// holding it open). Its phantom row would otherwise be picked up by a later open
/// and served as the node's last-good state, so [`reconcile`] refuses any data
/// file behind this marker and the open fails loudly instead. The marker is
/// cleared once a fresh data file has been installed (bootstrap or rebuild).
const INVALID_DATA_KEY: &str = "invalid_data_db";

/// Key of the marker [`begin_txn_probe`] writes inside the commit transaction.
const TXN_PROBE_KEY: &str = "txn_probe";

/// Writes a fresh, unpredictable marker into `__sharedb_meta` *inside* the
/// currently open commit transaction and returns its value.
///
/// The marker is the only reliable way to tell the two ways a transaction can
/// end up back in autocommit without our `COMMIT`: a user `COMMIT` (the data ran
/// ahead of the store, unrecoverable) and a statement that rolled the
/// transaction back for us (`INSERT OR ROLLBACK` on a constraint hit,
/// `SQLITE_FULL`, an I/O or memory error — the data is untouched and the store
/// still agrees with it). SQLite leaves both in autocommit and its change
/// counters move in both cases; a write made inside the transaction, by
/// contrast, survives exactly when the transaction committed.
///
/// The marker is never cleaned up: the next commit overwrites it, and a rolled
/// back transaction simply restores the previous one.
fn begin_txn_probe(conn: &Connection) -> Result<String> {
    let probe = materialize::new_id();
    data_meta_set(conn, TXN_PROBE_KEY, &probe)?;
    Ok(probe)
}

/// Whether the marker written by [`begin_txn_probe`] is still there, i.e. the
/// transaction it belonged to was committed and `data.db` is ahead of the store.
///
/// `None` means the marker could not be read at all, which says nothing about
/// whether the transaction committed: the caller must not conclude "committed"
/// from a failed probe, or an unreadable probe would disable a handle whose
/// repair succeeds.
fn txn_probe_committed(conn: &Connection, probe: &str) -> Option<bool> {
    data_meta_get(conn, TXN_PROBE_KEY)
        .map(|value| value.as_deref() == Some(probe))
        .ok()
}

fn read_schema_version(conn: &Connection) -> Result<u32> {
    let value = data_meta_get(conn, "schema_version")?;
    Ok(value
        .as_deref()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0))
}

/// The user tables of `data.db`: every table except SQLite internals and the
/// `__sharedb_*` bookkeeping tables.
fn user_tables(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master
         WHERE type = 'table'
           AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
           AND name NOT LIKE '\\_\\_sharedb\\_%' ESCAPE '\\'
         ORDER BY name",
    )?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(names)
}

impl ShareDb {
    /// Opens (and optionally creates) a database rooted at `folder`.
    pub fn open(folder: &Path, opts: OpenOptions) -> Result<ShareDb> {
        let manifest_path = folder.join(MANIFEST_NAME);
        let db_id = if manifest_path.exists() {
            read_manifest(&manifest_path)?
        } else if opts.create {
            fs::create_dir_all(folder)
                .map_err(|e| Error::Manifest(format!("cannot create folder: {e}")))?;
            create_manifest(&manifest_path)?
        } else {
            return Err(Error::NotFound(folder.display().to_string()));
        };

        // The state directory is keyed by `db_id`, so it is resolved after the
        // manifest is known.
        let state_dir = resolve_state_dir(folder, opts.state_dir.clone(), &db_id)?;
        fs::create_dir_all(&state_dir)
            .map_err(|e| Error::Store(format!("cannot create state dir: {e}")))?;
        // The segments directory is created empty now; step 3 fills it.
        fs::create_dir_all(folder.join("segments"))
            .map_err(|e| Error::Manifest(format!("cannot create segments dir: {e}")))?;

        let lock = FileLock::acquire(&state_dir)?;

        let store_conn = store::open(&state_dir.join("store.db"))?;
        // The store's frames are hashed over `db_id`: if the manifest was swapped
        // under a state directory that already holds another database's frames,
        // say so here instead of failing later on a hash check.
        store::verify_db_id(&store_conn, &db_id)?;
        let node_id = store::load_or_create_node_id(&store_conn)?;

        let frames = store::all_frames(&store_conn, &db_id)?;
        let data_path = state_dir.join("data.db");
        let env = ReconcileEnv {
            fs: &RealFs,
            ckpoint_dir: folder.join(checkpoint::CKPOINT_DIR),
            store: &store_conn,
        };
        let reconciled = reconcile(&data_path, &db_id, frames, Some(&env))?;
        let schema_version = read_schema_version(&reconciled.data)?;

        let mut data = reconciled.data;
        let txn_guard = Arc::new(AtomicBool::new(false));
        install_transaction_guard(&mut data, &txn_guard)?;
        // Built from the frames `reconcile` already read: the store is hashed
        // once per open, never per commit.
        let applied_hasher = hasher_for(&reconciled.frames, reconciled.applied_count);

        let mut db = ShareDb {
            folder: folder.to_path_buf(),
            state_dir,
            db_id,
            node_id,
            store: store_conn,
            data,
            schema_version,
            applied_count: reconciled.applied_count,
            applied_hasher,
            applied_digest: reconciled.applied_digest,
            own_seq: 0,
            own_prev_hash: [0u8; 32],
            lamport_max: 0,
            failed: None,
            materialize_pending: reconciled.materialize_pending,
            dropped_below_boundary: reconciled.dropped_below_boundary,
            txn_guard,
            fault: opts.fault,
            checkpoint_threshold: opts.checkpoint_threshold,
            retention_segments: opts.retention_segments,
            fs: Arc::new(RealFs),
            _lock: lock,
        };
        db.refresh_chain();
        Ok(db)
    }

    /// This node's id.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The highest schema version applied on this node.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// The synced folder this database lives in.
    pub fn folder(&self) -> &Path {
        &self.folder
    }

    /// The directory inside the synced folder that holds the segments.
    pub fn segments_dir(&self) -> PathBuf {
        self.folder.join(folder::SEGMENTS_DIR)
    }

    /// The id of this database (from the `MANIFEST`).
    pub fn db_id(&self) -> DbId {
        self.db_id
    }

    /// The local state directory.
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// Arms a fault-injection plan for the tests. Not part of the public API.
    #[doc(hidden)]
    pub fn set_fault(&mut self, fault: Option<FaultPlan>) {
        self.fault = fault;
    }

    /// Replaces the synced-folder access used by [`ShareDb::sync`].
    ///
    /// The seam exists so tests can inject hostile folder behaviour (a segment
    /// that disappears between listing and reading, a rename that keeps failing
    /// with a sharing violation). Not part of the public API.
    #[doc(hidden)]
    pub fn set_folder_fs(&mut self, fs: Arc<dyn FolderFs>) {
        self.fs = fs;
    }
}

/// Whether `sql` is a transaction-control statement.
///
/// SQLite reports `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` and `RELEASE` as
/// read-only (`sqlite3_stmt_readonly`), so [`ShareDb::query`] has to reject them
/// by name: running one would end or nest the transaction the commit protocol
/// owns.
fn is_transaction_control(sql: &str) -> bool {
    let first = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == ';')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(
        first.as_str(),
        "BEGIN" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE"
    )
}

/// Installs an authorizer on `conn` that denies the statements the commit
/// protocol must own — `BEGIN`/`COMMIT`/`ROLLBACK` (`SQLITE_TRANSACTION`),
/// savepoints (`SQLITE_SAVEPOINT`) and `ATTACH` — while `guard` is raised.
///
/// The authorizer lives for the lifetime of the connection; the flag decides
/// whether it is active, so the protocol's own `BEGIN`/`COMMIT` (issued outside
/// a raised guard) are unaffected.
fn install_transaction_guard(conn: &mut Connection, guard: &Arc<AtomicBool>) -> Result<()> {
    let flag = guard.clone();
    conn.authorizer(Some(move |ctx: AuthContext<'_>| {
        if !flag.load(Ordering::Relaxed) {
            return Authorization::Allow;
        }
        match ctx.action {
            AuthAction::Transaction { .. }
            | AuthAction::Savepoint { .. }
            | AuthAction::Attach { .. } => Authorization::Deny,
            _ => Authorization::Allow,
        }
    }))?;
    Ok(())
}

/// What [`reconcile`] produced: a usable data connection, its bookkeeping and
/// the frames it was brought in line with (so the caller can build the running
/// digest hasher without re-reading the store).
struct Reconciled {
    data: Connection,
    applied_count: u64,
    applied_digest: [u8; 32],
    frames: Vec<store::StoredFrame>,
    /// How the data file was brought in line: replayed onto the existing file
    /// ([`MaterializeKind::Fast`]) or rebuilt from scratch
    /// ([`MaterializeKind::Rebuilt`]).
    kind: MaterializeKind,
    /// Frames below a checkpoint's boundary that could not be applied because the
    /// checkpoint failed the USE gate and the store is behind that cut. Reported,
    /// never silently applied (see [`crate::checkpoint`]).
    dropped_below_boundary: usize,
    /// A bootstrapped node could not be brought in line with its store (no
    /// dominating, gate-passing checkpoint) and kept its last-good `data.db`
    /// instead of a store-only rebuild. Reads still work off that file; the node
    /// retries a re-bootstrap on every later sync (see [`crate::db`] R2).
    materialize_pending: bool,
}

/// The folder access, paths and store access a reconcile may need in order to
/// bootstrap from a checkpoint (see [`crate::checkpoint`]).
///
/// `None` is passed by the repair path, which must never reach into the synced
/// folder: a commit that failed to record its frame is put back in line with the
/// local store alone.
struct ReconcileEnv<'a> {
    fs: &'a dyn FolderFs,
    ckpoint_dir: PathBuf,
    /// Opened against `store.db`: the [`checkpoint::Base`] record lives there, so
    /// a bootstrap-aware reconcile has to read it.
    store: &'a Connection,
}

/// The running digest hasher over the first `applied` frame hashes, in total
/// order.
///
/// Built once per open (and once per rebuild) from frames already in memory, so
/// a commit never re-hashes the whole store. `applied` is the applied count the
/// data file records; for a bootstrapped node held pending it can be fewer than
/// `frames.len()` (the store gained frames the stale data file did not apply).
fn hasher_for(frames: &[store::StoredFrame], applied: u64) -> sha2::Sha256 {
    use sha2::Digest as _;
    let mut h = sha2::Sha256::new();
    for f in frames.iter().take(applied as usize) {
        h.update(f.frame.hash);
    }
    h
}

/// Opens `data_path` and brings it in line with `frames`.
///
/// Returns the (possibly rebuilt) data connection and the applied bookkeeping.
/// A missing, unreadable (garbage), foreign, or stale `data.db` is brought in
/// line by the cheapest of three routes:
///
/// 1. **fast** — replay the frames beyond the ones the file already recorded;
/// 2. **checkpoint** — install a checkpoint snapshot as the base and replay only
///    the frames its version vector does not cover (see [`crate::checkpoint`]);
/// 3. **rebuild** — replay every frame into a fresh file and swap it in.
///
/// Any error on the fast path — bad bookkeeping *or* a failing replay — means
/// the data file cannot be trusted and falls through to the next route rather
/// than being reported as an open failure. One exception: a node that carries a
/// checkpoint [`checkpoint::Base`] may never fall through to route 3, because a
/// store-only replay would drop the state the snapshot carried below the
/// boundary; such a node is re-bootstrapped, and if no checkpoint passes the USE
/// gate the open fails instead of silently losing data.
fn reconcile(
    data_path: &Path,
    db_id: &DbId,
    frames: Vec<store::StoredFrame>,
    env: Option<&ReconcileEnv<'_>>,
) -> Result<Reconciled> {
    let total = frames.len() as u64;
    let refs: Vec<FrameRef> = frames.iter().map(|f| f.as_ref()).collect();
    let full_digest = digest(&refs);
    // The Base lives in store.db, so a node knows it is bootstrapped even when
    // `data.db` is missing or garbage — exactly the case this function exists to
    // repair. Without a store handle (the repair path) no bootstrap is possible.
    let base = match env {
        Some(env) => store::base(env.store)?,
        None => None,
    };
    // A data file the store marked invalid (its phantom row could not be removed,
    // see [`INVALID_DATA_KEY`]) counts as absent: neither the fast path nor the
    // pending fallback may serve it. A store that cannot answer counts as marked —
    // the marker only guards against serving a file the store cannot account for.
    let invalid = match env {
        Some(env) => match store::meta_get(env.store, INVALID_DATA_KEY) {
            Ok(value) => value.is_some(),
            Err(_) => true,
        },
        // No store handle (the repair path): no marker to consult.
        None => false,
    };

    // Try the fast path against the existing data.db, if it is usable.
    if let Some(conn) = (!invalid).then(|| try_open_data(data_path)).flatten() {
        // Any fast-path error — bad bookkeeping or a failing replay — means the
        // data file cannot be trusted: fall through instead of failing the open.
        if fast_forward(&conn, db_id, &frames, total, &full_digest, base.as_ref()).is_ok() {
            return Ok(Reconciled {
                data: conn,
                applied_count: total,
                applied_digest: full_digest,
                frames,
                kind: MaterializeKind::Fast,
                dropped_below_boundary: 0,
                materialize_pending: false,
            });
        }
        // A failed replay means the data file disagrees with the store in a way
        // we cannot repair incrementally: drop it and try the cheaper of the two
        // full-materialise routes.
        drop(conn);
    }

    // A checkpoint in the folder can stand in for the frames it folded in. It is
    // used when the store cannot rebuild the state on its own: either the store
    // is *behind* the checkpoint (a new node, or one that lost its state), or the
    // node bootstrapped before and may only be rebuilt the same way. A node with
    // full history keeps using a plain store replay, which is cheaper than
    // trusting a snapshot. `read_best` has already applied the USE gate, so a
    // checkpoint whose snapshot does not account for everything this node holds
    // below the boundary is not returned at all.
    let read = env.map(|env| checkpoint::read_best(env.fs, &env.ckpoint_dir, db_id, &frames));
    let read_dropped = read.as_ref().map_or(0, |r| r.dropped_below_boundary);
    let best = read.and_then(|r| r.best);
    // R3: a bootstrapped node may only (re-)bootstrap from a checkpoint whose
    // version vector dominates its current base. A checkpoint that is lower for
    // some node would drop frames the current base's snapshot relied on, so it is
    // not usable here (in addition to the USE gate `read_best` already applied).
    let best = best.filter(|cp| match &base {
        Some(b) => b.dominates(&cp.vv),
        None => true,
    });
    let use_checkpoint = best
        .as_ref()
        .is_some_and(|best| base.is_some() || total < best.count);
    if use_checkpoint {
        let best = best.expect("checked above");
        let bootstrap_result = match env {
            Some(env) => bootstrap(
                data_path,
                db_id,
                env.store,
                &frames,
                total,
                &full_digest,
                &best,
            ),
            // No store handle: a bootstrap could not record its Base, so it must
            // not be attempted. The repair path never reaches here (it passes no
            // checkpoint directory either).
            None => Err(Error::Store(
                "cannot bootstrap without the frame store".into(),
            )),
        };
        match bootstrap_result {
            Ok(data) => {
                return Ok(Reconciled {
                    data,
                    applied_count: total,
                    applied_digest: full_digest,
                    frames,
                    kind: MaterializeKind::Rebuilt,
                    dropped_below_boundary: 0,
                    materialize_pending: false,
                })
            }
            // A snapshot that cannot be installed (unreadable, or it disagrees
            // with the store once replayed) is a wasted read for a node that can
            // still replay its whole store — but a bootstrapped node must not
            // fall through to a store-only rebuild, so it is held pending below.
            Err(err) if base.is_none() => {
                let _ = err;
            }
            Err(err) => {
                let base = base
                    .as_ref()
                    .expect("the `base.is_none()` arm was checked above");
                if let Some(kept) = keep_pending(data_path, frames, base, read_dropped, invalid) {
                    return Ok(kept);
                }
                return Err(err);
            }
        }
    }
    if let Some(base) = base.as_ref() {
        // R2: a bootstrapped node is never rebuilt from its store alone — its
        // store does not hold the frames below the cut that the snapshot carried.
        // Keep the last-good `data.db` and its bookkeeping unchanged, do not
        // advance `applied_count`, mark the handle pending and carry the dropped
        // count forward; the node retries a re-bootstrap on every later sync.
        // With no last-good file to keep (missing or garbage) this is the loss
        // case and the open fails rather than silently losing data.
        if let Some(kept) = keep_pending(data_path, frames, base, read_dropped, invalid) {
            return Ok(kept);
        }
        return Err(Error::Store(
            "a bootstrapped node cannot rebuild data.db from its store alone and no dominating \
             checkpoint is available; the state below the cut is not recoverable from the folder"
                .into(),
        ));
    }

    // Rebuild from scratch into a temp file, then swap it in.
    let mut reconciled = rebuild(data_path, db_id, frames, total, &full_digest)?;
    // The swap installed a fresh data file, so any invalid marker is stale now.
    if invalid {
        if let Some(env) = env {
            env.store
                .execute("DELETE FROM meta WHERE key = ?1", [INVALID_DATA_KEY])?;
        }
    }
    reconciled.dropped_below_boundary = read_dropped;
    Ok(reconciled)
}

/// Reopen the existing `data_path` and return it unchanged, as the state a
/// bootstrapped node keeps when it cannot re-bootstrap (R2).
///
/// The data file's own `(applied_count, applied_digest)` bookkeeping is kept
/// exactly as it stands — `applied_count` is deliberately **not** advanced to
/// `frames.len()`, because the store holds frames this data file never applied
/// and replaying them would drop the state below the cut. The handle is marked
/// `materialize_pending` and reports the dropped count (the greater of what the
/// checkpoint scan found and what this base's snapshot folded in that the store
/// does not hold), so the caller carries it forward rather than resetting it.
///
/// Returns `None` when there is no usable data file to keep (missing, garbage,
/// without bookkeeping, or marked invalid by [`INVALID_DATA_KEY`]): then there is
/// nothing stale-but-correct to fall back on, and the caller fails the open rather
/// than report a phantom row as this node's last-good state.
fn keep_pending(
    data_path: &Path,
    frames: Vec<store::StoredFrame>,
    base: &checkpoint::Base,
    read_dropped: usize,
    invalid: bool,
) -> Option<Reconciled> {
    if invalid {
        return None;
    }
    let conn = try_open_data(data_path)?;
    let (count, digest) = read_applied(&conn).ok().flatten()?;
    let dropped = read_dropped.max(base.missing_from_store(&frames));
    Some(Reconciled {
        data: conn,
        applied_count: count,
        applied_digest: digest,
        frames,
        kind: MaterializeKind::UpToDate,
        dropped_below_boundary: dropped,
        materialize_pending: true,
    })
}

/// Replays the frames beyond the ones `conn` already recorded, then moves its
/// bookkeeping to `(total, full_digest)`.
///
/// Fails — leaving the bookkeeping untouched — when the recorded frames are not
/// a prefix of `frames` in total order (which the digest check detects) or when
/// a replayed frame cannot be applied.
///
/// `base` is this node's [`checkpoint::Base`], when it bootstrapped. A bootstrapped
/// data file already carries every frame the checkpoint's version vector covers,
/// so a frame that arrives later and *is* covered — a peer frame inside the
/// retention window that this node's store did not hold at bootstrap time — must be
/// counted but **not** replayed, or its effect would be applied twice.
fn fast_forward(
    conn: &Connection,
    db_id: &DbId,
    frames: &[store::StoredFrame],
    total: u64,
    full_digest: &[u8; 32],
    base: Option<&checkpoint::Base>,
) -> Result<()> {
    let (count, stored) = read_applied(conn)?
        .ok_or_else(|| Error::Store("data.db has no applied bookkeeping".into()))?;
    if count > total || store::digest_first(frames, count as usize) != stored {
        return Err(Error::Store(
            "data.db digest disagrees with the store".into(),
        ));
    }
    // A bootstrapped node's data file already carries the base's snapshot, which
    // folded in every frame up to `base.boundary`. The fast path replays the
    // remainder straight onto that snapshot, so it is only correct while every
    // frame it replays sorts at or above the boundary. A frame that is NOT covered
    // by the base and sorts strictly BELOW `base.boundary` sorts before frames the
    // snapshot already folded in: replaying it on top of the snapshot hands the
    // conflict handler a total order that disagrees with a full-history node's
    // rebuild (a colliding row gets the wrong value), and the node reports
    // `dropped = 0`, so the divergence is silent and permanent. Refuse the fast
    // path here; `reconcile` then falls through to a re-bootstrap, and if none is
    // possible it keeps the last-good file and goes pending (stale but never
    // wrong), recovering once a covering checkpoint appears.
    if let Some(base) = base {
        let replays_below_boundary = frames
            .iter()
            .skip(count as usize)
            .filter(|f| !base.covered(&f.node_id, f.frame.seq))
            .any(|f| checkpoint::sort_key_of(f) < base.boundary);
        if replays_below_boundary {
            return Err(Error::Store(
                "a bootstrapped node cannot fast-path a below-boundary frame the base does not \
                 cover; it would replay out of order on top of the snapshot"
                    .into(),
            ));
        }
    }
    // The fast path replays the frames the store gained *after* the ones already
    // applied, so it is only correct while the applied frames are still a prefix
    // of the total order. The digest check above is what detects a late arrival
    // that sorts earlier: it re-hashes the first `count` frames of the *new*
    // order, which differ from the applied ones as soon as a new frame
    // interleaves into the applied prefix.
    // Replay and bookkeeping commit together: a frame that cannot be applied
    // must not leave the counters moved. `PRAGMA foreign_keys` is a no-op inside
    // a transaction, so it is lowered here, before `BEGIN`, and restored after.
    let fk_before: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    let result = (|| -> Result<()> {
        conn.execute_batch("BEGIN")?;
        let mut rollback = RollbackOnDrop::new(conn);
        let remainder: Vec<FrameRef> = frames
            .iter()
            .skip(count as usize)
            // A bootstrapped node's data file already carries the covered
            // frames, so they are counted in the new bookkeeping but skipped by
            // the replay (see the function docs).
            .filter(|f| !base.is_some_and(|base| base.covered(&f.node_id, f.frame.seq)))
            .map(|f| f.as_ref())
            .collect();
        replay(db_id, &remainder, conn)?;
        write_applied(conn, total, full_digest)?;
        conn.execute_batch("COMMIT")?;
        rollback.disarm();
        Ok(())
    })();
    conn.pragma_update(None, "foreign_keys", fk_before)?;
    result
}

/// Installs `cp`'s snapshot as `data_path` and replays the store frames that the
/// checkpoint's version vector does **not** cover onto it.
///
/// The snapshot is the materialised state of exactly the covered frames, so those
/// need no replay — which is the whole point of the checkpoint — and the uncovered
/// ones are exactly what the snapshot cannot account for. Coverage, not the sort
/// key, is the authority: a frame can sort below the boundary and still be
/// uncovered (a peer frame this node picked up after the cut was taken), and
/// replaying it from the store is required, while a covered frame must never be
/// replayed a second time. This covers both bootstrap cases uniformly:
///
/// * a node with full history: its uncovered frames are precisely the ones beyond
///   `count`, and [`checkpoint::cut_matches`] already proved its first `count`
///   frames *are* the checkpoint's;
/// * a new node (or one that lost its state): it ingests the segments the folder
///   retained above the boundary, and those are the uncovered ones.
///
/// The Base record — count, boundary, cut digest and the version vector — is
/// written to `store.db`, not to `data.db`: `store.db` is never pruned and is not
/// rebuilt from the folder, so the node still knows what its snapshot accounts for
/// after a lost `data.db`.
///
/// The bookkeeping describes the *store* — `(total, full_digest)` — so the
/// invariant `applied_count == store frames` that the commit protocol and
/// [`ShareDb::materialize_ingested`] rely on keeps holding, and the existing
/// digest fast path is unchanged: a later below-boundary arrival makes the
/// applied digest disagree with the store prefix and triggers the existing
/// full-store rebuild.
fn bootstrap(
    data_path: &Path,
    db_id: &DbId,
    store_conn: &Connection,
    frames: &[store::StoredFrame],
    total: u64,
    full_digest: &[u8; 32],
    cp: &checkpoint::Checkpoint,
) -> Result<Connection> {
    let conn = checkpoint::restore(&cp.snapshot, data_path)?;
    let fk_before: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    let result = (|| -> Result<()> {
        conn.execute_batch("BEGIN")?;
        let mut rollback = RollbackOnDrop::new(&conn);
        let uncovered: Vec<FrameRef> = frames
            .iter()
            .filter(|f| !cp.covered(&f.node_id, f.frame.seq))
            .map(|f| f.as_ref())
            .collect();
        replay(db_id, &uncovered, &conn)?;
        write_applied(&conn, total, full_digest)?;
        conn.execute_batch("COMMIT")?;
        rollback.disarm();
        Ok(())
    })();
    conn.pragma_update(None, "foreign_keys", fk_before)?;
    result?;
    // The Base is recorded only once the bootstrap actually succeeded: a node
    // that believes it is bootstrapped while its data file is not would refuse
    // every later store-only rebuild.
    store::set_base(store_conn, &checkpoint::Base::of(cp))?;
    // The snapshot just installed is a fresh data file: clear any invalid marker.
    store_conn.execute("DELETE FROM meta WHERE key = ?1", [INVALID_DATA_KEY])?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").ok();
    Ok(conn)
}

/// Opens `data_path` and checks it is a usable `sharepoint-db` data file.
///
/// Returns `None` when the file is missing, is not a database (garbage), or is a
/// valid database that predates our bookkeeping. Detection of garbage relies on
/// SQLite rejecting the file the moment we touch it: `ensure_meta` (a `CREATE
/// TABLE`) fails with "file is not a database" on random bytes.
fn try_open_data(data_path: &Path) -> Option<Connection> {
    if !data_path.exists() {
        return None;
    }
    let conn = Connection::open(data_path).ok()?;
    // Touch the file: a garbage file errors here and we treat it as unusable.
    if ensure_meta(&conn).is_err() {
        return None;
    }
    Some(conn)
}

/// Replays every frame into a fresh data file and swaps it into place.
fn rebuild(
    data_path: &Path,
    db_id: &DbId,
    frames: Vec<store::StoredFrame>,
    total: u64,
    full_digest: &[u8; 32],
) -> Result<Reconciled> {
    let tmp = data_path.with_extension("db.rebuild");
    let _ = fs::remove_file(&tmp);
    {
        let conn = Connection::open(&tmp)?;
        // Replay and bookkeeping commit together, exactly as on the fast path.
        let fk_before: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
        conn.pragma_update(None, "foreign_keys", "OFF")?;
        let result = (|| -> Result<()> {
            conn.execute_batch("BEGIN")?;
            let mut rollback = RollbackOnDrop::new(&conn);
            let refs: Vec<FrameRef> = frames.iter().map(|f| f.as_ref()).collect();
            replay(db_id, &refs, &conn)?;
            write_applied(&conn, total, full_digest)?;
            conn.execute_batch("COMMIT")?;
            rollback.disarm();
            Ok(())
        })();
        conn.pragma_update(None, "foreign_keys", fk_before)?;
        result?;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").ok();
    }
    // Replace the old data.db (and any sidecar journals) with the fresh copy.
    // A file that cannot be removed makes the rename below fail.
    let _ = remove_db_sidecars(data_path);
    fs::rename(&tmp, data_path)
        .map_err(|e| Error::Store(format!("cannot swap rebuilt data.db: {e}")))?;
    let conn = Connection::open(data_path)?;
    Ok(Reconciled {
        data: conn,
        applied_count: total,
        applied_digest: *full_digest,
        frames,
        kind: MaterializeKind::Rebuilt,
        dropped_below_boundary: 0,
        materialize_pending: false,
    })
}

/// Removes `data.db` and its `-wal`/`-shm`/`-journal` sidecars.
///
/// The sidecars are best-effort — a missing journal is the normal case — and a
/// missing `data.db` counts as removed. The error for `path` itself is returned:
/// a caller that must not leave the file behind (the repair of a bootstrapped
/// node's diverged `data.db`, whose phantom row must never be served again) has
/// to know that the file survived.
pub(crate) fn remove_db_sidecars(path: &Path) -> std::io::Result<()> {
    let removed = fs::remove_file(path);
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut p = path.as_os_str().to_os_string();
        p.push(suffix);
        let _ = fs::remove_file(PathBuf::from(p));
    }
    match removed {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// A user-facing transaction handle. All SQL run through it is captured into a
/// single frame when the enclosing [`ShareDb::transaction`] closure returns
/// `Ok`.
pub struct Transaction<'a> {
    conn: &'a Connection,
}

impl Transaction<'_> {
    /// Runs a statement, returning the number of rows changed.
    pub fn execute<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<usize> {
        Ok(self.conn.execute(sql, params)?)
    }

    /// Runs a read query, mapping each row with `f`.
    pub fn query<T, P, F>(&self, sql: &str, params: P, mut f: F) -> Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row) -> Result<T>,
    {
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(f(row)?);
        }
        Ok(out)
    }
}

impl ShareDb {
    /// Runs `f` inside one captured transaction and, if it changed any user
    /// table, records the change as a single frame. Returns whatever `f`
    /// produced.
    ///
    /// This is the commit protocol: `data.db` commits first, then the frame is
    /// appended to `store.db` (the durability point). If the frame cannot be
    /// appended, `data.db` is immediately rebuilt from the store, so the data
    /// file can never stay ahead of the frame log.
    fn commit_capture<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        self.ensure_usable()?;

        // Phase 1 borrows the handle immutably: the capture session holds a
        // borrow of `self.data`, so nothing mutable may happen until it ends.
        let captured = {
            let this = &*self;
            let tables = user_tables(&this.data)?;
            let schema_before: i64 = this
                .data
                .query_row("PRAGMA schema_version", [], |row| row.get(0))?;
            let mut session = materialize::attach_session(&this.data, &tables)?;
            this.data.execute_batch("BEGIN")?;
            let mut rollback = RollbackOnDrop::new(&this.data);
            // Written inside the transaction: its survival tells a user `COMMIT`
            // apart from a statement that rolled the transaction back for us.
            let probe = begin_txn_probe(&this.data)?;
            let body = (|| -> Result<(T, Option<Frame>)> {
                let out = {
                    let _guard = GuardTxn::new(&this.txn_guard);
                    f(&this.data)?
                };
                if this.data.is_autocommit() {
                    // The authorizer should have denied this; keep the check as
                    // a fallback for paths it cannot see.
                    return Err(Error::unsupported(
                        "the closure ended the transaction; the commit protocol must own BEGIN/COMMIT",
                    ));
                }
                let schema_after: i64 =
                    this.data
                        .query_row("PRAGMA schema_version", [], |row| row.get(0))?;
                if schema_after != schema_before {
                    return Err(Error::SchemaChangeInExecute);
                }
                let cs = changeset(&mut session)?;
                if cs.is_empty() {
                    // Nothing changed: commit the (empty) transaction, write no frame.
                    this.data.execute_batch("COMMIT")?;
                    rollback.disarm();
                    return Ok((out, None));
                }
                let next = this.build_frame(FrameKind::Changeset, cs)?;
                this.bump_applied_in_txn(&next)?;
                this.data.execute_batch("COMMIT")?;
                rollback.disarm();
                Ok((out, Some(next)))
            })();
            // Back in autocommit without our `COMMIT` has two very different
            // causes. A user `COMMIT` means `data.db` ran ahead of the store and
            // is unrecoverable; a statement that rolled the transaction back for
            // us (`INSERT OR ROLLBACK` on a constraint hit, `SQLITE_FULL`, an
            // I/O or memory error) left `data.db` exactly as the store recorded
            // it, and the handle must stay usable. The probe written inside the
            // transaction tells the two apart: it survives only if the
            // transaction committed. A probe that could not be read at all tells
            // us nothing, so it must not by itself condemn the handle.
            let externally_ended = body.is_err() && this.data.is_autocommit();
            let probe = txn_probe_committed(&this.data, &probe);
            let committed_externally = externally_ended && probe == Some(true);
            let probe_unreadable = externally_ended && probe.is_none();
            drop(rollback);
            (body, committed_externally, probe_unreadable)
        };
        let (body, committed_externally, probe_unreadable) = captured;
        let (out, frame) = match body {
            Ok(pair) => pair,
            Err(err) => {
                if committed_externally {
                    // The closure committed inside our transaction: `data.db` is
                    // ahead of the store and the store can never account for it.
                    // Put the file back in line, then refuse the handle: the
                    // unlogged change is lost, so nothing may build on it. Any
                    // other failure that merely rolled the transaction back left
                    // `data.db` in line with the store, so the handle survives.
                    self.repair_to_store();
                    self.failed.get_or_insert_with(|| {
                        "the closure ended the transaction; the commit protocol must own BEGIN/COMMIT"
                            .to_string()
                    });
                } else if probe_unreadable {
                    // The probe could not be read, so a user `COMMIT` cannot be
                    // ruled out: rebuild `data.db` from the store to be safe.
                    // For a node with full history `repair_to_store` marks the
                    // handle failed only when that repair itself fails, so an
                    // unreadable probe alone never disables such a handle. A node
                    // that carries a `Base` is the exception: it may never be
                    // store-only-rebuilt, so the repair invalidates its `data.db`
                    // and fails the handle instead. That costs availability until
                    // the next re-bootstrap, but never correctness — the alternative
                    // is a divergence no one reports.
                    self.repair_to_store();
                }
                return Err(err);
            }
        };

        // Phase 2: the store write (the durability point) and the bookkeeping.
        let Some(frame) = frame else {
            // An empty transaction: nothing to record.
            return Ok(out);
        };
        let recorded = self.maybe_fault().and_then(|()| self.record_frame(&frame));
        if let Err(err) = recorded {
            // `data.db` holds a change the store never recorded: put it back in
            // line with the store before the error reaches the caller.
            self.repair_to_store();
            return Err(err);
        }
        self.applied_count += 1;
        {
            use sha2::Digest as _;
            self.applied_hasher.update(frame.hash);
            self.applied_digest = self.applied_hasher.clone().finalize().into();
        }
        self.own_seq = frame.seq;
        self.own_prev_hash = frame.hash;
        self.lamport_max = frame.lamport;
        Ok(out)
    }

    /// Appends `frame` to the store (the durability point).
    fn record_frame(&self, frame: &Frame) -> Result<()> {
        store::append_frame(&self.store, &self.node_id, frame)
    }

    /// Rebuilds `data.db` from the frame store and swaps it into this handle,
    /// used after a commit that `data.db` recorded but the store did not.
    ///
    /// On success the handle is usable again; on failure it is marked failed so
    /// no later call can build on a data file that disagrees with the store.
    ///
    /// A node that carries a [`checkpoint::Base`] is never repaired this way: a
    /// store-only rebuild would drop everything the checkpoint's snapshot carried
    /// below the cut, which is exactly the state a bootstrapped node does not hold
    /// in its store. Nor may it keep the `data.db` the failed commit left behind:
    /// phase 1 already committed the change plus bookkeeping `(N+1)` into it, and
    /// only the store write failed, so that file now holds a phantom row the store
    /// never recorded. A later commit would reuse the same `seq`/lamport and rewrite
    /// the bookkeeping so `data.db` and the store agree *on paper* while the phantom
    /// row survives — a row only this node ever sees, reported nowhere. Instead the
    /// diverged `data.db` is invalidated (removed with its sidecars) and the handle
    /// is marked failed, so the next open re-bootstraps from a dominating, gate-
    /// passing checkpoint or fails loudly. The inverse changeset is deliberately not
    /// applied: it could not be trusted to undo exactly the failed commit.
    ///
    /// The `AfterDataCommitAndRepairFails` fault fires *just before* the rebuild
    /// call, so the placeholder swap, the failed rebuild and the `failed`
    /// assignment all run exactly as they would for a real rebuild failure.
    ///
    /// The `Base` read is matched, not swallowed: this runs right after a store
    /// write failed, so the read can fail the very same way, and "the store could
    /// not say" must never be read as "not bootstrapped" — that would store-only
    /// rebuild a bootstrapped node, drop the state its snapshot carried below the
    /// cut, and stamp the rebuild's `(total, full_digest)` as applied bookkeeping,
    /// which the next open would then fast-path in silence.
    fn repair_to_store(&mut self) {
        // Close any transaction the failed path may have left open.
        if !self.data.is_autocommit() {
            let _ = self.data.execute_batch("ROLLBACK");
        }
        // Bound before the match: the arms mutate `self`.
        let base = self.read_base();
        match base {
            Ok(None) => {}
            _ => {
                // F3: a bootstrapped node must not keep the diverged data.db. Drop
                // the handle off the file, invalidate it, and refuse the handle so
                // the next open re-bootstraps from a dominating checkpoint (or
                // fails loudly). A `Base` read that failed takes the same branch:
                // the node may well be bootstrapped, and a store-only rebuild is
                // the one thing that must not happen here.
                if let Ok(mem) = Connection::open_in_memory() {
                    self.data = mem;
                }
                let removed = if self.fault == Some(FaultPlan::BaseRepairRemoveFails) {
                    // Models a `data.db` the OS will not hand back (Windows file
                    // lock, antivirus): the file survives with its phantom row.
                    Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "fault: data.db could not be removed",
                    ))
                } else {
                    remove_db_sidecars(&self.state_dir.join("data.db"))
                };
                let mut reason = match &base {
                    Err(err) => format!(
                        "the store could not say whether this node is bootstrapped ({err}); a \
                         commit recorded its change in data.db but not in the store, so the \
                         diverged data.db was invalidated and the node must re-bootstrap"
                    ),
                    _ => "a bootstrapped node's commit recorded its change in data.db but not \
                         in the store; the diverged data.db was invalidated and the node must \
                         re-bootstrap"
                        .to_string(),
                };
                if removed.is_err() {
                    // The phantom row is still on disk. Record the file as invalid
                    // in `store.db` so no later open serves it: `reconcile` refuses
                    // a data file behind that marker, so the node fails loudly
                    // instead of reporting a row the store never recorded as its
                    // last-good (pending) state.
                    reason = format!("{reason}; the file itself could not be removed");
                    if let Err(err) = self.mark_data_invalid() {
                        reason = format!(
                            "{reason}, and the store refused to record it as invalid ({err})"
                        );
                    }
                }
                self.failed = Some(reason);
                return;
            }
        }
        // Move the handle off `data.db` before rebuilding: the rebuild deletes
        // and re-creates the file, which a still-open handle would fight with on
        // Windows. The placeholder is replaced by the rebuilt connection below.
        if let Ok(mem) = Connection::open_in_memory() {
            self.data = mem;
        }
        let rebuilt = (|| -> Result<Reconciled> {
            let frames = store::all_frames(&self.store, &self.db_id)?;
            let total = frames.len() as u64;
            let refs: Vec<FrameRef> = frames.iter().map(|f| f.as_ref()).collect();
            let full_digest = digest(&refs);
            // The injected repair failure fires here — after the placeholder
            // swap, before the rebuild — so the real `Err` branch below runs.
            if self.fault == Some(FaultPlan::AfterDataCommitAndRepairFails) {
                return Err(Error::Injected(
                    "could not rebuild data.db after a failed commit".into(),
                ));
            }
            let r = rebuild(
                &self.state_dir.join("data.db"),
                &self.db_id,
                frames,
                total,
                &full_digest,
            )?;
            let mut data = r.data;
            install_transaction_guard(&mut data, &self.txn_guard)?;
            Ok(Reconciled {
                data,
                applied_count: r.applied_count,
                applied_digest: r.applied_digest,
                frames: r.frames,
                kind: r.kind,
                dropped_below_boundary: 0,
                materialize_pending: false,
            })
        })();
        match rebuilt {
            Ok(r) => {
                self.data = r.data;
                self.applied_count = r.applied_count;
                self.applied_digest = r.applied_digest;
                self.applied_hasher = hasher_for(&r.frames, r.applied_count);
                self.refresh_chain();
                self.schema_version = read_schema_version(&self.data).unwrap_or(0);
            }
            Err(err) => {
                self.failed = Some(format!(
                    "could not rebuild data.db after a failed commit: {err}"
                ));
            }
        }
    }

    /// This node's [`checkpoint::Base`], or the error the store reported.
    ///
    /// Every in-handle read of the Base goes through here, so the
    /// [`FaultPlan::StoreBaseReadFails`] fault can model a `store.db` whose reads
    /// fail. A caller must then fail closed, never assume "not bootstrapped".
    fn read_base(&self) -> Result<Option<checkpoint::Base>> {
        if self.fault == Some(FaultPlan::StoreBaseReadFails)
            || self.fault == Some(FaultPlan::AfterDataCommitAndStoreBaseReadFails)
        {
            return Err(Error::Injected(
                "fault: store.db could not read the checkpoint base".into(),
            ));
        }
        store::base(&self.store)
    }

    /// Records that this node's `data.db` must never be opened again (see
    /// [`INVALID_DATA_KEY`]).
    fn mark_data_invalid(&self) -> Result<()> {
        store::meta_set(&self.store, INVALID_DATA_KEY, "1")
    }

    /// Re-reads this node's chain position and the global lamport high-water mark.
    ///
    /// The lamport high-water mark is the greater of the store's maximum and, for
    /// a bootstrapped node, its [`checkpoint::Base`]'s `cut_lamport`: a bootstrapped
    /// node's store does not hold the frames below the cut, so its store maximum
    /// can be far below the cut. Stamping a new own frame below the cut would make
    /// it sort before frames every other node already folded into the snapshot, so
    /// the peer's conflict handler would drop or misorder it — a silent divergence.
    /// Starting at the cut keeps the commit protocol's assumption that a new own
    /// frame sorts last in the total order true for a bootstrapped node too.
    fn refresh_chain(&mut self) {
        let last = store::last_own_frame(&self.store, &self.db_id, &self.node_id)
            .ok()
            .flatten();
        self.own_seq = last.as_ref().map(|f| f.frame.seq).unwrap_or(0);
        self.own_prev_hash = last.as_ref().map(|f| f.frame.hash).unwrap_or([0u8; 32]);
        let store_max = store::max_lamport(&self.store, &self.db_id).unwrap_or(0);
        self.lamport_max = match self.read_base() {
            Ok(base) => store_max.max(base.map(|b| b.cut_lamport).unwrap_or(0)),
            // The Base could not be read, so the cut is unknown. Dropping to 0 here
            // reintroduces F2: a bootstrapped node would stamp its next commit
            // below the cut, where every peer has already folded the snapshot's
            // frames into its own, and the conflict handler would drop or misorder
            // it in silence. Keep the floor the handle already carries instead — a
            // handle that has ever read its Base successfully carries one at or
            // above the cut — so the handle stays usable and every stamp it makes
            // is safe.
            Err(_) => self.lamport_max.max(store_max),
        };
    }

    /// Fails if this handle was marked unusable by an earlier unrecoverable error.
    fn ensure_usable(&self) -> Result<()> {
        match &self.failed {
            Some(reason) => Err(Error::Failed(reason.clone())),
            None => Ok(()),
        }
    }

    /// Builds the next frame for this node from its own hash chain and the
    /// global lamport clock, both kept in memory.
    fn build_frame(&self, kind: FrameKind, payload: Vec<u8>) -> Result<Frame> {
        Ok(Frame::new(
            &self.db_id,
            &self.node_id,
            self.own_seq + 1,
            self.lamport_max + 1,
            kind,
            self.own_prev_hash,
            payload,
        ))
    }

    /// Updates the `(applied_count, applied_digest)` bookkeeping row inside the
    /// currently open `data.db` transaction to account for `frame` (which sorts
    /// last, having the highest lamport).
    fn bump_applied_in_txn(&self, frame: &Frame) -> Result<()> {
        use sha2::Digest as _;
        let mut h = self.applied_hasher.clone();
        h.update(frame.hash);
        let new_digest: [u8; 32] = h.finalize().into();
        write_applied(&self.data, self.applied_count + 1, &new_digest)?;
        Ok(())
    }

    fn maybe_fault(&self) -> Result<()> {
        match self.fault {
            Some(FaultPlan::AfterDataCommitBeforeFrameInsert)
            | Some(FaultPlan::AfterDataCommitAndRepairFails)
            | Some(FaultPlan::AfterDataCommitAndStoreBaseReadFails)
            | Some(FaultPlan::BaseRepairRemoveFails) => Err(Error::Injected(
                "fault: crashed after data.db commit, before frame insert".into(),
            )),
            // Store writes still succeed here: only the `Base` read fails.
            Some(FaultPlan::StoreBaseReadFails) | None => Ok(()),
        }
    }

    /// Runs a single data-changing statement and records it as one frame.
    ///
    /// A statement that changes the schema is rejected with
    /// [`Error::SchemaChangeInExecute`]; use [`ShareDb::migrate`] instead.
    pub fn execute<P: rusqlite::Params>(&mut self, sql: &str, params: P) -> Result<usize> {
        self.commit_capture(move |conn| Ok(conn.execute(sql, params)?))
    }

    /// Runs `f` inside a single captured transaction, producing one frame.
    pub fn transaction<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Transaction) -> Result<T>,
    {
        self.commit_capture(|conn| {
            let mut txn = Transaction { conn };
            f(&mut txn)
        })
    }

    /// Applies a schema migration and records it as a migration frame.
    ///
    /// The frame carries the migration SQL (not a changeset): replay re-runs the
    /// DDL in total order. A migration never needs a capture session.
    ///
    /// * `version <= schema_version`: the migration is compared with the stored
    ///   one — [`MigrateOutcome::AlreadyApplied`] for identical SQL,
    ///   [`Error::MigrationMismatch`] otherwise. The stored SQL is looked up in
    ///   `data.db`'s `__sharedb_migrations` table first — it travels inside a
    ///   checkpoint snapshot and is written by a total-order replay, so it always
    ///   names the migration that was really applied — and only then in the frame
    ///   store, which a bootstrapped node may not hold at all (the frames below
    ///   its cut are folded into the snapshot) or may hold only a *losing*
    ///   duplicate of. Either way a node that bootstrapped from a checkpoint still
    ///   re-runs an already-applied migration as
    ///   [`MigrateOutcome::AlreadyApplied`]. Only when neither knows the version
    ///   is [`Error::InvalidVersion`] returned, explaining that the version can
    ///   never be applied here.
    /// * `version > schema_version + 1`: [`Error::MigrationGap`].
    ///
    /// The new schema is validated *inside* the transaction, so a migration that
    /// produces an unrepresentable schema (a table without a PRIMARY KEY, a
    /// trigger, …) rolls back completely and is not journaled.
    pub fn migrate(&mut self, version: u32, sql: &str) -> Result<MigrateOutcome> {
        self.ensure_usable()?;
        if version <= self.schema_version {
            // Already applied here: compare against the stored migration. The
            // data file is authoritative: `__sharedb_migrations` is written by a
            // total-order replay, so it records the winning SQL even on a node
            // that bootstrapped from a checkpoint. The frame store is only the
            // fallback — a bootstrapped node's store lacks the frames below its
            // cut, and may hold nothing but a losing duplicate of this one.
            let existing = match materialize::recorded_migration_sql(&self.data, version)? {
                Some(sql) => Some(sql),
                None => store::migration_sql_for_version(&self.store, version)?,
            };
            return match existing {
                Some(existing) if existing == sql => Ok(MigrateOutcome::AlreadyApplied),
                Some(_) => Err(Error::MigrationMismatch { version }),
                None => Err(Error::InvalidVersion {
                    version,
                    current: self.schema_version,
                }),
            };
        }
        // `saturating_add` so a schema version at `u32::MAX` cannot wrap to 0 and
        // turn every later version into a gap.
        if version > self.schema_version.saturating_add(1) {
            return Err(Error::MigrationGap {
                current: self.schema_version,
                requested: version,
            });
        }
        if let Some(existing) = store::migration_sql_for_version(&self.store, version)? {
            if existing == sql {
                return Ok(MigrateOutcome::AlreadyApplied);
            }
            return Err(Error::MigrationMismatch { version });
        }

        // Phase 1: run the DDL, validate the result and commit `data.db`. The
        // borrow of `self.data` taken by the rollback guard ends here, so the
        // store write below can take `&mut self`.
        let phase1 = {
            let this = &*self;
            this.data.execute_batch("BEGIN")?;
            let mut rollback = RollbackOnDrop::new(&this.data);
            let probe = begin_txn_probe(&this.data)?;
            let built = (|| -> Result<Frame> {
                {
                    // The migration SQL is user SQL: it must not be able to end
                    // the transaction we own, nor to `ATTACH` another database.
                    let _guard = GuardTxn::new(&this.txn_guard);
                    this.data.execute_batch(sql)?;
                }
                if this.data.is_autocommit() {
                    // The authorizer should have denied this; keep the check as
                    // a fallback for paths it cannot see.
                    return Err(Error::unsupported(
                        "the migration ended the transaction; the commit protocol must own BEGIN/COMMIT",
                    ));
                }
                // Record the version bump in the same transaction as the DDL,
                // and the `version -> sql` history alongside it (see
                // `materialize::ensure_meta`: the table travels in a checkpoint
                // snapshot, so a bootstrapped node keeps knowing what it applied).
                data_meta_set(&this.data, "schema_version", &version.to_string())?;
                materialize::record_migration(&this.data, version, sql)?;
                // Validate before the migration becomes permanent: an unsupported
                // schema must roll the DDL back, not be committed and then refused.
                validate_schema(&this.data)?;
                let frame = this.build_frame(
                    FrameKind::Migration(version),
                    crate::frame::migration_payload(version, sql),
                )?;
                this.bump_applied_in_txn(&frame)?;
                this.data.execute_batch("COMMIT")?;
                rollback.disarm();
                Ok(frame)
            })();
            let externally_ended = built.is_err() && this.data.is_autocommit();
            let probe = txn_probe_committed(&this.data, &probe);
            let ended_by_user = externally_ended && probe == Some(true);
            let probe_unreadable = externally_ended && probe.is_none();
            drop(rollback);
            (built, ended_by_user, probe_unreadable)
        };
        let frame = match phase1 {
            (Ok(frame), _, _) => frame,
            (Err(err), ended_by_user, probe_unreadable) => {
                if ended_by_user {
                    // The migration committed inside our transaction: `data.db`
                    // is ahead of the store and the store can never account for
                    // it, so rebuild before anything else touches the handle.
                    self.repair_to_store();
                    self.failed.get_or_insert_with(|| {
                        "the migration ended the transaction; the commit protocol must own BEGIN/COMMIT"
                            .to_string()
                    });
                } else if probe_unreadable {
                    // The probe could not be read: rebuild to be safe, and let
                    // `repair_to_store` decide whether the handle survives. As in
                    // `commit_capture`, a node with full history survives a
                    // successful repair, while a bootstrapped node has its
                    // `data.db` invalidated and its handle failed — an
                    // availability cost, never a divergence.
                    self.repair_to_store();
                }
                return Err(err);
            }
        };

        // Phase 2: `data.db` is ahead of the store until the frame lands.
        let recorded = self.maybe_fault().and_then(|()| self.record_frame(&frame));
        if let Err(err) = recorded {
            self.repair_to_store();
            return Err(err);
        }
        self.applied_count += 1;
        {
            use sha2::Digest as _;
            self.applied_hasher.update(frame.hash);
            self.applied_digest = self.applied_hasher.clone().finalize().into();
        }
        self.own_seq = frame.seq;
        self.own_prev_hash = frame.hash;
        self.lamport_max = frame.lamport;
        self.schema_version = version;
        Ok(MigrateOutcome::Applied)
    }

    /// Runs a read-only query, mapping each row with `f`.
    ///
    /// Statements that are not read-only are rejected with [`Error::NotReadOnly`].
    /// Transaction control statements are rejected explicitly: SQLite reports
    /// them as read-only, but running one would break the commit protocol.
    pub fn query<T, P, F>(&self, sql: &str, params: P, mut f: F) -> Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row) -> Result<T>,
    {
        self.ensure_usable()?;
        if is_transaction_control(sql) {
            return Err(Error::NotReadOnly);
        }
        let mut stmt = self.data.prepare(sql)?;
        if !stmt.readonly() {
            return Err(Error::NotReadOnly);
        }
        let mut rows = stmt.query(params)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(f(row)?);
        }
        Ok(out)
    }

    /// A logical dump of the user tables.
    pub fn dump(&self) -> Result<Dump> {
        self.ensure_usable()?;
        dump(&self.data)
    }

    /// Writes a plain SQLite snapshot of this node's `data.db` to `out`.
    ///
    /// The snapshot is produced with SQLite's online backup API, so it is a
    /// consistent single-file database that `sqlite3 <out>` (or any SQLite tool)
    /// can open — which is the point: the synced folder itself holds only
    /// segments and checkpoints, never a `.db` anyone else could read.
    ///
    /// The `__sharedb_*` bookkeeping tables are dropped from the copy and the
    /// file is VACUUMed, so it contains exactly the user's schema and data. An
    /// existing file at `out` is replaced.
    pub fn export(&self, out: &Path) -> Result<()> {
        self.ensure_usable()?;
        // A fresh file: backing up into a file that already holds a database of
        // a different page size would fail with a confusing SQLite error.
        if out.exists() {
            fs::remove_file(out)?;
        }
        let mut dest = Connection::open(out)?;
        {
            let backup = rusqlite::backup::Backup::new(&self.data, &mut dest)?;
            backup.run_to_completion(64, std::time::Duration::from_millis(0), None)?;
        }
        // The bookkeeping is this node's private state, not the user's data.
        let internal: Vec<String> = dest
            .prepare(
                "SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name LIKE '\\_\\_sharedb\\_%' ESCAPE '\\'",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for name in internal {
            dest.execute(&format!("DROP TABLE IF EXISTS \"{name}\""), [])?;
        }
        dest.execute_batch("VACUUM;")?;
        drop(dest);
        Ok(())
    }

    /// Publishes unpublished frames, merges the folder's segments into the local
    /// store and brings `data.db` in line with it.
    ///
    /// This is the only place that touches the synced folder besides
    /// [`ShareDb::status`] (which only lists). Everything it writes is
    /// write-once: a segment is created by an atomic rename and never modified
    /// afterwards, so two nodes can never conflict over a file and a sync client
    /// only ever has to upload new files.
    pub fn sync(&mut self) -> Result<SyncReport> {
        self.ensure_usable()?;
        let mut report = SyncReport::default();
        let dir = self.segments_dir();
        self.fs.create_dir_all(&dir)?;
        self.publish(&dir, &mut report)?;
        self.ingest(&dir, &mut report)?;
        self.materialize_ingested(&mut report)?;
        // Compaction is best-effort: a folder that refuses a write or a delete
        // leaves the folder as it was and the next sync tries again, so a folder
        // error here must not fail the sync whose publish/ingest already landed.
        if let Err(err) = self.compact() {
            let _ = err;
        }
        Ok(report)
    }

    /// Writes a checkpoint if enough frames accumulated past the newest one, then
    /// prunes the segments and superseded checkpoints that made it redundant.
    ///
    /// Prune and the publish-floor raise happen **only after a checkpoint for the
    /// intended cut is known to exist** — written now or already present and
    /// verified — because pruning what that checkpoint folded in is only safe once
    /// the snapshot is in the folder for others to build on. If the write failed,
    /// prune and floor are skipped entirely this sync.
    ///
    /// A **bootstrapped** node (one that carries a [`checkpoint::Base`]) never
    /// writes a checkpoint and never prunes (R1): its store does not hold the
    /// frames below its cut, so a checkpoint it wrote would carry a `count`,
    /// `cut_digest` and version vector taken from an incomplete store and could
    /// mislead every other node. Only full-history nodes produce checkpoints.
    /// It *does* still raise its publish floor from a dominating checkpoint (F4):
    /// without that, when a full-history node prunes this node's covered segments,
    /// this node's `publish` would walk its own chain from seq 1 and republish them
    /// every sync — the folder churns forever even though no data is lost.
    ///
    /// Nothing here can lose data: the local store is never touched, and every
    /// folder error is swallowed so the next sync retries.
    fn compact(&mut self) -> Result<()> {
        let base = store::base(&self.store)?;
        let frames = store::all_frames(&self.store, &self.db_id)?;
        let total = frames.len() as u64;
        let ckpoint_dir = self.folder.join(checkpoint::CKPOINT_DIR);
        self.fs.create_dir_all(&ckpoint_dir)?;
        let full_digest = {
            let refs: Vec<FrameRef> = frames.iter().map(|f| f.as_ref()).collect();
            digest(&refs)
        };
        let read = checkpoint::read_best(&*self.fs, &ckpoint_dir, &self.db_id, &frames);
        let best = read.best;

        // R1: a bootstrapped node writes no checkpoint and prunes nothing, but it
        // still raises its publish floor to a checkpoint whose version vector
        // dominates its base — that checkpoint already carries the state of the
        // segments a full-history node may prune, so this node must stop putting
        // them back into the folder (F4).
        if let Some(base) = base.as_ref() {
            if let Some(best) = best.filter(|cp| base.dominates(&cp.vv)) {
                self.set_publish_floor(&best.vv)?;
            }
            return Ok(());
        }

        let last_cut = best.as_ref().map(|b| b.count).unwrap_or(0);

        if total.saturating_sub(last_cut) >= self.checkpoint_threshold as u64 && total > 0 {
            let last = &frames[frames.len() - 1];
            let cp = checkpoint::Checkpoint {
                count: total,
                cut_lamport: store::max_lamport(&self.store, &self.db_id)?,
                boundary: checkpoint::sort_key_of(last),
                cut_digest: full_digest,
                vv: checkpoint::VersionVector::build(&frames, total as usize),
                snapshot: checkpoint::capture(&self.data, &self.state_dir.join("snapshot.db"))?,
            };
            if self.checkpoint_in_place(&ckpoint_dir, &cp) {
                // The checkpoint for this cut is in the folder: prune by its
                // version vector and stop republishing what it folded in.
                checkpoint::prune(
                    &*self.fs,
                    &self.segments_dir(),
                    &self.db_id,
                    &cp.vv,
                    self.retention_segments,
                );
                self.set_publish_floor(&cp.vv)?;
                checkpoint::prune_checkpoints(&*self.fs, &ckpoint_dir, &self.db_id);
            }
            return Ok(());
        }

        if let Some(best) = best {
            checkpoint::prune(
                &*self.fs,
                &self.segments_dir(),
                &self.db_id,
                &best.vv,
                self.retention_segments,
            );
            self.set_publish_floor(&best.vv)?;
        }
        Ok(())
    }

    /// Whether a checkpoint for exactly `cp`'s cut is readable at the name `cp`
    /// would be written under: written now, or already there and verified to carry
    /// the same cut digest and count.
    ///
    /// This is the gate that keeps prune honest. A name that is taken proves
    /// nothing on its own — a sync client may have delivered a conflicted or
    /// truncated file — so the file is decoded and compared against the intended
    /// cut before anything is deleted on its authority.
    fn checkpoint_in_place(&self, ckpoint_dir: &Path, cp: &checkpoint::Checkpoint) -> bool {
        let path = ckpoint_dir.join(cp.file_name());
        if !self.fs.exists(&path).unwrap_or(false) {
            // Write-once and idempotent: a folder that refuses the checkpoint
            // simply defers compaction to the next sync.
            if self
                .fs
                .write_atomic(&path, &cp.encode(&self.db_id))
                .is_err()
            {
                return false;
            }
            return true;
        }
        match self.fs.read(&path) {
            Ok(bytes) => match checkpoint::decode(&bytes, &self.db_id) {
                Ok(existing) => existing.count == cp.count && existing.cut_digest == cp.cut_digest,
                Err(_) => false,
            },
            Err(_) => false,
        }
    }

    /// Raises the publish floor to this node's highest `seq` the checkpoint's
    /// version vector covers.
    ///
    /// Without this, `publish` would find the pruned range missing from the
    /// folder and republish it on the next sync, undoing the prune. The frames
    /// stay in the local store, so nothing is lost: the checkpoint already
    /// carries their state to every other node. Every node that sees the same
    /// best checkpoint computes the same floor, so no node republishes what
    /// another node's prune removed.
    fn set_publish_floor(&mut self, vv: &checkpoint::VersionVector) -> Result<()> {
        let floor = vv.get(&self.node_id).map(|v| v.max_seq).unwrap_or(0);
        store::set_published_floor(&self.store, floor)
    }

    /// Writes every own frame that is not in the folder yet, one segment per
    /// contiguous range.
    ///
    /// Normally that is the tail above the last published `seq` (tracked in
    /// `store.db`'s meta). On top of that the file names are scanned, so a range
    /// that was deleted from the folder is republished — a segment we already
    /// published but that a peer (or a sync client) lost is indistinguishable
    /// from one we never wrote.
    ///
    /// A folder that refuses the write (a sync client holding the directory
    /// open, offline, read-only) leaves `publish_pending` set and the local data
    /// untouched: the frames are in `store.db` and will be published by a later
    /// sync.
    fn publish(&mut self, dir: &Path, report: &mut SyncReport) -> Result<()> {
        let own_max = store::own_max_seq(&self.store, &self.node_id)?;
        let published = store::last_published(&self.store)?;
        // Compaction may have deleted our own segments below the checkpoint
        // boundary; those frames must not be republished, or the prune would be
        // undone on the next sync. See [`store::published_floor`].
        let floor = store::published_floor(&self.store)?;

        // Which own ranges do the file names say are already in the folder?
        let mut covered: Vec<(u64, u64)> = Vec::new();
        for name in self.fs.list(dir)? {
            if let Some((node, first, last)) = folder::parse_segment_name(&name) {
                if node == self.node_id {
                    covered.push((first, last));
                }
            }
        }
        covered.sort();

        // Walk our own log and collect every uncovered range up to `own_max`.
        // The walk starts one past the publish floor, not at 1: a segment we
        // published long ago may have been deleted from the folder (a sync
        // client that dropped it, a user who cleaned up, or compaction), and a
        // missing range must be republished — except below the floor, where the
        // checkpoint already carries the state. The file names are the authority
        // on what the folder holds; the meta value only feeds
        // `Status::unpublished`.
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        let mut seq = floor + 1;
        while seq <= own_max {
            if let Some(&(_, last)) = covered.iter().find(|&&(f, l)| f <= seq && seq <= l) {
                seq = last + 1;
                continue;
            }
            let start = seq;
            let mut end = seq;
            loop {
                let next = end + 1;
                if next > own_max || covered.iter().any(|&(f, l)| f <= next && next <= l) {
                    break;
                }
                end = next;
            }
            ranges.push((start, end));
            seq = end + 1;
        }

        for (first, last) in ranges {
            let frames = store::frames_range(&self.store, &self.db_id, &self.node_id, first, last)?;
            if frames.is_empty() {
                continue;
            }
            let bytes = folder::encode_segment(&self.db_id, &self.node_id, &frames)?;
            let path = dir.join(folder::segment_name(&self.node_id, first, last));
            if self.fs.exists(&path)? {
                // A name that is already taken is never overwritten: the folder
                // is write-once. The content is deterministic, so the file that
                // is there already says the same thing.
                continue;
            }
            if let Err(err) = self.fs.write_atomic(&path, &bytes) {
                // The folder refused us (sync client holding the directory,
                // offline, read-only). Local data is intact: the frames are in
                // `store.db` and a later sync publishes them again.
                let _ = err;
                report.publish_pending = true;
                return Ok(());
            }
            report.published += frames.len();
        }

        if !report.publish_pending && own_max > published {
            store::set_last_published(&self.store, own_max)?;
        }
        Ok(())
    }

    /// Reads the folder's segments and appends everything that continues a node's
    /// chain to the local store.
    ///
    /// Files are identified by content, never by name: every segment whose name
    /// ends in `.seg` is read and verified, because a file's *content* is the only
    /// authority on what it holds. A name-based shortcut ("this range is below
    /// what we already have, so skip the read") would hide a fork that sits under
    /// the high-water mark — a conflicted copy of a peer segment carrying a
    /// different frame for a sequence we already recorded.
    ///
    /// A segment that does not verify as a whole is ignored and counted; a range
    /// that stops short of continuing a chain is recorded as a gap and the rest of
    /// that node's segments are left for a later sync; a frame that disagrees with
    /// the one already stored at its position is a fork and is never ingested.
    fn ingest(&self, dir: &Path, report: &mut SyncReport) -> Result<()> {
        let mut names = self.fs.list(dir)?;
        // Deterministic order, so the report is reproducible.
        names.sort();

        // Chain anchor per node, filled lazily from the store and then kept in
        // memory as frames are appended.
        let mut high: HashMap<NodeId, (u64, [u8; 32])> = HashMap::new();

        for name in names {
            if !folder::is_segment_name(&name) {
                continue;
            }
            let path = dir.join(&name);
            let claimed = folder::parse_segment_name(&name);
            let bytes = match self.fs.read(&path) {
                Ok(bytes) => bytes,
                Err(_) => {
                    report.ignored_files.push(path);
                    continue;
                }
            };
            let segment = match folder::decode_segment(&bytes, &self.db_id) {
                Ok(segment) => segment,
                Err(_) => {
                    report.ignored_files.push(path);
                    continue;
                }
            };
            // The name is a hint, the content is the truth: a file whose frames
            // are not the range its name announces is corrupt and ignored.
            if let Some((node, first, last)) = claimed {
                if node != segment.node_id
                    || segment.frames.first().map(|f| f.seq) != Some(first)
                    || segment.last_seq() != last
                {
                    report.ignored_files.push(path);
                    continue;
                }
            }
            self.ingest_segment(&segment, &mut high, report)?;
        }
        Ok(())
    }

    /// The chain anchor for `node`, remembered across the segment scan so each
    /// node's anchor is read from the store once and then kept in memory as
    /// frames are appended.
    ///
    /// This is [`store::anchor_of`], not the highest stored `seq`: a node that
    /// bootstrapped from a checkpoint does not hold the frames below the cut, and
    /// its anchor is the checkpoint's version-vector entry, so a peer's retained
    /// segment that starts just above the cut continues the chain instead of
    /// looking like a gap.
    fn anchor(
        &self,
        high: &mut HashMap<NodeId, (u64, [u8; 32])>,
        node: &NodeId,
    ) -> Result<(u64, [u8; 32])> {
        if let Some(anchor) = high.get(node) {
            return Ok(*anchor);
        }
        let anchor = store::anchor_of(&self.store, node)?;
        high.insert(*node, anchor);
        Ok(anchor)
    }

    /// Appends the frames of one segment, stopping at the first gap or fork.
    fn ingest_segment(
        &self,
        segment: &Segment,
        high: &mut HashMap<NodeId, (u64, [u8; 32])>,
        report: &mut SyncReport,
    ) -> Result<()> {
        let node = segment.node_id;
        let mut anchor = self.anchor(high, &node)?;
        for frame in &segment.frames {
            if frame.seq <= anchor.0 {
                // At or below the anchor: either already stored (a duplicate
                // segment, or a conflicted copy of the same range) or covered by
                // our checkpoint's snapshot, in which case the store legitimately
                // does not hold it. A stored frame with a different hash is a fork.
                match store::own_frame_hash(&self.store, &node, frame.seq) {
                    Ok(Some(hash)) if hash == frame.hash => report.skipped += 1,
                    Ok(None) => report.skipped += 1,
                    _ => report.forks.push((node, frame.seq)),
                }
                continue;
            }
            if frame.seq != anchor.0 + 1 {
                report.gaps.push((node, anchor.0 + 1));
                high.insert(node, anchor);
                return Ok(());
            }
            match store::append_frame(&self.store, &node, frame) {
                Ok(()) => {
                    report.ingested += 1;
                    anchor = (frame.seq, frame.hash);
                }
                // The store re-checks the anchor and the chain link: a rejected
                // frame is either a gap we did not see or a frame claiming a
                // different history. Either way this segment is done.
                Err(Error::FrameGap { expected, .. }) => {
                    report.gaps.push((node, expected));
                    high.insert(node, anchor);
                    return Ok(());
                }
                Err(_) => {
                    report.forks.push((node, frame.seq));
                    high.insert(node, anchor);
                    return Ok(());
                }
            }
        }
        high.insert(node, anchor);
        Ok(())
    }

    /// Brings `data.db` in line with the frames the store gained this sync.
    ///
    /// Reuses the open-time reconciliation: if the frames already applied are
    /// still a prefix of the store in total order the new frames are simply
    /// replayed onto the existing data file ([`MaterializeKind::Fast`]); if a
    /// frame arrived that sorts *before* them the data file is rebuilt from
    /// scratch ([`MaterializeKind::Rebuilt`]).
    fn materialize_ingested(&mut self, report: &mut SyncReport) -> Result<()> {
        let frames = store::all_frames(&self.store, &self.db_id)?;
        if frames.len() as u64 == self.applied_count {
            report.materialize = MaterializeKind::UpToDate;
            // Even with nothing new to ingest, a pending bootstrapped handle still
            // carries its pending flag and its carried-forward dropped count; the
            // report must reflect them rather than the `SyncReport::default()` zeros.
            report.materialize_pending = self.materialize_pending;
            report.dropped_below_boundary = self.dropped_below_boundary;
            return Ok(());
        }
        // Close our own handle on `data.db` first: the rebuild path replaces the
        // file, which a still-open handle would fight with on Windows.
        let stale = std::mem::replace(&mut self.data, Connection::open_in_memory()?);
        drop(stale);
        // Cloned so `env` does not borrow `self` while `reconcile` takes `&mut self`.
        let fs = self.fs.clone();
        let env = ReconcileEnv {
            fs: &*fs,
            ckpoint_dir: self.folder.join(checkpoint::CKPOINT_DIR),
            store: &self.store,
        };
        let reconciled = match reconcile(
            &self.state_dir.join("data.db"),
            &self.db_id,
            frames,
            Some(&env),
        ) {
            Ok(reconciled) => reconciled,
            Err(err) => {
                // The data file could not be brought in line with the store at
                // all: refuse to keep working on a handle whose data is unknown.
                self.failed = Some(format!("could not materialise the ingested frames: {err}"));
                return Err(err);
            }
        };
        report.materialize = reconciled.kind;
        // Carry the dropped count forward while the node stays pending: the max
        // of what this reconcile found and what was already carried, never reset
        // to `0` while pending (R2).
        self.materialize_pending = reconciled.materialize_pending;
        if reconciled.materialize_pending {
            self.dropped_below_boundary = self
                .dropped_below_boundary
                .max(reconciled.dropped_below_boundary);
        } else {
            self.dropped_below_boundary = reconciled.dropped_below_boundary;
        }
        report.dropped_below_boundary = self.dropped_below_boundary;
        report.materialize_pending = self.materialize_pending;
        let mut data = reconciled.data;
        install_transaction_guard(&mut data, &self.txn_guard)?;
        self.applied_hasher = hasher_for(&reconciled.frames, reconciled.applied_count);
        self.data = data;
        self.applied_count = reconciled.applied_count;
        self.applied_digest = reconciled.applied_digest;
        self.schema_version = read_schema_version(&self.data).unwrap_or(0);
        self.refresh_chain();
        Ok(())
    }

    /// This node's status.
    pub fn status(&self) -> Result<Status> {
        self.ensure_usable()?;
        let own_seq = store::own_max_seq(&self.store, &self.node_id)?;
        let published = store::last_published(&self.store)?;
        let peers = store::peer_high_water(&self.store, &self.node_id)?;
        let mut conflicted_copies = Vec::new();
        let dir = self.segments_dir();
        if let Ok(names) = self.fs.list(&dir) {
            for name in names {
                if folder::is_conflicted_copy(&name) {
                    conflicted_copies.push(dir.join(name));
                }
            }
            conflicted_copies.sort();
        }
        Ok(Status {
            node_id: self.node_id,
            own_seq,
            unpublished: own_seq.saturating_sub(published) as usize,
            peers,
            conflicted_copies,
            materialize_pending: self.materialize_pending,
            dropped_below_boundary: self.dropped_below_boundary,
        })
    }
}
