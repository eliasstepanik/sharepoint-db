//! Shared harness for the convergence tests.
//!
//! A [`Node`] is one writer: a local copy of a synced folder plus a local state
//! directory, with a [`ShareDb`] open on it. Two nodes never share a directory —
//! that is what makes them separate machines — and [`FakeSync`] plays the part of
//! the sync client by copying segment *files* from one folder to the other.

#![allow(dead_code)]

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use sharepoint_db::folder::{with_retry, FolderFs, RealFs};
use sharepoint_db::rusqlite;
use sharepoint_db::{Dump, Error, Frame, FrameKind, NodeId, OpenOptions, ShareDb};

/// The schema almost every test starts from.
pub const MIGRATE_T: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);";

/// One writer: a synced-folder copy, a state directory and an open database.
pub struct Node {
    /// A label used in assertions and to tell nodes apart.
    pub name: String,
    /// This node's local copy of the synced folder.
    pub folder: PathBuf,
    /// This node's local state directory (outside the synced folder).
    pub state: PathBuf,
    /// The open database.
    pub db: ShareDb,
}

fn paths(root: &Path, name: &str) -> (PathBuf, PathBuf) {
    (
        root.join(format!("{name}.sharedb")),
        root.join(format!("{name}.state")),
    )
}

fn open_at(folder: &Path, state: &Path) -> ShareDb {
    ShareDb::open(
        folder,
        OpenOptions {
            create: true,
            state_dir: Some(state.to_path_buf()),
            fault: None,
            ..Default::default()
        },
    )
    .expect("open node")
}

/// A synced folder plus a separate local state directory, for the single-node
/// tests that need explicit `open`/`reopen` (rather than the two-node [`Node`]).
pub struct Fixture {
    pub folder: PathBuf,
    pub state_dir: PathBuf,
}

impl Fixture {
    pub fn new(root: &Path, name: &str) -> Fixture {
        let (folder, state_dir) = paths(root, name);
        Fixture { folder, state_dir }
    }

    pub fn opts(&self) -> OpenOptions {
        OpenOptions {
            create: true,
            state_dir: Some(self.state_dir.clone()),
            fault: None,
            ..Default::default()
        }
    }

    pub fn open(&self) -> ShareDb {
        open_at(&self.folder, &self.state_dir)
    }

    pub fn reopen(&self) -> ShareDb {
        self.try_reopen().expect("reopen")
    }

    pub fn try_reopen(&self) -> sharepoint_db::Result<ShareDb> {
        ShareDb::open(
            &self.folder,
            OpenOptions {
                create: false,
                state_dir: Some(self.state_dir.clone()),
                fault: None,
                ..Default::default()
            },
        )
    }
}

impl Node {
    /// Creates a brand-new database (a fresh `MANIFEST` with a fresh `db_id`).
    pub fn create(root: &Path, name: &str) -> Node {
        let (folder, state) = paths(root, name);
        Node {
            name: name.to_string(),
            db: open_at(&folder, &state),
            folder,
            state,
        }
    }

    /// A second writer for an existing database: its folder copy carries the
    /// `MANIFEST` (that is what a sync client would deliver) and its state
    /// directory is empty, so it knows nothing until it syncs.
    pub fn join(root: &Path, name: &str, seed: &Node) -> Node {
        let (folder, state) = paths(root, name);
        fs::create_dir_all(&folder).expect("create folder");
        fs::copy(seed.folder.join("MANIFEST"), folder.join("MANIFEST")).expect("copy manifest");
        Node {
            name: name.to_string(),
            db: open_at(&folder, &state),
            folder,
            state,
        }
    }

    /// Reopens the node with an empty state directory (a machine that lost its
    /// local state and has to rebuild from the folder).
    pub fn reopen_fresh(self) -> Node {
        let Node {
            name,
            folder,
            state,
            db,
        } = self;
        drop(db);
        fs::remove_dir_all(&state).expect("remove state");
        Node {
            name,
            db: open_at(&folder, &state),
            folder,
            state,
        }
    }

    pub fn node_id(&self) -> sharepoint_db::NodeId {
        self.db.node_id()
    }

    pub fn db_id(&self) -> sharepoint_db::DbId {
        self.db.db_id()
    }

    pub fn segments_dir(&self) -> PathBuf {
        self.db.segments_dir()
    }

    pub fn dump(&self) -> Dump {
        self.db.dump().expect("dump")
    }

    /// The `v` column of `t`, ordered by `id`.
    ///
    /// A node that has not received the migration yet has no table `t`; that is
    /// reported as an empty list, because "nothing visible" is what the test
    /// wants to know.
    pub fn values(&self) -> Vec<String> {
        self.db
            .query("SELECT v FROM t ORDER BY id", [], |r| {
                Ok(r.get::<_, String>(0)?)
            })
            .unwrap_or_default()
    }

    pub fn migrate1(&mut self) {
        assert_eq!(
            self.db.migrate(1, MIGRATE_T).unwrap(),
            sharepoint_db::MigrateOutcome::Applied
        );
    }

    pub fn insert(&mut self, id: i64, v: &str) {
        self.db
            .execute(
                "INSERT INTO t (id, v) VALUES (?1, ?2)",
                rusqlite::params![id, v],
            )
            .expect("insert");
    }

    pub fn update(&mut self, id: i64, v: &str) {
        self.db
            .execute(
                "UPDATE t SET v = ?2 WHERE id = ?1",
                rusqlite::params![id, v],
            )
            .expect("update");
    }

    pub fn sync(&mut self) -> sharepoint_db::SyncReport {
        self.db.sync().expect("sync")
    }

    /// The segment files in this node's folder, by name.
    pub fn segments(&self) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(self.segments_dir())
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }
}

/// The fake sync client: it copies segment files between folders and nothing
/// else. Delivery is explicit so a test can withhold files on purpose.
pub struct FakeSync;

impl FakeSync {
    /// Copies every segment file of `from`'s folder into `to`'s folder.
    ///
    /// Files that already exist at the destination are left alone: the folder is
    /// write-once, and a real sync client never overwrites a file it cannot
    /// prove identical. Returns the number of files copied.
    pub fn deliver(from: &Node, to: &Node) -> usize {
        let src = from.segments_dir();
        let dst = to.segments_dir();
        fs::create_dir_all(&dst).expect("create segments dir");
        let mut copied = 0;
        let mut names: Vec<String> = fs::read_dir(&src)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        for name in names {
            if !name.ends_with(".seg") {
                continue;
            }
            let target = dst.join(&name);
            if target.exists() {
                continue;
            }
            fs::copy(src.join(&name), target).expect("copy segment");
            copied += 1;
        }
        copied
    }

    /// Delivers between every ordered pair of `nodes` and syncs every node,
    /// twice: once so each node publishes what it holds, once more after the
    /// copies so a frame that was published during the exchange still reaches
    /// everyone. After this call every node has seen every segment.
    pub fn exchange(nodes: &mut [&mut Node]) {
        for node in nodes.iter_mut() {
            node.sync();
        }
        for a in nodes.iter() {
            for b in nodes.iter() {
                if a.name != b.name {
                    Self::deliver(a, b);
                }
            }
        }
        for node in nodes.iter_mut() {
            node.sync();
        }
    }
}

/// Every file under a node's synced folder: `(relative path, contents)`.
///
/// Used to prove the folder is untouched (write-once) across operations.
pub fn folder_snapshot(node: &Node) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    walk(&node.folder, &node.folder, &mut out);
    out.sort();
    out
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(root, &path, out);
        } else {
            let rel = path.strip_prefix(root).unwrap().display().to_string();
            out.push((rel, fs::read(&path).unwrap()));
        }
    }
}

/// Writes `bytes` into a node's segments directory under `name` (used to drop a
/// corrupted or foreign segment into a folder).
pub fn put_segment(node: &Node, name: &str, bytes: &[u8]) -> PathBuf {
    let path = node.segments_dir().join(name);
    fs::write(&path, bytes).expect("write segment");
    path
}

/// Reads a segment file out of a node's segments directory.
pub fn read_segment(node: &Node, name: &str) -> Vec<u8> {
    fs::read(node.segments_dir().join(name)).expect("read segment")
}

/// The canonical segment name for `node`'s range `first..=last`, built the same
/// way the crate builds it (`<node16hex>-<first:010>-<last:010>.seg`).
pub fn seg_name(node: &sharepoint_db::NodeId, first: u64, last: u64) -> String {
    let hex: String = node.iter().map(|b| format!("{b:02x}")).collect();
    format!("{hex}-{first:010}-{last:010}.seg")
}

/// The name `name` gets when a sync client cannot merge it and renames it to a
/// "conflicted copy": the marker is inserted before the `.seg` suffix.
pub fn conflicted_copy_name(name: &str) -> String {
    let stem = name.strip_suffix(".seg").unwrap_or(name);
    format!("{stem} (conflicted copy 2024-01-01 10-11-12).seg")
}

/// Builds a valid, hash-chained run of changeset frames for `node_id`, starting
/// at `seq` 1 and increasing the lamport clock in step, from raw payload bytes.
///
/// The payloads are never applied by these tests (a fork is refused before it
/// reaches replay), so they may be arbitrary bytes; what matters is that the
/// frames chain and hash exactly as a real node's would, so the segment decodes.
pub fn craft_chain(
    db_id: &sharepoint_db::DbId,
    node_id: &NodeId,
    payloads: &[Vec<u8>],
) -> Vec<Frame> {
    let mut frames = Vec::with_capacity(payloads.len());
    let mut prev_hash = [0u8; 32];
    for (i, payload) in payloads.iter().enumerate() {
        let frame = Frame::new(
            db_id,
            node_id,
            (i + 1) as u64,
            (i + 1) as u64,
            FrameKind::Changeset,
            prev_hash,
            payload.clone(),
        );
        prev_hash = frame.hash;
        frames.push(frame);
    }
    frames
}

/// Installs a [`HostileFs`] on `node` and hands back the handle so the test can
/// program failures and read the attempt counters.
pub fn install_hostile_fs(node: &mut Node) -> Arc<HostileFs> {
    let hostile = HostileFs::new();
    node.db.set_folder_fs(hostile.clone());
    hostile
}

// ---------------------------------------------------------------------------
// Hostile filesystem
// ---------------------------------------------------------------------------

/// A [`FolderFs`] that wraps [`RealFs`] but can be programmed to fail the first
/// N write/read attempts (or every one of them) with a transient
/// [`io::ErrorKind::PermissionDenied`], counting every attempt it makes.
///
/// It runs its own operations through [`with_retry`] — the very helper the real
/// filesystem uses — so a test that fails the first two attempts and succeeds on
/// the third observes exactly the retry policy production uses, and can assert
/// the attempt count. A failure that outlives the retries is returned to the
/// caller, which is what drives `publish_pending`.
pub struct HostileFs {
    /// Writes still doomed to fail (decremented once per attempt).
    fail_first_writes: AtomicUsize,
    /// Every write fails while this is set.
    fail_writes: AtomicBool,
    /// Reads still doomed to fail (decremented once per attempt).
    fail_first_reads: AtomicUsize,
    /// Every read fails while this is set.
    fail_reads: AtomicBool,
    /// Total `write_atomic` attempts observed (each retry counts).
    write_attempts: AtomicUsize,
    /// Total `read` attempts observed (each retry counts).
    read_attempts: AtomicUsize,
}

impl HostileFs {
    pub fn new() -> Arc<HostileFs> {
        Arc::new(HostileFs {
            fail_first_writes: AtomicUsize::new(0),
            fail_writes: AtomicBool::new(false),
            fail_first_reads: AtomicUsize::new(0),
            fail_reads: AtomicBool::new(false),
            write_attempts: AtomicUsize::new(0),
            read_attempts: AtomicUsize::new(0),
        })
    }

    /// The next `n` write attempts fail with a transient error, then writes
    /// succeed.
    pub fn fail_first_writes(&self, n: usize) {
        self.fail_first_writes.store(n, Ordering::SeqCst);
    }

    /// Every write fails (or succeeds again when `on` is false).
    pub fn always_fail_writes(&self, on: bool) {
        self.fail_writes.store(on, Ordering::SeqCst);
    }

    /// The next `n` read attempts fail with a transient error, then reads
    /// succeed.
    pub fn fail_first_reads(&self, n: usize) {
        self.fail_first_reads.store(n, Ordering::SeqCst);
    }

    pub fn write_attempts(&self) -> usize {
        self.write_attempts.load(Ordering::SeqCst)
    }

    pub fn read_attempts(&self) -> usize {
        self.read_attempts.load(Ordering::SeqCst)
    }

    pub fn reset_attempts(&self) {
        self.write_attempts.store(0, Ordering::SeqCst);
        self.read_attempts.store(0, Ordering::SeqCst);
    }

    /// True when a write attempt should fail, consuming one of the budgeted
    /// "fail first N" attempts if any remain.
    fn doomed_write(&self) -> bool {
        if self.fail_writes.load(Ordering::SeqCst) {
            return true;
        }
        let left = self.fail_first_writes.load(Ordering::SeqCst);
        if left > 0 {
            self.fail_first_writes.store(left - 1, Ordering::SeqCst);
            return true;
        }
        false
    }

    fn doomed_read(&self) -> bool {
        if self.fail_reads.load(Ordering::SeqCst) {
            return true;
        }
        let left = self.fail_first_reads.load(Ordering::SeqCst);
        if left > 0 {
            self.fail_first_reads.store(left - 1, Ordering::SeqCst);
            return true;
        }
        false
    }

    fn transient() -> Error {
        Error::Io(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "simulated sync-client sharing violation",
        ))
    }
}

impl FolderFs for HostileFs {
    fn list(&self, dir: &Path) -> Result<Vec<String>, Error> {
        RealFs.list(dir)
    }

    fn read(&self, path: &Path) -> Result<Vec<u8>, Error> {
        with_retry(|| {
            self.read_attempts.fetch_add(1, Ordering::SeqCst);
            if self.doomed_read() {
                return Err(HostileFs::transient());
            }
            Ok(fs::read(path)?)
        })
    }

    fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), Error> {
        with_retry(|| {
            self.write_attempts.fetch_add(1, Ordering::SeqCst);
            if self.doomed_write() {
                return Err(HostileFs::transient());
            }
            // The same write-once atomic publish RealFs performs: a fsynced
            // `*.tmp` renamed onto the final name.
            let mut tmp = path.as_os_str().to_os_string();
            tmp.push(".tmp");
            let tmp = PathBuf::from(tmp);
            let _ = fs::remove_file(&tmp);
            use std::io::Write as _;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)?;
            let written = file.write_all(bytes).and_then(|()| file.sync_all());
            drop(file);
            written?;
            fs::rename(&tmp, path)?;
            Ok(())
        })
    }

    fn create_dir_all(&self, dir: &Path) -> Result<(), Error> {
        RealFs.create_dir_all(dir)
    }

    fn exists(&self, path: &Path) -> Result<bool, Error> {
        RealFs.exists(path)
    }

    fn delete(&self, path: &Path) -> Result<(), Error> {
        RealFs.delete(path)
    }
}
