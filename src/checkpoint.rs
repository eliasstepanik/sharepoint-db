//! Compaction: snapshot checkpoints, a per-node version vector at the cut and
//! a retention window.
//!
//! The synced folder would otherwise grow without bound: every frame ever
//! committed stays in a segment file forever. Compaction bounds it with a
//! *checkpoint*: a write-once file in `ckpoint/` that carries a full SQLite
//! image of the merged state after the first `count` frames (in total order)
//! plus the metadata needed to prove that image belongs to this history:
//!
//! ```text
//! ckpoint/<cut_lamport:020>-<cut_digest_hex>.cpt
//!   magic "SDBCKP02" | db_id[16] | count u64 | cut_lamport u64
//!   | boundary_lamport u64 | boundary_node[8] | boundary_seq u64
//!   | cut_digest[32]
//!   | vv_len u32 | ( node[8] | max_seq u64 | hash[32] ) * vv_len
//!   | snapshot_len u64 | snapshot bytes | hash[32]
//! hash = SHA-256(magic|db_id|count|cut_lamport|boundary_*|cut_digest|vv_bytes|snapshot)
//! ```
//!
//! `boundary_*` is the sort key `(lamport, node_id, seq)` of the `count`-th
//! frame — the *cut boundary*. `cut_digest` is [`crate::materialize::digest`]
//! over the first `count` frames, so a node that still holds those frames in
//! its store can verify that the checkpoint belongs to *its* history and not to
//! a fork. A node whose store is shorter than `count` trusts the file's own
//! hash instead: that is what lets a brand-new node bootstrap from a checkpoint
//! alone.
//!
//! ## The version vector, and why the boundary alone is not enough
//!
//! `vv` is a *version vector* over the cut: for every node that has at least one
//! frame among the first `count`, the entry `(max_seq, hash)` where `max_seq`
//! is that node's highest `seq` inside the prefix and `hash` is that frame's
//! hash. Define
//!
//! ```text
//! covered(f) := f.seq <= vv[f.node].max_seq
//! ```
//!
//! Because a node's `seq` and `lamport` both increase along its own chain, the
//! first `count` frames are *exactly* the covered frames. That is the property
//! every compaction decision relies on:
//!
//! * **bootstrap** installs the snapshot and replays only the uncovered frames —
//!   never a frame the snapshot already accounts for, and never a frame the
//!   snapshot does not account for;
//! * **prune** deletes a segment of node `N` only when `vv[N]` covers its last
//!   frame, which is what makes deleting it safe for *every* node, not just the
//!   one that happens to be looking;
//! * the **publish floor** is `vv[self].max_seq`, so a node never republishes a
//!   range the checkpoint already carries.
//!
//! Deciding these by sort key alone is unsound: a segment can straddle the cut
//! (some frames covered, some not), and a node whose store is shorter than the
//! cut can hold a frame that sorts below the boundary yet is *not* in the
//! checkpoint's prefix. Both cases lose or double-apply frames, so the version
//! vector — not the boundary — is the authority.
//!
//! ## The USE gate
//!
//! A node may *use* a checkpoint (to bootstrap, to prune, or to raise its
//! publish floor) only when its magic, `db_id` and hash verify **and** every
//! frame in its own store that sorts below the boundary is covered by `vv`. A
//! node holding an uncovered frame below the boundary has state the snapshot
//! does not account for, so the snapshot is not a valid base for it and the
//! checkpoint is simply not a candidate for that node (see [`use_gate_ok`]).
//!
//! ## The one inherent limitation
//!
//! There is no membership protocol, so no node can know who is still offline. A
//! node that stays offline longer than the retention window can come back
//! holding below-boundary edits that the current checkpoint does **not** cover.
//! Those edits are never silently dropped: the USE gate fails, the node refuses
//! the checkpoint, and the frames it cannot apply are counted in
//! [`crate::SyncReport::dropped_below_boundary`] instead of being claimed as
//! applied. Recovery in that situation requires that node's own `store.db`,
//! which is never pruned — a node that synced at least once per retention
//! window always has it, and is always correct. A node that both stayed away
//! past the window *and* lost its store cannot be repaired from the folder
//! alone; that is the loss case, and it is reported, never hidden.
//!
//! A **bootstrapped** node (one that carries a [`Base`]) handles the same
//! situation without ever diverging: when no checkpoint both passes the USE gate
//! and dominates its current base, it does **not** rebuild from its store (which
//! would drop the state below its cut). It keeps its last-good `data.db`, marks
//! itself `materialize_pending` and carries `dropped_below_boundary` forward
//! across syncs — stale but never wrong — and recovers automatically once a
//! full-history node publishes a covering, dominating checkpoint.
//!
//! ### Only a full-history node may compact
//!
//! A bootstrapped node never writes a checkpoint and never prunes. Its store does
//! not hold the frames below its cut, so a `count`, `cut_digest` and version
//! vector taken from that store would describe an incomplete history and mislead
//! every node that trusted it. **At least one node that was never bootstrapped —
//! one that has kept its complete local `store.db` since the database began —
//! must remain in the cluster for it to keep compacting.** If only bootstrapped
//! nodes are left, the folder stops shrinking and a node that falls behind the
//! retention window can no longer be repaired from a checkpoint.
//!
//! The shared folder stays bounded: roughly `retention_segments` segment files
//! plus two checkpoint files (see [`prune_checkpoints`]).

use std::path::Path;

use rusqlite::backup::Backup;
use rusqlite::Connection;
use sha2::{Digest as _, Sha256};

use crate::error::{Error, Result};
use crate::frame::{DbId, NodeId};
use crate::materialize;
use crate::store::StoredFrame;
use crate::util::hex as hex_encode;

/// Magic at the start of every checkpoint file.
#[doc(hidden)]
pub const CKPOINT_MAGIC: &[u8; 8] = b"SDBCKP02";

/// The directory (inside the synced folder) that holds the checkpoints.
pub(crate) const CKPOINT_DIR: &str = "ckpoint";

/// How many checkpoints a folder keeps: the best one and the one before it.
pub const CHECKPOINTS_KEPT: usize = 2;

/// Fixed part of a checkpoint body before the version vector: magic + db_id +
/// count + cut_lamport + boundary lamport/node/seq + cut_digest.
const HEADER_LEN: usize = 8 + 16 + 8 + 8 + 8 + 8 + 8 + 32;

/// Bytes of one version-vector entry: node + max_seq + hash.
const VV_ENTRY_LEN: usize = 8 + 8 + 32;

/// The sort key `(lamport, node_id, seq)` of a frame, owned so it can outlive
/// the frame it came from.
pub type SortKey = (u64, NodeId, u64);

/// The sort key of `frame`, as an owned tuple.
pub(crate) fn sort_key_of(frame: &StoredFrame) -> SortKey {
    (frame.frame.lamport, frame.node_id, frame.frame.seq)
}

/// One node's entry in a version vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    /// The node.
    pub node: NodeId,
    /// Its highest `seq` inside the checkpoint's prefix.
    pub max_seq: u64,
    /// The hash of that frame.
    pub hash: [u8; 32],
}

/// The version vector at a checkpoint cut, sorted by node id.
///
/// `covered(seq_of_node)` is true when the snapshot already accounts for that
/// frame, i.e. when `seq <= vv[node].max_seq`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionVector(pub Vec<Version>);

impl VersionVector {
    /// Builds the vector from the first `count` frames of a store in total
    /// order. Frames beyond `count` are ignored.
    pub fn build(frames: &[StoredFrame], count: usize) -> Self {
        let mut entries: Vec<Version> = Vec::new();
        for frame in frames.iter().take(count) {
            let node = frame.node_id;
            let seq = frame.frame.seq;
            let hash = frame.frame.hash;
            match entries.iter_mut().find(|v| v.node == node) {
                // A node's seq increases with its position in the total order,
                // so the last frame seen for a node is its highest.
                Some(entry) => {
                    entry.max_seq = seq;
                    entry.hash = hash;
                }
                None => entries.push(Version {
                    node,
                    max_seq: seq,
                    hash,
                }),
            }
        }
        entries.sort_by_key(|a| a.node);
        VersionVector(entries)
    }

    /// `node`'s entry, if the node has any frame in the prefix.
    pub fn get(&self, node: &NodeId) -> Option<&Version> {
        self.0.iter().find(|v| v.node == *node)
    }

    /// Whether the snapshot already accounts for `seq` of `node`.
    pub fn covered(&self, node: &NodeId, seq: u64) -> bool {
        self.get(node).is_some_and(|v| seq <= v.max_seq)
    }

    /// Encodes the vector: `vv_len u32` then one entry per node.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.0.len() * VV_ENTRY_LEN);
        out.extend_from_slice(&(self.0.len() as u32).to_le_bytes());
        for entry in &self.0 {
            out.extend_from_slice(&entry.node);
            out.extend_from_slice(&entry.max_seq.to_le_bytes());
            out.extend_from_slice(&entry.hash);
        }
        out
    }

    /// Decodes a vector, returning it and the number of bytes consumed.
    fn decode(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < 4 {
            return Err(Error::CorruptCheckpoint(
                "checkpoint version vector truncated".into(),
            ));
        }
        let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let overflow =
            || Error::CorruptCheckpoint("checkpoint version vector length overflows".into());
        let need = 4usize
            .checked_add(len.checked_mul(VV_ENTRY_LEN).ok_or_else(overflow)?)
            .ok_or_else(overflow)?;
        if bytes.len() < need {
            return Err(Error::CorruptCheckpoint(
                "checkpoint version vector longer than the file".into(),
            ));
        }
        let mut entries = Vec::with_capacity(len);
        for i in 0..len {
            let at = 4 + i * VV_ENTRY_LEN;
            let node = <[u8; 8]>::try_from(&bytes[at..at + 8]).expect("length checked above");
            let mut seq = [0u8; 8];
            seq.copy_from_slice(&bytes[at + 8..at + 16]);
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&bytes[at + 16..at + 48]);
            entries.push(Version {
                node,
                max_seq: u64::from_le_bytes(seq),
                hash,
            });
        }
        Ok((VersionVector(entries), need))
    }
}

/// A decoded checkpoint: the metadata of a cut, its version vector and the
/// snapshot it carries.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    /// Number of frames (in total order) folded into this checkpoint.
    pub count: u64,
    /// The highest lamport timestamp in the store when it was written.
    pub cut_lamport: u64,
    /// Sort key of the `count`-th frame: the cut boundary.
    pub boundary: SortKey,
    /// `digest(first count frames)`.
    pub cut_digest: [u8; 32],
    /// The version vector at the cut.
    pub vv: VersionVector,
    /// A full SQLite image of the materialised state after those frames.
    pub snapshot: Vec<u8>,
}

impl Checkpoint {
    /// The name this checkpoint is written under inside `ckpoint/`.
    pub fn file_name(&self) -> String {
        checkpoint_name(self.cut_lamport, &self.cut_digest)
    }

    /// The sort key of the last frame folded in (the cut boundary).
    pub fn boundary(&self) -> SortKey {
        self.boundary
    }

    /// Whether the snapshot already accounts for `seq` of `node`.
    pub fn covered(&self, node: &NodeId, seq: u64) -> bool {
        self.vv.covered(node, seq)
    }

    /// Encodes the file body (header, version vector, snapshot and trailing hash).
    pub fn encode(&self, db_id: &DbId) -> Vec<u8> {
        let vv = self.vv.encode();
        let mut out = Vec::with_capacity(HEADER_LEN + vv.len() + 8 + self.snapshot.len() + 32);
        out.extend_from_slice(CKPOINT_MAGIC);
        out.extend_from_slice(db_id);
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.cut_lamport.to_le_bytes());
        out.extend_from_slice(&self.boundary.0.to_le_bytes());
        out.extend_from_slice(&self.boundary.1);
        out.extend_from_slice(&self.boundary.2.to_le_bytes());
        out.extend_from_slice(&self.cut_digest);
        out.extend_from_slice(&vv);
        out.extend_from_slice(&(self.snapshot.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.snapshot);
        out.extend_from_slice(&body_hash(&out));
        out
    }
}

/// SHA-256 over everything before the trailing hash field.
fn body_hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// The fixed header of a checkpoint, without the version vector or the snapshot.
///
/// Read from the first bytes of a file so a folder scan can decide *which*
/// checkpoint is worth reading in full without decoding every snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Frames folded into the checkpoint.
    pub count: u64,
    /// The lamport the cut was taken at.
    pub cut_lamport: u64,
    /// The cut boundary.
    pub boundary: SortKey,
    /// `digest(first count frames)`.
    pub cut_digest: [u8; 32],
    /// Where the version vector starts (and the header ends).
    pub vv_at: usize,
}

/// Parses the fixed header of a checkpoint body, verifying magic and `db_id`.
///
/// The trailing hash covers the version vector and the snapshot too, so a header
/// alone proves nothing about them: it is a *candidate* read, and the full
/// [`decode`] is what verifies.
pub(crate) fn decode_header(bytes: &[u8], expected_db_id: &DbId) -> Result<Header> {
    if bytes.len() < HEADER_LEN + 4 {
        return Err(Error::CorruptCheckpoint(
            "checkpoint too short for a header".into(),
        ));
    }
    if &bytes[..8] != CKPOINT_MAGIC {
        return Err(Error::CorruptCheckpoint("bad checkpoint magic".into()));
    }
    let db_id = <[u8; 16]>::try_from(&bytes[8..24]).expect("length checked above");
    if &db_id != expected_db_id {
        return Err(Error::CorruptCheckpoint(
            "checkpoint belongs to another database".into(),
        ));
    }
    Ok(Header {
        count: read_u64(bytes, 24),
        cut_lamport: read_u64(bytes, 32),
        boundary: (
            read_u64(bytes, 40),
            read_node(bytes, 48),
            read_u64(bytes, 56),
        ),
        cut_digest: <[u8; 32]>::try_from(&bytes[64..HEADER_LEN]).expect("length checked above"),
        vv_at: HEADER_LEN,
    })
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(b)
}

fn read_node(bytes: &[u8], at: usize) -> NodeId {
    <[u8; 8]>::try_from(&bytes[at..at + 8]).expect("length checked by the caller")
}

/// Decodes a checkpoint body, verifying its magic, `db_id` and hash.
///
/// The `cut_digest` is *not* checked here: only a node that holds the folded
/// frames in its store can judge it, and that check is [`cut_matches`].
pub fn decode(bytes: &[u8], expected_db_id: &DbId) -> Result<Checkpoint> {
    let header = decode_header(bytes, expected_db_id)?;
    if bytes.len() < HEADER_LEN + 32 {
        return Err(Error::CorruptCheckpoint("checkpoint too short".into()));
    }
    let stored = &bytes[bytes.len() - 32..];
    if body_hash(&bytes[..bytes.len() - 32]) != stored {
        return Err(Error::CorruptCheckpoint(
            "checkpoint body hash mismatch".into(),
        ));
    }
    let (vv, used) = VersionVector::decode(&bytes[header.vv_at..])?;
    let after_vv = header.vv_at + used;
    if bytes.len() < after_vv + 8 + 32 {
        return Err(Error::CorruptCheckpoint(
            "checkpoint truncated after the version vector".into(),
        ));
    }
    let snapshot_len = read_u64(bytes, after_vv) as usize;
    let snapshot_at = after_vv + 8;
    let Some(end) = snapshot_at
        .checked_add(snapshot_len)
        .and_then(|e| e.checked_add(32))
    else {
        return Err(Error::CorruptCheckpoint(
            "checkpoint snapshot length overflows".into(),
        ));
    };
    if end != bytes.len() {
        return Err(Error::CorruptCheckpoint(
            "checkpoint snapshot length does not match the file".into(),
        ));
    }
    let snapshot = bytes[snapshot_at..snapshot_at + snapshot_len].to_vec();
    Ok(Checkpoint {
        count: header.count,
        cut_lamport: header.cut_lamport,
        boundary: header.boundary,
        cut_digest: header.cut_digest,
        vv,
        snapshot,
    })
}

/// The name a checkpoint is written under: `<cut_lamport:020>-<cut_digest>.cpt`.
///
/// The name is a hint only — the file's content and hash are the authority — but
/// it makes a checkpoint for a given cut unique, which is what makes writing one
/// idempotent.
pub(crate) fn checkpoint_name(cut_lamport: u64, cut_digest: &[u8; 32]) -> String {
    format!("{cut_lamport:020}-{}.cpt", hex_encode(cut_digest))
}

/// True for a file name a sync should read as a checkpoint.
pub(crate) fn is_checkpoint_name(name: &str) -> bool {
    name.ends_with(".cpt") && !name.ends_with(".tmp")
}

/// Whether `cp`'s `cut_digest` agrees with this store's first `cp.count` frames.
///
/// A node that holds fewer than `count` frames cannot judge the digest and
/// trusts the file's own hash (that is how a new node bootstraps). A node that
/// holds at least `count` frames must agree with the checkpoint's `cut_digest`
/// or the checkpoint belongs to a different history and is ignored.
pub(crate) fn cut_matches(cp: &Checkpoint, store_frame_count: u64, store_digest: [u8; 32]) -> bool {
    if cp.count > store_frame_count {
        return true;
    }
    cp.cut_digest == store_digest
}

/// The USE gate: may this node use `cp` as a base?
///
/// Beyond magic, `db_id` and hash (verified by [`decode`]) and the cut digest
/// ([`cut_matches`]), a node may only build on a snapshot that accounts for
/// *everything it holds below the boundary*: if any frame in this node's store
/// sorts strictly below `cp.boundary` yet is not covered by `cp.vv`, the
/// snapshot is missing state this node has, and using it would drop that state.
///
/// `frames` is the node's store in total order; the frames below the boundary
/// are its prefix, so the scan stops at the first frame at or above the cut.
pub(crate) fn use_gate_ok(cp: &Checkpoint, frames: &[StoredFrame]) -> bool {
    frames
        .iter()
        .take_while(|f| sort_key_of(f) < cp.boundary)
        .all(|f| cp.covered(&f.node_id, f.frame.seq))
}

/// How many frames a cut of `count` frames folded in that this store does **not**
/// hold — the frames that would be lost if the node had to fall back to a
/// store-only rebuild because the checkpoint failed [`use_gate_ok`].
///
/// A node that stayed online holds every covered frame, so this is `0` and a
/// plain full-store replay loses nothing. A node whose store is behind the cut
/// (it bootstrapped earlier, or lost its state) holds only the frames above the
/// boundary, so every covered frame it is missing counts here: that is the
/// documented loss case, and it is reported rather than silently applied.
pub(crate) fn missing_from_store(count: u64, vv: &VersionVector, frames: &[StoredFrame]) -> usize {
    let held = frames
        .iter()
        .filter(|f| vv.covered(&f.node_id, f.frame.seq))
        .count() as u64;
    count.saturating_sub(held) as usize
}

// ---------------------------------------------------------------------------
// Snapshot capture and restore
// ---------------------------------------------------------------------------

/// A full SQLite image of `data`, as bytes.
///
/// Captured with SQLite's online backup API into a throw-away file connection
/// (`scratch`, which must live in the state directory, never in the synced
/// folder), then read back. The backup API is used rather than
/// `Connection::serialize` because it produces a complete, self-consistent file
/// image without holding a lock on the source for the whole copy, and the image
/// round-trips by simply opening it again.
pub(crate) fn capture(data: &Connection, scratch: &Path) -> Result<Vec<u8>> {
    let _ = std::fs::remove_file(scratch);
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut p = scratch.as_os_str().to_os_string();
        p.push(suffix);
        let _ = std::fs::remove_file(Path::new(&p));
    }
    let mut dest = Connection::open(scratch)?;
    {
        let backup = Backup::new(data, &mut dest)?;
        backup.run_to_completion(64, std::time::Duration::from_millis(0), None)?;
    }
    drop(dest);
    let bytes = std::fs::read(scratch)?;
    // The scratch file is a copy of a copy: the snapshot lives in the checkpoint,
    // so nothing is lost by removing it here.
    let _ = std::fs::remove_file(scratch);
    Ok(bytes)
}

/// Installs `snapshot` as `data_path` and opens it.
///
/// The snapshot is a complete database image, so restoring is "write the bytes,
/// open the file": no replay is needed and the result is byte-identical to what
/// replaying the folded frames would have produced.
pub(crate) fn restore(snapshot: &[u8], data_path: &Path) -> Result<Connection> {
    // A file that cannot be removed makes the write below fail.
    let _ = crate::db::remove_db_sidecars(data_path);
    std::fs::write(data_path, snapshot)
        .map_err(|e| Error::Store(format!("cannot restore data.db: {e}")))?;
    let conn = Connection::open(data_path)?;
    materialize::ensure_meta(&conn)?;
    Ok(conn)
}

// ---------------------------------------------------------------------------
// Choosing the best checkpoint (the USE gate decides candidacy)
// ---------------------------------------------------------------------------

/// What [`read_best`] found: the best checkpoint this node may use, and — when a
/// candidate was rejected by the USE gate — how many frames that means this node
/// cannot account for.
pub(crate) struct ReadBest {
    /// The best checkpoint that passes the USE gate for this node, if any.
    pub(crate) best: Option<Checkpoint>,
    /// Frames the rejected candidate folded in that this node's store does not
    /// hold: the state that a full-store replay cannot rebuild. Non-zero only
    /// when a checkpoint failed the gate *and* the store is behind that cut.
    pub(crate) dropped_below_boundary: usize,
}

/// Reads and verifies every checkpoint in `dir`, applies the USE gate for this
/// node's store, and picks the best one.
///
/// `frames` is this node's whole store in total order: it is what the cut digest
/// and the USE gate are judged against. Files that do not decode, do not verify,
/// belong to a different history or fail the gate are ignored for this node, as
/// are read failures: a checkpoint is an optimisation, and a folder that refuses
/// a read simply means this node rebuilds the slow way.
///
/// Each file is read once and its *header* parsed first, so the cheap checks
/// (magic, `db_id`, count, boundary) decide the order candidates are tried in;
/// the full decode — hash over the whole body and the snapshot copy — runs only
/// for the candidates that can still win.
pub(crate) fn read_best(
    fs: &dyn crate::folder::FolderFs,
    dir: &Path,
    db_id: &DbId,
    frames: &[StoredFrame],
) -> ReadBest {
    let total = frames.len() as u64;
    let mut dropped_below_boundary = 0usize;
    let names = match fs.list(dir) {
        Ok(names) => names,
        Err(_) => {
            return ReadBest {
                best: None,
                dropped_below_boundary,
            }
        }
    };
    // Pass 1: header only, so candidates can be ordered by `count` before the
    // expensive full decode. Files that do not even have a valid header are
    // dropped here.
    let mut candidates: Vec<(String, Vec<u8>, Header)> = Vec::new();
    for name in names {
        if !is_checkpoint_name(&name) {
            continue;
        }
        let bytes = match fs.read(&dir.join(&name)) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let header = match decode_header(&bytes, db_id) {
            Ok(header) => header,
            Err(_) => continue,
        };
        candidates.push((name, bytes, header));
    }
    // Highest count first, ties to the smallest name: the first candidate that
    // survives the full checks is the winner.
    candidates.sort_by(|a, b| b.2.count.cmp(&a.2.count).then_with(|| a.0.cmp(&b.0)));
    for (_name, bytes, header) in candidates {
        let cp = match decode(&bytes, db_id) {
            Ok(cp) => cp,
            Err(_) => continue,
        };
        // The header is only a candidate read: its `count` and `boundary` must
        // agree with what the verified body says, or the name was paired with the
        // wrong bytes and the file is not trusted.
        if header.count != cp.count
            || header.boundary != cp.boundary
            || header.cut_digest != cp.cut_digest
        {
            continue;
        }
        if !cut_matches(
            &cp,
            total,
            crate::store::digest_first(frames, cp.count as usize),
        ) {
            continue;
        }
        if !use_gate_ok(&cp, frames) {
            // The snapshot is missing state this node holds below the boundary,
            // so it is not a valid base for this node. The fallback is a plain
            // full-store replay, which loses whatever the checkpoint folded in
            // and this store does not hold: that is the documented loss case,
            // and it is reported rather than silently claimed as applied.
            dropped_below_boundary =
                dropped_below_boundary.max(missing_from_store(cp.count, &cp.vv, frames));
            continue;
        }
        return ReadBest {
            best: Some(cp),
            dropped_below_boundary: 0,
        };
    }
    ReadBest {
        best: None,
        dropped_below_boundary,
    }
}

// ---------------------------------------------------------------------------
// Prune
// ---------------------------------------------------------------------------

/// A segment file in the folder, reduced to what a prune decision needs.
struct SegmentInfo {
    name: String,
    node: NodeId,
    /// The highest `seq` the file's name announces (used for the coverage test).
    last_seq: u64,
    /// The segment's maximum sort key `(lamport, node, seq)`, read from the
    /// decoded body. Used to order the retention window across nodes.
    max: SortKey,
}

/// Deletes the segment files that the checkpoint with version vector `vv` made
/// redundant.
///
/// A segment of node `N` is deletable iff **both** hold:
///
/// * `segment.last_seq <= vv[N].max_seq` — the checkpoint's snapshot already
///   accounts for *every* frame in the file, which is the same as saying every
///   frame in it is at or below the cut boundary (a frame is covered exactly when
///   it is inside the folded prefix). A segment that straddles the cut — some
///   frames covered, some not — is kept whole, because a segment is never
///   rewritten, only deleted;
/// * it is **not** among the `retention_segments` segments with the highest
///   maximum sort key overall — the retention window that keeps a node which
///   lags by that much able to catch up from the folder alone.
///
/// A segment whose name cannot be parsed is never deleted (its range is unknown),
/// and a checkpoint file is never a candidate: only `segments/` is scanned. The
/// coverage test uses the range from the file *name*; the retention window is
/// ordered by each segment's decoded maximum sort key, because `seq` is per-node
/// and so does not order segments from different nodes. Deletion is best-effort:
/// every IO error is ignored, because a folder that refuses a delete merely
/// leaves the folder bigger than it had to be.
///
/// Returns the `(node, name)` of every segment that was deleted, so the caller
/// can stop republishing its own ranges that compaction removed.
pub(crate) fn prune(
    fs: &dyn crate::folder::FolderFs,
    segments_dir: &Path,
    db_id: &DbId,
    vv: &VersionVector,
    retention_segments: usize,
) -> Vec<(NodeId, String)> {
    let mut deleted: Vec<(NodeId, String)> = Vec::new();
    let names = match fs.list(segments_dir) {
        Ok(names) => names,
        Err(_) => return deleted,
    };
    let mut segments: Vec<SegmentInfo> = Vec::new();
    for name in names {
        if !crate::folder::is_segment_name(&name) {
            continue;
        }
        // The name carries `(node, first_seq, last_seq)`; a name that cannot be
        // parsed says nothing, and an unknown range is never deleted.
        let Some((node, _first_seq, last_seq)) = crate::folder::parse_segment_name(&name) else {
            continue;
        };
        // The retention window is ordered by the segment's real maximum sort key,
        // which needs the decoded body: `seq` is per-node, so a peer's low-seq
        // segment can be far newer than ours in total order.
        let bytes = match fs.read(&segments_dir.join(&name)) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let Ok(segment) = crate::folder::decode_segment(&bytes, db_id) else {
            // A segment that cannot be decoded has an unknown sort key; it is
            // never deleted (its range is unknown to the retention window).
            continue;
        };
        let last = match segment.frames.last() {
            Some(frame) => frame,
            None => continue,
        };
        segments.push(SegmentInfo {
            name,
            node,
            last_seq,
            max: (last.lamport, node, last.seq),
        });
    }

    // The retention window: the `retention_segments` segments with the highest
    // maximum sort key are protected no matter how well covered they are.
    let protected: Vec<String> = {
        let mut by_max: Vec<&SegmentInfo> = segments.iter().collect();
        by_max.sort_by(|a, b| b.max.cmp(&a.max).then_with(|| a.name.cmp(&b.name)));
        by_max
            .into_iter()
            .take(retention_segments)
            .map(|s| s.name.clone())
            .collect()
    };

    for segment in segments {
        if !vv.covered(&segment.node, segment.last_seq) || protected.contains(&segment.name) {
            continue;
        }
        if fs.delete(&segments_dir.join(&segment.name)).is_ok() {
            deleted.push((segment.node, segment.name));
        }
    }
    deleted
}

/// Deletes checkpoints that the newest cut superseded, keeping
/// [`CHECKPOINTS_KEPT`] of them: the best one and the one before it (by `count`).
///
/// Without this the `ckpoint/` folder grows with every cut. Keeping the previous
/// checkpoint as well as the best one means a node that is one cut behind still
/// finds a checkpoint it can use, so the folder stays bounded at about
/// `retention_segments` segments plus two checkpoints.
///
/// Only files that decode as checkpoints for `db_id` are considered, so a
/// corrupted file is left alone rather than guessed at. Deletion is best-effort.
pub(crate) fn prune_checkpoints(
    fs: &dyn crate::folder::FolderFs,
    dir: &Path,
    db_id: &DbId,
) -> Vec<String> {
    let names = match fs.list(dir) {
        Ok(names) => names,
        Err(_) => return Vec::new(),
    };
    let mut known: Vec<(String, u64)> = Vec::new();
    for name in names {
        if !is_checkpoint_name(&name) {
            continue;
        }
        let bytes = match fs.read(&dir.join(&name)) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        match decode_header(&bytes, db_id) {
            Ok(header) => known.push((name, header.count)),
            Err(_) => continue,
        }
    }
    if known.len() <= CHECKPOINTS_KEPT {
        return Vec::new();
    }
    // Same order as `select`: highest count, ties to the smallest name.
    known.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut deleted = Vec::new();
    for (name, _) in known.into_iter().skip(CHECKPOINTS_KEPT) {
        if fs.delete(&dir.join(&name)).is_ok() {
            deleted.push(name);
        }
    }
    deleted
}

// ---------------------------------------------------------------------------
// The `Base` record: what a bootstrapped node remembers about its snapshot
// ---------------------------------------------------------------------------

/// What a bootstrapped `data.db` was built from, recorded in **`store.db`'s
/// meta** (not in `data.db`): `store.db` is never pruned and survives a lost or
/// corrupted `data.db`, so a node still knows it is bootstrapped — and which
/// frames the snapshot already accounts for — after its data file is gone.
///
/// A bootstrapped data file is *not* a replay of this node's store: it is the
/// checkpoint snapshot plus the store frames the version vector does not cover.
/// A store-only rebuild would therefore throw away everything the snapshot
/// carried below the boundary, so a node with a `Base` may only be brought in
/// line again by another bootstrap (see [`crate::db`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Base {
    /// Frames folded into the checkpoint this data file was built from.
    pub count: u64,
    /// The checkpoint's cut boundary.
    pub boundary: SortKey,
    /// The checkpoint's cut digest.
    pub cut_digest: [u8; 32],
    /// The highest lamport timestamp the checkpoint's store held when the cut was
    /// taken. A bootstrapped node's own store does not hold the frames below the
    /// cut, so its lamport clock must start here — not at its store's (empty or
    /// near-empty) maximum — or its own new commits would sort below the boundary
    /// and be dropped or misordered by every other node.
    pub cut_lamport: u64,
    /// The version vector at the cut.
    pub vv: VersionVector,
}

impl Base {
    /// The `Base` a bootstrap records for `cp`.
    pub(crate) fn of(cp: &Checkpoint) -> Base {
        Base {
            count: cp.count,
            boundary: cp.boundary,
            cut_digest: cp.cut_digest,
            cut_lamport: cp.cut_lamport,
            vv: cp.vv.clone(),
        }
    }

    /// Whether the snapshot this base came from already accounts for `seq` of
    /// `node`.
    pub fn covered(&self, node: &NodeId, seq: u64) -> bool {
        self.vv.covered(node, seq)
    }

    /// Encodes the record as a single hex string for `store.db`'s meta table.
    pub(crate) fn encode(&self) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.count.to_le_bytes());
        bytes.extend_from_slice(&self.boundary.0.to_le_bytes());
        bytes.extend_from_slice(&self.boundary.1);
        bytes.extend_from_slice(&self.boundary.2.to_le_bytes());
        bytes.extend_from_slice(&self.cut_digest);
        bytes.extend_from_slice(&self.cut_lamport.to_le_bytes());
        bytes.extend_from_slice(&self.vv.encode());
        hex_encode(&bytes)
    }

    /// Decodes [`Base::encode`]; `None` for anything malformed (a garbage meta
    /// value simply means "not bootstrapped").
    pub(crate) fn decode(hex: &str) -> Option<Base> {
        let bytes = crate::util::hex_decode(hex)?;
        if bytes.len() < 8 + 8 + 8 + 8 + 32 + 8 + 4 {
            return None;
        }
        let count = read_u64(&bytes, 0);
        let boundary = (
            read_u64(&bytes, 8),
            read_node(&bytes, 16),
            read_u64(&bytes, 24),
        );
        let cut_digest = <[u8; 32]>::try_from(&bytes[32..64]).ok()?;
        let cut_lamport = read_u64(&bytes, 64);
        let (vv, used) = VersionVector::decode(&bytes[72..]).ok()?;
        if 72 + used != bytes.len() {
            return None;
        }
        Some(Base {
            count,
            boundary,
            cut_digest,
            cut_lamport,
            vv,
        })
    }
}

impl Base {
    /// How many frames this base's snapshot folded in that `frames` (the node's
    /// store, in total order) does not hold.
    ///
    /// A node that stayed online holds every covered frame, so this is `0` and a
    /// plain full-store replay loses nothing. A non-zero value is the documented
    /// loss case: the state below the cut exists neither in the store nor in a
    /// usable checkpoint, so it cannot be rebuilt and must be reported.
    pub(crate) fn missing_from_store(&self, frames: &[StoredFrame]) -> usize {
        missing_from_store(self.count, &self.vv, frames)
    }

    /// Whether a checkpoint cut at version vector `other` may replace this base.
    ///
    /// A bootstrapped node may only (re-)bootstrap from a checkpoint whose
    /// version vector **dominates** its current base: for every node, the new
    /// vector's `max_seq` must be at least the base's `max_seq` (a node absent
    /// from the base counts as `0`). A checkpoint that is lower for some node
    /// would drop frames the current base's snapshot was relying on, so it is
    /// not usable by this node — in addition to the existing USE gate.
    pub(crate) fn dominates(&self, other: &VersionVector) -> bool {
        self.vv
            .0
            .iter()
            .all(|v| other.get(&v.node).is_some_and(|o| o.max_seq >= v.max_seq))
    }
}
