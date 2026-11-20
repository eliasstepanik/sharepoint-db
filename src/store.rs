//! The local frame store (`store.db`).
//!
//! `store.db` holds *every* frame this node has ever seen — its own commits and
//! its peers' — in a grow-only table written with `PRAGMA synchronous = FULL`.
//! It is the durability point of the commit protocol: once a frame is in the
//! store the change is committed, and the user database (`data.db`) can be
//! rebuilt from it at any time.
//!
//! The store also remembers this machine's node identity (a random 8-byte id).
//! The `(applied_count, applied_digest)` bookkeeping pair that tells
//! [`crate::ShareDb`] whether `data.db` is in sync with the store lives in
//! `data.db` itself (in `__sharedb_meta`), not here: it describes the data file,
//! so it must be durable and rolled back together with the data it describes.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension};

use crate::error::{Error, Result};
use crate::frame::{DbId, Frame, FrameKind, FrameRef, NodeId};
use crate::materialize::digest;

/// Opens (creating the schema if needed) the frame store at `path`.
///
/// The store is durable on every write: `synchronous = FULL`.
pub fn open(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "DELETE")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS frames (
             node_id   BLOB NOT NULL,
             seq       INTEGER NOT NULL,
             lamport   INTEGER NOT NULL,
             kind      INTEGER NOT NULL,
             version   INTEGER NOT NULL,
             prev_hash BLOB NOT NULL,
             payload   BLOB NOT NULL,
             hash      BLOB NOT NULL,
             PRIMARY KEY (node_id, seq)
         );
         CREATE TABLE IF NOT EXISTS meta (
             key   TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );",
    )?;
    Ok(conn)
}

/// Reads (or, on first use, creates and persists) this node's id.
///
/// The id lives in the store's `meta` table under `node_id`. A lost state
/// directory therefore yields a fresh node id, which is by design: the node
/// simply reappears as a new writer and its frames are re-merged by total order.
pub fn load_or_create_node_id(store: &Connection) -> Result<NodeId> {
    let existing: Option<String> = store
        .query_row("SELECT value FROM meta WHERE key = 'node_id'", [], |row| {
            row.get::<_, String>(0)
        })
        .optional()?;
    if let Some(hex) = existing {
        return parse_node_id(&hex);
    }
    let id = crate::materialize::new_id();
    store.execute(
        "INSERT INTO meta (key, value) VALUES ('node_id', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [&id],
    )?;
    parse_node_id(&id)
}

fn parse_node_id(hex: &str) -> Result<NodeId> {
    let bytes = parse_hex(hex, 8)?;
    Ok(<[u8; 8]>::try_from(bytes).expect("parse_hex(…, 8) yields 8 bytes"))
}

/// Records the database id this store belongs to, or checks it still agrees.
///
/// The store holds frames whose hashes are computed over the `db_id`, so a store
/// that was written for a different database can never be read correctly (every
/// frame would fail its hash check, at best). Persisting the id catches the
/// hazard where a `MANIFEST` is swapped under a state directory — the store and
/// the manifest then disagree, and the disagreement is reported as a manifest
/// error instead of surfacing much later as a hash mismatch.
pub(crate) fn verify_db_id(store: &Connection, db_id: &DbId) -> Result<()> {
    let hex = crate::util::hex(db_id);
    match meta_get(store, "db_id")? {
        Some(existing) => {
            if existing != hex {
                return Err(Error::Manifest(format!(
                    "the local frame store belongs to database {existing}, but the MANIFEST says {hex}"
                )));
            }
            Ok(())
        }
        None => meta_set(store, "db_id", &hex),
    }
}

fn parse_hex(hex: &str, len: usize) -> Result<Vec<u8>> {
    let bytes = hex.as_bytes();
    if bytes.len() != len * 2 {
        return Err(Error::Store(format!(
            "bad hex length: expected {}, got {}",
            len * 2,
            bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let hi = hex_val(bytes[i * 2])?;
        let lo = hex_val(bytes[i * 2 + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_val(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(Error::Store(format!("bad hex byte: {}", c as char))),
    }
}

/// Appends a frame produced by `node_id` to the store.
///
/// Inserting a frame that is already present (same `(node_id, seq)` and same
/// hash) is a no-op; a frame that claims an occupied position with a different
/// hash is a fork and is refused.
///
/// The frame must continue this node's *chain anchor*: its `seq` has to be one
/// past the anchor's `seq` and its `prev_hash` has to equal the anchor's hash
/// ([`Error::FrameGap`] / [`Error::HashMismatch`]). The anchor is
/// [`anchor_of`]: this node's own last stored frame, or — when the node
/// bootstrapped from a checkpoint and does not hold the frames below the cut —
/// the checkpoint's version-vector entry for that node. The anchor is what lets
/// a bootstrapped node ingest a peer's retained segments that start *above* the
/// cut, which a "highest stored seq" rule would reject as a gap.
pub fn append_frame(store: &Connection, node_id: &NodeId, frame: &Frame) -> Result<()> {
    let existing: Option<Vec<u8>> = store
        .query_row(
            "SELECT hash FROM frames WHERE node_id = ?1 AND seq = ?2",
            (node_id.as_slice(), frame.seq as i64),
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    if let Some(hash) = existing {
        if hash == frame.hash.as_slice() {
            return Ok(());
        }
        return Err(Error::Fork { seq: frame.seq });
    }
    let anchor = anchor_of(store, node_id)?;
    if frame.seq != anchor.0 + 1 {
        return Err(Error::FrameGap {
            expected: anchor.0 + 1,
            found: frame.seq,
        });
    }
    if anchor.1 != frame.prev_hash {
        return Err(Error::HashMismatch);
    }
    let version = frame.migration_version().unwrap_or(0) as i64;
    store.execute(
        "INSERT INTO frames (node_id, seq, lamport, kind, version, prev_hash, payload, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            node_id.as_slice(),
            frame.seq as i64,
            frame.lamport as i64,
            frame.kind.kind_byte() as i64,
            version,
            frame.prev_hash.as_slice(),
            frame.payload.as_slice(),
            frame.hash.as_slice(),
        ],
    )?;
    Ok(())
}

/// A frame together with the id of the node that produced it, owned by value.
pub struct StoredFrame {
    /// The node that produced the frame.
    pub node_id: NodeId,
    /// The frame.
    pub frame: Frame,
}

impl StoredFrame {
    /// Borrows this frame as a [`FrameRef`].
    pub fn as_ref(&self) -> FrameRef<'_> {
        FrameRef {
            node_id: &self.node_id,
            frame: &self.frame,
        }
    }
}

/// Reads every stored frame, ordered by the total order `(lamport, node_id, seq)`.
///
/// Each frame's stored hash is re-verified against its body; a corrupted row
/// surfaces as [`Error::HashMismatch`].
pub fn all_frames(store: &Connection, db_id: &DbId) -> Result<Vec<StoredFrame>> {
    let mut stmt = store.prepare(
        "SELECT node_id, seq, lamport, kind, version, prev_hash, payload, hash
         FROM frames
         ORDER BY lamport ASC, node_id ASC, seq ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, i64>(1)? as u64,
            row.get::<_, i64>(2)? as u64,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)? as u32,
            row.get::<_, Vec<u8>>(5)?,
            row.get::<_, Vec<u8>>(6)?,
            row.get::<_, Vec<u8>>(7)?,
        ))
    })?;

    let mut out = Vec::new();
    for row in rows {
        let (node, seq, lamport, kind, version, prev_hash, payload, hash) = row?;
        let node_id = <[u8; 8]>::try_from(node)
            .map_err(|_| Error::Store("stored frame node_id is not 8 bytes".into()))?;
        let prev_hash = fixed32(&prev_hash, "prev_hash")?;
        let stored_hash = fixed32(&hash, "hash")?;

        let frame_kind = frame_kind_from_byte(kind, version)?;
        let recomputed = crate::frame::frame_hash(
            db_id,
            &node_id,
            seq,
            lamport,
            frame_kind.kind_byte(),
            &prev_hash,
            &payload,
        );
        if recomputed != stored_hash {
            return Err(Error::HashMismatch);
        }
        let frame = Frame {
            seq,
            lamport,
            kind: frame_kind,
            prev_hash,
            payload,
            hash: stored_hash,
        };
        out.push(StoredFrame { node_id, frame });
    }
    Ok(out)
}

fn fixed32(bytes: &[u8], what: &str) -> Result<[u8; 32]> {
    <[u8; 32]>::try_from(bytes)
        .map_err(|_| Error::Store(format!("stored frame {what} is not 32 bytes")))
}

/// The highest `seq` stored for `node_id`, or `0` when it has no frames.
pub(crate) fn own_max_seq(store: &Connection, node_id: &NodeId) -> Result<u64> {
    let seq: Option<i64> = store
        .query_row(
            "SELECT max(seq) FROM frames WHERE node_id = ?1",
            [node_id.as_slice()],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()?
        .flatten();
    Ok(seq.unwrap_or(0) as u64)
}

/// The stored hash at `(node_id, seq)`, if any.
pub(crate) fn own_frame_hash(
    store: &Connection,
    node_id: &NodeId,
    seq: u64,
) -> Result<Option<[u8; 32]>> {
    let hash: Option<Vec<u8>> = store
        .query_row(
            "SELECT hash FROM frames WHERE node_id = ?1 AND seq = ?2",
            (node_id.as_slice(), seq as i64),
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    hash.map(|h| fixed32(&h, "hash")).transpose()
}

/// The last frame this node appended, if any.
///
/// Cheap by design: the row is looked up by its `(node_id, seq)` primary key
/// instead of scanning and re-hashing the whole store.
pub fn last_own_frame(
    store: &Connection,
    db_id: &DbId,
    node_id: &NodeId,
) -> Result<Option<StoredFrame>> {
    let seq = own_max_seq(store, node_id)?;
    if seq == 0 {
        return Ok(None);
    }
    let row = store
        .query_row(
            "SELECT lamport, kind, version, prev_hash, payload, hash
             FROM frames WHERE node_id = ?1 AND seq = ?2",
            (node_id.as_slice(), seq as i64),
            |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)? as u32,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((lamport, kind, version, prev_hash, payload, hash)) = row else {
        return Ok(None);
    };
    let prev_hash = fixed32(&prev_hash, "prev_hash")?;
    let stored_hash = fixed32(&hash, "hash")?;
    let frame_kind = frame_kind_from_byte(kind, version)?;
    let recomputed = crate::frame::frame_hash(
        db_id,
        node_id,
        seq,
        lamport,
        frame_kind.kind_byte(),
        &prev_hash,
        &payload,
    );
    if recomputed != stored_hash {
        return Err(Error::HashMismatch);
    }
    Ok(Some(StoredFrame {
        node_id: *node_id,
        frame: Frame {
            seq,
            lamport,
            kind: frame_kind,
            prev_hash,
            payload,
            hash: stored_hash,
        },
    }))
}

fn frame_kind_from_byte(kind: i64, version: u32) -> Result<FrameKind> {
    match kind {
        1 => Ok(FrameKind::Changeset),
        2 => Ok(FrameKind::Migration(version)),
        other => Err(Error::Store(format!(
            "stored frame has unknown kind {other}"
        ))),
    }
}

/// The highest lamport timestamp across all stored frames.
pub fn max_lamport(store: &Connection, _db_id: &DbId) -> Result<u64> {
    let lamport: Option<i64> = store
        .query_row("SELECT max(lamport) FROM frames", [], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .optional()?
        .flatten();
    Ok(lamport.unwrap_or(0) as u64)
}

/// The SQL text of an already-stored migration with the given version, if any.
///
/// Two nodes can record *different* SQL for the same version concurrently; the
/// total order `(lamport, node_id, seq)` decides which one every node applies
/// (the first one replayed wins, the duplicate is skipped). The comparison a
/// re-running `migrate` makes must therefore be against that **winner** — the
/// frame that sorts first — and not against an arbitrary row, or a node whose
/// migration lost the race would be told its own (losing) SQL is the applied
/// one. Hence the explicit `ORDER BY`: without it SQLite may return any row.
pub fn migration_sql_for_version(store: &Connection, version: u32) -> Result<Option<String>> {
    let sql: Option<String> = store
        .query_row(
            "SELECT CAST(substr(payload, 5) AS TEXT) FROM frames
             WHERE kind = 2 AND version = ?1
             ORDER BY lamport ASC, node_id ASC, seq ASC
             LIMIT 1",
            [version as i64],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(sql)
}

/// The digest of the first `count` frames in total order.
pub fn digest_first(frames: &[StoredFrame], count: usize) -> [u8; 32] {
    let refs: Vec<FrameRef> = frames.iter().take(count).map(|f| f.as_ref()).collect();
    digest(&refs)
}

// ---------------------------------------------------------------------------
// Sync support: frame ranges, peer high-water marks, publish bookkeeping.
// ---------------------------------------------------------------------------

/// The frames `node_id` stored between `from` and `to` (inclusive), in `seq`
/// order, each with its hash re-verified.
///
/// Used by publish, which needs its own frames in exactly this order.
pub(crate) fn frames_range(
    store: &Connection,
    db_id: &DbId,
    node_id: &NodeId,
    from: u64,
    to: u64,
) -> Result<Vec<Frame>> {
    let mut stmt = store.prepare(
        "SELECT seq, lamport, kind, version, prev_hash, payload, hash
         FROM frames WHERE node_id = ?1 AND seq BETWEEN ?2 AND ?3
         ORDER BY seq ASC",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![node_id.as_slice(), from as i64, to as i64],
        |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)? as u32,
                row.get::<_, Vec<u8>>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Vec<u8>>(6)?,
            ))
        },
    )?;
    let mut out = Vec::new();
    for row in rows {
        let (seq, lamport, kind, version, prev_hash, payload, hash) = row?;
        let prev_hash = fixed32(&prev_hash, "prev_hash")?;
        let stored_hash = fixed32(&hash, "hash")?;
        let frame_kind = frame_kind_from_byte(kind, version)?;
        let recomputed = crate::frame::frame_hash(
            db_id,
            node_id,
            seq,
            lamport,
            frame_kind.kind_byte(),
            &prev_hash,
            &payload,
        );
        if recomputed != stored_hash {
            return Err(Error::HashMismatch);
        }
        out.push(Frame {
            seq,
            lamport,
            kind: frame_kind,
            prev_hash,
            payload,
            hash: stored_hash,
        });
    }
    Ok(out)
}

/// The highest `seq` stored for every node except `exclude`, in node order.
///
/// This is what `status` reports as the peer high-water marks: the highest seq
/// this node has *seen*, which is not necessarily what the folder holds.
pub(crate) fn peer_high_water(store: &Connection, exclude: &NodeId) -> Result<Vec<(NodeId, u64)>> {
    let mut stmt = store.prepare(
        "SELECT node_id, max(seq) FROM frames WHERE node_id <> ?1 GROUP BY node_id ORDER BY node_id",
    )?;
    let rows = stmt.query_map([exclude.as_slice()], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)? as u64))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (node, seq) = row?;
        let node_id = <[u8; 8]>::try_from(node)
            .map_err(|_| Error::Store("stored frame node_id is not 8 bytes".into()))?;
        out.push((node_id, seq));
    }
    Ok(out)
}

/// Reads a store `meta` value.
pub(crate) fn meta_get(store: &Connection, key: &str) -> Result<Option<String>> {
    let value: Option<String> = store
        .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
            row.get::<_, String>(0)
        })
        .optional()?;
    Ok(value)
}

/// Writes a store `meta` value.
pub(crate) fn meta_set(store: &Connection, key: &str, value: &str) -> Result<()> {
    store.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

/// The highest own `seq` already published to the synced folder.
///
/// Bookkeeping for [`crate::ShareDb::sync`]: everything above it still has to
/// become a segment. A missing row means nothing has been published yet. The
/// value is per node and the store holds exactly one node identity, so a single
/// key is enough; a state directory that lost its value simply republishes frames
/// that are already in the folder, which the folder tolerates (a new segment
/// covering the same range is a byte-identical duplicate).
pub(crate) fn last_published(store: &Connection) -> Result<u64> {
    let value = meta_get(store, "published_seq")?;
    Ok(value
        .as_deref()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0))
}

/// Records the highest own `seq` published to the synced folder.
pub(crate) fn set_last_published(store: &Connection, seq: u64) -> Result<()> {
    meta_set(store, "published_seq", &seq.to_string())
}

/// The own `seq` below which publish no longer republishes ranges.
///
/// Set when compaction prunes one of this node's own segments: the checkpoint
/// that made those frames deletable already carries their state, so putting the
/// segment back into the folder would undo the prune. A missing row means "no
/// floor": every own range is republished when it is missing.
pub(crate) fn published_floor(store: &Connection) -> Result<u64> {
    let value = meta_get(store, "published_floor")?;
    Ok(value
        .as_deref()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0))
}

/// Raises the publish floor to `floor` (never lowers it).
pub(crate) fn set_published_floor(store: &Connection, floor: u64) -> Result<()> {
    let current = published_floor(store)?;
    if floor > current {
        meta_set(store, "published_floor", &floor.to_string())?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Chain anchor and checkpoint base record.
// ---------------------------------------------------------------------------

/// The store `meta` key under which the checkpoint [`crate::checkpoint::Base`]
/// is kept after a bootstrap.
const BASE_KEY: &str = "checkpoint_base";

/// The chain anchor for `node_id`: the `(seq, hash)` that the next appended
/// frame must continue from.
///
/// Normally this is the node's own highest stored frame. When the node
/// bootstrapped from a checkpoint and does not hold its own frames below the
/// cut, the anchor comes from the checkpoint's version-vector entry instead.
///
/// The **higher** of the two wins (compared by `seq`). A node that bootstrapped
/// and then appended locally still anchors on its own last frame; a node that
/// bootstrapped *twice* — so its own stored frames are behind the newer
/// snapshot's version vector — anchors on the snapshot's entry, so it can keep
/// ingesting that peer instead of being stuck at the lower stored value.
pub(crate) fn anchor_of(store: &Connection, node_id: &NodeId) -> Result<(u64, [u8; 32])> {
    let own_seq = own_max_seq(store, node_id)?;
    let own = if own_seq > 0 {
        own_frame_hash(store, node_id, own_seq)?.map(|hash| (own_seq, hash))
    } else {
        None
    };
    let base_anchor = base(store)?.and_then(|b| b.vv.get(node_id).map(|v| (v.max_seq, v.hash)));
    match (own, base_anchor) {
        (Some(o), Some(b)) => Ok(if b.0 > o.0 { b } else { o }),
        (own, base) => Ok(own.or(base).unwrap_or((0, [0u8; 32]))),
    }
}

/// Reads the [`crate::checkpoint::Base`] record from the store's meta table.
pub fn base(store: &Connection) -> Result<Option<crate::checkpoint::Base>> {
    match meta_get(store, BASE_KEY)? {
        Some(hex) => Ok(crate::checkpoint::Base::decode(&hex)),
        None => Ok(None),
    }
}

/// Writes the [`crate::checkpoint::Base`] record to the store's meta table.
pub fn set_base(store: &Connection, b: &crate::checkpoint::Base) -> Result<()> {
    meta_set(store, BASE_KEY, &b.encode())
}
