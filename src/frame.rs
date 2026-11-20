//! Frame format and hashing.
//!
//! A frame is the unit of replication: one captured changeset or one schema
//! migration, bound to its position in a node's hash-chained log.
//!
//! Wire format (all integers little-endian):
//!
//! ```text
//! frame   := body_len u32 | seq u64 | lamport u64 | kind u8 | prev_hash[32] | payload | hash[32]
//! hash    := SHA-256(db_id | node_id | seq | lamport | kind | prev_hash | payload)
//! kind 1  := raw SQLite session changeset bytes
//! kind 2  := migration: version u32 | UTF-8 SQL
//! ```
//!
//! `body_len` is the byte length of everything after the `body_len` field
//! itself, i.e. `seq` through the trailing `hash` inclusive.

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// Stable identifier of a writer node (8 random bytes).
pub type NodeId = [u8; 8];

/// Stable identifier of a database (16 random bytes).
pub type DbId = [u8; 16];

/// Kind byte values.
const KIND_CHANGESET: u8 = 1;
const KIND_MIGRATION: u8 = 2;

/// Size of the fixed part of a frame body, excluding the payload and the hash:
/// `seq` + `lamport` + `kind` + `prev_hash`.
const BODY_PREFIX_LEN: usize = 8 + 8 + 1 + 32;

/// What a frame carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameKind {
    /// A raw SQLite session changeset.
    Changeset,
    /// A schema migration with the given schema version.
    Migration(u32),
}

impl FrameKind {
    /// The `kind` byte for the frame header.
    pub fn kind_byte(&self) -> u8 {
        match self {
            FrameKind::Changeset => KIND_CHANGESET,
            FrameKind::Migration(_) => KIND_MIGRATION,
        }
    }

    /// The migration version, if this is a migration.
    pub fn version(&self) -> Option<u32> {
        match self {
            FrameKind::Changeset => None,
            FrameKind::Migration(v) => Some(*v),
        }
    }
}

/// A single frame in a node's log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Position in the owning node's log (1-based, contiguous).
    pub seq: u64,
    /// Lamport timestamp of the commit that produced this frame.
    pub lamport: u64,
    /// What the frame carries.
    pub kind: FrameKind,
    /// Hash of the previous frame in this node's log (zeros for the first).
    pub prev_hash: [u8; 32],
    /// Encoded body: for a migration, `version u32 | SQL`; otherwise the raw changeset.
    pub payload: Vec<u8>,
    /// SHA-256 over the frame header and payload (see [`frame_hash`]).
    pub hash: [u8; 32],
}

/// SHA-256 over `db_id | node_id | seq | lamport | kind | prev_hash | payload`.
pub fn frame_hash(
    db_id: &DbId,
    node_id: &NodeId,
    seq: u64,
    lamport: u64,
    kind_byte: u8,
    prev_hash: &[u8; 32],
    payload: &[u8],
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(db_id);
    h.update(node_id);
    h.update(seq.to_le_bytes());
    h.update(lamport.to_le_bytes());
    h.update([kind_byte]);
    h.update(prev_hash);
    h.update(payload);
    h.finalize().into()
}

/// Builds the `payload` for a migration frame: `version u32 | UTF-8 SQL`.
pub fn migration_payload(version: u32, sql: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(4 + sql.len());
    p.extend_from_slice(&version.to_le_bytes());
    p.extend_from_slice(sql.as_bytes());
    p
}

impl Frame {
    /// Creates a frame and computes its hash.
    pub fn new(
        db_id: &DbId,
        node_id: &NodeId,
        seq: u64,
        lamport: u64,
        kind: FrameKind,
        prev_hash: [u8; 32],
        payload: Vec<u8>,
    ) -> Frame {
        let hash = frame_hash(
            db_id,
            node_id,
            seq,
            lamport,
            kind.kind_byte(),
            &prev_hash,
            &payload,
        );
        Frame {
            seq,
            lamport,
            kind,
            prev_hash,
            payload,
            hash,
        }
    }

    /// Convenience constructor for a changeset frame.
    pub fn changeset(
        db_id: &DbId,
        node_id: &NodeId,
        seq: u64,
        lamport: u64,
        prev_hash: [u8; 32],
        changeset: Vec<u8>,
    ) -> Frame {
        Frame::new(
            db_id,
            node_id,
            seq,
            lamport,
            FrameKind::Changeset,
            prev_hash,
            changeset,
        )
    }

    /// Convenience constructor for a migration frame.
    pub fn migration(
        db_id: &DbId,
        node_id: &NodeId,
        seq: u64,
        lamport: u64,
        prev_hash: [u8; 32],
        version: u32,
        sql: &str,
    ) -> Frame {
        Frame::new(
            db_id,
            node_id,
            seq,
            lamport,
            FrameKind::Migration(version),
            prev_hash,
            migration_payload(version, sql),
        )
    }

    /// The migration version for a migration frame.
    pub fn migration_version(&self) -> Option<u32> {
        self.kind.version()
    }

    /// The SQL of a migration frame.
    pub fn migration_sql(&self) -> Option<&str> {
        match self.kind {
            FrameKind::Changeset => None,
            FrameKind::Migration(_) => {
                std::str::from_utf8(self.payload.get(4..).unwrap_or(&[])).ok()
            }
        }
    }

    /// Encodes the frame, including its hash.
    pub fn encode(&self, db_id: &DbId, node_id: &NodeId) -> Vec<u8> {
        let body_len = BODY_PREFIX_LEN + self.payload.len() + 32;
        let mut out = Vec::with_capacity(4 + body_len);
        out.extend_from_slice(&(body_len as u32).to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.lamport.to_le_bytes());
        out.push(self.kind.kind_byte());
        out.extend_from_slice(&self.prev_hash);
        out.extend_from_slice(&self.payload);
        // Write the frame's own hash: recomputing it here would silently accept
        // a `db_id`/`node_id` pair the frame was not built with and break the
        // hash chain. A mismatch is a programming error, caught in debug builds.
        debug_assert_eq!(
            self.hash,
            frame_hash(
                db_id,
                node_id,
                self.seq,
                self.lamport,
                self.kind.kind_byte(),
                &self.prev_hash,
                &self.payload
            ),
            "Frame::encode called with a db_id/node_id that does not match the frame's hash"
        );
        out.extend_from_slice(&self.hash);
        out
    }

    /// Decodes a frame produced by [`Frame::encode`], verifying its hash.
    pub fn decode(bytes: &[u8], db_id: &DbId, node_id: &NodeId) -> Result<Frame> {
        let (frame, used) = Frame::decode_prefix(bytes, db_id, node_id)?;
        if used != bytes.len() {
            return Err(Error::unsupported(format!(
                "frame length mismatch: header says {used} bytes, got {}",
                bytes.len()
            )));
        }
        Ok(frame)
    }

    /// Decodes the frame at the start of `bytes`, returning it with the number
    /// of bytes it occupies.
    ///
    /// This is what a segment reader needs: frames follow each other with no
    /// separator, so the length has to come from the frame itself.
    pub fn decode_prefix(bytes: &[u8], db_id: &DbId, node_id: &NodeId) -> Result<(Frame, usize)> {
        let body_len = read_u32(bytes, 0)? as usize;
        let expected = BODY_PREFIX_LEN + 32;
        if body_len < expected {
            return Err(Error::unsupported("frame body too small"));
        }
        let total = 4usize
            .checked_add(body_len)
            .ok_or_else(|| Error::unsupported("frame body length overflow"))?;
        if bytes.len() < total {
            return Err(Error::unsupported(format!(
                "frame length mismatch: header says {total} bytes, got {}",
                bytes.len()
            )));
        }
        let seq = read_u64(bytes, 4)?;
        let lamport = read_u64(bytes, 12)?;
        let kind_byte = bytes[20];
        let mut prev_hash = [0u8; 32];
        prev_hash.copy_from_slice(&bytes[21..53]);
        let payload_end = total - 32;
        let payload = bytes[53..payload_end].to_vec();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[payload_end..total]);

        let kind = match kind_byte {
            KIND_CHANGESET => FrameKind::Changeset,
            KIND_MIGRATION => {
                if payload.len() < 4 {
                    return Err(Error::unsupported("migration frame without version"));
                }
                let version = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
                // Validate the SQL is UTF-8 so later steps never see broken bytes.
                std::str::from_utf8(&payload[4..])
                    .map_err(|_| Error::unsupported("migration SQL is not valid UTF-8"))?;
                FrameKind::Migration(version)
            }
            other => return Err(Error::unsupported(format!("unknown frame kind {other}"))),
        };

        let expected_hash = frame_hash(
            db_id, node_id, seq, lamport, kind_byte, &prev_hash, &payload,
        );
        if expected_hash != hash {
            return Err(Error::HashMismatch);
        }

        Ok((
            Frame {
                seq,
                lamport,
                kind,
                prev_hash,
                payload,
                hash,
            },
            total,
        ))
    }
}

fn read_u32(bytes: &[u8], at: usize) -> Result<u32> {
    let end = at + 4;
    if bytes.len() < end {
        return Err(Error::unsupported("truncated frame: need u32"));
    }
    Ok(u32::from_le_bytes([
        bytes[at],
        bytes[at + 1],
        bytes[at + 2],
        bytes[at + 3],
    ]))
}

fn read_u64(bytes: &[u8], at: usize) -> Result<u64> {
    let end = at + 8;
    if bytes.len() < end {
        return Err(Error::unsupported("truncated frame: need u64"));
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[at..end]);
    Ok(u64::from_le_bytes(b))
}

/// A frame together with the node that produced it.
#[derive(Debug, Clone, Copy)]
pub struct FrameRef<'a> {
    /// The node that published the frame.
    pub node_id: &'a NodeId,
    /// The frame itself.
    pub frame: &'a Frame,
}

/// The total order used everywhere: `(lamport, node_id, seq)`.
pub fn sort_key<'a>(f: &FrameRef<'a>) -> (u64, &'a [u8; 8], u64) {
    (f.frame.lamport, f.node_id, f.frame.seq)
}
