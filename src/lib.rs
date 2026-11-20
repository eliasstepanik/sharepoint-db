//! `sharepoint-db`: a multi-writer SQL database that lives in a
//! OneDrive/SharePoint-synced folder.
//!
//! Writers keep their SQLite database in a local state directory outside the
//! sync root. Committed changes are captured as SQLite *session* changesets,
//! appended to a per-node hash-chained log, and published into the synced
//! folder as write-once segment files. Merging is the deterministic replay of
//! all known frames in one total order, `(lamport, node_id, seq)`.
//!
//! This crate contains the merge core — frame format and hashing ([`frame`]),
//! capture / apply / replay / dump ([`materialize`]), the local frame store
//! ([`store`]), the synced-folder access and segment codec ([`folder`]) — plus
//! the durable single-node database ([`db`]). Publishing and merging happen in
//! [`ShareDb::sync`], the only call that touches the synced folder.

pub mod checkpoint;
pub mod db;
pub mod error;
pub mod folder;
pub mod frame;
pub mod materialize;
pub mod store;
pub(crate) mod util;

pub use error::{Error, Result};
pub use frame::{
    frame_hash, migration_payload, sort_key, DbId, Frame, FrameKind, FrameRef, NodeId,
};
pub use materialize::{
    attach_session, capture, changeset, digest, dump, ensure_meta, new_id, register_functions,
    replay, validate_schema, ApplyReport, Dump,
};

#[doc(hidden)]
pub use crate::db::FaultPlan;
pub use crate::db::{
    MaterializeKind, MigrateOutcome, OpenOptions, ShareDb, Status, SyncReport, Transaction,
};
pub use crate::folder::{decode_segment, encode_segment, FolderFs, RealFs, Segment};

/// Re-exported so callers can build parameters and rows without depending on
/// `rusqlite` directly.
pub use rusqlite;
