//! Errors for the `sharepoint-db` merge core.

use std::fmt;

/// Crate-wide error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// SQL executed inside a captured session changed the database schema.
    ///
    /// The merge core only replicates data changes; schema changes must travel
    /// as [`crate::frame::FrameKind::Migration`] frames instead.
    #[error("SQL executed inside a session changed the schema; expected a migration frame")]
    SchemaChangeInExecute,

    /// The schema of a database cannot be represented by the merge core.
    #[error("unsupported schema: {0}")]
    SchemaUnsupported(String),

    /// An I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// An SQLite error.
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),

    /// A frame's stored hash does not match the hash recomputed from its body.
    #[error("frame hash mismatch")]
    HashMismatch,

    /// A segment file is malformed: bad magic, a wrong `db_id`, a frame count
    /// that cannot fit the file, a broken hash or chain, or trailing bytes.
    ///
    /// This is always a hostile or corrupted input (the synced folder is
    /// untrusted), never a panic.
    #[error("corrupt segment: {0}")]
    CorruptSegment(String),

    /// A checkpoint file is malformed: bad magic, a wrong `db_id`, a truncated
    /// body, a length that disagrees with the file, or a body-hash mismatch.
    ///
    /// Like [`Error::CorruptSegment`], this is untrusted input and never panics.
    #[error("corrupt checkpoint: {0}")]
    CorruptCheckpoint(String),

    /// A node's frame log is not contiguous in `seq`.
    #[error("frame sequence gap: expected seq {expected}, found {found}")]
    FrameGap {
        /// The `seq` that was expected.
        expected: u64,
        /// The `seq` that was actually seen.
        found: u64,
    },

    /// Two frames share a `seq` but disagree on their hash.
    #[error("fork detected at seq {seq}: two different frames claim the same position")]
    Fork {
        /// The `seq` at which the fork was detected.
        seq: u64,
    },

    /// The database folder does not exist and `create` was not set.
    #[error("database not found: {0}")]
    NotFound(String),

    /// The local state directory is inside the synced folder.
    ///
    /// The state directory must never be synced: it holds this machine's node
    /// identity and the frame log, which are per-machine by design.
    #[error("state directory must not be inside the synced folder")]
    StateDirInsideSyncRoot,

    /// Another process holds the database's lock file.
    #[error("database is busy: {0}")]
    Busy(String),

    /// `migrate` asked for a version that is not the next one.
    #[error("migration gap: current schema version is {current}, requested {requested}")]
    MigrationGap {
        /// The schema version this node has applied.
        current: u32,
        /// The version that was requested.
        requested: u32,
    },

    /// A migration frame with the same version but different SQL already exists.
    #[error("migration {version} already exists with different SQL")]
    MigrationMismatch {
        /// The conflicting migration version.
        version: u32,
    },

    /// `migrate` was given a version that can never be applied: `0` (versions
    /// are 1-based), or a version at or below the current schema version for
    /// which no migration frame is recorded in the store.
    #[error("invalid migration version {version}: current schema version is {current} and no migration {version} is recorded")]
    InvalidVersion {
        /// The version that was requested.
        version: u32,
        /// The schema version this node has applied.
        current: u32,
    },

    /// A statement given to [`crate::ShareDb::query`] was not read-only.
    #[error("query only accepts read-only statements")]
    NotReadOnly,

    /// The `MANIFEST` file is missing a field or carries an unknown format.
    #[error("bad manifest: {0}")]
    Manifest(String),

    /// The local frame store (`store.db`) is inconsistent or unreadable.
    #[error("frame store error: {0}")]
    Store(String),

    /// A fault injected through `OpenOptions::fault` fired.
    #[error("injected fault: {0}")]
    Injected(String),

    /// The handle hit an unrecoverable protocol error (a commit that could not
    /// be recorded in the frame store). All later calls fail.
    #[error("this database handle failed and cannot be used any more: {0}")]
    Failed(String),
}

/// Convenience alias used throughout the public API.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Wraps an [`Error::SchemaUnsupported`] built from anything displayable.
    pub(crate) fn unsupported(msg: impl fmt::Display) -> Self {
        Error::SchemaUnsupported(msg.to_string())
    }
}
