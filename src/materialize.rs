//! Capture, apply policy, ordered replay, schema validation and logical dump.
//!
//! Everything here works against a plain (in-memory) [`rusqlite::Connection`].

use std::cell::Cell;

use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::functions::FunctionFlags;
use rusqlite::session::{ConflictAction, ConflictType, Session};
use rusqlite::{Connection, OptionalExtension, Row};

use crate::error::{Error, Result};
use crate::frame::{sort_key, DbId, FrameKind, FrameRef};

/// Prefix of the internal tables the merge core owns. They are never captured
/// and never dumped.
pub(crate) const INTERNAL_PREFIX: &str = "__sharedb_";

/// Attaches a session capturing exactly `user_tables` to `conn`.
///
/// Internal `__sharedb_*` tables are refused: they must never be replicated.
pub fn attach_session<'a>(conn: &'a Connection, user_tables: &[String]) -> Result<Session<'a>> {
    if user_tables.is_empty() {
        return Err(Error::unsupported(
            "attach_session needs an explicit table list (attaching nothing would capture all tables)",
        ));
    }
    let mut session = Session::new(conn)?;
    for table in user_tables {
        if table.starts_with(INTERNAL_PREFIX) {
            return Err(Error::unsupported(format!(
                "internal table {table} must not be captured"
            )));
        }
        session.attach(Some(table.as_str()))?;
    }
    Ok(session)
}

/// The changeset recorded so far; empty `Vec` when nothing was recorded.
pub fn changeset(session: &mut Session) -> Result<Vec<u8>> {
    if session.is_empty() {
        return Ok(Vec::new());
    }
    let mut out: Vec<u8> = Vec::new();
    session.changeset_strm(&mut out)?;
    Ok(out)
}

/// Restores `PRAGMA foreign_keys` to the value it had when the guard was
/// created. The pragma is a no-op inside a transaction: when the guard is
/// created there it changes nothing and restores nothing, and the caller is
/// responsible for having turned foreign keys off before opening the
/// transaction.
struct FkGuard<'a> {
    conn: &'a Connection,
    previous: Option<i64>,
}

impl<'a> FkGuard<'a> {
    /// Reads the current `foreign_keys` value and switches foreign keys off.
    ///
    /// Inside an open transaction this is a no-op (see the struct docs).
    fn new(conn: &'a Connection) -> Self {
        if !conn.is_autocommit() {
            return Self {
                conn,
                previous: None,
            };
        }
        let previous: i64 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap_or(1);
        let _ = conn.pragma_update(None, "foreign_keys", "OFF");
        Self {
            conn,
            previous: Some(previous),
        }
    }
}

impl Drop for FkGuard<'_> {
    fn drop(&mut self) {
        if let Some(previous) = self.previous {
            let _ = self.conn.pragma_update(None, "foreign_keys", previous);
        }
    }
}

/// Rolls the open transaction back when dropped, unless it was disarmed.
///
/// The guard never ends a transaction the user ended themselves: if the
/// connection is already back in autocommit mode, dropping does nothing.
pub(crate) struct RollbackOnDrop<'a> {
    conn: &'a Connection,
    armed: bool,
}

impl<'a> RollbackOnDrop<'a> {
    pub(crate) fn new(conn: &'a Connection) -> Self {
        Self { conn, armed: true }
    }

    /// Stops the guard from rolling back: the caller committed (or rolled back)
    /// the transaction itself.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RollbackOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed && !self.conn.is_autocommit() {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

/// Runs `f` inside a transaction, capturing the data changes it makes to
/// `user_tables`, and returns the resulting changeset.
///
/// If `f` changes the schema the transaction is rolled back and
/// [`Error::SchemaChangeInExecute`] is returned: schema changes must travel as
/// migration frames, never as changesets. The same happens if `f` ends the
/// transaction itself (e.g. by running `COMMIT`): the capture would be
/// unreliable, so the call fails.
pub fn capture<F>(conn: &Connection, user_tables: &[String], f: F) -> Result<Vec<u8>>
where
    F: FnOnce(&Connection) -> Result<()>,
{
    let schema_before: i64 = conn.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
    let mut session = attach_session(conn, user_tables)?;
    conn.execute_batch("BEGIN")?;
    let mut rollback = RollbackOnDrop::new(conn);
    f(conn)?;
    if !conn.is_autocommit() {
        // The closure did not end the transaction: it is still ours to roll back
        // on any error path below.
        let schema_after: i64 = conn.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        if schema_after != schema_before {
            return Err(Error::SchemaChangeInExecute);
        }
        let cs = changeset(&mut session)?;
        conn.execute_batch("COMMIT")?;
        rollback.disarm();
        Ok(cs)
    } else {
        // `f` committed (or rolled back) inside the closure: the schema guard
        // cannot undo a committed change, so refuse the capture outright.
        Err(Error::unsupported(
            "the closure ended the transaction; capture must own BEGIN/COMMIT",
        ))
    }
}

/// Counters collected while applying frames.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyReport {
    /// Frames whose changes were applied. A frame with no applicable change
    /// counts as `skipped` instead.
    pub applied: usize,
    /// Frames skipped entirely (duplicate migration, changeset whose tables are
    /// all missing here).
    pub skipped: usize,
    /// Individual changes dropped by the CONSTRAINT / NOTFOUND / FOREIGN_KEY
    /// path of the conflict handler.
    pub dropped: usize,
    /// Tables named by a changeset that do not exist in this database.
    pub tables_missing: usize,
}

impl std::ops::AddAssign for ApplyReport {
    fn add_assign(&mut self, rhs: ApplyReport) {
        self.applied += rhs.applied;
        self.skipped += rhs.skipped;
        self.dropped += rhs.dropped;
        self.tables_missing += rhs.tables_missing;
    }
}

thread_local! {
    static DROPPED: Cell<usize> = const { Cell::new(0) };
}

/// Creates the internal bookkeeping tables. Idempotent.
///
/// Must be called before any migration frame is applied.
///
/// Two tables:
///
/// * `__sharedb_meta` — the `(applied_count, applied_digest)` pair and the
///   schema version, i.e. the bookkeeping that describes *this* data file;
/// * `__sharedb_migrations` — `version → SQL` for every migration this data
///   file has applied. It lives in `data.db` (not `store.db`) on purpose: a
///   checkpoint snapshot is a copy of `data.db`, so the migration history
///   travels with it and a node that bootstrapped from a checkpoint — whose
///   store is missing the frames below the cut — can still answer "was
///   migration N applied here, and with which SQL?".
pub fn ensure_meta(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS __sharedb_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS __sharedb_migrations (
             version INTEGER PRIMARY KEY,
             sql     TEXT NOT NULL
         );",
    )?;
    Ok(())
}

fn meta_version(conn: &Connection) -> Result<u32> {
    // A missing `__sharedb_meta` means "no migration applied yet"; any other
    // error (SQLITE_BUSY, ...) must surface instead of masquerading as 0.
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM __sharedb_meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(value
        .as_deref()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0))
}

fn set_meta_version(conn: &Connection, version: u32) -> Result<()> {
    conn.execute(
        "INSERT INTO __sharedb_meta (key, value) VALUES ('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [version.to_string()],
    )?;
    Ok(())
}

/// Records `version -> sql` in `__sharedb_migrations`.
///
/// Idempotent: re-recording the same version keeps the first SQL, which is the
/// one that was actually applied (a duplicate that lost the total-order race is
/// skipped before it reaches here, so what lands is always the winner).
pub(crate) fn record_migration(conn: &Connection, version: u32, sql: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO __sharedb_migrations (version, sql) VALUES (?1, ?2)
         ON CONFLICT(version) DO NOTHING",
        rusqlite::params![version as i64, sql],
    )?;
    Ok(())
}

/// The SQL recorded for `version` in this data file, if it applied that migration.
///
/// This is the fallback [`crate::ShareDb::migrate`] uses when the frame store no
/// longer holds the migration frame — which is exactly the case for a node that
/// bootstrapped from a checkpoint: the table travelled inside the snapshot, so
/// the node still knows what it applied and with which SQL.
pub(crate) fn recorded_migration_sql(conn: &Connection, version: u32) -> Result<Option<String>> {
    let sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM __sharedb_migrations WHERE version = ?1",
            [version as i64],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(sql)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let found: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    Ok(found.is_some())
}

/// Applies one frame to `conn` according to the merge policy.
///
/// Foreign keys are switched off for the duration of the apply: a merged frame
/// may legitimately leave a temporary FK violation behind, and cascades travel
/// in the captured changeset instead of being re-triggered. The value the
/// connection had before the call is restored on success *and* on error.
pub fn apply_frame(conn: &Connection, frame: &FrameRef, report: &mut ApplyReport) -> Result<()> {
    let _fk = FkGuard::new(conn);
    match frame.frame.kind {
        FrameKind::Changeset => apply_changeset(conn, frame, report),
        FrameKind::Migration(_) => apply_migration(conn, frame, report),
    }
}

fn apply_changeset(conn: &Connection, frame: &FrameRef, report: &mut ApplyReport) -> Result<()> {
    let payload = &frame.frame.payload;

    // Inspect the changeset: which tables does it touch, and are they present?
    let mut touched: Vec<String> = Vec::new();
    let mut changes: usize = 0;
    {
        let input: &mut dyn std::io::Read = &mut payload.as_slice();
        let mut iter = rusqlite::session::ChangesetIter::start_strm(&input)?;
        while let Some(item) = iter.by_ref().next()? {
            changes += 1;
            let table = item.op()?.table_name().to_string();
            if !touched.contains(&table) {
                touched.push(table);
            }
        }
    }

    let mut missing: usize = 0;
    for table in &touched {
        if !table_exists(conn, table)? {
            missing += 1;
        }
    }
    report.tables_missing += missing;

    if changes == 0 || missing == touched.len() {
        // Nothing to do, or nothing we can do: none of the tables exist here yet.
        report.skipped += 1;
        return Ok(());
    }

    DROPPED.with(|d| d.set(0));
    let apply_result = {
        let mut input: &mut dyn std::io::Read = &mut payload.as_slice();
        conn.apply_strm(
            &mut input,
            None::<fn(&str) -> bool>,
            |conflict_type: ConflictType, _item| match conflict_type {
                ConflictType::SQLITE_CHANGESET_DATA | ConflictType::SQLITE_CHANGESET_CONFLICT => {
                    // Last writer in total order wins: overwrite or delete the
                    // conflicting row, then retry the change.
                    ConflictAction::SQLITE_CHANGESET_REPLACE
                }
                ConflictType::SQLITE_CHANGESET_NOTFOUND
                | ConflictType::SQLITE_CHANGESET_CONSTRAINT
                | ConflictType::SQLITE_CHANGESET_FOREIGN_KEY => {
                    DROPPED.with(|d| d.set(d.get() + 1));
                    ConflictAction::SQLITE_CHANGESET_OMIT
                }
                _ => ConflictAction::SQLITE_CHANGESET_OMIT,
            },
        )
    };
    let dropped = DROPPED.with(|d| d.get());
    apply_result?;

    report.dropped += dropped;
    report.applied += 1;
    Ok(())
}

fn apply_migration(conn: &Connection, frame: &FrameRef, report: &mut ApplyReport) -> Result<()> {
    let version = frame
        .frame
        .migration_version()
        .ok_or_else(|| Error::unsupported("migration frame without version"))?;
    let sql = frame
        .frame
        .migration_sql()
        .ok_or_else(|| Error::unsupported("migration frame without SQL"))?;

    if version <= meta_version(conn)? {
        // Already applied — including a duplicate version that lost the
        // `(lamport, node_id, seq)` race.
        report.skipped += 1;
        return Ok(());
    }

    // The DDL and the version bump commit or roll back together, so a failing
    // statement can never leave half a migration applied.
    conn.execute_batch("SAVEPOINT sharedb_migration")?;
    let result = (|| -> Result<()> {
        conn.execute_batch(sql)?;
        set_meta_version(conn, version)?;
        // Record `version -> sql` alongside the version bump, so the migration
        // history is part of the data file (and of every checkpoint snapshot
        // taken from it). See `ensure_meta`.
        record_migration(conn, version, sql)?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            conn.execute_batch("RELEASE sharedb_migration")?;
            report.applied += 1;
            Ok(())
        }
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK TO sharedb_migration");
            let _ = conn.execute_batch("RELEASE sharedb_migration");
            Err(err)
        }
    }
}

/// Sorts `frames` by [`sort_key`] and applies them in that order.
///
/// The internal bookkeeping tables are created first, so replay works on a
/// fresh database. Foreign keys are off for the whole replay and the previous
/// value is restored afterwards.
pub fn replay(db_id: &DbId, frames: &[FrameRef], conn: &Connection) -> Result<ApplyReport> {
    let _ = db_id;
    ensure_meta(conn)?;
    let _fk = FkGuard::new(conn);
    let mut ordered: Vec<FrameRef> = frames.to_vec();
    ordered.sort_by_key(sort_key);
    let mut report = ApplyReport::default();
    for frame in ordered {
        apply_frame(conn, &frame, &mut report)?;
    }
    Ok(report)
}

/// SHA-256 over the frame hashes, in the given order.
pub fn digest(frames_in_order: &[FrameRef]) -> [u8; 32] {
    use sha2::Digest as _;
    let mut h = sha2::Sha256::new();
    for f in frames_in_order {
        h.update(f.frame.hash);
    }
    h.finalize().into()
}

/// A logical dump of a database: every user table with every row rendered as
/// strings. Two databases with equal dumps are logically equal.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Dump {
    /// `(table name, rows)` pairs: tables in name order, rows in PRIMARY KEY
    /// order. `NULL` is rendered as the literal `NULL`.
    pub tables: Vec<(String, Vec<Vec<String>>)>,
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn render_value(row: &Row, index: usize) -> rusqlite::Result<String> {
    use rusqlite::types::ValueRef;
    Ok(match row.get_ref(index)? {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Integer(i) => i.to_string(),
        ValueRef::Real(f) => f.to_string(),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
        ValueRef::Blob(b) => format!("x'{}'", hex(b)),
    })
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Dumps every user table (internal `__sharedb_*` and SQLite-internal
/// `sqlite_*` tables excluded).
pub fn dump(conn: &Connection) -> Result<Dump> {
    let mut names: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master
             WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
             ORDER BY name",
        )?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    names.retain(|name| !name.starts_with(INTERNAL_PREFIX));

    let mut tables = Vec::with_capacity(names.len());
    for name in names {
        let pk: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info(?1) WHERE pk > 0 ORDER BY pk")?
            .query_map([&name], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let quoted = quote_ident(&name);
        let sql = if pk.is_empty() {
            format!("SELECT * FROM {quoted}")
        } else {
            let order: Vec<String> = pk.iter().map(|c| quote_ident(c)).collect();
            format!("SELECT * FROM {quoted} ORDER BY {}", order.join(", "))
        };

        let mut rows = Vec::new();
        {
            let mut stmt = conn.prepare(&sql)?;
            let columns = stmt.column_count();
            let mapped = stmt.query_map([], |row| {
                let mut out = Vec::with_capacity(columns);
                for i in 0..columns {
                    out.push(render_value(row, i)?);
                }
                Ok(out)
            })?;
            for row in mapped {
                rows.push(row?);
            }
        }
        tables.push((name, rows));
    }
    Ok(Dump { tables })
}

/// 16 random hex characters.
///
/// Randomness comes from SQLite itself (`sqlite3_randomness`), so no extra
/// dependency is needed.
pub fn new_id() -> String {
    let mut bytes = [0u8; 8];
    // SAFETY: writing exactly 8 bytes into an 8-byte buffer.
    unsafe { rusqlite::ffi::sqlite3_randomness(8, bytes.as_mut_ptr().cast()) };
    hex(&bytes)
}

/// Registers the `sharedb_*` SQL functions on `conn`.
pub fn register_functions(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "sharedb_new_id",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY,
        move |_| Ok(new_id()),
    )?;
    Ok(())
}

/// Checks that `conn`'s schema can be replicated by the merge core.
///
/// Rejected: user tables without a declared PRIMARY KEY, `WITHOUT ROWID`
/// tables, and user triggers. Internal `__sharedb_*` tables and SQLite's own
/// `sqlite_*` tables (e.g. `sqlite_sequence`) are ignored.
pub fn validate_schema(conn: &Connection) -> Result<()> {
    let objects: Vec<(String, String, String)> = conn
        .prepare(
            "SELECT type, name, COALESCE(sql, '') FROM sqlite_master
             WHERE type IN ('table', 'trigger')
               AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
             ORDER BY name",
        )?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    for (kind, name, _sql) in objects {
        if name.starts_with(INTERNAL_PREFIX) {
            continue;
        }
        if kind == "trigger" {
            return Err(Error::unsupported(format!(
                "trigger {name} is not supported"
            )));
        }
        // Ask SQLite's own metadata instead of substring-matching the DDL text:
        // a string literal in a DEFAULT clause may contain "PRIMARY KEY" or
        // "WITHOUT ROWID" without meaning either.
        let pk_columns: i64 = conn.query_row(
            "SELECT count(*) FROM pragma_table_info(?1) WHERE pk > 0",
            [&name],
            |row| row.get(0),
        )?;
        if pk_columns == 0 {
            return Err(Error::unsupported(format!(
                "table {name} has no PRIMARY KEY"
            )));
        }
        let without_rowid: bool = conn.query_row(
            "SELECT wr FROM pragma_table_list WHERE name = ?1",
            [&name],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if without_rowid {
            return Err(Error::unsupported(format!("table {name} is WITHOUT ROWID")));
        }
    }
    Ok(())
}
