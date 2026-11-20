//! Durable single-node tests: the commit protocol, crash recovery and the
//! user-facing API surface of [`ShareDb`].
//!
//! Every test drives the real public API through `ShareDb::open` and the
//! commit path — no internal helpers. All databases live in `tempfile`
//! directories, outside any synced folder.

use std::fs;
use std::path::{Path, PathBuf};

use sharepoint_db::{Error, FaultPlan, MigrateOutcome, OpenOptions, ShareDb};

/// A synced folder and a separate local state directory, both under `root`.
struct Fixture {
    folder: PathBuf,
    state_dir: PathBuf,
}

impl Fixture {
    fn new(root: &Path, name: &str) -> Fixture {
        Fixture {
            folder: root.join(format!("{name}.sharedb")),
            state_dir: root.join(format!("{name}.state")),
        }
    }

    fn opts(&self) -> OpenOptions {
        OpenOptions {
            create: true,
            state_dir: Some(self.state_dir.clone()),
            fault: None,
            ..Default::default()
        }
    }

    fn open(&self) -> ShareDb {
        ShareDb::open(&self.folder, self.opts()).expect("open")
    }

    fn reopen(&self) -> ShareDb {
        ShareDb::open(
            &self.folder,
            OpenOptions {
                create: false,
                state_dir: Some(self.state_dir.clone()),
                fault: None,
                ..Default::default()
            },
        )
        .expect("reopen")
    }

    fn data_db(&self) -> PathBuf {
        self.state_dir.join("data.db")
    }
}

const MIGRATE_T: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);";

fn rows(db: &ShareDb, sql: &str) -> Vec<String> {
    db.query(sql, [], |r| Ok(r.get::<_, String>(0)?)).unwrap()
}

/// Reads a single integer column (e.g. `count(*)`).
fn count(db: &ShareDb, sql: &str) -> i64 {
    db.query(sql, [], |r| Ok(r.get::<_, i64>(0)?))
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

/// 1. A committed change survives closing and reopening the database.
#[test]
fn commit_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    assert_eq!(db.migrate(1, MIGRATE_T).unwrap(), MigrateOutcome::Applied);
    db.execute("INSERT INTO t (id, v) VALUES (1, 'hello')", [])
        .unwrap();
    assert_eq!(db.schema_version(), 1);
    drop(db);

    let db = fx.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec!["hello".to_string()]
    );
    assert_eq!(db.schema_version(), 1);
}

/// 2. Overwriting `data.db` with an older copy: the newer change is recovered
///    from the store frames on reopen.
#[test]
fn stale_data_db_is_recovered_from_frames() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'first')", [])
        .unwrap();
    drop(db);

    // Snapshot a data.db that reflects only the first insert.
    let backup = tmp.path().join("data.backup");
    fs::copy(fx.data_db(), &backup).unwrap();

    let mut db = fx.reopen();
    db.execute("INSERT INTO t (id, v) VALUES (2, 'second')", [])
        .unwrap();
    drop(db);

    // Roll data.db back to the older snapshot; the second frame is still in the
    // store, so reopening must re-apply it.
    fs::copy(&backup, fx.data_db()).unwrap();
    let db = fx.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM t ORDER BY id"),
        vec!["first".to_string(), "second".to_string()]
    );
}

/// 3. A `data.db` copied from a *different* database is replaced by a rebuild
///    from this database's own frames.
#[test]
fn foreign_data_db_triggers_rebuild() {
    let tmp = tempfile::tempdir().unwrap();
    let main = Fixture::new(tmp.path(), "main");
    let mut db = main.open();
    db.migrate(
        1,
        "CREATE TABLE main_t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
    )
    .unwrap();
    db.execute("INSERT INTO main_t (id, v) VALUES (1, 'mine')", [])
        .unwrap();
    drop(db);

    // A completely separate database with its own identity and frames.
    let other = Fixture::new(tmp.path(), "other");
    let mut db = other.open();
    db.migrate(
        1,
        "CREATE TABLE foreign_t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
    )
    .unwrap();
    db.execute("INSERT INTO foreign_t (id, v) VALUES (9, 'theirs')", [])
        .unwrap();
    drop(db);

    // Drop the foreign data.db into the main state directory.
    fs::copy(other.data_db(), main.data_db()).unwrap();

    let db = main.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM main_t WHERE id = 1"),
        vec!["mine".to_string()]
    );
    // The foreign table must be gone: data.db was rebuilt from main's frames.
    assert!(db
        .query(
            "SELECT v FROM foreign_t",
            [],
            |r| Ok(r.get::<_, String>(0)?)
        )
        .is_err());
}

/// 4. A deleted `data.db` is rebuilt from the store frames.
#[test]
fn deleted_data_db_is_rebuilt() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'kept')", [])
        .unwrap();
    drop(db);

    fs::remove_file(fx.data_db()).unwrap();
    let db = fx.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec!["kept".to_string()]
    );
}

/// 5. Random bytes in `data.db` are detected (SQLite rejects the file when we
///    first touch it) and the database is rebuilt from the store frames.
#[test]
fn garbage_data_db_is_rebuilt() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'kept')", [])
        .unwrap();
    drop(db);

    // Overwrite data.db with garbage that is not a SQLite database file.
    fs::write(
        fx.data_db(),
        b"this is definitely not a sqlite database file",
    )
    .unwrap();

    let db = fx.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec!["kept".to_string()]
    );
}

/// 6. A crash between the `data.db` commit and the frame insert leaves the
///    change unacknowledged; after reopen it is gone. Driven through the real
///    `execute` call site via the fault plan.
#[test]
fn crash_between_data_commit_and_frame_insert_rolls_back() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'committed')", [])
        .unwrap();

    // Arm the fault: the next execute commits data.db then fails before the
    // frame reaches the store.
    db.set_fault(Some(FaultPlan::AfterDataCommitBeforeFrameInsert));
    let err = db
        .execute("INSERT INTO t (id, v) VALUES (2, 'phantom')", [])
        .unwrap_err();
    assert!(matches!(err, Error::Injected(_)));
    drop(db);

    let db = fx.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM t ORDER BY id"),
        vec!["committed".to_string()]
    );
}

/// 7. A schema change attempted through `execute` is rejected.
#[test]
fn schema_change_in_execute_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    let err = db
        .execute("CREATE TABLE bad (id INTEGER PRIMARY KEY)", [])
        .unwrap_err();
    assert!(matches!(err, Error::SchemaChangeInExecute));
}

/// 8. A write statement given to `query` is rejected.
#[test]
fn write_in_query_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    let err = db
        .query("INSERT INTO t (id, v) VALUES (1, 'x')", [], |r| {
            Ok(r.get::<_, i64>(0)?)
        })
        .unwrap_err();
    assert!(matches!(err, Error::NotReadOnly));
}

/// 9. A second open of the same state directory fails with `Busy`.
#[test]
fn second_open_of_state_dir_fails_busy() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let _db = fx.open();
    let err = ShareDb::open(&fx.folder, fx.opts()).unwrap_err();
    assert!(matches!(err, Error::Busy(_)));
}

/// 10. A state directory inside the synced folder is rejected.
#[test]
fn state_dir_inside_synced_folder_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let inside = fx.folder.join("state");
    let err = ShareDb::open(
        &fx.folder,
        OpenOptions {
            create: true,
            state_dir: Some(inside),
            fault: None,
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::StateDirInsideSyncRoot));
}

/// 11. Opening a database whose parent directories do not exist creates them.
#[test]
fn open_creates_missing_parent_dirs() {
    let tmp = tempfile::tempdir().unwrap();
    let deep = tmp.path().join("a").join("b").join("c").join("db.sharedb");
    let state = tmp.path().join("deepstate");
    let mut db = ShareDb::open(
        &deep,
        OpenOptions {
            create: true,
            state_dir: Some(state),
            fault: None,
            ..Default::default()
        },
    )
    .expect("open deep");
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'deep')", [])
        .unwrap();
    assert!(deep.join("MANIFEST").exists());
    assert!(deep.join("segments").is_dir());
}

/// 12. A transaction that changes nothing writes no frame.
#[test]
fn empty_transaction_writes_no_frame() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    let seq_before = db.status().unwrap().own_seq;

    db.transaction(|txn| {
        // Read only: no captured change.
        let _ = txn.query("SELECT count(*) FROM t", [], |r| Ok(r.get::<_, i64>(0)?))?;
        Ok(())
    })
    .unwrap();

    assert_eq!(db.status().unwrap().own_seq, seq_before);
}

// ---------------------------------------------------------------------------
// Review round: failed store append, transaction ownership, migrate, locking.
// ---------------------------------------------------------------------------

/// 13. A failed store append does not leave `data.db` ahead of the log: the
///     handle repairs `data.db` from the store immediately, so a later commit on
///     the *same* handle cannot make the diverged file pass the digest check.
///     The lost row stays lost after a reopen, and the row committed afterwards
///     survives.
#[test]
fn failed_store_append_is_repaired_and_next_commit_is_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'kept')", [])
        .unwrap();

    // Fault: data.db commits, the frame never reaches the store.
    db.set_fault(Some(FaultPlan::AfterDataCommitBeforeFrameInsert));
    let err = db
        .execute("INSERT INTO t (id, v) VALUES (2, 'phantom')", [])
        .unwrap_err();
    assert!(matches!(err, Error::Injected(_)));

    // The same handle is still usable and its data.db is back in line with the
    // store: the phantom row is already gone, not merely hidden on reopen.
    db.set_fault(None);
    assert_eq!(rows(&db, "SELECT v FROM t ORDER BY id"), vec!["kept"]);
    db.execute("INSERT INTO t (id, v) VALUES (3, 'after')", [])
        .unwrap();
    drop(db);

    let db = fx.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM t ORDER BY id"),
        vec!["kept".to_string(), "after".to_string()]
    );
}

/// 14. The same repair for a migration whose frame could not be appended.
#[test]
fn failed_store_append_after_migration_is_repaired() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();

    db.set_fault(Some(FaultPlan::AfterDataCommitBeforeFrameInsert));
    let err = db
        .migrate(
            2,
            "CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
        )
        .unwrap_err();
    assert!(matches!(err, Error::Injected(_)));
    db.set_fault(None);

    // data.db was rebuilt from the store: the unrecorded table is gone and the
    // schema version went back to 1.
    assert_eq!(db.schema_version(), 1);
    assert!(db
        .query("SELECT count(*) FROM u", [], |r| Ok(r.get::<_, i64>(0)?))
        .is_err());

    // The migration can be retried successfully.
    assert_eq!(
        db.migrate(
            2,
            "CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT NOT NULL);"
        )
        .unwrap(),
        MigrateOutcome::Applied
    );
    drop(db);

    let db = fx.reopen();
    assert_eq!(db.schema_version(), 2);
    assert_eq!(count(&db, "SELECT count(*) FROM u"), 0);
}

/// 15. `COMMIT` run by the user closure is denied by the authorizer, so the
///     commit protocol keeps ownership of the transaction and nothing is lost.
#[test]
fn commit_inside_transaction_closure_is_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'kept')", [])
        .unwrap();

    let err = db
        .transaction(|txn| {
            txn.execute("INSERT INTO t (id, v) VALUES (2, 'sneaky')", [])?;
            txn.execute("COMMIT", [])?;
            Ok(())
        })
        .unwrap_err();
    // Denied by SQLite's authorizer ("not authorized"), not silently accepted.
    assert!(
        err.to_string().to_lowercase().contains("authoriz"),
        "expected an authorization error, got {err}"
    );

    // The handle is still usable and the sneaky row never landed.
    db.execute("INSERT INTO t (id, v) VALUES (3, 'after')", [])
        .unwrap();
    drop(db);

    let db = fx.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM t ORDER BY id"),
        vec!["kept".to_string(), "after".to_string()]
    );
}

/// 16. `BEGIN`, `SAVEPOINT` and `RELEASE` are denied inside a closure too, and
///     `query` rejects every transaction-control statement explicitly (SQLite
///     reports them read-only).
#[test]
fn transaction_control_is_denied_in_closures_and_query() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();

    for stmt in ["BEGIN", "SAVEPOINT sp", "RELEASE sp", "ROLLBACK"] {
        let err = db
            .transaction(|txn| {
                txn.execute("INSERT INTO t (id, v) VALUES (1, 'x')", [])?;
                txn.execute(stmt, [])?;
                Ok(())
            })
            .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("authoriz"),
            "{stmt} should be denied, got {err}"
        );
    }

    // The failed closures left nothing behind.
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 0);

    // `query` rejects transaction control even though SQLite calls it read-only.
    for stmt in [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT sp",
        "RELEASE sp",
        "begin; -- lowercase",
    ] {
        let err = db.query(stmt, [], |r| Ok(r.get::<_, i64>(0)?)).unwrap_err();
        assert!(matches!(err, Error::NotReadOnly), "{stmt} must be rejected");
    }
}

/// 17. `migrate` with a version that is already applied and identical SQL
///     reports `AlreadyApplied` instead of a gap error.
#[test]
fn migrate_already_applied_is_recognised() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    assert_eq!(db.migrate(1, MIGRATE_T).unwrap(), MigrateOutcome::Applied);
    assert_eq!(
        db.migrate(1, MIGRATE_T).unwrap(),
        MigrateOutcome::AlreadyApplied
    );
    // Reopening keeps the same answer.
    drop(db);
    let mut db = fx.reopen();
    assert_eq!(db.schema_version(), 1);
    assert_eq!(
        db.migrate(1, MIGRATE_T).unwrap(),
        MigrateOutcome::AlreadyApplied
    );
}

/// 18. `migrate` with a version that is already applied but different SQL is a
///     `MigrationMismatch`.
#[test]
fn migrate_with_different_sql_is_a_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    let err = db
        .migrate(1, "CREATE TABLE t (id INTEGER PRIMARY KEY);")
        .unwrap_err();
    assert!(matches!(err, Error::MigrationMismatch { version: 1 }));
    // Same after a reopen (the comparison uses the stored frame, not memory).
    drop(db);
    let mut db = fx.reopen();
    let err = db
        .migrate(1, "CREATE TABLE t (id INTEGER PRIMARY KEY);")
        .unwrap_err();
    assert!(matches!(err, Error::MigrationMismatch { version: 1 }));
}

/// 19. Skipping a version is a `MigrationGap`.
#[test]
fn migrate_skipping_a_version_is_a_gap() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    let err = db
        .migrate(
            3,
            "CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        Error::MigrationGap {
            current: 1,
            requested: 3
        }
    ));
    // Version 2 still applies normally.
    assert_eq!(
        db.migrate(
            2,
            "CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT NOT NULL);"
        )
        .unwrap(),
        MigrateOutcome::Applied
    );
}

/// 20. A migration whose result the merge core cannot represent (no PRIMARY
///     KEY) rolls back completely: nothing is committed, nothing is journaled,
///     and the schema version is unchanged.
#[test]
fn migration_with_no_pk_table_rolls_back_and_is_not_journaled() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    let seq_before = db.status().unwrap().own_seq;

    let err = db
        .migrate(2, "CREATE TABLE nopk (a INTEGER, b TEXT);")
        .unwrap_err();
    assert!(matches!(err, Error::SchemaUnsupported(_)), "got {err}");

    // Rolled back: the table does not exist and the version did not move.
    assert_eq!(db.schema_version(), 1);
    assert!(db
        .query("SELECT count(*) FROM nopk", [], |r| Ok(r.get::<_, i64>(0)?))
        .is_err());
    assert_eq!(db.status().unwrap().own_seq, seq_before);

    // And it was never journaled: a reopen sees the same clean state.
    drop(db);
    let db = fx.reopen();
    assert_eq!(db.schema_version(), 1);
    assert!(db
        .query("SELECT count(*) FROM nopk", [], |r| Ok(r.get::<_, i64>(0)?))
        .is_err());
}

/// 21. A `LOCK` file left behind by a crashed owner never blocks a later open:
///     the OS lock is released with the owning process, so the pid inside is
///     only diagnostic.
#[test]
fn stale_lock_file_does_not_block_open() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'kept')", [])
        .unwrap();
    drop(db);

    // Simulate a crash: a LOCK file whose pid belongs to no living process.
    fs::create_dir_all(&fx.state_dir).unwrap();
    fs::write(fx.state_dir.join("LOCK"), b"999999\n").unwrap();

    let db = fx.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec!["kept".to_string()]
    );
}

/// 22. The lock is released when the handle is dropped, so a reopen after a
///     clean close succeeds (and a held lock is refused). The `LOCK` file
///     itself is left in place: deleting it would reopen the classic
///     flock+unlink race between two openers.
#[test]
fn lock_is_released_on_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    {
        let _db = fx.open();
        assert!(fx.state_dir.join("LOCK").exists());
    }
    // Dropped: the OS lock is gone, the file stays, and a fresh open works.
    assert!(fx.state_dir.join("LOCK").exists());
    let _db = fx.reopen();
}

/// 23. The default state directory is machine-local, never inside the synced
///     folder.
#[cfg(unix)]
#[test]
fn default_state_dir_is_outside_the_sync_folder() {
    let tmp = tempfile::tempdir().unwrap();
    // Redirect the machine-local root into the temp dir for the duration.
    std::env::set_var("XDG_DATA_HOME", tmp.path().join("localdata"));
    let folder = tmp.path().join("db.sharedb");
    let db = ShareDb::open(
        &folder,
        OpenOptions {
            create: true,
            state_dir: None,
            fault: None,
            ..Default::default()
        },
    )
    .expect("open with default state dir");
    let state = db.state_dir().to_path_buf();
    assert!(
        !state.starts_with(&folder),
        "{state:?} is inside the sync root"
    );
    assert!(state.starts_with(tmp.path().join("localdata").canonicalize().unwrap()));
    assert!(state.join("data.db").exists());
    assert!(!folder.join("data.db").exists());
    assert!(!folder.join("store.db").exists());
    assert!(!folder.join("LOCK").exists());
}

/// 24. A state directory that only *looks* outside the sync root (through `..`)
///     is still rejected: the guard canonicalises the nearest existing ancestor.
#[test]
fn state_dir_escaping_through_dotdot_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    fs::create_dir_all(&fx.folder).unwrap();
    let sneaky = fx.folder.join("sub").join("..").join("outside");
    fs::create_dir_all(fx.folder.join("sub")).unwrap();
    let err = ShareDb::open(
        &fx.folder,
        OpenOptions {
            create: true,
            state_dir: Some(sneaky),
            fault: None,
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::StateDirInsideSyncRoot));
}

/// 25. A `data.db` whose bookkeeping row is corrupt is rebuilt instead of
///     failing the open (the fast path never fails open).
#[test]
fn corrupt_bookkeeping_triggers_rebuild() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'kept')", [])
        .unwrap();
    drop(db);

    // Corrupt `applied_count` so the fast path cannot trust the file.
    {
        let conn = rusqlite::Connection::open(fx.data_db()).unwrap();
        conn.execute(
            "UPDATE __sharedb_meta SET value = 'not-a-number' WHERE key = 'applied_count'",
            [],
        )
        .unwrap();
    }

    let db = fx.reopen();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec!["kept".to_string()]
    );
}

// ---------------------------------------------------------------------------
// Review round 2: migration transaction ownership, failed-handle gating.
// ---------------------------------------------------------------------------

/// 26. A migration whose SQL ends the transaction is refused: the authorizer
///     denies the `COMMIT`, so the DDL is rolled back instead of being committed
///     outside the frame log. The table is absent on the same handle *and* after
///     a reopen, so the digest fast path can never bless the divergence.
#[test]
fn migrate_with_commit_in_sql_is_refused_and_leaves_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    let seq_before = db.status().unwrap().own_seq;

    let err = db
        .migrate(2, "CREATE TABLE u (id INTEGER PRIMARY KEY); COMMIT;")
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("authoriz")
            || matches!(err, Error::SchemaUnsupported(_)),
        "expected the migration to be refused, got {err}"
    );

    // Same handle: the table does not exist and the version did not move.
    assert_eq!(db.schema_version(), 1);
    assert!(db
        .query("SELECT count(*) FROM u", [], |r| Ok(r.get::<_, i64>(0)?))
        .is_err());
    assert_eq!(db.status().unwrap().own_seq, seq_before);

    // And nothing reached the store: a reopen sees the same clean state.
    drop(db);
    let db = fx.reopen();
    assert_eq!(db.schema_version(), 1);
    assert!(db
        .query("SELECT count(*) FROM u", [], |r| Ok(r.get::<_, i64>(0)?))
        .is_err());
}

/// 27. `ATTACH` is denied inside a migration for the same reason: it would let
///     user SQL write outside `data.db`, which the frame log cannot represent.
#[test]
fn migrate_with_attach_is_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();

    let err = db
        .migrate(
            2,
            "CREATE TABLE u (id INTEGER PRIMARY KEY); ATTACH DATABASE ':memory:' AS aux;",
        )
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("authoriz"),
        "ATTACH must be denied, got {err}"
    );
    assert_eq!(db.schema_version(), 1);
    assert!(db
        .query("SELECT count(*) FROM u", [], |r| Ok(r.get::<_, i64>(0)?))
        .is_err());
    drop(db);
    let db = fx.reopen();
    assert_eq!(db.schema_version(), 1);
}

/// 28. When the repair after a failed commit cannot rebuild `data.db`, the
///     handle is marked failed and *every* read path refuses to answer — not
///     just the write paths. Answering `query` here would expose data the store
///     has no frame for.
#[test]
fn failed_repair_disables_every_read_path() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'kept')", [])
        .unwrap();

    // Fault: data.db commits, the frame never reaches the store, and the repair
    // that would put data.db back in line fails too.
    db.set_fault(Some(FaultPlan::AfterDataCommitAndRepairFails));
    let err = db
        .execute("INSERT INTO t (id, v) VALUES (2, 'phantom')", [])
        .unwrap_err();
    assert!(matches!(err, Error::Injected(_)));

    // Every public entry point that reads state reports the failed handle.
    let q = db
        .query("SELECT v FROM t", [], |r| Ok(r.get::<_, String>(0)?))
        .unwrap_err();
    assert!(matches!(q, Error::Failed(_)), "query must fail, got {q}");
    assert!(matches!(db.dump().unwrap_err(), Error::Failed(_)));
    assert!(matches!(db.status().unwrap_err(), Error::Failed(_)));
    assert!(matches!(db.sync().unwrap_err(), Error::Failed(_)));
    assert!(matches!(
        db.migrate(2, "CREATE TABLE u (id INTEGER PRIMARY KEY);")
            .unwrap_err(),
        Error::Failed(_)
    ));
    assert!(matches!(
        db.execute("INSERT INTO t (id, v) VALUES (3, 'x')", [])
            .unwrap_err(),
        Error::Failed(_)
    ));
    // The reason names the repair that failed.
    assert!(
        q.to_string().contains("rebuild"),
        "the failure should explain itself, got {q}"
    );
}

/// 29. A version that can never be applied here (no migration frame recorded at
///     or below the current schema version) is an `InvalidVersion`, not a
///     misleading `MigrationMismatch`.
#[test]
fn migrate_version_zero_is_an_invalid_version() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    // A fresh database: version 0 is the *current* version, and there is no
    // migration 0 to compare against.
    assert_eq!(db.schema_version(), 0);
    let err = db.migrate(0, MIGRATE_T).unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidVersion {
                version: 0,
                current: 0
            }
        ),
        "got {err}"
    );

    db.migrate(1, MIGRATE_T).unwrap();
    let err = db.migrate(0, MIGRATE_T).unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidVersion {
                version: 0,
                current: 1
            }
        ),
        "got {err}"
    );
    // A version that *is* recorded still reports AlreadyApplied.
    assert_eq!(
        db.migrate(1, MIGRATE_T).unwrap(),
        MigrateOutcome::AlreadyApplied
    );
    drop(db);
    let mut db = fx.reopen();
    let err = db.migrate(0, MIGRATE_T).unwrap_err();
    assert!(matches!(err, Error::InvalidVersion { .. }), "got {err}");
}
