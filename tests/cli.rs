//! End-to-end tests for the `sharedb` binary.
//!
//! These drive the *real* executable (`env!("CARGO_BIN_EXE_sharedb")`) through
//! `std::process::Command`, so they cover the argument parsing, the exit codes
//! and the printed output, not just the library behind them.
//!
//! The alice/bob demo needs no fake sync client: both nodes point at the *same*
//! folder, which is exactly what a synced folder is. A temp directory stands in
//! for the OneDrive folder, and the two state directories sit beside it — never
//! inside it, which the library itself refuses.

#![cfg(feature = "cli")]

use std::path::{Path, PathBuf};
use std::process::Output;

/// The schema both writers migrate to.
///
/// A text primary key, as the README recommends: two nodes minting their own
/// `INTEGER PRIMARY KEY` rowids collide, text ids from `sharedb_new_id()` do
/// not.
const MIGRATE: &str = "CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT NOT NULL);";

/// One CLI invocation: `sharedb --db <folder> --state-dir <state> <args>`.
fn sharedb(folder: &Path, state: &Path, args: &[&str]) -> Output {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_sharedb"))
        .arg("--db")
        .arg(folder)
        .arg("--state-dir")
        .arg(state)
        .args(args)
        .output()
        .expect("run sharedb");
    println!(
        "sharedb {} -> {}\n{}{}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Runs the command and returns stdout, asserting it succeeded.
fn ok(folder: &Path, state: &Path, args: &[&str]) -> String {
    let out = sharedb(folder, state, args);
    assert!(
        out.status.success(),
        "`sharedb {}` failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("stdout is utf-8")
}

/// The alice/bob demo, start to finish.
///
/// Two writers, one folder, two state directories. Both migrate the same
/// migration, both write a *conflicting* row without syncing, then both sync:
/// the folder merges the two histories and every node must end up printing the
/// same query output.
#[test]
fn alice_and_bob_converge_through_the_binary() {
    let tmp = tempfile::tempdir().unwrap();
    let folder = tmp.path().join("shared");
    let alice = tmp.path().join("alice");
    let bob = tmp.path().join("bob");

    // Two nodes, one folder: `init` is idempotent for the second node because
    // the MANIFEST it would write is already there and says the same thing.
    let init = ok(&folder, &alice, &["init"]);
    assert!(init.starts_with("initialized "), "{init}");
    ok(&folder, &bob, &["init"]);

    // Same migration, same SQL: the second one is a duplicate that loses the
    // ordering race and is skipped on replay.
    let alice_migrate = ok(&folder, &alice, &["migrate", "1", MIGRATE]);
    assert!(alice_migrate.contains("applied"), "{alice_migrate}");
    let bob_migrate = ok(&folder, &bob, &["migrate", "1", MIGRATE]);
    assert!(bob_migrate.contains("applied"), "{bob_migrate}");

    // Conflicting writes to the same primary key, neither publishing yet.
    for (state, value) in [(&alice, "alice"), (&bob, "bob")] {
        let exec = ok(
            &folder,
            state,
            &[
                "exec",
                &format!("INSERT INTO t (id, v) VALUES ('x', '{value}')"),
                "--no-sync",
            ],
        );
        assert!(exec.starts_with("1 row(s) changed"), "{exec}");
    }

    // Both still see only their own row: nothing has been published or merged.
    assert_eq!(
        query(&folder, &alice),
        vec!["x\talice".to_string()],
        "before sync alice must see only her own write"
    );
    assert_eq!(
        query(&folder, &bob),
        vec!["x\tbob".to_string()],
        "before sync bob must see only his own write"
    );

    // Two rounds of sync: the first publishes what each node holds and picks up
    // what the other published during the round, the second settles everyone.
    for _ in 0..2 {
        ok(&folder, &alice, &["sync"]);
        ok(&folder, &bob, &["sync"]);
    }

    // The point of the whole exercise: identical output on both nodes.
    let alice_out = ok(
        &folder,
        &alice,
        &["query", "--tsv", "SELECT id, v FROM t ORDER BY id"],
    );
    let bob_out = ok(
        &folder,
        &bob,
        &["query", "--tsv", "SELECT id, v FROM t ORDER BY id"],
    );
    assert_eq!(alice_out, bob_out, "the two nodes did not converge");

    // Exactly one row survives the conflict, and it is one of the two writes.
    let rows = parse_rows(&alice_out);
    assert_eq!(rows.len(), 1, "one row must survive: {rows:?}");
    assert!(
        rows[0] == "x\talice" || rows[0] == "x\tbob",
        "the survivor must be one of the two competing writes: {:?}",
        rows[0]
    );

    // A third node joining the same folder converges too, without ever having
    // written anything itself.
    let carol = tmp.path().join("carol");
    ok(&folder, &carol, &["init"]);
    ok(&folder, &carol, &["sync"]);
    let carol_out = ok(
        &folder,
        &carol,
        &["query", "--tsv", "SELECT id, v FROM t ORDER BY id"],
    );
    assert_eq!(
        carol_out, alice_out,
        "a joining node must see the same data"
    );
}

/// The `--tsv` rows of a query, header dropped.
fn query(folder: &Path, state: &Path) -> Vec<String> {
    parse_rows(&ok(
        folder,
        state,
        &["query", "--tsv", "SELECT id, v FROM t ORDER BY id"],
    ))
}

/// Splits TSV output into rows, dropping the header and the trailing newline.
fn parse_rows(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .skip(1)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

/// `status` prints the node's own identity and, after a sync, its peer.
#[test]
fn status_reports_node_peers_and_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let folder = tmp.path().join("shared");
    let alice = tmp.path().join("alice");
    let bob = tmp.path().join("bob");

    ok(&folder, &alice, &["init"]);
    ok(&folder, &bob, &["init"]);
    ok(&folder, &alice, &["migrate", "1", MIGRATE]);
    ok(&folder, &bob, &["migrate", "1", MIGRATE]);

    // Alice has written and not published: `unpublished` counts those frames —
    // the migration and the insert, neither of which reached the folder yet.
    ok(
        &folder,
        &alice,
        &[
            "exec",
            "INSERT INTO t (id, v) VALUES ('a', 'one')",
            "--no-sync",
        ],
    );
    let status = ok(&folder, &alice, &["status"]);
    assert!(status.contains("node_id:"), "{status}");
    assert!(status.contains("own_seq:                2"), "{status}");
    assert!(status.contains("unpublished:            2"), "{status}");
    assert!(status.contains("peers:                  none"), "{status}");
    assert!(status.contains("materialize_pending:    false"), "{status}");
    assert!(status.contains("dropped_below_boundary: 0"), "{status}");

    // After both sync, alice knows bob as a peer and has nothing pending.
    ok(&folder, &alice, &["sync"]);
    ok(&folder, &bob, &["sync"]);
    ok(&folder, &alice, &["sync"]);
    let status = ok(&folder, &alice, &["status"]);
    assert!(status.contains("unpublished:            0"), "{status}");
    assert!(!status.contains("peers:                  none"), "{status}");
}

/// `sync` prints every field of the report, so a script can grep it.
#[test]
fn sync_prints_the_whole_report() {
    let tmp = tempfile::tempdir().unwrap();
    let folder = tmp.path().join("shared");
    let alice = tmp.path().join("alice");
    ok(&folder, &alice, &["init"]);
    ok(&folder, &alice, &["migrate", "1", MIGRATE]);
    let out = ok(&folder, &alice, &["sync"]);
    for key in [
        "published:",
        "ingested:",
        "materialize:",
        "gaps:",
        "forks:",
        "publish_pending:",
        "dropped_below_boundary:",
        "materialize_pending:",
    ] {
        assert!(out.contains(key), "{key} missing from:\n{out}");
    }
}

/// `export` writes a plain SQLite file: openable as a database, containing the
/// user's tables and data and none of the `__sharedb_*` bookkeeping.
#[test]
fn export_writes_a_plain_sqlite_file() {
    let tmp = tempfile::tempdir().unwrap();
    let folder = tmp.path().join("shared");
    let alice = tmp.path().join("alice");
    ok(&folder, &alice, &["init"]);
    ok(&folder, &alice, &["migrate", "1", MIGRATE]);
    ok(
        &folder,
        &alice,
        &[
            "exec",
            "INSERT INTO t (id, v) VALUES ('a', 'one')",
            "--no-sync",
        ],
    );

    let out = tmp.path().join("snapshot.db");
    let msg = ok(&folder, &alice, &["export", out.to_str().unwrap()]);
    assert!(msg.starts_with("exported "), "{msg}");
    assert!(out.exists(), "the snapshot file must exist");

    // Read it back with a plain rusqlite connection — no sharepoint-db, no
    // session extension, nothing but SQLite.
    let conn = rusqlite::Connection::open(&out).expect("the snapshot is a SQLite file");
    let names: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(names, vec!["t".to_string()], "only the user table remains");
    let rows: Vec<(String, String)> = conn
        .prepare("SELECT id, v FROM t")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![("a".to_string(), "one".to_string())]);

    // Exporting again over the same path works (the file is replaced).
    ok(&folder, &alice, &["export", out.to_str().unwrap()]);
}

/// A failing command exits non-zero with the message on stderr and nothing on
/// stdout.
#[test]
fn errors_exit_nonzero_with_a_message_on_stderr() {
    let tmp = tempfile::tempdir().unwrap();
    let folder = tmp.path().join("shared");
    let alice = tmp.path().join("alice");

    // No database yet.
    let out = sharedb(&folder, &alice, &["status"]);
    assert!(!out.status.success(), "opening a missing db must fail");
    assert!(out.stdout.is_empty(), "nothing on stdout on error");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("database not found"),
        "{out:?}"
    );

    ok(&folder, &alice, &["init"]);
    ok(&folder, &alice, &["migrate", "1", MIGRATE]);

    // A write statement handed to `query`.
    let out = sharedb(
        &folder,
        &alice,
        &["query", "INSERT INTO t (id, v) VALUES ('a', 'x')"],
    );
    assert!(!out.status.success(), "query must refuse a write");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("read-only"),
        "{out:?}"
    );

    // A migration version that skips one.
    let out = sharedb(
        &folder,
        &alice,
        &["migrate", "7", "CREATE TABLE u (x INTEGER PRIMARY KEY);"],
    );
    assert!(!out.status.success(), "a version gap must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("migration gap"),
        "{out:?}"
    );

    // No `--db` at all, and no `SHAREDB_PATH` for the child either.
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_sharedb"))
        .arg("status")
        .env_remove("SHAREDB_PATH")
        .output()
        .expect("run sharedb");
    assert!(!out.status.success(), "a missing --db must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--db is required"),
        "{out:?}"
    );
}

/// The global options also come from the environment, which is what makes a
/// shell session with one database comfortable.
#[test]
fn global_options_come_from_the_environment() {
    let tmp = tempfile::tempdir().unwrap();
    let folder = tmp.path().join("shared");
    let state = tmp.path().join("state");
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_sharedb"))
            .env("SHAREDB_PATH", &folder)
            .env("SHAREDB_STATE_DIR", &state)
            .args(args)
            .output()
            .expect("run sharedb")
    };
    let out = run(&["init"]);
    assert!(out.status.success(), "{out:?}");
    let out = run(&["migrate", "1", MIGRATE]);
    assert!(out.status.success(), "{out:?}");
    let out = run(&["query", "SELECT 1 AS one"]);
    assert!(out.status.success(), "{out:?}");
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("one"),
        "{out:?}"
    );
}

/// The state directory must never live inside the synced folder; the CLI passes
/// that refusal through.
#[test]
fn state_dir_inside_the_folder_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let folder = tmp.path().join("shared");
    let inside: PathBuf = folder.join("state");
    let out = sharedb(&folder, &inside, &["init"]);
    assert!(!out.status.success(), "a synced state dir must be refused");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("state directory"),
        "{out:?}"
    );
}

/// `--help` works and lists every subcommand.
#[test]
fn help_lists_the_subcommands() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_sharedb"))
        .arg("--help")
        .output()
        .expect("run sharedb --help");
    assert!(out.status.success(), "{out:?}");
    let help = String::from_utf8_lossy(&out.stdout);
    for name in [
        "init", "migrate", "exec", "query", "sync", "status", "export",
    ] {
        assert!(help.contains(name), "`{name}` missing from --help:\n{help}");
    }
}
