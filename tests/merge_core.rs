//! Merge-core tests: frame hashing, changeset capture, the apply/conflict
//! policy, ordered replay and the logical dump.
//!
//! Every test drives the real public API: capture a changeset from a source
//! database, wrap it in frames and replay it onto a fresh target database.

use rusqlite::Connection;
use sharepoint_db::materialize::apply_frame;
use sharepoint_db::{
    attach_session, capture, changeset, digest, dump, ensure_meta, frame_hash, migration_payload,
    new_id, register_functions, replay, sort_key, validate_schema, ApplyReport, DbId, Frame,
    FrameKind, FrameRef, NodeId,
};

const SCHEMA_T: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);";
const SCHEMA_COLS: &str =
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL);";

fn node(n: u8) -> NodeId {
    [n; 8]
}

fn db_id() -> DbId {
    [7u8; 16]
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

/// A fresh in-memory database running `schema`, with foreign keys on (the
/// bundled SQLite build defaults them on; be explicit anyway).
fn db(schema: &str) -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
    conn.execute_batch(schema).expect("apply schema");
    conn
}

/// Captures one changeset from `conn` (schema already applied, rows seeded).
fn capture_cs(conn: &Connection, tables: &[&str], sql: &str) -> Vec<u8> {
    let list = names(tables);
    capture(conn, &list, |c| {
        c.execute_batch(sql)?;
        Ok(())
    })
    .expect("capture")
}

/// Captures a changeset and wraps it in a frame.
fn frame_for(
    conn: &Connection,
    tables: &[&str],
    sql: &str,
    node_id: &NodeId,
    seq: u64,
    lamport: u64,
) -> Frame {
    let cs = capture_cs(conn, tables, sql);
    Frame::changeset(&db_id(), node_id, seq, lamport, [0u8; 32], cs)
}

/// Replays `frames` onto a fresh database built from `schema` + `seed`.
fn replay_into(
    schema: &str,
    seed: &str,
    frames: &[(&NodeId, &Frame)],
) -> (Connection, ApplyReport) {
    let conn = db(schema);
    ensure_meta(&conn).unwrap();
    if !seed.is_empty() {
        conn.execute_batch(seed).unwrap();
    }
    let refs: Vec<FrameRef> = frames
        .iter()
        .map(|(n, f)| FrameRef {
            node_id: n,
            frame: f,
        })
        .collect();
    let report = replay(&db_id(), &refs, &conn).expect("replay");
    (conn, report)
}

fn query_strings(conn: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn meta_version(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT value FROM __sharedb_meta WHERE key = 'schema_version'",
        [],
        |r| r.get::<_, String>(0),
    )
    .ok()
    .and_then(|v| v.parse::<i64>().ok())
    .unwrap_or(0)
}

/// 1. Two nodes update the same column of the same row: the frame with the
///    higher `(lamport, node_id)` wins, in either node-id direction.
#[test]
fn update_update_same_column_later_key_wins() {
    let seed = "INSERT INTO t (id, v) VALUES (1, 'base');";
    let a = node(0x01);
    let b = node(0xff);

    // b has the higher node id, so b wins.
    let src_a = db(SCHEMA_T);
    src_a.execute_batch(seed).unwrap();
    let fa = frame_for(
        &src_a,
        &["t"],
        "UPDATE t SET v = 'A' WHERE id = 1;",
        &a,
        1,
        5,
    );
    let src_b = db(SCHEMA_T);
    src_b.execute_batch(seed).unwrap();
    let fb = frame_for(
        &src_b,
        &["t"],
        "UPDATE t SET v = 'B' WHERE id = 1;",
        &b,
        1,
        5,
    );

    let (conn, report) = replay_into(SCHEMA_T, seed, &[(&a, &fa), (&b, &fb)]);
    assert_eq!(report.applied, 2);
    assert_eq!(report.dropped, 0);
    assert_eq!(query_strings(&conn, "SELECT v FROM t WHERE id = 1"), ["B"]);

    // Swap the node ids: now the *other* frame has the higher key and wins.
    let src_a2 = db(SCHEMA_T);
    src_a2.execute_batch(seed).unwrap();
    let fa2 = frame_for(
        &src_a2,
        &["t"],
        "UPDATE t SET v = 'A' WHERE id = 1;",
        &b,
        1,
        5,
    );
    let src_b2 = db(SCHEMA_T);
    src_b2.execute_batch(seed).unwrap();
    let fb2 = frame_for(
        &src_b2,
        &["t"],
        "UPDATE t SET v = 'B' WHERE id = 1;",
        &a,
        1,
        5,
    );

    let (conn2, report2) = replay_into(SCHEMA_T, seed, &[(&b, &fa2), (&a, &fb2)]);
    assert_eq!(report2.applied, 2);
    assert_eq!(query_strings(&conn2, "SELECT v FROM t WHERE id = 1"), ["A"]);
}

/// 2. Concurrent updates of different columns both survive.
#[test]
fn update_update_different_columns_both_survive() {
    let seed = "INSERT INTO t (id, a, b) VALUES (1, 'a0', 'b0');";
    let a = node(0x01);
    let b = node(0xff);

    let src_a = db(SCHEMA_COLS);
    src_a.execute_batch(seed).unwrap();
    let fa = frame_for(
        &src_a,
        &["t"],
        "UPDATE t SET a = 'a1' WHERE id = 1;",
        &a,
        1,
        3,
    );
    let src_b = db(SCHEMA_COLS);
    src_b.execute_batch(seed).unwrap();
    let fb = frame_for(
        &src_b,
        &["t"],
        "UPDATE t SET b = 'b1' WHERE id = 1;",
        &b,
        1,
        3,
    );

    // Feed them in both orders: the total order decides, not the input order.
    for order in [0, 1] {
        let frames: Vec<(&NodeId, &Frame)> = if order == 0 {
            vec![(&a, &fa), (&b, &fb)]
        } else {
            vec![(&b, &fb), (&a, &fa)]
        };
        let (conn, report) = replay_into(SCHEMA_COLS, seed, &frames);
        assert_eq!(report.applied, 2, "order {order}");
        assert_eq!(report.dropped, 0, "order {order}");
        assert_eq!(
            query_strings(&conn, "SELECT a || '/' || b FROM t WHERE id = 1"),
            ["a1/b1"],
            "order {order}"
        );
    }
}

/// 3. A delete beats a concurrent update, in both key orders.
#[test]
fn delete_beats_concurrent_update_in_both_key_orders() {
    let seed = "INSERT INTO t (id, v) VALUES (1, 'base');";
    let low = node(0x01);
    let high = node(0xff);

    // `low` deletes, `high` updates: the delete has the lower key.
    let src_del = db(SCHEMA_T);
    src_del.execute_batch(seed).unwrap();
    let f_del = frame_for(&src_del, &["t"], "DELETE FROM t WHERE id = 1;", &low, 1, 2);
    let src_upd = db(SCHEMA_T);
    src_upd.execute_batch(seed).unwrap();
    let f_upd = frame_for(
        &src_upd,
        &["t"],
        "UPDATE t SET v = 'U' WHERE id = 1;",
        &high,
        1,
        2,
    );

    let (conn, report) = replay_into(SCHEMA_T, seed, &[(&low, &f_del), (&high, &f_upd)]);
    assert_eq!(report.applied, 2);
    assert_eq!(
        query_strings(&conn, "SELECT CAST(count(*) AS TEXT) FROM t"),
        ["0"],
        "delete with lower key must still win"
    );

    // Now the delete carries the higher key.
    let src_del2 = db(SCHEMA_T);
    src_del2.execute_batch(seed).unwrap();
    let f_del2 = frame_for(
        &src_del2,
        &["t"],
        "DELETE FROM t WHERE id = 1;",
        &high,
        1,
        2,
    );
    let src_upd2 = db(SCHEMA_T);
    src_upd2.execute_batch(seed).unwrap();
    let f_upd2 = frame_for(
        &src_upd2,
        &["t"],
        "UPDATE t SET v = 'U' WHERE id = 1;",
        &low,
        1,
        2,
    );

    let (conn2, report2) = replay_into(SCHEMA_T, seed, &[(&high, &f_del2), (&low, &f_upd2)]);
    assert_eq!(report2.applied, 2);
    assert_eq!(
        query_strings(&conn2, "SELECT CAST(count(*) AS TEXT) FROM t"),
        ["0"],
        "delete with higher key must win"
    );
}

/// 4. Two inserts of the same primary key: the later key replaces the row.
#[test]
fn insert_insert_same_pk_later_key_replaces_row() {
    let a = node(0x01);
    let b = node(0xff);

    let src_a = db(SCHEMA_T);
    let fa = frame_for(
        &src_a,
        &["t"],
        "INSERT INTO t (id, v) VALUES (1, 'A');",
        &a,
        1,
        1,
    );
    let src_b = db(SCHEMA_T);
    let fb = frame_for(
        &src_b,
        &["t"],
        "INSERT INTO t (id, v) VALUES (1, 'B');",
        &b,
        1,
        1,
    );

    let (conn, report) = replay_into(SCHEMA_T, "", &[(&a, &fa), (&b, &fb)]);
    assert_eq!(report.applied, 2);
    assert_eq!(report.dropped, 0);
    assert_eq!(query_strings(&conn, "SELECT v FROM t WHERE id = 1"), ["B"]);
    assert_eq!(
        query_strings(&conn, "SELECT CAST(count(*) AS TEXT) FROM t"),
        ["1"]
    );

    // Swapped node ids swap the winner.
    let src_a2 = db(SCHEMA_T);
    let fa2 = frame_for(
        &src_a2,
        &["t"],
        "INSERT INTO t (id, v) VALUES (1, 'A');",
        &b,
        1,
        1,
    );
    let src_b2 = db(SCHEMA_T);
    let fb2 = frame_for(
        &src_b2,
        &["t"],
        "INSERT INTO t (id, v) VALUES (1, 'B');",
        &a,
        1,
        1,
    );
    let (conn2, _) = replay_into(SCHEMA_T, "", &[(&b, &fa2), (&a, &fb2)]);
    assert_eq!(query_strings(&conn2, "SELECT v FROM t WHERE id = 1"), ["A"]);
}

/// 5. A UNIQUE violation drops the later change, and `dropped` counts it.
#[test]
fn unique_violation_drops_later_change() {
    let schema = "CREATE TABLE u (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE);";
    let low = node(0x01);
    let high = node(0xff);

    let src_low = db(schema);
    let f_low = frame_for(
        &src_low,
        &["u"],
        "INSERT INTO u (id, code) VALUES (1, 'X');",
        &low,
        1,
        1,
    );
    let src_high = db(schema);
    let f_high = frame_for(
        &src_high,
        &["u"],
        "INSERT INTO u (id, code) VALUES (2, 'X');",
        &high,
        1,
        1,
    );

    let (conn, report) = replay_into(schema, "", &[(&low, &f_low), (&high, &f_high)]);
    assert_eq!(report.applied, 2);
    assert_eq!(
        report.dropped, 1,
        "the UNIQUE-violating change must be dropped"
    );
    assert_eq!(
        query_strings(&conn, "SELECT code || ':' || id FROM u ORDER BY id"),
        ["X:1"],
        "the earlier key keeps its row, the later one is dropped"
    );
}

/// 6. Equal lamports: the higher node id wins.
#[test]
fn node_id_breaks_lamport_tie() {
    let seed = "INSERT INTO t (id, v) VALUES (1, 'base');";
    let low = node(0x01);
    let high = node(0x02);
    assert!(
        sort_key(&FrameRef {
            node_id: &low,
            frame: &Frame::changeset(&db_id(), &low, 1, 9, [0u8; 32], vec![])
        }) < sort_key(&FrameRef {
            node_id: &high,
            frame: &Frame::changeset(&db_id(), &high, 1, 9, [0u8; 32], vec![])
        }),
        "node id must break a lamport tie"
    );

    let src_low = db(SCHEMA_T);
    src_low.execute_batch(seed).unwrap();
    let f_low = frame_for(
        &src_low,
        &["t"],
        "UPDATE t SET v = 'low' WHERE id = 1;",
        &low,
        1,
        7,
    );
    let src_high = db(SCHEMA_T);
    src_high.execute_batch(seed).unwrap();
    let f_high = frame_for(
        &src_high,
        &["t"],
        "UPDATE t SET v = 'high' WHERE id = 1;",
        &high,
        1,
        7,
    );

    // Replay in reverse input order to prove the sort, not the order, decides.
    let (conn, report) = replay_into(SCHEMA_T, seed, &[(&high, &f_high), (&low, &f_low)]);
    assert_eq!(report.applied, 2);
    assert_eq!(
        query_strings(&conn, "SELECT v FROM t WHERE id = 1"),
        ["high"]
    );
}

/// 7. A multi-statement transaction including an FK cascade delete: replaying
///    the captured changeset onto a fresh database yields the same dump.
#[test]
fn local_sql_equals_replay_of_its_changeset() {
    let schema = "
        CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE child  (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parent(id) ON DELETE CASCADE, note TEXT NOT NULL);
    ";
    let tables = names(&["parent", "child"]);
    let seed = "
        INSERT INTO parent (id, name) VALUES (1, 'p1'), (2, 'p2'), (3, 'p3');
        INSERT INTO child (id, parent_id, note) VALUES
            (10, 1, 'c10'), (11, 1, 'c11'), (20, 2, 'c20'), (30, 3, 'c30');
    ";

    let local = db(schema);
    local.execute_batch(seed).unwrap();
    let before = dump(&local).unwrap();
    assert!(!before.tables.is_empty());

    // One transaction: an insert, an update, and a delete that cascades.
    let cs = capture(&local, &tables, |c| {
        c.execute_batch(
            "
            INSERT INTO parent (id, name) VALUES (4, 'p4');
            INSERT INTO child (id, parent_id, note) VALUES (40, 4, 'c40');
            UPDATE parent SET name = 'p1-renamed' WHERE id = 1;
            DELETE FROM parent WHERE id = 1;
            ",
        )?;
        Ok(())
    })
    .expect("capture multi-statement transaction");
    assert!(!cs.is_empty(), "the transaction must produce a changeset");

    let local_dump = dump(&local).unwrap();
    assert_ne!(
        local_dump, before,
        "the local db must actually have changed"
    );
    assert_eq!(
        query_strings(
            &local,
            "SELECT CAST(count(*) AS TEXT) FROM child WHERE parent_id = 1"
        ),
        ["0"],
        "the FK cascade must have removed the children locally"
    );

    let node_id = node(0x42);
    let frame = Frame::changeset(&db_id(), &node_id, 1, 1, [0u8; 32], cs);

    // Fresh database with the same starting state, then replay the changeset.
    let fresh = db(schema);
    ensure_meta(&fresh).unwrap();
    fresh.execute_batch(seed).unwrap();
    let mut report = ApplyReport::default();
    apply_frame(
        &fresh,
        &FrameRef {
            node_id: &node_id,
            frame: &frame,
        },
        &mut report,
    )
    .expect("apply captured changeset");
    assert_eq!(report.applied, 1);
    assert_eq!(report.tables_missing, 0);
    assert_eq!(report.dropped, 0, "nothing may be dropped: {report:?}");

    let fresh_dump = dump(&fresh).unwrap();
    assert_eq!(
        local_dump, fresh_dump,
        "replaying the changeset must reproduce the local state (FK cascades included)"
    );
    assert!(local_dump.tables.iter().any(|(name, _)| name == "child"));
}

/// 8. A changeset whose table does not exist here is skipped, not fatal.
#[test]
fn changeset_for_missing_table_is_skipped() {
    let a = node(0x01);
    let src = db(SCHEMA_T);
    let f = frame_for(
        &src,
        &["t"],
        "INSERT INTO t (id, v) VALUES (1, 'x');",
        &a,
        1,
        1,
    );

    // Target only knows a different table.
    let conn = db("CREATE TABLE other (id INTEGER PRIMARY KEY);");
    ensure_meta(&conn).unwrap();
    let refs = [FrameRef {
        node_id: &a,
        frame: &f,
    }];
    let report = replay(&db_id(), &refs, &conn).expect("replay must not abort");
    assert_eq!(report.applied, 0);
    assert_eq!(report.skipped, 1);
    assert_eq!(report.tables_missing, 1);

    // And a mixed changeset: the table that exists is still applied.
    let schema2 = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);
                   CREATE TABLE z (id INTEGER PRIMARY KEY, v TEXT NOT NULL);";
    let src2 = db(schema2);
    let cs = capture_cs(
        &src2,
        &["t", "z"],
        "INSERT INTO t (id, v) VALUES (1, 'keep'); INSERT INTO z (id, v) VALUES (9, 'gone');",
    );
    let f2 = Frame::changeset(&db_id(), &a, 2, 2, [0u8; 32], cs);
    let target = db("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);");
    ensure_meta(&target).unwrap();
    let report2 = replay(
        &db_id(),
        &[FrameRef {
            node_id: &a,
            frame: &f2,
        }],
        &target,
    )
    .expect("replay with one missing table");
    assert_eq!(report2.applied, 1);
    assert_eq!(report2.skipped, 0);
    assert_eq!(report2.tables_missing, 1);
    assert_eq!(query_strings(&target, "SELECT v FROM t"), ["keep"]);
}

/// 9. Two migrations with the same version: the first in total order applies,
///    the duplicate is skipped.
#[test]
fn duplicate_migration_version_applied_once() {
    let low = node(0x01);
    let high = node(0xff);
    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);";

    let m_low = Frame::migration(&db_id(), &low, 1, 1, [0u8; 32], 1, sql);
    let m_high = Frame::migration(
        &db_id(),
        &high,
        1,
        2,
        [0u8; 32],
        1,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL); -- duplicate",
    );
    assert_eq!(m_low.migration_version(), Some(1));
    assert_eq!(m_high.migration_version(), Some(1));

    // Feed the higher-key one first: replay must still sort and apply `low`.
    let conn = Connection::open_in_memory().unwrap();
    ensure_meta(&conn).unwrap();
    let refs = [
        FrameRef {
            node_id: &high,
            frame: &m_high,
        },
        FrameRef {
            node_id: &low,
            frame: &m_low,
        },
    ];
    let report = replay(&db_id(), &refs, &conn).expect("replay migrations");
    assert_eq!(report.applied, 1);
    assert_eq!(report.skipped, 1);

    // The winner is the first in total order (lower node id): its SQL ran, and
    // running the duplicate too would have failed with "table t already exists".
    conn.execute_batch("INSERT INTO t (id, v) VALUES (1, 'ok');")
        .expect("migration created t");
    assert_eq!(query_strings(&conn, "SELECT v FROM t"), ["ok"]);
    assert_eq!(meta_version(&conn), 1);

    // A later version applies; an older/equal one is skipped.
    let m2 = Frame::migration(
        &db_id(),
        &low,
        2,
        3,
        m_low.hash,
        2,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY);",
    );
    let m1_again = Frame::migration(&db_id(), &high, 2, 4, [0u8; 32], 1, "SELECT 1;");
    let refs2 = [
        FrameRef {
            node_id: &high,
            frame: &m1_again,
        },
        FrameRef {
            node_id: &low,
            frame: &m2,
        },
    ];
    let report2 = replay(&db_id(), &refs2, &conn).expect("replay more migrations");
    assert_eq!(report2.applied, 1);
    assert_eq!(report2.skipped, 1);
    assert_eq!(meta_version(&conn), 2);
}

/// 10. Schema validation rejects a table without a PRIMARY KEY and any trigger.
#[test]
fn schema_validation_rejects_no_pk_table_and_trigger() {
    // A healthy schema passes (internal tables are ignored).
    let ok = db(SCHEMA_T);
    ensure_meta(&ok).unwrap();
    validate_schema(&ok).expect("valid schema");

    let no_pk = db("CREATE TABLE x (a TEXT);");
    match validate_schema(&no_pk) {
        Err(sharepoint_db::Error::SchemaUnsupported(msg)) => {
            assert!(msg.contains('x'), "message should name the table: {msg}");
        }
        other => panic!("expected SchemaUnsupported, got {other:?}"),
    }

    let without_rowid = db("CREATE TABLE w (a TEXT PRIMARY KEY, b TEXT) WITHOUT ROWID;");
    assert!(matches!(
        validate_schema(&without_rowid),
        Err(sharepoint_db::Error::SchemaUnsupported(_))
    ));

    let triggered = db("CREATE TABLE s (id INTEGER PRIMARY KEY, v TEXT);
                        CREATE TRIGGER s_ai AFTER INSERT ON s BEGIN SELECT 1; END;");
    assert!(matches!(
        validate_schema(&triggered),
        Err(sharepoint_db::Error::SchemaUnsupported(_))
    ));
}

/// 11. A write to a `__sharedb_*` table produces an empty changeset, and such a
///     table can never be attached.
#[test]
fn internal_tables_not_captured() {
    let conn = db(SCHEMA_T);
    ensure_meta(&conn).unwrap();
    let tables = names(&["t"]);

    let cs = capture(&conn, &tables, |c| {
        c.execute(
            "INSERT INTO __sharedb_meta (key, value) VALUES ('k', 'v')",
            [],
        )?;
        Ok(())
    })
    .expect("capture");
    assert!(
        cs.is_empty(),
        "internal writes must not be captured: {cs:?}"
    );

    // A real write through the same code path *is* captured, so the empty
    // result above is meaningful and not a broken capture path.
    let cs2 = capture(&conn, &tables, |c| {
        c.execute("INSERT INTO t (id, v) VALUES (1, 'x')", [])?;
        Ok(())
    })
    .expect("capture");
    assert!(!cs2.is_empty());

    // Attaching an internal table is refused outright.
    let err = attach_session(&conn, &names(&["__sharedb_meta"]));
    assert!(matches!(
        err,
        Err(sharepoint_db::Error::SchemaUnsupported(_))
    ));
}

/// Frame encode/decode round-trip, hash verification and `digest`.
#[test]
fn frame_roundtrip_and_hash() {
    let d = db_id();
    let n = node(0x0a);
    let f = Frame::changeset(&d, &n, 3, 11, [9u8; 32], vec![1, 2, 3, 4]);

    assert_eq!(
        f.hash,
        frame_hash(&d, &n, 3, 11, 1, &[9u8; 32], &[1, 2, 3, 4]),
        "the hash must be exactly the specified SHA-256"
    );

    let bytes = f.encode(&d, &n);
    // body_len | seq | lamport | kind | prev_hash | payload | hash
    assert_eq!(bytes.len(), 4 + 8 + 8 + 1 + 32 + 4 + 32);
    assert_eq!(&bytes[0..4], &(8 + 8 + 1 + 32 + 4 + 32u32).to_le_bytes());
    assert_eq!(&bytes[20..21], &[1u8], "kind byte for a changeset");

    let decoded = Frame::decode(&bytes, &d, &n).expect("decode");
    assert_eq!(decoded, f);

    // A migration frame carries `version u32 | SQL` and kind byte 2.
    let sql = "CREATE TABLE q (id INTEGER PRIMARY KEY);";
    let m = Frame::migration(&d, &n, 1, 1, [0u8; 32], 7, sql);
    assert_eq!(m.payload, migration_payload(7, sql));
    let mbytes = m.encode(&d, &n);
    assert_eq!(&mbytes[20..21], &[2u8]);
    let mdecoded = Frame::decode(&mbytes, &d, &n).expect("decode migration");
    assert_eq!(mdecoded, m);
    assert_eq!(mdecoded.migration_version(), Some(7));
    assert_eq!(mdecoded.migration_sql(), Some(sql));

    // Tampering with the payload must be caught.
    let mut bad = bytes.clone();
    bad[4 + 8 + 8 + 1 + 32 + 3] ^= 0xff;
    assert!(matches!(
        Frame::decode(&bad, &d, &n),
        Err(sharepoint_db::Error::HashMismatch)
    ));

    // Decoding under a different db id or node id must also fail.
    assert!(matches!(
        Frame::decode(&bytes, &[8u8; 16], &n),
        Err(sharepoint_db::Error::HashMismatch)
    ));
    assert!(matches!(
        Frame::decode(&bytes, &d, &node(0x0b)),
        Err(sharepoint_db::Error::HashMismatch)
    ));

    // Truncation is an error, not a panic.
    assert!(Frame::decode(&bytes[..bytes.len() - 1], &d, &n).is_err());
    assert!(Frame::decode(&[], &d, &n).is_err());

    // `digest` is order-sensitive and depends on the frames' hashes.
    let g = Frame::changeset(&d, &n, 4, 12, f.hash, vec![5]);
    let refs_ab = [
        FrameRef {
            node_id: &n,
            frame: &f,
        },
        FrameRef {
            node_id: &n,
            frame: &g,
        },
    ];
    let refs_ba = [refs_ab[1], refs_ab[0]];
    assert_eq!(digest(&refs_ab), digest(&refs_ab));
    assert_ne!(digest(&refs_ab), digest(&refs_ba));
    // SHA-256 of the empty input: digest of no frames.
    assert_eq!(
        digest(&[]),
        [
            0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
            0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
            0x78, 0x52, 0xb8, 0x55
        ]
    );
}

/// 12. `replay` and `apply_frame` leave `PRAGMA foreign_keys` exactly as they
///     found it, on success and on error.
#[test]
fn replay_restores_foreign_keys_pragma() {
    let fk = |conn: &Connection| -> i64 {
        conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))
            .unwrap()
    };

    let conn = db(SCHEMA_T);
    assert_eq!(fk(&conn), 1, "the helper db turns foreign keys on");
    ensure_meta(&conn).unwrap();

    let a = node(0x01);
    let src = db(SCHEMA_T);
    let f = frame_for(
        &src,
        &["t"],
        "INSERT INTO t (id, v) VALUES (1, 'x');",
        &a,
        1,
        1,
    );
    let refs = [FrameRef {
        node_id: &a,
        frame: &f,
    }];
    replay(&db_id(), &refs, &conn).expect("replay");
    assert_eq!(fk(&conn), 1, "replay must restore foreign_keys = ON");

    // Same for a single `apply_frame`.
    let mut report = ApplyReport::default();
    apply_frame(&conn, &refs[0], &mut report).expect("apply_frame");
    assert_eq!(fk(&conn), 1, "apply_frame must restore foreign_keys = ON");

    // And with foreign keys off to begin with: it must stay off.
    let off = Connection::open_in_memory().unwrap();
    off.pragma_update(None, "foreign_keys", "OFF").unwrap();
    off.execute_batch(SCHEMA_T).unwrap();
    ensure_meta(&off).unwrap();
    assert_eq!(fk(&off), 0);
    replay(&db_id(), &refs, &off).expect("replay with fk off");
    assert_eq!(fk(&off), 0, "replay must not turn foreign_keys on");
}

/// 13. An `AUTOINCREMENT` table (which makes SQLite create the internal
///     `sqlite_sequence` table) validates, and `dump` ignores `sqlite_*`.
#[test]
fn autoincrement_table_validates_and_sqlite_tables_are_ignored() {
    let conn = db("CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT);");
    conn.execute_batch("INSERT INTO a (id) VALUES (5);")
        .expect("autoincrement insert");
    validate_schema(&conn).expect("AUTOINCREMENT schema must be valid");

    // `sqlite_sequence` exists but must not appear in the dump.
    let has_sequence: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name = 'sqlite_sequence'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(has_sequence, 1, "the test needs sqlite_sequence to exist");
    let d = dump(&conn).unwrap();
    assert_eq!(
        d.tables.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        ["a"],
        "dump must exclude sqlite_* tables"
    );
}

/// 14. Schema validation uses SQLite's own metadata, not a substring match on
///     the DDL text.
#[test]
fn schema_validation_uses_metadata_not_substrings() {
    // No PRIMARY KEY, but the string literal mentions one: must still be
    // rejected.
    let fake_pk = db("CREATE TABLE x (a TEXT DEFAULT 'PRIMARY KEY');");
    match validate_schema(&fake_pk) {
        Err(sharepoint_db::Error::SchemaUnsupported(msg)) => {
            assert!(msg.contains('x'), "message should name the table: {msg}");
        }
        other => panic!("expected SchemaUnsupported, got {other:?}"),
    }

    // A real PRIMARY KEY, but a string literal mentions WITHOUT ROWID: the
    // table must be accepted.
    let fake_wr = db("CREATE TABLE y (id INTEGER PRIMARY KEY, note TEXT DEFAULT 'WITHOUT ROWID');");
    validate_schema(&fake_wr).expect("string literal must not make a table WITHOUT ROWID");

    // And a genuine WITHOUT ROWID table is still rejected.
    let real_wr = db("CREATE TABLE z (a TEXT PRIMARY KEY, b TEXT) WITHOUT ROWID;");
    assert!(matches!(
        validate_schema(&real_wr),
        Err(sharepoint_db::Error::SchemaUnsupported(_))
    ));
}

/// 15. `capture` never leaves a transaction open: a failing closure rolls back,
///     and a closure that commits inside itself is refused.
#[test]
fn capture_never_leaves_transaction_open() {
    let conn = db(SCHEMA_T);
    conn.execute_batch("INSERT INTO t (id, v) VALUES (1, 'x');")
        .unwrap();
    let tables = names(&["t"]);

    // A closure that returns an error: the transaction is rolled back and the
    // connection is back in autocommit mode.
    let err = capture(&conn, &tables, |c| {
        c.execute_batch("INSERT INTO t (id, v) VALUES (2, 'y');")?;
        Err(sharepoint_db::Error::SchemaUnsupported("boom".into()))
    });
    assert!(err.is_err());
    assert!(conn.is_autocommit(), "capture must roll back on error");
    assert_eq!(
        query_strings(&conn, "SELECT CAST(count(*) AS TEXT) FROM t"),
        ["1"]
    );

    // A closure that commits inside itself: refused, because the schema guard
    // could not undo the committed change.
    let err = capture(&conn, &tables, |c| {
        c.execute_batch("INSERT INTO t (id, v) VALUES (3, 'z'); COMMIT;")?;
        Ok(())
    });
    assert!(
        matches!(err, Err(sharepoint_db::Error::SchemaUnsupported(_))),
        "a closure that commits must be refused, got {err:?}"
    );
    assert!(conn.is_autocommit(), "capture must not leave a transaction");
}

/// 16. A migration applies atomically: if one of its statements fails, nothing
///     it created survives and the schema version does not move. `replay` also
///     creates the bookkeeping tables itself.
#[test]
fn failed_migration_is_atomic() {
    let conn = Connection::open_in_memory().unwrap();
    // No `ensure_meta`: replay must create `__sharedb_meta` itself.
    let a = node(0x01);
    let bad = Frame::migration(
        &db_id(),
        &a,
        1,
        1,
        [0u8; 32],
        1,
        "CREATE TABLE m1 (id INTEGER PRIMARY KEY); SELECT syntax error here;",
    );
    let refs = [FrameRef {
        node_id: &a,
        frame: &bad,
    }];
    assert!(replay(&db_id(), &refs, &conn).is_err(), "bad SQL must fail");
    assert!(conn.is_autocommit(), "the savepoint must be closed");
    assert_eq!(
        query_strings(
            &conn,
            "SELECT CAST(count(*) AS TEXT) FROM sqlite_master WHERE name = 'm1'"
        ),
        ["0"],
        "the DDL of a failed migration must be rolled back"
    );
    assert_eq!(meta_version(&conn), 0, "the version must not move");

    // A good migration afterwards still works.
    let good = Frame::migration(
        &db_id(),
        &a,
        2,
        2,
        [0u8; 32],
        1,
        "CREATE TABLE m1 (id INTEGER PRIMARY KEY);",
    );
    let report = replay(
        &db_id(),
        &[FrameRef {
            node_id: &a,
            frame: &good,
        }],
        &conn,
    )
    .expect("replay after failed migration");
    assert_eq!(report.applied, 1);
    assert_eq!(meta_version(&conn), 1);
}

/// 17. `Frame::encode` writes the frame's own hash, so a frame can never be
///     silently re-hashed under a different identity.
#[test]
fn encode_writes_the_frames_own_hash() {
    let d = db_id();
    let n = node(0x0a);
    let f = Frame::changeset(&d, &n, 1, 1, [0u8; 32], vec![7, 8]);
    let bytes = f.encode(&d, &n);
    let tail = bytes.len() - 32;
    assert_eq!(
        &bytes[tail..],
        &f.hash[..],
        "encode must write self.hash verbatim"
    );
}

/// `capture` refuses schema-changing SQL and rolls the whole transaction back.
#[test]
fn capture_rejects_schema_change() {
    let conn = db(SCHEMA_T);
    conn.execute_batch("INSERT INTO t (id, v) VALUES (1, 'x');")
        .unwrap();
    let tables = names(&["t"]);
    let err = capture(&conn, &tables, |c| {
        c.execute_batch("INSERT INTO t (id, v) VALUES (2, 'y'); ALTER TABLE t ADD COLUMN c TEXT;")?;
        Ok(())
    });
    assert!(matches!(
        err,
        Err(sharepoint_db::Error::SchemaChangeInExecute)
    ));
    // The whole transaction, including the data write, was rolled back.
    assert_eq!(
        query_strings(&conn, "SELECT CAST(count(*) AS TEXT) FROM t"),
        ["1"]
    );
    assert_eq!(
        query_strings(
            &conn,
            "SELECT CAST(count(*) AS TEXT) FROM pragma_table_info('t') WHERE name = 'c'"
        ),
        ["0"]
    );
}

/// `dump` renders every column as a string, NULL as `NULL`, and orders rows by
/// PRIMARY KEY; `new_id` / `sharedb_new_id()` produce 16 hex chars.
#[test]
fn dump_rendering_and_ids() {
    let conn = db("CREATE TABLE d (k TEXT PRIMARY KEY, n INTEGER, f REAL, b BLOB, s TEXT);");
    conn.execute_batch(
        "INSERT INTO d VALUES ('b', 2, 2.5, x'00ff', NULL);
         INSERT INTO d VALUES ('a', 1, 1.5, x'0a', 'hi');
         INSERT INTO d VALUES ('c', NULL, NULL, NULL, NULL);",
    )
    .unwrap();
    let d = dump(&conn).unwrap();
    assert_eq!(d.tables.len(), 1);
    let (name, rows) = &d.tables[0];
    assert_eq!(name, "d");
    assert_eq!(
        rows,
        &vec![
            vec![
                "a".to_string(),
                "1".into(),
                "1.5".into(),
                "x'0a'".into(),
                "hi".into()
            ],
            vec![
                "b".to_string(),
                "2".into(),
                "2.5".into(),
                "x'00ff'".into(),
                "NULL".into()
            ],
            vec![
                "c".to_string(),
                "NULL".into(),
                "NULL".into(),
                "NULL".into(),
                "NULL".into()
            ],
        ],
        "rows must be in PRIMARY KEY order and NULL rendered as NULL"
    );
    assert_ne!(d, dump(&Connection::open_in_memory().unwrap()).unwrap());

    for _ in 0..5 {
        let id = new_id();
        assert_eq!(id.len(), 16, "id must be 16 hex chars: {id}");
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()),
            "id must be hex: {id}"
        );
    }
    assert_ne!(new_id(), new_id(), "ids must be random");

    let conn2 = Connection::open_in_memory().unwrap();
    register_functions(&conn2).unwrap();
    let from_sql: String = conn2
        .query_row("SELECT sharedb_new_id()", [], |r| r.get(0))
        .unwrap();
    assert_eq!(from_sql.len(), 16);
    let two: i64 = conn2
        .query_row(
            "SELECT count(DISTINCT i) FROM (
                 SELECT sharedb_new_id() AS i UNION ALL SELECT sharedb_new_id()
             )",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(two, 2, "the SQL function must return a fresh id per call");
}

/// `changeset()` on a session with nothing recorded is empty, and the frame
/// kinds expose what the apply policy needs.
#[test]
fn session_and_kind_basics() {
    let conn = db(SCHEMA_T);
    let mut session = attach_session(&conn, &names(&["t"])).unwrap();
    assert!(changeset(&mut session).unwrap().is_empty());
    conn.execute_batch("INSERT INTO t (id, v) VALUES (1, 'x');")
        .unwrap();
    assert!(!changeset(&mut session).unwrap().is_empty());
    // Attaching nothing is refused rather than silently capturing everything.
    assert!(attach_session(&conn, &[]).is_err());

    assert_eq!(FrameKind::Changeset.kind_byte(), 1);
    assert_eq!(FrameKind::Migration(3).kind_byte(), 2);
    assert_eq!(FrameKind::Migration(3).version(), Some(3));
    assert_eq!(FrameKind::Changeset.version(), None);
}

/// Frames round-trip through `encode`/`decode` for arbitrary bodies, and a
/// single flipped bit never decodes.
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn frame_encode_decode_roundtrip(
            seq in any::<u64>(),
            lamport in any::<u64>(),
            prev in any::<[u8; 32]>(),
            payload in proptest::collection::vec(any::<u8>(), 0..64),
            node in any::<[u8; 8]>(),
            version in any::<u32>(),
        ) {
            let d = db_id();
            let cs = Frame::changeset(&d, &node, seq, lamport, prev, payload.clone());
            let bytes = cs.encode(&d, &node);
            prop_assert_eq!(Frame::decode(&bytes, &d, &node)?.hash, cs.hash);

            let m = Frame::migration(&d, &node, seq, lamport, prev, version, "SELECT 1;");
            let mbytes = m.encode(&d, &node);
            prop_assert_eq!(Frame::decode(&mbytes, &d, &node)?.hash, m.hash);

            // Flipping any bit of the payload changes the hash and is rejected.
            if !payload.is_empty() {
                let mut bad = bytes.clone();
                let idx = 4 + 8 + 8 + 1 + 32;
                bad[idx] ^= 0x80;
                prop_assert!(matches!(
                    Frame::decode(&bad, &d, &node),
                    Err(sharepoint_db::Error::HashMismatch)
                ));
            }
        }
    }
}
