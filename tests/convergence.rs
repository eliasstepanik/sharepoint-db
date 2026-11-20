//! Folder sync and convergence: nodes exchange frames through the synced folder
//! as write-once segment files, and `sync()` merges them so every node ends up
//! with the same data.
//!
//! Nodes are separate directories (separate machines); [`FakeSync::deliver`]
//! copies segment files between them, which is exactly what a real sync client
//! does.

mod common;

use common::{folder_snapshot, put_segment, FakeSync, Node, MIGRATE_T};
use sharepoint_db::folder::segment_valid;
use sharepoint_db::{Error, MaterializeKind, MigrateOutcome};

/// 1. A segment is valid only as a whole: every single-byte flip in a sample of
///    positions — each byte of the magic, one byte in each header field, one byte
///    in each frame header field, and the last byte — is rejected.
#[test]
fn segment_rejects_any_single_byte_flip() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.insert(2, "two");
    alice.sync();

    let dir = alice.segments_dir();
    let name = alice
        .segments()
        .into_iter()
        .find(|n| n.ends_with(".seg"))
        .expect("a published segment");
    let good = std::fs::read(dir.join(&name)).unwrap();
    let db_id = alice.db_id();
    assert!(
        segment_valid(&good, &db_id),
        "the pristine segment must verify"
    );

    // Positions to flip: the whole magic, one byte per header field, one byte per
    // frame header field (body_len, seq, lamport, kind, prev_hash, the payload
    // and the trailing hash), plus the very last byte.
    let mut positions: Vec<usize> = (0..8).collect();
    positions.extend([8, 16, 24, 32, 40]); // db_id, node_id, first_seq, count
    let frame_start = 44usize;
    positions.push(frame_start); // body_len
    positions.push(frame_start + 4); // seq
    positions.push(frame_start + 12); // lamport
    positions.push(frame_start + 20); // kind
    positions.push(frame_start + 21); // prev_hash
    positions.push(frame_start + 53); // payload
    positions.push(good.len() - 1); // last byte (part of the frame hash)
                                    // Every byte of the last frame's trailing hash: a flip anywhere in the hash
                                    // must break the verification.
    for i in 0..32usize {
        positions.push(good.len() - 1 - i);
    }

    for pos in positions {
        assert!(pos < good.len(), "sample position {pos} out of range");
        let mut flipped = good.clone();
        flipped[pos] ^= 0x01;
        assert_ne!(flipped, good);
        assert!(
            !segment_valid(&flipped, &db_id),
            "flipping byte {pos} must invalidate the segment"
        );
        // And the ingest path agrees: the file is ignored, nothing is ingested.
        let mut bob = Node::join(tmp.path(), &format!("bob{}", pos), &alice);
        put_segment(&bob, &name, &flipped);
        let report = bob.sync();
        assert_eq!(
            report.ingested, 0,
            "byte {pos}: a flipped frame must not be ingested"
        );
        assert!(report.ignored_files.iter().any(|p| p.is_file()));
        assert!(
            bob.values().is_empty(),
            "byte {pos}: flipped data must not materialise"
        );
    }

    // Truncation and appended garbage are rejected too (EOF must land exactly
    // after the last frame).
    assert!(!segment_valid(&good[..good.len() - 1], &db_id));
    let mut extra = good.clone();
    extra.push(0);
    assert!(!segment_valid(&extra, &db_id));
    // A segment from another database is rejected.
    let carol = Node::create(tmp.path(), "carol-other-db");
    assert!(!segment_valid(&good, &carol.db_id()));
}

/// 2. Two nodes that both write, then exchange segments, end up with identical
///    dumps.
#[test]
fn two_nodes_converge_after_exchange() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.sync();

    // Bob joins: he has the manifest but no frames, so he first has to receive
    // alice's migration to have a table to write into.
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    assert_eq!(bob.db.schema_version(), 1);

    alice.insert(1, "from alice");
    bob.insert(2, "from bob");
    assert_eq!(alice.values(), vec!["from alice".to_string()]);
    assert_eq!(bob.values(), vec!["from bob".to_string()]);

    FakeSync::exchange(&mut [&mut alice, &mut bob]);

    assert_eq!(
        alice.values(),
        vec!["from alice".to_string(), "from bob".to_string()]
    );
    assert_eq!(alice.dump(), bob.dump());
}

/// 3. The same row and column updated on both nodes without syncing first: after
///    a full exchange both dumps agree and hold the last writer in total order.
#[test]
fn concurrent_conflicting_writes_converge_to_lww_winner() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "seed");
    alice.sync();
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    assert_eq!(bob.values(), vec!["seed".to_string()]);

    // Both overwrite the same row without seeing each other.
    alice.update(1, "alice wins or loses");
    bob.update(1, "bob wins or loses");

    FakeSync::exchange(&mut [&mut alice, &mut bob]);
    assert_eq!(alice.dump(), bob.dump());

    // The winner is the frame with the higher `(lamport, node_id, seq)`. Both
    // updates carry the same lamport, so the node id decides — and node ids are
    // random, so compute it instead of hard-coding a name.
    let alice_wins = alice.node_id() > bob.node_id();
    let expected = if alice_wins {
        "alice wins or loses"
    } else {
        "bob wins or loses"
    };
    assert_eq!(alice.values(), vec![expected.to_string()]);
    assert_eq!(bob.values(), vec![expected.to_string()]);
}

/// 4. Three nodes, some segments deliberately withheld: after the withheld files
///    are delivered, all three agree.
#[test]
fn three_nodes_converge_with_partial_delivery() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(0, "seed");
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    let mut carol = Node::join(tmp.path(), "carol", &alice);

    // Alice publishes; bob gets it, carol does not (deliberate partial delivery).
    alice.sync();
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    assert_eq!(bob.values(), vec!["seed".to_string()]);
    assert!(
        carol.values().is_empty(),
        "carol was never delivered anything"
    );

    bob.insert(1, "bob");
    carol.migrate1();
    carol.insert(2, "carol");
    bob.sync();
    carol.sync();

    // Deliver alice -> carol first, then bob -> alice: carol never receives
    // bob's segment, so it stays behind for now.
    FakeSync::deliver(&alice, &carol);
    FakeSync::deliver(&bob, &alice);
    alice.sync();
    carol.sync();
    assert!(
        !carol.values().contains(&"bob".to_string()),
        "carol must still be missing bob's segment"
    );
    assert!(alice.values().contains(&"bob".to_string()));

    // Complete the delivery: everything converges.
    FakeSync::deliver(&bob, &carol);
    FakeSync::exchange(&mut [&mut alice, &mut bob, &mut carol]);
    assert_eq!(alice.dump(), bob.dump());
    assert_eq!(alice.dump(), carol.dump());
    assert_eq!(
        alice.values(),
        vec!["seed".to_string(), "bob".to_string(), "carol".to_string()]
    );
}

/// 5. A frame that sorts *before* frames already applied arrives late: `data.db`
///    cannot simply be extended, it has to be rebuilt, and the node still
///    converges.
#[test]
fn late_lower_key_frame_triggers_rebuild_and_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "alice one");
    alice.insert(2, "alice two");
    alice.sync();

    // Bob joins and migrates the same version on his own, so his frames carry
    // lamport 1 and 2 and sort *inside* alice's already-applied prefix.
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    bob.migrate1();
    bob.insert(3, "bob one");
    bob.sync();

    // Alice finally receives bob's segment.
    FakeSync::deliver(&bob, &alice);
    let report = alice.sync();
    assert_eq!(report.ingested, 2);
    assert_eq!(
        report.materialize,
        MaterializeKind::Rebuilt,
        "a frame sorting before the applied ones must force a rebuild"
    );

    FakeSync::exchange(&mut [&mut alice, &mut bob]);
    assert_eq!(alice.dump(), bob.dump());
    assert_eq!(
        alice.values(),
        vec![
            "alice one".to_string(),
            "alice two".to_string(),
            "bob one".to_string()
        ]
    );
    assert_eq!(alice.db.schema_version(), 1);
}

/// 6. A node whose state directory is empty rebuilds everything from the folder.
#[test]
fn new_node_bootstraps_from_folder_only() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.insert(2, "two");
    alice.sync();

    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    let report = bob.sync();
    assert_eq!(report.ingested, 3, "migration + two inserts");
    assert_eq!(bob.values(), vec!["one".to_string(), "two".to_string()]);
    assert_eq!(bob.db.schema_version(), 1);

    // A node that loses its state directory afterwards recovers the same way:
    // the folder is the source of truth.
    let mut bob = bob.reopen_fresh();
    assert!(bob.values().is_empty());
    bob.sync();
    assert_eq!(bob.dump(), alice.dump());
}

/// 7. A second sync with nothing new does nothing.
#[test]
fn sync_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    let first = alice.sync();
    assert_eq!(first.published, 2);
    assert_eq!(first.ingested, 0);
    assert!(!first.publish_pending);

    let second = alice.sync();
    assert_eq!(second.published, 0, "nothing new to publish");
    assert_eq!(second.ingested, 0);
    assert_eq!(second.materialize, MaterializeKind::UpToDate);
    assert!(second.ignored_files.is_empty());
    assert_eq!(second.gaps, Vec::new());
    assert_eq!(second.forks, Vec::new());
    assert!(!second.publish_pending);
    assert_eq!(alice.segments().len(), 1, "no extra segment written");
    assert_eq!(alice.db.status().unwrap().unpublished, 0);
}

/// 8. `execute` and `transaction` never touch the synced folder.
#[test]
fn execute_never_touches_synced_folder() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "seed");
    let before = folder_snapshot(&alice);
    assert!(!before.is_empty(), "the manifest is in the folder");

    alice.insert(2, "two");
    alice.update(1, "one");
    alice
        .db
        .transaction(|txn| {
            txn.execute("INSERT INTO t (id, v) VALUES (3, 'three')", [])?;
            txn.execute("DELETE FROM t WHERE id = 3", [])?;
            Ok(())
        })
        .unwrap();
    let _ = alice.db.status().unwrap();

    assert_eq!(
        folder_snapshot(&alice),
        before,
        "commits must not write anything into the synced folder"
    );
    // Only `sync()` publishes.
    alice.sync();
    assert_ne!(folder_snapshot(&alice), before);
}

/// 9. Segment files already published are never rewritten, no matter how much
///    more the node commits.
#[test]
fn published_segments_are_never_modified() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.sync();

    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    bob.insert(2, "bob two");
    bob.sync();

    /// Every segment file of a node, by name and content.
    fn snapshot(node: &Node) -> Vec<(String, Vec<u8>)> {
        node.segments()
            .into_iter()
            .map(|name| {
                let bytes = std::fs::read(node.segments_dir().join(&name)).unwrap();
                (name, bytes)
            })
            .collect()
    }

    let alice_before = snapshot(&alice);
    let bob_before = snapshot(&bob);
    assert!(!alice_before.is_empty() && !bob_before.is_empty());

    for i in 3..=6 {
        alice.insert(i, &format!("row {i}"));
        bob.insert(i + 10, &format!("bob {i}"));
    }
    alice.sync();
    bob.sync();
    FakeSync::exchange(&mut [&mut alice, &mut bob]);

    // The files that existed before are still there, byte for byte.
    for (node, before) in [(&alice, &alice_before), (&bob, &bob_before)] {
        let after = snapshot(node);
        for (name, bytes) in before {
            let (_, now) = after
                .iter()
                .find(|(n, _)| n == name)
                .unwrap_or_else(|| panic!("{name} disappeared from {}", node.name));
            assert_eq!(now, bytes, "{name} was modified after being published");
        }
    }
    assert_eq!(alice.dump(), bob.dump());
}

/// 10. Both nodes run the same migration for the same version independently:
///     after the exchange the schema is applied once and the dumps agree.
#[test]
fn concurrent_same_version_migrations_converge() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    let mut bob = Node::join(tmp.path(), "bob", &alice);

    assert_eq!(
        alice.db.migrate(1, MIGRATE_T).unwrap(),
        MigrateOutcome::Applied
    );
    assert_eq!(
        bob.db.migrate(1, MIGRATE_T).unwrap(),
        MigrateOutcome::Applied
    );
    alice.insert(1, "alice");
    bob.insert(2, "bob");

    FakeSync::exchange(&mut [&mut alice, &mut bob]);

    assert_eq!(alice.db.schema_version(), 1);
    assert_eq!(bob.db.schema_version(), 1);
    // The duplicate migration is recognised as already applied, not a mismatch.
    assert_eq!(
        alice.db.migrate(1, MIGRATE_T).unwrap(),
        MigrateOutcome::AlreadyApplied
    );
    assert_eq!(
        bob.db.migrate(1, MIGRATE_T).unwrap(),
        MigrateOutcome::AlreadyApplied
    );
    assert_eq!(alice.dump(), bob.dump());
    assert_eq!(alice.values(), vec!["alice".to_string(), "bob".to_string()]);

    // Different SQL for an already-applied version is still a mismatch, and the
    // comparison sees the frame the *peer* produced.
    let err = alice
        .db
        .migrate(1, "CREATE TABLE t (id INTEGER PRIMARY KEY);");
    assert!(
        matches!(err, Err(Error::MigrationMismatch { version: 1 })),
        "got {err:?}"
    );
}

/// 11. A range missing from the folder is republished; a segment from another
///     database is ignored.
#[test]
fn deleted_segment_is_republished_and_foreign_segments_are_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.sync();
    let names = alice.segments();
    assert_eq!(names.len(), 1);

    // Someone deletes our segment from the folder.
    std::fs::remove_file(alice.segments_dir().join(&names[0])).unwrap();
    let report = alice.sync();
    assert_eq!(report.published, 2, "the missing range is republished");
    assert_eq!(alice.segments().len(), 1);

    // A segment from a different database is ignored, never ingested.
    let mut other = Node::create(tmp.path(), "other-db");
    other.migrate1();
    other.insert(9, "foreign");
    other.sync();
    let foreign = other.segments().into_iter().next().unwrap();
    let bytes = std::fs::read(other.segments_dir().join(&foreign)).unwrap();
    let path = put_segment(&alice, "foreign.seg", &bytes);
    let report = alice.sync();
    assert_eq!(report.ingested, 0);
    assert!(report.ignored_files.contains(&path));
    assert_eq!(alice.values(), vec!["one".to_string()]);
}

/// 12. A gap in a node's chain is reported and left for a later sync.
#[test]
fn gaps_are_reported_and_filled_by_a_later_sync() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.insert(2, "two");
    alice.sync();

    // Hand bob a segment that starts at seq 2 (the real frames 2 and 3, cut out
    // of alice's segment): bob's chain for alice is empty, so he cannot use it.
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    let name = alice.segments().into_iter().next().unwrap();
    let bytes = std::fs::read(alice.segments_dir().join(&name)).unwrap();
    let segment = sharepoint_db::decode_segment(&bytes, &alice.db_id()).unwrap();
    let tail = &segment.frames[1..];
    let cut = sharepoint_db::encode_segment(&alice.db_id(), &alice.node_id(), tail).unwrap();
    let node_hex = name[..16].to_string();
    let first = tail[0].seq;
    let last = tail[tail.len() - 1].seq;
    put_segment(
        &bob,
        &format!("{node_hex}-{first:010}-{last:010}.seg"),
        &cut,
    );
    let report = bob.sync();
    assert_eq!(
        report.ingested, 0,
        "a segment that does not continue the chain is unusable"
    );
    assert!(
        report
            .gaps
            .iter()
            .any(|&(node, seq)| node == alice.node_id() && seq == 1),
        "the missing seq 1 must be reported: {:?}",
        report.gaps
    );
    assert!(bob.values().is_empty());

    // Deliver the real segment: bob catches up.
    FakeSync::deliver(&alice, &bob);
    let report = bob.sync();
    assert_eq!(report.ingested, 3);
    assert_eq!(bob.values(), vec!["one".to_string(), "two".to_string()]);
}

/// 13. `status` reports the local chain position, what is still unpublished, the
///     peers seen so far, and any conflicted copies the sync client left behind.
#[test]
fn status_reports_peers_unpublished_and_conflicted_copies() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");

    let before = alice.db.status().unwrap();
    assert_eq!(before.node_id, alice.node_id());
    assert_eq!(before.own_seq, 2);
    assert_eq!(before.unpublished, 2, "nothing published yet");
    assert!(before.peers.is_empty());
    assert!(before.conflicted_copies.is_empty());

    alice.sync();
    let after = alice.db.status().unwrap();
    assert_eq!(after.unpublished, 0);

    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    let bob_status = bob.db.status().unwrap();
    assert_eq!(bob_status.peers, vec![(alice.node_id(), 2u64)]);

    // A sync client that could not merge a file renames it; we report it and
    // otherwise leave it alone.
    let conflicted = put_segment(&bob, "abc (conflicted copy 2024-01-01).seg", b"junk");
    let status = bob.db.status().unwrap();
    assert_eq!(status.conflicted_copies, vec![conflicted]);
    // It is not read as a segment, so it does not affect the data.
    assert_eq!(bob.values(), vec!["one".to_string()]);
}

/// 14. `export` is a library method: it writes a plain SQLite file holding the
///     user's schema and data and none of the `__sharedb_*` bookkeeping, and an
///     existing file at the target is replaced.
#[test]
fn export_writes_a_plain_sqlite_file_from_the_library() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");

    let out = tmp.path().join("snapshot.db");
    alice.db.export(&out).expect("export");
    let conn = rusqlite::Connection::open(&out).expect("the export is a SQLite file");
    let names: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(names, vec!["t".to_string()], "only the user table remains");
    let rows: Vec<(i64, String)> = conn
        .prepare("SELECT id, v FROM t")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![(1, "one".to_string())]);
    drop(conn);

    // Exporting again over the same path replaces the file.
    alice.insert(2, "two");
    alice.db.export(&out).expect("export over an existing file");
    let conn = rusqlite::Connection::open(&out).unwrap();
    let count: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2, "the second export reflects the newer data");
}
