//! Sync-client hazards: conflicted copies, truncated/partial files, gaps,
//! forks, transient IO errors, deleted files, foreign databases and manifest
//! conflicts.
//!
//! Everything is driven through the real public API plus the [`FolderFs`] seam
//! (a [`HostileFs`] that injects transient failures and counts attempts). All
//! databases live in `tempfile` directories, outside any synced folder.

mod common;

use common::{
    conflicted_copy_name, craft_chain, install_hostile_fs, put_segment, read_segment, seg_name,
    FakeSync, Node, MIGRATE_T,
};
use sharepoint_db::{encode_segment, Error, MaterializeKind};

/// 1. A "conflicted copy" of a segment we already hold, carrying the same frames,
///    is harmless: nothing is re-ingested, the data is unchanged, and the file is
///    reported in `Status::conflicted_copies`.
#[test]
fn conflicted_copy_of_segment_is_harmless_and_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.sync();

    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    assert_eq!(bob.values(), vec!["one".to_string()]);

    // The sync client could not merge alice's segment and left a conflicted copy
    // holding the *same* bytes.
    let real = alice.segments().into_iter().next().unwrap();
    let bytes = read_segment(&alice, &real);
    let conflicted = put_segment(&bob, &conflicted_copy_name(&real), &bytes);

    let report = bob.sync();
    assert_eq!(report.ingested, 0, "the duplicate adds nothing");
    // The duplicate carries frames this store already has: they are counted as
    // `skipped`, never as forks, and nothing here is a fork.
    assert!(report.forks.is_empty(), "a duplicate is not a fork");
    assert!(
        report.skipped > 0,
        "the duplicate frames are counted skipped"
    );
    assert_eq!(bob.values(), vec!["one".to_string()]);

    let status = bob.db.status().unwrap();
    assert_eq!(status.conflicted_copies, vec![conflicted]);
}

/// 2. A conflicted copy that is the ONLY valid copy of a range we lack is
///    ingested: files are identified by content, never by name.
#[test]
fn conflicted_copy_holding_only_valid_bytes_is_ingested() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.sync();

    let real = alice.segments().into_iter().next().unwrap();
    let bytes = read_segment(&alice, &real);

    // Bob joins but is given the segment ONLY under a conflicted-copy name —
    // the canonical name is withheld.
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    put_segment(&bob, &conflicted_copy_name(&real), &bytes);

    let report = bob.sync();
    assert_eq!(report.ingested, 2, "migration + one insert, via the copy");
    assert_eq!(bob.values(), vec!["one".to_string()]);
    assert_eq!(bob.db.schema_version(), 1);
}

/// 3. A truncated segment (a partial upload) is invalid and ignored now; once the
///    complete file arrives it is ingested.
#[test]
fn truncated_segment_ignored_then_ingested_when_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.sync();

    let real = alice.segments().into_iter().next().unwrap();
    let bytes = read_segment(&alice, &real);

    let mut bob = Node::join(tmp.path(), "bob", &alice);
    // Only the first 60% of the file has "uploaded".
    let cut = &bytes[..bytes.len() * 6 / 10];
    let partial = put_segment(&bob, &real, cut);
    let report = bob.sync();
    assert_eq!(report.ingested, 0, "a truncated segment is not usable");
    assert!(report.ignored_files.contains(&partial));
    assert!(bob.values().is_empty());

    // The upload completes: the same name now holds the whole file.
    std::fs::write(bob.segments_dir().join(&real), &bytes).unwrap();
    let report = bob.sync();
    assert_eq!(report.ingested, 2);
    assert_eq!(bob.values(), vec!["one".to_string()]);
}

/// 4. A file that is not a segment at all (garbage bytes under a `.seg` name) is
///    reported as ignored and never fatal.
#[test]
fn garbage_file_is_reported_not_fatal() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.sync();

    let mut bob = Node::join(tmp.path(), "bob", &alice);
    let junk = put_segment(
        &bob,
        "0000000000000000-0000000001-0000000001.seg",
        b"not a segment",
    );
    let report = bob.sync();
    assert!(report.ignored_files.contains(&junk));
    assert_eq!(report.ingested, 0);

    // A real segment still gets through afterwards.
    FakeSync::deliver(&alice, &bob);
    let report = bob.sync();
    assert_eq!(report.ingested, 2);
    assert_eq!(bob.values(), vec!["one".to_string()]);
}

/// 5. A gap in a peer's sequence: the contiguous prefix is still ingested, the
///    rest is held until the gap is filled, and the gap is reported.
#[test]
fn gap_in_peer_sequence_is_held_until_filled() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    for id in 1..=4 {
        alice.insert(id, &format!("row {id}"));
    }
    alice.sync();
    // alice's log: seq 1 = migration, 2..=5 = the four inserts.
    let real = alice.segments().into_iter().next().unwrap();
    let bytes = read_segment(&alice, &real);
    let whole = sharepoint_db::decode_segment(&bytes, &alice.db_id()).unwrap();
    assert_eq!(whole.frames.len(), 5);

    // Hand bob the prefix (seq 1..=2) and the tail (seq 4..=5), withholding seq 3.
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    let prefix = &whole.frames[0..2];
    let tail = &whole.frames[3..5];
    put_segment(
        &bob,
        &seg_name(
            &alice.node_id(),
            prefix[0].seq,
            prefix[prefix.len() - 1].seq,
        ),
        &encode_segment(&alice.db_id(), &alice.node_id(), prefix).unwrap(),
    );
    put_segment(
        &bob,
        &seg_name(&alice.node_id(), tail[0].seq, tail[tail.len() - 1].seq),
        &encode_segment(&alice.db_id(), &alice.node_id(), tail).unwrap(),
    );

    let report = bob.sync();
    assert_eq!(report.ingested, 2, "only the contiguous prefix is usable");
    assert!(
        report
            .gaps
            .iter()
            .any(|&(node, seq)| node == alice.node_id() && seq == 3),
        "the gap at seq 3 must be reported: {:?}",
        report.gaps
    );
    assert_eq!(bob.values(), vec!["row 1".to_string()]);

    // Fill the gap: the held tail becomes usable.
    FakeSync::deliver(&alice, &bob);
    let report = bob.sync();
    assert_eq!(report.ingested, 3, "seq 3, then the held 4 and 5");
    assert!(report.gaps.is_empty());
    assert!(report.forks.is_empty(), "filling a gap is not a fork");
    assert_eq!(
        bob.values(),
        vec![
            "row 1".to_string(),
            "row 2".to_string(),
            "row 3".to_string(),
            "row 4".to_string()
        ]
    );
}

/// 6. A segment of our own that was deleted from the folder is republished on the
///    next sync (the folder is the source of truth for what peers can see).
#[test]
fn deleted_own_segment_is_republished() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.sync();
    let names = alice.segments();
    assert_eq!(names.len(), 1);

    // A sync client (or a careless user) drops our only segment.
    std::fs::remove_file(alice.segments_dir().join(&names[0])).unwrap();
    assert!(alice.segments().is_empty());

    let report = alice.sync();
    assert_eq!(report.published, 2, "the missing range is republished");
    assert!(!report.publish_pending);
    assert_eq!(alice.segments().len(), 1);
    assert_eq!(alice.db.status().unwrap().unpublished, 0);
}

/// 7. A peer segment that vanishes from the folder after we already ingested it
///    loses nothing: the frames are in our store and the data in `data.db`.
#[test]
fn vanished_peer_segment_loses_no_applied_data() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "from alice");
    alice.sync();

    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    assert_eq!(bob.values(), vec!["from alice".to_string()]);

    // Alice's segment disappears from bob's folder.
    for name in bob.segments() {
        std::fs::remove_file(bob.segments_dir().join(&name)).unwrap();
    }
    assert!(bob.segments().is_empty());

    let report = bob.sync();
    assert_eq!(report.ingested, 0);
    assert_eq!(report.materialize, MaterializeKind::UpToDate);
    // Nothing was lost: the data bob already applied is still there.
    assert_eq!(bob.values(), vec!["from alice".to_string()]);
}

/// 8. A transient IO error is retried: the first two write attempts fail with a
///    sharing violation, the third succeeds, and the sync completes with no
///    pending publish. The attempt count proves the retry actually happened.
#[test]
fn transient_io_errors_are_retried() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");

    let hostile = install_hostile_fs(&mut alice);
    // Two doomed attempts, then success — all inside the retry budget.
    hostile.fail_first_writes(2);
    hostile.reset_attempts();

    let report = alice.db.sync().expect("sync survives transient failures");
    assert!(!report.publish_pending, "the write eventually succeeded");
    assert_eq!(report.published, 2);
    assert_eq!(hostile.write_attempts(), 3, "fail, fail, succeed");
    assert_eq!(alice.values(), vec!["one".to_string()]);
}

/// 9. A persistent write failure leaves `publish_pending` set and the local data
///    intact; once the folder recovers, the next sync publishes and a peer
///    converges.
#[test]
fn persistent_io_error_leaves_publish_pending_and_data_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");

    let hostile = install_hostile_fs(&mut alice);
    hostile.always_fail_writes(true);

    let report = alice
        .db
        .sync()
        .expect("sync is Ok even when publishing fails");
    assert!(report.publish_pending, "the folder refused every write");
    assert_eq!(report.published, 0);
    // The data is intact and still unacknowledged-but-safe in the store.
    assert_eq!(alice.values(), vec!["one".to_string()]);
    assert_eq!(alice.db.status().unwrap().unpublished, 2);

    // The folder recovers: the next sync publishes everything.
    hostile.always_fail_writes(false);
    let report = alice.db.sync().expect("sync after recovery");
    assert!(!report.publish_pending);
    assert_eq!(report.published, 2);
    assert_eq!(alice.db.status().unwrap().unpublished, 0);

    // A peer that joins now converges on the published data.
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    assert_eq!(bob.values(), vec!["one".to_string()]);
}

/// 10. A second, valid hash-chained chain for a node we already know, carrying
///     different changesets at the same sequences, is a fork: it is never
///     ingested, every conflicting position is reported, and the original data is
///     untouched.
#[test]
fn forked_node_chain_is_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "real one");
    alice.insert(2, "real two");
    alice.sync();
    let real = alice.segments().into_iter().next().unwrap();
    let whole =
        sharepoint_db::decode_segment(&read_segment(&alice, &real), &alice.db_id()).unwrap();
    let n = whole.frames.len();

    // Bob ingests alice's real chain.
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    assert_eq!(
        bob.values(),
        vec!["real one".to_string(), "real two".to_string()]
    );

    // A fork: a fresh chain from seq 1, same node id, different payloads. The
    // frames chain and hash correctly, so the segment decodes — but every frame
    // disagrees with what bob already stored at that position. The sync client
    // could not merge it and left it NEXT TO the real segment under a
    // conflicted-copy name; the real segment stays in place.
    let forged: Vec<Vec<u8>> = (0..n)
        .map(|i| format!("forged payload {i}").into_bytes())
        .collect();
    let fork_frames = craft_chain(&alice.db_id(), &alice.node_id(), &forged);
    let real_name = seg_name(&alice.node_id(), 1, n as u64);
    put_segment(
        &bob,
        &conflicted_copy_name(&real_name),
        &encode_segment(&alice.db_id(), &alice.node_id(), &fork_frames).unwrap(),
    );
    assert!(
        bob.segments().contains(&real_name),
        "the real segment is untouched"
    );

    let report = bob.sync();
    assert_eq!(report.ingested, 0, "a fork is never ingested");
    // Every forged position is reported, in `seq` order, and nothing else.
    let expected: Vec<(sharepoint_db::NodeId, u64)> =
        (1..=n as u64).map(|seq| (alice.node_id(), seq)).collect();
    assert_eq!(report.forks, expected, "the fork must be reported");
    // The original data is untouched.
    assert_eq!(
        bob.values(),
        vec!["real one".to_string(), "real two".to_string()]
    );
}

/// 11. A segment produced for a different database (its `db_id` does not match)
///     is ignored, never ingested.
#[test]
fn foreign_db_id_segment_is_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.sync();

    // A completely separate database with its own identity.
    let mut other = Node::create(tmp.path(), "other-db");
    other.migrate1();
    other.insert(99, "foreign");
    other.sync();
    let foreign_name = other.segments().into_iter().next().unwrap();
    let foreign_bytes = read_segment(&other, &foreign_name);

    let mut bob = Node::join(tmp.path(), "bob", &alice);
    let path = put_segment(&bob, &foreign_name, &foreign_bytes);
    let report = bob.sync();
    assert_eq!(report.ingested, 0);
    assert!(report.ignored_files.contains(&path));
    assert!(bob.values().is_empty());
}

/// 12. A `MANIFEST` whose `db_id` disagrees with the local frame store is a hard
///     `Error::Manifest`: the store's frames are hashed over the original id, so
///     a swapped manifest under a state directory must be caught, not replayed.
#[test]
fn manifest_conflict_with_different_db_id_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = common::Fixture::new(tmp.path(), "db");
    {
        let mut db = fx.open();
        db.migrate(1, MIGRATE_T).unwrap();
        db.execute("INSERT INTO t (id, v) VALUES (1, 'kept')", [])
            .unwrap();
    }

    // A different database, with its own manifest and db_id.
    let other = common::Fixture::new(tmp.path(), "other");
    other.open();

    // Swap the manifest under the original state directory.
    std::fs::copy(other.folder.join("MANIFEST"), fx.folder.join("MANIFEST")).unwrap();

    let err = fx.try_reopen().unwrap_err();
    assert!(
        matches!(err, Error::Manifest(_)),
        "a swapped manifest must be a manifest error, got {err}"
    );
}

/// 13. A leftover `*.tmp` file (a publish that crashed before its rename) is
///     ignored: it is not read as a segment and does not affect the data.
#[test]
fn leftover_temp_file_is_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "alice");
    alice.migrate1();
    alice.insert(1, "one");
    alice.sync();

    let mut bob = Node::join(tmp.path(), "bob", &alice);
    FakeSync::deliver(&alice, &bob);
    // A half-finished publish left a temp file behind.
    let leftover = put_segment(
        &bob,
        "0000000000000000-0000000001-0000000001.seg.tmp",
        b"partial",
    );
    let report = bob.sync();
    assert_eq!(report.ingested, 2, "the real segment still ingests");
    assert!(
        !report.ignored_files.contains(&leftover),
        "a .tmp file is not even considered a segment"
    );
    assert_eq!(bob.values(), vec!["one".to_string()]);
}

/// Fix A: a normal constraint error that SQLite rolls back for us (`INSERT OR
/// ROLLBACK` on a UNIQUE violation) returns an error but must NOT permanently
/// disable the handle — the user's write was rolled back with the transaction, so
/// `data.db` still agrees with the store.
#[test]
fn constraint_error_keeps_handle_usable() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = common::Fixture::new(tmp.path(), "db");
    let mut db = fx.open();
    db.migrate(1, MIGRATE_T).unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'kept')", [])
        .unwrap();

    // A UNIQUE/PK violation with OR ROLLBACK ends the transaction for us.
    let err = db
        .execute("INSERT OR ROLLBACK INTO t (id, v) VALUES (1, 'dup')", [])
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("unique"),
        "expected a constraint error, got {err}"
    );

    // The handle is still usable: a later write succeeds and is durable.
    db.execute("INSERT INTO t (id, v) VALUES (2, 'after')", [])
        .unwrap();
    assert_eq!(
        db.query("SELECT v FROM t ORDER BY id", [], |r| Ok(
            r.get::<_, String>(0)?
        ))
        .unwrap(),
        vec!["kept".to_string(), "after".to_string()]
    );
    drop(db);

    // And it survived a reopen with the same data (the dup never landed).
    let db = fx.reopen();
    assert_eq!(
        db.query("SELECT v FROM t ORDER BY id", [], |r| Ok(
            r.get::<_, String>(0)?
        ))
        .unwrap(),
        vec!["kept".to_string(), "after".to_string()]
    );
}
