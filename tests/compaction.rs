//! Compaction: snapshot checkpoints, prune, the retention window and
//! bootstrap-from-checkpoint.
//!
//! These tests drive the public [`ShareDb`] surface (open / write / sync) with a
//! small `checkpoint_threshold` so a checkpoint is written during the test, then
//! assert on the contents of the synced folder and on convergence after a node
//! loses its local state.

mod common;

use std::path::Path;

use common::{FakeSync, Node};
use sharepoint_db::OpenOptions;

/// A node with an explicit checkpoint threshold and retention window.
fn node_with(
    root: &Path,
    name: &str,
    checkpoint_threshold: usize,
    retention_segments: usize,
) -> Node {
    let folder = root.join(format!("{name}.sharedb"));
    let state = root.join(format!("{name}.state"));
    std::fs::create_dir_all(&folder).unwrap();
    let db = sharepoint_db::ShareDb::open(
        &folder,
        OpenOptions {
            create: true,
            state_dir: Some(state.clone()),
            checkpoint_threshold,
            retention_segments,
            ..Default::default()
        },
    )
    .unwrap();
    Node {
        name: name.to_string(),
        folder,
        state,
        db,
    }
}

/// The `ckpoint/` file names in a node's folder, sorted.
fn ckpoints(node: &Node) -> Vec<String> {
    let dir = node.folder.join("ckpoint");
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// Copies the `ckpoint/` directory from one node's folder to another's, the way
/// a real sync client would (the fake sync client only moves `.seg` files).
/// Files already present at the destination are left alone.
fn deliver_checkpoints(from: &Node, to: &Node) {
    let src = from.folder.join("ckpoint");
    let dst = to.folder.join("ckpoint");
    let Ok(entries) = std::fs::read_dir(&src) else {
        return;
    };
    std::fs::create_dir_all(&dst).unwrap();
    for entry in entries.flatten() {
        let target = dst.join(entry.file_name());
        if !target.exists() {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Copies a whole directory tree (files and subdirectories) to `dst`.
fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap().flatten() {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_tree(&from, &to);
        } else {
            std::fs::copy(from, to).unwrap();
        }
    }
}

/// 1. With a threshold of 1, every sync that adds a frame past the last cut
///    writes a checkpoint, the `ckpoint/` folder stays bounded at
///    [`CHECKPOINTS_KEPT`] files (the newest cut and the one before it), and the
///    newest checkpoint's snapshot round-trips: it decodes and re-encodes to the
///    same bytes.
#[test]
fn checkpoint_is_written_and_round_trips() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 0);
    alice.migrate1();
    for i in 1..=5 {
        alice.insert(i, &format!("v{i}"));
        alice.sync();
    }
    let names = ckpoints(&alice);
    // The folder is bounded: compaction keeps the newest cut and the one before
    // it, so a node one cut behind still finds a usable checkpoint.
    assert_eq!(
        names.len(),
        sharepoint_db::checkpoint::CHECKPOINTS_KEPT,
        "the ckpoint/ folder must stay bounded at CHECKPOINTS_KEPT, got {names:?}"
    );
    // Every checkpoint file decodes and re-encodes byte-for-byte.
    let db_id = alice.db_id();
    for name in &names {
        let bytes = std::fs::read(alice.folder.join("ckpoint").join(name)).unwrap();
        let cp = sharepoint_db::checkpoint::decode(&bytes, &db_id).expect("decode");
        assert_eq!(cp.file_name(), *name, "name must match content");
        assert_eq!(cp.encode(&db_id), bytes, "round-trip must be byte-exact");
    }
}

/// 2. Prune deletes the segments the newest checkpoint made redundant while
///    keeping the retention window. With `retention_segments = 1` and a
///    threshold of 1, after enough activity only the newest segment survives.
#[test]
fn prune_deletes_old_segments_keeps_retention_window() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 1);
    alice.migrate1();
    // Each insert+sync publishes a fresh single-frame segment and then prunes
    // everything below the new cut except the newest segment.
    for i in 1..=6 {
        alice.insert(i, &format!("v{i}"));
        alice.sync();
    }
    let segs = alice.segments();
    // The retention window is one segment, so at most one below-boundary
    // segment may remain; the newest segment is always kept.
    assert!(
        segs.len() <= 2,
        "prune should leave at most the retention window, got {segs:?}"
    );
    // The data is still fully correct after pruning.
    assert_eq!(alice.values(), vec!["v1", "v2", "v3", "v4", "v5", "v6"]);
}

/// 3. A brand-new node whose store is empty bootstraps from a checkpoint: it
///    never saw the pruned segments, yet it converges to the same state as the
///    node that compacted.
///
///    A new node bootstraps at *open* time (its store is behind the checkpoint),
///    so the whole folder — `MANIFEST`, `ckpoint/` and the retained segments —
///    must be in place before the database is opened, exactly as a sync client
///    would deliver it before the application opens it.
#[test]
fn new_node_bootstraps_from_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 1);
    alice.migrate1();
    for i in 1..=6 {
        alice.insert(i, &format!("v{i}"));
        alice.sync();
    }

    // Bob's folder is a full copy of alice's as it stands now: a checkpoint plus
    // only the retained segment(s). Its state directory is empty, so its store
    // is behind the checkpoint and it must bootstrap from the snapshot.
    let bob_folder = tmp.path().join("bob.sharedb");
    let bob_state = tmp.path().join("bob.state");
    copy_tree(&alice.folder, &bob_folder);
    let bob = sharepoint_db::ShareDb::open(
        &bob_folder,
        OpenOptions {
            create: false,
            state_dir: Some(bob_state.clone()),
            checkpoint_threshold: 1,
            retention_segments: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut bob = Node {
        name: "bob".to_string(),
        folder: bob_folder,
        state: bob_state,
        db: bob,
    };
    bob.sync();

    assert_eq!(bob.values(), vec!["v1", "v2", "v3", "v4", "v5", "v6"]);
    assert_eq!(alice.dump(), bob.dump());
}

/// 4. After prune deletes this node's own segments, a later sync must not
///    republish them (the publish floor). The folder must not grow back.
#[test]
fn pruned_segments_are_not_republished() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 1);
    alice.migrate1();
    for i in 1..=6 {
        alice.insert(i, &format!("v{i}"));
        alice.sync();
    }
    let before = alice.segments();
    // Another sync with nothing new: publish must not resurrect the pruned
    // ranges, so the segment set is unchanged.
    alice.sync();
    alice.sync();
    let after = alice.segments();
    assert_eq!(
        before, after,
        "a no-op sync must not republish pruned segments"
    );
}

/// 5. A node that loses its local state entirely (state dir wiped) rebuilds from
///    the folder: if a checkpoint is present and it is behind, it bootstraps;
///    either way it converges to the compacted state.
#[test]
fn node_that_lost_state_rebuilds_and_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 2);
    alice.migrate1();
    for i in 1..=8 {
        alice.insert(i, &format!("v{i}"));
        alice.sync();
    }
    let expected = alice.values();
    // Wipe the local state: the store and data are gone, only the folder remains.
    let alice = alice.reopen_fresh();
    assert_eq!(alice.values(), expected);
}

/// 6. Two nodes converge when one of them has compacted and pruned: the
///    non-compacting node still ends up with the full merged state, and the
///    compacting node agrees.
#[test]
fn compaction_preserves_convergence() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 1);
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    alice.migrate1();
    alice.sync();
    FakeSync::deliver(&alice, &bob);
    bob.sync();

    for i in 1..=6 {
        alice.insert(i, &format!("a{i}"));
        bob.insert(i + 100, &format!("b{i}"));
        alice.sync();
        bob.sync();
        FakeSync::deliver(&alice, &bob);
        FakeSync::deliver(&bob, &alice);
        deliver_checkpoints(&alice, &bob);
        deliver_checkpoints(&bob, &alice);
        alice.sync();
        bob.sync();
    }
    assert_eq!(alice.dump(), bob.dump());
    // Both see all twelve rows.
    assert_eq!(alice.values().len(), 12);
    assert_eq!(bob.values().len(), 12);
}

// ---------------------------------------------------------------------------
// Helpers shared by the end-to-end scenarios below.
// ---------------------------------------------------------------------------

/// The `ckpoint/` files of `node` as `(name, decoded checkpoint)`, skipping the
/// ones that do not decode (a corrupted file is exactly what some tests plant).
fn read_ckpoints(node: &Node) -> Vec<(String, sharepoint_db::checkpoint::Checkpoint)> {
    let db_id = node.db_id();
    let mut out = Vec::new();
    for name in ckpoints(node) {
        let bytes = std::fs::read(node.folder.join("ckpoint").join(&name)).unwrap();
        if let Ok(cp) = sharepoint_db::checkpoint::decode(&bytes, &db_id) {
            out.push((name, cp));
        }
    }
    out
}

/// The best checkpoint a node would pick from its own folder, recomputed here
/// with the crate's public rules (highest `count`, ties to the smallest name,
/// a checkpoint whose `cut_digest` disagrees with this store's prefix ignored).
///
/// Returns `(name, count)`; `None` when nothing in the folder is usable.
fn best_named(node: &Node) -> Option<(String, sharepoint_db::checkpoint::Checkpoint)> {
    use sharepoint_db::store;

    let db_id = node.db_id();
    let store_conn = store::open(&node.state.join("store.db")).unwrap();
    let frames = store::all_frames(&store_conn, &db_id).unwrap();
    drop(store_conn);
    let total = frames.len() as u64;

    let mut best: Option<(String, sharepoint_db::checkpoint::Checkpoint)> = None;
    for (name, cp) in read_ckpoints(node) {
        // `checkpoint::validate`: a store shorter than `count` trusts the file.
        if cp.count <= total && cp.cut_digest != store::digest_first(&frames, cp.count as usize) {
            continue;
        }
        let better = match &best {
            None => true,
            Some((best_name, best_cp)) => {
                cp.count > best_cp.count || (cp.count == best_cp.count && name < *best_name)
            }
        };
        if better {
            best = Some((name, cp));
        }
    }
    best
}

/// [`best_named`] reduced to `(name, count)`, so tests can compare selections.
fn best_checkpoint(node: &Node) -> Option<(String, u64)> {
    best_named(node).map(|(name, cp)| (name, cp.count))
}

/// Writes `bytes` into `node`'s `ckpoint/` directory under `name`.
fn put_checkpoint(node: &Node, name: &str, bytes: &[u8]) {
    let dir = node.folder.join("ckpoint");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), bytes).unwrap();
}

/// Flips one byte in the middle of `bytes`, far from the trailing hash, so the
/// file's own hash check fails.
fn corrupt(mut bytes: Vec<u8>) -> Vec<u8> {
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xff;
    bytes
}

/// The name a sync client gives a `.cpt` file it cannot merge.
fn conflicted_cpt_name(name: &str) -> String {
    let stem = name.strip_suffix(".cpt").unwrap_or(name);
    format!("{stem} (conflicted copy 2024-01-01 10-11-12).cpt")
}

/// Deletes `data.db` from a node's state directory, so the next open has to
/// bring the data file in line from scratch (checkpoint or full replay).
fn drop_data_db(node: &Node) {
    std::fs::remove_file(node.state.join("data.db")).unwrap();
}

/// A second writer for an existing database, with explicit compaction options.
fn join_with(
    root: &Path,
    name: &str,
    seed: &Node,
    checkpoint_threshold: usize,
    retention_segments: usize,
) -> Node {
    let folder = root.join(format!("{name}.sharedb"));
    let state = root.join(format!("{name}.state"));
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::copy(seed.folder.join("MANIFEST"), folder.join("MANIFEST")).unwrap();
    let db = sharepoint_db::ShareDb::open(
        &folder,
        OpenOptions {
            create: true,
            state_dir: Some(state.clone()),
            checkpoint_threshold,
            retention_segments,
            ..Default::default()
        },
    )
    .unwrap();
    Node {
        name: name.to_string(),
        folder,
        state,
        db,
    }
}

/// How many frames a node's local store holds.
fn store_len(node: &Node) -> usize {
    let store = sharepoint_db::store::open(&node.state.join("store.db")).unwrap();
    let n = sharepoint_db::store::all_frames(&store, &node.db_id())
        .unwrap()
        .len();
    drop(store);
    n
}

/// Every frame a node's local store holds, in total order.
fn store_frames(node: &Node) -> Vec<sharepoint_db::store::StoredFrame> {
    let store = sharepoint_db::store::open(&node.state.join("store.db")).unwrap();
    let frames = sharepoint_db::store::all_frames(&store, &node.db_id()).unwrap();
    drop(store);
    frames
}

/// Reopens `node` on the same folder and state directory.
fn reopen(node: Node) -> Node {
    let Node {
        name,
        folder,
        state,
        db,
    } = node;
    drop(db);
    let db = sharepoint_db::ShareDb::open(
        &folder,
        OpenOptions {
            create: false,
            state_dir: Some(state.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    Node {
        name,
        folder,
        state,
        db,
    }
}

// ---------------------------------------------------------------------------
// Mandated end-to-end scenarios.
// ---------------------------------------------------------------------------

/// 7. A node that stops syncing, then returns while it is still inside the
///    retention window, converges exactly: it keeps its own `store.db`, so it
///    never lost anything — it only missed the frames the folder still carries.
///
///    Alice and Bob advance well past `checkpoint_threshold`, so checkpoints are
///    written and old segments are pruned while Carol is away. Carol's own
///    below-boundary segments are gone from the folder, but Carol still holds
///    them locally, and everything Carol is missing is either above the cut
///    boundary or inside the retention window, so a plain deliver + sync brings
///    her all the way up.
#[test]
fn slow_node_reconnects_within_retention_and_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 8);
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    let mut carol = Node::join(tmp.path(), "carol", &alice);

    // Everyone starts together: the migration and one row each.
    alice.migrate1();
    alice.sync();
    for peer in [&mut bob, &mut carol] {
        FakeSync::deliver(&alice, peer);
        peer.sync();
    }
    for (node, v) in [(&mut alice, "a0"), (&mut bob, "b0"), (&mut carol, "c0")] {
        node.insert(100 + v.as_bytes()[0] as i64, v);
        node.sync();
    }
    FakeSync::deliver(&alice, &bob);
    FakeSync::deliver(&alice, &carol);
    FakeSync::deliver(&bob, &alice);
    FakeSync::deliver(&bob, &carol);
    FakeSync::deliver(&carol, &alice);
    FakeSync::deliver(&carol, &bob);
    for node in [&mut alice, &mut bob, &mut carol] {
        node.sync();
    }

    // Carol goes dark. Alice and Bob keep working: eight fresh segments' worth of
    // cluster activity, with checkpoints written and prunes run along the way.
    let mut carol_segments_before = carol.segments();
    carol_segments_before.sort();
    for i in 1..=4 {
        alice.insert(i, &format!("a{i}"));
        bob.insert(i + 10, &format!("b{i}"));
        alice.sync();
        bob.sync();
        FakeSync::deliver(&alice, &bob);
        FakeSync::deliver(&bob, &alice);
        deliver_checkpoints(&alice, &bob);
        deliver_checkpoints(&bob, &alice);
        alice.sync();
        bob.sync();
    }
    // Compaction really happened while Carol was away.
    assert!(
        !ckpoints(&alice).is_empty(),
        "alice should have written checkpoints while carol was away"
    );
    // Carol's own early segments were pruned out of the folder: they sit below
    // the cut boundary and outside the retention window, so catching her up
    // depends on her own store, not on the folder.
    let alice_segments = alice.segments();
    assert!(
        carol_segments_before
            .iter()
            .any(|name| !alice_segments.contains(name)),
        "carol's pruned segments should be gone from the folder: {:?} vs {alice_segments:?}",
        carol_segments_before
    );

    // Carol reconnects: the sync client delivers what the folder still holds,
    // checkpoints included, and Carol syncs.
    FakeSync::deliver(&alice, &carol);
    FakeSync::deliver(&bob, &carol);
    deliver_checkpoints(&alice, &carol);
    deliver_checkpoints(&bob, &carol);
    carol.sync();

    // One more full exchange, then all three agree byte-for-byte.
    for _ in 0..2 {
        for (from, to) in [
            (&alice, &bob),
            (&alice, &carol),
            (&bob, &alice),
            (&bob, &carol),
            (&carol, &alice),
            (&carol, &bob),
        ] {
            FakeSync::deliver(from, to);
            deliver_checkpoints(from, to);
        }
        for node in [&mut alice, &mut bob, &mut carol] {
            node.sync();
        }
    }
    assert_eq!(alice.dump(), bob.dump());
    assert_eq!(alice.dump(), carol.dump());
    // Carol kept her own row and gained all of theirs.
    assert_eq!(alice.values().len(), 11);
    assert_eq!(carol.values().len(), 11);
}

/// 8. A brand-new node bootstraps from the checkpoint *alone*: the folder holds
///    a checkpoint and only the retained segments — every older segment file has
///    been deleted — so the frames the checkpoint folded in exist nowhere but in
///    the snapshot. The new node's state directory is empty.
#[test]
fn new_node_bootstraps_from_checkpoint_only() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 1);
    alice.migrate1();
    for i in 1..=6 {
        alice.insert(i, &format!("v{i}"));
        alice.sync();
    }
    let checkpoints = ckpoints(&alice);
    assert!(!checkpoints.is_empty(), "alice must have a checkpoint");
    // Make the folder checkpoint-only: delete every segment file, so nothing
    // below the cut boundary survives anywhere but in the snapshot.
    let segments_dir = alice.segments_dir();
    let mut deleted = 0;
    for name in alice.segments() {
        std::fs::remove_file(segments_dir.join(&name)).unwrap();
        deleted += 1;
    }
    assert!(deleted > 0, "there must have been segments to delete");
    assert!(
        alice.segments().is_empty(),
        "the folder is now checkpoint-only"
    );

    // Bob gets a copy of that folder and an empty state directory.
    let bob_folder = tmp.path().join("bob.sharedb");
    let bob_state = tmp.path().join("bob.state");
    copy_tree(&alice.folder, &bob_folder);
    let bob_db = sharepoint_db::ShareDb::open(
        &bob_folder,
        OpenOptions {
            create: false,
            state_dir: Some(bob_state.clone()),
            checkpoint_threshold: 1,
            retention_segments: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut bob = Node {
        name: "bob".to_string(),
        folder: bob_folder,
        state: bob_state,
        db: bob_db,
    };
    // Nothing to deliver: the folder copy is already complete.
    bob.sync();

    assert_eq!(bob.values(), vec!["v1", "v2", "v3", "v4", "v5", "v6"]);
    assert_eq!(alice.dump(), bob.dump());
    // Bob really bootstrapped: his store holds no frames at all, so the state can
    // only have come from the snapshot.
    assert!(bob.segments().is_empty(), "bob published nothing new");
}

/// 9. A sync client that cannot merge a checkpoint file leaves a second copy
///    under a `(conflicted copy …).cpt` name. The duplicate must be harmless:
///    the folder still converges, and the best-checkpoint selection is unchanged
///    (the copy carries the same content, so the same `count` and a name that
///    sorts after the original).
#[test]
fn checkpoint_conflicted_copy_is_harmless() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 1);
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    alice.migrate1();
    alice.sync();
    FakeSync::deliver(&alice, &bob);
    bob.sync();

    for i in 1..=4 {
        alice.insert(i, &format!("a{i}"));
        alice.sync();
        FakeSync::deliver(&alice, &bob);
        deliver_checkpoints(&alice, &bob);
        bob.sync();
    }
    let names = ckpoints(&bob);
    assert!(!names.is_empty(), "bob should have received checkpoints");
    let best_before = best_named(&bob).expect("a checkpoint");

    // Duplicate every checkpoint under a conflicted-copy name.
    let dir = bob.folder.join("ckpoint");
    for name in &names {
        let bytes = std::fs::read(dir.join(name)).unwrap();
        let copy = conflicted_cpt_name(name);
        std::fs::write(dir.join(&copy), bytes).unwrap();
    }
    assert_eq!(ckpoints(&bob).len(), names.len() * 2);

    // Selection is unaffected. The copy's name sorts before the original (a
    // space sorts below the `.` of `.cpt`), so the winner may be the copy — what
    // must not change is *which checkpoint* wins: same count, same boundary and
    // same cut digest, because the copy carries byte-identical content.
    let best_after = best_named(&bob).expect("a checkpoint");
    let ckpoint = bob.folder.join("ckpoint");
    let bytes_of = |name: &str| std::fs::read(ckpoint.join(name)).unwrap();
    assert_eq!(
        best_after.1.count, best_before.1.count,
        "a conflicted copy must not change which checkpoint wins"
    );
    assert_eq!(
        best_after.1.boundary(),
        best_before.1.boundary(),
        "a conflicted copy must not change the winning cut"
    );
    assert_eq!(
        best_after.1.cut_digest, best_before.1.cut_digest,
        "a conflicted copy must not change the winning cut digest"
    );
    assert_eq!(
        bytes_of(&best_after.0),
        bytes_of(&best_before.0),
        "the winning file must carry the same bytes as before"
    );

    // And syncing with the duplicates in place still converges.
    bob.sync();
    alice.sync();
    FakeSync::deliver(&bob, &alice);
    deliver_checkpoints(&bob, &alice);
    alice.sync();
    assert_eq!(alice.dump(), bob.dump());
    assert_eq!(alice.values(), vec!["a1", "a2", "a3", "a4"]);
}

/// 10. A checkpoint whose bytes are corrupted (its own hash no longer matches)
///     is ignored end-to-end: a node that still holds the full history falls
///     back to replaying its whole store and converges, and a node that has to
///     rebuild from the folder does not fail — it simply gets what the retained
///     segments carry.
#[test]
fn corrupted_checkpoint_is_ignored_and_full_history_used() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 4);
    let mut bob = Node::join(tmp.path(), "bob", &alice);
    alice.migrate1();
    alice.sync();
    FakeSync::deliver(&alice, &bob);
    bob.sync();

    for i in 1..=6 {
        alice.insert(i, &format!("v{i}"));
        alice.sync();
        FakeSync::deliver(&alice, &bob);
        deliver_checkpoints(&alice, &bob);
        bob.sync();
    }
    let expected = alice.values();
    assert_eq!(expected, vec!["v1", "v2", "v3", "v4", "v5", "v6"]);

    // Corrupt every checkpoint in bob's folder: the file stays a plausible size
    // but its trailing hash no longer matches.
    let dir = bob.folder.join("ckpoint");
    let names = ckpoints(&bob);
    assert!(!names.is_empty(), "bob should hold checkpoints");
    for name in &names {
        let path = dir.join(name);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, corrupt(bytes)).unwrap();
    }
    // Nothing in the folder decodes any more.
    assert!(
        read_ckpoints(&bob).is_empty(),
        "all checkpoints are corrupt"
    );
    // ...and bob's own store still carries the full history, so a rebuild from
    // the store alone must produce the right state.
    drop_data_db(&bob);
    let mut bob = reopen(bob);
    bob.sync();
    assert_eq!(bob.values(), expected);
    assert_eq!(alice.dump(), bob.dump());
    // The corrupt files are still there: ignoring them is not the same as
    // deleting them, and a later sync can still deliver the good ones.
    assert_eq!(ckpoints(&bob), names);
}

/// 11. Two nodes race to write a checkpoint: both cross `checkpoint_threshold`
///     while neither has seen the other's checkpoint file, so each writes one of
///     its own — different cut, different content, different name. Once the
///     checkpoints are exchanged both converge, and both pick the *same* best
///     checkpoint: selection is deterministic, which is what keeps their prune
///     decisions and publish floors in agreement.
#[test]
fn two_nodes_race_to_write_same_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    // A generous retention window: this test is about the race, not about the
    // retention limit, and nothing may be pruned out from under either node.
    let mut alice = node_with(tmp.path(), "alice", 1, 32);
    let mut bob = join_with(tmp.path(), "bob", &alice, 1, 32);
    alice.migrate1();
    alice.sync();
    FakeSync::deliver(&alice, &bob);
    bob.sync();

    // Both write and fully exchange, so they share a history.
    for i in 1..=2 {
        alice.insert(i, &format!("a{i}"));
        alice.sync();
        bob.insert(i + 10, &format!("b{i}"));
        bob.sync();
        FakeSync::deliver(&alice, &bob);
        FakeSync::deliver(&bob, &alice);
        alice.sync();
        bob.sync();
    }

    // The race: alice publishes a third row and syncs, so she crosses the
    // threshold over her own store. Bob picks up alice's row (segments only —
    // no checkpoint is exchanged), writes his own third row and syncs, crossing
    // the threshold over a *longer* store. Neither has seen the other's file.
    alice.insert(3, "a3");
    alice.sync();
    FakeSync::deliver(&alice, &bob);
    bob.sync();
    bob.insert(13, "b3");
    bob.sync();
    alice.sync();

    let alice_cps = read_ckpoints(&alice);
    let bob_cps = read_ckpoints(&bob);
    assert!(
        !alice_cps.is_empty() && !bob_cps.is_empty(),
        "both nodes must have crossed the threshold: {:?} {:?}",
        alice_cps.len(),
        bob_cps.len()
    );
    let alice_names: Vec<&str> = alice_cps.iter().map(|(n, _)| n.as_str()).collect();
    let bob_names: Vec<&str> = bob_cps.iter().map(|(n, _)| n.as_str()).collect();
    assert_ne!(
        alice_names, bob_names,
        "the two races must have written different checkpoints"
    );

    // Exchange everything, twice, so a checkpoint published during the exchange
    // still reaches the other side.
    for _ in 0..2 {
        FakeSync::deliver(&alice, &bob);
        FakeSync::deliver(&bob, &alice);
        deliver_checkpoints(&alice, &bob);
        deliver_checkpoints(&bob, &alice);
        alice.sync();
        bob.sync();
    }

    // Both converge to the same six rows (three each).
    assert_eq!(alice.dump(), bob.dump());
    assert_eq!(alice.values().len(), 6);
    assert_eq!(bob.values().len(), 6);

    // Deterministic selection: seeing the same files, both nodes agree on the
    // best checkpoint, name included.
    let best_a = best_checkpoint(&alice).expect("a checkpoint");
    let best_b = best_checkpoint(&bob).expect("a checkpoint");
    assert_eq!(
        best_a, best_b,
        "both nodes must pick the same best checkpoint"
    );
    // The winner is the deeper of the two cuts, and it verifies against the
    // merged history both nodes now hold.
    let deepest_race = alice_cps
        .iter()
        .chain(&bob_cps)
        .map(|(_, cp)| cp.count)
        .max()
        .expect("a checkpoint");
    assert_eq!(
        best_a.1, deepest_race,
        "the highest cut must win: {best_a:?} vs {deepest_race}"
    );
    // And that checkpoint really is one of the two races' files, present in both
    // folders now.
    assert!(ckpoints(&alice).contains(&best_a.0) && ckpoints(&bob).contains(&best_a.0));
}

/// 12. A checkpoint whose `cut_digest` disagrees with this store's prefix belongs
///     to a different history (a fork) and must be ignored end-to-end, even
///     though its own hash is valid and it would win the selection on `count`.
///     The observable effect is prune: a checkpoint drives which segments a sync
///     deletes and how far the publish floor moves, so an ignored checkpoint must
///     leave the folder alone and the node must fall back to its own full history.
#[test]
fn checkpoint_cut_digest_mismatch_is_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    // A threshold nothing reaches: compaction never writes a checkpoint here, so
    // the only checkpoint in the folder is the one this test plants.
    let mut alice = node_with(tmp.path(), "alice", 1_000, 0);
    alice.migrate1();
    for i in 1..=6 {
        alice.insert(i, &format!("v{i}"));
        alice.sync();
    }
    let expected = alice.values();
    assert_eq!(expected, vec!["v1", "v2", "v3", "v4", "v5", "v6"]);
    let segments_before = alice.segments();
    assert!(segments_before.len() >= 6, "one segment per commit");
    assert!(ckpoints(&alice).is_empty(), "the threshold is unreachable");

    // A foreign database image: a valid SQLite file that is *not* this history.
    let other_folder = tmp.path().join("other.sharedb");
    let other_state = tmp.path().join("other.state");
    std::fs::create_dir_all(&other_folder).unwrap();
    {
        let mut other = sharepoint_db::ShareDb::open(
            &other_folder,
            OpenOptions {
                create: true,
                state_dir: Some(other_state.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        other.migrate(1, common::MIGRATE_T).unwrap();
        other
            .execute(
                "INSERT INTO t (id, v) VALUES (?1, ?2)",
                sharepoint_db::rusqlite::params![1, "FORK"],
            )
            .unwrap();
    }
    let foreign_snapshot = std::fs::read(other_state.join("data.db")).unwrap();

    // The forged checkpoint: it cuts at alice's whole store, so its `count` is
    // the largest possible and it would win the selection, and its boundary is
    // above every frame, so a prune driven by it would delete the whole folder.
    // Only the cut digest gives it away.
    let total = store_len(&alice) as u64;
    let bogus = sharepoint_db::checkpoint::Checkpoint {
        count: total,
        cut_lamport: 1,
        boundary: (u64::MAX, [0xff; 8], u64::MAX),
        cut_digest: [0xab; 32],
        vv: sharepoint_db::checkpoint::VersionVector::build(&store_frames(&alice), total as usize),
        snapshot: foreign_snapshot,
    };
    let bogus_name = bogus.file_name();
    put_checkpoint(&alice, &bogus_name, &bogus.encode(&alice.db_id()));
    assert!(ckpoints(&alice).contains(&bogus_name));
    // Judged against alice's own store, the checkpoint is not a candidate at all.
    assert_eq!(
        best_checkpoint(&alice),
        None,
        "a checkpoint with a foreign cut digest must be ignored"
    );

    // End to end: a real sync must not let it drive prune.
    alice.sync();
    assert_eq!(
        alice.segments(),
        segments_before,
        "an ignored checkpoint must not prune anything"
    );
    // And the data is still the full history, not the foreign snapshot.
    drop_data_db(&alice);
    let mut alice = reopen(alice);
    alice.sync();
    assert_eq!(alice.values(), expected);
    assert_eq!(alice.segments(), segments_before);
}

/// 13. Both knobs are observable: with `checkpoint_threshold = 1` and
///     `retention_segments = 0` a checkpoint appears on the first qualifying sync
///     and the prune is as aggressive as coverage allows; with a threshold
///     nothing reaches, no checkpoint is written and nothing is pruned.
#[test]
fn checkpoint_threshold_and_retention_are_configurable() {
    let tmp = tempfile::tempdir().unwrap();

    // Aggressive: threshold 1, retention 0.
    let mut eager = node_with(tmp.path(), "eager", 1, 0);
    eager.migrate1();
    eager.insert(1, "v1");
    eager.sync();
    assert!(
        !ckpoints(&eager).is_empty(),
        "threshold 1 must write a checkpoint on the first sync that qualifies"
    );
    // The checkpoint folded in every frame, so every segment is covered by its
    // version vector and retention 0 protects none of them: the folder is empty
    // and the snapshot alone carries the state.
    assert_eq!(
        eager.segments().len(),
        0,
        "retention 0 prunes every covered segment: {:?}",
        eager.segments()
    );
    assert_eq!(eager.values(), vec!["v1"]);

    // Lazy: a threshold nothing reaches.
    let mut lazy = node_with(tmp.path(), "lazy", 1_000, 0);
    lazy.migrate1();
    for i in 1..=5 {
        lazy.insert(i, &format!("v{i}"));
        lazy.sync();
    }
    assert!(
        ckpoints(&lazy).is_empty(),
        "an unreachable threshold must not write a checkpoint"
    );
    assert_eq!(
        lazy.segments().len(),
        5,
        "without a checkpoint nothing may be pruned: {:?}",
        lazy.segments()
    );
    assert_eq!(lazy.values(), vec!["v1", "v2", "v3", "v4", "v5"]);
}

/// 14. A prune triggered by a real sync never deletes a segment that carries an
///     **uncovered** frame — a frame whose sort key is strictly above the cut
///     boundary is by definition outside the folded prefix, so the snapshot does
///     not account for it and the segment must survive. And the `ckpoint/` folder
///     stays bounded: after the pruning sync at most [`CHECKPOINTS_KEPT`]
///     checkpoint files remain (the newest cut and the one before it).
#[test]
fn prune_never_deletes_above_boundary_or_checkpoints() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 1);
    let mut bob = join_with(tmp.path(), "bob", &alice, 1, 1);
    alice.migrate1();
    alice.sync();
    FakeSync::deliver(&alice, &bob);
    bob.sync();

    for i in 1..=3 {
        alice.insert(i, &format!("a{i}"));
        bob.insert(i + 10, &format!("b{i}"));
        alice.sync();
        bob.sync();
        FakeSync::deliver(&alice, &bob);
        FakeSync::deliver(&bob, &alice);
        deliver_checkpoints(&alice, &bob);
        deliver_checkpoints(&bob, &alice);
    }

    // What the folder holds right before the pruning sync: every segment, decoded
    // so its frames are known, and every checkpoint name.
    let before: Vec<(String, sharepoint_db::Segment)> = alice
        .segments()
        .into_iter()
        .map(|name| {
            let bytes = std::fs::read(alice.segments_dir().join(&name)).unwrap();
            let segment = sharepoint_db::decode_segment(&bytes, &alice.db_id()).unwrap();
            (name, segment)
        })
        .collect();
    let cps_before = ckpoints(&alice);
    assert!(!before.is_empty() && !cps_before.is_empty());

    // A real sync: it publishes, ingests, writes a fresh checkpoint and prunes.
    alice.sync();

    let (best_name, best) = best_named(&alice).expect("a checkpoint after the sync");
    let boundary = best.boundary();
    let survivors = alice.segments();
    // The safety property under coverage-based prune: a segment is only ever
    // deleted when the checkpoint's snapshot already accounts for every frame in
    // it — i.e. its maximum sort key is at or below the cut boundary. A segment
    // carrying a frame strictly above the boundary holds an uncovered frame and
    // must survive.
    let deleted: Vec<&(String, sharepoint_db::Segment)> = before
        .iter()
        .filter(|(name, _)| !survivors.contains(name))
        .collect();
    assert!(
        !deleted.is_empty(),
        "the pruning sync must actually delete something for this test to mean anything"
    );
    for (name, segment) in &before {
        let max = segment.frames.last().unwrap();
        let max_key = (max.lamport, segment.node_id, max.seq);
        if max_key > boundary {
            assert!(
                survivors.contains(name),
                "segment {name} carries a frame strictly above the boundary {boundary:?} \
                 (best checkpoint {best_name}) — an uncovered frame — and must survive: {survivors:?}"
            );
        }
    }
    for (name, segment) in deleted {
        let max = segment.frames.last().unwrap();
        let max_key = (max.lamport, segment.node_id, max.seq);
        assert!(
            max_key <= boundary,
            "segment {name} was pruned but its max sort key {max_key:?} is above the \
             boundary {boundary:?}: prune deleted an uncovered frame"
        );
    }
    // The ckpoint/ folder is bounded: compaction keeps the newest cut and the one
    // before it, so a node one cut behind still finds a usable checkpoint.
    let cps_after = ckpoints(&alice);
    assert!(
        cps_after.len() <= sharepoint_db::checkpoint::CHECKPOINTS_KEPT,
        "the ckpoint/ folder must stay bounded: {cps_after:?}"
    );
    // The best checkpoint — the one this node would use — is always kept.
    assert!(
        cps_after.contains(&best_name),
        "the best checkpoint {best_name} must survive: {cps_after:?}"
    );
    // The data is untouched by all of this.
    assert_eq!(alice.values().len(), 6);
}

// ---------------------------------------------------------------------------
// Bootstrapped-node invariant (R1-R4): a node with a `Base` is second-class.
// ---------------------------------------------------------------------------

/// Opens a brand-new node `name` on a full copy of `seed`'s folder (manifest,
/// checkpoints and retained segments included) with an empty state directory, so
/// its store is behind the newest checkpoint and it bootstraps at open.
fn bootstrap_node(root: &Path, name: &str, seed: &Node, thr: usize, ret: usize) -> Node {
    let folder = root.join(format!("{name}.sharedb"));
    let state = root.join(format!("{name}.state"));
    copy_tree(&seed.folder, &folder);
    let db = sharepoint_db::ShareDb::open(
        &folder,
        OpenOptions {
            create: false,
            state_dir: Some(state.clone()),
            checkpoint_threshold: thr,
            retention_segments: ret,
            ..Default::default()
        },
    )
    .unwrap();
    Node {
        name: name.to_string(),
        folder,
        state,
        db,
    }
}

/// Whether a node's store carries a checkpoint `Base` (i.e. it is bootstrapped).
fn is_bootstrapped(node: &Node) -> bool {
    let store = sharepoint_db::store::open(&node.state.join("store.db")).unwrap();
    let out = sharepoint_db::store::base(&store).unwrap().is_some();
    drop(store);
    out
}

/// 15. The concrete divergence case from the design review (F1/F6). A bootstrapped
///     node that later receives a below-boundary frame its checkpoint does not
///     cover must NOT fast-path-replay it on top of the snapshot: the frame sorts
///     *before* frames the snapshot already folded in, so replaying it last hands
///     the conflict handler a total order that disagrees with a full-history node's
///     rebuild. Here the two writers COLLIDE on the same row id, so the wrong order
///     gives D a different value for that row than A's rebuild — a permanent,
///     silent divergence reported as `dropped = 0`.
///
///     The fix makes `fast_forward` refuse the uncovered below-boundary frame, so
///     `reconcile` falls through to `keep_pending` (stale-but-never-wrong): D keeps
///     the snapshot value and, once a full-history node publishes a covering
///     checkpoint, re-bootstraps and converges. This test fails without the fix.
#[test]
fn bootstrapped_node_late_below_boundary_frame_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 64, 0);
    let mut c = Node::join(tmp.path(), "c", &a);
    a.migrate1();
    a.sync();
    FakeSync::deliver(&a, &c);
    c.sync();

    // 1. C commits c1 — it INSERTs the SAME row id (1) that A inserts inside
    //    checkpoint X, and it sorts below A's frames (low lamport). C does not
    //    sync yet.
    c.insert(1, "c1");

    // 2. A commits two filler rows first so its lamport clock is strictly above
    //    c1's (the ordering must not depend on how the two node ids sort), then
    //    a1..a64, and syncs: it crosses the threshold, writes checkpoint X and
    //    prunes (retention 0), so the folder keeps only the snapshot. X's snapshot
    //    is A's local state, which never saw c1, so it carries row 1 = "a1".
    a.insert(900, "f1");
    a.insert(901, "f2");
    for i in 1..=64 {
        a.insert(i, &format!("a{i}"));
    }
    a.sync();
    assert!(!ckpoints(&a).is_empty(), "A wrote checkpoint X");

    // 3. D is brand new: it opens on a copy of A's folder (X present, store empty)
    //    and bootstraps from X. Its store holds nothing below the cut; its snapshot
    //    already carries row 1 = "a1".
    let mut d = bootstrap_node(tmp.path(), "d", &a, 64, 0);
    d.sync();
    assert!(is_bootstrapped(&d), "D bootstrapped: it carries a Base");
    assert_eq!(store_len(&d), 0, "D's store is empty after bootstrap");
    assert_eq!(d.values().len(), 66, "D holds the snapshot state");

    // 4. C publishes c1 (sorts below X's boundary, not covered by X.vv).
    c.sync();
    FakeSync::deliver(&c, &a);
    FakeSync::deliver(&c, &d);
    deliver_checkpoints(&c, &a);
    deliver_checkpoints(&c, &d);

    // A is a full-history node: c1 interleaves into its applied prefix, so the
    // digest check forces a full rebuild in total order. The conflict handler lets
    // the later frame win, so row 1 keeps A's own value "a1" — that is the value
    // every node that rebuilds in total order agrees on.
    a.sync();
    let row1_a =
        a.db.query("SELECT v FROM t WHERE id = 1", [], |r| {
            Ok(r.get::<_, String>(0)?)
        })
        .unwrap();
    assert_eq!(
        row1_a,
        vec!["a1".to_string()],
        "A rebuilds in total order, where a1 sorts after c1"
    );

    // 5. D ingests c1. X now fails the USE gate for D (D holds uncovered
    //    below-boundary c1). The fast path must refuse to replay c1 on top of the
    //    snapshot: c1 sorts *before* the folded-in frames, so replaying it last
    //    hands the conflict handler the wrong order — it lets c1 overwrite row 1
    //    on top of a snapshot that already folded a1 in, leaving D with "c1" while
    //    every rebuilding node has "a1". The divergence is permanent and reported
    //    as `dropped = 0`. With the fix D goes pending instead: stale, but it knows.
    let rep = d.db.sync().unwrap();
    assert!(
        rep.materialize_pending,
        "D must go pending rather than fast-path an out-of-order frame"
    );
    assert!(
        rep.dropped_below_boundary > 0,
        "a pending node must report the dropped count, not reset it to 0"
    );
    // Row 1 must still be the snapshot value, not an out-of-order replay on top of
    // it: D is stale-but-never-wrong while pending.
    let row1 =
        d.db.query("SELECT v FROM t WHERE id = 1", [], |r| {
            Ok(r.get::<_, String>(0)?)
        })
        .unwrap();
    assert_eq!(
        row1,
        vec!["a1".to_string()],
        "D must not fast-path-replay the below-boundary frame on top of the snapshot"
    );

    // 6/7. A commits again past its threshold and publishes a checkpoint whose
    //    version vector covers c1; D re-bootstraps from it and converges with A.
    for i in 200..264 {
        a.insert(i, &format!("x{i}"));
    }
    a.sync();
    for _ in 0..3 {
        FakeSync::deliver(&a, &d);
        FakeSync::deliver(&c, &d);
        deliver_checkpoints(&a, &d);
        deliver_checkpoints(&c, &d);
        c.sync();
        d.sync();
    }
    assert_eq!(a.dump(), d.dump(), "A and D converge");
    let row1 =
        d.db.query("SELECT v FROM t WHERE id = 1", [], |r| {
            Ok(r.get::<_, String>(0)?)
        })
        .unwrap();
    assert_eq!(
        row1,
        vec!["a1".to_string()],
        "the total order puts a1 after c1, so row 1 converges to a1"
    );
    assert_eq!(d.values().len(), 130);
}

/// 16. R1 — a bootstrapped node never writes a checkpoint. D bootstraps from X,
///     then commits well past its (tiny) threshold; not one new `.cpt` appears in
///     its folder, because a checkpoint from an incomplete store would mislead
///     every other node.
#[test]
fn bootstrapped_node_never_writes_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 1, 1);
    a.migrate1();
    for i in 1..=4 {
        a.insert(i, &format!("v{i}"));
        a.sync();
    }
    let mut d = bootstrap_node(tmp.path(), "d", &a, 1, 1);
    d.sync();
    assert!(is_bootstrapped(&d), "D bootstrapped");
    let cps_after_bootstrap = ckpoints(&d);
    assert!(
        !cps_after_bootstrap.is_empty(),
        "D copied A's checkpoints when it bootstrapped"
    );

    // D commits far past its threshold of 1 and syncs repeatedly.
    for i in 1..=10 {
        d.insert(100 + i, &format!("d{i}"));
        d.sync();
    }
    assert_eq!(
        ckpoints(&d),
        cps_after_bootstrap,
        "a bootstrapped node must write no checkpoint (R1)"
    );
    // Its data is still correct locally.
    assert_eq!(d.values().len(), 14);
}

/// 17. R3 — a bootstrapped node may only re-bootstrap from a checkpoint whose
///     version vector dominates its current base. D bootstraps from X (base
///     `vv[A] = 65`), is driven pending, then a checkpoint Y is planted that
///     *wins* selection on `count` and passes the USE gate but has a lower
///     `vv[A]`. Re-bootstrapping to Y would drop frames X's snapshot relied on, so
///     D must refuse it: it keeps its state, stays `materialize_pending`, and its
///     base is unchanged.
#[test]
fn rebootstrap_requires_dominating_version_vector() {
    use sharepoint_db::{checkpoint, store};

    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 64, 0);
    let mut c = Node::join(tmp.path(), "c", &a);
    a.migrate1();
    a.sync();
    FakeSync::deliver(&a, &c);
    c.sync();
    c.insert(1000, "c1"); // low lamport, sorts below A's frames
    for i in 1..=64 {
        a.insert(i, &format!("a{i}"));
    }
    a.sync(); // X: count 65, vv[A] = 65
    let mut d = bootstrap_node(tmp.path(), "d", &a, 64, 0);
    d.sync();
    let base_before = store::base(&store::open(&d.state.join("store.db")).unwrap())
        .unwrap()
        .expect("D bootstrapped");

    // Drive D pending: one above-boundary frame (a65) then a below-boundary
    // frame (c1) that interleaves before it, so the fast path fails and X no
    // longer passes the USE gate for D.
    a.insert(65, "a65");
    a.sync();
    FakeSync::deliver(&a, &d);
    deliver_checkpoints(&a, &d);
    d.sync();
    c.sync();
    FakeSync::deliver(&c, &d);
    deliver_checkpoints(&c, &d);
    let rep = d.db.sync().unwrap();
    assert!(
        rep.materialize_pending,
        "D is pending after the below-boundary frame"
    );

    // Plant Y: it wins selection on `count` (larger than X) and passes the USE
    // gate (its boundary is below every frame, so nothing is below it to cover),
    // but its `vv[A]` is far below the base's — it does not dominate.
    let store_conn = store::open(&d.state.join("store.db")).unwrap();
    let frames = store::all_frames(&store_conn, &d.db_id()).unwrap();
    drop(store_conn);
    let y = checkpoint::Checkpoint {
        count: 10_000,
        cut_lamport: 0,
        boundary: (0, [0u8; 8], 0),
        cut_digest: store::digest_first(&frames, 1),
        vv: checkpoint::VersionVector(vec![checkpoint::Version {
            node: a.node_id(),
            max_seq: 1,
            hash: [7u8; 32],
        }]),
        snapshot: std::fs::read(d.state.join("data.db")).unwrap(),
    };
    put_checkpoint(&d, &y.file_name(), &y.encode(&d.db_id()));

    // Another below-boundary frame forces a reconcile; Y is the best checkpoint
    // but must be refused (R3), so D stays pending with its base unchanged.
    c.insert(1001, "c2");
    c.sync();
    FakeSync::deliver(&c, &d);
    deliver_checkpoints(&c, &d);
    let rep = d.db.sync().unwrap();
    assert!(
        rep.materialize_pending,
        "a non-dominating checkpoint must not be used to re-bootstrap (R3)"
    );
    let base_after = store::base(&store::open(&d.state.join("store.db")).unwrap())
        .unwrap()
        .expect("still bootstrapped");
    assert_eq!(
        base_after, base_before,
        "the base must be unchanged: a non-dominating checkpoint was refused"
    );
}

/// 18. R4 — a node that bootstraps twice can still ingest a peer. D bootstraps
///     from X (`vv[P] = 100`), ingests P's frames 101..110 (its own stored P is
///     now 110), then re-bootstraps from Z (`vv[P] = 300`). After the
///     re-bootstrap its own stored P is below the new base's `vv[P]`, so the
///     anchor must take the **max** (the base's 300), not the lower stored value
///     — otherwise P's retained segment starting above 300 would look like a gap.
#[test]
fn double_bootstrap_still_ingests_peer() {
    use sharepoint_db::store;

    let tmp = tempfile::tempdir().unwrap();
    // P is the only writer; a threshold of 100 puts a checkpoint at cut 100.
    let mut p = node_with(tmp.path(), "p", 100, 40);
    p.migrate1();
    for i in 1..=99 {
        p.insert(i, &format!("p{i}"));
    }
    p.sync(); // X: vv[P] = 100
    let mut d = bootstrap_node(tmp.path(), "d", &p, 100_000, 40);
    d.sync();
    let base_x = store::base(&store::open(&d.state.join("store.db")).unwrap())
        .unwrap()
        .expect("D bootstrapped from X");
    assert_eq!(
        base_x.vv.get(&p.node_id()).map(|v| v.max_seq),
        Some(100),
        "X's version vector has P at 100"
    );

    // P commits 101..110; D ingests them (anchor from base vv[P] = 100).
    for i in 100..=109 {
        p.insert(i, &format!("p{i}"));
    }
    p.sync();
    FakeSync::deliver(&p, &d);
    deliver_checkpoints(&p, &d);
    let rep = d.db.sync().unwrap();
    assert_eq!(rep.ingested, 10, "D ingested P 101..110");
    assert!(rep.gaps.is_empty(), "no gap: anchor came from the base vv");

    // P commits up to 300 (Z: vv[P] = 300, prune up to ~250) then 301..310 so a
    // retained segment extends above 300.
    for i in 110..=300 {
        p.insert(i, &format!("p{i}"));
    }
    p.sync();
    for i in 301..=310 {
        p.insert(i, &format!("p{i}"));
    }
    p.sync();

    // Force D to re-bootstrap from Z by dropping its data file (the store keeps
    // its Base and frames). Z dominates X, so the re-bootstrap is allowed.
    std::fs::remove_file(d.state.join("data.db")).unwrap();
    FakeSync::deliver(&p, &d);
    deliver_checkpoints(&p, &d);
    let rep = d.db.sync().unwrap();
    assert_eq!(
        rep.materialize,
        sharepoint_db::MaterializeKind::Rebuilt,
        "D re-bootstrapped from Z"
    );
    let base_z = store::base(&store::open(&d.state.join("store.db")).unwrap())
        .unwrap()
        .expect("still bootstrapped");
    assert!(
        base_z.vv.get(&p.node_id()).map(|v| v.max_seq).unwrap_or(0) >= 300,
        "Z's version vector has P at >= 300"
    );
    // The re-bootstrap must not have left a gap for P's retained frames above 300.
    assert!(
        rep.gaps.is_empty(),
        "no gap after re-bootstrap: {:?}",
        rep.gaps
    );
    // D's dump now carries every P frame (no gap in the middle).
    assert_eq!(d.values().len(), 310, "D holds p1..p310 with no gap");
}

/// 19. F2 — a bootstrapped node must stamp its own commits at or above the cut.
///     D bootstraps from X (its store is empty, so its store lamport maximum is 0)
///     and then UPDATEs a row that exists in the snapshot. Without the fix its
///     first own frame gets lamport 1, far below X's cut, so it sorts *before* the
///     frames A already folded into X: A's rebuild applies D's UPDATE first and
///     then overwrites it with A's own INSERT, while D applied the UPDATE on top of
///     the snapshot — a silent divergence. With the fix the new frame's lamport is
///     `max(store max, base.cut_lamport)`, so it sorts last everywhere and both
///     nodes agree. This test fails without the fix.
#[test]
fn bootstrapped_node_commit_above_cut_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 64, 8);
    a.migrate1();
    for i in 1..=64 {
        a.insert(i, &format!("a{i}"));
    }
    a.sync();
    assert!(!ckpoints(&a).is_empty(), "A wrote a checkpoint");

    // D bootstraps from A's checkpoint: empty store, snapshot carries a1..a64.
    let mut d = bootstrap_node(tmp.path(), "d", &a, 64, 8);
    d.sync();
    assert!(is_bootstrapped(&d), "D bootstrapped");
    assert_eq!(store_len(&d), 0, "D's store is empty after bootstrap");

    // D UPDATEs a row that exists in the snapshot. Its own lamport clock must not
    // restart at 1 — that would sort below the cut.
    d.update(1, "d1");
    d.sync();

    // Exchange everything with A, twice, so D's frame reaches A and A's frames
    // reach D, and both re-materialise.
    for _ in 0..3 {
        FakeSync::deliver(&a, &d);
        FakeSync::deliver(&d, &a);
        deliver_checkpoints(&a, &d);
        deliver_checkpoints(&d, &a);
        a.sync();
        d.sync();
    }

    // Both must agree on the colliding row: D's UPDATE sorts last, so "d1" wins.
    assert_eq!(a.dump(), d.dump(), "A and D converge on D's own commit");
    let row1 =
        d.db.query("SELECT v FROM t WHERE id = 1", [], |r| {
            Ok(r.get::<_, String>(0)?)
        })
        .unwrap();
    assert_eq!(row1, vec!["d1".to_string()], "D's UPDATE wins (sorts last)");
}

/// 20. F3 — on a bootstrapped node a failed commit must NOT leave its change in
///     `data.db`. Phase 1 already committed the change plus bookkeeping `(N+1)`
///     into `data.db`; only the store write failed. Keeping that file (the old R2
///     behaviour) leaves a phantom row that only this node ever sees: a later commit
///     reuses the same `seq`/lamport and rewrites the bookkeeping so `data.db` and
///     the store agree on paper while the phantom row survives. The fix invalidates
///     the diverged `data.db` and fails the handle, so the next open re-bootstraps
///     from a dominating checkpoint and the phantom row is gone.
#[test]
fn repair_to_store_on_bootstrapped_node_invalidates_data_db() {
    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 1, 1);
    a.migrate1();
    for i in 1..=4 {
        a.insert(i, &format!("v{i}"));
        a.sync();
    }
    let mut d = bootstrap_node(tmp.path(), "d", &a, 1, 1);
    d.sync();
    assert!(is_bootstrapped(&d), "D bootstrapped");
    let values_before = d.values();
    assert_eq!(values_before, vec!["v1", "v2", "v3", "v4"]);

    // Arm the fault: the next commit writes data.db but its frame never reaches
    // the store, so the commit protocol calls `repair_to_store`.
    d.db.set_fault(Some(
        sharepoint_db::FaultPlan::AfterDataCommitBeforeFrameInsert,
    ));
    let err = d.db.execute(
        "INSERT INTO t (id, v) VALUES (?1, ?2)",
        sharepoint_db::rusqlite::params![999, "boom"],
    );
    assert!(err.is_err(), "the faulted commit fails");
    d.db.set_fault(None);

    // The handle is refused: a bootstrapped node cannot keep a data.db the store
    // cannot account for, so it must re-bootstrap on the next open.
    assert!(
        d.db.status().is_err(),
        "the handle is failed after a failed commit on a bootstrapped node (F3)"
    );

    // Reopen: the node re-bootstraps from a dominating checkpoint. The phantom row
    // (id 999) must be ABSENT — it was never in the store, so it must not survive
    // into the re-bootstrapped data.db.
    let mut d = reopen(d);
    let vals = d.values();
    assert!(
        !vals.contains(&"boom".to_string()),
        "the phantom row must not survive the reopen: {vals:?}"
    );
    assert!(
        !d.db
            .query("SELECT count(*) FROM t WHERE id = 999", [], |r| {
                Ok(r.get::<_, i64>(0)?)
            })
            .unwrap()
            .iter()
            .any(|n| *n > 0),
        "id 999 is absent after reopen"
    );
    // The snapshot rows survive (v1..v4), not a store-only collapse.
    assert!(vals.contains(&"v1".to_string()), "snapshot rows survive");

    // One more clean commit and a reopen: the phantom row is still absent and the
    // new row is present, proving data.db and the store agree.
    d.insert(500, "after");
    d.sync();
    let d = reopen(d);
    let vals = d.values();
    assert!(
        !vals.contains(&"boom".to_string()),
        "the phantom row stays gone after another commit + reopen: {vals:?}"
    );
    assert!(vals.contains(&"after".to_string()), "the new row survives");
}

/// 21. R2/R5 — a bootstrapped node reopened with an uncovered below-boundary
///     frame and no usable checkpoint reports the dropped count and does not
///     silently rebuild. D is driven pending by a sync, then reopened: the
///     open-time reconcile cannot re-bootstrap (no dominating, gate-passing
///     checkpoint), so it keeps the last-good data, marks the handle pending and
///     carries `dropped_below_boundary` forward (never reset to 0).
#[test]
fn open_reports_dropped_below_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 64, 0);
    let mut c = Node::join(tmp.path(), "c", &a);
    a.migrate1();
    a.sync();
    FakeSync::deliver(&a, &c);
    c.sync();
    c.insert(1000, "c1"); // low lamport, sorts below A's frames
    for i in 1..=64 {
        a.insert(i, &format!("a{i}"));
    }
    a.sync(); // X: count 65
    let mut d = bootstrap_node(tmp.path(), "d", &a, 64, 0);
    d.sync();
    // Drive D pending: a65 above the boundary, then c1 below it.
    a.insert(65, "a65");
    a.sync();
    FakeSync::deliver(&a, &d);
    deliver_checkpoints(&a, &d);
    d.sync();
    c.sync();
    FakeSync::deliver(&c, &d);
    deliver_checkpoints(&c, &d);
    let rep = d.db.sync().unwrap();
    assert!(rep.materialize_pending);
    assert!(
        rep.dropped_below_boundary > 0,
        "the sync reported a dropped count"
    );

    // Reopen: the open-time reconcile keeps the last-good data and reports the
    // same pending state and dropped count (carried forward, not reset).
    let d = reopen(d);
    let status = d.db.status().unwrap();
    assert!(
        status.materialize_pending,
        "the reopened node is still pending (no silent rebuild)"
    );
    assert!(
        status.dropped_below_boundary > 0,
        "the dropped count is carried forward across the reopen, not reset to 0"
    );
    // Reads still work off the last-good snapshot (a1..a64), not a store-only
    // rebuild that would have collapsed the data.
    assert_eq!(
        d.values().len(),
        65,
        "the last-good snapshot state survives the reopen"
    );
}

/// 22. F4 — a bootstrapped node still raises its publish floor. D bootstraps from
///     X, commits and publishes its own frames, and later sees a checkpoint from a
///     full-history node whose version vector dominates its base and covers those
///     frames. Once that checkpoint exists, the folder no longer needs D's own
///     segments: a full-history node's prune may delete them. D must therefore stop
///     republishing them — otherwise the folder churns on every sync forever, even
///     though no data is lost. Before the fix `compact` returned early for a
///     bootstrapped node and never raised the floor.
#[test]
fn bootstrapped_node_raises_publish_floor() {
    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 1, 1);
    a.migrate1();
    for i in 1..=4 {
        a.insert(i, &format!("v{i}"));
        a.sync();
    }
    let mut d = bootstrap_node(tmp.path(), "d", &a, 1, 1);
    d.sync();
    assert!(is_bootstrapped(&d), "D bootstrapped");

    // D commits its own frames and publishes them as its own segments.
    for i in 1..=5 {
        d.insert(100 + i, &format!("d{i}"));
    }
    d.sync();
    let d_segs = d.segments();
    assert!(
        !d_segs.is_empty(),
        "D published its own segments before the checkpoint existed"
    );

    // A ingests D's frames and keeps committing, so its newest checkpoint's
    // version vector dominates D's base and covers D's own seq 1..=5.
    FakeSync::deliver(&d, &a);
    deliver_checkpoints(&d, &a);
    for i in 5..=12 {
        a.insert(i, &format!("v{i}"));
        a.sync();
    }
    deliver_checkpoints(&a, &d);

    // D sees the dominating checkpoint and raises its publish floor (F4). The
    // floor is persisted, so it survives the folder change below.
    d.sync();

    // A full-history node's prune deletes D's own segments from the folder; the
    // sync client propagates the deletion to D's copy too.
    for name in &d_segs {
        let _ = std::fs::remove_file(d.segments_dir().join(name));
    }
    d.sync();
    let republished: Vec<String> = d
        .segments()
        .into_iter()
        .filter(|n| d_segs.iter().any(|s| s == n))
        .collect();
    assert!(
        republished.is_empty(),
        "a bootstrapped node must not republish segments a dominating checkpoint \
         already carries: {republished:?}"
    );
    // Its own data is untouched (4 snapshot rows + 5 of its own).
    assert_eq!(d.values().len(), 9);
}

/// 23. F5 — a pending bootstrapped handle reports itself as pending on *every*
///     sync, not only on the one that found the problem. Once D is pending, a later
///     sync whose store and data file agree (here: after a local commit, which
///     advances the bookkeeping itself) takes the `UpToDate` early return; that
///     return must copy the handle's pending flag and carried-forward dropped count
///     into the report instead of leaving the `SyncReport::default()` zeros, or a
///     caller polling `sync` sees a healthy node that is in fact stale.
#[test]
fn pending_node_reports_pending_on_a_noop_sync() {
    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 64, 0);
    let mut c = Node::join(tmp.path(), "c", &a);
    a.migrate1();
    a.sync();
    FakeSync::deliver(&a, &c);
    c.sync();
    c.insert(1000, "c1"); // low lamport, sorts below A's frames
    for i in 1..=64 {
        a.insert(i, &format!("a{i}"));
    }
    a.sync(); // X: count 65
    let mut d = bootstrap_node(tmp.path(), "d", &a, 64, 0);
    d.sync();

    // Drive D pending: a65 above the boundary, then c1 below it.
    a.insert(65, "a65");
    a.sync();
    FakeSync::deliver(&a, &d);
    deliver_checkpoints(&a, &d);
    d.sync();
    c.sync();
    FakeSync::deliver(&c, &d);
    deliver_checkpoints(&c, &d);
    let rep = d.db.sync().unwrap();
    assert!(rep.materialize_pending, "D is pending");
    let dropped = rep.dropped_below_boundary;
    assert!(dropped > 0, "the sync reported a dropped count");

    // A local commit advances the bookkeeping on its own, so the next sync has
    // nothing left to ingest and takes the `UpToDate` early return.
    d.insert(700, "mine");
    let rep2 = d.db.sync().unwrap();
    assert_eq!(rep2.materialize, sharepoint_db::MaterializeKind::UpToDate);
    assert!(
        rep2.materialize_pending,
        "an UpToDate sync must still report the handle as pending"
    );
    assert_eq!(
        rep2.dropped_below_boundary, dropped,
        "the dropped count is carried forward, not reset to 0"
    );
    // `status` agrees with the report.
    let status = d.db.status().unwrap();
    assert!(status.materialize_pending);
    assert_eq!(status.dropped_below_boundary, dropped);
}

/// 24. F3 fail-closed — a `store.db` whose read of the `Base` fails must never be
///     read as "not bootstrapped". D bootstraps, then a commit fails while the
///     `Base` read fails too (the state a busy or failing `store.db` really leaves:
///     the read fails where the write just did). A store-only rebuild would drop
///     the snapshot's rows below the cut and stamp `(total, full_digest)` as
///     applied, which the next open would fast-path in silence. The repair must
///     instead invalidate the data file and fail the handle.
#[test]
fn store_base_read_failure_fails_closed_instead_of_store_only_rebuild() {
    use sharepoint_db::FaultPlan;

    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 1, 1);
    a.migrate1();
    for i in 1..=4 {
        a.insert(i, &format!("v{i}"));
        a.sync();
    }
    let mut d = bootstrap_node(tmp.path(), "d", &a, 1, 1);
    d.sync();
    assert!(is_bootstrapped(&d), "D bootstrapped");
    assert_eq!(d.values(), vec!["v1", "v2", "v3", "v4"]);

    // The store write fails *and* the Base read fails.
    d.db.set_fault(Some(FaultPlan::AfterDataCommitAndStoreBaseReadFails));
    let err = d.db.execute(
        "INSERT INTO t (id, v) VALUES (?1, ?2)",
        sharepoint_db::rusqlite::params![999, "boom"],
    );
    assert!(err.is_err(), "the faulted commit fails");
    d.db.set_fault(None);

    // The handle is refused: the node could be bootstrapped, so the repair may not
    // store-only-rebuild it.
    let q =
        d.db.query("SELECT v FROM t", [], |r| Ok(r.get::<_, String>(0)?))
            .unwrap_err();
    assert!(
        matches!(q, sharepoint_db::Error::Failed(_)),
        "an unreadable Base must fail the handle, got {q}"
    );

    // The decisive check: the diverged file was invalidated, not store-only-
    // rebuilt. A store-only rebuild would have left a `data.db` holding only D's
    // own (empty) store history with `(0, empty digest)` stamped as applied — and
    // the next open would fast-path that agreement and serve an empty table.
    assert!(
        !d.state.join("data.db").exists(),
        "the diverged data.db must be invalidated, not rebuilt from the store alone"
    );

    // Reopen: the node re-bootstraps from a dominating checkpoint, and the phantom
    // row is gone because the diverged file was invalidated, not rebuilt.
    let mut d = reopen(d);
    let vals = d.values();
    assert!(
        !vals.contains(&"boom".to_string()),
        "the phantom row must not survive the reopen: {vals:?}"
    );
    assert!(vals.contains(&"v1".to_string()), "snapshot rows survive");
    d.insert(500, "after");
    d.sync();
    let d = reopen(d);
    assert!(
        d.values().contains(&"after".to_string()),
        "new row survives"
    );
}

/// 25. F2 fail-closed — `refresh_chain` may not drop the lamport floor to 0 when
///     the `Base` read fails. D bootstraps from a checkpoint whose cut is far above
///     its store maximum, then ingests a low-lamport peer frame with the Base
///     unreadable, so the sync re-runs `refresh_chain` with the read failing. If the
///     floor fell to the store maximum (the old `unwrap_or(0)` cut) D would stamp its
///     next commit below the cut, where every peer has already folded the snapshot's
///     frames into its own: A's conflict handler would drop it and the two nodes
///     diverge in silence. The handle keeps its (safe, higher) floor instead.
#[test]
fn store_base_read_failure_keeps_the_lamport_floor_above_the_cut() {
    use sharepoint_db::FaultPlan;

    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 64, 0);
    let mut c = Node::join(tmp.path(), "c", &a);
    a.migrate1();
    a.sync();
    FakeSync::deliver(&a, &c);
    c.sync();
    c.insert(1000, "c1"); // low lamport, sorts far below A's frames
    for i in 1..=64 {
        a.insert(i, &format!("a{i}"));
    }
    a.sync(); // X: count 65, cut far above c1's lamport

    let mut d = bootstrap_node(tmp.path(), "d", &a, 64, 0);
    d.sync();
    assert!(is_bootstrapped(&d), "D bootstrapped");
    assert_eq!(store_len(&d), 0, "D's store is empty after bootstrap");

    // Hand D the low-lamport frame with the Base unreadable: ingesting it re-runs
    // `refresh_chain`, whose cut read now fails while the store maximum is 1.
    c.sync();
    FakeSync::deliver(&c, &d);
    deliver_checkpoints(&c, &d);
    d.db.set_fault(Some(FaultPlan::StoreBaseReadFails));
    d.db.sync().unwrap();
    d.db.set_fault(None);

    // D's own commit must still sort last: with a dropped floor it is stamped at
    // lamport 2, below the cut, and A's conflict handler drops it.
    d.update(1, "d1");
    d.sync();
    for _ in 0..3 {
        FakeSync::deliver(&a, &d);
        FakeSync::deliver(&d, &a);
        deliver_checkpoints(&a, &d);
        deliver_checkpoints(&d, &a);
        a.sync();
        d.sync();
    }
    assert_eq!(a.dump(), d.dump(), "A and D converge");
    let row1 =
        d.db.query("SELECT v FROM t WHERE id = 1", [], |r| {
            Ok(r.get::<_, String>(0)?)
        })
        .unwrap();
    assert_eq!(row1, vec!["d1".to_string()], "D's UPDATE wins");
    // The frame is stamped above the cut, not just above the store maximum.
    let cut = {
        let store = sharepoint_db::store::open(&d.state.join("store.db")).unwrap();
        sharepoint_db::store::base(&store)
            .unwrap()
            .expect("D is bootstrapped")
            .cut_lamport
    };
    let own_lamport = store_frames(&d)
        .into_iter()
        .filter(|f| f.node_id == d.db.node_id())
        .map(|f| f.frame.lamport)
        .max()
        .expect("D has its own frame");
    assert!(
        own_lamport >= cut,
        "the own frame must not sort below the cut: lamport {own_lamport}, cut {cut}"
    );
}

/// 26. A bootstrapped node whose diverged `data.db` cannot be removed (a Windows
///     file lock, an antivirus) must not have its phantom row served later as
///     "pending". The repair records the file as invalid in `store.db`, and a
///     later open refuses it: the node fails loudly instead of reporting a row the
///     store never recorded.
#[test]
fn unremovable_diverged_data_db_is_marked_invalid() {
    use sharepoint_db::FaultPlan;

    let tmp = tempfile::tempdir().unwrap();
    let mut a = node_with(tmp.path(), "a", 1, 1);
    a.migrate1();
    for i in 1..=4 {
        a.insert(i, &format!("v{i}"));
        a.sync();
    }
    let mut d = bootstrap_node(tmp.path(), "d", &a, 1, 1);
    d.sync();
    assert!(is_bootstrapped(&d), "D bootstrapped");

    d.db.set_fault(Some(FaultPlan::BaseRepairRemoveFails));
    let err = d.db.execute(
        "INSERT INTO t (id, v) VALUES (?1, ?2)",
        sharepoint_db::rusqlite::params![999, "boom"],
    );
    assert!(err.is_err(), "the faulted commit fails");
    let reason = d.db.status().unwrap_err().to_string();
    d.db.set_fault(None);
    assert!(
        reason.contains("could not be removed"),
        "the failure says the file survived: {reason}"
    );

    // The file is still on disk — that is what the fault models — and it holds the
    // phantom row, so the store must carry the invalid marker.
    assert!(
        d.state.join("data.db").exists(),
        "data.db survived the removal"
    );
    let marked: bool = {
        let store = sharepoint_db::rusqlite::Connection::open(d.state.join("store.db")).unwrap();
        store
            .query_row(
                "SELECT value FROM meta WHERE key = 'invalid_data_db'",
                [],
                |r| r.get::<_, String>(0),
            )
            .is_ok()
    };
    assert!(marked, "the store records data.db as invalid");
    drop(d);

    // With no checkpoint left to re-bootstrap from, the marked file may not be
    // served — neither as the fast path nor as a bootstrapped node's last-good
    // (pending) state — so the open fails loudly instead of reporting the phantom
    // row as this node's data.
    let ckpoints = tmp.path().join("d.sharedb").join("ckpoint");
    for entry in std::fs::read_dir(&ckpoints).unwrap().flatten() {
        std::fs::remove_file(entry.path()).unwrap();
    }
    let err = sharepoint_db::ShareDb::open(
        &tmp.path().join("d.sharedb"),
        OpenOptions {
            create: false,
            state_dir: Some(tmp.path().join("d.state")),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("bootstrapped"),
        "the open must refuse the marked data.db, got {err}"
    );
}

/// B1. A node that bootstrapped from a checkpoint re-runs an already-applied
///     migration as `AlreadyApplied`, not `InvalidVersion`.
///
///     The bug: `migrate` looked for the migration SQL only in the frame store,
///     but a bootstrapped node's store lacks the frames below the cut (the
///     migration frame is folded into the snapshot), so re-running migration 1
///     returned `InvalidVersion` and broke the "call migrate(1..n) every launch"
///     pattern. The fix records `version -> sql` in `__sharedb_migrations` inside
///     `data.db`, which travels in the checkpoint snapshot.
#[test]
fn migrate_on_bootstrapped_node_is_already_applied() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 1, 1);
    alice.migrate1();
    for i in 1..=6 {
        alice.insert(i, &format!("v{i}"));
        alice.sync();
    }

    // Bob bootstraps from the snapshot: his store holds no frames below the cut.
    let mut bob = bootstrap_node(tmp.path(), "bob", &alice, 1, 1);
    assert!(is_bootstrapped(&bob), "bob must be bootstrapped");
    assert_eq!(bob.db.schema_version(), 1, "the snapshot carries schema v1");

    // The premise of the bug: bob's store does NOT hold migration 1's frame, so
    // the store-only lookup would have nothing to compare against.
    let store = sharepoint_db::store::open(&bob.state.join("store.db")).unwrap();
    assert_eq!(
        sharepoint_db::store::migration_sql_for_version(&store, 1).unwrap(),
        None,
        "a bootstrapped node's store lacks the below-cut migration frame"
    );
    drop(store);

    // Re-running the already-applied migration is AlreadyApplied, not
    // InvalidVersion — the SQL is found in `__sharedb_migrations`.
    assert_eq!(
        bob.db.migrate(1, common::MIGRATE_T).unwrap(),
        sharepoint_db::MigrateOutcome::AlreadyApplied,
        "a bootstrapped node re-running an applied migration reports AlreadyApplied"
    );
    // Different SQL for the same version is still a mismatch, not InvalidVersion.
    let err = bob
        .db
        .migrate(1, "CREATE TABLE t (id INTEGER PRIMARY KEY);")
        .unwrap_err();
    assert!(
        matches!(err, sharepoint_db::Error::MigrationMismatch { version: 1 }),
        "conflicting SQL must still be a mismatch, got {err}"
    );

    // A fresh migration still applies on the bootstrapped node.
    assert_eq!(
        bob.db
            .migrate(2, "CREATE TABLE u (id TEXT PRIMARY KEY);")
            .unwrap(),
        sharepoint_db::MigrateOutcome::Applied
    );
    assert_eq!(bob.db.schema_version(), 2);
}

/// B2. Two nodes record *different* SQL for the same migration version while
///     offline; after they converge, the loser's re-run of its own (losing) SQL
///     must report `MigrationMismatch` against the total-order **winner**, never
///     `AlreadyApplied` for its own SQL — and the winner's re-run of its own SQL
///     must stay `AlreadyApplied`.
///
///     The bug: the conflict lookup was `... WHERE kind=2 AND version=?1 LIMIT 1`
///     with no `ORDER BY`, so SQLite returned whichever row its scan happened to
///     reach first — rows are keyed by `(node_id, seq)` — not the total-order
///     winner `(lamport, node_id, seq)`. The fix orders by the total order.
///
///     The two orders only disagree when the frames differ in lamport, so the
///     test engineers exactly that: the LOSER carries the *lower* `node_id` (its
///     row is the first an unordered scan reaches) but the *higher* lamport,
///     because a third node's frames lift its clock before it migrates, while
///     the winner migrates without ever seeing them.
#[test]
fn migrate_conflict_compares_against_winner() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = node_with(tmp.path(), "alice", 64, 8);
    let mut bob = join_with(tmp.path(), "bob", &alice, 64, 8);
    let mut carol = join_with(tmp.path(), "carol", &alice, 64, 8);

    // Everyone agrees on migration 1, so the schema exists everywhere and the
    // clock-raiser below can write rows.
    alice.migrate1();
    bob.migrate1();
    carol.migrate1();
    FakeSync::exchange(&mut [&mut alice, &mut bob, &mut carol]);

    const SQL_A: &str = "CREATE TABLE ta (id INTEGER PRIMARY KEY, v TEXT NOT NULL);";
    const SQL_B: &str = "CREATE TABLE tb (id INTEGER PRIMARY KEY, v TEXT NOT NULL);";

    let alice_is_loser = alice.node_id() < bob.node_id();
    let (sql_loser, sql_winner) = if alice_is_loser {
        (SQL_A, SQL_B)
    } else {
        (SQL_B, SQL_A)
    };
    let (mut loser, mut winner) = if alice_is_loser {
        (alice, bob)
    } else {
        (bob, alice)
    };

    // Carol writes ahead; only the loser sees it, so the loser's clock ends up
    // above the winner's and its migration sorts last in the total order.
    for i in 1..=3 {
        carol.insert(i, &format!("c{i}"));
    }
    carol.sync();
    FakeSync::deliver(&carol, &loser);
    loser.sync();

    let loser_clock = {
        let store = sharepoint_db::store::open(&loser.state.join("store.db")).unwrap();
        sharepoint_db::store::max_lamport(&store, &loser.db_id()).unwrap()
    };
    let winner_clock = {
        let store = sharepoint_db::store::open(&winner.state.join("store.db")).unwrap();
        sharepoint_db::store::max_lamport(&store, &winner.db_id()).unwrap()
    };
    assert!(
        loser_clock > winner_clock,
        "the loser must migrate at a higher lamport than the winner, or the two \
         orders agree and the test would not discriminate ({loser_clock} vs {winner_clock})"
    );

    assert_eq!(
        winner.db.migrate(2, sql_winner).unwrap(),
        sharepoint_db::MigrateOutcome::Applied
    );
    assert_eq!(
        loser.db.migrate(2, sql_loser).unwrap(),
        sharepoint_db::MigrateOutcome::Applied
    );

    // Converge: both nodes end up holding both migration-2 frames.
    winner.sync();
    loser.sync();
    FakeSync::deliver(&loser, &winner);
    FakeSync::deliver(&winner, &loser);
    winner.sync();
    loser.sync();

    for node in [&winner, &loser] {
        let store = sharepoint_db::store::open(&node.state.join("store.db")).unwrap();
        let frames = sharepoint_db::store::all_frames(&store, &node.db_id()).unwrap();
        let migrations: Vec<_> = frames
            .iter()
            .filter(|f| matches!(f.frame.kind, sharepoint_db::FrameKind::Migration(2)))
            .collect();
        assert_eq!(migrations.len(), 2, "{} must hold both frames", node.name);
        // `all_frames` returns the total order, so the first is the winner's.
        assert!(
            migrations[0].frame.lamport < migrations[1].frame.lamport,
            "the winner's frame must sort first by lamport, got {} and {}",
            migrations[0].frame.lamport,
            migrations[1].frame.lamport
        );
        drop(store);
    }

    // The premise of the bug, checked where it bites: in the loser's own store
    // the first row an unordered `LIMIT 1` scan reaches is the loser's own — a
    // node appends its own frame before it ever ingests the peer's — so without
    // the `ORDER BY` the loser would be told its own losing SQL is the applied
    // one.
    {
        let store = sharepoint_db::store::open(&loser.state.join("store.db")).unwrap();
        let first_by_rowid = store
            .query_row(
                "SELECT CAST(substr(payload, 5) AS TEXT) FROM frames
                 WHERE kind = 2 AND version = 2 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            first_by_rowid, sql_loser,
            "the unordered scan must reach the loser's own row first, or the test \
             would not discriminate the missing ORDER BY"
        );
        drop(store);
    }

    // Reopen both (each handle holds its state dir's LOCK) so `migrate` re-reads
    // the store the way a fresh launch would.
    let mut winner = reopen(winner);
    let mut loser = reopen(loser);

    // The winner re-running its own SQL: AlreadyApplied.
    assert_eq!(
        winner.db.migrate(2, sql_winner).unwrap(),
        sharepoint_db::MigrateOutcome::AlreadyApplied,
        "the winner's own SQL is the applied one"
    );

    // The loser re-running its own (losing) SQL: MigrationMismatch against the
    // winner's SQL — never AlreadyApplied. This is the assertion the missing
    // ORDER BY used to get wrong.
    let err = loser.db.migrate(2, sql_loser).unwrap_err();
    assert!(
        matches!(err, sharepoint_db::Error::MigrationMismatch { version: 2 }),
        "the loser's re-run must mismatch against the winner's SQL, got {err}"
    );
}

/// Reopens `node` on the same folder and state directory with a new
/// `checkpoint_threshold` (everything else as in [`reopen`]).
fn reopen_with_checkpoint_threshold(node: Node, checkpoint_threshold: usize) -> Node {
    let Node {
        name,
        folder,
        state,
        db,
    } = node;
    drop(db);
    let db = sharepoint_db::ShareDb::open(
        &folder,
        OpenOptions {
            create: false,
            state_dir: Some(state.clone()),
            checkpoint_threshold,
            ..Default::default()
        },
    )
    .unwrap();
    Node {
        name,
        folder,
        state,
        db,
    }
}

/// A node that bootstraps from `seed`'s checkpoint with an empty store: the
/// folder copy carries only the `MANIFEST` and the `ckpoint/` directory (no
/// segments), so the store is empty at open and the node bootstraps from the
/// snapshot alone.
fn bootstrap_from_checkpoint(root: &Path, name: &str, seed: &Node) -> Node {
    let folder = root.join(format!("{name}.sharedb"));
    let state = root.join(format!("{name}.state"));
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::copy(seed.folder.join("MANIFEST"), folder.join("MANIFEST")).unwrap();
    let ckpoint = folder.join("ckpoint");
    std::fs::create_dir_all(&ckpoint).unwrap();
    for entry in std::fs::read_dir(seed.folder.join("ckpoint"))
        .unwrap()
        .flatten()
    {
        std::fs::copy(entry.path(), ckpoint.join(entry.file_name())).unwrap();
    }
    let db = sharepoint_db::ShareDb::open(
        &folder,
        OpenOptions {
            create: false,
            state_dir: Some(state.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    Node {
        name: name.to_string(),
        folder,
        state,
        db,
    }
}

/// B3. A bootstrapped node whose frame store holds only the LOSING duplicate of
///     a migration must compare a re-run against the total-order **winner**, not
///     against the losing frame in its store.
///
///     The bug: `migrate` looked the stored SQL up in the frame store FIRST and
///     only fell back to `data.db`'s `__sharedb_migrations`. On a node that
///     bootstrapped from a checkpoint whose cut folded in the winner's frame, the
///     winner is below the cut and not in the store; the node then ingests the
///     loser's segment and its store holds only the loser's frame. The store-first
///     lookup therefore compares against the loser's SQL: `migrate(v, winner_sql)`
///     wrongly reports `MigrationMismatch` and `migrate(v, loser_sql)` wrongly
///     reports `AlreadyApplied`.
///
///     The fix reverses the lookup: `data.db` is always a total-order replay, so
///     its `__sharedb_migrations` table holds the winner; the store is only the
///     fallback. This test fails with the store-first order.
#[test]
fn migrate_conflict_on_bootstrapped_node_compares_against_winner() {
    let tmp = tempfile::tempdir().unwrap();
    // A high threshold: no checkpoint is written until the winner is reopened with
    // a threshold of 1 below.
    let mut alice = node_with(tmp.path(), "alice", 1000, 8);
    let mut bob = join_with(tmp.path(), "bob", &alice, 1000, 8);
    let mut carol = join_with(tmp.path(), "carol", &alice, 1000, 8);

    // Everyone agrees on migration 1, so the schema exists everywhere and the
    // clock-raiser below can write rows.
    alice.migrate1();
    bob.migrate1();
    carol.migrate1();
    FakeSync::exchange(&mut [&mut alice, &mut bob, &mut carol]);

    const SQL_A: &str = "CREATE TABLE ta (id INTEGER PRIMARY KEY, v TEXT NOT NULL);";
    const SQL_B: &str = "CREATE TABLE tb (id INTEGER PRIMARY KEY, v TEXT NOT NULL);";

    let alice_is_loser = alice.node_id() < bob.node_id();
    let (sql_loser, sql_winner) = if alice_is_loser {
        (SQL_A, SQL_B)
    } else {
        (SQL_B, SQL_A)
    };
    let (mut loser, mut winner) = if alice_is_loser {
        (alice, bob)
    } else {
        (bob, alice)
    };

    // Carol writes ahead; only the loser sees it, so the loser's clock ends up
    // above the winner's and its migration sorts last in the total order.
    for i in 1..=3 {
        carol.insert(i, &format!("c{i}"));
    }
    carol.sync();
    FakeSync::deliver(&carol, &loser);
    loser.sync();

    assert_eq!(
        winner.db.migrate(2, sql_winner).unwrap(),
        sharepoint_db::MigrateOutcome::Applied
    );
    assert_eq!(
        loser.db.migrate(2, sql_loser).unwrap(),
        sharepoint_db::MigrateOutcome::Applied
    );
    // The winner publishes its own frames (including its v2 frame) but never
    // receives the loser's, so its store is the checkpoint's whole history.
    winner.sync();
    loser.sync();

    let winner_v2_lamport = {
        let store = sharepoint_db::store::open(&winner.state.join("store.db")).unwrap();
        let frames = sharepoint_db::store::all_frames(&store, &winner.db_id()).unwrap();
        let winner_v2 = frames
            .iter()
            .find(|f| matches!(f.frame.kind, sharepoint_db::FrameKind::Migration(2)))
            .expect("the winner holds its own migration-2 frame");
        // The winner's v2 frame is the last frame in its store, so it is the cut
        // boundary of the checkpoint written below; the loser's v2 frame must sort
        // above it (uncovered, above the cut) for the store to be able to hold
        // only the loser's frame.
        assert_eq!(
            winner_v2.frame.hash,
            frames.last().expect("non-empty store").frame.hash,
            "the winner's v2 frame must be its last frame (the checkpoint boundary)"
        );
        winner_v2.frame.lamport
    };
    let loser_v2_lamport = {
        let store = sharepoint_db::store::open(&loser.state.join("store.db")).unwrap();
        let frames = sharepoint_db::store::all_frames(&store, &loser.db_id()).unwrap();
        frames
            .iter()
            .find(|f| matches!(f.frame.kind, sharepoint_db::FrameKind::Migration(2)))
            .expect("the loser holds its own migration-2 frame")
            .frame
            .lamport
    };
    assert!(
        loser_v2_lamport > winner_v2_lamport,
        "the loser's v2 frame must sort above the winner's (the checkpoint boundary), \
         got {loser_v2_lamport} vs {winner_v2_lamport}"
    );

    // The winner writes a checkpoint whose cut folds in its own v2 frame (and
    // everything below it) but not the loser's.
    let mut winner = reopen_with_checkpoint_threshold(winner, 1);
    winner.sync();
    assert!(
        !ckpoints(&winner).is_empty(),
        "the winner wrote a checkpoint"
    );

    // A third node bootstraps from that checkpoint with an empty store: its
    // `data.db` is the winner's snapshot (schema v2, winner SQL recorded), and its
    // store holds no migration frame at all.
    let mut dave = bootstrap_from_checkpoint(tmp.path(), "dave", &winner);
    assert!(is_bootstrapped(&dave), "dave must be bootstrapped");
    assert_eq!(dave.db.schema_version(), 2);

    // Dave then ingests the loser's segment: the loser's v2 frame sorts above the
    // cut, so it lands in dave's store — which now holds ONLY the losing frame.
    FakeSync::deliver(&loser, &dave);
    dave.sync();
    let store = sharepoint_db::store::open(&dave.state.join("store.db")).unwrap();
    assert_eq!(
        sharepoint_db::store::migration_sql_for_version(&store, 2)
            .unwrap()
            .as_deref(),
        Some(sql_loser),
        "dave's store must hold only the losing migration-2 frame — the premise of the bug"
    );
    drop(store);

    // Re-running the WINNER's SQL is AlreadyApplied: dave's `data.db` (the
    // snapshot) recorded the winner's SQL, and that is what the comparison must
    // use. With the store-first order this wrongly returned `MigrationMismatch`.
    assert_eq!(
        dave.db.migrate(2, sql_winner).unwrap(),
        sharepoint_db::MigrateOutcome::AlreadyApplied,
        "the winner's SQL is the applied one, even though it is below dave's cut"
    );
    // Re-running the LOSER's SQL — the very frame dave's store holds — is a
    // mismatch, never AlreadyApplied. With the store-first order this wrongly
    // returned `AlreadyApplied`.
    let err = dave.db.migrate(2, sql_loser).unwrap_err();
    assert!(
        matches!(err, sharepoint_db::Error::MigrationMismatch { version: 2 }),
        "the loser's SQL must mismatch against the winner's, got {err}"
    );
}
