//! Corrupt on-disk data is reported as an error, never a panic.
//!
//! The segment codec used to index a `u32` count straight into the buffer and
//! allocate from an unvalidated header field, and `ShareDb::open` converted the
//! `MANIFEST`'s `db_id` with `slice::try_into`, which panics on a short value.

use std::path::Path;

use sharepoint_db::{Error, OpenOptions, ShareDb};

mod common;
use common::Node;

/// The segment magic, as written by [`sharepoint_db::encode_segment`].
const MAGIC: &[u8] = b"SDBSEG01";

fn header(db_id: [u8; 16], node_id: [u8; 8], first_seq: u64, count: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&db_id);
    bytes.extend_from_slice(&node_id);
    bytes.extend_from_slice(&first_seq.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes
}

/// Reopens a folder with its own state directory, expecting failure.
fn expect_open_fails(folder: &Path, state: &Path, what: &str) -> Error {
    ShareDb::open(
        folder,
        OpenOptions {
            state_dir: Some(state.to_path_buf()),
            ..Default::default()
        },
    )
    .err()
    .unwrap_or_else(|| panic!("{what} must fail the open"))
}

/// A header claiming far more frames than the file holds is rejected before the
/// frames are allocated or read.
#[test]
fn segment_count_beyond_the_file_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let node = Node::create(tmp.path(), "a");
    let (db_id, node_id) = (node.db.db_id(), node.db.node_id());
    let err = sharepoint_db::decode_segment(&header(db_id, node_id, 1, u32::MAX), &db_id)
        .expect_err("a count past the end of the file must be rejected");
    assert!(
        matches!(err, Error::CorruptSegment(_)),
        "expected CorruptSegment, got {err:?}"
    );
}

/// A header that promises a frame the file does not contain is an error, not a
/// panic.
#[test]
fn segment_missing_its_only_frame_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let node = Node::create(tmp.path(), "a");
    let (db_id, node_id) = (node.db.db_id(), node.db.node_id());
    let err = sharepoint_db::decode_segment(&header(db_id, node_id, 1, 1), &db_id)
        .expect_err("a segment with no frame body must be rejected");
    assert!(
        matches!(err, Error::CorruptSegment(_)),
        "expected CorruptSegment, got {err:?}"
    );
}

/// A segment shorter than its fixed header is an error.
#[test]
fn segment_shorter_than_its_header_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let node = Node::create(tmp.path(), "a");
    let db_id = node.db.db_id();
    let err = sharepoint_db::decode_segment(b"SDBSEG0", &db_id)
        .expect_err("a segment shorter than the header must be rejected");
    assert!(
        matches!(err, Error::CorruptSegment(_)),
        "expected CorruptSegment, got {err:?}"
    );
}

/// Truncating a real segment mid-frame is an error, not a panic, and a sync
/// ignores the file instead of ingesting a partial frame.
#[test]
fn truncated_segment_is_ignored_by_a_sync() {
    let tmp = tempfile::tempdir().unwrap();
    let mut alice = Node::create(tmp.path(), "a");
    alice.db.migrate(1, common::MIGRATE_T).expect("migrate");
    alice.insert(1, "x");
    alice.db.sync().expect("sync");
    let bob = Node::join(tmp.path(), "b", &alice);
    // bob's folder copy holds alice's segments, but bob has not read them yet.
    common::FakeSync::deliver(&alice, &bob);
    let dir = bob.folder.join("segments");
    assert!(
        std::fs::read_dir(&dir).unwrap().count() > 0,
        "segments copied"
    );
    let mut names = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "seg") {
            let mut bytes = std::fs::read(&path).unwrap();
            bytes.truncate(bytes.len() - 3);
            std::fs::write(&path, &bytes).unwrap();
            names.push(path.file_name().unwrap().to_owned());
        }
    }

    let mut bob = bob;
    let report = bob.db.sync().expect("sync must not fail");
    let ignored: Vec<_> = report
        .ignored_files
        .iter()
        .filter_map(|p| p.file_name().map(|n| n.to_owned()))
        .collect();
    for name in &names {
        assert!(
            ignored.contains(name),
            "{name:?} must be ignored, got {ignored:?}"
        );
    }
    assert_eq!(report.ingested, 0, "nothing may be ingested");
    assert_eq!(
        bob.db.schema_version(),
        0,
        "the migration in the truncated segment may not have been applied"
    );
}

/// A frame count of zero is rejected at encode time instead of producing a
/// segment no reader can decode.
#[test]
fn encoding_no_frames_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let node = Node::create(tmp.path(), "a");
    let err = sharepoint_db::encode_segment(&node.db.db_id(), &node.db.node_id(), &[])
        .expect_err("an empty segment is not encodable");
    assert!(
        matches!(err, Error::CorruptSegment(_)),
        "expected CorruptSegment, got {err:?}"
    );
}

/// A checkpoint that does not decode is reported as `CorruptCheckpoint`, not a
/// panic and not a catch-all error kind.
#[test]
fn corrupt_checkpoint_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let folder = tmp.path().join("a.sharedb");
    let state = tmp.path().join("a.state");
    std::fs::create_dir_all(&folder).unwrap();
    let mut db = ShareDb::open(
        &folder,
        OpenOptions {
            create: true,
            state_dir: Some(state.clone()),
            checkpoint_threshold: 1,
            ..Default::default()
        },
    )
    .expect("open");
    db.migrate(1, common::MIGRATE_T).expect("migrate");
    for i in 1..=4 {
        db.execute(
            "INSERT INTO t (id, v) VALUES (?1, ?2)",
            sharepoint_db::rusqlite::params![i, format!("v{i}")],
        )
        .expect("insert");
        db.sync().expect("sync");
    }
    let dir = folder.join("ckpoint");
    let names: Vec<_> = std::fs::read_dir(&dir)
        .expect("the folder holds checkpoints")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    assert!(
        !names.is_empty(),
        "compaction must have written a checkpoint"
    );
    let db_id = db.db_id();

    for path in names {
        let good = std::fs::read(&path).unwrap();
        // A wrong magic is a corrupt checkpoint.
        let mut magic = good.clone();
        magic[0] ^= 0xff;
        assert!(
            matches!(
                sharepoint_db::checkpoint::decode(&magic, &db_id),
                Err(Error::CorruptCheckpoint(_))
            ),
            "a flipped magic byte must be CorruptCheckpoint"
        );
        // Truncating the file is a corrupt checkpoint, not a panic.
        let short = &good[..good.len() / 2];
        assert!(
            matches!(
                sharepoint_db::checkpoint::decode(short, &db_id),
                Err(Error::CorruptCheckpoint(_))
            ),
            "a truncated checkpoint must be CorruptCheckpoint"
        );
        // A truncated version-vector length field must not allocate wildly.
        // The version vector starts right after the fixed header.
        const VV_AT: usize = 8 + 16 + 8 + 8 + 8 + 8 + 8 + 32;
        let mut vv_len = good.clone();
        for byte in vv_len.iter_mut().skip(VV_AT).take(4) {
            *byte = 0xff;
        }
        assert!(
            matches!(
                sharepoint_db::checkpoint::decode(&vv_len, &db_id),
                Err(Error::CorruptCheckpoint(_))
            ),
            "a huge version-vector length must be CorruptCheckpoint"
        );
    }
}

/// A `MANIFEST` whose `db_id` field has the wrong length is an error, not a
/// panic.
#[test]
fn manifest_with_wrong_db_id_length_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let node = Node::create(tmp.path(), "a");
    let (folder, state) = (node.folder.clone(), node.state.clone());
    drop(node);
    std::fs::write(folder.join("MANIFEST"), "sharedb-format=1\ndb_id=abcd\n").unwrap();
    let err = expect_open_fails(&folder, &state, "a truncated db_id");
    assert!(
        matches!(err, Error::Manifest(_)),
        "expected Manifest, got {err:?}"
    );
}

/// A `MANIFEST` with an unknown line is an error.
#[test]
fn manifest_with_unknown_line_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let node = Node::create(tmp.path(), "a");
    let (folder, state) = (node.folder.clone(), node.state.clone());
    drop(node);
    std::fs::write(
        folder.join("MANIFEST"),
        "sharedb-format=1\ndb_id=00000000000000000000000000000000\nbogus=1\n",
    )
    .unwrap();
    let err = expect_open_fails(&folder, &state, "an unknown manifest line");
    assert!(
        matches!(err, Error::Manifest(_)),
        "expected Manifest, got {err:?}"
    );
}

/// Creating a database publishes its `MANIFEST` through a temp file and leaves
/// no `*.tmp` behind.
#[test]
fn manifest_publish_leaves_no_temp_file() {
    let tmp = tempfile::tempdir().unwrap();
    let mut node = Node::create(tmp.path(), "a");
    let folder = node.folder.clone();
    node.db
        .migrate(1, common::MIGRATE_T)
        .expect("migrate must apply");
    node.insert(1, "x");
    drop(node);
    let leftovers: Vec<_> = std::fs::read_dir(&folder)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .map(|n| n.to_string_lossy().ends_with(".tmp"))
                .unwrap_or(false)
        })
        .collect();
    assert!(
        leftovers.is_empty(),
        "temp files left behind: {leftovers:?}"
    );
}
