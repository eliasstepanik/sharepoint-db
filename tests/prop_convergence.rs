//! Property-based convergence: random interleavings of commits, syncs and
//! hostile deliveries across two or three nodes must always quiesce into one
//! single logical state.
//!
//! The universe is deliberately tiny — one table with a handful of keys and
//! values — so that a random run is likely to produce same-key concurrent
//! writes, while the *interleaving* is what the generator explores: commits,
//! `sync()` calls and file deliveries in any order, through a sync client that
//! may reorder, delay, duplicate as `(conflicted copy …)` or half-write files.

mod common;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestError, TestRunner};

use common::{conflicted_copy_name, install_hostile_fs, Node};
use sharepoint_db::folder::segment_valid;
use sharepoint_db::{Dump, MaterializeKind, OpenOptions, ShareDb};

/// Keys the generated writes choose from (`1..=KEYS`).
const KEYS: u8 = 4;
/// Distinct values the generated writes choose from.
const VALUES: u8 = 3;
/// Rounds of "deliver everything, sync everyone" before a run is declared
/// un-quiescible.
const QUIESCE_ROUNDS: usize = 8;

/// What one generated commit does to one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    /// `INSERT` or overwrite with this value.
    Upsert(u8),
    /// `DELETE` the key.
    Delete,
}

/// How the fake sync client behaves for one delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Style {
    /// Copy every missing segment, in name order.
    Plain,
    /// Copy every missing segment, in reverse name order.
    Reversed,
    /// Copy only the first half of the missing segments; the rest are delayed.
    Delayed,
    /// Deliver a segment only under a `(conflicted copy …)` name.
    Conflicted,
    /// Deliver a truncated copy now and complete it on a later delivery.
    Truncated,
}

/// One step of a generated run.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    /// `node` commits a change to `key`.
    Commit {
        node: usize,
        key: u8,
        action: Action,
    },
    /// `node` calls `sync()`; `flaky` arms one doomed folder write first.
    Sync { node: usize, flaky: bool },
    /// The sync client moves files from one node's folder to another's.
    Deliver {
        from: usize,
        to: usize,
        style: Style,
    },
}

/// The delivery styles a generated delivery may pick. `Plain` appears twice so
/// the honest case stays the common one.
const STYLES: [Style; 6] = [
    Style::Plain,
    Style::Plain,
    Style::Reversed,
    Style::Delayed,
    Style::Conflicted,
    Style::Truncated,
];

/// What one generated run tracked, for the vacuity guards.
#[derive(Debug, Default)]
struct Stats {
    /// Commits that hit a key another node had written since its last sync.
    same_key_writes: usize,
    /// Syncs that answered [`MaterializeKind::Rebuilt`].
    rebuilds: usize,
    /// Files the sync client renamed to a `(conflicted copy …)`.
    conflicted: usize,
    /// Files that were delivered truncated and completed on a later round.
    truncated: usize,
    /// Files a [`Style::Delayed`] delivery held back for a later round.
    delayed: usize,
    /// Syncs that published or ingested at least one frame.
    synced: usize,
}

/// One generated run: `n` nodes, their hostile folder handles, and the state the
/// guards need (which keys each node wrote since its last sync, half-written
/// files still waiting for the rest of their bytes).
struct Harness {
    /// Kept alive so the folders are not deleted mid-run.
    _tmp: tempfile::TempDir,
    nodes: Vec<Node>,
    /// One [`HostileFs`] per node, so a sync can be made to lose one write.
    hostile: Vec<std::sync::Arc<common::HostileFs>>,
    /// Keys each node changed since its own last sync: two nodes holding the
    /// same key at the same time is a concurrent write.
    dirty: Vec<HashSet<u8>>,
    /// `(destination path, complete bytes)` for a file a [`Style::Truncated`]
    /// delivery left half-written; the next delivery finishes the upload.
    half: Vec<(PathBuf, Vec<u8>)>,
    /// Counter for the scratch state directories of the cold rebuilds.
    rebuild_seq: std::cell::Cell<usize>,
    stats: Stats,
}

impl Harness {
    /// Brings up `n` separate machines: node 0 creates the database, the others
    /// join with only the `MANIFEST` in their folder copy.
    fn new(n: usize) -> Harness {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut nodes = vec![Node::create(tmp.path(), "node0")];
        for i in 1..n {
            nodes.push(Node::join(tmp.path(), &format!("node{i}"), &nodes[0]));
        }
        let hostile = nodes.iter_mut().map(install_hostile_fs).collect::<Vec<_>>();
        Harness {
            _tmp: tmp,
            nodes,
            hostile,
            dirty: (0..n).map(|_| HashSet::new()).collect(),
            half: Vec::new(),
            rebuild_seq: std::cell::Cell::new(0),
            stats: Stats::default(),
        }
    }

    /// One generated commit. A node that has no table yet migrates it itself, so
    /// its frames carry low lamport clocks and sort *inside* a peer's already
    /// applied prefix — the situation that forces a rebuild.
    fn commit(&mut self, node: usize, key: u8, action: Action) {
        if self.nodes[node].db.schema_version() == 0 {
            self.nodes[node].migrate1();
        }
        // Two nodes holding the same key dirty at the same time wrote it without
        // an intervening sync between them: that is the concurrent write.
        if self
            .dirty
            .iter()
            .enumerate()
            .any(|(i, keys)| i != node && keys.contains(&key))
        {
            self.stats.same_key_writes += 1;
        }
        self.dirty[node].insert(key);
        let id = i64::from(key);
        let db = &mut self.nodes[node].db;
        match action {
            Action::Upsert(v) => {
                let value = format!("v{v}");
                let changed = db
                    .execute(
                        "UPDATE t SET v = ?2 WHERE id = ?1",
                        sharepoint_db::rusqlite::params![id, value],
                    )
                    .expect("update");
                if changed == 0 {
                    self.nodes[node].insert(id, &value);
                }
            }
            Action::Delete => {
                db.execute(
                    "DELETE FROM t WHERE id = ?1",
                    sharepoint_db::rusqlite::params![id],
                )
                .expect("delete");
            }
        }
    }

    /// `node` calls `sync()`. With `flaky` set, the folder refuses the first
    /// publish attempt with a sharing violation, which the retry policy absorbs.
    fn sync(&mut self, node: usize, flaky: bool) {
        if flaky {
            self.hostile[node].fail_first_writes(1);
        }
        let report = self.nodes[node].sync();
        self.stats.synced += (report.published + report.ingested > 0) as usize;
        if report.materialize == MaterializeKind::Rebuilt {
            self.stats.rebuilds += 1;
        }
        // Everything this node knew is published now (or will be on the next
        // sync), so its writes are no longer "concurrent" with a peer's.
        self.dirty[node].clear();
    }

    /// The fake sync client moves files from one folder to another, in the mood
    /// `style` says. It never calls `sync()` — a real client does not either.
    fn deliver(&mut self, from: usize, to: usize, style: Style) {
        let dst = self.nodes[to].segments_dir();
        // A half-written file from an earlier round gets its missing bytes now:
        // that is an upload that simply took two rounds to finish.
        let mut finished = Vec::new();
        self.half.retain_mut(|(path, bytes)| {
            if path.parent() != Some(dst.as_path()) {
                return true;
            }
            if fs::write(&*path, &**bytes).is_ok() {
                finished.push(path.clone());
                false
            } else {
                true
            }
        });
        for _ in finished {
            self.stats.truncated += 1;
        }

        if from == to {
            return;
        }
        let src = self.nodes[from].segments_dir();
        fs::create_dir_all(&dst).expect("segments dir");
        let mut names: Vec<String> = fs::read_dir(&src)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names.retain(|name| name.ends_with(".seg"));
        if style == Style::Reversed {
            names.reverse();
        }
        // `Delayed` only gets through with the first half this round; the rest
        // arrives with the next delivery between the same pair.
        if style == Style::Delayed {
            let keep = names.len().div_ceil(2);
            self.stats.delayed += names.len() - keep;
            names.truncate(keep);
        }
        for name in names {
            let bytes = match fs::read(src.join(&name)) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            match style {
                Style::Conflicted => {
                    // The client could not merge the file and renamed it. The
                    // bytes are the same, so the ingest path (which reads files
                    // by content, never by name) still gets them.
                    let target = dst.join(conflicted_copy_name(&name));
                    if !target.exists() && fs::write(&target, &bytes).is_ok() {
                        self.stats.conflicted += 1;
                    }
                }
                Style::Truncated => {
                    // A partial upload: the name exists but the file is not a
                    // valid segment yet, so the sync ignores it and completes it
                    // on a later round.
                    let target = dst.join(&name);
                    if !target.exists() && !bytes.is_empty() {
                        let cut = bytes.len() / 2;
                        if fs::write(&target, &bytes[..cut]).is_ok() {
                            self.half.push((target, bytes));
                        }
                    }
                }
                _ => {
                    let target = dst.join(&name);
                    if !target.exists() {
                        let _ = fs::copy(src.join(&name), target);
                    }
                }
            }
        }
    }

    /// Makes every copy of every segment byte-identical to the copy its publisher
    /// uploaded, which is precisely the promise a sync client makes.
    ///
    /// The bytes for a name are the truth wherever a valid copy of them exists:
    /// a file that decodes as a segment for this database is authoritative for its
    /// name, and a copy that differs (a half-written upload, a file a careless
    /// client truncated) is replaced with it. A real client proves two copies
    /// differ by hash and then downloads the publisher's version; the folder is
    /// write-once for *us*, not for the client that owns it.
    fn reconcile_folder(&mut self) {
        let db_id = self.nodes[0].db_id();
        let mut good: HashMap<String, Vec<u8>> = HashMap::new();
        for node in &self.nodes {
            for name in node.segments() {
                if let Ok(bytes) = fs::read(node.segments_dir().join(&name)) {
                    if segment_valid(&bytes, &db_id) {
                        good.insert(name, bytes);
                    }
                }
            }
        }
        for node in &self.nodes {
            for name in node.segments() {
                let Some(bytes) = good.get(&name) else {
                    continue;
                };
                let path = node.segments_dir().join(&name);
                let differs = fs::read(&path).map(|now| &now != bytes).unwrap_or(true);
                if differs {
                    let _ = fs::write(&path, bytes);
                }
            }
        }
    }

    /// Delivers every segment between every pair of nodes and syncs everyone,
    /// until a full round brings nothing new anywhere.
    ///
    /// Returns the number of rounds it took. A run that still reports new frames
    /// after [`QUIESCE_ROUNDS`] rounds has not converged and the caller fails.
    fn quiesce(&mut self) -> usize {
        for round in 1..=QUIESCE_ROUNDS {
            let mut moved = false;
            self.reconcile_folder();
            for from in 0..self.nodes.len() {
                for to in 0..self.nodes.len() {
                    self.deliver(from, to, Style::Plain);
                }
            }
            for node in 0..self.nodes.len() {
                let report = self.nodes[node].sync();
                self.stats.synced += (report.published + report.ingested > 0) as usize;
                self.stats.rebuilds += (report.materialize == MaterializeKind::Rebuilt) as usize;
                if report.ingested > 0 || report.published > 0 || report.publish_pending {
                    moved = true;
                }
            }
            if !moved {
                return round;
            }
        }
        QUIESCE_ROUNDS
    }

    /// Every node's dump, after the run has quiesced.
    fn dumps(&self) -> Vec<Dump> {
        self.nodes.iter().map(|n| n.dump()).collect()
    }

    /// A cold rebuild of `node`'s own store: its `store.db` is copied into a
    /// fresh state directory and replayed into a brand-new `data.db`.
    ///
    /// Nothing about the node is disturbed — the copy is opened under its own
    /// state directory, so the live handle keeps its lock — which means this can
    /// be checked after every operation rather than only at the end.
    fn cold_rebuild_dump(&self, node: usize) -> Dump {
        let seq = self.rebuild_seq.get();
        self.rebuild_seq.set(seq + 1);
        let scratch = self._tmp.path().join(format!("rebuild-{seq}"));
        fs::create_dir_all(&scratch).expect("scratch state dir");
        fs::copy(
            self.nodes[node].state.join("store.db"),
            scratch.join("store.db"),
        )
        .expect("copy store.db");
        let rebuilt = ShareDb::open(
            &self.nodes[node].folder,
            OpenOptions {
                create: false,
                state_dir: Some(scratch),
                fault: None,
                ..Default::default()
            },
        )
        .expect("cold rebuild from the copied store");
        rebuilt.dump().expect("dump after cold rebuild")
    }
}

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// `Upsert` most of the time, `Delete` now and then.
fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        (1..=VALUES).prop_map(Action::Upsert),
        (1..=VALUES).prop_map(Action::Upsert),
        Just(Action::Delete),
    ]
}

/// One operation: a commit, a sync or a delivery, each with its own arguments.
fn op(n: usize) -> impl Strategy<Value = Op> {
    let node = 0..n;
    prop_oneof![
        (node.clone(), 1..=KEYS, action()).prop_map(|(node, key, action)| Op::Commit {
            node,
            key,
            action
        }),
        (node.clone(), any::<bool>()).prop_map(|(node, flaky)| Op::Sync { node, flaky }),
        (node.clone(), node, 0..STYLES.len()).prop_map(|(from, to, style)| Op::Deliver {
            from,
            to,
            style: STYLES[style],
        }),
    ]
}

/// A whole run: every node commits once up front (so every node has a migration
/// and a change of its own to publish), then a random interleaving.
fn run_ops(n: usize) -> impl Strategy<Value = Vec<Op>> {
    let warmup: Vec<Op> = (0..n)
        .map(|node| Op::Commit {
            node,
            key: 1,
            action: Action::Upsert(1),
        })
        .collect();
    prop::collection::vec(op(n), 8..=26).prop_map(move |ops| {
        let mut all = warmup.clone();
        all.extend(ops);
        all
    })
}

// ---------------------------------------------------------------------------
// One case
// ---------------------------------------------------------------------------

/// What a case asserts once its operations have run and the cluster quiesced.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Property {
    /// Property 1: every node's dump is identical.
    Converge,
    /// Property 2: each node's materialised `data.db` equals a fresh full replay
    /// of its own store.
    Rebuild,
}

/// Everything one case produced, for the vacuity guards.
struct Observed {
    stats: Stats,
    /// True when the final dump held at least one row.
    non_empty: bool,
}

/// Runs the operations, quiesces and checks `property`.
///
/// For [`Property::Rebuild`] the check also runs after every commit and sync, so
/// a divergence is caught at the operation that caused it rather than only after
/// the whole run.
fn check(n: usize, ops: &[Op], property: Property) -> Result<Observed, TestCaseError> {
    let mut h = Harness::new(n);
    for op in ops {
        match *op {
            Op::Commit { node, key, action } => h.commit(node, key, action),
            Op::Sync { node, flaky } => h.sync(node, flaky),
            Op::Deliver { from, to, style } => h.deliver(from, to, style),
        }
        if property == Property::Rebuild {
            if let Op::Commit { node, .. } | Op::Sync { node, .. } = *op {
                let live = h.nodes[node].dump();
                let rebuilt = h.cold_rebuild_dump(node);
                prop_assert_eq!(
                    &live,
                    &rebuilt,
                    "node {}: data.db diverged from a cold rebuild right after {:?}",
                    node,
                    op
                );
            }
        }
    }

    let rounds = h.quiesce();
    prop_assert!(
        rounds < QUIESCE_ROUNDS,
        "quiescing never settled: {} rounds without a quiet round",
        QUIESCE_ROUNDS
    );
    let dumps = h.dumps();
    match property {
        Property::Converge => {
            for (i, dump) in dumps.iter().enumerate() {
                prop_assert_eq!(
                    &dumps[0],
                    dump,
                    "node {} disagrees with node 0 after {} quiesce round(s)",
                    i,
                    rounds
                );
            }
        }
        Property::Rebuild => {
            for (i, dump) in dumps.iter().enumerate() {
                let rebuilt = h.cold_rebuild_dump(i);
                prop_assert_eq!(
                    dump,
                    &rebuilt,
                    "node {}: the materialised data.db differs from a full replay \
                     of its own store",
                    i
                );
            }
        }
    }
    Ok(Observed {
        non_empty: dumps
            .iter()
            .any(|dump| dump.tables.iter().any(|(_, rows)| !rows.is_empty())),
        stats: h.stats,
    })
}

// ---------------------------------------------------------------------------
// Vacuity guards
// ---------------------------------------------------------------------------

/// Counted across the cases of one test, so a property that would pass because
/// nothing interesting ever happened is caught instead of celebrated.
#[derive(Debug, Default)]
struct Guards {
    /// Cases with at least one same-key concurrent write (two nodes wrote the
    /// same key with no sync in between).
    same_key_cases: usize,
    /// Cases with at least one `Rebuilt` materialisation.
    rebuilt_cases: usize,
    /// Cases whose final dump held at least one row.
    non_empty_cases: usize,
    /// Cases that actually exchanged a frame.
    synced_cases: usize,
    /// Cases that exercised a conflicted copy, a truncation or a delay.
    hostile_cases: usize,
    cases: usize,
}

impl Guards {
    fn observe(&mut self, o: &Observed) {
        self.cases += 1;
        self.same_key_cases += (o.stats.same_key_writes > 0) as usize;
        self.rebuilt_cases += (o.stats.rebuilds > 0) as usize;
        self.non_empty_cases += o.non_empty as usize;
        self.synced_cases += (o.stats.synced > 0) as usize;
        self.hostile_cases +=
            ((o.stats.conflicted + o.stats.truncated + o.stats.delayed) > 0) as usize;
    }

    /// Fails when the generated cases never exercised the interesting situations,
    /// i.e. when the property that just passed would have passed vacuously.
    fn assert_not_vacuous(&self, what: &str) {
        assert!(self.cases > 0, "{what}: no cases ran");
        assert!(
            self.same_key_cases > 0,
            "{what}: 0/{cases} cases had a same-key concurrent write, so a real \
             conflict was never tested",
            cases = self.cases
        );
        assert!(
            self.rebuilt_cases > 0,
            "{what}: 0/{cases} cases rebuilt data.db, so the rebuild path was \
             never exercised",
            cases = self.cases
        );
        assert!(
            self.non_empty_cases > 0,
            "{what}: no case ended with a non-empty dump — equal empty dumps prove nothing"
        );
        assert!(
            self.synced_cases > 0,
            "{what}: no case ever published or ingested a frame"
        );
        assert!(
            self.hostile_cases > 0,
            "{what}: no case exercised a hostile delivery (conflicted copy, \
             truncation or delay)"
        );
    }
}

// ---------------------------------------------------------------------------
// The driver
// ---------------------------------------------------------------------------

/// Runs `n` nodes over the generated interleavings, checks `property` on every
/// case and the vacuity guards over the whole set.
///
/// A `proptest!` test cannot assert anything *after* its last case, which is what
/// a vacuity guard needs, so the runner is driven by hand: the counters live in a
/// [`RefCell`] the case closure updates, and the guard check runs once the run is
/// over. `Config::default()` keeps the usual case count (256, unless
/// `PROPTEST_CASES` says otherwise) and the usual failure persistence, so a
/// shrinking failure is still recorded under `proptest-regressions/`.
fn drive(n: usize, property: Property, what: &str) {
    let config = Config {
        // The case body is filesystem and SQLite work; forking would re-exec the
        // test binary for every single case.
        fork: false,
        timeout: 0,
        ..Config::default()
    };

    let guards = RefCell::new(Guards::default());
    let mut runner = TestRunner::new(config);
    let result = runner.run(&run_ops(n), |ops| {
        let observed = check(n, &ops, property)?;
        guards.borrow_mut().observe(&observed);
        Ok(())
    });

    match result {
        Ok(()) => {}
        Err(TestError::Fail(reason, ops)) => {
            panic!("prop_convergence: {what} failed: {reason}\nops: {ops:#?}")
        }
        Err(TestError::Abort(reason)) => {
            panic!("prop_convergence: {what} aborted: {reason}")
        }
    }
    guards.borrow().assert_not_vacuous(what);
    let g = guards.borrow();
    eprintln!(
        "[guards:{what}] cases={} same_key={} rebuilt={} non_empty={} synced={} hostile={}",
        g.cases,
        g.same_key_cases,
        g.rebuilt_cases,
        g.non_empty_cases,
        g.synced_cases,
        g.hostile_cases
    );
}

/// Property 1 over two nodes: any interleaving of commits, syncs and hostile
/// deliveries quiesces into one identical dump on every node.
#[test]
fn prop_any_interleaving_converges_two_nodes() {
    drive(2, Property::Converge, "convergence, two nodes")
}

/// Property 1 over three nodes: more interleavings, and a frame can reach a node
/// only through a peer that received it late.
#[test]
fn prop_any_interleaving_converges_three_nodes() {
    drive(3, Property::Converge, "convergence, three nodes")
}

/// Property 2 over two nodes: the incrementally maintained `data.db` always
/// equals a fresh full replay of the same store — checked after every commit and
/// sync, not only at the end.
#[test]
fn prop_incremental_equals_rebuild_two_nodes() {
    drive(2, Property::Rebuild, "incremental vs rebuild, two nodes")
}

/// Property 2 over three nodes.
#[test]
fn prop_incremental_equals_rebuild_three_nodes() {
    drive(3, Property::Rebuild, "incremental vs rebuild, three nodes")
}
