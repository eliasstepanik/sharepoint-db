# sharepoint-db

A multi-writer SQL database that lives in a **OneDrive / SharePoint-synced
folder**.

The engine is plain SQLite (`rusqlite`, bundled build), so the data is real
SQLite data and `sharedb export` hands you a real `.db` file. On top of it sits
a CRDT-style **delta log**: every committed change is captured as a SQLite
session changeset, appended to a per-node hash-chained log, and published into
the synced folder as a *write-once* segment file. Merging is the deterministic
replay of every known frame in one total order, `(lamport, node_id, seq)`, so
two writers who never talk to each other end up with the same data.

```text
shared/            <- the synced folder: only write-once files
  MANIFEST         <- sharedb-format=1 + db_id
  segments/        <- <node>-<first>-<last>.seg, one file per published run
  ckpoint/         <- snapshot checkpoints (compaction)
alice.state/       <- local, never synced: LOCK, store.db, data.db
bob.state/         <- a second writer on the same folder
```

## Why not just one `.db` file?

Because sync clients do not merge. Two people edit the same file and OneDrive
does not resolve the difference — it keeps one copy, reverts the other and
parks a `name (conflicted copy …)` file next to it. For a SQLite database that
is not a merge, it is a fork with a misleading name: one writer's committed
transactions silently disappear.

So the folder never holds a mutable database. It holds **write-once segment
files** (each one created by an atomic rename and never modified afterwards)
plus **checkpoints** that bound how many of them survive. Two nodes can never
disagree about the same file, so there is nothing for the sync client to
conflict over; the merge happens in `sync`, which replays every frame it knows
in total order. "Write-once" is the *creation* rule — a file is published by an
atomic rename and never edited in place — not a promise that files live forever:
compaction deletes segments below a checkpoint's boundary, and it also garbage-
collects superseded **checkpoints** (the best one and the one before it are
kept, so a node one cut behind still finds a usable snapshot). When you want an
ordinary database file — for a report, a backup, a tool that has never heard of
this crate — `sharedb export` writes one with SQLite's online backup API.

## Quick start

Two writers, one folder, two state directories. The state directory holds this
machine's node identity and frame log, so it must be *outside* the synced
folder (the library refuses it otherwise).

```sh
export DB=~/OneDrive/team.sharedb

# Alice
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/alice init
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/alice \
  migrate 1 "CREATE TABLE task (id TEXT PRIMARY KEY, title TEXT NOT NULL);"
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/alice \
  exec "INSERT INTO task (id, title) VALUES ('7f3c1a', 'write docs')"

# Bob, on his own machine, after the folder reached him
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/bob init
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/bob \
  migrate 1 "CREATE TABLE task (id TEXT PRIMARY KEY, title TEXT NOT NULL);"
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/bob \
  exec "INSERT INTO task (id, title) VALUES ('b92d04', 'ship it')"

# Both merge, then read
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/alice sync
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/bob sync
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/bob \
  query "SELECT id, title FROM task ORDER BY id"
sharedb --db "$DB" --state-dir ~/.local/share/sharedb/bob export snapshot.db
```

`--db` and `--state-dir` also read `SHAREDB_PATH` and `SHAREDB_STATE_DIR`, so a
shell with them exported only needs `sharedb query "…"`.

Subcommands: `init`, `migrate <version> <sql>`, `exec <sql> [--no-sync]`,
`query <sql> [--tsv]`, `sync`, `status`, `export <out.db>`. `exec` syncs after
the write unless you pass `--no-sync`; compaction is automatic inside `sync`.
Exit code is `0` on success and non-zero on any error, with the message on
stderr.

## Merge semantics

Frames are replayed in the total order `(lamport, node_id, seq)`. "Later"
below always means later in that order; the last writer in that order wins.

| Concurrent situation | Result |
| --- | --- |
| Two updates to the **same cell** | Last writer wins (LWW by `(lamport, node)`). |
| Two updates to **different columns** of one row | Both survive: a changeset carries only the columns it changed. |
| **Delete** vs a concurrent **update** of the same row | Delete wins. If the delete replays first the update finds no row and is dropped; if it replays second it removes the row. |
| Two **inserts with the same primary key** | The later one replaces the earlier one. |
| A merged change that **violates a constraint** (UNIQUE, NOT NULL, CHECK) | That change is dropped, the rest of the frame is applied, and the drop is counted. |
| A change to a **table this node does not have yet** | Skipped until the migration arrives; the frame is replayed once the table exists. |
| Two **migrations with the same version** | The one that sorts first is applied; the duplicate is skipped. Different SQL for one version is an error, never a silent choice. |
| The same frame seen **twice** | Idempotent: content-addressed frames are recognised and skipped. |
| A frame that **disagrees** with one already stored at its `(node, seq)` | A fork: refused, reported in `forks`, never applied. |

## Key guidance: use text keys

Give every table a **text primary key** and mint each id from randomness —
`uuidgen`, a timestamp plus a node name, or `sharepoint_db::new_id()` (16
random hex characters) when you use the library:

```sql
CREATE TABLE task (id TEXT PRIMARY KEY, title TEXT NOT NULL);
INSERT INTO task (id, title) VALUES ('9f2c4b8e1a7d3056', 'write docs');
```

Library users can also register the SQL function `sharedb_new_id()` on a
connection with `sharepoint_db::register_functions(&conn)` and call it from
plain SQL.

Do **not** use `INTEGER PRIMARY KEY` rowids for rows two nodes might create at
the same time. Rowids are chosen locally, so two offline nodes both pick `1`,
and the merge turns that into a same-PK collision where one row replaces the
other. Text ids minted from randomness cannot collide. (`INTEGER PRIMARY KEY`
is still fine for a table only one node ever inserts into, and for foreign keys
pointing at such a table.)

## Limits (v1)

- **No encryption.** The folder is as private as your OneDrive/SharePoint
  library, and segment files are readable by anyone with access to it.
- **No counter or intent merge.** Concurrent arithmetic is last-writer-wins on
  the stored value: `n = n + 1` on two nodes that both read `n = 5` converges
  to `6`, not `7`. Model counters as insert-only rows and count them.
- **No triggers, and every table needs a PRIMARY KEY.** `WITHOUT ROWID` tables
  are refused too. A migration that produces such a schema rolls back and is
  not journaled.
- **Foreign keys are enforced on local writes only.** A merged frame is applied
  with foreign keys off — a peer's insert can legitimately arrive before the
  row it references — so cross-node FK violations are possible and are not
  repaired afterwards.
- **Compaction retention.** The folder keeps roughly `retention_segments`
  segments above the newest checkpoint. A node that stays offline *past* that
  window and then loses its local `store.db` can come back with edits the
  current checkpoint does not cover. Those edits are never silently dropped:
  they are reported as `dropped_below_boundary` (and the node stays
  `materialize_pending`) instead of being claimed as applied. Recovery is from
  that node's own `store.db`, which is never pruned — a node that syncs at
  least once per retention window is always correct.
- **Compaction needs a full-history node.** Only a node that was never
  bootstrapped — one that has kept its complete local `store.db` history since
  the database began — ever writes a checkpoint or prunes the folder. A
  bootstrapped node (one that opened from a checkpoint, so its store does not
  hold the frames below the cut) never produces a checkpoint: one built from an
  incomplete store would carry a `count`, `cut_digest` and version vector that
  mislead every other node. **At least one full-history node must remain in the
  cluster for it to keep compacting.** If every node that ever bootstrapped is
  all that is left, the folder stops shrinking and a node that falls behind the
  retention window can no longer be repaired from a checkpoint. Keep at least
  one original, never-bootstrapped node online (or reachable) for the life of
  the database.
- **Sync is not instant.** OneDrive/SharePoint may take minutes to deliver a
  segment, and may deliver a `… (conflicted copy …)` file instead. `status`
  lists conflicted copies; the merge itself is content-addressed, so a
  duplicated segment is simply skipped.

## Manual OneDrive smoke checklist

The tests run against a temp directory standing in for the synced folder; this
is the check that a real sync client behaves the way the design assumes.

1. Create a folder in a synced library (e.g. `sharedb-smoke`) and wait until
   OneDrive reports it as up to date on **both** machines.
2. On machine A: `sharedb --db <path>/sharedb-smoke --state-dir ~/a init`,
   then `migrate 1 "CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT NOT NULL);"`,
   then `exec "INSERT INTO t VALUES ('x','alice')"`.
3. Wait for OneDrive to finish uploading. `sharedb … status` should show
   `unpublished: 0`, and the folder should contain `MANIFEST` and
   `segments/<node>-….seg`.
4. On machine B: `init` (the MANIFEST must already be there), then `sync`.
   `query "SELECT * FROM t"` must show `alice`.
5. On both machines, `exec` a write to the **same** id with different values,
   without waiting in between. Let OneDrive sync both folders fully, then run
   `sync` on both, twice.
6. `query "SELECT id, v FROM t"` must print the **same single row** on both
   machines, and `sharedb … status` must show no `forks`, no `gaps` and no
   `conflicted_copies`.
7. `export smoke.db` on one machine and open it with the `sqlite3` CLI:
   `sqlite3 smoke.db 'select * from t'`.

## Building and testing

```sh
cargo build --features cli
cargo install --path . --features cli
cargo test                       # library, integration and CLI tests
cargo test --no-default-features # the library alone, without the CLI
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The `cli` feature (default on) pulls in `clap` and builds the `sharedb` binary;
without it the crate is a library only. `tests/cli.rs` drives the built binary
itself, so it is skipped under `--no-default-features`.
