//! The `sharedb` command-line client.
//!
//! One database is a synced *folder* (`--db`, or `$SHAREDB_PATH`) plus a local
//! *state directory* (`--state-dir`, or `$SHAREDB_STATE_DIR`) that must never be
//! inside the folder. Every subcommand opens the database, does one thing and
//! exits: the state directory carries the node identity and the frame log, so
//! nothing needs to stay open between calls.
//!
//! Exit codes: `0` on success, `1` on any error (message on stderr).

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rusqlite::types::ValueRef;
use sharepoint_db::{
    Error, MaterializeKind, MigrateOutcome, OpenOptions, ShareDb, Status, SyncReport,
};

/// What a subcommand can fail with: a database error, or a missing `--db`.
#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error(transparent)]
    Db(#[from] Error),
    #[error("--db is required (or set SHAREDB_PATH)")]
    MissingDb,
}

// The CLI touches SQLite and the filesystem directly (export), so those errors
// reach it without going through the library.
impl From<rusqlite::Error> for CliError {
    fn from(err: rusqlite::Error) -> Self {
        CliError::Db(err.into())
    }
}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        CliError::Db(err.into())
    }
}

/// A multi-writer SQL database that syncs through a OneDrive/SharePoint folder.
#[derive(Parser)]
#[command(name = "sharedb", version, about, long_about = None)]
struct Cli {
    /// The synced folder holding the database (MANIFEST + segments). Required.
    ///
    /// Kept as an `Option` because clap refuses a global argument that is also
    /// `required`; [`folder`] reports the absence instead.
    #[arg(long, global = true, env = "SHAREDB_PATH")]
    db: Option<PathBuf>,
    /// Local state directory; must not be inside `--db`. Defaults to a
    /// machine-local directory keyed by the database id.
    #[arg(long, global = true, env = "SHAREDB_STATE_DIR")]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a new database folder (MANIFEST + segments/).
    Init,
    /// Run a schema migration (recorded as a migration frame).
    Migrate {
        /// Schema version to apply; must be the next one.
        version: u32,
        /// The DDL to run.
        sql: String,
    },
    /// Run a write statement, then sync (unless --no-sync).
    Exec {
        /// The statement to run.
        sql: String,
        /// Do not publish/merge after the write.
        #[arg(long)]
        no_sync: bool,
    },
    /// Run a read-only query and print the rows.
    Query {
        /// The statement to run; must be read-only.
        sql: String,
        /// Print tab-separated values instead of aligned columns.
        #[arg(long)]
        tsv: bool,
    },
    /// Publish local frames, merge the folder's, rebuild the data file.
    Sync,
    /// Print this node's status.
    Status,
    /// Write a plain SQLite snapshot of the current data to <out>.
    Export {
        /// Destination file (an existing file is replaced).
        out: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

/// The folder named by `--db` / `$SHAREDB_PATH`.
fn folder(cli: &Cli) -> Result<&Path, CliError> {
    cli.db.as_deref().ok_or(CliError::MissingDb)
}

/// Opens the database named by the global options.
fn open(cli: &Cli, create: bool) -> Result<ShareDb, CliError> {
    ShareDb::open(
        folder(cli)?,
        OpenOptions {
            create,
            state_dir: cli.state_dir.clone(),
            ..Default::default()
        },
    )
    .map_err(CliError::from)
}

fn run(cli: &Cli) -> Result<(), CliError> {
    match &cli.command {
        Command::Init => {
            let db = open(cli, true)?;
            println!("initialized {}", db.folder().display());
            println!("db_id   {}", hex(db.db_id().as_slice()));
            println!("node_id {}", hex(db.node_id().as_slice()));
            println!("state   {}", db.state_dir().display());
            Ok(())
        }
        Command::Migrate { version, sql } => {
            let mut db = open(cli, false)?;
            let outcome = db.migrate(*version, sql)?;
            let outcome = match outcome {
                MigrateOutcome::Applied => "applied",
                MigrateOutcome::AlreadyApplied => "already applied",
            };
            println!("migration {version}: {outcome} (run `sharedb sync` to publish it)");
            Ok(())
        }
        Command::Exec { sql, no_sync } => {
            let mut db = open(cli, false)?;
            let changed = db.execute(sql, rusqlite::params![])?;
            println!("{changed} row(s) changed");
            if *no_sync {
                return Ok(());
            }
            report_sync(&mut db)
        }
        Command::Query { sql, tsv } => {
            let db = open(cli, false)?;
            let mut header: Option<Vec<String>> = None;
            let rows = db.query(sql, rusqlite::params![], |row| {
                let stmt: &rusqlite::Statement<'_> = row.as_ref();
                if header.is_none() {
                    header = Some(stmt.column_names().map(str::to_string).collect());
                }
                let width = stmt.column_count();
                let mut cells = Vec::with_capacity(width);
                for i in 0..width {
                    cells.push(render_value(row.get_ref(i)?));
                }
                Ok(cells)
            })?;
            let header = header.unwrap_or_default();
            print_table(&header, &rows, *tsv);
            Ok(())
        }
        Command::Sync => {
            let mut db = open(cli, false)?;
            report_sync(&mut db)
        }
        Command::Status => {
            let db = open(cli, false)?;
            print_status(&db.status()?);
            Ok(())
        }
        Command::Export { out } => {
            let db = open(cli, false)?;
            db.export(out)?;
            println!("exported {}", out.display());
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

/// Runs `sync` and prints the report.
fn report_sync(db: &mut ShareDb) -> Result<(), CliError> {
    let report = db.sync()?;
    print_sync(&report);
    Ok(())
}

fn print_sync(report: &SyncReport) {
    let kind = match report.materialize {
        MaterializeKind::UpToDate => "up-to-date",
        MaterializeKind::Fast => "fast",
        MaterializeKind::Rebuilt => "rebuilt",
    };
    println!("published:            {}", report.published);
    println!("ingested:             {}", report.ingested);
    println!("materialize:          {kind}");
    println!("skipped:              {}", report.skipped);
    println!("gaps:                 {}", list_nodes(&report.gaps));
    println!("forks:                {}", list_nodes(&report.forks));
    println!("publish_pending:      {}", report.publish_pending);
    println!("dropped_below_boundary: {}", report.dropped_below_boundary);
    println!("materialize_pending:  {}", report.materialize_pending);
    for path in &report.ignored_files {
        println!("ignored file:         {}", path.display());
    }
}

fn print_status(status: &Status) {
    println!("node_id:                {}", hex(status.node_id.as_slice()));
    println!("own_seq:                {}", status.own_seq);
    println!("unpublished:            {}", status.unpublished);
    println!("peers:                  {}", list_nodes(&status.peers));
    println!(
        "conflicted_copies:      {}",
        if status.conflicted_copies.is_empty() {
            "none".to_string()
        } else {
            status
                .conflicted_copies
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        }
    );
    println!("dropped_below_boundary: {}", status.dropped_below_boundary);
    println!("materialize_pending:    {}", status.materialize_pending);
}

/// `(node, seq)` pairs rendered as `<node-hex>:<seq>`.
fn list_nodes(items: &[(sharepoint_db::NodeId, u64)]) -> String {
    if items.is_empty() {
        return "none".to_string();
    }
    items
        .iter()
        .map(|(node, seq)| format!("{}:{seq}", hex(node.as_slice())))
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// Query output
// ---------------------------------------------------------------------------

/// One value rendered the way the dump renders it: `NULL` for null, hex for a
/// blob, lossy UTF-8 for text.
fn render_value(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Integer(i) => i.to_string(),
        ValueRef::Real(f) => f.to_string(),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
        ValueRef::Blob(b) => {
            let mut out = String::with_capacity(b.len() * 2 + 3);
            out.push_str("x'");
            for byte in b {
                out.push_str(&format!("{byte:02x}"));
            }
            out.push('\'');
            out
        }
    }
}

/// Prints rows as aligned columns (the default) or as TSV.
fn print_table(header: &[String], rows: &[Vec<String>], tsv: bool) {
    if tsv {
        if !header.is_empty() {
            println!("{}", header.join("\t"));
        }
        for row in rows {
            println!("{}", row.join("\t"));
        }
        return;
    }
    let width = header.len();
    let mut widths = vec![0usize; width];
    for (i, name) in header.iter().enumerate() {
        widths[i] = widths[i].max(name.chars().count());
    }
    for row in rows {
        for (i, cell) in row.iter().enumerate().take(width) {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    if width > 0 {
        for (i, name) in header.iter().enumerate() {
            print!("{name:<width$}  ", width = widths[i]);
        }
        println!();
        let total: usize = widths.iter().sum::<usize>() + width.saturating_sub(1) * 2;
        println!("{}", "-".repeat(total));
    }
    for row in rows {
        for (i, &cell_width) in widths.iter().enumerate() {
            let cell = row.get(i).map(String::as_str).unwrap_or("");
            print!("{cell:<cell_width$}  ");
        }
        println!();
    }
}

/// Lowercase hex, used for the ids the CLI prints.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
