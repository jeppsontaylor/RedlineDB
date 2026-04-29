//! Lane E failpoint matrix runner.
//!
//! The runner spawns a child process per (engine, durability, case,
//! kill_after_n_hits) tuple. Each child arms one of the kernel
//! failpoints registered in `crates/kernel/src/{wal,engine,index,
//! catalog,storage}` with an action that kills the process the moment
//! the site fires (typically `panic`).
//!
//! Inside the child the workload follows the strict fsynced-ack
//! protocol used by the recovery matrix: for every row the child
//! issues `INSERT ... COMMIT` and only after the commit succeeds does
//! it append `key\n` to `ack_log` and `flush()` the file. The kernel
//! has already fsynced both the WAL record and the ack-log line by the
//! time we proceed to the next row. When the parent observes the
//! child has died, the highest line in `ack_log` is the contractual
//! upper bound on rows the engine guaranteed durable.
//!
//! The parent then opens a fresh `redlinedb::Database` over the same
//! directory and counts surviving rows. If `recovered < acked`, that
//! is a lost-acked-commit and the case fails the strict gate.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::{
    DurabilityKind, EngineKind, FailpointChildArgs, FailpointMatrixArgs, FailpointMatrixCase,
    FailpointMatrixConfig,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailpointMatrixReport {
    pub seed: u64,
    pub passed: bool,
    pub failed_cases: usize,
    pub runs: Vec<FailpointMatrixRun>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailpointMatrixRun {
    pub case: String,
    pub failpoint: String,
    pub action: String,
    pub engine: EngineKind,
    pub durability: DurabilityKind,
    pub kill_after_n_hits: u64,
    pub child_status: String,
    pub acknowledged: usize,
    pub recovered: usize,
    pub lost_acked_commits: i64,
    pub passed: bool,
}

pub fn run(args: &FailpointMatrixArgs) -> Result<FailpointMatrixReport> {
    let config = FailpointMatrixConfig::load(&args.config)?;
    let mut runs = Vec::new();
    let mut failed = 0_usize;

    // For now only redline supports the failpoint-matrix runner. Sqlite
    // has no equivalent scriptable injection point; the gate explicitly
    // targets the redline engine under strict durability.
    let engine = EngineKind::Redline;

    let total_planned: usize = config
        .cases
        .iter()
        .map(|case| {
            let durabilities = if case.durabilities.is_empty() {
                config.durabilities.len()
            } else {
                case.durabilities.len()
            };
            let kills = case.kill_after_n_hits.len().max(1);
            durabilities * kills
        })
        .sum();
    eprintln!(
        "failpoint-matrix: planning {total_planned} runs across {} cases",
        config.cases.len()
    );

    let mut completed = 0_usize;
    for case in &config.cases {
        let durabilities = if case.durabilities.is_empty() {
            config.durabilities.clone()
        } else {
            case.durabilities.clone()
        };
        let kill_hits = if case.kill_after_n_hits.is_empty() {
            vec![1]
        } else {
            case.kill_after_n_hits.clone()
        };
        for &durability in &durabilities {
            for &kill_after_n_hits in &kill_hits {
                eprintln!(
                    "failpoint-matrix: [{}/{}] case={} durability={} kill_after_n_hits={}",
                    completed + 1,
                    total_planned,
                    case.name,
                    durability.as_str(),
                    kill_after_n_hits
                );
                let run = run_case(engine, durability, case, kill_after_n_hits)?;
                if !run.passed {
                    failed += 1;
                }
                eprintln!(
                    "failpoint-matrix:   -> {} acked={} recovered={} lost={}",
                    if run.passed { "PASS" } else { "FAIL" },
                    run.acknowledged,
                    run.recovered,
                    run.lost_acked_commits
                );
                runs.push(run);
                completed += 1;
            }
        }
    }

    let passed = failed == 0;
    let report = FailpointMatrixReport {
        seed: args.seed,
        passed,
        failed_cases: failed,
        runs,
    };
    // Cross-check the strict gate via the same evaluator the gates
    // module uses; if the runner ever forgets to flip `passed`, the
    // gate-side audit will still surface a true/false discrepancy.
    let gate = crate::gates::gate_zero_lost_acked_commits(&report);
    if !gate.passed && report.passed {
        // Internal consistency error - fail loudly.
        anyhow::bail!(
            "failpoint matrix runner reported pass but strict gate disagreed: {}",
            gate.detail
        );
    }
    Ok(report)
}

pub fn write_report(out: &Path, report: &FailpointMatrixReport) -> Result<()> {
    if let Some(parent) = out.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create parent dir for {}", out.display()))?;
    }
    let mut file = File::create(out)
        .with_context(|| format!("create failpoint-matrix report {}", out.display()))?;
    let body = serde_json::to_string_pretty(report)?;
    file.write_all(body.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

pub fn run_child(args: &FailpointChildArgs) -> Result<()> {
    fs::create_dir_all(&args.db_dir)?;
    if let Some(parent) = args.ack_log.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    // Failpoints commonly fire on background threads (the WAL writer
    // thread, for instance). A Rust panic on a background thread
    // unwinds that thread but does not kill the process; subsequent
    // foreground operations would block forever waiting on the dead
    // worker. Install an abort-on-panic hook so any failpoint panic
    // anywhere in the child terminates the whole process and lets the
    // parent observe a clean death.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        original_hook(info);
        std::process::abort();
    }));

    // Arm the failpoint registry. `init` is idempotent and `cfg` only
    // succeeds when the `failpoints` feature was enabled at compile
    // time, which we guarantee via the bench's direct dep on
    // `redlinedb-kernel = { features = ["failpoints"] }`.
    redlinedb_kernel::failpoints::init();
    let action = translate_action(&args.action, args.kill_after_n_hits);
    redlinedb_kernel::failpoints::cfg(&args.failpoint, &action)
        .map_err(|err| anyhow::anyhow!("arm failpoint {}: {}", args.failpoint, err))?;

    let mut options = redlinedb::OpenOptions::default();
    options.memory.cache_bytes = 8 * 1024 * 1024;
    options.durability = match args.durability {
        DurabilityKind::Strict => redlinedb::Durability::Strict,
        DurabilityKind::Normal => redlinedb::Durability::Normal,
        DurabilityKind::Unsafe => redlinedb::Durability::UnsafeDev,
    };
    let db_path = args.db_dir.join("bench.redline");
    let db = redlinedb::Database::open_with_options(&db_path, options)?;
    let mut conn = db.connect()?;

    // Setup the schema. CREATE INDEX is included so that catalog and
    // index failpoints have a non-trivial schema to operate on.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv(k INTEGER PRIMARY KEY, tenant INTEGER, v BLOB, version INTEGER)",
        (),
    )?;
    conn.execute("CREATE INDEX IF NOT EXISTS kv_tenant_idx ON kv(tenant)", ())?;

    // Open the ack log in append mode. Each successful commit
    // contractually fsyncs the WAL; only after the commit returns
    // do we append the row's key. The flush() ensures the OS sees
    // the write before we proceed to the next row.
    let mut ack = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.ack_log)?;

    // Every 16 rows the child also calls `Database::checkpoint()` so
    // failpoints that gate the checkpoint path (`engine::checkpoint`,
    // `wal::flush_all`, `storage::control::write`) actually fire.
    // Without this hook the simple INSERT loop would never exercise
    // those sites and the corresponding matrix cases would silently
    // pass.
    const CHECKPOINT_EVERY_ROWS: usize = 16;

    for key in 0..args.rows {
        // Use INSERT OR REPLACE so the same key can be retried after
        // a successful commit if our process is restarted; under the
        // matrix runner each child has a unique db_dir so this never
        // triggers, but keeping the SQL idempotent matches the
        // recover.rs convention.
        let result = (|| -> Result<()> {
            let params = vec![
                redlinedb::Value::Integer(key as i64),
                redlinedb::Value::Integer((key % 32) as i64),
                redlinedb::Value::Blob(format!("value-{key:08}").into_bytes().into()),
                redlinedb::Value::Integer(1),
            ];
            conn.begin(redlinedb::BeginMode::Immediate)?;
            conn.execute(
                "INSERT OR REPLACE INTO kv(k, tenant, v, version) VALUES (?, ?, ?, ?)",
                params,
            )?;
            conn.commit()?;
            Ok(())
        })();
        if result.is_err() {
            // Failpoint that returned an error; the parent still gets
            // a meaningful ack count up to this point.
            return Ok(());
        }
        writeln!(ack, "{key}")?;
        ack.flush()?;

        if key > 0 && key.is_multiple_of(CHECKPOINT_EVERY_ROWS) && db.checkpoint().is_err() {
            // The checkpoint failpoint fires here; treat error as
            // workload termination so the parent observes a clean
            // ack-up-to-N before our process aborts.
            return Ok(());
        }
    }
    Ok(())
}

fn run_case(
    engine: EngineKind,
    durability: DurabilityKind,
    case: &FailpointMatrixCase,
    kill_after_n_hits: u64,
) -> Result<FailpointMatrixRun> {
    let tmp = tempfile::tempdir()
        .with_context(|| format!("create failpoint matrix tempdir for {}", case.name))?;
    let db_dir = tmp.path().join(format!("{engine:?}-{}", case.name));
    let ack_log = tmp.path().join(format!("{engine:?}-{}.ack", case.name));

    let exe = std::env::current_exe()?;
    let mut command = Command::new(exe);
    command
        .arg("failpoint-child")
        .arg("--engine")
        .arg(engine_name(engine))
        .arg("--durability")
        .arg(durability.as_str())
        .arg("--db-dir")
        .arg(&db_dir)
        .arg("--ack-log")
        .arg(&ack_log)
        .arg("--failpoint")
        .arg(&case.failpoint)
        .arg("--action")
        .arg(&case.action)
        .arg("--rows")
        .arg(case.rows.max(1).to_string())
        .arg("--kill-after-n-hits")
        .arg(kill_after_n_hits.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let status = command
        .status()
        .with_context(|| format!("spawn failpoint child for case {}", case.name))?;

    let acknowledged = read_ack_count(&ack_log)?;
    let recovered = match verify_recovered(durability, &db_dir) {
        Ok(value) => value,
        Err(err) => {
            eprintln!(
                "failpoint-matrix:   verify_recovered failed for {}: {err:#}",
                case.name
            );
            0
        }
    };
    let lost = acknowledged as i64 - recovered as i64;
    let passed = lost <= 0;

    Ok(FailpointMatrixRun {
        case: case.name.clone(),
        failpoint: case.failpoint.clone(),
        action: case.action.clone(),
        engine,
        durability,
        kill_after_n_hits,
        child_status: format_status(status.code(), status.success()),
        acknowledged,
        recovered,
        lost_acked_commits: lost.max(0),
        passed,
    })
}

fn verify_recovered(durability: DurabilityKind, db_dir: &Path) -> Result<usize> {
    let mut options = redlinedb::OpenOptions::default();
    options.memory.cache_bytes = 8 * 1024 * 1024;
    // CRITICAL: `create: true` (the default) routes the redlinedb facade
    // through `Database::create`, which re-initialises the page file
    // and wipes any existing data. For recovery verification we need
    // `Database::open`, which replays the WAL on top of the existing
    // page file.
    options.create = false;
    options.durability = match durability {
        DurabilityKind::Strict => redlinedb::Durability::Strict,
        DurabilityKind::Normal => redlinedb::Durability::Normal,
        DurabilityKind::Unsafe => redlinedb::Durability::UnsafeDev,
    };
    let path = db_dir.join("bench.redline");
    let db = redlinedb::Database::open_with_options(&path, options)
        .with_context(|| format!("open recovered db at {}", path.display()))?;
    let mut conn = db.connect().with_context(|| "connect to recovered db")?;
    // The kv table may not exist if the child died before the
    // CREATE TABLE durably committed. Treat that as zero recovered
    // rows rather than propagating the error: the gate is about
    // whether *acknowledged* rows are recovered, and ack count is
    // zero in that scenario, so the case naturally passes.
    let count = match conn.query("SELECT COUNT(*) FROM kv", ()) {
        Ok(mut rows) => match rows.step()? {
            redlinedb::Step::Row(row) => match row.get_ref(0)? {
                redlinedb::ValueRef::Integer(value) => value,
                redlinedb::ValueRef::Null => 0,
                _ => 0,
            },
            redlinedb::Step::Done => 0,
        },
        Err(err) => {
            eprintln!(
                "failpoint-matrix:   recovered db at {} has no kv table ({err})",
                path.display()
            );
            0
        }
    };
    Ok(count.max(0) as usize)
}

fn read_ack_count(path: &Path) -> Result<usize> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err.into()),
    };
    let reader = BufReader::new(file);
    let mut count = 0_usize;
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

fn engine_name(engine: EngineKind) -> &'static str {
    match engine {
        EngineKind::Redline => "redline",
        EngineKind::Sqlite => "sqlite",
    }
}

fn translate_action(raw: &str, kill_after_n_hits: u64) -> String {
    // The `fail` crate accepts `K*action` to fire `K` times before
    // disarming. We only pass the count when > 1 so the common
    // single-shot case stays readable in logs.
    let suffix = match raw.trim() {
        // `abort` and `panic` both translate to a Rust panic that kills
        // the process under typical bench-runner conditions. The runner
        // does not link `panic = "abort"`, but a panicking thread that
        // holds locks tends to abort the process via the unwind guard
        // in the WAL writer thread; either way the parent observes the
        // child terminate.
        "panic" | "abort" => "panic".to_string(),
        // `return` makes the failpoint short-circuit the function with
        // the type's default. Most kernel hot paths return `Result<_>`
        // so `Default::default()` evaluates to `Ok(_)` only when the
        // inner type has a sensible default — for now we map `return`
        // to `panic` to keep the semantics consistent across cases.
        "return" => "panic".to_string(),
        // Forward verbatim; the fail crate accepts `K%`, `sleep(N)`,
        // `pause`, `print`, etc.
        other => other.to_string(),
    };
    if kill_after_n_hits <= 1 {
        suffix
    } else {
        format!("{kill_after_n_hits}*{suffix}")
    }
}

fn format_status(code: Option<i32>, success: bool) -> String {
    match code {
        Some(code) => format!("exit({code})"),
        None if success => "success".to_string(),
        None => "signal".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_action_maps_keywords_to_panic() {
        assert_eq!(translate_action("panic", 1), "panic");
        assert_eq!(translate_action("abort", 1), "panic");
        assert_eq!(translate_action("return", 1), "panic");
        assert_eq!(translate_action("panic", 5), "5*panic");
    }

    #[test]
    fn translate_action_passes_through_unknown_directives() {
        assert_eq!(translate_action("sleep(10)", 1), "sleep(10)");
        assert_eq!(translate_action("50%panic", 1), "50%panic");
    }
}
