use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::config::{CertifyArgs, CompareConfig, EngineKind, RunSpec};
use crate::engine::SqliteEngine;
use crate::process_metrics::ProcessMetrics;
use crate::report::{self, RunRecord};
use crate::strace_capture;

#[derive(Debug, Serialize)]
pub struct CertificationReport {
    pub runs: Vec<RunRecord>,
    pub manifest: CertificationManifest,
}

#[derive(Debug, Serialize)]
pub struct CertificationManifest {
    pub out_dir: PathBuf,
    pub config_hash: String,
    pub runs_jsonl_hash: String,
    pub summary_csv_hash: String,
    pub report_md_hash: String,
    pub git_sha: Option<String>,
    pub git_dirty: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pragmas: Option<BTreeMap<String, BTreeMap<String, String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pragma_validation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksums: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strace_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strace_syscall_counts: Option<BTreeMap<String, u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_metrics_per_run: Option<Vec<ProcessMetrics>>,
}

pub fn run(config: &CompareConfig, args: &CertifyArgs) -> Result<CertificationReport> {
    fs::create_dir_all(&args.out_dir)?;
    let raw_dir = args.out_dir.join("raw");
    fs::create_dir_all(&raw_dir)?;

    // Capture per-engine PRAGMA / setting snapshots before any workload
    // mutates state. SQLite has structured PRAGMAs we can read; redline
    // exposes its settings via engine_stats embedded in run records.
    let pragmas = collect_pragmas(config, args)?;

    let with_strace = std::env::var_os("REDLINEDB_BENCH_WITH_STRACE")
        .filter(|value| !value.is_empty())
        .is_some();

    let mut runs = Vec::new();
    for rep in 0..args.repetitions.max(1) {
        for &engine in &config.engines {
            for &workload in &config.workloads {
                for &durability in &config.durabilities {
                    for &threads in &config.threads {
                        let seed = args.seed.wrapping_add(rep as u64);
                        let spec =
                            config.run_spec(&engine, &workload, &durability, threads, seed)?;
                        let record = run_child(&spec, &raw_dir, rep, with_strace)?;
                        runs.push(record);
                    }
                }
            }
        }
    }

    if runs.is_empty() {
        bail!("certify config produced no benchmark runs");
    }

    let runs_jsonl = args.out_dir.join("runs.jsonl");
    write_runs_jsonl(&runs_jsonl, &runs)?;
    let summary_csv = args.out_dir.join("summary.csv");
    write_summary_csv(&summary_csv, &runs)?;
    let report_md = args.out_dir.join("report.md");
    fs::write(&report_md, crate::gates::markdown_summary(&runs))?;

    let process_metrics_per_run: Vec<ProcessMetrics> = runs
        .iter()
        .filter_map(|run| run.process_metrics.clone())
        .collect();
    let process_metrics_per_run = if process_metrics_per_run.is_empty() {
        None
    } else {
        Some(process_metrics_per_run)
    };

    let checksums = checksum_map(&runs);
    let strace = strace_summary(with_strace, &args.out_dir)?;

    let manifest = CertificationManifest {
        out_dir: args.out_dir.clone(),
        config_hash: hash_file(&args.config)?,
        runs_jsonl_hash: hash_file(&runs_jsonl)?,
        summary_csv_hash: hash_file(&summary_csv)?,
        report_md_hash: hash_file(&report_md)?,
        git_sha: report::collect_environment().git_sha,
        git_dirty: report::collect_environment().git_dirty,
        pragma_validation: pragma_validation(&pragmas),
        pragmas: if pragmas.is_empty() {
            None
        } else {
            Some(pragmas)
        },
        checksums: Some(checksums),
        strace_reason: strace.reason,
        strace_syscall_counts: strace.syscall_counts,
        process_metrics_per_run,
    };
    let manifest_path = args.out_dir.join("manifest.json");
    report::write_json(Some(&manifest_path), &manifest)?;

    Ok(CertificationReport { runs, manifest })
}

fn collect_pragmas(
    config: &CompareConfig,
    args: &CertifyArgs,
) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let mut out = BTreeMap::new();
    let probe_dir = args.out_dir.join("probe");
    for &engine in &config.engines {
        match engine {
            EngineKind::Sqlite => {
                let workload = config
                    .workloads
                    .first()
                    .copied()
                    .unwrap_or(crate::config::WorkloadKind::SingleRowInsert);
                let durability = config
                    .durabilities
                    .first()
                    .copied()
                    .unwrap_or(crate::config::DurabilityKind::Strict);
                let threads = config.threads.first().copied().unwrap_or(1);
                let spec: RunSpec =
                    config.run_spec(&engine, &workload, &durability, threads, args.seed)?;
                let probe = probe_dir.join("sqlite-probe");
                fs::create_dir_all(&probe)?;
                let sqlite = SqliteEngine::open(&spec, &probe)?;
                out.insert("sqlite".to_owned(), sqlite.pragmas());
            }
            EngineKind::Redline => {
                // Redline does not expose SQL-level PRAGMAs; surface a
                // small placeholder so consumers know we tried.
                let mut redline_pragmas = BTreeMap::new();
                redline_pragmas.insert("kind".to_owned(), "redline-engine-stats-only".to_owned());
                out.insert("redline".to_owned(), redline_pragmas);
            }
        }
    }
    let _ = fs::remove_dir_all(&probe_dir);
    Ok(out)
}

fn pragma_validation(pragmas: &BTreeMap<String, BTreeMap<String, String>>) -> Option<String> {
    let sqlite = pragmas.get("sqlite")?;
    let journal = sqlite.get("journal_mode").map(String::as_str).unwrap_or("");
    if journal.eq_ignore_ascii_case("wal") {
        Some("ok".to_owned())
    } else {
        Some(format!("journal_mode={journal} (expected wal)"))
    }
}

fn checksum_map(runs: &[RunRecord]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for record in runs {
        let key = format!(
            "{:?}:{}:{}:t{}",
            record.engine,
            record.workload.as_str(),
            record.durability.as_str(),
            record.threads
        );
        let payload = serde_json::to_vec(&record.checksum).unwrap_or_default();
        let digest = Sha256::digest(&payload);
        out.insert(key, format!("{digest:x}"));
    }
    out
}

fn strace_summary(
    with_strace: bool,
    out_dir: &Path,
) -> Result<crate::strace_capture::StraceCapture> {
    if !with_strace {
        return Ok(crate::strace_capture::StraceCapture {
            syscall_counts: None,
            reason: Some(if cfg!(target_os = "linux") {
                "disabled (set REDLINEDB_BENCH_WITH_STRACE=1 to enable)".to_owned()
            } else {
                "linux required".to_owned()
            }),
            output_path: None,
        });
    }
    let pid = std::process::id() as i32;
    let path = out_dir.join("strace.txt");
    Ok(strace_capture::capture_or_skip(pid, &path))
}

fn run_child(
    spec: &crate::config::RunSpec,
    raw_dir: &Path,
    rep: usize,
    _with_strace: bool,
) -> Result<RunRecord> {
    let exe = std::env::current_exe().context("resolve bench executable")?;
    let run_dir = raw_dir.join(format!(
        "{:?}-{}-{}-t{}-r{}",
        spec.engine,
        spec.workload.as_str(),
        spec.durability.as_str(),
        spec.threads,
        rep
    ));
    fs::create_dir_all(&run_dir)?;
    let out_path = run_dir.join("record.json");
    let output = Command::new(exe)
        .arg("run")
        .arg("--engine")
        .arg(engine_arg(spec.engine))
        .arg("--workload")
        .arg(spec.workload.as_str())
        .arg("--durability")
        .arg(spec.durability.as_str())
        .arg("--threads")
        .arg(spec.threads.to_string())
        .arg("--rows")
        .arg(spec.rows.to_string())
        .arg("--seconds")
        .arg(spec.duration.as_secs().to_string())
        .arg("--cache-mib")
        .arg((spec.cache_bytes / (1024 * 1024)).max(1).to_string())
        .arg("--seed")
        .arg(spec.seed.to_string())
        .arg("--out")
        .arg(&out_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("spawn certification child for {:?}", spec.engine))?;
    fs::write(run_dir.join("stdout.log"), &output.stdout)?;
    fs::write(run_dir.join("stderr.log"), &output.stderr)?;
    if !output.status.success() {
        bail!(
            "certification child failed for {:?}: {}",
            spec.engine,
            output.status
        );
    }
    let raw = fs::read_to_string(&out_path)
        .with_context(|| format!("read run record {}", out_path.display()))?;
    Ok(serde_json::from_str(&raw)?)
}

fn write_runs_jsonl(path: &Path, runs: &[RunRecord]) -> Result<()> {
    let mut out = fs::File::create(path)?;
    for run in runs {
        writeln!(out, "{}", serde_json::to_string(run)?)?;
    }
    Ok(())
}

fn write_summary_csv(path: &Path, runs: &[RunRecord]) -> Result<()> {
    let mut out = fs::File::create(path)?;
    writeln!(
        out,
        "engine,workload,durability,threads,ops,failures,busy_errors,throughput_ops_per_sec,p99_us,p999_us,data_bytes,wal_bytes"
    )?;
    for run in runs {
        writeln!(
            out,
            "{:?},{},{},{},{},{},{},{:.2},{},{},{},{}",
            run.engine,
            run.workload.as_str(),
            run.durability.as_str(),
            run.threads,
            run.metrics.operations,
            run.metrics.failures,
            run.metrics.busy_errors,
            run.metrics.throughput_ops_per_sec,
            run.metrics.latency.p99_us,
            run.metrics.latency.p999_us,
            run.data_bytes,
            run.wal_bytes,
        )?;
    }
    Ok(())
}

fn engine_arg(engine: EngineKind) -> &'static str {
    match engine {
        EngineKind::Redline => "redline",
        EngineKind::Sqlite => "sqlite",
    }
}

fn hash_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path)?;
    let digest = Sha256::digest(&bytes);
    Ok(format!("{digest:x}"))
}
