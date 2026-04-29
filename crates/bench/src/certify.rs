use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::config::{CertifyArgs, CompareConfig, EngineKind};
use crate::report::{self, RunRecord};

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
}

pub fn run(config: &CompareConfig, args: &CertifyArgs) -> Result<CertificationReport> {
    fs::create_dir_all(&args.out_dir)?;
    let raw_dir = args.out_dir.join("raw");
    fs::create_dir_all(&raw_dir)?;
    let mut runs = Vec::new();

    for rep in 0..args.repetitions.max(1) {
        for &engine in &config.engines {
            for &workload in &config.workloads {
                for &durability in &config.durabilities {
                    for &threads in &config.threads {
                        let seed = args.seed.wrapping_add(rep as u64);
                        let spec =
                            config.run_spec(&engine, &workload, &durability, threads, seed)?;
                        let record = run_child(&spec, &raw_dir, rep)?;
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

    let manifest = CertificationManifest {
        out_dir: args.out_dir.clone(),
        config_hash: hash_file(&args.config)?,
        runs_jsonl_hash: hash_file(&runs_jsonl)?,
        summary_csv_hash: hash_file(&summary_csv)?,
        report_md_hash: hash_file(&report_md)?,
        git_sha: report::collect_environment().git_sha,
        git_dirty: report::collect_environment().git_dirty,
    };
    let manifest_path = args.out_dir.join("manifest.json");
    report::write_json(Some(&manifest_path), &manifest)?;

    Ok(CertificationReport { runs, manifest })
}

fn run_child(spec: &crate::config::RunSpec, raw_dir: &Path, rep: usize) -> Result<RunRecord> {
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
