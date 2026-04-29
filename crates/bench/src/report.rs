use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::{DurabilityKind, EngineKind, WorkloadKind};
use crate::{ensure_parent, gates};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    pub rows: i64,
    pub key_sum: i64,
    pub version_sum: i64,
    pub payload_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySummary {
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub operations: u64,
    pub failures: u64,
    pub busy_errors: u64,
    pub elapsed_ms: u64,
    pub throughput_ops_per_sec: f64,
    pub latency: LatencySummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    pub engine: EngineKind,
    pub workload: WorkloadKind,
    pub durability: DurabilityKind,
    pub threads: usize,
    pub seed: u64,
    pub cache_bytes: usize,
    pub environment: RunEnvironment,
    pub metrics: MetricsSummary,
    pub checksum: Checksum,
    pub data_bytes: u64,
    pub wal_bytes: u64,
    pub engine_stats: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEnvironment {
    pub hostname: String,
    pub git_sha: Option<String>,
    pub git_dirty: Option<bool>,
    pub rustc_version: Option<String>,
    pub sqlite_version: Option<String>,
    pub logical_cpus: usize,
    pub memory_mib: Option<u64>,
    pub image_digest: Option<String>,
}

pub fn write_run_output(path: Option<&Path>, record: &RunRecord) -> Result<()> {
    if let Some(path) = path {
        write_json(Some(path), record)
    } else {
        println!("{}", serde_json::to_string(record)?);
        Ok(())
    }
}

pub fn write_compare_output(
    out: Option<&Path>,
    report: Option<&Path>,
    records: &[RunRecord],
) -> Result<()> {
    if let Some(out) = out {
        ensure_parent(Some(out))?;
        let mut file = fs::File::create(out)?;
        for record in records {
            writeln!(file, "{}", serde_json::to_string(record)?)?;
        }
    }
    if let Some(report) = report {
        ensure_parent(Some(report))?;
        let markdown = gates::markdown_summary(records);
        fs::write(report, markdown)?;
    }
    if out.is_none() && report.is_none() {
        for record in records {
            println!("{}", serde_json::to_string(record)?);
        }
    }
    Ok(())
}

pub fn write_json(path: Option<&Path>, value: &impl Serialize) -> Result<()> {
    if let Some(path) = path {
        ensure_parent(Some(path))?;
        fs::write(path, serde_json::to_vec_pretty(value)?)?;
    } else {
        println!("{}", serde_json::to_string_pretty(value)?);
    }
    Ok(())
}

pub fn append_jsonl(path: &Path, record: &RunRecord) -> Result<()> {
    ensure_parent(Some(path))?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open jsonl output {}", path.display()))?;
    writeln!(file, "{}", serde_json::to_string(record)?)?;
    Ok(())
}

pub fn read_jsonl(path: &Path) -> Result<Vec<RunRecord>> {
    let file = fs::File::open(path)?;
    let mut records = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        records.push(serde_json::from_str(&line)?);
    }
    Ok(records)
}

pub fn next_run_id(engine: EngineKind, workload: WorkloadKind) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    format!("{engine:?}-{workload:?}-{millis}")
}

pub fn collect_environment() -> RunEnvironment {
    RunEnvironment {
        hostname: hostname(),
        git_sha: command_output(["git", "rev-parse", "HEAD"]),
        git_dirty: command_status(["git", "status", "--porcelain"])
            .map(|output| !output.is_empty()),
        rustc_version: command_output(["rustc", "-V"]),
        sqlite_version: Some(rusqlite::version().to_owned()),
        logical_cpus: std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1),
        memory_mib: total_memory_mib(),
        image_digest: std::env::var("REDLINEDB_BENCH_IMAGE_DIGEST").ok(),
    }
}

fn hostname() -> String {
    command_output(["hostname"]).unwrap_or_else(|| "unknown".to_owned())
}

fn command_output<const N: usize>(args: [&str; N]) -> Option<String> {
    let (cmd, tail) = args.split_first()?;
    let output = Command::new(cmd).args(tail).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.trim().to_owned())
}

fn command_status<const N: usize>(args: [&str; N]) -> Option<String> {
    let (cmd, tail) = args.split_first()?;
    let output = Command::new(cmd).args(tail).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.trim().to_owned())
}

fn total_memory_mib() -> Option<u64> {
    let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in contents.lines() {
        if let Some(value) = line.strip_prefix("MemTotal:") {
            let kib = value
                .split_whitespace()
                .next()
                .and_then(|number| number.parse::<u64>().ok())?;
            return Some(kib / 1024);
        }
    }
    None
}
