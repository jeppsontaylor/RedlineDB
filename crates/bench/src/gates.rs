use std::collections::BTreeMap;

use serde::Serialize;

use crate::config::{DurabilityKind, EngineKind, WorkloadKind};
use crate::report::RunRecord;

#[derive(Debug, Clone, Serialize)]
pub struct GateResult {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GateSummary {
    pub gates: Vec<GateResult>,
}

pub fn evaluate_records(records: &[RunRecord]) -> GateSummary {
    GateSummary {
        gates: vec![
            gate_nonzero(records),
            gate_checksums_match(records),
            gate_single_thread_parity(records),
            gate_writer_advantage(records),
        ],
    }
}

pub fn markdown_summary(records: &[RunRecord]) -> String {
    let summary = evaluate_records(records);
    let mut out = String::from(
        "| workload | engine | durability | threads | ops/s | p99 us | p999 us | data bytes | wal bytes |\n",
    );
    out.push_str("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for record in records {
        out.push_str(&format!(
            "| {} | {:?} | {} | {} | {:.1} | {} | {} | {} | {} |\n",
            record.workload.as_str(),
            record.engine,
            record.durability.as_str(),
            record.threads,
            record.metrics.throughput_ops_per_sec,
            record.metrics.latency.p99_us,
            record.metrics.latency.p999_us,
            record.data_bytes,
            record.wal_bytes
        ));
    }
    out.push_str("\n## Gates\n");
    for gate in summary.gates {
        out.push_str(&format!(
            "- {}: {} ({})\n",
            gate.name,
            if gate.passed { "PASS" } else { "FAIL" },
            gate.detail
        ));
    }
    out
}

fn gate_nonzero(records: &[RunRecord]) -> GateResult {
    let passed = records.iter().all(|record| record.metrics.operations > 0);
    GateResult {
        name: "harness_sanity".to_owned(),
        passed,
        detail: "all runs produced at least one successful operation".to_owned(),
    }
}

fn gate_checksums_match(records: &[RunRecord]) -> GateResult {
    let mut seen =
        BTreeMap::<(WorkloadKind, DurabilityKind, usize), crate::report::Checksum>::new();
    let mut passed = true;
    for record in records {
        let key = (record.workload, record.durability, record.threads);
        if let Some(existing) = seen.get(&key) {
            if existing != &record.checksum {
                passed = false;
                break;
            }
        } else {
            seen.insert(key, record.checksum);
        }
    }
    GateResult {
        name: "checksum_match".to_owned(),
        passed,
        detail: "matching engines agree on final workload checksum".to_owned(),
    }
}

fn gate_single_thread_parity(records: &[RunRecord]) -> GateResult {
    let passed = compare_ratio(records, 1, 0.90, WorkloadKind::PointReadPk);
    GateResult {
        name: "single_thread_parity".to_owned(),
        passed,
        detail: "redline point-read throughput stays within 90% of sqlite".to_owned(),
    }
}

fn gate_writer_advantage(records: &[RunRecord]) -> GateResult {
    let passed = compare_ratio(records, 8, 1.50, WorkloadKind::WritersDisjoint);
    GateResult {
        name: "concurrent_writer_advantage".to_owned(),
        passed,
        detail: "redline writers-disjoint throughput exceeds sqlite by 1.5x at 8 threads"
            .to_owned(),
    }
}

fn compare_ratio(
    records: &[RunRecord],
    threads: usize,
    min_ratio: f64,
    workload: WorkloadKind,
) -> bool {
    let sqlite = records.iter().find(|record| {
        record.engine == EngineKind::Sqlite
            && record.threads == threads
            && record.durability == DurabilityKind::Strict
            && record.workload == workload
    });
    let redline = records.iter().find(|record| {
        record.engine == EngineKind::Redline
            && record.threads == threads
            && record.durability == DurabilityKind::Strict
            && record.workload == workload
    });
    match (sqlite, redline) {
        (Some(sqlite), Some(redline)) => {
            if sqlite.metrics.throughput_ops_per_sec == 0.0 {
                false
            } else {
                (redline.metrics.throughput_ops_per_sec / sqlite.metrics.throughput_ops_per_sec)
                    >= min_ratio
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{LatencySummary, MetricsSummary, RunRecord};

    fn record(engine: EngineKind, throughput: f64) -> RunRecord {
        RunRecord {
            run_id: "run".to_owned(),
            engine,
            workload: WorkloadKind::PointReadPk,
            durability: DurabilityKind::Strict,
            threads: 1,
            seed: 7,
            cache_bytes: 1024,
            environment: crate::report::RunEnvironment {
                hostname: "test".to_owned(),
                git_sha: None,
                git_dirty: None,
                rustc_version: None,
                sqlite_version: None,
                logical_cpus: 1,
                memory_mib: None,
                image_digest: None,
            },
            metrics: MetricsSummary {
                operations: 10,
                failures: 0,
                busy_errors: 0,
                elapsed_ms: 10,
                throughput_ops_per_sec: throughput,
                latency: LatencySummary {
                    p50_us: 1,
                    p95_us: 1,
                    p99_us: 1,
                    p999_us: 1,
                    max_us: 1,
                },
            },
            checksum: crate::report::Checksum::default(),
            data_bytes: 1,
            wal_bytes: 1,
            engine_stats: serde_json::json!({}),
        }
    }

    #[test]
    fn parity_gate_passes_when_ratio_is_met() {
        let records = vec![
            record(EngineKind::Sqlite, 100.0),
            record(EngineKind::Redline, 95.0),
        ];
        assert!(gate_single_thread_parity(&records).passed);
    }
}
