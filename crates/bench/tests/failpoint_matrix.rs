//! Lane E bench-side smoke tests for the failpoint matrix.
//!
//! These guard the two seams that are easy to break in isolation:
//! the TOML schema (any field rename in `FailpointMatrixCase` breaks
//! parsing silently if no test loads the file) and the end-to-end
//! parent/child handshake (a regression in `failpoint::cfg` wiring or
//! the ack-log protocol means Lane E silently passes everything).

use std::path::PathBuf;

use redlinedb_bench::config::{
    DurabilityKind, EngineKind, FailpointMatrixArgs, FailpointMatrixConfig,
};
use redlinedb_bench::failpoint_matrix;

fn matrix_path() -> PathBuf {
    redlinedb_bench::default_failpoint_matrix_path()
}

#[test]
fn matrix_toml_parses_seven_canonical_cases() {
    let path = matrix_path();
    assert!(
        path.exists(),
        "failpoint-matrix.toml must ship at {}",
        path.display()
    );
    let config = FailpointMatrixConfig::load(&path).expect("parse failpoint-matrix.toml");
    assert_eq!(
        config.cases.len(),
        7,
        "matrix should ship the seven Lane E canonical cases"
    );

    // Spot-check a few critical cases so a silent rename is loud.
    let names: Vec<&str> = config.cases.iter().map(|case| case.name.as_str()).collect();
    assert!(names.contains(&"commit-publish-kill"));
    assert!(names.contains(&"wal-write-torn"));
    assert!(names.contains(&"catalog-rename-kill"));

    // Strict durability must appear in the global default so cases
    // that omit `durabilities` still cover the contract Lane E gates.
    assert!(
        config.durabilities.contains(&DurabilityKind::Strict),
        "default durabilities must cover strict"
    );
}

#[test]
fn matrix_toml_cases_target_only_known_failpoints() {
    let config = FailpointMatrixConfig::load(&matrix_path()).expect("parse failpoint-matrix.toml");
    let allowed = [
        "wal::write_encoded",
        "wal::flush",
        "wal::flush_until",
        "wal::flush_all",
        "wal::prune",
        "engine::commit::before_publish",
        "engine::checkpoint",
        "heap::mutation",
        "index::insert",
        "index::delete",
        "index::split",
        "catalog::save::temp_write",
        "catalog::save::fsync",
        "catalog::save::rename",
        "catalog::save::parent_fsync",
        "storage::control::write",
    ];
    for case in &config.cases {
        assert!(
            allowed.contains(&case.failpoint.as_str()),
            "case {} targets unknown failpoint {}",
            case.name,
            case.failpoint
        );
    }
}

/// End-to-end run of a single case. Picks
/// `engine::commit::before_publish` because it is the most stable
/// hook (after fsync, before publish): the workload is guaranteed to
/// see the failpoint fire before the first row gets acknowledged
/// when `kill_after_n_hits = 1`, which makes the test deterministic.
///
/// The expected outcome is `lost_acked_commits == 0`: with strict
/// durability the engine must republish the CSN watermark from the
/// WAL when the database is reopened.
#[test]
fn end_to_end_commit_publish_recovers_zero_lost_acked() {
    // Use an isolated tempdir so concurrent tests cannot poison each
    // other.
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg_path = tmp.path().join("matrix.toml");
    let out_path = tmp.path().join("report.json");

    std::fs::write(
        &cfg_path,
        r#"
durabilities = ["strict"]

[[cases]]
name = "commit-publish-smoke"
failpoint = "engine::commit::before_publish"
action = "panic"
durabilities = ["strict"]
rows = 16
kill_after_n_hits = [1]
"#,
    )
    .unwrap();

    let args = FailpointMatrixArgs {
        config: cfg_path,
        out: out_path.clone(),
        seed: 7,
    };
    let report = failpoint_matrix::run(&args).expect("run failpoint matrix");
    failpoint_matrix::write_report(&out_path, &report).expect("write report");

    assert!(
        out_path.exists(),
        "matrix report must be written to {}",
        out_path.display()
    );
    assert_eq!(report.runs.len(), 1, "exactly one run expected");
    let run = &report.runs[0];
    assert_eq!(run.engine, EngineKind::Redline);
    assert_eq!(run.durability, DurabilityKind::Strict);
    assert_eq!(
        run.lost_acked_commits, 0,
        "strict durability must lose zero acked commits"
    );
    assert!(run.passed, "case must pass: {:?}", run);
    assert!(report.passed, "report must report overall pass");
}
