//! Lane GC (phase 10) — group-commit telemetry, per-core lanes, and
//! the opt-in semantic combiner.
//!
//! These tests exercise the kernel-level WAL surface (no SQL
//! involvement) and are what fig8 of the paper will reference.

use redlinedb_kernel::format::TxId;
use redlinedb_kernel::wal::{GROUP_COMMIT_BUCKET_COUNT, WalConfig, WalCoordinator, WalRecordKind};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

// ---------- Telemetry: histogram + batch counters ----------

#[test]
fn group_commit_telemetry_starts_zeroed() {
    let temp = TempDir::new().unwrap();
    let coordinator = WalCoordinator::create(temp.path(), WalConfig::default()).unwrap();
    let snapshot = coordinator.sync_counters_snapshot();
    assert_eq!(snapshot.group_commits_issued, 0);
    assert_eq!(snapshot.group_commit_batch_record_count_sum, 0);
    assert_eq!(snapshot.group_commit_batch_bytes_sum, 0);
    assert_eq!(
        snapshot.group_commit_batch_buckets,
        [0_u64; GROUP_COMMIT_BUCKET_COUNT]
    );
    assert_eq!(snapshot.batch_record_count_percentile(0.5), 0);
    assert_eq!(snapshot.batch_record_count_max(), 0);
}

#[test]
fn group_commit_telemetry_singleton_path_lands_in_bucket_zero() {
    let temp = TempDir::new().unwrap();
    let config = WalConfig {
        // Disable the latency window so the very first append is
        // fsynced on its own — bucket 0 (singleton).
        group_commit_delay_us: 0,
        ..WalConfig::default()
    };
    let coordinator = WalCoordinator::create(temp.path(), config).unwrap();
    let append = coordinator
        .append(WalRecordKind::Commit, TxId(1), vec![0_u8; 32])
        .unwrap();
    coordinator.flush_until(append.end_lsn).unwrap();

    let snap = coordinator.sync_counters_snapshot();
    assert!(
        snap.group_commits_issued >= 1,
        "expected at least one group commit, got {}",
        snap.group_commits_issued
    );
    assert!(
        snap.group_commit_batch_record_count_sum >= 1,
        "expected >=1 record covered, got {}",
        snap.group_commit_batch_record_count_sum
    );
    assert!(
        snap.group_commit_batch_buckets[0] >= 1,
        "singleton commit must land in bucket 0; got buckets={:?}",
        snap.group_commit_batch_buckets
    );
    // p50 of a singleton-only workload is bucket 0 lower edge = 1.
    assert_eq!(snap.batch_record_count_percentile(0.5), 1);
    assert!(snap.batch_record_count_max() >= 1);
}

#[test]
fn group_commit_telemetry_observes_real_batching_under_concurrency() {
    // The contract: if N threads race to commit and the writer
    // batches them, then `batch_record_count_sum > group_commits_issued`.
    let temp = TempDir::new().unwrap();
    let config = WalConfig {
        // A modest delay window so the writer thread has time to
        // pull multiple appends into a single fsync.
        group_commit_delay_us: 2_000,
        ..WalConfig::default()
    };
    let coordinator = Arc::new(WalCoordinator::create(temp.path(), config).unwrap());
    let threads = 100_usize;
    let barrier = Arc::new(Barrier::new(threads));

    let handles: Vec<_> = (0..threads)
        .map(|idx| {
            let coordinator = Arc::clone(&coordinator);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let append = coordinator
                    .append(
                        WalRecordKind::Commit,
                        TxId(idx as u64 + 1),
                        vec![idx as u8; 16],
                    )
                    .unwrap();
                coordinator.flush_until(append.end_lsn).unwrap();
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let snap = coordinator.sync_counters_snapshot();
    assert!(snap.group_commits_issued > 0);
    assert_eq!(
        snap.group_commit_batch_record_count_sum, threads as u64,
        "telemetry must account for every committed record"
    );
    assert!(
        snap.group_commit_batch_record_count_sum > snap.group_commits_issued,
        "real batching must occur: got {} records over {} group commits",
        snap.group_commit_batch_record_count_sum,
        snap.group_commits_issued
    );
    assert!(
        snap.group_commit_batch_bytes_sum >= snap.group_commit_batch_record_count_sum,
        "every record contributes >= 1 byte"
    );
    // Histogram sum equals the number of group commits.
    let hist_total: u64 = snap.group_commit_batch_buckets.iter().sum();
    assert_eq!(hist_total, snap.group_commits_issued);
    // Max batch >= mean batch lower edge.
    assert!(snap.batch_record_count_max() >= 1);
}

#[test]
fn group_commit_histogram_buckets_cover_powers_of_two() {
    // Direct unit-style test of the bucket layout via the public
    // snapshot helper. We poke synthetic counts into a temporary
    // snapshot to verify the percentile estimator picks the right
    // bucket lower edges.
    use redlinedb_kernel::wal::WalSyncCountersSnapshot;

    let mut snap = WalSyncCountersSnapshot::default();
    snap.group_commits_issued = 10;
    // 2 in bucket 0 (size 1), 7 in bucket 3 (size 8..15), 1 in bucket 6 (size 64..127).
    // Cumulative: b0=2, b3=9, b6=10.
    snap.group_commit_batch_buckets[0] = 2;
    snap.group_commit_batch_buckets[3] = 7;
    snap.group_commit_batch_buckets[6] = 1;

    // p50: ceil(10*0.5) = 5 → cumulative >= 5 first reached at b3 (lower edge 8).
    assert_eq!(snap.batch_record_count_percentile(0.5), 8);
    // p99: ceil(10*0.99) = 10 → reached at b6 (lower edge 64).
    assert_eq!(snap.batch_record_count_percentile(0.99), 64);
    // max bucket lower edge.
    assert_eq!(snap.batch_record_count_max(), 64);
    // p0 falls in the first non-empty bucket — ceil(10*0)=0, clamped to 1, b0.
    assert_eq!(snap.batch_record_count_percentile(0.0), 1);
}
