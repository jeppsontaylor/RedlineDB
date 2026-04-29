use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::ValueEnum;
use crossbeam_utils::thread;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::config::{RunSpec, WorkloadKind};
use crate::engine::{self, BenchConn, BenchEngine, CellValue};
use crate::metrics::Metrics;
use crate::process_metrics;
use crate::report::{MetricsSummary, RunRecord};

pub fn run_once(spec: &RunSpec) -> Result<RunRecord> {
    let db_dir = spec.base_dir.join(format!(
        "{}-{}-{}-t{}-s{}",
        spec.engine.to_possible_value().expect("value").get_name(),
        spec.workload.as_str(),
        spec.durability.as_str(),
        spec.threads,
        spec.seed
    ));
    let _ = std::fs::remove_dir_all(&db_dir);
    let engine = engine::open(spec, &db_dir)?;
    engine.setup_schema()?;
    if !matches!(
        spec.workload,
        WorkloadKind::SingleRowInsert | WorkloadKind::BatchedInsert100
    ) {
        engine.seed_kv(spec.rows)?;
    }
    let started = Instant::now();
    let metrics = run_workload(engine.as_ref(), spec)?;
    engine.checkpoint()?;
    let snapshot = engine.snapshot()?;
    let checksum = engine.checksum()?;
    let elapsed = started.elapsed();
    let process = process_metrics::collect_self();
    Ok(RunRecord {
        run_id: crate::report::next_run_id(spec.engine, spec.workload),
        engine: spec.engine,
        workload: spec.workload,
        durability: spec.durability,
        threads: spec.threads,
        seed: spec.seed,
        cache_bytes: spec.cache_bytes,
        environment: crate::report::collect_environment(),
        metrics: MetricsSummary {
            operations: metrics.operations(),
            failures: metrics.failures(),
            busy_errors: metrics.busy_errors(),
            elapsed_ms: elapsed.as_millis() as u64,
            throughput_ops_per_sec: throughput(metrics.operations(), elapsed),
            latency: metrics.latency(),
        },
        checksum,
        data_bytes: snapshot.data_bytes,
        wal_bytes: snapshot.wal_bytes,
        engine_stats: snapshot.engine_stats,
        process_metrics: Some(process),
    })
}

fn run_workload(engine: &dyn BenchEngine, spec: &RunSpec) -> Result<Metrics> {
    let barrier = Arc::new(Barrier::new(spec.threads));
    let deadline = Instant::now() + spec.duration;
    let mut merged = Metrics::new();
    let scope_result = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(spec.threads);
        for worker in 0..spec.threads {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move |_| {
                let mut conn = engine.connect(worker)?;
                let mut rng = ChaCha8Rng::seed_from_u64(spec.seed ^ worker as u64);
                barrier.wait();
                let mut metrics = Metrics::new();
                while Instant::now() < deadline {
                    let start = Instant::now();
                    let result = match spec.workload {
                        WorkloadKind::SingleRowInsert => {
                            single_row_insert(&mut *conn, worker, &mut rng)
                        }
                        WorkloadKind::BatchedInsert100 => {
                            batched_insert(&mut *conn, worker, &mut rng)
                        }
                        WorkloadKind::PointReadPk => point_read(&mut *conn, spec.rows, &mut rng),
                        WorkloadKind::SecondaryIndexRead => {
                            secondary_index_read(&mut *conn, spec.rows, &mut rng)
                        }
                        WorkloadKind::SecondaryIndexRange => {
                            secondary_index_range(&mut *conn, spec.rows, &mut rng)
                        }
                        WorkloadKind::WritersDisjoint => {
                            update_disjoint(&mut *conn, worker, spec.threads, spec.rows, &mut rng)
                        }
                        WorkloadKind::HotRowUpdate => hot_row_update(&mut *conn, worker, &mut rng),
                        WorkloadKind::MixedOltp => {
                            mixed_oltp(&mut *conn, worker, spec.threads, spec.rows, &mut rng)
                        }
                        WorkloadKind::Mixed95Read5Write => {
                            mixed_ratio(&mut *conn, worker, spec.threads, spec.rows, &mut rng, 95)
                        }
                        WorkloadKind::Mixed80Read20Write => {
                            mixed_ratio(&mut *conn, worker, spec.threads, spec.rows, &mut rng, 80)
                        }
                        WorkloadKind::Mixed50Read50Write => {
                            mixed_ratio(&mut *conn, worker, spec.threads, spec.rows, &mut rng, 50)
                        }
                    };
                    match result {
                        Ok(()) => metrics.record_success(start.elapsed()),
                        Err(err) => metrics.record_failure(is_busy_error(&err)),
                    }
                }
                Ok::<Metrics, anyhow::Error>(metrics)
            }));
        }
        for handle in handles {
            merged.merge(&handle.join().expect("worker panicked")?);
        }
        Ok::<(), anyhow::Error>(())
    });
    match scope_result {
        Ok(result) => result?,
        Err(_) => anyhow::bail!("worker thread panicked"),
    }
    Ok(merged)
}

fn single_row_insert(conn: &mut dyn BenchConn, worker: usize, rng: &mut ChaCha8Rng) -> Result<()> {
    let key = ((worker as u64) << 32 | rng.random::<u32>() as u64) as i64;
    let params = [
        CellValue::Integer(key),
        CellValue::Integer((key % 32).abs()),
        CellValue::Blob(blob_for(key as usize)),
        CellValue::Integer(1),
    ];
    let _ = conn.execute(
        "INSERT INTO kv(k, tenant, v, version) VALUES (?1, ?2, ?3, ?4)",
        &params,
    )?;
    Ok(())
}

fn batched_insert(conn: &mut dyn BenchConn, worker: usize, rng: &mut ChaCha8Rng) -> Result<()> {
    conn.begin_immediate()?;
    for _ in 0..100 {
        single_row_insert(conn, worker, rng)?;
    }
    conn.commit()?;
    Ok(())
}

fn point_read(conn: &mut dyn BenchConn, rows: usize, rng: &mut ChaCha8Rng) -> Result<()> {
    let key = (rng.random_range(0..rows.max(1))) as i64;
    let _ = conn.query_row(
        "SELECT version, v FROM kv WHERE k = ?1",
        &[CellValue::Integer(key)],
    )?;
    Ok(())
}

fn secondary_index_read(conn: &mut dyn BenchConn, rows: usize, rng: &mut ChaCha8Rng) -> Result<()> {
    let tenant = (rng.random_range(0..rows.max(1)) % 32) as i64;
    let _ = conn.query_row(
        "SELECT k, v FROM kv WHERE tenant = ?1 ORDER BY k LIMIT 1",
        &[CellValue::Integer(tenant)],
    )?;
    Ok(())
}

fn secondary_index_range(
    conn: &mut dyn BenchConn,
    rows: usize,
    rng: &mut ChaCha8Rng,
) -> Result<()> {
    let tenant = (rng.random_range(0..rows.max(1)) % 32) as i64;
    let high = (tenant + 3).min(31);
    let _ = conn.query_row(
        "SELECT COUNT(*) FROM kv WHERE tenant BETWEEN ?1 AND ?2",
        &[CellValue::Integer(tenant), CellValue::Integer(high)],
    )?;
    Ok(())
}

fn update_disjoint(
    conn: &mut dyn BenchConn,
    worker: usize,
    threads: usize,
    rows: usize,
    rng: &mut ChaCha8Rng,
) -> Result<()> {
    let lane = worker % threads.max(1);
    let span = rows.max(threads) / threads.max(1);
    let start = lane * span;
    let key = (start + rng.random_range(0..span.max(1))) as i64;
    let params = [
        CellValue::Blob(blob_for((key as usize).wrapping_add(1))),
        CellValue::Integer(key),
    ];
    let _ = conn.execute(
        "UPDATE kv SET v = ?1, version = version + 1 WHERE k = ?2",
        &params,
    )?;
    Ok(())
}

fn hot_row_update(conn: &mut dyn BenchConn, worker: usize, rng: &mut ChaCha8Rng) -> Result<()> {
    let params = [
        CellValue::Blob(blob_for((worker << 24) ^ rng.random::<u32>() as usize)),
        CellValue::Integer(0),
    ];
    let _ = conn.execute(
        "UPDATE kv SET v = ?1, version = version + 1 WHERE k = ?2",
        &params,
    )?;
    Ok(())
}

fn mixed_oltp(
    conn: &mut dyn BenchConn,
    worker: usize,
    threads: usize,
    rows: usize,
    rng: &mut ChaCha8Rng,
) -> Result<()> {
    if rng.random_range(0..10) < 8 {
        point_read(conn, rows, rng)
    } else {
        update_disjoint(conn, worker, threads, rows, rng)
    }
}

fn mixed_ratio(
    conn: &mut dyn BenchConn,
    worker: usize,
    threads: usize,
    rows: usize,
    rng: &mut ChaCha8Rng,
    read_pct: u32,
) -> Result<()> {
    if rng.random_range(0..100) < read_pct {
        point_read(conn, rows, rng)
    } else {
        update_disjoint(conn, worker, threads, rows, rng)
    }
}

fn blob_for(seed: usize) -> Vec<u8> {
    format!("value-{seed:08}").into_bytes()
}

fn is_busy_error(err: &anyhow::Error) -> bool {
    err.to_string().to_ascii_lowercase().contains("busy")
        || err.to_string().to_ascii_lowercase().contains("locked")
}

fn throughput(operations: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds > 0.0 {
        operations as f64 / seconds
    } else {
        0.0
    }
}
