use std::env;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::Instant;

use redlinedb_sql::{Database, DbOptions, Step};
use tempfile::tempdir;

#[derive(Clone)]
struct BenchResult {
    name: String,
    query: String,
    plan: String,
    estimated_rows: f64,
    actual_rows: usize,
    elapsed_ms: f64,
    throughput_rows_per_sec: f64,
    memory_bytes: usize,
    spill_bytes: usize,
    concurrency: usize,
}

fn main() {
    let command = env::args().nth(1).unwrap_or_else(|| "smoke".to_owned());
    let results = match command.as_str() {
        "smoke" => run_smoke(),
        "optimizer-regression" => run_optimizer_regression(),
        "matrix" | "full" => run_matrix(),
        "concurrency" => run_concurrency_smoke(),
        other => {
            eprintln!("unknown benchmark command: {other}");
            std::process::exit(2);
        }
    };

    for result in results {
        println!("{}", to_json(&result));
    }
}

fn run_smoke() -> Vec<BenchResult> {
    with_database(|db| {
        let conn = db.connect();
        seed_smoke_schema(&conn);
        vec![
            bench_query(
                &conn,
                "prepared_point_lookup",
                "SELECT name FROM parent WHERE id = 64",
            ),
            bench_query(
                &conn,
                "join_lookup",
                "SELECT parent.name, child.value FROM parent INNER JOIN child ON parent.id = child.parent_id ORDER BY parent.id, child.id",
            ),
            bench_query(
                &conn,
                "group_count",
                "SELECT parent_id, COUNT(*) FROM child GROUP BY parent_id HAVING COUNT(*) = 1",
            ),
        ]
    })
}

fn run_optimizer_regression() -> Vec<BenchResult> {
    with_database(|db| {
        let conn = db.connect();
        seed_index_schema(&conn, 512);
        vec![
            bench_query(&conn, "point_lookup", "SELECT v FROM t WHERE id = 128"),
            bench_query(
                &conn,
                "secondary_index_eq",
                "SELECT id FROM t WHERE v = 'v128'",
            ),
            bench_query(
                &conn,
                "secondary_index_range",
                "SELECT id FROM t WHERE v BETWEEN 'v050' AND 'v060'",
            ),
            bench_query(
                &conn,
                "covering_projection",
                "SELECT v FROM t WHERE v = 'v128'",
            ),
            bench_query(
                &conn,
                "order_by_index_avoid_sort",
                "SELECT v FROM t ORDER BY v",
            ),
            bench_query(
                &conn,
                "order_by_limit_topn",
                "SELECT id FROM t ORDER BY v LIMIT 10",
            ),
            bench_query(
                &conn,
                "full_scan_projection_pruning",
                "SELECT id FROM t WHERE id > 0",
            ),
            bench_query(
                &conn,
                "hash_aggregate",
                "SELECT COUNT(*), SUM(id), AVG(id), MIN(id), MAX(id) FROM t",
            ),
            bench_query(
                &conn,
                "streaming_aggregate",
                "SELECT v, COUNT(*) FROM t GROUP BY v ORDER BY v",
            ),
            bench_query(
                &conn,
                "update_indexed_predicate",
                "UPDATE t SET v = 'v128-updated' WHERE v = 'v128'",
            ),
            bench_query(
                &conn,
                "delete_indexed_predicate",
                "DELETE FROM t WHERE v = 'v127'",
            ),
        ]
    })
}

fn run_concurrency_smoke() -> Vec<BenchResult> {
    with_database(|db| {
        let conn = db.connect();
        seed_index_schema(&conn, 1024);

        let results = vec![
            concurrent_point_reads(&db, 1, 256),
            concurrent_point_reads(&db, 4, 256),
            concurrent_point_reads(&db, 16, 128),
            concurrent_point_reads(&db, 64, 64),
            concurrent_mixed_workload(&db),
            concurrent_analyze_with_readers(&db),
            bench_query(
                &conn,
                "selective_index_nested_loop_join",
                "SELECT parent.name, child.value FROM parent INNER JOIN child ON parent.id = child.parent_id AND child.parent_id = 64",
            ),
        ];
        results
    })
}

fn run_matrix() -> Vec<BenchResult> {
    let mut out = Vec::new();
    out.extend(run_smoke());
    out.extend(run_optimizer_regression());
    out.extend(run_concurrency_smoke());
    out
}

fn seed_smoke_schema(conn: &Arc<redlinedb_sql::Connection>) {
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
        .expect("create parent");
    conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER, value TEXT)")
        .expect("create child");
    conn.execute("CREATE INDEX child_parent_idx ON child(parent_id)")
        .expect("create child index");
    for i in 0..256 {
        conn.execute(&format!("INSERT INTO parent VALUES ({i}, 'p{i}')"))
            .expect("insert parent");
        conn.execute(&format!(
            "INSERT INTO child VALUES ({}, {}, 'c{}')",
            i * 2,
            i,
            i * 2
        ))
        .expect("insert child");
    }
}

fn seed_index_schema(conn: &Arc<redlinedb_sql::Connection>, rows: usize) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .expect("create table");
    conn.execute("CREATE INDEX t_v_idx ON t(v)")
        .expect("create index");
    for i in 0..rows {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i:03}')"))
            .expect("insert row");
    }
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
        .expect("create parent");
    conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER, value TEXT)")
        .expect("create child");
    conn.execute("CREATE INDEX child_parent_idx ON child(parent_id)")
        .expect("create child index");
    for i in 0..rows.min(256) {
        conn.execute(&format!("INSERT INTO parent VALUES ({i}, 'p{i}')"))
            .expect("insert parent");
        conn.execute(&format!(
            "INSERT INTO child VALUES ({}, {}, 'c{}')",
            i * 2,
            i,
            i * 2
        ))
        .expect("insert child");
    }
}

fn with_database(f: impl FnOnce(&Arc<Database>) -> Vec<BenchResult>) -> Vec<BenchResult> {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("bench.db");
    let db = Database::create(&path, DbOptions::default()).expect("create db");
    f(&db)
}

fn bench_query(conn: &Arc<redlinedb_sql::Connection>, name: &str, query: &str) -> BenchResult {
    let plan = explain_detail(conn, query);
    let estimated_rows = explain_number_field(conn, query, "estimated_rows").unwrap_or(0.0);
    let memory_bytes = explain_number_field(conn, query, "memory_bytes").unwrap_or(0.0) as usize;
    let spill_bytes = explain_number_field(conn, query, "spill_bytes").unwrap_or(0.0) as usize;

    let start = Instant::now();
    let mut stmt = conn.prepare(query).expect("prepare");
    let mut rows = 0usize;
    while let Step::Row = stmt.step().expect("step") {
        rows += 1;
    }
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let actual_rows = if stmt.is_readonly() {
        rows
    } else {
        stmt.affected_rows()
    };
    BenchResult {
        name: name.to_owned(),
        query: query.to_owned(),
        plan,
        estimated_rows,
        actual_rows,
        elapsed_ms,
        throughput_rows_per_sec: if elapsed_ms > 0.0 {
            (actual_rows as f64) / (elapsed_ms / 1000.0)
        } else {
            0.0
        },
        memory_bytes,
        spill_bytes,
        concurrency: 1,
    }
}

fn concurrent_point_reads(db: &Arc<Database>, concurrency: usize, iters: usize) -> BenchResult {
    let plan = {
        let conn = db.connect();
        explain_detail(&conn, "SELECT v FROM t WHERE id = 128")
    };
    let estimated_rows = {
        let conn = db.connect();
        explain_number_field(&conn, "SELECT v FROM t WHERE id = 128", "estimated_rows")
            .unwrap_or(0.0)
    };
    let start = Instant::now();
    let barrier = Arc::new(Barrier::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let barrier = Arc::clone(&barrier);
        let conn = db.connect();
        handles.push(thread::spawn(move || {
            let mut stmt = conn
                .prepare("SELECT v FROM t WHERE id = 128")
                .expect("prepare");
            barrier.wait();
            let mut rows = 0usize;
            for _ in 0..iters {
                stmt.reset().expect("reset");
                while let Step::Row = stmt.step().expect("step") {
                    rows += 1;
                }
            }
            rows
        }));
    }

    let actual_rows = handles
        .into_iter()
        .map(|handle| handle.join().expect("join"))
        .sum::<usize>();
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    BenchResult {
        name: format!("concurrent_point_reads_{concurrency}"),
        query: "SELECT v FROM t WHERE id = 128".to_owned(),
        plan,
        estimated_rows,
        actual_rows,
        elapsed_ms,
        throughput_rows_per_sec: if elapsed_ms > 0.0 {
            (actual_rows as f64) / (elapsed_ms / 1000.0)
        } else {
            0.0
        },
        memory_bytes: 0,
        spill_bytes: 0,
        concurrency,
    }
}

fn concurrent_mixed_workload(db: &Arc<Database>) -> BenchResult {
    let concurrency = 8;
    let barrier = Arc::new(Barrier::new(concurrency));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for tid in 0..concurrency {
        let barrier = Arc::clone(&barrier);
        let conn = db.connect();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut rows = 0usize;
            for i in 0..64 {
                let id = (tid * 16 + i) % 1024;
                let mut select = conn
                    .prepare(&format!("SELECT v FROM t WHERE id = {id}"))
                    .expect("prepare select");
                while let Step::Row = select.step().expect("step select") {
                    rows += 1;
                }
                let _ = conn.execute(&format!("UPDATE t SET v = 'v{id:03}' WHERE id = {id}"));
            }
            rows
        }));
    }

    let actual_rows = handles
        .into_iter()
        .map(|handle| handle.join().expect("join"))
        .sum::<usize>();
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    BenchResult {
        name: "concurrent_mixed_read_write".to_owned(),
        query: "mixed read/write workload".to_owned(),
        plan: "concurrent mixed workload".to_owned(),
        estimated_rows: 0.0,
        actual_rows,
        elapsed_ms,
        throughput_rows_per_sec: if elapsed_ms > 0.0 {
            (actual_rows as f64) / (elapsed_ms / 1000.0)
        } else {
            0.0
        },
        memory_bytes: 0,
        spill_bytes: 0,
        concurrency,
    }
}

fn concurrent_analyze_with_readers(db: &Arc<Database>) -> BenchResult {
    let reader_count = 4;
    let barrier = Arc::new(Barrier::new(reader_count + 1));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(reader_count);
    for _ in 0..reader_count {
        let barrier = Arc::clone(&barrier);
        let conn = db.connect();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut total = 0usize;
            for _ in 0..32 {
                let mut stmt = conn
                    .prepare("SELECT COUNT(*) FROM t WHERE v >= 'v100'")
                    .expect("prepare reader");
                while let Step::Row = stmt.step().expect("step reader") {
                    total += 1;
                }
            }
            total
        }));
    }

    let analyzer = {
        let barrier = Arc::clone(&barrier);
        let conn = db.connect();
        thread::spawn(move || {
            barrier.wait();
            conn.execute("ANALYZE t").expect("analyze");
            0usize
        })
    };

    let actual_rows = handles
        .into_iter()
        .map(|handle| handle.join().expect("join"))
        .sum::<usize>()
        + analyzer.join().expect("join");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    BenchResult {
        name: "analyze_while_readers_run".to_owned(),
        query: "ANALYZE t".to_owned(),
        plan: "online analyze with readers".to_owned(),
        estimated_rows: 0.0,
        actual_rows,
        elapsed_ms,
        throughput_rows_per_sec: if elapsed_ms > 0.0 {
            (actual_rows as f64) / (elapsed_ms / 1000.0)
        } else {
            0.0
        },
        memory_bytes: 0,
        spill_bytes: 0,
        concurrency: reader_count + 1,
    }
}

fn explain_detail(conn: &Arc<redlinedb_sql::Connection>, query: &str) -> String {
    let explain = format!("EXPLAIN QUERY PLAN {query}");
    let mut stmt = conn.prepare(&explain).expect("prepare explain");
    let mut detail = String::new();
    while let Step::Row = stmt.step().expect("step explain") {
        if !detail.is_empty() {
            detail.push(' ');
        }
        detail.push_str(stmt.column_text(3).expect("detail"));
    }
    detail
}

fn explain_number_field(
    conn: &Arc<redlinedb_sql::Connection>,
    query: &str,
    field: &str,
) -> Option<f64> {
    let explain = format!("EXPLAIN FORMAT JSON {query}");
    let mut stmt = conn.prepare(&explain).expect("prepare explain json");
    let mut json = String::new();
    while let Step::Row = stmt.step().expect("step explain json") {
        json.push_str(stmt.column_text(0).expect("json"));
    }
    parse_json_number_field(&json, field)
}

fn parse_json_number_field(json: &str, field: &str) -> Option<f64> {
    let needle = format!("\"{field}\":");
    let start = json.find(&needle)? + needle.len();
    let tail = &json[start..];
    let end = tail.find([',', '}']).unwrap_or(tail.len());
    tail[..end].trim().parse().ok()
}

fn to_json(result: &BenchResult) -> String {
    format!(
        concat!(
            "{{",
            "\"name\":\"{}\",",
            "\"query\":\"{}\",",
            "\"plan\":\"{}\",",
            "\"estimated_rows\":{},",
            "\"actual_rows\":{},",
            "\"elapsed_ms\":{:.3},",
            "\"throughput_rows_per_sec\":{:.3},",
            "\"memory_bytes\":{},",
            "\"spill_bytes\":{},",
            "\"concurrency\":{}",
            "}}"
        ),
        escape(&result.name),
        escape(&result.query),
        escape(&result.plan),
        result.estimated_rows,
        result.actual_rows,
        result.elapsed_ms,
        result.throughput_rows_per_sec,
        result.memory_bytes,
        result.spill_bytes,
        result.concurrency,
    )
}

fn escape(input: &str) -> String {
    input
        .chars()
        .flat_map(|ch| match ch {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            other => vec![other],
        })
        .collect()
}
