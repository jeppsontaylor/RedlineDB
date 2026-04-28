use std::sync::Arc;

use redlinedb_sql::{Database, DbOptions, Step};
use tempfile::tempdir;

fn open_database() -> (tempfile::TempDir, Arc<redlinedb_sql::Connection>) {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("redlinedb-sql-smoke.db");
    let db = Database::create(&path, DbOptions::default()).expect("create database");
    let conn = db.connect();
    (dir, conn)
}

fn open_database_with_options(
    opts: DbOptions,
) -> (tempfile::TempDir, Arc<redlinedb_sql::Connection>) {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("redlinedb-sql-smoke.db");
    let db = Database::create(&path, opts).expect("create database");
    let conn = db.connect();
    (dir, conn)
}

fn step_done(stmt: &mut redlinedb_sql::Statement) {
    assert_eq!(stmt.step().expect("step"), Step::Done);
}

#[test]
fn create_insert_select_round_trip() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES (1, 'one')")
        .expect("insert row");
    conn.execute("INSERT INTO t VALUES (2, 'two')")
        .expect("insert row");

    let mut stmt = conn
        .prepare("SELECT a, b FROM t ORDER BY a")
        .expect("prepare select");

    assert_eq!(stmt.step().expect("first step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("a"), 1);
    assert_eq!(stmt.column_text(1).expect("b"), "one");

    assert_eq!(stmt.step().expect("second step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("a"), 2);
    assert_eq!(stmt.column_text(1).expect("b"), "two");

    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn begin_commit_and_rollback_persist_and_discard_rows() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
        .expect("create table");

    {
        let mut begin = conn.prepare("BEGIN").expect("prepare begin");
        step_done(&mut begin);
        let mut insert = conn
            .prepare("INSERT INTO t VALUES (1, 'committed')")
            .expect("prepare insert");
        step_done(&mut insert);
        let mut commit = conn.prepare("COMMIT").expect("prepare commit");
        step_done(&mut commit);
    }

    {
        let mut select = conn
            .prepare("SELECT a, b FROM t ORDER BY a")
            .expect("prepare select");
        assert_eq!(select.step().expect("step"), Step::Row);
        assert_eq!(select.column_i64(0).expect("a"), 1);
        assert_eq!(select.column_text(1).expect("b"), "committed");
        assert_eq!(select.step().expect("done"), Step::Done);
    }

    {
        let mut begin = conn.prepare("BEGIN").expect("prepare begin");
        step_done(&mut begin);
        let mut insert = conn
            .prepare("INSERT INTO t VALUES (2, 'rolled back')")
            .expect("prepare insert");
        step_done(&mut insert);
        let mut rollback = conn.prepare("ROLLBACK").expect("prepare rollback");
        step_done(&mut rollback);
    }

    let mut select = conn
        .prepare("SELECT a, b FROM t ORDER BY a")
        .expect("prepare select");
    assert_eq!(select.step().expect("step"), Step::Row);
    assert_eq!(select.column_i64(0).expect("a"), 1);
    assert_eq!(select.column_text(1).expect("b"), "committed");
    assert_eq!(select.step().expect("done"), Step::Done);
}

#[test]
fn sqlite_schema_lists_created_objects() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT NOT NULL)")
        .expect("create table");
    conn.execute("CREATE INDEX t_b_idx ON t(b)")
        .expect("create index");

    let mut stmt = conn
        .prepare("SELECT type, name, tbl_name FROM sqlite_schema ORDER BY name")
        .expect("prepare schema query");

    let mut rows = Vec::new();
    while let Step::Row = stmt.step().expect("step") {
        rows.push((
            stmt.column_text(0).expect("type").to_owned(),
            stmt.column_text(1).expect("name").to_owned(),
            stmt.column_text(2).expect("tbl").to_owned(),
        ));
    }

    assert!(
        rows.iter()
            .any(|row| row.0 == "table" && row.1 == "t" && row.2 == "t")
    );
    assert!(
        rows.iter()
            .any(|row| row.0 == "index" && row.1 == "t_b_idx" && row.2 == "t")
    );
}

#[test]
fn execute_participates_in_explicit_transactions() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .expect("create table");

    let mut begin = conn.prepare("BEGIN").expect("prepare begin");
    step_done(&mut begin);
    conn.execute("INSERT INTO t VALUES (1, 'tx row')")
        .expect("insert in tx");
    let mut commit = conn.prepare("COMMIT").expect("prepare commit");
    step_done(&mut commit);

    let mut stmt = conn
        .prepare("SELECT b FROM t WHERE a = 1")
        .expect("prepare select");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_text(0).expect("b"), "tx row");
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn sqlite_expressions_cover_case_like_and_blob_literals() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(x TEXT, b BLOB)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES ('Alpha', x'4142')")
        .expect("insert");

    let mut stmt = conn
        .prepare(
            "SELECT CASE WHEN x LIKE 'a%' THEN 'yes' ELSE 'no' END, \
             x IS DISTINCT FROM 'beta', \
             b = x'4142' \
             FROM t",
        )
        .expect("prepare select");

    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_text(0).expect("case"), "yes");
    assert_eq!(stmt.column_i64(1).expect("distinct"), 1);
    assert_eq!(stmt.column_i64(2).expect("blob"), 1);
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn sqlite_core_functions_cover_round_hex_quote_random_and_glob() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(x TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES ('alpha')")
        .expect("insert");

    let mut stmt = conn
        .prepare(
            "SELECT round(1.25, 1), hex(x'4142'), quote('O''Reilly'), \
             likely(1), unlikely(0), likelihood(1, 0.25), random(), \
             glob('alpha', 'a*'), glob('alpha', 'b*') FROM t",
        )
        .expect("prepare select");

    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_f64(0).expect("round"), 1.3);
    assert_eq!(stmt.column_text(1).expect("hex"), "4142");
    assert_eq!(stmt.column_text(2).expect("quote"), "'O''Reilly'");
    assert_eq!(stmt.column_i64(3).expect("likely"), 1);
    assert_eq!(stmt.column_i64(4).expect("unlikely"), 0);
    assert_eq!(stmt.column_i64(5).expect("likelihood"), 1);
    let _ = stmt.column_i64(6).expect("random");
    assert_eq!(stmt.column_i64(7).expect("glob true"), 1);
    assert_eq!(stmt.column_i64(8).expect("glob false"), 0);
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn statement_parameters_and_clear_bindings_work() {
    let (_dir, conn) = open_database();

    let mut stmt = conn
        .prepare("SELECT ?1 + ?2, :named, ?2")
        .expect("prepare statement");
    assert_eq!(stmt.parameter_count(), 3);
    assert_eq!(stmt.parameter_index("?1"), Some(1));
    assert_eq!(stmt.parameter_index("?2"), Some(2));
    assert_eq!(stmt.parameter_index(":named"), Some(3));

    stmt.bind_i64(1, 4).expect("bind 1");
    stmt.bind_i64(2, 5).expect("bind 2");
    stmt.bind_named(":named", redlinedb_sql::SqlValue::Integer(9))
        .expect("bind named");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("sum"), 9);
    assert_eq!(stmt.column_i64(1).expect("named"), 9);
    assert_eq!(stmt.column_i64(2).expect("repeat"), 5);
    assert_eq!(stmt.step().expect("done"), Step::Done);

    stmt.reset().expect("reset");
    stmt.clear_bindings();
    stmt.bind_i64(1, 1).expect("bind 1");
    stmt.bind_i64(2, 2).expect("bind 2");
    stmt.bind_named(":named", redlinedb_sql::SqlValue::Integer(3))
        .expect("bind named");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("sum"), 3);
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn implicit_rowids_come_from_the_kernel_allocator() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t(v) VALUES ('one')")
        .expect("insert 1");
    conn.execute("INSERT INTO t(v) VALUES ('two')")
        .expect("insert 2");

    let mut stmt = conn
        .prepare("SELECT id FROM t ORDER BY id")
        .expect("prepare select");

    assert_eq!(stmt.step().expect("step"), Step::Row);
    let first = stmt.column_i64(0).expect("first rowid");
    assert!(first > 0);
    assert_eq!(stmt.step().expect("step"), Step::Row);
    let second = stmt.column_i64(0).expect("second rowid");
    assert!(second > first);
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn execute_returns_read_and_write_counts() {
    let (_dir, conn) = open_database();

    assert_eq!(
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
            .expect("create table"),
        1
    );
    assert_eq!(
        conn.execute("INSERT INTO t VALUES (1, 'one')")
            .expect("insert"),
        1
    );
    assert_eq!(conn.execute("SELECT a, b FROM t").expect("select"), 1);
}

#[test]
fn prepared_statements_auto_reprepare_after_schema_change() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES (1, 'one')")
        .expect("insert row");

    let mut stmt = conn
        .prepare("SELECT b FROM t WHERE a = 1")
        .expect("prepare select");

    conn.execute("CREATE TABLE bump(x INTEGER)")
        .expect("bump schema");

    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_text(0).expect("b"), "one");
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn sqlite_oracle_smoke_if_available() {
    if std::env::var_os("REDLINEDB_SQLITE_DIFF").is_none() {
        return;
    }

    let sqlite3 = match std::process::Command::new("sqlite3")
        .arg("-version")
        .output()
    {
        Ok(output) if output.status.success() => "sqlite3",
        _ => return,
    };

    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("oracle.db");
    let db = Database::create(&path, DbOptions::default()).expect("create db");
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES (1, 'one')")
        .expect("insert");
    conn.execute("INSERT INTO t VALUES (2, 'two')")
        .expect("insert");

    let redline_rows = {
        let mut stmt = conn
            .prepare("SELECT a, b FROM t ORDER BY a")
            .expect("prepare select");
        let mut rows = Vec::new();
        while let Step::Row = stmt.step().expect("step") {
            rows.push((
                stmt.column_i64(0).expect("a"),
                stmt.column_text(1).expect("b").to_owned(),
            ));
        }
        rows
    };

    let sql = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); \
               INSERT INTO t VALUES (1, 'one'); \
               INSERT INTO t VALUES (2, 'two'); \
               SELECT a, b FROM t ORDER BY a;";
    let output = std::process::Command::new(sqlite3)
        .arg(&path)
        .arg("-batch")
        .arg("-noheader")
        .arg("-separator")
        .arg("|")
        .arg(sql)
        .output()
        .expect("run sqlite3");
    assert!(output.status.success(), "sqlite3 diff command failed");
    let sqlite_rows = String::from_utf8(output.stdout)
        .expect("sqlite utf8")
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut parts = line.split('|');
            let a = parts.next().expect("a").parse::<i64>().expect("a int");
            let b = parts.next().expect("b").to_owned();
            (a, b)
        })
        .collect::<Vec<_>>();

    assert_eq!(redline_rows, sqlite_rows);
}

#[test]
fn analyze_and_explain_return_rows() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES (1, 'one')")
        .expect("insert");
    conn.execute("INSERT INTO t VALUES (2, 'two')")
        .expect("insert");

    conn.execute("ANALYZE").expect("analyze");

    let mut explain = conn
        .prepare("EXPLAIN QUERY PLAN SELECT b FROM t WHERE a = 1")
        .expect("prepare explain");
    assert_eq!(explain.column_count(), 4);
    let mut plan_rows = 0usize;
    while let Step::Row = explain.step().expect("step") {
        plan_rows += 1;
        assert!(!explain.column_text(3).expect("detail").is_empty());
    }
    assert!(plan_rows >= 1);

    let mut explain_json = conn
        .prepare("EXPLAIN FORMAT JSON SELECT b FROM t")
        .expect("prepare explain json");
    assert_eq!(explain_json.step().expect("step"), Step::Row);
    assert!(
        explain_json
            .column_text(0)
            .expect("json")
            .contains("\"kind\"")
    );
    assert_eq!(explain_json.step().expect("done"), Step::Done);

    let mut analyze = conn
        .prepare("EXPLAIN ANALYZE SELECT b FROM t ORDER BY a")
        .expect("prepare explain analyze");
    assert_eq!(analyze.step().expect("step"), Step::Row);
    assert!(!analyze.column_text(0).expect("analyze").is_empty());
    assert_eq!(analyze.step().expect("done"), Step::Done);
}

#[test]
fn spill_files_are_created_and_removed_for_sort_queries() {
    let opts = DbOptions {
        query_memory: redlinedb_sql::QueryMemoryConfig {
            work_mem_bytes: 1,
            max_spill_bytes: 1024 * 1024,
            batch_rows: 1024,
        },
        ..DbOptions::default()
    };
    let (_dir, conn) = open_database_with_options(opts);

    conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
        .expect("create table");
    for i in 0..128 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 'value-{i:03}')"))
            .expect("insert row");
    }

    let baseline = spill_file_count();
    {
        let mut stmt = conn
            .prepare("SELECT a FROM t ORDER BY b")
            .expect("prepare select");
        assert_eq!(stmt.step().expect("first row"), Step::Row);
        assert!(
            spill_file_count() > baseline,
            "expected spill file while the statement was active"
        );
        while let Step::Row = stmt.step().expect("step to completion") {}
    }
    assert_eq!(spill_file_count(), baseline, "spill file should be removed");
}

fn spill_file_count() -> usize {
    std::fs::read_dir(std::env::temp_dir())
        .expect("temp dir listing")
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("redline-query-")
        })
        .count()
}

#[test]
fn inner_join_and_grouped_aggregate_work() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
        .expect("create parent");
    conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER, value TEXT)")
        .expect("create child");

    conn.execute("INSERT INTO parent VALUES (1, 'one')")
        .expect("insert parent");
    conn.execute("INSERT INTO parent VALUES (2, 'two')")
        .expect("insert parent");
    conn.execute("INSERT INTO child VALUES (10, 1, 'alpha')")
        .expect("insert child");
    conn.execute("INSERT INTO child VALUES (11, 1, 'beta')")
        .expect("insert child");
    conn.execute("INSERT INTO child VALUES (12, 2, 'gamma')")
        .expect("insert child");

    let mut join = conn
        .prepare(
            "SELECT parent.id, child.value \
             FROM parent INNER JOIN child ON parent.id = child.parent_id \
             ORDER BY parent.id, child.id",
        )
        .expect("prepare join");
    let mut join_rows = Vec::new();
    while let Step::Row = join.step().expect("join step") {
        join_rows.push((
            join.column_i64(0).expect("parent id"),
            join.column_text(1).expect("value").to_owned(),
        ));
    }
    assert_eq!(
        join_rows,
        vec![
            (1, "alpha".to_owned()),
            (1, "beta".to_owned()),
            (2, "gamma".to_owned())
        ]
    );

    let mut grouped = conn
        .prepare(
            "SELECT parent_id, COUNT(*), SUM(id) FROM child GROUP BY parent_id HAVING COUNT(*) > 1",
        )
        .expect("prepare grouped");
    assert_eq!(grouped.step().expect("grouped step"), Step::Row);
    assert_eq!(grouped.column_i64(0).expect("parent_id"), 1);
    assert_eq!(grouped.column_i64(1).expect("count"), 2);
    assert_eq!(grouped.column_i64(2).expect("sum"), 21);
    assert_eq!(grouped.step().expect("grouped done"), Step::Done);
}
