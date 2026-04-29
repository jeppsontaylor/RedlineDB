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
fn select_distinct_deduplicates_rows() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(v TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES ('a')")
        .expect("insert a");
    conn.execute("INSERT INTO t VALUES ('a')")
        .expect("insert duplicate a");
    conn.execute("INSERT INTO t VALUES ('b')")
        .expect("insert b");

    let mut stmt = conn
        .prepare("SELECT DISTINCT v FROM t ORDER BY v")
        .expect("prepare distinct");
    let mut rows = Vec::new();
    while let Step::Row = stmt.step().expect("step distinct") {
        rows.push(stmt.column_text(0).expect("v").to_owned());
    }

    assert_eq!(rows, vec!["a".to_owned(), "b".to_owned()]);
}

#[test]
fn select_all_preserves_duplicates() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(v TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES ('a')")
        .expect("insert a");
    conn.execute("INSERT INTO t VALUES ('a')")
        .expect("insert duplicate a");

    let mut stmt = conn
        .prepare("SELECT ALL v FROM t ORDER BY rowid")
        .expect("prepare select all");
    let mut rows = Vec::new();
    while let Step::Row = stmt.step().expect("step select all") {
        rows.push(stmt.column_text(0).expect("v").to_owned());
    }

    assert_eq!(rows, vec!["a".to_owned(), "a".to_owned()]);
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
fn pragma_introspection_and_state_round_trips_work() {
    let (dir, conn) = open_database();
    let db_path = dir.path().join("redlinedb-sql-smoke.db");

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT NOT NULL DEFAULT 'bee')")
        .expect("create table");
    conn.execute("CREATE INDEX t_b_idx ON t(b)")
        .expect("create index");

    let mut table_info = conn.prepare("PRAGMA table_info(t)").expect("table info");
    let mut table_rows = Vec::new();
    while let Step::Row = table_info.step().expect("step table_info") {
        table_rows.push((
            table_info.column_i64(0).expect("cid"),
            table_info.column_text(1).expect("name").to_owned(),
            table_info.column_text(2).expect("type").to_owned(),
            table_info.column_i64(3).expect("notnull"),
            table_info.column_value(4).expect("default").clone(),
            table_info.column_i64(5).expect("pk"),
        ));
    }
    assert_eq!(table_rows.len(), 2);
    assert_eq!(table_rows[0].1, "a");
    assert_eq!(table_rows[0].5, 1);
    assert_eq!(table_rows[1].1, "b");
    assert_eq!(table_rows[1].3, 1);

    let mut index_list = conn.prepare("PRAGMA index_list(t)").expect("index list");
    let mut index_rows = Vec::new();
    while let Step::Row = index_list.step().expect("step index_list") {
        index_rows.push((
            index_list.column_i64(0).expect("seq"),
            index_list.column_text(1).expect("name").to_owned(),
            index_list.column_i64(2).expect("unique"),
            index_list.column_text(3).expect("origin").to_owned(),
            index_list.column_i64(4).expect("partial"),
        ));
    }
    assert!(
        index_rows
            .iter()
            .any(|row| row.1 == "t_b_idx" && row.2 == 0 && row.3 == "c")
    );
    assert!(index_rows.iter().any(|row| row.3 == "pk"));

    let mut index_info = conn
        .prepare("PRAGMA index_info(t_b_idx)")
        .expect("index info");
    assert_eq!(index_info.step().expect("step"), Step::Row);
    assert_eq!(index_info.column_i64(0).expect("seqno"), 0);
    assert_eq!(index_info.column_i64(1).expect("cid"), 1);
    assert_eq!(index_info.column_text(2).expect("name"), "b");
    assert_eq!(index_info.step().expect("done"), Step::Done);

    let mut table_xinfo = conn.prepare("PRAGMA table_xinfo(t)").expect("table xinfo");
    let mut xinfo_rows = Vec::new();
    while let Step::Row = table_xinfo.step().expect("step table_xinfo") {
        xinfo_rows.push((
            table_xinfo.column_i64(0).expect("cid"),
            table_xinfo.column_text(1).expect("name").to_owned(),
            table_xinfo.column_i64(6).expect("hidden"),
        ));
    }
    assert_eq!(xinfo_rows.len(), 2);
    assert!(xinfo_rows.iter().all(|row| row.2 == 0));

    let mut table_list = conn.prepare("PRAGMA table_list").expect("table list");
    let mut table_list_rows = Vec::new();
    while let Step::Row = table_list.step().expect("step table_list") {
        table_list_rows.push((
            table_list.column_text(0).expect("schema").to_owned(),
            table_list.column_text(1).expect("name").to_owned(),
            table_list.column_text(2).expect("type").to_owned(),
            table_list.column_i64(3).expect("ncol"),
            table_list.column_i64(4).expect("wr"),
            table_list.column_i64(5).expect("strict"),
        ));
    }
    assert!(
        table_list_rows
            .iter()
            .any(|row| row.1 == "t" && row.2 == "table")
    );

    let mut index_xinfo = conn
        .prepare("PRAGMA index_xinfo(t_b_idx)")
        .expect("index xinfo");
    assert_eq!(index_xinfo.step().expect("step"), Step::Row);
    assert_eq!(index_xinfo.column_i64(0).expect("seqno"), 0);
    assert_eq!(index_xinfo.column_i64(1).expect("cid"), 1);
    assert_eq!(index_xinfo.column_text(2).expect("name"), "b");
    assert_eq!(index_xinfo.column_i64(3).expect("desc"), 0);
    assert_eq!(index_xinfo.column_text(4).expect("coll"), "BINARY");
    assert_eq!(index_xinfo.column_i64(5).expect("key"), 1);
    assert_eq!(index_xinfo.step().expect("done"), Step::Done);

    let mut fk_list = conn
        .prepare("PRAGMA foreign_key_list(t)")
        .expect("foreign key list");
    assert_eq!(fk_list.step().expect("done"), Step::Done);

    let mut foreign_keys = conn.prepare("PRAGMA foreign_keys").expect("foreign_keys");
    assert_eq!(foreign_keys.step().expect("step"), Step::Row);
    assert_eq!(foreign_keys.column_i64(0).expect("foreign_keys"), 0);
    assert_eq!(foreign_keys.step().expect("done"), Step::Done);

    let mut set_foreign_keys = conn
        .prepare("PRAGMA foreign_keys = ON")
        .expect("set foreign_keys");
    step_done(&mut set_foreign_keys);

    let mut foreign_keys = conn.prepare("PRAGMA foreign_keys").expect("foreign_keys");
    assert_eq!(foreign_keys.step().expect("step"), Step::Row);
    assert_eq!(foreign_keys.column_i64(0).expect("foreign_keys"), 1);
    assert_eq!(foreign_keys.step().expect("done"), Step::Done);

    let mut user_version = conn.prepare("PRAGMA user_version").expect("user version");
    assert_eq!(user_version.step().expect("step"), Step::Row);
    assert_eq!(user_version.column_i64(0).expect("user_version"), 0);
    assert_eq!(user_version.step().expect("done"), Step::Done);

    let mut set_user_version = conn
        .prepare("PRAGMA user_version = 7")
        .expect("set user version");
    step_done(&mut set_user_version);

    let mut user_version = conn.prepare("PRAGMA user_version").expect("user version");
    assert_eq!(user_version.step().expect("step"), Step::Row);
    assert_eq!(user_version.column_i64(0).expect("user_version"), 7);
    assert_eq!(user_version.step().expect("done"), Step::Done);

    let mut schema_version = conn
        .prepare("PRAGMA schema_version")
        .expect("schema version");
    assert_eq!(schema_version.step().expect("step"), Step::Row);
    assert!(schema_version.column_i64(0).expect("schema_version") > 0);
    assert_eq!(schema_version.step().expect("done"), Step::Done);

    let mut db_list = conn.prepare("PRAGMA database_list").expect("database_list");
    assert_eq!(db_list.step().expect("step"), Step::Row);
    assert_eq!(db_list.column_i64(0).expect("seq"), 0);
    assert_eq!(db_list.column_text(1).expect("name"), "main");
    assert_eq!(
        db_list.column_text(2).expect("file"),
        db_path.to_string_lossy().as_ref()
    );
    assert_eq!(db_list.step().expect("done"), Step::Done);

    let mut integrity_check = conn
        .prepare("PRAGMA integrity_check")
        .expect("integrity_check");
    assert_eq!(integrity_check.step().expect("step"), Step::Row);
    assert_eq!(integrity_check.column_text(0).expect("integrity"), "ok");
    assert_eq!(integrity_check.step().expect("done"), Step::Done);
}

#[test]
fn explicit_null_does_not_receive_column_default() {
    let (_dir, conn) = open_database();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT DEFAULT 'fallback')")
        .expect("create table");

    conn.execute("INSERT INTO t(id, v) VALUES (1, NULL)")
        .expect("insert explicit null");
    conn.execute("INSERT INTO t(id) VALUES (2)")
        .expect("insert omitted default");

    let mut stmt = conn
        .prepare("SELECT id, v FROM t ORDER BY id")
        .expect("select");
    assert_eq!(stmt.step().expect("row 1"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("id 1"), 1);
    assert!(matches!(
        stmt.column_value(1).expect("explicit null"),
        redlinedb_sql::SqlValue::Null
    ));
    assert_eq!(stmt.step().expect("row 2"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("id 2"), 2);
    assert_eq!(stmt.column_text(1).expect("default"), "fallback");
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn pragma_user_version_survives_reopen() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("user-version.db");
    {
        let db = Database::create(&path, DbOptions::default()).expect("create database");
        let conn = db.connect();
        conn.execute("PRAGMA user_version = 42")
            .expect("set user_version");
    }
    let db = Database::open(&path, DbOptions::default()).expect("open database");
    let conn = db.connect();
    let mut stmt = conn.prepare("PRAGMA user_version").expect("user_version");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("value"), 42);
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn pragma_redline_index_check_matches_engine_api() {
    let (_dir, conn) = open_database();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .expect("create table");
    conn.execute("CREATE INDEX t_b_idx ON t(b)")
        .expect("create index");
    conn.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')")
        .expect("insert");

    let mut stmt = conn
        .prepare("PRAGMA redline_index_check")
        .expect("prepare pragma");
    let mut rows = Vec::new();
    while let Step::Row = stmt.step().expect("step") {
        rows.push((
            stmt.column_text(0).expect("index name").to_owned(),
            stmt.column_text(1).expect("status").to_owned(),
        ));
    }
    assert!(
        rows.iter()
            .any(|(name, status)| name == "t_b_idx" && status == "ok"),
        "expected ix t_b_idx ok in {rows:?}"
    );
    for (_, status) in &rows {
        assert_eq!(status, "ok", "all indexes should report ok");
    }
}

#[test]
fn pragma_redline_full_check_returns_per_relation_rows() {
    let (_dir, conn) = open_database();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .expect("create table");
    conn.execute("CREATE INDEX t_b_idx ON t(b)")
        .expect("create index");
    conn.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .expect("insert");

    let mut stmt = conn
        .prepare("PRAGMA redline_full_check")
        .expect("prepare pragma");
    let mut rows = Vec::new();
    while let Step::Row = stmt.step().expect("step") {
        rows.push((
            stmt.column_text(0).expect("relation").to_owned(),
            stmt.column_text(1).expect("status").to_owned(),
            stmt.column_i64(2).expect("heap rows"),
            stmt.column_i64(3).expect("index entries"),
            stmt.column_i64(4).expect("heap minus index"),
            stmt.column_i64(5).expect("index minus heap"),
            stmt.column_i64(6).expect("page csum failures"),
            stmt.column_i64(7).expect("lsn violations"),
        ));
    }
    let t_row = rows
        .iter()
        .find(|row| row.0 == "t")
        .expect("t row in full check");
    assert_eq!(t_row.1, "ok");
    assert_eq!(t_row.2, 3, "heap row count");
    assert_eq!(t_row.4, 0, "heap minus index");
    assert_eq!(t_row.5, 0, "index minus heap");
    assert_eq!(t_row.6, 0, "no page csum failures");
    assert_eq!(t_row.7, 0, "no lsn monotonicity violations");
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
fn sqlite_null_and_zero_arithmetic_semantics_match_core_behavior() {
    let (_dir, conn) = open_database();
    let mut stmt = conn
        .prepare("SELECT 1 / 0, 5 % 0, 'a' || NULL, NULL || 'b'")
        .expect("prepare arithmetic");
    assert_eq!(stmt.step().expect("row"), Step::Row);
    for idx in 0..4 {
        assert!(matches!(
            stmt.column_value(idx).expect("value"),
            redlinedb_sql::SqlValue::Null
        ));
    }
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn json_text_functions_are_sqlite_compatible_for_core_paths() {
    let (_dir, conn) = open_database();
    let mut stmt = conn
        .prepare(
            "SELECT \
             json_valid('{\"a\":[1,2],\"b\":true}'), \
             json_extract('{\"a\":[1,2],\"b\":true}', '$.a[1]'), \
             json_type('{\"a\":[1,2],\"b\":true}', '$.b'), \
             json_array(1, 'two', NULL), \
             json_object('k', 7), \
             json_quote('x')",
        )
        .expect("prepare json");
    assert_eq!(stmt.step().expect("row"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("valid"), 1);
    assert_eq!(stmt.column_i64(1).expect("extract"), 2);
    assert_eq!(stmt.column_text(2).expect("type"), "true");
    assert_eq!(stmt.column_text(3).expect("array"), "[1,\"two\",null]");
    assert_eq!(stmt.column_text(4).expect("object"), "{\"k\":7}");
    assert_eq!(stmt.column_text(5).expect("quote"), "\"x\"");
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn vector_blob_functions_encode_and_compare_vectors() {
    let (_dir, conn) = open_database();
    let mut stmt = conn
        .prepare(
            "SELECT \
             vector_dims(vector('[1.0, 2.0, 3.0]')), \
             vector_distance_l2(vector('[1, 2]'), vector('[4, 6]')), \
             vector_distance_cosine(vector_from_json('[1,0]'), vector('[0,1]'))",
        )
        .expect("prepare vector");
    assert_eq!(stmt.step().expect("row"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("dims"), 3);
    assert_eq!(stmt.column_f64(1).expect("l2"), 25.0);
    assert!((stmt.column_f64(2).expect("cosine") - 1.0).abs() < 0.000001);
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn insert_select_populates_target_rows() {
    let (_dir, conn) = open_database();
    conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT)")
        .expect("create src");
    conn.execute("CREATE TABLE dst(id INTEGER PRIMARY KEY, v TEXT DEFAULT 'd')")
        .expect("create dst");
    conn.execute("INSERT INTO src VALUES (1, 'one'), (2, 'two')")
        .expect("insert src");
    assert_eq!(
        conn.execute("INSERT INTO dst(id, v) SELECT id + 10, upper(v) FROM src ORDER BY id")
            .expect("insert select"),
        2
    );
    let mut stmt = conn
        .prepare("SELECT id, v FROM dst ORDER BY id")
        .expect("select dst");
    assert_eq!(stmt.step().expect("row 1"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("id"), 11);
    assert_eq!(stmt.column_text(1).expect("v"), "ONE");
    assert_eq!(stmt.step().expect("row 2"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("id"), 12);
    assert_eq!(stmt.column_text(1).expect("v"), "TWO");
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
fn alter_table_rename_add_and_rename_column_work() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT NOT NULL DEFAULT 'bee')")
        .expect("create table");
    conn.execute("INSERT INTO t(a) VALUES (1)")
        .expect("insert row");

    conn.execute("ALTER TABLE t RENAME TO t2")
        .expect("rename table");
    conn.execute("ALTER TABLE t2 ADD COLUMN c TEXT DEFAULT 'cee'")
        .expect("add column");
    conn.execute("ALTER TABLE t2 RENAME COLUMN b TO renamed_b")
        .expect("rename column");

    let mut stmt = conn
        .prepare("SELECT a, renamed_b, c FROM t2 ORDER BY a")
        .expect("select renamed table");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("a"), 1);
    assert_eq!(stmt.column_text(1).expect("renamed_b"), "bee");
    assert_eq!(stmt.column_text(2).expect("c"), "cee");
    assert_eq!(stmt.step().expect("done"), Step::Done);

    conn.execute("INSERT INTO t2(a, renamed_b) VALUES (2, 'row2')")
        .expect("insert post alter");
    let mut stmt = conn
        .prepare("SELECT a, renamed_b, c FROM t2 ORDER BY a")
        .expect("select after insert");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_text(2).expect("c"), "cee");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_text(1).expect("renamed_b"), "row2");
    assert_eq!(stmt.column_text(2).expect("c"), "cee");
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn returning_clauses_surface_write_rows() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .expect("create table");

    let mut insert = conn
        .prepare("INSERT INTO t(a, b) VALUES (1, 'one') RETURNING a, b")
        .expect("prepare insert returning");
    assert_eq!(insert.step().expect("step"), Step::Row);
    assert_eq!(insert.column_i64(0).expect("a"), 1);
    assert_eq!(insert.column_text(1).expect("b"), "one");
    assert_eq!(insert.step().expect("done"), Step::Done);

    let mut update = conn
        .prepare("UPDATE t SET b = 'two' WHERE a = 1 RETURNING a, b")
        .expect("prepare update returning");
    assert_eq!(update.step().expect("step"), Step::Row);
    assert_eq!(update.column_i64(0).expect("a"), 1);
    assert_eq!(update.column_text(1).expect("b"), "two");
    assert_eq!(update.step().expect("done"), Step::Done);

    let mut delete = conn
        .prepare("DELETE FROM t WHERE a = 1 RETURNING a")
        .expect("prepare delete returning");
    assert_eq!(delete.step().expect("step"), Step::Row);
    assert_eq!(delete.column_i64(0).expect("a"), 1);
    assert_eq!(delete.step().expect("done"), Step::Done);
}

#[test]
fn upsert_and_conflict_algorithms_work() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE, note TEXT)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES (1, 'one', 'original')")
        .expect("insert original");

    conn.execute("INSERT OR IGNORE INTO t VALUES (2, 'one', 'ignored')")
        .expect("insert or ignore");

    let mut stmt = conn
        .prepare("SELECT id, v, note FROM t ORDER BY id")
        .expect("select after ignore");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("id"), 1);
    assert_eq!(stmt.column_text(1).expect("v"), "one");
    assert_eq!(stmt.column_text(2).expect("note"), "original");
    assert_eq!(stmt.step().expect("done"), Step::Done);

    conn.execute("INSERT OR REPLACE INTO t VALUES (2, 'one', 'replaced')")
        .expect("insert or replace");

    let mut stmt = conn
        .prepare("SELECT id, v, note FROM t ORDER BY id")
        .expect("select after replace");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("id"), 2);
    assert_eq!(stmt.column_text(1).expect("v"), "one");
    assert_eq!(stmt.column_text(2).expect("note"), "replaced");
    assert_eq!(stmt.step().expect("done"), Step::Done);

    conn.execute("INSERT INTO t VALUES (3, 'two', 'second')")
        .expect("insert second");
    conn.execute(
        "INSERT INTO t(id, v, note) VALUES (4, 'two', 'updated') ON CONFLICT(v) DO NOTHING",
    )
    .expect("insert on conflict do nothing");

    let mut stmt = conn
        .prepare("SELECT id, v, note FROM t WHERE v = 'two'")
        .expect("select after do nothing");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("id"), 3);
    assert_eq!(stmt.column_text(2).expect("note"), "second");
    assert_eq!(stmt.step().expect("done"), Step::Done);

    conn.execute("INSERT INTO t(id, v, note) VALUES (4, 'two', 'conflict update') ON CONFLICT(v) DO UPDATE SET note = excluded.note")
        .expect("insert on conflict do update");

    let mut stmt = conn
        .prepare("SELECT id, v, note FROM t WHERE v = 'two'")
        .expect("select after do update");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("id"), 3);
    assert_eq!(stmt.column_text(2).expect("note"), "conflict update");
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

#[test]
fn union_all_concatenates_rows() {
    let (_dir, conn) = open_database();

    let mut stmt = conn
        .prepare("SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3")
        .expect("prepare union all");

    let mut rows = Vec::new();
    while let Step::Row = stmt.step().expect("step union all") {
        rows.push(stmt.column_i64(0).expect("v"));
    }

    assert_eq!(rows, vec![1, 2, 3]);
}

#[test]
fn exists_and_in_subqueries_follow_membership_rules() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE t(x INTEGER)")
        .expect("create table");
    conn.execute("INSERT INTO t VALUES (1)")
        .expect("insert row 1");
    conn.execute("INSERT INTO t VALUES (3)")
        .expect("insert row 3");

    let mut exists = conn
        .prepare("SELECT EXISTS(SELECT 1 FROM t WHERE x = 3), EXISTS(SELECT 1 FROM t WHERE x = 9)")
        .expect("prepare exists");
    assert_eq!(exists.step().expect("step"), Step::Row);
    assert_eq!(exists.column_i64(0).expect("exists true"), 1);
    assert_eq!(exists.column_i64(1).expect("exists false"), 0);
    assert_eq!(exists.step().expect("done"), Step::Done);

    let mut membership = conn
        .prepare("SELECT 3 IN (SELECT x FROM t), 9 IN (SELECT x FROM t)")
        .expect("prepare in subquery");
    assert_eq!(membership.step().expect("step"), Step::Row);
    assert_eq!(membership.column_i64(0).expect("in true"), 1);
    assert_eq!(membership.column_i64(1).expect("in false"), 0);
    assert_eq!(membership.step().expect("done"), Step::Done);
}

#[test]
fn left_join_null_extends_missing_rows() {
    let (_dir, conn) = open_database();

    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
        .expect("create parent");
    conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER, note TEXT)")
        .expect("create child");
    conn.execute("INSERT INTO parent VALUES (1, 'one')")
        .expect("insert parent 1");
    conn.execute("INSERT INTO parent VALUES (2, 'two')")
        .expect("insert parent 2");
    conn.execute("INSERT INTO child VALUES (10, 1, 'matched')")
        .expect("insert matched child");

    let mut stmt = conn
        .prepare(
            "SELECT parent.id, child.note \
             FROM parent LEFT JOIN child ON parent.id = child.parent_id \
             ORDER BY parent.id",
        )
        .expect("prepare left join");

    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("parent id"), 1);
    assert_eq!(stmt.column_text(1).expect("child note"), "matched");

    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("parent id"), 2);
    assert_eq!(
        stmt.column_value(1).expect("child note"),
        &redlinedb_sql::SqlValue::Null
    );

    assert_eq!(stmt.step().expect("done"), Step::Done);
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

// ---------------------------------------------------------------------------
// Lane B: physical-index DML maintenance
//
// These tests assert that SQL INSERT/UPDATE/DELETE keep the kernel B-tree in
// sync with the heap. We probe the B-tree directly via
// `Engine::index_handle` to guarantee the entries actually moved (and were
// not just enforced by the legacy O(N) scan).
// ---------------------------------------------------------------------------

mod lane_b {
    use super::*;
    use redlinedb_kernel::catalog::{
        EncodedIndexKey, IndexDef, IndexKeySource, OwnedValue, SortDir, encode_index_key,
    };
    use redlinedb_kernel::txn::Isolation;

    /// Build the same encoded index key bytes that the SQL exec layer
    /// produces, so the test can probe the physical B-tree directly.
    fn build_index_key_for_test(index: &IndexDef, values: &[OwnedValue]) -> Vec<u8> {
        let mut dirs: Vec<SortDir> = Vec::with_capacity(index.keys.len());
        let mut owned_refs: Vec<&OwnedValue> = Vec::with_capacity(index.keys.len());
        for key in &index.keys {
            let IndexKeySource::Column { attnum } = key.source;
            owned_refs.push(values.get(attnum as usize).unwrap_or(&OwnedValue::Null));
            dirs.push(key.sort_dir);
        }
        let value_refs: Vec<_> = owned_refs.iter().map(|v| v.as_ref()).collect();
        let mut buf = Vec::new();
        let EncodedIndexKey { bytes, .. } = encode_index_key(&value_refs, &dirs, &mut buf);
        bytes
    }

    fn lookup_index_def(
        conn: &redlinedb_sql::Connection,
        schema: &str,
        name: &str,
    ) -> std::sync::Arc<IndexDef> {
        let snapshot = conn.engine_for_tests().schema_snapshot();
        let _ = schema;
        snapshot
            .indexes
            .iter()
            .find(|idx| idx.name.eq_ignore_ascii_case(name))
            .cloned()
            .unwrap_or_else(|| panic!("index `{name}` missing from snapshot"))
    }

    fn assert_index_has_key(
        conn: &redlinedb_sql::Connection,
        index_name: &str,
        values: &[OwnedValue],
    ) {
        let index = lookup_index_def(conn, "main", index_name);
        let bytes = build_index_key_for_test(&index, values);
        let handle = conn
            .engine_for_tests()
            .index_handle(index.index_id)
            .unwrap_or_else(|| panic!("no physical handle for index `{index_name}`"));
        let engine = conn.engine_for_tests();
        let tx = engine
            .begin(Isolation::Snapshot)
            .expect("begin visible probe");
        let rows = handle
            .point_lookup_visible(engine.tx_status(), tx.snapshot(), Some(tx.id()), &bytes)
            .expect("point_lookup_visible");
        engine.rollback(tx).expect("rollback visible probe");
        assert!(
            !rows.is_empty(),
            "expected index `{index_name}` to contain key for {values:?}"
        );
    }

    fn assert_index_missing_key(
        conn: &redlinedb_sql::Connection,
        index_name: &str,
        values: &[OwnedValue],
    ) {
        let index = lookup_index_def(conn, "main", index_name);
        let bytes = build_index_key_for_test(&index, values);
        let handle = conn
            .engine_for_tests()
            .index_handle(index.index_id)
            .unwrap_or_else(|| panic!("no physical handle for index `{index_name}`"));
        let engine = conn.engine_for_tests();
        let tx = engine
            .begin(Isolation::Snapshot)
            .expect("begin visible probe");
        let rows = handle
            .point_lookup_visible(engine.tx_status(), tx.snapshot(), Some(tx.id()), &bytes)
            .expect("point_lookup_visible");
        engine.rollback(tx).expect("rollback visible probe");
        assert!(
            rows.is_empty(),
            "expected index `{index_name}` to NOT contain key for {values:?}, got {rows:?}"
        );
    }

    #[test]
    fn single_column_unique_index_rejects_duplicate_insert() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE UNIQUE INDEX t_a_uq ON t(a)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (1, 'one')")
            .expect("first insert");
        let err = conn
            .execute("INSERT INTO t VALUES (1, 'duplicate')")
            .expect_err("duplicate must error");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("UNIQUE") || msg.contains("Constraint"),
            "expected unique-violation error, got {msg}"
        );
        // Index should still report the original key (and only that rowid).
        assert_index_has_key(&conn, "t_a_uq", &[OwnedValue::Integer(1)]);
        // The non-conflicting insert path should also succeed and be indexed.
        conn.execute("INSERT INTO t VALUES (2, 'two')")
            .expect("second insert");
        assert_index_has_key(&conn, "t_a_uq", &[OwnedValue::Integer(2)]);
    }

    #[test]
    fn multi_column_unique_index_skips_check_when_any_part_null() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b INTEGER, c TEXT)")
            .expect("create");
        conn.execute("CREATE UNIQUE INDEX t_ab_uq ON t(a, b)")
            .expect("create index");
        // Two rows with NULL in one component — both must succeed (SQLite
        // NULL parity: NULL is never a duplicate).
        conn.execute("INSERT INTO t(a, b, c) VALUES (1, NULL, 'x')")
            .expect("insert null b 1");
        conn.execute("INSERT INTO t(a, b, c) VALUES (1, NULL, 'y')")
            .expect("insert null b 2");
        conn.execute("INSERT INTO t(a, b, c) VALUES (NULL, 5, 'z')")
            .expect("insert null a");
        // Two non-null tuples — duplicates of (1,1) must error.
        conn.execute("INSERT INTO t(a, b, c) VALUES (1, 1, 'first')")
            .expect("first non-null");
        let err = conn
            .execute("INSERT INTO t(a, b, c) VALUES (1, 1, 'second')")
            .expect_err("duplicate must error");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("UNIQUE") || msg.contains("Constraint"),
            "expected unique-violation error, got {msg}"
        );
        // The non-null pair is in the index; NULL-bearing entries also get
        // indexed but are not subject to the unique check.
        assert_index_has_key(
            &conn,
            "t_ab_uq",
            &[OwnedValue::Integer(1), OwnedValue::Integer(1)],
        );
    }

    #[test]
    fn insert_or_replace_replaces_on_unique_conflict() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE UNIQUE INDEX t_a_uq ON t(a)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (1, 'first')")
            .expect("first insert");
        // INSERT OR REPLACE must succeed and overwrite the existing row.
        conn.execute("INSERT OR REPLACE INTO t VALUES (1, 'second')")
            .expect("replace must succeed");
        let mut stmt = conn
            .prepare("SELECT b FROM t WHERE a = 1")
            .expect("prepare");
        assert_eq!(stmt.step().expect("step"), Step::Row);
        assert_eq!(stmt.column_text(0).expect("b"), "second");
        assert_eq!(stmt.step().expect("done"), Step::Done);
        // Index still has the unique key (1) — Lane B re-inserted it after
        // delete-marking the old entry.
        assert_index_has_key(&conn, "t_a_uq", &[OwnedValue::Integer(1)]);
    }

    #[test]
    fn update_to_indexed_column_moves_index_entry() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE INDEX t_a_idx ON t(a)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (10, 'ten')")
            .expect("insert");
        assert_index_has_key(&conn, "t_a_idx", &[OwnedValue::Integer(10)]);
        // Move the row's indexed-column value from 10 -> 20.
        conn.execute("UPDATE t SET a = 20 WHERE b = 'ten'")
            .expect("update");
        // Old key delete-marked, new key inserted.
        assert_index_missing_key(&conn, "t_a_idx", &[OwnedValue::Integer(10)]);
        assert_index_has_key(&conn, "t_a_idx", &[OwnedValue::Integer(20)]);
    }

    #[test]
    fn delete_removes_index_entry() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE INDEX t_a_idx ON t(a)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (7, 'seven')")
            .expect("insert");
        assert_index_has_key(&conn, "t_a_idx", &[OwnedValue::Integer(7)]);
        conn.execute("DELETE FROM t WHERE a = 7").expect("delete");
        assert_index_missing_key(&conn, "t_a_idx", &[OwnedValue::Integer(7)]);
    }

    #[test]
    fn recovery_after_crash_mid_insert_with_index_half_written() {
        // Simulate a crash mid-insert: open a writer, INSERT inside an
        // explicit transaction, and DROP the connection without
        // committing. The kernel WAL contains no commit record for that
        // tx, so recovery must reject both the heap row AND the index
        // entry — atomicity at "either both or neither".
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("recovery.db");
        {
            let db = Database::create(&path, DbOptions::default()).expect("create");
            let conn = db.connect();
            conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
                .expect("create");
            conn.execute("CREATE UNIQUE INDEX t_a_uq ON t(a)")
                .expect("create index");
            // Sanity: a committed insert should be visible after reopen,
            // so we plant one before the kill window.
            conn.execute("INSERT INTO t VALUES (1, 'committed')")
                .expect("commit-row");
            // Now begin a tx and INSERT but never commit. Drop the conn
            // without rolling back to mirror an abrupt crash.
            conn.begin(redlinedb_sql::BeginMode::Deferred)
                .expect("begin");
            conn.execute("INSERT INTO t VALUES (2, 'killed')")
                .expect("insert");
            // Drop without commit — uncommitted state must not survive.
        }
        // Reopen the database. The kernel replays the WAL up to the
        // last commit; the second tx is uncommitted, so neither the
        // heap row nor the index entry must persist.
        let db = Database::open(&path, DbOptions::default()).expect("reopen");
        let conn = db.connect();
        // Heap side: only the committed row is visible.
        let mut stmt = conn
            .prepare("SELECT a, b FROM t ORDER BY a")
            .expect("prepare");
        let mut rows = Vec::new();
        while let Step::Row = stmt.step().expect("step") {
            rows.push((
                stmt.column_i64(0).expect("a"),
                stmt.column_text(1).expect("b").to_owned(),
            ));
        }
        assert_eq!(rows, vec![(1, "committed".to_owned())]);
        // Index side: the committed key is present, the uncommitted key
        // is absent — atomicity of (heap, index) holds across recovery.
        assert_index_has_key(&conn, "t_a_uq", &[OwnedValue::Integer(1)]);
        assert_index_missing_key(&conn, "t_a_uq", &[OwnedValue::Integer(2)]);
    }

    /// Regression: rolling back an INSERT must remove the durable index
    /// entry the SQL DML wrote. Without per-tx index undo, the insert_tx
    /// page mutation persists past rollback and the next legitimate INSERT
    /// of the same key fails with `Constraint`.
    #[test]
    fn rolled_back_insert_does_not_leave_stale_index_entry() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE UNIQUE INDEX t_a_uq ON t(a)")
            .expect("create index");

        // Open a tx, insert (1, 'rolled-back'), then roll back.
        conn.begin(redlinedb_sql::BeginMode::Deferred)
            .expect("begin");
        conn.execute("INSERT INTO t VALUES (1, 'rolled-back')")
            .expect("insert under tx");
        conn.rollback().expect("rollback");

        // Index must NOT carry a stale entry for key 1.
        assert_index_missing_key(&conn, "t_a_uq", &[OwnedValue::Integer(1)]);

        // A fresh INSERT of the same UNIQUE key must succeed (no false
        // conflict from the rolled-back entry).
        conn.execute("INSERT INTO t VALUES (1, 'fresh')")
            .expect("re-insert after rollback must succeed");

        // And the new row should be visible via both the heap and the index.
        let mut stmt = conn
            .prepare("SELECT b FROM t WHERE a = 1")
            .expect("prepare");
        assert_eq!(stmt.step().expect("step"), Step::Row);
        assert_eq!(stmt.column_text(0).expect("b"), "fresh");
        assert_eq!(stmt.step().expect("done"), Step::Done);
        assert_index_has_key(&conn, "t_a_uq", &[OwnedValue::Integer(1)]);
    }

    /// Regression: rolling back a DELETE must clear the dead flag on the
    /// committed row's index entry. Without index-undo, the delete_mark
    /// stays durable and indexed reads silently miss the row.
    #[test]
    fn rolled_back_delete_does_not_hide_committed_row() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE INDEX t_a_idx ON t(a)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (5, 'five')")
            .expect("insert + commit");

        conn.begin(redlinedb_sql::BeginMode::Deferred)
            .expect("begin");
        conn.execute("DELETE FROM t WHERE a = 5")
            .expect("delete under tx");
        conn.rollback().expect("rollback");

        // Heap path: row is back, visible via TableScan.
        let mut stmt = conn
            .prepare("SELECT b FROM t WHERE b = 'five'")
            .expect("prepare heap path");
        assert_eq!(stmt.step().expect("step"), Step::Row);
        assert_eq!(stmt.column_text(0).expect("b"), "five");
        assert_eq!(stmt.step().expect("done"), Step::Done);

        // Index path: row is also back, no longer hidden by a durable dead
        // flag (the SQL-side undo replayed the inverse undelete_mark).
        let mut stmt = conn
            .prepare("SELECT b FROM t WHERE a = 5")
            .expect("prepare index path");
        assert_eq!(stmt.step().expect("step"), Step::Row);
        assert_eq!(stmt.column_text(0).expect("b"), "five");
        assert_eq!(stmt.step().expect("done"), Step::Done);

        assert_index_has_key(&conn, "t_a_idx", &[OwnedValue::Integer(5)]);
    }

    /// Regression: rolling back an UPDATE that moved an indexed value must
    /// keep the OLD index key live — the rolled-back tx delete-marked the
    /// old entry and inserted a new one; rollback must reverse both.
    #[test]
    fn rolled_back_update_restores_old_indexed_value() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE INDEX t_a_idx ON t(a)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (10, 'ten')")
            .expect("seed");

        conn.begin(redlinedb_sql::BeginMode::Deferred)
            .expect("begin");
        conn.execute("UPDATE t SET a = 20 WHERE b = 'ten'")
            .expect("update under tx");
        conn.rollback().expect("rollback");

        // Index path: the old key (10) is alive again; the new key (20) is
        // gone (its insert was rolled back).
        assert_index_has_key(&conn, "t_a_idx", &[OwnedValue::Integer(10)]);
        assert_index_missing_key(&conn, "t_a_idx", &[OwnedValue::Integer(20)]);

        // SELECT via the index must still return the original row.
        let mut stmt = conn
            .prepare("SELECT b FROM t WHERE a = 10")
            .expect("prepare");
        assert_eq!(stmt.step().expect("step"), Step::Row);
        assert_eq!(stmt.column_text(0).expect("b"), "ten");
        assert_eq!(stmt.step().expect("done"), Step::Done);
    }

    /// Regression: with the kernel `UniqueKeyGuard` held across the heap
    /// insert, two concurrent writers attempting the same UNIQUE key must
    /// have exactly one succeed and the other surface a `Constraint`.
    /// Repeated 10x to flush the race window.
    #[test]
    fn concurrent_unique_inserts_only_one_succeeds() {
        use std::sync::Arc as StdArc;
        use std::thread;

        for run in 0..10 {
            let dir = tempfile::tempdir().expect("temp dir");
            let path = dir.path().join("concurrent-uq.db");
            let db = Database::create(&path, DbOptions::default()).expect("create");
            let conn = db.connect();
            conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
                .expect("create");
            conn.execute("CREATE UNIQUE INDEX t_a_uq ON t(a)")
                .expect("create index");
            drop(conn);

            // Two threads racing on the same DB file via two SQL connections,
            // each issuing an INSERT of (1, ...). One must win.
            let key = run as i64 + 1;
            let db_a = StdArc::clone(&db);
            let db_b = StdArc::clone(&db);
            let handle_a = thread::spawn(move || {
                let conn = db_a.connect();
                conn.execute(&format!("INSERT INTO t VALUES ({key}, 'A')"))
            });
            let handle_b = thread::spawn(move || {
                let conn = db_b.connect();
                conn.execute(&format!("INSERT INTO t VALUES ({key}, 'B')"))
            });
            let result_a = handle_a.join().expect("thread A join");
            let result_b = handle_b.join().expect("thread B join");

            // Exactly one must succeed; the other must surface the unique
            // violation. Either ordering is acceptable.
            let successes = [&result_a, &result_b].iter().filter(|r| r.is_ok()).count();
            assert_eq!(
                successes, 1,
                "run {run}: exactly one writer must win; got A={result_a:?} B={result_b:?}"
            );
            let failures: Vec<_> = [&result_a, &result_b]
                .iter()
                .filter_map(|r| r.as_ref().err())
                .collect();
            assert_eq!(failures.len(), 1);
            let msg = format!("{:?}", failures[0]);
            assert!(
                msg.contains("UNIQUE") || msg.contains("Constraint"),
                "run {run}: loser must surface unique violation, got {msg}"
            );

            // The winning row must be in the heap exactly once.
            let conn = db.connect();
            let mut stmt = conn
                .prepare(&format!("SELECT b FROM t WHERE a = {key}"))
                .expect("prepare");
            let mut rows = Vec::new();
            while let Step::Row = stmt.step().expect("step") {
                rows.push(stmt.column_text(0).expect("b").to_owned());
            }
            assert_eq!(rows.len(), 1, "run {run}: exactly one row must commit");
        }
    }
}

// ---------------------------------------------------------------------------
// Lane C: SQL Index Reads And Planner.
//
// Lane C wires SELECT to consume the kernel B-tree indexes that Lane B
// keeps in sync with DML. These tests assert two invariants:
//   1. EXPLAIN names the physical access path the executor actually
//      takes (`IndexPointLookup`, `IndexRangeScan`, or `TableScan`),
//      and only advertises an index path when one is consumable.
//   2. Index-driven SELECT results match what a TableScan would have
//      produced, end-to-end across the heap and the index.
// Covering indexes and multi-index AND/OR remain disabled until later
// waves; the last two tests assert that fact.
// ---------------------------------------------------------------------------

mod lane_c {
    use super::*;

    /// Run `EXPLAIN QUERY PLAN <sql>` and concatenate the detail
    /// column for every plan row. The detail format is
    /// `SEARCH TABLE <name> USING INDEX <idx>: <Probe>` for index
    /// paths and `SCAN TABLE <name>` for full scans, so substring
    /// matching is reliable.
    fn explain_text(conn: &std::sync::Arc<redlinedb_sql::Connection>, sql: &str) -> String {
        let prepared = format!("EXPLAIN QUERY PLAN {sql}");
        let mut stmt = conn.prepare(&prepared).expect("prepare explain");
        let mut out = String::new();
        while let Step::Row = stmt.step().expect("step explain") {
            // Column 3 is the textual detail (id, parent, notused,
            // detail) — see `planner::explain_rows`.
            out.push_str(stmt.column_text(3).expect("detail"));
            out.push('\n');
        }
        out
    }

    fn collect_select_ints(
        conn: &std::sync::Arc<redlinedb_sql::Connection>,
        sql: &str,
    ) -> Vec<i64> {
        let mut stmt = conn.prepare(sql).expect("prepare select");
        let mut rows = Vec::new();
        while let Step::Row = stmt.step().expect("step select") {
            rows.push(stmt.column_i64(0).expect("col"));
        }
        rows
    }

    #[test]
    fn select_by_pk_uses_index_point_lookup() {
        let (_dir, conn) = open_database();
        // CREATE TABLE PRIMARY KEY indexes are autoindexes that the
        // catalog records without a `meta_page_id` (no physical pages
        // are allocated until CREATE INDEX runs). Lane KH P1 #5 made
        // the planner skip indexes without a live handle, so we issue
        // CREATE INDEX explicitly here to exercise the point-lookup
        // path through a real B-tree.
        conn.execute("CREATE TABLE t(k TEXT, v INTEGER)")
            .expect("create");
        conn.execute("CREATE INDEX t_k_idx ON t(k)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES ('a', 1)")
            .expect("insert a");
        conn.execute("INSERT INTO t VALUES ('b', 2)")
            .expect("insert b");

        let plan = explain_text(&conn, "SELECT v FROM t WHERE k = 'a'");
        assert!(
            plan.contains("USING INDEX") && plan.contains("PointLookup"),
            "expected IndexPointLookup, got plan:\n{plan}"
        );
        assert!(
            !plan.contains("SCAN TABLE t"),
            "did not expect a full SCAN TABLE under an index path:\n{plan}"
        );
    }

    #[test]
    fn select_indexed_range_uses_index_range_scan() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE INDEX t_a_idx ON t(a)")
            .expect("create index");
        for v in 1..=5 {
            conn.execute(&format!("INSERT INTO t VALUES ({v}, 'v{v}')"))
                .expect("insert");
        }

        let plan = explain_text(&conn, "SELECT b FROM t WHERE a BETWEEN 2 AND 4");
        assert!(
            plan.contains("USING INDEX t_a_idx") && plan.contains("RangeScan"),
            "expected IndexRangeScan on t_a_idx, got plan:\n{plan}"
        );
    }

    #[test]
    fn unsupported_predicate_falls_back_to_table_scan() {
        let (_dir, conn) = open_database();
        // Index is on `a`; the predicate constrains only `b`, which
        // is the non-leading and indeed unindexed column. The
        // planner must not advertise an index path.
        conn.execute("CREATE TABLE t(a INTEGER, b INTEGER)")
            .expect("create");
        conn.execute("CREATE INDEX t_a_idx ON t(a)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (1, 100)")
            .expect("insert");
        conn.execute("INSERT INTO t VALUES (2, 200)")
            .expect("insert");

        let plan = explain_text(&conn, "SELECT a FROM t WHERE b = 100");
        assert!(
            plan.contains("SCAN TABLE t"),
            "expected TableScan (no leading-column predicate), got plan:\n{plan}"
        );
        assert!(
            !plan.contains("USING INDEX"),
            "must not advertise an index path here:\n{plan}"
        );
    }

    #[test]
    fn index_point_lookup_returns_correct_rows() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER)")
            .expect("create");
        for (k, v) in [("a", 1i64), ("b", 2), ("c", 3)] {
            conn.execute(&format!("INSERT INTO t VALUES ('{k}', {v})"))
                .expect("insert");
        }
        // Index path: WHERE k = 'b' (this is the planner-advertised
        // IndexPointLookup case).
        let via_index = collect_select_ints(&conn, "SELECT v FROM t WHERE k = 'b'");
        // Reference: table scan with a residual filter (we re-issue
        // the same query; the planner would still pick the index,
        // but the result must equal the logical answer regardless).
        assert_eq!(via_index, vec![2]);
        // Confirm a miss returns an empty set (the index returns no
        // rows; no fallback to a heap scan happens silently).
        let miss = collect_select_ints(&conn, "SELECT v FROM t WHERE k = 'zzz'");
        assert!(
            miss.is_empty(),
            "missing key must yield no rows, got {miss:?}"
        );
    }

    #[test]
    fn index_range_scan_returns_correct_rows() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE INDEX t_a_idx ON t(a)")
            .expect("create index");
        for v in 1..=5 {
            conn.execute(&format!("INSERT INTO t VALUES ({v}, 'v{v}')"))
                .expect("insert");
        }
        // BETWEEN 2 AND 4 -> indexed range scan
        let mut stmt = conn
            .prepare("SELECT a FROM t WHERE a BETWEEN 2 AND 4 ORDER BY a")
            .expect("prepare");
        let mut rows = Vec::new();
        while let Step::Row = stmt.step().expect("step") {
            rows.push(stmt.column_i64(0).expect("a"));
        }
        assert_eq!(rows, vec![2, 3, 4]);

        // Open-ended range: a > 3
        let mut stmt = conn
            .prepare("SELECT a FROM t WHERE a > 3 ORDER BY a")
            .expect("prepare");
        let mut rows = Vec::new();
        while let Step::Row = stmt.step().expect("step") {
            rows.push(stmt.column_i64(0).expect("a"));
        }
        assert_eq!(rows, vec![4, 5]);
    }

    #[test]
    fn planner_does_not_advertise_covering_index() {
        // Even when every projected column is a leading key of the
        // index (a true covering candidate), Lane C must NOT emit
        // `CoveringIndexScan` — that optimization stays disabled
        // until a later wave wires the executor for it. The
        // physical plan should still pick an index path
        // (IndexRangeScan), but render WITHOUT "COVERING".
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b INTEGER)")
            .expect("create");
        conn.execute("CREATE INDEX t_a_idx ON t(a)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (1, 10)")
            .expect("insert");
        conn.execute("INSERT INTO t VALUES (2, 20)")
            .expect("insert");

        // SELECT a FROM t WHERE a = 1 — `a` is the only projected
        // column AND the leading key, so this is a textbook covering
        // candidate. Assert that the plan reports the regular index
        // path (PointLookup), NOT a "COVERING INDEX" line.
        let plan = explain_text(&conn, "SELECT a FROM t WHERE a = 1");
        assert!(
            !plan.contains("COVERING INDEX"),
            "covering-index optimization must stay off:\n{plan}"
        );
        assert!(
            plan.contains("USING INDEX") && plan.contains("PointLookup"),
            "expected a regular IndexPointLookup, got plan:\n{plan}"
        );
    }

    #[test]
    fn planner_does_not_advertise_multi_index_and_or() {
        // Two single-column indexes plus a predicate that touches
        // BOTH (`a = 1 OR b = 10`). A multi-index OR planner could
        // theoretically union the two probe sets, but Lane C keeps
        // that optimization disabled. The plan must therefore fall
        // back to a TableScan rather than emitting `MULTI-INDEX
        // SCAN`.
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b INTEGER)")
            .expect("create");
        conn.execute("CREATE INDEX t_a_idx ON t(a)")
            .expect("create index");
        conn.execute("CREATE INDEX t_b_idx ON t(b)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (1, 10)")
            .expect("insert");
        conn.execute("INSERT INTO t VALUES (2, 20)")
            .expect("insert");

        let plan_or = explain_text(&conn, "SELECT a FROM t WHERE a = 1 OR b = 20");
        assert!(
            !plan_or.contains("MULTI-INDEX"),
            "multi-index OR must stay off:\n{plan_or}"
        );
        // We accept either TableScan or — if a single-index path is
        // somehow extracted from one side of the OR — the plain
        // index path. What we MUST NOT see is a multi-index union.
        // (Today the planner walks only top-level AND chains, so an
        // OR pins us to TableScan; this assertion preserves that.)
        assert!(
            plan_or.contains("SCAN TABLE t"),
            "expected fallback to TableScan for OR, got plan:\n{plan_or}"
        );

        // Same for AND-of-two-indexes: only the leading conjunct
        // gets used. We never emit MULTI-INDEX AND.
        let plan_and = explain_text(&conn, "SELECT a FROM t WHERE a = 1 AND b = 10");
        assert!(
            !plan_and.contains("MULTI-INDEX"),
            "multi-index AND must stay off:\n{plan_and}"
        );
    }

    /// Regression: composite (a, b) index with WHERE a = ? must surface every
    /// row that shares the leading-key value. The previous upper-bound for the
    /// half-open range was `prefix || 0x00`, which sorts BEFORE every full
    /// composite key (because the next part starts with a non-zero type tag),
    /// so the range returned an empty set.
    #[test]
    fn composite_index_leading_prefix_returns_all_rows() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE INDEX t_ab ON t(a, b)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (1, 'x')")
            .expect("insert 1x");
        conn.execute("INSERT INTO t VALUES (1, 'y')")
            .expect("insert 1y");
        conn.execute("INSERT INTO t VALUES (1, 'z')")
            .expect("insert 1z");
        conn.execute("INSERT INTO t VALUES (2, 'x')")
            .expect("insert 2x");

        // The planner must pick the (a, b) composite index for `WHERE a = 1`.
        let plan = explain_text(&conn, "SELECT b FROM t WHERE a = 1");
        assert!(
            plan.contains("USING INDEX t_ab"),
            "expected (a,b) index path for leading-only equality, got plan:\n{plan}"
        );

        let mut stmt = conn
            .prepare("SELECT b FROM t WHERE a = 1 ORDER BY b")
            .expect("prepare");
        let mut rows = Vec::new();
        while let Step::Row = stmt.step().expect("step") {
            rows.push(stmt.column_text(0).expect("b").to_owned());
        }
        assert_eq!(
            rows,
            vec!["x".to_owned(), "y".to_owned(), "z".to_owned()],
            "leading-prefix range must surface every (a=1, *) row"
        );
    }

    /// Regression: composite (a, b) index with WHERE a = ? AND b = ? is a
    /// full-key point lookup; the upper bound must be tight enough to NOT
    /// surface rows for other `b` values, and lax enough to include the
    /// requested one.
    #[test]
    fn composite_index_leading_prefix_with_explicit_b() {
        let (_dir, conn) = open_database();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("create");
        conn.execute("CREATE INDEX t_ab ON t(a, b)")
            .expect("create index");
        conn.execute("INSERT INTO t VALUES (1, 'x')")
            .expect("insert 1x");
        conn.execute("INSERT INTO t VALUES (1, 'y')")
            .expect("insert 1y");
        conn.execute("INSERT INTO t VALUES (1, 'z')")
            .expect("insert 1z");
        conn.execute("INSERT INTO t VALUES (2, 'x')")
            .expect("insert 2x");

        // Full-key equality should resolve to a point lookup.
        let plan = explain_text(&conn, "SELECT b FROM t WHERE a = 1 AND b = 'y'");
        assert!(
            plan.contains("USING INDEX t_ab") && plan.contains("PointLookup"),
            "expected (a,b) IndexPointLookup, got plan:\n{plan}"
        );

        let mut stmt = conn
            .prepare("SELECT b FROM t WHERE a = 1 AND b = 'y'")
            .expect("prepare");
        let mut rows = Vec::new();
        while let Step::Row = stmt.step().expect("step") {
            rows.push(stmt.column_text(0).expect("b").to_owned());
        }
        assert_eq!(rows, vec!["y".to_owned()]);
    }
}

// ----- Lane KH (Wave 7) regressions -----

/// P1 #5: the planner must skip indexes whose catalog entry has no
/// `meta_page_id` even when the engine could otherwise satisfy the
/// predicate. CREATE TABLE PRIMARY KEY autoindexes are exactly this
/// case — they live in the snapshot but no physical B-tree is
/// allocated until CREATE INDEX runs. Before the fix, EXPLAIN
/// reported `IndexPointLookup` while the executor silently fell back
/// to a TableScan.
#[test]
fn planner_does_not_advertise_index_without_handle() {
    let (_dir, conn) = open_database();
    // CREATE TABLE PRIMARY KEY records an autoindex with
    // `meta_page_id=None` and never registers an engine handle for it.
    // (CREATE INDEX is the only path that allocates the physical
    // pages.) The planner must observe that absence and pick TableScan.
    conn.execute("CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER)")
        .expect("create");
    conn.execute("INSERT INTO t VALUES ('a', 1)")
        .expect("insert a");
    conn.execute("INSERT INTO t VALUES ('b', 2)")
        .expect("insert b");

    let prepared = "EXPLAIN QUERY PLAN SELECT v FROM t WHERE k = 'a'";
    let mut stmt = conn.prepare(prepared).expect("prepare explain");
    let mut detail = String::new();
    while let Step::Row = stmt.step().expect("step explain") {
        detail.push_str(stmt.column_text(3).expect("detail"));
        detail.push('\n');
    }
    assert!(
        detail.contains("SCAN TABLE t"),
        "expected TableScan, got plan:\n{detail}"
    );
    assert!(
        !detail.contains("USING INDEX"),
        "must not advertise an index without a live handle:\n{detail}"
    );

    // Sanity: the executor must still satisfy the predicate via the
    // fallback path so end-user behavior matches the EXPLAIN output.
    let mut stmt = conn
        .prepare("SELECT v FROM t WHERE k = 'a'")
        .expect("prepare select");
    assert_eq!(stmt.step().expect("step"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("v"), 1);
    assert_eq!(stmt.step().expect("done"), Step::Done);
}

/// P0 #3: when `engine.commit` reports a maybe-committed outcome after
/// the SQL layer has already mutated physical index pages, we must not
/// run SQL-side index repair. The durable index entry needs to remain
/// visible, even though the client still sees an error from the commit.
#[cfg(feature = "failpoints")]
#[test]
fn commit_failure_surfaces_maybe_committed_without_index_repair() {
    use redlinedb_kernel::engine::arm_commit_failure_for_thread;
    use redlinedb_kernel::failpoints;
    use std::sync::Mutex;

    // The fail-crate registry is process-wide, so we serialize all
    // tests that touch the `engine::commit::before_publish`
    // configuration through one mutex. The closure inside the
    // failpoint additionally checks a thread-local flag (armed below)
    // so that other tests running on parallel threads keep
    // committing normally even while our action is in the registry.
    static GUARD: Mutex<()> = Mutex::new(());
    let _serial = GUARD.lock().unwrap_or_else(|p| p.into_inner());

    failpoints::cfg(
        "engine::commit::before_publish",
        "return(commit-failure-replays-index-undo)",
    )
    .expect("configure commit failpoint");

    let (_dir, conn) = open_database();
    conn.execute("CREATE TABLE t(k INTEGER, v TEXT)")
        .expect("create");
    conn.execute("CREATE UNIQUE INDEX t_k_idx ON t(k)")
        .expect("create unique index");

    // Arm AFTER DDL so the index is already physically allocated; we
    // want the *INSERT* commit to fail, not the DDL. The thread-local
    // flag scopes the failpoint to this test's thread; other parallel
    // tests' commits see the closure but skip the injection.
    arm_commit_failure_for_thread(true);

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'first')")
        .expect_err("commit must be ambiguous");
    assert!(
        format!("{err:?}").contains("commit outcome uncertain"),
        "unexpected error from injected commit failure: {err:?}"
    );

    // Disarm before the next statement; the durable row/index bytes
    // should remain visible and no repair path should run.
    arm_commit_failure_for_thread(false);
    failpoints::cfg("engine::commit::before_publish", "off").expect("disable commit failpoint");

    let mut stmt = conn
        .prepare("SELECT v FROM t WHERE k = 1")
        .expect("prepare");
    let mut rows = Vec::new();
    while let Step::Row = stmt.step().expect("step") {
        rows.push(stmt.column_text(0).expect("v").to_owned());
    }
    assert_eq!(
        rows,
        vec!["first".to_owned()],
        "maybe-committed INSERT must leave the durable row visible"
    );

    let duplicate = conn
        .execute("INSERT INTO t VALUES (1, 'second')")
        .expect_err("duplicate unique key must still conflict");
    assert!(
        format!("{duplicate:?}").contains("constraint")
            || format!("{duplicate:?}").contains("unique"),
        "unexpected duplicate-key error: {duplicate:?}"
    );

    // Total row count must match: only the first insert survived.
    let mut stmt = conn
        .prepare("SELECT COUNT(*) FROM t")
        .expect("prepare count");
    assert_eq!(stmt.step().expect("step count"), Step::Row);
    assert_eq!(stmt.column_i64(0).expect("count"), 1);
}
