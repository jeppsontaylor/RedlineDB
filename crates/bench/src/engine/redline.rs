use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::config::RunSpec;
use crate::engine::{BenchConn, BenchEngine, CellValue, EngineSnapshot, apply_durability};
use anyhow::{Result, anyhow};

pub struct RedlineEngine {
    path: PathBuf,
    db: Arc<Mutex<redlinedb::Database>>,
}

impl RedlineEngine {
    pub fn open(spec: &RunSpec, db_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(db_dir)?;
        let path = db_dir.join("bench.redline");
        let mut options = redlinedb::OpenOptions::default();
        options.memory.cache_bytes = spec.cache_bytes;
        apply_durability(&mut options, spec.durability);
        let db = redlinedb::Database::open_with_options(&path, options)?;
        Ok(Self {
            path,
            db: Arc::new(Mutex::new(db)),
        })
    }

    fn open_conn(&self) -> Result<redlinedb::Connection> {
        self.db
            .lock()
            .map_err(|_| anyhow!("redline database mutex poisoned"))?
            .connect()
            .map_err(Into::into)
    }
}

impl BenchEngine for RedlineEngine {
    fn connect(&self, _worker_id: usize) -> Result<Box<dyn BenchConn>> {
        Ok(Box::new(RedlineConn {
            conn: self.open_conn()?,
        }))
    }

    fn setup_schema(&self) -> Result<()> {
        let mut conn = self.open_conn()?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv(k INTEGER PRIMARY KEY, tenant INTEGER, v BLOB, version INTEGER)",
            (),
        )?;
        conn.execute("CREATE INDEX IF NOT EXISTS kv_tenant_idx ON kv(tenant)", ())?;
        Ok(())
    }

    fn seed_kv(&self, rows: usize) -> Result<()> {
        let mut conn = self.open_conn()?;
        conn.execute("DELETE FROM kv", ())?;
        conn.begin(redlinedb::BeginMode::Immediate)?;
        {
            let mut stmt =
                conn.prepare("INSERT INTO kv(k, tenant, v, version) VALUES (?, ?, ?, ?)")?;
            for i in 0..rows {
                stmt.reset()?;
                stmt.clear_bindings();
                stmt.bind_i64(1, i as i64)?;
                stmt.bind_i64(2, (i % 32) as i64)?;
                stmt.bind_blob(3, seeded_blob(i))?;
                stmt.bind_i64(4, 1)?;
                while matches!(stmt.step()?, redlinedb::Step::Row(_)) {}
            }
        }
        let _ = conn.commit()?;
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        self.db
            .lock()
            .map_err(|_| anyhow!("redline database mutex poisoned"))?
            .checkpoint()?;
        Ok(())
    }

    fn snapshot(&self) -> Result<EngineSnapshot> {
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow!("redline database mutex poisoned"))?;
        let stats = db.benchmark_stats()?;
        Ok(EngineSnapshot {
            data_bytes: file_len(&self.path),
            wal_bytes: dir_wal_bytes(self.path.parent().unwrap_or(Path::new(".")), "wal", ".wal"),
            engine_stats: serde_json::to_value(stats)?,
        })
    }

    fn checksum(&self) -> Result<crate::report::Checksum> {
        let mut conn = self.open_conn()?;
        Ok(crate::report::Checksum {
            rows: scalar_i64(&mut conn, "SELECT COUNT(*) FROM kv")?,
            key_sum: scalar_i64(&mut conn, "SELECT MAX(k) FROM kv")?,
            version_sum: scalar_i64(&mut conn, "SELECT MAX(version) FROM kv")?,
            payload_bytes: scalar_i64(&mut conn, "SELECT COUNT(*) FROM kv WHERE tenant >= 0")?,
        })
    }
}

struct RedlineConn {
    conn: redlinedb::Connection,
}

impl BenchConn for RedlineConn {
    fn execute(&mut self, sql: &str, params: &[CellValue]) -> Result<u64> {
        let summary = self.conn.execute(sql, values(params))?;
        Ok(summary.rows_affected)
    }

    fn query_row(&mut self, sql: &str, params: &[CellValue]) -> Result<Vec<CellValue>> {
        let rows = self.query_all(sql, params)?;
        Ok(rows.into_iter().next().unwrap_or_default())
    }

    fn query_all(&mut self, sql: &str, params: &[CellValue]) -> Result<Vec<Vec<CellValue>>> {
        let mut stmt = self.conn.prepare(sql)?;
        stmt.bind_all(values(params))?;
        let count = stmt.column_count();
        let mut rows = Vec::new();
        while let redlinedb::Step::Row(row) = stmt.step()? {
            let mut out = Vec::with_capacity(count);
            for idx in 0..count {
                out.push(match row.get_ref(idx)? {
                    redlinedb::ValueRef::Null => CellValue::Null,
                    redlinedb::ValueRef::Integer(v) => CellValue::Integer(v),
                    redlinedb::ValueRef::Real(v) => CellValue::Real(v),
                    redlinedb::ValueRef::Text(v) => CellValue::Text(v.to_owned()),
                    redlinedb::ValueRef::Blob(v) => CellValue::Blob(v.to_vec()),
                });
            }
            rows.push(out);
        }
        Ok(rows)
    }

    fn begin_immediate(&mut self) -> Result<()> {
        self.conn.begin(redlinedb::BeginMode::Immediate)?;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let _ = self.conn.commit()?;
        Ok(())
    }
}

fn values(params: &[CellValue]) -> Vec<redlinedb::Value> {
    params
        .iter()
        .map(|value| match value {
            CellValue::Null => redlinedb::Value::Null,
            CellValue::Integer(v) => redlinedb::Value::Integer(*v),
            CellValue::Real(v) => redlinedb::Value::Real(*v),
            CellValue::Text(v) => redlinedb::Value::Text(v.clone().into()),
            CellValue::Blob(v) => redlinedb::Value::Blob(v.clone().into()),
        })
        .collect()
}

fn seeded_blob(seed: usize) -> Vec<u8> {
    format!("value-{seed:08}").into_bytes()
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

fn dir_wal_bytes(dir: &Path, name: &str, ext: &str) -> u64 {
    let wal_dir = dir.join(name);
    std::fs::read_dir(&wal_dir)
        .ok()
        .into_iter()
        .flat_map(|entries| entries.flatten())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|value| value == &ext[1..])
        })
        .map(|entry| entry.metadata().map(|meta| meta.len()).unwrap_or(0))
        .sum()
}

fn scalar_i64(conn: &mut redlinedb::Connection, sql: &str) -> Result<i64> {
    let mut rows = conn.query(sql, ())?;
    match rows.step()? {
        redlinedb::Step::Row(row) => match row.get_ref(0)? {
            redlinedb::ValueRef::Null => Ok(0),
            redlinedb::ValueRef::Integer(value) => Ok(value),
            other => Err(anyhow!("expected integer scalar, got {other:?}")),
        },
        redlinedb::Step::Done => Ok(0),
    }
}
