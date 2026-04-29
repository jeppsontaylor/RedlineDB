mod redline;
mod sqlite;

use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use crate::config::{DurabilityKind, EngineKind, RunSpec};

pub use redline::RedlineEngine;
pub use sqlite::SqliteEngine;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CellValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct EngineSnapshot {
    pub data_bytes: u64,
    pub wal_bytes: u64,
    pub engine_stats: serde_json::Value,
    /// Lane BH P1 #7: durability syscall counters surfaced by the
    /// engine (currently only Redline populates these from the
    /// kernel's WAL coordinator). Left `None` for engines that
    /// can't report them so the JSON omits the keys instead of
    /// emitting bogus zeros.
    #[serde(default)]
    pub fsyncs_issued: Option<u64>,
    #[serde(default)]
    pub fdatasyncs_issued: Option<u64>,
    #[serde(default)]
    pub pwrites_issued: Option<u64>,
}

pub trait BenchEngine: Send + Sync {
    fn connect(&self, worker_id: usize) -> Result<Box<dyn BenchConn>>;
    fn setup_schema(&self) -> Result<()>;
    fn seed_kv(&self, rows: usize) -> Result<()>;
    fn checkpoint(&self) -> Result<()>;
    fn snapshot(&self) -> Result<EngineSnapshot>;
    fn checksum(&self) -> Result<crate::report::Checksum>;
}

pub trait BenchConn: Send {
    fn execute(&mut self, sql: &str, params: &[CellValue]) -> Result<u64>;
    fn query_row(&mut self, sql: &str, params: &[CellValue]) -> Result<Vec<CellValue>>;
    fn query_all(&mut self, sql: &str, params: &[CellValue]) -> Result<Vec<Vec<CellValue>>>;
    fn begin_immediate(&mut self) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
}

pub fn open(spec: &RunSpec, db_dir: &Path) -> Result<Box<dyn BenchEngine>> {
    match spec.engine {
        EngineKind::Redline => Ok(Box::new(RedlineEngine::open(spec, db_dir)?)),
        EngineKind::Sqlite => Ok(Box::new(SqliteEngine::open(spec, db_dir)?)),
    }
}

pub fn apply_durability(options: &mut redlinedb::OpenOptions, durability: DurabilityKind) {
    options.durability = match durability {
        DurabilityKind::Strict => redlinedb::Durability::Strict,
        DurabilityKind::Normal => redlinedb::Durability::Normal,
        DurabilityKind::Unsafe => redlinedb::Durability::UnsafeDev,
    };
}
