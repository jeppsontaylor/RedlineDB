use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use parking_lot::RwLock;
use redlinedb_kernel::catalog::{SchemaSnapshot, StatsEpoch, StatsSnapshot, StatsStore};
use redlinedb_kernel::engine::page_heap::VacuumStats;
use redlinedb_kernel::engine::{
    CheckpointStats, Engine, EngineConfig, RecoveryTarget, StorageStatsSnapshot, Txn,
};
use redlinedb_kernel::txn::Isolation;

use crate::error::{Error, Result};
use crate::parser::parse_prepared_template;
use crate::session::{BeginMode, SessionState, UniqueLockTable};
use crate::statement::{PreparedTemplate, Statement, Step};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct StatementCacheKey {
    schema_epoch: u64,
    stats_epoch: u64,
    optimizer_hash: u64,
    sql: Arc<str>,
}

#[derive(Debug, Default)]
struct StatementCache {
    shards: Vec<RwLock<HashMap<StatementCacheKey, Arc<PreparedTemplate>>>>,
}

impl StatementCache {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(64);
        for _ in 0..64 {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self { shards }
    }

    fn shard_index(&self, key: &StatementCacheKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len().max(1)
    }

    fn get(&self, key: &StatementCacheKey) -> Option<Arc<PreparedTemplate>> {
        let shard = self.shard_index(key);
        self.shards[shard].read().get(key).cloned()
    }

    fn insert(&self, key: StatementCacheKey, template: Arc<PreparedTemplate>) {
        let shard = self.shard_index(&key);
        self.shards[shard].write().insert(key, template);
    }
}

#[derive(Debug, Clone)]
pub struct DbOptions {
    pub engine: EngineConfig,
    pub unique_lock_shards: usize,
    pub optimizer: OptimizerConfig,
    pub query_memory: QueryMemoryConfig,
    pub stats: StatsConfig,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            engine: EngineConfig::default(),
            unique_lock_shards: 128,
            optimizer: OptimizerConfig::default(),
            query_memory: QueryMemoryConfig::default(),
            stats: StatsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OptimizerConfig {
    pub enabled: bool,
    pub max_exact_join_tables: usize,
    pub max_join_alternatives: usize,
    pub enable_multi_index_or: bool,
    pub enable_multi_index_and: bool,
    pub enable_covering_index: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_exact_join_tables: 8,
            max_join_alternatives: 4,
            enable_multi_index_or: true,
            enable_multi_index_and: true,
            enable_covering_index: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryMemoryConfig {
    pub work_mem_bytes: usize,
    pub max_spill_bytes: usize,
    pub batch_rows: usize,
}

impl Default for QueryMemoryConfig {
    fn default() -> Self {
        Self {
            work_mem_bytes: 8 * 1024 * 1024,
            max_spill_bytes: 1024 * 1024 * 1024,
            batch_rows: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatsConfig {
    pub exact_analyze_row_threshold: usize,
    pub sample_rows: usize,
    pub mcv_capacity: usize,
    pub histogram_buckets: usize,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            exact_analyze_row_threshold: 100_000,
            sample_rows: 32_768,
            mcv_capacity: 100,
            histogram_buckets: 100,
        }
    }
}

#[derive(Debug)]
pub struct Database {
    engine: Arc<Engine>,
    unique_locks: Arc<UniqueLockTable>,
    stmt_cache: StatementCache,
    optimizer_hash: u64,
    stats_store: StatsStore,
    stats: ArcSwap<StatsSnapshot>,
    stats_config: StatsConfig,
    query_memory: QueryMemoryConfig,
    optimizer: OptimizerConfig,
}

#[derive(Debug)]
pub struct Connection {
    db: Arc<Database>,
    session: Mutex<SessionState>,
    local_cache: StatementCache,
}

impl Database {
    pub fn create(path: impl AsRef<Path>, opts: DbOptions) -> Result<Arc<Self>> {
        let base = path.as_ref();
        let engine = Engine::create(base, opts.engine)?;
        let stats_store = StatsStore::new(base);
        let stats = stats_store
            .load()?
            .unwrap_or_else(|| Arc::new(StatsSnapshot::default()));
        let optimizer_hash = hash_optimizer(&opts.optimizer, &opts.query_memory);
        Ok(Arc::new(Self {
            engine,
            unique_locks: UniqueLockTable::new(opts.unique_lock_shards),
            stmt_cache: StatementCache::new(),
            optimizer_hash,
            stats_store,
            stats: ArcSwap::from(stats),
            stats_config: opts.stats,
            query_memory: opts.query_memory,
            optimizer: opts.optimizer,
        }))
    }

    pub fn open(path: impl AsRef<Path>, opts: DbOptions) -> Result<Arc<Self>> {
        let base = path.as_ref();
        let engine = Engine::open(base, opts.engine)?;
        let stats_store = StatsStore::new(base);
        let stats = stats_store
            .load()?
            .unwrap_or_else(|| Arc::new(StatsSnapshot::default()));
        let optimizer_hash = hash_optimizer(&opts.optimizer, &opts.query_memory);
        Ok(Arc::new(Self {
            engine,
            unique_locks: UniqueLockTable::new(opts.unique_lock_shards),
            stmt_cache: StatementCache::new(),
            optimizer_hash,
            stats_store,
            stats: ArcSwap::from(stats),
            stats_config: opts.stats,
            query_memory: opts.query_memory,
            optimizer: opts.optimizer,
        }))
    }

    pub fn open_with_recovery_target(
        path: impl AsRef<Path>,
        opts: DbOptions,
        target: RecoveryTarget,
    ) -> Result<Arc<Self>> {
        let base = path.as_ref();
        let engine = Engine::open_with_recovery_target(base, opts.engine, target)?;
        let stats_store = StatsStore::new(base);
        let stats = stats_store
            .load()?
            .unwrap_or_else(|| Arc::new(StatsSnapshot::default()));
        let optimizer_hash = hash_optimizer(&opts.optimizer, &opts.query_memory);
        Ok(Arc::new(Self {
            engine,
            unique_locks: UniqueLockTable::new(opts.unique_lock_shards),
            stmt_cache: StatementCache::new(),
            optimizer_hash,
            stats_store,
            stats: ArcSwap::from(stats),
            stats_config: opts.stats,
            query_memory: opts.query_memory,
            optimizer: opts.optimizer,
        }))
    }

    pub fn connect(self: &Arc<Self>) -> Arc<Connection> {
        Arc::new(Connection {
            db: Arc::clone(self),
            session: Mutex::new(SessionState::default()),
            local_cache: StatementCache::new(),
        })
    }

    pub(crate) fn stats_epoch(&self) -> StatsEpoch {
        self.stats.load_full().epoch
    }

    pub(crate) fn stats_snapshot(&self) -> Arc<StatsSnapshot> {
        self.stats.load_full()
    }

    pub(crate) fn optimizer_hash(&self) -> u64 {
        self.optimizer_hash
    }

    pub(crate) fn stats_config(&self) -> &StatsConfig {
        &self.stats_config
    }

    pub(crate) fn query_memory(&self) -> &QueryMemoryConfig {
        &self.query_memory
    }

    pub(crate) fn optimizer_config(&self) -> &OptimizerConfig {
        &self.optimizer
    }

    pub(crate) fn publish_stats(&self, snapshot: Arc<StatsSnapshot>) -> Result<()> {
        self.stats_store.save(snapshot.as_ref())?;
        self.stats.store(snapshot);
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<CheckpointStats> {
        Ok(self.engine.checkpoint_with_stats()?)
    }

    pub fn vacuum(&self) -> Result<VacuumStats> {
        Ok(self.engine.vacuum()?)
    }

    pub fn stats(&self) -> Result<StorageStatsSnapshot> {
        Ok(self.engine.storage_stats()?)
    }

    pub fn tx_status_stats(&self) -> redlinedb_kernel::engine::TxStatusStats {
        self.engine.tx_status_stats()
    }

    pub fn schema_epoch(&self) -> redlinedb_kernel::catalog::SchemaEpoch {
        self.engine.schema_epoch()
    }

    pub fn schema_snapshot(&self) -> Arc<SchemaSnapshot> {
        self.engine.schema_snapshot()
    }

    pub fn engine_config(&self) -> EngineConfig {
        self.engine.config().clone()
    }
}

impl Connection {
    pub fn prepare(self: &Arc<Self>, sql: &str) -> Result<Statement> {
        let template = self.prepare_cached(sql)?;
        Ok(Statement::new(Arc::clone(self), template))
    }

    pub fn execute(self: &Arc<Self>, sql: &str) -> Result<usize> {
        let mut stmt = self.prepare(sql)?;
        let mut rows = 0usize;
        while let Step::Row = stmt.step()? {
            rows += 1;
        }
        if stmt.is_readonly() {
            Ok(rows)
        } else {
            Ok(stmt.affected_rows())
        }
    }

    pub fn begin(&self, mode: BeginMode) -> Result<()> {
        let mut session = self.session.lock().expect("session poisoned");
        if session.tx.is_some() {
            return Err(Error::TransactionState("transaction already active"));
        }
        let tx = self.db.engine.begin(match mode {
            BeginMode::Deferred | BeginMode::Immediate | BeginMode::Exclusive => {
                Isolation::Snapshot
            }
        })?;
        session.tx = Some(tx);
        session.failed = false;
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let mut session = self.session.lock().expect("session poisoned");
        if session.failed {
            return Err(Error::TransactionState(
                "transaction is failed and must roll back",
            ));
        }
        let tx = session
            .tx
            .take()
            .ok_or(Error::TransactionState("no active transaction"))?;
        let result = self.db.engine.commit(tx);
        session.unique_guards.clear();
        result?;
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        let mut session = self.session.lock().expect("session poisoned");
        let tx = session
            .tx
            .take()
            .ok_or(Error::TransactionState("no active transaction"))?;
        let result = self.db.engine.rollback(tx);
        session.unique_guards.clear();
        session.failed = false;
        result?;
        Ok(())
    }

    pub fn last_insert_rowid(&self) -> Option<i64> {
        self.session
            .lock()
            .expect("session poisoned")
            .last_insert_rowid
    }

    pub fn changes(&self) -> usize {
        self.session.lock().expect("session poisoned").changes
    }

    pub(crate) fn schema_epoch(&self) -> redlinedb_kernel::catalog::SchemaEpoch {
        self.db.engine.schema_epoch()
    }

    pub(crate) fn stats_epoch(&self) -> StatsEpoch {
        self.db.stats_epoch()
    }

    pub(crate) fn optimizer_hash(&self) -> u64 {
        self.db.optimizer_hash()
    }

    pub(crate) fn stats_config(&self) -> &StatsConfig {
        self.db.stats_config()
    }

    pub(crate) fn query_memory(&self) -> &QueryMemoryConfig {
        self.db.query_memory()
    }

    pub(crate) fn optimizer_config(&self) -> &OptimizerConfig {
        self.db.optimizer_config()
    }

    pub(crate) fn stats_snapshot(&self) -> Arc<StatsSnapshot> {
        self.db.stats_snapshot()
    }

    pub(crate) fn publish_stats(&self, snapshot: Arc<StatsSnapshot>) -> Result<()> {
        self.db.publish_stats(snapshot)
    }

    pub(crate) fn engine(&self) -> &Arc<Engine> {
        &self.db.engine
    }

    pub(crate) fn unique_locks(&self) -> &Arc<UniqueLockTable> {
        &self.db.unique_locks
    }

    pub(crate) fn with_session<T>(
        &self,
        f: impl FnOnce(&mut SessionState) -> Result<T>,
    ) -> Result<T> {
        let mut session = self.session.lock().expect("session poisoned");
        f(&mut session)
    }

    pub(crate) fn prepare_cached(self: &Arc<Self>, sql: &str) -> Result<Arc<PreparedTemplate>> {
        let normalized = sql.trim();
        let key = StatementCacheKey {
            schema_epoch: self.schema_epoch().0,
            stats_epoch: self.stats_epoch().0,
            optimizer_hash: self.optimizer_hash(),
            sql: Arc::from(normalized),
        };

        if let Some(template) = self.local_cache.get(&key) {
            return Ok(template);
        }

        if let Some(template) = self.db.stmt_cache.get(&key) {
            self.local_cache.insert(key, Arc::clone(&template));
            return Ok(template);
        }

        let mut template = parse_prepared_template(self.db.engine.as_ref(), sql)?;
        template.stats_epoch = self.stats_epoch().0;
        template.optimizer_hash = self.optimizer_hash();
        let template = Arc::new(template);
        self.db
            .stmt_cache
            .insert(key.clone(), Arc::clone(&template));
        self.local_cache.insert(key, Arc::clone(&template));
        Ok(template)
    }
}

fn hash_optimizer(optimizer: &OptimizerConfig, query_memory: &QueryMemoryConfig) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    optimizer.hash(&mut hasher);
    query_memory.hash(&mut hasher);
    hasher.finish()
}

#[allow(dead_code)]
fn _keep_txn_use(_: Txn) {}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn new_db() -> (tempfile::TempDir, Arc<Database>, Arc<Connection>) {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("sql-conn-test.db");
        let db = Database::create(&path, DbOptions::default()).expect("db");
        let conn = db.connect();
        (dir, db, conn)
    }

    #[test]
    fn execute_uses_active_transaction() {
        let (_dir, db, conn1) = new_db();
        let conn2 = db.connect();

        conn1
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .expect("create");
        conn1.begin(BeginMode::Deferred).expect("begin");
        conn1
            .execute("INSERT INTO t VALUES (1, 'one')")
            .expect("insert");

        let mut stmt = conn2
            .prepare("SELECT v FROM t WHERE id = 1")
            .expect("prepare");
        assert_eq!(stmt.step().expect("step"), Step::Done);

        conn1.commit().expect("commit");

        let mut stmt = conn2
            .prepare("SELECT v FROM t WHERE id = 1")
            .expect("prepare");
        assert_eq!(stmt.step().expect("step"), Step::Row);
        assert_eq!(stmt.column_text(0).expect("value"), "one");
    }

    #[test]
    fn prepare_reuses_cached_templates() {
        let (_dir, _db, conn) = new_db();

        let stmt1 = conn.prepare("SELECT 1").expect("prepare");
        let stmt2 = conn.prepare("SELECT 1").expect("prepare");

        assert!(Arc::ptr_eq(&stmt1.template, &stmt2.template));
    }
}
