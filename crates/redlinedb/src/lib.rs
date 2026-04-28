mod backup;
mod error;
mod machine;
mod options;
mod params;
mod registry;
mod value;

use std::cell::Cell;
use std::cmp::Ordering;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::Duration;

pub use error::{Error, ErrorCode, Result};
pub use machine::{
    BinaryOp, ColumnRef, DeleteSpec, ExprSpec, InsertSpec, OrderSpec, QuerySpec, SchemaHandle,
    SelectSpec, TableRef, UnaryOp, UpdateSpec,
};
pub use options::{
    AnalyzeOptions, BackupOptions, BackupStats, CheckpointStats, CommitStats, ConnectionStats,
    DatabaseStats, Durability, ExecuteSummary, FunctionArity, FunctionFlags, MemoryOptions,
    OpenOptions, OptimizerOptions, QueryMemoryOptions, VacuumStats,
};
pub use params::Params;
pub use value::{Value, ValueRef};

pub use redlinedb_sql::BeginMode;

pub struct Database {
    inner: Arc<registry::DatabaseEntry>,
}

pub struct Connection {
    inner: Arc<redlinedb_sql::Connection>,
    read_only: bool,
    busy_timeout: Duration,
    interrupted: Arc<AtomicBool>,
    _sync_marker: Cell<()>,
}

#[derive(Clone, Debug)]
pub struct Prepared {
    template: Arc<redlinedb_sql::PreparedTemplate>,
}

pub struct Statement<'conn> {
    inner: redlinedb_sql::Statement,
    interrupted: Arc<AtomicBool>,
    _marker: Rc<()>,
    _conn: std::marker::PhantomData<&'conn mut Connection>,
}

pub struct Row<'stmt> {
    stmt: &'stmt Statement<'stmt>,
}

pub enum Step<'a> {
    Row(Row<'a>),
    Done,
}

pub struct Rows<'conn> {
    stmt: Statement<'conn>,
}

pub struct Transaction<'conn> {
    conn: &'conn mut Connection,
    committed: bool,
}

#[derive(Clone)]
pub struct InterruptHandle {
    flag: Arc<AtomicBool>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, OpenOptions::default())
    }

    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = OpenOptions::default();
        options.create = true;
        Self::open_with_options(path, options)
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let inner = registry::open_database(path, &options, options.create)?;
        Ok(Self { inner })
    }

    pub fn connect(&self) -> Result<Connection> {
        Ok(Connection {
            inner: self.inner.db.connect(),
            read_only: self.inner.fingerprint.read_only,
            busy_timeout: self.inner.busy_timeout,
            interrupted: Arc::clone(&self.inner.interrupt),
            _sync_marker: Cell::new(()),
        })
    }

    pub fn prepare(&self, sql: &str) -> Result<Prepared> {
        let mut conn = self.connect()?;
        let stmt = conn.prepare(sql)?;
        Ok(Prepared {
            template: stmt.template(),
        })
    }

    pub fn checkpoint(&self) -> Result<CheckpointStats> {
        let checkpoint = self.inner.db.checkpoint()?;
        Ok(CheckpointStats {
            generation: checkpoint.control.generation,
            checkpoint_lsn: checkpoint.control.checkpoint_lsn.0,
            page_count: checkpoint.control.page_count,
            flushed_pages: checkpoint.flushed_pages,
            flush_batches: checkpoint.flush_batches,
        })
    }

    pub fn vacuum(&self) -> Result<VacuumStats> {
        let vacuum = self.inner.db.vacuum()?;
        Ok(VacuumStats {
            rows_scanned: vacuum.rows_scanned,
            chains_pruned: vacuum.chains_pruned,
            undo_links_removed: vacuum.undo_links_removed,
            dead_rows_removed: vacuum.dead_rows_removed,
            oldest_active_snapshot_csn: vacuum.oldest_active_snapshot_csn.0,
        })
    }

    pub fn stats(&self) -> Result<DatabaseStats> {
        let stats = self.inner.db.stats()?;
        Ok(DatabaseStats {
            schema_epoch: self.inner.db.schema_epoch().0,
            checkpoint_generation: stats.checkpoint.map(|control| control.generation),
            resident_heap_pages: stats.resident_heap_pages,
            wal_written_lsn: stats.wal_written_lsn.0,
            wal_durable_lsn: stats.wal_durable_lsn.0,
            vacuum_horizon_csn: stats.vacuum_horizon_csn.0,
            table_count: stats.tx.committed_states,
            column_count: stats.tx.active_transactions,
            index_count: stats.tx.active_snapshots,
        })
    }

    pub fn backup_to_path(
        &self,
        dst: impl AsRef<Path>,
        options: BackupOptions,
    ) -> Result<BackupStats> {
        backup::backup_to_path(self, dst, options)
    }

    pub fn interrupt_all(&self) {
        self.inner.interrupt.store(true, AtomicOrdering::Relaxed);
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Prepared {
    pub fn sql(&self) -> &str {
        self.template.sql.as_ref()
    }

    pub fn parameter_count(&self) -> usize {
        self.template.param_layout.count()
    }

    pub fn column_count(&self) -> usize {
        self.template.output_columns.len()
    }

    pub fn column_name(&self, index: usize) -> &str {
        self.template.output_columns[index].as_str()
    }

    pub fn is_readonly(&self) -> bool {
        self.template.readonly
    }
}

impl Connection {
    pub fn prepare<'c>(&'c mut self, sql: &str) -> Result<Statement<'c>> {
        self.check_interrupt()?;
        let stmt = self.inner.prepare(sql)?;
        if self.read_only && !stmt.is_readonly() {
            return Err(Error::new(ErrorCode::ReadOnly, "connection is read-only"));
        }
        Ok(Statement {
            inner: stmt,
            interrupted: Arc::clone(&self.interrupted),
            _marker: Rc::new(()),
            _conn: std::marker::PhantomData,
        })
    }

    pub fn prepare_cached<'c>(&'c mut self, sql: &str) -> Result<Statement<'c>> {
        self.prepare(sql)
    }

    pub fn query<'c, P: Params>(&'c mut self, sql: &str, params: P) -> Result<Rows<'c>> {
        let mut stmt = self.prepare(sql)?;
        stmt.bind_all(params)?;
        Ok(Rows { stmt })
    }

    pub fn execute<P: Params>(&mut self, sql: &str, params: P) -> Result<ExecuteSummary> {
        let mut stmt = self.prepare(sql)?;
        stmt.bind_all(params)?;
        let mut rows = 0_u64;
        while let Step::Row(_) = stmt.step()? {
            rows += 1;
        }
        Ok(ExecuteSummary {
            rows_affected: stmt.affected_rows() as u64,
            rows_returned: rows,
        })
    }

    pub fn begin(&mut self, mode: BeginMode) -> Result<()> {
        self.check_interrupt()?;
        self.inner.begin(mode)?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<CommitStats> {
        self.check_interrupt()?;
        self.inner.commit()?;
        Ok(CommitStats {
            changes: self.inner.changes() as u64,
        })
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.inner.rollback()?;
        Ok(())
    }

    pub fn transaction<T>(
        &mut self,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        self.begin(BeginMode::Deferred)?;
        let mut tx = Transaction {
            conn: self,
            committed: false,
        };
        let result = f(&mut tx);
        if result.is_ok() && !tx.committed {
            tx.commit()?;
        } else if result.is_err() && !tx.committed {
            let _ = tx.rollback();
        }
        result
    }

    pub fn set_busy_timeout(&mut self, timeout: Duration) {
        self.busy_timeout = timeout;
    }

    pub fn interrupt_handle(&self) -> InterruptHandle {
        InterruptHandle {
            flag: Arc::clone(&self.interrupted),
        }
    }

    pub fn changes(&self) -> u64 {
        self.inner.changes() as u64
    }

    pub fn last_insert_rowid(&self) -> Option<i64> {
        self.inner.last_insert_rowid()
    }

    pub fn stats(&self) -> ConnectionStats {
        ConnectionStats {
            changes: self.changes(),
            last_insert_rowid: self.last_insert_rowid(),
            busy_timeout_ms: self.busy_timeout.as_millis() as u64,
            interrupted: self.interrupted.load(AtomicOrdering::Relaxed),
        }
    }

    pub fn create_scalar_function<F>(
        &mut self,
        _name: &str,
        _arity: FunctionArity,
        _flags: FunctionFlags,
        _f: F,
    ) -> Result<()>
    where
        F: Send + Sync + 'static + Fn(&[ValueRef<'_>]) -> Result<Value>,
    {
        Err(Error::unsupported(
            "scalar function hooks are not implemented yet",
        ))
    }

    pub fn create_collation<F>(&mut self, _name: &str, _cmp: F) -> Result<()>
    where
        F: Send + Sync + 'static + Fn(&str, &str) -> Ordering,
    {
        Err(Error::unsupported(
            "collation hooks are not implemented yet",
        ))
    }

    fn check_interrupt(&self) -> Result<()> {
        if self.interrupted.load(AtomicOrdering::Relaxed) {
            return Err(Error::new(ErrorCode::Interrupt, "interrupted"));
        }
        Ok(())
    }
}

impl<'conn> Statement<'conn> {
    pub fn bind_all<P: Params>(&mut self, params: P) -> Result<()> {
        params.bind_into(self)
    }

    pub fn bind_null(&mut self, index: usize) -> Result<()> {
        Ok(self.inner.bind_null(index)?)
    }

    pub fn bind_i64(&mut self, index: usize, value: i64) -> Result<()> {
        Ok(self.inner.bind_i64(index, value)?)
    }

    pub fn bind_f64(&mut self, index: usize, value: f64) -> Result<()> {
        Ok(self.inner.bind_f64(index, value)?)
    }

    pub fn bind_text(&mut self, index: usize, value: impl Into<Arc<str>>) -> Result<()> {
        Ok(self.inner.bind_text(index, value)?)
    }

    pub fn bind_blob(&mut self, index: usize, value: impl Into<Arc<[u8]>>) -> Result<()> {
        Ok(self.inner.bind_blob(index, value)?)
    }

    pub fn bind_value(&mut self, index: usize, value: Value) -> Result<()> {
        match value {
            Value::Null => self.bind_null(index),
            Value::Integer(value) => self.bind_i64(index, value),
            Value::Real(value) => self.bind_f64(index, value),
            Value::Text(value) => self.bind_text(index, value),
            Value::Blob(value) => self.bind_blob(index, value),
        }
    }

    pub fn bind_named(&mut self, name: &str, value: Value) -> Result<()> {
        let sql_value: redlinedb_sql::SqlValue = value.into();
        self.inner.bind_named(name, sql_value)?;
        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        self.inner.reset()?;
        Ok(())
    }

    pub fn clear_bindings(&mut self) {
        self.inner.clear_bindings();
    }

    pub fn step(&mut self) -> Result<Step<'_>> {
        if self.interrupted.load(AtomicOrdering::Relaxed) {
            return Err(Error::new(ErrorCode::Interrupt, "interrupted"));
        }
        match self.inner.step()? {
            redlinedb_sql::Step::Row => Ok(Step::Row(Row { stmt: self })),
            redlinedb_sql::Step::Done => Ok(Step::Done),
        }
    }

    pub fn is_readonly(&self) -> bool {
        self.inner.is_readonly()
    }

    pub fn affected_rows(&self) -> usize {
        self.inner.affected_rows()
    }

    pub fn parameter_count(&self) -> usize {
        self.inner.parameter_count()
    }

    pub fn parameter_index(&self, name: &str) -> Option<usize> {
        self.inner.parameter_index(name)
    }

    pub fn column_count(&self) -> usize {
        self.inner.column_count()
    }

    pub fn column_name(&self, index: usize) -> &str {
        self.inner.column_name(index)
    }

    pub fn template(&self) -> Arc<redlinedb_sql::PreparedTemplate> {
        self.inner.template()
    }
}

impl<'stmt> Row<'stmt> {
    pub fn get<T: FromValue>(&self, index: usize) -> Result<T> {
        T::from_statement(self.stmt, index)
    }

    pub fn get_ref(&self, index: usize) -> Result<ValueRef<'_>> {
        let value = self.stmt.inner.column_value(index)?;
        Ok(match value {
            redlinedb_sql::SqlValue::Null => ValueRef::Null,
            redlinedb_sql::SqlValue::Integer(value) => ValueRef::Integer(*value),
            redlinedb_sql::SqlValue::Real(value) => ValueRef::Real(*value),
            redlinedb_sql::SqlValue::Text(value) => ValueRef::Text(value.as_ref()),
            redlinedb_sql::SqlValue::Blob(value) => ValueRef::Blob(value.as_ref()),
        })
    }
}

impl<'conn> Rows<'conn> {
    pub fn step(&mut self) -> Result<Step<'_>> {
        self.stmt.step()
    }

    pub fn statement(&mut self) -> &mut Statement<'conn> {
        &mut self.stmt
    }
}

impl<'conn> Transaction<'conn> {
    pub fn execute<P: Params>(&mut self, sql: &str, params: P) -> Result<ExecuteSummary> {
        self.conn.execute(sql, params)
    }

    pub fn prepare<'a>(&'a mut self, sql: &str) -> Result<Statement<'a>> {
        self.conn.prepare(sql)
    }

    pub fn commit(&mut self) -> Result<()> {
        if !self.committed {
            self.conn.commit()?;
            self.committed = true;
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        if !self.committed {
            self.conn.rollback()?;
            self.committed = true;
        }
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.conn.rollback();
        }
    }
}

impl InterruptHandle {
    pub fn interrupt(&self) {
        self.flag.store(true, AtomicOrdering::Relaxed);
    }
}

pub trait FromValue: Sized {
    fn from_statement(stmt: &Statement<'_>, index: usize) -> Result<Self>;
}

impl FromValue for i64 {
    fn from_statement(stmt: &Statement<'_>, index: usize) -> Result<Self> {
        Ok(stmt.inner.column_i64(index)?)
    }
}

impl FromValue for f64 {
    fn from_statement(stmt: &Statement<'_>, index: usize) -> Result<Self> {
        Ok(stmt.inner.column_f64(index)?)
    }
}

impl FromValue for String {
    fn from_statement(stmt: &Statement<'_>, index: usize) -> Result<Self> {
        Ok(stmt.inner.column_text(index)?.to_owned())
    }
}

impl FromValue for Value {
    fn from_statement(stmt: &Statement<'_>, index: usize) -> Result<Self> {
        Ok(
            match match stmt.inner.column_value(index)? {
                redlinedb_sql::SqlValue::Null => ValueRef::Null,
                redlinedb_sql::SqlValue::Integer(value) => ValueRef::Integer(*value),
                redlinedb_sql::SqlValue::Real(value) => ValueRef::Real(*value),
                redlinedb_sql::SqlValue::Text(value) => ValueRef::Text(value.as_ref()),
                redlinedb_sql::SqlValue::Blob(value) => ValueRef::Blob(value.as_ref()),
            } {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(value) => Value::Integer(value),
                ValueRef::Real(value) => Value::Real(value),
                ValueRef::Text(value) => Value::Text(Arc::from(value)),
                ValueRef::Blob(value) => Value::Blob(Arc::from(value)),
            },
        )
    }
}

fn sql_options(options: &OpenOptions) -> redlinedb_sql::DbOptions {
    let mut db = redlinedb_sql::DbOptions::default();
    let page_size = db.engine.page_size.max(1);
    let buffer_pages = (options.memory.cache_bytes / page_size).max(16);
    db.engine.buffer_pool_pages = buffer_pages;
    db.optimizer.enabled = options.optimizer.enabled;
    db.optimizer.max_exact_join_tables = options.optimizer.max_exact_join_tables;
    db.optimizer.max_join_alternatives = options.optimizer.max_join_alternatives;
    db.optimizer.enable_multi_index_or = options.optimizer.enable_multi_index_or;
    db.optimizer.enable_multi_index_and = options.optimizer.enable_multi_index_and;
    db.optimizer.enable_covering_index = options.optimizer.enable_covering_index;
    db.query_memory.work_mem_bytes = options.query_memory.work_mem_bytes;
    db.query_memory.max_spill_bytes = options.query_memory.max_spill_bytes;
    db.query_memory.batch_rows = options.query_memory.batch_rows;
    db.stats.exact_analyze_row_threshold = options.stats.exact_analyze_row_threshold;
    db.stats.sample_rows = options.stats.sample_rows;
    db.stats.mcv_capacity = options.stats.mcv_capacity;
    db.stats.histogram_buckets = options.stats.histogram_buckets;
    db
}
