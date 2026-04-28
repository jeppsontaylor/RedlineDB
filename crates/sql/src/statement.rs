use std::sync::Arc;

use redlinedb_kernel::catalog::{
    CreateIndexSpec, CreateTableSpec, DropIndexSpec, DropTableSpec, SchemaEpoch, SqliteSchemaRow,
    TableDef,
};
use redlinedb_kernel::engine::Txn;
use redlinedb_kernel::format::RowId;
use sqlparser::ast::{Expr, OrderByExpr, SelectItem};

use crate::batch::{ExecContext, MaterializeNode, QueryMemoryBroker, RowBatch};
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::exec::execute_prepared;
use crate::session::BeginMode;
use crate::value::SqlValue;

#[derive(Debug, Clone, Default)]
pub struct ParamLayout {
    pub(crate) slots: Vec<Option<String>>,
    pub(crate) named: std::collections::HashMap<String, usize>,
}

impl ParamLayout {
    pub fn push_anonymous(&mut self) -> usize {
        self.slots.push(None);
        self.slots.len()
    }

    pub fn push_numbered(&mut self, index: usize) -> usize {
        if index == 0 {
            return 0;
        }
        if self.slots.len() < index {
            self.slots.resize(index, None);
        }
        index
    }

    pub fn push_named(&mut self, name: String) -> usize {
        if let Some(&slot) = self.named.get(&name) {
            return slot;
        }
        self.slots.push(Some(name.clone()));
        let slot = self.slots.len();
        self.named.insert(name, slot);
        slot
    }

    pub fn slot_for_name(&self, name: &str) -> Option<usize> {
        if let Some(rest) = name.strip_prefix('?') {
            if let Ok(slot) = rest.parse::<usize>() {
                return Some(slot);
            }
        }
        self.named
            .get(name)
            .copied()
            .or_else(|| self.named.get(&format!(":{name}")).copied())
            .or_else(|| self.named.get(&format!("@{name}")).copied())
            .or_else(|| self.named.get(&format!("${name}")).copied())
    }

    pub fn count(&self) -> usize {
        self.slots.len()
    }
}

#[derive(Debug, Clone)]
pub struct PreparedTemplate {
    pub sql: Arc<str>,
    pub schema_epoch: SchemaEpoch,
    pub stats_epoch: u64,
    pub optimizer_hash: u64,
    pub param_layout: ParamLayout,
    pub output_columns: Arc<[String]>,
    pub readonly: bool,
    pub kind: PreparedKind,
}

#[derive(Debug, Clone)]
pub enum PreparedKind {
    Begin(BeginMode),
    Commit,
    Rollback,
    CreateTable(CreateTableSpec),
    CreateIndex(CreateIndexSpec),
    DropTable(DropTableSpec),
    DropIndex(DropIndexSpec),
    Analyze(AnalyzePlan),
    Explain(ExplainPlan),
    Select(SelectPlan),
    Insert(InsertPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
}

#[derive(Debug, Clone)]
pub struct AnalyzePlan {
    pub table: Option<Arc<TableDef>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    QueryPlan,
    Text,
    Json,
}

#[derive(Debug, Clone)]
pub struct ExplainPlan {
    pub format: ExplainFormat,
    pub analyze: bool,
    pub inner: Arc<PreparedTemplate>,
}

#[derive(Debug, Clone)]
pub enum SelectSource {
    Table(Arc<TableDef>),
    Tables(Vec<BoundTable>),
    SqliteSchema,
    Empty,
}

#[derive(Debug, Clone)]
pub struct BoundTable {
    pub table: Arc<TableDef>,
    pub alias: Option<Arc<str>>,
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    pub source: SelectSource,
    pub projection: Vec<SelectItem>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub table: Arc<TableDef>,
    pub columns: Vec<usize>,
    pub rows: Vec<Vec<Expr>>,
    pub default_values: bool,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table: Arc<TableDef>,
    pub assignments: Vec<(usize, Expr)>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub table: Arc<TableDef>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Default)]
pub(crate) enum RuntimeState {
    #[default]
    Idle,
    Select(SelectRuntime),
    Done,
}

#[derive(Debug)]
pub(crate) struct ExecutionResult {
    pub(crate) runtime: RuntimeState,
    pub(crate) affected_rows: usize,
}

#[derive(Debug)]
pub(crate) struct SelectRuntime {
    pub(crate) tx: Option<Txn>,
    pub(crate) restore_tx: bool,
    pub(crate) source: SelectRuntimeSource,
    pub(crate) selection: Option<Expr>,
    pub(crate) projection: Vec<SelectItem>,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
    pub(crate) seen: usize,
    pub(crate) yielded: usize,
    pub(crate) memory: QueryMemoryBroker,
}

#[derive(Debug)]
pub(crate) enum SelectRuntimeSource {
    Table {
        table: Arc<TableDef>,
        rowids: Vec<RowId>,
        cursor: usize,
    },
    SqliteSchema {
        rows: Vec<SqliteSchemaRow>,
        cursor: usize,
    },
    Batched {
        node: MaterializeNode,
        ctx: ExecContext,
        batch: RowBatch,
        cursor: usize,
    },
    Empty,
}

#[derive(Debug)]
pub struct Statement {
    pub(crate) conn: Arc<Connection>,
    pub(crate) template: Arc<PreparedTemplate>,
    bindings: Vec<Option<SqlValue>>,
    runtime: RuntimeState,
    current_row: Option<Vec<SqlValue>>,
    affected_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Row,
    Done,
}

impl Statement {
    pub(crate) fn new(conn: Arc<Connection>, template: Arc<PreparedTemplate>) -> Self {
        let mut bindings = Vec::with_capacity(template.param_layout.count() + 1);
        bindings.resize(template.param_layout.count() + 1, None);
        Self {
            conn,
            template,
            bindings,
            runtime: RuntimeState::Idle,
            current_row: None,
            affected_rows: 0,
        }
    }

    pub fn bind_null(&mut self, index: usize) -> Result<()> {
        self.set_binding(index, SqlValue::Null)
    }

    pub fn bind_i64(&mut self, index: usize, value: i64) -> Result<()> {
        self.set_binding(index, SqlValue::Integer(value))
    }

    pub fn bind_f64(&mut self, index: usize, value: f64) -> Result<()> {
        self.set_binding(index, SqlValue::Real(value))
    }

    pub fn bind_text(&mut self, index: usize, value: impl Into<Arc<str>>) -> Result<()> {
        self.set_binding(index, SqlValue::Text(value.into()))
    }

    pub fn bind_blob(&mut self, index: usize, value: impl Into<Arc<[u8]>>) -> Result<()> {
        self.set_binding(index, SqlValue::Blob(value.into()))
    }

    pub fn bind_named(&mut self, name: &str, value: SqlValue) -> Result<()> {
        let slot = self
            .template
            .param_layout
            .slot_for_name(name)
            .ok_or_else(|| Error::ParameterOutOfRange(0))?;
        self.set_binding(slot, value)
    }

    pub fn reset(&mut self) -> Result<()> {
        crate::exec::finalize_runtime(self.conn.as_ref(), &mut self.runtime)?;
        self.runtime = RuntimeState::Idle;
        self.current_row = None;
        self.affected_rows = 0;
        Ok(())
    }

    pub fn step(&mut self) -> Result<Step> {
        if matches!(self.runtime, RuntimeState::Idle) {
            if self.template.schema_epoch != self.conn.schema_epoch()
                || self.template.stats_epoch != self.conn.stats_epoch().0
                || self.template.optimizer_hash != self.conn.optimizer_hash()
            {
                let new_template = self.conn.prepare_cached(self.template.sql.as_ref())?;
                let mut new_bindings = Vec::with_capacity(new_template.param_layout.count() + 1);
                new_bindings.resize(new_template.param_layout.count() + 1, None);
                for (idx, value) in self.bindings.iter().cloned().enumerate() {
                    if idx < new_bindings.len() {
                        new_bindings[idx] = value;
                    }
                }
                self.template = new_template;
                self.bindings = new_bindings;
            }
            let result = execute_prepared(self.conn.as_ref(), &self.template, &self.bindings)?;
            self.affected_rows = result.affected_rows;
            self.runtime = result.runtime;
        }
        match &mut self.runtime {
            RuntimeState::Select(runtime) => {
                let done = crate::exec::step_select_runtime(
                    self.conn.as_ref(),
                    runtime,
                    &self.template,
                    &self.bindings,
                    &mut self.current_row,
                )?;
                if done {
                    self.runtime = RuntimeState::Done;
                    Ok(Step::Done)
                } else {
                    Ok(Step::Row)
                }
            }
            RuntimeState::Done => Ok(Step::Done),
            RuntimeState::Idle => unreachable!("runtime state should be initialized before match"),
        }
    }

    pub fn column_count(&self) -> usize {
        self.template.output_columns.len()
    }

    pub fn column_name(&self, index: usize) -> &str {
        self.template.output_columns[index].as_str()
    }

    pub fn column_value(&self, index: usize) -> Result<&SqlValue> {
        self.current_row
            .as_ref()
            .and_then(|row| row.get(index))
            .ok_or_else(|| Error::Bind("no current row".to_owned()))
    }

    pub fn column_i64(&self, index: usize) -> Result<i64> {
        match self.column_value(index)? {
            SqlValue::Integer(v) => Ok(*v),
            SqlValue::Real(v) => Ok(*v as i64),
            _ => Err(Error::DatatypeMismatch),
        }
    }

    pub fn column_f64(&self, index: usize) -> Result<f64> {
        match self.column_value(index)? {
            SqlValue::Integer(v) => Ok(*v as f64),
            SqlValue::Real(v) => Ok(*v),
            _ => Err(Error::DatatypeMismatch),
        }
    }

    pub fn column_text(&self, index: usize) -> Result<&str> {
        match self.column_value(index)? {
            SqlValue::Text(v) => Ok(v.as_ref()),
            _ => Err(Error::DatatypeMismatch),
        }
    }

    pub fn column_blob(&self, index: usize) -> Result<&[u8]> {
        match self.column_value(index)? {
            SqlValue::Blob(v) => Ok(v.as_ref()),
            _ => Err(Error::DatatypeMismatch),
        }
    }

    pub fn parameter_count(&self) -> usize {
        self.template.param_layout.count()
    }

    pub fn parameter_index(&self, name: &str) -> Option<usize> {
        self.template.param_layout.slot_for_name(name)
    }

    pub fn clear_bindings(&mut self) {
        for slot in &mut self.bindings {
            *slot = None;
        }
    }

    pub fn is_readonly(&self) -> bool {
        self.template.readonly
    }

    pub fn affected_rows(&self) -> usize {
        self.affected_rows
    }

    pub fn template(&self) -> Arc<PreparedTemplate> {
        Arc::clone(&self.template)
    }

    fn set_binding(&mut self, index: usize, value: SqlValue) -> Result<()> {
        if index == 0 || index >= self.bindings.len() {
            return Err(Error::ParameterOutOfRange(index));
        }
        self.bindings[index] = Some(value);
        Ok(())
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        let _ = crate::exec::finalize_runtime(self.conn.as_ref(), &mut self.runtime);
    }
}
