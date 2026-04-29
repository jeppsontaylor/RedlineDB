use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use redlinedb_kernel::catalog::{
    ColumnStats, ConstraintKind, EvalScratch, HistogramBucket, IndexStats, MostCommonValue,
    OwnedValue, RecordRef, RecordScratch, RowValueSource, SqliteSchemaRow, StatsEpoch,
    StatsSnapshot, TableDef, TableStats, ValueRef, apply_affinity, encode_record, eval_expr,
};
use redlinedb_kernel::engine::{Engine, Txn};
use redlinedb_kernel::format::RowId;
use redlinedb_kernel::txn::Isolation;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, OrderByExpr, SelectItem,
    UnaryOperator, Value,
};

use crate::batch::{
    ExecContext, ExecNode, ExecState, MaterializeNode, QueryMemoryBroker, RowBatch, RowLayout,
};
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::planner::{self, ExplainMetrics};
use crate::session::SessionState;
use crate::statement::{
    AnalyzePlan, ExecutionResult, ExplainPlan, PragmaPlan, PreparedKind, PreparedTemplate,
    RuntimeState, SelectPlan, SelectRuntime, SelectRuntimeSource, SelectSource,
};
use crate::value::{SqlValue, canonicalize, compare_values, is_truthy};

mod expr;
use expr::*;
pub(crate) mod index_access;
pub(crate) mod index_dml;
mod tail;
use tail::*;

thread_local! {
    static CURRENT_CONNECTION: Cell<*const Connection> = const { Cell::new(std::ptr::null()) };
}

pub(crate) fn with_current_connection<T>(conn: &Connection, f: impl FnOnce() -> T) -> T {
    CURRENT_CONNECTION.with(|cell| {
        let prev = cell.replace(conn as *const Connection);
        let result = f();
        cell.set(prev);
        result
    })
}

pub(crate) fn current_connection() -> Option<&'static Connection> {
    CURRENT_CONNECTION.with(|cell| {
        let ptr = cell.get();
        if ptr.is_null() {
            None
        } else {
            // SAFETY: the pointer is installed only for the duration of statement execution.
            unsafe { ptr.as_ref() }
        }
    })
}

pub fn execute_prepared(
    conn: &Connection,
    template: &PreparedTemplate,
    bindings: &[Option<SqlValue>],
) -> Result<ExecutionResult> {
    match &template.kind {
        PreparedKind::Begin(mode) => {
            conn.begin(*mode)?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 0,
            })
        }
        PreparedKind::Commit => {
            conn.commit()?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 0,
            })
        }
        PreparedKind::Rollback => {
            conn.rollback()?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 0,
            })
        }
        PreparedKind::Pragma(plan) => {
            execute_pragma(conn, plan)?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 0,
            })
        }
        PreparedKind::CreateTable(spec) => {
            with_write_tx(conn, |session, tx| {
                let table = conn.engine().create_table(tx, spec.clone())?;
                session.changes += 1;
                session.total_changes += 1;
                session.last_insert_rowid = Some(table.table_id.0 as i64);
                Ok(())
            })?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 1,
            })
        }
        PreparedKind::CreateIndex(spec) => {
            with_write_tx(conn, |session, tx| {
                conn.engine().create_index(tx, spec.clone())?;
                session.changes += 1;
                session.total_changes += 1;
                Ok(())
            })?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 1,
            })
        }
        PreparedKind::DropTable(spec) => {
            with_write_tx(conn, |session, tx| {
                conn.engine().drop_table(tx, spec.clone())?;
                session.changes += 1;
                session.total_changes += 1;
                Ok(())
            })?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 1,
            })
        }
        PreparedKind::DropIndex(spec) => {
            with_write_tx(conn, |session, tx| {
                conn.engine().drop_index(tx, spec.clone())?;
                session.changes += 1;
                session.total_changes += 1;
                Ok(())
            })?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 1,
            })
        }
        PreparedKind::AlterTable(spec) => {
            with_write_tx(conn, |session, tx| {
                conn.engine().alter_table(tx, spec.clone())?;
                session.changes += 1;
                session.total_changes += 1;
                Ok(())
            })?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 1,
            })
        }
        PreparedKind::Insert(plan) => {
            let result = execute_insert(conn, plan, bindings)?;
            if result.affected_rows > 0 {
                conn.with_session(|session| {
                    session.changes += result.affected_rows;
                    session.total_changes += result.affected_rows;
                    Ok(())
                })?;
            }
            Ok(result)
        }
        PreparedKind::Update(plan) => {
            let result = execute_update(conn, plan, bindings)?;
            if result.affected_rows > 0 {
                conn.with_session(|session| {
                    session.changes += result.affected_rows;
                    session.total_changes += result.affected_rows;
                    Ok(())
                })?;
            }
            Ok(result)
        }
        PreparedKind::Delete(plan) => {
            let result = execute_delete(conn, plan, bindings)?;
            if result.affected_rows > 0 {
                conn.with_session(|session| {
                    session.changes += result.affected_rows;
                    session.total_changes += result.affected_rows;
                    Ok(())
                })?;
            }
            Ok(result)
        }
        PreparedKind::Analyze(plan) => {
            analyze_database(conn, plan)?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 0,
            })
        }
        PreparedKind::Explain(plan) => {
            let runtime = execute_explain(conn, plan, bindings)?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Select(runtime),
                affected_rows: 0,
            })
        }
        PreparedKind::Select(plan) => {
            let runtime = execute_select(conn, plan, bindings)?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Select(runtime),
                affected_rows: 0,
            })
        }
    }
}

pub(crate) fn materialize_prepared_rows(
    conn: &Connection,
    template: &PreparedTemplate,
    bindings: &[Option<SqlValue>],
) -> Result<Vec<Vec<SqlValue>>> {
    let result = execute_prepared(conn, template, bindings)?;
    let mut rows = Vec::new();
    if let RuntimeState::Select(mut runtime) = result.runtime {
        let mut current = None;
        loop {
            if step_select_runtime(conn, &mut runtime, bindings, &mut current)? {
                break;
            }
            rows.push(current.take().unwrap_or_default());
        }
    }
    Ok(rows)
}

pub(crate) fn materialize_select_plan_rows(
    conn: &Connection,
    plan: &SelectPlan,
    bindings: &[Option<SqlValue>],
) -> Result<Vec<Vec<SqlValue>>> {
    let template = PreparedTemplate {
        sql: Arc::from("<compound-branch>"),
        schema_epoch: conn.schema_epoch(),
        stats_epoch: conn.stats_epoch().0,
        optimizer_hash: conn.optimizer_hash(),
        param_layout: crate::statement::ParamLayout::default(),
        output_columns: Arc::from([]),
        readonly: true,
        kind: PreparedKind::Select(plan.clone()),
    };
    materialize_prepared_rows(conn, &template, bindings)
}

fn execute_pragma(conn: &Connection, plan: &PragmaPlan) -> Result<()> {
    match plan {
        PragmaPlan::SetForeignKeys(value) => {
            conn.set_foreign_keys(*value);
            Ok(())
        }
        PragmaPlan::SetUserVersion(value) => {
            conn.set_user_version(*value);
            Ok(())
        }
    }
}

fn with_write_tx<T>(
    conn: &Connection,
    f: impl FnOnce(&mut SessionState, &mut Txn) -> Result<T>,
) -> Result<T> {
    conn.with_session(|session| {
        if session.failed {
            return Err(Error::TransactionState(
                "transaction is failed and must roll back",
            ));
        }
        if session.tx.is_some() {
            let mut tx = session.tx.take().expect("checked some");
            let result = f(session, &mut tx);
            session.tx = Some(tx);
            if result.is_err() {
                session.failed = true;
            }
            result
        } else {
            let mut tx = conn.engine().begin(Isolation::Snapshot)?;
            let result = f(session, &mut tx);
            match result {
                Ok(value) => {
                    let commit_result = conn.engine().commit(tx);
                    session.index_undo.clear();
                    session.kernel_unique_guards.clear();
                    session.unique_guards.clear();
                    commit_result?;
                    Ok(value)
                }
                Err(err) => {
                    // SQL DML mutates physical index pages immediately
                    // (insert_tx / delete_mark_tx are not MVCC-aware), so
                    // the kernel rollback alone leaves stale entries
                    // visible. Replay the inverse of every recorded index
                    // op against the still-live tx BEFORE we ask the
                    // engine to abort it; that lets the inverses ride the
                    // same WAL records and the in-memory page state ends
                    // at the inverse's view (no entry visible).
                    replay_index_undo(conn.engine(), session, &mut tx);
                    let _ = conn.engine().rollback(tx);
                    session.kernel_unique_guards.clear();
                    session.unique_guards.clear();
                    Err(err)
                }
            }
        }
    })
}

/// Drain `session.index_undo` and replay each op's inverse against the live
/// `tx`. Errors from the inverse path are best-effort: if undelete_mark or
/// the inverse delete-mark fails we fall through to the normal rollback —
/// the caller's transaction is already on the error path. Each replayed op
/// is dropped from the log regardless of outcome.
pub(crate) fn replay_index_undo(
    engine: &redlinedb_kernel::engine::Engine,
    session: &mut SessionState,
    tx: &mut Txn,
) {
    let ops = std::mem::take(&mut session.index_undo);
    for op in ops.into_iter().rev() {
        // Replay inverses in reverse order: a tx that did insert(A) then
        // delete_mark(A) must un-delete_mark(A) first, then delete-mark(A).
        let _ = op.replay_inverse(engine, tx);
    }
}

pub(crate) fn finalize_runtime(conn: &Connection, runtime: &mut RuntimeState) -> Result<()> {
    match runtime {
        RuntimeState::Select(select) => {
            finish_select_runtime(conn, select)?;
            *runtime = RuntimeState::Done;
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn step_select_runtime(
    conn: &Connection,
    runtime: &mut SelectRuntime,
    bindings: &[Option<SqlValue>],
    current_row: &mut Option<Vec<SqlValue>>,
) -> Result<bool> {
    match &mut runtime.source {
        SelectRuntimeSource::Batched {
            node,
            ctx,
            batch,
            cursor,
        } => {
            if runtime.yielded >= runtime.limit {
                finish_select_runtime(conn, runtime)?;
                *current_row = None;
                return Ok(true);
            }
            if *cursor >= batch.len {
                batch.clear();
                match node.next_batch(ctx, batch)? {
                    ExecState::Yield | ExecState::Done if batch.len > 0 => {
                        *cursor = 0;
                    }
                    ExecState::Done => {
                        finish_select_runtime(conn, runtime)?;
                        *current_row = None;
                        return Ok(true);
                    }
                    ExecState::Yield => {
                        *cursor = 0;
                    }
                }
            }
            if *cursor >= batch.len {
                finish_select_runtime(conn, runtime)?;
                *current_row = None;
                return Ok(true);
            }
            let row = batch
                .row(*cursor)
                .ok_or_else(|| Error::Bind("batch cursor out of range".to_owned()))?;
            *current_row = Some(row);
            *cursor += 1;
            runtime.yielded += 1;
            Ok(false)
        }
        SelectRuntimeSource::SqliteSchema { rows, cursor } => {
            while *cursor < rows.len() {
                let row = SqlRow::SqliteSchema(rows[*cursor].clone());
                *cursor += 1;
                if !selection_passes(&runtime.selection, &row, bindings)? {
                    continue;
                }
                runtime.seen += 1;
                if runtime.seen <= runtime.offset {
                    continue;
                }
                if runtime.yielded >= runtime.limit {
                    finish_select_runtime(conn, runtime)?;
                    *current_row = None;
                    return Ok(true);
                }
                *current_row = Some(project_row(&runtime.projection, &row, bindings)?);
                runtime.yielded += 1;
                return Ok(false);
            }
            finish_select_runtime(conn, runtime)?;
            *current_row = None;
            Ok(true)
        }
        SelectRuntimeSource::StaticRows { rows, cursor } => {
            if *cursor >= rows.len() {
                finish_select_runtime(conn, runtime)?;
                *current_row = None;
                return Ok(true);
            }
            *current_row = Some(rows[*cursor].clone());
            *cursor += 1;
            runtime.yielded += 1;
            Ok(false)
        }
        SelectRuntimeSource::Table {
            table,
            rowids,
            cursor,
        } => {
            let tx = runtime
                .tx
                .as_mut()
                .ok_or(Error::TransactionState("transaction closed"))?;
            while *cursor < rowids.len() {
                let rowid = rowids[*cursor];
                *cursor += 1;
                if let Some(row) = load_table_row_by_rowid(conn.engine(), tx, table, rowid)? {
                    let row = SqlRow::Table(row);
                    if !selection_passes(&runtime.selection, &row, bindings)? {
                        continue;
                    }
                    runtime.seen += 1;
                    if runtime.seen <= runtime.offset {
                        continue;
                    }
                    if runtime.yielded >= runtime.limit {
                        finish_select_runtime(conn, runtime)?;
                        *current_row = None;
                        return Ok(true);
                    }
                    *current_row = Some(project_row(&runtime.projection, &row, bindings)?);
                    runtime.yielded += 1;
                    return Ok(false);
                }
            }
            finish_select_runtime(conn, runtime)?;
            *current_row = None;
            Ok(true)
        }
        SelectRuntimeSource::Empty => {
            if runtime.yielded > 0 || runtime.offset > 0 || runtime.limit == 0 {
                finish_select_runtime(conn, runtime)?;
                *current_row = None;
                return Ok(true);
            }
            if !selection_passes(&runtime.selection, &SqlRow::Empty, bindings)? {
                finish_select_runtime(conn, runtime)?;
                *current_row = None;
                return Ok(true);
            }
            runtime.seen = runtime.seen.saturating_add(1);
            if runtime.seen <= runtime.offset {
                finish_select_runtime(conn, runtime)?;
                *current_row = None;
                return Ok(true);
            }
            *current_row = Some(project_row(&runtime.projection, &SqlRow::Empty, bindings)?);
            runtime.yielded = 1;
            Ok(false)
        }
    }
}

fn finish_select_runtime(conn: &Connection, runtime: &mut SelectRuntime) -> Result<()> {
    if let Some(tx) = runtime.tx.take() {
        if runtime.restore_tx {
            conn.with_session(|session| {
                if session.tx.is_some() {
                    return Err(Error::TransactionState("transaction already active"));
                }
                session.tx = Some(tx);
                Ok(())
            })?;
        } else {
            let _ = conn.engine().rollback(tx);
        }
    }
    runtime.source = SelectRuntimeSource::Empty;
    Ok(())
}

fn begin_select_tx(conn: &Connection) -> Result<(Option<Txn>, bool)> {
    conn.with_session(|session| {
        if let Some(tx) = session.tx.take() {
            return Ok((Some(tx), true));
        }
        let tx = conn.engine().begin(Isolation::Snapshot)?;
        Ok((Some(tx), false))
    })
}

fn execute_select(
    conn: &Connection,
    plan: &crate::statement::SelectPlan,
    bindings: &[Option<SqlValue>],
) -> Result<SelectRuntime> {
    let (mut tx, restore_tx) = begin_select_tx(conn)?;
    let mut memory = QueryMemoryBroker::new(
        conn.query_memory().work_mem_bytes,
        conn.query_memory().max_spill_bytes,
    );
    let result = (|| -> Result<SelectRuntime> {
        let limit = match &plan.limit {
            Some(expr) => scalar_to_usize(&eval_scalar(expr, &RowContext::Empty, bindings)?)?,
            None => usize::MAX,
        };
        let offset = match &plan.offset {
            Some(expr) => scalar_to_usize(&eval_scalar(expr, &RowContext::Empty, bindings)?)?,
            None => 0,
        };

        let source = if plan.group_by.is_empty()
            && !select_requires_aggregation(plan)
            && !plan.distinct
        {
            match &plan.source {
                SelectSource::Table(table) => {
                    if plan.order_by.is_empty() {
                        // Lane C access-path resolution. Order of
                        // preference matches the planner:
                        //   1. rowid PK fast path (already covered by
                        //      `selection_rowid_eq` / RowIdGet).
                        //   2. physical-index probe (point or range).
                        //   3. fallback: full heap scan.
                        let rowids = if let Some(rowid) =
                            selection_rowid_eq(table, &plan.selection, bindings)?
                        {
                            vec![rowid]
                        } else if let Some(matched) =
                            index_access::try_match_index_access(table, &plan.selection, bindings)
                        {
                            let tx = tx.as_mut().expect("tx present");
                            // Conservatism: if the kernel can't honor
                            // the probe (e.g. the index has no live
                            // physical handle yet), fall through to a
                            // table scan rather than returning an empty
                            // result. The planner only emits
                            // IndexPointLookup/IndexRangeScan when the
                            // executor can satisfy them, but stale
                            // schema snapshots still exist as a
                            // possibility.
                            if index_access::open_handle(conn.engine(), &matched.index).is_some() {
                                index_access::execute_index_probe(
                                    conn.engine(),
                                    tx,
                                    table,
                                    &matched.index,
                                    &matched.probe,
                                )?
                            } else {
                                collect_table_rowids(conn.engine(), tx, table)?
                            }
                        } else {
                            let tx = tx.as_mut().expect("tx present");
                            collect_table_rowids(conn.engine(), tx, table)?
                        };
                        SelectRuntimeSource::Table {
                            table: Arc::clone(table),
                            rowids,
                            cursor: 0,
                        }
                    } else {
                        let rows = collect_table_rows(
                            conn.engine(),
                            tx.as_mut().expect("tx present"),
                            table,
                        )?
                        .into_iter()
                        .map(SqlRow::Table)
                        .collect::<Vec<_>>();
                        SelectRuntimeSource::Batched {
                            node: MaterializeNode::new(order_and_project_rows(
                                rows,
                                &plan.selection,
                                &plan.order_by,
                                bindings,
                                &plan.projection,
                                limit,
                                offset,
                                &mut memory,
                            )?),
                            ctx: ExecContext::new(
                                conn.query_memory().work_mem_bytes,
                                conn.query_memory().max_spill_bytes,
                            ),
                            batch: RowBatch::new(Arc::new(RowLayout {
                                columns: Arc::from([]),
                            })),
                            cursor: 0,
                        }
                    }
                }
                SelectSource::Tables(tables) => {
                    let rows =
                        collect_join_rows(conn.engine(), tx.as_mut().expect("tx present"), tables)?;
                    SelectRuntimeSource::Batched {
                        node: MaterializeNode::new(order_and_project_rows(
                            rows,
                            &plan.selection,
                            &plan.order_by,
                            bindings,
                            &plan.projection,
                            limit,
                            offset,
                            &mut memory,
                        )?),
                        ctx: ExecContext::new(
                            conn.query_memory().work_mem_bytes,
                            conn.query_memory().max_spill_bytes,
                        ),
                        batch: RowBatch::new(Arc::new(RowLayout {
                            columns: Arc::from([]),
                        })),
                        cursor: 0,
                    }
                }
                SelectSource::Joined(source) => {
                    let rows = collect_join_source_rows(
                        conn.engine(),
                        tx.as_mut().expect("tx present"),
                        source,
                        bindings,
                    )?;
                    SelectRuntimeSource::Batched {
                        node: MaterializeNode::new(order_and_project_rows(
                            rows,
                            &plan.selection,
                            &plan.order_by,
                            bindings,
                            &plan.projection,
                            limit,
                            offset,
                            &mut memory,
                        )?),
                        ctx: ExecContext::new(
                            conn.query_memory().work_mem_bytes,
                            conn.query_memory().max_spill_bytes,
                        ),
                        batch: RowBatch::new(Arc::new(RowLayout {
                            columns: Arc::from([]),
                        })),
                        cursor: 0,
                    }
                }
                SelectSource::SqliteSchema => {
                    let rows = conn.engine().sqlite_schema();
                    if !plan.order_by.is_empty() {
                        let sqlite_rows = rows
                            .into_iter()
                            .map(SqlRow::SqliteSchema)
                            .collect::<Vec<_>>();
                        SelectRuntimeSource::Batched {
                            node: MaterializeNode::new(order_and_project_rows(
                                sqlite_rows,
                                &plan.selection,
                                &plan.order_by,
                                bindings,
                                &plan.projection,
                                limit,
                                offset,
                                &mut memory,
                            )?),
                            ctx: ExecContext::new(
                                conn.query_memory().work_mem_bytes,
                                conn.query_memory().max_spill_bytes,
                            ),
                            batch: RowBatch::new(Arc::new(RowLayout {
                                columns: Arc::from([]),
                            })),
                            cursor: 0,
                        }
                    } else {
                        SelectRuntimeSource::SqliteSchema { rows, cursor: 0 }
                    }
                }
                SelectSource::StaticRows { rows } => SelectRuntimeSource::StaticRows {
                    rows: Arc::clone(rows),
                    cursor: 0,
                },
                SelectSource::CompoundAll(branches) => {
                    let rows = collect_compound_all_rows(conn, branches, bindings)?
                        .into_iter()
                        .map(SqlRow::Static)
                        .collect::<Vec<_>>();
                    SelectRuntimeSource::Batched {
                        node: MaterializeNode::new(order_and_project_rows(
                            rows,
                            &plan.selection,
                            &plan.order_by,
                            bindings,
                            &plan.projection,
                            limit,
                            offset,
                            &mut memory,
                        )?),
                        ctx: ExecContext::new(
                            conn.query_memory().work_mem_bytes,
                            conn.query_memory().max_spill_bytes,
                        ),
                        batch: RowBatch::new(Arc::new(RowLayout {
                            columns: Arc::from([]),
                        })),
                        cursor: 0,
                    }
                }
                SelectSource::Empty => SelectRuntimeSource::Empty,
            }
        } else {
            let rows = collect_select_rows(
                conn,
                conn.engine(),
                tx.as_mut().expect("tx present"),
                &plan.source,
                bindings,
            )?;
            let rows = execute_grouped_select(plan, rows, bindings, limit, offset, &mut memory)?;
            SelectRuntimeSource::Batched {
                node: MaterializeNode::new(rows),
                ctx: ExecContext::new(
                    conn.query_memory().work_mem_bytes,
                    conn.query_memory().max_spill_bytes,
                ),
                batch: RowBatch::new(Arc::new(RowLayout {
                    columns: Arc::from([]),
                })),
                cursor: 0,
            }
        };

        let runtime_tx = tx.take();

        Ok(SelectRuntime {
            tx: runtime_tx,
            restore_tx,
            source,
            selection: plan.selection.clone(),
            projection: plan.projection.clone(),
            limit,
            offset,
            seen: 0,
            yielded: 0,
            memory,
        })
    })();

    match result {
        Ok(runtime) => Ok(runtime),
        Err(err) => {
            if let Some(tx) = tx.take() {
                if restore_tx {
                    conn.with_session(|session| {
                        session.tx = Some(tx);
                        Ok(())
                    })?;
                } else {
                    let _ = conn.engine().rollback(tx);
                }
            }
            Err(err)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn order_and_project_rows(
    rows: Vec<SqlRow>,
    selection: &Option<Expr>,
    order_by: &[OrderByExpr],
    bindings: &[Option<SqlValue>],
    projection: &[SelectItem],
    limit: usize,
    offset: usize,
    memory: &mut QueryMemoryBroker,
) -> Result<Vec<Vec<SqlValue>>> {
    let mut filtered = Vec::with_capacity(rows.len());
    for row in rows {
        if selection_passes(selection, &row, bindings)? {
            filtered.push(row);
        }
    }
    let memory_bytes = filtered.iter().try_fold(0usize, |acc, row| {
        row.values().map(|values| acc + row_width(&values))
    })?;
    memory.request(memory_bytes)?;
    filtered.sort_by(|left, right| {
        compare_row_ordering(left, right, order_by, bindings).unwrap_or(Ordering::Equal)
    });

    let mut out = Vec::new();
    for row in filtered.into_iter().skip(offset).take(limit) {
        out.push(project_row(projection, &row, bindings)?);
    }
    Ok(out)
}

fn select_requires_aggregation(plan: &crate::statement::SelectPlan) -> bool {
    !plan.group_by.is_empty()
        || plan.having.as_ref().is_some_and(expr_contains_aggregate)
        || plan.projection.iter().any(select_item_contains_aggregate)
}

fn select_item_contains_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => expr_contains_aggregate(expr),
        SelectItem::ExprWithAlias { expr, .. } => expr_contains_aggregate(expr),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
    }
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_ascii_lowercase();
            matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max")
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => {
            expr_contains_aggregate(expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => expr_contains_aggregate(expr),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(expr_contains_aggregate)
                || conditions.iter().any(|when| {
                    expr_contains_aggregate(&when.condition)
                        || expr_contains_aggregate(&when.result)
                })
                || else_result.as_deref().is_some_and(expr_contains_aggregate)
        }
        _ => false,
    }
}

fn collect_select_rows(
    conn: &Connection,
    engine: &Engine,
    tx: &mut Txn,
    source: &SelectSource,
    bindings: &[Option<SqlValue>],
) -> Result<Vec<SqlRow>> {
    match source {
        SelectSource::Table(table) => Ok(collect_table_rows(engine, tx, table)?
            .into_iter()
            .map(SqlRow::Table)
            .collect()),
        SelectSource::Tables(tables) => collect_join_rows(engine, tx, tables),
        SelectSource::Joined(source) => collect_join_source_rows(engine, tx, source, bindings),
        SelectSource::SqliteSchema => Ok(engine
            .sqlite_schema()
            .into_iter()
            .map(SqlRow::SqliteSchema)
            .collect()),
        SelectSource::StaticRows { rows } => Ok(rows.iter().cloned().map(SqlRow::Static).collect()),
        SelectSource::CompoundAll(branches) => {
            Ok(collect_compound_all_rows(conn, branches, bindings)?
                .into_iter()
                .map(SqlRow::Static)
                .collect())
        }
        SelectSource::Empty => Ok(vec![SqlRow::Empty]),
    }
}

fn collect_compound_all_rows(
    conn: &Connection,
    branches: &[SelectPlan],
    bindings: &[Option<SqlValue>],
) -> Result<Vec<Vec<SqlValue>>> {
    let mut out = Vec::new();
    for branch in branches {
        out.extend(materialize_select_plan_rows(conn, branch, bindings)?);
    }
    Ok(out)
}

fn execute_grouped_select(
    plan: &crate::statement::SelectPlan,
    rows: Vec<SqlRow>,
    bindings: &[Option<SqlValue>],
    limit: usize,
    offset: usize,
    memory: &mut QueryMemoryBroker,
) -> Result<Vec<Vec<SqlValue>>> {
    let mut filtered = Vec::new();
    for row in rows {
        if selection_passes(&plan.selection, &row, bindings)? {
            filtered.push(row);
        }
    }

    if plan.distinct && plan.group_by.is_empty() {
        let mut out = Vec::with_capacity(filtered.len());
        let memory_bytes = filtered.iter().try_fold(0usize, |acc, row| {
            row.values().map(|values| acc + row_width(&values))
        })?;
        memory.request(memory_bytes)?;
        for row in filtered {
            let first_context = Some(row.context());
            if let Some(having) = &plan.having
                && !is_truthy(&eval_group_scalar_with_ctx(
                    having,
                    std::slice::from_ref(&row),
                    first_context.as_ref(),
                    bindings,
                )?)
            {
                continue;
            }
            out.push(project_row(&plan.projection, &row, bindings)?);
        }
        out.sort_by(|left, right| compare_rows(left, right));
        out.dedup_by(|left, right| compare_rows(left, right) == Ordering::Equal);
        return Ok(out.into_iter().skip(offset).take(limit).collect());
    }

    let groups = if plan.group_by.is_empty() {
        vec![filtered]
    } else {
        let mut groups: Vec<(Vec<SqlValue>, Vec<SqlRow>)> = Vec::new();
        for row in filtered {
            let key = eval_group_key(&plan.group_by, &row, bindings)?;
            if let Some((_, rows)) = groups.iter_mut().find(|(existing, _)| *existing == key) {
                rows.push(row);
            } else {
                groups.push((key, vec![row]));
            }
        }
        groups.into_iter().map(|(_, rows)| rows).collect()
    };

    let memory_bytes = groups.iter().try_fold(0usize, |acc, group| {
        let group_bytes = group.iter().try_fold(0usize, |group_acc, row| {
            row.values().map(|values| group_acc + row_width(&values))
        })?;
        Ok::<usize, Error>(acc + group_bytes)
    })?;
    memory.request(memory_bytes)?;

    let mut out = Vec::new();
    if plan.distinct {
        for group in groups {
            let first_context = group.first().map(|row| row.context());
            if group.is_empty() && !plan.projection.iter().any(select_item_contains_aggregate) {
                continue;
            }
            if let Some(having) = &plan.having
                && !is_truthy(&eval_group_scalar_with_ctx(
                    having,
                    &group,
                    first_context.as_ref(),
                    bindings,
                )?)
            {
                continue;
            }
            out.push(project_group_row(&plan.projection, &group, bindings)?);
        }
        out.sort_by(|left, right| compare_rows(left, right));
        out.dedup_by(|left, right| compare_rows(left, right) == Ordering::Equal);
        return Ok(out.into_iter().skip(offset).take(limit).collect());
    }

    for group in groups {
        let first_context = group.first().map(|row| row.context());
        if group.is_empty() && !plan.projection.iter().any(select_item_contains_aggregate) {
            continue;
        }
        if let Some(having) = &plan.having
            && !is_truthy(&eval_group_scalar_with_ctx(
                having,
                &group,
                first_context.as_ref(),
                bindings,
            )?)
        {
            continue;
        }
        out.push(project_group_row(&plan.projection, &group, bindings)?);
    }

    if !plan.order_by.is_empty() {
        // Keep grouped queries deterministic enough for now by sorting on projected text.
        out.sort_by(|left, right| compare_rows(left, right));
    }

    Ok(out.into_iter().skip(offset).take(limit).collect())
}

fn eval_group_key(
    group_by: &[Expr],
    row: &SqlRow,
    bindings: &[Option<SqlValue>],
) -> Result<Vec<SqlValue>> {
    let mut out = Vec::with_capacity(group_by.len());
    for expr in group_by {
        out.push(eval_scalar(expr, &row.context(), bindings)?);
    }
    Ok(out)
}

fn project_group_row(
    projection: &[SelectItem],
    group: &[SqlRow],
    bindings: &[Option<SqlValue>],
) -> Result<Vec<SqlValue>> {
    let first = group.first();
    let first_context = first.map(|row| row.context());
    let mut out = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                if let Some(row) = first {
                    out.extend(row.values()?);
                }
            }
            SelectItem::UnnamedExpr(expr) => out.push(eval_group_scalar_with_ctx(
                expr,
                group,
                first_context.as_ref(),
                bindings,
            )?),
            SelectItem::ExprWithAlias { expr, .. } => out.push(eval_group_scalar_with_ctx(
                expr,
                group,
                first_context.as_ref(),
                bindings,
            )?),
        }
    }
    Ok(out)
}

fn eval_group_scalar_with_ctx(
    expr: &Expr,
    group: &[SqlRow],
    first_context: Option<&RowContext<'_>>,
    bindings: &[Option<SqlValue>],
) -> Result<SqlValue> {
    if !expr_contains_aggregate(expr) {
        return match first_context {
            Some(ctx) => eval_scalar(expr, ctx, bindings),
            None => eval_scalar(expr, &RowContext::Empty, bindings),
        };
    }
    match expr {
        Expr::Function(func) => eval_group_function(func, group, bindings),
        Expr::BinaryOp { left, op, right } => {
            let left_value = eval_group_scalar_with_ctx(left, group, first_context, bindings)?;
            let right_value = eval_group_scalar_with_ctx(right, group, first_context, bindings)?;
            Ok(match op {
                BinaryOperator::And => match (truthy_opt(&left_value), truthy_opt(&right_value)) {
                    (Some(false), _) | (_, Some(false)) => SqlValue::Integer(0),
                    (Some(true), Some(true)) => SqlValue::Integer(1),
                    _ => SqlValue::Null,
                },
                BinaryOperator::Or => match (truthy_opt(&left_value), truthy_opt(&right_value)) {
                    (Some(true), _) | (_, Some(true)) => SqlValue::Integer(1),
                    (Some(false), Some(false)) => SqlValue::Integer(0),
                    _ => SqlValue::Null,
                },
                BinaryOperator::Plus => {
                    arithmetic(left_value, right_value, |a, b| a + b, |a, b| a + b)?
                }
                BinaryOperator::Minus => {
                    arithmetic(left_value, right_value, |a, b| a - b, |a, b| a - b)?
                }
                BinaryOperator::Multiply => {
                    arithmetic(left_value, right_value, |a, b| a * b, |a, b| a * b)?
                }
                BinaryOperator::Divide => {
                    arithmetic(left_value, right_value, |a, b| a / b, |a, b| a / b)?
                }
                BinaryOperator::Modulo => match (&left_value, &right_value) {
                    (SqlValue::Integer(a), SqlValue::Integer(b)) => SqlValue::Integer(a % b),
                    _ => return Err(Error::DatatypeMismatch),
                },
                BinaryOperator::Eq => {
                    compare_binary(left_value, right_value, |o| o == Ordering::Equal)?
                }
                BinaryOperator::NotEq | BinaryOperator::Spaceship => {
                    compare_binary(left_value, right_value, |o| o != Ordering::Equal)?
                }
                BinaryOperator::Gt => {
                    compare_binary(left_value, right_value, |o| o == Ordering::Greater)?
                }
                BinaryOperator::GtEq => {
                    compare_binary(left_value, right_value, |o| o != Ordering::Less)?
                }
                BinaryOperator::Lt => {
                    compare_binary(left_value, right_value, |o| o == Ordering::Less)?
                }
                BinaryOperator::LtEq => {
                    compare_binary(left_value, right_value, |o| o != Ordering::Greater)?
                }
                BinaryOperator::StringConcat => SqlValue::Text(Arc::from(format!(
                    "{}{}",
                    value_to_string(&left_value),
                    value_to_string(&right_value)
                ))),
                other => {
                    return Err(Error::UnsupportedSql(format!(
                        "unsupported binary op {other:?}"
                    )));
                }
            })
        }
        Expr::UnaryOp { op, expr } => {
            let value = eval_group_scalar_with_ctx(expr, group, first_context, bindings)?;
            match op {
                UnaryOperator::Not => match truthy_opt(&value) {
                    Some(v) => Ok(SqlValue::Integer(if !v { 1 } else { 0 })),
                    None => Ok(SqlValue::Null),
                },
                UnaryOperator::Minus => negate_value(value),
                UnaryOperator::Plus => Ok(value),
                _ => Err(Error::UnsupportedSql(format!(
                    "unsupported unary op {op:?}"
                ))),
            }
        }
        Expr::Nested(expr) => eval_group_scalar_with_ctx(expr, group, first_context, bindings),
        Expr::Cast {
            expr, data_type, ..
        } => cast_value(
            eval_group_scalar_with_ctx(expr, group, first_context, bindings)?,
            data_type,
        ),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value = eval_group_scalar_with_ctx(expr, group, first_context, bindings)?;
            let low = eval_group_scalar_with_ctx(low, group, first_context, bindings)?;
            let high = eval_group_scalar_with_ctx(high, group, first_context, bindings)?;
            if matches!(value, SqlValue::Null)
                || matches!(low, SqlValue::Null)
                || matches!(high, SqlValue::Null)
            {
                Ok(SqlValue::Null)
            } else {
                let mut ok = compare_values(&value, &low) != Ordering::Less
                    && compare_values(&value, &high) != Ordering::Greater;
                if *negated {
                    ok = !ok;
                }
                Ok(SqlValue::Integer(if ok { 1 } else { 0 }))
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let value = eval_group_scalar_with_ctx(expr, group, first_context, bindings)?;
            if matches!(value, SqlValue::Null) {
                Ok(SqlValue::Null)
            } else {
                let mut found = false;
                let mut saw_null = false;
                for item in list {
                    let candidate =
                        eval_group_scalar_with_ctx(item, group, first_context, bindings)?;
                    match candidate {
                        SqlValue::Null => saw_null = true,
                        _ if compare_values(&value, &candidate) == Ordering::Equal => {
                            found = true;
                            break;
                        }
                        _ => {}
                    }
                }
                let mut ok = found;
                if *negated {
                    ok = !ok;
                }
                if !ok && saw_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Integer(if ok { 1 } else { 0 }))
                }
            }
        }
        Expr::IsNull(expr) => Ok(SqlValue::Integer(
            if matches!(
                eval_group_scalar_with_ctx(expr, group, first_context, bindings)?,
                SqlValue::Null
            ) {
                1
            } else {
                0
            },
        )),
        Expr::IsNotNull(expr) => Ok(SqlValue::Integer(
            if !matches!(
                eval_group_scalar_with_ctx(expr, group, first_context, bindings)?,
                SqlValue::Null
            ) {
                1
            } else {
                0
            },
        )),
        Expr::IsTrue(expr) => Ok(sql_truth_result(eval_group_scalar_with_ctx(
            expr,
            group,
            first_context,
            bindings,
        )?)),
        Expr::IsNotTrue(expr) => Ok(sql_truth_result_not(eval_group_scalar_with_ctx(
            expr,
            group,
            first_context,
            bindings,
        )?)),
        Expr::IsFalse(expr) => Ok(sql_false_result(eval_group_scalar_with_ctx(
            expr,
            group,
            first_context,
            bindings,
        )?)),
        Expr::IsNotFalse(expr) => Ok(sql_false_result_not(eval_group_scalar_with_ctx(
            expr,
            group,
            first_context,
            bindings,
        )?)),
        Expr::IsUnknown(expr) => Ok(SqlValue::Integer(
            if matches!(
                eval_group_scalar_with_ctx(expr, group, first_context, bindings)?,
                SqlValue::Null
            ) {
                1
            } else {
                0
            },
        )),
        Expr::IsNotUnknown(expr) => Ok(SqlValue::Integer(
            if !matches!(
                eval_group_scalar_with_ctx(expr, group, first_context, bindings)?,
                SqlValue::Null
            ) {
                1
            } else {
                0
            },
        )),
        Expr::Case { .. } => Err(Error::UnsupportedSql(
            "aggregate expressions in CASE are not supported".to_owned(),
        )),
        _ => Err(Error::UnsupportedSql(
            "aggregate expressions in this query are not supported".to_owned(),
        )),
    }
}

fn eval_group_function(
    func: &sqlparser::ast::Function,
    group: &[SqlRow],
    bindings: &[Option<SqlValue>],
) -> Result<SqlValue> {
    let name = func.name.to_string().to_ascii_lowercase();
    match name.as_str() {
        "count" => {
            if let FunctionArguments::List(list) = &func.args {
                if list.args.len() == 1
                    && matches!(
                        list.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    )
                {
                    return Ok(SqlValue::Integer(group.len() as i64));
                }
                let mut count = 0i64;
                for row in group {
                    let ctx = row.context();
                    let mut include = true;
                    for arg in &list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg
                            && matches!(eval_scalar(expr, &ctx, bindings)?, SqlValue::Null)
                        {
                            include = false;
                        }
                    }
                    if include {
                        count += 1;
                    }
                }
                Ok(SqlValue::Integer(count))
            } else {
                Ok(SqlValue::Integer(group.len() as i64))
            }
        }
        "sum" => {
            let mut total_i: i64 = 0;
            let mut total_r: f64 = 0.0;
            let mut saw_real = false;
            let mut saw_value = false;
            for row in group {
                let ctx = row.context();
                if let FunctionArguments::List(list) = &func.args
                    && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) =
                        list.args.first()
                {
                    match eval_scalar(expr, &ctx, bindings)? {
                        SqlValue::Null => {}
                        SqlValue::Integer(v) if !saw_real => {
                            total_i += v;
                            saw_value = true;
                        }
                        SqlValue::Integer(v) => {
                            total_r += v as f64;
                            saw_value = true;
                        }
                        SqlValue::Real(v) => {
                            if !saw_real {
                                total_r = total_i as f64;
                                saw_real = true;
                            }
                            total_r += v;
                            saw_value = true;
                        }
                        other => {
                            let real = value_to_string(&other)
                                .trim()
                                .parse::<f64>()
                                .map_err(|_| Error::DatatypeMismatch)?;
                            if !saw_real {
                                total_r = total_i as f64;
                                saw_real = true;
                            }
                            total_r += real;
                            saw_value = true;
                        }
                    }
                }
            }
            if !saw_value {
                Ok(SqlValue::Null)
            } else if saw_real {
                Ok(canonicalize(SqlValue::Real(total_r)))
            } else {
                Ok(SqlValue::Integer(total_i))
            }
        }
        "avg" => {
            let mut count = 0i64;
            let mut sum = 0.0f64;
            for row in group {
                let ctx = row.context();
                if let FunctionArguments::List(list) = &func.args
                    && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) =
                        list.args.first()
                {
                    match eval_scalar(expr, &ctx, bindings)? {
                        SqlValue::Null => {}
                        SqlValue::Integer(v) => {
                            sum += v as f64;
                            count += 1;
                        }
                        SqlValue::Real(v) => {
                            sum += v;
                            count += 1;
                        }
                        other => {
                            sum += value_to_string(&other)
                                .trim()
                                .parse::<f64>()
                                .map_err(|_| Error::DatatypeMismatch)?;
                            count += 1;
                        }
                    }
                }
            }
            if count == 0 {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Real(sum / count as f64))
            }
        }
        "min" | "max" => {
            let mut best: Option<SqlValue> = None;
            for row in group {
                let ctx = row.context();
                if let FunctionArguments::List(list) = &func.args
                    && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) =
                        list.args.first()
                {
                    let value = eval_scalar(expr, &ctx, bindings)?;
                    if matches!(value, SqlValue::Null) {
                        continue;
                    }
                    best = match best {
                        None => Some(value),
                        Some(current) => {
                            let ord = compare_values(&value, &current);
                            if (name == "min" && ord == Ordering::Less)
                                || (name == "max" && ord == Ordering::Greater)
                            {
                                Some(value)
                            } else {
                                Some(current)
                            }
                        }
                    };
                }
            }
            Ok(best.unwrap_or(SqlValue::Null))
        }
        _ => Err(Error::UnsupportedSql(format!(
            "unsupported aggregate function: {name}"
        ))),
    }
}

fn negate_value(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Integer(v) => Ok(SqlValue::Integer(-v)),
        SqlValue::Real(v) => Ok(SqlValue::Real(-v)),
        SqlValue::Null => Ok(SqlValue::Null),
        _ => Err(Error::DatatypeMismatch),
    }
}

fn execute_insert(
    conn: &Connection,
    plan: &crate::statement::InsertPlan,
    bindings: &[Option<SqlValue>],
) -> Result<ExecutionResult> {
    with_write_tx(conn, |session, tx| {
        let mut count = 0usize;
        let mut returning_rows = Vec::new();
        if plan.default_values {
            let mut values = build_default_row(&plan.table)?;
            match insert_row_with_resolution(
                conn,
                session,
                tx,
                &plan.table,
                &mut values,
                plan.conflict.as_ref(),
                bindings,
            )? {
                InsertOutcome::Inserted { rowid, values }
                | InsertOutcome::Updated { rowid, values } => {
                    if let Some(returning) = &plan.returning {
                        returning_rows.push(project_returning_row(
                            &plan.table,
                            &values,
                            rowid,
                            returning,
                            bindings,
                        )?);
                    }
                    return Ok(build_dml_execution_result(
                        1,
                        returning_rows,
                        plan.returning.is_some(),
                    ));
                }
                InsertOutcome::Ignored => {
                    return Ok(build_dml_execution_result(
                        0,
                        returning_rows,
                        plan.returning.is_some(),
                    ));
                }
            }
        }
        for row in &plan.rows {
            if row.len() != plan.columns.len() {
                return Err(Error::Bind(
                    "INSERT row arity does not match column list".to_owned(),
                ));
            }
            let mut values = build_row(&plan.table, row, &plan.columns, bindings)?;
            match insert_row_with_resolution(
                conn,
                session,
                tx,
                &plan.table,
                &mut values,
                plan.conflict.as_ref(),
                bindings,
            )? {
                InsertOutcome::Inserted { rowid, values }
                | InsertOutcome::Updated { rowid, values } => {
                    if let Some(returning) = &plan.returning {
                        returning_rows.push(project_returning_row(
                            &plan.table,
                            &values,
                            rowid,
                            returning,
                            bindings,
                        )?);
                    }
                    count += 1;
                }
                InsertOutcome::Ignored => {}
            }
        }
        Ok(build_dml_execution_result(
            count,
            returning_rows,
            plan.returning.is_some(),
        ))
    })
}
