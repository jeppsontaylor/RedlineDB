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
    AnalyzePlan, ExecutionResult, ExplainPlan, PreparedKind, PreparedTemplate, RuntimeState,
    SelectRuntime, SelectRuntimeSource, SelectSource,
};
use crate::value::{SqlValue, canonicalize, compare_values, is_truthy};

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
        PreparedKind::CreateTable(spec) => {
            with_write_tx(conn, |session, tx| {
                let table = conn.engine().create_table(tx, spec.clone())?;
                session.changes += 1;
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
                Ok(())
            })?;
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: 1,
            })
        }
        PreparedKind::Insert(plan) => {
            let affected = execute_insert(conn, plan, bindings)?;
            if affected > 0 {
                conn.with_session(|session| {
                    session.changes += affected;
                    Ok(())
                })?;
            }
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: affected,
            })
        }
        PreparedKind::Update(plan) => {
            let affected = execute_update(conn, plan, bindings)?;
            if affected > 0 {
                conn.with_session(|session| {
                    session.changes += affected;
                    Ok(())
                })?;
            }
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: affected,
            })
        }
        PreparedKind::Delete(plan) => {
            let affected = execute_delete(conn, plan, bindings)?;
            if affected > 0 {
                conn.with_session(|session| {
                    session.changes += affected;
                    Ok(())
                })?;
            }
            Ok(ExecutionResult {
                runtime: RuntimeState::Done,
                affected_rows: affected,
            })
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
                    session.unique_guards.clear();
                    commit_result?;
                    Ok(value)
                }
                Err(err) => {
                    let _ = conn.engine().rollback(tx);
                    session.unique_guards.clear();
                    Err(err)
                }
            }
        }
    })
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
    _template: &crate::statement::PreparedTemplate,
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

        let source = if plan.group_by.is_empty() && !select_requires_aggregation(plan) {
            match &plan.source {
                SelectSource::Table(table) => {
                    if plan.order_by.is_empty() {
                        let rowids = if let Some(rowid) =
                            selection_rowid_eq(table, &plan.selection, bindings)?
                        {
                            vec![rowid]
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
                SelectSource::Empty => SelectRuntimeSource::Empty,
            }
        } else {
            let rows = collect_select_rows(
                conn.engine(),
                tx.as_mut().expect("tx present"),
                &plan.source,
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
        || plan
            .projection
            .iter()
            .any(|item| select_item_contains_aggregate(item))
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
    engine: &Engine,
    tx: &mut Txn,
    source: &SelectSource,
) -> Result<Vec<SqlRow>> {
    match source {
        SelectSource::Table(table) => Ok(collect_table_rows(engine, tx, table)?
            .into_iter()
            .map(SqlRow::Table)
            .collect()),
        SelectSource::Tables(tables) => collect_join_rows(engine, tx, tables),
        SelectSource::SqliteSchema => Ok(engine
            .sqlite_schema()
            .into_iter()
            .map(SqlRow::SqliteSchema)
            .collect()),
        SelectSource::Empty => Ok(vec![SqlRow::Empty]),
    }
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
    for group in groups {
        let first_context = group.first().map(|row| row.context());
        if group.is_empty() && !plan.projection.iter().any(select_item_contains_aggregate) {
            continue;
        }
        if let Some(having) = &plan.having {
            if !is_truthy(&eval_group_scalar_with_ctx(
                having,
                &group,
                first_context.as_ref(),
                bindings,
            )?) {
                continue;
            }
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
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                            if matches!(eval_scalar(expr, &ctx, bindings)?, SqlValue::Null) {
                                include = false;
                            }
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
                if let FunctionArguments::List(list) = &func.args {
                    if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) =
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
                if let FunctionArguments::List(list) = &func.args {
                    if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) =
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
                if let FunctionArguments::List(list) = &func.args {
                    if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) =
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
) -> Result<usize> {
    with_write_tx(conn, |session, tx| {
        let mut count = 0usize;
        if plan.default_values {
            let mut values = build_default_row(&plan.table)?;
            insert_row(conn, session, tx, &plan.table, &mut values)?;
            return Ok(1);
        }
        for row in &plan.rows {
            if row.len() != plan.columns.len() {
                return Err(Error::Bind(
                    "INSERT row arity does not match column list".to_owned(),
                ));
            }
            let mut values = build_row(&plan.table, row, &plan.columns, bindings)?;
            insert_row(conn, session, tx, &plan.table, &mut values)?;
            count += 1;
        }
        Ok(count)
    })
}

fn analyze_database(conn: &Connection, plan: &AnalyzePlan) -> Result<()> {
    let schema = conn.engine().schema_snapshot();
    let mut tx = conn.engine().begin(Isolation::Snapshot)?;
    let result = (|| -> Result<()> {
        let current = conn.stats_snapshot();
        let mut next = StatsSnapshot::empty(StatsEpoch(current.epoch.0.saturating_add(1)));
        next.tables = current.tables.clone();
        next.columns = current.columns.clone();
        next.indexes = current.indexes.clone();

        let tables = match &plan.table {
            Some(table) => vec![Arc::clone(table)],
            None => schema.tables.iter().cloned().collect::<Vec<_>>(),
        };

        for table in tables {
            let rows = collect_table_rows(conn.engine(), &mut tx, &table)?
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>();
            let table_stats = build_table_stats(conn, &table, &rows)?;
            next.tables.insert(table.table_id, table_stats);

            let sample = sample_rows(conn.stats_config(), &rows);
            for (ordinal, column) in table.columns.iter().enumerate() {
                let stats = build_column_stats(conn.stats_config(), &sample, ordinal);
                next.columns
                    .insert((table.table_id, column.column_id), stats);
            }
            for index in &table.indexes {
                let stats = build_index_stats(conn.stats_config(), &sample, index);
                next.indexes.insert(index.index_id, stats);
            }
        }

        conn.publish_stats(Arc::new(next))
    })();
    let _ = conn.engine().rollback(tx);
    result
}

fn execute_explain(
    conn: &Connection,
    plan: &ExplainPlan,
    bindings: &[Option<SqlValue>],
) -> Result<SelectRuntime> {
    let rows = if plan.analyze {
        let start = Instant::now();
        let mut result = execute_prepared(conn, &plan.inner, bindings)?;
        let mut actual_rows = result.affected_rows;
        let mut loops = 0usize;
        if let RuntimeState::Select(runtime) = &mut result.runtime {
            let mut current_row = None;
            loop {
                loops += 1;
                if step_select_runtime(conn, runtime, &plan.inner, bindings, &mut current_row)? {
                    break;
                }
                actual_rows += 1;
            }
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let (peak_memory_bytes, spill_bytes) = match &result.runtime {
            RuntimeState::Select(runtime) => {
                (runtime.memory.used_bytes, runtime.memory.spilled_bytes)
            }
            RuntimeState::Done | RuntimeState::Idle => (0, 0),
        };
        planner::explain_rows(
            conn,
            &plan.inner.kind,
            bindings,
            Some(ExplainMetrics {
                actual_rows: Some(actual_rows),
                loops: Some(loops),
                elapsed_ms: Some(elapsed_ms),
                peak_memory_bytes: Some(peak_memory_bytes),
                spill_bytes: Some(spill_bytes),
            }),
            plan.format,
        )
    } else {
        planner::explain_rows(conn, &plan.inner.kind, bindings, None, plan.format)
    };

    Ok(SelectRuntime {
        tx: None,
        restore_tx: false,
        source: SelectRuntimeSource::Batched {
            node: MaterializeNode::new(rows),
            ctx: ExecContext::new(
                conn.query_memory().work_mem_bytes,
                conn.query_memory().max_spill_bytes,
            ),
            batch: RowBatch::new(Arc::new(RowLayout {
                columns: Arc::from([]),
            })),
            cursor: 0,
        },
        selection: None,
        projection: Vec::new(),
        limit: usize::MAX,
        offset: 0,
        seen: 0,
        yielded: 0,
        memory: QueryMemoryBroker::new(
            conn.query_memory().work_mem_bytes,
            conn.query_memory().max_spill_bytes,
        ),
    })
}

fn build_table_stats(
    conn: &Connection,
    table: &TableDef,
    rows: &[Vec<SqlValue>],
) -> Result<TableStats> {
    let current = conn.stats_snapshot();
    let preserved = current.tables.get(&table.table_id).cloned();
    let row_count = rows.len() as u64;
    let avg_row_bytes = if rows.is_empty() {
        0.0
    } else {
        rows.iter().map(|row| row_width(row)).sum::<usize>() as f64 / rows.len() as f64
    };
    Ok(TableStats {
        table_id: table.table_id,
        rel_id: table.relation_id,
        row_count,
        live_row_count: row_count,
        heap_pages: if row_count == 0 {
            0
        } else {
            ((row_count + 63) / 64).max(1)
        },
        avg_row_bytes,
        analyzed_at_csn: preserved
            .as_ref()
            .map(|stats| stats.analyzed_at_csn)
            .unwrap_or(redlinedb_kernel::format::Csn::ZERO),
        data_change_count: preserved.map(|stats| stats.data_change_count).unwrap_or(0),
    })
}

fn build_column_stats(
    cfg: &crate::connection::StatsConfig,
    rows: &[Vec<SqlValue>],
    ordinal: usize,
) -> ColumnStats {
    if rows.is_empty() {
        return ColumnStats {
            null_frac: 1.0,
            ndv: 0.0,
            avg_width: 0.0,
            min: None,
            max: None,
            mcv: Vec::new(),
            histogram: Vec::new(),
        };
    }
    let mut nulls = 0usize;
    let mut widths = 0usize;
    let mut min: Option<SqlValue> = None;
    let mut max: Option<SqlValue> = None;
    let mut counts: HashMap<String, (usize, SqlValue)> = HashMap::new();
    let mut non_null_values = Vec::new();
    for row in rows {
        let value = row.get(ordinal).cloned().unwrap_or(SqlValue::Null);
        if matches!(value, SqlValue::Null) {
            nulls += 1;
            continue;
        }
        widths += row_width_value(&value);
        if min
            .as_ref()
            .map(|current| compare_values(&value, current) == Ordering::Less)
            .unwrap_or(true)
        {
            min = Some(value.clone());
        }
        if max
            .as_ref()
            .map(|current| compare_values(&value, current) == Ordering::Greater)
            .unwrap_or(true)
        {
            max = Some(value.clone());
        }
        non_null_values.push(value.clone());
        let key = stats_value_key(&value);
        let entry = counts.entry(key).or_insert((0, value));
        entry.0 += 1;
    }

    non_null_values.sort_by(compare_values);
    let ndv = counts.len() as f64;
    let mut mcv: Vec<_> = counts
        .into_iter()
        .map(|(_, (count, value))| MostCommonValue {
            value,
            frequency: count as f64 / rows.len() as f64,
        })
        .collect();
    mcv.sort_by(|left, right| {
        right
            .frequency
            .partial_cmp(&left.frequency)
            .unwrap_or(Ordering::Equal)
            .then_with(|| compare_values(&left.value, &right.value))
    });
    mcv.truncate(cfg.mcv_capacity);

    let histogram = build_histogram(cfg, &non_null_values, rows.len());
    ColumnStats {
        null_frac: nulls as f64 / rows.len() as f64,
        ndv,
        avg_width: if non_null_values.is_empty() {
            0.0
        } else {
            widths as f64 / non_null_values.len() as f64
        },
        min,
        max,
        mcv,
        histogram,
    }
}

fn build_index_stats(
    _cfg: &crate::connection::StatsConfig,
    rows: &[Vec<SqlValue>],
    index: &redlinedb_kernel::catalog::IndexDef,
) -> IndexStats {
    let mut distinct_prefix_counts = Vec::new();
    for prefix_len in 1..=index.keys.len() {
        let mut seen = std::collections::BTreeSet::new();
        for row in rows {
            let mut key = String::new();
            for key_def in index.keys.iter().take(prefix_len) {
                let value = row
                    .get(key_def.ordinal as usize)
                    .cloned()
                    .unwrap_or(SqlValue::Null);
                key.push_str(&stats_value_key(&value));
                key.push('|');
            }
            seen.insert(key);
        }
        distinct_prefix_counts.push(seen.len() as f64);
    }
    let avg_key_bytes = if rows.is_empty() {
        0.0
    } else {
        let total = rows
            .iter()
            .map(|row| {
                index
                    .keys
                    .iter()
                    .map(|key_def| {
                        row.get(key_def.ordinal as usize)
                            .map(row_width_value)
                            .unwrap_or(0)
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();
        total as f64 / rows.len() as f64
    };
    IndexStats {
        index_id: index.index_id,
        entries: rows.len() as u64,
        leaf_pages: if rows.is_empty() {
            0
        } else {
            ((rows.len() as u64 + 63) / 64).max(1)
        },
        height: if rows.is_empty() { 0 } else { 1 },
        distinct_prefix_counts,
        avg_key_bytes,
        clustering_factor: if rows.is_empty() { 0.0 } else { 1.0 },
    }
}

fn sample_rows(cfg: &crate::connection::StatsConfig, rows: &[Vec<SqlValue>]) -> Vec<Vec<SqlValue>> {
    if rows.len() <= cfg.exact_analyze_row_threshold {
        return rows.to_vec();
    }
    let mut sample = rows
        .iter()
        .cloned()
        .map(|row| (stable_row_score(&row), row))
        .collect::<Vec<_>>();
    sample.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| compare_rows(&left.1, &right.1))
    });
    sample.truncate(cfg.sample_rows.min(sample.len()));
    sample.into_iter().map(|(_, row)| row).collect()
}

fn stable_row_score(row: &[SqlValue]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for value in row {
        stats_value_key(value).hash(&mut hasher);
    }
    hasher.finish()
}

fn build_histogram(
    cfg: &crate::connection::StatsConfig,
    values: &[SqlValue],
    total_rows: usize,
) -> Vec<HistogramBucket> {
    if values.is_empty() || cfg.histogram_buckets == 0 {
        return Vec::new();
    }
    let bucket_count = cfg.histogram_buckets.min(values.len()).max(1);
    let mut buckets = Vec::with_capacity(bucket_count);
    let chunk = (values.len() + bucket_count - 1) / bucket_count;
    let mut start = 0usize;
    while start < values.len() {
        let end = (start + chunk).min(values.len());
        buckets.push(HistogramBucket {
            lower: Some(values[start].clone()),
            upper: Some(values[end - 1].clone()),
            frequency: (end - start) as f64 / total_rows as f64,
        });
        start = end;
    }
    buckets
}

fn compare_rows(left: &[SqlValue], right: &[SqlValue]) -> Ordering {
    for (l, r) in left.iter().zip(right.iter()) {
        let ord = compare_values(l, r);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    left.len().cmp(&right.len())
}

fn row_width(row: &[SqlValue]) -> usize {
    row.iter().map(row_width_value).sum()
}

fn row_width_value(value: &SqlValue) -> usize {
    match value {
        SqlValue::Null => 0,
        SqlValue::Integer(_) | SqlValue::Real(_) => 8,
        SqlValue::Text(v) => v.len(),
        SqlValue::Blob(v) => v.len(),
    }
}

fn stats_value_key(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "n".to_owned(),
        SqlValue::Integer(v) => format!("i:{v}"),
        SqlValue::Real(v) => format!("r:{:016x}", v.to_bits()),
        SqlValue::Text(v) => format!("t:{v}"),
        SqlValue::Blob(v) => {
            let mut out = String::from("b:");
            for byte in v.iter() {
                use std::fmt::Write;
                let _ = write!(&mut out, "{byte:02x}");
            }
            out
        }
    }
}

fn execute_update(
    conn: &Connection,
    plan: &crate::statement::UpdatePlan,
    bindings: &[Option<SqlValue>],
) -> Result<usize> {
    with_write_tx(conn, |session, tx| {
        let rows = collect_table_rows(conn.engine(), tx, &plan.table)?;
        let mut count = 0usize;
        for row in rows {
            if !selection_passes(&plan.selection, &SqlRow::Table(row.clone()), bindings)? {
                continue;
            }
            let mut values = row.values.clone();
            for (ordinal, expr) in &plan.assignments {
                if *ordinal >= values.len() {
                    return Err(Error::UnknownColumn(format!("ordinal {ordinal}")));
                }
                values[*ordinal] = eval_scalar(expr, &RowContext::Table(&row), bindings)?;
            }
            values = apply_row_affinity(&plan.table, values)?;
            let new_rowid =
                choose_rowid_for_update(conn.engine(), &plan.table, &values, row.rowid)?;
            if let Some(alias) = plan.table.rowid_alias_column {
                if let Some(slot) = values.get_mut(alias as usize) {
                    if matches!(slot, SqlValue::Null) {
                        *slot = SqlValue::Integer(new_rowid.0 as i64);
                    }
                }
            }
            apply_constraints(&plan.table, &values)?;
            ensure_unique_constraints(conn, session, tx, &plan.table, &values, Some(row.rowid))?;
            let payload = encode_sql_row(plan.table.table_id.0, &values)?;
            if new_rowid == row.rowid {
                conn.engine().update_for_relation(
                    tx,
                    plan.table.relation_id,
                    row.rowid,
                    payload,
                )?;
            } else {
                conn.engine()
                    .delete_for_relation(tx, plan.table.relation_id, row.rowid)?;
                conn.engine().insert_for_relation(
                    tx,
                    plan.table.relation_id,
                    new_rowid,
                    payload,
                )?;
            }
            count += 1;
        }
        Ok(count)
    })
}

fn execute_delete(
    conn: &Connection,
    plan: &crate::statement::DeletePlan,
    bindings: &[Option<SqlValue>],
) -> Result<usize> {
    with_write_tx(conn, |_, tx| {
        let rows = collect_table_rows(conn.engine(), tx, &plan.table)?;
        let mut count = 0usize;
        for row in rows {
            if !selection_passes(&plan.selection, &SqlRow::Table(row.clone()), bindings)? {
                continue;
            }
            conn.engine()
                .delete_for_relation(tx, plan.table.relation_id, row.rowid)?;
            count += 1;
        }
        Ok(count)
    })
}

fn insert_row(
    conn: &Connection,
    session: &mut SessionState,
    tx: &mut Txn,
    table: &Arc<TableDef>,
    values: &mut Vec<SqlValue>,
) -> Result<RowId> {
    values.resize(table.columns.len(), SqlValue::Null);
    *values = apply_row_affinity(table, std::mem::take(values))?;
    let rowid = choose_rowid_for_insert(conn.engine(), table, values)?;
    apply_constraints(table, values)?;
    ensure_unique_constraints(conn, session, tx, table, values, None)?;
    let payload = encode_sql_row(table.table_id.0, values)?;
    conn.engine()
        .insert_for_relation(tx, table.relation_id, rowid, payload)?;
    session.last_insert_rowid = Some(rowid.0 as i64);
    Ok(rowid)
}

fn build_row(
    table: &Arc<TableDef>,
    row: &[Expr],
    columns: &[usize],
    bindings: &[Option<SqlValue>],
) -> Result<Vec<SqlValue>> {
    let mut values = vec![SqlValue::Null; table.columns.len()];
    for (ordinal, expr) in columns.iter().copied().zip(row.iter()) {
        values[ordinal] = eval_scalar(expr, &RowContext::Empty, bindings)?;
    }
    build_default_values(table, values)
}

fn build_default_row(table: &Arc<TableDef>) -> Result<Vec<SqlValue>> {
    build_default_values(table, vec![SqlValue::Null; table.columns.len()])
}

fn build_default_values(table: &Arc<TableDef>, mut values: Vec<SqlValue>) -> Result<Vec<SqlValue>> {
    for (idx, column) in table.columns.iter().enumerate() {
        if matches!(values[idx], SqlValue::Null) {
            if let Some(default) = &column.default_value {
                values[idx] = default.clone();
            }
        }
    }
    apply_row_affinity(table, values)
}

fn apply_row_affinity(table: &TableDef, values: Vec<SqlValue>) -> Result<Vec<SqlValue>> {
    let mut out = values;
    for (idx, column) in table.columns.iter().enumerate() {
        out[idx] = apply_affinity(out[idx].clone(), column.affinity)
            .map_err(|_| Error::DatatypeMismatch)?;
    }
    Ok(out)
}

fn apply_constraints(table: &TableDef, values: &[SqlValue]) -> Result<()> {
    let mut scratch = EvalScratch::default();
    for (idx, column) in table.columns.iter().enumerate() {
        let value = values
            .get(idx)
            .ok_or_else(|| Error::UnknownColumn(column.name.to_string()))?;
        if column.not_null && matches!(value, SqlValue::Null) {
            return Err(Error::ConstraintViolation(format!(
                "NOT NULL constraint failed: {}.{}",
                table.name, column.name
            )));
        }
    }

    for check in &table.checks {
        let row = TableRowSource { values };
        let result = eval_expr(&check.expr, &row, &mut scratch).map_err(|_| {
            Error::ConstraintViolation(format!("CHECK constraint failed: {}", table.name))
        })?;
        if matches!(result, SqlValue::Null) || is_truthy(&result) {
            continue;
        }
        return Err(Error::ConstraintViolation(format!(
            "CHECK constraint failed: {}",
            table.name
        )));
    }
    Ok(())
}

fn ensure_unique_constraints(
    conn: &Connection,
    session: &mut SessionState,
    tx: &mut Txn,
    table: &Arc<TableDef>,
    values: &[SqlValue],
    skip_rowid: Option<RowId>,
) -> Result<()> {
    let rows = collect_table_rows(conn.engine(), tx, table)?;
    for constraint in &table.constraints {
        if constraint.kind != ConstraintKind::Unique
            && constraint.kind != ConstraintKind::PrimaryKey
        {
            continue;
        }
        let index = match constraint.index_id {
            Some(index_id) => table
                .indexes
                .iter()
                .find(|index| index.index_id == index_id)
                .ok_or(Error::UnsupportedSql(
                    "constraint references missing index".to_owned(),
                ))?,
            None => continue,
        };
        let key_values = index
            .keys
            .iter()
            .map(|key| {
                values
                    .get(key.ordinal as usize)
                    .cloned()
                    .unwrap_or(SqlValue::Null)
            })
            .collect::<Vec<_>>();
        if key_values
            .iter()
            .any(|value| matches!(value, SqlValue::Null))
        {
            continue;
        }
        let key = unique_key_bytes(table.table_id.0, index.index_id.0, &key_values)?;
        let guard = conn.unique_locks().lock(key, tx.id().0);
        session.unique_guards.push(guard);
        for row in &rows {
            if skip_rowid == Some(row.rowid) {
                continue;
            }
            let other = index
                .keys
                .iter()
                .map(|key| {
                    row.values
                        .get(key.ordinal as usize)
                        .cloned()
                        .unwrap_or(SqlValue::Null)
                })
                .collect::<Vec<_>>();
            if key_values_equal(&key_values, &other) {
                return Err(Error::ConstraintViolation(format!(
                    "UNIQUE constraint failed: {}",
                    table.name
                )));
            }
        }
    }
    Ok(())
}

fn choose_rowid_for_insert(
    engine: &Engine,
    table: &TableDef,
    values: &mut [SqlValue],
) -> Result<RowId> {
    if let Some(alias) = table.rowid_alias_column {
        let slot = alias as usize;
        match values.get(slot).cloned().unwrap_or(SqlValue::Null) {
            SqlValue::Null => {
                let rowid = engine.reserve_row_id();
                values[slot] = SqlValue::Integer(rowid.0 as i64);
                Ok(rowid)
            }
            SqlValue::Integer(v) if v >= 0 => Ok(RowId::new(v as u64)),
            SqlValue::Real(v) if v >= 0.0 && v.fract() == 0.0 => Ok(RowId::new(v as u64)),
            SqlValue::Integer(_) | SqlValue::Real(_) => Err(Error::DatatypeMismatch),
            _ => Err(Error::DatatypeMismatch),
        }
    } else {
        Ok(engine.reserve_row_id())
    }
}

fn choose_rowid_for_update(
    engine: &Engine,
    table: &TableDef,
    values: &[SqlValue],
    current_rowid: RowId,
) -> Result<RowId> {
    if let Some(alias) = table.rowid_alias_column {
        match values
            .get(alias as usize)
            .cloned()
            .unwrap_or(SqlValue::Null)
        {
            SqlValue::Null => Ok(engine.reserve_row_id()),
            SqlValue::Integer(v) if v >= 0 => Ok(RowId::new(v as u64)),
            SqlValue::Real(v) if v >= 0.0 && v.fract() == 0.0 => Ok(RowId::new(v as u64)),
            SqlValue::Integer(_) | SqlValue::Real(_) => Err(Error::DatatypeMismatch),
            _ => Err(Error::DatatypeMismatch),
        }
    } else {
        Ok(current_rowid)
    }
}

fn collect_table_rows(
    engine: &Engine,
    tx: &mut Txn,
    table: &Arc<TableDef>,
) -> Result<Vec<TableRow>> {
    collect_table_rows_with_alias(engine, tx, table, None)
}

fn collect_table_rows_with_alias(
    engine: &Engine,
    tx: &mut Txn,
    table: &Arc<TableDef>,
    alias: Option<Arc<str>>,
) -> Result<Vec<TableRow>> {
    let mut rows = Vec::new();
    let mut rowids = engine.relation_rowids(table.relation_id)?;
    if rowids.is_empty() {
        rowids = engine
            .row_directory_entries()?
            .into_iter()
            .map(|(rowid, _)| rowid)
            .collect();
    }
    for rowid in rowids {
        if let Some(row) = load_table_row_by_rowid(engine, tx, table, rowid)? {
            let mut row = row;
            row.alias = alias.clone();
            rows.push(row);
        }
    }
    Ok(rows)
}

fn collect_join_rows(
    engine: &Engine,
    tx: &mut Txn,
    tables: &[crate::statement::BoundTable],
) -> Result<Vec<SqlRow>> {
    let mut joined: Vec<Vec<TableRow>> = vec![Vec::new()];
    for table in tables {
        let rows = collect_table_rows_with_alias(engine, tx, &table.table, table.alias.clone())?;
        let mut next = Vec::new();
        for prefix in &joined {
            for row in &rows {
                let mut combined = prefix.clone();
                combined.push(row.clone());
                next.push(combined);
            }
        }
        joined = next;
    }
    Ok(joined.into_iter().map(SqlRow::Joined).collect())
}

fn collect_table_rowids(
    engine: &Engine,
    tx: &mut Txn,
    table: &Arc<TableDef>,
) -> Result<Vec<RowId>> {
    let mut rowids = Vec::new();
    let mut scan = engine.relation_rowids(table.relation_id)?;
    if scan.is_empty() {
        scan = engine
            .row_directory_entries()?
            .into_iter()
            .map(|(rowid, _)| rowid)
            .collect();
    }
    for rowid in scan {
        if load_table_row_by_rowid(engine, tx, table, rowid)?.is_some() {
            rowids.push(rowid);
        }
    }
    Ok(rowids)
}

fn load_table_row_by_rowid(
    engine: &Engine,
    tx: &mut Txn,
    table: &Arc<TableDef>,
    rowid: RowId,
) -> Result<Option<TableRow>> {
    if let Some(payload) = engine.get(tx, rowid)? {
        if let Some((table_id, values)) = decode_sql_row(&payload)? {
            if table_id == table.table_id.0 {
                return Ok(Some(TableRow {
                    rowid,
                    values,
                    table: Arc::clone(table),
                    alias: None,
                }));
            }
        }
    }
    Ok(None)
}

fn selection_rowid_eq(
    table: &Arc<TableDef>,
    selection: &Option<Expr>,
    bindings: &[Option<SqlValue>],
) -> Result<Option<RowId>> {
    let Some(expr) = selection else {
        return Ok(None);
    };
    let rowid_col = |name: &str| {
        name.eq_ignore_ascii_case("rowid")
            || name.eq_ignore_ascii_case("_rowid_")
            || name.eq_ignore_ascii_case("oid")
            || table
                .rowid_alias_column
                .and_then(|alias| table.columns.get(alias as usize))
                .is_some_and(|col| col.folded.as_ref().eq_ignore_ascii_case(name))
    };
    let expr_rowid = match expr {
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::Eq) => {
            if let Some(value) = rowid_eq_side(table, left, right, bindings, &rowid_col)? {
                value
            } else if let Some(value) = rowid_eq_side(table, right, left, bindings, &rowid_col)? {
                value
            } else {
                return Ok(None);
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(expr_rowid))
}

fn rowid_eq_side(
    _table: &Arc<TableDef>,
    ident_side: &Expr,
    value_side: &Expr,
    bindings: &[Option<SqlValue>],
    rowid_col: &impl Fn(&str) -> bool,
) -> Result<Option<RowId>> {
    let name = match ident_side {
        Expr::Identifier(ident) if rowid_col(&ident.value) => Some(ident.value.as_str()),
        Expr::CompoundIdentifier(parts) => parts.last().and_then(|ident| {
            if rowid_col(&ident.value) {
                Some(ident.value.as_str())
            } else {
                None
            }
        }),
        _ => None,
    };
    if name.is_none() {
        return Ok(None);
    }
    let value = eval_scalar(value_side, &RowContext::Empty, bindings)?;
    match value {
        SqlValue::Integer(v) if v >= 0 => Ok(Some(RowId::new(v as u64))),
        SqlValue::Real(v) if v >= 0.0 && v.fract() == 0.0 => Ok(Some(RowId::new(v as u64))),
        SqlValue::Null => Ok(None),
        _ => Err(Error::DatatypeMismatch),
    }
}

fn selection_passes(
    selection: &Option<Expr>,
    row: &SqlRow,
    bindings: &[Option<SqlValue>],
) -> Result<bool> {
    match selection {
        Some(expr) => Ok(is_truthy(&eval_scalar(expr, &row.context(), bindings)?)),
        None => Ok(true),
    }
}

fn project_row(
    projection: &[SelectItem],
    row: &SqlRow,
    bindings: &[Option<SqlValue>],
) -> Result<Vec<SqlValue>> {
    if projection.is_empty() {
        return row.values();
    }

    let mut out = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                out.extend(row.values()?);
            }
            SelectItem::UnnamedExpr(expr) => out.push(eval_scalar(expr, &row.context(), bindings)?),
            SelectItem::ExprWithAlias { expr, .. } => {
                out.push(eval_scalar(expr, &row.context(), bindings)?)
            }
        }
    }
    Ok(out)
}

fn compare_row_ordering(
    left: &SqlRow,
    right: &SqlRow,
    order_by: &[OrderByExpr],
    bindings: &[Option<SqlValue>],
) -> Result<Ordering> {
    for order in order_by {
        let left_value = eval_scalar(&order.expr, &left.context(), bindings)?;
        let right_value = eval_scalar(&order.expr, &right.context(), bindings)?;
        let mut ord = compare_values(&left_value, &right_value);
        if matches!(order.options.asc, Some(false)) {
            ord = ord.reverse();
        }
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

fn eval_scalar(
    expr: &Expr,
    row: &RowContext<'_>,
    bindings: &[Option<SqlValue>],
) -> Result<SqlValue> {
    Ok(match expr {
        Expr::Value(v) => match &v.value {
            Value::Null => SqlValue::Null,
            Value::Boolean(v) => SqlValue::Integer(if *v { 1 } else { 0 }),
            Value::Number(n, _) => parse_number(n)?,
            Value::SingleQuotedString(s)
            | Value::DoubleQuotedString(s)
            | Value::EscapedStringLiteral(s)
            | Value::TripleSingleQuotedString(s)
            | Value::TripleDoubleQuotedString(s)
            | Value::UnicodeStringLiteral(s)
            | Value::SingleQuotedRawStringLiteral(s)
            | Value::DoubleQuotedRawStringLiteral(s)
            | Value::TripleSingleQuotedRawStringLiteral(s)
            | Value::TripleDoubleQuotedRawStringLiteral(s) => SqlValue::Text(Arc::from(s.as_str())),
            Value::SingleQuotedByteStringLiteral(s)
            | Value::DoubleQuotedByteStringLiteral(s)
            | Value::TripleSingleQuotedByteStringLiteral(s)
            | Value::TripleDoubleQuotedByteStringLiteral(s) => {
                SqlValue::Blob(Arc::from(s.as_bytes()))
            }
            Value::HexStringLiteral(s) => SqlValue::Blob(hex_string_to_bytes(s)?),
            Value::DollarQuotedString(s) => SqlValue::Text(Arc::from(s.value.as_str())),
            Value::Placeholder(name) => resolve_binding(name, bindings)?,
            other => {
                return Err(Error::UnsupportedSql(format!(
                    "unsupported SQL literal: {other:?}"
                )));
            }
        },
        Expr::Identifier(ident) => lookup_column(row, &ident.value)?,
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [ident] => lookup_column(row, &ident.value)?,
            [qualifier, ident] => lookup_qualified_column(row, &qualifier.value, &ident.value)?,
            _ => {
                return Err(Error::UnsupportedSql(format!(
                    "unsupported identifier: {parts:?}"
                )));
            }
        },
        Expr::Nested(expr) => eval_scalar(expr, row, bindings)?,
        Expr::UnaryOp { op, expr } => {
            let value = eval_scalar(expr, row, bindings)?;
            match op {
                UnaryOperator::Not => match truthy_opt(&value) {
                    Some(v) => SqlValue::Integer(if !v { 1 } else { 0 }),
                    None => SqlValue::Null,
                },
                UnaryOperator::Minus => negate(value)?,
                UnaryOperator::Plus => value,
                _ => {
                    return Err(Error::UnsupportedSql(format!(
                        "unsupported unary op {op:?}"
                    )));
                }
            }
        }
        Expr::BinaryOp { left, op, right } => eval_binary(left, op, right, row, bindings)?,
        Expr::Cast {
            expr, data_type, ..
        } => cast_value(eval_scalar(expr, row, bindings)?, data_type)?,
        Expr::Function(func) => eval_function(func, row, bindings)?,
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any {
                return Err(Error::UnsupportedSql(
                    "LIKE ANY is not supported".to_owned(),
                ));
            }
            let value = eval_scalar(expr, row, bindings)?;
            let pattern = eval_scalar(pattern, row, bindings)?;
            like_result(value, pattern, *negated, escape_char.clone(), true)?
        }
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any {
                return Err(Error::UnsupportedSql(
                    "ILIKE ANY is not supported".to_owned(),
                ));
            }
            let value = eval_scalar(expr, row, bindings)?;
            let pattern = eval_scalar(pattern, row, bindings)?;
            like_result(value, pattern, *negated, escape_char.clone(), true)?
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value = eval_scalar(expr, row, bindings)?;
            let low = eval_scalar(low, row, bindings)?;
            let high = eval_scalar(high, row, bindings)?;
            if matches!(value, SqlValue::Null)
                || matches!(low, SqlValue::Null)
                || matches!(high, SqlValue::Null)
            {
                SqlValue::Null
            } else {
                let mut ok = compare_values(&value, &low) != Ordering::Less
                    && compare_values(&value, &high) != Ordering::Greater;
                if *negated {
                    ok = !ok;
                }
                SqlValue::Integer(if ok { 1 } else { 0 })
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let value = eval_scalar(expr, row, bindings)?;
            if matches!(value, SqlValue::Null) {
                SqlValue::Null
            } else {
                let mut found = false;
                let mut saw_null = false;
                for item in list {
                    let candidate = eval_scalar(item, row, bindings)?;
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
                    SqlValue::Null
                } else {
                    SqlValue::Integer(if ok { 1 } else { 0 })
                }
            }
        }
        Expr::IsNull(expr) => SqlValue::Integer(
            if matches!(eval_scalar(expr, row, bindings)?, SqlValue::Null) {
                1
            } else {
                0
            },
        ),
        Expr::IsNotNull(expr) => SqlValue::Integer(
            if !matches!(eval_scalar(expr, row, bindings)?, SqlValue::Null) {
                1
            } else {
                0
            },
        ),
        Expr::IsTrue(expr) => sql_truth_result(eval_scalar(expr, row, bindings)?),
        Expr::IsNotTrue(expr) => sql_truth_result_not(eval_scalar(expr, row, bindings)?),
        Expr::IsFalse(expr) => sql_false_result(eval_scalar(expr, row, bindings)?),
        Expr::IsNotFalse(expr) => sql_false_result_not(eval_scalar(expr, row, bindings)?),
        Expr::IsUnknown(expr) => SqlValue::Integer(
            if matches!(eval_scalar(expr, row, bindings)?, SqlValue::Null) {
                1
            } else {
                0
            },
        ),
        Expr::IsNotUnknown(expr) => SqlValue::Integer(
            if !matches!(eval_scalar(expr, row, bindings)?, SqlValue::Null) {
                1
            } else {
                0
            },
        ),
        Expr::IsDistinctFrom(left, right) => {
            let left = eval_scalar(left, row, bindings)?;
            let right = eval_scalar(right, row, bindings)?;
            SqlValue::Integer(if is_distinct(&left, &right) { 1 } else { 0 })
        }
        Expr::IsNotDistinctFrom(left, right) => {
            let left = eval_scalar(left, row, bindings)?;
            let right = eval_scalar(right, row, bindings)?;
            SqlValue::Integer(if !is_distinct(&left, &right) { 1 } else { 0 })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => eval_case(
            operand.as_deref(),
            conditions,
            else_result.as_deref(),
            row,
            bindings,
        )?,
        other => {
            return Err(Error::UnsupportedSql(format!(
                "unsupported expression: {other:?}"
            )));
        }
    })
}

fn truthy_opt(value: &SqlValue) -> Option<bool> {
    match value {
        SqlValue::Null => None,
        _ => Some(is_truthy(value)),
    }
}

fn eval_binary(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    row: &RowContext<'_>,
    bindings: &[Option<SqlValue>],
) -> Result<SqlValue> {
    let left_value = eval_scalar(left, row, bindings)?;
    let right_value = eval_scalar(right, row, bindings)?;
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
        BinaryOperator::Plus => arithmetic(left_value, right_value, |a, b| a + b, |a, b| a + b)?,
        BinaryOperator::Minus => arithmetic(left_value, right_value, |a, b| a - b, |a, b| a - b)?,
        BinaryOperator::Multiply => {
            arithmetic(left_value, right_value, |a, b| a * b, |a, b| a * b)?
        }
        BinaryOperator::Divide => arithmetic(left_value, right_value, |a, b| a / b, |a, b| a / b)?,
        BinaryOperator::Modulo => match (&left_value, &right_value) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => SqlValue::Integer(a % b),
            _ => return Err(Error::DatatypeMismatch),
        },
        BinaryOperator::Eq => compare_binary(left_value, right_value, |o| o == Ordering::Equal)?,
        BinaryOperator::NotEq | BinaryOperator::Spaceship => {
            compare_binary(left_value, right_value, |o| o != Ordering::Equal)?
        }
        BinaryOperator::Gt => compare_binary(left_value, right_value, |o| o == Ordering::Greater)?,
        BinaryOperator::GtEq => compare_binary(left_value, right_value, |o| o != Ordering::Less)?,
        BinaryOperator::Lt => compare_binary(left_value, right_value, |o| o == Ordering::Less)?,
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

fn compare_binary(
    left: SqlValue,
    right: SqlValue,
    accept: impl FnOnce(Ordering) -> bool,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    Ok(SqlValue::Integer(
        if accept(compare_values(&left, &right)) {
            1
        } else {
            0
        },
    ))
}

fn sql_truth_result(value: SqlValue) -> SqlValue {
    SqlValue::Integer(if is_truthy(&value) { 1 } else { 0 })
}

fn sql_truth_result_not(value: SqlValue) -> SqlValue {
    SqlValue::Integer(if !is_truthy(&value) { 1 } else { 0 })
}

fn sql_false_result(value: SqlValue) -> SqlValue {
    match value {
        SqlValue::Null => SqlValue::Null,
        other => SqlValue::Integer(if !is_truthy(&other) { 1 } else { 0 }),
    }
}

fn sql_false_result_not(value: SqlValue) -> SqlValue {
    match value {
        SqlValue::Null => SqlValue::Null,
        other => SqlValue::Integer(if is_truthy(&other) { 1 } else { 0 }),
    }
}

fn is_distinct(left: &SqlValue, right: &SqlValue) -> bool {
    matches!(left, SqlValue::Null) != matches!(right, SqlValue::Null)
        || (!matches!(left, SqlValue::Null)
            && !matches!(right, SqlValue::Null)
            && compare_values(left, right) != Ordering::Equal)
}

fn eval_case(
    operand: Option<&Expr>,
    conditions: &[sqlparser::ast::CaseWhen],
    else_result: Option<&Expr>,
    row: &RowContext<'_>,
    bindings: &[Option<SqlValue>],
) -> Result<SqlValue> {
    if let Some(operand) = operand {
        let operand = eval_scalar(operand, row, bindings)?;
        for when in conditions {
            let condition = eval_scalar(&when.condition, row, bindings)?;
            if matches!(condition, SqlValue::Null) {
                continue;
            }
            if compare_values(&operand, &condition) == Ordering::Equal {
                return eval_scalar(&when.result, row, bindings);
            }
        }
    } else {
        for when in conditions {
            let condition = eval_scalar(&when.condition, row, bindings)?;
            if !matches!(condition, SqlValue::Null) && is_truthy(&condition) {
                return eval_scalar(&when.result, row, bindings);
            }
        }
    }
    match else_result {
        Some(expr) => eval_scalar(expr, row, bindings),
        None => Ok(SqlValue::Null),
    }
}

fn like_result(
    value: SqlValue,
    pattern: SqlValue,
    negated: bool,
    escape_char: Option<Value>,
    case_insensitive: bool,
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) || matches!(pattern, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let text = value_to_string(&value);
    let pattern = value_to_string(&pattern);
    let escape = match escape_char {
        Some(Value::SingleQuotedString(s)) if s.chars().count() == 1 => {
            Some(s.chars().next().unwrap())
        }
        Some(Value::DoubleQuotedString(s)) if s.chars().count() == 1 => {
            Some(s.chars().next().unwrap())
        }
        Some(Value::SingleQuotedRawStringLiteral(s)) if s.chars().count() == 1 => {
            Some(s.chars().next().unwrap())
        }
        Some(Value::DoubleQuotedRawStringLiteral(s)) if s.chars().count() == 1 => {
            Some(s.chars().next().unwrap())
        }
        Some(Value::TripleSingleQuotedString(s)) if s.chars().count() == 1 => {
            Some(s.chars().next().unwrap())
        }
        Some(Value::TripleDoubleQuotedString(s)) if s.chars().count() == 1 => {
            Some(s.chars().next().unwrap())
        }
        Some(Value::EscapedStringLiteral(s)) if s.chars().count() == 1 => {
            Some(s.chars().next().unwrap())
        }
        Some(Value::UnicodeStringLiteral(s)) if s.chars().count() == 1 => {
            Some(s.chars().next().unwrap())
        }
        Some(Value::DollarQuotedString(s)) if s.value.chars().count() == 1 => {
            Some(s.value.chars().next().unwrap())
        }
        None => None,
        Some(other) => {
            return Err(Error::UnsupportedSql(format!(
                "unsupported LIKE escape literal: {other:?}"
            )));
        }
    };
    let matched = like_match(&text, &pattern, escape, case_insensitive);
    Ok(SqlValue::Integer(if matched ^ negated { 1 } else { 0 }))
}

fn like_match(text: &str, pattern: &str, escape: Option<char>, case_insensitive: bool) -> bool {
    let text = if case_insensitive {
        text.to_ascii_lowercase()
    } else {
        text.to_owned()
    };
    let pattern = if case_insensitive {
        pattern.to_ascii_lowercase()
    } else {
        pattern.to_owned()
    };
    like_match_inner(
        text.as_bytes(),
        pattern.as_bytes(),
        escape.map(|c| c.to_ascii_lowercase()),
    )
}

fn like_match_inner(text: &[u8], pattern: &[u8], escape: Option<char>) -> bool {
    fn inner(text: &[u8], pattern: &[u8], escape: Option<u8>) -> bool {
        let mut ti = 0usize;
        let mut pi = 0usize;
        while pi < pattern.len() {
            match pattern[pi] {
                b'%' => {
                    pi += 1;
                    if pi == pattern.len() {
                        return true;
                    }
                    while ti <= text.len() {
                        if inner(&text[ti..], &pattern[pi..], escape) {
                            return true;
                        }
                        if ti == text.len() {
                            break;
                        }
                        ti += 1;
                    }
                    return false;
                }
                b'_' => {
                    if ti == text.len() {
                        return false;
                    }
                    ti += 1;
                    pi += 1;
                }
                b if Some(b) == escape => {
                    pi += 1;
                    if pi >= pattern.len() || ti >= text.len() || pattern[pi] != text[ti] {
                        return false;
                    }
                    ti += 1;
                    pi += 1;
                }
                ch => {
                    if ti >= text.len() || text[ti] != ch {
                        return false;
                    }
                    ti += 1;
                    pi += 1;
                }
            }
        }
        ti == text.len()
    }
    inner(text, pattern, escape.map(|c| c as u8))
}

fn glob_result(value: SqlValue, pattern: SqlValue, negated: bool) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) || matches!(pattern, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let text = value_to_string(&value);
    let pattern = value_to_string(&pattern);
    let matched = glob_match(text.as_bytes(), pattern.as_bytes());
    Ok(SqlValue::Integer(if matched ^ negated { 1 } else { 0 }))
}

fn glob_match(text: &[u8], pattern: &[u8]) -> bool {
    fn inner(text: &[u8], pattern: &[u8]) -> bool {
        let mut ti = 0usize;
        let mut pi = 0usize;
        while pi < pattern.len() {
            match pattern[pi] {
                b'*' => {
                    pi += 1;
                    if pi == pattern.len() {
                        return true;
                    }
                    while ti <= text.len() {
                        if inner(&text[ti..], &pattern[pi..]) {
                            return true;
                        }
                        if ti == text.len() {
                            break;
                        }
                        ti += 1;
                    }
                    return false;
                }
                b'?' => {
                    if ti == text.len() {
                        return false;
                    }
                    ti += 1;
                    pi += 1;
                }
                ch => {
                    if ti >= text.len() || text[ti] != ch {
                        return false;
                    }
                    ti += 1;
                    pi += 1;
                }
            }
        }
        ti == text.len()
    }
    inner(text, pattern)
}

fn round_function(values: &[SqlValue]) -> Result<SqlValue> {
    if values.is_empty() {
        return Ok(SqlValue::Null);
    }
    let value = numeric_value(&values[0])?;
    let digits = if values.len() > 1 {
        numeric_value(&values[1])? as i32
    } else {
        0
    };
    let factor = 10f64.powi(digits);
    Ok(canonicalize(SqlValue::Real(
        (value * factor).round() / factor,
    )))
}

fn numeric_value(value: &SqlValue) -> Result<f64> {
    match value {
        SqlValue::Null => Ok(0.0),
        SqlValue::Integer(v) => Ok(*v as f64),
        SqlValue::Real(v) => Ok(*v),
        SqlValue::Text(v) => v.trim().parse::<f64>().map_err(|_| Error::DatatypeMismatch),
        SqlValue::Blob(v) => String::from_utf8_lossy(v)
            .trim()
            .parse::<f64>()
            .map_err(|_| Error::DatatypeMismatch),
    }
}

fn hex_value(value: &SqlValue) -> String {
    let bytes: Vec<u8> = match value {
        SqlValue::Null => Vec::new(),
        SqlValue::Integer(v) => v.to_string().into_bytes(),
        SqlValue::Real(v) => v.to_string().into_bytes(),
        SqlValue::Text(v) => v.as_bytes().to_vec(),
        SqlValue::Blob(v) => v.to_vec(),
    };
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{:02X}", byte);
    }
    out
}

fn quote_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "NULL".to_owned(),
        SqlValue::Integer(v) => v.to_string(),
        SqlValue::Real(v) => v.to_string(),
        SqlValue::Text(v) => format!("'{}'", v.replace('\'', "''")),
        SqlValue::Blob(v) => {
            let mut out = String::from("X'");
            for byte in v.iter() {
                use std::fmt::Write;
                let _ = write!(&mut out, "{:02X}", byte);
            }
            out.push('\'');
            out
        }
    }
}

fn random_i64() -> i64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let state = now
        ^ COUNTER
            .fetch_add(1, AtomicOrdering::Relaxed)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut x = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    (x as i64).wrapping_abs()
}

fn hex_string_to_bytes(input: &str) -> Result<Arc<[u8]>> {
    if input.len() % 2 != 0 {
        return Err(Error::UnsupportedSql(format!(
            "invalid hex string literal: {input}"
        )));
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    for pair in input.as_bytes().chunks_exact(2) {
        let hi = hex_digit(pair[0])?;
        let lo = hex_digit(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(Arc::from(out))
}

fn hex_digit(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(Error::UnsupportedSql(format!(
            "invalid hex digit in blob literal: {}",
            byte as char
        ))),
    }
}

fn eval_function(
    func: &sqlparser::ast::Function,
    row: &RowContext<'_>,
    bindings: &[Option<SqlValue>],
) -> Result<SqlValue> {
    let mut values = Vec::new();
    if let FunctionArguments::List(list) = &func.args {
        for arg in &list.args {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    values.push(eval_scalar(expr, row, bindings)?)
                }
                _ => {
                    return Err(Error::UnsupportedSql(
                        "unsupported function argument".to_owned(),
                    ));
                }
            }
        }
    } else if !matches!(func.args, FunctionArguments::None) {
        return Err(Error::UnsupportedSql(
            "unsupported function call form".to_owned(),
        ));
    }

    let name = func.name.to_string().to_ascii_lowercase();
    match name.as_str() {
        "length" => Ok(SqlValue::Integer(
            value_to_string(values.get(0).unwrap_or(&SqlValue::Null)).len() as i64,
        )),
        "lower" => Ok(SqlValue::Text(Arc::from(
            value_to_string(values.get(0).unwrap_or(&SqlValue::Null)).to_ascii_lowercase(),
        ))),
        "upper" => Ok(SqlValue::Text(Arc::from(
            value_to_string(values.get(0).unwrap_or(&SqlValue::Null)).to_ascii_uppercase(),
        ))),
        "abs" => match values.get(0) {
            Some(SqlValue::Integer(v)) => Ok(SqlValue::Integer(v.abs())),
            Some(SqlValue::Real(v)) => Ok(SqlValue::Real(v.abs())),
            _ => Err(Error::DatatypeMismatch),
        },
        "coalesce" | "ifnull" => {
            for value in values {
                if !matches!(value, SqlValue::Null) {
                    return Ok(value);
                }
            }
            Ok(SqlValue::Null)
        }
        "nullif" => {
            if values.len() != 2 {
                return Err(Error::UnsupportedSql("nullif requires 2 args".to_owned()));
            }
            if compare_values(&values[0], &values[1]) == Ordering::Equal {
                Ok(SqlValue::Null)
            } else {
                Ok(values.remove(0))
            }
        }
        "round" => round_function(&values),
        "hex" => Ok(SqlValue::Text(Arc::from(hex_value(
            values.get(0).unwrap_or(&SqlValue::Null),
        )))),
        "quote" => Ok(SqlValue::Text(Arc::from(quote_value(
            values.get(0).unwrap_or(&SqlValue::Null),
        )))),
        "random" => Ok(SqlValue::Integer(random_i64())),
        "likely" | "unlikely" => Ok(values.into_iter().next().unwrap_or(SqlValue::Null)),
        "likelihood" => Ok(values.into_iter().next().unwrap_or(SqlValue::Null)),
        "glob" => {
            if values.len() < 2 {
                return Err(Error::UnsupportedSql("glob requires 2 args".to_owned()));
            }
            glob_result(values[0].clone(), values[1].clone(), false)
        }
        "typeof" => Ok(SqlValue::Text(Arc::from(match values.get(0) {
            Some(SqlValue::Null) | None => "null",
            Some(SqlValue::Integer(_)) => "integer",
            Some(SqlValue::Real(_)) => "real",
            Some(SqlValue::Text(_)) => "text",
            Some(SqlValue::Blob(_)) => "blob",
        }))),
        _ => Err(Error::UnsupportedSql(format!(
            "unsupported function {name}"
        ))),
    }
}

fn cast_value(value: SqlValue, data_type: &sqlparser::ast::DataType) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let text = value_to_string(&value);
    let type_name = data_type.to_string().to_ascii_lowercase();
    if type_name.contains("int") {
        return Ok(SqlValue::Integer(
            text.trim()
                .parse::<i64>()
                .map_err(|_| Error::DatatypeMismatch)?,
        ));
    }
    if type_name.contains("real") || type_name.contains("floa") || type_name.contains("doub") {
        return Ok(SqlValue::Real(
            text.trim()
                .parse::<f64>()
                .map_err(|_| Error::DatatypeMismatch)?,
        ));
    }
    if type_name.contains("text") || type_name.contains("char") || type_name.contains("clob") {
        return Ok(SqlValue::Text(Arc::from(text)));
    }
    Ok(value)
}

fn negate(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Integer(v) => Ok(SqlValue::Integer(-v)),
        SqlValue::Real(v) => Ok(SqlValue::Real(-v)),
        SqlValue::Null => Ok(SqlValue::Null),
        _ => Err(Error::DatatypeMismatch),
    }
}

fn arithmetic(
    left: SqlValue,
    right: SqlValue,
    int_op: impl FnOnce(i64, i64) -> i64,
    real_op: impl FnOnce(f64, f64) -> f64,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    match (left, right) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => Ok(SqlValue::Integer(int_op(a, b))),
        (SqlValue::Integer(a), SqlValue::Real(b)) => Ok(SqlValue::Real(real_op(a as f64, b))),
        (SqlValue::Real(a), SqlValue::Integer(b)) => Ok(SqlValue::Real(real_op(a, b as f64))),
        (SqlValue::Real(a), SqlValue::Real(b)) => Ok(SqlValue::Real(real_op(a, b))),
        (SqlValue::Text(a), SqlValue::Text(b)) => {
            let a = a
                .trim()
                .parse::<f64>()
                .map_err(|_| Error::DatatypeMismatch)?;
            let b = b
                .trim()
                .parse::<f64>()
                .map_err(|_| Error::DatatypeMismatch)?;
            Ok(SqlValue::Real(real_op(a, b)))
        }
        _ => Err(Error::DatatypeMismatch),
    }
}

fn parse_number(input: &str) -> Result<SqlValue> {
    if let Ok(v) = input.parse::<i64>() {
        return Ok(SqlValue::Integer(v));
    }
    if let Ok(v) = input.parse::<f64>() {
        return Ok(canonicalize(SqlValue::Real(v)));
    }
    Err(Error::Parse(format!("invalid numeric literal {input}")))
}

fn value_to_string(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => String::new(),
        SqlValue::Integer(v) => v.to_string(),
        SqlValue::Real(v) => v.to_string(),
        SqlValue::Text(v) => v.to_string(),
        SqlValue::Blob(v) => String::from_utf8_lossy(v).into_owned(),
    }
}

fn resolve_binding(name: &str, bindings: &[Option<SqlValue>]) -> Result<SqlValue> {
    if let Some(rest) = name.strip_prefix('?') {
        let slot = rest
            .parse::<usize>()
            .map_err(|_| Error::Parse(format!("invalid parameter {name}")))?;
        return Ok(bindings
            .get(slot)
            .and_then(|v| v.clone())
            .unwrap_or(SqlValue::Null));
    }
    Err(Error::Bind(format!("unknown parameter {name}")))
}

fn lookup_column(row: &RowContext<'_>, name: &str) -> Result<SqlValue> {
    match row {
        RowContext::Table(row) => lookup_table_column(row, name),
        RowContext::Joined(rows) => {
            let mut found = None;
            for row in rows.iter() {
                if let Ok(value) = lookup_table_column(row, name) {
                    if found.is_some() {
                        return Err(Error::UnsupportedSql(format!(
                            "ambiguous column name: {name}"
                        )));
                    }
                    found = Some(value);
                }
            }
            found.ok_or_else(|| Error::UnknownColumn(name.to_owned()))
        }
        RowContext::SqliteSchema(row) => match name.to_ascii_lowercase().as_str() {
            "type" => Ok(SqlValue::Text(Arc::from(row.type_name.as_ref()))),
            "name" => Ok(SqlValue::Text(Arc::from(row.name.as_ref()))),
            "tbl_name" => Ok(SqlValue::Text(Arc::from(row.tbl_name.as_ref()))),
            "rootpage" => Ok(SqlValue::Integer(row.rootpage as i64)),
            "sql" => Ok(SqlValue::Text(Arc::from(row.sql.as_ref()))),
            _ => Err(Error::UnknownColumn(name.to_owned())),
        },
        RowContext::Empty => Err(Error::UnknownColumn(name.to_owned())),
    }
}

fn lookup_qualified_column(row: &RowContext<'_>, qualifier: &str, name: &str) -> Result<SqlValue> {
    match row {
        RowContext::Table(row) => {
            if row_matches_qualifier(row, qualifier) {
                lookup_table_column(row, name)
            } else {
                Err(Error::UnknownColumn(format!("{qualifier}.{name}")))
            }
        }
        RowContext::Joined(rows) => {
            let mut found = None;
            for row in rows.iter() {
                if row_matches_qualifier(row, qualifier) {
                    let value = lookup_table_column(row, name)?;
                    if found.is_some() {
                        return Err(Error::UnsupportedSql(format!(
                            "ambiguous column reference: {qualifier}.{name}"
                        )));
                    }
                    found = Some(value);
                }
            }
            found.ok_or_else(|| Error::UnknownColumn(format!("{qualifier}.{name}")))
        }
        RowContext::SqliteSchema(row) => match qualifier.to_ascii_lowercase().as_str() {
            "sqlite_schema" | "sqlite_master" => lookup_schema_column(row, name),
            _ => Err(Error::UnknownColumn(format!("{qualifier}.{name}"))),
        },
        RowContext::Empty => Err(Error::UnknownColumn(format!("{qualifier}.{name}"))),
    }
}

fn row_matches_qualifier(row: &TableRow, qualifier: &str) -> bool {
    if let Some(alias) = &row.alias {
        if alias.as_ref().eq_ignore_ascii_case(qualifier) {
            return true;
        }
    }
    row.table.name.to_string().eq_ignore_ascii_case(qualifier)
}

fn lookup_schema_column(row: &SqliteSchemaRow, name: &str) -> Result<SqlValue> {
    match name.to_ascii_lowercase().as_str() {
        "type" => Ok(SqlValue::Text(Arc::from(row.type_name.as_ref()))),
        "name" => Ok(SqlValue::Text(Arc::from(row.name.as_ref()))),
        "tbl_name" => Ok(SqlValue::Text(Arc::from(row.tbl_name.as_ref()))),
        "rootpage" => Ok(SqlValue::Integer(row.rootpage as i64)),
        "sql" => Ok(SqlValue::Text(Arc::from(row.sql.as_ref()))),
        _ => Err(Error::UnknownColumn(name.to_owned())),
    }
}

fn lookup_table_column(row: &TableRow, name: &str) -> Result<SqlValue> {
    if name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
    {
        return Ok(SqlValue::Integer(row.rowid.0 as i64));
    }
    let idx = row
        .table
        .columns
        .iter()
        .position(|col| col.folded.as_ref().eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::UnknownColumn(name.to_owned()))?;
    Ok(row.values[idx].clone())
}

fn unique_key_bytes(table_id: u64, constraint_id: u64, values: &[SqlValue]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&table_id.to_le_bytes());
    out.extend_from_slice(&constraint_id.to_le_bytes());
    let refs = values.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    encode_record(&refs, &mut out).map_err(|_| Error::DatatypeMismatch)?;
    Ok(out)
}

fn key_values_equal(left: &[SqlValue], right: &[SqlValue]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(a, b)| compare_values(a, b) == Ordering::Equal)
}

fn encode_sql_row(table_id: u64, values: &[SqlValue]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut refs = Vec::with_capacity(values.len() + 1);
    refs.push(ValueRef::Integer(table_id as i64));
    refs.extend(values.iter().map(|value| value.as_ref()));
    encode_record(&refs, &mut out).map_err(|_| Error::DatatypeMismatch)?;
    Ok(out)
}

fn decode_sql_row(bytes: &[u8]) -> Result<Option<(u64, Vec<SqlValue>)>> {
    let record = RecordRef::new(bytes).map_err(|_| Error::DatatypeMismatch)?;
    let mut scratch = RecordScratch::default();
    record
        .decode_into(&mut scratch)
        .map_err(|_| Error::DatatypeMismatch)?;
    let mut values = Vec::new();
    let table_id = match record
        .value_at(&scratch, 0)
        .map_err(|_| Error::DatatypeMismatch)?
    {
        ValueRef::Integer(v) => v as u64,
        _ => return Err(Error::DatatypeMismatch),
    };
    for idx in 1..record.column_count().map_err(|_| Error::DatatypeMismatch)? {
        let value = record
            .value_at(&scratch, idx)
            .map_err(|_| Error::DatatypeMismatch)?;
        values.push(value.to_owned());
    }
    Ok(Some((table_id, values)))
}

fn scalar_to_usize(value: &SqlValue) -> Result<usize> {
    match value {
        SqlValue::Integer(v) => Ok((*v).max(0) as usize),
        SqlValue::Real(v) => Ok((*v).max(0.0) as usize),
        SqlValue::Null => Ok(0),
        _ => Err(Error::DatatypeMismatch),
    }
}

#[derive(Clone)]
struct TableRow {
    rowid: RowId,
    values: Vec<SqlValue>,
    table: Arc<TableDef>,
    alias: Option<Arc<str>>,
}

struct TableRowSource<'a> {
    values: &'a [SqlValue],
}

impl RowValueSource for TableRowSource<'_> {
    fn value_at(&self, col: u16) -> Option<OwnedValue> {
        self.values.get(col as usize).cloned()
    }
}

#[derive(Clone)]
enum SqlRow {
    Table(TableRow),
    Joined(Vec<TableRow>),
    SqliteSchema(SqliteSchemaRow),
    Empty,
}

enum RowContext<'a> {
    Table(&'a TableRow),
    Joined(&'a [TableRow]),
    SqliteSchema(&'a SqliteSchemaRow),
    Empty,
}

impl SqlRow {
    fn context(&self) -> RowContext<'_> {
        match self {
            SqlRow::Table(row) => RowContext::Table(row),
            SqlRow::Joined(rows) => RowContext::Joined(rows),
            SqlRow::SqliteSchema(row) => RowContext::SqliteSchema(row),
            SqlRow::Empty => RowContext::Empty,
        }
    }

    fn values(&self) -> Result<Vec<SqlValue>> {
        match self {
            SqlRow::Table(row) => Ok(row.values.clone()),
            SqlRow::Joined(rows) => Ok(rows
                .iter()
                .flat_map(|row| row.values.clone())
                .collect::<Vec<_>>()),
            SqlRow::SqliteSchema(row) => Ok(vec![
                SqlValue::Text(Arc::from(row.type_name.as_ref())),
                SqlValue::Text(Arc::from(row.name.as_ref())),
                SqlValue::Text(Arc::from(row.tbl_name.as_ref())),
                SqlValue::Integer(row.rootpage as i64),
                SqlValue::Text(Arc::from(row.sql.as_ref())),
            ]),
            SqlRow::Empty => Ok(Vec::new()),
        }
    }
}

#[cfg(any())]
mod legacy_exec {

    fn project_row(
        projection: &[SelectItem],
        row: &SqlRow<'_>,
        bindings: &[Option<SqlValue>],
    ) -> Result<Vec<SqlValue>> {
        if projection.is_empty() {
            return row.values();
        }

        let mut out = Vec::new();
        for item in projection {
            match item {
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                    out.extend(row.values()?);
                }
                SelectItem::UnnamedExpr(expr) => {
                    out.push(eval_scalar(expr, &row.context(), bindings)?)
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    out.push(eval_scalar(expr, &row.context(), bindings)?)
                }
            }
        }
        Ok(out)
    }

    fn compare_row_ordering(
        left: &SqlRow<'_>,
        right: &SqlRow<'_>,
        order_by: &[OrderByExpr],
        bindings: &[Option<SqlValue>],
    ) -> Result<Ordering> {
        for order in order_by {
            let left_value = eval_scalar(&order.expr, &left.context(), bindings)?;
            let right_value = eval_scalar(&order.expr, &right.context(), bindings)?;
            let mut ord = compare_values(&left_value, &right_value);
            if matches!(order.options.asc, Some(false)) {
                ord = ord.reverse();
            }
            if ord != Ordering::Equal {
                return Ok(ord);
            }
        }
        Ok(Ordering::Equal)
    }

    fn eval_scalar(
        expr: &Expr,
        row: &RowContext<'_>,
        bindings: &[Option<SqlValue>],
    ) -> Result<SqlValue> {
        Ok(match expr {
            Expr::Value(v) => match &v.value {
                Value::Null => SqlValue::Null,
                Value::Boolean(v) => SqlValue::Integer(if *v { 1 } else { 0 }),
                Value::Number(n, _) => parse_number(n)?,
                Value::SingleQuotedString(s)
                | Value::DoubleQuotedString(s)
                | Value::EscapedStringLiteral(s)
                | Value::TripleSingleQuotedString(s)
                | Value::TripleDoubleQuotedString(s)
                | Value::UnicodeStringLiteral(s)
                | Value::SingleQuotedByteStringLiteral(s)
                | Value::DoubleQuotedByteStringLiteral(s)
                | Value::TripleSingleQuotedByteStringLiteral(s)
                | Value::TripleDoubleQuotedByteStringLiteral(s)
                | Value::SingleQuotedRawStringLiteral(s)
                | Value::DoubleQuotedRawStringLiteral(s)
                | Value::TripleSingleQuotedRawStringLiteral(s)
                | Value::TripleDoubleQuotedRawStringLiteral(s) => {
                    SqlValue::Text(Arc::from(s.as_str()))
                }
                Value::DollarQuotedString(s) => SqlValue::Text(Arc::from(s.value.as_str())),
                Value::Placeholder(name) => resolve_binding(name, bindings)?,
                other => {
                    return Err(Error::UnsupportedSql(format!(
                        "unsupported SQL literal: {other:?}"
                    )));
                }
            },
            Expr::Identifier(ident) => lookup_column(row, &ident.value)?,
            Expr::CompoundIdentifier(parts) => match parts.as_slice() {
                [ident] => lookup_column(row, &ident.value)?,
                [qualifier, ident] => lookup_qualified_column(row, &qualifier.value, &ident.value)?,
                _ => {
                    return Err(Error::UnsupportedSql(format!(
                        "unsupported identifier: {}",
                        Expr::CompoundIdentifier(parts.clone())
                    )));
                }
            },
            Expr::Nested(expr) => eval_scalar(expr, row, bindings)?,
            Expr::UnaryOp { op, expr } => {
                let value = eval_scalar(expr, row, bindings)?;
                match op {
                    UnaryOperator::Not => match truthy_opt(&value) {
                        Some(v) => SqlValue::Integer(if !v { 1 } else { 0 }),
                        None => SqlValue::Null,
                    },
                    UnaryOperator::Minus => negate(value)?,
                    UnaryOperator::Plus => value,
                    _ => {
                        return Err(Error::UnsupportedSql(format!(
                            "unsupported unary op {op:?}"
                        )));
                    }
                }
            }
            Expr::BinaryOp { left, op, right } => eval_binary(left, op, right, row, bindings)?,
            Expr::Cast {
                expr, data_type, ..
            } => cast_value(eval_scalar(expr, row, bindings)?, data_type)?,
            Expr::Function(func) => eval_function(func, row, bindings)?,
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let value = eval_scalar(expr, row, bindings)?;
                let low = eval_scalar(low, row, bindings)?;
                let high = eval_scalar(high, row, bindings)?;
                if matches!(value, SqlValue::Null)
                    || matches!(low, SqlValue::Null)
                    || matches!(high, SqlValue::Null)
                {
                    SqlValue::Null
                } else {
                    let mut ok = compare_values(&value, &low) != Ordering::Less
                        && compare_values(&value, &high) != Ordering::Greater;
                    if *negated {
                        ok = !ok;
                    }
                    SqlValue::Integer(if ok { 1 } else { 0 })
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let value = eval_scalar(expr, row, bindings)?;
                if matches!(value, SqlValue::Null) {
                    SqlValue::Null
                } else {
                    let mut found = false;
                    let mut saw_null = false;
                    for item in list {
                        let candidate = eval_scalar(item, row, bindings)?;
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
                        SqlValue::Null
                    } else {
                        SqlValue::Integer(if ok { 1 } else { 0 })
                    }
                }
            }
            Expr::IsNull(expr) => SqlValue::Integer(
                if matches!(eval_scalar(expr, row, bindings)?, SqlValue::Null) {
                    1
                } else {
                    0
                },
            ),
            Expr::IsNotNull(expr) => SqlValue::Integer(
                if !matches!(eval_scalar(expr, row, bindings)?, SqlValue::Null) {
                    1
                } else {
                    0
                },
            ),
            other => {
                return Err(Error::UnsupportedSql(format!(
                    "unsupported expression: {other:?}"
                )));
            }
        })
    }

    fn truthy_opt(value: &SqlValue) -> Option<bool> {
        match value {
            SqlValue::Null => None,
            _ => Some(is_truthy(value)),
        }
    }

    fn eval_binary(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        row: &RowContext<'_>,
        bindings: &[Option<SqlValue>],
    ) -> Result<SqlValue> {
        let left_value = eval_scalar(left, row, bindings)?;
        let right_value = eval_scalar(right, row, bindings)?;
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
            BinaryOperator::Lt => compare_binary(left_value, right_value, |o| o == Ordering::Less)?,
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

    fn compare_binary(
        left: SqlValue,
        right: SqlValue,
        accept: impl FnOnce(Ordering) -> bool,
    ) -> Result<SqlValue> {
        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        Ok(SqlValue::Integer(
            if accept(compare_values(&left, &right)) {
                1
            } else {
                0
            },
        ))
    }

    fn eval_function(
        func: &sqlparser::ast::Function,
        row: &RowContext<'_>,
        bindings: &[Option<SqlValue>],
    ) -> Result<SqlValue> {
        let mut values = Vec::new();
        if let FunctionArguments::List(list) = &func.args {
            for arg in &list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        values.push(eval_scalar(expr, row, bindings)?)
                    }
                    _ => {
                        return Err(Error::UnsupportedSql(
                            "unsupported function argument".to_owned(),
                        ));
                    }
                }
            }
        } else if !matches!(func.args, FunctionArguments::None) {
            return Err(Error::UnsupportedSql(
                "unsupported function call form".to_owned(),
            ));
        }

        let name = func.name.to_string().to_ascii_lowercase();
        match name.as_str() {
            "length" => Ok(SqlValue::Integer(
                value_to_string(values.get(0).unwrap_or(&SqlValue::Null)).len() as i64,
            )),
            "lower" => Ok(SqlValue::Text(Arc::from(
                value_to_string(values.get(0).unwrap_or(&SqlValue::Null)).to_ascii_lowercase(),
            ))),
            "upper" => Ok(SqlValue::Text(Arc::from(
                value_to_string(values.get(0).unwrap_or(&SqlValue::Null)).to_ascii_uppercase(),
            ))),
            "abs" => match values.get(0) {
                Some(SqlValue::Integer(v)) => Ok(SqlValue::Integer(v.abs())),
                Some(SqlValue::Real(v)) => Ok(SqlValue::Real(v.abs())),
                _ => Err(Error::DatatypeMismatch),
            },
            "coalesce" | "ifnull" => {
                for value in values {
                    if !matches!(value, SqlValue::Null) {
                        return Ok(value);
                    }
                }
                Ok(SqlValue::Null)
            }
            "nullif" => {
                if values.len() != 2 {
                    return Err(Error::UnsupportedSql("nullif requires 2 args".to_owned()));
                }
                if compare_values(&values[0], &values[1]) == Ordering::Equal {
                    Ok(SqlValue::Null)
                } else {
                    Ok(values.remove(0))
                }
            }
            "typeof" => Ok(SqlValue::Text(Arc::from(match values.get(0) {
                Some(SqlValue::Null) | None => "null",
                Some(SqlValue::Integer(_)) => "integer",
                Some(SqlValue::Real(_)) => "real",
                Some(SqlValue::Text(_)) => "text",
                Some(SqlValue::Blob(_)) => "blob",
            }))),
            _ => Err(Error::UnsupportedSql(format!(
                "unsupported function {name}"
            ))),
        }
    }

    fn cast_value(value: SqlValue, data_type: &sqlparser::ast::DataType) -> Result<SqlValue> {
        if matches!(value, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        let text = value_to_string(&value);
        let type_name = data_type.to_string().to_ascii_lowercase();
        if type_name.contains("int") {
            return Ok(SqlValue::Integer(
                text.trim()
                    .parse::<i64>()
                    .map_err(|_| Error::DatatypeMismatch)?,
            ));
        }
        if type_name.contains("real") || type_name.contains("floa") || type_name.contains("doub") {
            return Ok(SqlValue::Real(
                text.trim()
                    .parse::<f64>()
                    .map_err(|_| Error::DatatypeMismatch)?,
            ));
        }
        if type_name.contains("text") || type_name.contains("char") || type_name.contains("clob") {
            return Ok(SqlValue::Text(Arc::from(text)));
        }
        Ok(value)
    }

    fn negate(value: SqlValue) -> Result<SqlValue> {
        match value {
            SqlValue::Integer(v) => Ok(SqlValue::Integer(-v)),
            SqlValue::Real(v) => Ok(SqlValue::Real(-v)),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err(Error::DatatypeMismatch),
        }
    }

    fn arithmetic(
        left: SqlValue,
        right: SqlValue,
        int_op: impl FnOnce(i64, i64) -> i64,
        real_op: impl FnOnce(f64, f64) -> f64,
    ) -> Result<SqlValue> {
        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        match (left, right) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => Ok(SqlValue::Integer(int_op(a, b))),
            (SqlValue::Integer(a), SqlValue::Real(b)) => {
                Ok(canonicalize(SqlValue::Real(real_op(a as f64, b))))
            }
            (SqlValue::Real(a), SqlValue::Integer(b)) => {
                Ok(canonicalize(SqlValue::Real(real_op(a, b as f64))))
            }
            (SqlValue::Real(a), SqlValue::Real(b)) => {
                Ok(canonicalize(SqlValue::Real(real_op(a, b))))
            }
            (SqlValue::Text(a), SqlValue::Text(b)) => {
                let a = a
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| Error::DatatypeMismatch)?;
                let b = b
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| Error::DatatypeMismatch)?;
                Ok(canonicalize(SqlValue::Real(real_op(a, b))))
            }
            _ => Err(Error::DatatypeMismatch),
        }
    }

    fn parse_number(input: &str) -> Result<SqlValue> {
        if let Ok(v) = input.parse::<i64>() {
            return Ok(SqlValue::Integer(v));
        }
        if let Ok(v) = input.parse::<f64>() {
            return Ok(SqlValue::Real(v));
        }
        Err(Error::Parse(format!("invalid numeric literal {input}")))
    }

    fn value_to_string(value: &SqlValue) -> String {
        match value {
            SqlValue::Null => String::new(),
            SqlValue::Integer(v) => v.to_string(),
            SqlValue::Real(v) => v.to_string(),
            SqlValue::Text(v) => v.to_string(),
            SqlValue::Blob(v) => String::from_utf8_lossy(v).into_owned(),
        }
    }

    fn resolve_binding(name: &str, bindings: &[Option<SqlValue>]) -> Result<SqlValue> {
        if let Some(rest) = name.strip_prefix('?') {
            let slot = rest
                .parse::<usize>()
                .map_err(|_| Error::Parse(format!("invalid parameter {name}")))?;
            return Ok(bindings
                .get(slot)
                .and_then(|v| v.clone())
                .unwrap_or(SqlValue::Null));
        }
        Err(Error::Bind(format!("unknown parameter {name}")))
    }

    fn lookup_column(row: &RowContext<'_>, name: &str) -> Result<SqlValue> {
        match row {
            RowContext::Table(row) => lookup_table_column(row, name),
            RowContext::Joined(rows) => {
                let mut found = None;
                for row in rows {
                    if let Ok(value) = lookup_table_column(row, name) {
                        if found.is_some() {
                            return Err(Error::UnsupportedSql(format!(
                                "ambiguous column name: {name}"
                            )));
                        }
                        found = Some(value);
                    }
                }
                found.ok_or_else(|| Error::UnknownColumn(name.to_owned()))
            }
            RowContext::SqliteSchema(row) => match name.to_ascii_lowercase().as_str() {
                "type" => Ok(SqlValue::Text(Arc::from(row.type_name.as_ref()))),
                "name" => Ok(SqlValue::Text(Arc::from(row.name.as_ref()))),
                "tbl_name" => Ok(SqlValue::Text(Arc::from(row.tbl_name.as_ref()))),
                "rootpage" => Ok(SqlValue::Integer(row.rootpage as i64)),
                "sql" => Ok(SqlValue::Text(Arc::from(row.sql.as_ref()))),
                _ => Err(Error::UnknownColumn(name.to_owned())),
            },
            RowContext::Empty => Err(Error::UnknownColumn(name.to_owned())),
        }
    }

    fn lookup_table_column(row: &TableRow, name: &str) -> Result<SqlValue> {
        if name.eq_ignore_ascii_case("rowid")
            || name.eq_ignore_ascii_case("_rowid_")
            || name.eq_ignore_ascii_case("oid")
        {
            return Ok(SqlValue::Integer(row.rowid.0 as i64));
        }
        let idx = row
            .table
            .columns
            .iter()
            .position(|col| col.folded.as_ref().eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::UnknownColumn(name.to_owned()))?;
        Ok(row.values[idx].clone())
    }

    fn unique_key_bytes(table_id: u64, constraint_id: u64, values: &[SqlValue]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&table_id.to_le_bytes());
        out.extend_from_slice(&constraint_id.to_le_bytes());
        let refs = values.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
        encode_record(&refs, &mut out).map_err(|_| Error::DatatypeMismatch)?;
        Ok(out)
    }

    fn key_values_equal(left: &[SqlValue], right: &[SqlValue]) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right.iter())
                .all(|(a, b)| compare_values(a, b) == Ordering::Equal)
    }

    fn constraint_key_columns(table: &TableDef, constraint: &ConstraintDef) -> Result<Vec<usize>> {
        if let Some(index_id) = constraint.index_id {
            if let Some(index) = table
                .indexes
                .iter()
                .find(|index| index.index_id == index_id)
            {
                return Ok(index.keys.iter().map(|key| key.ordinal as usize).collect());
            }
        }
        if let Some(column_id) = constraint.column_id {
            let idx = table
                .columns
                .iter()
                .position(|column| column.column_id == column_id)
                .ok_or_else(|| Error::UnknownColumn("constraint column".to_owned()))?;
            return Ok(vec![idx]);
        }
        Err(Error::UnsupportedSql(
            "unsupported constraint shape".to_owned(),
        ))
    }

    fn encode_sql_row(table_id: u64, values: &[SqlValue]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(SQL_ROW_MAGIC);
        let mut refs = Vec::with_capacity(values.len() + 1);
        refs.push(ValueRef::Integer(table_id as i64));
        refs.extend(values.iter().map(|value| value.as_ref()));
        encode_record(&refs, &mut out).map_err(|_| Error::DatatypeMismatch)?;
        Ok(out)
    }

    fn decode_sql_row(bytes: &[u8]) -> Result<Option<(u64, Vec<SqlValue>)>> {
        if !bytes.starts_with(SQL_ROW_MAGIC) {
            return Ok(None);
        }
        let record =
            RecordRef::new(&bytes[SQL_ROW_MAGIC.len()..]).map_err(|_| Error::DatatypeMismatch)?;
        let mut scratch = RecordScratch::default();
        record
            .decode_into(&mut scratch)
            .map_err(|_| Error::DatatypeMismatch)?;
        let mut values = Vec::new();
        let table_id = match record
            .value_at(&scratch, 0)
            .map_err(|_| Error::DatatypeMismatch)?
        {
            ValueRef::Integer(v) => v as u64,
            _ => return Err(Error::DatatypeMismatch),
        };
        for idx in 1..record.column_count().map_err(|_| Error::DatatypeMismatch)? {
            let value = record
                .value_at(&scratch, idx)
                .map_err(|_| Error::DatatypeMismatch)?;
            values.push(value.to_owned());
        }
        Ok(Some((table_id, values)))
    }

    fn scalar_to_usize(value: &SqlValue) -> Result<usize> {
        match value {
            SqlValue::Integer(v) => Ok((*v).max(0) as usize),
            SqlValue::Real(v) => Ok((*v).max(0.0) as usize),
            SqlValue::Null => Ok(0),
            _ => Err(Error::DatatypeMismatch),
        }
    }

    #[derive(Clone)]
    struct TableRow {
        rowid: RowId,
        values: Vec<SqlValue>,
        table: Arc<TableDef>,
        alias: Option<Arc<str>>,
    }

    struct TableRowSource<'a> {
        values: &'a [SqlValue],
    }

    impl RowValueSource for TableRowSource<'_> {
        fn value_at(&self, col: u16) -> Option<OwnedValue> {
            self.values.get(col as usize).cloned()
        }
    }

    #[derive(Clone)]
    enum SqlRow<'a> {
        Table(TableRow),
        Joined(Vec<TableRow>),
        SqliteSchema(SqliteSchemaRow),
        Empty,
        #[allow(dead_code)]
        BorrowedTable(&'a TableRow),
    }

    enum RowContext<'a> {
        Table(&'a TableRow),
        Joined(&'a [TableRow]),
        SqliteSchema(&'a SqliteSchemaRow),
        Empty,
    }

    impl<'a> SqlRow<'a> {
        fn context(&'a self) -> RowContext<'a> {
            match self {
                SqlRow::Table(row) => RowContext::Table(row),
                SqlRow::Joined(rows) => RowContext::Joined(rows),
                SqlRow::SqliteSchema(row) => RowContext::SqliteSchema(row),
                SqlRow::Empty => RowContext::Empty,
                SqlRow::BorrowedTable(row) => RowContext::Table(row),
            }
        }

        fn values(&self) -> Result<Vec<SqlValue>> {
            match self {
                SqlRow::Table(row) => Ok(row.values.clone()),
                SqlRow::Joined(rows) => Ok(rows
                    .iter()
                    .flat_map(|row| row.values.clone())
                    .collect::<Vec<_>>()),
                SqlRow::SqliteSchema(row) => Ok(vec![
                    SqlValue::Text(Arc::from(row.type_name.as_ref())),
                    SqlValue::Text(Arc::from(row.name.as_ref())),
                    SqlValue::Text(Arc::from(row.tbl_name.as_ref())),
                    SqlValue::Integer(row.rootpage as i64),
                    SqlValue::Text(Arc::from(row.sql.as_ref())),
                ]),
                SqlRow::Empty => Ok(Vec::new()),
                SqlRow::BorrowedTable(row) => Ok(row.values.clone()),
            }
        }
    }

    fn collect_table_rows(
        engine: &Engine,
        tx: &mut Txn,
        table: &Arc<TableDef>,
    ) -> Result<Vec<TableRow>> {
        collect_table_rows_with_alias(engine, tx, table, None)
    }

    fn collect_table_rows_with_alias(
        engine: &Engine,
        tx: &mut Txn,
        table: &Arc<TableDef>,
        alias: Option<Arc<str>>,
    ) -> Result<Vec<TableRow>> {
        let mut rows = Vec::new();
        for (rowid, _) in engine.row_directory_entries()? {
            if let Some(payload) = engine.get(tx, rowid)? {
                if let Some((table_id, values)) = decode_sql_row(&payload)? {
                    if table_id == table.table_id.0 {
                        rows.push(TableRow {
                            rowid,
                            values,
                            table: Arc::clone(table),
                            alias: alias.clone(),
                        });
                    }
                }
            }
        }
        Ok(rows)
    }

    fn collect_join_rows(
        engine: &Engine,
        tx: &mut Txn,
        tables: &[crate::statement::BoundTable],
    ) -> Result<Vec<SqlRow<'static>>> {
        let mut joined: Vec<Vec<TableRow>> = vec![Vec::new()];
        for table in tables {
            let rows =
                collect_table_rows_with_alias(engine, tx, &table.table, table.alias.clone())?;
            let mut next = Vec::new();
            for prefix in &joined {
                for row in &rows {
                    let mut combined = prefix.clone();
                    combined.push(row.clone());
                    next.push(combined);
                }
            }
            joined = next;
        }
        Ok(joined.into_iter().map(SqlRow::Joined).collect())
    }

    fn selection_passes(
        selection: &Option<Expr>,
        row: &SqlRow<'_>,
        bindings: &[Option<SqlValue>],
    ) -> Result<bool> {
        match selection {
            Some(expr) => Ok(is_truthy(&eval_scalar(expr, &row.context(), bindings)?)),
            None => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use redlinedb_kernel::catalog::{DbName, QualifiedName, lookup_table};
    use redlinedb_kernel::txn::Isolation;
    use tempfile::tempdir;

    use super::*;
    use crate::statement::Step;

    fn new_connection() -> (tempfile::TempDir, Arc<Connection>) {
        let dir = tempdir().expect("temp dir");
        let path: PathBuf = dir.path().join("sql-exec-test.db");
        let db = crate::Database::create(&path, crate::DbOptions::default()).expect("db");
        let conn = db.connect();
        (dir, conn)
    }

    #[test]
    fn collect_table_rows_sees_inserted_data() {
        let (_dir, conn) = new_connection();
        let mut create = conn
            .prepare("CREATE TABLE t(a INTEGER, b TEXT)")
            .expect("prepare");
        assert_eq!(create.step().expect("create"), Step::Done);
        let mut insert = conn
            .prepare("INSERT INTO t VALUES (1, 'one')")
            .expect("prepare");
        assert_eq!(insert.step().expect("insert"), Step::Done);

        let snapshot = conn.engine().schema_snapshot();
        let table = lookup_table(
            &snapshot,
            &QualifiedName {
                schema: DbName::new("main"),
                name: DbName::new("t"),
            },
        )
        .expect("table");
        let mut tx = conn.engine().begin(Isolation::Snapshot).expect("tx");
        let rows = collect_table_rows(conn.engine(), &mut tx, &table).expect("rows");
        conn.engine().rollback(tx).expect("rollback");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values.len(), 2);
    }
}
