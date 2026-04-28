#![allow(dead_code)]

use std::cmp::Ordering;
use std::fmt::Write as _;
use std::sync::Arc;

use redlinedb_kernel::catalog::{IndexDef, TableDef, TableStats};
use redlinedb_kernel::format::RowId;
use sqlparser::ast::{BinaryOperator, Expr, OrderByExpr, SelectItem};

use crate::connection::{Connection, OptimizerConfig};
use crate::error::{Error, Result};
use crate::statement::{BoundTable, ExplainFormat, PreparedKind, SelectPlan, SelectSource};
use crate::value::SqlValue;

mod helpers;
use helpers::*;

const SEQ_PAGE_COST: f64 = 1.0;
const RANDOM_PAGE_COST: f64 = 4.0;
const CPU_TUPLE_COST: f64 = 0.01;
const CPU_OPERATOR_COST: f64 = 0.0025;
const INDEX_PROBE_STARTUP: f64 = 2.0;
const UNKNOWN_EQ_SELECTIVITY: f64 = 0.10;
const UNKNOWN_RANGE_SELECTIVITY: f64 = 0.33;
const UNKNOWN_PREDICATE_SELECTIVITY: f64 = 0.333;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost {
    pub startup: f64,
    pub total: f64,
    pub rows: f64,
    pub width: f64,
    pub memory_bytes: usize,
    pub spill_bytes: usize,
}

impl Cost {
    pub fn zero() -> Self {
        Self {
            startup: 0.0,
            total: 0.0,
            rows: 0.0,
            width: 0.0,
            memory_bytes: 0,
            spill_bytes: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan(LogicalScan),
    Filter {
        input: Box<LogicalPlan>,
        predicate: String,
    },
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<String>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        kind: JoinKind,
        on: Vec<String>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        aggs: Vec<String>,
    },
    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<String>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<String>,
        offset: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct LogicalScan {
    pub source: String,
    pub access: AccessPath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    NestedLoop,
    IndexNestedLoop,
    Hash,
    Cross,
}

#[derive(Debug, Clone)]
pub enum AccessPath {
    TableScan,
    RowIdGet {
        rowid: RowId,
    },
    IndexPointLookup {
        index: Arc<IndexDef>,
        predicates: Vec<String>,
    },
    IndexRangeScan {
        index: Arc<IndexDef>,
        predicates: Vec<String>,
    },
    CoveringIndexScan {
        index: Arc<IndexDef>,
        predicates: Vec<String>,
        projected_columns: Vec<String>,
    },
    MultiIndexOr {
        inputs: Vec<AccessPath>,
    },
    MultiIndexAnd {
        inputs: Vec<AccessPath>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalKind {
    TableScan,
    RowIdGet,
    IndexScan,
    MultiIndexScan,
    Filter,
    Project,
    NestedLoopJoin,
    IndexNestedLoopJoin,
    HashJoin,
    StreamingAggregate,
    HashAggregate,
    Sort,
    TopN,
    Limit,
    Explain,
    Constant,
}

#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub kind: PhysicalKind,
    pub relation: Option<String>,
    pub index: Option<String>,
    pub estimated_rows: f64,
    pub cost: Cost,
    pub access_predicates: Vec<String>,
    pub residual_predicates: Vec<String>,
    pub output_order: Vec<String>,
    pub projected_columns: Vec<String>,
    pub memory_budget: usize,
    pub actual_rows: Option<usize>,
    pub loops: Option<usize>,
    pub elapsed_ms: Option<f64>,
    pub peak_memory_bytes: Option<usize>,
    pub spill_bytes: Option<usize>,
    pub children: Vec<PhysicalPlan>,
}

impl PhysicalPlan {
    fn new(kind: PhysicalKind) -> Self {
        Self {
            kind,
            relation: None,
            index: None,
            estimated_rows: 0.0,
            cost: Cost::zero(),
            access_predicates: Vec::new(),
            residual_predicates: Vec::new(),
            output_order: Vec::new(),
            projected_columns: Vec::new(),
            memory_budget: 0,
            actual_rows: None,
            loops: None,
            elapsed_ms: None,
            peak_memory_bytes: None,
            spill_bytes: None,
            children: Vec::new(),
        }
    }

    fn leaf(kind: PhysicalKind, relation: Option<String>) -> Self {
        let mut plan = Self::new(kind);
        plan.relation = relation;
        plan
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExplainMetrics {
    pub actual_rows: Option<usize>,
    pub loops: Option<usize>,
    pub elapsed_ms: Option<f64>,
    pub peak_memory_bytes: Option<usize>,
    pub spill_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
struct FlattenedNode {
    id: usize,
    parent: Option<usize>,
    detail: String,
}

pub(crate) fn explain_rows(
    conn: &Connection,
    kind: &PreparedKind,
    bindings: &[Option<SqlValue>],
    metrics: Option<ExplainMetrics>,
    format: ExplainFormat,
) -> Vec<Vec<SqlValue>> {
    let plan = build_plan(conn, kind, bindings, metrics);
    match format {
        ExplainFormat::QueryPlan => flatten_query_plan(&plan)
            .into_iter()
            .map(|node| {
                vec![
                    SqlValue::Integer(node.id as i64),
                    SqlValue::Integer(node.parent.map(|id| id as i64).unwrap_or(0)),
                    SqlValue::Integer(0),
                    SqlValue::Text(Arc::from(node.detail)),
                ]
            })
            .collect(),
        ExplainFormat::Text => vec![vec![SqlValue::Text(Arc::from(render_text(&plan)))]],
        ExplainFormat::Json => vec![vec![SqlValue::Text(Arc::from(render_json(&plan)))]],
    }
}

pub(crate) fn build_plan(
    conn: &Connection,
    kind: &PreparedKind,
    bindings: &[Option<SqlValue>],
    metrics: Option<ExplainMetrics>,
) -> PhysicalPlan {
    let mut plan = match kind {
        PreparedKind::Select(select) => build_select_plan(conn, select, bindings),
        PreparedKind::Insert(plan) => simple_node(
            PhysicalKind::Constant,
            format!("INSERT INTO {}", plan.table.name),
        ),
        PreparedKind::Update(plan) => simple_node(
            PhysicalKind::Constant,
            format!("UPDATE {}", plan.table.name),
        ),
        PreparedKind::Delete(plan) => simple_node(
            PhysicalKind::Constant,
            format!("DELETE FROM {}", plan.table.name),
        ),
        PreparedKind::Analyze(plan) => match &plan.table {
            Some(table) => simple_node(PhysicalKind::Constant, format!("ANALYZE {}", table.name)),
            None => simple_node(PhysicalKind::Constant, "ANALYZE".to_owned()),
        },
        PreparedKind::Explain(plan) => {
            build_plan(conn, &plan.inner.kind, bindings, metrics.clone())
        }
        PreparedKind::Begin(_) => simple_node(PhysicalKind::Constant, "BEGIN".to_owned()),
        PreparedKind::Commit => simple_node(PhysicalKind::Constant, "COMMIT".to_owned()),
        PreparedKind::Rollback => simple_node(PhysicalKind::Constant, "ROLLBACK".to_owned()),
        PreparedKind::CreateTable(_) => {
            simple_node(PhysicalKind::Constant, "CREATE TABLE".to_owned())
        }
        PreparedKind::CreateIndex(_) => {
            simple_node(PhysicalKind::Constant, "CREATE INDEX".to_owned())
        }
        PreparedKind::DropTable(_) => simple_node(PhysicalKind::Constant, "DROP TABLE".to_owned()),
        PreparedKind::DropIndex(_) => simple_node(PhysicalKind::Constant, "DROP INDEX".to_owned()),
    };

    if let Some(metrics) = metrics {
        plan.actual_rows = metrics.actual_rows;
        plan.loops = metrics.loops;
        plan.elapsed_ms = metrics.elapsed_ms;
        plan.peak_memory_bytes = metrics.peak_memory_bytes;
        plan.spill_bytes = metrics.spill_bytes;
    }
    plan
}

fn simple_node(kind: PhysicalKind, detail: String) -> PhysicalPlan {
    let mut node = PhysicalPlan::new(kind);
    node.relation = Some(detail);
    node
}

fn build_select_plan(
    conn: &Connection,
    plan: &SelectPlan,
    bindings: &[Option<SqlValue>],
) -> PhysicalPlan {
    let optimizer = conn.optimizer_config().clone();
    let mut base = match &plan.source {
        SelectSource::Table(table) => build_table_scan_plan(
            conn,
            table,
            &plan.projection,
            &plan.selection,
            &plan.order_by,
            bindings,
            &optimizer,
        ),
        SelectSource::Tables(tables) => {
            build_join_plan(conn, tables, &plan.selection, bindings, &optimizer)
        }
        SelectSource::SqliteSchema => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::TableScan, Some("sqlite_schema".to_owned()));
            node.estimated_rows = conn.engine().sqlite_schema().len() as f64;
            node.cost = estimate_scan_cost(node.estimated_rows, estimate_width_for_schema());
            node.projected_columns = vec![
                "type".into(),
                "name".into(),
                "tbl_name".into(),
                "rootpage".into(),
                "sql".into(),
            ];
            node
        }
        SelectSource::Empty => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::Constant, Some("constant row".to_owned()));
            node.estimated_rows = 1.0;
            node.cost = Cost::zero();
            node
        }
    };

    if requires_aggregate(plan) {
        base = wrap_aggregate(base, plan);
    }

    if !plan.order_by.is_empty() && !output_order_satisfies(&base.output_order, &plan.order_by) {
        base = wrap_ordering(base, plan, bindings, &optimizer);
    }

    if has_projection_exprs(&plan.projection) {
        base = wrap_project(base, plan);
    }

    if plan.limit.is_some() || plan.offset.is_some() {
        base = wrap_limit(base, plan);
    }

    base
}

fn build_table_scan_plan(
    conn: &Connection,
    table: &Arc<TableDef>,
    projection: &[SelectItem],
    selection: &Option<Expr>,
    order_by: &[OrderByExpr],
    bindings: &[Option<SqlValue>],
    optimizer: &OptimizerConfig,
) -> PhysicalPlan {
    let stats = conn.stats_snapshot();
    let table_stats = stats.tables.get(&table.table_id);
    let row_estimate = estimate_table_rows(table_stats);
    let width = estimate_table_width(table_stats, table);
    let rowid = selection_rowid_eq(table, selection, bindings)
        .ok()
        .flatten();
    let access = choose_access_path(
        table,
        projection,
        selection,
        order_by,
        rowid,
        table_stats,
        optimizer,
    );
    let is_covering = matches!(&access, AccessPath::CoveringIndexScan { .. });
    let ordering_satisfied = satisfies_ordering(table, &access, order_by);

    let mut node = match access {
        AccessPath::TableScan => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::TableScan, Some(table.name.to_string()));
            node.estimated_rows = row_estimate;
            node.cost = estimate_scan_cost(row_estimate, width);
            node
        }
        AccessPath::RowIdGet { rowid } => {
            let mut node = PhysicalPlan::leaf(PhysicalKind::RowIdGet, Some(table.name.to_string()));
            node.estimated_rows = 1.0;
            node.cost = Cost {
                startup: INDEX_PROBE_STARTUP,
                total: INDEX_PROBE_STARTUP + CPU_OPERATOR_COST + CPU_TUPLE_COST,
                rows: 1.0,
                width,
                memory_bytes: 0,
                spill_bytes: 0,
            };
            node.access_predicates = vec![format!("rowid = {}", rowid.0)];
            node
        }
        AccessPath::IndexPointLookup { index, predicates } => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::IndexScan, Some(table.name.to_string()));
            node.index = Some(index.name.to_string());
            node.estimated_rows = estimate_eq_rows(table_stats, &index, &predicates);
            node.cost = estimate_index_cost(node.estimated_rows, width, true);
            node.access_predicates = predicates;
            node
        }
        AccessPath::IndexRangeScan { index, predicates } => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::IndexScan, Some(table.name.to_string()));
            node.index = Some(index.name.to_string());
            node.estimated_rows = estimate_range_rows(table_stats, &index, &predicates);
            node.cost = estimate_index_cost(node.estimated_rows, width, false);
            node.access_predicates = predicates;
            node
        }
        AccessPath::CoveringIndexScan {
            index,
            predicates,
            projected_columns,
        } => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::IndexScan, Some(table.name.to_string()));
            node.index = Some(index.name.to_string());
            node.estimated_rows = estimate_index_rows(table_stats, &index);
            node.cost = estimate_index_cost(node.estimated_rows, width, true);
            node.access_predicates = predicates;
            node.projected_columns = projected_columns;
            node
        }
        AccessPath::MultiIndexOr { inputs } | AccessPath::MultiIndexAnd { inputs } => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::MultiIndexScan, Some(table.name.to_string()));
            node.children = inputs
                .into_iter()
                .map(|input| access_plan_to_node(table, input, width))
                .collect();
            node.estimated_rows = node.children.iter().map(|child| child.estimated_rows).sum();
            node.cost = Cost {
                startup: INDEX_PROBE_STARTUP,
                total: node
                    .children
                    .iter()
                    .map(|child| child.cost.total)
                    .sum::<f64>()
                    + CPU_OPERATOR_COST,
                rows: node.estimated_rows,
                width,
                memory_bytes: 0,
                spill_bytes: 0,
            };
            node
        }
    };

    if selection.is_none() {
        node.residual_predicates.clear();
    } else if !node.access_predicates.is_empty() {
        node.residual_predicates
            .push(expr_to_string(selection.as_ref().unwrap()));
    }

    if node.projected_columns.is_empty() {
        node.projected_columns = projected_columns_from_select(table, projection);
    }
    if is_covering {
        node.kind = PhysicalKind::IndexScan;
    }

    if ordering_satisfied {
        node.output_order = order_by
            .iter()
            .map(|item| expr_to_string(&item.expr))
            .collect();
    }

    node
}

fn access_plan_to_node(table: &Arc<TableDef>, access: AccessPath, width: f64) -> PhysicalPlan {
    match access {
        AccessPath::TableScan => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::TableScan, Some(table.name.to_string()));
            node.cost = estimate_scan_cost(estimate_table_rows(None), width);
            node
        }
        AccessPath::RowIdGet { rowid } => {
            let mut node = PhysicalPlan::leaf(PhysicalKind::RowIdGet, Some(table.name.to_string()));
            node.access_predicates = vec![format!("rowid = {}", rowid.0)];
            node
        }
        AccessPath::IndexPointLookup { index, predicates }
        | AccessPath::IndexRangeScan { index, predicates } => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::IndexScan, Some(table.name.to_string()));
            node.index = Some(index.name.to_string());
            node.access_predicates = predicates;
            node
        }
        AccessPath::CoveringIndexScan {
            index,
            predicates,
            projected_columns,
        } => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::IndexScan, Some(table.name.to_string()));
            node.index = Some(index.name.to_string());
            node.access_predicates = predicates;
            node.projected_columns = projected_columns;
            node
        }
        AccessPath::MultiIndexOr { inputs } | AccessPath::MultiIndexAnd { inputs } => {
            let mut node =
                PhysicalPlan::leaf(PhysicalKind::MultiIndexScan, Some(table.name.to_string()));
            node.children = inputs
                .into_iter()
                .map(|input| access_plan_to_node(table, input, width))
                .collect();
            node
        }
    }
}

fn build_join_plan(
    conn: &Connection,
    tables: &[BoundTable],
    selection: &Option<Expr>,
    bindings: &[Option<SqlValue>],
    optimizer: &OptimizerConfig,
) -> PhysicalPlan {
    let stats = conn.stats_snapshot();
    let scans: Vec<(BoundTable, PhysicalPlan, f64)> = tables
        .iter()
        .map(|table| {
            let scan =
                build_table_scan_plan(conn, &table.table, &[], &None, &[], bindings, optimizer);
            let rows = estimate_table_rows(stats.tables.get(&table.table.table_id));
            (table.clone(), scan, rows)
        })
        .collect();

    if scans.len() <= 1 {
        return scans
            .into_iter()
            .next()
            .map(|(_, plan, _)| plan)
            .unwrap_or_else(|| {
                PhysicalPlan::leaf(PhysicalKind::Constant, Some("empty".to_owned()))
            });
    }

    if scans.len() <= optimizer.max_exact_join_tables
        && let Some(best) = plan_join_exact(&scans, selection, bindings, optimizer)
    {
        return best;
    }

    plan_join_greedy(&scans, selection, bindings, optimizer)
}

#[derive(Clone)]
struct JoinCandidate {
    plan: PhysicalPlan,
    rows: f64,
    cost: Cost,
}

fn plan_join_exact(
    scans: &[(BoundTable, PhysicalPlan, f64)],
    selection: &Option<Expr>,
    bindings: &[Option<SqlValue>],
    optimizer: &OptimizerConfig,
) -> Option<PhysicalPlan> {
    let n = scans.len();
    let max_alternatives = optimizer.max_join_alternatives.max(1);
    let mut dp: Vec<Vec<JoinCandidate>> = vec![Vec::new(); 1 << n];

    for (idx, (_, plan, rows)) in scans.iter().enumerate() {
        dp[1 << idx].push(JoinCandidate {
            plan: plan.clone(),
            rows: *rows,
            cost: plan.cost,
        });
    }

    for mask in 1usize..(1usize << n) {
        if mask.count_ones() <= 1 {
            continue;
        }
        let mut candidates = Vec::new();
        let mut remaining = mask;
        while remaining != 0 {
            let bit = remaining & (!remaining + 1);
            remaining ^= bit;
            let idx = bit.trailing_zeros() as usize;
            let prev_mask = mask ^ bit;
            for prev in &dp[prev_mask] {
                let table = &scans[idx].0.table;
                let next_scan = scans[idx].1.clone();
                let right_rows = scans[idx].2;
                let join_kind = choose_join_kind(prev.rows, right_rows, table, selection, bindings);
                let join_cost = join_cost(join_kind, prev.rows, right_rows);
                let mut join = PhysicalPlan::new(match join_kind {
                    JoinKind::Hash => PhysicalKind::HashJoin,
                    JoinKind::IndexNestedLoop => PhysicalKind::IndexNestedLoopJoin,
                    JoinKind::NestedLoop | JoinKind::Cross => PhysicalKind::NestedLoopJoin,
                });
                let join_rows = join_cost.rows;
                let join_plan_cost = Cost {
                    startup: prev.cost.startup + join_cost.startup,
                    total: prev.cost.total + join_cost.total,
                    rows: join_rows,
                    width: prev.cost.width + scans[idx].1.cost.width,
                    memory_bytes: prev
                        .cost
                        .memory_bytes
                        .saturating_add(join_cost.memory_bytes),
                    spill_bytes: prev.cost.spill_bytes.saturating_add(join_cost.spill_bytes),
                };
                join.children = vec![prev.plan.clone(), next_scan];
                join.estimated_rows = join_rows;
                join.access_predicates = selection
                    .as_ref()
                    .map(|expr| vec![expr_to_string(expr)])
                    .unwrap_or_default();
                join.relation = Some(table.name.to_string());
                join.cost = join_plan_cost;
                candidates.push(JoinCandidate {
                    plan: join,
                    rows: join_rows,
                    cost: join_plan_cost,
                });
            }
        }
        candidates.sort_by(|left, right| {
            left.cost
                .total
                .partial_cmp(&right.cost.total)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    left.rows
                        .partial_cmp(&right.rows)
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| {
                    left.plan
                        .relation
                        .as_deref()
                        .cmp(&right.plan.relation.as_deref())
                })
        });
        candidates.truncate(max_alternatives);
        if !candidates.is_empty() {
            dp[mask] = candidates;
        }
    }

    dp[(1usize << n) - 1]
        .first()
        .map(|candidate| candidate.plan.clone())
}

fn plan_join_greedy(
    scans: &[(BoundTable, PhysicalPlan, f64)],
    selection: &Option<Expr>,
    bindings: &[Option<SqlValue>],
    _optimizer: &OptimizerConfig,
) -> PhysicalPlan {
    let mut ordered = scans.to_vec();
    ordered.sort_by(|left, right| left.2.partial_cmp(&right.2).unwrap_or(Ordering::Equal));
    let mut iter = ordered.into_iter();
    let (_, mut current, mut current_rows) = iter.next().unwrap();
    for (table, next_scan, rows) in iter {
        let join_kind = choose_join_kind(current_rows, rows, &table.table, selection, bindings);
        let join_cost = join_cost(join_kind, current_rows, rows);
        let current_width = current.cost.width;
        let next_width = next_scan.cost.width;
        let current_plan = current;
        let mut join = PhysicalPlan::new(match join_kind {
            JoinKind::Hash => PhysicalKind::HashJoin,
            JoinKind::IndexNestedLoop => PhysicalKind::IndexNestedLoopJoin,
            JoinKind::NestedLoop | JoinKind::Cross => PhysicalKind::NestedLoopJoin,
        });
        join.children = vec![current_plan, next_scan];
        join.estimated_rows = join_cost.rows;
        join.cost = Cost {
            startup: join_cost.startup,
            total: join_cost.total + join.children[0].cost.total,
            rows: join_cost.rows,
            width: current_width + next_width,
            memory_bytes: join_cost.memory_bytes,
            spill_bytes: join_cost.spill_bytes,
        };
        join.access_predicates = selection
            .as_ref()
            .map(|expr| vec![expr_to_string(expr)])
            .unwrap_or_default();
        join.relation = Some(table.table.name.to_string());
        current = join;
        current_rows = join_cost.rows;
    }
    current
}

fn choose_join_kind(
    left_rows: f64,
    right_rows: f64,
    right_table: &Arc<TableDef>,
    selection: &Option<Expr>,
    bindings: &[Option<SqlValue>],
) -> JoinKind {
    if left_rows <= 16.0 || right_rows <= 16.0 {
        return JoinKind::NestedLoop;
    }
    let Some(expr) = selection else {
        return JoinKind::Cross;
    };
    if join_has_indexable_equality(expr, right_table, bindings)
        && (left_rows <= 256.0 || right_rows <= 1024.0)
    {
        return JoinKind::IndexNestedLoop;
    }
    if join_has_equality(expr, right_table, bindings) {
        return JoinKind::Hash;
    }
    JoinKind::NestedLoop
}

fn join_has_equality(expr: &Expr, table: &Arc<TableDef>, bindings: &[Option<SqlValue>]) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, BinaryOperator::Eq) {
                join_operand_references_table(left, table)
                    || join_operand_references_table(right, table)
                    || eval_constant(left, bindings).is_some()
                    || eval_constant(right, bindings).is_some()
            } else if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                join_has_equality(left, table, bindings)
                    || join_has_equality(right, table, bindings)
            } else {
                false
            }
        }
        Expr::Nested(inner) => join_has_equality(inner, table, bindings),
        _ => false,
    }
}

fn join_has_indexable_equality(
    expr: &Expr,
    table: &Arc<TableDef>,
    bindings: &[Option<SqlValue>],
) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, BinaryOperator::Eq) {
                if let Some(ordinal) = join_table_column_ordinal(left, table)
                    .or_else(|| join_table_column_ordinal(right, table))
                {
                    return table.indexes.iter().any(|index| {
                        index
                            .keys
                            .first()
                            .is_some_and(|key| key.ordinal as usize == ordinal)
                    });
                }
                eval_constant(left, bindings).is_some() || eval_constant(right, bindings).is_some()
            } else if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                join_has_indexable_equality(left, table, bindings)
                    || join_has_indexable_equality(right, table, bindings)
            } else {
                false
            }
        }
        Expr::Nested(inner) => join_has_indexable_equality(inner, table, bindings),
        _ => false,
    }
}

fn join_operand_references_table(expr: &Expr, table: &TableDef) -> bool {
    join_table_column_ordinal(expr, table).is_some()
}

fn join_table_column_ordinal(expr: &Expr, table: &TableDef) -> Option<usize> {
    match expr {
        Expr::Identifier(ident) => column_ordinal_for_table(&ident.value, table),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .and_then(|ident| column_ordinal_for_table(&ident.value, table)),
        Expr::Nested(inner) => join_table_column_ordinal(inner, table),
        _ => None,
    }
}

fn wrap_aggregate(input: PhysicalPlan, plan: &SelectPlan) -> PhysicalPlan {
    let input_rows = input.cost.rows;
    let input_width = input.cost.width;
    let input_total = input.cost.total;
    let mut node = PhysicalPlan::new(if has_group_by_ordering(plan) {
        PhysicalKind::StreamingAggregate
    } else {
        PhysicalKind::HashAggregate
    });
    node.children = vec![input];
    node.estimated_rows = estimate_group_rows(input_rows, plan.group_by.len());
    node.cost = Cost {
        startup: CPU_OPERATOR_COST,
        total: CPU_OPERATOR_COST + input_total + node.estimated_rows * CPU_TUPLE_COST,
        rows: node.estimated_rows,
        width: input_width,
        memory_bytes: if matches!(node.kind, PhysicalKind::HashAggregate) {
            (input_rows.max(1.0) * input_width.max(1.0)) as usize
        } else {
            0
        },
        spill_bytes: 0,
    };
    node.memory_budget = node.cost.memory_bytes;
    node.projected_columns = projected_columns_from_projection(&plan.projection);
    node.access_predicates = plan.group_by.iter().map(expr_to_string).collect();
    if matches!(node.kind, PhysicalKind::StreamingAggregate) {
        node.output_order = plan.group_by.iter().map(expr_to_string).collect();
    }
    node
}

fn wrap_ordering(
    input: PhysicalPlan,
    plan: &SelectPlan,
    _bindings: &[Option<SqlValue>],
    _optimizer: &OptimizerConfig,
) -> PhysicalPlan {
    let limit_small = plan
        .limit
        .as_ref()
        .and_then(|expr| eval_constant(expr, &[]))
        .and_then(|value| match value {
            SqlValue::Integer(v) if v > 0 => Some(v as usize),
            _ => None,
        })
        .unwrap_or(usize::MAX);
    let kind = if limit_small != usize::MAX {
        PhysicalKind::TopN
    } else {
        PhysicalKind::Sort
    };
    let mut node = PhysicalPlan::new(kind);
    node.children = vec![input];
    node.output_order = plan
        .order_by
        .iter()
        .map(|order| expr_to_string(&order.expr))
        .collect();
    node.estimated_rows = node.children[0].estimated_rows;
    node.cost = Cost {
        startup: CPU_OPERATOR_COST,
        total: node.children[0].cost.total + (node.estimated_rows * CPU_TUPLE_COST),
        rows: node.estimated_rows,
        width: node.children[0].cost.width,
        memory_bytes: if kind == PhysicalKind::TopN {
            limit_small.saturating_mul(node.children[0].cost.width as usize)
        } else {
            (node.estimated_rows * node.children[0].cost.width.max(1.0)) as usize
        },
        spill_bytes: 0,
    };
    node.memory_budget = node.cost.memory_bytes;
    node
}

fn estimate_group_rows(input_rows: f64, group_by_len: usize) -> f64 {
    if group_by_len == 0 {
        return 1.0;
    }
    let divisor = (group_by_len as f64 * 2.0).max(1.0);
    (input_rows / divisor).clamp(1.0, input_rows.max(1.0))
}

fn wrap_project(input: PhysicalPlan, plan: &SelectPlan) -> PhysicalPlan {
    let mut node = PhysicalPlan::new(PhysicalKind::Project);
    node.children = vec![input];
    node.projected_columns = projected_columns_from_projection(&plan.projection);
    node.estimated_rows = node.children[0].estimated_rows;
    node.cost = Cost {
        startup: CPU_OPERATOR_COST,
        total: node.children[0].cost.total + node.estimated_rows * CPU_TUPLE_COST,
        rows: node.estimated_rows,
        width: node.children[0].cost.width,
        memory_bytes: 0,
        spill_bytes: 0,
    };
    node
}

fn wrap_limit(input: PhysicalPlan, plan: &SelectPlan) -> PhysicalPlan {
    let input_rows = input.cost.rows;
    let input_width = input.cost.width;
    let input_total = input.cost.total;
    let mut node = PhysicalPlan::new(PhysicalKind::Limit);
    node.children = vec![input];
    node.estimated_rows = input_rows;
    node.cost = Cost {
        startup: 0.0,
        total: input_total,
        rows: input_rows,
        width: input_width,
        memory_bytes: 0,
        spill_bytes: 0,
    };
    node.relation = Some(
        plan.limit
            .as_ref()
            .map(expr_to_string)
            .unwrap_or_else(|| "LIMIT".to_owned()),
    );
    node
}

fn choose_access_path(
    table: &Arc<TableDef>,
    projection: &[SelectItem],
    selection: &Option<Expr>,
    order_by: &[OrderByExpr],
    rowid: Option<RowId>,
    table_stats: Option<&TableStats>,
    optimizer: &OptimizerConfig,
) -> AccessPath {
    if let Some(rowid) = rowid {
        return AccessPath::RowIdGet { rowid };
    }

    if optimizer.enable_multi_index_or
        && let Some((left, right)) = selection.as_ref().and_then(or_children)
    {
        return AccessPath::MultiIndexOr {
            inputs: vec![
                choose_access_path(
                    table,
                    projection,
                    &Some(left.clone()),
                    order_by,
                    None,
                    table_stats,
                    optimizer,
                ),
                choose_access_path(
                    table,
                    projection,
                    &Some(right.clone()),
                    order_by,
                    None,
                    table_stats,
                    optimizer,
                ),
            ],
        };
    }
    if optimizer.enable_multi_index_and
        && let Some((left, right)) = selection.as_ref().and_then(and_children)
    {
        return AccessPath::MultiIndexAnd {
            inputs: vec![
                choose_access_path(
                    table,
                    projection,
                    &Some(left.clone()),
                    order_by,
                    None,
                    table_stats,
                    optimizer,
                ),
                choose_access_path(
                    table,
                    projection,
                    &Some(right.clone()),
                    order_by,
                    None,
                    table_stats,
                    optimizer,
                ),
            ],
        };
    }

    if optimizer.enable_covering_index
        && let Some(index) = best_index_for_table(table, selection, order_by, table_stats)
    {
        let predicates = predicates_for_index(selection);
        let projected_columns = projected_columns_from_projection(projection);
        if projected_columns_are_covered(table, &index, projection) {
            return AccessPath::CoveringIndexScan {
                index,
                predicates,
                projected_columns,
            };
        }
        if is_range_predicate(selection) {
            return AccessPath::IndexRangeScan { index, predicates };
        }
        return AccessPath::IndexPointLookup { index, predicates };
    }

    AccessPath::TableScan
}

fn best_index_for_table(
    table: &Arc<TableDef>,
    selection: &Option<Expr>,
    order_by: &[OrderByExpr],
    _table_stats: Option<&TableStats>,
) -> Option<Arc<IndexDef>> {
    let candidate_column = selection
        .as_ref()
        .and_then(|expr| first_indexable_column(expr, table))
        .or_else(|| {
            order_by
                .first()
                .and_then(|order| order_expr_column(order, table))
        });
    let candidate_column = candidate_column?;
    for index in &table.indexes {
        if index
            .keys
            .first()
            .is_some_and(|key| key.ordinal as usize == candidate_column)
        {
            return Some(Arc::new(index.clone()));
        }
    }
    None
}

fn first_indexable_column(expr: &Expr, table: &TableDef) -> Option<usize> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ) {
                column_ordinal(left, table).or_else(|| column_ordinal(right, table))
            } else if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                first_indexable_column(left, table).or_else(|| first_indexable_column(right, table))
            } else {
                None
            }
        }
        Expr::Nested(expr) => first_indexable_column(expr, table),
        _ => None,
    }
}

fn or_children(expr: &Expr) -> Option<(&Expr, &Expr)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Some((left.as_ref(), right.as_ref())),
        Expr::Nested(inner) => or_children(inner),
        _ => None,
    }
}

fn and_children(expr: &Expr) -> Option<(&Expr, &Expr)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Some((left.as_ref(), right.as_ref())),
        Expr::Nested(inner) => and_children(inner),
        _ => None,
    }
}

fn column_ordinal(expr: &Expr, table: &TableDef) -> Option<usize> {
    match expr {
        Expr::Identifier(ident) => table
            .columns
            .iter()
            .position(|column| column.folded.as_ref().eq_ignore_ascii_case(&ident.value)),
        Expr::CompoundIdentifier(parts) => parts.last().and_then(|ident| {
            table
                .columns
                .iter()
                .position(|column| column.folded.as_ref().eq_ignore_ascii_case(&ident.value))
        }),
        _ => None,
    }
}

fn order_expr_column(order: &OrderByExpr, table: &TableDef) -> Option<usize> {
    match &order.expr {
        Expr::Identifier(ident) => table
            .columns
            .iter()
            .position(|column| column.folded.as_ref().eq_ignore_ascii_case(&ident.value)),
        Expr::CompoundIdentifier(parts) => parts.last().and_then(|ident| {
            table
                .columns
                .iter()
                .position(|column| column.folded.as_ref().eq_ignore_ascii_case(&ident.value))
        }),
        _ => None,
    }
}

fn projected_columns_from_projection(projection: &[SelectItem]) -> Vec<String> {
    projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                Some(expr_to_string(expr))
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
        })
        .collect()
}

fn projected_columns_from_select(table: &Arc<TableDef>, projection: &[SelectItem]) -> Vec<String> {
    if projection.is_empty() {
        return table
            .columns
            .iter()
            .map(|column| column.name.to_string())
            .collect();
    }
    let projected = projected_columns_from_projection(projection);
    if projected.is_empty() {
        table
            .columns
            .iter()
            .map(|column| column.name.to_string())
            .collect()
    } else {
        projected
    }
}

fn projected_columns_are_covered(
    table: &Arc<TableDef>,
    index: &Arc<IndexDef>,
    projection: &[SelectItem],
) -> bool {
    if projection.is_empty() {
        return false;
    }
    let covered_ordinals = index
        .keys
        .iter()
        .map(|key| key.ordinal as usize)
        .collect::<std::collections::BTreeSet<_>>();
    projection.iter().all(|item| match item {
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            projection_expr_covered(table, &covered_ordinals, expr)
        }
    })
}

fn predicates_for_index(selection: &Option<Expr>) -> Vec<String> {
    selection
        .as_ref()
        .map(|expr| vec![expr_to_string(expr)])
        .unwrap_or_default()
}

fn is_range_predicate(selection: &Option<Expr>) -> bool {
    matches!(
        selection,
        Some(Expr::BinaryOp {
            op: BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq,
            ..
        })
    )
}

fn has_projection_exprs(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
        )
    })
}

fn requires_aggregate(plan: &SelectPlan) -> bool {
    !plan.group_by.is_empty() || plan.projection.iter().any(select_item_contains_aggregate)
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

fn has_group_by_ordering(plan: &SelectPlan) -> bool {
    if plan.group_by.is_empty() {
        return false;
    }
    let order_by: Vec<_> = plan
        .order_by
        .iter()
        .map(|item| expr_to_string(&item.expr))
        .collect();
    let group_by: Vec<_> = plan.group_by.iter().map(expr_to_string).collect();
    order_by == group_by
}

fn output_order_satisfies(output_order: &[String], order_by: &[OrderByExpr]) -> bool {
    if order_by.is_empty() {
        return true;
    }
    if output_order.len() < order_by.len() {
        return false;
    }
    output_order
        .iter()
        .zip(order_by.iter())
        .all(|(left, right)| *left == expr_to_string(&right.expr))
}
