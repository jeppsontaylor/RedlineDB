use std::sync::Arc;

use redlinedb_kernel::catalog::{
    ColumnConstraintSpec, ColumnSpec, ConflictAction, CreateIndexSpec, CreateTableSpec, DbName,
    DropIndexSpec, DropTableSpec, ExprAst, IndexColumnSpec, IndexOrigin, OwnedValue, QualifiedName,
    SchemaEpoch, SchemaSnapshot, SortDir, TableConstraintSpec, lookup_table,
};
use redlinedb_kernel::engine::Engine;
use sqlparser::ast::{
    Analyze as SqlAnalyze, AnalyzeFormat, AnalyzeFormatKind, BinaryOperator, ColumnDef,
    ColumnOption, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    IndexColumn, JoinConstraint, JoinOperator, LimitClause, ObjectName, ObjectNamePart,
    OrderByExpr, OrderByKind, Query, SelectItem, SetExpr, Statement as SqlStatement, TableFactor,
    TableObject, TableWithJoins, UnaryOperator, Value, ValueWithSpan,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::error::{Error, Result};
use crate::session::BeginMode;
use crate::statement::{
    BoundTable, DeletePlan, InsertPlan, ParamLayout, PreparedKind, PreparedTemplate, SelectPlan,
    SelectSource, UpdatePlan,
};

mod helpers;
use helpers::*;

pub fn parse_prepared_template(engine: &Engine, sql: &str) -> Result<PreparedTemplate> {
    let trimmed = sql.trim();
    let lower = trimmed.trim_end_matches(';').trim().to_ascii_lowercase();
    let schema = engine.schema_snapshot();
    let schema_epoch = engine.schema_epoch();

    if lower == "begin" || lower == "begin deferred" {
        return Ok(template(
            trimmed,
            schema_epoch,
            false,
            PreparedKind::Begin(BeginMode::Deferred),
        ));
    }
    if lower == "begin immediate" {
        return Ok(template(
            trimmed,
            schema_epoch,
            false,
            PreparedKind::Begin(BeginMode::Immediate),
        ));
    }
    if lower == "begin exclusive" {
        return Ok(template(
            trimmed,
            schema_epoch,
            false,
            PreparedKind::Begin(BeginMode::Exclusive),
        ));
    }
    if lower == "commit" {
        return Ok(template(trimmed, schema_epoch, false, PreparedKind::Commit));
    }
    if lower == "rollback" {
        return Ok(template(
            trimmed,
            schema_epoch,
            false,
            PreparedKind::Rollback,
        ));
    }

    let dialect = SQLiteDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;
    if statements.len() != 1 {
        return Err(Error::UnsupportedSql(
            "only single-statement prepares are supported".to_owned(),
        ));
    }

    bind_statement(engine, schema, schema_epoch, trimmed, statements.remove(0))
}

fn bind_statement(
    engine: &Engine,
    schema: Arc<SchemaSnapshot>,
    schema_epoch: SchemaEpoch,
    sql: &str,
    statement: SqlStatement,
) -> Result<PreparedTemplate> {
    match statement {
        SqlStatement::Query(query) => bind_query(engine, schema, schema_epoch, sql, *query),
        SqlStatement::Insert(insert) => bind_insert(schema, schema_epoch, sql, insert),
        SqlStatement::Update(update) => bind_update(schema, schema_epoch, sql, update),
        SqlStatement::Delete(delete) => bind_delete(schema, schema_epoch, sql, delete),
        SqlStatement::CreateTable(create_table) => {
            bind_create_table(schema_epoch, sql, create_table)
        }
        SqlStatement::CreateIndex(create_index) => {
            bind_create_index(schema_epoch, sql, create_index)
        }
        SqlStatement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => bind_drop(sql, schema_epoch, object_type, if_exists, names),
        SqlStatement::Analyze(analyze) => bind_analyze(schema, schema_epoch, sql, analyze),
        SqlStatement::Explain {
            analyze,
            verbose: _,
            query_plan,
            estimate: _,
            statement,
            format,
            ..
        } => bind_explain(
            engine,
            schema,
            schema_epoch,
            sql,
            analyze,
            query_plan,
            format,
            *statement,
        ),
        SqlStatement::ExplainTable { .. } => Err(Error::UnsupportedSql(
            "EXPLAIN TABLE is not supported".to_owned(),
        )),
        other => Err(Error::UnsupportedSql(format!(
            "statement not supported yet: {other:?}"
        ))),
    }
}

fn template(
    sql: &str,
    schema_epoch: SchemaEpoch,
    readonly: bool,
    kind: PreparedKind,
) -> PreparedTemplate {
    PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: ParamLayout::default(),
        output_columns: Arc::from([]),
        readonly,
        kind,
    }
}

fn bind_query(
    _engine: &Engine,
    schema: Arc<SchemaSnapshot>,
    schema_epoch: SchemaEpoch,
    sql: &str,
    query: Query,
) -> Result<PreparedTemplate> {
    let Query {
        body,
        order_by,
        limit_clause,
        ..
    } = query;
    let select = match *body {
        SetExpr::Select(select) => select,
        _ => {
            return Err(Error::UnsupportedSql(
                "only simple SELECT queries are supported".to_owned(),
            ));
        }
    };

    let mut params = ParamLayout::default();
    let mut projection = Vec::new();
    let mut output_columns = Vec::new();

    let (source, mut selection) = bind_select_from(&schema, select.from, &mut params)?;

    for item in select.projection {
        let item = normalize_select_item(item, &mut params)?;
        match &item {
            SelectItem::Wildcard(_) => push_projection_columns(&source, &mut output_columns),
            SelectItem::QualifiedWildcard(_, _) => {
                push_projection_columns(&source, &mut output_columns)
            }
            SelectItem::UnnamedExpr(expr) => output_columns.push(render_expr_name(expr)),
            SelectItem::ExprWithAlias { alias, .. } => output_columns.push(alias.value.clone()),
        }
        projection.push(item);
    }
    if projection.is_empty() {
        push_projection_columns(&source, &mut output_columns);
    }

    if let Some(expr) = select.selection {
        selection = Some(match selection {
            Some(join_expr) => and_expr(join_expr, normalize_expr(expr, &mut params)?),
            None => normalize_expr(expr, &mut params)?,
        });
    }

    let group_by = match select.group_by {
        GroupByExpr::All(_) => {
            return Err(Error::UnsupportedSql(
                "GROUP BY ALL is not supported".to_owned(),
            ));
        }
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err(Error::UnsupportedSql(
                    "GROUP BY modifiers are not supported".to_owned(),
                ));
            }
            exprs
                .into_iter()
                .map(|expr| normalize_expr(expr, &mut params))
                .collect::<Result<Vec<_>>>()?
        }
    };

    let having = match select.having {
        Some(expr) => Some(normalize_expr(expr, &mut params)?),
        None => None,
    };

    let order_by = match order_by {
        Some(order_by) => match order_by.kind {
            OrderByKind::Expressions(exprs) => exprs
                .into_iter()
                .map(|expr| {
                    let options = expr.options;
                    let with_fill = expr.with_fill;
                    let expr = normalize_expr(expr.expr, &mut params)?;
                    Ok(OrderByExpr {
                        expr,
                        options,
                        with_fill,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            OrderByKind::All(_) => {
                return Err(Error::UnsupportedSql(
                    "ORDER BY ALL is not supported".to_owned(),
                ));
            }
        },
        None => Vec::new(),
    };

    let (limit, offset) = match limit_clause {
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by: _,
        }) => {
            let limit = match limit {
                Some(expr) => Some(normalize_expr(expr, &mut params)?),
                None => None,
            };
            let offset = match offset {
                Some(offset) => Some(normalize_expr(offset.value, &mut params)?),
                None => None,
            };
            (limit, offset)
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (
            Some(normalize_expr(limit, &mut params)?),
            Some(normalize_expr(offset, &mut params)?),
        ),
        None => (None, None),
    };

    let readonly = true;
    if params.count() == 0 {
        scan_sql_parameters(sql, &mut params);
    }
    Ok(PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: params,
        output_columns: output_columns.into(),
        readonly,
        kind: PreparedKind::Select(SelectPlan {
            source,
            projection,
            selection,
            group_by,
            having,
            order_by,
            limit,
            offset,
        }),
    })
}

fn bind_insert(
    schema: Arc<SchemaSnapshot>,
    schema_epoch: SchemaEpoch,
    sql: &str,
    insert: sqlparser::ast::Insert,
) -> Result<PreparedTemplate> {
    if !insert.assignments.is_empty() {
        return Err(Error::UnsupportedSql(
            "INSERT ... SET is not supported".to_owned(),
        ));
    }
    if insert.returning.is_some() {
        return Err(Error::UnsupportedSql(
            "RETURNING is not supported".to_owned(),
        ));
    }
    let table = bind_table_object(&schema, &insert.table)?;
    let mut params = ParamLayout::default();
    let columns = if insert.columns.is_empty() {
        (0..table.columns.len()).collect::<Vec<_>>()
    } else {
        insert
            .columns
            .into_iter()
            .map(|column| resolve_column_ordinal_in_table(&table, &column.value))
            .collect::<Result<Vec<_>>>()?
    };

    let mut rows = Vec::new();
    let mut default_values = false;
    if let Some(source) = insert.source {
        match *source.body {
            SetExpr::Values(values) => {
                for row in values.rows {
                    let mut exprs = Vec::with_capacity(row.len());
                    for expr in row {
                        exprs.push(normalize_expr(expr, &mut params)?);
                    }
                    rows.push(exprs);
                }
            }
            _ => {
                return Err(Error::UnsupportedSql(
                    "INSERT source must be VALUES".to_owned(),
                ));
            }
        }
    } else {
        default_values = true;
    }

    if params.count() == 0 {
        scan_sql_parameters(sql, &mut params);
    }
    Ok(PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: params,
        output_columns: Arc::from([]),
        readonly: false,
        kind: PreparedKind::Insert(InsertPlan {
            table,
            columns,
            rows,
            default_values,
        }),
    })
}

fn bind_update(
    schema: Arc<SchemaSnapshot>,
    schema_epoch: SchemaEpoch,
    sql: &str,
    update: sqlparser::ast::Update,
) -> Result<PreparedTemplate> {
    if update.returning.is_some() {
        return Err(Error::UnsupportedSql(
            "RETURNING is not supported".to_owned(),
        ));
    }
    if update.from.is_some() {
        return Err(Error::UnsupportedSql(
            "UPDATE ... FROM is not supported".to_owned(),
        ));
    }
    if update.limit.is_some() {
        return Err(Error::UnsupportedSql(
            "UPDATE LIMIT is not supported".to_owned(),
        ));
    }

    let table = bind_table_with_joins(&schema, &update.table)?;
    let mut params = ParamLayout::default();
    let mut assignments = Vec::new();
    for assignment in update.assignments {
        let ordinal = match assignment.target {
            sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                resolve_column_ordinal_in_object_name(&table, &name)?
            }
            sqlparser::ast::AssignmentTarget::Tuple(_) => {
                return Err(Error::UnsupportedSql(
                    "tuple assignment is not supported".to_owned(),
                ));
            }
        };
        assignments.push((ordinal, normalize_expr(assignment.value, &mut params)?));
    }
    let selection = match update.selection {
        Some(expr) => Some(normalize_expr(expr, &mut params)?),
        None => None,
    };
    if params.count() == 0 {
        scan_sql_parameters(sql, &mut params);
    }
    Ok(PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: params,
        output_columns: Arc::from([]),
        readonly: false,
        kind: PreparedKind::Update(UpdatePlan {
            table,
            assignments,
            selection,
        }),
    })
}

fn bind_delete(
    schema: Arc<SchemaSnapshot>,
    schema_epoch: SchemaEpoch,
    sql: &str,
    delete: sqlparser::ast::Delete,
) -> Result<PreparedTemplate> {
    if delete.using.is_some() {
        return Err(Error::UnsupportedSql(
            "DELETE ... USING is not supported".to_owned(),
        ));
    }
    if !delete.order_by.is_empty() {
        return Err(Error::UnsupportedSql(
            "DELETE ORDER BY is not supported".to_owned(),
        ));
    }
    if delete.limit.is_some() {
        return Err(Error::UnsupportedSql(
            "DELETE LIMIT is not supported".to_owned(),
        ));
    }

    let from = match delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(from)
        | sqlparser::ast::FromTable::WithoutKeyword(from) => from,
    };
    if from.len() != 1 {
        return Err(Error::UnsupportedSql(
            "only single-table DELETE is supported".to_owned(),
        ));
    }
    let table = bind_table_with_joins(&schema, &from[0])?;
    let mut params = ParamLayout::default();
    let selection = match delete.selection {
        Some(expr) => Some(normalize_expr(expr, &mut params)?),
        None => None,
    };
    Ok(PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: params,
        output_columns: Arc::from([]),
        readonly: false,
        kind: PreparedKind::Delete(DeletePlan { table, selection }),
    })
}

fn bind_create_table(
    schema_epoch: SchemaEpoch,
    sql: &str,
    create_table: sqlparser::ast::CreateTable,
) -> Result<PreparedTemplate> {
    if create_table.query.is_some() {
        return Err(Error::UnsupportedSql(
            "CREATE TABLE AS SELECT is not supported".to_owned(),
        ));
    }
    if create_table.or_replace
        || create_table.temporary
        || create_table.external
        || create_table.dynamic
        || create_table.global.is_some()
        || create_table.transient
        || create_table.volatile
        || create_table.iceberg
        || create_table.query.is_some()
        || create_table.like.is_some()
        || create_table.clone.is_some()
        || create_table.version.is_some()
        || create_table.comment.is_some()
        || create_table.on_commit.is_some()
        || create_table.on_cluster.is_some()
        || create_table.primary_key.is_some()
        || create_table.order_by.is_some()
        || create_table.partition_by.is_some()
        || create_table.cluster_by.is_some()
        || create_table.clustered_by.is_some()
        || create_table.inherits.is_some()
        || create_table.partition_of.is_some()
        || create_table.for_values.is_some()
        || create_table.copy_grants
        || create_table.enable_schema_evolution.is_some()
        || create_table.change_tracking.is_some()
    {
        return Err(Error::UnsupportedSql(
            "CREATE TABLE modifiers are not supported".to_owned(),
        ));
    }

    let (schema, name) = split_name(create_table.name)?;
    let mut columns = Vec::with_capacity(create_table.columns.len());
    let mut column_lookup = std::collections::HashMap::new();
    for (ordinal, column) in create_table.columns.iter().enumerate() {
        column_lookup.insert(column.name.value.to_ascii_lowercase(), ordinal);
    }

    for (ordinal, column) in create_table.columns.into_iter().enumerate() {
        columns.push(convert_column_def(column, ordinal, &column_lookup)?);
    }

    let mut constraints = Vec::new();
    for constraint in create_table.constraints {
        constraints.push(convert_table_constraint(constraint, &column_lookup)?);
    }

    Ok(PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: ParamLayout::default(),
        output_columns: Arc::from([]),
        readonly: false,
        kind: PreparedKind::CreateTable(CreateTableSpec {
            schema,
            name,
            if_not_exists: create_table.if_not_exists,
            columns,
            constraints,
            strict: create_table.strict,
            without_rowid: create_table.without_rowid,
            normalized_sql: Some(sql.to_owned()),
        }),
    })
}

fn bind_create_index(
    schema_epoch: SchemaEpoch,
    sql: &str,
    create_index: sqlparser::ast::CreateIndex,
) -> Result<PreparedTemplate> {
    if create_index.concurrently
        || create_index.using.is_some()
        || !create_index.include.is_empty()
        || create_index.nulls_distinct.is_some()
        || !create_index.with.is_empty()
        || create_index.predicate.is_some()
        || !create_index.index_options.is_empty()
        || !create_index.alter_options.is_empty()
    {
        return Err(Error::UnsupportedSql(
            "CREATE INDEX modifiers are not supported".to_owned(),
        ));
    }
    let name = create_index
        .name
        .ok_or_else(|| Error::UnsupportedSql("CREATE INDEX requires a name".to_owned()))?;
    let (schema, name) = split_name(name)?;
    let table = parse_qualified_name(create_index.table_name)?;
    let mut columns = Vec::with_capacity(create_index.columns.len());
    for column in create_index.columns {
        columns.push(convert_index_column(column)?);
    }

    Ok(PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: ParamLayout::default(),
        output_columns: Arc::from([]),
        readonly: false,
        kind: PreparedKind::CreateIndex(CreateIndexSpec {
            schema,
            name,
            table,
            unique: create_index.unique,
            columns,
            origin: IndexOrigin::User,
            normalized_sql: Some(sql.to_owned()),
        }),
    })
}

fn bind_drop(
    sql: &str,
    schema_epoch: SchemaEpoch,
    object_type: sqlparser::ast::ObjectType,
    if_exists: bool,
    names: Vec<ObjectName>,
) -> Result<PreparedTemplate> {
    if names.len() != 1 {
        return Err(Error::UnsupportedSql(
            "only single-object DROP is supported".to_owned(),
        ));
    }
    let name = parse_qualified_name(names.into_iter().next().unwrap())?;
    let kind = match object_type {
        sqlparser::ast::ObjectType::Table => {
            PreparedKind::DropTable(DropTableSpec { name, if_exists })
        }
        sqlparser::ast::ObjectType::Index => {
            PreparedKind::DropIndex(DropIndexSpec { name, if_exists })
        }
        _ => {
            return Err(Error::UnsupportedSql(
                "only DROP TABLE and DROP INDEX are supported".to_owned(),
            ));
        }
    };
    Ok(PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: ParamLayout::default(),
        output_columns: Arc::from([]),
        readonly: false,
        kind,
    })
}

fn bind_analyze(
    schema: Arc<SchemaSnapshot>,
    schema_epoch: SchemaEpoch,
    sql: &str,
    analyze: SqlAnalyze,
) -> Result<PreparedTemplate> {
    let table = match analyze.table_name {
        Some(name) => Some(bind_table_name(&schema, &name)?),
        None => None,
    };
    Ok(PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: ParamLayout::default(),
        output_columns: Arc::from([]),
        readonly: false,
        kind: PreparedKind::Analyze(crate::statement::AnalyzePlan { table }),
    })
}

#[allow(clippy::too_many_arguments)]
fn bind_explain(
    engine: &Engine,
    schema: Arc<SchemaSnapshot>,
    schema_epoch: SchemaEpoch,
    sql: &str,
    analyze: bool,
    query_plan: bool,
    format: Option<AnalyzeFormatKind>,
    statement: SqlStatement,
) -> Result<PreparedTemplate> {
    let inner = Arc::new(bind_statement(
        engine,
        Arc::clone(&schema),
        schema_epoch,
        sql,
        statement,
    )?);
    let explain_format = if query_plan {
        crate::statement::ExplainFormat::QueryPlan
    } else {
        match format {
            Some(AnalyzeFormatKind::Keyword(AnalyzeFormat::JSON))
            | Some(AnalyzeFormatKind::Assignment(AnalyzeFormat::JSON)) => {
                crate::statement::ExplainFormat::Json
            }
            _ => crate::statement::ExplainFormat::Text,
        }
    };
    let output_columns = match explain_format {
        crate::statement::ExplainFormat::QueryPlan => Arc::from([
            "id".to_owned(),
            "parent".to_owned(),
            "notused".to_owned(),
            "detail".to_owned(),
        ]),
        crate::statement::ExplainFormat::Text | crate::statement::ExplainFormat::Json => {
            Arc::from(["explain".to_owned()])
        }
    };
    Ok(PreparedTemplate {
        sql: Arc::from(sql),
        schema_epoch,
        stats_epoch: 0,
        optimizer_hash: 0,
        param_layout: inner.param_layout.clone(),
        output_columns,
        readonly: true,
        kind: PreparedKind::Explain(crate::statement::ExplainPlan {
            format: explain_format,
            analyze,
            inner,
        }),
    })
}

fn normalize_select_item(item: SelectItem, params: &mut ParamLayout) -> Result<SelectItem> {
    Ok(match item {
        SelectItem::UnnamedExpr(expr) => SelectItem::UnnamedExpr(normalize_expr(expr, params)?),
        SelectItem::ExprWithAlias { expr, alias } => SelectItem::ExprWithAlias {
            expr: normalize_expr(expr, params)?,
            alias,
        },
        other => other,
    })
}

fn normalize_expr(expr: Expr, params: &mut ParamLayout) -> Result<Expr> {
    Ok(match expr {
        Expr::Value(v) => match &v.value {
            Value::Placeholder(name) => {
                let name = normalize_placeholder(name, params)?;
                Expr::Value(ValueWithSpan {
                    value: Value::Placeholder(name),
                    span: v.span,
                })
            }
            _ => Expr::Value(v),
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(normalize_expr(*left, params)?),
            op,
            right: Box::new(normalize_expr(*right, params)?),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op,
            expr: Box::new(normalize_expr(*expr, params)?),
        },
        Expr::Nested(expr) => Expr::Nested(Box::new(normalize_expr(*expr, params)?)),
        Expr::Function(mut func) => {
            normalize_function_args(&mut func.args, params)?;
            normalize_function_args(&mut func.parameters, params)?;
            Expr::Function(func)
        }
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::Like {
            negated,
            any,
            expr: Box::new(normalize_expr(*expr, params)?),
            pattern: Box::new(normalize_expr(*pattern, params)?),
            escape_char,
        },
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::ILike {
            negated,
            any,
            expr: Box::new(normalize_expr(*expr, params)?),
            pattern: Box::new(normalize_expr(*pattern, params)?),
            escape_char,
        },
        Expr::Cast {
            expr,
            data_type,
            kind,
            format,
            array,
        } => Expr::Cast {
            expr: Box::new(normalize_expr(*expr, params)?),
            data_type,
            kind,
            format,
            array,
        },
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(normalize_expr(*expr, params)?),
            negated,
            low: Box::new(normalize_expr(*low, params)?),
            high: Box::new(normalize_expr(*high, params)?),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(normalize_expr(*expr, params)?),
            list: list
                .into_iter()
                .map(|expr| normalize_expr(expr, params))
                .collect::<Result<Vec<_>>>()?,
            negated,
        },
        Expr::IsNull(expr) => Expr::IsNull(Box::new(normalize_expr(*expr, params)?)),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(normalize_expr(*expr, params)?)),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(normalize_expr(*left, params)?),
            Box::new(normalize_expr(*right, params)?),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(normalize_expr(*left, params)?),
            Box::new(normalize_expr(*right, params)?),
        ),
        Expr::IsTrue(expr) => Expr::IsTrue(Box::new(normalize_expr(*expr, params)?)),
        Expr::IsNotTrue(expr) => Expr::IsNotTrue(Box::new(normalize_expr(*expr, params)?)),
        Expr::IsFalse(expr) => Expr::IsFalse(Box::new(normalize_expr(*expr, params)?)),
        Expr::IsNotFalse(expr) => Expr::IsNotFalse(Box::new(normalize_expr(*expr, params)?)),
        Expr::IsUnknown(expr) => Expr::IsUnknown(Box::new(normalize_expr(*expr, params)?)),
        Expr::IsNotUnknown(expr) => Expr::IsNotUnknown(Box::new(normalize_expr(*expr, params)?)),
        Expr::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            case_token,
            end_token,
            operand: operand
                .map(|expr| normalize_expr(*expr, params))
                .transpose()?
                .map(Box::new),
            conditions: conditions
                .into_iter()
                .map(|when| {
                    Ok(sqlparser::ast::CaseWhen {
                        condition: normalize_expr(when.condition, params)?,
                        result: normalize_expr(when.result, params)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            else_result: else_result
                .map(|expr| normalize_expr(*expr, params))
                .transpose()?
                .map(Box::new),
        },
        other => other,
    })
}

fn normalize_function_args(args: &mut FunctionArguments, params: &mut ParamLayout) -> Result<()> {
    if let FunctionArguments::List(list) = args {
        for arg in &mut list.args {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    *expr = normalize_expr(expr.clone(), params)?;
                }
                FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } => {
                    *expr = normalize_expr(expr.clone(), params)?;
                }
                FunctionArg::ExprNamed {
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } => {
                    *expr = normalize_expr(expr.clone(), params)?;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn normalize_placeholder(name: &str, params: &mut ParamLayout) -> Result<String> {
    if name == "?" {
        let slot = params.push_anonymous();
        return Ok(format!("?{slot}"));
    }
    if let Some(rest) = name.strip_prefix('?') {
        let slot = rest
            .parse::<usize>()
            .map_err(|_| Error::Parse(format!("invalid parameter {name}")))?;
        if slot == 0 {
            return Err(Error::Parse("parameter indices are 1-based".to_owned()));
        }
        params.push_numbered(slot);
        return Ok(format!("?{slot}"));
    }
    if name.starts_with(':') || name.starts_with('@') || name.starts_with('$') {
        let slot = params.push_named(name.to_owned());
        return Ok(format!("?{slot}"));
    }
    Err(Error::Parse(format!(
        "unsupported parameter syntax: {name}"
    )))
}

fn scan_sql_parameters(sql: &str, params: &mut ParamLayout) {
    enum State {
        Default,
        Single,
        Double,
        LineComment,
        BlockComment,
    }

    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut state = State::Default;
    while i < bytes.len() {
        match state {
            State::Default => match bytes[i] {
                b'\'' => {
                    state = State::Single;
                    i += 1;
                }
                b'"' => {
                    state = State::Double;
                    i += 1;
                }
                b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                    state = State::LineComment;
                    i += 2;
                }
                b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                    state = State::BlockComment;
                    i += 2;
                }
                b'?' => {
                    i += 1;
                    let start = i;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                    if i > start {
                        if let Ok(index) = sql[start..i].parse::<usize>()
                            && index > 0
                        {
                            params.push_numbered(index);
                        }
                    } else {
                        params.push_anonymous();
                    }
                }
                b':' | b'@' | b'$' => {
                    let prefix = bytes[i] as char;
                    i += 1;
                    let start = i;
                    while i < bytes.len() && is_param_char(bytes[i]) {
                        i += 1;
                    }
                    if i > start {
                        let name = format!("{prefix}{}", &sql[start..i]);
                        params.push_named(name);
                    }
                }
                _ => i += 1,
            },
            State::Single => {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        state = State::Default;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            State::Double => {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                    } else {
                        state = State::Default;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            State::LineComment => {
                if bytes[i] == b'\n' {
                    state = State::Default;
                }
                i += 1;
            }
            State::BlockComment => {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    state = State::Default;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        }
    }
}
