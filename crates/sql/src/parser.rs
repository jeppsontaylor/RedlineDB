use std::sync::Arc;

#[allow(unused_imports)]
use redlinedb_kernel::catalog::{
    ColumnConstraintSpec, ColumnSpec, ConflictAction, CreateIndexSpec, CreateTableSpec, DbName,
    DropIndexSpec, DropTableSpec, ExprAst, IndexColumnSpec, IndexOrigin, OwnedValue, QualifiedName,
    SchemaEpoch, SchemaSnapshot, SortDir, TableConstraintSpec, lookup_index, lookup_table,
};
#[allow(unused_imports)]
use sqlparser::ast::{
    AlterTableOperation, Analyze as SqlAnalyze, AnalyzeFormat, AnalyzeFormatKind, BinaryOperator,
    ColumnDef, ColumnOption, ConflictTarget, Distinct, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, IndexColumn, JoinConstraint, JoinOperator, LimitClause,
    ObjectName, ObjectNamePart, OnConflictAction, OnInsert, OrderByExpr, OrderByKind, Query,
    SelectItem, SetExpr, SetOperator, SetQuantifier, SqliteOnConflict, Statement as SqlStatement,
    TableFactor, TableObject, TableWithJoins, UnaryOperator, Value, ValueWithSpan,
};
#[allow(unused_imports)]
use sqlparser::dialect::SQLiteDialect;
#[allow(unused_imports)]
use sqlparser::parser::Parser;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::session::BeginMode;
#[allow(unused_imports)]
use crate::statement::*;
use crate::value::SqlValue;

mod helpers;
#[allow(unused_imports)]
pub(crate) use helpers::*;
mod ddl;
#[allow(unused_imports)]
pub(crate) use ddl::*;
mod dml;
#[allow(unused_imports)]
pub(crate) use dml::*;
mod pragma;
#[allow(unused_imports)]
pub(crate) use pragma::*;
mod select;
#[allow(unused_imports)]
pub(crate) use select::*;

pub(crate) fn is_pragma_sql(sql: &str) -> bool {
    sql.trim_start()
        .trim_end_matches(';')
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("pragma")
}

pub fn parse_prepared_template(conn: &Connection, sql: &str) -> Result<PreparedTemplate> {
    let trimmed = sql.trim();
    let lower = trimmed.trim_end_matches(';').trim().to_ascii_lowercase();
    let engine = conn.engine();
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

    if let Some(template) = parse_pragma_template(conn, trimmed, &lower, schema_epoch, &schema)? {
        return Ok(template);
    }

    let dialect = SQLiteDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;
    if statements.len() != 1 {
        return Err(Error::UnsupportedSql(
            "only single-statement prepares are supported".to_owned(),
        ));
    }

    bind_statement(conn, schema, schema_epoch, trimmed, statements.remove(0))
}

fn bind_statement(
    conn: &Connection,
    schema: Arc<SchemaSnapshot>,
    schema_epoch: SchemaEpoch,
    sql: &str,
    statement: SqlStatement,
) -> Result<PreparedTemplate> {
    match statement {
        SqlStatement::Query(query) => bind_query(conn, schema, schema_epoch, sql, *query),
        SqlStatement::Insert(insert) => bind_insert(conn, schema, schema_epoch, sql, insert),
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
        SqlStatement::AlterTable(alter_table) => bind_alter_table(
            schema_epoch,
            sql,
            alter_table.name,
            alter_table.if_exists,
            alter_table.only,
            alter_table.operations,
        ),
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
            conn,
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
        param_layout: crate::statement::ParamLayout::default(),
        output_columns: Arc::from([]),
        readonly,
        kind,
    }
}
