use super::*;

pub(crate) fn analyze_database(conn: &Connection, plan: &AnalyzePlan) -> Result<()> {
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
            None => schema.tables.to_vec(),
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

pub(crate) fn execute_explain(
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

pub(crate) fn build_table_stats(
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
            row_count.div_ceil(64).max(1)
        },
        avg_row_bytes,
        analyzed_at_csn: preserved
            .as_ref()
            .map(|stats| stats.analyzed_at_csn)
            .unwrap_or(redlinedb_kernel::format::Csn::ZERO),
        data_change_count: preserved.map(|stats| stats.data_change_count).unwrap_or(0),
    })
}

pub(crate) fn build_column_stats(
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

pub(crate) fn build_index_stats(
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
            (rows.len() as u64).div_ceil(64).max(1)
        },
        height: if rows.is_empty() { 0 } else { 1 },
        distinct_prefix_counts,
        avg_key_bytes,
        clustering_factor: if rows.is_empty() { 0.0 } else { 1.0 },
    }
}

pub(crate) fn sample_rows(
    cfg: &crate::connection::StatsConfig,
    rows: &[Vec<SqlValue>],
) -> Vec<Vec<SqlValue>> {
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

pub(crate) fn stable_row_score(row: &[SqlValue]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for value in row {
        stats_value_key(value).hash(&mut hasher);
    }
    hasher.finish()
}

pub(crate) fn build_histogram(
    cfg: &crate::connection::StatsConfig,
    values: &[SqlValue],
    total_rows: usize,
) -> Vec<HistogramBucket> {
    if values.is_empty() || cfg.histogram_buckets == 0 {
        return Vec::new();
    }
    let bucket_count = cfg.histogram_buckets.min(values.len()).max(1);
    let mut buckets = Vec::with_capacity(bucket_count);
    let chunk = values.len().div_ceil(bucket_count);
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

pub(crate) fn stats_value_key(value: &SqlValue) -> String {
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

pub(crate) fn execute_update(
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
            if let Some(alias) = plan.table.rowid_alias_column
                && let Some(slot) = values.get_mut(alias as usize)
                && matches!(slot, SqlValue::Null)
            {
                *slot = SqlValue::Integer(new_rowid.0 as i64);
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

pub(crate) fn execute_delete(
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

pub(crate) fn insert_row(
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

pub(crate) fn build_row(
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

pub(crate) fn build_default_row(table: &Arc<TableDef>) -> Result<Vec<SqlValue>> {
    build_default_values(table, vec![SqlValue::Null; table.columns.len()])
}

pub(crate) fn build_default_values(
    table: &Arc<TableDef>,
    mut values: Vec<SqlValue>,
) -> Result<Vec<SqlValue>> {
    for (idx, column) in table.columns.iter().enumerate() {
        if matches!(values[idx], SqlValue::Null)
            && let Some(default) = &column.default_value
        {
            values[idx] = default.clone();
        }
    }
    apply_row_affinity(table, values)
}

pub(crate) fn apply_row_affinity(table: &TableDef, values: Vec<SqlValue>) -> Result<Vec<SqlValue>> {
    let mut out = values;
    for (idx, column) in table.columns.iter().enumerate() {
        out[idx] = apply_affinity(out[idx].clone(), column.affinity)
            .map_err(|_| Error::DatatypeMismatch)?;
    }
    Ok(out)
}

pub(crate) fn apply_constraints(table: &TableDef, values: &[SqlValue]) -> Result<()> {
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

pub(crate) fn ensure_unique_constraints(
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

pub(crate) fn choose_rowid_for_insert(
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

pub(crate) fn choose_rowid_for_update(
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

pub(crate) fn collect_table_rows(
    engine: &Engine,
    tx: &mut Txn,
    table: &Arc<TableDef>,
) -> Result<Vec<TableRow>> {
    collect_table_rows_with_alias(engine, tx, table, None)
}

pub(crate) fn collect_table_rows_with_alias(
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

pub(crate) fn collect_join_rows(
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

pub(crate) fn collect_table_rowids(
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

pub(crate) fn load_table_row_by_rowid(
    engine: &Engine,
    tx: &mut Txn,
    table: &Arc<TableDef>,
    rowid: RowId,
) -> Result<Option<TableRow>> {
    if let Some(payload) = engine.get(tx, rowid)?
        && let Some((table_id, values)) = decode_sql_row(&payload)?
        && table_id == table.table_id.0
    {
        return Ok(Some(TableRow {
            rowid,
            values,
            table: Arc::clone(table),
            alias: None,
        }));
    }
    Ok(None)
}

pub(crate) fn selection_rowid_eq(
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
    let Expr::BinaryOp { left, op, right } = expr else {
        return Ok(None);
    };
    if !matches!(op, BinaryOperator::Eq) {
        return Ok(None);
    }
    let expr_rowid = if let Some(value) = rowid_eq_side(table, left, right, bindings, &rowid_col)? {
        value
    } else if let Some(value) = rowid_eq_side(table, right, left, bindings, &rowid_col)? {
        value
    } else {
        return Ok(None);
    };
    Ok(Some(expr_rowid))
}

pub(crate) fn rowid_eq_side(
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
