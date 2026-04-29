use super::*;

pub(crate) fn project_row(
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

pub(crate) fn selection_passes(
    selection: &Option<Expr>,
    row: &SqlRow,
    bindings: &[Option<SqlValue>],
) -> Result<bool> {
    match selection {
        Some(expr) => Ok(is_truthy(&eval_scalar(expr, &row.context(), bindings)?)),
        None => Ok(true),
    }
}

pub(crate) fn compare_rows(left: &[SqlValue], right: &[SqlValue]) -> Ordering {
    for (l, r) in left.iter().zip(right.iter()) {
        let ord = compare_values(l, r);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    left.len().cmp(&right.len())
}

pub(crate) fn row_width(row: &[SqlValue]) -> usize {
    row.iter().map(row_width_value).sum()
}

pub(crate) fn row_width_value(value: &SqlValue) -> usize {
    match value {
        SqlValue::Null => 0,
        SqlValue::Integer(_) | SqlValue::Real(_) => 8,
        SqlValue::Text(value) => value.len(),
        SqlValue::Blob(value) => value.len(),
    }
}

pub(crate) fn compare_row_ordering(
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

pub(crate) fn eval_scalar(
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
                // SQLite semantics: compute the base IN as TRUE / FALSE / NULL,
                // then apply NOT only on TRUE / FALSE — NULL must propagate
                // through unchanged.
                //   `5 NOT IN (1, NULL)`  → NULL  (cannot prove 5 != NULL)
                //   `1 NOT IN (1, NULL)`  → FALSE (we found a match)
                //   `5 NOT IN (1, 2, 3)`  → TRUE  (no NULL, no match)
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
                let base_in: Option<bool> = if found {
                    Some(true)
                } else if saw_null {
                    None
                } else {
                    Some(false)
                };
                match (base_in, *negated) {
                    (Some(b), false) => SqlValue::Integer(if b { 1 } else { 0 }),
                    (Some(b), true) => SqlValue::Integer(if !b { 1 } else { 0 }),
                    (None, _) => SqlValue::Null,
                }
            }
        }
        Expr::Exists { subquery, negated } => {
            let rows = evaluate_subquery_rows(subquery, bindings)?;
            let exists = !rows.is_empty();
            SqlValue::Integer(if exists ^ *negated { 1 } else { 0 })
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let value = eval_scalar(expr, row, bindings)?;
            if matches!(value, SqlValue::Null) {
                SqlValue::Null
            } else {
                let rows = evaluate_subquery_rows(subquery, bindings)?;
                let mut found = false;
                let mut saw_null = false;
                for row in rows {
                    if row.len() != 1 {
                        return Err(Error::UnsupportedSql(
                            "IN subquery must return exactly one column".to_owned(),
                        ));
                    }
                    let candidate = row.into_iter().next().unwrap_or(SqlValue::Null);
                    match candidate {
                        SqlValue::Null => saw_null = true,
                        _ if compare_values(&value, &candidate) == Ordering::Equal => {
                            found = true;
                            break;
                        }
                        _ => {}
                    }
                }
                // Same NOT-IN-with-NULL semantics as InList: compute base IN as
                // TRUE / FALSE / NULL, then apply NOT only on TRUE / FALSE.
                let base_in: Option<bool> = if found {
                    Some(true)
                } else if saw_null {
                    None
                } else {
                    Some(false)
                };
                match (base_in, *negated) {
                    (Some(b), false) => SqlValue::Integer(if b { 1 } else { 0 }),
                    (Some(b), true) => SqlValue::Integer(if !b { 1 } else { 0 }),
                    (None, _) => SqlValue::Null,
                }
            }
        }
        Expr::Subquery(subquery) => {
            let rows = evaluate_subquery_rows(subquery, bindings)?;
            match rows.as_slice() {
                [] => SqlValue::Null,
                [row] if row.len() == 1 => row[0].clone(),
                [row] if row.is_empty() => SqlValue::Null,
                _ => {
                    return Err(Error::UnsupportedSql(
                        "scalar subquery must return exactly one row and one column".to_owned(),
                    ));
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

pub(crate) fn truthy_opt(value: &SqlValue) -> Option<bool> {
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
        BinaryOperator::Plus => arithmetic(
            left_value,
            right_value,
            |a, b| Some(a.wrapping_add(b)),
            |a, b| Some(a + b),
        )?,
        BinaryOperator::Minus => arithmetic(
            left_value,
            right_value,
            |a, b| Some(a.wrapping_sub(b)),
            |a, b| Some(a - b),
        )?,
        BinaryOperator::Multiply => arithmetic(
            left_value,
            right_value,
            |a, b| Some(a.wrapping_mul(b)),
            |a, b| Some(a * b),
        )?,
        BinaryOperator::Divide => arithmetic(
            left_value,
            right_value,
            // SQLite: division by zero returns NULL, not an error.
            |a, b| if b == 0 { None } else { a.checked_div(b) },
            |a, b| {
                if b == 0.0 {
                    None
                } else {
                    Some(a / b)
                }
            },
        )?,
        BinaryOperator::Modulo => arithmetic(
            left_value,
            right_value,
            // SQLite: modulo by zero returns NULL, not an error.
            |a, b| if b == 0 { None } else { a.checked_rem(b) },
            |a, b| {
                if b == 0.0 {
                    None
                } else {
                    Some(a % b)
                }
            },
        )?,
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
        BinaryOperator::StringConcat => {
            // SQLite: NULL || anything = NULL (NULL propagates through ||).
            // Previously this coerced NULL via `value_to_string` to empty
            // string, yielding `'x'` instead of NULL for `NULL || 'x'`.
            if matches!(left_value, SqlValue::Null) || matches!(right_value, SqlValue::Null) {
                SqlValue::Null
            } else {
                SqlValue::Text(Arc::from(format!(
                    "{}{}",
                    value_to_string(&left_value),
                    value_to_string(&right_value)
                )))
            }
        }
        other => {
            return Err(Error::UnsupportedSql(format!(
                "unsupported binary op {other:?}"
            )));
        }
    })
}

pub(crate) fn compare_binary(
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

pub(crate) fn sql_truth_result(value: SqlValue) -> SqlValue {
    SqlValue::Integer(if is_truthy(&value) { 1 } else { 0 })
}

pub(crate) fn sql_truth_result_not(value: SqlValue) -> SqlValue {
    SqlValue::Integer(if !is_truthy(&value) { 1 } else { 0 })
}

pub(crate) fn sql_false_result(value: SqlValue) -> SqlValue {
    match value {
        SqlValue::Null => SqlValue::Null,
        other => SqlValue::Integer(if !is_truthy(&other) { 1 } else { 0 }),
    }
}

pub(crate) fn sql_false_result_not(value: SqlValue) -> SqlValue {
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

fn evaluate_subquery_rows(
    subquery: &sqlparser::ast::Query,
    bindings: &[Option<SqlValue>],
) -> Result<Vec<Vec<SqlValue>>> {
    let Some(conn) = current_connection() else {
        return Err(Error::TransactionState(
            "subquery evaluation requires an active connection",
        ));
    };
    let schema = conn.engine().schema_snapshot();
    let template = crate::parser::bind_query(
        conn,
        schema,
        conn.schema_epoch(),
        "<subquery>",
        subquery.clone(),
    )?;
    materialize_prepared_rows(conn, &template, bindings)
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
    // SQLite GLOB grammar:
    //   *           — matches zero or more characters
    //   ?           — matches exactly one character
    //   [abc]       — character class (any of)
    //   [a-z]       — character range
    //   [!abc]      — negated class (matches one char NOT in abc)
    //   [^abc]      — also a negated class (compatibility)
    //   anything else — literal (case-sensitive, unlike LIKE)
    // An unterminated `[` is treated as a literal `[`.
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
                b'[' => {
                    if let Some((matched, advance)) = match_glob_class(&pattern[pi..], text.get(ti))
                    {
                        if !matched {
                            return false;
                        }
                        ti += 1;
                        pi += advance;
                    } else {
                        // Unterminated class: treat `[` as a literal.
                        if ti >= text.len() || text[ti] != b'[' {
                            return false;
                        }
                        ti += 1;
                        pi += 1;
                    }
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

/// Try to match a `[...]` character class at the start of `pattern` against
/// the optional next byte of the input. Returns `(matched, pattern_advance)`
/// on success; `None` when the class is unterminated (caller should treat
/// the leading `[` as a literal).
fn match_glob_class(pattern: &[u8], target: Option<&u8>) -> Option<(bool, usize)> {
    debug_assert!(pattern.first() == Some(&b'['));
    let mut idx = 1usize;
    let negate = matches!(pattern.get(idx), Some(&b'!') | Some(&b'^'));
    if negate {
        idx += 1;
    }
    let class_start = idx;
    let mut matched = false;
    let target_byte = match target {
        Some(&b) => b,
        None => 0,
    };

    // SQLite allows a literal `]` only as the first character of the class.
    // So `[]abc]` matches `]`, `a`, `b`, or `c`.
    if pattern.get(idx) == Some(&b']') {
        if target.is_some() && target_byte == b']' {
            matched = true;
        }
        idx += 1;
    }

    while idx < pattern.len() && pattern[idx] != b']' {
        let lo = pattern[idx];
        if idx + 2 < pattern.len() && pattern[idx + 1] == b'-' && pattern[idx + 2] != b']' {
            let hi = pattern[idx + 2];
            if target.is_some() && target_byte >= lo.min(hi) && target_byte <= lo.max(hi) {
                matched = true;
            }
            idx += 3;
        } else {
            if target.is_some() && target_byte == lo {
                matched = true;
            }
            idx += 1;
        }
    }

    if idx >= pattern.len() {
        // No closing `]` — pattern is malformed. Caller falls back to literal.
        if class_start == idx {
            return None;
        }
        return None;
    }

    let final_match = if target.is_none() {
        false
    } else if negate {
        !matched
    } else {
        matched
    };

    Some((final_match, idx + 1))
}

fn round_function(values: &[SqlValue]) -> Result<SqlValue> {
    // SQLite: round(NULL, ...) and round(x, NULL) return NULL.
    if values.is_empty() || matches!(values[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if values.len() > 1 && matches!(values[1], SqlValue::Null) {
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
    if !input.len().is_multiple_of(2) {
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
        "length" => match values.first() {
            // SQLite: length(NULL) is NULL, not 0.
            Some(SqlValue::Null) | None => Ok(SqlValue::Null),
            Some(other) => Ok(SqlValue::Integer(value_to_string(other).len() as i64)),
        },
        "lower" => match values.first() {
            Some(SqlValue::Null) | None => Ok(SqlValue::Null),
            Some(other) => Ok(SqlValue::Text(Arc::from(
                value_to_string(other).to_ascii_lowercase(),
            ))),
        },
        "upper" => match values.first() {
            Some(SqlValue::Null) | None => Ok(SqlValue::Null),
            Some(other) => Ok(SqlValue::Text(Arc::from(
                value_to_string(other).to_ascii_uppercase(),
            ))),
        },
        "abs" => match values.first() {
            // SQLite: abs(NULL) is NULL, not an error.
            Some(SqlValue::Null) | None => Ok(SqlValue::Null),
            Some(SqlValue::Integer(v)) => Ok(SqlValue::Integer(v.wrapping_abs())),
            Some(SqlValue::Real(v)) => Ok(SqlValue::Real(v.abs())),
            // Coerce text / blob to numeric then abs (SQLite implicit-numeric).
            Some(SqlValue::Text(_)) | Some(SqlValue::Blob(_)) => {
                match numeric_value(values.first().unwrap()) {
                    Ok(v) => Ok(SqlValue::Real(v.abs())),
                    Err(_) => Ok(SqlValue::Real(0.0)),
                }
            }
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
        "hex" => match values.first() {
            // SQLite: hex(NULL) is NULL, not the empty string.
            Some(SqlValue::Null) | None => Ok(SqlValue::Null),
            Some(other) => Ok(SqlValue::Text(Arc::from(hex_value(other)))),
        },
        "quote" => Ok(SqlValue::Text(Arc::from(quote_value(
            values.first().unwrap_or(&SqlValue::Null),
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
        "typeof" => Ok(SqlValue::Text(Arc::from(match values.first() {
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

pub(crate) fn cast_value(
    value: SqlValue,
    data_type: &sqlparser::ast::DataType,
) -> Result<SqlValue> {
    // SQLite: CAST(NULL AS anything) is NULL.
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let type_name = data_type.to_string().to_ascii_lowercase();

    // CAST AS BLOB: text becomes its UTF-8 bytes; numerics become their
    // textual representation as bytes.
    if type_name.contains("blob") {
        return Ok(match value {
            SqlValue::Blob(_) => value,
            SqlValue::Text(s) => SqlValue::Blob(Arc::from(s.as_bytes())),
            other => SqlValue::Blob(Arc::from(value_to_string(&other).into_bytes())),
        });
    }

    if type_name.contains("text") || type_name.contains("char") || type_name.contains("clob") {
        // CAST AS TEXT: numbers become their text form; blobs become UTF-8
        // (lossy if invalid bytes); text is unchanged.
        return Ok(match value {
            SqlValue::Text(_) => value,
            SqlValue::Integer(v) => SqlValue::Text(Arc::from(v.to_string())),
            SqlValue::Real(v) => SqlValue::Text(Arc::from(v.to_string())),
            SqlValue::Blob(v) => SqlValue::Text(Arc::from(String::from_utf8_lossy(&v).into_owned())),
            SqlValue::Null => SqlValue::Null,
        });
    }

    if type_name.contains("real") || type_name.contains("floa") || type_name.contains("doub") {
        return Ok(SqlValue::Real(cast_to_real(&value)));
    }

    if type_name.contains("int") {
        // SQLite truncates real toward zero (not floor / round):
        //   CAST(3.7 AS INTEGER)  → 3
        //   CAST(-3.7 AS INTEGER) → -3
        return Ok(SqlValue::Integer(cast_to_integer(&value)));
    }

    // CAST AS NUMERIC (or any unrecognized type): return integer if it parses
    // cleanly, otherwise real, otherwise the integer prefix.
    if type_name.contains("numeric") {
        return Ok(cast_to_numeric(&value));
    }

    Ok(value)
}

/// SQLite-style implicit text → integer conversion: parse the longest valid
/// integer prefix; if no valid digits, return 0. Real values truncate toward
/// zero; blobs are interpreted as their UTF-8 text.
fn cast_to_integer(value: &SqlValue) -> i64 {
    match value {
        SqlValue::Null => 0,
        SqlValue::Integer(v) => *v,
        // Truncate toward zero — matches SQLite (not floor / round).
        SqlValue::Real(v) => {
            if v.is_nan() {
                0
            } else if *v > i64::MAX as f64 {
                i64::MAX
            } else if *v < i64::MIN as f64 {
                i64::MIN
            } else {
                *v as i64
            }
        }
        SqlValue::Text(s) => parse_integer_prefix(s),
        SqlValue::Blob(b) => parse_integer_prefix(&String::from_utf8_lossy(b)),
    }
}

fn cast_to_real(value: &SqlValue) -> f64 {
    match value {
        SqlValue::Null => 0.0,
        SqlValue::Integer(v) => *v as f64,
        SqlValue::Real(v) => *v,
        SqlValue::Text(s) => parse_real_prefix(s),
        SqlValue::Blob(b) => parse_real_prefix(&String::from_utf8_lossy(b)),
    }
}

fn cast_to_numeric(value: &SqlValue) -> SqlValue {
    match value {
        SqlValue::Null => SqlValue::Null,
        SqlValue::Integer(_) | SqlValue::Real(_) => value.clone(),
        SqlValue::Text(t) => parse_numeric_text(t.as_ref()),
        SqlValue::Blob(b) => parse_numeric_text(&String::from_utf8_lossy(b)),
    }
}

fn parse_numeric_text(text: &str) -> SqlValue {
    let trimmed = text.trim();
    if let Ok(v) = trimmed.parse::<i64>() {
        SqlValue::Integer(v)
    } else if let Ok(v) = trimmed.parse::<f64>() {
        SqlValue::Real(v)
    } else {
        SqlValue::Integer(parse_integer_prefix(trimmed))
    }
}

/// Parse the longest valid integer prefix of `s`, mirroring SQLite's
/// `CAST(<text> AS INTEGER)`. Stops at the first non-digit character.
/// Leading whitespace and a single optional sign are accepted.
fn parse_integer_prefix(s: &str) -> i64 {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut idx = 0usize;
    let mut neg = false;
    if let Some(&first) = bytes.first() {
        if first == b'+' {
            idx = 1;
        } else if first == b'-' {
            idx = 1;
            neg = true;
        }
    }
    let start = idx;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == start {
        return 0;
    }
    let digits = &s[start..idx];
    let mag: i64 = digits.parse().unwrap_or(i64::MAX);
    if neg { mag.wrapping_neg() } else { mag }
}

/// Parse the longest valid real-number prefix of `s` (sign, digits,
/// fractional part, optional exponent). Returns 0.0 on no match.
fn parse_real_prefix(s: &str) -> f64 {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut idx = 0usize;
    if let Some(&first) = bytes.first()
        && (first == b'+' || first == b'-')
    {
        idx = 1;
    }
    let mut saw_digit = false;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
        saw_digit = true;
    }
    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
            saw_digit = true;
        }
    }
    if saw_digit && idx < bytes.len() && (bytes[idx] == b'e' || bytes[idx] == b'E') {
        let mut after_e = idx + 1;
        if after_e < bytes.len() && (bytes[after_e] == b'+' || bytes[after_e] == b'-') {
            after_e += 1;
        }
        let exp_start = after_e;
        while after_e < bytes.len() && bytes[after_e].is_ascii_digit() {
            after_e += 1;
        }
        if after_e > exp_start {
            idx = after_e;
        }
    }
    if !saw_digit {
        return 0.0;
    }
    s[..idx].parse::<f64>().unwrap_or(0.0)
}

fn negate(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Integer(v) => Ok(SqlValue::Integer(-v)),
        SqlValue::Real(v) => Ok(SqlValue::Real(-v)),
        SqlValue::Null => Ok(SqlValue::Null),
        _ => Err(Error::DatatypeMismatch),
    }
}

pub(crate) fn arithmetic(
    left: SqlValue,
    right: SqlValue,
    int_op: impl FnOnce(i64, i64) -> Option<i64>,
    real_op: impl FnOnce(f64, f64) -> Option<f64>,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    // Closures may return None (e.g. divide / modulo by zero) — in that case
    // SQLite returns NULL rather than raising an error or panicking.
    fn lift_int(opt: Option<i64>) -> SqlValue {
        match opt {
            Some(v) => SqlValue::Integer(v),
            None => SqlValue::Null,
        }
    }
    fn lift_real(opt: Option<f64>) -> SqlValue {
        match opt {
            Some(v) => SqlValue::Real(v),
            None => SqlValue::Null,
        }
    }
    match (left, right) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => Ok(lift_int(int_op(a, b))),
        (SqlValue::Integer(a), SqlValue::Real(b)) => Ok(lift_real(real_op(a as f64, b))),
        (SqlValue::Real(a), SqlValue::Integer(b)) => Ok(lift_real(real_op(a, b as f64))),
        (SqlValue::Real(a), SqlValue::Real(b)) => Ok(lift_real(real_op(a, b))),
        (SqlValue::Text(a), SqlValue::Text(b)) => {
            let a = a
                .trim()
                .parse::<f64>()
                .map_err(|_| Error::DatatypeMismatch)?;
            let b = b
                .trim()
                .parse::<f64>()
                .map_err(|_| Error::DatatypeMismatch)?;
            Ok(lift_real(real_op(a, b)))
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

pub(crate) fn value_to_string(value: &SqlValue) -> String {
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
        RowContext::Upsert { current, .. } => lookup_table_column(current, name),
        RowContext::Joined(rows) => {
            let mut found = None;
            for row in rows.iter() {
                if row.row.is_none() {
                    continue;
                }
                if let Ok(value) = lookup_joined_row_column(row, name) {
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
                if row_matches_joined_qualifier(row, qualifier) {
                    let value = lookup_joined_row_column(row, name)?;
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
        RowContext::Upsert { current, excluded } => {
            if row_matches_qualifier(current, qualifier) {
                lookup_table_column(current, name)
            } else if qualifier.eq_ignore_ascii_case("excluded") {
                lookup_excluded_column(current.table.as_ref(), excluded, name)
            } else {
                Err(Error::UnknownColumn(format!("{qualifier}.{name}")))
            }
        }
        RowContext::SqliteSchema(row) => match qualifier.to_ascii_lowercase().as_str() {
            "sqlite_schema" | "sqlite_master" => lookup_schema_column(row, name),
            _ => Err(Error::UnknownColumn(format!("{qualifier}.{name}"))),
        },
        RowContext::Empty => Err(Error::UnknownColumn(format!("{qualifier}.{name}"))),
    }
}

fn row_matches_qualifier(row: &TableRow, qualifier: &str) -> bool {
    if let Some(alias) = &row.alias
        && alias.as_ref().eq_ignore_ascii_case(qualifier)
    {
        return true;
    }
    row.table.name.to_string().eq_ignore_ascii_case(qualifier)
}

fn row_matches_joined_qualifier(row: &JoinedRow, qualifier: &str) -> bool {
    if let Some(alias) = &row.alias
        && alias.as_ref().eq_ignore_ascii_case(qualifier)
    {
        return true;
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

fn lookup_joined_row_column(row: &JoinedRow, name: &str) -> Result<SqlValue> {
    match &row.row {
        Some(present) => lookup_table_column(present, name),
        None => {
            if name.eq_ignore_ascii_case("rowid")
                || name.eq_ignore_ascii_case("_rowid_")
                || name.eq_ignore_ascii_case("oid")
            {
                return Ok(SqlValue::Null);
            }
            row.table
                .columns
                .iter()
                .position(|col| col.folded.as_ref().eq_ignore_ascii_case(name))
                .ok_or_else(|| Error::UnknownColumn(name.to_owned()))?;
            Ok(SqlValue::Null)
        }
    }
}

fn lookup_excluded_column(table: &TableDef, excluded: &[SqlValue], name: &str) -> Result<SqlValue> {
    if name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
    {
        if let Some(alias) = table.rowid_alias_column
            && let Some(value) = excluded.get(alias as usize)
        {
            return Ok(value.clone());
        }
        return Err(Error::UnknownColumn(name.to_owned()));
    }
    let idx = table
        .columns
        .iter()
        .position(|col| col.folded.as_ref().eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::UnknownColumn(name.to_owned()))?;
    Ok(excluded.get(idx).cloned().unwrap_or(SqlValue::Null))
}

pub(crate) fn unique_key_bytes(
    table_id: u64,
    constraint_id: u64,
    values: &[SqlValue],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&table_id.to_le_bytes());
    out.extend_from_slice(&constraint_id.to_le_bytes());
    let refs = values.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    encode_record(&refs, &mut out).map_err(|_| Error::DatatypeMismatch)?;
    Ok(out)
}

pub(crate) fn key_values_equal(left: &[SqlValue], right: &[SqlValue]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(a, b)| compare_values(a, b) == Ordering::Equal)
}

pub(crate) fn encode_sql_row(table_id: u64, values: &[SqlValue]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut refs = Vec::with_capacity(values.len() + 1);
    refs.push(ValueRef::Integer(table_id as i64));
    refs.extend(values.iter().map(|value| value.as_ref()));
    encode_record(&refs, &mut out).map_err(|_| Error::DatatypeMismatch)?;
    Ok(out)
}

pub(crate) fn decode_sql_row(bytes: &[u8]) -> Result<Option<(u64, Vec<SqlValue>)>> {
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

pub(crate) fn scalar_to_usize(value: &SqlValue) -> Result<usize> {
    match value {
        SqlValue::Integer(v) => Ok((*v).max(0) as usize),
        SqlValue::Real(v) => Ok((*v).max(0.0) as usize),
        SqlValue::Null => Ok(0),
        _ => Err(Error::DatatypeMismatch),
    }
}

#[derive(Clone)]
pub(crate) struct TableRow {
    pub(crate) rowid: RowId,
    pub(crate) values: Vec<SqlValue>,
    pub(crate) table: Arc<TableDef>,
    pub(crate) alias: Option<Arc<str>>,
}

#[derive(Clone)]
pub(crate) struct JoinedRow {
    pub(crate) table: Arc<TableDef>,
    pub(crate) alias: Option<Arc<str>>,
    pub(crate) row: Option<TableRow>,
}

pub(crate) struct TableRowSource<'a> {
    pub(crate) values: &'a [SqlValue],
}

impl RowValueSource for TableRowSource<'_> {
    fn value_at(&self, col: u16) -> Option<OwnedValue> {
        self.values.get(col as usize).cloned()
    }
}

#[derive(Clone)]
pub(crate) enum SqlRow {
    Table(TableRow),
    Joined(Vec<JoinedRow>),
    SqliteSchema(SqliteSchemaRow),
    Static(Vec<SqlValue>),
    Empty,
}

pub(crate) enum RowContext<'a> {
    Table(&'a TableRow),
    Joined(&'a [JoinedRow]),
    Upsert {
        current: &'a TableRow,
        excluded: &'a [SqlValue],
    },
    SqliteSchema(&'a SqliteSchemaRow),
    Empty,
}

impl SqlRow {
    pub(crate) fn context(&self) -> RowContext<'_> {
        match self {
            SqlRow::Table(row) => RowContext::Table(row),
            SqlRow::Joined(rows) => RowContext::Joined(rows),
            SqlRow::SqliteSchema(row) => RowContext::SqliteSchema(row),
            SqlRow::Static(_) => RowContext::Empty,
            SqlRow::Empty => RowContext::Empty,
        }
    }

    pub(crate) fn values(&self) -> Result<Vec<SqlValue>> {
        match self {
            SqlRow::Table(row) => Ok(row.values.clone()),
            SqlRow::Joined(rows) => Ok(rows
                .iter()
                .flat_map(|row| match &row.row {
                    Some(present) => present.values.clone(),
                    None => vec![SqlValue::Null; row.table.columns.len()],
                })
                .collect::<Vec<_>>()),
            SqlRow::SqliteSchema(row) => Ok(vec![
                SqlValue::Text(Arc::from(row.type_name.as_ref())),
                SqlValue::Text(Arc::from(row.name.as_ref())),
                SqlValue::Text(Arc::from(row.tbl_name.as_ref())),
                SqlValue::Integer(row.rootpage as i64),
                SqlValue::Text(Arc::from(row.sql.as_ref())),
            ]),
            SqlRow::Static(values) => Ok(values.clone()),
            SqlRow::Empty => Ok(Vec::new()),
        }
    }
}
