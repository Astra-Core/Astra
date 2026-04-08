use super::types::{SnapshotExecutionOptions, SnapshotTableChunk, SnapshotTableProgress};
use anyhow::{anyhow, bail, Context};
use tokio_postgres::{types::Type, Client, Row};

/// Execute a snapshot against `target_tables`, returning all staged chunks and per-table progress.
pub(super) async fn execute_snapshot(
    client: &Client,
    target_tables: &[String],
    options: &SnapshotExecutionOptions,
    configured_chunk_size: Option<u64>,
) -> anyhow::Result<(Vec<SnapshotTableChunk>, Vec<SnapshotTableProgress>)> {
    let mut chunks = Vec::new();
    let mut progress = Vec::new();

    for table_name in target_tables {
        let start_sequence = options
            .start_sequence_by_table
            .get(table_name.as_str())
            .copied()
            .unwrap_or(0);
        let mut next_sequence = start_sequence;
        let mut rows_emitted = 0_u64;
        let mut finished = false;
        let mut max_cursor_value: Option<serde_json::Value> = None;

        let last_cursor = options
            .cursor_field
            .as_deref()
            .and_then(|_| options.last_cursor_by_table.get(table_name.as_str()));

        if let Some(base_chunk_size) = configured_chunk_size {
            if base_chunk_size == 0 {
                bail!("snapshot chunk size must be greater than zero");
            }

            loop {
                let remaining = options
                    .max_rows_per_table
                    .map(|max_rows| max_rows.saturating_sub(rows_emitted));

                if matches!(remaining, Some(0)) {
                    break;
                }

                let limit = remaining
                    .map(|rows| rows.min(base_chunk_size))
                    .unwrap_or(base_chunk_size);

                let sql = build_snapshot_json_sql(
                    table_name,
                    options.cursor_field.as_deref(),
                    max_cursor_value.as_ref().or(last_cursor),
                    Some(limit),
                    // offset-based pagination only for full (no cursor) mode
                    if options.cursor_field.is_none() {
                        Some(next_sequence.saturating_mul(base_chunk_size))
                    } else {
                        None
                    },
                )?;

                let rows = client
                    .query(&sql, &[])
                    .await
                    .with_context(|| format!("failed to snapshot table {table_name}"))?;

                if rows.is_empty() {
                    finished = true;
                    break;
                }

                let row_count = rows.len() as u64;
                if let Some(cursor_field) = options.cursor_field.as_deref() {
                    if let Some(v) = extract_max_cursor(&rows, cursor_field) {
                        max_cursor_value = Some(v);
                    }
                }
                let rows_jsonl_gzip = encode_rows_as_gzip_jsonl(&rows, table_name)?;
                chunks.push(SnapshotTableChunk {
                    table: table_name.clone(),
                    sql,
                    row_count,
                    sequence: next_sequence,
                    rows_jsonl_gzip,
                });

                rows_emitted = rows_emitted.saturating_add(row_count);
                next_sequence += 1;

                if row_count < limit {
                    finished = true;
                    break;
                }
            }
        } else {
            // Unchunked: read the whole table (or up to max_rows) in one query.
            let sql = build_snapshot_json_sql(
                table_name,
                options.cursor_field.as_deref(),
                last_cursor,
                options.max_rows_per_table,
                None,
            )?;
            let rows = client
                .query(&sql, &[])
                .await
                .with_context(|| format!("failed to snapshot table {table_name}"))?;
            let row_count = rows.len() as u64;
            if let Some(cursor_field) = options.cursor_field.as_deref() {
                if let Some(v) = extract_max_cursor(&rows, cursor_field) {
                    max_cursor_value = Some(v);
                }
            }
            let rows_jsonl_gzip = encode_rows_as_gzip_jsonl(&rows, table_name)?;
            chunks.push(SnapshotTableChunk {
                table: table_name.clone(),
                sql,
                row_count,
                sequence: next_sequence,
                rows_jsonl_gzip,
            });
            rows_emitted = row_count;
            next_sequence += 1;
            finished = options
                .max_rows_per_table
                .map(|limit| row_count < limit)
                .unwrap_or(true);
        }

        progress.push(SnapshotTableProgress {
            table: table_name.clone(),
            next_sequence,
            finished,
            rows_emitted,
            max_cursor_value,
        });
    }

    Ok((chunks, progress))
}

/// Build the snapshot SQL that wraps each row in a JSONB record.
pub(super) fn build_snapshot_json_sql(
    table_name: &str,
    cursor_field: Option<&str>,
    last_cursor_value: Option<&serde_json::Value>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> anyhow::Result<String> {
    let table = super::quote_qualified_table(table_name);

    let inner = if let Some(cursor) = cursor_field {
        let cursor_ident = super::quote_ident(cursor);
        match last_cursor_value {
            Some(last) => {
                let literal = cursor_value_to_sql_literal(last)?;
                format!("SELECT * FROM {table} WHERE {cursor_ident} > {literal} ORDER BY {cursor_ident}")
            }
            None => {
                // Initial incremental load — full scan ordered by cursor so keyset pagination works.
                format!("SELECT * FROM {table} ORDER BY {cursor_ident}")
            }
        }
    } else {
        format!("SELECT * FROM {table}")
    };

    let mut sql = format!("SELECT to_jsonb(snapshot_row) AS record FROM ({inner}) AS snapshot_row");
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    Ok(sql)
}

/// Render a JSON cursor value as a SQL literal safe for embedding in a query.
/// Only scalar types that make sense as cursor values are supported.
fn cursor_value_to_sql_literal(value: &serde_json::Value) -> anyhow::Result<String> {
    match value {
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::String(s) => {
            // Escape single quotes by doubling them — safe for embedding as a SQL string literal.
            Ok(format!("'{}'", s.replace('\'', "''")))
        }
        other => bail!("cursor value must be a number or string, got: {}", other),
    }
}

/// Extract the maximum value of `cursor_field` from a batch of JSONB rows.
fn extract_max_cursor(rows: &[Row], cursor_field: &str) -> Option<serde_json::Value> {
    rows.iter()
        .filter_map(|row| {
            let record: serde_json::Value = row.try_get("record").ok()?;
            record.get(cursor_field).cloned()
        })
        .filter(|v| !v.is_null())
        .max_by(compare_cursor_values)
}

pub(super) fn compare_cursor_values(
    a: &serde_json::Value,
    b: &serde_json::Value,
) -> std::cmp::Ordering {
    match (a, b) {
        (serde_json::Value::Number(x), serde_json::Value::Number(y)) => {
            let xf = x.as_f64().unwrap_or(f64::NEG_INFINITY);
            let yf = y.as_f64().unwrap_or(f64::NEG_INFINITY);
            xf.partial_cmp(&yf).unwrap_or(std::cmp::Ordering::Equal)
        }
        (serde_json::Value::String(x), serde_json::Value::String(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Serialize `rows` to JSONL and compress with gzip.
fn encode_rows_as_gzip_jsonl(rows: &[Row], table_name: &str) -> anyhow::Result<Vec<u8>> {
    let mut jsonl = Vec::new();
    for row in rows {
        let value: serde_json::Value = read_json_value(row, "record")?;
        serde_json::to_writer(&mut jsonl, &value)
            .with_context(|| format!("failed to encode staged row for {table_name}"))?;
        jsonl.push(b'\n');
    }
    gzip_bytes(&jsonl)
}

fn read_json_value(row: &Row, column: &str) -> anyhow::Result<serde_json::Value> {
    let idx = row
        .columns()
        .iter()
        .position(|c| c.name() == column)
        .ok_or_else(|| anyhow!("column {column} was not returned from snapshot query"))?;

    match *row.columns()[idx].type_() {
        Type::JSON | Type::JSONB => Ok(row.get(idx)),
        _ => bail!("snapshot query column {column} was not json/jsonb"),
    }
}

pub(super) fn gzip_bytes(input: &[u8]) -> anyhow::Result<Vec<u8>> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(input)
        .context("failed to gzip snapshot rows for staging")?;
    encoder
        .finish()
        .context("failed to finalize gzipped snapshot rows")
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    #[test]
    fn snapshot_json_sql_full_mode_with_limit_and_offset() {
        assert_eq!(
            build_snapshot_json_sql("public.orders", None, None, Some(25), Some(50))
                .expect("sql builds"),
            "SELECT to_jsonb(snapshot_row) AS record FROM (SELECT * FROM \"public\".\"orders\") AS snapshot_row LIMIT 25 OFFSET 50"
        );
    }

    #[test]
    fn snapshot_json_sql_omits_offset_when_not_requested() {
        assert_eq!(
            build_snapshot_json_sql("public.orders", None, None, Some(25), None)
                .expect("sql builds"),
            "SELECT to_jsonb(snapshot_row) AS record FROM (SELECT * FROM \"public\".\"orders\") AS snapshot_row LIMIT 25"
        );
    }

    #[test]
    fn snapshot_json_sql_incremental_initial_load() {
        assert_eq!(
            build_snapshot_json_sql("public.orders", Some("updated_at"), None, Some(1000), None)
                .expect("sql builds"),
            "SELECT to_jsonb(snapshot_row) AS record FROM (SELECT * FROM \"public\".\"orders\" ORDER BY \"updated_at\") AS snapshot_row LIMIT 1000"
        );
    }

    #[test]
    fn snapshot_json_sql_incremental_with_string_cursor() {
        let last = serde_json::json!("2024-06-01T00:00:00Z");
        assert_eq!(
            build_snapshot_json_sql(
                "public.orders",
                Some("updated_at"),
                Some(&last),
                Some(1000),
                None
            )
            .expect("sql builds"),
            "SELECT to_jsonb(snapshot_row) AS record FROM (SELECT * FROM \"public\".\"orders\" WHERE \"updated_at\" > '2024-06-01T00:00:00Z' ORDER BY \"updated_at\") AS snapshot_row LIMIT 1000"
        );
    }

    #[test]
    fn snapshot_json_sql_incremental_with_integer_cursor() {
        let last = serde_json::json!(42);
        assert_eq!(
            build_snapshot_json_sql("public.orders", Some("id"), Some(&last), Some(500), None)
                .expect("sql builds"),
            "SELECT to_jsonb(snapshot_row) AS record FROM (SELECT * FROM \"public\".\"orders\" WHERE \"id\" > 42 ORDER BY \"id\") AS snapshot_row LIMIT 500"
        );
    }

    #[test]
    fn cursor_value_sql_literal_escapes_single_quotes() {
        let v = serde_json::json!("it's a test");
        assert_eq!(
            cursor_value_to_sql_literal(&v).expect("literal builds"),
            "'it''s a test'"
        );
    }

    #[test]
    fn compare_cursor_values_numbers() {
        let nums = vec![
            serde_json::json!(10),
            serde_json::json!(3),
            serde_json::json!(99),
        ];
        let max = nums
            .iter()
            .filter(|v| !v.is_null())
            .max_by(|a, b| compare_cursor_values(*a, *b))
            .cloned();
        assert_eq!(max, Some(serde_json::json!(99)));
    }

    #[test]
    fn compare_cursor_values_strings() {
        let values = vec![
            serde_json::json!("2024-03-01T00:00:00Z"),
            serde_json::json!("2024-06-01T00:00:00Z"),
            serde_json::json!("2024-01-01T00:00:00Z"),
        ];
        let max = values
            .iter()
            .filter(|v| !v.is_null())
            .max_by(|a, b| compare_cursor_values(*a, *b))
            .cloned();
        assert_eq!(max, Some(serde_json::json!("2024-06-01T00:00:00Z")));
    }

    #[test]
    fn gzip_helper_round_trips() {
        let encoded = gzip_bytes(b"{\"id\":1}\n{\"id\":2}\n").expect("gzip works");
        let mut decoded = String::new();
        GzDecoder::new(encoded.as_slice())
            .read_to_string(&mut decoded)
            .expect("gzip decodes");
        assert_eq!(decoded, "{\"id\":1}\n{\"id\":2}\n");
    }
}
