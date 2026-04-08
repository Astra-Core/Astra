use super::types::{LoadChunkOutcome, RawLoadChunkResult, RawLoadReport};
use crate::postgres::connect_destination;
use anyhow::Context;
use flate2::read::GzDecoder;
use std::io::{BufRead, BufReader};

pub(super) async fn load_chunks(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password_ref: Option<&str>,
    application_name: Option<&str>,
    schema: &str,
    table_prefix: &str,
    chunks: Vec<(astra_runtime::StageChunk, Vec<u8>)>,
) -> anyhow::Result<RawLoadReport> {
    let mut client = connect_destination(
        host,
        port,
        database,
        username,
        password_ref,
        application_name,
    )
    .await?;

    ensure_metadata_tables(&client, schema).await?;

    let mut applied_chunks = Vec::new();
    for (chunk, bytes) in chunks {
        let table_name = raw_table_name(schema, table_prefix, &chunk.stream_name);
        let outcome = load_chunk(&mut client, schema, &table_name, &chunk, &bytes).await?;
        applied_chunks.push(RawLoadChunkResult {
            object_key: chunk.object_key,
            table_name,
            rows_written: outcome.rows_written,
            skipped: outcome.skipped,
        });
    }

    Ok(RawLoadReport {
        destination_kind: "postgres".to_string(),
        schema: schema.to_string(),
        applied_chunks,
    })
}

async fn ensure_metadata_tables(
    client: &tokio_postgres::Client,
    schema: &str,
) -> anyhow::Result<()> {
    client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema_ident};\
             CREATE TABLE IF NOT EXISTS {schema_ident}._applied_chunks (\
               object_key text PRIMARY KEY,\
               pipeline_name text NOT NULL,\
               stream_name text NOT NULL,\
               sequence bigint NOT NULL,\
               row_count bigint NOT NULL,\
               loaded_at timestamptz NOT NULL DEFAULT now()\
             );",
            schema_ident = quote_ident(schema)
        ))
        .await
        .with_context(|| format!("failed to initialize destination schema {schema}"))?;
    Ok(())
}

async fn load_chunk(
    client: &mut tokio_postgres::Client,
    schema: &str,
    table_name: &str,
    chunk: &astra_runtime::StageChunk,
    bytes: &[u8],
) -> anyhow::Result<LoadChunkOutcome> {
    let txn = client.transaction().await?;
    let inserted = txn
        .execute(
            &format!(
                "INSERT INTO {schema_ident}._applied_chunks \
                 (object_key, pipeline_name, stream_name, sequence, row_count)\
                 VALUES ($1, $2, $3, $4, $5)\
                 ON CONFLICT (object_key) DO NOTHING",
                schema_ident = quote_ident(schema)
            ),
            &[
                &chunk.object_key,
                &chunk.pipeline_name,
                &chunk.stream_name,
                &(chunk.sequence as i64),
                &(chunk.row_count as i64),
            ],
        )
        .await?;

    if inserted == 0 {
        txn.rollback().await.ok();
        return Ok(LoadChunkOutcome {
            rows_written: 0,
            skipped: true,
        });
    }

    txn.batch_execute(&format!(
        "CREATE TABLE IF NOT EXISTS {table_ident} (\
           _object_key text NOT NULL,\
           _sequence bigint NOT NULL,\
           _row_number bigint NOT NULL,\
           _loaded_at timestamptz NOT NULL DEFAULT now(),\
           _data jsonb NOT NULL,\
           PRIMARY KEY (_object_key, _row_number)\
         );",
        table_ident = table_name
    ))
    .await
    .with_context(|| format!("failed to initialize raw table {table_name}"))?;

    let rows = decode_jsonl_gzip(bytes)?;
    let rows_written = rows.len() as u64;
    if rows_written != chunk.row_count {
        txn.execute(
            &format!(
                "UPDATE {schema_ident}._applied_chunks SET row_count = $2 WHERE object_key = $1",
                schema_ident = quote_ident(schema)
            ),
            &[&chunk.object_key, &(rows_written as i64)],
        )
        .await?;
    }

    for (row_number, value) in rows.into_iter().enumerate() {
        txn.execute(
            &format!(
                "INSERT INTO {table_ident} (_object_key, _sequence, _row_number, _data) \
                 VALUES ($1, $2, $3, $4)",
                table_ident = table_name
            ),
            &[
                &chunk.object_key,
                &(chunk.sequence as i64),
                &((row_number + 1) as i64),
                &value,
            ],
        )
        .await
        .with_context(|| {
            format!(
                "failed to insert raw row {} into {}",
                row_number + 1,
                table_name
            )
        })?;
    }

    txn.commit().await?;
    Ok(LoadChunkOutcome {
        rows_written,
        skipped: false,
    })
}

pub(super) fn decode_jsonl_gzip(bytes: &[u8]) -> anyhow::Result<Vec<serde_json::Value>> {
    let reader = BufReader::new(GzDecoder::new(bytes));
    let mut rows = Vec::new();
    for (line_number, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("failed to read gzipped JSONL line {}", line_number + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        rows.push(
            serde_json::from_str(trimmed)
                .with_context(|| format!("invalid JSONL payload on line {}", line_number + 1))?,
        );
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// SQL identifier helpers (destination-specific)
// ---------------------------------------------------------------------------

pub(super) fn raw_table_name(schema: &str, prefix: &str, stream_name: &str) -> String {
    format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident(&format!("{}{}", prefix, sanitize_ident(stream_name)))
    )
}

fn sanitize_ident(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_string()
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    #[test]
    fn decodes_jsonl_gzip_payload() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(
                br#"{"id":1}
{"id":2}
"#,
            )
            .unwrap();
        let bytes = encoder.finish().unwrap();
        let rows = decode_jsonl_gzip(&bytes).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[1]["id"], 2);
    }

    #[test]
    fn decode_jsonl_gzip_counts_non_empty_rows() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(
                br#"{"id":1}

{"id":2}
{"id":3}
"#,
            )
            .unwrap();
        let bytes = encoder.finish().unwrap();
        let rows = decode_jsonl_gzip(&bytes).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn sanitizes_stream_names_for_raw_tables() {
        assert_eq!(
            raw_table_name("astra_raw", "raw_", "public.orders"),
            "\"astra_raw\".\"raw_public_orders\""
        );
    }

    #[test]
    fn load_chunk_outcome_can_represent_applied_and_skipped_states() {
        assert_eq!(
            LoadChunkOutcome {
                rows_written: 3,
                skipped: false,
            },
            LoadChunkOutcome {
                rows_written: 3,
                skipped: false,
            }
        );
        assert_eq!(
            LoadChunkOutcome {
                rows_written: 0,
                skipped: true,
            },
            LoadChunkOutcome {
                rows_written: 0,
                skipped: true,
            }
        );
    }
}
