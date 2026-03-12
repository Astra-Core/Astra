use anyhow::{anyhow, bail, Context};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use tokio_postgres::NoTls;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresDestinationConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    #[serde(default, rename = "passwordRef")]
    pub password_ref: Option<String>,
    #[serde(default, rename = "sslMode")]
    pub ssl_mode: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default, rename = "tablePrefix")]
    pub table_prefix: Option<String>,
    #[serde(default, rename = "applicationName")]
    pub application_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawLoadChunkResult {
    pub object_key: String,
    pub table_name: String,
    pub rows_written: u64,
    pub skipped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawLoadReport {
    pub destination_kind: String,
    pub schema: String,
    pub applied_chunks: Vec<RawLoadChunkResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LoadChunkOutcome {
    rows_written: u64,
    skipped: bool,
}

#[derive(Debug, Clone)]
pub struct PostgresDestinationLoader {
    config: PostgresDestinationConfig,
}

impl PostgresDestinationLoader {
    pub fn from_spec(spec: &astra_yaml::AstraSpec) -> anyhow::Result<Self> {
        if spec.destination.kind != "postgres" {
            bail!(
                "expected destination.kind=postgres, got {}",
                spec.destination.kind
            );
        }
        let values = spec
            .destination
            .connection
            .as_ref()
            .context("destination.connection is required for postgres destination loading")?;
        Ok(Self {
            config: PostgresDestinationConfig {
                host: require_string(values, "host")?,
                port: require_u16(values, "port")?,
                database: require_string(values, "database")?,
                username: require_string(values, "username")?,
                password_ref: optional_string(values, "passwordRef")?,
                ssl_mode: optional_string(values, "sslMode")?,
                schema: optional_string(values, "schema")?,
                table_prefix: optional_string(values, "tablePrefix")?,
                application_name: optional_string(values, "applicationName")?,
            },
        })
    }

    pub fn config(&self) -> &PostgresDestinationConfig {
        &self.config
    }

    pub async fn load_local_stage_chunks(
        &self,
        chunks: Vec<(astra_runtime::StageChunk, Vec<u8>)>,
    ) -> anyhow::Result<RawLoadReport> {
        let mut pg_config = tokio_postgres::Config::new();
        pg_config.host(&self.config.host);
        pg_config.port(self.config.port);
        pg_config.dbname(&self.config.database);
        pg_config.user(&self.config.username);
        if let Some(application_name) = &self.config.application_name {
            pg_config.application_name(application_name);
        }
        if let Some(password_ref) = &self.config.password_ref {
            if let Some(password) = resolve_password_ref(password_ref)? {
                pg_config.password(password);
            }
        }

        let (mut client, connection) = pg_config
            .connect(NoTls)
            .await
            .context("failed to connect to Postgres destination")?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(?error, "postgres destination connection task exited");
            }
        });

        let schema = self
            .config
            .schema
            .clone()
            .unwrap_or_else(|| "astra_raw".to_string());
        ensure_metadata_tables(&client, &schema).await?;

        let mut applied_chunks = Vec::new();
        for (chunk, bytes) in chunks {
            let table_name = raw_table_name(
                &schema,
                self.config.table_prefix.as_deref().unwrap_or("raw_"),
                &chunk.stream_name,
            );
            let outcome = load_chunk(&mut client, &schema, &table_name, &chunk, &bytes).await?;
            applied_chunks.push(RawLoadChunkResult {
                object_key: chunk.object_key,
                table_name,
                rows_written: outcome.rows_written,
                skipped: outcome.skipped,
            });
        }

        Ok(RawLoadReport {
            destination_kind: "postgres".to_string(),
            schema,
            applied_chunks,
        })
    }
}

async fn ensure_metadata_tables(
    client: &tokio_postgres::Client,
    schema: &str,
) -> anyhow::Result<()> {
    client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema_ident};\n             CREATE TABLE IF NOT EXISTS {schema_ident}._applied_chunks (\n               object_key text PRIMARY KEY,\n               pipeline_name text NOT NULL,\n               stream_name text NOT NULL,\n               sequence bigint NOT NULL,\n               row_count bigint NOT NULL,\n               loaded_at timestamptz NOT NULL DEFAULT now()\n             );",
            schema_ident = quote_ident(schema)
        ))
        .await
        .with_context(|| format!("failed to initialize destination schema {}", schema))?;
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
                "INSERT INTO {schema_ident}._applied_chunks (object_key, pipeline_name, stream_name, sequence, row_count)\n                 VALUES ($1, $2, $3, $4, $5)\n                 ON CONFLICT (object_key) DO NOTHING",
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
        "CREATE TABLE IF NOT EXISTS {table_ident} (\n           _object_key text NOT NULL,\n           _sequence bigint NOT NULL,\n           _row_number bigint NOT NULL,\n           _loaded_at timestamptz NOT NULL DEFAULT now(),\n           _data jsonb NOT NULL,\n           PRIMARY KEY (_object_key, _row_number)\n         );",
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
                "INSERT INTO {table_ident} (_object_key, _sequence, _row_number, _data) VALUES ($1, $2, $3, $4)",
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
        .with_context(|| format!("failed to insert raw row {} into {}", row_number + 1, table_name))?;
    }

    txn.commit().await?;
    Ok(LoadChunkOutcome {
        rows_written,
        skipped: false,
    })
}

fn decode_jsonl_gzip(bytes: &[u8]) -> anyhow::Result<Vec<serde_json::Value>> {
    let decoder = GzDecoder::new(bytes);
    let reader = BufReader::new(decoder);
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

fn raw_table_name(schema: &str, prefix: &str, stream_name: &str) -> String {
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

fn require_string(
    values: &BTreeMap<String, serde_yaml::Value>,
    key: &str,
) -> anyhow::Result<String> {
    match values.get(key) {
        Some(serde_yaml::Value::String(value)) if !value.trim().is_empty() => Ok(value.clone()),
        Some(_) => bail!("destination.connection.{key} must be a non-empty string"),
        None => bail!("destination.connection.{key} is required"),
    }
}

fn optional_string(
    values: &BTreeMap<String, serde_yaml::Value>,
    key: &str,
) -> anyhow::Result<Option<String>> {
    match values.get(key) {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(serde_yaml::Value::String(value)) if value.trim().is_empty() => Ok(None),
        Some(serde_yaml::Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => bail!("destination.connection.{key} must be a string"),
    }
}

fn require_u16(values: &BTreeMap<String, serde_yaml::Value>, key: &str) -> anyhow::Result<u16> {
    match values.get(key) {
        Some(serde_yaml::Value::Number(value)) => value
            .as_u64()
            .and_then(|n| u16::try_from(n).ok())
            .ok_or_else(|| anyhow!("destination.connection.{key} must be a valid port")),
        Some(serde_yaml::Value::String(value)) => value
            .parse::<u16>()
            .with_context(|| format!("destination.connection.{key} must be a valid port")),
        Some(_) => bail!("destination.connection.{key} must be a valid port"),
        None => bail!("destination.connection.{key} is required"),
    }
}

fn resolve_password_ref(password_ref: &str) -> anyhow::Result<Option<String>> {
    if let Some(env_name) = password_ref.strip_prefix("env:") {
        return std::env::var(env_name)
            .ok()
            .filter(|value| !value.is_empty())
            .map(Some)
            .ok_or_else(|| anyhow!("environment variable {env_name} is not set for passwordRef"));
    }
    bail!("passwordRef currently supports env:NAME for local Postgres testing")
}

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
