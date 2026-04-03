use anyhow::{anyhow, bail, Context};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio_postgres::{types::Type, Client, NoTls};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    #[serde(default, rename = "passwordRef")]
    pub password_ref: Option<String>,
    #[serde(default, rename = "sslMode")]
    pub ssl_mode: Option<String>,
    #[serde(default)]
    pub application_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PostgresCdcSettings {
    #[serde(default, rename = "slotName")]
    pub slot_name: Option<String>,
    #[serde(default, rename = "publicationName")]
    pub publication_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSourceConfig {
    pub connection: PostgresConnectionConfig,
    pub tables: Vec<String>,
    #[serde(default)]
    pub snapshot: Option<astra_yaml::Snapshot>,
    #[serde(default)]
    pub cdc: Option<PostgresCdcSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresDiscoverOptions {
    #[serde(default)]
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceCatalog {
    pub source_kind: String,
    pub tables: Vec<SourceTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceTable {
    pub schema: String,
    pub name: String,
    pub fully_qualified_name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresSnapshotPlan {
    pub source_kind: String,
    pub tables: Vec<SnapshotTablePlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotTablePlan {
    pub table: String,
    pub sql: String,
    pub chunk_size: Option<u64>,
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverReport {
    pub config: PostgresSourceConfig,
    pub catalog: SourceCatalog,
    pub snapshot_plan: PostgresSnapshotPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SnapshotExecutionOptions {
    #[serde(default)]
    pub tables: Vec<String>,
    #[serde(default)]
    pub max_rows_per_table: Option<u64>,
    #[serde(default)]
    pub chunk_size: Option<u64>,
    #[serde(default)]
    pub start_sequence_by_table: BTreeMap<String, u64>,
    /// Column name used as the incremental cursor (e.g. `updated_at`, `id`).
    /// When set, the snapshot query filters `WHERE cursor_field > last_cursor_by_table[table]`
    /// and orders by `cursor_field` ascending (keyset pagination).
    #[serde(default)]
    pub cursor_field: Option<String>,
    /// Last observed cursor value per table, loaded from the checkpoint ledger.
    /// Absent or `None` for a given table means this is the initial load for that table.
    #[serde(default)]
    pub last_cursor_by_table: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotTableChunk {
    pub table: String,
    pub sql: String,
    pub row_count: u64,
    pub sequence: u64,
    pub rows_jsonl_gzip: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotTableProgress {
    pub table: String,
    pub next_sequence: u64,
    pub finished: bool,
    pub rows_emitted: u64,
    /// The maximum cursor value observed across all rows staged in this run for this table.
    /// `None` when no cursor field is configured or no rows were emitted.
    pub max_cursor_value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotExecutionReport {
    pub source_kind: String,
    pub tables: Vec<SnapshotTableChunk>,
    pub table_progress: Vec<SnapshotTableProgress>,
}

pub struct PostgresSource {
    config: PostgresSourceConfig,
}

impl PostgresSource {
    pub fn from_spec(spec: &astra_yaml::AstraSpec) -> anyhow::Result<Self> {
        if spec.source.kind != "postgres" {
            bail!("expected source.kind=postgres, got {}", spec.source.kind);
        }

        let connection = parse_connection(&spec.source.connection)?;
        let tables = normalize_tables(&spec.source.capture.tables)?;
        if tables.is_empty() {
            bail!("postgres source requires at least one captured table");
        }

        let cdc = parse_cdc_settings(spec.source.capture.cdc.as_ref())?;

        Ok(Self {
            config: PostgresSourceConfig {
                connection,
                tables,
                snapshot: spec.source.capture.snapshot.clone(),
                cdc,
            },
        })
    }

    pub fn config(&self) -> &PostgresSourceConfig {
        &self.config
    }

    pub fn snapshot_plan(&self) -> PostgresSnapshotPlan {
        let mode = self
            .config
            .snapshot
            .as_ref()
            .map(|snapshot| format!("{:?}", snapshot.mode).to_lowercase())
            .unwrap_or_else(|| "full".to_string());
        let chunk_size = self
            .config
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.chunk_size);

        PostgresSnapshotPlan {
            source_kind: "postgres".to_string(),
            tables: self
                .config
                .tables
                .iter()
                .map(|table| SnapshotTablePlan {
                    table: table.clone(),
                    sql: format!("SELECT * FROM {}", quote_qualified_table(table)),
                    chunk_size,
                    mode: mode.clone(),
                })
                .collect(),
        }
    }

    pub async fn discover(
        &self,
        options: PostgresDiscoverOptions,
    ) -> anyhow::Result<DiscoverReport> {
        let target_tables = if options.tables.is_empty() {
            self.config.tables.clone()
        } else {
            normalize_tables(&options.tables)?
        };

        let client = connect(&self.config.connection).await?;
        let tables = discover_tables(&client, &target_tables).await?;
        Ok(DiscoverReport {
            config: self.config.clone(),
            catalog: SourceCatalog {
                source_kind: "postgres".to_string(),
                tables,
            },
            snapshot_plan: self.snapshot_plan(),
        })
    }

    pub async fn snapshot_to_jsonl_gzip(
        &self,
        options: SnapshotExecutionOptions,
    ) -> anyhow::Result<SnapshotExecutionReport> {
        let target_tables = if options.tables.is_empty() {
            self.config.tables.clone()
        } else {
            normalize_tables(&options.tables)?
        };

        let client = connect(&self.config.connection).await?;
        let mut tables = Vec::new();
        let mut table_progress = Vec::new();
        let configured_chunk_size = options.chunk_size.or_else(|| {
            self.config
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.chunk_size)
        });

        for table_name in target_tables {
            let start_sequence = options
                .start_sequence_by_table
                .get(&table_name)
                .copied()
                .unwrap_or(0);
            let mut next_sequence = start_sequence;
            let mut rows_emitted = 0_u64;
            let mut finished = false;
            let mut max_cursor_value: Option<serde_json::Value> = None;

            let last_cursor = options
                .cursor_field
                .as_deref()
                .and_then(|_| options.last_cursor_by_table.get(&table_name));

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
                        &table_name,
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
                    let rows_jsonl_gzip = encode_rows_as_gzip_jsonl(&rows, &table_name)?;
                    tables.push(SnapshotTableChunk {
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
                let sql = build_snapshot_json_sql(
                    &table_name,
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
                let rows_jsonl_gzip = encode_rows_as_gzip_jsonl(&rows, &table_name)?;
                tables.push(SnapshotTableChunk {
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

            table_progress.push(SnapshotTableProgress {
                table: table_name,
                next_sequence,
                finished,
                rows_emitted,
                max_cursor_value,
            });
        }

        Ok(SnapshotExecutionReport {
            source_kind: "postgres".to_string(),
            tables,
            table_progress,
        })
    }
}

fn parse_connection(
    values: &BTreeMap<String, serde_yaml::Value>,
) -> anyhow::Result<PostgresConnectionConfig> {
    Ok(PostgresConnectionConfig {
        host: require_string(values, "host")?,
        port: require_u16(values, "port")?,
        database: require_string(values, "database")?,
        username: require_string(values, "username")?,
        password_ref: optional_string(values, "passwordRef")?,
        ssl_mode: optional_string(values, "sslMode")?,
        application_name: optional_string(values, "applicationName")?,
    })
}

fn parse_cdc_settings(
    values: Option<&BTreeMap<String, serde_yaml::Value>>,
) -> anyhow::Result<Option<PostgresCdcSettings>> {
    let Some(values) = values else {
        return Ok(None);
    };

    Ok(Some(PostgresCdcSettings {
        slot_name: optional_string(values, "slotName")?,
        publication_name: optional_string(values, "publicationName")?,
    }))
}

fn require_string(
    values: &BTreeMap<String, serde_yaml::Value>,
    key: &str,
) -> anyhow::Result<String> {
    match values.get(key) {
        Some(serde_yaml::Value::String(value)) if !value.trim().is_empty() => Ok(value.clone()),
        Some(_) => bail!("source.connection.{key} must be a non-empty string"),
        None => bail!("source.connection.{key} is required"),
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
        Some(_) => bail!("source.connection.{key} must be a string"),
    }
}

fn require_u16(values: &BTreeMap<String, serde_yaml::Value>, key: &str) -> anyhow::Result<u16> {
    match values.get(key) {
        Some(serde_yaml::Value::Number(value)) => value
            .as_u64()
            .and_then(|n| u16::try_from(n).ok())
            .ok_or_else(|| anyhow!("source.connection.{key} must be a valid port")),
        Some(serde_yaml::Value::String(value)) => value
            .parse::<u16>()
            .with_context(|| format!("source.connection.{key} must be a valid port")),
        Some(_) => bail!("source.connection.{key} must be a valid port"),
        None => bail!("source.connection.{key} is required"),
    }
}

fn normalize_tables(tables: &[String]) -> anyhow::Result<Vec<String>> {
    let mut normalized = Vec::new();
    for table in tables {
        let trimmed = table.trim();
        if trimmed.is_empty() {
            bail!("captured table names must not be empty");
        }
        let parts: Vec<_> = trimmed.split('.').collect();
        if parts.len() != 2 || parts.iter().any(|part| part.trim().is_empty()) {
            bail!("captured table '{trimmed}' must use schema.table format");
        }
        normalized.push(format!("{}.{}", parts[0].trim(), parts[1].trim()));
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

async fn connect(config: &PostgresConnectionConfig) -> anyhow::Result<Client> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.host(&config.host);
    pg_config.port(config.port);
    pg_config.dbname(&config.database);
    pg_config.user(&config.username);
    if let Some(application_name) = &config.application_name {
        pg_config.application_name(application_name);
    }

    let password = match &config.password_ref {
        Some(password_ref) => resolve_password_ref(password_ref)?,
        None => None,
    };
    if let Some(password) = password.as_deref() {
        pg_config.password(password);
    }

    let (client, connection) = pg_config
        .connect(NoTls)
        .await
        .context("failed to connect to Postgres source")?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(?error, "postgres source connection task exited");
        }
    });

    Ok(client)
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

async fn discover_tables(
    client: &Client,
    table_names: &[String],
) -> anyhow::Result<Vec<SourceTable>> {
    let mut tables = Vec::new();

    for table_name in table_names {
        let (schema, table) = split_table_name(table_name)?;
        let columns = client
            .query(
                r#"
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
                "#,
                &[&schema, &table],
            )
            .await
            .with_context(|| format!("failed to inspect columns for {table_name}"))?;

        if columns.is_empty() {
            bail!("table {table_name} was not found in Postgres");
        }

        let primary_key_rows = client
            .query(
                r#"
                SELECT a.attname AS column_name
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                WHERE i.indisprimary = true AND n.nspname = $1 AND c.relname = $2
                ORDER BY array_position(i.indkey, a.attnum)
                "#,
                &[&schema, &table],
            )
            .await
            .with_context(|| format!("failed to inspect primary key for {table_name}"))?;

        tables.push(SourceTable {
            schema: schema.to_string(),
            name: table.to_string(),
            fully_qualified_name: format!("{schema}.{table}"),
            columns: columns
                .into_iter()
                .map(|row| ColumnSchema {
                    name: row.get("column_name"),
                    data_type: row.get("data_type"),
                    is_nullable: matches!(row.get::<_, String>("is_nullable").as_str(), "YES"),
                })
                .collect(),
            primary_key: primary_key_rows
                .into_iter()
                .map(|row| row.get("column_name"))
                .collect(),
        });
    }

    Ok(tables)
}

fn split_table_name(table_name: &str) -> anyhow::Result<(&str, &str)> {
    let parts: Vec<_> = table_name.split('.').collect();
    if parts.len() != 2 {
        bail!("table {table_name} must use schema.table format");
    }
    Ok((parts[0], parts[1]))
}

fn quote_qualified_table(table_name: &str) -> String {
    let (schema, table) = split_table_name(table_name).expect("table name already validated");
    format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    )
}

fn build_snapshot_json_sql(
    table_name: &str,
    cursor_field: Option<&str>,
    last_cursor_value: Option<&serde_json::Value>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> anyhow::Result<String> {
    let table = quote_qualified_table(table_name);

    let inner = if let Some(cursor) = cursor_field {
        let cursor_ident = quote_ident(cursor);
        match last_cursor_value {
            Some(last) => {
                let literal = cursor_value_to_sql_literal(last)?;
                format!("SELECT * FROM {table} WHERE {cursor_ident} > {literal} ORDER BY {cursor_ident}")
            }
            None => {
                // Initial incremental load — full scan ordered by cursor so keyset pagination works
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
/// Returns `None` if the field is absent or null in every row.
fn extract_max_cursor(
    rows: &[tokio_postgres::Row],
    cursor_field: &str,
) -> Option<serde_json::Value> {
    rows.iter()
        .filter_map(|row| {
            let record: serde_json::Value = row.try_get("record").ok()?;
            record.get(cursor_field).cloned()
        })
        .filter(|v| !v.is_null())
        .max_by(compare_cursor_values)
}

fn compare_cursor_values(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
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

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn encode_rows_as_gzip_jsonl(
    rows: &[tokio_postgres::Row],
    table_name: &str,
) -> anyhow::Result<Vec<u8>> {
    let mut jsonl = Vec::new();
    for row in rows {
        let value: serde_json::Value = read_json_value(row, "record")?;
        serde_json::to_writer(&mut jsonl, &value)
            .with_context(|| format!("failed to encode staged row for {table_name}"))?;
        jsonl.push(b'\n');
    }

    gzip_bytes(&jsonl)
}

fn read_json_value(row: &tokio_postgres::Row, column: &str) -> anyhow::Result<serde_json::Value> {
    let idx = row
        .columns()
        .iter()
        .position(|candidate| candidate.name() == column)
        .ok_or_else(|| anyhow!("column {column} was not returned from snapshot query"))?;

    match *row.columns()[idx].type_() {
        Type::JSON | Type::JSONB => Ok(row.get(idx)),
        _ => bail!("snapshot query column {column} was not json/jsonb"),
    }
}

fn gzip_bytes(input: &[u8]) -> anyhow::Result<Vec<u8>> {
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
    fn builds_source_from_example_spec() {
        let raw = include_str!("../../../examples/postgres-to-warehouse.astra.yaml");
        let spec = astra_yaml::AstraSpec::parse_yaml(raw).expect("spec parses");
        spec.validate().expect("spec validates");

        let source = PostgresSource::from_spec(&spec).expect("postgres source builds");
        assert_eq!(source.config.tables, vec!["public.orders", "public.users"]);
        assert_eq!(source.config.connection.port, 5432);
        assert_eq!(source.config.cdc, None);
    }

    #[test]
    fn builds_source_with_cdc_settings_when_present() {
        let raw = r#"
version: v1alpha1
pipeline:
  name: postgres-cdc
  mode: cdc
  schedule: continuous
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
    passwordRef: env:POSTGRES_PASSWORD
  capture:
    tables:
      - public.users
    cdc:
      slotName: astra_slot
      publicationName: astra_publication
destination:
  kind: snowflake
  staging:
    kind: s3
    bucket: astra-staging
  write:
    mode: merge
runtime: {}
"#;
        let spec = astra_yaml::AstraSpec::parse_yaml(raw).expect("spec parses");
        spec.validate().expect("spec validates");

        let source = PostgresSource::from_spec(&spec).expect("postgres source builds");
        assert_eq!(
            source.config.cdc,
            Some(PostgresCdcSettings {
                slot_name: Some("astra_slot".to_string()),
                publication_name: Some("astra_publication".to_string()),
            })
        );
    }

    #[test]
    fn snapshot_plan_quotes_table_names() {
        let raw = include_str!("../../../examples/postgres-to-warehouse.astra.yaml");
        let spec = astra_yaml::AstraSpec::parse_yaml(raw).expect("spec parses");
        let source = PostgresSource::from_spec(&spec).expect("postgres source builds");

        let plan = source.snapshot_plan();
        assert_eq!(plan.tables.len(), 2);
        assert_eq!(plan.tables[0].sql, "SELECT * FROM \"public\".\"orders\"");
        assert_eq!(plan.tables[0].chunk_size, Some(50000));
        assert_eq!(plan.tables[0].mode, "incremental");
    }

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
        // No prior cursor value — full scan ordered by cursor field.
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
    fn extract_max_cursor_returns_max_string_value() {
        use flate2::{write::GzEncoder, Compression};
        use std::io::Write;

        // We can't easily create tokio_postgres::Row in a unit test, so test the
        // helper functions directly.
        let values = vec![
            serde_json::json!("2024-03-01T00:00:00Z"),
            serde_json::json!("2024-06-01T00:00:00Z"),
            serde_json::json!("2024-01-01T00:00:00Z"),
        ];
        let max = values
            .iter()
            .filter(|v| !v.is_null())
            .max_by(compare_cursor_values)
            .cloned();
        assert_eq!(max, Some(serde_json::json!("2024-06-01T00:00:00Z")));

        // Numeric comparison
        let nums = vec![
            serde_json::json!(10),
            serde_json::json!(3),
            serde_json::json!(99),
        ];
        let max_num = nums
            .iter()
            .filter(|v| !v.is_null())
            .max_by(compare_cursor_values)
            .cloned();
        assert_eq!(max_num, Some(serde_json::json!(99)));

        // gzip helper still round-trips (reuse existing coverage)
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(b"{\"id\":1}\n").unwrap();
        let _ = encoder.finish().unwrap();
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
    fn gzip_helper_round_trips() {
        let encoded = gzip_bytes(b"{\"id\":1}\n{\"id\":2}\n").expect("gzip works");
        let mut decoded = String::new();
        GzDecoder::new(encoded.as_slice())
            .read_to_string(&mut decoded)
            .expect("gzip decodes");
        assert_eq!(decoded, "{\"id\":1}\n{\"id\":2}\n");
    }

    #[test]
    fn rejects_invalid_table_names() {
        let error = normalize_tables(&["users".to_string()]).expect_err("invalid table rejected");
        assert!(error.to_string().contains("schema.table"));
    }
}
