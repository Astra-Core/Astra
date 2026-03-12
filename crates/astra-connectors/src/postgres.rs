use anyhow::{anyhow, bail, Context};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio_postgres::{Client, NoTls};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_source_from_example_spec() {
        let raw = include_str!("../../../examples/postgres-to-warehouse.astra.yaml");
        let spec = astra_yaml::AstraSpec::parse_yaml(raw).expect("spec parses");
        spec.validate().expect("spec validates");

        let source = PostgresSource::from_spec(&spec).expect("postgres source builds");
        assert_eq!(source.config.tables, vec!["public.orders", "public.users"]);
        assert_eq!(source.config.connection.port, 5432);
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
    fn rejects_invalid_table_names() {
        let error = normalize_tables(&["users".to_string()]).expect_err("invalid table rejected");
        assert!(error.to_string().contains("schema.table"));
    }
}
