mod connection;
mod discover;
mod snapshot;
pub mod types;

pub(crate) use connection::connect_destination;
pub use connection::{test_postgres_connection, ConnectionTestResult};
pub use types::*;

use crate::config_parser::{optional_string, require_string, require_u16};
use anyhow::bail;

// ---------------------------------------------------------------------------
// SQL identifier utilities — shared by discover and snapshot sub-modules.
// ---------------------------------------------------------------------------

/// Double-quote a Postgres identifier, escaping any embedded double-quotes.
pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Produce a fully-qualified, double-quoted table reference from `schema.table`.
pub(crate) fn quote_qualified_table(table_name: &str) -> String {
    let (schema, table) = split_table_name(table_name).expect("table name already validated");
    format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    )
}

/// Split `"schema.table"` into its two parts.
pub(crate) fn split_table_name(table_name: &str) -> anyhow::Result<(&str, &str)> {
    let parts: Vec<_> = table_name.split('.').collect();
    if parts.len() != 2 {
        bail!("table {table_name} must use schema.table format");
    }
    Ok((parts[0], parts[1]))
}

/// Normalise and deduplicate a list of table names in `schema.table` format.
pub(crate) fn normalize_tables(tables: &[String]) -> anyhow::Result<Vec<String>> {
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

// ---------------------------------------------------------------------------
// Spec parsing helpers (source-specific)
// ---------------------------------------------------------------------------

fn parse_connection(
    values: &std::collections::BTreeMap<String, serde_yaml::Value>,
) -> anyhow::Result<PostgresConnectionConfig> {
    let ctx = "source.connection";
    Ok(PostgresConnectionConfig {
        host: require_string(values, "host", ctx)?,
        port: require_u16(values, "port", ctx)?,
        database: require_string(values, "database", ctx)?,
        username: require_string(values, "username", ctx)?,
        password_ref: optional_string(values, "passwordRef", ctx)?,
        ssl_mode: optional_string(values, "sslMode", ctx)?,
        application_name: optional_string(values, "applicationName", ctx)?,
    })
}

fn parse_cdc_settings(
    values: Option<&std::collections::BTreeMap<String, serde_yaml::Value>>,
) -> anyhow::Result<Option<PostgresCdcSettings>> {
    let Some(values) = values else {
        return Ok(None);
    };
    let ctx = "source.cdc";
    Ok(Some(PostgresCdcSettings {
        slot_name: optional_string(values, "slotName", ctx)?,
        publication_name: optional_string(values, "publicationName", ctx)?,
    }))
}

// ---------------------------------------------------------------------------
// PostgresSource — public entry point for the source connector.
// ---------------------------------------------------------------------------

/// Source connector for PostgreSQL databases.
///
/// Supports schema discovery, connection testing, and both full and incremental
/// snapshot execution.
pub struct PostgresSource {
    config: PostgresSourceConfig,
}

impl PostgresSource {
    /// Build a `PostgresSource` from a validated [`astra_yaml::AstraSpec`].
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

    /// Return the snapshot plan derived from the spec (no I/O).
    pub fn snapshot_plan(&self) -> PostgresSnapshotPlan {
        let mode = self
            .config
            .snapshot
            .as_ref()
            .map(|s| format!("{:?}", s.mode).to_lowercase())
            .unwrap_or_else(|| "full".to_string());
        let chunk_size = self.config.snapshot.as_ref().and_then(|s| s.chunk_size);

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

    /// Test connectivity to this Postgres source.
    pub async fn test_connection(&self) -> ConnectionTestResult {
        connection::test_postgres_connection(&self.config.connection, &self.config.tables).await
    }

    /// Discover the schema for each configured (or requested) table.
    pub async fn discover(
        &self,
        options: PostgresDiscoverOptions,
    ) -> anyhow::Result<DiscoverReport> {
        let target_tables = if options.tables.is_empty() {
            self.config.tables.clone()
        } else {
            normalize_tables(&options.tables)?
        };

        let client = connection::connect(&self.config.connection).await?;
        let tables = discover::discover_tables(&client, &target_tables).await?;
        Ok(DiscoverReport {
            config: self.config.clone(),
            catalog: SourceCatalog {
                source_kind: "postgres".to_string(),
                tables,
            },
            snapshot_plan: self.snapshot_plan(),
        })
    }

    /// Execute a snapshot, encoding all rows as gzip-compressed JSONL chunks.
    pub async fn snapshot_to_jsonl_gzip(
        &self,
        options: SnapshotExecutionOptions,
    ) -> anyhow::Result<SnapshotExecutionReport> {
        let target_tables = if options.tables.is_empty() {
            self.config.tables.clone()
        } else {
            normalize_tables(&options.tables)?
        };

        let configured_chunk_size = options
            .chunk_size
            .or_else(|| self.config.snapshot.as_ref().and_then(|s| s.chunk_size));

        let client = connection::connect(&self.config.connection).await?;
        let (tables, table_progress) =
            snapshot::execute_snapshot(&client, &target_tables, &options, configured_chunk_size)
                .await?;

        Ok(SnapshotExecutionReport {
            source_kind: "postgres".to_string(),
            tables,
            table_progress,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_source_from_example_spec() {
        let raw = include_str!("../../../../examples/postgres-to-warehouse.astra.yaml");
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
        let raw = include_str!("../../../../examples/postgres-to-warehouse.astra.yaml");
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
