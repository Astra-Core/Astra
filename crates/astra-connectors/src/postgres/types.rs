use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Low-level PostgreSQL connection parameters.
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

/// CDC-specific settings for logical replication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PostgresCdcSettings {
    #[serde(default, rename = "slotName")]
    pub slot_name: Option<String>,
    #[serde(default, rename = "publicationName")]
    pub publication_name: Option<String>,
}

/// Resolved configuration for a Postgres source connector instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSourceConfig {
    pub connection: PostgresConnectionConfig,
    pub tables: Vec<String>,
    #[serde(default)]
    pub snapshot: Option<astra_yaml::Snapshot>,
    #[serde(default)]
    pub cdc: Option<PostgresCdcSettings>,
}

/// Options passed to `PostgresSource::discover`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresDiscoverOptions {
    #[serde(default)]
    pub tables: Vec<String>,
}

/// Catalog of all discovered source tables.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceCatalog {
    pub source_kind: String,
    pub tables: Vec<SourceTable>,
}

/// Metadata for a single discovered table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceTable {
    pub schema: String,
    pub name: String,
    pub fully_qualified_name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<String>,
}

/// Metadata for a single column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

/// High-level snapshot plan produced by `PostgresSource::snapshot_plan`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresSnapshotPlan {
    pub source_kind: String,
    pub tables: Vec<SnapshotTablePlan>,
}

/// Per-table snapshot plan entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotTablePlan {
    pub table: String,
    pub sql: String,
    pub chunk_size: Option<u64>,
    pub mode: String,
}

/// Combined result of a discovery run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverReport {
    pub config: PostgresSourceConfig,
    pub catalog: SourceCatalog,
    pub snapshot_plan: PostgresSnapshotPlan,
}

/// Options passed to `PostgresSource::snapshot_to_jsonl_gzip`.
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
    #[serde(default)]
    pub cursor_field: Option<String>,
    /// Last observed cursor value per table loaded from the checkpoint ledger.
    #[serde(default)]
    pub last_cursor_by_table: BTreeMap<String, serde_json::Value>,
}

/// A single staged chunk of rows from one table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotTableChunk {
    pub table: String,
    pub sql: String,
    pub row_count: u64,
    pub sequence: u64,
    pub rows_jsonl_gzip: Vec<u8>,
}

/// Per-table progress summary after a snapshot run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotTableProgress {
    pub table: String,
    pub next_sequence: u64,
    pub finished: bool,
    pub rows_emitted: u64,
    /// Maximum cursor value observed across all staged rows for this table.
    pub max_cursor_value: Option<serde_json::Value>,
}

/// Aggregate result of a snapshot execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotExecutionReport {
    pub source_kind: String,
    pub tables: Vec<SnapshotTableChunk>,
    pub table_progress: Vec<SnapshotTableProgress>,
}
