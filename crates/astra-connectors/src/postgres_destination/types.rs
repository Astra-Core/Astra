use crate::postgres::types::PostgresConnectionConfig;
use serde::{Deserialize, Serialize};

/// Configuration for a Postgres destination connector.
///
/// The `connection` field is the same type used by the source connector so that
/// the two share a single definition for connection parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresDestinationConfig {
    pub connection: PostgresConnectionConfig,
    /// Target schema for raw tables.  Defaults to `astra_raw`.
    #[serde(default)]
    pub schema: Option<String>,
    /// Prefix prepended to raw table names.  Defaults to `raw_`.
    #[serde(default, rename = "tablePrefix")]
    pub table_prefix: Option<String>,
}

/// Result of loading one staged chunk into the destination.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawLoadChunkResult {
    pub object_key: String,
    pub table_name: String,
    pub rows_written: u64,
    pub skipped: bool,
}

/// Aggregate result of a raw-load operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawLoadReport {
    pub destination_kind: String,
    pub schema: String,
    pub applied_chunks: Vec<RawLoadChunkResult>,
}

/// Options for a batch load operation.
#[derive(Debug)]
pub(super) struct LoadChunkRequest {
    pub schema: String,
    pub table_prefix: String,
    pub chunks: Vec<(astra_runtime::StageChunk, Vec<u8>)>,
}

/// Internal outcome of processing a single chunk (not exposed publicly).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LoadChunkOutcome {
    pub rows_written: u64,
    pub skipped: bool,
}
