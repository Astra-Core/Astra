use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

pub const CRATE_NAME: &str = "astra-yaml";
const SUPPORTED_VERSION: &str = "v1alpha1";

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("unsupported spec version: {0}")]
    UnsupportedVersion(String),
    #[error("pipeline.name must not be empty")]
    EmptyPipelineName,
    #[error("pipeline.schedule must not be empty")]
    EmptySchedule,
    #[error("continuous schedule requires incremental or cdc mode")]
    InvalidContinuousSchedule,
    #[error("cdc mode requires a source kind that supports CDC")]
    CdcNotSupported,
    #[error("snapshot.chunkSize must be greater than zero")]
    InvalidChunkSize,
    #[error("destination.write.batchSize must be greater than zero")]
    InvalidBatchSize,
    #[error("unrecognized secret reference: {0}")]
    InvalidSecretReference(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AstraSpec {
    pub version: String,
    pub pipeline: Pipeline,
    pub source: Source,
    pub destination: Destination,
    pub runtime: Runtime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub name: String,
    pub mode: PipelineMode,
    pub schedule: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PipelineMode {
    Snapshot,
    Incremental,
    Cdc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub kind: String,
    pub connection: BTreeMap<String, serde_yaml::Value>,
    pub capture: Capture,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capture {
    #[serde(default)]
    pub tables: Vec<String>,
    #[serde(default)]
    pub streams: Vec<String>,
    #[serde(default)]
    pub snapshot: Option<Snapshot>,
    #[serde(default)]
    pub cdc: Option<BTreeMap<String, serde_yaml::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub mode: SnapshotMode,
    #[serde(default, rename = "chunkSize")]
    pub chunk_size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    Full,
    Incremental,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Destination {
    pub kind: String,
    #[serde(default)]
    pub connection: Option<BTreeMap<String, serde_yaml::Value>>,
    #[serde(default)]
    pub staging: Option<Staging>,
    pub write: WriteBehavior,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Staging {
    pub kind: String,
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteBehavior {
    pub mode: WriteMode,
    #[serde(default, rename = "batchSize")]
    pub batch_size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Append,
    Upsert,
    Merge,
    Replace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Runtime {
    #[serde(default)]
    pub parallelism: Option<Parallelism>,
    #[serde(default)]
    pub checkpointing: Option<Checkpointing>,
    #[serde(default, rename = "schemaEvolution")]
    pub schema_evolution: Option<SchemaEvolution>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Parallelism {
    #[serde(default)]
    pub tables: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpointing {
    #[serde(default, rename = "intervalSeconds")]
    pub interval_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvolution {
    #[serde(rename = "additiveChanges")]
    pub additive_changes: AdditiveChanges,
    #[serde(rename = "breakingChanges")]
    pub breaking_changes: BreakingChanges,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AdditiveChanges {
    AutoApply,
    Ignore,
    Pause,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum BreakingChanges {
    Pause,
    Ignore,
}

impl AstraSpec {
    pub fn parse_yaml(input: &str) -> anyhow::Result<Self> {
        Ok(serde_yaml::from_str(input)?)
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        Self::parse_yaml(&raw)
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.version != SUPPORTED_VERSION {
            return Err(ValidationError::UnsupportedVersion(self.version.clone()));
        }
        if self.pipeline.name.trim().is_empty() {
            return Err(ValidationError::EmptyPipelineName);
        }
        if self.pipeline.schedule.trim().is_empty() {
            return Err(ValidationError::EmptySchedule);
        }
        if self.pipeline.schedule == "continuous" && self.pipeline.mode == PipelineMode::Snapshot {
            return Err(ValidationError::InvalidContinuousSchedule);
        }
        if self.pipeline.mode == PipelineMode::Cdc && !supports_cdc(&self.source.kind) {
            return Err(ValidationError::CdcNotSupported);
        }
        if let Some(snapshot) = &self.source.capture.snapshot {
            if matches!(snapshot.chunk_size, Some(0)) {
                return Err(ValidationError::InvalidChunkSize);
            }
        }
        if matches!(self.destination.write.batch_size, Some(0)) {
            return Err(ValidationError::InvalidBatchSize);
        }
        for value in self.source.connection.values() {
            validate_secret_ref_value(value)?;
        }
        if let Some(connection) = &self.destination.connection {
            for value in connection.values() {
                validate_secret_ref_value(value)?;
            }
        }
        Ok(())
    }
}

fn supports_cdc(kind: &str) -> bool {
    matches!(kind, "postgres" | "mysql")
}

fn validate_secret_ref_value(value: &serde_yaml::Value) -> Result<(), ValidationError> {
    if let serde_yaml::Value::String(s) = value {
        if s.contains(":") && s.ends_with("_PASSWORD") && !is_supported_secret_ref(s) {
            return Err(ValidationError::InvalidSecretReference(s.clone()));
        }
    }
    Ok(())
}

fn is_supported_secret_ref(value: &str) -> bool {
    value.starts_with("env:") || value.starts_with("file:") || value.starts_with("vault:")
}

pub fn status() -> &'static str {
    "yaml models defined"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_validates_example() {
        let raw = include_str!("../../../examples/postgres-to-warehouse.astra.yaml");
        let spec = AstraSpec::parse_yaml(raw).expect("spec parses");
        spec.validate().expect("spec validates");
    }

    #[test]
    fn parses_destination_connection_when_present() {
        let raw = include_str!("../../../examples/postgres-to-postgres-raw.astra.yaml");
        let spec = AstraSpec::parse_yaml(raw).expect("spec parses");
        spec.validate().expect("spec validates");
        assert_eq!(spec.destination.kind, "postgres");
        assert!(spec.destination.connection.is_some());
    }
}
