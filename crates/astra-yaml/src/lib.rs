use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

pub const CRATE_NAME: &str = "astra-yaml";
const SUPPORTED_VERSION: &str = "v1alpha1";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("unsupported spec version: {0}")]
    UnsupportedVersion(String),
    #[error("pipeline.name must not be empty")]
    EmptyPipelineName,
    #[error("pipeline.schedule must not be empty")]
    EmptySchedule,
    #[error("pipeline.schedule must be manual, continuous, or a valid 5-field cron expression")]
    InvalidSchedule,
    #[error("continuous schedule requires cdc mode with a source kind that supports continuous execution")]
    InvalidContinuousSchedule,
    #[error("cdc mode requires a source kind that supports CDC")]
    CdcNotSupported,
    #[error("source.capture must include at least one table or stream")]
    MissingCaptureTargets,
    #[error("postgres sources require at least one captured table")]
    MissingPostgresTables,
    #[error("snapshot.chunkSize must be greater than zero")]
    InvalidChunkSize,
    #[error("snapshot.cursorField is required when snapshot.mode is incremental")]
    MissingCursorField,
    #[error("destination.write.batchSize must be greater than zero")]
    InvalidBatchSize,
    #[error("destination.staging.kind must not be empty")]
    EmptyStagingKind,
    #[error("destination.staging.bucket must not be empty")]
    EmptyStagingBucket,
    #[error("destination.staging is required for destination kind: {0}")]
    MissingStaging(String),
    #[error("source.connection.{field} is required for source kind: {kind}")]
    MissingSourceConnectionField { kind: String, field: &'static str },
    #[error("destination.connection is required for destination kind: {0}")]
    MissingDestinationConnection(String),
    #[error("destination.connection.{field} is required for destination kind: {kind}")]
    MissingDestinationConnectionField { kind: String, field: &'static str },
    #[error("postgres cdc requires source.capture.cdc.slotName")]
    MissingPostgresCdcSlotName,
    #[error("postgres cdc requires source.capture.cdc.publicationName")]
    MissingPostgresCdcPublicationName,
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
    #[serde(default, rename = "cursorField")]
    pub cursor_field: Option<String>,
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

    pub fn requests_cdc(&self) -> bool {
        self.pipeline.mode == PipelineMode::Cdc || self.source.capture.cdc.is_some()
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.version != SUPPORTED_VERSION {
            return Err(ValidationError::UnsupportedVersion(self.version.clone()));
        }
        if self.pipeline.name.trim().is_empty() {
            return Err(ValidationError::EmptyPipelineName);
        }

        validate_schedule(self)?;

        if self.pipeline.mode == PipelineMode::Cdc && !supports_cdc(&self.source.kind) {
            return Err(ValidationError::CdcNotSupported);
        }
        if self.source.capture.tables.is_empty() && self.source.capture.streams.is_empty() {
            return Err(ValidationError::MissingCaptureTargets);
        }
        if let Some(snapshot) = &self.source.capture.snapshot {
            if matches!(snapshot.chunk_size, Some(0)) {
                return Err(ValidationError::InvalidChunkSize);
            }
            if snapshot.mode == SnapshotMode::Incremental
                && snapshot
                    .cursor_field
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty()
            {
                return Err(ValidationError::MissingCursorField);
            }
        }
        if matches!(self.destination.write.batch_size, Some(0)) {
            return Err(ValidationError::InvalidBatchSize);
        }

        validate_source(self)?;
        validate_destination(self)?;

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

fn supports_continuous_execution(kind: &str) -> bool {
    matches!(kind, "postgres" | "mysql")
}

fn destination_requires_staging(kind: &str) -> bool {
    matches!(kind, "postgres" | "snowflake" | "bigquery")
}

fn destination_requires_connection(kind: &str) -> bool {
    matches!(kind, "postgres")
}

fn validate_schedule(spec: &AstraSpec) -> Result<(), ValidationError> {
    let schedule = spec.pipeline.schedule.trim();
    if schedule.is_empty() {
        return Err(ValidationError::EmptySchedule);
    }

    if schedule == "manual" {
        return Ok(());
    }

    if schedule == "continuous" {
        if spec.pipeline.mode != PipelineMode::Cdc
            || !supports_continuous_execution(&spec.source.kind)
        {
            return Err(ValidationError::InvalidContinuousSchedule);
        }
        return Ok(());
    }

    if is_valid_five_field_cron(schedule) {
        return Ok(());
    }

    Err(ValidationError::InvalidSchedule)
}

fn validate_source(spec: &AstraSpec) -> Result<(), ValidationError> {
    match spec.source.kind.as_str() {
        "postgres" => {
            require_non_empty_fields(
                &spec.source.connection,
                "postgres",
                true,
                &["host", "port", "database", "username"],
            )?;
            if spec.source.capture.tables.is_empty() {
                return Err(ValidationError::MissingPostgresTables);
            }
            if spec.requests_cdc() {
                let cdc = spec.source.capture.cdc.as_ref();
                if !map_has_non_empty_string(cdc, "slotName") {
                    return Err(ValidationError::MissingPostgresCdcSlotName);
                }
                if !map_has_non_empty_string(cdc, "publicationName") {
                    return Err(ValidationError::MissingPostgresCdcPublicationName);
                }
            }
        }
        "mysql" => {
            require_non_empty_fields(
                &spec.source.connection,
                "mysql",
                true,
                &["host", "port", "database", "username"],
            )?;
        }
        _ => {}
    }

    Ok(())
}

fn validate_destination(spec: &AstraSpec) -> Result<(), ValidationError> {
    if destination_requires_connection(&spec.destination.kind) {
        let Some(connection) = spec.destination.connection.as_ref() else {
            return Err(ValidationError::MissingDestinationConnection(
                spec.destination.kind.clone(),
            ));
        };
        require_non_empty_fields(
            connection,
            &spec.destination.kind,
            false,
            &["host", "port", "database", "username"],
        )?;
    }

    if destination_requires_staging(&spec.destination.kind) {
        let Some(staging) = spec.destination.staging.as_ref() else {
            return Err(ValidationError::MissingStaging(
                spec.destination.kind.clone(),
            ));
        };
        if staging.kind.trim().is_empty() {
            return Err(ValidationError::EmptyStagingKind);
        }
        if staging.bucket.trim().is_empty() {
            return Err(ValidationError::EmptyStagingBucket);
        }
    }

    Ok(())
}

fn require_non_empty_fields(
    values: &BTreeMap<String, serde_yaml::Value>,
    kind: &str,
    is_source: bool,
    fields: &[&'static str],
) -> Result<(), ValidationError> {
    for field in fields {
        if !has_non_empty_scalar(values, field) {
            return Err(if is_source {
                ValidationError::MissingSourceConnectionField {
                    kind: kind.to_string(),
                    field,
                }
            } else {
                ValidationError::MissingDestinationConnectionField {
                    kind: kind.to_string(),
                    field,
                }
            });
        }
    }
    Ok(())
}

fn has_non_empty_scalar(values: &BTreeMap<String, serde_yaml::Value>, key: &str) -> bool {
    match values.get(key) {
        Some(serde_yaml::Value::String(value)) => !value.trim().is_empty(),
        Some(serde_yaml::Value::Number(_)) => true,
        _ => false,
    }
}

fn map_has_non_empty_string(
    values: Option<&BTreeMap<String, serde_yaml::Value>>,
    key: &str,
) -> bool {
    values
        .and_then(|map| map.get(key))
        .and_then(|value| match value {
            serde_yaml::Value::String(value) if !value.trim().is_empty() => Some(value),
            _ => None,
        })
        .is_some()
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

fn is_valid_five_field_cron(schedule: &str) -> bool {
    let fields: Vec<_> = schedule.split_whitespace().collect();
    if fields.len() != 5 {
        return false;
    }

    fields.into_iter().all(is_valid_cron_field)
}

fn is_valid_cron_field(field: &str) -> bool {
    if field.is_empty() {
        return false;
    }

    field.split(',').all(is_valid_cron_segment)
}

fn is_valid_cron_segment(segment: &str) -> bool {
    let Some((base, step)) = segment.split_once('/') else {
        return is_valid_cron_base(segment);
    };

    is_valid_cron_base(base) && is_positive_integer(step)
}

fn is_valid_cron_base(base: &str) -> bool {
    if base == "*" {
        return true;
    }

    let Some((start, end)) = base.split_once('-') else {
        return is_positive_integer(base);
    };

    is_positive_integer(start) && is_positive_integer(end)
}

fn is_positive_integer(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
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

    #[test]
    fn detects_when_a_spec_requests_cdc() {
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
        let spec = AstraSpec::parse_yaml(raw).expect("spec parses");

        assert!(spec.requests_cdc());
        spec.validate().expect("spec validates");
    }

    #[test]
    fn rejects_invalid_cron_schedule() {
        let error = validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: bad-schedule
  mode: snapshot
  schedule: every five minutes
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
destination:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: warehouse
    username: warehouse_user
  staging:
    kind: local
    bucket: astra-staging
  write:
    mode: append
runtime: {}
"#,
        )
        .expect_err("invalid schedule rejected");

        assert_eq!(error, ValidationError::InvalidSchedule);
    }

    #[test]
    fn rejects_continuous_schedule_for_snapshot_mode() {
        let error = validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: bad-continuous
  mode: snapshot
  schedule: continuous
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
destination:
  kind: snowflake
  staging:
    kind: s3
    bucket: astra-staging
  write:
    mode: merge
runtime: {}
"#,
        )
        .expect_err("continuous schedule rejected");

        assert_eq!(error, ValidationError::InvalidContinuousSchedule);
    }

    #[test]
    fn rejects_postgres_source_without_required_connection_fields() {
        let error = validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: missing-source-host
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
destination:
  kind: snowflake
  staging:
    kind: s3
    bucket: astra-staging
  write:
    mode: merge
runtime: {}
"#,
        )
        .expect_err("missing source field rejected");

        assert_eq!(
            error,
            ValidationError::MissingSourceConnectionField {
                kind: "postgres".to_string(),
                field: "host",
            }
        );
    }

    #[test]
    fn rejects_postgres_cdc_without_replication_settings() {
        let error = validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: missing-cdc-settings
  mode: cdc
  schedule: continuous
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
    cdc:
      slotName: astra_slot
destination:
  kind: snowflake
  staging:
    kind: s3
    bucket: astra-staging
  write:
    mode: merge
runtime: {}
"#,
        )
        .expect_err("missing publication rejected");

        assert_eq!(error, ValidationError::MissingPostgresCdcPublicationName);
    }

    #[test]
    fn rejects_staged_destinations_without_staging_block() {
        let error = validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: missing-staging
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
destination:
  kind: snowflake
  write:
    mode: merge
runtime: {}
"#,
        )
        .expect_err("missing staging rejected");

        assert_eq!(
            error,
            ValidationError::MissingStaging("snowflake".to_string())
        );
    }

    #[test]
    fn rejects_postgres_destination_without_connection() {
        let error = validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: missing-destination-connection
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
destination:
  kind: postgres
  staging:
    kind: local
    bucket: astra-staging
  write:
    mode: append
runtime: {}
"#,
        )
        .expect_err("missing destination connection rejected");

        assert_eq!(
            error,
            ValidationError::MissingDestinationConnection("postgres".to_string())
        );
    }

    #[test]
    fn rejects_empty_capture_targets() {
        let error = validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: empty-capture
  mode: snapshot
  schedule: manual
source:
  kind: mysql
  connection:
    host: localhost
    port: 3306
    database: app
    username: app_user
  capture: {}
destination:
  kind: snowflake
  staging:
    kind: s3
    bucket: astra-staging
  write:
    mode: merge
runtime: {}
"#,
        )
        .expect_err("missing capture targets rejected");

        assert_eq!(error, ValidationError::MissingCaptureTargets);
    }

    #[test]
    fn rejects_incremental_snapshot_without_cursor_field() {
        let error = validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: missing-cursor
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
    snapshot:
      mode: incremental
destination:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: warehouse
    username: warehouse_user
  staging:
    kind: local
    bucket: astra-staging
  write:
    mode: append
runtime: {}
"#,
        )
        .expect_err("missing cursor field rejected");

        assert_eq!(error, ValidationError::MissingCursorField);
    }

    #[test]
    fn accepts_incremental_snapshot_with_cursor_field() {
        validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: incremental-with-cursor
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
    snapshot:
      mode: incremental
      cursorField: updated_at
destination:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: warehouse
    username: warehouse_user
  staging:
    kind: local
    bucket: astra-staging
  write:
    mode: append
runtime: {}
"#,
        )
        .expect("incremental with cursor field should be valid");
    }

    #[test]
    fn accepts_full_snapshot_without_cursor_field() {
        validate_yaml(
            r#"
version: v1alpha1
pipeline:
  name: full-no-cursor
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
    snapshot:
      mode: full
destination:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: warehouse
    username: warehouse_user
  staging:
    kind: local
    bucket: astra-staging
  write:
    mode: append
runtime: {}
"#,
        )
        .expect("full snapshot without cursor field should be valid");
    }

    fn validate_yaml(raw: &str) -> Result<(), ValidationError> {
        AstraSpec::parse_yaml(raw).expect("spec parses").validate()
    }
}
