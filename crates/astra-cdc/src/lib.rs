use serde::{Deserialize, Serialize};

pub const CRATE_NAME: &str = "astra-cdc";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CdcPhase {
    SnapshotPlanning,
    SnapshotRunning,
    SnapshotComplete,
    CdcStarting,
    CdcRunning,
    Paused,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password_ref: String,
    pub ssl_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    pub mode: SnapshotMode,
    pub chunk_size: u64,
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    Full,
    Incremental,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    pub slot_name: String,
    pub publication_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSourceConnector {
    pub config: PostgresConfig,
    pub snapshot: SnapshotConfig,
    pub cdc: Option<CdcConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamProgress {
    pub stream_name: String,
    pub phase: CdcPhase,
    pub last_snapshot_chunk: Option<String>,
    pub last_lsn: Option<String>,
}

impl PostgresSourceConnector {
    pub fn supports_cdc(&self) -> bool {
        self.cdc.is_some()
    }

    pub fn initial_phase(&self) -> CdcPhase {
        match self.snapshot.mode {
            SnapshotMode::None if self.supports_cdc() => CdcPhase::CdcStarting,
            SnapshotMode::None => CdcPhase::Paused,
            _ => CdcPhase::SnapshotPlanning,
        }
    }
}

pub fn status() -> &'static str {
    "cdc skeleton defined"
}
