use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use crate::utils::{sanitize_name, unix_time_ms};

// ── Checkpoint data types ─────────────────────────────────────────────────────

/// Persisted resume state for a snapshot pipeline, keyed by table name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SnapshotCheckpointLedger {
    pub pipeline_name: String,
    pub updated_at_unix_ms: u64,
    #[serde(default)]
    pub tables: BTreeMap<String, SnapshotTableCheckpoint>,
}

/// Per-table checkpoint tracking how far a snapshot has progressed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SnapshotTableCheckpoint {
    pub next_sequence: u64,
    pub rows_staged: u64,
    pub last_chunk_key: Option<String>,
    pub completed: bool,
    pub updated_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_cursor_value: Option<serde_json::Value>,
}

// ── LocalCheckpointStore ──────────────────────────────────────────────────────

/// Persists [`SnapshotCheckpointLedger`] files to the local filesystem.
///
/// Each pipeline gets a single JSON file at
/// `<root_dir>/<sanitized-pipeline-name>.snapshot-checkpoints.json`.
#[derive(Debug, Clone)]
pub struct LocalCheckpointStore {
    root_dir: PathBuf,
}

impl LocalCheckpointStore {
    pub fn new(root_dir: PathBuf) -> Self {
        Self { root_dir }
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    /// Creates the root directory if it does not already exist.
    pub fn ensure_ready(&self) -> Result<()> {
        fs::create_dir_all(&self.root_dir).with_context(|| {
            format!(
                "failed to create local checkpoint root at {}",
                self.root_dir.display()
            )
        })
    }

    /// Returns the path to the ledger file for `pipeline_name`.
    pub fn ledger_path(&self, pipeline_name: &str) -> PathBuf {
        self.root_dir.join(format!(
            "{}.snapshot-checkpoints.json",
            sanitize_name(pipeline_name)
        ))
    }

    /// Loads the ledger for `pipeline_name`, returning an empty ledger if none exists yet.
    pub fn load(&self, pipeline_name: &str) -> Result<SnapshotCheckpointLedger> {
        self.ensure_ready()?;
        let path = self.ledger_path(pipeline_name);
        if !path.exists() {
            return Ok(SnapshotCheckpointLedger {
                pipeline_name: pipeline_name.to_string(),
                updated_at_unix_ms: unix_time_ms(),
                tables: BTreeMap::new(),
            });
        }

        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read checkpoint ledger at {}", path.display()))?;
        let mut ledger: SnapshotCheckpointLedger = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse checkpoint ledger at {}", path.display()))?;
        if ledger.pipeline_name.is_empty() {
            ledger.pipeline_name = pipeline_name.to_string();
        }
        Ok(ledger)
    }

    /// Atomically persists `ledger` to disk.
    pub fn save(&self, ledger: &SnapshotCheckpointLedger) -> Result<()> {
        self.ensure_ready()?;
        let path = self.ledger_path(&ledger.pipeline_name);
        let payload =
            serde_json::to_vec_pretty(ledger).context("failed to encode checkpoint ledger")?;
        let mut file = fs::File::create(&path)
            .with_context(|| format!("failed to create checkpoint ledger at {}", path.display()))?;
        file.write_all(&payload)
            .with_context(|| format!("failed to write checkpoint ledger at {}", path.display()))?;
        file.write_all(b"\n").with_context(|| {
            format!("failed to finalize checkpoint ledger at {}", path.display())
        })?;
        file.sync_all()
            .with_context(|| format!("failed to flush checkpoint ledger at {}", path.display()))
    }

    /// Records that a chunk has been staged and advances the checkpoint for `table_name`.
    ///
    /// If `cursor_value` is `Some`, it overwrites the stored cursor; passing `None`
    /// leaves the existing cursor intact.
    pub fn record_chunk_staged(
        &self,
        pipeline_name: &str,
        table_name: &str,
        sequence: u64,
        row_count: u64,
        chunk_key: &str,
        cursor_value: Option<serde_json::Value>,
    ) -> Result<SnapshotCheckpointLedger> {
        let mut ledger = self.load(pipeline_name)?;
        let now = unix_time_ms();
        let checkpoint = ledger.tables.entry(table_name.to_string()).or_default();
        checkpoint.next_sequence = sequence + 1;
        checkpoint.rows_staged = checkpoint.rows_staged.saturating_add(row_count);
        checkpoint.last_chunk_key = Some(chunk_key.to_string());
        checkpoint.completed = false;
        checkpoint.updated_at_unix_ms = now;
        if cursor_value.is_some() {
            checkpoint.last_cursor_value = cursor_value;
        }
        ledger.updated_at_unix_ms = now;
        self.save(&ledger)?;
        Ok(ledger)
    }

    /// Updates only the cursor value for `table_name` without advancing the sequence.
    pub fn update_cursor_value(
        &self,
        pipeline_name: &str,
        table_name: &str,
        cursor_value: serde_json::Value,
    ) -> Result<SnapshotCheckpointLedger> {
        let mut ledger = self.load(pipeline_name)?;
        let now = unix_time_ms();
        let checkpoint = ledger.tables.entry(table_name.to_string()).or_default();
        checkpoint.last_cursor_value = Some(cursor_value);
        checkpoint.updated_at_unix_ms = now;
        ledger.updated_at_unix_ms = now;
        self.save(&ledger)?;
        Ok(ledger)
    }

    /// Marks `table_name` as fully snapshotted.
    pub fn mark_table_complete(
        &self,
        pipeline_name: &str,
        table_name: &str,
    ) -> Result<SnapshotCheckpointLedger> {
        let mut ledger = self.load(pipeline_name)?;
        let now = unix_time_ms();
        let checkpoint = ledger.tables.entry(table_name.to_string()).or_default();
        checkpoint.completed = true;
        checkpoint.updated_at_unix_ms = now;
        ledger.updated_at_unix_ms = now;
        self.save(&ledger)?;
        Ok(ledger)
    }
}
