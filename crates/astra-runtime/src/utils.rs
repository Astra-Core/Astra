use anyhow::{anyhow, Context, Result};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::StageChunk;

/// Builds the canonical object-storage key for a staged chunk.
///
/// Does **not** include any prefix — callers that need a prefix should wrap this
/// with [`StagingConfig::chunk_key`].
pub fn build_chunk_key(
    pipeline_name: &str,
    stream_name: &str,
    partition_key: &str,
    sequence: u64,
) -> String {
    format!(
        "pipelines/{pipeline_name}/streams/{stream_name}/partitions/{partition_key}/chunks/{sequence:020}.jsonl.gz"
    )
}

/// Strips leading/trailing whitespace and `/` characters from a prefix string.
pub(crate) fn normalize_prefix(prefix: &str) -> String {
    prefix.trim().trim_matches('/').to_string()
}

/// Replaces characters that are unsafe in file/object names with `-`.
pub(crate) fn sanitize_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '-',
        })
        .collect()
}

/// Returns an error if `chunk.bucket` does not match the expected bucket name.
pub(crate) fn ensure_chunk_belongs_to_bucket(
    expected_bucket: &str,
    chunk: &StageChunk,
) -> Result<()> {
    if chunk.bucket == expected_bucket {
        Ok(())
    } else {
        Err(anyhow!(
            "chunk bucket mismatch: expected {}, got {}",
            expected_bucket,
            chunk.bucket
        ))
    }
}

/// Reads a required environment variable, returning a descriptive error if absent.
pub(crate) fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing {name}"))
}

/// Parses the zero-padded sequence number from a staged chunk file name (e.g. `00000000000000000042.jsonl.gz`).
pub(crate) fn parse_sequence(file_name: &str) -> Result<u64> {
    let trimmed = file_name.trim_end_matches(".jsonl.gz");
    trimmed
        .parse::<u64>()
        .with_context(|| format!("invalid staged chunk sequence in file name {file_name}"))
}

/// Returns the current wall-clock time as milliseconds since the Unix epoch.
pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
