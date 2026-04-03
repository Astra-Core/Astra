use crate::{
    models::api::UpsertTableExecutionRequest, repositories::RecordStagedArtifactRecord,
    services::PipelineService,
};
use astra_connectors::{PostgresDestinationLoader, PostgresSource, SnapshotExecutionOptions};
use astra_runtime::{
    LocalCheckpointStore, LocalStageChunkStore, LocalStagingConfig, StageChunkPayload,
    StageChunkRequest, StageChunkStore, StagingConfig, StagingKind,
};
use astra_yaml::AstraSpec;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

/// Runs a full snapshot → load pipeline in a background Tokio task.
/// On completion or failure, updates the run record via `PipelineService`.
/// Intended to be called with `tokio::spawn`.
pub async fn execute_pipeline(service: Arc<PipelineService>, run_id: Uuid, yaml: String) {
    tracing::info!(%run_id, "embedded executor: pipeline run started");
    match run_pipeline(Arc::clone(&service), run_id, &yaml).await {
        Ok(()) => {
            tracing::info!(%run_id, "embedded executor: pipeline run succeeded");
        }
        Err(e) => {
            tracing::error!(%run_id, error = ?e, "embedded executor: pipeline run failed");
            let _ = service
                .update_pipeline_run_status(
                    run_id,
                    "failed".to_string(),
                    None,
                    None,
                    Some(json!({ "error": e.to_string() })),
                )
                .await;
        }
    }
}

async fn run_pipeline(
    service: Arc<PipelineService>,
    run_id: Uuid,
    yaml: &str,
) -> anyhow::Result<()> {
    let spec = AstraSpec::parse_yaml(yaml)?;
    spec.validate()?;

    if spec.requests_cdc() {
        anyhow::bail!("CDC execution is not yet supported by the embedded executor");
    }
    if spec.source.kind != "postgres" {
        anyhow::bail!("embedded executor currently supports source.kind=postgres only");
    }
    if spec.destination.kind != "postgres" {
        anyhow::bail!("embedded executor currently supports destination.kind=postgres only");
    }

    // Ephemeral staging area, unique per run — cleaned up on completion
    let staging_root = std::env::temp_dir().join(format!("astra/{}", run_id));
    let checkpoint_root = staging_root.join("checkpoints");

    let staging_config = StagingConfig {
        kind: StagingKind::Local,
        bucket: "astra-staging".to_string(),
        prefix: String::new(),
    };
    let store = LocalStageChunkStore::new(LocalStagingConfig {
        root_dir: staging_root.clone(),
        storage: staging_config,
    });
    store.ensure_ready().await?;

    let checkpoint_store = LocalCheckpointStore::new(checkpoint_root);
    checkpoint_store.ensure_ready()?;

    // ── Phase 1: Snapshot ────────────────────────────────────────────────────

    service
        .update_pipeline_run_status(
            run_id,
            "running".to_string(),
            Some("snapshot".to_string()),
            None,
            None,
        )
        .await?;

    let source = PostgresSource::from_spec(&spec)?;
    let snapshot = source
        .snapshot_to_jsonl_gzip(SnapshotExecutionOptions {
            tables: spec
                .source
                .capture
                .tables
                .iter()
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
            max_rows_per_table: None,
            chunk_size: None,
            start_sequence_by_table: BTreeMap::new(),
        })
        .await?;

    for table in snapshot.tables {
        let chunk = store
            .write_chunk(StageChunkRequest {
                pipeline_name: spec.pipeline.name.clone(),
                stream_name: table.table.clone(),
                partition_key: "default".to_string(),
                sequence: table.sequence,
                payload: StageChunkPayload::jsonl_gzip(table.row_count, table.rows_jsonl_gzip),
            })
            .await?;

        checkpoint_store.record_chunk_staged(
            &spec.pipeline.name,
            &table.table,
            table.sequence,
            chunk.row_count,
            &chunk.object_key,
        )?;

        service
            .record_staged_artifact(RecordStagedArtifactRecord {
                pipeline_run_id: run_id,
                stream_name: table.table.clone(),
                partition_key: "default".to_string(),
                sequence: table.sequence.try_into()?,
                bucket: chunk.bucket.clone(),
                object_key: chunk.object_key.clone(),
                bytes_written: chunk.bytes_written as i64,
                row_count: chunk.row_count as i64,
                content_type: "application/jsonl".to_string(),
                content_encoding: "gzip".to_string(),
                schema_fingerprint: None,
                metadata_json: json!({ "sql": table.sql }),
            })
            .await?;

        service
            .upsert_table_execution(
                run_id,
                UpsertTableExecutionRequest {
                    stream_name: table.table.clone(),
                    status: "staged".to_string(),
                    rows_processed: chunk.row_count as i64,
                    rows_total: Some(chunk.row_count as i64),
                    error_summary: None,
                    checkpoint_next_sequence: Some((table.sequence + 1).try_into()?),
                    checkpoint_rows_staged: Some(chunk.row_count as i64),
                    checkpoint_last_chunk_key: Some(chunk.object_key.clone()),
                    checkpoint_completed: Some(false),
                },
            )
            .await?;
    }

    for progress in snapshot.table_progress {
        if progress.finished {
            checkpoint_store.mark_table_complete(&spec.pipeline.name, &progress.table)?;
        }
        service
            .upsert_table_execution(
                run_id,
                UpsertTableExecutionRequest {
                    stream_name: progress.table.clone(),
                    status: if progress.finished {
                        "snapshot_complete"
                    } else {
                        "snapshot_running"
                    }
                    .to_string(),
                    rows_processed: progress.rows_emitted as i64,
                    rows_total: None,
                    error_summary: None,
                    checkpoint_next_sequence: Some(progress.next_sequence.try_into()?),
                    checkpoint_rows_staged: Some(progress.rows_emitted as i64),
                    checkpoint_last_chunk_key: None,
                    checkpoint_completed: Some(progress.finished),
                },
            )
            .await?;
    }

    // ── Phase 2: Load to destination ─────────────────────────────────────────

    service
        .update_pipeline_run_status(
            run_id,
            "running".to_string(),
            Some("load".to_string()),
            None,
            None,
        )
        .await?;

    let loader = PostgresDestinationLoader::from_spec(&spec)?;
    let staged_chunks = store.list_chunks_for_pipeline(&spec.pipeline.name)?;

    if staged_chunks.is_empty() {
        anyhow::bail!("snapshot produced no chunks — nothing to load");
    }

    let mut chunk_payloads = Vec::new();
    for mut chunk in staged_chunks {
        let bytes = store.read_chunk(&chunk).await?;
        chunk.bytes_written = bytes.len() as u64;
        chunk_payloads.push((chunk, bytes));
    }

    let report = loader.load_local_stage_chunks(chunk_payloads).await?;
    let total_rows: i64 = report
        .applied_chunks
        .iter()
        .map(|c| c.rows_written as i64)
        .sum();

    for chunk in &report.applied_chunks {
        service
            .upsert_table_execution(
                run_id,
                UpsertTableExecutionRequest {
                    stream_name: chunk.table_name.clone(),
                    status: "applied".to_string(),
                    rows_processed: chunk.rows_written as i64,
                    rows_total: Some(chunk.rows_written as i64),
                    error_summary: None,
                    checkpoint_next_sequence: None,
                    checkpoint_rows_staged: Some(chunk.rows_written as i64),
                    checkpoint_last_chunk_key: Some(chunk.object_key.clone()),
                    checkpoint_completed: Some(true),
                },
            )
            .await?;
    }

    service
        .update_pipeline_run_status(
            run_id,
            "succeeded".to_string(),
            None,
            None,
            Some(json!({
                "chunks_applied": report.applied_chunks.len(),
                "rows_written": total_rows,
                "schema": report.schema,
            })),
        )
        .await?;

    // Clean up ephemeral staging
    let _ = std::fs::remove_dir_all(&staging_root);

    Ok(())
}
