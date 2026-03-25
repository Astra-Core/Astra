use std::sync::Arc;

use anyhow::Result;
use astra_control_plane::{
    models::api::{TableExecutionResponse, UpsertTableExecutionRequest},
    services::PipelineService,
    InMemoryPipelineRepository,
};

#[tokio::test]
async fn table_execution_checkpoint_metadata_round_trips_via_service() -> Result<()> {
    let repository = Arc::new(InMemoryPipelineRepository::default());
    let service = PipelineService::new(repository);

    let yaml = r#"
version: v1alpha1
pipeline:
  name: test-checkpoint-pipeline
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: astra
    username: astra
    passwordRef: env:POSTGRES_PASSWORD
  capture:
    tables:
      - public.orders
    snapshot:
      mode: full
destination:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: astra
    username: astra
    passwordRef: env:POSTGRES_PASSWORD
    schema: astra
  staging:
    kind: local
    bucket: astra-staging
    prefix: test-checkpoint-pipeline/
  write:
    mode: append
runtime:
  parallelism:
    tables: 1
"#;

    service.apply_spec(yaml.to_string(), None).await?;
    let run = service
        .create_pipeline_run(
            "test-checkpoint-pipeline".to_string(),
            "manual".to_string(),
            None,
            None,
            None,
        )
        .await?;

    let table = service
        .upsert_table_execution(
            run.id,
            UpsertTableExecutionRequest {
                stream_name: "public.orders".to_string(),
                status: "snapshot_running".to_string(),
                rows_processed: 42,
                rows_total: Some(100),
                error_summary: None,
                checkpoint_next_sequence: Some(2),
                checkpoint_rows_staged: Some(42),
                checkpoint_last_chunk_key: Some(
                    "pipelines/test/streams/public.orders/chunks/00000000000000000001.jsonl.gz"
                        .to_string(),
                ),
                checkpoint_completed: Some(false),
            },
        )
        .await?;

    assert_eq!(table.checkpoint_next_sequence, Some(2));
    assert_eq!(table.checkpoint_rows_staged, Some(42));
    assert_eq!(
        table.checkpoint_last_chunk_key.as_deref(),
        Some("pipelines/test/streams/public.orders/chunks/00000000000000000001.jsonl.gz")
    );
    assert!(!table.checkpoint_completed);

    let updated = service
        .upsert_table_execution(
            run.id,
            UpsertTableExecutionRequest {
                stream_name: "public.orders".to_string(),
                status: "snapshot_complete".to_string(),
                rows_processed: 100,
                rows_total: Some(100),
                error_summary: None,
                checkpoint_next_sequence: Some(3),
                checkpoint_rows_staged: Some(100),
                checkpoint_last_chunk_key: Some(
                    "pipelines/test/streams/public.orders/chunks/00000000000000000002.jsonl.gz"
                        .to_string(),
                ),
                checkpoint_completed: Some(true),
            },
        )
        .await?;

    assert_eq!(updated.status, "snapshot_complete");
    assert_eq!(updated.checkpoint_next_sequence, Some(3));
    assert_eq!(updated.checkpoint_rows_staged, Some(100));
    assert!(updated.checkpoint_completed);

    let tables: Vec<TableExecutionResponse> = service.list_table_executions(run.id).await?;
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].stream_name, "public.orders");
    assert_eq!(tables[0].checkpoint_next_sequence, Some(3));
    assert_eq!(tables[0].checkpoint_rows_staged, Some(100));
    assert!(tables[0].checkpoint_completed);

    Ok(())
}
