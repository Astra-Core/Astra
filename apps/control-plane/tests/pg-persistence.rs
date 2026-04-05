use anyhow::{Context, Result};
use astra_control_plane::repositories::{
    pipeline_repository::UpsertTableExecutionRecord, ApplySpecRecord, CreatePipelineRunRecord,
    PipelineRecord, PipelineRepository, PipelineRunRecord, PostgresPipelineRepository,
    RecordStagedArtifactRecord, StagedArtifactRecord,
};
use astra_metadata::PipelineStatus;
use astra_yaml::AstraSpec;
use chrono::Utc;
use serde_json::json;
use testcontainers::{core::RunnableImage, core::WaitFor, runners::AsyncRunner, GenericImage};

#[tokio::test]
async fn test_pg_repo_full_flow() -> Result<()> {
    let postgres_image = RunnableImage::from(
        GenericImage::new("postgres", "15")
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_DB", "postgres")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            )),
    )
    .with_mapped_port((55432, 5432));
    let _node = postgres_image.start().await?;
    let pg_url = "postgres://postgres:postgres@localhost:55432".to_string();

    // First repo instance
    let repo1 = PostgresPipelineRepository::connect(&pg_url).await?;

    let pipeline_name = "test-pipeline".to_string();

    let spec_yaml = r#"
version: v1alpha1
pipeline:
  name: test-pipeline
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
    prefix: test-pipeline/
  write:
    mode: append
runtime:
  parallelism:
    tables: 1
"#;
    let spec: AstraSpec = serde_yaml::from_str(spec_yaml).context("parse spec")?;
    let raw_yaml = spec_yaml.to_string();

    // apply_spec
    let apply_record = ApplySpecRecord {
        name: spec.pipeline.name.clone(),
        source_kind: spec.source.kind.clone(),
        destination_kind: spec.destination.kind.clone(),
        mode: serde_json::to_value(&spec.pipeline.mode)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default(),
        spec_version: spec.version.clone(),
        spec_json: serde_json::to_value(&spec).context("serialize spec")?,
        raw_yaml: raw_yaml.clone(),
        created_by: None,
    };
    let applied = repo1.apply_spec(apply_record).await?;
    assert_eq!(applied.pipeline.name, pipeline_name);
    assert_eq!(applied.pipeline.status, PipelineStatus::Active);

    // create_run
    let now = Utc::now();
    let create_rec = CreatePipelineRunRecord {
        pipeline_name: pipeline_name.clone(),
        trigger_mode: "manual".to_string(),
        status: "running".to_string(),
        worker_id: Some("test-worker".to_string()),
        started_at: now,
    };
    let run = repo1.create_pipeline_run(create_rec).await?;
    let run_id = run.id;

    // update_status
    let stats = json!({ "rows_processed": 42 });
    let updated_run = repo1
        .update_pipeline_run_status(run_id, "succeeded".to_string(), stats.clone())
        .await?;
    assert_eq!(updated_run.status, "succeeded");

    // list_runs / latest / history
    let runs: Vec<PipelineRunRecord> = repo1.list_pipeline_runs(&pipeline_name).await?;
    assert_eq!(runs.len(), 1);
    let latest: Option<PipelineRunRecord> = repo1.get_latest_run(&pipeline_name).await?;
    assert!(latest.is_some());
    let latest = latest.unwrap();
    assert_eq!(latest.id, run_id);
    let history: Vec<PipelineRunRecord> = repo1.get_run_history(&pipeline_name, 10).await?;
    assert_eq!(history.len(), 1);

    // record/list_artifacts
    let art_rec = RecordStagedArtifactRecord {
        pipeline_run_id: run_id,
        stream_name: "public.orders".to_string(),
        partition_key: "default".to_string(),
        sequence: 0i64,
        bucket: "astra-staging".to_string(),
        object_key: "test/chunk.jsonl.gz".to_string(),
        bytes_written: 1024i64,
        row_count: 42i64,
        content_type: "application/jsonl+gzip".to_string(),
        content_encoding: "gzip".to_string(),
        schema_fingerprint: Some("sha256:abc123".to_string()),
        metadata_json: json!({ "table": "orders" }),
    };
    let artifact = repo1.record_staged_artifact(art_rec).await?;
    assert_eq!(artifact.row_count, 42);
    let artifacts: Vec<StagedArtifactRecord> = repo1.list_staged_artifacts(run_id).await?;
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].stream_name, "public.orders");

    // Verify persistence (restart repo)
    drop(repo1);
    let repo2 = PostgresPipelineRepository::connect(&pg_url).await?;

    let runs2: Vec<PipelineRunRecord> = repo2.list_pipeline_runs(&pipeline_name).await?;
    assert_eq!(runs2.len(), 1);
    let artifacts2: Vec<StagedArtifactRecord> = repo2.list_staged_artifacts(run_id).await?;
    assert_eq!(artifacts2.len(), 1);

    // Test table execution lifecycle and persistence
    let upsert_rec = UpsertTableExecutionRecord {
        pipeline_run_id: run_id,
        stream_name: "test_stream".to_string(),
        status: "running".to_string(),
        rows_processed: 0i64,
        rows_total: Some(100i64),
        error_summary: None,
        checkpoint_next_sequence: Some(1),
        checkpoint_rows_staged: Some(42),
        checkpoint_last_chunk_key: Some(
            "pipelines/test/streams/test_stream/chunks/00000000000000000000.jsonl.gz".to_string(),
        ),
        checkpoint_completed: Some(false),
    };
    let table_exec = repo2.upsert_table_execution(upsert_rec).await?;
    assert_eq!(table_exec.status, "running");
    assert_eq!(table_exec.rows_processed, 0i64);
    assert_eq!(table_exec.checkpoint_next_sequence, Some(1));
    assert_eq!(table_exec.checkpoint_rows_staged, Some(42));
    assert_eq!(
        table_exec.checkpoint_last_chunk_key.as_deref(),
        Some("pipelines/test/streams/test_stream/chunks/00000000000000000000.jsonl.gz")
    );
    assert!(!table_exec.checkpoint_completed);

    let execs = repo2.list_table_executions(run_id).await?;
    assert_eq!(execs.len(), 1);

    let upsert_rec_terminal = UpsertTableExecutionRecord {
        pipeline_run_id: run_id,
        stream_name: "test_stream".to_string(),
        status: "snapshot_complete".to_string(),
        rows_processed: 50i64,
        rows_total: Some(100i64),
        error_summary: None,
        checkpoint_next_sequence: Some(2),
        checkpoint_rows_staged: Some(100),
        checkpoint_last_chunk_key: Some(
            "pipelines/test/streams/test_stream/chunks/00000000000000000001.jsonl.gz".to_string(),
        ),
        checkpoint_completed: Some(true),
    };
    let table_exec_terminal = repo2.upsert_table_execution(upsert_rec_terminal).await?;
    assert_eq!(table_exec_terminal.status, "snapshot_complete");
    assert!(table_exec_terminal.finished_at.is_some());
    assert_eq!(table_exec_terminal.error_summary, None);
    assert_eq!(table_exec_terminal.checkpoint_next_sequence, Some(2));
    assert_eq!(table_exec_terminal.checkpoint_rows_staged, Some(100));
    assert_eq!(
        table_exec_terminal.checkpoint_last_chunk_key.as_deref(),
        Some("pipelines/test/streams/test_stream/chunks/00000000000000000001.jsonl.gz")
    );
    assert!(table_exec_terminal.checkpoint_completed);

    // Verify table exec persistence
    drop(repo2);
    let repo3 = PostgresPipelineRepository::connect(&pg_url).await?;
    let execs3 = repo3.list_table_executions(run_id).await?;
    assert_eq!(execs3.len(), 1);
    assert_eq!(execs3[0].status, "snapshot_complete");
    assert_eq!(execs3[0].checkpoint_next_sequence, Some(2));
    assert_eq!(execs3[0].checkpoint_rows_staged, Some(100));
    assert_eq!(
        execs3[0].checkpoint_last_chunk_key.as_deref(),
        Some("pipelines/test/streams/test_stream/chunks/00000000000000000001.jsonl.gz")
    );
    assert!(execs3[0].checkpoint_completed);

    // Test cancelled run status
    let now = Utc::now();
    let create_rec_cancel = CreatePipelineRunRecord {
        pipeline_name: pipeline_name.clone(),
        trigger_mode: "manual".to_string(),
        status: "running".to_string(),
        worker_id: Some("test-worker-cancel".to_string()),
        started_at: now,
    };
    let run_cancel = repo3.create_pipeline_run(create_rec_cancel).await?;
    let stats_cancel = json!({});
    let updated_cancel = repo3
        .update_pipeline_run_status(run_cancel.id, "cancelled".to_string(), stats_cancel)
        .await?;
    assert_eq!(updated_cancel.status, "cancelled");
    assert!(updated_cancel.finished_at.is_some());

    // Test disable / enable (update_pipeline_status)
    let disabled = repo3
        .update_pipeline_status(&pipeline_name, PipelineStatus::Disabled)
        .await?;
    assert_eq!(disabled.status, PipelineStatus::Disabled);

    let enabled = repo3
        .update_pipeline_status(&pipeline_name, PipelineStatus::Active)
        .await?;
    assert_eq!(enabled.status, PipelineStatus::Active);

    // Test delete_pipeline removes the pipeline and cascades
    repo3.delete_pipeline(&pipeline_name).await?;
    let pipelines_after = repo3.list_pipelines().await?;
    assert!(!pipelines_after.iter().any(|p| p.name == pipeline_name));

    // Deleting again should return an error
    let delete_err = repo3.delete_pipeline(&pipeline_name).await;
    assert!(delete_err.is_err());

    Ok(())
}

#[tokio::test]
async fn test_pg_repo_list_pipelines() -> Result<()> {
    let postgres_image = RunnableImage::from(
        GenericImage::new("postgres", "15")
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_DB", "postgres")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            )),
    )
    .with_mapped_port((55433, 5432));
    let _node = postgres_image.start().await?;
    let pg_url = "postgres://postgres:postgres@localhost:55433".to_string();

    let repo = PostgresPipelineRepository::connect(&pg_url).await?;

    let pipeline_name = "test-list-pipeline".to_string();

    let spec_yaml = r#"
version: v1alpha1
pipeline:
  name: test-list-pipeline
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
    prefix: test-list-pipeline/
  write:
    mode: append
runtime:
  parallelism:
    tables: 1
"#;
    let spec: AstraSpec = serde_yaml::from_str(spec_yaml)?;
    let raw_yaml = spec_yaml.to_string();

    let apply_record = ApplySpecRecord {
        name: spec.pipeline.name.clone(),
        source_kind: spec.source.kind.clone(),
        destination_kind: spec.destination.kind.clone(),
        mode: serde_json::to_value(&spec.pipeline.mode)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default(),
        spec_version: spec.version.clone(),
        spec_json: serde_json::to_value(&spec)?,
        raw_yaml: raw_yaml.clone(),
        created_by: None,
    };
    repo.apply_spec(apply_record).await?;

    let pipelines: Vec<PipelineRecord> = repo.list_pipelines().await?;
    assert!(pipelines.iter().any(|p| p.name == pipeline_name));

    Ok(())
}
