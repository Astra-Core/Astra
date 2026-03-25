use anyhow::{Context, Result};
use astra_control_plane::repositories::{
    pipeline_repository::UpsertTableExecutionRecord, CreatePipelineRunRecord, PipelineRecord,
    PipelineRepository, PipelineRunRecord, PostgresPipelineRepository, RecordStagedArtifactRecord,
    StagedArtifactRecord,
};
use astra_yaml::AstraSpec;
use chrono::Utc;
use serde_json::json;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

#[tokio::test]
async fn test_pg_repo_full_flow() -> Result<()> {
    let postgres_image = Postgres::default();
    let node = postgres_image.start().await?;
    let pg_url = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432).await?
    );

    // First repo instance
    let repo1 = PostgresPipelineRepository::connect(&pg_url).await?;

    let pipeline_name = format!("test-pg-{}", Uuid::new_v4());

    let spec_yaml = r#"
pipeline:
  name: test-pipeline
source:
  kind: postgres
  config:
    connection_string: postgres://foo
destination:
  kind: s3
  config:
    bucket: bar
version: 1.0
"#;
    let spec: AstraSpec = serde_yaml::from_str(spec_yaml).context("parse spec")?;
    let raw_yaml = spec_yaml.to_string();

    // apply_spec
    let applied = repo1
        .apply_spec(spec.clone(), raw_yaml.clone(), None)
        .await?;
    assert_eq!(applied.pipeline.name, pipeline_name);
    assert_eq!(applied.pipeline.status, "active");

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
    };
    let table_exec = repo2.upsert_table_execution(upsert_rec).await?;
    assert_eq!(table_exec.status, "running");
    assert_eq!(table_exec.rows_processed, 0i64);

    let execs = repo2.list_table_executions(run_id).await?;
    assert_eq!(execs.len(), 1);

    let upsert_rec_terminal = UpsertTableExecutionRecord {
        pipeline_run_id: run_id,
        stream_name: "test_stream".to_string(),
        status: "failed".to_string(),
        rows_processed: 50i64,
        rows_total: Some(100i64),
        error_summary: Some("test error message".to_string()),
    };
    let table_exec_terminal = repo2.upsert_table_execution(upsert_rec_terminal).await?;
    assert_eq!(table_exec_terminal.status, "failed");
    assert!(table_exec_terminal.finished_at.is_some());
    assert_eq!(
        table_exec_terminal.error_summary,
        Some("test error message".to_string())
    );

    // Verify table exec persistence
    drop(repo2);
    let repo3 = PostgresPipelineRepository::connect(&pg_url).await?;
    let execs3 = repo3.list_table_executions(run_id).await?;
    assert_eq!(execs3.len(), 1);
    assert_eq!(execs3[0].status, "failed");

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

    Ok(())
}

#[tokio::test]
async fn test_pg_repo_list_pipelines() -> Result<()> {
    let postgres_image = Postgres::default();
    let node = postgres_image.start().await?;
    let pg_url = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432).await?
    );

    let repo = PostgresPipelineRepository::connect(&pg_url).await?;

    let pipeline_name = format!("test-list-{}", Uuid::new_v4());

    let spec_yaml = r#"
pipeline:
  name: test-list-pipeline
source:
  kind: postgres
destination:
  kind: s3
version: 1.0
"#;
    let spec: AstraSpec = serde_yaml::from_str(spec_yaml)?;
    let raw_yaml = spec_yaml.to_string();

    repo.apply_spec(spec, raw_yaml, None).await?;

    let pipelines: Vec<PipelineRecord> = repo.list_pipelines().await?;
    assert!(pipelines.iter().any(|p| p.name == pipeline_name));

    Ok(())
}
