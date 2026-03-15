use astra_control_plane::repositories::{
    postgres::PostgresPipelineRepository,
    pipeline_repository::{
        AppliedPipelineRecord, CreatePipelineRunRecord, PipelineRecord, PipelineRepository,
        PipelineRunRecord, RecordStagedArtifactRecord, StagedArtifactRecord,
    },
};
use astra_yaml::AstraSpec;
use chrono::{DateTime, Utc};
use serde_json::json;
use testcontainers::clients::Cli;
use testcontainers::images::postgres::Postgres;
use uuid::Uuid;
use anyhow::Result;

#[tokio::test]
async fn test_pg_persistence_full_cycle() -> Result<()> {
    let docker = Cli::default();

    let postgres_image = Postgres::default();
    let node = docker.run(postgres_image);
    let pg_url = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432)?
    );

    // First repo instance
    let repo1 = PostgresPipelineRepository::connect(&pg_url).await?;

    // Minimal valid spec
    let spec_yaml = r#"version: v1alpha1
pipeline:
  name: test-pipeline
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
  capture:
    tables:
      - users
destination:
  kind: s3
  staging:
    kind: local
    bucket: staging-bucket
  write:
    mode: append
"#;
    let spec = AstraSpec::parse_yaml(spec_yaml)?;
    let applied = repo1.apply_spec(spec, spec_yaml.to_string(), Some("test".to_string())).await?;
    assert_eq!(applied.pipeline.name, "test-pipeline");
    assert_eq!(applied.pipeline.status, "active");

    // create_run
    let started_at = Utc::now();
    let create_run = CreatePipelineRunRecord {
        pipeline_name: "test-pipeline".to_string(),
        trigger_mode: "manual".to_string(),
        status: "running".to_string(),
        worker_id: Some("worker-1".to_string()),
        started_at,
    };
    let run = repo1.create_pipeline_run(create_run).await?;
    let run_id = run.id;

    // update_status
    let updated_run = repo1
        .update_pipeline_run_status(run_id, "succeeded".to_string(), json!({}))
        .await?;
    assert_eq!(updated_run.status, "succeeded");

    // list_runs / latest_run
    let runs = repo1.list_pipeline_runs("test-pipeline").await?;
    assert_eq!(runs.len(), 1);
    let latest = repo1.get_latest_run("test-pipeline").await?;
    assert!(latest.is_some());
    let latest = latest.unwrap();
    assert_eq!(latest.id, run_id);
    assert_eq!(latest.status, "succeeded");

    // record_artifact
    let artifact = RecordStagedArtifactRecord {
        pipeline_run_id: run_id,
        stream_name: "test-stream".to_string(),
        partition_key: "2026-01".to_string(),
        sequence: 1,
        bucket: "test-bucket".to_string(),
        object_key: "obj1.parquet".to_string(),
        bytes_written: 1024,
        row_count: 100,
        content_type: "application/parquet".to_string(),
        content_encoding: "snappy".to_string(),
        schema_fingerprint: Some("abc123".to_string()),
        metadata_json: json!({ "foo": "bar" }),
    };
    let recorded = repo1.record_staged_artifact(artifact).await?;
    assert_eq!(recorded.pipeline_run_id, run_id);

    // list_artifacts
    let artifacts = repo1.list_staged_artifacts(run_id).await?;
    assert_eq!(artifacts.len(), 1);

    // Restart: new repo instance (persistence test)
    drop(repo1);
    let repo2 = PostgresPipelineRepository::connect(&pg_url).await?;

    // Verify data persists
    let runs2 = repo2.list_pipeline_runs("test-pipeline").await?;
    assert_eq!(runs2.len(), 1);
    let latest2 = repo2.get_latest_run("test-pipeline").await?;
    let latest2 = latest2.unwrap();
    assert_eq!(latest2.status, "succeeded");

    let artifacts2 = repo2.list_staged_artifacts(run_id).await?;
    assert_eq!(artifacts2.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_pg_list_pipelines() -> Result<()> {
    let docker = Cli::default();
    let postgres_image = Postgres::default();
    let node = docker.run(postgres_image);
    let pg_url = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432)?
    );

    let repo = PostgresPipelineRepository::connect(&pg_url).await?;
    let spec_yaml = r#"version: v1alpha1
pipeline:
  name: list-test
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
  capture:
    tables:
      - users
destination:
  kind: s3
  staging:
    kind: local
    bucket: staging-bucket
  write:
    mode: append
"#;
    let spec = AstraSpec::parse_yaml(spec_yaml)?;
    repo.apply_spec(spec, spec_yaml.to_string(), None).await?;

    let pipelines = repo.list_pipelines().await?;
    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].name, "list-test");

    Ok(())
}
