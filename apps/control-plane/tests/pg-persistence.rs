use anyhow::{Context, Result};
use astra_control_plane::repositories::{
    AppliedPipelineRecord, CreatePipelineRunRecord, PipelineRecord, PipelineRepository,
    PipelineRunRecord, PostgresPipelineRepository, RecordStagedArtifactRecord,
    StagedArtifactRecord,
};
use astra_yaml::AstraSpec;
use chrono::Utc;
use serde_json::json;
use testcontainers::clients::Cli;
use testcontainers::core::WaitFor;
use testcontainers::images::generic::GenericImage;
use testcontainers::IntoContainerPort;
use uuid::Uuid;

#[tokio::test]
async fn test_pg_repo_full_flow() -> Result<()> {
    let docker = Cli::default();
    let postgres_image = GenericImage::new("postgres", "latest")
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ));
    let node = docker.run(postgres_image);
    let pg_url = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432)?
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

    Ok(())
}

#[tokio::test]
async fn test_pg_repo_list_pipelines() -> Result<()> {
    let docker = Cli::default();
    let postgres_image = GenericImage::new("postgres", "latest")
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ));
    let node = docker.run(postgres_image);
    let pg_url = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432)?
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
