// use anyhow::{anyhow, Context, Result};
use anyhow::Result;

use astra_control_plane::repositories::{
    AppliedPipelineRecord, CreatePipelineRunRecord, InMemoryPipelineRepository, PipelineRecord,
    PipelineRepository, PipelineRunRecord, PostgresPipelineRepository, RecordStagedArtifactRecord,
    StagedArtifactRecord,
};
use astra_yaml::AstraSpec;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::fs;
use uuid::Uuid;

const TEST_DB_URL: &str = "postgres://astra:astra@localhost:5432/astra_test_pg_persistence";

// Assumes local Postgres (Podman docker-compose) is running.
// Cleans up test data after each test using unique pipeline names.

#[tokio::test]
async fn test_pg_repo_full_flow() -> Result<()> {
    let repo = PostgresPipelineRepository::connect(TEST_DB_URL)
        .await
        .context("PG connect")?;

    let pipeline_name = format!("test-pg-{}", Uuid::new_v4());

    let example_yaml = fs::read_to_string("../../../examples/postgres-to-warehouse.astra.yaml")
        .context("load example yaml")?;
    let mut spec: AstraSpec = serde_yaml::from_str(&example_yaml).context("parse spec")?;
    spec.pipeline.name = pipeline_name.clone();
    let raw_yaml = serde_yaml::to_string(&spec).context("serialize spec yaml")?;

    // apply_spec
    let applied = repo
        .apply_spec(spec.clone(), raw_yaml.clone(), None)
        .await?;
    assert_eq!(applied.pipeline.name, pipeline_name);
    assert_eq!(applied.pipeline.status, "active");
    assert_eq!(applied.pipeline.source_kind, "postgres");

    // create_run
    let now = Utc::now();
    let create_rec = CreatePipelineRunRecord {
        pipeline_name: pipeline_name.clone(),
        trigger_mode: "manual".to_string(),
        status: "running".to_string(),
        worker_id: Some("test-worker".to_string()),
        started_at: now,
    };
    let run = repo.create_pipeline_run(create_rec).await?;
    assert_eq!(run.pipeline_name, pipeline_name);
    assert_eq!(run.status, "running");
    assert_eq!(run.worker_id, Some("test-worker".to_string()));

    // update_status
    let stats = json!({ "rows_processed": 42 });
    let updated_run = repo
        .update_pipeline_run_status(run.id, "succeeded".to_string(), stats.clone())
        .await?;
    assert_eq!(updated_run.status, "succeeded");
    assert!(updated_run.finished_at.is_some());

    // list_runs / latest / history
    let runs = repo.list_pipeline_runs(&pipeline_name).await?;
    assert_eq!(runs.len(), 1);
    let latest = repo.get_latest_run(&pipeline_name).await?;
    assert!(latest.is_some());
    let latest = latest.unwrap();
    assert_eq!(latest.id, run.id);
    let history = repo.get_run_history(&pipeline_name, 10).await?;
    assert_eq!(history.len(), 1);

    // artifacts
    let art_rec = RecordStagedArtifactRecord {
        pipeline_run_id: run.id,
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
    let artifact = repo.record_staged_artifact(art_rec).await?;
    assert_eq!(artifact.row_count, 42);
    let artifacts = repo.list_staged_artifacts(run.id).await?;
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].stream_name, "public.orders");

    // Optional: compare to memory repo
    let mem_repo = InMemoryPipelineRepository::default();
    let mem_applied = mem_repo.apply_spec(spec, raw_yaml, None).await?;
    let mem_run = mem_repo.create_pipeline_run(create_rec).await?;
    mem_repo
        .update_pipeline_run_status(mem_run.id, "succeeded".to_string(), stats)
        .await?;
    let mem_runs = mem_repo.list_pipeline_runs(&pipeline_name).await?;
    let pg_runs = repo.list_pipeline_runs(&pipeline_name).await?;
    assert_eq!(mem_runs.len(), pg_runs.len());
    let mem_run = &mem_runs[0];
    let pg_run = &pg_runs[0];
    assert_eq!(mem_run.pipeline_name, pg_run.pipeline_name);
    assert_eq!(mem_run.status, pg_run.status);
    // skip timestamps/ids

    Ok(())
}

#[tokio::test]
async fn test_pg_repo_list_pipelines() -> Result<()> {
    let repo = PostgresPipelineRepository::connect(TEST_DB_URL).await?;

    let pipeline_name = format!("test-list-{}", Uuid::new_v4());

    let example_yaml = fs::read_to_string("../../../examples/postgres-to-warehouse.astra.yaml")
        .context("load example yaml")?;
    let mut spec: AstraSpec = serde_yaml::from_str(&example_yaml).context("parse spec")?;
    spec.pipeline.name = pipeline_name.clone();
    let raw_yaml = serde_yaml::to_string(&spec).context("serialize spec yaml")?;

    let applied = repo.apply_spec(spec, raw_yaml, None).await?;
    let pipelines = repo.list_pipelines().await?;
    assert!(pipelines.iter().any(|p| p.name == pipeline_name));
    assert_eq!(
        applied.pipeline.spec_version,
        pipelines
            .iter()
            .find(|p| p.name == pipeline_name)
            .unwrap()
            .spec_version
    );

    Ok(())
}
