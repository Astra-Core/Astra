use anyhow::{anyhow, bail, Context};
use astra_connectors::{
    PostgresDestinationLoader, PostgresDiscoverOptions, PostgresSource, SnapshotExecutionOptions,
};
use astra_runtime::{
    LocalCheckpointStore, LocalStageChunkStore, LocalStagingConfig, MinioStageChunkStore,
    MinioStagingConfig, SnapshotCheckpointLedger, StageChunkPayload, StageChunkRequest,
    StageChunkStore, StagingConfig, StagingKind,
};
use astra_yaml::AstraSpec;
use chrono::Utc;
use reqwest::Client;
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::PathBuf};
use tracing::{debug, info};

pub fn validate(file: &str) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    let source = if spec.source.kind == "postgres" {
        Some(PostgresSource::from_spec(&spec)?)
    } else {
        None
    };

    info!(
        pipeline = %spec.pipeline.name,
        mode = ?spec.pipeline.mode,
        source = %spec.source.kind,
        destination = %spec.destination.kind,
        "valid Astra spec"
    );
    if let Some(source) = source {
        info!(tables = %source.config().tables.join(", "), "postgres tables");
    }
    if spec.requests_cdc() {
        info!("note: CDC settings are modeled in the spec, but Astra does not execute Postgres CDC yet; use discovery/snapshot commands only.");
    }
    Ok(())
}

pub fn apply(file: &str) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    ensure_cdc_runtime_not_requested(&spec, "apply")?;
    info!(pipeline = %spec.pipeline.name, "apply stub for validated pipeline");
    info!("next step: send normalized spec to control-plane API");
    Ok(())
}

pub async fn discover_source(file: &str) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    let source = PostgresSource::from_spec(&spec)?;
    let report = source
        .discover(PostgresDiscoverOptions { tables: vec![] })
        .await?;

    info!(pipeline = %spec.pipeline.name, "discovered postgres source");
    println!("tables:");
    for table in &report.catalog.tables {
        println!("- {}", table.fully_qualified_name);
        if !table.primary_key.is_empty() {
            println!("  primary key: {}", table.primary_key.join(", "));
        }
        for column in &table.columns {
            println!(
                "  - {}: {}{}",
                column.name,
                column.data_type,
                if column.is_nullable {
                    " (nullable)"
                } else {
                    ""
                }
            );
        }
    }

    println!("snapshot skeleton:");
    // Use spec table names (parsed from YAML before password resolution) to avoid
    // flowing credential-tainted data into output. SQL is trivially derived from table name.
    for table in &spec.source.capture.tables {
        let sql = {
            let parts: Vec<&str> = table.splitn(2, '.').collect();
            if parts.len() == 2 {
                format!(
                    "SELECT * FROM \"{}\".\"{}\"",
                    parts[0].replace('"', "\"\""),
                    parts[1].replace('"', "\"\"")
                )
            } else {
                format!("SELECT * FROM \"{table}\"")
            }
        };
        println!("- {table} -> {sql}");
    }

    Ok(())
}

pub async fn test_connection(file: &str, target: &str) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;

    let result = match target {
        "source" => {
            if spec.source.kind != "postgres" {
                bail!(
                    "test-connection: source kind '{}' is not supported yet",
                    spec.source.kind
                );
            }
            let source = PostgresSource::from_spec(&spec)?;
            source.test_connection().await
        }
        "destination" => {
            if spec.destination.kind != "postgres" {
                bail!(
                    "test-connection: destination kind '{}' is not supported yet",
                    spec.destination.kind
                );
            }
            let loader = PostgresDestinationLoader::from_spec(&spec)?;
            loader.test_connection().await
        }
        other => bail!(
            "unknown target '{}'; expected 'source' or 'destination'",
            other
        ),
    };

    if result.status == "ok" {
        println!("status: ok\nlatency_ms: {}", result.latency_ms.unwrap_or(0));
    } else {
        eprintln!(
            "status: error\nmessage: {}",
            result.message.as_deref().unwrap_or("unknown error")
        );
        if !result.missing_tables.is_empty() {
            eprintln!("missing_tables: {}", result.missing_tables.join(", "));
        }
        bail!("connection test failed");
    }

    Ok(())
}

pub async fn snapshot_to_local_staging(
    file: &str,
    max_rows_per_table: Option<u64>,
    staging_root: Option<PathBuf>,
    checkpoint_root: Option<PathBuf>,
    chunk_size: Option<u64>,
    no_resume: bool,
    control_plane_url: Option<String>,
) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    ensure_cdc_runtime_not_requested(&spec, "snapshot-to-local-staging")?;
    ensure_supported_snapshot_source(&spec)?;

    let staging = staging_from_spec(&spec)?;
    let source = PostgresSource::from_spec(&spec)?;
    let discovery = source
        .discover(PostgresDiscoverOptions { tables: vec![] })
        .await?;

    let root_dir = staging_root
        .or_else(default_staging_root_from_env)
        .unwrap_or_else(|| PathBuf::from(".astra/staging"));
    let checkpoint_root = checkpoint_root
        .or_else(default_checkpoint_root_from_env)
        .unwrap_or_else(|| PathBuf::from(".astra/checkpoints"));
    let checkpoint_store = LocalCheckpointStore::new(checkpoint_root.clone());
    let existing_ledger = if no_resume {
        SnapshotCheckpointLedger {
            pipeline_name: spec.pipeline.name.clone(),
            updated_at_unix_ms: 0,
            tables: BTreeMap::new(),
        }
    } else {
        checkpoint_store.load(&spec.pipeline.name)?
    };

    let snapshot_options =
        snapshot_execution_options(&spec, &existing_ledger, max_rows_per_table, chunk_size);
    let no_tables_remaining = no_tables_remaining(&spec, no_resume, &snapshot_options);

    let snapshot = if no_tables_remaining {
        astra_connectors::SnapshotExecutionReport {
            source_kind: "postgres".to_string(),
            tables: Vec::new(),
            table_progress: Vec::new(),
        }
    } else {
        source.snapshot_to_jsonl_gzip(snapshot_options).await?
    };

    let store = LocalStageChunkStore::new(LocalStagingConfig {
        root_dir: root_dir.clone(),
        storage: StagingConfig {
            kind: StagingKind::Local,
            bucket: staging.bucket,
            prefix: staging.prefix,
        },
    });

    store.ensure_ready().await?;
    checkpoint_store.ensure_ready()?;

    let control_plane = control_plane_url
        .as_deref()
        .map(ControlPlaneClient::new)
        .transpose()?;
    let run_id = if let Some(control_plane) = &control_plane {
        Some(
            control_plane
                .create_pipeline_run(&spec.pipeline.name, "manual")
                .await?,
        )
    } else {
        None
    };

    info!(
        pipeline = %spec.pipeline.name,
        catalog_tables = discovery.catalog.tables.len(),
        staging_root = %root_dir.display(),
        checkpoint_root = %checkpoint_root.display(),
        "discovered postgres source"
    );
    if no_resume {
        info!("resume mode: disabled (--no-resume)");
    } else {
        let resumable = existing_ledger
            .tables
            .iter()
            .filter(|(_, cp)| !cp.completed)
            .count();
        let completed = existing_ledger
            .tables
            .iter()
            .filter(|(_, cp)| cp.completed)
            .count();
        info!(
            resumable_tables = resumable,
            completed_tables = completed,
            "resume mode: enabled"
        );
    }
    info!("staging chunks");

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
            None, // cursor value persisted after all chunks via table_progress
        )?;

        let resolved = store.resolve_path(&chunk.object_key);
        if let (Some(control_plane), Some(run_id)) = (&control_plane, run_id.as_deref()) {
            control_plane
                .record_staged_artifact(
                    run_id,
                    &table.table,
                    "default",
                    table.sequence.try_into().context("sequence overflow")?,
                    &chunk.bucket,
                    &chunk.object_key,
                    chunk.bytes_written as i64,
                    chunk.row_count as i64,
                    json!({"sql": table.sql}),
                )
                .await?;
            control_plane
                .upsert_table_execution(
                    run_id,
                    &table.table,
                    "staged",
                    chunk.row_count as i64,
                    Some(chunk.row_count as i64),
                    None,
                    Some(
                        (table.sequence + 1)
                            .try_into()
                            .context("sequence overflow")?,
                    ),
                    Some(chunk.row_count as i64),
                    Some(chunk.object_key.clone()),
                    Some(false),
                )
                .await?;
        }
        info!(
            table = %table.table,
            rows = chunk.row_count,
            path = %resolved.display(),
            "staged chunk"
        );
        debug!(sql = %table.sql, bucket = %chunk.bucket, key = %chunk.object_key, bytes = chunk.bytes_written, sequence = chunk.sequence, "chunk detail");
    }

    info!("recording table progress");
    for progress in snapshot.table_progress {
        if let Some(cursor_value) = progress.max_cursor_value.clone() {
            checkpoint_store.update_cursor_value(
                &spec.pipeline.name,
                &progress.table,
                cursor_value,
            )?;
        }
        if progress.finished {
            checkpoint_store.mark_table_complete(&spec.pipeline.name, &progress.table)?;
        }
        if let (Some(control_plane), Some(run_id)) = (&control_plane, run_id.as_deref()) {
            control_plane
                .upsert_table_execution(
                    run_id,
                    &progress.table,
                    if progress.finished {
                        "snapshot_complete"
                    } else {
                        "snapshot_running"
                    },
                    progress.rows_emitted as i64,
                    None,
                    None,
                    Some(
                        progress
                            .next_sequence
                            .try_into()
                            .context("sequence overflow")?,
                    ),
                    Some(progress.rows_emitted as i64),
                    existing_ledger
                        .tables
                        .get(&progress.table)
                        .and_then(|cp| cp.last_chunk_key.clone()),
                    Some(progress.finished),
                )
                .await?;
        }
        info!(
            table = %progress.table,
            next_sequence = progress.next_sequence,
            rows_emitted = progress.rows_emitted,
            finished = progress.finished,
            "table progress"
        );
    }

    let final_ledger = checkpoint_store.load(&spec.pipeline.name)?;
    info!(
        ledger_path = %checkpoint_store.ledger_path(&spec.pipeline.name).display(),
        table_checkpoints = final_ledger.tables.len(),
        "checkpoint ledger updated"
    );

    if let (Some(control_plane), Some(run_id)) = (&control_plane, run_id.as_deref()) {
        let tables_completed = final_ledger
            .tables
            .values()
            .filter(|cp| cp.completed)
            .count();
        let rows_staged: i64 = final_ledger
            .tables
            .values()
            .map(|cp| cp.rows_staged as i64)
            .sum();
        control_plane
            .update_run_status(
                run_id,
                "succeeded",
                Some(json!({
                    "tables_completed": tables_completed,
                    "table_count": final_ledger.tables.len(),
                    "rows_staged": rows_staged,
                })),
            )
            .await?;
    }

    Ok(())
}

pub async fn snapshot_to_minio_staging(
    file: &str,
    max_rows_per_table: Option<u64>,
    endpoint: Option<String>,
    region: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    ensure_cdc_runtime_not_requested(&spec, "snapshot-to-minio-staging")?;
    ensure_supported_snapshot_source(&spec)?;

    let staging = staging_from_spec(&spec)?;
    let source = PostgresSource::from_spec(&spec)?;
    let discovery = source
        .discover(PostgresDiscoverOptions { tables: vec![] })
        .await?;
    let snapshot = source
        .snapshot_to_jsonl_gzip(SnapshotExecutionOptions {
            tables: vec![],
            max_rows_per_table,
            chunk_size: None,
            start_sequence_by_table: BTreeMap::new(),
            cursor_field: None,
            last_cursor_by_table: BTreeMap::new(),
        })
        .await?;

    let storage = StagingConfig {
        kind: StagingKind::Minio,
        bucket: staging.bucket,
        prefix: staging.prefix,
    };
    let mut config = MinioStagingConfig::from_env(storage.clone())?;
    if let Some(endpoint) = endpoint {
        config.endpoint = endpoint;
    }
    if let Some(region) = region {
        config.region = region;
    }
    if let Some(access_key) = access_key {
        config.access_key = access_key;
    }
    if let Some(secret_key) = secret_key {
        config.secret_key = secret_key;
    }

    let store = MinioStageChunkStore::new(config.clone());
    store.ensure_ready().await?;

    info!(
        pipeline = %spec.pipeline.name,
        catalog_tables = discovery.catalog.tables.len(),
        minio_endpoint = %config.endpoint,
        staging_bucket = %config.storage.bucket,
        "discovered postgres source, staging to MinIO"
    );

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

        info!(table = %table.table, rows = chunk.row_count, "staged chunk");
        debug!(sql = %table.sql, s3_path = %format!("s3://{}/{}", chunk.bucket, chunk.object_key), bytes = chunk.bytes_written, "chunk detail");
    }

    Ok(())
}

pub async fn execute_local_snapshot(
    file: &str,
    max_rows_per_table: Option<u64>,
    staging_root: Option<PathBuf>,
    checkpoint_root: Option<PathBuf>,
    chunk_size: Option<u64>,
    no_resume: bool,
    control_plane_url: Option<String>,
) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    ensure_cdc_runtime_not_requested(&spec, "execute-local-snapshot")?;
    ensure_supported_snapshot_source(&spec)?;
    ensure_supported_local_snapshot_destination(&spec)?;

    info!(pipeline = %spec.pipeline.name, "local snapshot execution started");
    info!("stage 1/2: snapshot -> local staging");
    snapshot_to_local_staging(
        file,
        max_rows_per_table,
        staging_root.clone(),
        checkpoint_root,
        chunk_size,
        no_resume,
        control_plane_url.clone(),
    )
    .await?;

    info!("stage 2/2: local staging -> postgres");
    load_local_staging_to_postgres(file, staging_root, control_plane_url).await?;

    info!(pipeline = %spec.pipeline.name, "local snapshot execution complete: snapshot -> local staging -> postgres");

    Ok(())
}

pub async fn load_local_staging_to_postgres(
    file: &str,
    staging_root: Option<PathBuf>,
    control_plane_url: Option<String>,
) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;

    let staging = spec
        .destination
        .staging
        .as_ref()
        .context("destination.staging is required for local destination loading")?;
    let loader = PostgresDestinationLoader::from_spec(&spec)?;
    let root_dir = staging_root
        .or_else(default_staging_root_from_env)
        .unwrap_or_else(|| PathBuf::from(".astra/staging"));
    let store = LocalStageChunkStore::new(LocalStagingConfig {
        root_dir: root_dir.clone(),
        storage: StagingConfig {
            kind: StagingKind::Local,
            bucket: staging.bucket.clone(),
            prefix: staging.prefix.clone().unwrap_or_default(),
        },
    });

    let control_plane = control_plane_url
        .as_deref()
        .map(ControlPlaneClient::new)
        .transpose()?;
    let run_id = if let Some(control_plane) = &control_plane {
        Some(
            control_plane
                .create_pipeline_run(&spec.pipeline.name, "load-local-staging")
                .await?,
        )
    } else {
        None
    };

    let staged_chunks = store.list_chunks_for_pipeline(&spec.pipeline.name)?;
    if staged_chunks.is_empty() {
        bail!(
            "no staged chunks found for pipeline {} under {}",
            spec.pipeline.name,
            root_dir.display()
        );
    }

    let mut chunk_payloads = Vec::new();
    for mut chunk in staged_chunks {
        let bytes = store.read_chunk(&chunk).await?;
        chunk.bytes_written = bytes.len() as u64;
        chunk_payloads.push((chunk, bytes));
    }

    let report = loader.load_local_stage_chunks(chunk_payloads).await?;
    info!(
        schema = %report.schema,
        destination_host = %format!("{}:{}", loader.config().host, loader.config().port),
        staging_root = %root_dir.display(),
        "loaded staged chunks into postgres raw schema"
    );
    for chunk in &report.applied_chunks {
        if let (Some(control_plane), Some(run_id)) = (&control_plane, run_id.as_deref()) {
            control_plane
                .upsert_table_execution(
                    run_id,
                    &chunk.table_name,
                    "applied",
                    chunk.rows_written as i64,
                    Some(chunk.rows_written as i64),
                    None,
                    None,
                    Some(chunk.rows_written as i64),
                    Some(chunk.object_key.clone()),
                    Some(true),
                )
                .await?;
        }
        info!(
            object_key = %chunk.object_key,
            table = %chunk.table_name,
            status = if chunk.skipped { "already applied" } else { "applied" },
            "chunk applied"
        );
        debug!(rows_written = chunk.rows_written, "chunk rows written");
    }

    if let (Some(control_plane), Some(run_id)) = (&control_plane, run_id.as_deref()) {
        let total_rows: i64 = report
            .applied_chunks
            .iter()
            .map(|chunk| chunk.rows_written as i64)
            .sum();
        control_plane
            .update_run_status(
                run_id,
                "succeeded",
                Some(json!({
                    "chunks_applied": report.applied_chunks.len(),
                    "rows_written": total_rows,
                    "schema": report.schema,
                })),
            )
            .await?;
    }

    Ok(())
}

struct ResolvedStaging {
    bucket: String,
    prefix: String,
}

fn ensure_cdc_runtime_not_requested(spec: &AstraSpec, command: &str) -> anyhow::Result<()> {
    if spec.requests_cdc() {
        return Err(anyhow!(
            "{command} does not execute CDC yet. Astra's Postgres source is currently limited to schema discovery and snapshot staging; remove pipeline.mode=cdc and source.capture.cdc for this command."
        ));
    }

    Ok(())
}

fn ensure_supported_snapshot_source(spec: &AstraSpec) -> anyhow::Result<()> {
    if spec.source.kind != "postgres" {
        bail!("snapshot staging currently supports source.kind=postgres only");
    }

    Ok(())
}

fn ensure_supported_local_snapshot_destination(spec: &AstraSpec) -> anyhow::Result<()> {
    if spec.destination.kind != "postgres" {
        bail!(
            "execute-local-snapshot currently supports destination.kind=postgres only because the end-to-end local loader is the Postgres raw loader"
        );
    }

    if spec.destination.staging.is_none() {
        bail!("execute-local-snapshot requires destination.staging for the local staging handoff");
    }

    Ok(())
}

fn staging_from_spec(spec: &AstraSpec) -> anyhow::Result<ResolvedStaging> {
    let staging = spec
        .destination
        .staging
        .as_ref()
        .context("destination.staging is required for snapshot staging")?;

    Ok(ResolvedStaging {
        bucket: staging.bucket.clone(),
        prefix: staging.prefix.clone().unwrap_or_default(),
    })
}

fn snapshot_execution_options(
    spec: &AstraSpec,
    ledger: &SnapshotCheckpointLedger,
    max_rows_per_table: Option<u64>,
    chunk_size: Option<u64>,
) -> SnapshotExecutionOptions {
    let is_incremental = spec
        .source
        .capture
        .snapshot
        .as_ref()
        .map(|s| s.mode == astra_yaml::SnapshotMode::Incremental)
        .unwrap_or(false);

    // In incremental mode every table runs on every invocation — the cursor
    // filters which rows are actually fetched. In full mode completed tables
    // are skipped (resume-within-a-single-run behaviour).
    let tables = spec
        .source
        .capture
        .tables
        .iter()
        .map(|table| table.trim().to_string())
        .filter(|table| !table.is_empty())
        .filter(|table| {
            is_incremental
                || !ledger
                    .tables
                    .get(table)
                    .map(|checkpoint| checkpoint.completed)
                    .unwrap_or(false)
        })
        .collect();

    // Always carry the sequence counter forward so object keys never collide
    // across runs.
    let start_sequence_by_table = ledger
        .tables
        .iter()
        .map(|(table, checkpoint)| (table.clone(), checkpoint.next_sequence))
        .collect();

    let cursor_field = spec
        .source
        .capture
        .snapshot
        .as_ref()
        .and_then(|s| s.cursor_field.clone())
        .filter(|s| !s.trim().is_empty());

    let last_cursor_by_table = if cursor_field.is_some() {
        ledger
            .tables
            .iter()
            .filter_map(|(table, checkpoint)| {
                checkpoint
                    .last_cursor_value
                    .clone()
                    .map(|v| (table.clone(), v))
            })
            .collect()
    } else {
        BTreeMap::new()
    };

    SnapshotExecutionOptions {
        tables,
        max_rows_per_table,
        chunk_size,
        start_sequence_by_table,
        cursor_field,
        last_cursor_by_table,
    }
}

fn no_tables_remaining(
    spec: &AstraSpec,
    no_resume: bool,
    options: &SnapshotExecutionOptions,
) -> bool {
    !no_resume && !spec.source.capture.tables.is_empty() && options.tables.is_empty()
}

fn default_staging_root_from_env() -> Option<PathBuf> {
    std::env::var_os("ASTRA_STAGING_LOCAL_ROOT").map(PathBuf::from)
}

fn default_checkpoint_root_from_env() -> Option<PathBuf> {
    std::env::var_os("ASTRA_CHECKPOINT_LOCAL_ROOT").map(PathBuf::from)
}

struct ControlPlaneClient {
    base_url: String,
    http: Client,
}

impl ControlPlaneClient {
    fn new(base_url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: Client::new(),
        })
    }

    async fn create_pipeline_run(
        &self,
        pipeline_name: &str,
        trigger_mode: &str,
    ) -> anyhow::Result<String> {
        let response: Value = self
            .http
            .post(format!("{}/api/v1/pipeline-runs", self.base_url))
            .json(&json!({
                "pipeline_name": pipeline_name,
                "trigger_mode": trigger_mode,
                "status": "running",
                "started_at": Utc::now(),
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response
            .get("id")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .context("control-plane create_pipeline_run response missing id")
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_staged_artifact(
        &self,
        run_id: &str,
        stream_name: &str,
        partition_key: &str,
        sequence: i64,
        bucket: &str,
        object_key: &str,
        bytes_written: i64,
        row_count: i64,
        metadata_json: Value,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!(
                "{}/api/v1/pipeline-runs/{}/artifacts",
                self.base_url, run_id
            ))
            .json(&json!({
                "stream_name": stream_name,
                "partition_key": partition_key,
                "sequence": sequence,
                "bucket": bucket,
                "object_key": object_key,
                "bytes_written": bytes_written,
                "row_count": row_count,
                "content_type": "application/jsonl",
                "content_encoding": "gzip",
                "metadata_json": metadata_json,
            }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn upsert_table_execution(
        &self,
        run_id: &str,
        stream_name: &str,
        status: &str,
        rows_processed: i64,
        rows_total: Option<i64>,
        error_summary: Option<String>,
        checkpoint_next_sequence: Option<i64>,
        checkpoint_rows_staged: Option<i64>,
        checkpoint_last_chunk_key: Option<String>,
        checkpoint_completed: Option<bool>,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!(
                "{}/api/v1/pipeline-runs/{}/table-executions",
                self.base_url, run_id
            ))
            .json(&json!({
                "stream_name": stream_name,
                "status": status,
                "rows_processed": rows_processed,
                "rows_total": rows_total,
                "error_summary": error_summary,
                "checkpoint_next_sequence": checkpoint_next_sequence,
                "checkpoint_rows_staged": checkpoint_rows_staged,
                "checkpoint_last_chunk_key": checkpoint_last_chunk_key,
                "checkpoint_completed": checkpoint_completed,
            }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn update_run_status(
        &self,
        run_id: &str,
        status: &str,
        stats_json: Option<Value>,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!(
                "{}/api/v1/pipeline-runs/{}/status",
                self.base_url, run_id
            ))
            .json(&json!({
                "status": status,
                "stats_json": stats_json,
            }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use astra_runtime::SnapshotTableCheckpoint;

    fn sample_spec() -> AstraSpec {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/postgres-to-warehouse.astra.yaml");
        AstraSpec::from_file(path.to_str().expect("utf-8 path")).expect("sample spec loads")
    }

    fn full_mode_spec() -> AstraSpec {
        AstraSpec::parse_yaml(
            r#"
version: v1alpha1
pipeline:
  name: postgres-full
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
  capture:
    tables:
      - public.users
      - public.orders
    snapshot:
      mode: full
      chunkSize: 25
destination:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: warehouse
    username: warehouse_user
  staging:
    kind: local
    bucket: astra-staging
  write:
    mode: append
runtime: {}
"#,
        )
        .expect("full mode spec parses")
    }

    #[test]
    fn rejects_cdc_for_snapshot_commands() {
        let spec = AstraSpec::parse_yaml(
            r#"
version: v1alpha1
pipeline:
  name: postgres-cdc
  mode: cdc
  schedule: continuous
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
    passwordRef: env:POSTGRES_PASSWORD
  capture:
    tables:
      - public.users
    cdc:
      slotName: astra_slot
      publicationName: astra_publication
destination:
  kind: snowflake
  staging:
    kind: s3
    bucket: astra-staging
  write:
    mode: merge
runtime: {}
"#,
        )
        .expect("spec parses");

        let error = ensure_cdc_runtime_not_requested(&spec, "snapshot-to-local-staging")
            .expect_err("cdc should be rejected");

        assert!(error
            .to_string()
            .contains("snapshot-to-local-staging does not execute CDC yet"));
    }

    #[test]
    fn snapshot_execution_options_skip_completed_tables_in_full_mode() {
        // Use a full-mode spec — completed tables should be skipped (resume behaviour).
        let spec = full_mode_spec();
        let mut ledger = SnapshotCheckpointLedger {
            pipeline_name: spec.pipeline.name.clone(),
            updated_at_unix_ms: 0,
            tables: BTreeMap::new(),
        };
        ledger.tables.insert(
            "public.users".to_string(),
            astra_runtime::SnapshotTableCheckpoint {
                next_sequence: 1,
                rows_staged: 10,
                last_chunk_key: Some("users/0".to_string()),
                completed: true,
                updated_at_unix_ms: 1,
                last_cursor_value: None,
            },
        );
        ledger.tables.insert(
            "public.orders".to_string(),
            astra_runtime::SnapshotTableCheckpoint {
                next_sequence: 3,
                rows_staged: 30,
                last_chunk_key: Some("orders/2".to_string()),
                completed: false,
                updated_at_unix_ms: 2,
                last_cursor_value: None,
            },
        );

        let options = snapshot_execution_options(&spec, &ledger, Some(100), Some(25));

        // users is completed → skipped; orders is incomplete → included.
        assert_eq!(options.tables, vec!["public.orders".to_string()]);
        // Sequences are carried forward for all tables to prevent object key collisions.
        assert_eq!(
            options.start_sequence_by_table.get("public.orders"),
            Some(&3)
        );
        assert_eq!(
            options.start_sequence_by_table.get("public.users"),
            Some(&1)
        );
        assert_eq!(options.max_rows_per_table, Some(100));
        assert_eq!(options.chunk_size, Some(25));
    }

    #[test]
    fn snapshot_execution_options_reruns_completed_tables_in_incremental_mode() {
        // In incremental mode completed tables must run again (cursor filters the delta).
        let spec = sample_spec(); // incremental mode
        let mut ledger = SnapshotCheckpointLedger {
            pipeline_name: spec.pipeline.name.clone(),
            updated_at_unix_ms: 0,
            tables: BTreeMap::new(),
        };
        ledger.tables.insert(
            "public.users".to_string(),
            astra_runtime::SnapshotTableCheckpoint {
                next_sequence: 1,
                rows_staged: 10,
                last_chunk_key: Some("users/0".to_string()),
                completed: true,
                updated_at_unix_ms: 1,
                last_cursor_value: Some(serde_json::json!("2024-01-01T00:00:00Z")),
            },
        );

        let options = snapshot_execution_options(&spec, &ledger, None, None);

        // users is completed but incremental — must still be included.
        assert!(
            options.tables.contains(&"public.users".to_string()),
            "completed table should still run in incremental mode"
        );
        // Sequence must continue from ledger to avoid object key collision.
        assert_eq!(
            options.start_sequence_by_table.get("public.users"),
            Some(&1)
        );
        // Cursor value must be loaded so the WHERE clause is applied.
        assert_eq!(
            options.last_cursor_by_table.get("public.users"),
            Some(&serde_json::json!("2024-01-01T00:00:00Z"))
        );
    }

    #[test]
    fn snapshot_execution_options_keep_uncheckpointed_tables() {
        let spec = sample_spec();
        let ledger = SnapshotCheckpointLedger {
            pipeline_name: spec.pipeline.name.clone(),
            updated_at_unix_ms: 0,
            tables: BTreeMap::new(),
        };

        let options = snapshot_execution_options(&spec, &ledger, None, None);

        assert_eq!(
            options.tables,
            vec!["public.users".to_string(), "public.orders".to_string()]
        );
        assert!(options.start_sequence_by_table.is_empty());
        assert!(!no_tables_remaining(&spec, false, &options));
    }

    #[test]
    fn no_tables_remaining_when_everything_is_complete_in_full_mode() {
        let spec = full_mode_spec();
        let mut ledger = SnapshotCheckpointLedger {
            pipeline_name: spec.pipeline.name.clone(),
            updated_at_unix_ms: 0,
            tables: BTreeMap::new(),
        };
        for table in ["public.users", "public.orders"] {
            ledger.tables.insert(
                table.to_string(),
                SnapshotTableCheckpoint {
                    next_sequence: 1,
                    rows_staged: 10,
                    last_chunk_key: Some(format!("{table}/0")),
                    completed: true,
                    updated_at_unix_ms: 1,
                    last_cursor_value: None,
                },
            );
        }

        let options = snapshot_execution_options(&spec, &ledger, None, Some(25));

        assert!(options.tables.is_empty());
        assert!(no_tables_remaining(&spec, false, &options));
        assert!(!no_tables_remaining(&spec, true, &options));
    }

    #[test]
    fn execute_local_snapshot_rejects_non_postgres_destination() {
        let spec = AstraSpec::parse_yaml(
            r#"
version: v1alpha1
pipeline:
  name: postgres-to-snowflake
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
    passwordRef: env:POSTGRES_PASSWORD
  capture:
    tables:
      - public.users
    snapshot:
      mode: full
      chunkSize: 1000
destination:
  kind: snowflake
  staging:
    kind: s3
    bucket: astra-staging
  write:
    mode: merge
runtime: {}
"#,
        )
        .expect("spec parses");

        let error = ensure_supported_local_snapshot_destination(&spec)
            .expect_err("non-postgres destination should be rejected");

        assert!(error
            .to_string()
            .contains("execute-local-snapshot currently supports destination.kind=postgres only"));
    }
}
