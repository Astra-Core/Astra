use anyhow::{bail, Context};
use astra_connectors::{
    PostgresDestinationLoader, PostgresDiscoverOptions, PostgresSource, SnapshotExecutionOptions,
};
use astra_runtime::{
    LocalStageChunkStore, LocalStagingConfig, MinioStageChunkStore, MinioStagingConfig,
    StageChunkPayload, StageChunkRequest, StageChunkStore, StagingConfig, StagingKind,
};
use astra_yaml::AstraSpec;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "astra", about = "Astra CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate an Astra YAML spec
    Validate { file: String },
    /// Apply an Astra YAML spec to the control plane
    Apply { file: String },
    /// Discover schema details for a Postgres source using a local/self-hosted database
    DiscoverSource { file: String },
    /// Execute a minimal local Postgres snapshot and write staged chunks to the filesystem adapter
    SnapshotToLocalStaging {
        file: String,
        #[arg(long)]
        max_rows_per_table: Option<u64>,
        #[arg(long)]
        staging_root: Option<PathBuf>,
    },
    /// Execute a minimal local Postgres snapshot and write staged chunks to MinIO/S3-compatible storage
    SnapshotToMinioStaging {
        file: String,
        #[arg(long)]
        max_rows_per_table: Option<u64>,
        #[arg(long)]
        endpoint: Option<String>,
        #[arg(long)]
        region: Option<String>,
        #[arg(long)]
        access_key: Option<String>,
        #[arg(long)]
        secret_key: Option<String>,
    },
    /// Load locally staged JSONL.gz chunks into raw Postgres destination tables
    LoadLocalStagingToPostgres {
        file: String,
        #[arg(long)]
        staging_root: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Validate { file } => validate(&file)?,
        Commands::Apply { file } => apply(&file)?,
        Commands::DiscoverSource { file } => discover_source(&file).await?,
        Commands::SnapshotToLocalStaging {
            file,
            max_rows_per_table,
            staging_root,
        } => snapshot_to_local_staging(&file, max_rows_per_table, staging_root).await?,
        Commands::SnapshotToMinioStaging {
            file,
            max_rows_per_table,
            endpoint,
            region,
            access_key,
            secret_key,
        } => {
            snapshot_to_minio_staging(
                &file,
                max_rows_per_table,
                endpoint,
                region,
                access_key,
                secret_key,
            )
            .await?
        }
        Commands::LoadLocalStagingToPostgres { file, staging_root } => {
            load_local_staging_to_postgres(&file, staging_root).await?
        }
    }
    Ok(())
}

fn validate(file: &str) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    let source = if spec.source.kind == "postgres" {
        Some(PostgresSource::from_spec(&spec)?)
    } else {
        None
    };

    println!("valid Astra spec: {}", spec.pipeline.name);
    println!("mode: {:?}", spec.pipeline.mode);
    println!(
        "source: {} -> destination: {}",
        spec.source.kind, spec.destination.kind
    );
    if let Some(source) = source {
        println!("postgres tables: {}", source.config().tables.join(", "));
    }
    Ok(())
}

fn apply(file: &str) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    println!("apply stub for validated pipeline: {}", spec.pipeline.name);
    println!("next step: send normalized spec to control-plane API");
    Ok(())
}

async fn discover_source(file: &str) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    let source = PostgresSource::from_spec(&spec)?;
    let report = source
        .discover(PostgresDiscoverOptions { tables: vec![] })
        .await?;

    println!("discovered postgres source: {}", spec.pipeline.name);
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
    for table in &report.snapshot_plan.tables {
        println!("- {} -> {}", table.table, table.sql);
    }

    Ok(())
}

async fn snapshot_to_local_staging(
    file: &str,
    max_rows_per_table: Option<u64>,
    staging_root: Option<PathBuf>,
) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
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
        })
        .await?;

    let root_dir = staging_root
        .or_else(default_staging_root_from_env)
        .unwrap_or_else(|| PathBuf::from(".astra/staging"));
    let store = LocalStageChunkStore::new(LocalStagingConfig {
        root_dir: root_dir.clone(),
        storage: StagingConfig {
            kind: StagingKind::Local,
            bucket: staging.bucket,
            prefix: staging.prefix,
        },
    });

    store.ensure_ready().await?;

    println!("discovered postgres source: {}", spec.pipeline.name);
    println!("catalog tables: {}", discovery.catalog.tables.len());
    println!("local staging root: {}", root_dir.display());
    println!("staged chunks:");

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

        let resolved = store.resolve_path(&chunk.object_key);
        println!(
            "- {} -> {} rows -> {}",
            table.table,
            chunk.row_count,
            resolved.display()
        );
        println!("  sql: {}", table.sql);
        println!(
            "  chunk: bucket={} key={} bytes={}",
            chunk.bucket, chunk.object_key, chunk.bytes_written
        );
    }

    Ok(())
}

async fn snapshot_to_minio_staging(
    file: &str,
    max_rows_per_table: Option<u64>,
    endpoint: Option<String>,
    region: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
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

    println!("discovered postgres source: {}", spec.pipeline.name);
    println!("catalog tables: {}", discovery.catalog.tables.len());
    println!("minio endpoint: {}", config.endpoint);
    println!("staging bucket: {}", config.storage.bucket);
    println!("staged chunks:");

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

        println!("- {} -> {} rows", table.table, chunk.row_count);
        println!("  sql: {}", table.sql);
        println!(
            "  chunk: s3://{}/{} bytes={}",
            chunk.bucket, chunk.object_key, chunk.bytes_written
        );
    }

    Ok(())
}

struct ResolvedStaging {
    bucket: String,
    prefix: String,
}

fn ensure_supported_snapshot_source(spec: &AstraSpec) -> anyhow::Result<()> {
    if spec.source.kind != "postgres" {
        bail!("snapshot staging currently supports source.kind=postgres only");
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

async fn load_local_staging_to_postgres(
    file: &str,
    staging_root: Option<PathBuf>,
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
    println!(
        "loaded staged chunks into postgres raw schema: {}",
        report.schema
    );
    println!(
        "destination host: {}:{}",
        loader.config().host,
        loader.config().port
    );
    println!("local staging root: {}", root_dir.display());
    for chunk in report.applied_chunks {
        println!(
            "- {} -> {} ({})",
            chunk.object_key,
            chunk.table_name,
            if chunk.skipped {
                "already applied"
            } else {
                "applied"
            }
        );
        println!("  rows written: {}", chunk.rows_written);
    }

    Ok(())
}

fn default_staging_root_from_env() -> Option<PathBuf> {
    std::env::var_os("ASTRA_STAGING_LOCAL_ROOT").map(PathBuf::from)
}
