use anyhow::{bail, Context};
use astra_connectors::{PostgresDiscoverOptions, PostgresSource, SnapshotExecutionOptions};
use astra_runtime::{
    LocalStageChunkStore, LocalStagingConfig, StageChunkRequest, StageChunkStore, StagingConfig,
    StagingKind,
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

    if spec.source.kind != "postgres" {
        bail!("snapshot-to-local-staging currently supports source.kind=postgres only");
    }

    let staging = spec
        .destination
        .staging
        .as_ref()
        .context("destination.staging is required for local snapshot staging")?;
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
            bucket: staging.bucket.clone(),
            prefix: staging.prefix.clone().unwrap_or_default(),
        },
    });

    store.ensure_ready()?;

    println!("discovered postgres source: {}", spec.pipeline.name);
    println!("catalog tables: {}", discovery.catalog.tables.len());
    println!("local staging root: {}", root_dir.display());
    println!("staged chunks:");

    for table in snapshot.tables {
        let chunk = store.write_chunk(StageChunkRequest {
            pipeline_name: spec.pipeline.name.clone(),
            stream_name: table.table.clone(),
            partition_key: "default".to_string(),
            sequence: table.sequence,
            payload: astra_runtime::StageChunkPayload::jsonl_gzip(
                table.row_count,
                table.rows_jsonl_gzip,
            ),
        })?;

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

fn default_staging_root_from_env() -> Option<PathBuf> {
    std::env::var_os("ASTRA_STAGING_LOCAL_ROOT").map(PathBuf::from)
}
