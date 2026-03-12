use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "astra", about = "Astra CLI", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Validate an Astra YAML spec
    Validate { file: String },
    /// Apply an Astra YAML spec to the control plane
    Apply { file: String },
    /// Discover schema details for a Postgres source using a local/self-hosted database
    DiscoverSource { file: String },
    /// Execute a local Postgres snapshot, chunk rows into staged files, and persist resume checkpoints
    SnapshotToLocalStaging {
        file: String,
        #[arg(long)]
        max_rows_per_table: Option<u64>,
        #[arg(long)]
        staging_root: Option<PathBuf>,
        #[arg(long)]
        checkpoint_root: Option<PathBuf>,
        #[arg(long)]
        chunk_size: Option<u64>,
        #[arg(long)]
        no_resume: bool,
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
