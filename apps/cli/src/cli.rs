use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "astra", about = "Astra CLI", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
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
        #[arg(long)]
        control_plane_url: Option<String>,
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
        #[arg(long)]
        control_plane_url: Option<String>,
    },
    /// Run the narrow local snapshot happy path end to end: snapshot -> local staging -> Postgres load
    ExecuteLocalSnapshot {
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
        #[arg(long)]
        control_plane_url: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_snapshot_to_local_staging_flags() {
        let cli = Cli::parse_from([
            "astra",
            "snapshot-to-local-staging",
            "examples/postgres-to-warehouse.astra.yaml",
            "--max-rows-per-table",
            "25",
            "--staging-root",
            "/tmp/astra/staging",
            "--checkpoint-root",
            "/tmp/astra/checkpoints",
            "--chunk-size",
            "10",
            "--no-resume",
        ]);

        assert_eq!(
            cli.command,
            Command::SnapshotToLocalStaging {
                file: "examples/postgres-to-warehouse.astra.yaml".to_string(),
                max_rows_per_table: Some(25),
                staging_root: Some(PathBuf::from("/tmp/astra/staging")),
                checkpoint_root: Some(PathBuf::from("/tmp/astra/checkpoints")),
                chunk_size: Some(10),
                no_resume: true,
                control_plane_url: None,
            }
        );
    }

    #[test]
    fn parses_snapshot_to_minio_staging_flags() {
        let cli = Cli::parse_from([
            "astra",
            "snapshot-to-minio-staging",
            "examples/postgres-to-postgres-raw.astra.yaml",
            "--max-rows-per-table",
            "100",
            "--endpoint",
            "http://localhost:9000",
            "--region",
            "us-east-1",
            "--access-key",
            "minio",
            "--secret-key",
            "miniostorage",
        ]);

        assert_eq!(
            cli.command,
            Command::SnapshotToMinioStaging {
                file: "examples/postgres-to-postgres-raw.astra.yaml".to_string(),
                max_rows_per_table: Some(100),
                endpoint: Some("http://localhost:9000".to_string()),
                region: Some("us-east-1".to_string()),
                access_key: Some("minio".to_string()),
                secret_key: Some("miniostorage".to_string()),
            }
        );
    }

    #[test]
    fn parses_load_local_staging_to_postgres_flags() {
        let cli = Cli::parse_from([
            "astra",
            "load-local-staging-to-postgres",
            "examples/postgres-to-postgres-raw.astra.yaml",
            "--staging-root",
            "/tmp/astra/staging",
        ]);

        assert_eq!(
            cli.command,
            Command::LoadLocalStagingToPostgres {
                file: "examples/postgres-to-postgres-raw.astra.yaml".to_string(),
                staging_root: Some(PathBuf::from("/tmp/astra/staging")),
                control_plane_url: None,
            }
        );
    }

    #[test]
    fn parses_execute_local_snapshot_flags() {
        let cli = Cli::parse_from([
            "astra",
            "execute-local-snapshot",
            "examples/postgres-to-postgres-raw.astra.yaml",
            "--max-rows-per-table",
            "100",
            "--staging-root",
            "/tmp/astra/staging",
            "--checkpoint-root",
            "/tmp/astra/checkpoints",
            "--chunk-size",
            "50",
            "--no-resume",
            "--control-plane-url",
            "http://127.0.0.1:8080",
        ]);

        assert_eq!(
            cli.command,
            Command::ExecuteLocalSnapshot {
                file: "examples/postgres-to-postgres-raw.astra.yaml".to_string(),
                max_rows_per_table: Some(100),
                staging_root: Some(PathBuf::from("/tmp/astra/staging")),
                checkpoint_root: Some(PathBuf::from("/tmp/astra/checkpoints")),
                chunk_size: Some(50),
                no_resume: true,
                control_plane_url: Some("http://127.0.0.1:8080".to_string()),
            }
        );
    }
}
