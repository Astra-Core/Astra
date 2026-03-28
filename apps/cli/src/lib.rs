pub mod cli;
pub mod commands;

use clap::Parser;
use cli::{Cli, Command};

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    dispatch(cli.command).await
}

async fn dispatch(command: Command) -> anyhow::Result<()> {
    match command {
        Command::Validate { file } => commands::validate(&file),
        Command::Apply { file } => commands::apply(&file),
        Command::DiscoverSource { file } => commands::discover_source(&file).await,
        Command::SnapshotToLocalStaging {
            file,
            max_rows_per_table,
            staging_root,
            checkpoint_root,
            chunk_size,
            no_resume,
            control_plane_url,
        } => {
            commands::snapshot_to_local_staging(
                &file,
                max_rows_per_table,
                staging_root,
                checkpoint_root,
                chunk_size,
                no_resume,
                control_plane_url,
            )
            .await
        }
        Command::SnapshotToMinioStaging {
            file,
            max_rows_per_table,
            endpoint,
            region,
            access_key,
            secret_key,
        } => {
            commands::snapshot_to_minio_staging(
                &file,
                max_rows_per_table,
                endpoint,
                region,
                access_key,
                secret_key,
            )
            .await
        }
        Command::LoadLocalStagingToPostgres {
            file,
            staging_root,
            control_plane_url,
        } => commands::load_local_staging_to_postgres(&file, staging_root, control_plane_url).await,
    }
}
