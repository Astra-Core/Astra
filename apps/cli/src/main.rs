use astra_connectors::{PostgresDiscoverOptions, PostgresSource};
use astra_yaml::AstraSpec;
use clap::{Parser, Subcommand};

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Validate { file } => validate(&file)?,
        Commands::Apply { file } => apply(&file)?,
        Commands::DiscoverSource { file } => discover_source(&file).await?,
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
