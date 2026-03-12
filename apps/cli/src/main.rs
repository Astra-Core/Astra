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
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Validate { file } => validate(&file)?,
        Commands::Apply { file } => apply(&file)?,
    }
    Ok(())
}

fn validate(file: &str) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    println!("valid Astra spec: {}", spec.pipeline.name);
    println!("mode: {:?}", spec.pipeline.mode);
    println!(
        "source: {} -> destination: {}",
        spec.source.kind, spec.destination.kind
    );
    Ok(())
}

fn apply(file: &str) -> anyhow::Result<()> {
    let spec = AstraSpec::from_file(file)?;
    spec.validate()?;
    println!("apply stub for validated pipeline: {}", spec.pipeline.name);
    println!("next step: send normalized spec to control-plane API");
    Ok(())
}
