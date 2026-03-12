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

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Validate { file } => {
            println!("validate stub: {file}");
            println!("real schema validation lands next");
        }
        Commands::Apply { file } => {
            println!("apply stub: {file}");
            println!("real control-plane apply path lands next");
        }
    }
}
