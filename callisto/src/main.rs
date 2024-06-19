use clap::Parser;
use serde::Serialize;

/// Multi-engine data exploration terminal UI
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
struct Args {
    /// Command to execute
    #[arg(long, short)]
    command: String,

    /// Command to execute
    #[arg(long, short, default_value_t, value_enum)]
    engine: Engine,
}

#[derive(clap::ValueEnum, Clone, Debug, Serialize, Default)]
enum Engine {
    #[default]
    Polars,
    DuckDB,
}

fn main() {
    let args = Args::parse();

    println!(
        "Running command '{}' on engine '{}'",
        args.command,
        &serde_json::to_string(&args.engine).unwrap()
    );
}
