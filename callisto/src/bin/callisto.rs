use clap::Parser;
use serde::Serialize;

/// Multi-engine data exploration terminal UI
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    Exec {
        // TODO(akesling): Add output format control
        /// Command to execute
        command: String,

        /// Engine on which to execute
        #[arg(long, short, default_value_t, value_enum)]
        engine: Engine,
    },
    Repl {
        /// Engine on which to execute
        #[arg(long, short, default_value_t, value_enum)]
        engine: Engine,
    },
    Console {},
}

#[derive(clap::ValueEnum, Clone, Debug, Serialize, Default)]
enum Engine {
    Polars,
    DuckDB,
    #[default]
    DataFusion,
}

impl Engine {
    pub fn new(&self) -> anyhow::Result<Box<dyn callisto::EngineInterface>> {
        match self {
            Engine::Polars => callisto::Engine::Polars.new(),
            Engine::DuckDB => callisto::Engine::DuckDB.new(),
            Engine::DataFusion => callisto::Engine::DataFusion.new(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use futures::stream::StreamExt as _;
    let args = Args::parse();

    match args.command {
        Command::Exec {
            command,
            engine: engine_type,
        } => {
            println!(
                "Running command '{}' on engine '{}'",
                command,
                &serde_json::to_string(&engine_type).unwrap()
            );

            let mut engine = engine_type.new()?;
            let executions = engine.execute(&command).await?;
            for (statement, mut stream) in executions {
                println!("\n$ {}", statement.to_string());
                let mut batches = Vec::new();
                while let Some(items) = stream.next().await {
                    batches.push(items?);
                }
                let pretty_results =
                    arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
                println!("Results:\n{}", pretty_results);
            }
            Ok(())
        }
        Command::Repl {
            engine: engine_type,
        } => {
            let mut engine = engine_type.new()?;

            callisto::Repl::run(&mut engine, tokio::io::stdin(), tokio::io::stdout()).await?;
            Ok(())
        }
        Command::Console {} => {
            println!("Console not yet supported");
            std::process::exit(1);
        }
    }
}
