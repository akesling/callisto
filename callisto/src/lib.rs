pub use callisto_engines::{Engine, EngineInterface};

pub struct Repl {}

impl Repl {
    pub async fn run(engine: &mut Box<dyn EngineInterface>) -> anyhow::Result<()> {
        use futures::stream::StreamExt as _;
        use std::io::Write as _;
        use tokio::io::AsyncBufReadExt as _;

        let stdin = tokio::io::stdin();
        let reader = tokio::io::BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Some(line) = {
            print!("> ");
            std::io::stdout().flush()?;
            lines.next_line().await.unwrap()
        } {
            let command = line.trim();
            if ["exit", "bye", "q", "quit"].contains(&command.to_lowercase().as_str()) {
                break;
            }

            let executions = match engine.execute(&command).await {
                Ok(e) => e,
                Err(error) => {
                    println!("Error: {:?}", error);
                    continue;
                }
            };
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
        }
        println!("\nGoodbye!");
        Ok(())
    }
}
