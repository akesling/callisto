pub use callisto_engines::{Engine, EngineInterface};

pub mod console;

pub struct Repl<Output> {
    output: Output,
}

impl<Output> Repl<Output>
where
    Output: tokio::io::AsyncWriteExt + Unpin,
{
    async fn print(&mut self, text: &str) -> tokio::io::Result<()> {
        self.output.write_all(text.as_bytes()).await
    }

    async fn println(&mut self, text: &str) -> tokio::io::Result<()> {
        self.print(text).await?;
        self.print("\n").await
    }

    pub async fn run<Input>(
        engine: &mut Box<dyn EngineInterface>,
        input: Input,
        output: Output,
    ) -> anyhow::Result<()>
    where
        Input: tokio::io::AsyncRead + Unpin,
    {
        use futures::stream::StreamExt as _;
        use tokio::io::AsyncBufReadExt as _;

        let mut repl = Repl { output };

        let reader = tokio::io::BufReader::new(input);
        let mut lines = reader.lines();

        while let Some(line) = {
            repl.print("> ").await?;
            repl.output.flush().await?;
            lines.next_line().await.unwrap()
        } {
            let command = line.trim();
            if ["exit", "bye", "q", "quit"].contains(&command.to_lowercase().as_str()) {
                break;
            }

            let executions = match engine.execute(&command).await {
                Ok(e) => e,
                Err(error) => {
                    repl.println(&format!("Error: {:?}", error)).await?;
                    continue;
                }
            };
            for (statement, mut stream) in executions {
                repl.println(&format!("\n$ {}", statement.to_string()))
                    .await?;
                let mut batches = Vec::new();
                while let Some(items) = stream.next().await {
                    batches.push(items?);
                }
                let pretty_results =
                    arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
                repl.println(&format!("Results:\n{}", pretty_results))
                    .await?;
            }
        }
        repl.println("\nGoodbye!").await?;
        Ok(())
    }
}
