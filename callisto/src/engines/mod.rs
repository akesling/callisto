pub enum Engine {
    Polars,
    DuckDB,
}

impl Engine {
    pub fn new(&self) -> anyhow::Result<Box<dyn EngineInterface>> {
        Ok(match self {
            Engine::Polars => Box::new(PolarsImpl {}),
            _ => todo!("Only Polars engine is implemented."),
        })
    }
}

pub trait EngineInterface {
    fn execute(&mut self, query: &str) -> anyhow::Result<()>;
}

struct PolarsImpl {}

impl EngineInterface for PolarsImpl {
    fn execute(&mut self, query: &str) -> anyhow::Result<()> {
        println!("Executing Polars query: {}", query);
        Ok(())
    }
}
