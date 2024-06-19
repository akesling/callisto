use std::collections::BTreeMap;

use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserOptions};

use polars_lazy::frame::LazyFrame;

pub enum Engine {
    Polars,
    DuckDB,
    DataFusion,
}

impl Engine {
    pub fn new(&self) -> anyhow::Result<Box<dyn EngineInterface>> {
        Ok(match self {
            Engine::Polars => Box::new(PolarsImpl::default()),
            _ => todo!("Only Polars engine is implemented."),
        })
    }
}

pub trait EngineInterface {
    fn execute(&mut self, query: &str) -> anyhow::Result<()>;
}

#[derive(Default)]
struct PolarsImpl {
    fs_name_to_table_name: BTreeMap<String, String>,
    context: polars::sql::SQLContext,
}

impl PolarsImpl {
    fn load_tables(&mut self, query: &ast::Statement) -> anyhow::Result<ast::Statement> {
        let mut rewritten = query.clone();
        ast::visit_relations_mut(&mut rewritten, |table| {
            let symbol_or_file: &str = &table.0[0].value;
            // println!(
            //     "Relation ({}) visited: {}",
            //     symbol_or_file,
            //     serde_json::to_string_pretty(&table).unwrap()
            // );
            if let Some(table_name) = self.fs_name_to_table_name.get(symbol_or_file) {
                table.0[0].value = table.0[0].value.replace(symbol_or_file, table_name);
            } else if symbol_or_file.ends_with(".parquet") {
                let table_name = format!("tbl_{}", symbol_or_file
                    .split('/')
                    .last()
                    .unwrap()
                    .replace(".", "_")
                    .replace("-", "_")
                    .replace("*", "_"));
                let frame = LazyFrame::scan_parquet(&symbol_or_file, Default::default());
                match frame {
                    Ok(frame) => {
                        self.fs_name_to_table_name
                            .insert(symbol_or_file.to_string(), table_name.clone());
                        table.0[0].value = table.0[0].value.replace(symbol_or_file, &table_name);
                        self.context.register(&table_name, frame);
                    }
                    Err(error) => println!(
                        "Warning -- loading referenced parquet path ({}) failed with error: {}",
                        symbol_or_file, error
                    ),
                }
            }
            core::ops::ControlFlow::<()>::Continue(())
        });
        Ok(rewritten)
    }
}

impl EngineInterface for PolarsImpl {
    fn execute(&mut self, query: &str) -> anyhow::Result<()> {
        println!("Executing query: {query}");
        let mut parser = Parser::new(&GenericDialect);
        parser = parser.with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        });

        let ast = parser.try_with_sql(query)?.parse_statements()?;

        for statement in ast {
            // println!(
            //     "Executing parse query statement: {}",
            //     serde_json::to_string_pretty(&statement)?
            // );
            // TODO(alex): Table loading should be column aware so we don't load unnecessary
            // columns here.
            let transformed_stmt = self.load_tables(&statement)?;
            // println!(
            //     "Transformed query statement: {}",
            //     serde_json::to_string_pretty(&transformed_stmt)?
            // );
            let res = self.context.execute(&transformed_stmt.to_string())?;
            print!("Results:\n{}", res.collect()?);
        }
        Ok(())
    }
}
