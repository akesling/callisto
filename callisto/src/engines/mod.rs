use std::collections::BTreeMap;

use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserOptions};

use datafusion::datasource::file_format::options::ParquetReadOptions;
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
            Engine::DuckDB => Box::new(DuckDbImpl::default()),
            Engine::DataFusion => Box::new(DataFusionImpl::default()),
        })
    }
}

#[async_trait::async_trait]
pub trait EngineInterface {
    async fn execute(&mut self, query: &str) -> anyhow::Result<()>;
}

#[derive(Default)]
struct PolarsImpl {
    fs_name_to_table_name: BTreeMap<String, String>,
    context: polars::sql::SQLContext,
}

impl PolarsImpl {
    fn load_tables(&mut self, query: &ast::Statement) -> anyhow::Result<ast::Statement> {
        let mut rewritten = query.clone();
        let mut new_tables = Vec::new();
        ast::visit_relations_mut(&mut rewritten, |table| {
            let symbol_or_file: &str = &table.0[0].value;
            let table_name =
                if let Some(table_name) = self.fs_name_to_table_name.get(symbol_or_file) {
                    table_name.to_string()
                } else {
                    let table_name = derive_table_from_fs_name(symbol_or_file);
                    new_tables.push((symbol_or_file.to_string(), table_name.clone()));
                    table_name
                };
            table.0[0].value = table.0[0].value.replace(symbol_or_file, &table_name);
            core::ops::ControlFlow::<()>::Continue(())
        });

        for (fs_name, table_name) in new_tables {
            let frame = LazyFrame::scan_parquet(&fs_name, Default::default());
            match frame {
                Ok(frame) => {
                    self.fs_name_to_table_name
                        .insert(fs_name.to_string(), table_name.clone());
                    self.context.register(&table_name, frame);
                }
                Err(error) => println!(
                    "Warning -- loading referenced parquet path ({}) failed with error: {}",
                    fs_name, error
                ),
            }
        }
        Ok(rewritten)
    }
}

#[async_trait::async_trait]
impl EngineInterface for PolarsImpl {
    async fn execute(&mut self, query: &str) -> anyhow::Result<()> {
        let mut parser = Parser::new(&GenericDialect);
        parser = parser.with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        });

        let ast = parser.try_with_sql(query)?.parse_statements()?;

        for statement in ast {
            println!("\n$ {}", statement.to_string());
            // TODO(alex): Table loading should be column aware so we don't load unnecessary
            // columns here.
            let res: LazyFrame = tokio::task::block_in_place(|| {
                self.load_tables(&statement).and_then(|transformed_stmt| {
                    self.context
                        .execute(&transformed_stmt.to_string())
                        .map_err(|error| error.into())
                })
            })?;
            println!("Results:\n{}", res.collect()?);
        }
        Ok(())
    }
}

struct DuckDbImpl {
    fs_name_to_table_name: BTreeMap<String, String>,
    connection: duckdb::Connection,
}

impl Default for DuckDbImpl {
    fn default() -> DuckDbImpl {
        DuckDbImpl {
            connection: duckdb::Connection::open_in_memory().unwrap(),
            fs_name_to_table_name: Default::default(),
        }
    }
}

impl DuckDbImpl {
    fn load_tables(&mut self, query: &ast::Statement) -> anyhow::Result<ast::Statement> {
        let mut rewritten = query.clone();
        let mut new_tables = Vec::new();
        ast::visit_relations_mut(&mut rewritten, |table| {
            let symbol_or_file: &str = &table.0[0].value;
            let table_name =
                if let Some(table_name) = self.fs_name_to_table_name.get(symbol_or_file) {
                    table_name.to_string()
                } else {
                    let table_name = derive_table_from_fs_name(symbol_or_file);
                    new_tables.push((symbol_or_file.to_string(), table_name.clone()));
                    table_name
                };
            table.0[0].value = table.0[0].value.replace(symbol_or_file, &table_name);
            core::ops::ControlFlow::<()>::Continue(())
        });

        for (fs_name, table_name) in new_tables {
            self.connection.execute(
                &format!(
                    "CREATE TABLE {} AS SELECT * FROM READ_PARQUET('{}', union_by_name=true);",
                    table_name, fs_name
                ),
                duckdb::params![],
            )?;
            self.fs_name_to_table_name
                .insert(fs_name.to_string(), table_name.clone());
        }
        Ok(rewritten)
    }
}

#[async_trait::async_trait]
impl EngineInterface for DuckDbImpl {
    async fn execute(&mut self, query: &str) -> anyhow::Result<()> {
        let mut parser = Parser::new(&GenericDialect);
        parser = parser.with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        });

        let ast = parser.try_with_sql(query)?.parse_statements()?;

        for statement in ast {
            println!("\n$ {}", statement.to_string());
            // TODO(alex): Table loading should be column aware so we don't load unnecessary
            // columns here.
            let res: Vec<duckdb::arrow::record_batch::RecordBatch> =
                tokio::task::block_in_place(|| {
                    self.load_tables(&statement).and_then(|transformed_stmt| {
                        let stmt = self
                            .connection
                            .prepare(&transformed_stmt.to_string())
                            .map_err(|error| error.into());
                        stmt.and_then(|mut stmt| stmt.query_arrow([]).map(|query| query.collect()))
                            .map_err(|error| error.into())
                    })
                })?;
            println!("Results:");
            duckdb::arrow::util::pretty::print_batches(&res)?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct DataFusionImpl {
    fs_name_to_table_name: BTreeMap<String, String>,
    context: datafusion::execution::context::SessionContext,
}

impl DataFusionImpl {
    async fn load_tables(&mut self, query: &ast::Statement) -> anyhow::Result<ast::Statement> {
        let mut rewritten = query.clone();
        let mut new_tables = Vec::new();
        ast::visit_relations_mut(&mut rewritten, |table| {
            let symbol_or_file: &str = &table.0[0].value;
            let table_name =
                if let Some(table_name) = self.fs_name_to_table_name.get(symbol_or_file) {
                    table_name.to_string()
                } else {
                    let table_name = derive_table_from_fs_name(symbol_or_file);
                    new_tables.push((symbol_or_file.to_string(), table_name.clone()));
                    table_name
                };
            table.0[0].value = table.0[0].value.replace(symbol_or_file, &table_name);
            core::ops::ControlFlow::<()>::Continue(())
        });

        for (fs_name, table_name) in new_tables {
            let res = self
                .context
                .register_parquet(&table_name, &fs_name, ParquetReadOptions::default())
                .await;
            match res {
                Ok(()) => {
                    self.fs_name_to_table_name
                        .insert(fs_name.to_string(), table_name.clone());
                }
                Err(error) => println!(
                    "Warning -- loading referenced parquet path ({}) failed with error: {}",
                    fs_name, error
                ),
            }
        }
        Ok(rewritten)
    }
}

#[async_trait::async_trait]
impl EngineInterface for DataFusionImpl {
    async fn execute(&mut self, query: &str) -> anyhow::Result<()> {
        let parser = Parser::new(&GenericDialect).with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        });

        let ast = parser.try_with_sql(query)?.parse_statements()?;

        for statement in ast {
            println!("\n$ {}", statement.to_string());
            // TODO(alex): Table loading should be column aware so we don't load unnecessary
            // columns here.
            let transformed_stmt = self.load_tables(&statement).await?;
            let results = self.context.sql(&transformed_stmt.to_string()).await?;
            let pretty_results =
                arrow::util::pretty::pretty_format_batches(&results.collect().await?)?.to_string();
            println!("Results:\n{}", pretty_results);
        }
        Ok(())
    }
}

fn derive_table_from_fs_name(fs_name: &str) -> String {
    format!(
        "tbl_{}",
        fs_name
            .split('/')
            .last()
            .unwrap()
            .replace(".", "_")
            .replace("-", "_")
            .replace("*", "_")
    )
}
