use core::pin::Pin;
use std::collections::BTreeMap;
use std::sync::Arc;

use futures::Stream;

use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserOptions};

use arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::physical_plan::SendableRecordBatchStream;
use polars_lazy::frame::LazyFrame;

mod polars_to_arrow;

pub enum Engine {
    Polars,
    DuckDB,
    DataFusion,
}

impl Engine {
    pub fn new(&self) -> anyhow::Result<Box<dyn EngineInterface>> {
        Ok(match self {
            Engine::Polars => Box::new(polars_engine::default()),
            Engine::DuckDB => Box::new(duckdb_engine::default()),
            Engine::DataFusion => Box::new(datafusion_engine::default()),
        })
    }
}

#[async_trait::async_trait]
pub trait EngineInterface {
    async fn execute(
        &mut self,
        query: &str,
    ) -> anyhow::Result<Vec<(sqlparser::ast::Statement, SendableRecordBatchStream)>>;
}

mod polars_engine {
    use super::*;

    pub fn default() -> PolarsImpl {
        PolarsImpl::default()
    }

    #[derive(Default)]
    pub struct PolarsImpl {
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
        async fn execute(
            &mut self,
            query: &str,
        ) -> anyhow::Result<Vec<(sqlparser::ast::Statement, SendableRecordBatchStream)>> {
            use polars::prelude::SerWriter as _;
            let mut parser = Parser::new(&GenericDialect);
            parser = parser.with_options(ParserOptions {
                trailing_commas: true,
                ..Default::default()
            });

            let ast = parser.try_with_sql(query)?.parse_statements()?;

            let mut executions = Vec::new();
            for statement in ast {
                // TODO(alex): Table loading should be column aware so we don't load unnecessary
                // columns here.
                let mut df: polars::frame::DataFrame = tokio::task::block_in_place(|| {
                    self.load_tables(&statement).and_then(|transformed_stmt| {
                        let lazy_frame = self
                            .context
                            .execute(&transformed_stmt.to_string())
                            .map_err(|error| error.into());
                        lazy_frame.and_then(|frame| frame.collect().map_err(|error| error.into()))
                    })
                })?;
                let schema = Arc::new(polars_to_arrow::convert_schema(
                    df.schema().to_arrow(false),
                )?);
                let (arrow_client, mut polars_server) = tokio::io::duplex(1024);
                // TODO(alex): Figure out how to refactor this so it performs fewer (preferably no)
                // copies.  Perhaps convert the Polars arrays in memory, returning a an object
                // implmenting the stream which holds the dataframe memory?
                let polars_writer_handle = tokio::task::spawn_blocking(move || {
                    polars_io::ipc::IpcStreamWriter::new(tokio_util::io::SyncIoBridge::new(
                        &mut polars_server,
                    ))
                    .finish(&mut df)
                });
                let (datafusion_tx, datafusion_rx) = tokio::sync::mpsc::channel(100);
                // TODO(alex): Handle this join
                let _join_handle = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                    let arrow_stream =
                        datafusion::common::arrow::ipc::reader::StreamReader::try_new(
                            tokio_util::io::SyncIoBridge::new(arrow_client),
                            None,
                        )?;
                    for record_batch in arrow_stream {
                        datafusion_tx.blocking_send(record_batch.map_err(|error| {
                            datafusion::error::DataFusionError::ArrowError(error, None)
                        }))?;
                    }
                    Ok(polars_writer_handle)
                });
                let stream: SendableRecordBatchStream = Box::pin(StreamFromPolars {
                    stream: tokio_stream::wrappers::ReceiverStream::new(datafusion_rx),
                    schema,
                });
                // TODO(alex): Figure out how to push this streamification down into the execution
                // instead of post-collection.
                executions.push((statement, stream));
            }
            Ok(executions)
        }
    }

    #[pin_project::pin_project]
    struct StreamFromPolars<S> {
        #[pin]
        stream: S,
        schema: Arc<arrow::datatypes::Schema>,
    }

    impl<S> datafusion::physical_plan::RecordBatchStream for StreamFromPolars<S>
    where
        S: Stream<Item = Result<RecordBatch, datafusion::common::DataFusionError>>,
    {
        fn schema(&self) -> Arc<arrow::datatypes::Schema> {
            self.schema.clone()
        }
    }

    impl<S> Stream for StreamFromPolars<S>
    where
        S: Stream<Item = Result<RecordBatch, datafusion::common::DataFusionError>>,
    {
        type Item = S::Item;

        fn poll_next(
            self: Pin<&mut Self>,
            cx: &mut futures::task::Context<'_>,
        ) -> futures::task::Poll<Option<Self::Item>> {
            self.project().stream.poll_next(cx)
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.stream.size_hint()
        }
    }
}

mod duckdb_engine {
    use super::*;

    pub fn default() -> DuckDbImpl {
        DuckDbImpl::default()
    }

    pub struct DuckDbImpl {
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
        async fn execute(
            &mut self,
            query: &str,
        ) -> anyhow::Result<Vec<(sqlparser::ast::Statement, SendableRecordBatchStream)>> {
            let mut parser = Parser::new(&GenericDialect);
            parser = parser.with_options(ParserOptions {
                trailing_commas: true,
                ..Default::default()
            });

            let ast = parser.try_with_sql(query)?.parse_statements()?;

            let mut executions = Vec::new();
            for statement in ast {
                // TODO(alex): Table loading should be column aware so we don't load unnecessary
                // columns here.
                let res: Vec<duckdb::arrow::record_batch::RecordBatch> =
                    tokio::task::block_in_place(|| {
                        self.load_tables(&statement).and_then(|transformed_stmt| {
                            let stmt = self
                                .connection
                                .prepare(&transformed_stmt.to_string())
                                .map_err(|error| error.into());
                            stmt.and_then(|mut stmt| {
                                stmt.query_arrow([]).map(|query| query.collect())
                            })
                            .map_err(|error| error.into())
                        })
                    })?;
                let schema = res[0].schema().clone();
                let mem_stream =
                    datafusion::physical_plan::memory::MemoryStream::try_new(res, schema, None)?;
                let stream: SendableRecordBatchStream = Box::pin(mem_stream);
                // TODO(alex): Figure out how to push this streamification down into the execution
                // instead of post-collection.
                executions.push((statement, stream));
            }
            Ok(executions)
        }
    }
}

mod datafusion_engine {
    use super::*;

    pub fn default() -> DataFusionImpl {
        DataFusionImpl::default()
    }

    #[derive(Default)]
    pub struct DataFusionImpl {
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
        async fn execute(
            &mut self,
            query: &str,
        ) -> anyhow::Result<Vec<(sqlparser::ast::Statement, SendableRecordBatchStream)>> {
            let parser = Parser::new(&GenericDialect).with_options(ParserOptions {
                trailing_commas: true,
                ..Default::default()
            });

            let ast = parser.try_with_sql(query)?.parse_statements()?;

            let mut executions = Vec::new();
            for statement in ast {
                // TODO(alex): Table loading should be column aware so we don't load unnecessary
                // columns here.
                let transformed_stmt = self.load_tables(&statement).await?;
                let stream = self
                    .context
                    .sql(&transformed_stmt.to_string())
                    .await?
                    .execute_stream()
                    .await?;
                executions.push((statement, stream))
            }
            Ok(executions)
        }
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
