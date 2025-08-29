use crate::pg_replicate::conversions::table_row::TableRow;
use crate::pg_replicate::initial_copy_writer::{
    create_batch_channel, ArrowBatchBuilder, InitialCopyWriterConfig, ParquetFileWriter,
};
use crate::pg_replicate::postgres_source::TableCopyStreamError;
use crate::pg_replicate::postgres_source::{PostgresSource, TableCopyStream};
use crate::pg_replicate::table::{ColumnSchema, LookupKey, SrcTableId, TableName, TableSchema};
use crate::pg_replicate::util::postgres_schema_to_moonlink_schema;
use crate::Result;
use futures::{pin_mut, Stream, StreamExt};
use moonlink::TableEvent;
use moonlink_error::ErrorStatus;
use moonlink_error::ErrorStruct;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tokio_postgres::types::Type;

/// Represents progress information for an ongoing copy.
#[derive(Debug)]
pub struct CopyProgress {
    /// Last WAL LSN processed from the CDC stream.
    pub last_lsn: PgLsn,
    /// Number of rows copied so far.
    pub rows_copied: u64,
}

/// Reads rows from `stream` and sends them to the provided `event_sender`.
pub async fn copy_table_stream<S>(
    table_schema: TableSchema,
    mut stream: S,
    event_sender: &Sender<TableEvent>,
    start_lsn: u64,
    config: Option<InitialCopyWriterConfig>,
) -> Result<CopyProgress>
where
    S: Stream<Item = std::result::Result<TableRow, TableCopyStreamError>>,
{
    // Convert PostgreSQL schema to Arrow schema
    let (arrow_schema, _identity_prop) = postgres_schema_to_moonlink_schema(&table_schema);
    let arrow_schema = Arc::new(arrow_schema);

    // Create output directory for initial copy files
    let output_dir = std::env::temp_dir()
        .join("moonlink_initial_copy")
        .join(format!("table_{}", table_schema.src_table_id));

    // Create batch channel for RecordBatches
    let (batch_tx, batch_rx) = create_batch_channel(16); // Channel capacity of 16

    let config = config.unwrap_or_default();

    // Create Arrow batch builder
    let mut batch_builder = ArrowBatchBuilder::new(arrow_schema.clone(), config.max_rows_per_batch);

    // Create and spawn Parquet file writer
    let parquet_writer = ParquetFileWriter {
        output_dir,
        schema: arrow_schema,
        config,
    };
    let writer_handle =
        tokio::spawn(async move { parquet_writer.write_from_channel(batch_rx).await });

    // Stream rows into batches
    pin_mut!(stream);
    let mut rows_copied = 0u64;
    while let Some(row) = stream.next().await {
        let row = row.map_err(|e| crate::Error::from(e))?;

        // Append row to batch builder and check if a batch is ready
        if let Some(finished_batch) = batch_builder.append_table_row(row)? {
            if let Err(e) = batch_tx.send(finished_batch).await {
                tracing::error!(error = ?e, "failed to send batch to Parquet writer");
                break;
            }
        }
        rows_copied += 1;
    }

    // Finalize any remaining batch
    if let Some(final_batch) = batch_builder.finish()? {
        if let Err(e) = batch_tx.send(final_batch).await {
            tracing::error!(error = ?e, "failed to send final batch to Parquet writer");
        }
    }

    // Close channel to signal writer completion
    drop(batch_tx);

    // Wait for writer to finish and get list of files written
    let files_written = writer_handle.await??;

    tracing::info!(
        "Initial copy completed: {} rows, {} files written",
        rows_copied,
        files_written.len()
    );

    // Send LoadFiles event to batch ingest the Parquet files
    if let Err(e) = event_sender
        .send(TableEvent::LoadFiles {
            files: files_written,
            lsn: start_lsn,
        })
        .await
    {
        tracing::warn!(error = ?e, "failed to send LoadFiles event");
    }

    Ok(CopyProgress {
        last_lsn: PgLsn::from(start_lsn),
        rows_copied,
    })
}

/// Tests for the initial copy functionality
#[cfg(test)]
mod tests {
    use std::fs;
    use std::panic::Location;
    use std::path::Path;

    use super::*;
    use crate::pg_replicate::conversions::table_row::{TableRow, TableRowConversionError};
    use crate::pg_replicate::conversions::Cell;
    use crate::pg_replicate::postgres_source::TableCopyStreamError;
    use crate::pg_replicate::table::{ColumnSchema, LookupKey, TableName};
    use futures::stream;
    use moonlink_error::ErrorStruct;
    use tempfile;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};
    use tokio_postgres::types::Type;

    //----------------------------------------------------------------------
    // Minimal helpers/stubs
    //----------------------------------------------------------------------

    /// Build a simple TableSchema for testing
    fn make_test_schema(name: &str) -> TableSchema {
        TableSchema {
            table_name: TableName {
                schema: "public".to_string(),
                name: name.to_string(),
            },
            src_table_id: 1,
            column_schemas: vec![ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT4,
                modifier: 0,
                nullable: false,
            }],
            lookup_key: LookupKey::FullRow,
        }
    }

    /// Create a simple test error for testing error propagation
    fn make_test_error() -> crate::Error {
        crate::Error::PostgresSourceError(ErrorStruct {
            message: "Postgres source error".to_string(),
            status: moonlink_error::ErrorStatus::Permanent,
            source: Some(Arc::new(
                crate::pg_replicate::postgres_source::PostgresSourceError::MissingPublication
                    .into(),
            )),
            location: Some(Location::caller().to_string()),
        })
    }

    //----------------------------------------------------------------------
    // 1. Happy-path – emits LoadFiles
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_copies_rows_and_reports_progress() {
        // Create a stream of successful TableRow results
        let table_row1 = crate::pg_replicate::conversions::table_row::TableRow {
            values: vec![crate::pg_replicate::conversions::Cell::I32(1)],
        };
        let table_row2 = crate::pg_replicate::conversions::table_row::TableRow {
            values: vec![crate::pg_replicate::conversions::Cell::I32(2)],
        };
        let table_row3 = crate::pg_replicate::conversions::table_row::TableRow {
            values: vec![crate::pg_replicate::conversions::Cell::I32(3)],
        };

        let rows = vec![Ok(table_row1), Ok(table_row2), Ok(table_row3)];
        let stream = stream::iter(rows);

        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let schema = make_test_schema("test");
        let start_lsn = 1000u64;

        let progress = copy_table_stream(schema, Box::pin(stream), &tx, start_lsn, None)
            .await
            .expect("copy failed");

        assert_eq!(progress.rows_copied, 3);
        assert_eq!(progress.last_lsn, PgLsn::from(start_lsn));

        // Should receive LoadFiles event
        let evt = timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("channel lag")
            .expect("sender closed");
        let files: Vec<String> = match evt {
            TableEvent::LoadFiles { files, lsn } => {
                assert_eq!(lsn, start_lsn);
                assert!(!files.is_empty(), "should have written at least one file");
                files
            }
            _ => panic!("expected LoadFiles event, got {evt:?}"),
        };
        // Files exist
        for f in &files {
            assert!(std::path::Path::new(f).exists(), "file not found: {}", f);
        }

        // Explicitly drop sender to close channel and avoid hanging
        drop(tx);

        // No more events
        assert!(rx.recv().await.is_none(), "no extra events expected");
    }

    //----------------------------------------------------------------------
    // 2. Empty snapshot – emits LoadFiles(empty)
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_handles_empty_stream() {
        let stream =
            stream::iter(vec![] as Vec<std::result::Result<TableRow, TableCopyStreamError>>);
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let schema = make_test_schema("empty");

        let progress = copy_table_stream(schema, Box::pin(stream), &tx, 0u64, None)
            .await
            .expect("copy failed");
        assert_eq!(progress.rows_copied, 0);

        // LoadFiles(empty)
        let evt = timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("channel lag")
            .expect("sender closed");
        match evt {
            TableEvent::LoadFiles { files, lsn } => {
                assert!(files.is_empty());
                assert_eq!(lsn, 0);
            }
            _ => panic!("expected LoadFiles event"),
        }

        drop(tx);
        assert!(rx.recv().await.is_none(), "no events expected");
    }

    //----------------------------------------------------------------------
    // 3. Rotation – force multiple files via tiny config
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_writes_multiple_files_with_rotation() {
        // Force tiny file size and small batches
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: 1024, // ~1KB
            max_rows_per_batch: 10,
        };

        // Build many rows
        let mut rows_vec = Vec::new();
        for i in 0..200 {
            let tr = TableRow {
                values: vec![Cell::I32(i)],
            };
            rows_vec.push(Ok(tr));
        }
        let stream = stream::iter(rows_vec);
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let schema = make_test_schema("rotate");
        let start_lsn = 42u64;

        let _ = copy_table_stream(schema, Box::pin(stream), &tx, start_lsn, Some(config))
            .await
            .expect("copy failed");

        // LoadFiles with multiple files
        let evt = timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("channel lag")
            .expect("sender closed");
        let files = match evt {
            TableEvent::LoadFiles { files, lsn } => {
                assert_eq!(lsn, start_lsn);
                assert!(
                    files.len() >= 2,
                    "expected at least 2 files, got {}",
                    files.len()
                );
                files
            }
            _ => panic!("expected LoadFiles"),
        };
        for f in &files {
            assert!(std::path::Path::new(f).exists(), "file not found: {}", f);
        }

        drop(tx);

        // No explicit no-more-events assertion here
    }

    //----------------------------------------------------------------------
    // 4. Stream error – bubbles up to the caller
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_propagates_stream_error() {
        // Use a conversion error that maps to our Error type
        let stream = stream::iter(vec![Err(TableCopyStreamError::ConversionError(
            TableRowConversionError::UnterminatedRow,
        ))]
            as Vec<std::result::Result<TableRow, TableCopyStreamError>>);
        let (tx, _) = mpsc::channel::<TableEvent>(1);
        let schema = make_test_schema("broken");

        let err = copy_table_stream(schema, Box::pin(stream), &tx, 0u64, None)
            .await
            .expect_err("expected failure");

        // Just verify we got an error - the exact format may vary
        let err_str = err.to_string().to_lowercase();
        assert!(
            err_str.contains("unterminated row") || err_str.contains("conversion error"),
            "unexpected error string: {}",
            err.to_string()
        );
    }
}
