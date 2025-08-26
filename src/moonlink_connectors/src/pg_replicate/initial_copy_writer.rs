use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use moonlink_error::{ErrorStatus, ErrorStruct};
use tokio::sync::mpsc;

use crate::pg_replicate::conversions::table_row::TableRow;
use crate::Result;
use moonlink::row::RowValue;
use moonlink::{BatchIdCounter, ColumnStoreBuffer, DiskSliceWriterConfig, MooncakeTableConfig};

/// Configuration for initial-copy Parquet writing.
#[derive(Clone, Debug)]
pub struct InitialCopyWriterConfig {
    /// Target max file size in bytes before rotating to a new Parquet file.
    pub target_file_size_bytes: usize,
    /// Max number of rows per Arrow RecordBatch before flushing to the writer.
    pub max_rows_per_batch: usize,
}

impl Default for InitialCopyWriterConfig {
    fn default() -> Self {
        Self {
            // Align with disk slice writer default parquet file size
            target_file_size_bytes: DiskSliceWriterConfig::default_disk_slice_parquet_file_size(),
            // Align batch size with mooncake table default batch size
            max_rows_per_batch: MooncakeTableConfig::default_batch_size(),
        }
    }
}

/// Create a bounded channel for passing Arrow RecordBatches from table copy to writer.
pub fn create_batch_channel(capacity: usize) -> (BatchSender, BatchReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    (BatchSender(tx), BatchReceiver(rx))
}

/// Sending end of the batch channel.
pub struct BatchSender(mpsc::Sender<RecordBatch>);

impl BatchSender {
    /// Send a RecordBatch to the writer. Returns Err if the receiver is closed.
    pub async fn send(&self, batch: RecordBatch) -> Result<()> {
        self.0.send(batch).await.map_err(|_| {
            crate::Error::MpscChannelSendError(ErrorStruct::new(
                "batch sender closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?;
        Ok(())
    }
}

/// Receiving end of the batch channel.
pub struct BatchReceiver(mpsc::Receiver<RecordBatch>);

impl BatchReceiver {
    /// Receive the next RecordBatch from the channel. Returns None if closed.
    pub async fn recv(&mut self) -> Option<RecordBatch> {
        self.0.recv().await
    }
}

/// A batch builder that accumulates TableRow values from PG copy and produces RecordBatches.
///
/// Reuses moonlink::ColumnStoreBuffer to avoid duplicating Arrow builder logic.
/// Converts PG TableRow cells to RowValue and delegates to the internal buffer.
pub struct ArrowBatchBuilder {
    buffer: ColumnStoreBuffer,
}

impl ArrowBatchBuilder {
    pub fn new(schema: Arc<Schema>, max_rows: usize) -> Self {
        let batch_id_counter = Arc::new(BatchIdCounter::new(false)); // Temporary instance of batch id counter
        let buffer = ColumnStoreBuffer::new(schema, max_rows, batch_id_counter);
        Self { buffer }
    }

    /// Append a TableRow from PG copy. Returns an immediately-finished
    /// RecordBatch if the buffer is full, otherwise None.
    pub fn append_table_row(&mut self, table_row: TableRow) -> Result<Option<RecordBatch>> {
        // Convert TableRow cells to RowValue
        let row_values: Vec<RowValue> = table_row
            .values
            .into_iter()
            .map(|cell| cell.into())
            .collect();

        let (_batch_id, _row_offset, finished_batch) =
            self.buffer.append_initial_copy_row(row_values)?;

        Ok(finished_batch.map(|(_, batch)| batch.as_ref().clone()))
    }

    /// Finish the current batch and return a RecordBatch.
    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self
            .buffer
            .finalize_current_batch()?
            .map(|(_, batch)| (*batch).clone()))
    }
}

/// Writes RecordBatches into Parquet files with rotation.
///
/// Implemented with parquet::arrow::AsyncArrowWriter and file rotation based on writer.memory_size().
pub struct ParquetFileWriter {
    pub output_dir: PathBuf,
    pub schema: Arc<Schema>,
    pub config: InitialCopyWriterConfig,
}

impl ParquetFileWriter {
    pub fn new(output_dir: PathBuf, schema: Arc<Schema>, config: InitialCopyWriterConfig) -> Self {
        Self {
            output_dir,
            schema,
            config,
        }
    }

    fn next_file_path(&self) -> PathBuf {
        let filename = format!("ic-{}.parquet", uuid::Uuid::now_v7());
        self.output_dir.join(filename)
    }

    /// Consume RecordBatches and write Parquet files.
    /// Returns the list of file paths written.
    pub async fn write_from_channel(mut self, mut rx: BatchReceiver) -> Result<Vec<String>> {
        use moonlink::get_default_parquet_properties;
        use parquet::arrow::AsyncArrowWriter;

        let mut files_written: Vec<String> = Vec::new();
        let mut writer: Option<AsyncArrowWriter<tokio::fs::File>> = None;
        let mut current_file_path: Option<PathBuf> = None;

        while let Some(batch) = rx.recv().await {
            if batch.num_columns() == 0 || batch.num_rows() == 0 {
                continue;
            }

            if writer.is_none() {
                let path = self.next_file_path();
                // Ensure directory exists
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await.ok();
                }
                let file = tokio::fs::File::create(&path).await?;
                let props = get_default_parquet_properties();
                let w = AsyncArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
                writer = Some(w);
                current_file_path = Some(path.clone());
            }

            let w = writer.as_mut().unwrap();
            w.write(&batch).await?;

            // Rotate when current writer exceeds target size.
            if w.memory_size() >= self.config.target_file_size_bytes {
                w.finish().await?;
                if let Some(p) = current_file_path.take() {
                    files_written.push(p.to_string_lossy().to_string());
                }
                writer = None;
            }
        }

        // Finalize any open writer.
        if let Some(mut w) = writer.take() {
            w.finish().await?;
            if let Some(p) = current_file_path.take() {
                files_written.push(p.to_string_lossy().to_string());
            }
        }

        Ok(files_written)
    }
}

// TODO: Add unit tests
