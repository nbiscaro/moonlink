use crate::{
    create_data_file, storage::mooncake_table::transaction_stream::TransactionStreamCommit,
};

use super::*;

use futures::{stream, StreamExt};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use std::path::PathBuf;
use tokio::fs::File;

use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;

impl MooncakeTable {
    /// Batch ingestion the given [`parquet_files`] into mooncake table.
    ///
    /// TODO(hjiang):
    /// 1. Record table events.
    /// 2. It involves IO operations, should be placed at background thread.s
    pub(crate) async fn batch_ingest(&mut self, parquet_files: Vec<String>, lsn: u64) {
        const MAX_IN_FLIGHT: usize = 64;
        let start_id = self.next_file_id;
        self.next_file_id += parquet_files.len() as u32;

        let disk_files = stream::iter(parquet_files.into_iter().enumerate().map(
            |(idx, cur_file)| {
                let cur_file_id = (start_id as u64) + idx as u64;
                async move {
                    // TODO(hjiang): Handle remote data file ingestion as well.
                    let file = File::open(&cur_file)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to open {cur_file}"));
                    let file_size = file
                        .metadata()
                        .await
                        .unwrap_or_else(|_| panic!("Failed to stat {cur_file}"))
                        .len() as usize;
                    let stream_builder = ParquetRecordBatchStreamBuilder::new(file)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to read parquet footer for {cur_file}"));
                    let num_rows = stream_builder.metadata().file_metadata().num_rows() as usize;

                    let mooncake_data_file = create_data_file(cur_file_id, cur_file.clone());
                    let disk_file_entry = DiskFileEntry {
                        cache_handle: None,
                        num_rows,
                        file_size,
                        batch_deletion_vector: BatchDeletionVector::new(num_rows),
                        puffin_deletion_blob: None,
                    };

                    (mooncake_data_file, disk_file_entry)
                }
            },
        ))
        .buffer_unordered(MAX_IN_FLIGHT) // run up to N at once
        .collect::<hashbrown::HashMap<_, _>>()
        .await;

        // Commit the current crafted streaming transaction.
        let mut commit = TransactionStreamCommit::from_disk_files(disk_files, lsn);

        // Build file index if needed (skip for append-only tables)
        if !matches!(self.metadata.identity, IdentityProp::None) {
            // Clone owned inputs needed for async build without capturing &self.
            let files = commit.get_flushed_data_files();
            let identity = self.metadata.identity.clone();
            let table_dir: PathBuf = self.metadata.path.clone();
            let index_file_id = self.next_file_id as u64;

            if let Ok(file_index) =
                Self::build_index_for_files(files, identity, table_dir, index_file_id).await
            {
                commit.add_file_index(file_index);
            } else {
                tracing::error!(
                    "failed to build file index for batch_ingest; proceeding without index"
                );
            }
        }

        self.next_snapshot_task
            .new_streaming_xact
            .push(TransactionStreamOutput::Commit(commit));
        self.next_snapshot_task.new_flush_lsn = Some(lsn);
        self.next_snapshot_task.commit_lsn_baseline = lsn;
    }

    /// Build a single GlobalIndex spanning `files` by scanning Parquet with identity projection.
    async fn build_index_for_files(
        files: Vec<MooncakeDataFileRef>,
        identity: IdentityProp,
        table_dir: PathBuf,
        index_file_id: u64,
    ) -> Result<FileIndex> {
        // Accumulate (hash, seg_idx, row_idx)
        let mut entries: Vec<(u64, usize, usize)> = Vec::new();

        for (seg_idx, data_file) in files.iter().enumerate() {
            let file = tokio::fs::File::open(data_file.file_path()).await?;
            let mut stream_builder = ParquetRecordBatchStreamBuilder::new(file).await?;
            let schema_descr = stream_builder.metadata().file_metadata().schema_descr();
            let indices = identity.get_key_indices(schema_descr.num_columns());
            let mask = ProjectionMask::roots(schema_descr, indices);
            stream_builder = stream_builder.with_projection(mask);

            let mut reader = stream_builder.build()?;
            let mut row_idx_within_file: usize = 0;
            while let Some(row_group_reader) = reader.next_row_group().await? {
                let mut batch_stream = row_group_reader;
                while let Some(batch) = batch_stream.next().transpose()? {
                    let rows = MoonlinkRow::from_record_batch(&batch);
                    for row in rows {
                        let hash = identity.get_lookup_key(&row);
                        entries.push((hash, seg_idx, row_idx_within_file));
                        row_idx_within_file += 1;
                    }
                }
            }
        }

        // Build index blocks and attach data files
        let mut builder = GlobalIndexBuilder::new();
        builder.set_directory(table_dir);
        builder.set_files(files);
        let index = builder.build_from_flush(entries, index_file_id).await?;
        Ok(index)
    }
}
