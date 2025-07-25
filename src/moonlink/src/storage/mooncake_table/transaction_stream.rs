use super::*;
use crate::storage::cache::object_storage::base_cache::{CacheEntry, CacheTrait, FileMetadata};
use crate::storage::index::cache_utils as index_cache_utils;
use crate::storage::mooncake_table::DiskFileEntry;
use crate::storage::storage_utils::{ProcessedDeletionRecord, TableUniqueFileId};
use crate::Error;
use fastbloom::BloomFilter;
/// Used to track the state of a streamed transaction
/// Holds appending rows in memslice and files.
/// Deletes are more complex,
/// 1. row belong to stream state memslice, directly delete it.
/// 2. row belong to stream state flushed file, add to `local_deletions`
/// 3. row belong to main table's flushed files, directly pushed to snapshot_task.new_deletions and let snapshot handle it.
/// 4. row belong to main table's memslice, add to `pending_deletions_in_main_mem_slice`, and handle at commit time`
///
pub(crate) struct TransactionStreamState {
    pub(crate) mem_slice: MemSlice,
    pub(crate) local_deletions: Vec<ProcessedDeletionRecord>,
    pub(crate) pending_deletions_in_main_mem_slice: Vec<RawDeletionRecord>,
    index_bloom_filter: BloomFilter,
    pub(crate) flushed_file_index: MooncakeIndex,
    pub(crate) flushed_files: hashbrown::HashMap<MooncakeDataFileRef, DiskFileEntry>,
    // This is not really used as a snapshot task, just used to hold state during flush.
    pub(crate) next_snapshot_task: SnapshotTask,
}

pub enum TransactionStreamOutput {
    Commit(TransactionStreamCommit),
    Abort(u32),
}

pub struct TransactionStreamCommit {
    pub(crate) xact_id: u32,
    pub(crate) commit_lsn: u64,
    pub(crate) flushed_file_index: MooncakeIndex,
    pub(crate) flushed_files: hashbrown::HashMap<MooncakeDataFileRef, DiskFileEntry>,
    pub(crate) local_deletions: Vec<ProcessedDeletionRecord>,
    pub(crate) pending_deletions: Vec<RawDeletionRecord>,
}

impl TransactionStreamCommit {
    /// Get flushed data files for the current streaming commit.
    pub(crate) fn get_flushed_data_files(&self) -> Vec<MooncakeDataFileRef> {
        self.flushed_files.keys().cloned().collect::<Vec<_>>()
    }
    /// Get flushed file indices for the current streaming commit.
    pub(crate) fn get_file_indices(&self) -> Vec<FileIndex> {
        self.flushed_file_index.file_indices.clone()
    }
    /// Import file index into cache.
    /// Return evicted files to delete.
    pub(crate) async fn import_file_index_into_cache(
        &mut self,
        object_storage_cache: ObjectStorageCache,
        table_id: TableId,
    ) -> Vec<String> {
        let file_indices = &mut self.flushed_file_index.file_indices;
        index_cache_utils::import_file_indices_to_cache(
            file_indices,
            object_storage_cache,
            table_id,
        )
        .await
    }
}

impl TransactionStreamState {
    fn new(
        schema: Arc<Schema>,
        batch_size: usize,
        identity: IdentityProp,
        streaming_counter: Arc<BatchIdCounter>,
    ) -> Self {
        Self {
            mem_slice: MemSlice::new(schema, batch_size, identity, streaming_counter),
            local_deletions: Vec::new(),
            pending_deletions_in_main_mem_slice: Vec::new(),
            index_bloom_filter: BloomFilter::with_num_bits(1 << 24).expected_items(1_000_000),
            flushed_file_index: MooncakeIndex::new(),
            flushed_files: hashbrown::HashMap::new(),
            next_snapshot_task: SnapshotTask::new(MooncakeTableConfig::default()),
        }
    }

    pub(crate) fn insert_flushed_file(&mut self, file: MooncakeDataFileRef, entry: DiskFileEntry) {
        self.flushed_files.insert(file, entry);
    }

    pub(crate) fn insert_flushed_file_index(&mut self, index: FileIndex) {
        self.flushed_file_index.insert_file_index(index);
    }
}

pub(crate) const LSN_START_FOR_STREAMING_XACT: u64 = 0xFFFF_FFFF_0000_0000;
// DevNote:
// This is a trick to track xact of uncommitted deletions
// we set first 32 bits to 1, so it will be 'uncommitted' as the value is larger than any possible lsn.
// And we use the last 32 bits to store the xact_id, so we can find deletion for a given xact_id.
fn get_lsn_for_pending_xact(xact_id: u32) -> u64 {
    LSN_START_FOR_STREAMING_XACT | xact_id as u64
}

impl MooncakeTable {
    fn get_or_create_stream_state(&mut self, xact_id: u32) -> &mut TransactionStreamState {
        let metadata = self.metadata.clone();

        self.transaction_stream_states
            .entry(xact_id)
            .or_insert_with(|| {
                TransactionStreamState::new(
                    metadata.schema.clone(),
                    metadata.config.batch_size,
                    metadata.identity.clone(),
                    Arc::clone(&self.streaming_batch_id_counter),
                )
            })
    }

    pub(crate) fn get_transaction_stream_state(
        &mut self,
        xact_id: u32,
    ) -> Result<&mut TransactionStreamState> {
        self.transaction_stream_states
            .get_mut(&xact_id)
            .ok_or(Error::TransactionNotFound(xact_id))
    }

    pub(crate) fn remove_transaction_stream_state(
        &mut self,
        xact_id: u32,
    ) -> TransactionStreamState {
        self.transaction_stream_states.remove(&xact_id).unwrap()
    }

    pub fn should_transaction_flush(&self, xact_id: u32) -> bool {
        self.transaction_stream_states
            .get(&xact_id)
            .unwrap()
            .mem_slice
            .get_num_rows()
            >= self.metadata.config.mem_slice_size
    }

    pub fn append_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) -> Result<()> {
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let identity_for_key = self.metadata.identity.extract_identity_for_key(&row);

        let stream_state = self.get_or_create_stream_state(xact_id);
        if let Some(batch) = stream_state
            .mem_slice
            .append(lookup_key, row, identity_for_key)?
        {
            stream_state
                .next_snapshot_task
                .new_record_batches
                .push(batch);
        }
        stream_state.index_bloom_filter.insert(&lookup_key);
        Ok(())
    }

    pub async fn delete_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) {
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let metadata_identity = self.metadata.identity.clone();
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn: get_lsn_for_pending_xact(xact_id), // at commit time we will update this with the actual lsn
            pos: None,
            row_identity: metadata_identity.extract_identity_columns(row),
        };

        let stream_state = self.get_or_create_stream_state(xact_id);

        // it is very unlikely to delete a row in current transaction,
        // only very weird query shape could do it.
        // use a bloom filter to skip any index lookup (which could be costly)
        let bloom_filter_pass = stream_state.index_bloom_filter.contains(&lookup_key);
        // skip any index lookup if bloom filter don't pass
        if bloom_filter_pass {
            // Delete from stream mem slice
            if stream_state
                .mem_slice
                .delete(&record, &metadata_identity)
                .await
                .is_some()
            {
                return;
            }
            // Delete from stream flushed files
            let matches = stream_state.flushed_file_index.find_record(&record).await;
            if !matches.is_empty() {
                for loc in matches {
                    let RecordLocation::DiskFile(file_id, row_id) = loc else {
                        panic!("Unexpected record location: {record:?}");
                    };
                    let (file, disk_file_entry) = stream_state
                        .flushed_files
                        .get_key_value_mut(&file_id)
                        .expect("missing disk file");
                    if disk_file_entry.batch_deletion_vector.is_deleted(row_id) {
                        continue;
                    }
                    if record.row_identity.is_none()
                        || record
                            .row_identity
                            .as_ref()
                            .unwrap()
                            .equals_parquet_at_offset(file.file_path(), row_id, &metadata_identity)
                            .await
                    {
                        stream_state.local_deletions.push(ProcessedDeletionRecord {
                            pos: loc,
                            lsn: record.lsn,
                        });
                        disk_file_entry.batch_deletion_vector.delete_row(row_id);
                        return;
                    }
                }
            }
        }

        // Scope the main table deletion lookup
        record.pos = {
            self.mem_slice
                .find_non_deleted_position(&record, &metadata_identity)
                .await
        };

        let stream_state = self.get_or_create_stream_state(xact_id);
        if record.pos.is_some() {
            stream_state
                .pending_deletions_in_main_mem_slice
                .push(record);
        // Delete from stream state snapshot task.
        // TODO: Search the stream state snapshot task
        // record.pos =
        } else {
            self.next_snapshot_task.new_deletions.push(record);
        }
    }

    pub fn abort_in_stream_batch(&mut self, xact_id: u32) {
        // Record abortion in snapshot task so we can remove any uncomitted deletions
        self.transaction_stream_states.remove(&xact_id);
        self.next_snapshot_task
            .new_streaming_xact
            .push(TransactionStreamOutput::Abort(xact_id));
    }

    /// Commit a transaction stream commit.
    pub fn commit_transaction_stream(&mut self, xact_id: u32, lsn: u64) -> Result<()> {
        let stream_state = self.get_transaction_stream_state(xact_id)?;
        let commit_point = stream_state.mem_slice.get_commit_check_point();
        // let latest_rows = stream_state.mem_slice.get_latest_rows();

        let mut batches = stream_state
            .next_snapshot_task
            .new_record_batches
            .drain(..)
            .collect::<Vec<_>>();
        // Finalize latest rows in a new batch
        if let Some(latest_batch) = stream_state
            .mem_slice
            .column_store
            .finalize_current_batch()?
        {
            batches.push(latest_batch);
        }

        let indices = stream_state
            .next_snapshot_task
            .new_mem_indices
            .drain(..)
            .collect::<Vec<_>>();

        // Extract disk slices from streaming state before moving other data
        let disk_slices = stream_state
            .next_snapshot_task
            .new_disk_slices
            .drain(..)
            .collect::<Vec<_>>();

        // We update our delete records with the last lsn of the transaction
        // Note that in the stream case we dont have this until commit time
        for deletion in stream_state.pending_deletions_in_main_mem_slice.iter_mut() {
            let pos = deletion.pos.unwrap();
            // If the row is no longer in memslice, it must be flushed, let snapshot task find it.
            if !stream_state.mem_slice.try_delete_at_pos(pos) {
                deletion.pos = None;
            }
        }
        for deletion in stream_state.local_deletions.iter_mut() {
            deletion.lsn = lsn - 1;
        }
        // TODO: Do we need to update the lsn of the records that were added to the main snapshot task? I dont think this was done in the original imol so probs not but double check this.

        // We move the indices and batches out of the stream state and into the tables next snapshot task.
        // This makes them discoverable by the next mooncake snapshot task even while the flush is in progress.
        self.next_snapshot_task.new_mem_indices.extend(indices);
        self.next_snapshot_task.new_record_batches.extend(batches);

        // Also move the row buffer from streaming state so that reads work during background flush
        // self.next_snapshot_task.new_rows = Some(latest_rows);

        self.next_snapshot_task.new_commit_lsn = lsn;
        self.next_snapshot_task.new_commit_point = Some(commit_point);
        self.flush(Some(lsn), Some(xact_id))?;

        Ok(())
    }
}

impl SnapshotTableState {
    /// Return files evicted from object storage cache.
    pub(super) async fn apply_transaction_stream(
        &mut self,
        task: &mut SnapshotTask,
    ) -> Vec<String> {
        // Aggregate evicted data cache files to delete.
        let mut evicted_files = vec![];

        let new_streaming_xact = task.new_streaming_xact.drain(..);
        for output in new_streaming_xact {
            match output {
                TransactionStreamOutput::Commit(commit) => {
                    // Integrate files into current snapshot and import into object storage cache.
                    for (file, mut disk_file_entry) in commit.flushed_files.into_iter() {
                        task.disk_file_lsn_map
                            .insert(file.file_id(), commit.commit_lsn);

                        // Import data files into cache.
                        let file_id = TableUniqueFileId {
                            table_id: TableId(self.mooncake_table_metadata.table_id),
                            file_id: file.file_id(),
                        };
                        let (cache_handle, cur_evicted_files) = self
                            .object_storage_cache
                            .import_cache_entry(
                                file_id,
                                CacheEntry {
                                    cache_filepath: file.file_path().clone(),
                                    file_metadata: FileMetadata {
                                        file_size: disk_file_entry.file_size as u64,
                                    },
                                },
                            )
                            .await;
                        disk_file_entry.cache_handle = Some(cache_handle);
                        evicted_files.extend(cur_evicted_files);
                        self.current_snapshot
                            .disk_files
                            .insert(file, disk_file_entry);
                    }

                    // add index
                    commit
                        .flushed_file_index
                        .file_indices
                        .into_iter()
                        .for_each(|file_index| {
                            self.current_snapshot.indices.insert_file_index(file_index);
                        });
                    // add local deletions
                    self.committed_deletion_log
                        .extend(commit.local_deletions.into_iter());
                    // add pending deletions
                    task.new_deletions
                        .extend(commit.pending_deletions.into_iter());
                    // set lsn for pending deletions
                    self.uncommitted_deletion_log.iter_mut().for_each(|row| {
                        if let Some(deletion) = row {
                            if deletion.lsn == get_lsn_for_pending_xact(commit.xact_id) {
                                deletion.lsn = commit.commit_lsn - 1;
                            }
                        }
                    });
                    task.new_deletions.iter_mut().for_each(|deletion| {
                        if deletion.lsn == get_lsn_for_pending_xact(commit.xact_id) {
                            deletion.lsn = commit.commit_lsn - 1;
                        }
                    });
                }
                TransactionStreamOutput::Abort(xact_id) => {
                    for row in self.uncommitted_deletion_log.iter_mut() {
                        if let Some(deletion) = row {
                            if deletion.lsn == get_lsn_for_pending_xact(xact_id) {
                                *row = None;
                            }
                        }
                    }
                    task.new_deletions
                        .retain(|deletion| deletion.lsn != get_lsn_for_pending_xact(xact_id));
                }
            }
        }

        evicted_files
    }
}
