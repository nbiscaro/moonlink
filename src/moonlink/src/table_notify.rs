use crate::storage::mooncake_table::DataCompactionPayload;
use crate::storage::mooncake_table::DataCompactionResult;
use crate::storage::mooncake_table::FileIndiceMergePayload;
use crate::storage::mooncake_table::FileIndiceMergeResult;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::IcebergSnapshotResult;

use crate::NonEvictableHandle;
use crate::Result;

/// Completion notifications for mooncake table, including snapshot creation and compaction, etc.
///
/// TODO(hjiang): Revisit whether we need to place the payload into box.
#[allow(clippy::large_enum_variant)]
pub enum TableNotify {
    /// Mooncake snapshot completes.
    MooncakeTableSnapshot {
        /// Mooncake snapshot LSN.
        lsn: u64,
        /// Payload used to create an iceberg snapshot.
        iceberg_snapshot_payload: Option<IcebergSnapshotPayload>,
        /// Payload used to trigger a data compaction.
        data_compaction_payload: Option<DataCompactionPayload>,
        /// Payload used to trigger an index merge.
        file_indice_merge_payload: Option<FileIndiceMergePayload>,
        /// Evicted object storage cache to delete.
        evicted_data_files_to_delete: Vec<String>,
    },
    /// Iceberg snapshot completes.
    IcebergSnapshot {
        /// Result for iceberg snapshot.
        iceberg_snapshot_result: Result<IcebergSnapshotResult>,
    },
    /// Index merge completes.
    IndexMerge {
        /// Result for index merge.
        index_merge_result: FileIndiceMergeResult,
    },
    /// Data compaction completes.
    DataCompaction {
        /// Result for data compaction.
        data_compaction_result: Result<DataCompactionResult>,
    },
    /// Read request completion.
    ReadRequest {
        /// Cache handles, which are pinned before query.
        cache_handles: Vec<NonEvictableHandle>,
    },
    /// Evicted data files to delete.
    EvictedDataFilesToDelete {
        /// Evicted data files by object storage cache.
        evicted_data_files: Vec<String>,
    },
}
