use tokio::sync::mpsc::Sender;

use crate::row::MoonlinkRow;
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
/// Event types that can be processed by the TableHandler
#[derive(Debug)]
pub enum TableEvent {
    /// ==============================
    /// Replication events
    /// ==============================
    ///
    /// Append a row to the table
    Append {
        row: MoonlinkRow,
        xact_id: Option<u32>,
        is_copied: bool,
    },
    /// Delete a row from the table
    Delete {
        row: MoonlinkRow,
        lsn: u64,
        xact_id: Option<u32>,
    },
    /// Commit all pending operations with a given LSN and xact_id
    Commit { lsn: u64, xact_id: Option<u32> },
    /// Abort current stream with given xact_id
    StreamAbort { xact_id: u32 },
    /// Flush the table to disk
    Flush { lsn: u64 },
    /// Flush the transaction stream with given xact_id
    StreamFlush { xact_id: u32 },
    /// Shutdown the handler
    Shutdown,
    /// ==============================
    /// Interactive blocking events
    /// ==============================
    ///
    /// Force a mooncake and iceberg snapshot.
    ForceSnapshot { lsn: u64, tx: Sender<Result<()>> },
    /// Drop table.
    DropTable,
    /// ==============================
    /// Table internal events
    /// ==============================
    ///
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
    /// Start initial table copy.
    StartInitialCopy,
    /// Finish initial table copy and merge buffered changes.
    FinishInitialCopy,
}
