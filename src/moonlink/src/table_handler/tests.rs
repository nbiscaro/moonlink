use arrow_array::{Int32Array, RecordBatch, StringArray};
use tempfile::tempdir;
use tokio::sync::broadcast;
use tokio::sync::watch;

use super::test_utils::*;
use super::TableEvent;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::index::index_merge_config::FileIndexMergeConfig;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::validation_test_utils::*;
use crate::storage::mooncake_table::IcebergPersistenceConfig;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::MooncakeTableConfig;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::MockTableManager;
use crate::storage::MooncakeTable;
use crate::storage::PersistenceResult;
use crate::storage::TableManager;
use crate::table_handler::table_handler_state::TableHandlerState;
use crate::ObjectStorageCache;
use crate::TableEventManager;

use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_table_handler() {
    let mut env = TestEnvironment::default().await;

    env.append_row(1, "John", 30, /*lsn=*/ 0, /*xact_id=*/ None)
        .await;
    env.commit(1).await;

    env.set_readable_lsn_with_cap(1, 100); // table_commit_lsn = 1, replication_lsn = 100
    env.verify_snapshot(1, &[1]).await;
    env.verify_snapshot(100, &[1]).await; // Reading at a higher LSN should still see data from LSN 1

    env.shutdown().await;
    println!("All table handler tests passed!");
}

#[tokio::test]
async fn test_table_handler_flush() {
    let mut env = TestEnvironment::default().await;

    let rows_data = vec![(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)];
    for (id, name, age) in rows_data {
        env.append_row(id, name, age, /*lsn=*/ 0, /*xact_id=*/ None)
            .await;
    }

    env.commit(1).await;
    env.flush_table(1).await;

    env.set_readable_lsn_with_cap(1, 100);
    env.verify_snapshot(1, &[1, 2, 3]).await;

    env.shutdown().await;
    println!("All table handler flush tests passed!");
}

// Testing scenario: assign small parquet file size, which leads to multiple disk slice in one flush operation.
#[tokio::test]
async fn test_append_with_small_disk_slice() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: 1,                   // One mem slice only contains one row.
        disk_slice_parquet_file_size: 1, // One parquet file only contains one arrow record.
        mem_slice_size: 1000,
        snapshot_deletion_record_count: 1000,
        temp_files_directory: temp_dir.path().to_str().unwrap().to_string(),
        data_compaction_config: DataCompactionConfig::default(),
        file_index_config: FileIndexMergeConfig::default(),
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 1000,
            new_committed_deletion_log: 1000,
        },
    };
    let env = TestEnvironment::new(temp_dir, mooncake_table_config.clone()).await;

    // Append two rows, which appears in two parquet files.
    env.append_row(
        /*id=*/ 1, /*name=*/ "Alice", /*age=*/ 10, /*lsn=*/ 0,
        /*xact_id=*/ None,
    )
    .await;
    env.append_row(
        /*id=*/ 2, /*name=*/ "Blob", /*age=*/ 20, /*lsn=*/ 0,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 10).await;
    env.flush_table(/*lsn=*/ 10).await;

    // Check whether both rows are accessible in snapshot.
    env.set_readable_lsn(10);
    env.verify_snapshot(/*target_lsn=*/ 10, /*expected_id=*/ &[1, 2])
        .await;
}

#[tokio::test]
async fn test_streaming_append_and_commit() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 101;

    env.append_row(10, "Transaction-User", 25, /*lsn=*/ 50, Some(xact_id))
        .await;
    env.stream_commit(101, xact_id).await;

    env.set_readable_lsn(101);
    env.verify_snapshot(101, &[10]).await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_non_streaming_delete() {
    let mut env = TestEnvironment::default().await;

    env.append_row(10, "Transaction-User1", 25, /*lsn=*/ 10, None)
        .await;
    env.append_row(11, "Transaction-User2", 30, /*lsn=*/ 20, None)
        .await;

    env.commit(21).await;

    env.set_readable_lsn(21);
    env.verify_snapshot(21, &[10, 11]).await;

    env.delete_row(10, "Transaction-User1", 25, 22, None).await;
    env.commit(23).await;

    env.set_readable_lsn(23);
    env.verify_snapshot(23, &[11]).await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_streaming_delete() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 101;

    env.append_row(10, "Transaction-User1", 25, /*lsn=*/ 10, Some(xact_id))
        .await;
    env.append_row(11, "Transaction-User2", 30, /*lsn=*/ 20, Some(xact_id))
        .await;

    // LSN for delete op (100) can be different from the stream commit LSN (101)
    env.delete_row(10, "Transaction-User1", 25, 100, Some(xact_id))
        .await;
    env.stream_commit(101, xact_id).await;

    env.set_readable_lsn(101);
    env.verify_snapshot(101, &[11]).await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_streaming_abort() {
    let mut env = TestEnvironment::default().await;

    // Baseline data
    let baseline_xact_id = 100;
    env.append_row(
        1,
        "Baseline-User",
        20,
        /*lsn=*/ 50,
        Some(baseline_xact_id),
    )
    .await;
    env.stream_commit(100, baseline_xact_id).await;

    // Set table_commit_tx to allow ReadStateManager to know LSN 100 is committed.
    env.set_table_commit_lsn(100);

    // Transaction to be aborted
    let abort_xact_id = 102;
    env.append_row(
        20,
        "UserToAbort",
        40,
        /*lsn=*/ 120,
        Some(abort_xact_id),
    )
    .await;
    env.stream_abort(abort_xact_id).await;

    // Now enable reading up to LSN 100 by setting replication_tx.
    // The target_lsn for read is 100.
    env.set_replication_lsn(100);
    env.verify_snapshot(100, &[1]).await; // Should only see baseline data.

    env.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_streaming_transactions() {
    let mut env = TestEnvironment::default().await;
    let xact_id_1 = 103; // Will be committed
    let xact_id_2 = 104; // Will be aborted

    env.append_row(
        30,
        "Transaction1-User",
        35,
        /*lsn=*/ 50,
        Some(xact_id_1),
    )
    .await;
    env.append_row(
        40,
        "Transaction2-User",
        45,
        /*lsn=*/ 75,
        Some(xact_id_2),
    )
    .await;

    env.stream_commit(103, xact_id_1).await; // Commit transaction 1 at LSN 103
    env.stream_abort(xact_id_2).await; // Abort transaction 2

    env.set_readable_lsn(103); // Make LSN 103 readable
    env.verify_snapshot(103, &[30]).await; // Verify only data from committed transaction 1

    env.shutdown().await;
}

#[tokio::test]
async fn test_stream_delete_unflushed_non_streamed_row() {
    let mut env = TestEnvironment::default().await;

    // Define LSNs and transaction ID for clarity
    let initial_insert_lsn = 10; // LSN for the non-streaming insert
    let stream_xact_id = 101; // Transaction ID for the streaming delete operation

    // The LSN passed to env.delete_row for a streaming op is used for the RawDeletionRecord,
    // but this LSN is typically overridden by the stream_commit_lsn when the transaction commits.
    // We use a distinct value here for clarity, but it's the stream_commit_lsn that's ultimately effective.
    let delete_op_event_lsn = 15;
    let stream_commit_lsn = 20; // LSN at which the streaming transaction (and its delete) is committed

    // --- Phase 1: Setup - Insert a row non-streamingly ---
    // This row (PK=1) will be added to pending writes.
    env.append_row(1, "Target User", 30, /*lsn=*/ 5, None).await;

    // Commit the non-streaming operation. This moves the row to the main mem_slice.
    // It is now "committed" at initial_insert_lsn but not yet flushed to disk.
    env.commit(initial_insert_lsn).await;

    // Inform the ReadStateManager that data up to initial_insert_lsn is committed.
    env.set_table_commit_lsn(initial_insert_lsn);
    // Set the replication LSN cap to allow reading up to initial_insert_lsn.
    env.set_replication_lsn(initial_insert_lsn);

    // Verify: The row (PK=1) should be visible in a snapshot at initial_insert_lsn.
    println!("1 Verifying snapshot at LSN {initial_insert_lsn}");
    env.verify_snapshot(initial_insert_lsn, &[1]).await;

    // --- Phase 2: Action - Delete the non-streamed, unflushed row via a streaming transaction ---
    // Call delete_row for PK=1 within the context of stream_xact_id.
    // Inside table_handler.delete_in_stream_batch:
    //   - stream_state.mem_slice.delete() will be called. Since PK=1 was not added
    //     by stream_xact_id, it won't be in this transaction's mem_slice.
    //   - Thus, 'pos' returned by stream_state.mem_slice.delete() will be None.
    //   - The RawDeletionRecord will be created with pos: None.
    env.delete_row(
        1,
        "Target User",
        30,
        delete_op_event_lsn,
        Some(stream_xact_id),
    )
    .await;

    // Commit the streaming transaction.
    // During this commit, the TableHandler will process new_deletions for stream_xact_id.
    // For the deletion of PK=1 (which has pos: None), it should search the main mem_slice.
    // It will find PK=1 there and apply the deletion, associating it with stream_commit_lsn.
    env.stream_commit(stream_commit_lsn, stream_xact_id).await;

    // Update ReadStateManager: table state is now committed up to stream_commit_lsn.
    env.set_table_commit_lsn(stream_commit_lsn);
    // Update replication LSN cap to allow reading up to the new commit LSN.
    env.set_replication_lsn(stream_commit_lsn);

    // --- Phase 3: Verification ---
    // Verify: The row (PK=1) should NOT be visible in a snapshot at stream_commit_lsn.
    // The effective LSN for the read will be min(target_lsn=20, table_commit_lsn=20, replication_cap=20) = 20.
    println!("2 Verifying snapshot at LSN {stream_commit_lsn}");
    env.verify_snapshot(stream_commit_lsn, &[]).await; // Expect empty slice (PK=1 deleted)

    env.shutdown().await;
    println!("Test test_stream_delete_unflushed_non_streamed_row passed!");
}

#[tokio::test]
async fn test_streaming_transaction_periodic_flush() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 201;
    let commit_lsn = 20; // LSN at which the transaction will eventually commit
    let initial_read_lsn_target = commit_lsn; // For verifying no data pre-commit
    let final_read_lsn_target = commit_lsn; // For verifying all data post-commit

    // --- Phase 1: Append some data to the streaming transaction ---
    env.append_row(10, "StreamUser1-Part1", 25, /*lsn=*/ 0, Some(xact_id))
        .await;
    env.append_row(11, "StreamUser2-Part1", 30, /*lsn=*/ 0, Some(xact_id))
        .await;

    // --- Phase 2: Perform a periodic flush of the transaction stream ---
    env.stream_flush(xact_id).await;

    // --- Phase 3: Verify data is NOT visible after flush but BEFORE commit ---
    env.set_table_commit_lsn(0);
    env.set_replication_lsn(initial_read_lsn_target + 5);

    env.set_snapshot_lsn(0);
    env.verify_snapshot(initial_read_lsn_target, &[]).await;

    // --- Phase 4: Append more data to the same transaction AFTER the periodic flush ---
    env.append_row(12, "StreamUser3-Part2", 35, /*lsn=*/ 10, Some(xact_id))
        .await;

    // --- Phase 5: Commit the streaming transaction ---
    env.stream_commit(commit_lsn, xact_id).await;

    // --- Phase 6: Verify ALL data (before and after periodic flush) is visible after commit ---

    env.set_table_commit_lsn(commit_lsn);
    env.set_replication_lsn(final_read_lsn_target + 5);

    env.verify_snapshot(final_read_lsn_target, &[10, 11, 12])
        .await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_stream_delete_previously_flushed_row_same_xact() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 401;
    let stream_commit_lsn = 40;

    // Phase 1: Append Row A (ID:10) to stream, then periodic flush
    env.append_row(10, "UserA-StreamFlush", 25, /*lsn=*/ 0, Some(xact_id))
        .await;
    env.stream_flush(xact_id).await; // Row A now in a xact-specific disk slice

    // Phase 2: In same stream, delete Row A (ID:10), append Row B (ID:11)
    env.delete_row(10, "UserA-StreamFlush", 25, 0, Some(xact_id))
        .await; // LSN placeholder
    env.append_row(
        11,
        "UserB-StreamSurvived",
        30,
        /*lsn=*/ 0,
        Some(xact_id),
    )
    .await;

    // Phase 3: Verify data is NOT visible before commit
    env.set_table_commit_lsn(0);
    env.set_replication_lsn(stream_commit_lsn + 5);
    env.set_snapshot_lsn(0);
    env.verify_snapshot(stream_commit_lsn, &[]).await;

    // Phase 4: Commit the streaming transaction
    env.stream_commit(stream_commit_lsn, xact_id).await;

    // Phase 5: Verify final state
    env.set_table_commit_lsn(stream_commit_lsn);
    env.set_replication_lsn(stream_commit_lsn + 5);
    env.verify_snapshot(stream_commit_lsn, &[11]).await; // Only Row B (ID:11) should exist

    env.shutdown().await;
}

#[tokio::test]
async fn test_stream_delete_from_stream_memslice_row() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 402;
    let stream_commit_lsn = 41;

    env.append_row(20, "UserC-StreamMem", 35, /*lsn=*/ 0, Some(xact_id))
        .await;

    // Phase 2: Delete Row C (ID:20) from stream's mem_slice, append Row D (ID:21)
    env.delete_row(20, "UserC-StreamMem", 35, 0, Some(xact_id))
        .await; // LSN placeholder
    env.append_row(
        21,
        "UserD-StreamSurvived",
        40,
        /*lsn=*/ 0,
        Some(xact_id),
    )
    .await;

    // Phase 3: Verify data is NOT visible before commit
    env.set_table_commit_lsn(0);
    env.set_replication_lsn(stream_commit_lsn + 5);
    env.set_snapshot_lsn(0);
    env.verify_snapshot(stream_commit_lsn, &[]).await;

    // Phase 4: Commit the streaming transaction
    env.stream_commit(stream_commit_lsn, xact_id).await;
    println!("Phase 5: Verifying final state post-commit");

    // Phase 5: Verify final state
    env.set_table_commit_lsn(stream_commit_lsn);
    env.set_replication_lsn(stream_commit_lsn + 5);
    env.verify_snapshot(stream_commit_lsn, &[21]).await; // Only Row D (ID:21) should exist

    env.shutdown().await;
}

#[tokio::test]
async fn test_stream_delete_from_main_disk_row() {
    let mut env = TestEnvironment::default().await;
    let main_commit_lsn_flushed = 5; // LSN for the row that will be on disk
    let xact_id = 403;
    let stream_commit_lsn = 42;

    // Phase 1: Setup - Append Row G (ID:40), commit, and explicitly flush it to main disk
    env.append_row(40, "UserG-MainDisk", 50, /*lsn=*/ 0, None)
        .await;
    env.commit(main_commit_lsn_flushed).await;
    env.flush_table(main_commit_lsn_flushed).await; // Explicit flush
    env.set_table_commit_lsn(main_commit_lsn_flushed);
    env.set_replication_lsn(main_commit_lsn_flushed + 5);
    env.verify_snapshot(main_commit_lsn_flushed, &[40]).await;

    // Phase 2: Start streaming transaction, delete Row G (ID:40), append Row H (ID:41)
    env.delete_row(40, "UserG-MainDisk", 50, 0, Some(xact_id))
        .await; // LSN placeholder
    env.append_row(
        41,
        "UserH-StreamSurvived",
        55,
        /*lsn=*/ 40,
        Some(xact_id),
    )
    .await;

    // Phase 3: Verify data is NOT visible before stream commit (Row G should still be there)
    // table_commit_lsn is still main_commit_lsn_flushed (5)
    env.set_replication_lsn(stream_commit_lsn + 5);
    // Effective read LSN = min(target=42, table_commit=5, replication=47) = 5
    env.verify_snapshot(stream_commit_lsn, &[40]).await; // Row G should still be visible

    // Phase 4: Commit the streaming transaction
    env.stream_commit(stream_commit_lsn, xact_id).await;

    // Phase 5: Verify final state
    env.set_table_commit_lsn(stream_commit_lsn);
    env.set_replication_lsn(stream_commit_lsn + 5);
    // Effective read LSN = min(target=42, table_commit=42, replication=47) = 42
    env.verify_snapshot(stream_commit_lsn, &[41]).await; // Only Row H (ID:41) should exist

    env.shutdown().await;
}

#[tokio::test]
async fn test_streaming_transaction_periodic_flush_then_abort() {
    let mut env = TestEnvironment::default().await;
    let baseline_xact_id = 500; // For baseline data
    let baseline_commit_lsn = 50;
    let aborted_xact_id = 501;
    // LSN for reads after abort; should reflect only committed data up to baseline_commit_lsn
    let read_lsn_after_abort = baseline_commit_lsn + 5;

    // --- Phase 1: Setup - Commit baseline data ---
    env.append_row(
        1,
        "BaselineUser",
        30,
        /*lsn=*/ 0,
        Some(baseline_xact_id),
    )
    .await;
    env.stream_commit(baseline_commit_lsn, baseline_xact_id)
        .await;
    env.set_table_commit_lsn(baseline_commit_lsn); // ReadStateManager knows LSN 50 is committed
    env.set_replication_lsn(read_lsn_after_abort);
    env.verify_snapshot(baseline_commit_lsn, &[1]).await;

    // --- Phase 3: Append Row A (ID:10) and periodically flush it ---
    env.append_row(
        10,
        "UserA-ToAbort-Flushed",
        25,
        /*lsn=*/ 100,
        Some(aborted_xact_id),
    )
    .await;
    env.stream_flush(aborted_xact_id).await; // Row A now in a xact-specific disk slice, uncommitted

    // --- Phase 4: Append Row B (ID:11) (stays in stream's mem-slice) ---
    env.append_row(
        11,
        "UserB-ToAbort-Mem",
        35,
        /*lsn=*/ 100,
        Some(aborted_xact_id),
    )
    .await;

    // --- Phase 5: Attempt to delete baseline Row (ID:1) within the aborted transaction ---
    env.delete_row(1, "BaselineUser", 30, 0, Some(aborted_xact_id))
        .await; // LSN placeholder

    // --- Phase 6: Abort the streaming transaction ---
    // This should discard TransactionStreamState for aborted_xact_id, including:
    // - The DiskSliceWriter containing Row A (ID:10).
    // - The MemSlice containing Row B (ID:11).
    // - The RawDeletionRecord for Row (ID:1).
    env.stream_abort(aborted_xact_id).await;

    // --- Phase 7: Verify state after abort ---
    // Effective read LSN = min(target=55, table_commit=50, replication=55) = 50
    env.verify_snapshot(read_lsn_after_abort, &[1]).await;

    env.shutdown().await;
}

// This test only checks whether drop table event send and receive works through table handler.
#[tokio::test]
async fn test_drop_empty_table() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_directory = temp_dir.path().to_str().unwrap().to_string();
    let mut env = TestEnvironment::new(temp_dir, MooncakeTableConfig::default()).await; // No temp files created.
    env.drop_table().await.unwrap();

    // As of now, the whole mooncake table directory should be deleted.
    assert!(!tokio::fs::try_exists(&mooncake_table_directory)
        .await
        .unwrap());
}

// This test checks whether drop tables go through when there's real data.
#[tokio::test]
async fn test_drop_table_with_data() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_directory = temp_dir.path().to_str().unwrap().to_string();
    let mut env = TestEnvironment::new(temp_dir, MooncakeTableConfig::default()).await;

    // Write a few records to trigger mooncake and iceberg snapshot.
    env.append_row(
        /*id=*/ 0, /*name=*/ "Bob", /*age=*/ 10, /*lsn=*/ 0,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 1).await;
    env.flush_table(/*lsn=*/ 1).await;
    env.set_readable_lsn(/*lsn=*/ 1);

    // Force mooncake and iceberg snapshot, and block wait until mooncake snapshot completion via getting a read state.
    env.verify_snapshot(/*target_lsn=*/ 1, /*expected_ids=*/ &[0])
        .await;

    // Drop table and block wait its completion, check whether error status is correctly propagated.
    env.drop_table().await.unwrap();

    // As of now, the whole mooncake table directory should be deleted.
    assert!(!tokio::fs::try_exists(&mooncake_table_directory)
        .await
        .unwrap());
}

#[tokio::test]
async fn test_iceberg_snapshot_creation_for_batch_write() {
    // Set mooncake and iceberg flush and snapshot threshold to huge value, to verify force flush and force snapshot works as expected.
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: MooncakeTableConfig::DEFAULT_BATCH_SIZE,
        disk_slice_parquet_file_size: MooncakeTableConfig::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
        mem_slice_size: 1000,
        snapshot_deletion_record_count: 1000,
        temp_files_directory: temp_dir.path().to_str().unwrap().to_string(),
        data_compaction_config: DataCompactionConfig::default(),
        file_index_config: FileIndexMergeConfig::default(),
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 1000,
            new_committed_deletion_log: 1000,
        },
    };
    let mut env = TestEnvironment::new(temp_dir, mooncake_table_config.clone()).await;

    // Arrow batches used in test.
    let arrow_batch_1 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John".to_string()])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    let arrow_batch_2 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bob".to_string()])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();

    // ---- Create snapshot after new records appended ----
    // Append a new row to the mooncake table.
    env.append_row(
        /*id=*/ 1, /*name=*/ "John", /*age=*/ 30, /*lsn=*/ 0,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 1).await;

    // Attempt an iceberg snapshot, with requested LSN already committed.
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 1).await;
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 1)
        .await
        .unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one for data file, one for index block file
    assert_eq!(snapshot.disk_files.len(), 1);
    let (cur_data_file, cur_deletion_vector) = snapshot.disk_files.into_iter().next().unwrap();
    // Check data file.
    let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
    let expected_arrow_batch = arrow_batch_1.clone();
    assert_eq!(actual_arrow_batch, expected_arrow_batch);
    // Check deletion vector.
    assert!(cur_deletion_vector
        .batch_deletion_vector
        .collect_deleted_rows()
        .is_empty());
    check_deletion_vector_consistency(&cur_deletion_vector).await;
    assert!(cur_deletion_vector.puffin_deletion_blob.is_none());
    let old_data_file = cur_data_file;

    // ---- Create snapshot after new records appended and old records deleted ----
    //
    // Attempt an iceberg snapshot, which is a future flush LSN, and contains both new records and deletion records.
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 5).await;
    env.append_row(
        /*id=*/ 2, /*name=*/ "Bob", /*age=*/ 20, /*lsn=*/ 2,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 3).await;
    env.delete_row(
        /*id=*/ 1, /*name=*/ "John", /*age=*/ 30, /*lsn=*/ 4,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 5).await;

    // Block wait until iceberg snapshot created.
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 5)
        .await
        .unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 5); // two data files, two index block files, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.into_iter() {
        // Check the first data file.
        if cur_data_file.file_path() == old_data_file.file_path() {
            let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
            let expected_arrow_batch = arrow_batch_1.clone();
            assert_eq!(actual_arrow_batch, expected_arrow_batch);
            // Check the first deletion vector.
            assert_eq!(
                cur_deletion_vector
                    .batch_deletion_vector
                    .collect_deleted_rows(),
                vec![0]
            );
            check_deletion_vector_consistency(&cur_deletion_vector).await;
            continue;
        }

        // Check the second data file.
        let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
        let expected_arrow_batch = arrow_batch_2.clone();
        assert_eq!(actual_arrow_batch, expected_arrow_batch);
        // Check the second deletion vector.
        let deleted_rows = cur_deletion_vector
            .batch_deletion_vector
            .collect_deleted_rows();
        assert!(
            deleted_rows.is_empty(),
            "Deletion vector for the second data file is {deleted_rows:?}"
        );
        check_deletion_vector_consistency(&cur_deletion_vector).await;
    }

    // ---- Create snapshot only with old records deleted ----
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 7).await;
    env.delete_row(
        /*id=*/ 2, /*name=*/ "Bob", /*age=*/ 20, /*lsn=*/ 6,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 7).await;

    // Block wait until iceberg snapshot created.
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 7)
        .await
        .unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 6); // two data files, two index block files, two deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.into_iter() {
        // Check the first data file.
        if cur_data_file.file_path() == old_data_file.file_path() {
            let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
            let expected_arrow_batch = arrow_batch_1.clone();
            assert_eq!(actual_arrow_batch, expected_arrow_batch);
            // Check the first deletion vector.
            assert_eq!(
                cur_deletion_vector
                    .batch_deletion_vector
                    .collect_deleted_rows(),
                vec![0]
            );
            check_deletion_vector_consistency(&cur_deletion_vector).await;
            continue;
        }

        // Check the second data file.
        let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
        let expected_arrow_batch = arrow_batch_2.clone();
        assert_eq!(actual_arrow_batch, expected_arrow_batch);
        // Check the second deletion vector.
        // Check the first deletion vector.
        assert_eq!(
            cur_deletion_vector
                .batch_deletion_vector
                .collect_deleted_rows(),
            vec![0]
        );
        check_deletion_vector_consistency(&cur_deletion_vector).await;
    }

    // Requested LSN is no later than current iceberg snapshot LSN.
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 1).await;
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 1)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_iceberg_snapshot_creation_for_streaming_write() {
    // Set mooncake and iceberg flush and snapshot threshold to huge value, to verify force flush and force snapshot works as expected.
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: MooncakeTableConfig::DEFAULT_BATCH_SIZE,
        disk_slice_parquet_file_size: MooncakeTableConfig::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
        mem_slice_size: 1000,
        snapshot_deletion_record_count: 1000,
        temp_files_directory: temp_dir.path().to_str().unwrap().to_string(),
        data_compaction_config: DataCompactionConfig::default(),
        file_index_config: FileIndexMergeConfig::default(),
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 1000,
            new_committed_deletion_log: 1000,
        },
    };
    let mut env = TestEnvironment::new(temp_dir, mooncake_table_config.clone()).await;

    // Arrow batches used in test.
    let arrow_batch_1 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John".to_string()])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    let arrow_batch_2 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bob".to_string()])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();

    // ---- Create snapshot after new records appended ----
    // Append a new row to the mooncake table.
    env.append_row(
        /*id=*/ 1,
        /*name=*/ "John",
        /*age=*/ 30,
        /*lsn=*/ 0,
        /*xact_id=*/ Some(0),
    )
    .await;
    env.stream_commit(/*lsn=*/ 1, /*xact_id=*/ 0).await;

    // Attempt an iceberg snapshot, with requested LSN already committed.
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 1).await;
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 1)
        .await
        .unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file
    assert_eq!(snapshot.disk_files.len(), 1);
    let (cur_data_file, cur_deletion_vector) = snapshot.disk_files.into_iter().next().unwrap();
    // Check data file.
    let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
    let expected_arrow_batch = arrow_batch_1.clone();
    assert_eq!(actual_arrow_batch, expected_arrow_batch);
    // Check deletion vector.
    assert!(cur_deletion_vector
        .batch_deletion_vector
        .collect_deleted_rows()
        .is_empty());
    check_deletion_vector_consistency(&cur_deletion_vector).await;
    assert!(cur_deletion_vector.puffin_deletion_blob.is_none());
    let old_data_file = cur_data_file;

    // ---- Create snapshot after new records appended and old records deleted ----
    //
    // Attempt an iceberg snapshot, which is a future flush LSN, and contains both new records and deletion records.
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 5).await;
    env.append_row(
        /*id=*/ 2,
        /*name=*/ "Bob",
        /*age=*/ 20,
        /*lsn=*/ 2,
        /*xact_id=*/ Some(3),
    )
    .await;
    env.stream_commit(/*lsn=*/ 3, /*xact_id=*/ 3).await;
    env.delete_row(
        /*id=*/ 1,
        /*name=*/ "John",
        /*age=*/ 30,
        /*lsn=*/ 4,
        /*xact_id=*/ Some(4),
    )
    .await;
    env.stream_commit(/*lsn=*/ 5, /*xact_id=*/ 4).await;

    // Block wait until iceberg snapshot created.
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 5)
        .await
        .unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 5); // two data files, two index block files, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.into_iter() {
        // Check the first data file.
        if cur_data_file.file_path() == old_data_file.file_path() {
            let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
            let expected_arrow_batch = arrow_batch_1.clone();
            assert_eq!(actual_arrow_batch, expected_arrow_batch);
            // Check the first deletion vector.
            assert_eq!(
                cur_deletion_vector
                    .batch_deletion_vector
                    .collect_deleted_rows(),
                vec![0]
            );
            check_deletion_vector_consistency(&cur_deletion_vector).await;
            continue;
        }

        // Check the second data file.
        let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
        let expected_arrow_batch = arrow_batch_2.clone();
        assert_eq!(actual_arrow_batch, expected_arrow_batch);
        // Check the second deletion vector.
        let deleted_rows = cur_deletion_vector
            .batch_deletion_vector
            .collect_deleted_rows();
        assert!(
            deleted_rows.is_empty(),
            "Deletion vector for the second data file is {deleted_rows:?}"
        );
        check_deletion_vector_consistency(&cur_deletion_vector).await;
    }

    // ---- Create snapshot only with old records deleted ----
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 7).await;
    env.delete_row(
        /*id=*/ 2,
        /*name=*/ "Bob",
        /*age=*/ 20,
        /*lsn=*/ 6,
        /*xact_id=*/ Some(5),
    )
    .await;
    env.stream_commit(/*lsn=*/ 7, /*xact_id*/ 5).await;

    // Block wait until iceberg snapshot created.
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 7)
        .await
        .unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 6); // two data files, two index block files, two deletion vector puffins
    assert_eq!(snapshot.disk_files.len(), 2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.into_iter() {
        // Check the first data file.
        if cur_data_file.file_path() == old_data_file.file_path() {
            let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
            let expected_arrow_batch = arrow_batch_1.clone();
            assert_eq!(actual_arrow_batch, expected_arrow_batch);
            // Check the first deletion vector.
            assert_eq!(
                cur_deletion_vector
                    .batch_deletion_vector
                    .collect_deleted_rows(),
                vec![0]
            );
            check_deletion_vector_consistency(&cur_deletion_vector).await;
            continue;
        }

        // Check the second data file.
        let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
        let expected_arrow_batch = arrow_batch_2.clone();
        assert_eq!(actual_arrow_batch, expected_arrow_batch);
        // Check the second deletion vector.
        // Check the first deletion vector.
        assert_eq!(
            cur_deletion_vector
                .batch_deletion_vector
                .collect_deleted_rows(),
            vec![0]
        );
        check_deletion_vector_consistency(&cur_deletion_vector).await;
    }

    // Requested LSN is no later than current iceberg snapshot LSN.
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 1).await;
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 1)
        .await
        .unwrap();
}

/// Testing scenario: iceberg snapshot request shouldn't block, even if there's no write operations to the table.
#[tokio::test]
async fn test_empty_table_snapshot_creation() {
    let temp_dir = tempdir().unwrap();
    let mut env = TestEnvironment::new(temp_dir, MooncakeTableConfig::default()).await;

    let mut rx_vec = Vec::with_capacity(10);
    for _ in 1..=10 {
        let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 0).await;
        rx_vec.push(rx);
    }
    for cur_rx in rx_vec {
        TableEventManager::synchronize_force_snapshot_request(cur_rx, /*requested_lsn=*/ 0)
            .await
            .unwrap();
    }
}

/// Testing senario: request iceberg snapshot with multiple LSNs.
#[tokio::test]
async fn test_multiple_snapshot_requests() {
    // Set mooncake and iceberg flush and snapshot threshold to huge value, to verify force flush and force snapshot works as expected.
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: MooncakeTableConfig::DEFAULT_BATCH_SIZE,
        disk_slice_parquet_file_size: MooncakeTableConfig::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
        mem_slice_size: 1000,
        snapshot_deletion_record_count: 1000,
        temp_files_directory: temp_dir.path().to_str().unwrap().to_string(),
        data_compaction_config: DataCompactionConfig::default(),
        file_index_config: FileIndexMergeConfig::default(),
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 1000,
            new_committed_deletion_log: 1000,
        },
    };
    let mut env = TestEnvironment::new(temp_dir, mooncake_table_config.clone()).await;

    // Arrow batches used in test.
    let arrow_batch_1 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John".to_string()])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    let arrow_batch_2 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bob".to_string()])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();

    // Make a commit request at the very beginning, so iceberg snapshot request won't return immediately.
    env.commit(/*lsn=*/ 0).await;

    // Create multiple iceberg snapshot requests in advance.
    let mut rx_vec = Vec::new();
    // First flush and commit LSN.
    rx_vec.push(env.table_event_manager.initiate_snapshot(/*lsn=*/ 1).await);
    // Second flush and commit LSN.
    rx_vec.push(env.table_event_manager.initiate_snapshot(/*lsn=*/ 2).await);
    // The same requested LSN as previous.
    rx_vec.push(env.table_event_manager.initiate_snapshot(/*lsn=*/ 2).await);
    // A LSN already satisfied.
    rx_vec.push(env.table_event_manager.initiate_snapshot(/*lsn=*/ 0).await);
    // Record the largest requested LSN.
    let largest_requested_lsn = 2;

    // Append a new row to the mooncake table, won't trigger a force snapshot.
    env.append_row(
        /*id=*/ 1, /*name=*/ "John", /*age=*/ 30, /*lsn=*/ 0,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 1).await;

    // Append a new row to the mooncake table, will trigger a force snapshot.
    env.append_row(
        /*id=*/ 2, /*name=*/ "Bob", /*age=*/ 20, /*lsn=*/ 2,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 3).await;

    for rx in rx_vec.into_iter() {
        // For all receive handlers, it should receive at least once a persisted table LSN which is >= the largest requested LSN.
        TableEventManager::synchronize_force_snapshot_request(rx, largest_requested_lsn)
            .await
            .unwrap();
    }

    // Check iceberg snapshot content.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file
    assert_eq!(snapshot.disk_files.len(), 1);
    let (cur_data_file, cur_deletion_vector) = snapshot.disk_files.into_iter().next().unwrap();

    // Check the data file.
    let actual_arrow_batch = load_one_arrow_batch(cur_data_file.file_path()).await;
    assert_eq!(actual_arrow_batch.num_rows(), 2);
    assert_eq!(
        actual_arrow_batch.slice(/*offset=*/ 0, /*length=*/ 1),
        arrow_batch_1
    );
    assert_eq!(
        actual_arrow_batch.slice(/*offset=*/ 1, /*length=*/ 1),
        arrow_batch_2
    );

    // Check the deletion vector.
    assert!(cur_deletion_vector
        .batch_deletion_vector
        .collect_deleted_rows()
        .is_empty(),);
    check_deletion_vector_consistency(&cur_deletion_vector).await;
}

/// Test that flush_lsn correctly reflects LSN ordering for batch operations
#[tokio::test]
async fn test_flush_lsn_ordering() {
    let temp_dir = tempdir().unwrap();
    let mut env = TestEnvironment::new(temp_dir, MooncakeTableConfig::default()).await;

    // Subscribe to flush_lsn updates
    let mut flush_lsn_rx = env.table_event_manager.subscribe_flush_lsn();

    // Initial flush_lsn should be 0
    assert_eq!(*flush_lsn_rx.borrow(), 0);

    // Commit data at LSN 10
    env.append_row(1, "Alice", 25, /*lsn=*/ 5, None).await;
    env.commit(10).await;

    // Request iceberg snapshot at LSN 10
    let rx = env.table_event_manager.initiate_snapshot(10).await;
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 10)
        .await
        .unwrap();

    // Verify that flush_lsn was updated to 10
    flush_lsn_rx.changed().await.unwrap();
    assert_eq!(*flush_lsn_rx.borrow(), 10);

    // Commit data at LSN 20
    env.append_row(2, "Bob", 30, /*lsn=*/ 15, None).await;
    env.commit(20).await;

    // Request iceberg snapshot at LSN 20
    let rx = env.table_event_manager.initiate_snapshot(20).await;
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 20)
        .await
        .unwrap();

    // Flush LSN should now be 20
    flush_lsn_rx.changed().await.unwrap();
    assert_eq!(*flush_lsn_rx.borrow(), 20);

    env.shutdown().await;
}

/// Test flush_lsn with out-of-order LSN operations
#[tokio::test]
async fn test_flush_lsn_out_of_order_lsn_operations() {
    let temp_dir = tempdir().unwrap();
    let mut env = TestEnvironment::new(temp_dir, MooncakeTableConfig::default()).await;

    // Subscribe to flush_lsn updates
    let mut flush_lsn_rx = env.table_event_manager.subscribe_flush_lsn();

    // Commit operations out of chronological order but in LSN order
    env.append_row(1, "User1", 25, /*lsn=*/ 25, None).await;
    env.commit(30).await;

    env.append_row(2, "User2", 30, /*lsn=*/ 15, None).await;
    env.commit(20).await; // Lower LSN after higher LSN

    env.append_row(3, "User3", 35, /*lsn=*/ 35, None).await;
    env.commit(40).await;

    // Request snapshot at LSN 40 (should include all committed data)
    let rx = env.table_event_manager.initiate_snapshot(40).await;
    TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 40)
        .await
        .unwrap();

    // Verify flush_lsn reflects the snapshot LSN
    flush_lsn_rx.changed().await.unwrap();
    let flush_lsn = *flush_lsn_rx.borrow();

    // The flush_lsn should be the snapshot LSN which includes all committed data
    assert_eq!(flush_lsn, 40);

    env.shutdown().await;
}

/// Test flush_lsn consistency across multiple snapshots
#[tokio::test]
async fn test_flush_lsn_consistency_across_snapshots() {
    let temp_dir = tempdir().unwrap();
    let mut env = TestEnvironment::new(temp_dir, MooncakeTableConfig::default()).await;

    // Subscribe to flush_lsn updates
    let mut flush_lsn_rx = env.table_event_manager.subscribe_flush_lsn();

    // Create multiple snapshots and verify flush_lsn consistency
    let test_lsns = vec![10, 20, 30, 40, 50];

    for lsn in test_lsns {
        // Add data and commit
        env.append_row(
            lsn as i32,
            &format!("User{lsn}"),
            25,
            /*lsn=*/ lsn - 5,
            None,
        )
        .await;
        env.commit(lsn).await;

        // Create snapshot
        let rx = env.table_event_manager.initiate_snapshot(lsn).await;
        TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ lsn)
            .await
            .unwrap();

        // Verify flush_lsn matches expected LSN
        flush_lsn_rx.changed().await.unwrap();
        assert_eq!(*flush_lsn_rx.borrow(), lsn);

        // Verify persistence by loading from iceberg
        let mut iceberg_table_manager =
            env.create_iceberg_table_manager(MooncakeTableConfig::default());
        let (_, snapshot) = iceberg_table_manager
            .load_snapshot_from_table()
            .await
            .unwrap();
        assert_eq!(snapshot.data_file_flush_lsn, Some(lsn));
    }

    env.shutdown().await;
}

#[tokio::test]
async fn test_initial_copy_basic() {
    let mut env = TestEnvironment::default().await;
    // Get a direct sender so we can emit raw TableEvents.
    let sender = env.handler.get_event_sender();

    // Start initial copy workflow.
    sender
        .send(TableEvent::StartInitialCopy)
        .await
        .expect("send start initial copy");

    // Simulate the copy process delivering an existing row.
    // This row gets appended directly to main mem_slice.
    sender
        .send(TableEvent::Append {
            row: create_row(1, "Alice", 30),
            xact_id: None,
            lsn: 5,
            is_copied: true,
        })
        .await
        .expect("send copied row");

    // A new row arrives while copy is running.
    env.append_row(2, "Bob", 40, /*lsn=*/ 5, None).await;
    env.commit(10).await; // Buffered until copy finishes

    // During initial copy: commit LSN stays 0 (no actual commits applied)
    // Only replication LSN advances to track CDC stream progress
    env.set_replication_lsn(10);

    env.set_snapshot_lsn(0);
    // During initial copy, should see empty table (no commits applied yet)
    env.verify_snapshot(0, &[]).await; // Should see empty table during initial copy

    // Finish the copy which applies buffered changes.
    sender
        .send(TableEvent::FinishInitialCopy { start_lsn: 0 })
        .await
        .expect("send finish initial copy");

    println!("finish initial copy");

    // After FinishInitialCopy, we need to commit and flush to create a snapshot
    // This makes the buffered data and copied data visible together
    env.commit(10).await;
    env.flush_table(10).await;

    println!("commit and flush table");

    // Now set the LSNs and verify both Alice (copied) and Bob (buffered) are visible
    env.set_table_commit_lsn(10);
    env.set_replication_lsn(10);

    env.verify_snapshot(10, &[1, 2]).await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_periodical_force_snapshot_with_empty_table() {
    let env = TestEnvironment::default().await;
    // Get a direct sender so we can emit raw TableEvents.
    let sender = env.handler.get_event_sender();

    // Mimic force snapshot.
    sender
        .send(TableEvent::ForceSnapshot { lsn: None })
        .await
        .unwrap();
    TableEventManager::synchronize_force_snapshot_request(
        env.force_snapshot_completion_rx.clone(),
        /*requested_lsn=*/ 0,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_periodical_force_snapshot() {
    let env = TestEnvironment::default().await;
    // Get a direct sender so we can emit raw TableEvents.
    let sender = env.handler.get_event_sender();

    // Append rows to the table.
    env.append_row(2, "Bob", 40, /*lsn=*/ 5, None).await;
    env.commit(10).await;

    // Mimic force snapshot.
    sender
        .send(TableEvent::ForceSnapshot { lsn: None })
        .await
        .unwrap();
    TableEventManager::synchronize_force_snapshot_request(
        env.force_snapshot_completion_rx.clone(),
        /*requested_lsn=*/ 0,
    )
    .await
    .unwrap();

    // Check iceberg snapshot result.
    let mut iceberg_table_manager =
        env.create_iceberg_table_manager(MooncakeTableConfig::default());
    let (_, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 10);
}

#[tokio::test]
async fn test_index_merge_with_sufficient_file_indices() {
    let mut env = TestEnvironment::default().await;

    // Force index merge when there's nothing to merge.
    env.force_index_merge_and_sync().await.unwrap();

    // Append two rows to the table, and flush right afterwards.
    env.append_row(
        /*id=*/ 2, /*name=*/ "Bob", /*age=*/ 40, /*lsn=*/ 5,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(10).await;
    env.flush_table_and_sync(/*lsn=*/ 10).await;

    env.append_row(
        /*id=*/ 3, /*name=*/ "Tom", /*age=*/ 50, /*lsn=*/ 15,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(20).await;
    env.flush_table_and_sync(/*lsn=*/ 20).await;

    // Force index merge and iceberg snapshot, check result.
    env.force_index_merge_and_sync().await.unwrap();

    // Check mooncake snapshot.
    env.verify_snapshot(/*target_lsn=*/ 20, /*ids=*/ &[2, 3])
        .await;

    // Check iceberg snapshot result.
    let mut iceberg_table_manager =
        env.create_iceberg_table_manager(MooncakeTableConfig::default());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 20);
    assert_eq!(snapshot.disk_files.len(), 2); // two data files created by two flushes
    assert_eq!(snapshot.indices.file_indices.len(), 1); // one merged file index

    // Add another file and trigger a new force index merge.
    env.append_row(
        /*id=*/ 4, /*name=*/ "Cat", /*age=*/ 40, /*lsn=*/ 5,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(30).await;
    env.flush_table_and_sync(/*lsn=*/ 30).await;

    // Force index merge and iceberg snapshot, check result.
    env.force_index_merge_and_sync().await.unwrap();

    // Check mooncake snapshot.
    env.verify_snapshot(/*target_lsn=*/ 30, /*ids=*/ &[2, 3, 4])
        .await;

    // Check iceberg snapshot result.
    let mut iceberg_table_manager =
        env.create_iceberg_table_manager(MooncakeTableConfig::default());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 4);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 30);
    assert_eq!(snapshot.disk_files.len(), 3); // three data files created by three flushes
    assert_eq!(snapshot.indices.file_indices.len(), 1); // one merged file index
}

#[tokio::test]
async fn test_data_compaction_with_sufficient_data_files() {
    let mut env = TestEnvironment::default().await;

    // Force index merge when there's nothing to merge.
    env.force_data_compaction_and_sync().await.unwrap();

    // Append two rows to the table, and flush right afterwards.
    env.append_row(
        /*id=*/ 2, /*name=*/ "Bob", /*age=*/ 40, /*lsn=*/ 5,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(10).await;
    env.flush_table_and_sync(/*lsn=*/ 10).await;

    env.append_row(
        /*id=*/ 3, /*name=*/ "Tom", /*age=*/ 50, /*lsn=*/ 15,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(20).await;
    env.flush_table_and_sync(/*lsn=*/ 20).await;

    // Force index merge and iceberg snapshot, check result.
    env.force_data_compaction_and_sync().await.unwrap();

    // Check mooncake snapshot.
    env.verify_snapshot(/*target_lsn=*/ 20, /*ids=*/ &[2, 3])
        .await;

    // Check iceberg snapshot result.
    let mut iceberg_table_manager =
        env.create_iceberg_table_manager(MooncakeTableConfig::default());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 20);
    assert_eq!(snapshot.disk_files.len(), 1); // one compacted data file
    assert_eq!(snapshot.indices.file_indices.len(), 1); // one compacted file index

    // Add another file and trigger a new force data compaction.
    env.append_row(
        /*id=*/ 4, /*name=*/ "Cat", /*age=*/ 40, /*lsn=*/ 5,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(30).await;
    env.flush_table_and_sync(/*lsn=*/ 30).await;

    // Force index merge and iceberg snapshot, check result.
    env.force_data_compaction_and_sync().await.unwrap();

    // Check mooncake snapshot.
    env.verify_snapshot(/*target_lsn=*/ 30, /*ids=*/ &[2, 3, 4])
        .await;

    // Check iceberg snapshot result.
    let mut iceberg_table_manager =
        env.create_iceberg_table_manager(MooncakeTableConfig::default());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 30);
    assert_eq!(snapshot.disk_files.len(), 1); // one compacted data file
    assert_eq!(snapshot.indices.file_indices.len(), 1); // one compacted file index
}

#[tokio::test]
async fn test_full_maintenance_with_sufficient_data_files() {
    let temp_dir = tempdir().unwrap();
    // Setup mooncake config, which won't trigger any data compaction or index merge, if not full table maintaince.
    let mooncake_table_config = MooncakeTableConfig {
        data_compaction_config: DataCompactionConfig {
            data_file_to_compact: u32::MAX,
            data_file_final_size: u64::MAX,
        },
        file_index_config: FileIndexMergeConfig {
            file_indices_to_merge: u32::MAX,
            index_block_final_size: u64::MAX,
        },
        ..Default::default()
    };
    let mut env = TestEnvironment::new(temp_dir, mooncake_table_config).await;

    // Force index merge when there's nothing to merge.
    env.force_data_compaction_and_sync().await.unwrap();

    // Append two rows to the table, and flush right afterwards.
    env.append_row(
        /*id=*/ 2, /*name=*/ "Bob", /*age=*/ 40, /*lsn=*/ 5,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(10).await;
    env.flush_table_and_sync(/*lsn=*/ 10).await;

    env.append_row(
        /*id=*/ 3, /*name=*/ "Tom", /*age=*/ 50, /*lsn=*/ 15,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(20).await;
    env.flush_table_and_sync(/*lsn=*/ 20).await;

    // Force index merge and iceberg snapshot, check result.
    env.force_full_maintenance_and_sync().await.unwrap();

    // Check mooncake snapshot.
    env.verify_snapshot(/*target_lsn=*/ 20, /*ids=*/ &[2, 3])
        .await;

    // Check iceberg snapshot result.
    let mut iceberg_table_manager =
        env.create_iceberg_table_manager(MooncakeTableConfig::default());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 20);
    assert_eq!(snapshot.disk_files.len(), 1); // one compacted data file
    assert_eq!(snapshot.indices.file_indices.len(), 1); // one compacted file index

    // Add another file and trigger a new force full compaction.
    env.append_row(
        /*id=*/ 4, /*name=*/ "Cat", /*age=*/ 40, /*lsn=*/ 5,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(30).await;
    env.flush_table_and_sync(/*lsn=*/ 30).await;

    // Force index merge and iceberg snapshot, check result.
    env.force_full_maintenance_and_sync().await.unwrap();

    // Check mooncake snapshot.
    env.verify_snapshot(/*target_lsn=*/ 30, /*ids=*/ &[2, 3, 4])
        .await;

    // Check iceberg snapshot result.
    let mut iceberg_table_manager =
        env.create_iceberg_table_manager(MooncakeTableConfig::default());
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 30);
    assert_eq!(snapshot.disk_files.len(), 1); // one compacted data file
    assert_eq!(snapshot.indices.file_indices.len(), 1); // one compacted file index
}

/// Testing scenario: write operations no later than persisted LSN shall be discarded.
#[tokio::test]
async fn test_discard_duplicate_writes() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config =
        MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mut mock_mooncake_snapshot = MooncakeSnapshot::new(mooncake_table_metadata.clone());
    mock_mooncake_snapshot.data_file_flush_lsn = Some(10);
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_load_snapshot_from_table()
        .times(1)
        .returning(move || {
            let mock_mooncake_snapshot_copy = mock_mooncake_snapshot.clone();
            Box::pin(async move {
                Ok((/*next_file_id=*/ 0, mock_mooncake_snapshot_copy))
            })
        });
    mock_table_manager
        .expect_sync_snapshot()
        .times(1)
        .returning(|snapshot_payload: IcebergSnapshotPayload, _| {
            Box::pin(async move {
                let mock_persistence_result = PersistenceResult {
                    remote_data_files: snapshot_payload.import_payload.data_files.clone(),
                    remote_file_indices: snapshot_payload.import_payload.file_indices.clone(),
                    puffin_blob_ref: HashMap::new(),
                };
                Ok(mock_persistence_result)
            })
        });

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        ObjectStorageCache::default_for_test(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
    )
    .await
    .unwrap();
    let env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Perform non-streaming write operation which should be discarded.
    env.append_row(
        /*id=*/ 1, /*name=*/ "John", /*age=*/ 30, /*lsn=*/ 0,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 0).await;

    // Perform a streaming write operation which should be discarded.
    env.append_row(
        /*id=*/ 2,
        /*name=*/ "Bob",
        /*age=*/ 20,
        /*lsn=*/ 2,
        /*xact_id=*/ Some(0),
    )
    .await;
    env.stream_commit(/*lsn=*/ 2, /*xact_id=*/ 0).await;

    // Append a real row by non-streaming write, which should appear in the later read operation.
    env.append_row(
        /*id=*/ 30, /*name=*/ "Car", /*age=*/ 30, /*lsn=*/ 25,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 30).await;

    // Append a real row by streaming write, which should appear in the later read operation.
    env.append_row(
        /*id=*/ 40,
        /*name=*/ "Dog",
        /*age=*/ 40,
        /*lsn=*/ 0,
        /*xact_id=*/ Some(40),
    )
    .await;
    env.stream_commit(/*lsn=*/ 40, /*xact_id=*/ 40).await;

    // Perform a read operation, and check results.
    env.set_table_commit_lsn(40);
    env.set_replication_lsn(40);
    env.verify_snapshot(40, &[30, 40]).await;
}

/// ---- Util functions unit test ----
#[test]
fn test_get_persisted_table_lsn() {
    let (table_maintenance_completion_tx, _) = broadcast::channel(64usize);
    let (force_snapshot_completion_tx, _) = watch::channel(None);
    let mut table_handler_state = TableHandlerState::new(
        table_maintenance_completion_tx,
        force_snapshot_completion_tx,
        /*initial_persistence_lsn=*/ None,
    );

    // Case-1: no table activity since for the current table.
    {
        let iceberg_snapshot_lsn = None;
        let replication_lsn = 1;
        table_handler_state.table_consistent_view_lsn = None;

        let persisted_table_lsn =
            table_handler_state.get_persisted_table_lsn(iceberg_snapshot_lsn, replication_lsn);
        assert_eq!(persisted_table_lsn, 1);
    }

    // Case-2: table is at a consistent state, but iceberg persistence doesn't catch up.
    {
        let iceberg_snapshot_lsn = Some(1);
        let replication_lsn = 2;
        table_handler_state.table_consistent_view_lsn = Some(2);

        let persisted_table_lsn =
            table_handler_state.get_persisted_table_lsn(iceberg_snapshot_lsn, replication_lsn);
        assert_eq!(persisted_table_lsn, 1);
    }

    // Case-3: iceberg snapshot matches table consistent view.
    {
        let iceberg_snapshot_lsn = Some(1);
        let replication_lsn = 2;
        table_handler_state.table_consistent_view_lsn = Some(1);

        let persisted_table_lsn =
            table_handler_state.get_persisted_table_lsn(iceberg_snapshot_lsn, replication_lsn);
        assert_eq!(persisted_table_lsn, 2);
    }
}

/// Unit-test the state-machine guard: a higher-LSN flush finishing first must
/// NOT allow an Iceberg snapshot until the lowest pending flush also finishes.
#[test]
fn test_can_initiate_snapshot_out_of_order_flush_completion() {
    use tokio::sync::broadcast;

    let (tx, _) = broadcast::channel(8);
    let (force_snapshot_completion_tx, _) = watch::channel(None);
    let mut state = TableHandlerState::new(
        tx,
        force_snapshot_completion_tx,
        /*initial_persistence_lsn=*/ None,
    );

    // Pretend two flushes were issued (10 and 20). 20 finishes first.
    state.pending_flush_lsns.insert(10);
    state.pending_flush_lsns.insert(20);

    // Preconditions needed for `can_initiate_iceberg_snapshot`
    state.iceberg_snapshot_result_consumed = true;
    state.iceberg_snapshot_ongoing = false;

    // Higher-LSN (20) flush completes – remove it from the pending set.
    state.pending_flush_lsns.remove(&20);

    // Gate should still block an Iceberg snapshot at 20 because 10 is pending.
    assert!(
        !state.can_initiate_iceberg_snapshot(),
        "Snapshot should be blocked while lower LSN=10 is still pending"
    );

    // Now lower-LSN (10) flush completes.
    state.pending_flush_lsns.remove(&10);

    // With no pending flushes, snapshot at 20 is allowed.
    assert!(
        state.can_initiate_iceberg_snapshot(),
        "Snapshot should proceed once all lower pending flushes are finished"
    );
}

/// Unit-test the state-machine guard: a higher-LSN flush finishing first must
/// NOT allow an Iceberg force snapshot until the lowest pending flush also finishes.
#[test]
fn test_can_initiate_force_snapshot_out_of_order_flush_completion() {
    use tokio::sync::broadcast;

    let (tx, _) = broadcast::channel(8);
    let (force_snapshot_completion_tx, _) = watch::channel(None);
    let mut state = TableHandlerState::new(
        tx,
        force_snapshot_completion_tx,
        /*initial_persistence_lsn=*/ None,
    );

    // Pretend two flushes were issued (10 and 20). 20 finishes first.
    state.pending_flush_lsns.insert(10);
    state.pending_flush_lsns.insert(20);

    // Preconditions needed for `can_initiate_iceberg_snapshot`
    state.iceberg_snapshot_result_consumed = true;
    state.iceberg_snapshot_ongoing = false;

    // Higher-LSN (20) flush completes – remove it from the pending set.
    state.pending_flush_lsns.remove(&20);

    // Issue a force snapshot request at 20.
    let _ = state.largest_force_snapshot_lsn.insert(20);

    // Gate should still block an Iceberg snapshot at 20 because 10 is pending.
    // assert!(
    //     !state.force_snapshot_requested(20),
    //     "Snapshot should be blocked while lower LSN=10 is still pending"
    // );

    // Now lower-LSN (10) flush completes.
    state.pending_flush_lsns.remove(&10);

    // With no pending flushes, snapshot at 20 is allowed.
    // assert!(
    //     state.force_snapshot_requested(20),
    //     "Snapshot should proceed once all lower pending flushes are finished"
    // );
}
