use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::mooncake_table::INITIAL_COPY_XACT_ID;
use crate::storage::{io_utils, MooncakeTable};
use crate::table_notify::TableEvent;
use crate::{Error, Result};
use more_asserts as ma;
use std::collections::BTreeMap;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tracing::Instrument;
use tracing::{debug, error, info, info_span, warn};

/// Handler for table operations
pub struct TableHandler {
    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the table event queue
    event_sender: Sender<TableEvent>,
}

/// Contains a few senders, which notifies after certain iceberg events completion.
pub struct EventSyncSender {
    /// Notifies when drop table completes.
    pub drop_table_completion_tx: oneshot::Sender<Result<()>>,
    /// Notifies when iceberg flush LSN advances.
    pub flush_lsn_tx: watch::Sender<u64>,
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub async fn new(mut table: MooncakeTable, event_sync_sender: EventSyncSender) -> Self {
        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        // Create channel for internal control events.
        table.register_table_notify(event_sender.clone()).await;

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(
            async move {
                Self::event_loop(event_sync_sender, event_receiver, table).await;
            }
            .instrument(info_span!("table_event_loop")),
        ));

        // Create the handler
        Self {
            _event_handle: event_handle,
            event_sender,
        }
    }

    /// Get the event sender to send events to this handler
    pub fn get_event_sender(&self) -> Sender<TableEvent> {
        self.event_sender.clone()
    }

    /// Main event processing loop
    #[tracing::instrument(name = "table_event_loop", skip_all)]
    async fn event_loop(
        event_sync_sender: EventSyncSender,
        mut event_receiver: Receiver<TableEvent>,
        mut table: MooncakeTable,
    ) {
        let mut periodic_snapshot_interval = time::interval(Duration::from_millis(500));

        // Requested minimum LSN for a force snapshot request.
        let mut force_snapshot_lsns: BTreeMap<u64, Vec<Sender<Result<()>>>> = BTreeMap::new();

        // Record LSN if the last handled table event is committed, which indicates mooncake table stays at a consistent view, so table could be flushed safely.
        let mut table_consistent_view_lsn: Option<u64> = None;

        // Whether there's an ongoing mooncake snapshot operation.
        let mut mooncake_snapshot_ongoing = false;

        // Whether there's an ongoing iceberg snapshot operation.
        let mut iceberg_snapshot_ongoing = false;

        // Whether there's an ongoing background maintenance operation, for example, index merge, data compaction, etc.
        // To simplify state management, we have at most one ongoing maintainance operation at the same time.
        let mut maintainance_ongoing = false;

        // Whether current table receives any update events.
        let mut table_updated = false;

        // Whether requested to drop table.
        let mut drop_table_requested = false;

        // Whether iceberg snapshot result has been consumed by the latest mooncake snapshot, when creating a mooncake snapshot.
        //
        // There're three possible states for an iceberg snapshot:
        // - snapshot ongoing = false, result consumed = true: no active iceberg snapshot
        // - snapshot ongoing = true, result consumed = true: iceberg snapshot is ongoing
        // - snapshot ongoing = false, result consumed = false: iceberg snapshot completes, but wait for mooncake snapshot to consume the result
        //
        // Invariant of impossible state: snapshot ongoing = true, result consumed = false
        let mut iceberg_snapshot_result_consumed = true;

        // Invoked before mooncake snapshot creation, this function checks and resets iceberg snapshot state.
        let reset_iceberg_state_at_mooncake_snapshot =
            |iceberg_consumed: &mut bool, iceberg_ongoing: &mut bool| {
                // Validate iceberg snapshot state before mooncake snapshot creation.
                //
                // Assertion on impossible state.
                assert!(!*iceberg_ongoing || *iceberg_consumed);

                // If there's pending iceberg snapshot result unconsumed, the following mooncake snapshot will properly handle it.
                if !*iceberg_consumed {
                    *iceberg_consumed = true;
                    *iceberg_ongoing = false;
                }
            };

        // Used to decide whether we could create an iceberg snapshot.
        // The completion of an iceberg snapshot is **NOT** marked as the finish of snapshot thread, but the handling of its results.
        // We can only create a new iceberg snapshot when (1) there's no ongoing iceberg snapshot; and (2) previous snapshot results have been acknowledged.
        let can_initiate_iceberg_snapshot =
            |iceberg_consumed: bool, iceberg_ongoing: bool| iceberg_consumed && !iceberg_ongoing;

        // Used to clean up mooncake table status, and send completion notification.
        let drop_table = async |table: &mut MooncakeTable, event_sync_sender: EventSyncSender| {
            // Step-1: shutdown the table, which unreferences and deletes all cache files.
            if let Err(e) = table.shutdown().await {
                event_sync_sender
                    .drop_table_completion_tx
                    .send(Err(e))
                    .unwrap();
                return;
            }

            // Step-2: delete the iceberg table.
            if let Err(e) = table.drop_iceberg_table().await {
                event_sync_sender
                    .drop_table_completion_tx
                    .send(Err(e))
                    .unwrap();
                return;
            }

            // Step-3: delete the mooncake table.
            if let Err(e) = table.drop_mooncake_table().await {
                event_sync_sender
                    .drop_table_completion_tx
                    .send(Err(e))
                    .unwrap();
                return;
            }

            // Step-4: send back completion notification.
            event_sync_sender
                .drop_table_completion_tx
                .send(Ok(()))
                .unwrap();
        };

        // Util function to spawn a detached task to delete evicted data files.
        let start_task_to_delete_evicted = |evicted_file_to_delete: Vec<String>| {
            if evicted_file_to_delete.is_empty() {
                return;
            }
            tokio::task::spawn(async move {
                if let Err(err) = io_utils::delete_local_files(evicted_file_to_delete).await {
                    error!("Failed to delete object storage cache: {:?}", err);
                }
            });
        };

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    table_consistent_view_lsn = match event {
                        TableEvent::Commit { lsn, .. } => Some(lsn),
                        // All events apart from replication events don't affect whether mooncake is at a committed state.
                        TableEvent::ForceSnapshot { .. }
                        | TableEvent::MooncakeTableSnapshot { .. }
                        | TableEvent::IcebergSnapshot { .. }
                        | TableEvent::IndexMerge { .. }
                        | TableEvent::DataCompaction { .. }
                        | TableEvent::ReadRequest { .. }
                        | TableEvent::EvictedDataFilesToDelete { .. } => table_consistent_view_lsn,
                        _ => None,
                    };
                    table_updated = match event {
                        TableEvent::ForceSnapshot { .. } => table_updated,
                        _ => true,
                    };

                    match event {
                        // ==============================
                        // Replication events
                        // ==============================
                        //
                        TableEvent::Append { is_copied, row, xact_id } => {
                            if is_copied || (!table.is_in_initial_copy() && xact_id.is_none()) {
                                if let Err(e) = table.append(row) {
                                    warn!(error = %e, "failed to append row");
                                }
                                if table.should_flush() {
                                    if let Err(e) = table.flush(0).await {
                                        warn!(error = %e, "flush failed in append");
                                    }
                                }
                                continue;
                            }

                            let xid = xact_id.unwrap_or(INITIAL_COPY_XACT_ID);

                            if let Err(e) = table.append_in_stream_batch(row, xid) {
                                warn!(error = %e, "failed to append row in batch");
                                continue;
                            }

                            if table.should_transaction_flush(xid) {
                                if let Err(e) = table.flush_transaction_stream(xid).await {
                                    warn!(error = %e, "flush failed in append");
                                }
                            }
                        }
                        TableEvent::Delete { row, lsn, xact_id } => {
                            if !table.is_in_initial_copy() && xact_id.is_none() {
                                table.delete(row, lsn).await;
                                continue;
                            }

                            let xid = xact_id.unwrap_or(INITIAL_COPY_XACT_ID);
                            table.delete_in_stream_batch(row, xid).await;
                        }
                        TableEvent::Commit { lsn, xact_id } => {
                            // Force create snapshot if
                            // 1. force snapshot is requested
                            // and 2. LSN which meets force snapshot requirement has appeared, before that we still allow buffering
                            // and 3. there's no snapshot creation operation ongoing
                            let force_snapshot = !force_snapshot_lsns.is_empty()
                                && lsn >= *force_snapshot_lsns.iter().next().as_ref().unwrap().0
                                && !mooncake_snapshot_ongoing;

                            if table.is_in_initial_copy() {
                                let xid = xact_id.unwrap_or(INITIAL_COPY_XACT_ID);
                                let commit = table
                                    .prepare_transaction_stream_commit(xid, lsn)
                                    .await.unwrap();
                                table.buffer_initial_copy_commit(commit);
                            } else if let Some(xid) = xact_id {
                                let commit = table
                                    .prepare_transaction_stream_commit(xid, lsn)
                                    .await.unwrap();
                                if let Err(e) = table.commit_transaction_stream(commit).await {
                                    warn!(error = %e, "stream commit flush failed");
                                }
                            } else {
                                table.commit(lsn);
                                if table.should_flush() || force_snapshot {
                                    if let Err(e) = table.flush(lsn).await {
                                        warn!(error = %e, "flush failed in commit");
                                    }
                                }
                            }

                            if force_snapshot {
                                reset_iceberg_state_at_mooncake_snapshot(&mut iceberg_snapshot_result_consumed, &mut iceberg_snapshot_ongoing);
                                table.create_snapshot(SnapshotOption {
                                    force_create: true,
                                    skip_iceberg_snapshot: iceberg_snapshot_ongoing,
                                    skip_file_indices_merge: maintainance_ongoing,
                                    skip_data_file_compaction: maintainance_ongoing,
                                });
                                mooncake_snapshot_ongoing = true;
                            }
                        }
                        TableEvent::StreamAbort { xact_id } => {
                            table.abort_in_stream_batch(xact_id);
                        }
                        TableEvent::Flush { lsn } => {
                            if table.is_in_initial_copy() {
                                if let Err(e) = table.flush_transaction_stream(INITIAL_COPY_XACT_ID).await {
                                    warn!(error = %e, "explicit flush failed");
                                }
                            } else if let Err(e) = table.flush(lsn).await {
                                warn!(error = %e, "explicit flush failed");
                            }
                        }
                        TableEvent::StreamFlush { xact_id } => {
                            if let Err(e) = table.flush_transaction_stream(xact_id).await {
                                error!(error = %e, "stream flush failed");
                            }
                        }
                        TableEvent::Shutdown => {
                            if let Err(e) = table.shutdown().await {
                                error!(error = %e, "failed to shutdown table");
                            }
                            info!("shutting down table handler");
                            break;
                        }
                        // ==============================
                        // Interactive blocking events
                        // ==============================
                        //
                        TableEvent::ForceSnapshot { lsn, tx } => {
                            // A workaround to avoid create snapshot call gets stuck, when there's no write operations to the table.
                            if !table_updated {
                                tx.send(Ok(())).await.unwrap();
                                continue;
                            }

                            // Fast-path: if iceberg snapshot requirement is already satisfied, notify directly.
                            let last_iceberg_snapshot_lsn = table.get_iceberg_snapshot_lsn();
                            if last_iceberg_snapshot_lsn.is_some() && lsn <= last_iceberg_snapshot_lsn.unwrap() {
                                tx.send(Ok(())).await.unwrap();
                            }
                            // Iceberg snapshot LSN requirement is not met, record the required LSN, so later commit will pick up.
                            else {
                                force_snapshot_lsns.entry(lsn).or_default().push(tx);
                            }
                        }
                        // Branch to drop the iceberg table and clear pinned data files from the global object storage cache, only used when the whole table requested to drop.
                        // So we block wait for asynchronous request completion.
                        TableEvent::DropTable => {
                            // Fast-path: no other concurrent events, directly clean up states and ack back.
                            if !mooncake_snapshot_ongoing && !iceberg_snapshot_ongoing {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }

                            // Otherwise, leave a drop marker to clean up states later.
                            drop_table_requested = true;
                        }
                        TableEvent::StartInitialCopy => {
                            debug!("starting initial copy");
                            table.start_initial_copy();
                        }
                        TableEvent::FinishInitialCopy => {
                            debug!("finishing initial copy");
                            if let Err(e) = table.finish_initial_copy().await {
                                warn!(error = %e, "failed to finish initial copy");
                            }
                        }
                        // ==============================
                        // Table internal events
                        // ==============================
                        //
                        TableEvent::MooncakeTableSnapshot { lsn, iceberg_snapshot_payload, data_compaction_payload, file_indice_merge_payload, evicted_data_files_to_delete } => {
                            // Spawn a detached best-effort task to delete evicted object storage cache.
                            start_task_to_delete_evicted(evicted_data_files_to_delete);

                            // Drop table if requested, and table at a clean state.
                            if drop_table_requested && !iceberg_snapshot_ongoing {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }

                            // Notify read the mooncake table commit of LSN.
                            table.notify_snapshot_reader(lsn);

                            // Process iceberg snapshot and trigger iceberg snapshot if necessary.
                            if can_initiate_iceberg_snapshot(iceberg_snapshot_result_consumed, iceberg_snapshot_ongoing) {
                                if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
                                    iceberg_snapshot_ongoing = true;
                                    table.persist_iceberg_snapshot(iceberg_snapshot_payload);
                                }
                            }

                            // Attempt to process data compaction.
                            // Unlike snapshot, we can actually have multiple file index merge operations ongoing concurrently,
                            // to simplify workflow we limit at most one ongoing.
                            if !maintainance_ongoing {
                                if let Some(data_compaction_payload) = data_compaction_payload {
                                    maintainance_ongoing = true;
                                    table.perform_data_compaction(data_compaction_payload);
                                }
                            }

                            // Attempt to process file indices merge.
                            // Unlike snapshot, we can actually have multiple file index merge operations ongoing concurrently,
                            // to simplify workflow we limit at most one ongoing.
                            if !maintainance_ongoing {
                                if let Some(file_indice_merge_payload) = file_indice_merge_payload {
                                    maintainance_ongoing = true;
                                    table.perform_index_merge(file_indice_merge_payload);
                                }
                            }

                            mooncake_snapshot_ongoing = false;
                        }
                        TableEvent::IcebergSnapshot { iceberg_snapshot_result } => {
                            iceberg_snapshot_ongoing = false;
                            match iceberg_snapshot_result {
                                Ok(snapshot_res) => {
                                    let iceberg_flush_lsn = snapshot_res.flush_lsn;
                                    event_sync_sender.flush_lsn_tx.send(iceberg_flush_lsn).unwrap();
                                    table.set_iceberg_snapshot_res(snapshot_res);
                                    iceberg_snapshot_result_consumed = false;

                                    // Notify all waiters with LSN satisfied.
                                    let new_map = force_snapshot_lsns.split_off(&(iceberg_flush_lsn + 1));
                                    for (requested_lsn, tx) in force_snapshot_lsns.iter() {
                                        ma::assert_le!(*requested_lsn, iceberg_flush_lsn);
                                        for cur_tx in tx {
                                            cur_tx.send(Ok(())).await.unwrap();
                                        }
                                    }
                                    force_snapshot_lsns = new_map;
                                }
                                Err(e) => {
                                    for (_, tx) in force_snapshot_lsns.iter() {
                                        for cur_tx in tx {
                                            let err = Error::IcebergMessage(format!("Failed to create iceberg snapshot: {:?}", e));
                                            cur_tx.send(Err(err)).await.unwrap();
                                        }
                                    }
                                    force_snapshot_lsns.clear();
                                }
                            }

                            // Drop table if requested, and table at a clean state.
                            if drop_table_requested && !mooncake_snapshot_ongoing {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }
                        }
                        TableEvent::IndexMerge { index_merge_result } => {
                            table.set_file_indices_merge_res(index_merge_result);
                            maintainance_ongoing = false;
                        }
                        TableEvent::DataCompaction { data_compaction_result } => {
                            match data_compaction_result {
                                Ok(data_compaction_res) => {
                                    table.set_data_compaction_res(data_compaction_res)
                                }
                                Err(err) => {
                                    error!(error = ?err, "failed to perform compaction");
                                }
                            }
                            maintainance_ongoing = false;
                        }
                        TableEvent::ReadRequest { cache_handles } => {
                            table.set_read_request_res(cache_handles);
                        }
                        TableEvent::EvictedDataFilesToDelete { evicted_data_files } => {
                            start_task_to_delete_evicted(evicted_data_files);
                        }
                    }
                }
                // Periodic snapshot based on time
                _ = periodic_snapshot_interval.tick() => {
                    // Only create a periodic snapshot if there isn't already one in progress
                    if mooncake_snapshot_ongoing {
                        continue;
                    }

                    // Check whether a flush and force snapshot is needed.
                    if !force_snapshot_lsns.is_empty() {
                        if let Some(commit_lsn) = table_consistent_view_lsn {
                            if *force_snapshot_lsns.iter().next().as_ref().unwrap().0 <= commit_lsn {
                                table.flush(/*lsn=*/ commit_lsn).await.unwrap();
                                reset_iceberg_state_at_mooncake_snapshot(&mut iceberg_snapshot_result_consumed, &mut iceberg_snapshot_ongoing);
                                table.create_snapshot(SnapshotOption {
                                    force_create: true,
                                    skip_iceberg_snapshot: iceberg_snapshot_ongoing,
                                    skip_file_indices_merge: maintainance_ongoing,
                                    skip_data_file_compaction: maintainance_ongoing,
                                });
                                mooncake_snapshot_ongoing = true;
                                continue;
                            }
                        }
                    }

                    // Fallback to normal periodic snapshot.
                    reset_iceberg_state_at_mooncake_snapshot(&mut iceberg_snapshot_result_consumed, &mut iceberg_snapshot_ongoing);
                    mooncake_snapshot_ongoing = table.create_snapshot(SnapshotOption {
                        force_create: false,
                        skip_iceberg_snapshot: iceberg_snapshot_ongoing,
                        skip_file_indices_merge: maintainance_ongoing,
                        skip_data_file_compaction: maintainance_ongoing,
                    });
                }
                // If all senders have been dropped, exit the loop
                else => {
                    if let Err(e) = table.shutdown().await {
                        error!(error = %e, "failed to shutdown table");
                    }
                    info!("all event senders dropped, shutting down table handler");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_utils;
