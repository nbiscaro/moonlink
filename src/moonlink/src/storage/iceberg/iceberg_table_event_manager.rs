/// This module interacts with iceberg snapshot status.
use tokio::sync::{mpsc, watch};

use crate::Result;
use crate::TableEvent;

/// Contains a few receivers, which get notified after certain iceberg events completion.
pub struct IcebergEventSyncReceiver {
    /// Get notified when iceberg drop table completes.
    pub iceberg_drop_table_completion_rx: mpsc::Receiver<Result<()>>,
    /// Get notified when iceberg flush lsn advances.
    pub flush_lsn_rx: watch::Receiver<u64>,
}

/// At most one outstanding snapshot request is allowed.
pub struct IcebergTableEventManager {
    /// Used to initiate a mooncake and iceberg snapshot operation.
    table_event_tx: mpsc::Sender<TableEvent>,
    /// Used to synchronize on the completion of an iceberg drop table.
    drop_table_completion_rx: mpsc::Receiver<Result<()>>,
    /// Channel to observe latest flush LSN reported by iceberg.
    flush_lsn_rx: watch::Receiver<u64>,
}

impl IcebergTableEventManager {
    pub fn new(
        table_event_tx: mpsc::Sender<TableEvent>,
        iceberg_event_sync_rx: IcebergEventSyncReceiver,
    ) -> Self {
        Self {
            table_event_tx,
            drop_table_completion_rx: iceberg_event_sync_rx.iceberg_drop_table_completion_rx,
            flush_lsn_rx: iceberg_event_sync_rx.flush_lsn_rx,
        }
    }

    /// Subscribe to flush LSN updates.
    pub fn subscribe_flush_lsn(&self) -> watch::Receiver<u64> {
        self.flush_lsn_rx.clone()
    }

    /// Initiate an iceberg snapshot event, return the channel for synchronization.
    pub async fn initiate_snapshot(&mut self, lsn: u64) -> mpsc::Receiver<Result<()>> {
        let (tx, rx) = mpsc::channel(1);
        self.table_event_tx
            .send(TableEvent::ForceSnapshot { lsn, tx })
            .await
            .unwrap();
        rx
    }

    /// Drop an iceberg table.
    pub async fn drop_table(&mut self) -> Result<()> {
        self.table_event_tx
            .send(TableEvent::DropTable)
            .await
            .unwrap();
        self.drop_table_completion_rx.recv().await.unwrap()
    }
}
