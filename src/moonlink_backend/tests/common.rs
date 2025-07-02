use arrow_array::Int64Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
use tempfile::TempDir;
use tokio_postgres::{connect, types::PgLsn, Client, NoTls};

use std::{collections::HashSet, fs::File};

use moonlink::decode_read_state_for_testing;
use moonlink_backend::{
    recreate_directory, MoonlinkBackend, ReadState, DEFAULT_MOONLINK_TEMP_FILE_PATH,
};

pub const SRC_URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";
pub const DST_URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

type DatabaseId = u64;
type TableId = u64;
pub const DATABASE_ID: DatabaseId = 0;
pub const TABLE_ID: TableId = 0;

pub struct TestGuard {
    backend: Arc<MoonlinkBackend<DatabaseId, TableId>>,
    tmp: Option<TempDir>,
}

impl TestGuard {
    pub async fn new(table_name: Option<&'static str>) -> (Self, Client) {
        let (tmp, backend, client) = setup_backend(table_name).await;
        let guard = Self {
            backend: Arc::new(backend),
            tmp: Some(tmp),
        };
        (guard, client)
    }

    pub fn backend(&self) -> &Arc<MoonlinkBackend<DatabaseId, TableId>> {
        &self.backend
    }

    #[allow(dead_code)]
    pub fn tmp(&self) -> Option<&TempDir> {
        self.tmp.as_ref()
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        // move everything we need into the async block
        let backend = Arc::clone(&self.backend);
        let tmp = self.tmp.take();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let _ = backend.drop_table(DATABASE_ID, TABLE_ID).await;
                let _ = backend.shutdown_connection(SRC_URI).await;
                let _ = recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH);
                drop(tmp);
            });
        });
    }
}

/// Return the current WAL LSN as a simple `u64`.
pub async fn current_wal_lsn(client: &Client) -> u64 {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()", &[])
        .await
        .unwrap();
    let lsn: PgLsn = row.get(0);
    lsn.into()
}

/// Read the first column of a Parquet file into a `Vec<Option<i64>>`.
pub fn read_ids_from_parquet(path: &str) -> Vec<Option<i64>> {
    let file = File::open(path).unwrap_or_else(|_| panic!("open {path}"));
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..col.len()).map(|i| Some(col.value(i))).collect()
}

/// Extract **all** primary-key IDs referenced in `read_state`.
pub fn ids_from_state(read_state: &ReadState) -> HashSet<i64> {
    let (files, _, _, _) = decode_read_state_for_testing(read_state);
    files
        .into_iter()
        .flat_map(|f| read_ids_from_parquet(&f).into_iter().flatten())
        .collect()
}

/// Extract primary-key IDs from `read_state` **after applying deletion vectors and position deletes**.
#[allow(dead_code)]
pub async fn ids_from_state_with_deletes(read_state: &ReadState) -> HashSet<i64> {
    use iceberg::io::FileIOBuilder;
    use iceberg::puffin::PuffinReader;

    let (data_files, puffin_files, deletion_vectors, mut position_deletes) =
        decode_read_state_for_testing(read_state);

    // Load deletion vector blobs and convert to position deletes
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    for cur_blob in deletion_vectors.iter() {
        let puffin_file_path = puffin_files
            .get(cur_blob.puffin_file_index as usize)
            .unwrap();

        // Load puffin file and read blob
        let input_file = file_io.new_input(puffin_file_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let puffin_metadata = puffin_reader.file_metadata().await.unwrap();

        // Assume single blob per puffin file (as per moonlink convention)
        let blob_metadata = &puffin_metadata.blobs()[0];
        let blob = puffin_reader.blob(blob_metadata).await.unwrap();

        // Parse deletion vector from blob data
        let deleted_row_indices = parse_deletion_vector_blob(blob.data());

        if !deleted_row_indices.is_empty() {
            position_deletes.extend(
                deleted_row_indices
                    .iter()
                    .map(|row_idx| (cur_blob.data_file_index, *row_idx as u32)),
            );
        }
    }

    // Apply position deletes to get final set of IDs
    apply_position_deletes_to_files(&data_files, &position_deletes)
}

/// Parse deletion vector blob data to extract deleted row indices.
/// This is a simplified parser for the deletion vector format.
fn parse_deletion_vector_blob(blob_data: &[u8]) -> Vec<u64> {
    // Deletion vector format: | len (4 bytes) | magic (4 bytes) | roaring bitmap | crc32c (4 bytes) |
    if blob_data.len() < 12 {
        return Vec::new();
    }

    // Skip length and magic bytes, get bitmap portion (excluding CRC at end)
    let bitmap_start = 8;
    let bitmap_end = blob_data.len() - 4;
    let bitmap_data = &blob_data[bitmap_start..bitmap_end];

    // Parse roaring bitmap
    match roaring::RoaringTreemap::deserialize_from(bitmap_data) {
        Ok(bitmap) => bitmap.iter().collect(),
        Err(_) => Vec::new(), // Return empty if parsing fails
    }
}

/// Helper function to apply position deletes to data files and return the remaining IDs
fn apply_position_deletes_to_files(
    data_files: &[String],
    position_deletes: &[(u32, u32)], // (file_index, row_index)
) -> HashSet<i64> {
    // Group deletes by file index
    let mut deletes_by_file: std::collections::HashMap<u32, HashSet<u32>> =
        std::collections::HashMap::new();
    for (file_index, row_index) in position_deletes {
        deletes_by_file
            .entry(*file_index)
            .or_default()
            .insert(*row_index);
    }

    let mut result = HashSet::new();
    for (file_index, file_path) in data_files.iter().enumerate() {
        let ids = read_ids_from_parquet(file_path);
        let deletes = deletes_by_file.get(&(file_index as u32));

        for (row_index, id_opt) in ids.into_iter().enumerate() {
            if let Some(id) = id_opt {
                // Only include the ID if it's not in the delete set for this file
                if deletes.is_none_or(|d| !d.contains(&(row_index as u32))) {
                    result.insert(id);
                }
            }
        }
    }
    result
}

/// Spin up a backend + scratch TempDir + psql client, and guarantee
/// a **fresh table** named `table_name` exists and is registered with
/// Moonlink.
async fn setup_backend(
    table_name: Option<&'static str>,
) -> (TempDir, MoonlinkBackend<DatabaseId, TableId>, Client) {
    let temp_dir = TempDir::new().unwrap();
    let backend =
        MoonlinkBackend::<DatabaseId, TableId>::new(temp_dir.path().to_str().unwrap().into());

    // Connect to Postgres.
    let (client, connection) = connect(SRC_URI, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Clear any leftover replication slot from previous runs.
    let _ = client
        .simple_query(
            "SELECT pg_terminate_backend(active_pid)
             FROM pg_replication_slots
             WHERE slot_name = 'moonlink_slot_postgres';",
        )
        .await;
    let _ = client
        .simple_query("SELECT pg_drop_replication_slot('moonlink_slot_postgres')")
        .await;

    // Re-create the working table.
    if let Some(table_name) = table_name {
        client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {0};
                 CREATE TABLE {0} (id BIGINT PRIMARY KEY, name TEXT);",
                table_name
            ))
            .await
            .unwrap();
        backend
            .create_table(
                DATABASE_ID,
                TABLE_ID,
                DST_URI.to_string(),
                format!("public.{table_name}"),
                SRC_URI.to_string(),
            )
            .await
            .unwrap();
    }

    (temp_dir, backend, client)
}

/// Reusable helper for the "create table / insert rows / detect change"
/// scenario used in two places.
#[allow(dead_code)]
pub async fn smoke_create_and_insert(
    backend: &MoonlinkBackend<DatabaseId, TableId>,
    client: &Client,
    uri: &str,
) {
    client
        .simple_query(
            "DROP TABLE IF EXISTS test;
                           CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT);",
        )
        .await
        .unwrap();

    backend
        .create_table(
            DATABASE_ID,
            TABLE_ID,
            DST_URI.to_string(),
            "public.test".to_string(),
            uri.to_string(),
        )
        .await
        .unwrap();

    // First two rows.
    client
        .simple_query("INSERT INTO test VALUES (1,'foo'),(2,'bar');")
        .await
        .unwrap();

    let old = backend
        .scan_table(DATABASE_ID, TABLE_ID, None)
        .await
        .unwrap();
    let lsn = current_wal_lsn(client).await;
    let new = backend
        .scan_table(DATABASE_ID, TABLE_ID, Some(lsn))
        .await
        .unwrap();
    assert_ne!(old.data, new.data);

    recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
}
