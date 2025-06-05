#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use moonlink::decode_read_state_for_testing;
    use moonlink_backend::recreate_directory;
    use moonlink_backend::MoonlinkBackend;
    use moonlink_backend::ReadState;
    use moonlink_backend::DEFAULT_MOONLINK_TEMP_FILE_PATH;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serial_test::serial;
    use std::collections::HashSet;
    use std::fs::File;
    use tempfile::TempDir;
    use tokio_postgres::{connect, types::PgLsn, Client, NoTls};

    #[test]
    #[serial]
    fn test_recreate_directory() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("temp_file.txt");
        let file = std::fs::File::create(&file_path).unwrap();
        drop(file);
        assert!(std::fs::exists(&file_path).unwrap());

        // Re-create an exising directory.
        recreate_directory(temp_dir.path().to_str().unwrap()).unwrap();
        assert!(!std::fs::exists(&file_path).unwrap());

        // Re-create a non-existent directory.
        let internal_dir = temp_dir.path().join("internal_dir");
        assert!(!std::fs::exists(&internal_dir).unwrap());
        recreate_directory(internal_dir.to_str().unwrap()).unwrap();
        assert!(std::fs::exists(&internal_dir).unwrap());
    }

    /// Test util function to create a table and attempt basic sql statements to verify creation success.
    async fn test_table_creation_impl(
        service: &MoonlinkBackend<&'static str>,
        client: &Client,
        uri: &str,
    ) {
        client.simple_query("DROP TABLE IF EXISTS test; CREATE TABLE test (id bigint PRIMARY KEY, name VARCHAR(255));").await.unwrap();
        service
            .create_table("test", "public.test", uri)
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test VALUES (1 ,'foo');")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test VALUES (2 ,'bar');")
            .await
            .unwrap();
        let old = service.scan_table(&"test", None).await.unwrap();
        // wait 2 second
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let new = service.scan_table(&"test", None).await.unwrap();
        assert_ne!(old.data, new.data);

        // Clean up temporary files directory after test.
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
    }

    async fn current_wal_lsn(client: &Client) -> u64 {
        let row = client
            .query_one("SELECT pg_current_wal_lsn()", &[])
            .await
            .unwrap();
        let lsn: PgLsn = row.get(0);
        lsn.into()
    }

    fn read_ids_from_parquet(file_path: &str) -> Vec<Option<i64>> {
        let file = File::open(file_path).expect(&format!("Failed to open file: {}", file_path));
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("Failed to create parquet reader")
            .build()
            .expect("Failed to build parquet reader");
        let batch = reader
            .into_iter()
            .next()
            .expect("No batches in parquet file")
            .expect("Failed to read batch");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast to Int64Array");
        (0..col.len()).map(|i| Some(col.value(i))).collect()
    }

    fn ids_from_state(read_state: &ReadState) -> HashSet<i64> {
        let (data_files, _, _, _) = decode_read_state_for_testing(read_state);
        let mut ids = HashSet::new();
        for path in data_files {
            for id in read_ids_from_parquet(&path).into_iter().flatten() {
                ids.insert(id);
            }
        }
        ids
    }

    async fn setup_backend(
        table_name: &'static str,
    ) -> (TempDir, MoonlinkBackend<&'static str>, Client) {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let uri = "postgresql://postgres:postgres@postgres:5432/postgres";
        let service =
            MoonlinkBackend::<&'static str>::new(temp_dir.path().to_str().unwrap().to_string());
        let (client, connection) = connect(uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Force terminate any active replication connections and drop the slot if it exists
        let _ = client.simple_query(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'moonlink_slot_postgres'"
        ).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let _ = client
            .simple_query("SELECT pg_drop_replication_slot('moonlink_slot_postgres')")
            .await;
        // Wait a bit to ensure the slot is fully dropped
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {0}; CREATE TABLE {0} (id BIGINT PRIMARY KEY, name TEXT);",
                table_name
            ))
            .await
            .unwrap();
        service
            .create_table(table_name, &format!("public.{table_name}"), uri)
            .await
            .unwrap();
        (temp_dir, service, client)
    }

    // Test table creation and drop.
    #[tokio::test]
    #[serial]
    async fn test_moonlink_service() {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let uri = "postgresql://postgres:postgres@postgres:5432/postgres";
        let service =
            MoonlinkBackend::<&'static str>::new(temp_dir.path().to_str().unwrap().to_string());
        // connect to postgres and create a table
        let (client, connection) = connect(uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        test_table_creation_impl(&service, &client, uri).await;
        service.drop_table("test").await.unwrap();
        test_table_creation_impl(&service, &client, uri).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_returns_inserted_rows() {
        let (temp_dir, service, client) = setup_backend("scan_test").await;

        client
            .simple_query("INSERT INTO scan_test VALUES (1, 'a'), (2, 'b');")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let state = service.scan_table(&"scan_test", None).await.unwrap();
        let ids = ids_from_state(&state);
        assert_eq!(ids, HashSet::from([1, 2]));

        client
            .simple_query("INSERT INTO scan_test VALUES (3, 'c');")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let state = service.scan_table(&"scan_test", None).await.unwrap();
        let ids = ids_from_state(&state);
        assert_eq!(ids, HashSet::from([1, 2, 3]));

        service.drop_table("scan_test").await.unwrap();
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
        drop(temp_dir);
    }

    #[tokio::test]
    #[serial]
    async fn test_scan_table_with_lsn() {
        let (_temp_dir, service, client) = setup_backend("lsn_test").await;

        client
            .simple_query("INSERT INTO lsn_test VALUES (1, 'a');")
            .await
            .unwrap();
        let lsn1 = current_wal_lsn(&client).await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let state = service.scan_table(&"lsn_test", Some(lsn1)).await.unwrap();
        assert_eq!(ids_from_state(&state), HashSet::from([1]));

        client
            .simple_query("INSERT INTO lsn_test VALUES (2, 'b');")
            .await
            .unwrap();
        let lsn2 = current_wal_lsn(&client).await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let state = service.scan_table(&"lsn_test", Some(lsn2)).await.unwrap();
        assert_eq!(ids_from_state(&state), HashSet::from([1, 2]));

        service.drop_table("lsn_test").await.unwrap();
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_create_iceberg_snapshot() {
        let (temp_dir, service, client) = setup_backend("snapshot_test").await;

        client
            .simple_query("INSERT INTO snapshot_test VALUES (1, 'a');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        service
            .create_iceberg_snapshot(&"snapshot_test", lsn)
            .await
            .unwrap();

        let metadata_dir = temp_dir
            .path()
            .canonicalize()
            .unwrap()
            .join("default")
            .join("public.snapshot_test")
            .join("metadata");
        assert!(metadata_dir.exists());
        let files = std::fs::read_dir(metadata_dir).unwrap();
        assert!(files.count() > 0);

        service.drop_table("snapshot_test").await.unwrap();
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
    }
}
