// ── Sub-modules ───────────────────────────────────────────────────────────────

mod checkpoint;
pub mod staging;
mod types;
mod utils;

// ── Public re-exports ─────────────────────────────────────────────────────────

pub use checkpoint::{LocalCheckpointStore, SnapshotCheckpointLedger, SnapshotTableCheckpoint};
pub use staging::{LocalStageChunkStore, MinioStageChunkStore, StageChunkStore};
pub use types::{
    LocalStagingConfig, MinioStagingConfig, SinkCommit, StageChunk, StageChunkPayload,
    StageChunkRequest, StagingConfig, StagingKind, DEFAULT_AWS_REGION,
    STAGING_CONTENT_ENCODING_GZIP, STAGING_CONTENT_TYPE_JSONL,
};
pub use utils::build_chunk_key;

// ── Crate-level constants ─────────────────────────────────────────────────────

pub const CRATE_NAME: &str = "astra-runtime";

// ── Miscellaneous ─────────────────────────────────────────────────────────────

pub fn status() -> &'static str {
    "runtime staging contract implemented"
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::*;
    use crate::utils::unix_time_ms;

    fn temp_root(name: &str) -> PathBuf {
        let unique = format!("astra-runtime-{name}-{}", unix_time_ms());
        std::env::temp_dir().join(unique)
    }

    #[test]
    fn builds_predictable_chunk_key() {
        assert_eq!(
            build_chunk_key("postgres-analytics", "public.orders", "default", 42),
            "pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000042.jsonl.gz"
        );
    }

    #[test]
    fn prefixes_chunk_keys_without_double_slashes() {
        let config = StagingConfig {
            kind: StagingKind::Local,
            bucket: "astra-staging".to_string(),
            prefix: "/postgres-analytics//".to_string(),
        };

        assert_eq!(
            config.chunk_key("postgres-analytics", "public.orders", "default", 7),
            "postgres-analytics/pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000007.jsonl.gz"
        );
    }

    #[tokio::test]
    async fn local_store_writes_and_reads_chunks() {
        let root_dir = temp_root("write-read");
        let store = LocalStageChunkStore::new(LocalStagingConfig {
            root_dir: root_dir.clone(),
            storage: StagingConfig {
                kind: StagingKind::Local,
                bucket: "astra-staging".to_string(),
                prefix: "dev".to_string(),
            },
        });

        let request = StageChunkRequest {
            pipeline_name: "postgres-analytics".to_string(),
            stream_name: "public.orders".to_string(),
            partition_key: "default".to_string(),
            sequence: 42,
            payload: StageChunkPayload::jsonl_gzip(2, b"pretend-gzip-jsonl".to_vec()),
        };

        let chunk = store.write_chunk(request).await.expect("chunk writes");
        assert_eq!(chunk.bucket, "astra-staging");
        assert_eq!(chunk.row_count, 2);
        assert_eq!(chunk.bytes_written, 18);
        assert_eq!(
            chunk.object_key,
            "dev/pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000042.jsonl.gz"
        );

        let resolved = store.resolve_path(&chunk.object_key);
        assert!(
            resolved.exists(),
            "expected staged file at {}",
            resolved.display()
        );

        let bytes = store.read_chunk(&chunk).await.expect("chunk reads");
        assert_eq!(bytes, b"pretend-gzip-jsonl");

        fs::remove_dir_all(root_dir).ok();
    }

    #[tokio::test]
    async fn local_store_lists_chunks_for_pipeline() {
        let root_dir = temp_root("list");
        let store = LocalStageChunkStore::new(LocalStagingConfig {
            root_dir: root_dir.clone(),
            storage: StagingConfig {
                kind: StagingKind::Local,
                bucket: "astra-staging".to_string(),
                prefix: "dev".to_string(),
            },
        });

        store
            .write_chunk(StageChunkRequest {
                pipeline_name: "postgres-analytics".to_string(),
                stream_name: "public.orders".to_string(),
                partition_key: "default".to_string(),
                sequence: 2,
                payload: StageChunkPayload::jsonl_gzip(1, vec![1, 2, 3]),
            })
            .await
            .unwrap();
        store
            .write_chunk(StageChunkRequest {
                pipeline_name: "postgres-analytics".to_string(),
                stream_name: "public.users".to_string(),
                partition_key: "default".to_string(),
                sequence: 1,
                payload: StageChunkPayload::jsonl_gzip(1, vec![4, 5, 6]),
            })
            .await
            .unwrap();

        let chunks = store
            .list_chunks_for_pipeline("postgres-analytics")
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].stream_name, "public.orders");
        assert_eq!(chunks[0].sequence, 2);
        assert_eq!(chunks[1].stream_name, "public.users");

        fs::remove_dir_all(root_dir).ok();
    }

    #[tokio::test]
    async fn local_store_rejects_bucket_mismatch() {
        let root_dir = temp_root("bucket-mismatch");
        let store = LocalStageChunkStore::new(LocalStagingConfig {
            root_dir: root_dir.clone(),
            storage: StagingConfig {
                kind: StagingKind::Local,
                bucket: "astra-staging".to_string(),
                prefix: String::new(),
            },
        });

        store.ensure_ready().await.expect("store initializes");
        let chunk = StageChunk {
            pipeline_name: "postgres-analytics".to_string(),
            stream_name: "public.orders".to_string(),
            partition_key: "default".to_string(),
            sequence: 1,
            bucket: "wrong-bucket".to_string(),
            object_key: build_chunk_key("postgres-analytics", "public.orders", "default", 1),
            bytes_written: 0,
            row_count: 0,
            content_type: STAGING_CONTENT_TYPE_JSONL.to_string(),
            content_encoding: STAGING_CONTENT_ENCODING_GZIP.to_string(),
            schema_fingerprint: None,
            created_at_unix_ms: unix_time_ms(),
        };

        let error = store
            .read_chunk(&chunk)
            .await
            .expect_err("bucket mismatch should fail");
        assert!(error.to_string().contains("chunk bucket mismatch"));

        fs::remove_dir_all(root_dir).ok();
    }

    #[test]
    fn minio_config_reads_local_first_env() {
        let storage = StagingConfig {
            kind: StagingKind::Minio,
            bucket: "astra-staging".to_string(),
            prefix: "dev".to_string(),
        };
        std::env::set_var("ASTRA_S3_ENDPOINT", "http://127.0.0.1:9000");
        std::env::set_var("ASTRA_S3_ACCESS_KEY", "astra");
        std::env::set_var("ASTRA_S3_SECRET_KEY", "astrastorage");
        std::env::remove_var("ASTRA_S3_REGION");
        std::env::remove_var("AWS_REGION");

        let config = MinioStagingConfig::from_env(storage).expect("env config loads");
        assert_eq!(config.endpoint, "http://127.0.0.1:9000");
        assert_eq!(config.region, DEFAULT_AWS_REGION);
        assert_eq!(config.access_key, "astra");
        assert_eq!(config.secret_key, "astrastorage");
    }

    #[test]
    fn checkpoint_store_persists_resume_state() {
        let root_dir = temp_root("checkpoint-store");
        let store = LocalCheckpointStore::new(root_dir.clone());

        store
            .record_chunk_staged(
                "postgres-analytics",
                "public.orders",
                0,
                500,
                "dev/pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000000.jsonl.gz",
                None,
            )
            .expect("checkpoint writes");
        store
            .mark_table_complete("postgres-analytics", "public.orders")
            .expect("completion writes");

        let ledger = store.load("postgres-analytics").expect("ledger loads");
        let checkpoint = ledger.tables.get("public.orders").expect("table exists");
        assert_eq!(checkpoint.next_sequence, 1);
        assert_eq!(checkpoint.rows_staged, 500);
        assert!(checkpoint.completed);
        assert_eq!(
            checkpoint.last_chunk_key.as_deref(),
            Some("dev/pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000000.jsonl.gz")
        );

        fs::remove_dir_all(root_dir).ok();
    }

    #[test]
    fn checkpoint_store_persists_cursor_value() {
        let root_dir = temp_root("checkpoint-cursor");
        let store = LocalCheckpointStore::new(root_dir.clone());

        store
            .record_chunk_staged(
                "postgres-analytics",
                "public.orders",
                0,
                100,
                "some/chunk/key",
                Some(serde_json::json!("2024-06-01T00:00:00Z")),
            )
            .expect("checkpoint with cursor writes");

        let ledger = store.load("postgres-analytics").expect("ledger loads");
        let checkpoint = ledger.tables.get("public.orders").expect("table exists");
        assert_eq!(
            checkpoint.last_cursor_value,
            Some(serde_json::json!("2024-06-01T00:00:00Z"))
        );

        // A subsequent chunk with a later cursor overwrites the stored value.
        store
            .record_chunk_staged(
                "postgres-analytics",
                "public.orders",
                1,
                50,
                "some/chunk/key2",
                Some(serde_json::json!("2024-07-01T00:00:00Z")),
            )
            .expect("second checkpoint writes");

        let ledger = store.load("postgres-analytics").expect("ledger loads");
        let checkpoint = ledger.tables.get("public.orders").expect("table exists");
        assert_eq!(
            checkpoint.last_cursor_value,
            Some(serde_json::json!("2024-07-01T00:00:00Z"))
        );

        // Passing None does not clear the stored cursor value.
        store
            .record_chunk_staged(
                "postgres-analytics",
                "public.orders",
                2,
                10,
                "some/chunk/key3",
                None,
            )
            .expect("third checkpoint writes");

        let ledger = store.load("postgres-analytics").expect("ledger loads");
        let checkpoint = ledger.tables.get("public.orders").expect("table exists");
        assert_eq!(
            checkpoint.last_cursor_value,
            Some(serde_json::json!("2024-07-01T00:00:00Z"))
        );

        fs::remove_dir_all(root_dir).ok();
    }
}
