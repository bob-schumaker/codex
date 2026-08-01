use std::io::ErrorKind;
use std::path::Path;

use pretty_assertions::assert_eq;

use super::*;

#[tokio::test]
async fn publish_metadata_creates_private_directory_and_owner_only_regular_file() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let metadata = test_metadata("launch-1", "nonce-1", "thread-1");

    let guard = publish_local_controller_metadata(temp_dir.path(), &metadata)
        .await
        .expect("metadata should publish");
    let paths = local_controller_endpoint_paths(temp_dir.path(), &metadata.launch_id)
        .expect("paths should resolve");
    assert_eq!(guard.metadata_path(), &paths.metadata_path);

    let stored = read_metadata(paths.metadata_path.as_path()).await;
    assert_eq!(stored, metadata);
    let metadata_file_type = tokio::fs::symlink_metadata(paths.metadata_path.as_path())
        .await
        .expect("metadata file should exist")
        .file_type();
    assert!(metadata_file_type.is_file());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let directory_mode = tokio::fs::symlink_metadata(paths.directory.as_path())
            .await
            .expect("directory should exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(directory_mode, 0o700);

        let metadata_mode = tokio::fs::symlink_metadata(paths.metadata_path.as_path())
            .await
            .expect("metadata file should exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(metadata_mode, 0o600);
    }

    drop(guard);
    assert!(!paths.metadata_path.as_path().exists());
}

#[tokio::test]
async fn publish_replaces_stale_metadata_owned_by_same_launch_and_nonce() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let original = test_metadata("launch-stale", "nonce-stale", "old-thread");
    let paths = local_controller_endpoint_paths(temp_dir.path(), &original.launch_id)
        .expect("paths should resolve");
    codex_uds::prepare_private_socket_directory(paths.directory.as_path())
        .await
        .expect("directory should prepare");
    write_metadata(paths.metadata_path.as_path(), &original).await;

    let replacement = test_metadata("launch-stale", "nonce-stale", "new-thread");
    let _guard = publish_local_controller_metadata(temp_dir.path(), &replacement)
        .await
        .expect("owned stale metadata should be replaced");

    assert_eq!(
        read_metadata(paths.metadata_path.as_path()).await,
        replacement
    );
}

#[tokio::test]
async fn publish_rejects_foreign_metadata_at_launch_path() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let foreign = test_metadata("launch-foreign", "foreign-nonce", "thread-foreign");
    let paths = local_controller_endpoint_paths(temp_dir.path(), &foreign.launch_id)
        .expect("paths should resolve");
    codex_uds::prepare_private_socket_directory(paths.directory.as_path())
        .await
        .expect("directory should prepare");
    write_metadata(paths.metadata_path.as_path(), &foreign).await;

    let current = test_metadata("launch-foreign", "current-nonce", "thread-current");
    let err = publish_local_controller_metadata(temp_dir.path(), &current)
        .await
        .expect_err("foreign metadata should be rejected");
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
    assert_eq!(read_metadata(paths.metadata_path.as_path()).await, foreign);
}

#[tokio::test]
async fn cleanup_guard_preserves_replaced_foreign_metadata() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let owned = test_metadata("launch-guard", "owned-nonce", "thread-owned");
    let guard = publish_local_controller_metadata(temp_dir.path(), &owned)
        .await
        .expect("owned metadata should publish");
    let paths = local_controller_endpoint_paths(temp_dir.path(), &owned.launch_id)
        .expect("paths should resolve");

    let foreign = test_metadata("launch-guard", "foreign-nonce", "thread-foreign");
    write_metadata(paths.metadata_path.as_path(), &foreign).await;
    drop(guard);

    assert_eq!(read_metadata(paths.metadata_path.as_path()).await, foreign);
}

#[cfg(unix)]
#[test]
fn cleanup_guard_does_not_follow_or_remove_symlink_metadata_path() {
    use std::os::unix::fs::symlink;

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let metadata = test_metadata("launch-symlink", "nonce-symlink", "thread-symlink");
    let paths = local_controller_endpoint_paths(temp_dir.path(), &metadata.launch_id)
        .expect("paths should resolve");
    std::fs::create_dir_all(paths.directory.as_path()).expect("directory should exist");
    let target_path = temp_dir.path().join("foreign-target.json");
    std::fs::write(&target_path, b"foreign target").expect("target should write");
    symlink(&target_path, paths.metadata_path.as_path()).expect("metadata symlink should create");

    let guard = LocalControllerEndpointGuard {
        metadata_path: paths.metadata_path.clone(),
        launch_id: metadata.launch_id,
        launch_nonce: metadata.launch_nonce,
    };
    drop(guard);

    assert!(
        std::fs::symlink_metadata(paths.metadata_path.as_path())
            .expect("metadata symlink should remain")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read(&target_path).expect("target should remain"),
        b"foreign target"
    );
}

fn test_metadata(
    launch_id: &str,
    launch_nonce: &str,
    main_thread_id: &str,
) -> LocalControllerEndpointMetadata {
    LocalControllerEndpointMetadata {
        version: LOCAL_CONTROLLER_METADATA_VERSION,
        launch_id: launch_id.to_string(),
        launch_nonce: launch_nonce.to_string(),
        endpoint_uri: format!("unix:///tmp/{launch_id}.sock"),
        process_id: 123,
        created_at: 456,
        protocol_version: LOCAL_CONTROLLER_PROTOCOL_VERSION,
        main_thread_id: Some(main_thread_id.to_string()),
    }
}

async fn write_metadata(path: &Path, metadata: &LocalControllerEndpointMetadata) {
    tokio::fs::write(
        path,
        serde_json::to_vec(metadata).expect("metadata should serialize"),
    )
    .await
    .expect("metadata should write");
}

async fn read_metadata(path: &Path) -> LocalControllerEndpointMetadata {
    serde_json::from_slice(&tokio::fs::read(path).await.expect("metadata should read"))
        .expect("metadata should deserialize")
}
