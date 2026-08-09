use std::io::ErrorKind;
use std::path::Path;

#[cfg(unix)]
use codex_app_server_protocol::JSONRPCMessage;
#[cfg(unix)]
use codex_app_server_protocol::JSONRPCNotification;
#[cfg(unix)]
use codex_uds::PeerCredentials;
#[cfg(unix)]
use codex_uds::UnixStream;
#[cfg(unix)]
use futures::SinkExt;
use pretty_assertions::assert_eq;
#[cfg(unix)]
use tokio::sync::mpsc;
use tokio::sync::oneshot;
#[cfg(unix)]
use tokio::time::Duration;
#[cfg(unix)]
use tokio::time::timeout;
#[cfg(unix)]
use tokio_tungstenite::client_async;
#[cfg(unix)]
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
#[cfg(unix)]
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
#[cfg(unix)]
use tokio_tungstenite::tungstenite::http::HeaderValue;
#[cfg(unix)]
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use super::super::CHANNEL_CAPACITY;
#[cfg(unix)]
use super::super::ConnectionOrigin;
#[cfg(unix)]
use super::super::TransportEvent;
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
async fn terminal_accept_error_reports_endpoint_failure() {
    let (failure_tx, failure_rx) = oneshot::channel();
    let mut failure_tx = Some(failure_tx);

    report_local_controller_acceptor_failure(
        &mut failure_tx,
        std::io::Error::new(ErrorKind::AddrNotAvailable, "accept failed"),
    );

    assert!(failure_tx.is_none());
    assert_eq!(
        failure_rx.await.expect("accept failure should be reported"),
        LocalControllerEndpointFailure {
            reason: "local-controller socket accept error: accept failed".to_string(),
        }
    );
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
fn endpoint_support_reports_available_when_peer_credentials_are_supported() {
    assert_eq!(
        local_controller_endpoint_support(),
        LocalControllerEndpointSupport::Available
    );
}

#[cfg(not(unix))]
#[test]
fn endpoint_support_reports_unavailable_without_peer_credentials() {
    assert_eq!(
        local_controller_endpoint_support(),
        LocalControllerEndpointSupport::Unavailable {
            reason: LocalControllerUnavailableReason::PeerCredentialsUnavailable,
        }
    );
}

#[test]
fn endpoint_unavailable_fallback_is_terminal_before_binding() {
    let err =
        ensure_local_controller_endpoint_available(LocalControllerEndpointSupport::Unavailable {
            reason: LocalControllerUnavailableReason::PeerCredentialsUnavailable,
        })
        .expect_err("unavailable platform should reject");

    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert_eq!(
        err.to_string(),
        "local-controller endpoint is unavailable because peer credential verification is unsupported on this platform"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn local_controller_acceptor_publishes_metadata_and_forwards_websocket_messages_with_nonce() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = short_temp_dir();
    let (transport_event_tx, mut transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let handle = start_local_controller_acceptor(
        temp_dir.path(),
        Some("main-thread".to_string()),
        transport_event_tx,
        shutdown_token.clone(),
    )
    .await
    .expect("local-controller acceptor should start");
    let paths = local_controller_endpoint_paths(temp_dir.path(), &handle.metadata().launch_id)
        .expect("paths should resolve");
    assert_eq!(handle.socket_path(), &paths.socket_path);
    assert_eq!(
        handle.metadata().endpoint_uri,
        format!("unix://{}", paths.socket_path.display())
    );
    assert_eq!(
        read_metadata(paths.metadata_path.as_path()).await,
        handle.metadata().clone()
    );
    assert_eq!(
        tokio::fs::metadata(paths.socket_path.as_path())
            .await
            .expect("socket metadata should exist")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let (mut websocket, response) = connect_local_controller(
        paths.socket_path.as_path(),
        Some(handle.metadata().launch_nonce.as_str()),
    )
    .await
    .expect("websocket should connect with launch nonce");
    assert_eq!(response.status().as_u16(), 101);

    let opened = recv_transport_event(&mut transport_event_rx).await;
    let connection_id = match opened {
        TransportEvent::ConnectionOpened {
            connection_id,
            origin,
            ..
        } => {
            assert_eq!(origin, ConnectionOrigin::ExternalController);
            connection_id
        }
        _ => panic!("expected connection opened event"),
    };

    let notification = JSONRPCMessage::Notification(JSONRPCNotification {
        method: "initialized".to_string(),
        params: None,
    });
    websocket
        .send(WebSocketMessage::Text(
            serde_json::to_string(&notification)
                .expect("notification should serialize")
                .into(),
        ))
        .await
        .expect("notification should send");
    assert_eq!(
        match recv_transport_event(&mut transport_event_rx).await {
            TransportEvent::IncomingMessage {
                connection_id: incoming_connection_id,
                message,
            } => (incoming_connection_id, message),
            _ => panic!("expected incoming message event"),
        },
        (connection_id, notification)
    );

    websocket.close(None).await.expect("close should send");
    assert!(matches!(
        recv_transport_event(&mut transport_event_rx).await,
        TransportEvent::ConnectionClosed {
            connection_id: closed_connection_id,
        } if closed_connection_id == connection_id
    ));

    handle.shutdown().await.expect("acceptor should join");
    assert!(!paths.socket_path.as_path().exists());
    assert!(!paths.metadata_path.as_path().exists());
}

#[cfg(unix)]
#[tokio::test]
async fn local_controller_acceptor_republishes_metadata_with_main_thread_id() {
    let temp_dir = short_temp_dir();
    let (transport_event_tx, _transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let mut handle = start_local_controller_acceptor(
        temp_dir.path(),
        /*main_thread_id*/ None,
        transport_event_tx,
        shutdown_token.clone(),
    )
    .await
    .expect("local-controller acceptor should start");
    let paths = local_controller_endpoint_paths(temp_dir.path(), &handle.metadata().launch_id)
        .expect("paths should resolve");

    assert_eq!(
        read_metadata(paths.metadata_path.as_path())
            .await
            .main_thread_id,
        None
    );

    handle
        .publish_main_thread_id("main-thread".to_string())
        .await
        .expect("main thread metadata should publish");

    assert_eq!(
        handle.metadata().main_thread_id,
        Some("main-thread".to_string())
    );
    assert_eq!(
        read_metadata(paths.metadata_path.as_path()).await,
        handle.metadata().clone()
    );

    handle
        .publish_main_thread_id("other-thread".to_string())
        .await
        .expect("second main thread metadata publish should be a no-op");
    assert_eq!(
        handle.metadata().main_thread_id,
        Some("main-thread".to_string())
    );
    assert_eq!(
        read_metadata(paths.metadata_path.as_path()).await,
        handle.metadata().clone()
    );

    handle.shutdown().await.expect("acceptor should join");
    assert!(!paths.socket_path.as_path().exists());
    assert!(!paths.metadata_path.as_path().exists());
}

#[cfg(unix)]
#[tokio::test]
async fn local_controller_acceptor_rejects_missing_or_wrong_launch_nonce() {
    let temp_dir = short_temp_dir();
    let (transport_event_tx, mut transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let handle = start_local_controller_acceptor(
        temp_dir.path(),
        Some("main-thread".to_string()),
        transport_event_tx,
        shutdown_token.clone(),
    )
    .await
    .expect("local-controller acceptor should start");

    for launch_nonce in [None, Some("wrong-nonce")] {
        let err = match connect_local_controller(handle.socket_path().as_path(), launch_nonce).await
        {
            Ok(_) => panic!("websocket should reject invalid launch nonce"),
            Err(err) => err,
        };
        assert_websocket_http_status(err, 401);
        assert!(
            timeout(Duration::from_millis(100), transport_event_rx.recv())
                .await
                .is_err()
        );
    }

    handle.shutdown().await.expect("acceptor should join");
}

#[test]
fn peer_credential_verification_rejects_missing_credentials() {
    let err = verify_peer_credentials(Err(std::io::Error::new(ErrorKind::Unsupported, "missing")))
        .expect_err("missing credentials should reject");

    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
}

#[cfg(unix)]
#[tokio::test]
async fn peer_credential_verification_accepts_current_user_and_rejects_mismatch() {
    let credentials = connected_peer_credentials().await;
    verify_peer_credentials(Ok(credentials)).expect("current user should verify");

    let err = verify_peer_credentials(Ok(PeerCredentials {
        user_id: different_user_id(credentials.user_id),
        ..credentials
    }))
    .expect_err("different user should reject");

    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
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

#[cfg(unix)]
async fn connect_local_controller(
    socket_path: &Path,
    launch_nonce: Option<&str>,
) -> Result<
    (
        tokio_tungstenite::WebSocketStream<UnixStream>,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    tokio_tungstenite::tungstenite::Error,
> {
    let stream = UnixStream::connect(socket_path)
        .await
        .expect("client should connect");
    let mut request = "ws://localhost/rpc"
        .into_client_request()
        .expect("request should build");
    if let Some(launch_nonce) = launch_nonce {
        request.headers_mut().insert(
            LOCAL_CONTROLLER_LAUNCH_NONCE_HEADER,
            HeaderValue::from_str(launch_nonce).expect("nonce should be a header value"),
        );
    }
    client_async(request, stream).await
}

#[cfg(unix)]
async fn recv_transport_event(
    transport_event_rx: &mut mpsc::Receiver<TransportEvent>,
) -> TransportEvent {
    timeout(Duration::from_secs(1), transport_event_rx.recv())
        .await
        .expect("transport event should arrive")
        .expect("transport event")
}

#[cfg(unix)]
fn assert_websocket_http_status(err: tokio_tungstenite::tungstenite::Error, status: u16) {
    match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            assert_eq!(response.status().as_u16(), status);
        }
        _ => panic!("expected websocket HTTP error, got {err:?}"),
    }
}

#[cfg(unix)]
async fn connected_peer_credentials() -> PeerCredentials {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = temp_dir.path().join("credentials.sock");
    let mut listener = codex_uds::UnixListener::bind(&socket_path)
        .await
        .expect("listener should bind");
    let client_task = tokio::spawn(async move {
        UnixStream::connect(&socket_path)
            .await
            .expect("client should connect")
    });
    let server_stream = listener.accept().await.expect("server should accept");
    let _client_stream = client_task.await.expect("client task should join");

    server_stream
        .peer_credentials()
        .expect("peer credentials should resolve")
}

#[cfg(unix)]
fn short_temp_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("lc")
        .tempdir_in("/tmp")
        .expect("short temp dir")
}

#[cfg(unix)]
fn different_user_id(user_id: u32) -> u32 {
    if user_id == u32::MAX {
        user_id - 1
    } else {
        user_id + 1
    }
}
