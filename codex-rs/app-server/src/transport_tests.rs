use super::*;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerNotificationEnvelope;
use codex_app_server_protocol::ServerRequestEnvelope;
use codex_app_server_protocol::ThreadRealtimeStartedNotification;
use codex_protocol::protocol::RealtimeConversationVersion;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use serde_json::json;
use tokio::time::Duration;
use tokio::time::timeout;

fn absolute_path(path: &str) -> AbsolutePathBuf {
    AbsolutePathBuf::from_absolute_path(path).expect("absolute path")
}

fn thread_realtime_started_notification() -> ServerNotification {
    ServerNotification::ThreadRealtimeStarted(ThreadRealtimeStartedNotification {
        thread_id: "thread-1".to_string(),
        realtime_session_id: None,
        version: RealtimeConversationVersion::V1,
    })
}

fn app_server_notification(notification: ServerNotification) -> OutgoingMessage {
    OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
        notification,
        thread_sequence: None,
        emitted_at_ms: Some(1_234),
    })
}

#[tokio::test]
async fn to_connection_notification_respects_opt_out_filters() {
    let connection_id = ConnectionId(7);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);
    let initialized = Arc::new(AtomicBool::new(true));
    let opted_out_notification_methods =
        Arc::new(RwLock::new(HashSet::from(["configWarning".to_string()])));

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            initialized,
            Arc::new(AtomicBool::new(true)),
            opted_out_notification_methods,
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: app_server_notification(ServerNotification::ConfigWarning(
                ConfigWarningNotification {
                    summary: "task_started".to_string(),
                    details: None,
                    path: None,
                    range: None,
                },
            )),
            write_complete_tx: None,
        },
    )
    .await;

    assert!(
        writer_rx.try_recv().is_err(),
        "opted-out notification should be dropped"
    );
}

#[tokio::test]
async fn to_connection_notifications_are_dropped_for_opted_out_clients() {
    let connection_id = ConnectionId(10);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::from(["configWarning".to_string()]))),
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: app_server_notification(ServerNotification::ConfigWarning(
                ConfigWarningNotification {
                    summary: "task_started".to_string(),
                    details: None,
                    path: None,
                    range: None,
                },
            )),
            write_complete_tx: None,
        },
    )
    .await;

    assert!(
        writer_rx.try_recv().is_err(),
        "opted-out notifications should not reach clients"
    );
}

#[tokio::test]
async fn to_connection_notifications_are_preserved_for_non_opted_out_clients() {
    let connection_id = ConnectionId(11);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: app_server_notification(ServerNotification::ConfigWarning(
                ConfigWarningNotification {
                    summary: "task_started".to_string(),
                    details: None,
                    path: None,
                    range: None,
                },
            )),
            write_complete_tx: None,
        },
    )
    .await;

    let message = writer_rx
        .recv()
        .await
        .expect("notification should reach non-opted-out clients");
    assert!(matches!(
        message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "task_started"
    ));
}

#[tokio::test]
async fn experimental_notifications_are_dropped_without_capability() {
    let connection_id = ConnectionId(12);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: app_server_notification(thread_realtime_started_notification()),
            write_complete_tx: None,
        },
    )
    .await;

    assert!(
        writer_rx.try_recv().is_err(),
        "experimental notifications should not reach clients without capability"
    );
}

#[tokio::test]
async fn experimental_notifications_are_preserved_with_capability() {
    let connection_id = ConnectionId(13);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: app_server_notification(thread_realtime_started_notification()),
            write_complete_tx: None,
        },
    )
    .await;

    let message = writer_rx
        .recv()
        .await
        .expect("experimental notification should reach opted-in client");
    assert!(matches!(
        message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ThreadRealtimeStarted(_),
            ..
        })
    ));
}

#[tokio::test]
async fn command_execution_request_approval_strips_additional_permissions_without_capability() {
    let connection_id = ConnectionId(8);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: OutgoingMessage::Request(ServerRequest::CommandExecutionRequestApproval {
                request_id: RequestId::Integer(1),
                params: codex_app_server_protocol::CommandExecutionRequestApprovalParams {
                    thread_id: "thr_123".to_string(),
                    turn_id: "turn_123".to_string(),
                    item_id: "call_123".to_string(),
                    started_at_ms: 0,
                    approval_id: None,
                    environment_id: None,
                    reason: Some("Need extra read access".to_string()),
                    network_approval_context: None,
                    command: Some("cat file".to_string()),
                    cwd: Some(absolute_path("/tmp").into()),
                    command_actions: None,
                    additional_permissions: Some(
                        codex_app_server_protocol::AdditionalPermissionProfile {
                            network: None,
                            file_system: Some(
                                codex_app_server_protocol::AdditionalFileSystemPermissions {
                                    read: Some(vec![absolute_path("/tmp/allowed").into()]),
                                    write: None,
                                    glob_scan_max_depth: None,
                                    entries: None,
                                },
                            ),
                        },
                    ),
                    proposed_execpolicy_amendment: None,
                    proposed_network_policy_amendments: None,
                    available_decisions: None,
                },
            }),
            write_complete_tx: None,
        },
    )
    .await;

    let message = writer_rx
        .recv()
        .await
        .expect("request should be delivered to the connection");
    let json = serde_json::to_value(message.message).expect("request should serialize");
    assert_eq!(json["params"].get("additionalPermissions"), None);
}

#[tokio::test]
async fn sequenced_command_execution_request_approval_strips_experimental_fields_without_capability()
 {
    let connection_id = ConnectionId(10);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: OutgoingMessage::SequencedRequest(ServerRequestEnvelope {
                request: ServerRequest::CommandExecutionRequestApproval {
                    request_id: RequestId::Integer(1),
                    params: codex_app_server_protocol::CommandExecutionRequestApprovalParams {
                        thread_id: "thr_123".to_string(),
                        turn_id: "turn_123".to_string(),
                        item_id: "call_123".to_string(),
                        started_at_ms: 0,
                        approval_id: None,
                        environment_id: None,
                        reason: Some("Need extra read access".to_string()),
                        network_approval_context: None,
                        command: Some("cat file".to_string()),
                        cwd: Some(absolute_path("/tmp").into()),
                        command_actions: None,
                        additional_permissions: Some(
                            codex_app_server_protocol::AdditionalPermissionProfile {
                                network: None,
                                file_system: Some(
                                    codex_app_server_protocol::AdditionalFileSystemPermissions {
                                        read: Some(vec![absolute_path("/tmp/allowed").into()]),
                                        write: None,
                                        glob_scan_max_depth: None,
                                        entries: None,
                                    },
                                ),
                            },
                        ),
                        proposed_execpolicy_amendment: None,
                        proposed_network_policy_amendments: None,
                        available_decisions: None,
                    },
                },
                thread_sequence: Some(7),
            }),
            write_complete_tx: None,
        },
    )
    .await;

    let message = writer_rx
        .recv()
        .await
        .expect("request should be delivered to the connection");
    let json = serde_json::to_value(message.message).expect("request should serialize");
    assert_eq!(json["threadSequence"], 7);
    assert_eq!(json["params"].get("additionalPermissions"), None);
}

#[tokio::test]
async fn command_execution_request_approval_keeps_additional_permissions_with_capability() {
    let connection_id = ConnectionId(9);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: OutgoingMessage::Request(ServerRequest::CommandExecutionRequestApproval {
                request_id: RequestId::Integer(1),
                params: codex_app_server_protocol::CommandExecutionRequestApprovalParams {
                    thread_id: "thr_123".to_string(),
                    turn_id: "turn_123".to_string(),
                    item_id: "call_123".to_string(),
                    started_at_ms: 0,
                    approval_id: None,
                    environment_id: None,
                    reason: Some("Need extra read access".to_string()),
                    network_approval_context: None,
                    command: Some("cat file".to_string()),
                    cwd: Some(absolute_path("/tmp").into()),
                    command_actions: None,
                    additional_permissions: Some(
                        codex_app_server_protocol::AdditionalPermissionProfile {
                            network: None,
                            file_system: Some(
                                codex_app_server_protocol::AdditionalFileSystemPermissions {
                                    read: Some(vec![absolute_path("/tmp/allowed").into()]),
                                    write: None,
                                    glob_scan_max_depth: None,
                                    entries: None,
                                },
                            ),
                        },
                    ),
                    proposed_execpolicy_amendment: None,
                    proposed_network_policy_amendments: None,
                    available_decisions: None,
                },
            }),
            write_complete_tx: None,
        },
    )
    .await;

    let message = writer_rx
        .recv()
        .await
        .expect("request should be delivered to the connection");
    let json = serde_json::to_value(message.message).expect("request should serialize");
    let allowed_path = absolute_path("/tmp/allowed").to_string_lossy().into_owned();
    assert_eq!(
        json["params"]["additionalPermissions"],
        json!({
            "network": null,
            "fileSystem": {
                "read": [allowed_path],
            "write": null,
            },
        })
    );
}

#[tokio::test]
async fn broadcast_does_not_block_on_slow_connection() {
    let fast_connection_id = ConnectionId(1);
    let slow_connection_id = ConnectionId(2);

    let (fast_writer_tx, mut fast_writer_rx) = mpsc::channel(1);
    let (slow_writer_tx, mut slow_writer_rx) = mpsc::channel(1);
    let fast_disconnect_token = CancellationToken::new();
    let slow_disconnect_token = CancellationToken::new();

    let mut connections = HashMap::new();
    connections.insert(
        fast_connection_id,
        OutboundConnectionState::new(
            fast_writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            Some(fast_disconnect_token.clone()),
        ),
    );
    connections.insert(
        slow_connection_id,
        OutboundConnectionState::new(
            slow_writer_tx.clone(),
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            Some(slow_disconnect_token.clone()),
        ),
    );

    let queued_message = app_server_notification(ServerNotification::ConfigWarning(
        ConfigWarningNotification {
            summary: "already-buffered".to_string(),
            details: None,
            path: None,
            range: None,
        },
    ));
    slow_writer_tx
        .try_send(QueuedOutgoingMessage::new(queued_message))
        .expect("channel should have room");

    let broadcast_message = app_server_notification(ServerNotification::ConfigWarning(
        ConfigWarningNotification {
            summary: "test".to_string(),
            details: None,
            path: None,
            range: None,
        },
    ));
    timeout(
        Duration::from_millis(100),
        route_outgoing_envelope(
            &mut connections,
            OutgoingEnvelope::Broadcast {
                message: broadcast_message,
            },
        ),
    )
    .await
    .expect("broadcast should return even when one connection is slow");
    assert!(!connections.contains_key(&slow_connection_id));
    assert!(slow_disconnect_token.is_cancelled());
    assert!(!fast_disconnect_token.is_cancelled());
    let fast_message = fast_writer_rx
        .try_recv()
        .expect("fast connection should receive the broadcast notification");
    assert!(matches!(
        fast_message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "test"
    ));

    let slow_message = slow_writer_rx
        .try_recv()
        .expect("slow connection should retain its original buffered message");
    assert!(matches!(
        slow_message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "already-buffered"
    ));
}

#[tokio::test]
async fn broadcast_skips_external_controller_connections() {
    let primary_connection_id = ConnectionId(31);
    let external_connection_id = ConnectionId(32);
    let (primary_writer_tx, mut primary_writer_rx) = mpsc::channel(1);
    let (external_writer_tx, mut external_writer_rx) = mpsc::channel(1);

    let mut connections = HashMap::new();
    connections.insert(
        primary_connection_id,
        OutboundConnectionState::new_with_origin(
            ConnectionOrigin::WebSocket,
            primary_writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );
    connections.insert(
        external_connection_id,
        OutboundConnectionState::new_with_origin(
            ConnectionOrigin::ExternalController,
            external_writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::Broadcast {
            message: app_server_notification(ServerNotification::ConfigWarning(
                ConfigWarningNotification {
                    summary: "broadcast".to_string(),
                    details: None,
                    path: None,
                    range: None,
                },
            )),
        },
    )
    .await;

    let primary_message = primary_writer_rx
        .recv()
        .await
        .expect("primary connection should receive broadcast notification");
    assert!(matches!(
        primary_message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "broadcast"
    ));
    assert!(
        external_writer_rx.try_recv().is_err(),
        "external controller should not receive generic broadcasts"
    );
}

#[tokio::test]
async fn targeted_messages_reach_external_controller_connections() {
    let external_connection_id = ConnectionId(33);
    let (external_writer_tx, mut external_writer_rx) = mpsc::channel(1);

    let mut connections = HashMap::new();
    connections.insert(
        external_connection_id,
        OutboundConnectionState::new_with_origin(
            ConnectionOrigin::ExternalController,
            external_writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id: external_connection_id,
            message: app_server_notification(ServerNotification::ConfigWarning(
                ConfigWarningNotification {
                    summary: "targeted".to_string(),
                    details: None,
                    path: None,
                    range: None,
                },
            )),
            write_complete_tx: None,
        },
    )
    .await;

    let external_message = external_writer_rx
        .recv()
        .await
        .expect("targeted notification should reach external controller");
    assert!(matches!(
        external_message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "targeted"
    ));
}

#[tokio::test]
async fn external_controller_queue_overflow_disconnects_only_external_connection() {
    let primary_connection_id = ConnectionId(34);
    let external_connection_id = ConnectionId(35);
    let (primary_writer_tx, mut primary_writer_rx) = mpsc::channel(1);
    let (external_writer_tx, mut external_writer_rx) = mpsc::channel(1);
    let primary_disconnect_token = CancellationToken::new();
    let external_disconnect_token = CancellationToken::new();

    external_writer_tx
        .try_send(QueuedOutgoingMessage::new(app_server_notification(
            ServerNotification::ConfigWarning(ConfigWarningNotification {
                summary: "already-buffered".to_string(),
                details: None,
                path: None,
                range: None,
            }),
        )))
        .expect("external writer should accept its initial buffered message");

    let mut connections = HashMap::new();
    connections.insert(
        primary_connection_id,
        OutboundConnectionState::new_with_origin(
            ConnectionOrigin::WebSocket,
            primary_writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            Some(primary_disconnect_token.clone()),
        ),
    );
    connections.insert(
        external_connection_id,
        OutboundConnectionState::new_with_origin(
            ConnectionOrigin::ExternalController,
            external_writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            Some(external_disconnect_token.clone()),
        ),
    );

    timeout(
        Duration::from_millis(100),
        route_outgoing_envelope(
            &mut connections,
            OutgoingEnvelope::ToConnection {
                connection_id: external_connection_id,
                message: app_server_notification(ServerNotification::ConfigWarning(
                    ConfigWarningNotification {
                        summary: "external-targeted".to_string(),
                        details: None,
                        path: None,
                        range: None,
                    },
                )),
                write_complete_tx: None,
            },
        ),
    )
    .await
    .expect("external controller overflow should not block routing");

    assert!(!connections.contains_key(&external_connection_id));
    assert!(external_disconnect_token.is_cancelled());
    assert!(connections.contains_key(&primary_connection_id));
    assert!(!primary_disconnect_token.is_cancelled());

    route_outgoing_envelope(
        &mut connections,
        OutgoingEnvelope::ToConnection {
            connection_id: primary_connection_id,
            message: app_server_notification(ServerNotification::ConfigWarning(
                ConfigWarningNotification {
                    summary: "primary-targeted".to_string(),
                    details: None,
                    path: None,
                    range: None,
                },
            )),
            write_complete_tx: None,
        },
    )
    .await;

    let primary_message = primary_writer_rx
        .recv()
        .await
        .expect("primary connection should receive subsequent egress");
    assert!(matches!(
        primary_message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "primary-targeted"
    ));

    let retained_external_message = external_writer_rx
        .try_recv()
        .expect("external connection should retain only its initial buffered message");
    assert!(matches!(
        retained_external_message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "already-buffered"
    ));
    assert!(external_writer_rx.try_recv().is_err());
}

#[tokio::test]
async fn to_connection_disconnects_slow_socket_connection_without_waiting() {
    let connection_id = ConnectionId(14);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);
    let disconnect_token = CancellationToken::new();

    let queued_message = app_server_notification(ServerNotification::ConfigWarning(
        ConfigWarningNotification {
            summary: "already-buffered".to_string(),
            details: None,
            path: None,
            range: None,
        },
    ));
    writer_tx
        .try_send(QueuedOutgoingMessage::new(queued_message))
        .expect("channel should have room for initial message");

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            Some(disconnect_token.clone()),
        ),
    );

    timeout(
        Duration::from_millis(100),
        route_outgoing_envelope(
            &mut connections,
            OutgoingEnvelope::ToConnection {
                connection_id,
                message: app_server_notification(ServerNotification::ConfigWarning(
                    ConfigWarningNotification {
                        summary: "second".to_string(),
                        details: None,
                        path: None,
                        range: None,
                    },
                )),
                write_complete_tx: None,
            },
        ),
    )
    .await
    .expect("routing should not wait for a full disconnectable writer queue");

    assert!(!connections.contains_key(&connection_id));
    assert!(disconnect_token.is_cancelled());
    let retained_message = writer_rx
        .try_recv()
        .expect("slow connection should retain only its original buffered message");
    assert!(matches!(
        retained_message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "already-buffered"
    ));
    assert!(writer_rx.try_recv().is_err());
}

#[tokio::test]
async fn to_connection_then_disconnect_waits_for_final_write() {
    let connection_id = ConnectionId(15);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);
    let disconnect_token = CancellationToken::new();

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            Some(disconnect_token.clone()),
        ),
    );

    let route_task = tokio::spawn(async move {
        route_outgoing_envelope(
            &mut connections,
            OutgoingEnvelope::ToConnectionThenDisconnect {
                connection_id,
                message: app_server_notification(ServerNotification::ConfigWarning(
                    ConfigWarningNotification {
                        summary: "final".to_string(),
                        details: None,
                        path: None,
                        range: None,
                    },
                )),
            },
        )
        .await;
        connections.contains_key(&connection_id)
    });

    let queued_message = timeout(Duration::from_secs(1), writer_rx.recv())
        .await
        .expect("final message should be queued before disconnect")
        .expect("final message should exist");
    assert!(!disconnect_token.is_cancelled());
    assert!(matches!(
        queued_message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "final"
    ));
    queued_message
        .write_complete_tx
        .expect("final message should track write completion")
        .complete();

    let still_connected = timeout(Duration::from_secs(1), route_task)
        .await
        .expect("routing should finish after write completion")
        .expect("routing task should succeed");
    assert!(!still_connected);
    assert!(disconnect_token.is_cancelled());
}

#[tokio::test]
async fn to_connection_then_disconnect_waits_for_slow_queue_space() {
    let connection_id = ConnectionId(16);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);
    let disconnect_token = CancellationToken::new();

    writer_tx
        .send(QueuedOutgoingMessage::new(app_server_notification(
            ServerNotification::ConfigWarning(ConfigWarningNotification {
                summary: "already-buffered".to_string(),
                details: None,
                path: None,
                range: None,
            }),
        )))
        .await
        .expect("writer queue should accept the initial message");

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            Some(disconnect_token.clone()),
        ),
    );

    let route_task = tokio::spawn(async move {
        route_outgoing_envelope(
            &mut connections,
            OutgoingEnvelope::ToConnectionThenDisconnect {
                connection_id,
                message: app_server_notification(ServerNotification::ConfigWarning(
                    ConfigWarningNotification {
                        summary: "final".to_string(),
                        details: None,
                        path: None,
                        range: None,
                    },
                )),
            },
        )
        .await;
        connections.contains_key(&connection_id)
    });

    assert!(
        timeout(Duration::from_millis(100), async {
            loop {
                if disconnect_token.is_cancelled() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err(),
        "connection should stay open while waiting to queue the final message"
    );

    let buffered = writer_rx
        .recv()
        .await
        .expect("initial buffered message should be readable");
    assert!(matches!(
        buffered.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "already-buffered"
    ));

    let final_message = timeout(Duration::from_secs(1), writer_rx.recv())
        .await
        .expect("final message should be queued after space is available")
        .expect("final message should exist");
    assert!(!disconnect_token.is_cancelled());
    assert!(matches!(
        final_message.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "final"
    ));
    final_message
        .write_complete_tx
        .expect("final message should track write completion")
        .complete();

    let still_connected = timeout(Duration::from_secs(1), route_task)
        .await
        .expect("routing should finish after write completion")
        .expect("routing task should succeed");
    assert!(!still_connected);
    assert!(disconnect_token.is_cancelled());
}

#[tokio::test]
async fn to_connection_stdio_waits_instead_of_disconnecting_when_writer_queue_is_full() {
    let connection_id = ConnectionId(3);
    let (writer_tx, mut writer_rx) = mpsc::channel(1);
    writer_tx
        .send(QueuedOutgoingMessage::new(app_server_notification(
            ServerNotification::ConfigWarning(ConfigWarningNotification {
                summary: "queued".to_string(),
                details: None,
                path: None,
                range: None,
            }),
        )))
        .await
        .expect("channel should accept the first queued message");

    let mut connections = HashMap::new();
    connections.insert(
        connection_id,
        OutboundConnectionState::new(
            writer_tx,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            Arc::new(RwLock::new(HashSet::new())),
            /*disconnect_sender*/ None,
        ),
    );

    let route_task = tokio::spawn(async move {
        route_outgoing_envelope(
            &mut connections,
            OutgoingEnvelope::ToConnection {
                connection_id,
                message: app_server_notification(ServerNotification::ConfigWarning(
                    ConfigWarningNotification {
                        summary: "second".to_string(),
                        details: None,
                        path: None,
                        range: None,
                    },
                )),
                write_complete_tx: None,
            },
        )
        .await
    });

    let first = timeout(Duration::from_millis(100), writer_rx.recv())
        .await
        .expect("first queued message should be readable")
        .expect("first queued message should exist");
    timeout(Duration::from_millis(100), route_task)
        .await
        .expect("routing should finish after the first queued message is drained")
        .expect("routing task should succeed");

    assert!(matches!(
        first.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "queued"
    ));
    let second = writer_rx
        .try_recv()
        .expect("second notification should be delivered once the queue has room");
    assert!(matches!(
        second.message,
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification: ServerNotification::ConfigWarning(ConfigWarningNotification { summary, .. }),
            ..
        }) if summary == "second"
    ));
}
