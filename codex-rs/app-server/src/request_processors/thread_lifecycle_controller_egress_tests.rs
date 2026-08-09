use super::*;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::ServerRequestRecipients;
use codex_app_server_protocol::ThreadGoalStatus;
use codex_app_server_protocol::WarningNotification;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;

#[tokio::test]
async fn listener_goal_update_targets_external_controller_recipients() {
    let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        tx,
        codex_analytics::AnalyticsEventsClient::disabled(),
    ));
    let thread_id = ThreadId::new();
    let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_controller_recipients(
        outgoing,
        ServerRequestRecipients::normal(vec![ConnectionId(1), ConnectionId(2)]),
        vec![ConnectionId(1), ConnectionId(2)],
        vec![ConnectionId(2)],
        thread_id,
    );
    let goal = ThreadGoal {
        thread_id: thread_id.to_string(),
        objective: "ship controller support".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 2,
    };

    send_thread_goal_updated_notification(
        &thread_outgoing,
        thread_id,
        Some("turn-1".to_string()),
        goal.clone(),
    )
    .await;

    assert_eq!(
        recv_broadcast_goal_update(&mut rx).await,
        ThreadGoalUpdatedNotification {
            thread_id: thread_id.to_string(),
            turn_id: Some("turn-1".to_string()),
            goal: goal.clone(),
        },
    );
    let (connection_id, notification) = recv_targeted_goal_update(&mut rx).await;
    assert_eq!(connection_id, ConnectionId(2));
    assert_eq!(
        notification,
        ThreadGoalUpdatedNotification {
            thread_id: thread_id.to_string(),
            turn_id: Some("turn-1".to_string()),
            goal,
        },
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn listener_warning_targets_thread_notification_recipients() {
    let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        tx,
        codex_analytics::AnalyticsEventsClient::disabled(),
    ));
    let thread_id = ThreadId::new();
    let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_controller_recipients(
        outgoing,
        ServerRequestRecipients::normal(vec![ConnectionId(1), ConnectionId(2)]),
        vec![ConnectionId(1), ConnectionId(2)],
        vec![ConnectionId(2)],
        thread_id,
    );

    send_thread_warning_notification(&thread_outgoing, thread_id, "extension warning".to_string())
        .await;

    assert_eq!(
        recv_targeted_warning(&mut rx).await,
        (
            ConnectionId(1),
            WarningNotification {
                thread_id: Some(thread_id.to_string()),
                message: "extension warning".to_string(),
            },
        ),
    );
    assert_eq!(
        recv_targeted_warning(&mut rx).await,
        (
            ConnectionId(2),
            WarningNotification {
                thread_id: Some(thread_id.to_string()),
                message: "extension warning".to_string(),
            },
        ),
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn listener_server_request_resolved_targets_thread_notification_recipients() {
    let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        tx,
        codex_analytics::AnalyticsEventsClient::disabled(),
    ));
    let thread_id = ThreadId::new();
    let request_id = RequestId::Integer(42);
    let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_controller_recipients(
        outgoing,
        ServerRequestRecipients::normal(vec![ConnectionId(1), ConnectionId(2)]),
        vec![ConnectionId(1), ConnectionId(2)],
        vec![ConnectionId(2)],
        thread_id,
    );

    send_server_request_resolved_notification(&thread_outgoing, thread_id, request_id.clone())
        .await;

    assert_eq!(
        recv_targeted_server_request_resolved(&mut rx).await,
        (
            ConnectionId(1),
            ServerRequestResolvedNotification {
                thread_id: thread_id.to_string(),
                request_id: request_id.clone(),
            },
        ),
    );
    assert_eq!(
        recv_targeted_server_request_resolved(&mut rx).await,
        (
            ConnectionId(2),
            ServerRequestResolvedNotification {
                thread_id: thread_id.to_string(),
                request_id,
            },
        ),
    );
    assert!(rx.try_recv().is_err());
}

async fn recv_broadcast_goal_update(
    rx: &mut mpsc::Receiver<OutgoingEnvelope>,
) -> ThreadGoalUpdatedNotification {
    let OutgoingEnvelope::Broadcast { message } = rx
        .recv()
        .await
        .expect("broadcast goal update should be sent")
    else {
        panic!("expected broadcast notification");
    };
    let OutgoingMessage::AppServerNotification(envelope) = message else {
        panic!("expected app-server notification");
    };
    let ServerNotification::ThreadGoalUpdated(notification) = envelope.notification else {
        panic!("expected thread goal update");
    };
    notification
}

async fn recv_targeted_goal_update(
    rx: &mut mpsc::Receiver<OutgoingEnvelope>,
) -> (ConnectionId, ThreadGoalUpdatedNotification) {
    let OutgoingEnvelope::ToConnection {
        connection_id,
        message,
        write_complete_tx: None,
    } = rx
        .recv()
        .await
        .expect("targeted external controller goal update should be sent")
    else {
        panic!("expected targeted notification");
    };
    let OutgoingMessage::AppServerNotification(envelope) = message else {
        panic!("expected app-server notification");
    };
    let ServerNotification::ThreadGoalUpdated(notification) = envelope.notification else {
        panic!("expected thread goal update");
    };
    (connection_id, notification)
}

async fn recv_targeted_warning(
    rx: &mut mpsc::Receiver<OutgoingEnvelope>,
) -> (ConnectionId, WarningNotification) {
    let OutgoingEnvelope::ToConnection {
        connection_id,
        message,
        write_complete_tx: None,
    } = rx.recv().await.expect("targeted warning should be sent")
    else {
        panic!("expected targeted notification");
    };
    let OutgoingMessage::AppServerNotification(envelope) = message else {
        panic!("expected app-server notification");
    };
    let ServerNotification::Warning(notification) = envelope.notification else {
        panic!("expected warning notification");
    };
    (connection_id, notification)
}

async fn recv_targeted_server_request_resolved(
    rx: &mut mpsc::Receiver<OutgoingEnvelope>,
) -> (ConnectionId, ServerRequestResolvedNotification) {
    let OutgoingEnvelope::ToConnection {
        connection_id,
        message,
        write_complete_tx: None,
    } = rx
        .recv()
        .await
        .expect("targeted server-request resolution should be sent")
    else {
        panic!("expected targeted notification");
    };
    let OutgoingMessage::AppServerNotification(envelope) = message else {
        panic!("expected app-server notification");
    };
    let ServerNotification::ServerRequestResolved(notification) = envelope.notification else {
        panic!("expected server request resolved notification");
    };
    (connection_id, notification)
}
