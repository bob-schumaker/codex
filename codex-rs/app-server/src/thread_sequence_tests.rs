use codex_app_server_protocol::ControllerControlOwnershipChangedNotification;
use codex_app_server_protocol::ControllerControlOwnershipChangedReason;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::ThreadStatusChangedNotification;
use codex_app_server_protocol::WarningNotification;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

use super::ThreadSequenceTracker;
use super::notification_thread_id;

#[tokio::test]
async fn tracker_advances_per_thread() {
    let tracker = ThreadSequenceTracker::default();
    let first_thread = ThreadId::new();
    let second_thread = ThreadId::new();

    assert_eq!(tracker.current(first_thread).await, 0);
    assert_eq!(tracker.advance(first_thread).await, 1);
    assert_eq!(tracker.advance(first_thread).await, 2);
    assert_eq!(tracker.advance(second_thread).await, 1);
    assert_eq!(tracker.current(first_thread).await, 2);
    assert_eq!(tracker.current(second_thread).await, 1);
}

#[test]
fn extracts_thread_targets_from_thread_and_controller_notifications() {
    let thread_id = ThreadId::new();
    let thread_notification =
        ServerNotification::ThreadStatusChanged(ThreadStatusChangedNotification {
            thread_id: thread_id.to_string(),
            status: ThreadStatus::Idle,
        });
    let controller_notification = ServerNotification::ControllerControlOwnershipChanged(
        ControllerControlOwnershipChangedNotification {
            session_id: "session-1".to_string(),
            main_thread_id: thread_id.to_string(),
            reason: ControllerControlOwnershipChangedReason::Acquired,
            authorization_epoch: 1,
            owner_epoch: 2,
            session_sequence: 3,
            active_lease: None,
        },
    );

    assert_eq!(
        notification_thread_id(&thread_notification),
        Some(thread_id)
    );
    assert_eq!(
        notification_thread_id(&controller_notification),
        Some(thread_id)
    );
}

#[test]
fn ignores_global_notifications_and_malformed_thread_targets() {
    assert_eq!(
        notification_thread_id(&ServerNotification::Warning(WarningNotification {
            thread_id: None,
            message: "global warning".to_string(),
        })),
        None
    );
    assert_eq!(
        notification_thread_id(&ServerNotification::Warning(WarningNotification {
            thread_id: Some("not-a-thread-id".to_string()),
            message: "bad warning".to_string(),
        })),
        None
    );
}
