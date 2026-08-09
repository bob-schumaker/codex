use super::*;
use codex_app_server_protocol::WarningNotification;

pub(super) async fn controller_aware_thread_outgoing(
    conversation_id: ThreadId,
    thread_state_manager: &ThreadStateManager,
    controller_processor: &ControllerRequestProcessor,
    outgoing: &Arc<OutgoingMessageSender>,
) -> ThreadScopedOutgoingMessageSender {
    let subscribed_connection_ids = thread_state_manager
        .subscribed_connection_ids(conversation_id)
        .await;
    let request_recipients = controller_processor
        .prompt_request_recipients(conversation_id, subscribed_connection_ids.clone());
    let notification_connection_ids = controller_processor
        .thread_notification_recipients(conversation_id, subscribed_connection_ids.clone());
    let external_notification_connection_ids = controller_processor
        .external_controller_thread_notification_recipients(
            conversation_id,
            subscribed_connection_ids,
        );
    ThreadScopedOutgoingMessageSender::new_with_controller_recipients(
        Arc::clone(outgoing),
        request_recipients,
        notification_connection_ids,
        external_notification_connection_ids,
        conversation_id,
    )
}

pub(super) async fn send_thread_goal_updated_notification(
    outgoing: &ThreadScopedOutgoingMessageSender,
    thread_id: ThreadId,
    turn_id: Option<String>,
    goal: ThreadGoal,
) {
    outgoing
        .send_global_server_notification(ServerNotification::ThreadGoalUpdated(
            ThreadGoalUpdatedNotification {
                thread_id: thread_id.to_string(),
                turn_id,
                goal,
            },
        ))
        .await;
}

pub(super) async fn send_thread_goal_cleared_notification(
    outgoing: &ThreadScopedOutgoingMessageSender,
    thread_id: ThreadId,
) {
    outgoing
        .send_global_server_notification(ServerNotification::ThreadGoalCleared(
            ThreadGoalClearedNotification {
                thread_id: thread_id.to_string(),
            },
        ))
        .await;
}

pub(super) async fn send_thread_warning_notification(
    outgoing: &ThreadScopedOutgoingMessageSender,
    thread_id: ThreadId,
    message: String,
) {
    outgoing
        .send_server_notification(ServerNotification::Warning(WarningNotification {
            thread_id: Some(thread_id.to_string()),
            message,
        }))
        .await;
}

pub(super) async fn send_thread_goal_snapshot_notification_to_thread(
    outgoing: &ThreadScopedOutgoingMessageSender,
    thread_id: ThreadId,
    state_db: &StateDbHandle,
) {
    match state_db.thread_goals().get_thread_goal(thread_id).await {
        Ok(Some(goal)) => {
            send_thread_goal_updated_notification(
                outgoing,
                thread_id,
                None,
                api_thread_goal_from_state(goal),
            )
            .await;
        }
        Ok(None) => {
            send_thread_goal_cleared_notification(outgoing, thread_id).await;
        }
        Err(err) => {
            tracing::warn!(
                thread_id = %thread_id,
                "failed to read thread goal for resume snapshot: {err}"
            );
        }
    }
}

#[cfg(test)]
#[path = "thread_lifecycle_controller_egress_tests.rs"]
mod tests;
