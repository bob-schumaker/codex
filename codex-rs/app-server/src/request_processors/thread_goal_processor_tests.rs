use std::sync::Arc;
use std::time::Duration;

use codex_analytics::AnalyticsEventsClient;
use codex_app_server_protocol::ControllerParticipationStatus;
use codex_app_server_protocol::ControllerRequestParticipationParams;
use codex_app_server_protocol::ThreadGoalStatus;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;

use super::*;
use crate::controller_enrollment::ControllerEnrollmentPolicy;
use crate::controller_enrollment::EmptyControllerEnrollmentSource;
use crate::controller_native_approval::NativeControllerParticipationDecision;
use crate::controller_session::ControllerSessionClock;
use crate::controller_session::ControllerSessionConfig;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::thread_state::ConnectionCapabilities;
use crate::transport::ConnectionOrigin;

#[tokio::test]
async fn thread_goal_update_fallback_targets_external_controller_recipients() {
    let ThreadGoalFallbackHarness {
        outgoing,
        mut outgoing_rx,
        thread_state_manager,
        controller_processor,
        thread_id,
        controller_connection_id,
    } = thread_goal_fallback_harness().await;
    let goal = thread_goal(thread_id);

    emit_thread_goal_updated_fallback(
        &outgoing,
        &thread_state_manager,
        &controller_processor,
        thread_id,
        goal.clone(),
    )
    .await;

    assert_eq!(
        recv_broadcast_goal_update(&mut outgoing_rx).await,
        ThreadGoalUpdatedNotification {
            thread_id: thread_id.to_string(),
            turn_id: None,
            goal: goal.clone(),
        },
    );
    let (connection_id, notification) = recv_targeted_goal_update(&mut outgoing_rx).await;
    assert_eq!(connection_id, controller_connection_id);
    assert_eq!(
        notification,
        ThreadGoalUpdatedNotification {
            thread_id: thread_id.to_string(),
            turn_id: None,
            goal,
        },
    );
    assert!(outgoing_rx.try_recv().is_err());
}

#[tokio::test]
async fn thread_goal_clear_fallback_targets_external_controller_recipients() {
    let ThreadGoalFallbackHarness {
        outgoing,
        mut outgoing_rx,
        thread_state_manager,
        controller_processor,
        thread_id,
        controller_connection_id,
    } = thread_goal_fallback_harness().await;

    emit_thread_goal_cleared_fallback(
        &outgoing,
        &thread_state_manager,
        &controller_processor,
        thread_id,
    )
    .await;

    assert_eq!(
        recv_broadcast_goal_clear(&mut outgoing_rx).await,
        ThreadGoalClearedNotification {
            thread_id: thread_id.to_string(),
        },
    );
    let (connection_id, notification) = recv_targeted_goal_clear(&mut outgoing_rx).await;
    assert_eq!(connection_id, controller_connection_id);
    assert_eq!(
        notification,
        ThreadGoalClearedNotification {
            thread_id: thread_id.to_string(),
        },
    );
    assert!(outgoing_rx.try_recv().is_err());
}

struct ThreadGoalFallbackHarness {
    outgoing: Arc<OutgoingMessageSender>,
    outgoing_rx: mpsc::Receiver<OutgoingEnvelope>,
    thread_state_manager: ThreadStateManager,
    controller_processor: ControllerRequestProcessor,
    thread_id: ThreadId,
    controller_connection_id: ConnectionId,
}

async fn thread_goal_fallback_harness() -> ThreadGoalFallbackHarness {
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel(/*buffer*/ 8);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let controller_processor = ControllerRequestProcessor::new(
        Arc::clone(&outgoing),
        Arc::new(EmptyControllerEnrollmentSource),
        Some(Arc::new(|_request| {
            Box::pin(async { NativeControllerParticipationDecision::Approved })
        })),
        None,
        ControllerEnrollmentPolicy::BestEffort,
        ControllerSessionClock::from_fn(std::time::Instant::now),
        ControllerSessionConfig {
            lease_duration: Duration::from_secs(/*secs*/ 300),
        },
    );
    let thread_state_manager = ThreadStateManager::new();
    let tui_connection_id = ConnectionId(1);
    let controller_connection_id = ConnectionId(2);
    let unrelated_connection_id = ConnectionId(3);
    let thread_id = ThreadId::new();
    controller_processor.register_main_thread(thread_id, tui_connection_id);

    for connection_id in [
        tui_connection_id,
        controller_connection_id,
        unrelated_connection_id,
    ] {
        thread_state_manager
            .connection_initialized(connection_id, ConnectionCapabilities::default())
            .await;
        thread_state_manager
            .try_ensure_connection_subscribed(
                thread_id,
                connection_id,
                /*experimental_raw_events*/ false,
            )
            .await
            .expect("connection should subscribe to the test thread");
    }

    let participation = controller_processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect("native participation approval should create a controller session");
    assert_eq!(
        participation.status,
        ControllerParticipationStatus::Approved
    );
    while outgoing_rx.try_recv().is_ok() {}

    ThreadGoalFallbackHarness {
        outgoing,
        outgoing_rx,
        thread_state_manager,
        controller_processor,
        thread_id,
        controller_connection_id,
    }
}

fn participation_params() -> ControllerRequestParticipationParams {
    ControllerRequestParticipationParams {
        controller_name: "codex-waveshare".to_string(),
        description: "external test controller".to_string(),
    }
}

fn thread_goal(thread_id: ThreadId) -> ThreadGoal {
    ThreadGoal {
        thread_id: thread_id.to_string(),
        objective: "ship controller support".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 2,
    }
}

async fn recv_broadcast_goal_update(
    outgoing_rx: &mut mpsc::Receiver<OutgoingEnvelope>,
) -> ThreadGoalUpdatedNotification {
    let OutgoingEnvelope::Broadcast { message } = outgoing_rx
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
    outgoing_rx: &mut mpsc::Receiver<OutgoingEnvelope>,
) -> (ConnectionId, ThreadGoalUpdatedNotification) {
    let OutgoingEnvelope::ToConnection {
        connection_id,
        message,
        write_complete_tx: None,
    } = outgoing_rx
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

async fn recv_broadcast_goal_clear(
    outgoing_rx: &mut mpsc::Receiver<OutgoingEnvelope>,
) -> ThreadGoalClearedNotification {
    let OutgoingEnvelope::Broadcast { message } = outgoing_rx
        .recv()
        .await
        .expect("broadcast goal clear should be sent")
    else {
        panic!("expected broadcast notification");
    };
    let OutgoingMessage::AppServerNotification(envelope) = message else {
        panic!("expected app-server notification");
    };
    let ServerNotification::ThreadGoalCleared(notification) = envelope.notification else {
        panic!("expected thread goal clear");
    };
    notification
}

async fn recv_targeted_goal_clear(
    outgoing_rx: &mut mpsc::Receiver<OutgoingEnvelope>,
) -> (ConnectionId, ThreadGoalClearedNotification) {
    let OutgoingEnvelope::ToConnection {
        connection_id,
        message,
        write_complete_tx: None,
    } = outgoing_rx
        .recv()
        .await
        .expect("targeted external controller goal clear should be sent")
    else {
        panic!("expected targeted notification");
    };
    let OutgoingMessage::AppServerNotification(envelope) = message else {
        panic!("expected app-server notification");
    };
    let ServerNotification::ThreadGoalCleared(notification) = envelope.notification else {
        panic!("expected thread goal clear");
    };
    (connection_id, notification)
}
