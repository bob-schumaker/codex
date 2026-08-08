use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use codex_analytics::AnalyticsEventsClient;
use codex_app_server_protocol::CommandExecutionRequestApprovalParams;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::ControllerLaunchState;
use codex_app_server_protocol::ControllerRequestParticipationParams;
use codex_app_server_protocol::ControllerRetryDisposition;
use codex_app_server_protocol::ServerRequestPayload;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;

use super::*;
use crate::controller_enrollment::EmptyControllerEnrollmentSource;
use crate::controller_native_approval::NativeControllerParticipationDecision;
use crate::outgoing_message::OutgoingMessageSender;

#[tokio::test]
async fn native_tui_unavailable_marks_controller_launch_terminal() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*buffer*/ 4);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let approval_calls = Arc::new(AtomicUsize::new(0));
    let approval_calls_for_closure = Arc::clone(&approval_calls);
    let processor = ControllerRequestProcessor::new(
        Arc::clone(&outgoing),
        Arc::new(EmptyControllerEnrollmentSource),
        Some(Arc::new(move |_request| {
            approval_calls_for_closure.fetch_add(/*val*/ 1, Ordering::Relaxed);
            Box::pin(async {
                NativeControllerParticipationDecision::TuiUnavailable {
                    reason: "owning TUI is gone".to_string(),
                }
            })
        })),
        ControllerEnrollmentPolicy::BestEffort,
        ControllerSessionClock::from_fn(std::time::Instant::now),
        ControllerSessionConfig {
            lease_duration: Duration::from_secs(/*secs*/ 300),
        },
    );
    let tui_connection_id = ConnectionId(1);
    let controller_connection_id = ConnectionId(2);
    let main_thread_id = ThreadId::new();
    processor.register_main_thread(main_thread_id, tui_connection_id);
    let (_prompt_request_id, mut wait_for_prompt) = outgoing
        .send_request_to_connections(
            Some(&[controller_connection_id]),
            command_execution_approval_payload(main_thread_id),
            Some(main_thread_id),
        )
        .await;

    let first_error = processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect_err("TUI-unavailable native decision should reject participation");
    assert_controller_error(
        first_error,
        ControllerErrorCode::TuiUnavailable,
        ControllerRetryDisposition::DoNotRetry,
        Some(ControllerLaunchState::TuiUnavailable),
    );
    assert_eq!(approval_calls.load(Ordering::Relaxed), 1);

    let second_error = processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect_err("terminal TUI-unavailable launch should reject later participation");
    assert_controller_error(
        second_error,
        ControllerErrorCode::TuiUnavailable,
        ControllerRetryDisposition::DoNotRetry,
        Some(ControllerLaunchState::TuiUnavailable),
    );
    assert_eq!(
        approval_calls.load(Ordering::Relaxed),
        1,
        "terminal launch should not re-prompt the unavailable TUI"
    );

    let read_result = processor
        .authorize_normal_request(
            controller_connection_id,
            AdmissionRule {
                target: TargetExtraction::ExactThread,
                required_authority: RequiredAuthority::StandingSession,
            },
            ControllerRequestTarget::ExactThread(main_thread_id.to_string()),
        )
        .await;
    let read_error = match read_result {
        Ok(_) => panic!("terminal TUI-unavailable launch should reject normal interface reads"),
        Err(error) => error,
    };
    assert_controller_error(
        read_error,
        ControllerErrorCode::TuiUnavailable,
        ControllerRetryDisposition::DoNotRetry,
        Some(ControllerLaunchState::TuiUnavailable),
    );

    assert_eq!(
        processor.thread_notification_recipients(
            main_thread_id,
            vec![tui_connection_id, controller_connection_id],
        ),
        Vec::<ConnectionId>::new(),
        "terminal TUI-unavailable launch should stop main-thread notifications"
    );
    let prompt_error = tokio::time::timeout(Duration::from_secs(/*secs*/ 1), &mut wait_for_prompt)
        .await
        .expect("pending prompt should fail after terminal TUI-unavailable")
        .expect("pending prompt waiter should receive cancellation")
        .expect_err("pending prompt should receive TUI-unavailable error");
    assert_controller_error(
        prompt_error,
        ControllerErrorCode::TuiUnavailable,
        ControllerRetryDisposition::DoNotRetry,
        Some(ControllerLaunchState::TuiUnavailable),
    );

    let other_thread_id = ThreadId::new();
    assert_eq!(
        processor.thread_notification_recipients(
            other_thread_id,
            vec![tui_connection_id, controller_connection_id],
        ),
        vec![tui_connection_id, controller_connection_id],
        "terminal TUI-unavailable launch should not affect unrelated threads"
    );
}

fn participation_params() -> ControllerRequestParticipationParams {
    ControllerRequestParticipationParams {
        controller_name: "codex-waveshare".to_string(),
        description: "external test controller".to_string(),
    }
}

fn command_execution_approval_payload(thread_id: ThreadId) -> ServerRequestPayload {
    ServerRequestPayload::CommandExecutionRequestApproval(CommandExecutionRequestApprovalParams {
        thread_id: thread_id.to_string(),
        turn_id: "turn-1".to_string(),
        item_id: "item-1".to_string(),
        started_at_ms: 0,
        approval_id: None,
        environment_id: None,
        reason: None,
        network_approval_context: None,
        command: Some("echo hi".to_string()),
        cwd: None,
        command_actions: None,
        additional_permissions: None,
        proposed_execpolicy_amendment: None,
        proposed_network_policy_amendments: None,
        available_decisions: None,
    })
}

fn assert_controller_error(
    error: JSONRPCErrorError,
    code: ControllerErrorCode,
    retry: ControllerRetryDisposition,
    launch_state: Option<ControllerLaunchState>,
) {
    let data: ControllerErrorData = serde_json::from_value(
        error
            .data
            .expect("controller error should include typed data"),
    )
    .expect("controller error data should deserialize");
    assert_eq!(data.code, code);
    assert_eq!(data.retry, retry);
    assert_eq!(data.launch_state, launch_state);
}
