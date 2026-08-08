use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use codex_analytics::AnalyticsEventsClient;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::ControllerLaunchState;
use codex_app_server_protocol::ControllerRequestParticipationParams;
use codex_app_server_protocol::ControllerRetryDisposition;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;

use super::*;
use crate::controller_enrollment::EmptyControllerEnrollmentSource;
use crate::controller_native_approval::NativeControllerParticipationDecision;
use crate::outgoing_message::OutgoingMessageSender;

#[tokio::test]
async fn native_tui_unavailable_marks_controller_launch_terminal() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*buffer*/ 4);
    let approval_calls = Arc::new(AtomicUsize::new(0));
    let approval_calls_for_closure = Arc::clone(&approval_calls);
    let processor = ControllerRequestProcessor::new(
        Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            AnalyticsEventsClient::disabled(),
        )),
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
}

fn participation_params() -> ControllerRequestParticipationParams {
    ControllerRequestParticipationParams {
        controller_name: "codex-waveshare".to_string(),
        description: "external test controller".to_string(),
    }
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
