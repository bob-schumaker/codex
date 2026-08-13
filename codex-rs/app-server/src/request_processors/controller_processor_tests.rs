use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use codex_analytics::AnalyticsEventsClient;
use codex_app_server_protocol::CommandExecutionRequestApprovalParams;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::ControllerLaunchState;
use codex_app_server_protocol::ControllerParticipationStatus;
use codex_app_server_protocol::ControllerRequestParticipationParams;
use codex_app_server_protocol::ControllerRetryDisposition;
use codex_app_server_protocol::ServerRequestPayload;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::*;
use crate::controller_enrollment::EmptyControllerEnrollmentSource;
use crate::controller_native_approval::NativeControllerParticipationDecision;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;
use crate::outgoing_message::ThreadScopedOutgoingMessageSender;

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
        None,
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

#[tokio::test]
async fn late_native_participation_approval_after_disconnect_does_not_keep_control() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let (first_decision_tx, first_decision_rx) = oneshot::channel();
    let (second_decision_tx, second_decision_rx) = oneshot::channel();
    let decisions = Arc::new(std::sync::Mutex::new(VecDeque::from([
        first_decision_rx,
        second_decision_rx,
    ])));
    let decisions_for_closure = Arc::clone(&decisions);
    let (called_tx, mut called_rx) = mpsc::unbounded_channel();
    let processor = ControllerRequestProcessor::new(
        Arc::clone(&outgoing),
        Arc::new(EmptyControllerEnrollmentSource),
        Some(Arc::new(move |_request| {
            called_tx
                .send(())
                .expect("test call notification receiver should be open");
            let decision_rx = decisions_for_closure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("test should provide a native participation decision");
            Box::pin(async move {
                decision_rx
                    .await
                    .expect("test should send a native participation decision")
            })
        })),
        None,
        ControllerEnrollmentPolicy::BestEffort,
        ControllerSessionClock::from_fn(std::time::Instant::now),
        ControllerSessionConfig {
            lease_duration: Duration::from_secs(/*secs*/ 300),
        },
    );
    let tui_connection_id = ConnectionId(1);
    let disconnected_controller = ConnectionId(2);
    let fresh_controller = ConnectionId(3);
    let main_thread_id = ThreadId::new();
    processor.register_main_thread(main_thread_id, tui_connection_id);

    let processor_for_task = processor.clone();
    let pending_participation = tokio::spawn(async move {
        processor_for_task
            .request_participation(
                disconnected_controller,
                ConnectionOrigin::ExternalController,
                /*credential_proof*/ None,
                participation_params(),
            )
            .await
    });
    called_rx
        .recv()
        .await
        .expect("native participation request should reach the approver");

    assert_eq!(
        processor.connection_closed(disconnected_controller).await,
        None
    );
    first_decision_tx
        .send(NativeControllerParticipationDecision::Approved)
        .expect("late approval receiver should be pending");
    let stale_error = pending_participation
        .await
        .expect("participation task should not panic")
        .expect_err("late approval for a disconnected controller must not create a session");
    assert_controller_error(
        stale_error,
        ControllerErrorCode::TransportClosing,
        ControllerRetryDisposition::DoNotRetry,
        None,
    );

    second_decision_tx
        .send(NativeControllerParticipationDecision::Approved)
        .expect("fresh approval receiver should be pending");
    let response = processor
        .request_participation(
            fresh_controller,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect("fresh controller should be able to participate after stale disconnect");
    assert_eq!(response.status, ControllerParticipationStatus::Approved);
    let acquired = processor
        .acquire_control(fresh_controller, ConnectionOrigin::ExternalController)
        .await
        .expect("fresh controller should be able to acquire control");
    assert_eq!(acquired.session.main_thread_id, main_thread_id.to_string());
}

#[tokio::test]
async fn lifecycle_notification_recipients_include_only_authorized_main_thread_controllers() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let processor = ControllerRequestProcessor::new(
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
    let tui_connection_id = ConnectionId(1);
    let controller_connection_id = ConnectionId(2);
    let unrelated_connection_id = ConnectionId(3);
    let main_thread_id = ThreadId::new();
    let other_thread_id = ThreadId::new();
    processor.register_main_thread(main_thread_id, tui_connection_id);

    let response = processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect("native participation approval should create a controller session");
    assert_eq!(response.status, ControllerParticipationStatus::Approved);

    assert_eq!(
        processor.external_controller_thread_notification_recipients(
            main_thread_id,
            vec![
                tui_connection_id,
                controller_connection_id,
                unrelated_connection_id,
            ],
        ),
        vec![controller_connection_id],
    );
    assert_eq!(
        processor.external_controller_thread_notification_recipients(
            other_thread_id,
            vec![controller_connection_id],
        ),
        Vec::<ConnectionId>::new(),
    );
}

#[tokio::test]
async fn external_prompt_delivery_failure_reclaims_controller_ownership() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let processor = ControllerRequestProcessor::new(
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
    let tui_connection_id = ConnectionId(1);
    let controller_connection_id = ConnectionId(2);
    let main_thread_id = ThreadId::new();
    processor.register_main_thread(main_thread_id, tui_connection_id);
    processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect("native participation approval should create a controller session");
    let thread_outgoing =
        crate::outgoing_message::ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            Arc::clone(&outgoing),
            processor.prompt_request_recipients(
                main_thread_id,
                vec![tui_connection_id, controller_connection_id],
            ),
            vec![tui_connection_id, controller_connection_id],
            main_thread_id,
        );
    let (request_id, _waiter) = thread_outgoing
        .send_request(command_execution_approval_payload(main_thread_id))
        .await;

    processor
        .recover_external_prompt_delivery_failure(
            main_thread_id,
            controller_connection_id,
            &request_id,
        )
        .await;

    let status = processor
        .ownership_status_snapshot(main_thread_id)
        .expect("main thread should retain ownership state");
    assert_eq!(
        status.owner,
        crate::controller_session::ControllerOwnershipStatusOwner::Tui
    );
}

#[tokio::test]
async fn expired_controller_lease_rebinds_pending_prompts_to_tui() {
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let clock_now = Arc::new(std::sync::Mutex::new(Instant::now()));
    let clock_for_processor = Arc::clone(&clock_now);
    let processor = ControllerRequestProcessor::new(
        Arc::clone(&outgoing),
        Arc::new(EmptyControllerEnrollmentSource),
        Some(Arc::new(|_request| {
            Box::pin(async { NativeControllerParticipationDecision::Approved })
        })),
        None,
        ControllerEnrollmentPolicy::BestEffort,
        ControllerSessionClock::from_fn(move || {
            *clock_for_processor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }),
        ControllerSessionConfig {
            lease_duration: Duration::from_millis(/*millis*/ 10),
        },
    );
    let tui_connection_id = ConnectionId(1);
    let controller_connection_id = ConnectionId(2);
    let main_thread_id = ThreadId::new();
    processor.register_main_thread(main_thread_id, tui_connection_id);
    let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
        Arc::clone(&outgoing),
        vec![tui_connection_id],
        main_thread_id,
    );
    let (request_id, waiter) = thread_outgoing
        .send_request(command_execution_approval_payload(main_thread_id))
        .await;
    let _ = outgoing_rx
        .recv()
        .await
        .expect("initial TUI prompt should be sent");

    processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect("controller participation should succeed");
    let write_complete_tx = loop {
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = outgoing_rx
            .recv()
            .await
            .expect("controller prompt transfer should be sent")
        else {
            continue;
        };
        let request_id_matches = match &message {
            OutgoingMessage::Request(request) => request.id() == &request_id,
            OutgoingMessage::SequencedRequest(envelope) => envelope.request.id() == &request_id,
            OutgoingMessage::AppServerNotification(_)
            | OutgoingMessage::Response(_)
            | OutgoingMessage::Error(_) => false,
        };
        if connection_id == controller_connection_id && request_id_matches {
            break write_complete_tx.expect("controller prompt should track its write");
        }
    };
    assert!(write_complete_tx.begin_write());
    write_complete_tx.complete();

    *clock_now
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) +=
        Duration::from_millis(/*millis*/ 11);
    processor.expire_deadlines().await;

    loop {
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = outgoing_rx
            .recv()
            .await
            .expect("expired lease should rebind the prompt to the TUI")
        else {
            continue;
        };
        let request_id_matches = match &message {
            OutgoingMessage::Request(request) => request.id() == &request_id,
            OutgoingMessage::SequencedRequest(envelope) => envelope.request.id() == &request_id,
            OutgoingMessage::AppServerNotification(_)
            | OutgoingMessage::Response(_)
            | OutgoingMessage::Error(_) => false,
        };
        if connection_id == tui_connection_id && request_id_matches {
            break;
        }
    }
    assert_eq!(
        processor
            .ownership_status_snapshot(main_thread_id)
            .expect("main thread should retain ownership state")
            .owner,
        crate::controller_session::ControllerOwnershipStatusOwner::Tui
    );
    assert!(
        !outgoing
            .notify_client_response_from_connection(
                controller_connection_id,
                request_id.clone(),
                serde_json::json!({ "decision": "accept" }),
            )
            .await,
        "expired controller must no longer resolve the prompt"
    );
    assert!(
        outgoing
            .notify_client_response_from_connection(
                tui_connection_id,
                request_id,
                serde_json::json!({ "decision": "accept" }),
            )
            .await,
        "TUI should resolve the prompt after lease expiry"
    );
    waiter
        .await
        .expect("prompt waiter should receive the TUI response")
        .expect("TUI response should be successful");
}

#[tokio::test]
async fn external_prompt_delivery_recovery_waits_for_prompt_transition() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let processor = ControllerRequestProcessor::new(
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
    let tui_connection_id = ConnectionId(1);
    let controller_connection_id = ConnectionId(2);
    let main_thread_id = ThreadId::new();
    processor.register_main_thread(main_thread_id, tui_connection_id);
    processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect("controller participation should succeed");
    processor
        .acquire_control(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
        )
        .await
        .expect("controller control acquisition should succeed");
    let thread_outgoing =
        crate::outgoing_message::ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            Arc::clone(&outgoing),
            processor.prompt_request_recipients(
                main_thread_id,
                vec![tui_connection_id, controller_connection_id],
            ),
            vec![tui_connection_id, controller_connection_id],
            main_thread_id,
        );
    let (request_id, mut waiter) = thread_outgoing
        .send_request(command_execution_approval_payload(main_thread_id))
        .await;

    let transition = outgoing.lock_prompt_transition().await;
    let recovery_processor = processor.clone();
    let recovery_request_id = request_id.clone();
    let recovery = tokio::spawn(async move {
        recovery_processor
            .recover_external_prompt_delivery_failure(
                main_thread_id,
                controller_connection_id,
                &recovery_request_id,
            )
            .await;
    });
    tokio::task::yield_now().await;
    let reply_outgoing = Arc::clone(&outgoing);
    let reply_request_id = request_id.clone();
    let controller_reply = tokio::spawn(async move {
        reply_outgoing
            .notify_client_response_from_connection(
                controller_connection_id,
                reply_request_id,
                serde_json::json!({ "decision": "accept" }),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(!recovery.is_finished());
    assert!(!controller_reply.is_finished());
    drop(transition);
    recovery.await.expect("recovery task should not panic");
    assert!(
        !controller_reply
            .await
            .expect("controller reply task should not panic"),
        "the recovery winner must make the old controller reply stale"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(/*millis*/ 10), &mut waiter)
            .await
            .is_err(),
        "a stale controller reply must not resolve the recovered prompt"
    );
    assert_eq!(
        processor
            .ownership_status_snapshot(main_thread_id)
            .expect("main thread should retain ownership state")
            .owner,
        crate::controller_session::ControllerOwnershipStatusOwner::Tui
    );
    assert!(
        outgoing
            .notify_client_response_from_connection(
                tui_connection_id,
                request_id,
                serde_json::json!({ "decision": "accept" }),
            )
            .await,
        "the recovered TUI is the only authorized resolver"
    );
    waiter
        .await
        .expect("prompt waiter should receive the TUI response")
        .expect("TUI response should be successful");
}

#[tokio::test]
async fn transferred_prompt_former_tui_reply_loses_to_acquire_transition() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let processor = ControllerRequestProcessor::new(
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
    let tui_connection_id = ConnectionId(1);
    let controller_connection_id = ConnectionId(2);
    let main_thread_id = ThreadId::new();
    processor.register_main_thread(main_thread_id, tui_connection_id);
    processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect("native participation approval should create a controller session");
    let thread_outgoing =
        crate::outgoing_message::ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            Arc::clone(&outgoing),
            processor.prompt_request_recipients(
                main_thread_id,
                vec![tui_connection_id, controller_connection_id],
            ),
            vec![tui_connection_id, controller_connection_id],
            main_thread_id,
        );
    let (request_id, mut waiter) = thread_outgoing
        .send_request(command_execution_approval_payload(main_thread_id))
        .await;

    // Tokio's mutex queues waiters FIFO. Queue the acquire before the former TUI reply
    // so this exercises the exact transfer/reply handoff boundary.
    let transition = outgoing.lock_prompt_transition().await;
    let acquire_processor = processor.clone();
    let acquire = tokio::spawn(async move {
        acquire_processor
            .acquire_control(
                controller_connection_id,
                ConnectionOrigin::ExternalController,
            )
            .await
    });
    tokio::task::yield_now().await;
    let reply_outgoing = Arc::clone(&outgoing);
    let reply_request_id = request_id.clone();
    let former_tui_reply = tokio::spawn(async move {
        reply_outgoing
            .notify_client_response_from_connection(
                tui_connection_id,
                reply_request_id,
                serde_json::json!({ "decision": "accept" }),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(!acquire.is_finished());
    assert!(!former_tui_reply.is_finished());
    drop(transition);

    acquire
        .await
        .expect("acquire task should not panic")
        .expect("controller control acquisition should succeed");
    assert!(
        !former_tui_reply
            .await
            .expect("former TUI reply task should not panic"),
        "former TUI reply must be stale after the transfer commits"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(/*millis*/ 10), &mut waiter)
            .await
            .is_err(),
        "the stale TUI reply must not resolve the prompt"
    );
    assert!(
        outgoing
            .notify_client_response_from_connection(
                controller_connection_id,
                request_id,
                serde_json::json!({ "decision": "accept" }),
            )
            .await,
        "the controller remains the only authorized resolver"
    );
    waiter
        .await
        .expect("prompt waiter should receive the controller response")
        .expect("controller response should be successful");
}

#[tokio::test]
async fn unrecoverable_external_prompt_delivery_marks_launch_tui_unavailable() {
    let (outgoing_tx, outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let processor = ControllerRequestProcessor::new(
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
    let tui_connection_id = ConnectionId(1);
    let controller_connection_id = ConnectionId(2);
    let main_thread_id = ThreadId::new();
    processor.register_main_thread(main_thread_id, tui_connection_id);
    processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect("native participation approval should create a controller session");
    let thread_outgoing =
        crate::outgoing_message::ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            Arc::clone(&outgoing),
            processor.prompt_request_recipients(
                main_thread_id,
                vec![tui_connection_id, controller_connection_id],
            ),
            vec![tui_connection_id, controller_connection_id],
            main_thread_id,
        );
    let (request_id, _waiter) = thread_outgoing
        .send_request(command_execution_approval_payload(main_thread_id))
        .await;
    drop(outgoing_rx);

    processor
        .recover_external_prompt_delivery_failure(
            main_thread_id,
            controller_connection_id,
            &request_id,
        )
        .await;

    let status = processor
        .ownership_status_snapshot(main_thread_id)
        .expect("main thread should retain ownership state");
    assert_eq!(
        status.owner,
        crate::controller_session::ControllerOwnershipStatusOwner::TuiUnavailable
    );
}

#[tokio::test]
async fn resolved_external_prompt_does_not_reclaim_controller_on_late_delivery_failure() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        AnalyticsEventsClient::disabled(),
    ));
    let processor = ControllerRequestProcessor::new(
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
    let tui_connection_id = ConnectionId(1);
    let controller_connection_id = ConnectionId(2);
    let main_thread_id = ThreadId::new();
    processor.register_main_thread(main_thread_id, tui_connection_id);
    processor
        .request_participation(
            controller_connection_id,
            ConnectionOrigin::ExternalController,
            /*credential_proof*/ None,
            participation_params(),
        )
        .await
        .expect("native participation approval should create a controller session");
    let thread_outgoing =
        crate::outgoing_message::ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            Arc::clone(&outgoing),
            processor.prompt_request_recipients(
                main_thread_id,
                vec![tui_connection_id, controller_connection_id],
            ),
            vec![tui_connection_id, controller_connection_id],
            main_thread_id,
        );
    let (request_id, _waiter) = thread_outgoing
        .send_request(command_execution_approval_payload(main_thread_id))
        .await;
    assert!(
        outgoing
            .notify_client_response_from_connection(
                controller_connection_id,
                request_id.clone(),
                serde_json::json!({ "decision": "accept" }),
            )
            .await
    );

    processor
        .recover_external_prompt_delivery_failure(
            main_thread_id,
            controller_connection_id,
            &request_id,
        )
        .await;

    let status = processor
        .ownership_status_snapshot(main_thread_id)
        .expect("main thread should retain ownership state");
    assert!(matches!(
        status.owner,
        crate::controller_session::ControllerOwnershipStatusOwner::Controller { .. }
    ));
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
