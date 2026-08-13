use std::sync::Arc;
use std::sync::Weak;

use axum::http::HeaderValue;
use codex_app_server_protocol::AttestationGenerateParams;
use codex_app_server_protocol::AttestationGenerateResponse;
use codex_app_server_protocol::ServerRequestPayload;
use codex_core::AttestationContext;
use codex_core::AttestationProvider;
use codex_core::GenerateAttestationFuture;
use serde::Serialize;
use tokio::time::Duration;
use tokio::time::timeout;
use tracing::warn;

use crate::outgoing_message::OutgoingMessageSender;
use crate::request_processors::ControllerRequestProcessor;
use crate::thread_state::ThreadStateManager;

const ATTESTATION_GENERATE_TIMEOUT: Duration = Duration::from_millis(100);

pub(crate) fn app_server_attestation_provider(
    outgoing: Arc<OutgoingMessageSender>,
    thread_state_manager: ThreadStateManager,
    controller_processor: ControllerRequestProcessor,
) -> Arc<dyn AttestationProvider> {
    Arc::new(AppServerAttestationProvider {
        outgoing: Arc::downgrade(&outgoing),
        thread_state_manager,
        controller_processor,
    })
}

struct AppServerAttestationProvider {
    outgoing: Weak<OutgoingMessageSender>,
    thread_state_manager: ThreadStateManager,
    controller_processor: ControllerRequestProcessor,
}

impl std::fmt::Debug for AppServerAttestationProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppServerAttestationProvider")
            .finish()
    }
}

impl AttestationProvider for AppServerAttestationProvider {
    fn header_for_request(&self, context: AttestationContext) -> GenerateAttestationFuture<'_> {
        let Some(outgoing) = self.outgoing.upgrade() else {
            return Box::pin(async { None });
        };
        let thread_state_manager = self.thread_state_manager.clone();
        let controller_processor = self.controller_processor.clone();
        Box::pin(async move {
            request_attestation_header_value_with_timeout(
                outgoing,
                thread_state_manager,
                controller_processor,
                context.thread_id,
                ATTESTATION_GENERATE_TIMEOUT,
            )
            .await
            .and_then(|value| HeaderValue::from_bytes(value.as_bytes()).ok())
        })
    }
}

async fn request_attestation_header_value_with_timeout(
    outgoing: Arc<OutgoingMessageSender>,
    thread_state_manager: ThreadStateManager,
    controller_processor: ControllerRequestProcessor,
    thread_id: codex_protocol::ThreadId,
    timeout_duration: Duration,
) -> Option<String> {
    let connection_ids = thread_state_manager
        .subscribed_connection_ids(thread_id)
        .await;
    let request_recipients = controller_processor.tui_request_recipients(thread_id, connection_ids);
    let connection_id = request_recipients.connection_ids().first().copied()?;
    let connection_id = thread_state_manager
        .attestation_capable_connection_for_thread(thread_id, connection_id)
        .await?;

    let connection_ids = [connection_id];
    let (request_id, rx) = outgoing
        .send_request_to_connections(
            Some(&connection_ids),
            ServerRequestPayload::AttestationGenerate(AttestationGenerateParams {}),
            /*thread_id*/ None,
        )
        .await;

    let result = match timeout(timeout_duration, rx).await {
        Ok(Ok(Ok(result))) => result,
        Ok(Ok(Err(err))) => {
            warn!(
                code = err.code,
                message = %err.message,
                "attestation generation request failed"
            );
            return app_server_attestation_header_value(
                AppServerAttestationStatus::RequestFailed,
                /*token*/ None,
            );
        }
        Ok(Err(err)) => {
            warn!("attestation generation request canceled: {err}");
            return app_server_attestation_header_value(
                AppServerAttestationStatus::RequestCanceled,
                /*token*/ None,
            );
        }
        Err(_) => {
            let _canceled = outgoing.cancel_request(&request_id).await;
            warn!(
                timeout_seconds = timeout_duration.as_secs(),
                "attestation generation request timed out"
            );
            return app_server_attestation_header_value(
                AppServerAttestationStatus::Timeout,
                /*token*/ None,
            );
        }
    };

    match serde_json::from_value::<AttestationGenerateResponse>(result) {
        Ok(response) => app_server_attestation_header_value(
            AppServerAttestationStatus::Ok,
            Some(&response.token),
        ),
        Err(err) => {
            warn!("failed to deserialize attestation generation response: {err}");
            app_server_attestation_header_value(
                AppServerAttestationStatus::MalformedResponse,
                /*token*/ None,
            )
        }
    }
}

#[derive(Clone, Copy)]
enum AppServerAttestationStatus {
    Ok,
    Timeout,
    RequestFailed,
    RequestCanceled,
    MalformedResponse,
}

impl AppServerAttestationStatus {
    const fn code(self) -> u8 {
        match self {
            Self::Ok => 0,
            Self::Timeout => 1,
            Self::RequestFailed => 2,
            Self::RequestCanceled => 3,
            Self::MalformedResponse => 4,
        }
    }
}

#[derive(Serialize)]
struct AppServerAttestationEnvelope<'a> {
    v: u8,
    s: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    t: Option<&'a str>,
}

fn app_server_attestation_header_value(
    status: AppServerAttestationStatus,
    token: Option<&str>,
) -> Option<String> {
    serde_json::to_string(&AppServerAttestationEnvelope {
        v: 1,
        s: status.code(),
        t: token,
    })
    .map_err(|err| warn!("failed to serialize app-server attestation envelope: {err}"))
    .ok()
}

#[cfg(test)]
mod tests {
    use super::AppServerAttestationStatus;
    use super::app_server_attestation_header_value;
    use super::request_attestation_header_value_with_timeout;
    use crate::controller_enrollment::ControllerEnrollmentPolicy;
    use crate::controller_enrollment::EmptyControllerEnrollmentSource;
    use crate::controller_session::ControllerSessionClock;
    use crate::controller_session::ControllerSessionConfig;
    use crate::outgoing_message::ConnectionId;
    use crate::outgoing_message::OutgoingEnvelope;
    use crate::outgoing_message::OutgoingMessageSender;
    use crate::request_processors::ControllerRequestProcessor;
    use crate::thread_state::ConnectionCapabilities;
    use crate::thread_state::ThreadStateManager;
    use codex_analytics::AnalyticsEventsClient;
    use codex_app_server_protocol::AttestationGenerateResponse;
    use codex_protocol::ThreadId;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[test]
    fn app_server_attestation_header_value_wraps_opaque_client_payloads() {
        assert_eq!(
            app_server_attestation_header_value(
                AppServerAttestationStatus::Ok,
                Some("v1.opaque-client-payload"),
            ),
            Some(r#"{"v":1,"s":0,"t":"v1.opaque-client-payload"}"#.to_string())
        );
    }

    #[test]
    fn app_server_attestation_header_value_reports_app_server_failures() {
        assert_eq!(
            app_server_attestation_header_value(
                AppServerAttestationStatus::Timeout,
                /*token*/ None,
            ),
            Some(r#"{"v":1,"s":1}"#.to_string())
        );
        assert_eq!(
            app_server_attestation_header_value(
                AppServerAttestationStatus::RequestFailed,
                /*token*/ None,
            ),
            Some(r#"{"v":1,"s":2}"#.to_string())
        );
        assert_eq!(
            app_server_attestation_header_value(
                AppServerAttestationStatus::RequestCanceled,
                /*token*/ None,
            ),
            Some(r#"{"v":1,"s":3}"#.to_string())
        );
        assert_eq!(
            app_server_attestation_header_value(
                AppServerAttestationStatus::MalformedResponse,
                /*token*/ None
            ),
            Some(r#"{"v":1,"s":4}"#.to_string())
        );
    }

    #[tokio::test]
    async fn controller_owned_attestation_request_stays_with_tui() {
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(/*buffer*/ 4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            AnalyticsEventsClient::disabled(),
        ));
        let thread_state_manager = ThreadStateManager::new();
        let tui_connection_id = ConnectionId(1);
        let controller_connection_id = ConnectionId(2);
        let thread_id = ThreadId::new();
        for connection_id in [tui_connection_id, controller_connection_id] {
            thread_state_manager
                .connection_initialized(
                    connection_id,
                    ConnectionCapabilities {
                        request_attestation: true,
                    },
                )
                .await;
            assert!(
                thread_state_manager
                    .try_add_connection_to_thread(thread_id, connection_id)
                    .await
            );
        }
        let controller_processor = ControllerRequestProcessor::new(
            Arc::clone(&outgoing),
            Arc::new(EmptyControllerEnrollmentSource),
            None,
            None,
            ControllerEnrollmentPolicy::BestEffort,
            ControllerSessionClock::from_fn(std::time::Instant::now),
            ControllerSessionConfig {
                lease_duration: Duration::from_secs(/*secs*/ 300),
            },
        );
        controller_processor.register_main_thread(thread_id, tui_connection_id);

        let request = tokio::spawn(request_attestation_header_value_with_timeout(
            Arc::clone(&outgoing),
            thread_state_manager,
            controller_processor,
            thread_id,
            Duration::from_secs(/*secs*/ 1),
        ));
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = outgoing_rx
            .recv()
            .await
            .expect("attestation request should be sent")
        else {
            panic!("expected attestation request envelope");
        };
        assert_eq!(connection_id, tui_connection_id);
        let request_id = match message {
            crate::outgoing_message::OutgoingMessage::Request(request) => request.id().clone(),
            crate::outgoing_message::OutgoingMessage::SequencedRequest(envelope) => {
                envelope.request.id().clone()
            }
            crate::outgoing_message::OutgoingMessage::AppServerNotification(_)
            | crate::outgoing_message::OutgoingMessage::Response(_)
            | crate::outgoing_message::OutgoingMessage::Error(_) => {
                panic!("expected attestation request message")
            }
        };
        assert!(
            outgoing
                .notify_client_response_from_connection(
                    tui_connection_id,
                    request_id,
                    serde_json::to_value(AttestationGenerateResponse {
                        token: "tui-attestation".to_string(),
                    })
                    .expect("attestation response should serialize"),
                )
                .await
        );
        assert_eq!(
            request.await.expect("attestation task should not panic"),
            Some(r#"{"v":1,"s":0,"t":"tui-attestation"}"#.to_string())
        );
    }
}
