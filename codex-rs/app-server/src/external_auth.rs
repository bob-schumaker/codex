use std::sync::Arc;
use std::sync::RwLock;

use codex_app_server_protocol::ChatgptAuthTokensRefreshParams;
use codex_app_server_protocol::ChatgptAuthTokensRefreshReason;
use codex_app_server_protocol::ChatgptAuthTokensRefreshResponse;
use codex_app_server_protocol::ServerRequestPayload;
use codex_login::CodexAuth;
use codex_login::ExternalAuthFuture;
use codex_login::auth::ExternalAuth;
use codex_login::auth::ExternalAuthRefreshContext;
use codex_login::auth::ExternalAuthRefreshReason;
use tokio::time::Duration;
use tokio::time::timeout;

use crate::outgoing_message::OutgoingMessageSender;
use crate::request_processors::ControllerRequestProcessor;

const EXTERNAL_AUTH_REFRESH_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct ExternalAuthBridge {
    outgoing: Arc<OutgoingMessageSender>,
    auth: RwLock<CodexAuth>,
    controller_processor: ControllerRequestProcessor,
}

impl ExternalAuthBridge {
    pub(crate) fn new(
        outgoing: Arc<OutgoingMessageSender>,
        auth: CodexAuth,
        controller_processor: ControllerRequestProcessor,
    ) -> Self {
        Self {
            outgoing,
            auth: RwLock::new(auth),
            controller_processor,
        }
    }

    async fn refresh(&self, context: ExternalAuthRefreshContext) -> std::io::Result<CodexAuth> {
        let reason = match context.reason {
            ExternalAuthRefreshReason::Unauthorized => ChatgptAuthTokensRefreshReason::Unauthorized,
        };
        let params = ChatgptAuthTokensRefreshParams {
            reason,
            previous_account_id: context.previous_account_id,
        };

        let (request_id, rx) = match self.controller_processor.tui_connection_id() {
            Some(tui_connection_id) => {
                self.outgoing
                    .send_request_to_connections(
                        Some(&[tui_connection_id]),
                        ServerRequestPayload::ChatgptAuthTokensRefresh(params),
                        /*thread_id*/ None,
                    )
                    .await
            }
            None => {
                self.outgoing
                    .send_request(ServerRequestPayload::ChatgptAuthTokensRefresh(params))
                    .await
            }
        };
        let result = match timeout(EXTERNAL_AUTH_REFRESH_TIMEOUT, rx).await {
            Ok(result) => {
                let result = result.map_err(|err| {
                    std::io::Error::other(format!("auth refresh request canceled: {err}"))
                })?;
                result.map_err(|err| {
                    std::io::Error::other(format!(
                        "auth refresh request failed: code={} message={}",
                        err.code, err.message
                    ))
                })?
            }
            Err(_) => {
                let _canceled = self.outgoing.cancel_request(&request_id).await;
                return Err(std::io::Error::other(format!(
                    "auth refresh request timed out after {}s",
                    EXTERNAL_AUTH_REFRESH_TIMEOUT.as_secs()
                )));
            }
        };

        let response: ChatgptAuthTokensRefreshResponse =
            serde_json::from_value(result).map_err(std::io::Error::other)?;
        let auth = CodexAuth::from_external_chatgpt_tokens(
            response.access_token.as_str(),
            response.chatgpt_account_id.as_str(),
            response.chatgpt_plan_type.as_deref(),
        )?;
        *self
            .auth
            .write()
            .map_err(|_| std::io::Error::other("external auth lock is poisoned"))? = auth.clone();
        Ok(auth)
    }
}

impl ExternalAuth for ExternalAuthBridge {
    fn resolve(&self) -> ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(async {
            self.auth
                .read()
                .map(|auth| auth.clone())
                .map_err(|_| std::io::Error::other("external auth lock is poisoned"))
        })
    }

    fn refresh(&self, context: ExternalAuthRefreshContext) -> ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(ExternalAuthBridge::refresh(self, context))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller_enrollment::ControllerEnrollmentPolicy;
    use crate::controller_enrollment::EmptyControllerEnrollmentSource;
    use crate::controller_session::ControllerSessionClock;
    use crate::controller_session::ControllerSessionConfig;
    use crate::outgoing_message::ConnectionId;
    use crate::outgoing_message::OutgoingEnvelope;
    use crate::outgoing_message::OutgoingMessage;
    use crate::request_processors::ControllerRequestProcessor;
    use codex_analytics::AnalyticsEventsClient;
    use codex_login::ExternalAuthRefreshContext;
    use codex_login::ExternalAuthRefreshReason;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn controller_owned_refresh_targets_registered_tui() {
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(/*buffer*/ 4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            AnalyticsEventsClient::disabled(),
        ));
        let tui_connection_id = ConnectionId(1);
        let controller_connection_id = ConnectionId(2);
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
        controller_processor
            .register_main_thread(codex_protocol::ThreadId::new(), tui_connection_id);
        let bridge = ExternalAuthBridge::new(
            Arc::clone(&outgoing),
            CodexAuth::create_dummy_chatgpt_auth_for_testing(),
            controller_processor,
        );
        let refresh = tokio::spawn(async move {
            bridge
                .refresh(ExternalAuthRefreshContext {
                    reason: ExternalAuthRefreshReason::Unauthorized,
                    previous_account_id: None,
                })
                .await
        });
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = outgoing_rx
            .recv()
            .await
            .expect("refresh request should be sent")
        else {
            panic!("expected targeted refresh request");
        };
        assert_eq!(connection_id, tui_connection_id);
        assert_ne!(connection_id, controller_connection_id);
        let request_id = match message {
            OutgoingMessage::Request(request) => request.id().clone(),
            OutgoingMessage::SequencedRequest(envelope) => envelope.request.id().clone(),
            OutgoingMessage::AppServerNotification(_)
            | OutgoingMessage::Response(_)
            | OutgoingMessage::Error(_) => panic!("expected refresh request"),
        };
        assert!(
            outgoing
                .notify_client_response_from_connection(
                    tui_connection_id,
                    request_id,
                    serde_json::json!({
                        "accessToken": "access-token",
                        "chatgptAccountId": "account-id",
                        "chatgptPlanType": null,
                    }),
                )
                .await
        );
        assert!(
            refresh
                .await
                .expect("refresh task should not panic")
                .is_err(),
            "the TUI response should be consumed before auth-token validation"
        );
    }
}
