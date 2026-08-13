use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use codex_analytics::AnalyticsEventsClient;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::Result;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerNotificationEnvelope;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ServerRequestEnvelope;
use codex_app_server_protocol::ServerRequestPayload;
use codex_app_server_protocol::ServerResponse;
use codex_diagnostics::Gauge;
use codex_diagnostics::GaugeGuard;
use codex_otel::span_w3c_trace_context;
use codex_protocol::ThreadId;
use codex_protocol::protocol::W3cTraceContext;
use codex_protocol::request_permissions::RequestPermissionsResponse;
use tokio::sync::Mutex;
use tokio::sync::OwnedMutexGuard;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::Span;
use tracing::warn;

use crate::error_code::internal_error;
use crate::server_request_error::TURN_TRANSITION_PENDING_REQUEST_ERROR_REASON;
use crate::thread_sequence::ThreadSequenceTracker;
use crate::thread_sequence::notification_thread_id;
pub(crate) use codex_app_server_transport::ConnectionId;
pub(crate) use codex_app_server_transport::OutgoingError;
pub(crate) use codex_app_server_transport::OutgoingMessage;
pub(crate) use codex_app_server_transport::OutgoingResponse;
pub(crate) use codex_app_server_transport::QueuedOutgoingMessage;
pub(crate) use codex_app_server_transport::TrackedWriteCompletion;

#[cfg(test)]
use codex_protocol::account::PlanType;

pub(crate) type ClientRequestResult = std::result::Result<Result, JSONRPCErrorError>;
type ExternalPromptDeliveryFailureHandler = dyn Fn(ExternalPromptDeliveryFailure) + Send + Sync;

static IN_FLIGHT_REQUESTS: Gauge = Gauge::new("app.requests.in_flight");
static PENDING_SERVER_REQUESTS: Gauge = Gauge::new("app.server_requests.pending");

/// Stable identifier for a client request scoped to a transport connection.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ConnectionRequestId {
    pub(crate) connection_id: ConnectionId,
    pub(crate) request_id: RequestId,
}

/// Trace data we keep for an incoming request until we send its final
/// response or error.
#[derive(Clone)]
pub(crate) struct RequestContext {
    request_id: ConnectionRequestId,
    span: Span,
    parent_trace: Option<W3cTraceContext>,
    _diagnostics_guard: Arc<GaugeGuard>,
}

impl RequestContext {
    pub(crate) fn new(
        request_id: ConnectionRequestId,
        span: Span,
        parent_trace: Option<W3cTraceContext>,
    ) -> Self {
        Self {
            request_id,
            span,
            parent_trace,
            _diagnostics_guard: Arc::new(IN_FLIGHT_REQUESTS.track()),
        }
    }

    pub(crate) fn request_trace(&self) -> Option<W3cTraceContext> {
        span_w3c_trace_context(&self.span).or_else(|| self.parent_trace.clone())
    }

    pub(crate) fn span(&self) -> Span {
        self.span.clone()
    }

    fn record_turn_id(&self, turn_id: &str) {
        self.span.record("turn.id", turn_id);
    }
}

#[derive(Debug)]
pub(crate) enum OutgoingEnvelope {
    ToConnection {
        connection_id: ConnectionId,
        message: OutgoingMessage,
        write_complete_tx: Option<TrackedWriteCompletion>,
    },
    ToConnectionThenDisconnect {
        connection_id: ConnectionId,
        message: OutgoingMessage,
    },
    Broadcast {
        message: OutgoingMessage,
    },
}

/// Sends messages to the client and manages request callbacks.
pub(crate) struct OutgoingMessageSender {
    next_server_request_id: AtomicI64,
    sender: mpsc::Sender<OutgoingEnvelope>,
    request_id_to_callback: Arc<Mutex<HashMap<RequestId, PendingCallbackEntry>>>,
    /// Incoming requests that are still waiting on a final response or error.
    /// We keep them here because this is where responses, errors, and
    /// disconnect cleanup all get handled.
    request_contexts: Mutex<HashMap<ConnectionRequestId, RequestContext>>,
    /// Serializes controller ownership changes with prompt-binding consumption.
    ///
    /// The controller coordinator takes this guard before changing ownership and
    /// holds it through sequenced status publication and prompt rebinding. A
    /// responder takes the same guard before consuming its prompt binding.
    prompt_transition_barrier: Arc<Mutex<()>>,
    external_prompt_delivery_failure_handler:
        StdMutex<Option<Arc<ExternalPromptDeliveryFailureHandler>>>,
    analytics_events_client: AnalyticsEventsClient,
    thread_sequences: ThreadSequenceTracker,
}

#[derive(Clone)]
pub(crate) struct ThreadScopedOutgoingMessageSender {
    outgoing: Arc<OutgoingMessageSender>,
    request_recipients: Arc<ServerRequestRecipients>,
    notification_connection_ids: Arc<Vec<ConnectionId>>,
    external_notification_connection_ids: Arc<Vec<ConnectionId>>,
    thread_id: ThreadId,
}

struct PendingCallbackEntry {
    callback: oneshot::Sender<ClientRequestResult>,
    recipient_connection_ids: Option<Vec<ConnectionId>>,
    external_delivery_connection_ids: Vec<ConnectionId>,
    external_delivery_fallback_connection_id: Option<ConnectionId>,
    external_delivery_write_permits: HashMap<ConnectionId, ExternalDeliveryWritePermit>,
    external_controller_owner_epoch: Option<u64>,
    requires_external_controller_epoch: bool,
    externally_transferred_from_tui: bool,
    thread_id: Option<ThreadId>,
    thread_sequence: Option<u64>,
    request: ServerRequest,
    _diagnostics_guard: GaugeGuard,
}

#[derive(Clone)]
pub(crate) struct PendingRequestReplay {
    pub(crate) request: ServerRequest,
    pub(crate) thread_sequence: Option<u64>,
}

struct ExternalDeliveryWritePermit {
    permit_tx: watch::Sender<bool>,
    write_started: Arc<AtomicBool>,
}

#[derive(Clone)]
pub(crate) struct ExternalPromptDeliveryFailure {
    pub(crate) connection_id: ConnectionId,
    pub(crate) request_id: RequestId,
    pub(crate) thread_id: ThreadId,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ServerRequestRecipients {
    connection_ids: Vec<ConnectionId>,
    external_controller_connection_ids: Vec<ConnectionId>,
    external_delivery_fallback_connection_id: Option<ConnectionId>,
    external_controller_owner_epoch: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct PendingServerRequest {
    pub(crate) thread_id: Option<ThreadId>,
    #[cfg(test)]
    pub(crate) thread_sequence: Option<u64>,
    pub(crate) request: ServerRequest,
    pub(crate) external_controller_owner_epoch: Option<u64>,
    pub(crate) requires_external_controller_epoch: bool,
    pub(crate) externally_transferred_from_tui: bool,
}

enum TakeRequestCallbackResult {
    Found(RequestId, Box<PendingCallbackEntry>),
    Missing,
    UnauthorizedConnection,
}

impl PendingCallbackEntry {
    fn can_resolve_from(&self, connection_id: ConnectionId) -> bool {
        self.recipient_connection_ids
            .as_ref()
            .is_none_or(|connection_ids| connection_ids.contains(&connection_id))
    }

    fn mark_external_delivered_to(&mut self, connection_id: ConnectionId) {
        if self.can_resolve_from(connection_id)
            && !self
                .external_delivery_connection_ids
                .contains(&connection_id)
        {
            self.external_delivery_connection_ids.push(connection_id);
        }
        self.external_delivery_write_permits.remove(&connection_id);
    }

    fn replace_external_delivery_write_permit(
        &mut self,
        connection_id: ConnectionId,
    ) -> (watch::Receiver<bool>, Arc<AtomicBool>) {
        if let Some(write_permit) = self.external_delivery_write_permits.remove(&connection_id) {
            let _ = write_permit.permit_tx.send(false);
        }
        let (write_permit_tx, write_permit_rx) = watch::channel(true);
        let write_started = Arc::new(AtomicBool::new(false));
        self.external_delivery_write_permits.insert(
            connection_id,
            ExternalDeliveryWritePermit {
                permit_tx: write_permit_tx,
                write_started: Arc::clone(&write_started),
            },
        );
        (write_permit_rx, write_started)
    }

    fn revoke_external_delivery_write_permits(&mut self) {
        for (_, write_permit) in self.external_delivery_write_permits.drain() {
            let _ = write_permit.permit_tx.send(false);
        }
    }

    fn has_external_delivery_or_started_write(&self) -> bool {
        !self.external_delivery_connection_ids.is_empty()
            || self
                .external_delivery_write_permits
                .values()
                .any(|permit| permit.write_started.load(Ordering::Acquire))
    }

    fn clear_external_delivery(&mut self) {
        self.external_delivery_connection_ids.clear();
    }
}

pub(crate) fn is_transferable_external_controller_prompt(request: &ServerRequest) -> bool {
    matches!(
        request,
        ServerRequest::CommandExecutionRequestApproval { .. }
            | ServerRequest::FileChangeRequestApproval { .. }
            | ServerRequest::PermissionsRequestApproval { .. }
            | ServerRequest::ToolRequestUserInput { .. }
    )
}

impl ThreadScopedOutgoingMessageSender {
    pub(crate) fn new(
        outgoing: Arc<OutgoingMessageSender>,
        connection_ids: Vec<ConnectionId>,
        thread_id: ThreadId,
    ) -> Self {
        let request_recipients = ServerRequestRecipients::normal(connection_ids.clone());
        Self {
            outgoing,
            request_recipients: Arc::new(request_recipients),
            notification_connection_ids: Arc::new(connection_ids),
            external_notification_connection_ids: Arc::new(Vec::new()),
            thread_id,
        }
    }

    pub(crate) fn new_with_request_recipients(
        outgoing: Arc<OutgoingMessageSender>,
        request_recipients: ServerRequestRecipients,
        notification_connection_ids: Vec<ConnectionId>,
        thread_id: ThreadId,
    ) -> Self {
        Self {
            outgoing,
            request_recipients: Arc::new(request_recipients),
            notification_connection_ids: Arc::new(notification_connection_ids),
            external_notification_connection_ids: Arc::new(Vec::new()),
            thread_id,
        }
    }

    pub(crate) fn new_with_controller_recipients(
        outgoing: Arc<OutgoingMessageSender>,
        request_recipients: ServerRequestRecipients,
        notification_connection_ids: Vec<ConnectionId>,
        external_notification_connection_ids: Vec<ConnectionId>,
        thread_id: ThreadId,
    ) -> Self {
        Self {
            outgoing,
            request_recipients: Arc::new(request_recipients),
            notification_connection_ids: Arc::new(notification_connection_ids),
            external_notification_connection_ids: Arc::new(external_notification_connection_ids),
            thread_id,
        }
    }

    pub(crate) async fn send_request(
        &self,
        payload: ServerRequestPayload,
    ) -> (RequestId, oneshot::Receiver<ClientRequestResult>) {
        self.outgoing
            .send_request_to_recipients(&self.request_recipients, payload, Some(self.thread_id))
            .await
    }

    pub(crate) fn track_effective_permissions_approval_response(
        &self,
        request_id: RequestId,
        response: RequestPermissionsResponse,
    ) {
        self.outgoing
            .analytics_events_client
            .track_effective_permissions_approval_response(
                now_unix_timestamp_ms(),
                request_id,
                response,
            );
    }

    pub(crate) async fn send_server_notification(&self, notification: ServerNotification) {
        self.outgoing
            .analytics_events_client
            .track_notification(&notification);
        if self.notification_connection_ids.is_empty() {
            return;
        }
        self.outgoing
            .send_server_notification_to_connections(
                self.notification_connection_ids.as_slice(),
                notification,
            )
            .await;
    }

    pub(crate) async fn send_global_server_notification(&self, notification: ServerNotification) {
        self.outgoing
            .send_server_notification(notification.clone())
            .await;
        if !self.external_notification_connection_ids.is_empty() {
            self.outgoing
                .send_server_notification_to_connections(
                    self.external_notification_connection_ids.as_slice(),
                    notification,
                )
                .await;
        }
    }

    pub(crate) async fn abort_pending_server_requests(&self) {
        self.outgoing
            .cancel_requests_for_thread(
                self.thread_id,
                Some({
                    let mut error = internal_error(
                        "client request resolved because the turn state was changed",
                    );
                    error.data = Some(serde_json::json!({
                        "reason": TURN_TRANSITION_PENDING_REQUEST_ERROR_REASON,
                    }));
                    error
                }),
            )
            .await
    }

    pub(crate) async fn send_response<T>(&self, request_id: ConnectionRequestId, response: T)
    where
        T: Into<ClientResponsePayload>,
    {
        self.outgoing.send_response(request_id, response).await;
    }

    pub(crate) async fn send_error(
        &self,
        request_id: ConnectionRequestId,
        error: impl Into<JSONRPCErrorError>,
    ) {
        self.outgoing.send_error(request_id, error).await;
    }
}

impl ServerRequestRecipients {
    pub(crate) fn normal(connection_ids: Vec<ConnectionId>) -> Self {
        Self {
            connection_ids,
            external_controller_connection_ids: Vec::new(),
            external_delivery_fallback_connection_id: None,
            external_controller_owner_epoch: None,
        }
    }

    pub(crate) fn external_controller_with_fallback(
        controller_connection_id: ConnectionId,
        fallback_connection_id: Option<ConnectionId>,
        owner_epoch: u64,
    ) -> Self {
        Self {
            connection_ids: vec![controller_connection_id],
            external_controller_connection_ids: vec![controller_connection_id],
            external_delivery_fallback_connection_id: fallback_connection_id,
            external_controller_owner_epoch: Some(owner_epoch),
        }
    }

    pub(crate) fn connection_ids(&self) -> &[ConnectionId] {
        &self.connection_ids
    }

    fn delivery_for(&self, connection_id: ConnectionId) -> ServerRequestDelivery {
        if self
            .external_controller_connection_ids
            .contains(&connection_id)
        {
            ServerRequestDelivery::ExternalController
        } else {
            ServerRequestDelivery::Normal
        }
    }

    fn for_request(&self, request: &ServerRequest) -> Self {
        if self.external_controller_owner_epoch.is_some()
            && !is_transferable_external_controller_prompt(request)
            && let Some(fallback_connection_id) = self.external_delivery_fallback_connection_id
        {
            return Self::normal(vec![fallback_connection_id]);
        }
        self.clone()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServerRequestDelivery {
    Normal,
    ExternalController,
}

impl OutgoingMessageSender {
    pub(crate) fn new(
        sender: mpsc::Sender<OutgoingEnvelope>,
        analytics_events_client: AnalyticsEventsClient,
    ) -> Self {
        Self {
            next_server_request_id: AtomicI64::new(0),
            sender,
            request_id_to_callback: Arc::new(Mutex::new(HashMap::new())),
            request_contexts: Mutex::new(HashMap::new()),
            prompt_transition_barrier: Arc::new(Mutex::new(())),
            external_prompt_delivery_failure_handler: StdMutex::new(None),
            analytics_events_client,
            thread_sequences: ThreadSequenceTracker::default(),
        }
    }

    pub(crate) fn thread_sequence(&self, thread_id: ThreadId) -> u64 {
        self.thread_sequences.current(thread_id)
    }

    pub(crate) fn advance_thread_sequence(&self, thread_id: ThreadId) -> u64 {
        self.thread_sequences.advance(thread_id)
    }

    pub(crate) async fn lock_prompt_transition(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.prompt_transition_barrier)
            .lock_owned()
            .await
    }

    pub(crate) fn set_external_prompt_delivery_failure_handler(
        &self,
        handler: Arc<ExternalPromptDeliveryFailureHandler>,
    ) {
        let mut failure_handler = self
            .external_prompt_delivery_failure_handler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *failure_handler = Some(handler);
    }

    pub(crate) async fn register_request_context(&self, request_context: RequestContext) {
        let mut request_contexts = self.request_contexts.lock().await;
        if request_contexts
            .insert(request_context.request_id.clone(), request_context)
            .is_some()
        {
            warn!("replaced unresolved request context");
        }
    }

    pub(crate) async fn connection_closed(&self, connection_id: ConnectionId) {
        let mut request_contexts = self.request_contexts.lock().await;
        request_contexts.retain(|request_id, _| request_id.connection_id != connection_id);
    }

    pub(crate) async fn request_trace_context(
        &self,
        request_id: &ConnectionRequestId,
    ) -> Option<W3cTraceContext> {
        let request_contexts = self.request_contexts.lock().await;
        request_contexts
            .get(request_id)
            .and_then(RequestContext::request_trace)
    }

    pub(crate) async fn record_request_turn_id(
        &self,
        request_id: &ConnectionRequestId,
        turn_id: &str,
    ) {
        let request_contexts = self.request_contexts.lock().await;
        if let Some(request_context) = request_contexts.get(request_id) {
            request_context.record_turn_id(turn_id);
        }
    }

    async fn take_request_context(
        &self,
        request_id: &ConnectionRequestId,
    ) -> Option<RequestContext> {
        let mut request_contexts = self.request_contexts.lock().await;
        request_contexts.remove(request_id)
    }

    #[cfg(test)]
    async fn request_context_count(&self) -> usize {
        self.request_contexts.lock().await.len()
    }

    pub(crate) async fn send_request(
        &self,
        request: ServerRequestPayload,
    ) -> (RequestId, oneshot::Receiver<ClientRequestResult>) {
        self.send_request_to_connections(
            /*connection_ids*/ None, request, /*thread_id*/ None,
        )
        .await
    }

    fn next_request_id(&self) -> RequestId {
        RequestId::Integer(self.next_server_request_id.fetch_add(1, Ordering::Relaxed))
    }

    fn next_thread_sequence(&self, thread_id: Option<ThreadId>) -> Option<u64> {
        thread_id.map(|thread_id| self.thread_sequences.advance(thread_id))
    }

    pub(crate) async fn send_request_to_connections(
        &self,
        connection_ids: Option<&[ConnectionId]>,
        request: ServerRequestPayload,
        thread_id: Option<ThreadId>,
    ) -> (RequestId, oneshot::Receiver<ClientRequestResult>) {
        let id = self.next_request_id();
        let outgoing_message_id = id.clone();
        let request = request.request_with_id(outgoing_message_id.clone());

        let (tx_approve, rx_approve) = oneshot::channel();
        let thread_sequence = {
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            let thread_sequence = self.next_thread_sequence(thread_id);
            request_id_to_callback.insert(
                id,
                PendingCallbackEntry {
                    callback: tx_approve,
                    recipient_connection_ids: connection_ids.map(<[ConnectionId]>::to_vec),
                    external_delivery_connection_ids: Vec::new(),
                    external_delivery_fallback_connection_id: None,
                    external_delivery_write_permits: HashMap::new(),
                    external_controller_owner_epoch: None,
                    requires_external_controller_epoch: false,
                    externally_transferred_from_tui: false,
                    thread_id,
                    thread_sequence,
                    request: request.clone(),
                    _diagnostics_guard: PENDING_SERVER_REQUESTS.track(),
                },
            );
            thread_sequence
        };

        let outgoing_message = server_request_outgoing_message(request.clone(), thread_sequence);
        let send_result = match connection_ids {
            None => {
                self.sender
                    .send(OutgoingEnvelope::Broadcast {
                        message: outgoing_message,
                    })
                    .await
            }
            Some(connection_ids) => {
                let mut send_error = None;
                for connection_id in connection_ids {
                    if let Err(err) = self
                        .sender
                        .send(OutgoingEnvelope::ToConnection {
                            connection_id: *connection_id,
                            message: outgoing_message.clone(),
                            write_complete_tx: None,
                        })
                        .await
                    {
                        send_error = Some(err);
                        break;
                    } else {
                        self.analytics_events_client
                            .track_server_request(connection_id.0, request.clone());
                    }
                }
                match send_error {
                    Some(err) => Err(err),
                    None => Ok(()),
                }
            }
        };

        if let Err(err) = send_result {
            warn!("failed to send request {outgoing_message_id:?} to client: {err:?}");
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            if let Some(mut entry) = request_id_to_callback.remove(&outgoing_message_id) {
                entry.revoke_external_delivery_write_permits();
            }
        }
        (outgoing_message_id, rx_approve)
    }

    async fn send_request_to_recipients(
        &self,
        recipients: &ServerRequestRecipients,
        request: ServerRequestPayload,
        thread_id: Option<ThreadId>,
    ) -> (RequestId, oneshot::Receiver<ClientRequestResult>) {
        let id = self.next_request_id();
        let outgoing_message_id = id.clone();
        let request = request.request_with_id(outgoing_message_id.clone());
        let recipients = recipients.for_request(&request);

        let (tx_approve, rx_approve) = oneshot::channel();
        let thread_sequence = {
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            let thread_sequence = self.next_thread_sequence(thread_id);
            request_id_to_callback.insert(
                id,
                PendingCallbackEntry {
                    callback: tx_approve,
                    recipient_connection_ids: Some(recipients.connection_ids().to_vec()),
                    external_delivery_connection_ids: Vec::new(),
                    external_delivery_fallback_connection_id: recipients
                        .external_delivery_fallback_connection_id,
                    external_delivery_write_permits: HashMap::new(),
                    external_controller_owner_epoch: recipients.external_controller_owner_epoch,
                    requires_external_controller_epoch: recipients
                        .external_controller_owner_epoch
                        .is_some(),
                    externally_transferred_from_tui: false,
                    thread_id,
                    thread_sequence,
                    request: request.clone(),
                    _diagnostics_guard: PENDING_SERVER_REQUESTS.track(),
                },
            );
            thread_sequence
        };

        let mut send_error = None;
        for connection_id in recipients.connection_ids() {
            let write_complete_tx = self
                .tracked_write_completion(
                    outgoing_message_id.clone(),
                    *connection_id,
                    recipients.delivery_for(*connection_id),
                )
                .await;
            if let Err(err) = self
                .sender
                .send(OutgoingEnvelope::ToConnection {
                    connection_id: *connection_id,
                    message: server_request_outgoing_message(request.clone(), thread_sequence),
                    write_complete_tx,
                })
                .await
            {
                send_error = Some(err);
                break;
            }
            self.analytics_events_client
                .track_server_request(connection_id.0, request.clone());
        }

        if let Some(err) = send_error {
            warn!("failed to send request {outgoing_message_id:?} to client: {err:?}");
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            if let Some(mut entry) = request_id_to_callback.remove(&outgoing_message_id) {
                entry.revoke_external_delivery_write_permits();
            }
        }
        (outgoing_message_id, rx_approve)
    }

    async fn tracked_write_completion(
        &self,
        request_id: RequestId,
        connection_id: ConnectionId,
        delivery: ServerRequestDelivery,
    ) -> Option<TrackedWriteCompletion> {
        if delivery == ServerRequestDelivery::Normal {
            return None;
        }

        let (write_complete_tx, write_complete_rx) = oneshot::channel();
        let (write_permit_rx, write_started) = {
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            let entry = request_id_to_callback.get_mut(&request_id)?;
            entry.replace_external_delivery_write_permit(connection_id)
        };
        let request_id_to_callback = Arc::clone(&self.request_id_to_callback);
        let prompt_transition_barrier = Arc::clone(&self.prompt_transition_barrier);
        let sender = self.sender.clone();
        let failure_handler = self
            .external_prompt_delivery_failure_handler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        tokio::spawn(async move {
            let write_completed = write_complete_rx.await.is_ok();
            let _transition = prompt_transition_barrier.lock_owned().await;

            let (thread_id, fallback_request) = {
                let mut request_id_to_callback = request_id_to_callback.lock().await;
                let Some(entry) = request_id_to_callback.get_mut(&request_id) else {
                    return;
                };
                if write_completed {
                    entry.mark_external_delivered_to(connection_id);
                    return;
                }

                entry.external_delivery_write_permits.remove(&connection_id);
                if !entry.can_resolve_from(connection_id) {
                    return;
                }
                if failure_handler.is_some() {
                    (entry.thread_id, None)
                } else {
                    let Some(fallback_connection_id) =
                        entry.external_delivery_fallback_connection_id
                    else {
                        return;
                    };
                    entry.recipient_connection_ids = Some(vec![fallback_connection_id]);
                    entry.external_delivery_fallback_connection_id = None;
                    entry.revoke_external_delivery_write_permits();
                    entry.clear_external_delivery();
                    entry.external_controller_owner_epoch = None;
                    (
                        None,
                        Some((
                            fallback_connection_id,
                            entry.request.clone(),
                            entry.thread_sequence,
                        )),
                    )
                }
            };

            if let (Some(handler), Some(thread_id)) = (failure_handler, thread_id) {
                handler(ExternalPromptDeliveryFailure {
                    connection_id,
                    request_id,
                    thread_id,
                });
            } else if let Some((fallback_connection_id, request, thread_sequence)) =
                fallback_request
                && let Err(err) = sender
                    .send(OutgoingEnvelope::ToConnection {
                        connection_id: fallback_connection_id,
                        message: server_request_outgoing_message(request, thread_sequence),
                        write_complete_tx: None,
                    })
                    .await
            {
                warn!(
                    "failed to rebind externally undelivered request to fallback client: {err:?}"
                );
            }
        });
        Some(TrackedWriteCompletion::with_write_permit(
            write_complete_tx,
            write_permit_rx,
            write_started,
        ))
    }

    pub(crate) async fn replay_requests_to_connection_for_thread(
        &self,
        connection_id: ConnectionId,
        thread_id: ThreadId,
    ) {
        let requests = self.pending_requests_for_thread(thread_id).await;
        self.send_pending_requests_to_connection(connection_id, requests)
            .await;
    }

    #[cfg(test)]
    pub(crate) async fn rebind_requests_for_thread_to_connection(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> bool {
        let requests = {
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            let mut requests = request_id_to_callback
                .values_mut()
                .filter_map(|entry| {
                    if entry.thread_id == Some(thread_id) {
                        let thread_sequence = self.thread_sequences.advance(thread_id);
                        entry.recipient_connection_ids = Some(vec![connection_id]);
                        entry.external_delivery_fallback_connection_id = None;
                        entry.revoke_external_delivery_write_permits();
                        entry.clear_external_delivery();
                        entry.external_controller_owner_epoch = None;
                        entry.externally_transferred_from_tui = false;
                        entry.thread_sequence = Some(thread_sequence);
                        Some(PendingRequestReplay {
                            request: entry.request.clone(),
                            thread_sequence: Some(thread_sequence),
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            requests.sort_by(|left, right| left.request.id().cmp(right.request.id()));
            requests
        };
        self.send_pending_requests_to_connection(connection_id, requests)
            .await
    }

    /// Rebinds only prompts whose current authorized recipient is not the TUI.
    pub(crate) async fn rebind_transferred_requests_for_thread_to_connection(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> bool {
        let requests = {
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            let mut requests = request_id_to_callback
                .values_mut()
                .filter_map(|entry| {
                    if entry.thread_id == Some(thread_id)
                        && entry
                            .recipient_connection_ids
                            .as_ref()
                            .is_some_and(|connection_ids| !connection_ids.contains(&connection_id))
                    {
                        let thread_sequence = self.thread_sequences.advance(thread_id);
                        entry.recipient_connection_ids = Some(vec![connection_id]);
                        entry.external_delivery_fallback_connection_id = None;
                        entry.revoke_external_delivery_write_permits();
                        entry.clear_external_delivery();
                        entry.external_controller_owner_epoch = None;
                        entry.externally_transferred_from_tui = false;
                        entry.thread_sequence = Some(thread_sequence);
                        Some(PendingRequestReplay {
                            request: entry.request.clone(),
                            thread_sequence: Some(thread_sequence),
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            requests.sort_by(|left, right| left.request.id().cmp(right.request.id()));
            requests
        };
        self.send_pending_requests_to_connection(connection_id, requests)
            .await
    }

    pub(crate) async fn rebind_requests_for_thread_to_external_controller_connection(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        fallback_connection_id: Option<ConnectionId>,
        owner_epoch: u64,
    ) {
        let requests = {
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            let mut requests = request_id_to_callback
                .values_mut()
                .filter_map(|entry| {
                    if entry.thread_id == Some(thread_id)
                        && is_transferable_external_controller_prompt(&entry.request)
                        && !(entry.external_controller_owner_epoch == Some(owner_epoch)
                            && entry.can_resolve_from(connection_id))
                    {
                        entry.externally_transferred_from_tui =
                            fallback_connection_id.is_some_and(|fallback_connection_id| {
                                entry.can_resolve_from(fallback_connection_id)
                            });
                        entry.recipient_connection_ids = Some(vec![connection_id]);
                        entry.external_delivery_fallback_connection_id = fallback_connection_id;
                        entry.revoke_external_delivery_write_permits();
                        entry.external_controller_owner_epoch = Some(owner_epoch);
                        entry.requires_external_controller_epoch = true;
                        Some(PendingRequestReplay {
                            request: entry.request.clone(),
                            thread_sequence: entry.thread_sequence,
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            requests.sort_by(|left, right| left.request.id().cmp(right.request.id()));
            requests
        };
        self.send_pending_requests_to_external_controller_connection(connection_id, requests)
            .await;
    }

    pub(crate) async fn pending_server_request(
        &self,
        id: &RequestId,
    ) -> Option<PendingServerRequest> {
        let request_id_to_callback = self.request_id_to_callback.lock().await;
        request_id_to_callback
            .get(id)
            .map(|entry| PendingServerRequest {
                thread_id: entry.thread_id,
                #[cfg(test)]
                thread_sequence: entry.thread_sequence,
                request: entry.request.clone(),
                external_controller_owner_epoch: entry.external_controller_owner_epoch,
                requires_external_controller_epoch: entry.requires_external_controller_epoch,
                externally_transferred_from_tui: entry.externally_transferred_from_tui,
            })
    }

    pub(crate) async fn rebind_request_resolution_to_connection(
        &self,
        id: &RequestId,
        thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> bool {
        let mut request_id_to_callback = self.request_id_to_callback.lock().await;
        let Some(entry) = request_id_to_callback.get_mut(id) else {
            return false;
        };
        if entry.thread_id != Some(thread_id) {
            return false;
        }

        entry.recipient_connection_ids = Some(vec![connection_id]);
        entry.external_delivery_fallback_connection_id = None;
        entry.revoke_external_delivery_write_permits();
        entry.clear_external_delivery();
        entry.external_controller_owner_epoch = None;
        entry.externally_transferred_from_tui = false;
        true
    }

    async fn send_pending_requests_to_connection(
        &self,
        connection_id: ConnectionId,
        requests: Vec<PendingRequestReplay>,
    ) -> bool {
        let mut enqueued = true;
        for PendingRequestReplay {
            request,
            thread_sequence,
        } in requests
        {
            if let Err(err) = self
                .sender
                .send(OutgoingEnvelope::ToConnection {
                    connection_id,
                    message: server_request_outgoing_message(request, thread_sequence),
                    write_complete_tx: None,
                })
                .await
            {
                warn!("failed to resend request to client: {err:?}");
                enqueued = false;
            }
        }
        enqueued
    }

    async fn send_pending_requests_to_external_controller_connection(
        &self,
        connection_id: ConnectionId,
        requests: Vec<PendingRequestReplay>,
    ) {
        for PendingRequestReplay {
            request,
            thread_sequence,
        } in requests
        {
            let request_id = request.id().clone();
            let write_complete_tx = self
                .tracked_write_completion(
                    request_id,
                    connection_id,
                    ServerRequestDelivery::ExternalController,
                )
                .await;
            if let Err(err) = self
                .sender
                .send(OutgoingEnvelope::ToConnection {
                    connection_id,
                    message: server_request_outgoing_message(request, thread_sequence),
                    write_complete_tx,
                })
                .await
            {
                warn!("failed to resend request to client: {err:?}");
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn notify_client_response_from_connection(
        &self,
        connection_id: ConnectionId,
        id: RequestId,
        result: Result,
    ) -> bool {
        let _transition = self.lock_prompt_transition().await;
        self.notify_client_response_from_connection_with_transition_held(connection_id, id, result)
            .await
    }

    /// Resolves a response while the caller holds the prompt-transition barrier.
    pub(crate) async fn notify_client_response_from_connection_with_transition_held(
        &self,
        connection_id: ConnectionId,
        id: RequestId,
        result: Result,
    ) -> bool {
        let entry = self
            .take_request_callback_from_connection(connection_id, &id)
            .await;
        self.notify_client_response_entry(entry, id, result).await
    }

    async fn notify_client_response_entry(
        &self,
        entry: TakeRequestCallbackResult,
        id: RequestId,
        result: Result,
    ) -> bool {
        match entry {
            TakeRequestCallbackResult::Found(id, entry) => {
                let entry = *entry;
                let completed_at_ms = now_unix_timestamp_ms();
                if let Ok(response) = entry.request.response_from_result(result.clone())
                    && !matches!(response, ServerResponse::PermissionsRequestApproval { .. })
                {
                    self.analytics_events_client
                        .track_server_response(completed_at_ms, response);
                }
                if let Err(err) = entry.callback.send(Ok(result)) {
                    warn!("could not notify callback for {id:?} due to: {err:?}");
                }
                true
            }
            TakeRequestCallbackResult::Missing => {
                warn!("could not find callback for {id:?}");
                false
            }
            TakeRequestCallbackResult::UnauthorizedConnection => false,
        }
    }

    pub(crate) async fn notify_client_error_from_connection(
        &self,
        connection_id: ConnectionId,
        id: RequestId,
        error: JSONRPCErrorError,
    ) -> bool {
        let _transition = self.lock_prompt_transition().await;
        self.notify_client_error_from_connection_with_transition_held(connection_id, id, error)
            .await
    }

    /// Rejects a response while the caller holds the prompt-transition barrier.
    pub(crate) async fn notify_client_error_from_connection_with_transition_held(
        &self,
        connection_id: ConnectionId,
        id: RequestId,
        error: JSONRPCErrorError,
    ) -> bool {
        let entry = self
            .take_request_callback_from_connection(connection_id, &id)
            .await;
        self.notify_client_error_entry(entry, id, error).await
    }

    async fn notify_client_error_entry(
        &self,
        entry: TakeRequestCallbackResult,
        id: RequestId,
        error: JSONRPCErrorError,
    ) -> bool {
        match entry {
            TakeRequestCallbackResult::Found(id, entry) => {
                let entry = *entry;
                warn!("client responded with error for {id:?}: {error:?}");
                self.analytics_events_client
                    .track_server_request_aborted(now_unix_timestamp_ms(), id.clone());
                if let Err(err) = entry.callback.send(Err(error)) {
                    warn!("could not notify callback for {id:?} due to: {err:?}");
                }
                true
            }
            TakeRequestCallbackResult::Missing => {
                warn!("could not find callback for {id:?}");
                false
            }
            TakeRequestCallbackResult::UnauthorizedConnection => false,
        }
    }

    pub(crate) async fn cancel_request(&self, id: &RequestId) -> bool {
        let entry = self.take_request_callback(id).await;
        match entry {
            TakeRequestCallbackResult::Found(request_id, _entry) => {
                self.analytics_events_client
                    .track_server_request_aborted(now_unix_timestamp_ms(), request_id);
                true
            }
            TakeRequestCallbackResult::Missing
            | TakeRequestCallbackResult::UnauthorizedConnection => false,
        }
    }

    pub(crate) async fn cancel_all_requests(&self, error: Option<JSONRPCErrorError>) {
        let entries = {
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            request_id_to_callback
                .drain()
                .map(|(_, mut entry)| {
                    entry.revoke_external_delivery_write_permits();
                    entry
                })
                .collect::<Vec<_>>()
        };

        for entry in entries {
            self.analytics_events_client
                .track_server_request_aborted(now_unix_timestamp_ms(), entry.request.id().clone());
            if let Some(error) = error.as_ref()
                && let Err(err) = entry.callback.send(Err(error.clone()))
            {
                let request_id = entry.request.id();
                warn!("could not notify callback for {request_id:?} due to: {err:?}");
            }
        }
    }

    async fn take_request_callback(&self, id: &RequestId) -> TakeRequestCallbackResult {
        let mut request_id_to_callback = self.request_id_to_callback.lock().await;
        request_id_to_callback
            .remove_entry(id)
            .map(|(request_id, mut entry)| {
                entry.revoke_external_delivery_write_permits();
                TakeRequestCallbackResult::Found(request_id, Box::new(entry))
            })
            .unwrap_or(TakeRequestCallbackResult::Missing)
    }

    async fn take_request_callback_from_connection(
        &self,
        connection_id: ConnectionId,
        id: &RequestId,
    ) -> TakeRequestCallbackResult {
        let mut request_id_to_callback = self.request_id_to_callback.lock().await;
        let Some(entry) = request_id_to_callback.get(id) else {
            return TakeRequestCallbackResult::Missing;
        };
        if !entry.can_resolve_from(connection_id) {
            warn!(
                request_id = ?id,
                ?connection_id,
                "dropping server-request response from non-recipient connection"
            );
            return TakeRequestCallbackResult::UnauthorizedConnection;
        }
        request_id_to_callback
            .remove_entry(id)
            .map(|(request_id, mut entry)| {
                entry.revoke_external_delivery_write_permits();
                TakeRequestCallbackResult::Found(request_id, Box::new(entry))
            })
            .unwrap_or(TakeRequestCallbackResult::Missing)
    }

    pub(crate) async fn pending_requests_for_thread(
        &self,
        thread_id: ThreadId,
    ) -> Vec<PendingRequestReplay> {
        let request_id_to_callback = self.request_id_to_callback.lock().await;
        let mut requests = request_id_to_callback
            .values()
            .filter_map(|entry| {
                (entry.thread_id == Some(thread_id)
                    && !entry.has_external_delivery_or_started_write())
                .then_some(PendingRequestReplay {
                    request: entry.request.clone(),
                    thread_sequence: entry.thread_sequence,
                })
            })
            .collect::<Vec<_>>();
        requests.sort_by(|left, right| left.request.id().cmp(right.request.id()));
        requests
    }

    pub(crate) async fn thread_sequence_and_pending_requests_for_thread(
        &self,
        thread_id: ThreadId,
    ) -> (u64, Vec<PendingRequestReplay>) {
        let request_id_to_callback = self.request_id_to_callback.lock().await;
        let last_sequence = self.thread_sequences.current(thread_id);
        let mut requests = request_id_to_callback
            .values()
            .filter_map(|entry| {
                (entry.thread_id == Some(thread_id)
                    && !entry.has_external_delivery_or_started_write())
                .then_some(PendingRequestReplay {
                    request: entry.request.clone(),
                    thread_sequence: entry.thread_sequence,
                })
            })
            .collect::<Vec<_>>();
        requests.sort_by(|left, right| left.request.id().cmp(right.request.id()));
        (last_sequence, requests)
    }

    #[cfg(test)]
    pub(crate) async fn request_has_external_delivery(
        &self,
        id: &RequestId,
        connection_id: ConnectionId,
    ) -> bool {
        let request_id_to_callback = self.request_id_to_callback.lock().await;
        request_id_to_callback.get(id).is_some_and(|entry| {
            entry
                .external_delivery_connection_ids
                .contains(&connection_id)
        })
    }

    pub(crate) async fn cancel_requests_for_thread(
        &self,
        thread_id: ThreadId,
        error: Option<JSONRPCErrorError>,
    ) {
        let entries = {
            let mut request_id_to_callback = self.request_id_to_callback.lock().await;
            let request_ids = request_id_to_callback
                .iter()
                .filter_map(|(request_id, entry)| {
                    (entry.thread_id == Some(thread_id)).then_some(request_id.clone())
                })
                .collect::<Vec<_>>();

            let mut entries = Vec::with_capacity(request_ids.len());
            for request_id in request_ids {
                if let Some(mut entry) = request_id_to_callback.remove(&request_id) {
                    entry.revoke_external_delivery_write_permits();
                    entries.push(entry);
                }
            }
            entries
        };

        for entry in entries {
            self.analytics_events_client
                .track_server_request_aborted(now_unix_timestamp_ms(), entry.request.id().clone());
            if let Some(error) = error.as_ref()
                && let Err(err) = entry.callback.send(Err(error.clone()))
            {
                let request_id = entry.request.id();
                warn!("could not notify callback for {request_id:?} due to: {err:?}",);
            }
        }
    }

    pub(crate) async fn send_response<T>(&self, request_id: ConnectionRequestId, response: T)
    where
        T: Into<ClientResponsePayload>,
    {
        self.send_response_as_inner(
            request_id,
            response.into(),
            /*thread_originator*/ None,
            /*disconnect_after_write*/ false,
        )
        .await;
    }

    pub(crate) async fn send_response_with_thread_originator<T>(
        &self,
        request_id: ConnectionRequestId,
        response: T,
        thread_originator: String,
    ) where
        T: Into<ClientResponsePayload>,
    {
        self.send_response_as_inner(
            request_id,
            response.into(),
            Some(thread_originator),
            /*disconnect_after_write*/ false,
        )
        .await;
    }

    pub(crate) async fn send_response_as(
        &self,
        request_id: ConnectionRequestId,
        response: ClientResponsePayload,
    ) {
        self.send_response_as_inner(
            request_id, response, /*thread_originator*/ None,
            /*disconnect_after_write*/ false,
        )
        .await;
    }

    pub(crate) async fn send_response_as_then_disconnect(
        &self,
        request_id: ConnectionRequestId,
        response: ClientResponsePayload,
    ) {
        self.send_response_as_inner(
            request_id, response, /*thread_originator*/ None,
            /*disconnect_after_write*/ true,
        )
        .await;
    }

    async fn send_response_as_inner(
        &self,
        request_id: ConnectionRequestId,
        response: ClientResponsePayload,
        thread_originator: Option<String>,
        disconnect_after_write: bool,
    ) {
        let connection_id = request_id.connection_id;
        let request_id_for_analytics = request_id.request_id.clone();
        match thread_originator {
            Some(thread_originator) => {
                self.analytics_events_client
                    .track_response_with_thread_originator(
                        connection_id.0,
                        request_id_for_analytics,
                        &response,
                        thread_originator,
                    );
            }
            None => {
                self.analytics_events_client.track_response(
                    connection_id.0,
                    request_id_for_analytics,
                    &response,
                );
            }
        }
        let response = Box::new(response);
        let request_context = self.take_request_context(&request_id).await;
        let outgoing_message = OutgoingMessage::Response(OutgoingResponse {
            id: request_id.request_id,
            result: response,
        });
        if disconnect_after_write {
            self.send_outgoing_message_to_connection_then_disconnect(
                request_context,
                connection_id,
                outgoing_message,
                "response",
            )
            .await;
        } else {
            self.send_outgoing_message_to_connection(
                request_context,
                connection_id,
                outgoing_message,
                "response",
            )
            .await;
        }
    }

    pub(crate) async fn send_server_notification(&self, notification: ServerNotification) {
        if matches!(
            notification,
            ServerNotification::ThreadArchived(_) | ServerNotification::ThreadUnarchived(_)
        ) {
            self.analytics_events_client
                .track_notification(&notification);
        }
        self.send_server_notification_to_connections(&[], notification)
            .await;
    }

    pub(crate) async fn send_server_notification_to_connections(
        &self,
        connection_ids: &[ConnectionId],
        notification: ServerNotification,
    ) {
        tracing::trace!(
            targeted_connections = connection_ids.len(),
            "app-server event: {notification}"
        );
        let outgoing_message = self.timestamped_server_notification(notification).await;
        if connection_ids.is_empty() {
            if let Err(err) = self
                .sender
                .send(OutgoingEnvelope::Broadcast {
                    message: outgoing_message,
                })
                .await
            {
                warn!("failed to send server notification to client: {err:?}");
            }
            return;
        }
        for connection_id in connection_ids {
            if let Err(err) = self
                .sender
                .send(OutgoingEnvelope::ToConnection {
                    connection_id: *connection_id,
                    message: outgoing_message.clone(),
                    write_complete_tx: None,
                })
                .await
            {
                warn!("failed to send server notification to client: {err:?}");
            }
        }
    }

    pub(crate) async fn send_server_notification_to_connection_and_wait(
        &self,
        connection_id: ConnectionId,
        notification: ServerNotification,
    ) {
        tracing::trace!("app-server event: {notification}");
        let outgoing_message = self.timestamped_server_notification(notification).await;
        let (write_complete_tx, write_complete_rx) = oneshot::channel();
        if let Err(err) = self
            .sender
            .send(OutgoingEnvelope::ToConnection {
                connection_id,
                message: outgoing_message,
                write_complete_tx: Some(TrackedWriteCompletion::new(write_complete_tx)),
            })
            .await
        {
            warn!("failed to send server notification to client: {err:?}");
        }
        let _ = write_complete_rx.await;
    }

    pub(crate) async fn send_error(
        &self,
        request_id: ConnectionRequestId,
        error: impl Into<JSONRPCErrorError>,
    ) {
        let request_context = self.take_request_context(&request_id).await;
        self.send_error_inner(request_context, request_id, error.into())
            .await;
    }

    pub(crate) async fn send_error_to_connection(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        error: impl Into<JSONRPCErrorError>,
    ) {
        let outgoing_message = OutgoingMessage::Error(OutgoingError {
            id: request_id,
            error: error.into(),
        });
        self.send_outgoing_message_to_connection(
            None,
            connection_id,
            outgoing_message,
            "connection-scoped error",
        )
        .await;
    }

    pub(crate) async fn send_result<T, E>(
        &self,
        request_id: ConnectionRequestId,
        result: std::result::Result<T, E>,
    ) where
        T: Into<ClientResponsePayload>,
        E: Into<JSONRPCErrorError>,
    {
        match result {
            Ok(response) => {
                self.send_response(request_id, response).await;
            }
            Err(error) => self.send_error(request_id, error).await,
        }
    }

    async fn send_error_inner(
        &self,
        request_context: Option<RequestContext>,
        request_id: ConnectionRequestId,
        error: JSONRPCErrorError,
    ) {
        let outgoing_message = OutgoingMessage::Error(OutgoingError {
            id: request_id.request_id,
            error,
        });
        self.send_outgoing_message_to_connection(
            request_context,
            request_id.connection_id,
            outgoing_message,
            "error",
        )
        .await;
    }

    async fn send_outgoing_message_to_connection(
        &self,
        request_context: Option<RequestContext>,
        connection_id: ConnectionId,
        message: OutgoingMessage,
        message_kind: &'static str,
    ) {
        let send_fut = self.sender.send(OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx: None,
        });
        let send_result = if let Some(request_context) = request_context {
            send_fut.instrument(request_context.span()).await
        } else {
            send_fut.await
        };

        if let Err(err) = send_result {
            warn!("failed to send {message_kind} to client: {err:?}");
        }
    }

    async fn send_outgoing_message_to_connection_then_disconnect(
        &self,
        request_context: Option<RequestContext>,
        connection_id: ConnectionId,
        message: OutgoingMessage,
        message_kind: &'static str,
    ) {
        let send_fut = self
            .sender
            .send(OutgoingEnvelope::ToConnectionThenDisconnect {
                connection_id,
                message,
            });
        let send_result = if let Some(request_context) = request_context {
            send_fut.instrument(request_context.span()).await
        } else {
            send_fut.await
        };

        if let Err(err) = send_result {
            warn!("failed to send {message_kind} to client: {err:?}");
        }
    }

    async fn timestamped_server_notification(
        &self,
        notification: ServerNotification,
    ) -> OutgoingMessage {
        let thread_sequence = notification_thread_id(&notification)
            .map(|thread_id| self.thread_sequences.advance(thread_id));
        OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
            notification,
            thread_sequence,
            emitted_at_ms: Some(now_unix_timestamp_ms().try_into().unwrap_or_default()),
        })
    }
}

fn now_unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or_default()
}

fn server_request_outgoing_message(
    request: ServerRequest,
    thread_sequence: Option<u64>,
) -> OutgoingMessage {
    match thread_sequence {
        Some(thread_sequence) => OutgoingMessage::SequencedRequest(ServerRequestEnvelope {
            request,
            thread_sequence: Some(thread_sequence),
        }),
        None => OutgoingMessage::Request(request),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::RwLock;
    use std::time::Duration;

    use crate::transport::ConnectionOrigin;
    use crate::transport::OutboundConnectionState;
    use crate::transport::route_outgoing_envelope;
    use codex_app_server_protocol::AccountLoginCompletedNotification;
    use codex_app_server_protocol::AccountRateLimitsUpdatedNotification;
    use codex_app_server_protocol::AccountUpdatedNotification;
    use codex_app_server_protocol::ApplyPatchApprovalParams;
    use codex_app_server_protocol::AuthMode;
    use codex_app_server_protocol::CommandExecutionApprovalDecision;
    use codex_app_server_protocol::CommandExecutionRequestApprovalParams;
    use codex_app_server_protocol::ConfigWarningNotification;
    use codex_app_server_protocol::DynamicToolCallParams;
    use codex_app_server_protocol::FileChangeRequestApprovalParams;
    use codex_app_server_protocol::GuardianWarningNotification;
    use codex_app_server_protocol::McpServerElicitationRequest;
    use codex_app_server_protocol::McpServerElicitationRequestParams;
    use codex_app_server_protocol::ModelRerouteReason;
    use codex_app_server_protocol::ModelReroutedNotification;
    use codex_app_server_protocol::ModelVerification;
    use codex_app_server_protocol::ModelVerificationNotification;
    use codex_app_server_protocol::PermissionsRequestApprovalParams;
    use codex_app_server_protocol::RateLimitSnapshot;
    use codex_app_server_protocol::RateLimitWindow;
    use codex_app_server_protocol::RequestPermissionProfile;
    use codex_app_server_protocol::ServerResponse;
    use codex_app_server_protocol::ThreadStatus;
    use codex_app_server_protocol::ThreadStatusChangedNotification;
    use codex_app_server_protocol::ToolRequestUserInputParams;
    use codex_app_server_protocol::TurnModerationMetadataNotification;
    use codex_protocol::ThreadId;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn verify_server_notification_serialization() {
        let notification =
            ServerNotification::AccountLoginCompleted(AccountLoginCompletedNotification {
                login_id: Some(Uuid::nil().to_string()),
                success: true,
                error: None,
                onboarding_entrypoint: None,
            });

        let jsonrpc_notification =
            OutgoingMessage::AppServerNotification(ServerNotificationEnvelope {
                notification,
                thread_sequence: None,
                emitted_at_ms: Some(1_234),
            });
        assert_eq!(
            json!({
                "method": "account/login/completed",
                "params": {
                    "loginId": Uuid::nil().to_string(),
                    "success": true,
                    "error": null,
                    "onboardingEntrypoint": null,
                },
                "emittedAtMs": 1_234,
            }),
            serde_json::to_value(jsonrpc_notification)
                .expect("ensure the strum macros serialize the method field correctly"),
            "ensure the strum macros serialize the method field correctly"
        );
    }

    #[test]
    fn verify_account_login_completed_notification_serialization() {
        let notification =
            ServerNotification::AccountLoginCompleted(AccountLoginCompletedNotification {
                login_id: Some(Uuid::nil().to_string()),
                success: true,
                error: None,
                onboarding_entrypoint: None,
            });

        assert_eq!(
            json!({
                "method": "account/login/completed",
                "params": {
                    "loginId": Uuid::nil().to_string(),
                    "success": true,
                    "error": null,
                    "onboardingEntrypoint": null,
                },
            }),
            serde_json::to_value(notification)
                .expect("ensure the notification serializes correctly"),
            "ensure the notification serializes correctly"
        );
    }

    #[test]
    fn verify_account_rate_limits_notification_serialization() {
        let notification =
            ServerNotification::AccountRateLimitsUpdated(AccountRateLimitsUpdatedNotification {
                rate_limits: RateLimitSnapshot {
                    limit_id: Some("codex".to_string()),
                    limit_name: None,
                    primary: Some(RateLimitWindow {
                        used_percent: 25,
                        window_duration_mins: Some(15),
                        resets_at: Some(123),
                    }),
                    secondary: None,
                    credits: None,
                    individual_limit: None,
                    spend_control_reached: None,
                    plan_type: Some(PlanType::SelfServeBusinessProLite),
                    rate_limit_reached_type: None,
                },
            });

        assert_eq!(
            json!({
                "method": "account/rateLimits/updated",
                "params": {
                        "rateLimits": {
                        "limitId": "codex",
                        "limitName": null,
                        "primary": {
                            "usedPercent": 25,
                            "windowDurationMins": 15,
                            "resetsAt": 123
                        },
                        "secondary": null,
                        "credits": null,
                        "individualLimit": null,
                        "spendControlReached": null,
                        "planType": "self_serve_business_prolite",
                        "rateLimitReachedType": null
                    }
                },
            }),
            serde_json::to_value(notification)
                .expect("ensure the notification serializes correctly"),
            "ensure the notification serializes correctly"
        );
    }

    #[test]
    fn verify_account_updated_notification_serialization() {
        let notification = ServerNotification::AccountUpdated(AccountUpdatedNotification {
            auth_mode: Some(AuthMode::Chatgpt),
            plan_type: Some(PlanType::SelfServeBusinessProLite),
        });

        assert_eq!(
            json!({
                "method": "account/updated",
                "params": {
                    "authMode": "chatgpt",
                    "planType": "self_serve_business_prolite"
                },
            }),
            serde_json::to_value(notification)
                .expect("ensure the notification serializes correctly"),
            "ensure the notification serializes correctly"
        );
    }

    #[test]
    fn verify_config_warning_notification_serialization() {
        let notification = ServerNotification::ConfigWarning(ConfigWarningNotification {
            summary: "Config error: using defaults".to_string(),
            details: Some("error loading config: bad config".to_string()),
            path: None,
            range: None,
        });

        assert_eq!(
            json!( {
                "method": "configWarning",
                "params": {
                    "summary": "Config error: using defaults",
                    "details": "error loading config: bad config",
                },
            }),
            serde_json::to_value(notification)
                .expect("ensure the notification serializes correctly"),
            "ensure the notification serializes correctly"
        );
    }

    #[tokio::test]
    async fn thread_scoped_global_notifications_target_external_controllers() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_controller_recipients(
            outgoing,
            ServerRequestRecipients::normal(vec![ConnectionId(1), ConnectionId(2)]),
            vec![ConnectionId(1), ConnectionId(2)],
            vec![ConnectionId(2)],
            ThreadId::new(),
        );
        let notification = ServerNotification::ConfigWarning(ConfigWarningNotification {
            summary: "summary".to_string(),
            details: Some("details".to_string()),
            path: None,
            range: None,
        });

        thread_outgoing
            .send_global_server_notification(notification.clone())
            .await;

        let OutgoingEnvelope::Broadcast { message } = rx
            .recv()
            .await
            .expect("broadcast notification should be sent")
        else {
            panic!("expected broadcast notification");
        };
        let OutgoingMessage::AppServerNotification(envelope) = message else {
            panic!("expected app-server notification");
        };
        assert_eq!(
            serde_json::to_value(envelope.notification).expect("notification should serialize"),
            serde_json::to_value(&notification).expect("notification should serialize"),
        );

        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx: None,
        } = rx
            .recv()
            .await
            .expect("targeted external controller notification should be sent")
        else {
            panic!("expected targeted notification");
        };
        assert_eq!(connection_id, ConnectionId(2));
        let OutgoingMessage::AppServerNotification(envelope) = message else {
            panic!("expected app-server notification");
        };
        assert_eq!(
            serde_json::to_value(envelope.notification).expect("notification should serialize"),
            serde_json::to_value(notification).expect("notification should serialize"),
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn verify_guardian_warning_notification_serialization() {
        let notification = ServerNotification::GuardianWarning(GuardianWarningNotification {
            thread_id: "thread-1".to_string(),
            message: "Automatic approval review denied the requested action.".to_string(),
        });

        assert_eq!(
            json!({
                "method": "guardianWarning",
                "params": {
                    "threadId": "thread-1",
                    "message": "Automatic approval review denied the requested action.",
                },
            }),
            serde_json::to_value(notification)
                .expect("ensure the notification serializes correctly"),
            "ensure the notification serializes correctly"
        );
    }

    #[test]
    fn verify_model_rerouted_notification_serialization() {
        let notification = ServerNotification::ModelRerouted(ModelReroutedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            from_model: "gpt-5.3-codex".to_string(),
            to_model: "gpt-5.2".to_string(),
            reason: ModelRerouteReason::HighRiskCyberActivity,
        });

        assert_eq!(
            json!({
                "method": "model/rerouted",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "fromModel": "gpt-5.3-codex",
                    "toModel": "gpt-5.2",
                    "reason": "highRiskCyberActivity",
                },
            }),
            serde_json::to_value(notification)
                .expect("ensure the notification serializes correctly"),
            "ensure the notification serializes correctly"
        );
    }

    #[test]
    fn verify_model_verification_notification_serialization() {
        let notification = ServerNotification::ModelVerification(ModelVerificationNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            verifications: vec![ModelVerification::TrustedAccessForCyber],
        });

        assert_eq!(
            json!({
                "method": "model/verification",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "verifications": ["trustedAccessForCyber"],
                },
            }),
            serde_json::to_value(notification)
                .expect("ensure the notification serializes correctly"),
            "ensure the notification serializes correctly"
        );
    }

    #[test]
    fn verify_turn_moderation_metadata_notification_serialization() {
        let notification =
            ServerNotification::TurnModerationMetadata(TurnModerationMetadataNotification {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                metadata: json!({"presentation": "inline"}),
            });

        assert_eq!(
            json!({
                "method": "turn/moderationMetadata",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "metadata": {"presentation": "inline"},
                },
            }),
            serde_json::to_value(notification)
                .expect("ensure the notification serializes correctly"),
            "ensure the notification serializes correctly"
        );
    }

    #[test]
    fn server_request_response_from_result_decodes_typed_response() {
        let request = ServerRequest::CommandExecutionRequestApproval {
            request_id: RequestId::Integer(7),
            params: CommandExecutionRequestApprovalParams {
                thread_id: "thread-1".to_string(),
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
            },
        };

        let response = request
            .response_from_result(json!({
                "decision": "acceptForSession",
            }))
            .expect("decode typed server response");

        let ServerResponse::CommandExecutionRequestApproval {
            request_id,
            response,
        } = response
        else {
            panic!("expected command execution approval response");
        };
        assert_eq!(request_id, RequestId::Integer(7));
        assert_eq!(
            response.decision,
            CommandExecutionApprovalDecision::AcceptForSession
        );
    }
    #[tokio::test]
    async fn send_response_routes_to_target_connection() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());
        let request_id = ConnectionRequestId {
            connection_id: ConnectionId(42),
            request_id: RequestId::Integer(7),
        };

        outgoing
            .send_response(
                request_id.clone(),
                ClientResponsePayload::ThreadArchive(
                    codex_app_server_protocol::ThreadArchiveResponse {},
                ),
            )
            .await;

        let envelope = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("should receive envelope before timeout")
            .expect("channel should contain one message");

        match envelope {
            OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                ..
            } => {
                assert_eq!(connection_id, ConnectionId(42));
                let OutgoingMessage::Response(response) = message else {
                    panic!("expected response message");
                };
                assert_eq!(response.id, request_id.request_id);
                assert_eq!(
                    serde_json::to_value(response.result).expect("result should serialize"),
                    json!({})
                );
            }
            other => panic!("expected targeted response envelope, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_response_clears_registered_request_context() {
        let (tx, _rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());
        let request_id = ConnectionRequestId {
            connection_id: ConnectionId(42),
            request_id: RequestId::Integer(7),
        };

        outgoing
            .register_request_context(RequestContext::new(
                request_id.clone(),
                tracing::info_span!("app_server.request", rpc.method = "thread/start"),
                /*parent_trace*/ None,
            ))
            .await;
        assert_eq!(outgoing.request_context_count().await, 1);

        outgoing
            .send_response(
                request_id,
                ClientResponsePayload::ThreadArchive(
                    codex_app_server_protocol::ThreadArchiveResponse {},
                ),
            )
            .await;

        assert_eq!(outgoing.request_context_count().await, 0);
    }

    #[tokio::test]
    async fn send_error_routes_to_target_connection() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());
        let request_id = ConnectionRequestId {
            connection_id: ConnectionId(9),
            request_id: RequestId::Integer(3),
        };
        let error = internal_error("boom");

        outgoing.send_error(request_id.clone(), error.clone()).await;

        let envelope = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("should receive envelope before timeout")
            .expect("channel should contain one message");

        match envelope {
            OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                ..
            } => {
                assert_eq!(connection_id, ConnectionId(9));
                let OutgoingMessage::Error(outgoing_error) = message else {
                    panic!("expected error message");
                };
                assert_eq!(outgoing_error.id, RequestId::Integer(3));
                assert_eq!(outgoing_error.error, error);
            }
            other => panic!("expected targeted error envelope, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_server_notification_to_connections_reuses_timestamp() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(2);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());

        outgoing
            .send_server_notification_to_connections(
                &[ConnectionId(1), ConnectionId(2)],
                ServerNotification::ConfigWarning(ConfigWarningNotification {
                    summary: "test".to_string(),
                    details: None,
                    path: None,
                    range: None,
                }),
            )
            .await;

        let timestamps = [
            rx.recv()
                .await
                .expect("first connection should receive notification"),
            rx.recv()
                .await
                .expect("second connection should receive notification"),
        ]
        .map(|envelope| match envelope {
            OutgoingEnvelope::ToConnection {
                message: OutgoingMessage::AppServerNotification(envelope),
                ..
            } => envelope.emitted_at_ms,
            _ => panic!("expected targeted server notification"),
        });

        assert_eq!(timestamps[0], timestamps[1]);
    }

    #[tokio::test]
    async fn thread_scoped_notifications_include_one_sequence_per_fanout() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(2);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());
        let thread_id = ThreadId::new();

        outgoing
            .send_server_notification_to_connections(
                &[ConnectionId(1), ConnectionId(2)],
                ServerNotification::ThreadStatusChanged(ThreadStatusChangedNotification {
                    thread_id: thread_id.to_string(),
                    status: ThreadStatus::Idle,
                }),
            )
            .await;

        let envelopes = [
            rx.recv()
                .await
                .expect("first connection should receive notification"),
            rx.recv()
                .await
                .expect("second connection should receive notification"),
        ]
        .map(|envelope| match envelope {
            OutgoingEnvelope::ToConnection {
                message: OutgoingMessage::AppServerNotification(envelope),
                ..
            } => envelope,
            _ => panic!("expected targeted server notification"),
        });

        assert_eq!(envelopes[0].thread_sequence, Some(1));
        assert_eq!(envelopes[1].thread_sequence, Some(1));
        assert_eq!(outgoing.thread_sequence(thread_id), 1);
    }

    #[tokio::test]
    async fn thread_scoped_server_requests_include_sequence_in_pending_state() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(1);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
            outgoing.clone(),
            vec![ConnectionId(1)],
            thread_id,
        );

        let (request_id, _waiter) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;

        let OutgoingEnvelope::ToConnection {
            message: OutgoingMessage::SequencedRequest(envelope),
            ..
        } = rx.recv().await.expect("request should be sent")
        else {
            panic!("expected sequenced request");
        };
        assert_eq!(envelope.request.id(), &request_id);
        assert_eq!(envelope.thread_sequence, Some(1));

        let pending_request = outgoing
            .pending_server_request(&request_id)
            .await
            .expect("request should stay pending");
        assert_eq!(pending_request.thread_id, Some(thread_id));
        assert_eq!(pending_request.thread_sequence, Some(1));
        assert_eq!(outgoing.thread_sequence(thread_id), 1);
    }

    #[tokio::test]
    async fn send_server_notification_to_connection_and_wait_tracks_write_completion() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());
        let send_task = tokio::spawn(async move {
            outgoing
                .send_server_notification_to_connection_and_wait(
                    ConnectionId(42),
                    ServerNotification::ModelRerouted(ModelReroutedNotification {
                        thread_id: "thread-1".to_string(),
                        turn_id: "turn-1".to_string(),
                        from_model: "gpt-5.3-codex".to_string(),
                        to_model: "gpt-5.2".to_string(),
                        reason: ModelRerouteReason::HighRiskCyberActivity,
                    }),
                )
                .await
        });

        let envelope = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("should receive envelope before timeout")
            .expect("channel should contain one message");
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = envelope
        else {
            panic!("expected targeted server notification envelope");
        };
        assert_eq!(connection_id, ConnectionId(42));
        let OutgoingMessage::AppServerNotification(envelope) = message else {
            panic!("expected app-server notification");
        };
        assert!(
            envelope
                .emitted_at_ms
                .is_some_and(|emitted_at_ms| emitted_at_ms > 0)
        );
        write_complete_tx
            .expect("write completion sender should be attached")
            .complete();

        timeout(Duration::from_secs(1), send_task)
            .await
            .expect("send task should finish after write completion is signaled")
            .expect("send task should not panic");
    }

    #[tokio::test]
    async fn connection_closed_clears_registered_request_contexts() {
        let (tx, _rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());
        let closed_connection_request = ConnectionRequestId {
            connection_id: ConnectionId(9),
            request_id: RequestId::Integer(3),
        };
        let open_connection_request = ConnectionRequestId {
            connection_id: ConnectionId(10),
            request_id: RequestId::Integer(4),
        };

        outgoing
            .register_request_context(RequestContext::new(
                closed_connection_request,
                tracing::info_span!("app_server.request", rpc.method = "turn/interrupt"),
                /*parent_trace*/ None,
            ))
            .await;
        outgoing
            .register_request_context(RequestContext::new(
                open_connection_request,
                tracing::info_span!("app_server.request", rpc.method = "turn/start"),
                /*parent_trace*/ None,
            ))
            .await;
        assert_eq!(outgoing.request_context_count().await, 2);

        outgoing.connection_closed(ConnectionId(9)).await;

        assert_eq!(outgoing.request_context_count().await, 1);
    }

    #[tokio::test]
    async fn notify_client_error_forwards_error_to_waiter() {
        let (tx, _rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());

        let (request_id, wait_for_result) = outgoing
            .send_request(ServerRequestPayload::ApplyPatchApproval(
                ApplyPatchApprovalParams {
                    conversation_id: ThreadId::new(),
                    call_id: "call-id".to_string(),
                    file_changes: HashMap::new(),
                    reason: None,
                    grant_root: None,
                },
            ))
            .await;

        let error = internal_error("refresh failed");

        outgoing
            .notify_client_error_from_connection(ConnectionId(1), request_id, error.clone())
            .await;

        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback");
        assert_eq!(result, Err(error));
    }

    #[tokio::test]
    async fn server_request_response_ignores_non_recipient_connection() {
        let (tx, _rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());

        let (request_id, mut wait_for_result) = outgoing
            .send_request_to_connections(
                Some(&[ConnectionId(1)]),
                ServerRequestPayload::ApplyPatchApproval(ApplyPatchApprovalParams {
                    conversation_id: ThreadId::new(),
                    call_id: "call-id".to_string(),
                    file_changes: HashMap::new(),
                    reason: None,
                    grant_root: None,
                }),
                /*thread_id*/ None,
            )
            .await;

        outgoing
            .notify_client_error_from_connection(
                ConnectionId(2),
                request_id.clone(),
                internal_error("wrong connection"),
            )
            .await;
        assert!(
            timeout(Duration::from_millis(10), &mut wait_for_result)
                .await
                .is_err()
        );

        let error = internal_error("target connection");
        outgoing
            .notify_client_error_from_connection(ConnectionId(1), request_id, error.clone())
            .await;
        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback");
        assert_eq!(result, Err(error));
    }

    #[tokio::test]
    async fn rebind_requests_for_thread_moves_resolution_to_new_connection() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing =
            OutgoingMessageSender::new(tx, codex_analytics::AnalyticsEventsClient::disabled());
        let thread_id = ThreadId::new();

        let (request_id, mut wait_for_result) = outgoing
            .send_request_to_connections(
                Some(&[ConnectionId(1)]),
                command_execution_request_approval(thread_id),
                Some(thread_id),
            )
            .await;

        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = rx.recv().await.expect("initial request should be sent")
        else {
            panic!("expected initial request envelope");
        };
        let (initial_request, initial_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(1));
        assert_eq!(initial_request.id(), &request_id);
        assert_eq!(initial_thread_sequence, Some(1));

        outgoing
            .rebind_requests_for_thread_to_connection(thread_id, ConnectionId(2))
            .await;
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = rx.recv().await.expect("rebound request should be sent")
        else {
            panic!("expected rebound request envelope");
        };
        let (rebound_request, rebound_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(2));
        assert_eq!(rebound_request.id(), &request_id);
        assert_eq!(rebound_thread_sequence, Some(2));

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(1),
                request_id.clone(),
                json!({ "decision": "accept" }),
            )
            .await;
        assert!(
            timeout(Duration::from_millis(10), &mut wait_for_result)
                .await
                .is_err()
        );

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(2),
                request_id,
                json!({ "decision": "accept" }),
            )
            .await;
        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback")
            .expect("authorized response should resolve successfully");
        assert_eq!(result, json!({ "decision": "accept" }));
    }

    #[tokio::test]
    async fn externally_rebinds_prompt_already_delivered_to_tui() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
            Arc::clone(&outgoing),
            vec![ConnectionId(1)],
            thread_id,
        );

        let (request_id, mut wait_for_result) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx.recv().await.expect("TUI request should be sent")
        else {
            panic!("expected TUI request envelope");
        };
        let (tui_request, tui_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(1));
        assert_eq!(tui_request.id(), &request_id);
        assert_eq!(tui_thread_sequence, Some(1));
        assert!(write_complete_tx.is_none());

        outgoing
            .rebind_requests_for_thread_to_external_controller_connection(
                thread_id,
                ConnectionId(2),
                Some(ConnectionId(1)),
                /*owner_epoch*/ 1,
            )
            .await;
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx
            .recv()
            .await
            .expect("controller redelivery should be sent")
        else {
            panic!("expected controller request envelope");
        };
        let (controller_request, controller_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(2));
        assert_eq!(controller_request.id(), &request_id);
        assert_eq!(controller_thread_sequence, Some(1));
        assert!(write_complete_tx.is_some());

        outgoing
            .rebind_requests_for_thread_to_external_controller_connection(
                thread_id,
                ConnectionId(2),
                Some(ConnectionId(1)),
                /*owner_epoch*/ 1,
            )
            .await;
        assert!(timeout(Duration::from_millis(10), rx.recv()).await.is_err());

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(1),
                request_id.clone(),
                json!({ "decision": "accept" }),
            )
            .await;
        assert!(
            timeout(Duration::from_millis(10), &mut wait_for_result)
                .await
                .is_err()
        );

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(2),
                request_id,
                json!({ "decision": "accept" }),
            )
            .await;
        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback")
            .expect("controller response should resolve successfully");
        assert_eq!(result, json!({ "decision": "accept" }));
    }

    #[tokio::test]
    async fn external_rebind_replays_only_eligible_pending_prompt_variants() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(8);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
            Arc::clone(&outgoing),
            vec![ConnectionId(1)],
            thread_id,
        );

        let (command_approval_id, _command_waiter) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;

        let (dynamic_tool_id, _dynamic_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::DynamicToolCall(
                DynamicToolCallParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    call_id: "call-1".to_string(),
                    namespace: None,
                    tool: "tool".to_string(),
                    arguments: json!({}),
                },
            ))
            .await;
        let (mcp_elicitation_id, _mcp_elicitation_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::McpServerElicitationRequest(
                McpServerElicitationRequestParams {
                    thread_id: thread_id.to_string(),
                    turn_id: None,
                    server_name: "test-mcp".to_string(),
                    request: McpServerElicitationRequest::Url {
                        meta: None,
                        message: "Open this page".to_string(),
                        url: "https://example.test".to_string(),
                        elicitation_id: "elicitation-1".to_string(),
                    },
                },
            ))
            .await;
        let (user_input_id, _user_input_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::ToolRequestUserInput(
                ToolRequestUserInputParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-input".to_string(),
                    questions: vec![],
                    is_blocking: true,
                    auto_resolution_ms: None,
                },
            ))
            .await;
        let (permissions_id, _permissions_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::PermissionsRequestApproval(
                PermissionsRequestApprovalParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-permissions".to_string(),
                    environment_id: None,
                    started_at_ms: 0,
                    cwd: AbsolutePathBuf::try_from(std::env::temp_dir())
                        .expect("temporary directory should be absolute"),
                    reason: None,
                    permissions: RequestPermissionProfile {
                        network: None,
                        file_system: None,
                    },
                },
            ))
            .await;
        let (file_change_id, _file_change_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::FileChangeRequestApproval(
                FileChangeRequestApprovalParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-file-change".to_string(),
                    started_at_ms: 0,
                    reason: None,
                    grant_root: None,
                },
            ))
            .await;

        for _ in 0..6 {
            let envelope = rx.recv().await.expect("TUI prompt should be sent");
            let OutgoingEnvelope::ToConnection { connection_id, .. } = envelope else {
                panic!("expected TUI prompt envelope");
            };
            assert_eq!(connection_id, ConnectionId(1));
        }

        outgoing
            .rebind_requests_for_thread_to_external_controller_connection(
                thread_id,
                ConnectionId(2),
                Some(ConnectionId(1)),
                /*owner_epoch*/ 1,
            )
            .await;

        let mut replayed_ids = Vec::new();
        let mut write_completions = Vec::new();
        for _ in 0..4 {
            let envelope = rx
                .recv()
                .await
                .expect("controller prompt replay should be sent");
            let OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                write_complete_tx,
            } = envelope
            else {
                panic!("expected controller prompt envelope");
            };
            assert_eq!(connection_id, ConnectionId(2));
            replayed_ids.push(request_from_message(message).0.id().clone());
            write_completions.push(
                write_complete_tx.expect("controller prompt replay should track write completion"),
            );
        }
        replayed_ids.sort();
        let mut expected_replayed_ids = vec![
            command_approval_id,
            user_input_id,
            permissions_id,
            file_change_id,
        ];
        expected_replayed_ids.sort();
        assert_eq!(replayed_ids, expected_replayed_ids);
        assert!(!replayed_ids.contains(&dynamic_tool_id));
        assert!(!replayed_ids.contains(&mcp_elicitation_id));
        for write_completion in write_completions {
            assert!(write_completion.begin_write());
            write_completion.complete();
        }
        assert!(timeout(Duration::from_millis(10), rx.recv()).await.is_err());

        outgoing
            .rebind_transferred_requests_for_thread_to_connection(thread_id, ConnectionId(1))
            .await;
        let mut rebound_ids = Vec::new();
        for _ in 0..4 {
            let envelope = rx
                .recv()
                .await
                .expect("only transferred prompts should rebind to the TUI");
            let OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                write_complete_tx,
            } = envelope
            else {
                panic!("expected TUI prompt envelope");
            };
            assert_eq!(connection_id, ConnectionId(1));
            assert!(write_complete_tx.is_none());
            rebound_ids.push(request_from_message(message).0.id().clone());
        }
        rebound_ids.sort();
        assert_eq!(rebound_ids, expected_replayed_ids);
        assert!(timeout(Duration::from_millis(10), rx.recv()).await.is_err());

        outgoing
            .rebind_requests_for_thread_to_external_controller_connection(
                thread_id,
                ConnectionId(2),
                Some(ConnectionId(1)),
                /*owner_epoch*/ 2,
            )
            .await;
        let mut reacquired_ids = Vec::new();
        for _ in 0..4 {
            let OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                write_complete_tx,
            } = rx
                .recv()
                .await
                .expect("each recovered prompt should redeliver once in the next epoch")
            else {
                panic!("expected controller prompt envelope");
            };
            assert_eq!(connection_id, ConnectionId(2));
            reacquired_ids.push(request_from_message(message).0.id().clone());
            let write_completion =
                write_complete_tx.expect("controller prompt redelivery should track its write");
            assert!(write_completion.begin_write());
            write_completion.complete();
        }
        reacquired_ids.sort();
        assert_eq!(reacquired_ids, expected_replayed_ids);

        outgoing
            .rebind_requests_for_thread_to_external_controller_connection(
                thread_id,
                ConnectionId(2),
                Some(ConnectionId(1)),
                /*owner_epoch*/ 2,
            )
            .await;
        assert!(timeout(Duration::from_millis(10), rx.recv()).await.is_err());
    }

    #[tokio::test]
    async fn external_rebind_skips_resolved_and_cancelled_prompts() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
            Arc::clone(&outgoing),
            vec![ConnectionId(1)],
            thread_id,
        );

        let (resolved_id, resolved_waiter) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;
        let (cancelled_id, cancelled_waiter) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;
        for _ in 0..2 {
            rx.recv().await.expect("TUI request should be sent");
        }

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(1),
                resolved_id.clone(),
                json!({ "decision": "accept" }),
            )
            .await;
        assert_eq!(
            resolved_waiter
                .await
                .expect("resolved callback should complete")
                .expect("resolved callback should succeed"),
            json!({ "decision": "accept" })
        );
        assert!(outgoing.cancel_request(&cancelled_id).await);
        assert!(cancelled_waiter.await.is_err());

        outgoing
            .rebind_requests_for_thread_to_external_controller_connection(
                thread_id,
                ConnectionId(2),
                Some(ConnectionId(1)),
                /*owner_epoch*/ 1,
            )
            .await;
        assert!(timeout(Duration::from_millis(10), rx.recv()).await.is_err());
        assert!(
            outgoing
                .pending_server_request(&resolved_id)
                .await
                .is_none()
        );
        assert!(
            outgoing
                .pending_server_request(&cancelled_id)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn controller_owned_excluded_prompts_stay_with_tui_fallback() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            Arc::clone(&outgoing),
            ServerRequestRecipients::external_controller_with_fallback(
                ConnectionId(2),
                Some(ConnectionId(1)),
                /*owner_epoch*/ 1,
            ),
            vec![ConnectionId(1), ConnectionId(2)],
            thread_id,
        );

        let (dynamic_tool_id, _dynamic_tool_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::DynamicToolCall(
                DynamicToolCallParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    call_id: "call-1".to_string(),
                    namespace: None,
                    tool: "tool".to_string(),
                    arguments: json!({}),
                },
            ))
            .await;
        let (mcp_elicitation_id, _mcp_elicitation_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::McpServerElicitationRequest(
                McpServerElicitationRequestParams {
                    thread_id: thread_id.to_string(),
                    turn_id: None,
                    server_name: "test-mcp".to_string(),
                    request: McpServerElicitationRequest::Url {
                        meta: None,
                        message: "Open this page".to_string(),
                        url: "https://example.test".to_string(),
                        elicitation_id: "elicitation-1".to_string(),
                    },
                },
            ))
            .await;

        let mut tui_request_ids = Vec::new();
        for _ in 0..2 {
            let envelope = rx
                .recv()
                .await
                .expect("excluded prompt should be delivered to the TUI");
            let OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                write_complete_tx,
            } = envelope
            else {
                panic!("expected TUI prompt envelope");
            };
            assert_eq!(connection_id, ConnectionId(1));
            assert!(write_complete_tx.is_none());
            tui_request_ids.push(request_from_message(message).0.id().clone());
        }
        tui_request_ids.sort();
        let mut expected_request_ids = vec![dynamic_tool_id, mcp_elicitation_id];
        expected_request_ids.sort();
        assert_eq!(tui_request_ids, expected_request_ids);
        assert!(timeout(Duration::from_millis(10), rx.recv()).await.is_err());
    }

    #[tokio::test]
    async fn external_controller_request_rebinds_before_external_delivery() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            outgoing.clone(),
            ServerRequestRecipients::external_controller_with_fallback(
                ConnectionId(2),
                None,
                /*owner_epoch*/ 1,
            ),
            vec![ConnectionId(1), ConnectionId(2)],
            thread_id,
        );

        let (request_id, mut wait_for_result) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;

        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx.recv().await.expect("initial request should be sent")
        else {
            panic!("expected initial request envelope");
        };
        let (initial_request, initial_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(2));
        assert_eq!(initial_request.id(), &request_id);
        assert_eq!(initial_thread_sequence, Some(1));
        let write_complete_tx =
            write_complete_tx.expect("external controller request should track write completion");
        assert!(write_complete_tx.is_write_permitted());

        outgoing
            .rebind_requests_for_thread_to_connection(thread_id, ConnectionId(1))
            .await;
        assert!(!write_complete_tx.is_write_permitted());
        drop(write_complete_tx);
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx.recv().await.expect("rebound request should be sent")
        else {
            panic!("expected rebound request envelope");
        };
        let (rebound_request, rebound_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(1));
        assert_eq!(rebound_request.id(), &request_id);
        assert_eq!(rebound_thread_sequence, Some(2));
        assert!(write_complete_tx.is_none());

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(2),
                request_id.clone(),
                json!({ "decision": "accept" }),
            )
            .await;
        assert!(
            timeout(Duration::from_millis(10), &mut wait_for_result)
                .await
                .is_err()
        );

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(1),
                request_id,
                json!({ "decision": "accept" }),
            )
            .await;
        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback")
            .expect("authorized response should resolve successfully");
        assert_eq!(result, json!({ "decision": "accept" }));
    }

    #[tokio::test]
    async fn external_controller_write_failure_rebinds_to_fallback_before_external_delivery() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            outgoing.clone(),
            ServerRequestRecipients::external_controller_with_fallback(
                ConnectionId(2),
                Some(ConnectionId(1)),
                /*owner_epoch*/ 1,
            ),
            vec![ConnectionId(1), ConnectionId(2)],
            thread_id,
        );

        let (request_id, mut wait_for_result) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;

        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx.recv().await.expect("initial request should be sent")
        else {
            panic!("expected initial request envelope");
        };
        let (initial_request, initial_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(2));
        assert_eq!(initial_request.id(), &request_id);
        assert_eq!(initial_thread_sequence, Some(1));
        let external_write_complete_tx =
            write_complete_tx.expect("external controller request should track write completion");
        assert!(external_write_complete_tx.begin_write());
        drop(external_write_complete_tx);

        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("fallback rebind should not time out")
            .expect("fallback request should be sent")
        else {
            panic!("expected fallback request envelope");
        };
        let (rebound_request, rebound_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(1));
        assert_eq!(rebound_request.id(), &request_id);
        assert_eq!(rebound_thread_sequence, Some(1));
        assert!(write_complete_tx.is_none());

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(2),
                request_id.clone(),
                json!({ "decision": "accept" }),
            )
            .await;
        assert!(
            timeout(Duration::from_millis(10), &mut wait_for_result)
                .await
                .is_err()
        );

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(1),
                request_id,
                json!({ "decision": "accept" }),
            )
            .await;
        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback")
            .expect("authorized response should resolve successfully");
        assert_eq!(result, json!({ "decision": "accept" }));
    }

    #[tokio::test]
    async fn external_controller_queue_overflow_rebinds_to_fallback_before_external_delivery() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            outgoing.clone(),
            ServerRequestRecipients::external_controller_with_fallback(
                ConnectionId(2),
                Some(ConnectionId(1)),
                /*owner_epoch*/ 1,
            ),
            vec![ConnectionId(1), ConnectionId(2)],
            thread_id,
        );

        let (request_id, mut wait_for_result) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;

        let initial_envelope = rx.recv().await.expect("initial request should be sent");
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = &initial_envelope
        else {
            panic!("expected initial request envelope");
        };
        let (initial_request, initial_thread_sequence) = request_from_message_ref(message);
        assert_eq!(*connection_id, ConnectionId(2));
        assert_eq!(initial_request.id(), &request_id);
        assert_eq!(initial_thread_sequence, Some(1));

        let (external_writer_tx, mut external_writer_rx) = mpsc::channel(1);
        external_writer_tx
            .try_send(QueuedOutgoingMessage::new(message.clone()))
            .expect("external writer should accept its initial buffered request");
        let external_disconnect_token = CancellationToken::new();
        let mut connections = HashMap::new();
        connections.insert(
            ConnectionId(2),
            OutboundConnectionState::new_with_origin(
                ConnectionOrigin::ExternalController,
                external_writer_tx,
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(true)),
                Arc::new(RwLock::new(HashSet::new())),
                Some(external_disconnect_token.clone()),
            ),
        );

        route_outgoing_envelope(&mut connections, initial_envelope).await;

        assert!(!connections.contains_key(&ConnectionId(2)));
        assert!(external_disconnect_token.is_cancelled());
        let retained_request = external_writer_rx
            .try_recv()
            .expect("external queue should retain only its buffered request");
        let (request, retained_thread_sequence) = request_from_message(retained_request.message);
        assert_eq!(request.id(), &request_id);
        assert_eq!(retained_thread_sequence, Some(1));
        assert!(external_writer_rx.try_recv().is_err());

        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("fallback rebind should not time out")
            .expect("fallback request should be sent")
        else {
            panic!("expected fallback request envelope");
        };
        let (rebound_request, rebound_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(1));
        assert_eq!(rebound_request.id(), &request_id);
        assert_eq!(rebound_thread_sequence, Some(1));
        assert!(write_complete_tx.is_none());

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(2),
                request_id.clone(),
                json!({ "decision": "accept" }),
            )
            .await;
        assert!(
            timeout(Duration::from_millis(10), &mut wait_for_result)
                .await
                .is_err()
        );

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(1),
                request_id,
                json!({ "decision": "accept" }),
            )
            .await;
        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback")
            .expect("authorized response should resolve successfully");
        assert_eq!(result, json!({ "decision": "accept" }));
    }

    #[tokio::test]
    async fn external_controller_request_rebinds_after_external_delivery() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            outgoing.clone(),
            ServerRequestRecipients::external_controller_with_fallback(
                ConnectionId(2),
                None,
                /*owner_epoch*/ 1,
            ),
            vec![ConnectionId(1), ConnectionId(2)],
            thread_id,
        );

        let (request_id, wait_for_result) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;

        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx.recv().await.expect("initial request should be sent")
        else {
            panic!("expected initial request envelope");
        };
        let (initial_request, initial_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(2));
        assert_eq!(initial_request.id(), &request_id);
        assert_eq!(initial_thread_sequence, Some(1));
        let external_write_complete_tx =
            write_complete_tx.expect("external controller request should track write completion");
        assert!(external_write_complete_tx.begin_write());
        external_write_complete_tx.complete();
        timeout(Duration::from_secs(1), async {
            loop {
                if outgoing
                    .request_has_external_delivery(&request_id, ConnectionId(2))
                    .await
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("external delivery should be recorded");

        outgoing
            .rebind_requests_for_thread_to_connection(thread_id, ConnectionId(1))
            .await;
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx.recv().await.expect("rebound request should be sent")
        else {
            panic!("expected rebound request envelope");
        };
        let (rebound_request, rebound_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(1));
        assert_eq!(rebound_request.id(), &request_id);
        assert_eq!(rebound_thread_sequence, Some(2));
        assert!(write_complete_tx.is_none());

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(1),
                request_id.clone(),
                json!({ "decision": "accept" }),
            )
            .await;
        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback")
            .expect("authorized response should resolve successfully");
        assert_eq!(result, json!({ "decision": "accept" }));
    }

    #[tokio::test]
    async fn external_controller_request_rebinds_after_write_begins() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            outgoing.clone(),
            ServerRequestRecipients::external_controller_with_fallback(
                ConnectionId(2),
                None,
                /*owner_epoch*/ 1,
            ),
            vec![ConnectionId(1), ConnectionId(2)],
            thread_id,
        );

        let (request_id, wait_for_result) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;

        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx.recv().await.expect("initial request should be sent")
        else {
            panic!("expected initial request envelope");
        };
        let (initial_request, initial_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(2));
        assert_eq!(initial_request.id(), &request_id);
        assert_eq!(initial_thread_sequence, Some(1));
        let external_write_complete_tx =
            write_complete_tx.expect("external controller request should track write completion");
        assert!(external_write_complete_tx.begin_write());

        outgoing
            .rebind_requests_for_thread_to_connection(thread_id, ConnectionId(1))
            .await;
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx.recv().await.expect("rebound request should be sent")
        else {
            panic!("expected rebound request envelope");
        };
        let (rebound_request, rebound_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(1));
        assert_eq!(rebound_request.id(), &request_id);
        assert_eq!(rebound_thread_sequence, Some(2));
        assert!(write_complete_tx.is_none());
        external_write_complete_tx.complete();

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(1),
                request_id.clone(),
                json!({ "decision": "accept" }),
            )
            .await;
        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback")
            .expect("authorized response should resolve successfully");
        assert_eq!(result, json!({ "decision": "accept" }));
    }

    #[tokio::test]
    async fn external_controller_request_is_not_replayed_after_external_delivery() {
        let (tx, mut rx) = mpsc::channel::<OutgoingEnvelope>(4);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            outgoing.clone(),
            ServerRequestRecipients::external_controller_with_fallback(
                ConnectionId(2),
                None,
                /*owner_epoch*/ 1,
            ),
            vec![ConnectionId(1), ConnectionId(2)],
            thread_id,
        );

        let (request_id, mut wait_for_result) = thread_outgoing
            .send_request(command_execution_request_approval(thread_id))
            .await;

        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = rx.recv().await.expect("initial request should be sent")
        else {
            panic!("expected initial request envelope");
        };
        let (initial_request, initial_thread_sequence) = request_from_message(message);
        assert_eq!(connection_id, ConnectionId(2));
        assert_eq!(initial_request.id(), &request_id);
        assert_eq!(initial_thread_sequence, Some(1));
        let write_complete_tx =
            write_complete_tx.expect("external controller request should track write completion");
        assert!(write_complete_tx.begin_write());
        write_complete_tx.complete();
        timeout(Duration::from_secs(1), async {
            loop {
                if outgoing
                    .request_has_external_delivery(&request_id, ConnectionId(2))
                    .await
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("external delivery should be recorded");

        outgoing
            .replay_requests_to_connection_for_thread(ConnectionId(1), thread_id)
            .await;
        assert!(timeout(Duration::from_millis(10), rx.recv()).await.is_err());

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(1),
                request_id.clone(),
                json!({ "decision": "accept" }),
            )
            .await;
        assert!(
            timeout(Duration::from_millis(10), &mut wait_for_result)
                .await
                .is_err()
        );

        outgoing
            .notify_client_response_from_connection(
                ConnectionId(2),
                request_id,
                json!({ "decision": "accept" }),
            )
            .await;
        let result = timeout(Duration::from_secs(1), wait_for_result)
            .await
            .expect("wait should not time out")
            .expect("waiter should receive a callback")
            .expect("authorized response should resolve successfully");
        assert_eq!(result, json!({ "decision": "accept" }));
    }

    fn request_from_message(message: OutgoingMessage) -> (ServerRequest, Option<u64>) {
        match message {
            OutgoingMessage::Request(request) => (request, None),
            OutgoingMessage::SequencedRequest(envelope) => {
                (envelope.request, envelope.thread_sequence)
            }
            OutgoingMessage::AppServerNotification(_)
            | OutgoingMessage::Response(_)
            | OutgoingMessage::Error(_) => panic!("expected server request"),
        }
    }

    fn request_from_message_ref(message: &OutgoingMessage) -> (&ServerRequest, Option<u64>) {
        match message {
            OutgoingMessage::Request(request) => (request, None),
            OutgoingMessage::SequencedRequest(envelope) => {
                (&envelope.request, envelope.thread_sequence)
            }
            OutgoingMessage::AppServerNotification(_)
            | OutgoingMessage::Response(_)
            | OutgoingMessage::Error(_) => panic!("expected server request"),
        }
    }

    fn command_execution_request_approval(thread_id: ThreadId) -> ServerRequestPayload {
        ServerRequestPayload::CommandExecutionRequestApproval(
            CommandExecutionRequestApprovalParams {
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
            },
        )
    }

    #[tokio::test]
    async fn pending_requests_for_thread_returns_thread_requests_in_request_id_order() {
        let (tx, _rx) = mpsc::channel::<OutgoingEnvelope>(8);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
            outgoing.clone(),
            vec![ConnectionId(1)],
            thread_id,
        );

        let (dynamic_tool_request_id, _dynamic_tool_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::DynamicToolCall(
                DynamicToolCallParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    call_id: "call-0".to_string(),
                    namespace: None,
                    tool: "tool".to_string(),
                    arguments: json!({}),
                },
            ))
            .await;
        let (first_request_id, _first_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::ToolRequestUserInput(
                ToolRequestUserInputParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "call-1".to_string(),
                    questions: vec![],
                    is_blocking: true,
                    auto_resolution_ms: None,
                },
            ))
            .await;
        let (second_request_id, _second_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::FileChangeRequestApproval(
                FileChangeRequestApprovalParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "call-2".to_string(),
                    started_at_ms: 0,
                    reason: None,
                    grant_root: None,
                },
            ))
            .await;
        let (last_sequence, pending_requests) = outgoing
            .thread_sequence_and_pending_requests_for_thread(thread_id)
            .await;
        assert_eq!(last_sequence, 3);
        assert_eq!(
            pending_requests
                .iter()
                .map(|request| request.request.id())
                .collect::<Vec<_>>(),
            vec![
                &dynamic_tool_request_id,
                &first_request_id,
                &second_request_id
            ]
        );
    }

    #[tokio::test]
    async fn cancel_requests_for_thread_cancels_all_thread_requests() {
        let (tx, _rx) = mpsc::channel::<OutgoingEnvelope>(8);
        let outgoing = Arc::new(OutgoingMessageSender::new(
            tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let thread_id = ThreadId::new();
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new(
            outgoing.clone(),
            vec![ConnectionId(1)],
            thread_id,
        );

        let (_dynamic_tool_request_id, dynamic_tool_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::DynamicToolCall(
                DynamicToolCallParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    call_id: "call-0".to_string(),
                    namespace: None,
                    tool: "tool".to_string(),
                    arguments: json!({}),
                },
            ))
            .await;
        let (_request_id, user_input_waiter) = thread_outgoing
            .send_request(ServerRequestPayload::ToolRequestUserInput(
                ToolRequestUserInputParams {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "call-1".to_string(),
                    questions: vec![],
                    is_blocking: true,
                    auto_resolution_ms: None,
                },
            ))
            .await;
        let error = internal_error("tracked request cancelled");

        outgoing
            .cancel_requests_for_thread(thread_id, Some(error.clone()))
            .await;

        let dynamic_tool_result = timeout(Duration::from_secs(1), dynamic_tool_waiter)
            .await
            .expect("dynamic tool waiter should resolve")
            .expect("dynamic tool waiter should receive a callback");
        let user_input_result = timeout(Duration::from_secs(1), user_input_waiter)
            .await
            .expect("user input waiter should resolve")
            .expect("user input waiter should receive a callback");
        assert_eq!(dynamic_tool_result, Err(error.clone()));
        assert_eq!(user_input_result, Err(error));
        assert!(
            outgoing
                .pending_requests_for_thread(thread_id)
                .await
                .is_empty()
        );
    }
}
