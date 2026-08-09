//! In-process app-server runtime host for local embedders.
//!
//! This module runs the existing [`MessageProcessor`] and outbound routing logic
//! on Tokio tasks, but replaces socket/stdio transports with bounded in-memory
//! channels. The intent is to preserve app-server semantics while avoiding a
//! process boundary for CLI surfaces that run in the same process.
//!
//! # Lifecycle
//!
//! 1. Construct runtime state with [`InProcessStartArgs`].
//! 2. Call [`start`], which performs the `initialize` / `initialized` handshake
//!    internally and returns a ready-to-use [`InProcessClientHandle`].
//! 3. Send requests via [`InProcessClientHandle::request`], notifications via
//!    [`InProcessClientHandle::notify`], and consume events via
//!    [`InProcessClientHandle::next_event`].
//! 4. Terminate with [`InProcessClientHandle::shutdown`].
//!
//! # Transport model
//!
//! The runtime is transport-local but not protocol-free. Incoming requests are
//! typed [`ClientRequest`] values, yet responses still come back through the
//! same JSON-RPC result envelope that `MessageProcessor` uses for stdio and
//! websocket transports. This keeps in-process behavior aligned with
//! app-server rather than creating a second execution contract.
//!
//! # Backpressure
//!
//! Command submission uses `try_send` and can return `WouldBlock`, while event
//! fanout may drop non-lossless notifications under saturation. Transcript,
//! terminal, and controller ownership events block for delivery so the TUI does
//! not render a corrupt or stale thread. Server requests are never silently
//! abandoned: if they cannot be queued they are failed back into
//! `MessageProcessor` with overload or internal errors so approval flows do not
//! hang indefinitely.
//!
//! # Relationship to `codex-app-server-client`
//!
//! This module provides the low-level runtime handle ([`InProcessClientHandle`]).
//! Higher-level callers (TUI, exec) should go through `codex-app-server-client`,
//! which wraps this module behind a worker task with async request/response
//! helpers, surface-specific startup policy, and bounded shutdown.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::analytics_utils::analytics_events_client_from_config;
use crate::config_manager::ConfigManager;
#[cfg(test)]
use crate::controller_enrollment::ControllerCredentialProof;
use crate::controller_enrollment::EmptyControllerEnrollmentSource;
pub use crate::controller_native_approval::InProcessControllerParticipationRequest;
use crate::controller_native_approval::NativeControllerParticipationApprover;
pub use crate::controller_native_approval::NativeControllerParticipationDecision;
use crate::controller_native_approval::NativeControllerParticipationRequest;
pub use crate::controller_native_approval::NativeControllerParticipationRequestId;
pub use crate::controller_session::ControllerOwnershipStatus as InProcessControllerOwnershipStatus;
pub use crate::controller_session::ControllerOwnershipStatusOwner as InProcessControllerOwnershipStatusOwner;
use crate::error_code::OVERLOADED_ERROR_CODE;
use crate::error_code::internal_error;
use crate::error_code::invalid_request;
use crate::message_processor::MessageProcessor;
use crate::message_processor::MessageProcessorArgs;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;
use crate::outgoing_message::QueuedOutgoingMessage;
use crate::transport::CHANNEL_CAPACITY;
use crate::transport::ConnectionOrigin;
use crate::transport::ConnectionState;
use crate::transport::OutboundConnectionState;
use crate::transport::TransportEvent;
use crate::transport::route_outgoing_envelope;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::ClientNotification;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::Result;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
pub use codex_app_server_transport::local_controller::LOCAL_CONTROLLER_LAUNCH_NONCE_HEADER;
pub use codex_app_server_transport::local_controller::LocalControllerEndpointMetadata;
pub use codex_app_server_transport::local_controller::LocalControllerEndpointSupport;
pub use codex_app_server_transport::local_controller::LocalControllerUnavailableReason;
pub use codex_app_server_transport::local_controller::local_controller_endpoint_support;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_config::ThreadConfigLoader;
use codex_core::check_execpolicy_for_warnings;
use codex_core::config::Config;
use codex_core::resolve_installation_id;
use codex_exec_server::EnvironmentManager;
use codex_feedback::CodexFeedback;
use codex_login::AuthManager;
use codex_protocol::protocol::SessionSource;
pub use codex_rollout::StateDbHandle;
pub use codex_state::log_db::LogDbLayer;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use toml::Value as TomlValue;
use tracing::warn;

const IN_PROCESS_CONNECTION_ID: ConnectionId = ConnectionId(u64::MAX);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
// Covers both bounded runtime drains plus the analytics client's 25-second best-effort flush.
const SHUTDOWN_ACK_TIMEOUT: Duration = Duration::from_secs(35);
/// Default bounded channel capacity for in-process runtime queues.
pub const DEFAULT_IN_PROCESS_CHANNEL_CAPACITY: usize = CHANNEL_CAPACITY;

/// Optional local endpoint that lets external controllers connect to an
/// embedded in-process app-server.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum InProcessLocalControllerEndpointConfig {
    /// Do not expose a local-controller socket.
    #[default]
    Disabled,
    /// Try to expose a local-controller socket backed by this in-process
    /// runtime. Startup continues without controllers if endpoint setup fails.
    BestEffort {
        /// Main thread known at launch time, if any. Fresh TUI launches usually
        /// start before the main thread exists, so `None` is valid.
        main_thread_id: Option<String>,
    },
    /// Expose a local-controller socket backed by this in-process runtime.
    Enabled {
        /// Main thread known at launch time, if any. Fresh TUI launches usually
        /// start before the main thread exists, so `None` is valid.
        main_thread_id: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InProcessLocalControllerEndpointStatus {
    /// The caller did not request a local-controller endpoint.
    Disabled,
    /// The local-controller endpoint was published successfully.
    Available(LocalControllerEndpointMetadata),
    /// Endpoint setup failed and the runtime continued without controllers.
    Unavailable { reason: String },
}

impl InProcessLocalControllerEndpointStatus {
    pub fn metadata(&self) -> Option<&LocalControllerEndpointMetadata> {
        match self {
            Self::Available(metadata) => Some(metadata),
            Self::Disabled | Self::Unavailable { .. } => None,
        }
    }
}

type PendingClientRequestResponse = std::result::Result<Result, JSONRPCErrorError>;

/// Returns true when an embedded in-process app-server notification must block
/// for delivery instead of using best-effort `try_send`.
///
/// Keep this as the single classifier for both the embedded runtime writer and
/// the app-server-client bridge so transcript, terminal, and controller
/// ownership events cannot be dropped before the TUI can reflect canonical
/// thread state.
pub fn server_notification_requires_delivery(notification: &ServerNotification) -> bool {
    matches!(
        notification,
        ServerNotification::TurnCompleted(_)
            | ServerNotification::ThreadStatusChanged(_)
            | ServerNotification::ThreadArchived(_)
            | ServerNotification::ThreadDeleted(_)
            | ServerNotification::ThreadUnarchived(_)
            | ServerNotification::ThreadClosed(_)
            | ServerNotification::ThreadNameUpdated(_)
            | ServerNotification::ThreadSettingsUpdated(_)
            | ServerNotification::ControllerAuthorizationChanged(_)
            | ServerNotification::ControllerControlOwnershipChanged(_)
            | ServerNotification::ItemCompleted(_)
            | ServerNotification::ExternalAgentConfigImportCompleted(_)
            | ServerNotification::AgentMessageDelta(_)
            | ServerNotification::PlanDelta(_)
            | ServerNotification::ReasoningSummaryTextDelta(_)
            | ServerNotification::ReasoningTextDelta(_)
    )
}

/// Input needed to start an in-process app-server runtime.
///
/// These fields mirror the pieces of ambient process state that stdio and
/// websocket transports normally assemble before `MessageProcessor` starts.
#[derive(Clone)]
pub struct InProcessStartArgs {
    /// Resolved argv0 dispatch paths used by command execution internals.
    pub arg0_paths: Arg0DispatchPaths,
    /// Shared base config used to initialize core components.
    pub config: Arc<Config>,
    /// CLI config overrides that are already parsed into TOML values.
    pub cli_overrides: Vec<(String, TomlValue)>,
    /// Loader override knobs used by config API paths.
    pub loader_overrides: LoaderOverrides,
    /// Whether config API paths should reject unknown config fields.
    pub strict_config: bool,
    /// Preloaded cloud config bundle provider.
    pub cloud_config_bundle: CloudConfigBundleLoader,
    /// Loader used to fetch typed thread config sources before a thread starts.
    pub thread_config_loader: Arc<dyn ThreadConfigLoader>,
    /// Feedback sink used by app-server/core telemetry and logs.
    pub feedback: CodexFeedback,
    /// SQLite tracing layer used to flush recently emitted logs before feedback upload.
    pub log_db: Option<LogDbLayer>,
    /// Process-wide SQLite state handle shared with embedded app-server consumers.
    pub state_db: Option<StateDbHandle>,
    /// Environment manager used by core execution and filesystem operations.
    pub environment_manager: Arc<EnvironmentManager>,
    /// Startup warnings emitted after initialize succeeds.
    pub config_warnings: Vec<ConfigWarningNotification>,
    /// Session source stamped into thread/session metadata.
    pub session_source: SessionSource,
    /// Whether auth loading should honor the `CODEX_API_KEY` environment variable.
    pub enable_codex_api_key_env: bool,
    /// Initialize params used for initial handshake.
    pub initialize: InitializeParams,
    /// Capacity used for all runtime queues (clamped to at least 1).
    pub channel_capacity: usize,
    /// Optional embedded local-controller endpoint startup policy.
    pub local_controller_endpoint: InProcessLocalControllerEndpointConfig,
    #[cfg(test)]
    pub(crate) controller_enrollment_source:
        Arc<dyn crate::controller_enrollment::ControllerEnrollmentSource>,
    #[cfg(test)]
    pub(crate) controller_credential_proof_factory:
        Option<Arc<dyn Fn(ConnectionId) -> ControllerCredentialProof + Send + Sync>>,
}

/// Event emitted from the app-server to the in-process client.
///
/// [`Lagged`](Self::Lagged) is a transport health marker, not an application
/// event — it signals that the consumer fell behind and some events were dropped.
#[derive(Debug, Clone)]
pub enum InProcessServerEvent {
    /// Local-controller participation request that requires owning TUI approval.
    ControllerParticipationRequest(Box<InProcessControllerParticipationRequest>),
    /// Controller input-ownership status update for the owning in-process TUI.
    ControllerOwnershipStatus(Box<InProcessControllerOwnershipStatus>),
    /// Local-controller endpoint failed after startup; controllers are no
    /// longer available for this embedded launch.
    LocalControllerEndpointUnavailable { reason: String },
    /// Server request that requires client response/rejection.
    ServerRequest(Box<ServerRequest>),
    /// App-server notification directed to the embedded client.
    ServerNotification(Box<ServerNotification>),
    /// Indicates one or more events were dropped due to backpressure.
    Lagged { skipped: usize },
}

/// Internal message sent from [`InProcessClientHandle`] methods to the runtime task.
///
/// Requests carry a oneshot sender for the response; notifications and server-request
/// replies are fire-and-forget from the caller's perspective (transport errors are
/// caught by `try_send` on the outer channel).
enum InProcessClientMessage {
    Request {
        request: Box<ClientRequest>,
        response_tx: oneshot::Sender<PendingClientRequestResponse>,
    },
    Notification {
        notification: ClientNotification,
    },
    ServerRequestResponse {
        request_id: RequestId,
        result: Result,
    },
    ServerRequestError {
        request_id: RequestId,
        error: JSONRPCErrorError,
    },
    ControllerParticipationResponse {
        request_id: NativeControllerParticipationRequestId,
        decision: NativeControllerParticipationDecision,
    },
    PublishLocalControllerMainThreadId {
        main_thread_id: String,
        response_tx: oneshot::Sender<IoResult<()>>,
    },
    Shutdown {
        done_tx: oneshot::Sender<()>,
    },
}

enum ProcessorCommand {
    Request(Box<ClientRequest>),
    Notification(ClientNotification),
    LocalControllerEndpointFailed {
        reason: String,
        closed_tx: oneshot::Sender<()>,
    },
}

enum InProcessOutboundControlEvent {
    Opened {
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
        writer: mpsc::Sender<QueuedOutgoingMessage>,
        disconnect_sender: Option<CancellationToken>,
        initialized: Arc<AtomicBool>,
        experimental_api_enabled: Arc<AtomicBool>,
        opted_out_notification_methods: Arc<RwLock<HashSet<String>>>,
    },
    Closed {
        connection_id: ConnectionId,
    },
    DisconnectAndClose {
        connection_id: ConnectionId,
    },
}

#[derive(Clone)]
pub struct InProcessClientSender {
    client_tx: mpsc::Sender<InProcessClientMessage>,
}

impl InProcessClientSender {
    pub async fn request(&self, request: ClientRequest) -> IoResult<PendingClientRequestResponse> {
        let (response_tx, response_rx) = oneshot::channel();
        self.try_send_client_message(InProcessClientMessage::Request {
            request: Box::new(request),
            response_tx,
        })?;
        response_rx.await.map_err(|err| {
            IoError::new(
                ErrorKind::BrokenPipe,
                format!("in-process request response channel closed: {err}"),
            )
        })
    }

    pub fn notify(&self, notification: ClientNotification) -> IoResult<()> {
        self.try_send_client_message(InProcessClientMessage::Notification { notification })
    }

    pub fn respond_to_server_request(&self, request_id: RequestId, result: Result) -> IoResult<()> {
        self.try_send_client_message(InProcessClientMessage::ServerRequestResponse {
            request_id,
            result,
        })
    }

    pub fn fail_server_request(
        &self,
        request_id: RequestId,
        error: JSONRPCErrorError,
    ) -> IoResult<()> {
        self.try_send_client_message(InProcessClientMessage::ServerRequestError {
            request_id,
            error,
        })
    }

    pub fn respond_to_controller_participation_request(
        &self,
        request_id: NativeControllerParticipationRequestId,
        decision: NativeControllerParticipationDecision,
    ) -> IoResult<()> {
        self.try_send_client_message(InProcessClientMessage::ControllerParticipationResponse {
            request_id,
            decision,
        })
    }

    pub async fn publish_local_controller_main_thread_id(
        &self,
        main_thread_id: String,
    ) -> IoResult<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.try_send_client_message(InProcessClientMessage::PublishLocalControllerMainThreadId {
            main_thread_id,
            response_tx,
        })?;
        response_rx.await.map_err(|err| {
            IoError::new(
                ErrorKind::BrokenPipe,
                format!("in-process local-controller metadata update channel closed: {err}"),
            )
        })?
    }

    fn try_send_client_message(&self, message: InProcessClientMessage) -> IoResult<()> {
        match self.client_tx.try_send(message) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(IoError::new(
                ErrorKind::WouldBlock,
                "in-process app-server client queue is full",
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(IoError::new(
                ErrorKind::BrokenPipe,
                "in-process app-server runtime is closed",
            )),
        }
    }
}

/// Handle used by an in-process client to call app-server and consume events.
///
/// This is the low-level runtime handle. Higher-level callers should usually go
/// through `codex-app-server-client`, which adds worker-task buffering,
/// request/response helpers, and surface-specific startup policy.
pub struct InProcessClientHandle {
    client: InProcessClientSender,
    event_rx: mpsc::Receiver<InProcessServerEvent>,
    runtime_handle: tokio::task::JoinHandle<()>,
    local_controller_endpoint_status: InProcessLocalControllerEndpointStatus,
    #[cfg(test)]
    _test_codex_home: Option<tempfile::TempDir>,
}

impl InProcessClientHandle {
    /// Sends a typed client request into the in-process runtime.
    ///
    /// The returned value is a transport-level `IoResult` containing either a
    /// JSON-RPC success payload or JSON-RPC error payload. Callers must keep
    /// request IDs unique among concurrent requests; reusing an in-flight ID
    /// produces an `INVALID_REQUEST` response and can make request routing
    /// ambiguous in the caller.
    pub async fn request(&self, request: ClientRequest) -> IoResult<PendingClientRequestResponse> {
        self.client.request(request).await
    }

    /// Sends a typed client notification into the in-process runtime.
    ///
    /// Notifications do not have an application-level response. Transport
    /// errors indicate queue saturation or closed runtime.
    pub fn notify(&self, notification: ClientNotification) -> IoResult<()> {
        self.client.notify(notification)
    }

    /// Resolves a pending [`ServerRequest`](InProcessServerEvent::ServerRequest).
    ///
    /// This should be used only with request IDs received from the current
    /// runtime event stream; sending arbitrary IDs has no effect on app-server
    /// state and can mask a stuck approval flow in the caller.
    pub fn respond_to_server_request(&self, request_id: RequestId, result: Result) -> IoResult<()> {
        self.client.respond_to_server_request(request_id, result)
    }

    /// Rejects a pending [`ServerRequest`](InProcessServerEvent::ServerRequest).
    ///
    /// Use this when the embedder cannot satisfy a server request; leaving
    /// requests unanswered can stall turn progress.
    pub fn fail_server_request(
        &self,
        request_id: RequestId,
        error: JSONRPCErrorError,
    ) -> IoResult<()> {
        self.client.fail_server_request(request_id, error)
    }

    /// Resolves a pending native local-controller participation prompt.
    ///
    /// This should only be used with request IDs received from
    /// [`InProcessServerEvent::ControllerParticipationRequest`] on the owning
    /// in-process TUI connection.
    pub fn respond_to_controller_participation_request(
        &self,
        request_id: NativeControllerParticipationRequestId,
        decision: NativeControllerParticipationDecision,
    ) -> IoResult<()> {
        self.client
            .respond_to_controller_participation_request(request_id, decision)
    }

    /// Publishes the owning TUI's primary thread ID to the per-launch
    /// local-controller metadata, when a local-controller endpoint is active.
    pub async fn publish_local_controller_main_thread_id(
        &self,
        main_thread_id: String,
    ) -> IoResult<()> {
        self.client
            .publish_local_controller_main_thread_id(main_thread_id)
            .await
    }

    /// Receives the next server event from the in-process runtime.
    ///
    /// Returns `None` when the runtime task exits and no more events are
    /// available.
    pub async fn next_event(&mut self) -> Option<InProcessServerEvent> {
        self.event_rx.recv().await
    }

    /// Metadata for the embedded local-controller endpoint, when enabled.
    pub fn local_controller_endpoint(&self) -> Option<&LocalControllerEndpointMetadata> {
        self.local_controller_endpoint_status.metadata()
    }

    /// Startup status for the embedded local-controller endpoint.
    pub fn local_controller_endpoint_status(&self) -> &InProcessLocalControllerEndpointStatus {
        &self.local_controller_endpoint_status
    }

    /// Requests runtime shutdown and waits for worker termination.
    ///
    /// Shutdown is bounded by internal timeouts and may abort background tasks
    /// if graceful drain does not complete in time.
    pub async fn shutdown(self) -> IoResult<()> {
        let mut runtime_handle = self.runtime_handle;
        let (done_tx, done_rx) = oneshot::channel();

        if self
            .client
            .client_tx
            .send(InProcessClientMessage::Shutdown { done_tx })
            .await
            .is_ok()
        {
            let _ = timeout(SHUTDOWN_ACK_TIMEOUT, done_rx).await;
        }

        if let Err(_elapsed) = timeout(SHUTDOWN_TIMEOUT, &mut runtime_handle).await {
            runtime_handle.abort();
            let _ = runtime_handle.await;
        }
        Ok(())
    }

    pub fn sender(&self) -> InProcessClientSender {
        self.client.clone()
    }
}

/// Starts an in-process app-server runtime and performs initialize handshake.
///
/// This function sends `initialize` followed by `initialized` before returning
/// the handle, so callers receive a ready-to-use runtime. If initialize fails,
/// the runtime is shut down and an `InvalidData` error is returned.
pub async fn start(mut args: InProcessStartArgs) -> IoResult<InProcessClientHandle> {
    if let Ok(Some(err)) = check_execpolicy_for_warnings(&args.config.config_layer_stack).await {
        let (path, range) = crate::exec_policy_warning_location(&err);
        args.config_warnings.push(ConfigWarningNotification {
            summary: "Error parsing rules; custom rules not applied.".to_string(),
            details: Some(err.to_string()),
            path,
            range,
        });
    }
    let initialize = args.initialize.clone();
    let client = start_uninitialized(args).await?;

    let initialize_response = client
        .request(ClientRequest::Initialize {
            request_id: RequestId::Integer(0),
            params: initialize,
        })
        .await?;
    if let Err(error) = initialize_response {
        let _ = client.shutdown().await;
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("in-process initialize failed: {}", error.message),
        ));
    }
    client.notify(ClientNotification::Initialized)?;

    Ok(client)
}

async fn run_outbound_router(
    mut outgoing_rx: mpsc::Receiver<OutgoingEnvelope>,
    mut control_rx: mpsc::Receiver<InProcessOutboundControlEvent>,
    mut outbound_connections: HashMap<ConnectionId, OutboundConnectionState>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                for connection_state in outbound_connections.values() {
                    connection_state.request_disconnect();
                }
                break;
            }
            event = control_rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                match event {
                    InProcessOutboundControlEvent::Opened {
                        connection_id,
                        origin,
                        writer,
                        disconnect_sender,
                        initialized,
                        experimental_api_enabled,
                        opted_out_notification_methods,
                    } => {
                        outbound_connections.insert(
                            connection_id,
                            OutboundConnectionState::new_with_origin(
                                origin,
                                writer,
                                initialized,
                                experimental_api_enabled,
                                opted_out_notification_methods,
                                disconnect_sender,
                            ),
                        );
                    }
                    InProcessOutboundControlEvent::Closed { connection_id } => {
                        outbound_connections.remove(&connection_id);
                    }
                    InProcessOutboundControlEvent::DisconnectAndClose { connection_id } => {
                        if let Some(connection_state) = outbound_connections.remove(&connection_id)
                        {
                            connection_state.request_disconnect();
                        }
                    }
                }
            }
            envelope = outgoing_rx.recv() => {
                let Some(envelope) = envelope else {
                    break;
                };
                route_outgoing_envelope(&mut outbound_connections, envelope).await;
            }
        }
    }
}

fn sync_outbound_state_from_session(connection_state: &ConnectionState) {
    let opted_out_notification_methods_snapshot =
        connection_state.session.opted_out_notification_methods();
    let experimental_api_enabled = connection_state.session.experimental_api_enabled();
    if let Ok(mut opted_out_notification_methods) = connection_state
        .outbound_opted_out_notification_methods
        .write()
    {
        *opted_out_notification_methods = opted_out_notification_methods_snapshot;
    } else {
        warn!("failed to update outbound opted-out notifications");
    }
    connection_state
        .outbound_experimental_api_enabled
        .store(experimental_api_enabled, Ordering::Release);
}

fn initialized_connections(
    connections: &HashMap<ConnectionId, ConnectionState>,
) -> Vec<(ConnectionId, ConnectionOrigin)> {
    connections
        .iter()
        .filter_map(|(connection_id, connection_state)| {
            connection_state
                .session
                .initialized()
                .then_some((*connection_id, connection_state.origin))
        })
        .collect()
}

async fn close_external_controller_connections(
    processor: &Arc<MessageProcessor>,
    connections: &mut HashMap<ConnectionId, ConnectionState>,
    outbound_control_tx: &mpsc::Sender<InProcessOutboundControlEvent>,
) -> bool {
    let connection_ids = connections
        .iter()
        .filter_map(|(connection_id, connection_state)| {
            matches!(
                connection_state.origin,
                ConnectionOrigin::ExternalController
            )
            .then_some(*connection_id)
        })
        .collect::<Vec<_>>();

    for connection_id in connection_ids {
        if outbound_control_tx
            .send(InProcessOutboundControlEvent::DisconnectAndClose { connection_id })
            .await
            .is_err()
        {
            return false;
        }
        if let Some(connection_state) = connections.remove(&connection_id) {
            processor
                .connection_closed(connection_id, &connection_state.session)
                .await;
        }
    }

    true
}

type PendingControllerParticipationRequests = Arc<
    AsyncMutex<
        HashMap<
            NativeControllerParticipationRequestId,
            oneshot::Sender<NativeControllerParticipationDecision>,
        >,
    >,
>;

fn native_controller_participation_approver(
    event_tx: mpsc::Sender<InProcessServerEvent>,
    pending_requests: PendingControllerParticipationRequests,
    next_request_id: Arc<AtomicU64>,
) -> NativeControllerParticipationApprover {
    Arc::new(move |request: NativeControllerParticipationRequest| {
        let event_tx = event_tx.clone();
        let pending_requests = Arc::clone(&pending_requests);
        let request_id =
            NativeControllerParticipationRequestId(next_request_id.fetch_add(1, Ordering::Relaxed));
        Box::pin(async move {
            let (decision_tx, decision_rx) = oneshot::channel();
            pending_requests
                .lock()
                .await
                .insert(request_id, decision_tx);

            let event = InProcessServerEvent::ControllerParticipationRequest(Box::new(
                InProcessControllerParticipationRequest {
                    request_id,
                    controller_name: request.controller_name,
                    description: request.description,
                    main_thread_id: request.main_thread_id,
                },
            ));

            if event_tx.send(event).await.is_err() {
                pending_requests.lock().await.remove(&request_id);
                return NativeControllerParticipationDecision::TuiUnavailable {
                    reason: "owning TUI is not available for controller participation".to_string(),
                };
            }

            match decision_rx.await {
                Ok(decision) => decision,
                Err(_) => NativeControllerParticipationDecision::TuiUnavailable {
                    reason: "owning TUI stopped before answering controller participation"
                        .to_string(),
                },
            }
        })
    })
}

async fn start_uninitialized(args: InProcessStartArgs) -> IoResult<InProcessClientHandle> {
    args.config.auth_config().validate()?;
    let channel_capacity = args.channel_capacity.max(1);
    let installation_id = resolve_installation_id(&args.config.codex_home).await?;
<<<<<<< HEAD
    let auth_manager =
        AuthManager::shared_from_config(args.config.as_ref(), args.enable_codex_api_key_env)
            .await
            .map_err(IoError::other)?;
    #[cfg(test)]
    let controller_enrollment_source = Arc::clone(&args.controller_enrollment_source);
    #[cfg(not(test))]
    let controller_enrollment_source: Arc<
        dyn crate::controller_enrollment::ControllerEnrollmentSource,
    > = Arc::new(EmptyControllerEnrollmentSource);
    #[cfg(test)]
    let controller_credential_proof_factory = args.controller_credential_proof_factory.clone();
    let (client_tx, mut client_rx) = mpsc::channel::<InProcessClientMessage>(channel_capacity);
    let (event_tx, event_rx) = mpsc::channel::<InProcessServerEvent>(channel_capacity);
    let pending_controller_participation = Arc::new(AsyncMutex::new(HashMap::new()));
    let next_controller_participation_request_id = Arc::new(AtomicU64::new(1));
    let native_controller_participation_approver = native_controller_participation_approver(
        event_tx.clone(),
        Arc::clone(&pending_controller_participation),
        next_controller_participation_request_id,
    );
    let (external_transport_event_tx, external_transport_event_rx) =
        mpsc::channel::<TransportEvent>(channel_capacity);
    let external_transport_shutdown_token = CancellationToken::new();
    let (local_controller_endpoint_handle, local_controller_endpoint_status) = match &args
        .local_controller_endpoint
    {
        InProcessLocalControllerEndpointConfig::Disabled => {
            (None, InProcessLocalControllerEndpointStatus::Disabled)
        }
        InProcessLocalControllerEndpointConfig::BestEffort { main_thread_id } => {
            match codex_app_server_transport::local_controller::start_local_controller_acceptor(
                args.config.codex_home.as_path(),
                main_thread_id.clone(),
                external_transport_event_tx.clone(),
                external_transport_shutdown_token.clone(),
            )
            .await
            {
                Ok(handle) => {
                    let metadata = handle.metadata().clone();
                    (
                        Some(handle),
                        InProcessLocalControllerEndpointStatus::Available(metadata),
                    )
                }
                Err(err) => {
                    warn!(%err, "local-controller endpoint unavailable; continuing without controllers");
                    (
                        None,
                        InProcessLocalControllerEndpointStatus::Unavailable {
                            reason: err.to_string(),
                        },
                    )
                }
            }
        }
        InProcessLocalControllerEndpointConfig::Enabled { main_thread_id } => {
            let handle =
                codex_app_server_transport::local_controller::start_local_controller_acceptor(
                    args.config.codex_home.as_path(),
                    main_thread_id.clone(),
                    external_transport_event_tx.clone(),
                    external_transport_shutdown_token.clone(),
                )
                .await?;
            let metadata = handle.metadata().clone();
            (
                Some(handle),
                InProcessLocalControllerEndpointStatus::Available(metadata),
            )
        }
    };
    drop(external_transport_event_tx);

    let runtime_handle = tokio::spawn(async move {
        let mut local_controller_endpoint_handle = local_controller_endpoint_handle;
        let mut local_controller_endpoint_failure_rx = local_controller_endpoint_handle
            .as_mut()
            .and_then(codex_app_server_transport::local_controller::LocalControllerEndpointHandle::take_failure_receiver);
        let (outgoing_tx, outgoing_rx) = mpsc::channel::<OutgoingEnvelope>(channel_capacity);
        let analytics_events_client =
            analytics_events_client_from_config(Arc::clone(&auth_manager), args.config.as_ref());
        let analytics_events_flush_client = analytics_events_client.clone();
        let outgoing_message_sender = Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            analytics_events_client.clone(),
        ));

        let (writer_tx, mut writer_rx) = mpsc::channel::<QueuedOutgoingMessage>(channel_capacity);
        let outbound_initialized = Arc::new(AtomicBool::new(false));
        let outbound_experimental_api_enabled = Arc::new(AtomicBool::new(false));
        let outbound_opted_out_notification_methods = Arc::new(RwLock::new(HashSet::new()));

        let mut outbound_connections = HashMap::<ConnectionId, OutboundConnectionState>::new();
        outbound_connections.insert(
            IN_PROCESS_CONNECTION_ID,
            OutboundConnectionState::new_with_origin(
                ConnectionOrigin::InProcess,
                writer_tx,
                Arc::clone(&outbound_initialized),
                Arc::clone(&outbound_experimental_api_enabled),
                Arc::clone(&outbound_opted_out_notification_methods),
                /*disconnect_sender*/ None,
            ),
        );
        let (outbound_control_tx, outbound_control_rx) =
            mpsc::channel::<InProcessOutboundControlEvent>(channel_capacity);
        let (outbound_shutdown_tx, outbound_shutdown_rx) = oneshot::channel();
        let mut outbound_handle = tokio::spawn(run_outbound_router(
            outgoing_rx,
            outbound_control_rx,
            outbound_connections,
            outbound_shutdown_rx,
        ));

        let processor_outgoing = Arc::clone(&outgoing_message_sender);
        let config_manager = ConfigManager::new(
            args.config.codex_home.to_path_buf(),
            args.cli_overrides,
            args.loader_overrides,
            args.strict_config,
            args.cloud_config_bundle,
            args.arg0_paths.clone(),
            args.thread_config_loader,
        );
        let (processor_tx, mut processor_rx) = mpsc::channel::<ProcessorCommand>(channel_capacity);
        let (controller_ownership_status_tx, mut controller_ownership_status_rx) =
            mpsc::channel::<InProcessControllerOwnershipStatus>(channel_capacity);
        let mut processor_handle = tokio::spawn(async move {
            let processor = Arc::new(MessageProcessor::new(MessageProcessorArgs {
                outgoing: Arc::clone(&processor_outgoing),
                analytics_events_client,
                arg0_paths: args.arg0_paths,
                config: args.config,
                config_manager,
                environment_manager: args.environment_manager,
                feedback: args.feedback,
                log_db: args.log_db,
                state_db: args.state_db,
                config_warnings: args.config_warnings,
                session_source: args.session_source,
                auth_manager,
                installation_id,
                code_mode_session_provider: None,
                rpc_transport: AppServerRpcTransport::InProcess,
                remote_control_handle: None,
                controller_enrollment_source,
                native_controller_participation_approver: Some(
                    native_controller_participation_approver,
                ),
                controller_ownership_status_tx: Some(controller_ownership_status_tx),
                plugin_startup_tasks: crate::PluginStartupTasks::Start,
            }));
            let mut thread_created_rx = processor.thread_created_receiver();
            let mut external_transport_event_rx = external_transport_event_rx;
            let mut listen_for_threads = true;
            let mut listen_for_external_transport = true;
            let mut connections = HashMap::<ConnectionId, ConnectionState>::new();
            connections.insert(
                IN_PROCESS_CONNECTION_ID,
                ConnectionState::new(
                    ConnectionOrigin::InProcess,
                    outbound_initialized,
                    outbound_experimental_api_enabled,
                    outbound_opted_out_notification_methods,
                ),
            );

            loop {
                tokio::select! {
                    command = processor_rx.recv() => {
                        match command {
                            Some(ProcessorCommand::Request(request)) => {
                                let Some(connection_state) =
                                    connections.get_mut(&IN_PROCESS_CONNECTION_ID)
                                else {
                                    break;
                                };
                                let was_initialized = connection_state.session.initialized();
                                processor
                                    .process_client_request(
                                        IN_PROCESS_CONNECTION_ID,
                                        ConnectionOrigin::InProcess,
                                        *request,
                                        Arc::clone(&connection_state.session),
                                        &connection_state.outbound_initialized,
                                    )
                                    .await;
                                sync_outbound_state_from_session(connection_state);
                                if !was_initialized && connection_state.session.initialized() {
                                    processor.send_initialize_notifications().await;
                                }
                            }
                            Some(ProcessorCommand::Notification(notification)) => {
                                processor.process_client_notification(notification).await;
                            }
                            Some(ProcessorCommand::LocalControllerEndpointFailed {
                                reason,
                                closed_tx,
                            }) => {
                                warn!(
                                    %reason,
                                    "local-controller endpoint failed; closing external controllers"
                                );
                                if !close_external_controller_connections(
                                    &processor,
                                    &mut connections,
                                    &outbound_control_tx,
                                )
                                .await
                                {
                                    break;
                                }
                                let _ = closed_tx.send(());
                            }
                            None => {
                                break;
                            }
                        }
                    }
                    event = external_transport_event_rx.recv(), if listen_for_external_transport => {
                        let Some(event) = event else {
                            listen_for_external_transport = false;
                            continue;
                        };
                        match event {
                            TransportEvent::ConnectionOpened {
                                connection_id,
                                origin,
                                writer,
                                disconnect_sender,
                            } => {
                                let outbound_initialized = Arc::new(AtomicBool::new(false));
                                let outbound_experimental_api_enabled =
                                    Arc::new(AtomicBool::new(false));
                                let outbound_opted_out_notification_methods =
                                    Arc::new(RwLock::new(HashSet::new()));
                                if outbound_control_tx
                                    .send(InProcessOutboundControlEvent::Opened {
                                        connection_id,
                                        origin,
                                        writer,
                                        disconnect_sender,
                                        initialized: Arc::clone(&outbound_initialized),
                                        experimental_api_enabled: Arc::clone(
                                            &outbound_experimental_api_enabled,
                                        ),
                                        opted_out_notification_methods: Arc::clone(
                                            &outbound_opted_out_notification_methods,
                                        ),
                                    })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                                let connection_state = ConnectionState::new(
                                    origin,
                                    outbound_initialized,
                                    outbound_experimental_api_enabled,
                                    outbound_opted_out_notification_methods,
                                );
                                #[cfg(test)]
                                if matches!(origin, ConnectionOrigin::ExternalController)
                                    && let Some(proof_factory) =
                                        controller_credential_proof_factory.as_ref()
                                {
                                    connection_state
                                        .session
                                        .bind_controller_credential_proof(
                                            proof_factory(connection_id),
                                        );
                                }
                                connections.insert(connection_id, connection_state);
                            }
                            TransportEvent::ConnectionClosed { connection_id } => {
                                let Some(connection_state) = connections.remove(&connection_id) else {
                                    continue;
                                };
                                processor
                                    .connection_closed(connection_id, &connection_state.session)
                                    .await;
                                if outbound_control_tx
                                    .send(InProcessOutboundControlEvent::Closed { connection_id })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            TransportEvent::IncomingMessage {
                                connection_id,
                                message,
                            } => {
                                match message {
                                    JSONRPCMessage::Request(request) => {
                                        let Some(connection_state) =
                                            connections.get_mut(&connection_id)
                                        else {
                                            warn!(
                                                "dropping request from unknown in-process external connection: {connection_id:?}"
                                            );
                                            continue;
                                        };
                                        let was_initialized =
                                            connection_state.session.initialized();
                                        processor
                                            .process_request(
                                                connection_id,
                                                connection_state.origin,
                                                request,
                                                &crate::transport::AppServerTransport::Off,
                                                Arc::clone(&connection_state.session),
                                            )
                                            .await;
                                        sync_outbound_state_from_session(connection_state);
                                        if !was_initialized && connection_state.session.initialized() {
                                            if !matches!(
                                                connection_state.origin,
                                                ConnectionOrigin::ExternalController
                                            ) {
                                                processor
                                                    .send_initialize_notifications_to_connection(
                                                        connection_id,
                                                    )
                                                    .await;
                                            }
                                            processor
                                                .connection_initialized(
                                                    connection_id,
                                                    connection_state.session.request_attestation(),
                                                )
                                                .await;
                                            connection_state
                                                .outbound_initialized
                                                .store(true, Ordering::Release);
                                        }
                                    }
                                    JSONRPCMessage::Response(response) => {
                                        let Some(connection_state) =
                                            connections.get(&connection_id)
                                        else {
                                            warn!(
                                                "dropping response from unknown in-process external connection: {connection_id:?}"
                                            );
                                            continue;
                                        };
                                        processor
                                            .process_response(
                                                connection_id,
                                                connection_state.origin,
                                                response,
                                            )
                                            .await;
                                    }
                                    JSONRPCMessage::Notification(notification) => {
                                        if !connections.contains_key(&connection_id) {
                                            warn!(
                                                "dropping notification from unknown in-process external connection: {connection_id:?}"
                                            );
                                            continue;
                                        }
                                        processor.process_notification(notification).await;
                                    }
                                    JSONRPCMessage::Error(err) => {
                                        let Some(connection_state) =
                                            connections.get(&connection_id)
                                        else {
                                            warn!(
                                                "dropping error from unknown in-process external connection: {connection_id:?}"
                                            );
                                            continue;
                                        };
                                        processor
                                            .process_error(
                                                connection_id,
                                                connection_state.origin,
                                                err,
                                            )
                                            .await;
                                    }
                                }
                            }
                        }
                    }
                    created = thread_created_rx.recv(), if listen_for_threads => {
                        match created {
                            Ok(thread_id) => {
                                processor
                                    .try_attach_thread_listener_for_initialized_connections(
                                        thread_id,
                                        initialized_connections(&connections),
                                    )
                                    .await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                warn!("thread_created receiver lagged; skipping resync");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                listen_for_threads = false;
                            }
                        }
                    }
                }
            }

            processor.clear_runtime_references();
            processor.cancel_active_login().await;
            for (connection_id, connection_state) in connections {
                processor
                    .connection_closed(connection_id, &connection_state.session)
                    .await;
            }
            processor.clear_all_thread_listeners().await;
            processor.drain_background_tasks().await;
            processor.shutdown_threads().await;
        });
        let mut pending_request_responses =
            HashMap::<RequestId, oneshot::Sender<PendingClientRequestResponse>>::new();
        let mut shutdown_ack = None;
        let mut listen_for_controller_ownership_status = true;

        loop {
            tokio::select! {
                message = client_rx.recv() => {
                    match message {
                        Some(InProcessClientMessage::Request { request, response_tx }) => {
                            let request = *request;
                            let request_id = request.id().clone();
                            match pending_request_responses.entry(request_id.clone()) {
                                Entry::Vacant(entry) => {
                                    entry.insert(response_tx);
                                }
                                Entry::Occupied(_) => {
                                    let _ = response_tx.send(Err(invalid_request(format!(
                                        "duplicate request id: {request_id:?}"
                                    ))));
                                    continue;
                                }
                            }

                            match processor_tx.try_send(ProcessorCommand::Request(Box::new(request))) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    if let Some(response_tx) =
                                        pending_request_responses.remove(&request_id)
                                    {
                                        let _ = response_tx.send(Err(JSONRPCErrorError {
                                            code: OVERLOADED_ERROR_CODE,
                                            message: "in-process app-server request queue is full"
                                                .to_string(),
                                            data: None,
                                        }));
                                    }
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    if let Some(response_tx) =
                                        pending_request_responses.remove(&request_id)
                                    {
                                        let _ = response_tx.send(Err(internal_error(
                                            "in-process app-server request processor is closed",
                                        )));
                                    }
                                    break;
                                }
                            }
                        }
                        Some(InProcessClientMessage::Notification { notification }) => {
                            match processor_tx.try_send(ProcessorCommand::Notification(notification)) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!("dropping in-process client notification (queue full)");
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    break;
                                }
                            }
                        }
                        Some(InProcessClientMessage::ServerRequestResponse { request_id, result }) => {
                            outgoing_message_sender
                                .notify_client_response_from_connection(
                                    IN_PROCESS_CONNECTION_ID,
                                    request_id,
                                    result,
                                )
                                .await;
                        }
                        Some(InProcessClientMessage::ServerRequestError { request_id, error }) => {
                            outgoing_message_sender
                                .notify_client_error_from_connection(
                                    IN_PROCESS_CONNECTION_ID,
                                    request_id,
                                    error,
                                )
                                .await;
                        }
                        Some(InProcessClientMessage::ControllerParticipationResponse {
                            request_id,
                            decision,
                        }) => {
                            let response_tx =
                                pending_controller_participation.lock().await.remove(&request_id);
                            if let Some(response_tx) = response_tx {
                                let _ = response_tx.send(decision);
                            } else {
                                warn!(
                                    ?request_id,
                                    "dropping unmatched controller participation response"
                                );
                            }
                        }
                        Some(InProcessClientMessage::PublishLocalControllerMainThreadId {
                            main_thread_id,
                            response_tx,
                        }) => {
                            let result = match local_controller_endpoint_handle.as_mut() {
                                Some(handle) => {
                                    handle.publish_main_thread_id(main_thread_id).await
                                }
                                None => Ok(()),
                            };
                            let _ = response_tx.send(result);
                        }
                        Some(InProcessClientMessage::Shutdown { done_tx }) => {
                            shutdown_ack = Some(done_tx);
                            break;
                        }
                        None => {
                            break;
                        }
                    }
                }
                failure = async {
                    match local_controller_endpoint_failure_rx.as_mut() {
                        Some(failure_rx) => failure_rx.await,
                        None => std::future::pending().await,
                    }
                } => {
                    local_controller_endpoint_failure_rx = None;
                    let reason = match failure {
                        Ok(failure) => failure.reason,
                        Err(err) => format!("local-controller endpoint failure channel closed: {err}"),
                    };
                    let (closed_tx, closed_rx) = oneshot::channel();
                    if processor_tx
                        .send(ProcessorCommand::LocalControllerEndpointFailed {
                            reason: reason.clone(),
                            closed_tx,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                    if let Some(local_controller_endpoint_handle) =
                        local_controller_endpoint_handle.take()
                        && let Err(err) = local_controller_endpoint_handle.shutdown().await {
                            warn!(%reason, ?err, "failed to join closed local-controller endpoint");
                        }
                    if timeout(SHUTDOWN_TIMEOUT, closed_rx).await.is_err() {
                        warn!(
                            %reason,
                            "timed out waiting for local-controller endpoint failure cleanup"
                        );
                    }
                    if event_tx
                        .send(InProcessServerEvent::LocalControllerEndpointUnavailable {
                            reason,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                status = controller_ownership_status_rx.recv(), if listen_for_controller_ownership_status => {
                    let Some(status) = status else {
                        listen_for_controller_ownership_status = false;
                        continue;
                    };
                    if event_tx
                        .send(InProcessServerEvent::ControllerOwnershipStatus(Box::new(status)))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                queued_message = writer_rx.recv() => {
                    let Some(queued_message) = queued_message else {
                        break;
                    };
                    let outgoing_message = queued_message.message;
                    match outgoing_message {
                        OutgoingMessage::Response(response) => {
                            if let Some(response_tx) = pending_request_responses.remove(&response.id) {
                                let result = serde_json::to_value(response.result).map_err(|err| {
                                    internal_error(format!("failed to serialize response: {err}"))
                                });
                                let _ = response_tx.send(result);
                            } else {
                                warn!(
                                    request_id = ?response.id,
                                    "dropping unmatched in-process response"
                                );
                            }
                        }
                        OutgoingMessage::Error(error) => {
                            if let Some(response_tx) = pending_request_responses.remove(&error.id) {
                                let _ = response_tx.send(Err(error.error));
                            } else {
                                warn!(
                                    request_id = ?error.id,
                                    "dropping unmatched in-process error response"
                                );
                            }
                        }
                        OutgoingMessage::Request(request) => {
                            // Send directly to avoid cloning; on failure the
                            // original value is returned inside the error.
                            if let Err(send_error) = event_tx
                                .try_send(InProcessServerEvent::ServerRequest(Box::new(request)))
                            {
                                let (error, inner) = match send_error {
                                    mpsc::error::TrySendError::Full(inner) => (
                                        JSONRPCErrorError {
                                            code: OVERLOADED_ERROR_CODE,
                                            message:
                                                "in-process server request queue is full".to_string(),
                                            data: None,
                                        },
                                        inner,
                                    ),
                                    mpsc::error::TrySendError::Closed(inner) => (
                                        internal_error(
                                            "in-process server request consumer is closed",
                                        ),
                                        inner,
                                    ),
                                };
                                let request_id = match inner {
                                    InProcessServerEvent::ServerRequest(req) => req.id().clone(),
                                    _ => unreachable!("we just sent a ServerRequest variant"),
                                };
                                outgoing_message_sender
                                    .notify_client_error_from_connection(
                                        IN_PROCESS_CONNECTION_ID,
                                        request_id,
                                        error,
                                    )
                                    .await;
                            }
                        }
                        OutgoingMessage::AppServerNotification(envelope) => {
                            let notification = envelope.notification;
                            if server_notification_requires_delivery(&notification) {
                                if event_tx
                                    .send(InProcessServerEvent::ServerNotification(Box::new(
                                        notification,
                                    )))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            } else if let Err(send_error) =
                                event_tx.try_send(InProcessServerEvent::ServerNotification(
                                    Box::new(notification),
                                ))
                            {
                                match send_error {
                                    mpsc::error::TrySendError::Full(_) => {
                                        warn!("dropping in-process server notification (queue full)");
                                    }
                                    mpsc::error::TrySendError::Closed(_) => {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if let Some(write_complete_tx) = queued_message.write_complete_tx {
                        let _ = write_complete_tx.send(());
                    }
                }
            }
        }

        external_transport_shutdown_token.cancel();
        if let Some(local_controller_endpoint_handle) = local_controller_endpoint_handle.take() {
            let _ = local_controller_endpoint_handle.shutdown().await;
        }
        drop(writer_rx);
        drop(processor_tx);
        outgoing_message_sender
            .cancel_all_requests(Some(internal_error(
                "in-process app-server runtime is shutting down",
            )))
            .await;
        let pending_native_participation = pending_controller_participation
            .lock()
            .await
            .drain()
            .map(|(_, response_tx)| response_tx)
            .collect::<Vec<_>>();
        for response_tx in pending_native_participation {
            let _ = response_tx.send(NativeControllerParticipationDecision::TuiUnavailable {
                reason: "in-process app-server runtime is shutting down".to_string(),
            });
        }
        // Detached processor work can retain outgoing senders, so channel
        // closure alone cannot be used to shut down the outbound router.
        drop(outgoing_message_sender);
        for (_, response_tx) in pending_request_responses {
            let _ = response_tx.send(Err(internal_error(
                "in-process app-server runtime is shutting down",
            )));
        }

        if let Err(_elapsed) = timeout(SHUTDOWN_TIMEOUT, &mut processor_handle).await {
            processor_handle.abort();
            let _ = processor_handle.await;
        }
        let _ = outbound_shutdown_tx.send(());
        if let Err(_elapsed) = timeout(SHUTDOWN_TIMEOUT, &mut outbound_handle).await {
            outbound_handle.abort();
            let _ = outbound_handle.await;
        }

        analytics_events_flush_client.flush().await;

        if let Some(done_tx) = shutdown_ack {
            let _ = done_tx.send(());
        }
    });

    Ok(InProcessClientHandle {
        client: InProcessClientSender { client_tx },
        event_rx,
        runtime_handle,
        local_controller_endpoint_status,
        #[cfg(test)]
        _test_codex_home: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::ClientInfo;
    use codex_app_server_protocol::ConfigRequirementsReadResponse;
    use codex_app_server_protocol::ControllerAcquireControlResponse;
    use codex_app_server_protocol::ControllerControlOwnershipChangedReason;
    use codex_app_server_protocol::ControllerErrorCode;
    use codex_app_server_protocol::ControllerErrorData;
    use codex_app_server_protocol::ControllerParticipationStatus;
    use codex_app_server_protocol::ControllerReleaseControlResponse;
    use codex_app_server_protocol::ControllerRequestParticipationParams;
    use codex_app_server_protocol::ControllerRequestParticipationResponse;
    use codex_app_server_protocol::ControllerSignOffResponse;
    use codex_app_server_protocol::ExternalAgentConfigImportCompletedNotification;
    use codex_app_server_protocol::ItemCompletedNotification;
    use codex_app_server_protocol::JSONRPCError;
    use codex_app_server_protocol::JSONRPCRequest;
    use codex_app_server_protocol::SessionSource as ApiSessionSource;
    use codex_app_server_protocol::ThreadArchivedNotification;
    use codex_app_server_protocol::ThreadClosedNotification;
    use codex_app_server_protocol::ThreadDeletedNotification;
    use codex_app_server_protocol::ThreadItem;
    use codex_app_server_protocol::ThreadListResponse;
    use codex_app_server_protocol::ThreadLoadedListParams;
    use codex_app_server_protocol::ThreadLoadedListResponse;
    use codex_app_server_protocol::ThreadNameUpdatedNotification;
    use codex_app_server_protocol::ThreadReadParams;
    use codex_app_server_protocol::ThreadReadResponse;
    use codex_app_server_protocol::ThreadSearchParams;
    use codex_app_server_protocol::ThreadSearchResponse;
    use codex_app_server_protocol::ThreadSetNameParams;
    use codex_app_server_protocol::ThreadSetNameResponse;
    use codex_app_server_protocol::ThreadStartParams;
    use codex_app_server_protocol::ThreadStartResponse;
    use codex_app_server_protocol::ThreadStatus;
    use codex_app_server_protocol::ThreadStatusChangedNotification;
    use codex_app_server_protocol::ThreadUnarchivedNotification;
    use codex_app_server_protocol::Turn;
    use codex_app_server_protocol::TurnCompletedNotification;
    use codex_app_server_protocol::TurnItemsView;
    use codex_app_server_protocol::TurnStatus;
    use codex_core::config::ConfigBuilder;
    #[cfg(unix)]
    use futures::SinkExt;
    #[cfg(unix)]
    use futures::StreamExt;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;
    #[cfg(unix)]
    use tokio::net::UnixStream;
    #[cfg(unix)]
    use tokio_tungstenite::client_async;
    #[cfg(unix)]
    use tokio_tungstenite::tungstenite::Message;
    #[cfg(unix)]
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    #[cfg(unix)]
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    async fn build_test_config(codex_home: &Path) -> Config {
        match ConfigBuilder::default()
            .codex_home(codex_home.to_path_buf())
            .build()
            .await
        {
            Ok(config) => config,
            Err(_) => Config::load_default_with_cli_overrides_for_codex_home(
                codex_home.to_path_buf(),
                Vec::new(),
            )
            .await
            .expect("default config should load"),
        }
    }

    async fn start_test_client_with_capacity(
        session_source: SessionSource,
        channel_capacity: usize,
    ) -> InProcessClientHandle {
        let codex_home = TempDir::new().expect("temp dir");
        let args = build_test_start_args(
            codex_home.path(),
            session_source,
            channel_capacity,
            InProcessLocalControllerEndpointConfig::Disabled,
        )
        .await;
        let mut client = start(args).await.expect("in-process runtime should start");
        client._test_codex_home = Some(codex_home);
        client
    }

    async fn build_test_start_args(
        codex_home: &Path,
        session_source: SessionSource,
        channel_capacity: usize,
        local_controller_endpoint: InProcessLocalControllerEndpointConfig,
    ) -> InProcessStartArgs {
        let config = Arc::new(build_test_config(codex_home).await);
        let state_db = codex_rollout::state_db::try_init(config.as_ref())
            .await
            .expect("state db should initialize for in-process test");
        InProcessStartArgs {
            arg0_paths: Arg0DispatchPaths::default(),
            config,
            cli_overrides: Vec::new(),
            loader_overrides: LoaderOverrides::default(),
            strict_config: false,
            cloud_config_bundle: CloudConfigBundleLoader::default(),
            thread_config_loader: Arc::new(codex_config::NoopThreadConfigLoader),
            feedback: CodexFeedback::new(),
            log_db: None,
            state_db: Some(state_db),
            environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
            config_warnings: Vec::new(),
            session_source,
            enable_codex_api_key_env: false,
            initialize: InitializeParams {
                client_info: ClientInfo {
                    name: "codex-in-process-test".to_string(),
                    title: None,
                    version: "0.0.0".to_string(),
                },
                capabilities: None,
            },
            channel_capacity,
            local_controller_endpoint,
            controller_enrollment_source: Arc::new(EmptyControllerEnrollmentSource),
            controller_credential_proof_factory: None,
        }
    }

    async fn start_test_client(session_source: SessionSource) -> InProcessClientHandle {
        start_test_client_with_capacity(session_source, DEFAULT_IN_PROCESS_CHANNEL_CAPACITY).await
    }

    #[cfg(unix)]
    async fn connect_local_controller_websocket(
        metadata: &LocalControllerEndpointMetadata,
    ) -> tokio_tungstenite::WebSocketStream<UnixStream> {
        let socket_path = metadata
            .endpoint_uri
            .strip_prefix("unix://")
            .expect("local-controller endpoint should use unix URI");
        let stream = UnixStream::connect(socket_path)
            .await
            .expect("local-controller socket should accept connections");
        let mut request = "ws://codex-local-controller/"
            .into_client_request()
            .expect("websocket request should build");
        request.headers_mut().insert(
            LOCAL_CONTROLLER_LAUNCH_NONCE_HEADER,
            HeaderValue::from_str(metadata.launch_nonce.as_str())
                .expect("launch nonce should be a valid header"),
        );
        client_async(request, stream)
            .await
            .expect("local-controller websocket should upgrade")
            .0
    }

    #[cfg(unix)]
    async fn approve_next_native_controller_participation(
        client: &mut InProcessClientHandle,
        expected_controller_name: &str,
        expected_description: &str,
        expected_main_thread_id: &str,
    ) {
        let native_request = timeout(Duration::from_secs(2), async {
            loop {
                match client
                    .next_event()
                    .await
                    .expect("event stream should stay open")
                {
                    InProcessServerEvent::ControllerParticipationRequest(native_request) => {
                        break native_request;
                    }
                    InProcessServerEvent::ControllerOwnershipStatus(_)
                    | InProcessServerEvent::LocalControllerEndpointUnavailable { .. }
                    | InProcessServerEvent::ServerRequest(_)
                    | InProcessServerEvent::ServerNotification(_)
                    | InProcessServerEvent::Lagged { .. } => {}
                }
            }
        })
        .await
        .expect("native participation request should arrive before timeout");
        assert_eq!(
            *native_request,
            InProcessControllerParticipationRequest {
                request_id: native_request.request_id,
                controller_name: expected_controller_name.to_string(),
                description: expected_description.to_string(),
                main_thread_id: expected_main_thread_id.to_string(),
            }
        );
        client
            .respond_to_controller_participation_request(
                native_request.request_id,
                NativeControllerParticipationDecision::Approved,
            )
            .expect("native participation response should send");
    }

    #[cfg(unix)]
    async fn expect_next_controller_ownership_status(
        client: &mut InProcessClientHandle,
        expected_main_thread_id: &str,
        expected_owner: InProcessControllerOwnershipStatusOwner,
        expected_owner_epoch: u64,
        expected_reason: ControllerControlOwnershipChangedReason,
    ) {
        let status = timeout(Duration::from_secs(2), async {
            loop {
                match client
                    .next_event()
                    .await
                    .expect("event stream should stay open")
                {
                    InProcessServerEvent::ControllerOwnershipStatus(status) => break status,
                    InProcessServerEvent::ControllerParticipationRequest(_)
                    | InProcessServerEvent::LocalControllerEndpointUnavailable { .. }
                    | InProcessServerEvent::ServerRequest(_)
                    | InProcessServerEvent::ServerNotification(_)
                    | InProcessServerEvent::Lagged { .. } => {}
                }
            }
        })
        .await
        .expect("controller ownership status should arrive before timeout");
        assert_eq!(
            *status,
            InProcessControllerOwnershipStatus {
                main_thread_id: codex_protocol::ThreadId::from_string(expected_main_thread_id)
                    .expect("expected main thread id should parse"),
                owner: expected_owner,
                owner_epoch: expected_owner_epoch,
                reason: expected_reason,
            }
        );
    }

    #[cfg(unix)]
    async fn send_websocket_request<S>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
        request_id: i64,
        method: &str,
        params: Option<serde_json::Value>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        write_websocket_message(
            websocket,
            JSONRPCMessage::Request(JSONRPCRequest {
                id: RequestId::Integer(request_id),
                method: method.to_string(),
                params,
                trace: None,
            }),
        )
        .await;
    }

    #[cfg(unix)]
    async fn send_websocket_typed_request<S, P>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
        request_id: i64,
        method: &str,
        params: &P,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
        P: serde::Serialize,
    {
        send_websocket_request(
            websocket,
            request_id,
            method,
            Some(serde_json::to_value(params).expect("params should serialize")),
        )
        .await;
    }

    #[cfg(unix)]
    async fn read_websocket_response<S, T>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
        expected_id: i64,
    ) -> T
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
        T: serde::de::DeserializeOwned,
    {
        loop {
            match read_websocket_message(websocket).await {
                JSONRPCMessage::Response(response)
                    if response.id == RequestId::Integer(expected_id) =>
                {
                    return serde_json::from_value(response.result)
                        .expect("response should match expected type");
                }
                JSONRPCMessage::Notification(_) => continue,
                message => panic!("unexpected websocket response message: {message:?}"),
            }
        }
    }

    #[cfg(unix)]
    async fn read_websocket_error<S>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
        expected_id: i64,
    ) -> JSONRPCError
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        loop {
            match read_websocket_message(websocket).await {
                JSONRPCMessage::Error(error) if error.id == RequestId::Integer(expected_id) => {
                    return error;
                }
                JSONRPCMessage::Notification(_) => continue,
                message => panic!("unexpected websocket error message: {message:?}"),
            }
        }
    }

    #[cfg(unix)]
    async fn read_websocket_message<S>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
    ) -> JSONRPCMessage
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        loop {
            let frame = websocket
                .next()
                .await
                .expect("frame should be available")
                .expect("frame should decode");
            match frame {
                Message::Text(text) => {
                    return serde_json::from_str::<JSONRPCMessage>(&text)
                        .expect("text frame should be valid JSON-RPC");
                }
                Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                    continue;
                }
                Message::Close(_) => panic!("unexpected close frame"),
            }
        }
    }

    #[cfg(unix)]
    async fn expect_websocket_closed<S>(websocket: &mut tokio_tungstenite::WebSocketStream<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        timeout(Duration::from_secs(2), async {
            loop {
                match websocket.next().await {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                    Some(Ok(Message::Ping(_)))
                    | Some(Ok(Message::Pong(_)))
                    | Some(Ok(Message::Frame(_))) => continue,
                    Some(Ok(message)) => {
                        panic!("expected websocket close, got: {message:?}");
                    }
                }
            }
        })
        .await
        .expect("websocket should close after controller/signOff");
    }

    #[cfg(unix)]
    async fn write_websocket_message<S>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
        message: JSONRPCMessage,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        websocket
            .send(Message::Text(
                serde_json::to_string(&message)
                    .expect("message should serialize")
                    .into(),
            ))
            .await
            .expect("message should send");
    }

    fn block_local_controller_endpoint_dir(codex_home: &Path) {
        fs::write(codex_home.join("local-controllers"), b"not a directory")
            .expect("local-controller directory blocker should be created");
    }

    #[tokio::test]
    async fn in_process_start_initializes_and_handles_typed_v2_request() {
        let client = start_test_client(SessionSource::Cli).await;
        let response = client
            .request(ClientRequest::ConfigRequirementsRead {
                request_id: RequestId::Integer(1),
                params: None,
            })
            .await
            .expect("request transport should work")
            .expect("request should succeed");
        assert!(response.is_object());

        let _parsed: ConfigRequirementsReadResponse =
            serde_json::from_value(response).expect("response should match v2 schema");
        client
            .shutdown()
            .await
            .expect("in-process runtime should shutdown cleanly");
    }

    #[tokio::test]
    async fn in_process_start_uses_requested_session_source_for_thread_start() {
        for (requested_source, expected_source) in [
            (SessionSource::Cli, ApiSessionSource::Cli),
            (SessionSource::Exec, ApiSessionSource::Exec),
        ] {
            let client = start_test_client(requested_source).await;
            let response = client
                .request(ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(2),
                    params: ThreadStartParams {
                        ephemeral: Some(true),
                        ..ThreadStartParams::default()
                    },
                })
                .await
                .expect("request transport should work")
                .expect("thread/start should succeed");
            let parsed: ThreadStartResponse =
                serde_json::from_value(response).expect("thread/start response should parse");
            assert_eq!(parsed.thread.source, expected_source);
            client
                .shutdown()
                .await
                .expect("in-process runtime should shutdown cleanly");
        }
    }

    #[tokio::test]
    async fn in_process_start_clamps_zero_channel_capacity() {
        let client =
            start_test_client_with_capacity(SessionSource::Cli, /*channel_capacity*/ 0).await;
        let response = loop {
            match client
                .request(ClientRequest::ConfigRequirementsRead {
                    request_id: RequestId::Integer(4),
                    params: None,
                })
                .await
            {
                Ok(response) => break response.expect("request should succeed"),
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::task::yield_now().await;
                }
                Err(err) => panic!("request transport should work: {err}"),
            }
        };
        let _parsed: ConfigRequirementsReadResponse =
            serde_json::from_value(response).expect("response should match v2 schema");
        client
            .shutdown()
            .await
            .expect("in-process runtime should shutdown cleanly");
    }

    #[tokio::test]
    async fn best_effort_local_controller_endpoint_failure_allows_startup() {
        let codex_home = TempDir::new().expect("temp dir");
        block_local_controller_endpoint_dir(codex_home.path());
        let args = build_test_start_args(
            codex_home.path(),
            SessionSource::Cli,
            DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
            InProcessLocalControllerEndpointConfig::BestEffort {
                main_thread_id: None,
            },
        )
        .await;

        let mut client = start(args)
            .await
            .expect("best-effort startup should continue");
        client._test_codex_home = Some(codex_home);

        assert!(matches!(
            client.local_controller_endpoint_status(),
            InProcessLocalControllerEndpointStatus::Unavailable { reason } if !reason.is_empty()
        ));
        assert_eq!(client.local_controller_endpoint(), None);
        client
            .shutdown()
            .await
            .expect("in-process runtime should shutdown cleanly");
    }

    #[tokio::test]
    async fn enabled_local_controller_endpoint_failure_fails_startup() {
        let codex_home = TempDir::new().expect("temp dir");
        block_local_controller_endpoint_dir(codex_home.path());
        let args = build_test_start_args(
            codex_home.path(),
            SessionSource::Cli,
            DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
            InProcessLocalControllerEndpointConfig::Enabled {
                main_thread_id: None,
            },
        )
        .await;

        let err = match start(args).await {
            Ok(client) => {
                let _ = client.shutdown().await;
                panic!("enabled local-controller endpoint failure should fail startup");
            }
            Err(err) => err,
        };
        assert!(
            !err.to_string().is_empty(),
            "startup error should explain endpoint setup failure"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_controller_request_participation_uses_native_tui_approval() {
        let codex_home = TempDir::new_in("/tmp").expect("temp dir");
        let args = build_test_start_args(
            codex_home.path(),
            SessionSource::Cli,
            DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
            InProcessLocalControllerEndpointConfig::Enabled {
                main_thread_id: None,
            },
        )
        .await;
        let mut client = start(args)
            .await
            .expect("local-controller startup should succeed");
        client._test_codex_home = Some(codex_home);

        let started: ThreadStartResponse = serde_json::from_value(
            client
                .request(ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(10_101),
                    params: ThreadStartParams::default(),
                })
                .await
                .expect("thread/start transport should work")
                .expect("thread/start should succeed"),
        )
        .expect("thread/start response should parse");
        let other_started: ThreadStartResponse = serde_json::from_value(
            client
                .request(ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(10_004),
                    params: ThreadStartParams::default(),
                })
                .await
                .expect("second thread/start transport should work")
                .expect("second thread/start should succeed"),
        )
        .expect("second thread/start response should parse");
        let _: ThreadSetNameResponse = serde_json::from_value(
            client
                .request(ClientRequest::ThreadSetName {
                    request_id: RequestId::Integer(10_005),
                    params: ThreadSetNameParams {
                        thread_id: started.thread.id.clone(),
                        name: "main-controller-filter-needle".to_string(),
                    },
                })
                .await
                .expect("main thread/name/set transport should work")
                .expect("main thread/name/set should succeed"),
        )
        .expect("main thread/name/set response should parse");
        let _: ThreadSetNameResponse = serde_json::from_value(
            client
                .request(ClientRequest::ThreadSetName {
                    request_id: RequestId::Integer(10_006),
                    params: ThreadSetNameParams {
                        thread_id: other_started.thread.id.clone(),
                        name: "other-controller-filter-needle".to_string(),
                    },
                })
                .await
                .expect("other thread/name/set transport should work")
                .expect("other thread/name/set should succeed"),
        )
        .expect("other thread/name/set response should parse");

        let metadata = client
            .local_controller_endpoint()
            .cloned()
            .expect("local-controller endpoint should be published");
        let mut websocket = connect_local_controller_websocket(&metadata).await;
        send_websocket_request(
            &mut websocket,
            /*request_id*/ 20_101,
            "initialize",
            Some(serde_json::json!({
                "clientInfo": {
                    "name": "codex-waveshare",
                    "version": "0.0.0-test",
                },
                "capabilities": {
                    "experimentalApi": true,
                },
            })),
        )
        .await;
        let initialize_response: serde_json::Value =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_101).await;
        assert!(initialize_response.get("userAgent").is_some());

        send_websocket_typed_request(
            &mut websocket,
            /*request_id*/ 20_102,
            "controller/requestParticipation",
            &ControllerRequestParticipationParams {
                controller_name: "codex-waveshare".to_string(),
                description: "external test controller".to_string(),
            },
        )
        .await;
        approve_next_native_controller_participation(
            &mut client,
            "codex-waveshare",
            "external test controller",
            started.thread.id.as_str(),
        )
        .await;

        let participation: ControllerRequestParticipationResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_102).await;
        assert_eq!(
            participation.status,
            ControllerParticipationStatus::Approved
        );
        let session = participation.session.expect("approved session");
        assert_eq!(session.main_thread_id, started.thread.id);
        assert!(session.active_lease.is_some());
        assert!(session.effective_capabilities.read_main_thread);
        assert!(session.effective_capabilities.mutate_main_thread);
        expect_next_controller_ownership_status(
            &mut client,
            started.thread.id.as_str(),
            InProcessControllerOwnershipStatusOwner::Controller {
                session_id: session.session_id.clone(),
            },
            /*expected_owner_epoch*/ 1,
            ControllerControlOwnershipChangedReason::InitialLeaseGranted,
        )
        .await;

        client
            .shutdown()
            .await
            .expect("in-process runtime should shutdown cleanly");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_controller_initialize_suppresses_pre_participation_notifications() {
        let codex_home = TempDir::new_in("/tmp").expect("temp dir");
        let mut args = build_test_start_args(
            codex_home.path(),
            SessionSource::Cli,
            DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
            InProcessLocalControllerEndpointConfig::Enabled {
                main_thread_id: None,
            },
        )
        .await;
        args.config_warnings = vec![ConfigWarningNotification {
            summary: "pre-participation warning".to_string(),
            details: None,
            path: None,
            range: None,
        }];
        let mut client = start(args)
            .await
            .expect("local-controller startup should succeed");
        client._test_codex_home = Some(codex_home);

        let metadata = client
            .local_controller_endpoint()
            .cloned()
            .expect("local-controller endpoint should be published");
        let mut websocket = connect_local_controller_websocket(&metadata).await;
        send_websocket_request(
            &mut websocket,
            /*request_id*/ 19_001,
            "initialize",
            Some(serde_json::json!({
                "clientInfo": {
                    "name": "codex-waveshare",
                    "version": "0.0.0-test",
                },
                "capabilities": {
                    "experimentalApi": true,
                },
            })),
        )
        .await;

        let JSONRPCMessage::Response(initialize_response) =
            read_websocket_message(&mut websocket).await
        else {
            panic!("external controller should receive initialize response first");
        };
        assert_eq!(initialize_response.id, RequestId::Integer(19_001));
        assert!(initialize_response.result.get("userAgent").is_some());
        assert!(
            timeout(
                Duration::from_millis(50),
                read_websocket_message(&mut websocket)
            )
            .await
            .is_err(),
            "external controller should not receive runtime notifications before participation"
        );

        client
            .shutdown()
            .await
            .expect("in-process runtime should shutdown cleanly");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_controller_socket_uses_main_thread_interface_and_tui_reclaim() {
        let codex_home = TempDir::new_in("/tmp").expect("temp dir");
        let args = build_test_start_args(
            codex_home.path(),
            SessionSource::Cli,
            DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
            InProcessLocalControllerEndpointConfig::Enabled {
                main_thread_id: None,
            },
        )
        .await;
        let mut client = start(args)
            .await
            .expect("local-controller startup should succeed");
        client._test_codex_home = Some(codex_home);

        let started: ThreadStartResponse = serde_json::from_value(
            client
                .request(ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(10_001),
                    params: ThreadStartParams::default(),
                })
                .await
                .expect("thread/start transport should work")
                .expect("thread/start should succeed"),
        )
        .expect("thread/start response should parse");

        let metadata = client
            .local_controller_endpoint()
            .cloned()
            .expect("local-controller endpoint should be published");
        let mut websocket = connect_local_controller_websocket(&metadata).await;
        send_websocket_request(
            &mut websocket,
            /*request_id*/ 20_001,
            "initialize",
            Some(serde_json::json!({
                "clientInfo": {
                    "name": "codex-waveshare",
                    "version": "0.0.0-test",
                },
                "capabilities": {
                    "experimentalApi": true,
                },
            })),
        )
        .await;
        let initialize_response: serde_json::Value =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_001).await;
        assert!(initialize_response.get("userAgent").is_some());

        send_websocket_typed_request(
            &mut websocket,
            /*request_id*/ 20_002,
            "controller/requestParticipation",
            &ControllerRequestParticipationParams {
                controller_name: "codex-waveshare".to_string(),
                description: "external test controller".to_string(),
            },
        )
        .await;
        approve_next_native_controller_participation(
            &mut client,
            "codex-waveshare",
            "external test controller",
            started.thread.id.as_str(),
        )
        .await;

        let participation: ControllerRequestParticipationResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_002).await;
        assert_eq!(
            participation.status,
            ControllerParticipationStatus::Approved
        );
        let session = participation.session.expect("approved session");
        assert_eq!(session.main_thread_id, started.thread.id);
        assert!(session.active_lease.is_some());
        assert!(session.effective_capabilities.read_main_thread);
        assert!(session.effective_capabilities.mutate_main_thread);
        expect_next_controller_ownership_status(
            &mut client,
            started.thread.id.as_str(),
            InProcessControllerOwnershipStatusOwner::Controller {
                session_id: session.session_id.clone(),
            },
            /*expected_owner_epoch*/ 1,
            ControllerControlOwnershipChangedReason::InitialLeaseGranted,
        )
        .await;

        send_websocket_typed_request(
            &mut websocket,
            /*request_id*/ 20_003,
            "thread/list",
            &serde_json::json!({ "limit": 100 }),
        )
        .await;
        let listed: ThreadListResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_003).await;
        assert_eq!(
            listed
                .data
                .iter()
                .map(|thread| thread.id.clone())
                .collect::<Vec<_>>(),
            vec![started.thread.id.clone()]
        );

        send_websocket_typed_request(
            &mut websocket,
            /*request_id*/ 20_009,
            "thread/loaded/list",
            &ThreadLoadedListParams {
                cursor: None,
                limit: Some(100),
            },
        )
        .await;
        let loaded: ThreadLoadedListResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_009).await;
        assert_eq!(loaded.data, vec![started.thread.id.clone()]);
        assert_eq!(loaded.next_cursor, None);

        send_websocket_typed_request(
            &mut websocket,
            /*request_id*/ 20_010,
            "thread/search",
            &ThreadSearchParams {
                cursor: None,
                limit: Some(10),
                sort_key: None,
                sort_direction: None,
                source_kinds: None,
                archived: None,
                search_term: "controller-filter-needle".to_string(),
            },
        )
        .await;
        let searched: ThreadSearchResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_010).await;
        assert_eq!(searched.data, Vec::new());
        assert_eq!(searched.next_cursor, None);
        assert_eq!(searched.backwards_cursor, None);

        send_websocket_typed_request(
            &mut websocket,
            /*request_id*/ 20_004,
            "thread/name/set",
            &ThreadSetNameParams {
                thread_id: started.thread.id.clone(),
                name: "controller-owned".to_string(),
            },
        )
        .await;
        let _: ThreadSetNameResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_004).await;

        let _: ThreadSetNameResponse = serde_json::from_value(
            client
                .request(ClientRequest::ThreadSetName {
                    request_id: RequestId::Integer(10_002),
                    params: ThreadSetNameParams {
                        thread_id: started.thread.id.clone(),
                        name: "tui-reclaimed".to_string(),
                    },
                })
                .await
                .expect("TUI thread/name/set transport should work")
                .expect("TUI thread/name/set should succeed"),
        )
        .expect("thread/name/set response should parse");
        expect_next_controller_ownership_status(
            &mut client,
            started.thread.id.as_str(),
            InProcessControllerOwnershipStatusOwner::Tui,
            /*expected_owner_epoch*/ 2,
            ControllerControlOwnershipChangedReason::ReclaimedByTui,
        )
        .await;

        send_websocket_typed_request(
            &mut websocket,
            /*request_id*/ 20_005,
            "thread/name/set",
            &ThreadSetNameParams {
                thread_id: started.thread.id.clone(),
                name: "stale-controller".to_string(),
            },
        )
        .await;
        let stale_error = read_websocket_error(&mut websocket, /*expected_id*/ 20_005).await;
        let stale_error_data: ControllerErrorData = serde_json::from_value(
            stale_error
                .error
                .data
                .expect("stale ownership error should include data"),
        )
        .expect("controller error data should parse");
        assert_eq!(stale_error_data.code, ControllerErrorCode::StaleOwnership);

        send_websocket_typed_request(
            &mut websocket,
            /*request_id*/ 20_006,
            "thread/read",
            &ThreadReadParams {
                thread_id: started.thread.id.clone(),
                include_turns: false,
            },
        )
        .await;
        let read_after_reclaim: ThreadReadResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_006).await;
        assert_eq!(
            read_after_reclaim.thread.name,
            Some("tui-reclaimed".to_string())
        );

        send_websocket_request(
            &mut websocket,
            /*request_id*/ 20_007,
            "controller/acquireControl",
            None,
        )
        .await;
        let reacquired: ControllerAcquireControlResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_007).await;
        assert!(reacquired.session.active_lease.is_some());
        assert!(reacquired.session.effective_capabilities.mutate_main_thread);
        expect_next_controller_ownership_status(
            &mut client,
            started.thread.id.as_str(),
            InProcessControllerOwnershipStatusOwner::Controller {
                session_id: reacquired.session.session_id.clone(),
            },
            /*expected_owner_epoch*/ 3,
            ControllerControlOwnershipChangedReason::Acquired,
        )
        .await;

        send_websocket_typed_request(
            &mut websocket,
            /*request_id*/ 20_008,
            "thread/name/set",
            &ThreadSetNameParams {
                thread_id: started.thread.id.clone(),
                name: "controller-reacquired".to_string(),
            },
        )
        .await;
        let _: ThreadSetNameResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_008).await;

        let final_read: ThreadReadResponse = serde_json::from_value(
            client
                .request(ClientRequest::ThreadRead {
                    request_id: RequestId::Integer(10_003),
                    params: ThreadReadParams {
                        thread_id: started.thread.id.clone(),
                        include_turns: false,
                    },
                })
                .await
                .expect("TUI thread/read transport should work")
                .expect("TUI thread/read should succeed"),
        )
        .expect("thread/read response should parse");
        assert_eq!(
            final_read.thread.name,
            Some("controller-reacquired".to_string())
        );

        send_websocket_request(
            &mut websocket,
            /*request_id*/ 20_011,
            "controller/signOff",
            None,
        )
        .await;
        let _: ControllerSignOffResponse =
            read_websocket_response(&mut websocket, /*expected_id*/ 20_011).await;
        expect_next_controller_ownership_status(
            &mut client,
            started.thread.id.as_str(),
            InProcessControllerOwnershipStatusOwner::Tui,
            /*expected_owner_epoch*/ 4,
            ControllerControlOwnershipChangedReason::SignOff,
        )
        .await;
        expect_websocket_closed(&mut websocket).await;

        client
            .shutdown()
            .await
            .expect("in-process runtime should shutdown cleanly");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_controller_socket_preserves_single_active_controller_lease() {
        let codex_home = TempDir::new_in("/tmp").expect("temp dir");
        let args = build_test_start_args(
            codex_home.path(),
            SessionSource::Cli,
            DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
            InProcessLocalControllerEndpointConfig::Enabled {
                main_thread_id: None,
            },
        )
        .await;
        let mut client = start(args)
            .await
            .expect("local-controller startup should succeed");
        client._test_codex_home = Some(codex_home);

        let started: ThreadStartResponse = serde_json::from_value(
            client
                .request(ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(11_001),
                    params: ThreadStartParams::default(),
                })
                .await
                .expect("thread/start transport should work")
                .expect("thread/start should succeed"),
        )
        .expect("thread/start response should parse");

        let metadata = client
            .local_controller_endpoint()
            .cloned()
            .expect("local-controller endpoint should be published");
        let mut first_websocket = connect_local_controller_websocket(&metadata).await;
        let mut second_websocket = connect_local_controller_websocket(&metadata).await;

        for (request_id, websocket) in [
            (21_001, &mut first_websocket),
            (22_001, &mut second_websocket),
        ] {
            send_websocket_request(
                websocket,
                request_id,
                "initialize",
                Some(serde_json::json!({
                    "clientInfo": {
                        "name": "codex-waveshare",
                        "version": "0.0.0-test",
                    },
                    "capabilities": {
                        "experimentalApi": true,
                    },
                })),
            )
            .await;
            let initialize_response: serde_json::Value =
                read_websocket_response(websocket, request_id).await;
            assert!(initialize_response.get("userAgent").is_some());
        }

        send_websocket_typed_request(
            &mut first_websocket,
            /*request_id*/ 21_002,
            "controller/requestParticipation",
            &ControllerRequestParticipationParams {
                controller_name: "codex-waveshare-primary".to_string(),
                description: "primary external test controller".to_string(),
            },
        )
        .await;
        approve_next_native_controller_participation(
            &mut client,
            "codex-waveshare-primary",
            "primary external test controller",
            started.thread.id.as_str(),
        )
        .await;

        let first_participation: ControllerRequestParticipationResponse =
            read_websocket_response(&mut first_websocket, /*expected_id*/ 21_002).await;
        assert_eq!(
            first_participation.status,
            ControllerParticipationStatus::Approved
        );
        let first_session = first_participation.session.expect("approved session");
        assert_eq!(first_session.main_thread_id, started.thread.id);
        assert!(first_session.active_lease.is_some());
        assert!(first_session.effective_capabilities.mutate_main_thread);
        expect_next_controller_ownership_status(
            &mut client,
            started.thread.id.as_str(),
            InProcessControllerOwnershipStatusOwner::Controller {
                session_id: first_session.session_id.clone(),
            },
            /*expected_owner_epoch*/ 1,
            ControllerControlOwnershipChangedReason::InitialLeaseGranted,
        )
        .await;

        send_websocket_typed_request(
            &mut second_websocket,
            /*request_id*/ 22_002,
            "controller/requestParticipation",
            &ControllerRequestParticipationParams {
                controller_name: "codex-waveshare-secondary".to_string(),
                description: "secondary external test controller".to_string(),
            },
        )
        .await;
        approve_next_native_controller_participation(
            &mut client,
            "codex-waveshare-secondary",
            "secondary external test controller",
            started.thread.id.as_str(),
        )
        .await;

        let second_participation: ControllerRequestParticipationResponse =
            read_websocket_response(&mut second_websocket, /*expected_id*/ 22_002).await;
        assert_eq!(
            second_participation.status,
            ControllerParticipationStatus::Approved
        );
        let second_session = second_participation.session.expect("approved session");
        assert_eq!(second_session.main_thread_id, started.thread.id);
        assert_eq!(second_session.active_lease, None);
        assert!(second_session.effective_capabilities.read_main_thread);
        assert!(!second_session.effective_capabilities.acquire_control);
        assert!(!second_session.effective_capabilities.mutate_main_thread);

        send_websocket_request(
            &mut second_websocket,
            /*request_id*/ 22_003,
            "controller/acquireControl",
            None,
        )
        .await;
        let acquire_conflict =
            read_websocket_error(&mut second_websocket, /*expected_id*/ 22_003).await;
        let acquire_conflict_data: ControllerErrorData = serde_json::from_value(
            acquire_conflict
                .error
                .data
                .expect("ownership conflict error should include data"),
        )
        .expect("controller error data should parse");
        assert_eq!(
            acquire_conflict_data.code,
            ControllerErrorCode::OwnershipConflict
        );

        send_websocket_typed_request(
            &mut second_websocket,
            /*request_id*/ 22_004,
            "thread/read",
            &ThreadReadParams {
                thread_id: started.thread.id.clone(),
                include_turns: false,
            },
        )
        .await;
        let second_read: ThreadReadResponse =
            read_websocket_response(&mut second_websocket, /*expected_id*/ 22_004).await;
        assert_eq!(second_read.thread.id, started.thread.id);

        send_websocket_request(
            &mut first_websocket,
            /*request_id*/ 21_003,
            "controller/releaseControl",
            None,
        )
        .await;
        let first_release: ControllerReleaseControlResponse =
            read_websocket_response(&mut first_websocket, /*expected_id*/ 21_003).await;
        assert_eq!(first_release.session.active_lease, None);
        expect_next_controller_ownership_status(
            &mut client,
            started.thread.id.as_str(),
            InProcessControllerOwnershipStatusOwner::Tui,
            /*expected_owner_epoch*/ 2,
            ControllerControlOwnershipChangedReason::Released,
        )
        .await;

        send_websocket_request(
            &mut second_websocket,
            /*request_id*/ 22_005,
            "controller/acquireControl",
            None,
        )
        .await;
        let second_acquire: ControllerAcquireControlResponse =
            read_websocket_response(&mut second_websocket, /*expected_id*/ 22_005).await;
        assert!(second_acquire.session.active_lease.is_some());
        assert!(
            second_acquire
                .session
                .effective_capabilities
                .mutate_main_thread
        );
        expect_next_controller_ownership_status(
            &mut client,
            started.thread.id.as_str(),
            InProcessControllerOwnershipStatusOwner::Controller {
                session_id: second_session.session_id.clone(),
            },
            /*expected_owner_epoch*/ 3,
            ControllerControlOwnershipChangedReason::Acquired,
        )
        .await;

        send_websocket_typed_request(
            &mut first_websocket,
            /*request_id*/ 21_004,
            "thread/name/set",
            &ThreadSetNameParams {
                thread_id: started.thread.id.clone(),
                name: "first-controller-should-not-mutate".to_string(),
            },
        )
        .await;
        let stale_first_mutation =
            read_websocket_error(&mut first_websocket, /*expected_id*/ 21_004).await;
        let stale_first_mutation_data: ControllerErrorData = serde_json::from_value(
            stale_first_mutation
                .error
                .data
                .expect("ownership conflict error should include data"),
        )
        .expect("controller error data should parse");
        assert_eq!(
            stale_first_mutation_data.code,
            ControllerErrorCode::OwnershipConflict
        );

        client
            .shutdown()
            .await
            .expect("in-process runtime should shutdown cleanly");
    }

    #[tokio::test(start_paused = true)]
    async fn in_process_outbound_router_shutdown_does_not_wait_for_retained_sender() {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(/*buffer*/ 1);
        let retained_outgoing_tx = outgoing_tx.clone();
        drop(outgoing_tx);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (_control_tx, control_rx) = mpsc::channel(/*buffer*/ 1);
        let mut outbound_handle = tokio::spawn(run_outbound_router(
            outgoing_rx,
            control_rx,
            HashMap::new(),
            shutdown_rx,
        ));

        assert!(!retained_outgoing_tx.is_closed());
        shutdown_tx
            .send(())
            .expect("outbound router should accept explicit shutdown");
        timeout(SHUTDOWN_TIMEOUT, &mut outbound_handle)
            .await
            .expect("outbound router should not wait for its retained sender")
            .expect("outbound router should complete successfully");
        assert!(retained_outgoing_tx.is_closed());
    }

    #[tokio::test]
    async fn in_process_outbound_router_disconnect_and_close_requests_disconnect() {
        let (_outgoing_tx, outgoing_rx) = mpsc::channel(/*buffer*/ 1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (control_tx, control_rx) = mpsc::channel(/*buffer*/ 1);
        let (writer_tx, mut writer_rx) = mpsc::channel(/*buffer*/ 1);
        let disconnect_token = CancellationToken::new();
        let connection_id = ConnectionId(42);
        let outbound_connections = HashMap::from([(
            connection_id,
            OutboundConnectionState::new_with_origin(
                ConnectionOrigin::ExternalController,
                writer_tx,
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(true)),
                Arc::new(RwLock::new(HashSet::new())),
                Some(disconnect_token.clone()),
            ),
        )]);
        let mut outbound_handle = tokio::spawn(run_outbound_router(
            outgoing_rx,
            control_rx,
            outbound_connections,
            shutdown_rx,
        ));

        control_tx
            .send(InProcessOutboundControlEvent::DisconnectAndClose { connection_id })
            .await
            .expect("disconnect control event should send");

        timeout(SHUTDOWN_TIMEOUT, disconnect_token.cancelled())
            .await
            .expect("disconnect token should be cancelled");
        assert!(
            writer_rx.recv().await.is_none(),
            "outbound writer should be dropped after disconnect-and-close"
        );

        drop(control_tx);
        let _ = shutdown_tx.send(());
        timeout(SHUTDOWN_TIMEOUT, &mut outbound_handle)
            .await
            .expect("outbound router should stop")
            .expect("outbound router should complete successfully");
    }

    #[tokio::test(start_paused = true)]
    async fn in_process_shutdown_waits_for_analytics_flush_budget() {
        let (client_tx, mut client_rx) = mpsc::channel(/*buffer*/ 1);
        let (_event_tx, event_rx) = mpsc::channel(/*buffer*/ 1);
        let completed = Arc::new(AtomicBool::new(false));
        let runtime_completed = Arc::clone(&completed);
        let runtime_handle = tokio::spawn(async move {
            let done_tx = match client_rx.recv().await {
                Some(InProcessClientMessage::Shutdown { done_tx }) => done_tx,
                _ => panic!("expected in-process shutdown request"),
            };
            tokio::time::sleep(SHUTDOWN_TIMEOUT + SHUTDOWN_TIMEOUT + Duration::from_secs(24)).await;
            runtime_completed.store(true, Ordering::Release);
            let _ = done_tx.send(());
        });
        let client = InProcessClientHandle {
            client: InProcessClientSender { client_tx },
            event_rx,
            runtime_handle,
            local_controller_endpoint_status: InProcessLocalControllerEndpointStatus::Disabled,
            _test_codex_home: None,
        };

        client
            .shutdown()
            .await
            .expect("in-process runtime should shutdown cleanly");
        assert!(completed.load(Ordering::Acquire));
    }

    #[test]
    fn guaranteed_delivery_helpers_cover_transcript_and_terminal_server_notifications() {
        assert!(server_notification_requires_delivery(
            &ServerNotification::TurnCompleted(TurnCompletedNotification {
                thread_id: "thread-1".to_string(),
                turn: Turn {
                    id: "turn-1".to_string(),
                    items: Vec::new(),
                    items_view: TurnItemsView::NotLoaded,
                    status: TurnStatus::Completed,
                    error: None,
                    started_at: None,
                    completed_at: Some(0),
                    duration_ms: None,
                },
            })
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ThreadStatusChanged(ThreadStatusChangedNotification {
                thread_id: "thread-1".to_string(),
                status: ThreadStatus::Idle,
            })
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ThreadArchived(ThreadArchivedNotification {
                thread_id: "thread-1".to_string(),
            })
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ThreadDeleted(ThreadDeletedNotification {
                thread_id: "thread-1".to_string(),
            })
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ThreadUnarchived(ThreadUnarchivedNotification {
                thread_id: "thread-1".to_string(),
            })
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ThreadClosed(ThreadClosedNotification {
                thread_id: "thread-1".to_string(),
            })
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ThreadNameUpdated(ThreadNameUpdatedNotification {
                thread_id: "thread-1".to_string(),
                thread_name: Some("renamed".to_string()),
            })
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::AgentMessageDelta(
                codex_app_server_protocol::AgentMessageDeltaNotification {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    delta: "hello".to_string(),
                },
            )
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::PlanDelta(codex_app_server_protocol::PlanDeltaNotification {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "item-1".to_string(),
                delta: "plan".to_string(),
            },)
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ReasoningSummaryTextDelta(
                codex_app_server_protocol::ReasoningSummaryTextDeltaNotification {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    delta: "summary".to_string(),
                    summary_index: 0,
                },
            )
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ReasoningTextDelta(
                codex_app_server_protocol::ReasoningTextDeltaNotification {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    delta: "reasoning".to_string(),
                    content_index: 0,
                },
            )
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ItemCompleted(ItemCompletedNotification {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                completed_at_ms: 0,
                item: ThreadItem::AgentMessage {
                    id: "item-1".to_string(),
                    text: "hello".to_string(),
                    phase: None,
                    memory_citation: None,
                },
            })
        ));
        assert!(server_notification_requires_delivery(
            &ServerNotification::ExternalAgentConfigImportCompleted(
                ExternalAgentConfigImportCompletedNotification {
                    import_id: "import".to_string(),
                    item_type_results: Vec::new(),
                },
            )
        ));
    }
}
