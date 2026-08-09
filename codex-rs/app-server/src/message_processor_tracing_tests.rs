use super::ConnectionSessionState;
use super::MessageProcessor;
use super::MessageProcessorArgs;
use crate::analytics_utils::analytics_events_client_from_config;
use crate::config_manager::ConfigManager;
use crate::connection_rpc_gate::EXTERNAL_CONTROLLER_CONTROL_RPC_QUEUE_CAPACITY;
use crate::connection_rpc_gate::EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY;
use crate::controller_enrollment::ControllerCredentialProof;
use crate::controller_enrollment::ControllerEnrollmentRecord;
use crate::controller_enrollment::ControllerEnrollmentSource;
use crate::controller_enrollment::EmptyControllerEnrollmentSource;
use crate::current_time::current_time_request_recipients;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingMessageSender;
use crate::outgoing_message::ThreadScopedOutgoingMessageSender;
use crate::request_serialization::RequestSerializationAccess;
use crate::request_serialization::RequestSerializationQueueKey;
use crate::transport::AppServerTransport;
use crate::transport::ConnectionOrigin;
use anyhow::Result;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::write_mock_responses_config_toml;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::CommandExecutionApprovalDecision;
use codex_app_server_protocol::CommandExecutionRequestApprovalParams;
use codex_app_server_protocol::CommandExecutionRequestApprovalResponse;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::ControllerAcquireControlResponse;
use codex_app_server_protocol::ControllerAuthorizationChangedNotification;
use codex_app_server_protocol::ControllerAuthorizationChangedReason;
use codex_app_server_protocol::ControllerControlOwnershipChangedNotification;
use codex_app_server_protocol::ControllerControlOwnershipChangedReason;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::ControllerParticipationStatus;
use codex_app_server_protocol::ControllerReleaseControlResponse;
use codex_app_server_protocol::ControllerRequestParticipationParams;
use codex_app_server_protocol::ControllerRequestParticipationResponse;
use codex_app_server_protocol::ControllerRetryDisposition;
use codex_app_server_protocol::ControllerSignOffResponse;
use codex_app_server_protocol::CurrentTimeReadParams;
use codex_app_server_protocol::CurrentTimeReadResponse;
use codex_app_server_protocol::ExecPolicyAmendment;
use codex_app_server_protocol::FileChangeApprovalDecision;
use codex_app_server_protocol::FileChangeRequestApprovalParams;
use codex_app_server_protocol::FileChangeRequestApprovalResponse;
use codex_app_server_protocol::GrantedPermissionProfile;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::InitializeResponse;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::McpServerOauthLoginCompletedNotification;
use codex_app_server_protocol::NetworkPolicyAmendment;
use codex_app_server_protocol::NetworkPolicyRuleAction;
use codex_app_server_protocol::PermissionGrantScope;
use codex_app_server_protocol::PermissionsRequestApprovalParams;
use codex_app_server_protocol::PermissionsRequestApprovalResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::RequestPermissionProfile;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ServerRequestPayload;
use codex_app_server_protocol::ThreadArchiveParams;
use codex_app_server_protocol::ThreadDeleteParams;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadSetNameParams;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use codex_exec_server::EnvironmentManager;
use codex_feedback::CodexFeedback;
use codex_login::AuthManager;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::W3cTraceContext;
use codex_state::DirectionalThreadSpawnEdgeStatus;
use codex_state::StateRuntime;
use codex_utils_absolute_path::test_support::PathExt;
use opentelemetry::global;
use opentelemetry::trace::SpanId;
use opentelemetry::trace::SpanKind;
use opentelemetry::trace::TraceId;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::InMemorySpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::SpanData;
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing_subscriber::layer::SubscriberExt;
use wiremock::MockServer;

const TEST_CONNECTION_ID: ConnectionId = ConnectionId(7);
const EXTERNAL_CONNECTION_ID: ConnectionId = ConnectionId(8);
const SECOND_EXTERNAL_CONNECTION_ID: ConnectionId = ConnectionId(9);

struct TestTracing {
    exporter: InMemorySpanExporter,
    provider: SdkTracerProvider,
}

struct RemoteTrace {
    trace_id: TraceId,
    parent_span_id: SpanId,
    context: W3cTraceContext,
}

impl RemoteTrace {
    fn new(trace_id: &str, parent_span_id: &str) -> Self {
        let trace_id = TraceId::from_hex(trace_id).expect("trace id");
        let parent_span_id = SpanId::from_hex(parent_span_id).expect("parent span id");
        let context = W3cTraceContext {
            traceparent: Some(format!("00-{trace_id}-{parent_span_id}-01")),
            tracestate: Some("vendor=value".to_string()),
        };

        Self {
            trace_id,
            parent_span_id,
            context,
        }
    }
}

fn init_test_tracing() -> &'static TestTracing {
    static TEST_TRACING: OnceLock<TestTracing> = OnceLock::new();
    TEST_TRACING.get_or_init(|| {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("codex-app-server-message-processor-tests");
        global::set_text_map_propagator(TraceContextPropagator::new());
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        tracing::subscriber::set_global_default(subscriber)
            .expect("global tracing subscriber should only be installed once");
        TestTracing { exporter, provider }
    })
}

fn request_from_client_request(request: ClientRequest) -> JSONRPCRequest {
    serde_json::from_value(serde_json::to_value(request).expect("serialize client request"))
        .expect("client request should convert to JSON-RPC")
}

fn integer_request_id(request: &ClientRequest) -> i64 {
    match request.id() {
        RequestId::Integer(request_id) => *request_id,
        request_id => panic!("expected integer request id in test harness, got {request_id:?}"),
    }
}

#[derive(Default)]
struct TestControllerEnrollmentSource {
    records: StdMutex<HashMap<String, ControllerEnrollmentRecord>>,
}

impl TestControllerEnrollmentSource {
    fn insert(&self, record: ControllerEnrollmentRecord) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(record.subject_id.clone(), record);
    }
}

impl ControllerEnrollmentSource for TestControllerEnrollmentSource {
    fn enrollment_for(&self, subject_id: &str) -> Option<ControllerEnrollmentRecord> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(subject_id)
            .cloned()
    }
}

struct TracingHarness {
    _server: MockServer,
    _codex_home: TempDir,
    state_db: Option<Arc<StateRuntime>>,
    processor: Arc<MessageProcessor>,
    outgoing_rx: mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    session: Arc<ConnectionSessionState>,
    tracing: &'static TestTracing,
}

impl TracingHarness {
    async fn new() -> Result<Self> {
        Self::new_with_controller_enrollment_source(Arc::new(EmptyControllerEnrollmentSource)).await
    }

    async fn new_with_controller_enrollment_source(
        controller_enrollment_source: Arc<dyn ControllerEnrollmentSource>,
    ) -> Result<Self> {
        Self::new_inner(controller_enrollment_source, /*use_state_db*/ false).await
    }

    async fn new_inner(
        controller_enrollment_source: Arc<dyn ControllerEnrollmentSource>,
        use_state_db: bool,
    ) -> Result<Self> {
        let server = create_mock_responses_server_repeating_assistant("Done").await;
        let codex_home = TempDir::new()?;
        let config = Arc::new(build_test_config(codex_home.path(), &server.uri()).await?);
        let state_db = if use_state_db {
            Some(
                StateRuntime::init(
                    codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
                    "mock_provider".into(),
                )
                .await?,
            )
        } else {
            None
        };
        let (processor, outgoing_rx) =
            build_test_processor(config, controller_enrollment_source, state_db.clone()).await;
        let tracing = init_test_tracing();
        tracing.exporter.reset();
        tracing::callsite::rebuild_interest_cache();
        let mut harness = Self {
            _server: server,
            _codex_home: codex_home,
            state_db,
            processor,
            outgoing_rx,
            session: Arc::new(ConnectionSessionState::new()),
            tracing,
        };

        let _: InitializeResponse = harness
            .request(
                ClientRequest::Initialize {
                    request_id: RequestId::Integer(1),
                    params: InitializeParams {
                        client_info: ClientInfo {
                            name: "codex-app-server-tests".to_string(),
                            title: None,
                            version: "0.1.0".to_string(),
                        },
                        capabilities: Some(InitializeCapabilities {
                            experimental_api: true,
                            ..Default::default()
                        }),
                    },
                },
                /*trace*/ None,
            )
            .await;
        assert!(harness.session.initialized());

        Ok(harness)
    }

    fn reset_tracing(&self) {
        self.tracing.exporter.reset();
    }

    async fn shutdown(self) {
        self.processor.shutdown_threads().await;
        self.processor.drain_background_tasks().await;
    }

    async fn request<T>(&mut self, request: ClientRequest, trace: Option<W3cTraceContext>) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        self.request_with_origin(request, ConnectionOrigin::Stdio, trace)
            .await
    }

    async fn request_with_origin<T>(
        &mut self,
        request: ClientRequest,
        connection_origin: ConnectionOrigin,
        trace: Option<W3cTraceContext>,
    ) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        let request_id = match request.id() {
            RequestId::Integer(request_id) => *request_id,
            request_id => panic!("expected integer request id in test harness, got {request_id:?}"),
        };
        let mut request = request_from_client_request(request);
        request.trace = trace;

        self.processor
            .process_request(
                TEST_CONNECTION_ID,
                connection_origin,
                request,
                &AppServerTransport::Stdio,
                Arc::clone(&self.session),
            )
            .await;
        read_response(&mut self.outgoing_rx, request_id).await
    }

    async fn request_for_connection<T>(
        &mut self,
        connection_id: ConnectionId,
        connection_origin: ConnectionOrigin,
        session: Arc<ConnectionSessionState>,
        request: ClientRequest,
    ) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        let request_id = self
            .submit_for_connection(connection_id, connection_origin, session, request)
            .await;
        read_response_for_connection(&mut self.outgoing_rx, connection_id, request_id).await
    }

    async fn request_error_for_connection(
        &mut self,
        connection_id: ConnectionId,
        connection_origin: ConnectionOrigin,
        session: Arc<ConnectionSessionState>,
        request: ClientRequest,
    ) -> JSONRPCError {
        let request_id = self
            .submit_for_connection(connection_id, connection_origin, session, request)
            .await;
        read_error_for_connection(&mut self.outgoing_rx, connection_id, request_id).await
    }

    async fn submit_for_connection(
        &mut self,
        connection_id: ConnectionId,
        connection_origin: ConnectionOrigin,
        session: Arc<ConnectionSessionState>,
        request: ClientRequest,
    ) -> i64 {
        let request_id = integer_request_id(&request);
        let request = request_from_client_request(request);
        self.processor
            .process_request(
                connection_id,
                connection_origin,
                request,
                &AppServerTransport::Stdio,
                session,
            )
            .await;
        request_id
    }

    async fn raw_request_error_with_origin(
        &mut self,
        request: JSONRPCRequest,
        connection_origin: ConnectionOrigin,
    ) -> JSONRPCError {
        let request_id = match &request.id {
            RequestId::Integer(request_id) => *request_id,
            request_id => panic!("expected integer request id in test harness, got {request_id:?}"),
        };
        self.processor
            .process_request(
                TEST_CONNECTION_ID,
                connection_origin,
                request,
                &AppServerTransport::Stdio,
                Arc::clone(&self.session),
            )
            .await;
        read_error(&mut self.outgoing_rx, request_id).await
    }

    async fn start_thread(
        &mut self,
        request_id: i64,
        trace: Option<W3cTraceContext>,
    ) -> ThreadStartResponse {
        let response = self
            .request(
                ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(request_id),
                    params: ThreadStartParams {
                        ephemeral: Some(true),
                        ..ThreadStartParams::default()
                    },
                },
                trace,
            )
            .await;
        read_thread_started_notification(&mut self.outgoing_rx).await;
        response
    }
}

async fn build_test_config(codex_home: &Path, server_uri: &str) -> Result<Config> {
    write_mock_responses_config_toml(
        codex_home,
        server_uri,
        &BTreeMap::new(),
        /*auto_compact_limit*/ 8_192,
        Some(false),
        "mock_provider",
        "compact",
    )?;

    Ok(ConfigBuilder::default()
        .codex_home(codex_home.to_path_buf())
        .build()
        .await?)
}

async fn build_test_processor(
    config: Arc<Config>,
    controller_enrollment_source: Arc<dyn ControllerEnrollmentSource>,
    state_db: Option<Arc<StateRuntime>>,
) -> (
    Arc<MessageProcessor>,
    mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
) {
    let (outgoing_tx, outgoing_rx) = mpsc::channel(16);
    let auth_manager =
        AuthManager::shared_from_config(config.as_ref(), /*enable_codex_api_key_env*/ false)
            .await
            .expect("test auth manager");
    let config_manager = ConfigManager::new(
        config.codex_home.to_path_buf(),
        Vec::new(),
        LoaderOverrides::default(),
        /*strict_config*/ false,
        CloudConfigBundleLoader::default(),
        Arg0DispatchPaths::default(),
        Arc::new(codex_config::NoopThreadConfigLoader),
    );
    let analytics_events_client =
        analytics_events_client_from_config(Arc::clone(&auth_manager), config.as_ref());
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        analytics_events_client.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(MessageProcessorArgs {
        outgoing,
        analytics_events_client,
        arg0_paths: Arg0DispatchPaths::default(),
        config,
        config_manager,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db,
        config_warnings: Vec::new(),
        session_source: SessionSource::VSCode,
        auth_manager,
        installation_id: "11111111-1111-4111-8111-111111111111".to_string(),
        code_mode_session_provider: None,
        rpc_transport: AppServerRpcTransport::Stdio,
        remote_control_handle: None,
        controller_enrollment_source,
        native_controller_participation_approver: None,
        controller_ownership_status_tx: None,
        plugin_startup_tasks: crate::PluginStartupTasks::Start,
    }));
    (processor, outgoing_rx)
}

fn run_current_thread_test_with_stack<F>(name: &str, future: F) -> Result<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    const TEST_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

    let handle = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(TEST_STACK_SIZE_BYTES)
        .spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(Box::pin(future))
        })?;

    match handle.join() {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!("{name} thread panicked")),
    }
}

fn span_attr<'a>(span: &'a SpanData, key: &str) -> Option<&'a str> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .and_then(|kv| match &kv.value {
            opentelemetry::Value::String(value) => Some(value.as_str()),
            _ => None,
        })
}

fn find_rpc_span_with_trace<'a>(
    spans: &'a [SpanData],
    kind: SpanKind,
    method: &str,
    trace_id: TraceId,
) -> &'a SpanData {
    spans
        .iter()
        .find(|span| {
            span.span_kind == kind
                && span_attr(span, "rpc.system") == Some("jsonrpc")
                && span_attr(span, "rpc.method") == Some(method)
                && span.span_context.trace_id() == trace_id
        })
        .unwrap_or_else(|| {
            panic!(
                "missing {kind:?} span for rpc.method={method} trace={trace_id}; exported spans:\n{}",
                format_spans(spans)
            )
        })
}

fn find_span_with_trace<'a, F>(
    spans: &'a [SpanData],
    trace_id: TraceId,
    description: &str,
    predicate: F,
) -> &'a SpanData
where
    F: Fn(&SpanData) -> bool,
{
    spans
        .iter()
        .find(|span| span.span_context.trace_id() == trace_id && predicate(span))
        .unwrap_or_else(|| {
            panic!(
                "missing span matching {description} for trace={trace_id}; exported spans:\n{}",
                format_spans(spans)
            )
        })
}

fn format_spans(spans: &[SpanData]) -> String {
    spans
        .iter()
        .map(|span| {
            let rpc_method = span_attr(span, "rpc.method").unwrap_or("-");
            format!(
                "name={} span_id={} kind={:?} parent={} trace={} rpc.method={}",
                span.name,
                span.span_context.span_id(),
                span.span_kind,
                span.parent_span_id,
                span.span_context.trace_id(),
                rpc_method
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn span_depth_from_ancestor(
    spans: &[SpanData],
    child: &SpanData,
    ancestor: &SpanData,
) -> Option<usize> {
    let ancestor_span_id = ancestor.span_context.span_id();
    let mut parent_span_id = child.parent_span_id;
    let mut depth = 1;
    while parent_span_id != SpanId::INVALID {
        if parent_span_id == ancestor_span_id {
            return Some(depth);
        }
        let Some(parent_span) = spans
            .iter()
            .find(|span| span.span_context.span_id() == parent_span_id)
        else {
            break;
        };
        parent_span_id = parent_span.parent_span_id;
        depth += 1;
    }

    None
}

fn assert_span_descends_from(spans: &[SpanData], child: &SpanData, ancestor: &SpanData) {
    if span_depth_from_ancestor(spans, child, ancestor).is_some() {
        return;
    }

    panic!(
        "span {} does not descend from {}; exported spans:\n{}",
        child.name,
        ancestor.name,
        format_spans(spans)
    );
}

fn assert_has_internal_descendant_at_min_depth(
    spans: &[SpanData],
    ancestor: &SpanData,
    min_depth: usize,
) {
    if spans.iter().any(|span| {
        span.span_kind == SpanKind::Internal
            && span.span_context.trace_id() == ancestor.span_context.trace_id()
            && span_depth_from_ancestor(spans, span, ancestor)
                .is_some_and(|depth| depth >= min_depth)
    }) {
        return;
    }

    panic!(
        "missing internal descendant at depth >= {min_depth} below {}; exported spans:\n{}",
        ancestor.name,
        format_spans(spans)
    );
}

async fn read_response<T: serde::de::DeserializeOwned>(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    request_id: i64,
) -> T {
    read_response_for_connection(outgoing_rx, TEST_CONNECTION_ID, request_id).await
}

async fn read_response_for_connection<T: serde::de::DeserializeOwned>(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    expected_connection_id: ConnectionId,
    request_id: i64,
) -> T {
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("timed out waiting for response")
            .expect("outgoing channel closed");
        let (connection_id, message) = match envelope {
            crate::outgoing_message::OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                write_complete_tx,
            } => {
                acknowledge_write(write_complete_tx);
                (connection_id, message)
            }
            crate::outgoing_message::OutgoingEnvelope::ToConnectionThenDisconnect {
                connection_id,
                message,
            } => (connection_id, message),
            crate::outgoing_message::OutgoingEnvelope::Broadcast { .. } => continue,
        };
        if connection_id != expected_connection_id {
            continue;
        }
        let crate::outgoing_message::OutgoingMessage::Response(response) = message else {
            continue;
        };
        if response.id != RequestId::Integer(request_id) {
            continue;
        }
        return serde_json::from_value(
            serde_json::to_value(response.result).expect("response payload should serialize"),
        )
        .expect("response payload should deserialize");
    }
}

async fn read_error(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    request_id: i64,
) -> JSONRPCError {
    read_error_for_connection(outgoing_rx, TEST_CONNECTION_ID, request_id).await
}

async fn read_error_for_connection(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    expected_connection_id: ConnectionId,
    request_id: i64,
) -> JSONRPCError {
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("timed out waiting for error")
            .expect("outgoing channel closed");
        let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = envelope
        else {
            continue;
        };
        acknowledge_write(write_complete_tx);
        if connection_id != expected_connection_id {
            continue;
        }
        let crate::outgoing_message::OutgoingMessage::Error(error) = message else {
            continue;
        };
        if error.id != RequestId::Integer(request_id) {
            continue;
        }
        return JSONRPCError {
            error: error.error,
            id: error.id,
        };
    }
}

async fn read_thread_started_notification(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
) {
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("timed out waiting for thread/started notification")
            .expect("outgoing channel closed");
        match envelope {
            crate::outgoing_message::OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                ..
            }
            | crate::outgoing_message::OutgoingEnvelope::ToConnectionThenDisconnect {
                connection_id,
                message,
            } => {
                if connection_id != TEST_CONNECTION_ID {
                    continue;
                }
                let crate::outgoing_message::OutgoingMessage::AppServerNotification(notification) =
                    message
                else {
                    continue;
                };
                if matches!(
                    notification.notification,
                    codex_app_server_protocol::ServerNotification::ThreadStarted(_)
                ) {
                    return;
                }
            }
            crate::outgoing_message::OutgoingEnvelope::Broadcast { message } => {
                let crate::outgoing_message::OutgoingMessage::AppServerNotification(notification) =
                    message
                else {
                    continue;
                };
                if matches!(
                    notification.notification,
                    codex_app_server_protocol::ServerNotification::ThreadStarted(_)
                ) {
                    return;
                }
            }
        }
    }
}

async fn read_controller_notification_for_connection(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    expected_connection_id: ConnectionId,
) -> ServerNotification {
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("timed out waiting for controller notification")
            .expect("outgoing channel closed");
        let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = envelope
        else {
            continue;
        };
        acknowledge_write(write_complete_tx);
        if connection_id != expected_connection_id {
            continue;
        }
        let crate::outgoing_message::OutgoingMessage::AppServerNotification(notification) = message
        else {
            continue;
        };
        if matches!(
            notification.notification,
            ServerNotification::ControllerAuthorizationChanged(_)
                | ServerNotification::ControllerControlOwnershipChanged(_)
        ) {
            return notification.notification;
        }
    }
}

async fn read_mcp_oauth_completed_notification_for_connection(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    expected_connection_id: ConnectionId,
) -> McpServerOauthLoginCompletedNotification {
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("timed out waiting for MCP OAuth completion notification")
            .expect("outgoing channel closed");
        let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = envelope
        else {
            continue;
        };
        acknowledge_write(write_complete_tx);
        if connection_id != expected_connection_id {
            continue;
        }
        let crate::outgoing_message::OutgoingMessage::AppServerNotification(notification) = message
        else {
            continue;
        };
        let ServerNotification::McpServerOauthLoginCompleted(notification) =
            notification.notification
        else {
            continue;
        };
        return notification;
    }
}

async fn read_controller_authorization_changed_for_connection(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    expected_connection_id: ConnectionId,
) -> ControllerAuthorizationChangedNotification {
    let ServerNotification::ControllerAuthorizationChanged(notification) =
        read_controller_notification_for_connection(outgoing_rx, expected_connection_id).await
    else {
        panic!("expected controller authorization notification");
    };
    notification
}

async fn read_controller_ownership_changed_for_connection(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    expected_connection_id: ConnectionId,
) -> ControllerControlOwnershipChangedNotification {
    let ServerNotification::ControllerControlOwnershipChanged(notification) =
        read_controller_notification_for_connection(outgoing_rx, expected_connection_id).await
    else {
        panic!("expected controller ownership notification");
    };
    notification
}

async fn fill_outgoing_queue(outgoing: &Arc<OutgoingMessageSender>) {
    for index in 0..16 {
        outgoing
            .send_server_notification_to_connections(
                &[TEST_CONNECTION_ID],
                ServerNotification::ConfigWarning(ConfigWarningNotification {
                    summary: format!("queue blocker {index}"),
                    details: None,
                    path: None,
                    range: None,
                }),
            )
            .await;
    }
}

async fn drain_outgoing_envelopes(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    count: usize,
) {
    for _ in 0..count {
        let envelope = tokio::time::timeout(Duration::from_secs(1), outgoing_rx.recv())
            .await
            .expect("timed out waiting to drain outgoing envelope")
            .expect("outgoing channel closed");
        if let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            write_complete_tx, ..
        } = envelope
        {
            acknowledge_write(write_complete_tx);
        }
    }
}

async fn read_server_request_for_connection(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    expected_connection_id: ConnectionId,
    expected_request_id: &RequestId,
) -> ServerRequest {
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("timed out waiting for server request")
            .expect("outgoing channel closed");
        let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } = envelope
        else {
            continue;
        };
        acknowledge_write(write_complete_tx);
        if connection_id != expected_connection_id {
            continue;
        }
        let crate::outgoing_message::OutgoingMessage::Request(request) = message else {
            continue;
        };
        if request.id() == expected_request_id {
            return request;
        }
    }
}

fn acknowledge_write(write_complete_tx: Option<tokio::sync::oneshot::Sender<()>>) {
    if let Some(write_complete_tx) = write_complete_tx {
        let _ = write_complete_tx.send(());
    }
}

async fn wait_for_exported_spans<F>(tracing: &TestTracing, predicate: F) -> Vec<SpanData>
where
    F: Fn(&[SpanData]) -> bool,
{
    let mut last_spans = Vec::new();
    for _ in 0..200 {
        tokio::task::yield_now().await;
        tracing
            .provider
            .force_flush()
            .expect("force flush should succeed");
        let spans = tracing.exporter.get_finished_spans().expect("span export");
        last_spans = spans.clone();
        if predicate(&spans) {
            return spans;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    panic!(
        "timed out waiting for expected exported spans:\n{}",
        format_spans(&last_spans)
    );
}

async fn wait_for_new_exported_spans<F>(
    tracing: &TestTracing,
    baseline_len: usize,
    predicate: F,
) -> Vec<SpanData>
where
    F: Fn(&[SpanData]) -> bool,
{
    let spans = wait_for_exported_spans(tracing, |spans| {
        spans.len() > baseline_len && predicate(&spans[baseline_len..])
    })
    .await;
    spans.into_iter().skip(baseline_len).collect()
}

fn controller_initialize_request(request_id: i64) -> ClientRequest {
    ClientRequest::Initialize {
        request_id: RequestId::Integer(request_id),
        params: InitializeParams {
            client_info: ClientInfo {
                name: "codex-waveshare".to_string(),
                title: Some("Codex Waveshare".to_string()),
                version: "0.1.0".to_string(),
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: true,
                ..Default::default()
            }),
        },
    }
}

fn controller_participation_request(request_id: i64) -> ClientRequest {
    ClientRequest::ControllerRequestParticipation {
        request_id: RequestId::Integer(request_id),
        params: ControllerRequestParticipationParams {
            controller_name: "codex-waveshare".to_string(),
            description: "external input device".to_string(),
        },
    }
}

fn controller_no_params_request(request_id: i64, method: &str) -> ClientRequest {
    ClientRequest::try_from(JSONRPCRequest {
        id: RequestId::Integer(request_id),
        method: method.to_string(),
        params: None,
        trace: None,
    })
    .expect("controller request should parse")
}

fn controller_thread_list_request(request_id: i64) -> ClientRequest {
    ClientRequest::ThreadList {
        request_id: RequestId::Integer(request_id),
        params: ThreadListParams {
            cursor: None,
            limit: Some(100),
            sort_key: None,
            sort_direction: None,
            model_providers: None,
            source_kinds: None,
            archived: None,
            section_id: None,
            cwd: None,
            use_state_db_only: false,
            search_term: None,
            parent_thread_id: None,
            ancestor_thread_id: None,
        },
    }
}

fn controller_thread_read_request(request_id: i64, thread_id: impl Into<String>) -> ClientRequest {
    ClientRequest::ThreadRead {
        request_id: RequestId::Integer(request_id),
        params: ThreadReadParams {
            thread_id: thread_id.into(),
            include_turns: false,
        },
    }
}

fn thread_set_name_request(
    request_id: i64,
    thread_id: impl Into<String>,
    name: impl Into<String>,
) -> ClientRequest {
    ClientRequest::ThreadSetName {
        request_id: RequestId::Integer(request_id),
        params: ThreadSetNameParams {
            thread_id: thread_id.into(),
            name: name.into(),
        },
    }
}

fn controller_turn_start_request(request_id: i64, thread_id: impl Into<String>) -> ClientRequest {
    ClientRequest::TurnStart {
        request_id: RequestId::Integer(request_id),
        params: TurnStartParams {
            environments: None,
            thread_id: thread_id.into(),
            client_user_message_id: None,
            input: vec![UserInput::Text {
                text: "controller input".to_string(),
                text_elements: Vec::new(),
            }],
            responsesapi_client_metadata: None,
            additional_context: None,
            cwd: None,
            runtime_workspace_roots: None,
            approval_policy: None,
            sandbox_policy: None,
            permissions: None,
            approvals_reviewer: None,
            model: None,
            service_tier: None,
            effort: None,
            summary: None,
            personality: None,
            output_schema: None,
            collaboration_mode: None,
            multi_agent_mode: None,
        },
    }
}

fn controller_turn_start_with_model_request(
    request_id: i64,
    thread_id: impl Into<String>,
) -> ClientRequest {
    let mut request = controller_turn_start_request(request_id, thread_id);
    let ClientRequest::TurnStart { params, .. } = &mut request else {
        panic!("expected turn/start request");
    };
    params.model = Some("gpt-test".to_string());
    request
}

fn controller_thread_resume_with_history_request(
    request_id: i64,
    thread_id: impl Into<String>,
) -> ClientRequest {
    ClientRequest::ThreadResume {
        request_id: RequestId::Integer(request_id),
        params: ThreadResumeParams {
            history: Some(Vec::new()),
            ..controller_thread_resume_params(thread_id)
        },
    }
}

fn controller_thread_resume_params(thread_id: impl Into<String>) -> ThreadResumeParams {
    ThreadResumeParams {
        thread_id: thread_id.into(),
        history: None,
        path: None,
        model: None,
        model_provider: None,
        service_tier: None,
        cwd: None,
        runtime_workspace_roots: None,
        approval_policy: None,
        approvals_reviewer: None,
        sandbox: None,
        permissions: None,
        config: None,
        base_instructions: None,
        developer_instructions: None,
        personality: None,
        exclude_turns: false,
        initial_turns_page: None,
    }
}

fn command_execution_approval_payload(thread_id: impl Into<String>) -> ServerRequestPayload {
    ServerRequestPayload::CommandExecutionRequestApproval(CommandExecutionRequestApprovalParams {
        thread_id: thread_id.into(),
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

fn current_time_read_payload(thread_id: impl Into<String>) -> ServerRequestPayload {
    ServerRequestPayload::CurrentTimeRead(CurrentTimeReadParams {
        thread_id: thread_id.into(),
    })
}

fn controller_proof(connection_id: ConnectionId) -> ControllerCredentialProof {
    ControllerCredentialProof {
        subject_id: "controller-subject".to_string(),
        credential_fingerprint: "credential-fingerprint".to_string(),
        connection_id,
    }
}

fn controller_record(main_thread_id: ThreadId) -> ControllerEnrollmentRecord {
    controller_record_with_duration(main_thread_id, Duration::from_secs(60))
}

fn controller_record_with_duration(
    main_thread_id: ThreadId,
    authorization_duration: Duration,
) -> ControllerEnrollmentRecord {
    ControllerEnrollmentRecord {
        subject_id: "controller-subject".to_string(),
        credential_fingerprint: "credential-fingerprint".to_string(),
        main_thread_id,
        authorization_epoch: 7,
        revocation_epoch: 6,
        expires_at: std::time::Instant::now() + authorization_duration,
    }
}

#[test]
#[serial(app_server_tracing)]
fn external_controller_origin_is_denied_before_initialized_dispatch() -> Result<()> {
    run_current_thread_test_with_stack(
        "external_controller_origin_is_denied_before_initialized_dispatch",
        async {
            let mut harness = TracingHarness::new().await?;
            let error = harness
                .raw_request_error_with_origin(
                    JSONRPCRequest {
                        id: RequestId::Integer(30_001),
                        method: "thread/list".to_string(),
                        params: Some(serde_json::json!({})),
                        trace: None,
                    },
                    ConnectionOrigin::ExternalController,
                )
                .await;

            assert_eq!(
                error.error.code,
                crate::error_code::INVALID_REQUEST_ERROR_CODE
            );
            assert_eq!(
                error.error.message,
                "controller main thread is not available yet"
            );
            let data: ControllerErrorData = serde_json::from_value(
                error
                    .error
                    .data
                    .expect("controller error should include data"),
            )?;
            assert_eq!(data.code, ControllerErrorCode::MainThreadUnavailable);
            assert_eq!(data.retry, ControllerRetryDisposition::SameConnection);
            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn saturated_external_controller_ingress_returns_typed_overload() -> Result<()> {
    run_current_thread_test_with_stack(
        "saturated_external_controller_ingress_returns_typed_overload",
        async {
            let mut harness = TracingHarness::new().await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 30_101),
                )
                .await;
            let mut reservations = Vec::new();
            for _ in 0..EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY {
                reservations.push(
                    external_session
                        .rpc_gate
                        .try_reserve_external_controller_request()
                        .expect("external controller ingress permit should be available"),
                );
            }

            let error = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_thread_list_request(/*request_id*/ 30_102),
                )
                .await;

            assert_eq!(error.error.code, crate::error_code::OVERLOADED_ERROR_CODE);
            assert_eq!(
                error.error.message,
                "external controller ingress is overloaded; retry later"
            );
            let data: ControllerErrorData = serde_json::from_value(
                error
                    .error
                    .data
                    .expect("controller overload should include typed data"),
            )?;
            assert_eq!(data.code, ControllerErrorCode::ControllerOverloaded);
            assert_eq!(data.retry, ControllerRetryDisposition::SameConnection);
            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn saturated_external_controller_control_ingress_returns_typed_overload() -> Result<()> {
    run_current_thread_test_with_stack(
        "saturated_external_controller_control_ingress_returns_typed_overload",
        async {
            let mut harness = TracingHarness::new().await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 30_151),
                )
                .await;
            let mut reservations = Vec::new();
            for _ in 0..EXTERNAL_CONTROLLER_CONTROL_RPC_QUEUE_CAPACITY {
                reservations.push(
                    external_session
                        .rpc_gate
                        .try_reserve_external_controller_control_request()
                        .expect("external controller control ingress permit should be available"),
                );
            }

            let error = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 30_152),
                )
                .await;

            assert_eq!(error.error.code, crate::error_code::OVERLOADED_ERROR_CODE);
            assert_eq!(
                error.error.message,
                "external controller ingress is overloaded; retry later"
            );
            let data: ControllerErrorData = serde_json::from_value(
                error
                    .error
                    .data
                    .expect("controller overload should include typed data"),
            )?;
            assert_eq!(data.code, ControllerErrorCode::ControllerOverloaded);
            assert_eq!(data.retry, ControllerRetryDisposition::SameConnection);
            assert_eq!(
                reservations.len(),
                EXTERNAL_CONTROLLER_CONTROL_RPC_QUEUE_CAPACITY
            );
            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn saturated_external_controller_normal_ingress_allows_control_plane() -> Result<()> {
    run_current_thread_test_with_stack(
        "saturated_external_controller_normal_ingress_allows_control_plane",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 30_201),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));
            let started = harness
                .start_thread(/*request_id*/ 30_202, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 30_203),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .expect("approved session")
                    .active_lease
                    .is_some()
            );

            let mut normal_reservations = Vec::new();
            for _ in 0..EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY {
                normal_reservations.push(
                    external_session
                        .rpc_gate
                        .try_reserve_external_controller_request()
                        .expect("external controller normal ingress permit should be available"),
                );
            }

            let error = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_thread_list_request(/*request_id*/ 30_204),
                )
                .await;
            let data: ControllerErrorData = serde_json::from_value(
                error
                    .error
                    .data
                    .expect("controller overload should include typed data"),
            )?;
            assert_eq!(data.code, ControllerErrorCode::ControllerOverloaded);

            let released: ControllerReleaseControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 30_205,
                        "controller/releaseControl",
                    ),
                )
                .await;
            assert_eq!(released.session.active_lease, None);

            let reacquired: ControllerAcquireControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 30_206,
                        "controller/acquireControl",
                    ),
                )
                .await;
            assert!(reacquired.session.active_lease.is_some());

            let _: ControllerSignOffResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(/*request_id*/ 30_207, "controller/signOff"),
                )
                .await;

            assert_eq!(
                normal_reservations.len(),
                EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY
            );
            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn active_controller_archive_delete_reject_spawned_descendant_targets() -> Result<()> {
    run_current_thread_test_with_stack(
        "active_controller_archive_delete_reject_spawned_descendant_targets",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_inner(enrollment_source.clone(), /*use_state_db*/ true).await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 39_001),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

            let started = harness
                .start_thread(/*request_id*/ 39_002, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            let child_thread_id = ThreadId::from_string("00000000-0000-7000-8000-000000000222")?;
            harness
                .state_db
                .as_ref()
                .expect("state db should be enabled for this harness")
                .upsert_thread_spawn_edge(
                    main_thread_id,
                    child_thread_id,
                    DirectionalThreadSpawnEdgeStatus::Open,
                )
                .await?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 39_003),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .as_ref()
                    .and_then(|session| session.active_lease.as_ref())
                    .is_some()
            );

            let archive_error = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    ClientRequest::ThreadArchive {
                        request_id: RequestId::Integer(39_004),
                        params: ThreadArchiveParams {
                            thread_id: started.thread.id.clone(),
                        },
                    },
                )
                .await;
            assert_eq!(
                archive_error.error.message,
                "external controller thread/archive may not target spawned descendant threads"
            );
            let archive_data: ControllerErrorData =
                serde_json::from_value(archive_error.error.data.expect("typed controller error"))?;
            assert_eq!(archive_data.code, ControllerErrorCode::ControllerNotAllowed);

            let delete_error = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    ClientRequest::ThreadDelete {
                        request_id: RequestId::Integer(39_005),
                        params: ThreadDeleteParams {
                            thread_id: started.thread.id.clone(),
                        },
                    },
                )
                .await;
            assert_eq!(
                delete_error.error.message,
                "external controller thread/delete may not target spawned descendant threads"
            );
            let delete_data: ControllerErrorData =
                serde_json::from_value(delete_error.error.data.expect("typed controller error"))?;
            assert_eq!(delete_data.code, ControllerErrorCode::ControllerNotAllowed);

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_main_thread_close_marks_launch_closed() -> Result<()> {
    run_current_thread_test_with_stack("controller_main_thread_close_marks_launch_closed", async {
        let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
        let mut harness =
            TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                .await?;
        let external_session = Arc::new(ConnectionSessionState::new());
        let _: InitializeResponse = harness
            .request_for_connection(
                EXTERNAL_CONNECTION_ID,
                ConnectionOrigin::ExternalController,
                Arc::clone(&external_session),
                controller_initialize_request(/*request_id*/ 39_101),
            )
            .await;
        external_session.bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

        let started: ThreadStartResponse = harness
            .request(
                ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(39_102),
                    params: ThreadStartParams {
                        ephemeral: Some(false),
                        ..ThreadStartParams::default()
                    },
                },
                /*trace*/ None,
            )
            .await;
        read_thread_started_notification(&mut harness.outgoing_rx).await;
        let main_thread_id = ThreadId::from_string(&started.thread.id)?;
        enrollment_source.insert(controller_record(main_thread_id));

        let participation: ControllerRequestParticipationResponse = harness
            .request_for_connection(
                EXTERNAL_CONNECTION_ID,
                ConnectionOrigin::ExternalController,
                Arc::clone(&external_session),
                controller_participation_request(/*request_id*/ 39_103),
            )
            .await;
        assert!(
            participation
                .session
                .as_ref()
                .and_then(|session| session.active_lease.as_ref())
                .is_some()
        );

        let prompt_recipients = harness
            .processor
            .controller_processor
            .prompt_request_recipients(
                main_thread_id,
                vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
            );
        let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
            Arc::clone(&harness.processor.outgoing),
            prompt_recipients,
            vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
            main_thread_id,
        );
        let (prompt_request_id, wait_for_prompt) = thread_outgoing
            .send_request(command_execution_approval_payload(
                started.thread.id.clone(),
            ))
            .await;
        let _ = read_server_request_for_connection(
            &mut harness.outgoing_rx,
            EXTERNAL_CONNECTION_ID,
            &prompt_request_id,
        )
        .await;

        harness
            .processor
            .controller_processor
            .mark_main_thread_closed(main_thread_id)
            .await;

        let ownership_notification = read_controller_ownership_changed_for_connection(
            &mut harness.outgoing_rx,
            EXTERNAL_CONNECTION_ID,
        )
        .await;
        assert_eq!(
            ownership_notification.reason,
            ControllerControlOwnershipChangedReason::MainThreadClosed
        );
        assert_eq!(ownership_notification.main_thread_id, started.thread.id);
        assert_eq!(ownership_notification.active_lease, None);

        let authorization_notification = read_controller_authorization_changed_for_connection(
            &mut harness.outgoing_rx,
            EXTERNAL_CONNECTION_ID,
        )
        .await;
        assert_eq!(
            authorization_notification.reason,
            ControllerAuthorizationChangedReason::MainThreadClosed
        );
        assert_eq!(authorization_notification.main_thread_id, started.thread.id);
        assert_eq!(authorization_notification.session, None);

        let prompt_error = tokio::time::timeout(Duration::from_secs(1), wait_for_prompt)
            .await
            .expect("pending prompt should be cancelled when main thread closes")
            .expect("prompt waiter should receive terminal error")
            .expect_err("pending prompt should not resolve successfully");
        let prompt_error_data: ControllerErrorData =
            serde_json::from_value(prompt_error.data.expect("typed controller error"))?;
        assert_eq!(
            prompt_error_data.code,
            ControllerErrorCode::MainThreadClosed
        );

        let read_after_close = harness
            .request_error_for_connection(
                EXTERNAL_CONNECTION_ID,
                ConnectionOrigin::ExternalController,
                Arc::clone(&external_session),
                controller_thread_read_request(
                    /*request_id*/ 39_105,
                    started.thread.id.clone(),
                ),
            )
            .await;
        let read_after_close_data: ControllerErrorData =
            serde_json::from_value(read_after_close.error.data.expect("typed controller error"))?;
        assert_eq!(
            read_after_close_data.code,
            ControllerErrorCode::MainThreadClosed
        );
        assert_eq!(
            read_after_close_data.retry,
            ControllerRetryDisposition::DoNotRetry
        );

        harness.shutdown().await;
        Ok(())
    })
}

#[test]
#[serial(app_server_tracing)]
fn controller_control_notifications_are_emitted_for_session_transitions() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_control_notifications_are_emitted_for_session_transitions",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 40_101),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

            let started = harness
                .start_thread(/*request_id*/ 40_102, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation_request_id = harness
                .submit_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 40_103),
                )
                .await;
            let authorization_notification = read_controller_authorization_changed_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
            )
            .await;
            assert_eq!(
                authorization_notification.reason,
                ControllerAuthorizationChangedReason::Approved
            );
            assert_eq!(authorization_notification.main_thread_id, started.thread.id);
            assert_eq!(
                authorization_notification
                    .session
                    .as_ref()
                    .map(|session| session.active_lease.is_some()),
                Some(false)
            );

            let ownership_notification = read_controller_ownership_changed_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
            )
            .await;
            assert_eq!(
                ownership_notification.reason,
                ControllerControlOwnershipChangedReason::InitialLeaseGranted
            );
            assert_eq!(ownership_notification.main_thread_id, started.thread.id);
            let initial_lease = ownership_notification
                .active_lease
                .clone()
                .expect("initial ownership notification should include lease");

            let participation: ControllerRequestParticipationResponse =
                read_response_for_connection(
                    &mut harness.outgoing_rx,
                    EXTERNAL_CONNECTION_ID,
                    participation_request_id,
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert_eq!(
                participation
                    .session
                    .as_ref()
                    .and_then(|session| session.active_lease.clone()),
                Some(initial_lease)
            );

            let release_request_id = harness
                .submit_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_104,
                        "controller/releaseControl",
                    ),
                )
                .await;
            let release_notification = read_controller_ownership_changed_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
            )
            .await;
            assert_eq!(
                release_notification.reason,
                ControllerControlOwnershipChangedReason::Released
            );
            assert_eq!(release_notification.active_lease, None);
            let released: ControllerReleaseControlResponse = read_response_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                release_request_id,
            )
            .await;
            assert_eq!(released.session.active_lease, None);

            let acquire_request_id = harness
                .submit_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_105,
                        "controller/acquireControl",
                    ),
                )
                .await;
            let acquired_notification = read_controller_ownership_changed_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
            )
            .await;
            assert_eq!(
                acquired_notification.reason,
                ControllerControlOwnershipChangedReason::Acquired
            );
            assert_eq!(acquired_notification.main_thread_id, started.thread.id);
            let acquired_lease = acquired_notification
                .active_lease
                .clone()
                .expect("acquire notification should include lease");
            let acquired: ControllerAcquireControlResponse = read_response_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                acquire_request_id,
            )
            .await;
            assert_eq!(acquired.session.active_lease, Some(acquired_lease));

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_control_plane_round_trips_after_enrollment() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_control_plane_round_trips_after_enrollment",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 40_001),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

            let unavailable = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 40_002),
                )
                .await;
            let unavailable_data: ControllerErrorData =
                serde_json::from_value(unavailable.error.data.expect("typed controller error"))?;
            assert_eq!(
                unavailable_data.code,
                ControllerErrorCode::MainThreadUnavailable
            );
            assert_eq!(
                unavailable_data.retry,
                ControllerRetryDisposition::SameConnection
            );

            let started = harness
                .start_thread(/*request_id*/ 40_003, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 40_004),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            let approved_session = participation.session.expect("approved session");
            assert_eq!(approved_session.main_thread_id, started.thread.id);
            let approved_lease = approved_session
                .active_lease
                .clone()
                .expect("approved session should include an active lease");

            let already_acquired: ControllerAcquireControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_019,
                        "controller/acquireControl",
                    ),
                )
                .await;
            assert_eq!(already_acquired.session.active_lease, Some(approved_lease));
            assert!(
                already_acquired
                    .session
                    .effective_capabilities
                    .mutate_main_thread
            );

            let released: ControllerReleaseControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_005,
                        "controller/releaseControl",
                    ),
                )
                .await;
            assert_eq!(released.session.active_lease, None);
            assert!(released.session.effective_capabilities.read_main_thread);
            assert!(!released.session.effective_capabilities.mutate_main_thread);

            let listed: ThreadListResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_thread_list_request(/*request_id*/ 40_006),
                )
                .await;
            assert_eq!(
                listed
                    .data
                    .iter()
                    .map(|thread| thread.id.clone())
                    .collect::<Vec<_>>(),
                vec![started.thread.id.clone()]
            );
            assert_eq!(listed.next_cursor, None);
            assert_eq!(listed.backwards_cursor, None);

            let wrong_thread = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_thread_read_request(
                        /*request_id*/ 40_007,
                        "00000000-0000-7000-8000-000000000000",
                    ),
                )
                .await;
            let wrong_thread_data: ControllerErrorData =
                serde_json::from_value(wrong_thread.error.data.expect("typed controller error"))?;
            assert_eq!(
                wrong_thread_data.code,
                ControllerErrorCode::DifferentThreadTarget
            );

            let mutation_without_lease = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_turn_start_request(
                        /*request_id*/ 40_008,
                        started.thread.id.clone(),
                    ),
                )
                .await;
            let mutation_without_lease_data: ControllerErrorData = serde_json::from_value(
                mutation_without_lease
                    .error
                    .data
                    .expect("typed controller error"),
            )?;
            assert_eq!(
                mutation_without_lease_data.code,
                ControllerErrorCode::StaleOwnership
            );

            let reacquired: ControllerAcquireControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_009,
                        "controller/acquireControl",
                    ),
                )
                .await;
            assert!(reacquired.session.active_lease.is_some());
            assert!(reacquired.session.effective_capabilities.mutate_main_thread);

            let unsafe_resume = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_thread_resume_with_history_request(
                        /*request_id*/ 40_050,
                        started.thread.id.clone(),
                    ),
                )
                .await;
            let unsafe_resume_data: ControllerErrorData =
                serde_json::from_value(unsafe_resume.error.data.expect("typed controller error"))?;
            assert_eq!(
                unsafe_resume_data.code,
                ControllerErrorCode::ControllerNotAllowed
            );

            let unsafe_turn_start = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_turn_start_with_model_request(
                        /*request_id*/ 40_051,
                        started.thread.id.clone(),
                    ),
                )
                .await;
            let unsafe_turn_start_data: ControllerErrorData = serde_json::from_value(
                unsafe_turn_start
                    .error
                    .data
                    .expect("typed controller error"),
            )?;
            assert_eq!(
                unsafe_turn_start_data.code,
                ControllerErrorCode::ControllerNotAllowed
            );

            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            harness
                .processor
                .thread_processor
                .subscribe_test_connection_for_thread(main_thread_id, EXTERNAL_CONNECTION_ID)
                .await;
            assert!(
                harness
                    .processor
                    .thread_processor
                    .subscribed_connection_ids_for_thread(main_thread_id)
                    .await
                    .contains(&EXTERNAL_CONNECTION_ID)
            );

            let (approval_request_id, wait_for_approval) = harness
                .processor
                .outgoing
                .send_request_to_connections(
                    Some(&[EXTERNAL_CONNECTION_ID]),
                    command_execution_approval_payload(started.thread.id.clone()),
                    Some(main_thread_id),
                )
                .await;
            let _ = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &approval_request_id,
            )
            .await;
            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: approval_request_id,
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            let approval_result = tokio::time::timeout(Duration::from_secs(1), wait_for_approval)
                .await
                .expect("approval response should not time out")
                .expect("approval waiter should receive response")
                .expect("controller accept should resolve successfully");
            assert_eq!(approval_result, serde_json::json!({ "decision": "accept" }));

            let (session_scoped_request_id, mut wait_for_session_scoped) = harness
                .processor
                .outgoing
                .send_request_to_connections(
                    Some(&[EXTERNAL_CONNECTION_ID]),
                    command_execution_approval_payload(started.thread.id.clone()),
                    Some(main_thread_id),
                )
                .await;
            let _ = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &session_scoped_request_id,
            )
            .await;
            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: session_scoped_request_id.clone(),
                        result: serde_json::json!({ "decision": "acceptForSession" }),
                    },
                )
                .await;
            let session_scoped_error = match &session_scoped_request_id {
                RequestId::Integer(request_id) => {
                    read_error_for_connection(
                        &mut harness.outgoing_rx,
                        EXTERNAL_CONNECTION_ID,
                        *request_id,
                    )
                    .await
                }
                request_id => panic!("expected integer server request id, got {request_id:?}"),
            };
            assert_eq!(session_scoped_error.id, session_scoped_request_id);
            let session_scoped_error_data: ControllerErrorData = serde_json::from_value(
                session_scoped_error
                    .error
                    .data
                    .expect("typed controller error"),
            )?;
            assert_eq!(
                session_scoped_error_data.code,
                ControllerErrorCode::ControllerNotAllowed
            );
            assert_eq!(
                session_scoped_error_data.retry,
                ControllerRetryDisposition::DoNotRetry
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait_for_session_scoped)
                    .await
                    .is_err()
            );
            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: session_scoped_request_id,
                        result: serde_json::json!({ "decision": "cancel" }),
                    },
                )
                .await;
            let session_scoped_cleanup_result =
                tokio::time::timeout(Duration::from_secs(1), wait_for_session_scoped)
                    .await
                    .expect("cleanup response should not time out")
                    .expect("approval waiter should receive cleanup response")
                    .expect("controller cancel should resolve successfully");
            assert_eq!(
                session_scoped_cleanup_result,
                serde_json::json!({ "decision": "cancel" })
            );

            let (release_prompt_request_id, mut wait_for_release_prompt) = harness
                .processor
                .outgoing
                .send_request_to_connections(
                    Some(&[EXTERNAL_CONNECTION_ID]),
                    command_execution_approval_payload(started.thread.id.clone()),
                    Some(main_thread_id),
                )
                .await;
            let _ = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &release_prompt_request_id,
            )
            .await;
            let release_control_request_id = harness
                .submit_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_010,
                        "controller/releaseControl",
                    ),
                )
                .await;
            let rebound_release_prompt = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                TEST_CONNECTION_ID,
                &release_prompt_request_id,
            )
            .await;
            assert_eq!(rebound_release_prompt.id(), &release_prompt_request_id);
            let released: ControllerReleaseControlResponse = read_response_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                release_control_request_id,
            )
            .await;
            assert_eq!(released.session.active_lease, None);
            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: release_prompt_request_id.clone(),
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait_for_release_prompt)
                    .await
                    .is_err()
            );
            harness
                .processor
                .process_response(
                    TEST_CONNECTION_ID,
                    ConnectionOrigin::Stdio,
                    JSONRPCResponse {
                        id: release_prompt_request_id,
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            let release_prompt_result =
                tokio::time::timeout(Duration::from_secs(1), wait_for_release_prompt)
                    .await
                    .expect("release-rebound prompt response should not time out")
                    .expect("approval waiter should receive release-rebound response")
                    .expect("TUI accept should resolve release-rebound prompt");
            assert_eq!(
                release_prompt_result,
                serde_json::json!({ "decision": "accept" })
            );

            let (acquire_prompt_request_id, mut wait_for_acquire_prompt) = harness
                .processor
                .outgoing
                .send_request_to_connections(
                    Some(&[TEST_CONNECTION_ID]),
                    command_execution_approval_payload(started.thread.id.clone()),
                    Some(main_thread_id),
                )
                .await;
            let _ = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                TEST_CONNECTION_ID,
                &acquire_prompt_request_id,
            )
            .await;
            let acquire_control_request_id = harness
                .submit_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_011,
                        "controller/acquireControl",
                    ),
                )
                .await;
            let rebound_acquire_prompt = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &acquire_prompt_request_id,
            )
            .await;
            assert_eq!(rebound_acquire_prompt.id(), &acquire_prompt_request_id);
            let reacquired_after_prompt_rebind: ControllerAcquireControlResponse =
                read_response_for_connection(
                    &mut harness.outgoing_rx,
                    EXTERNAL_CONNECTION_ID,
                    acquire_control_request_id,
                )
                .await;
            assert!(
                reacquired_after_prompt_rebind
                    .session
                    .effective_capabilities
                    .mutate_main_thread
            );
            harness
                .processor
                .process_response(
                    TEST_CONNECTION_ID,
                    ConnectionOrigin::Stdio,
                    JSONRPCResponse {
                        id: acquire_prompt_request_id.clone(),
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait_for_acquire_prompt)
                    .await
                    .is_err()
            );
            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: acquire_prompt_request_id,
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            let acquire_prompt_result =
                tokio::time::timeout(Duration::from_secs(1), wait_for_acquire_prompt)
                    .await
                    .expect("acquire-rebound prompt response should not time out")
                    .expect("approval waiter should receive acquire-rebound response")
                    .expect("controller accept should resolve acquire-rebound prompt");
            assert_eq!(
                acquire_prompt_result,
                serde_json::json!({ "decision": "accept" })
            );

            let primary_session = Arc::clone(&harness.session);
            let primary_turn_request_id = harness
                .submit_for_connection(
                    TEST_CONNECTION_ID,
                    ConnectionOrigin::Stdio,
                    primary_session,
                    controller_turn_start_request(
                        /*request_id*/ 40_012,
                        started.thread.id.clone(),
                    ),
                )
                .await;
            let stale_controller_turn_request_id = harness
                .submit_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_turn_start_request(
                        /*request_id*/ 40_013,
                        started.thread.id.clone(),
                    ),
                )
                .await;

            let _: TurnStartResponse = read_response_for_connection(
                &mut harness.outgoing_rx,
                TEST_CONNECTION_ID,
                primary_turn_request_id,
            )
            .await;
            let stale_controller_turn = read_error_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                stale_controller_turn_request_id,
            )
            .await;
            let stale_controller_turn_data: ControllerErrorData = serde_json::from_value(
                stale_controller_turn
                    .error
                    .data
                    .expect("typed controller error"),
            )?;
            assert_eq!(
                stale_controller_turn_data.code,
                ControllerErrorCode::StaleOwnership
            );

            let second_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    SECOND_EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&second_session),
                    controller_initialize_request(/*request_id*/ 40_014),
                )
                .await;
            second_session
                .bind_controller_credential_proof(controller_proof(SECOND_EXTERNAL_CONNECTION_ID));
            let second_participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    SECOND_EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&second_session),
                    controller_participation_request(/*request_id*/ 40_015),
                )
                .await;
            assert_eq!(
                second_participation.status,
                ControllerParticipationStatus::Approved
            );
            let second_approved_session = second_participation
                .session
                .expect("second approved session");
            assert!(second_approved_session.active_lease.is_some());
            assert!(
                second_approved_session
                    .effective_capabilities
                    .mutate_main_thread
            );

            let (disconnect_prompt_request_id, mut wait_for_disconnect_prompt) = harness
                .processor
                .outgoing
                .send_request_to_connections(
                    Some(&[SECOND_EXTERNAL_CONNECTION_ID]),
                    command_execution_approval_payload(started.thread.id.clone()),
                    Some(main_thread_id),
                )
                .await;
            let _ = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                SECOND_EXTERNAL_CONNECTION_ID,
                &disconnect_prompt_request_id,
            )
            .await;
            harness
                .processor
                .connection_closed(SECOND_EXTERNAL_CONNECTION_ID, &second_session)
                .await;
            let rebound_disconnect_prompt = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                TEST_CONNECTION_ID,
                &disconnect_prompt_request_id,
            )
            .await;
            assert_eq!(
                rebound_disconnect_prompt.id(),
                &disconnect_prompt_request_id
            );
            harness
                .processor
                .process_response(
                    SECOND_EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: disconnect_prompt_request_id.clone(),
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait_for_disconnect_prompt)
                    .await
                    .is_err()
            );
            harness
                .processor
                .process_response(
                    TEST_CONNECTION_ID,
                    ConnectionOrigin::Stdio,
                    JSONRPCResponse {
                        id: disconnect_prompt_request_id,
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            let disconnect_prompt_result =
                tokio::time::timeout(Duration::from_secs(1), wait_for_disconnect_prompt)
                    .await
                    .expect("disconnect-rebound prompt response should not time out")
                    .expect("approval waiter should receive disconnect-rebound response")
                    .expect("TUI accept should resolve disconnect-rebound prompt");
            assert_eq!(
                disconnect_prompt_result,
                serde_json::json!({ "decision": "accept" })
            );

            let signoff_reacquired: ControllerAcquireControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_016,
                        "controller/acquireControl",
                    ),
                )
                .await;
            assert!(
                signoff_reacquired
                    .session
                    .effective_capabilities
                    .mutate_main_thread
            );
            let (signoff_prompt_request_id, mut wait_for_signoff_prompt) = harness
                .processor
                .outgoing
                .send_request_to_connections(
                    Some(&[EXTERNAL_CONNECTION_ID]),
                    command_execution_approval_payload(started.thread.id.clone()),
                    Some(main_thread_id),
                )
                .await;
            let _ = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &signoff_prompt_request_id,
            )
            .await;
            let signoff_request_id = harness
                .submit_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(/*request_id*/ 40_017, "controller/signOff"),
                )
                .await;
            let rebound_signoff_prompt = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                TEST_CONNECTION_ID,
                &signoff_prompt_request_id,
            )
            .await;
            assert_eq!(rebound_signoff_prompt.id(), &signoff_prompt_request_id);
            let _: ControllerSignOffResponse = read_response_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                signoff_request_id,
            )
            .await;
            assert!(
                !harness
                    .processor
                    .thread_processor
                    .subscribed_connection_ids_for_thread(main_thread_id)
                    .await
                    .contains(&EXTERNAL_CONNECTION_ID)
            );
            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: signoff_prompt_request_id.clone(),
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait_for_signoff_prompt)
                    .await
                    .is_err()
            );
            harness
                .processor
                .process_response(
                    TEST_CONNECTION_ID,
                    ConnectionOrigin::Stdio,
                    JSONRPCResponse {
                        id: signoff_prompt_request_id,
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            let signoff_prompt_result =
                tokio::time::timeout(Duration::from_secs(1), wait_for_signoff_prompt)
                    .await
                    .expect("signoff-rebound prompt response should not time out")
                    .expect("approval waiter should receive signoff-rebound response")
                    .expect("TUI accept should resolve signoff-rebound prompt");
            assert_eq!(
                signoff_prompt_result,
                serde_json::json!({ "decision": "accept" })
            );

            let after_signoff = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_018,
                        "controller/releaseControl",
                    ),
                )
                .await;
            let after_signoff_data: ControllerErrorData =
                serde_json::from_value(after_signoff.error.data.expect("typed controller error"))?;
            assert_eq!(
                after_signoff_data.code,
                ControllerErrorCode::TransportClosing
            );

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_rejects_session_scoped_permission_approval() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_rejects_session_scoped_permission_approval",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 40_151),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

            let started = harness
                .start_thread(/*request_id*/ 40_152, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 40_153),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .expect("approved session")
                    .active_lease
                    .is_some()
            );

            let permissions_cwd = TempDir::new()?;
            let prompt_recipients = harness
                .processor
                .controller_processor
                .prompt_request_recipients(
                    main_thread_id,
                    vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                );
            let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
                Arc::clone(&harness.processor.outgoing),
                prompt_recipients,
                vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                main_thread_id,
            );
            let (permissions_request_id, mut wait_for_permissions) = thread_outgoing
                .send_request(ServerRequestPayload::PermissionsRequestApproval(
                    PermissionsRequestApprovalParams {
                        thread_id: started.thread.id.clone(),
                        turn_id: "turn-1".to_string(),
                        item_id: "item-permissions".to_string(),
                        environment_id: None,
                        started_at_ms: 0,
                        cwd: permissions_cwd.path().abs(),
                        reason: Some("Need additional access".to_string()),
                        permissions: RequestPermissionProfile {
                            network: None,
                            file_system: None,
                        },
                    },
                ))
                .await;
            let delivered_request = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &permissions_request_id,
            )
            .await;
            assert!(matches!(
                delivered_request,
                ServerRequest::PermissionsRequestApproval { .. }
            ));

            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: permissions_request_id.clone(),
                        result: serde_json::to_value(PermissionsRequestApprovalResponse {
                            permissions: GrantedPermissionProfile::default(),
                            scope: PermissionGrantScope::Session,
                            strict_auto_review: None,
                        })?,
                    },
                )
                .await;
            let session_scope_error = match &permissions_request_id {
                RequestId::Integer(request_id) => {
                    read_error_for_connection(
                        &mut harness.outgoing_rx,
                        EXTERNAL_CONNECTION_ID,
                        *request_id,
                    )
                    .await
                }
                request_id => panic!("expected integer server request id, got {request_id:?}"),
            };
            assert_eq!(session_scope_error.id, permissions_request_id);
            let session_scope_error_data: ControllerErrorData = serde_json::from_value(
                session_scope_error
                    .error
                    .data
                    .expect("typed controller error"),
            )?;
            assert_eq!(
                session_scope_error_data.code,
                ControllerErrorCode::ControllerNotAllowed
            );
            assert_eq!(
                session_scope_error_data.retry,
                ControllerRetryDisposition::DoNotRetry
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait_for_permissions)
                    .await
                    .is_err()
            );

            let turn_scoped_response = serde_json::to_value(PermissionsRequestApprovalResponse {
                permissions: GrantedPermissionProfile::default(),
                scope: PermissionGrantScope::Turn,
                strict_auto_review: None,
            })?;
            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: permissions_request_id,
                        result: turn_scoped_response.clone(),
                    },
                )
                .await;
            let permissions_result =
                tokio::time::timeout(Duration::from_secs(1), wait_for_permissions)
                    .await
                    .expect("turn-scoped permissions response should not time out")
                    .expect("permissions waiter should receive response")
                    .expect("turn-scoped permissions approval should resolve successfully");
            assert_eq!(permissions_result, turn_scoped_response);

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_rejects_persistent_command_approval_decisions() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_rejects_persistent_command_approval_decisions",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 40_161),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

            let started = harness
                .start_thread(/*request_id*/ 40_162, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 40_163),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .expect("approved session")
                    .active_lease
                    .is_some()
            );

            let prompt_recipients = harness
                .processor
                .controller_processor
                .prompt_request_recipients(
                    main_thread_id,
                    vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                );
            let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
                Arc::clone(&harness.processor.outgoing),
                prompt_recipients,
                vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                main_thread_id,
            );
            for decision in [
                CommandExecutionApprovalDecision::AcceptWithExecpolicyAmendment {
                    execpolicy_amendment: ExecPolicyAmendment {
                        command: vec!["echo".to_string(), "hi".to_string()],
                    },
                },
                CommandExecutionApprovalDecision::ApplyNetworkPolicyAmendment {
                    network_policy_amendment: NetworkPolicyAmendment {
                        host: "example.com".to_string(),
                        action: NetworkPolicyRuleAction::Allow,
                    },
                },
            ] {
                let (command_request_id, mut wait_for_command) = thread_outgoing
                    .send_request(command_execution_approval_payload(
                        started.thread.id.clone(),
                    ))
                    .await;
                let delivered_request = read_server_request_for_connection(
                    &mut harness.outgoing_rx,
                    EXTERNAL_CONNECTION_ID,
                    &command_request_id,
                )
                .await;
                assert!(matches!(
                    delivered_request,
                    ServerRequest::CommandExecutionRequestApproval { .. }
                ));

                harness
                    .processor
                    .process_response(
                        EXTERNAL_CONNECTION_ID,
                        ConnectionOrigin::ExternalController,
                        JSONRPCResponse {
                            id: command_request_id.clone(),
                            result: serde_json::to_value(
                                CommandExecutionRequestApprovalResponse { decision },
                            )?,
                        },
                    )
                    .await;
                let persistent_approval_error = match &command_request_id {
                    RequestId::Integer(request_id) => {
                        read_error_for_connection(
                            &mut harness.outgoing_rx,
                            EXTERNAL_CONNECTION_ID,
                            *request_id,
                        )
                        .await
                    }
                    request_id => panic!("expected integer server request id, got {request_id:?}"),
                };
                assert_eq!(persistent_approval_error.id, command_request_id);
                let persistent_approval_error_data: ControllerErrorData = serde_json::from_value(
                    persistent_approval_error
                        .error
                        .data
                        .expect("typed controller error"),
                )?;
                assert_eq!(
                    persistent_approval_error_data.code,
                    ControllerErrorCode::ControllerNotAllowed
                );
                assert_eq!(
                    persistent_approval_error_data.retry,
                    ControllerRetryDisposition::DoNotRetry
                );
                assert!(
                    tokio::time::timeout(Duration::from_millis(10), &mut wait_for_command)
                        .await
                        .is_err()
                );

                let decline_response =
                    serde_json::to_value(CommandExecutionRequestApprovalResponse {
                        decision: CommandExecutionApprovalDecision::Decline,
                    })?;
                harness
                    .processor
                    .process_response(
                        EXTERNAL_CONNECTION_ID,
                        ConnectionOrigin::ExternalController,
                        JSONRPCResponse {
                            id: command_request_id,
                            result: decline_response.clone(),
                        },
                    )
                    .await;
                let command_result = tokio::time::timeout(Duration::from_secs(1), wait_for_command)
                    .await
                    .expect("command approval decline should not time out")
                    .expect("command approval waiter should receive response")
                    .expect("command approval decline should resolve successfully");
                assert_eq!(command_result, decline_response);
            }

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_rejects_session_scoped_file_change_approval() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_rejects_session_scoped_file_change_approval",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 40_171),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

            let started = harness
                .start_thread(/*request_id*/ 40_172, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 40_173),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .expect("approved session")
                    .active_lease
                    .is_some()
            );

            let prompt_recipients = harness
                .processor
                .controller_processor
                .prompt_request_recipients(
                    main_thread_id,
                    vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                );
            let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
                Arc::clone(&harness.processor.outgoing),
                prompt_recipients,
                vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                main_thread_id,
            );
            let (file_change_request_id, mut wait_for_file_change) = thread_outgoing
                .send_request(ServerRequestPayload::FileChangeRequestApproval(
                    FileChangeRequestApprovalParams {
                        thread_id: started.thread.id.clone(),
                        turn_id: "turn-1".to_string(),
                        item_id: "item-file-change".to_string(),
                        started_at_ms: 0,
                        reason: Some("Need write approval".to_string()),
                        grant_root: None,
                    },
                ))
                .await;
            let delivered_request = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &file_change_request_id,
            )
            .await;
            assert!(matches!(
                delivered_request,
                ServerRequest::FileChangeRequestApproval { .. }
            ));

            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: file_change_request_id.clone(),
                        result: serde_json::to_value(FileChangeRequestApprovalResponse {
                            decision: FileChangeApprovalDecision::AcceptForSession,
                        })?,
                    },
                )
                .await;
            let session_scope_error = match &file_change_request_id {
                RequestId::Integer(request_id) => {
                    read_error_for_connection(
                        &mut harness.outgoing_rx,
                        EXTERNAL_CONNECTION_ID,
                        *request_id,
                    )
                    .await
                }
                request_id => panic!("expected integer server request id, got {request_id:?}"),
            };
            assert_eq!(session_scope_error.id, file_change_request_id);
            let session_scope_error_data: ControllerErrorData = serde_json::from_value(
                session_scope_error
                    .error
                    .data
                    .expect("typed controller error"),
            )?;
            assert_eq!(
                session_scope_error_data.code,
                ControllerErrorCode::ControllerNotAllowed
            );
            assert_eq!(
                session_scope_error_data.retry,
                ControllerRetryDisposition::DoNotRetry
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait_for_file_change)
                    .await
                    .is_err()
            );

            let approval_response = serde_json::to_value(FileChangeRequestApprovalResponse {
                decision: FileChangeApprovalDecision::Accept,
            })?;
            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: file_change_request_id,
                        result: approval_response.clone(),
                    },
                )
                .await;
            let file_change_result =
                tokio::time::timeout(Duration::from_secs(1), wait_for_file_change)
                    .await
                    .expect("file-change approval response should not time out")
                    .expect("file-change waiter should receive response")
                    .expect("file-change approval should resolve successfully");
            assert_eq!(file_change_result, approval_response);

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn queued_primary_thread_input_reclaims_after_controller_reacquires() -> Result<()> {
    run_current_thread_test_with_stack(
        "queued_primary_thread_input_reclaims_after_controller_reacquires",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 40_201),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

            let started = harness
                .start_thread(/*request_id*/ 40_202, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 40_203),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .expect("approved session")
                    .active_lease
                    .is_some()
            );

            let released: ControllerReleaseControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_204,
                        "controller/releaseControl",
                    ),
                )
                .await;
            assert_eq!(released.session.active_lease, None);

            let (blocker_started_tx, blocker_started_rx) = oneshot::channel::<()>();
            let (blocker_release_tx, blocker_release_rx) = oneshot::channel::<()>();
            harness
                .processor
                .request_serialization_queues
                .enqueue_background(
                    RequestSerializationQueueKey::Thread {
                        thread_id: started.thread.id.clone(),
                    },
                    RequestSerializationAccess::Exclusive,
                    async move {
                        blocker_started_tx
                            .send(())
                            .expect("blocker start receiver should be open");
                        let _ = blocker_release_rx.await;
                    },
                )
                .await;
            tokio::time::timeout(Duration::from_secs(1), blocker_started_rx)
                .await
                .expect("thread queue blocker should start")
                .expect("blocker start sender should be open");

            let primary_session = Arc::clone(&harness.session);
            let _primary_request_id = harness
                .submit_for_connection(
                    TEST_CONNECTION_ID,
                    ConnectionOrigin::Stdio,
                    primary_session,
                    thread_set_name_request(
                        /*request_id*/ 40_205,
                        started.thread.id.clone(),
                        "queued primary input",
                    ),
                )
                .await;

            let reacquired: ControllerAcquireControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 40_206,
                        "controller/acquireControl",
                    ),
                )
                .await;
            assert!(reacquired.session.effective_capabilities.mutate_main_thread);

            blocker_release_tx
                .send(())
                .expect("thread queue blocker should still be waiting");

            let reclaimed = read_controller_ownership_changed_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
            )
            .await;
            assert_eq!(
                reclaimed.reason,
                ControllerControlOwnershipChangedReason::ReclaimedByTui
            );
            assert_eq!(reclaimed.active_lease, None);

            let stale_controller_mutation = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    thread_set_name_request(
                        /*request_id*/ 40_207,
                        started.thread.id,
                        "stale controller input",
                    ),
                )
                .await;
            let stale_controller_mutation_data: ControllerErrorData = serde_json::from_value(
                stale_controller_mutation
                    .error
                    .data
                    .expect("typed controller error"),
            )?;
            assert_eq!(
                stale_controller_mutation_data.code,
                ControllerErrorCode::StaleOwnership
            );

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn auto_attach_filters_external_controller_subscriptions_to_main_thread() -> Result<()> {
    run_current_thread_test_with_stack(
        "auto_attach_filters_external_controller_subscriptions_to_main_thread",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 40_301),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

            let main = harness
                .start_thread(/*request_id*/ 40_302, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&main.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));
            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 40_303),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );

            harness
                .processor
                .connection_initialized(TEST_CONNECTION_ID, harness.session.request_attestation())
                .await;
            harness
                .processor
                .connection_initialized(
                    EXTERNAL_CONNECTION_ID,
                    external_session.request_attestation(),
                )
                .await;

            harness
                .processor
                .try_attach_thread_listener_for_initialized_connections(
                    main_thread_id,
                    vec![(EXTERNAL_CONNECTION_ID, ConnectionOrigin::ExternalController)],
                )
                .await;
            assert!(
                harness
                    .processor
                    .thread_processor
                    .subscribed_connection_ids_for_thread(main_thread_id)
                    .await
                    .contains(&EXTERNAL_CONNECTION_ID)
            );

            let secondary = harness
                .start_thread(/*request_id*/ 40_304, /*trace*/ None)
                .await;
            let secondary_thread_id = ThreadId::from_string(&secondary.thread.id)?;
            harness
                .processor
                .try_attach_thread_listener_for_initialized_connections(
                    secondary_thread_id,
                    vec![
                        (TEST_CONNECTION_ID, ConnectionOrigin::Stdio),
                        (EXTERNAL_CONNECTION_ID, ConnectionOrigin::ExternalController),
                    ],
                )
                .await;
            let secondary_subscriptions = harness
                .processor
                .thread_processor
                .subscribed_connection_ids_for_thread(secondary_thread_id)
                .await;
            assert!(secondary_subscriptions.contains(&TEST_CONNECTION_ID));
            assert!(!secondary_subscriptions.contains(&EXTERNAL_CONNECTION_ID));

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn thread_scoped_mcp_oauth_completion_targets_external_controller_subscriber() -> Result<()> {
    run_current_thread_test_with_stack(
        "thread_scoped_mcp_oauth_completion_targets_external_controller_subscriber",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 40_401),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));

            let main = harness
                .start_thread(/*request_id*/ 40_402, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&main.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));
            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 40_403),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );

            harness
                .processor
                .connection_initialized(
                    EXTERNAL_CONNECTION_ID,
                    external_session.request_attestation(),
                )
                .await;
            harness
                .processor
                .try_attach_thread_listener_for_initialized_connections(
                    main_thread_id,
                    vec![(EXTERNAL_CONNECTION_ID, ConnectionOrigin::ExternalController)],
                )
                .await;

            let expected = McpServerOauthLoginCompletedNotification {
                name: "server-a".to_string(),
                thread_id: Some(main_thread_id.to_string()),
                success: true,
                error: None,
            };
            harness
                .processor
                .mcp_processor
                .send_oauth_login_completed_notification(expected.clone())
                .await;

            assert_eq!(
                read_mcp_oauth_completed_notification_for_connection(
                    &mut harness.outgoing_rx,
                    EXTERNAL_CONNECTION_ID,
                )
                .await,
                expected,
            );

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_participation_rejects_unproven_display_claims() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_participation_rejects_unproven_display_claims",
        async {
            let mut harness = TracingHarness::new().await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 41_001),
                )
                .await;
            let started = harness
                .start_thread(/*request_id*/ 41_002, /*trace*/ None)
                .await;
            let unapproved_signoff = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(/*request_id*/ 41_003, "controller/signOff"),
                )
                .await;
            let unapproved_signoff_data: ControllerErrorData = serde_json::from_value(
                unapproved_signoff
                    .error
                    .data
                    .expect("typed controller error"),
            )?;
            assert_eq!(
                unapproved_signoff_data.code,
                ControllerErrorCode::ParticipationRequired
            );

            let rejected: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    external_session,
                    controller_participation_request(/*request_id*/ 41_004),
                )
                .await;

            assert_eq!(rejected.status, ControllerParticipationStatus::Rejected);
            assert_eq!(rejected.session, None);
            let denial = rejected.denial.expect("rejection should include denial");
            assert_eq!(denial.data.code, ControllerErrorCode::EnrollmentDenied);
            assert_eq!(denial.data.main_thread_id, Some(started.thread.id));
            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_signoff_unsubscribes_before_terminal_notification() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_signoff_unsubscribes_before_terminal_notification",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 45_001),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));
            let second_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    SECOND_EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&second_session),
                    controller_initialize_request(/*request_id*/ 45_002),
                )
                .await;
            second_session
                .bind_controller_credential_proof(controller_proof(SECOND_EXTERNAL_CONNECTION_ID));

            let started = harness
                .start_thread(/*request_id*/ 45_003, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 45_004),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .expect("approved session")
                    .active_lease
                    .is_some()
            );
            let second_participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    SECOND_EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&second_session),
                    controller_participation_request(/*request_id*/ 45_005),
                )
                .await;
            assert_eq!(
                second_participation.status,
                ControllerParticipationStatus::Approved
            );
            assert_eq!(
                second_participation
                    .session
                    .expect("second approved session")
                    .active_lease,
                None
            );

            harness
                .processor
                .thread_processor
                .subscribe_test_connection_for_thread(main_thread_id, EXTERNAL_CONNECTION_ID)
                .await;
            assert!(
                harness
                    .processor
                    .thread_processor
                    .subscribed_connection_ids_for_thread(main_thread_id)
                    .await
                    .contains(&EXTERNAL_CONNECTION_ID)
            );

            fill_outgoing_queue(&harness.processor.outgoing).await;
            let processor = Arc::clone(&harness.processor);
            let session = Arc::clone(&external_session);
            let signoff_request = request_from_client_request(controller_no_params_request(
                /*request_id*/ 45_006,
                "controller/signOff",
            ));
            let signoff_task = tokio::spawn(async move {
                let transport = AppServerTransport::Stdio;
                processor
                    .process_request(
                        EXTERNAL_CONNECTION_ID,
                        ConnectionOrigin::ExternalController,
                        signoff_request,
                        &transport,
                        session,
                    )
                    .await;
            });

            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if !harness
                        .processor
                        .thread_processor
                        .subscribed_connection_ids_for_thread(main_thread_id)
                        .await
                        .contains(&EXTERNAL_CONNECTION_ID)
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("sign-off should unsubscribe before emitting terminal notification");

            drain_outgoing_envelopes(&mut harness.outgoing_rx, /*count*/ 2).await;
            tokio::time::timeout(Duration::from_secs(1), signoff_task)
                .await
                .expect("sign-off should finish after outgoing queue has capacity")
                .expect("sign-off task should not panic");
            drain_outgoing_envelopes(&mut harness.outgoing_rx, /*count*/ 16).await;

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_prompt_response_is_bound_to_owner_epoch() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_prompt_response_is_bound_to_owner_epoch",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 42_001),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));
            let started = harness
                .start_thread(/*request_id*/ 42_002, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 42_003),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .expect("approved session")
                    .active_lease
                    .is_some()
            );

            let prompt_recipients = harness
                .processor
                .controller_processor
                .prompt_request_recipients(
                    main_thread_id,
                    vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                );
            let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
                Arc::clone(&harness.processor.outgoing),
                prompt_recipients,
                vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                main_thread_id,
            );
            let (prompt_request_id, mut wait_for_prompt) = thread_outgoing
                .send_request(command_execution_approval_payload(
                    started.thread.id.clone(),
                ))
                .await;
            let _ = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &prompt_request_id,
            )
            .await;
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if harness
                        .processor
                        .outgoing
                        .request_has_external_delivery(&prompt_request_id, EXTERNAL_CONNECTION_ID)
                        .await
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("external delivery should be recorded");

            let released: ControllerReleaseControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 42_004,
                        "controller/releaseControl",
                    ),
                )
                .await;
            assert_eq!(released.session.active_lease, None);
            let reacquired: ControllerAcquireControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 42_005,
                        "controller/acquireControl",
                    ),
                )
                .await;
            assert!(reacquired.session.active_lease.is_some());

            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: prompt_request_id.clone(),
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait_for_prompt)
                    .await
                    .is_err()
            );
            assert!(
                harness
                    .processor
                    .outgoing
                    .cancel_request(&prompt_request_id)
                    .await
            );

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_disconnect_rebinds_prompts_before_rpc_drain() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_disconnect_rebinds_prompts_before_rpc_drain",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 44_001),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));
            let started = harness
                .start_thread(/*request_id*/ 44_002, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 44_003),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .expect("approved session")
                    .active_lease
                    .is_some()
            );

            let request_recipients = harness
                .processor
                .controller_processor
                .prompt_request_recipients(
                    main_thread_id,
                    vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                );
            assert_eq!(
                request_recipients.connection_ids(),
                &[EXTERNAL_CONNECTION_ID]
            );
            harness
                .processor
                .thread_processor
                .subscribe_test_connection_for_thread(main_thread_id, EXTERNAL_CONNECTION_ID)
                .await;
            assert!(
                harness
                    .processor
                    .thread_processor
                    .subscribed_connection_ids_for_thread(main_thread_id)
                    .await
                    .contains(&EXTERNAL_CONNECTION_ID)
            );
            let thread_outgoing = ThreadScopedOutgoingMessageSender::new_with_request_recipients(
                Arc::clone(&harness.processor.outgoing),
                request_recipients,
                vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
                main_thread_id,
            );
            let (prompt_request_id, mut wait_for_prompt) = thread_outgoing
                .send_request(command_execution_approval_payload(
                    started.thread.id.clone(),
                ))
                .await;

            let external_write_complete_tx = loop {
                let envelope = tokio::time::timeout(
                    Duration::from_secs(/*secs*/ 5),
                    harness.outgoing_rx.recv(),
                )
                .await
                .expect("timed out waiting for external controller prompt")
                .expect("outgoing channel closed");
                let crate::outgoing_message::OutgoingEnvelope::ToConnection {
                    connection_id,
                    message,
                    write_complete_tx,
                } = envelope
                else {
                    continue;
                };
                if connection_id != EXTERNAL_CONNECTION_ID {
                    continue;
                }
                let crate::outgoing_message::OutgoingMessage::Request(request) = message else {
                    continue;
                };
                if request.id() == &prompt_request_id {
                    break write_complete_tx
                        .expect("external controller prompt should track write completion");
                }
            };

            let (gate_entered_tx, gate_entered_rx) = oneshot::channel();
            let (gate_release_tx, gate_release_rx) = oneshot::channel();
            let gate = Arc::clone(&external_session.rpc_gate);
            let gate_task = tokio::spawn(async move {
                gate.run(async move {
                    gate_entered_tx.send(()).expect("receiver should be open");
                    let _ = gate_release_rx.await;
                })
                .await;
            });
            gate_entered_rx.await.expect("gate run should start");

            let processor = Arc::clone(&harness.processor);
            let session = Arc::clone(&external_session);
            let mut close_task = tokio::spawn(async move {
                processor
                    .connection_closed(EXTERNAL_CONNECTION_ID, &session)
                    .await;
            });
            tokio::time::timeout(Duration::from_millis(/*millis*/ 50), &mut close_task)
                .await
                .expect_err("connection close should still wait for the running RPC");
            assert!(
                !harness
                    .processor
                    .thread_processor
                    .subscribed_connection_ids_for_thread(main_thread_id)
                    .await
                    .contains(&EXTERNAL_CONNECTION_ID)
            );

            let rebound_prompt = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                TEST_CONNECTION_ID,
                &prompt_request_id,
            )
            .await;
            assert_eq!(rebound_prompt.id(), &prompt_request_id);

            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: prompt_request_id.clone(),
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(/*millis*/ 10), &mut wait_for_prompt)
                    .await
                    .is_err()
            );

            harness
                .processor
                .process_response(
                    TEST_CONNECTION_ID,
                    ConnectionOrigin::Stdio,
                    JSONRPCResponse {
                        id: prompt_request_id,
                        result: serde_json::json!({ "decision": "accept" }),
                    },
                )
                .await;
            let prompt_result =
                tokio::time::timeout(Duration::from_secs(/*secs*/ 1), wait_for_prompt)
                    .await
                    .expect("TUI prompt response should not time out")
                    .expect("prompt waiter should receive TUI response")
                    .expect("TUI accept should resolve rebound prompt");
            assert_eq!(prompt_result, serde_json::json!({ "decision": "accept" }));

            drop(external_write_complete_tx);
            gate_release_tx
                .send(())
                .expect("running gate future should still be waiting");
            gate_task.await.expect("gate task should finish");
            close_task
                .await
                .expect("connection close task should finish");
            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_current_time_request_is_bound_to_owner_epoch() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_current_time_request_is_bound_to_owner_epoch",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 43_001),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));
            let started = harness
                .start_thread(/*request_id*/ 43_002, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record(main_thread_id));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 43_003),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            assert!(
                participation
                    .session
                    .expect("approved session")
                    .active_lease
                    .is_some()
            );

            let stale_request_recipients = current_time_request_recipients(
                &harness.processor.controller_processor,
                main_thread_id,
                vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
            )?;
            assert_eq!(
                stale_request_recipients.connection_ids(),
                &[EXTERNAL_CONNECTION_ID]
            );
            let stale_thread_outgoing =
                ThreadScopedOutgoingMessageSender::new_with_request_recipients(
                    Arc::clone(&harness.processor.outgoing),
                    stale_request_recipients,
                    vec![EXTERNAL_CONNECTION_ID],
                    main_thread_id,
                );
            let (stale_request_id, mut wait_for_stale_time) = stale_thread_outgoing
                .send_request(current_time_read_payload(started.thread.id.clone()))
                .await;
            let stale_request = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &stale_request_id,
            )
            .await;
            let ServerRequest::CurrentTimeRead { params, .. } = stale_request else {
                panic!("expected CurrentTimeRead request");
            };
            assert_eq!(params.thread_id, started.thread.id);

            let released: ControllerReleaseControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 43_004,
                        "controller/releaseControl",
                    ),
                )
                .await;
            assert_eq!(released.session.active_lease, None);
            let reacquired: ControllerAcquireControlResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_no_params_request(
                        /*request_id*/ 43_005,
                        "controller/acquireControl",
                    ),
                )
                .await;
            assert!(reacquired.session.active_lease.is_some());

            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: stale_request_id.clone(),
                        result: serde_json::to_value(CurrentTimeReadResponse {
                            current_time_at: 1_781_717_655,
                        })?,
                    },
                )
                .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait_for_stale_time)
                    .await
                    .is_err()
            );
            assert!(
                harness
                    .processor
                    .outgoing
                    .cancel_request(&stale_request_id)
                    .await
            );

            let active_request_recipients = current_time_request_recipients(
                &harness.processor.controller_processor,
                main_thread_id,
                vec![TEST_CONNECTION_ID, EXTERNAL_CONNECTION_ID],
            )?;
            assert_eq!(
                active_request_recipients.connection_ids(),
                &[EXTERNAL_CONNECTION_ID]
            );
            let active_thread_outgoing =
                ThreadScopedOutgoingMessageSender::new_with_request_recipients(
                    Arc::clone(&harness.processor.outgoing),
                    active_request_recipients,
                    vec![EXTERNAL_CONNECTION_ID],
                    main_thread_id,
                );
            let (active_request_id, wait_for_active_time) = active_thread_outgoing
                .send_request(current_time_read_payload(started.thread.id))
                .await;
            let _ = read_server_request_for_connection(
                &mut harness.outgoing_rx,
                EXTERNAL_CONNECTION_ID,
                &active_request_id,
            )
            .await;
            harness
                .processor
                .process_response(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    JSONRPCResponse {
                        id: active_request_id,
                        result: serde_json::to_value(CurrentTimeReadResponse {
                            current_time_at: 1_781_717_656,
                        })?,
                    },
                )
                .await;
            let active_result = tokio::time::timeout(Duration::from_secs(1), wait_for_active_time)
                .await
                .expect("current-time response should not time out")
                .expect("current-time waiter should receive response")
                .expect("active controller response should resolve successfully");
            assert_eq!(
                active_result,
                serde_json::json!({ "currentTimeAt": 1_781_717_656 })
            );

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn controller_authorization_expiry_removes_main_thread_subscription() -> Result<()> {
    run_current_thread_test_with_stack(
        "controller_authorization_expiry_removes_main_thread_subscription",
        async {
            let enrollment_source = Arc::new(TestControllerEnrollmentSource::default());
            let mut harness =
                TracingHarness::new_with_controller_enrollment_source(enrollment_source.clone())
                    .await?;
            let external_session = Arc::new(ConnectionSessionState::new());
            let _: InitializeResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_initialize_request(/*request_id*/ 42_001),
                )
                .await;
            external_session
                .bind_controller_credential_proof(controller_proof(EXTERNAL_CONNECTION_ID));
            let started = harness
                .start_thread(/*request_id*/ 42_002, /*trace*/ None)
                .await;
            let main_thread_id = ThreadId::from_string(&started.thread.id)?;
            enrollment_source.insert(controller_record_with_duration(
                main_thread_id,
                Duration::from_millis(200),
            ));

            let participation: ControllerRequestParticipationResponse = harness
                .request_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_participation_request(/*request_id*/ 42_003),
                )
                .await;
            assert_eq!(
                participation.status,
                ControllerParticipationStatus::Approved
            );
            harness
                .processor
                .thread_processor
                .subscribe_test_connection_for_thread(main_thread_id, EXTERNAL_CONNECTION_ID)
                .await;
            assert!(
                harness
                    .processor
                    .thread_processor
                    .subscribed_connection_ids_for_thread(main_thread_id)
                    .await
                    .contains(&EXTERNAL_CONNECTION_ID)
            );

            tokio::time::sleep(Duration::from_millis(250)).await;
            let expired = harness
                .request_error_for_connection(
                    EXTERNAL_CONNECTION_ID,
                    ConnectionOrigin::ExternalController,
                    Arc::clone(&external_session),
                    controller_thread_read_request(/*request_id*/ 42_004, started.thread.id),
                )
                .await;
            let expired_data: ControllerErrorData =
                serde_json::from_value(expired.error.data.expect("typed controller error"))?;
            assert_eq!(expired_data.code, ControllerErrorCode::AuthorizationExpired);
            assert!(
                !harness
                    .processor
                    .thread_processor
                    .subscribed_connection_ids_for_thread(main_thread_id)
                    .await
                    .contains(&EXTERNAL_CONNECTION_ID)
            );

            harness.shutdown().await;
            Ok(())
        },
    )
}

#[test]
#[serial(app_server_tracing)]
fn thread_start_jsonrpc_span_exports_server_span_and_parents_children() -> Result<()> {
    run_current_thread_test_with_stack(
        "thread_start_jsonrpc_span_exports_server_span_and_parents_children",
        async {
            let mut harness = TracingHarness::new().await?;

            let RemoteTrace {
                trace_id: remote_trace_id,
                parent_span_id: remote_parent_span_id,
                context: remote_trace,
                ..
            } = RemoteTrace::new("00000000000000000000000000000011", "0000000000000022");

            let _: ThreadStartResponse = harness
                .start_thread(/*request_id*/ 20_002, /*trace*/ None)
                .await;
            let untraced_spans = wait_for_exported_spans(harness.tracing, |spans| {
                spans.iter().any(|span| {
                    span.span_kind == SpanKind::Server
                        && span_attr(span, "rpc.method") == Some("thread/start")
                })
            })
            .await;
            let untraced_server_span = find_rpc_span_with_trace(
                &untraced_spans,
                SpanKind::Server,
                "thread/start",
                untraced_spans
                    .iter()
                    .rev()
                    .find(|span| {
                        span.span_kind == SpanKind::Server
                            && span_attr(span, "rpc.system") == Some("jsonrpc")
                            && span_attr(span, "rpc.method") == Some("thread/start")
                    })
                    .unwrap_or_else(|| {
                        panic!(
                            "missing latest thread/start server span; exported spans:\n{}",
                            format_spans(&untraced_spans)
                        )
                    })
                    .span_context
                    .trace_id(),
            );
            assert_has_internal_descendant_at_min_depth(
                &untraced_spans,
                untraced_server_span,
                /*min_depth*/ 1,
            );

            let baseline_len = untraced_spans.len();
            let _: ThreadStartResponse = harness
                .start_thread(/*request_id*/ 20_003, Some(remote_trace))
                .await;
            let spans = wait_for_new_exported_spans(harness.tracing, baseline_len, |spans| {
                spans.iter().any(|span| {
                    span.span_kind == SpanKind::Server
                        && span_attr(span, "rpc.method") == Some("thread/start")
                        && span.span_context.trace_id() == remote_trace_id
                }) && spans.iter().any(|span| {
                    span.name.as_ref() == "app_server.thread_start.notify_started"
                        && span.span_context.trace_id() == remote_trace_id
                })
            })
            .await;

            let server_request_span =
                find_rpc_span_with_trace(&spans, SpanKind::Server, "thread/start", remote_trace_id);
            assert_eq!(server_request_span.name.as_ref(), "thread/start");
            assert_eq!(server_request_span.parent_span_id, remote_parent_span_id);
            assert!(server_request_span.parent_span_is_remote);
            assert_eq!(server_request_span.span_context.trace_id(), remote_trace_id);
            assert_ne!(server_request_span.span_context.span_id(), SpanId::INVALID);
            assert_has_internal_descendant_at_min_depth(
                &spans,
                server_request_span,
                /*min_depth*/ 1,
            );
            assert_has_internal_descendant_at_min_depth(
                &spans,
                server_request_span,
                /*min_depth*/ 2,
            );
            harness.shutdown().await;

            Ok(())
        },
    )
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn turn_start_jsonrpc_span_parents_core_turn_spans() -> Result<()> {
    let mut harness = TracingHarness::new().await?;
    let thread_start_response = harness.start_thread(/*request_id*/ 2, /*trace*/ None).await;
    let thread_id = thread_start_response.thread.id.clone();

    harness.reset_tracing();

    let RemoteTrace {
        trace_id: remote_trace_id,
        parent_span_id: remote_parent_span_id,
        context: remote_trace,
    } = RemoteTrace::new("00000000000000000000000000000077", "0000000000000088");
    let turn_start_response: TurnStartResponse = harness
        .request(
            ClientRequest::TurnStart {
                request_id: RequestId::Integer(3),
                params: TurnStartParams {
                    environments: None,
                    thread_id,
                    client_user_message_id: None,
                    input: vec![UserInput::Text {
                        text: "hello".to_string(),
                        text_elements: Vec::new(),
                    }],
                    responsesapi_client_metadata: None,
                    additional_context: None,
                    cwd: None,
                    runtime_workspace_roots: None,
                    approval_policy: None,
                    sandbox_policy: None,
                    permissions: None,
                    approvals_reviewer: None,
                    model: None,
                    service_tier: None,
                    effort: None,
                    summary: None,
                    personality: None,
                    output_schema: None,
                    collaboration_mode: None,
                    multi_agent_mode: None,
                },
            },
            Some(remote_trace),
        )
        .await;
    let spans = wait_for_exported_spans(harness.tracing, |spans| {
        spans.iter().any(|span| {
            span.span_kind == SpanKind::Server
                && span_attr(span, "rpc.method") == Some("turn/start")
                && span.span_context.trace_id() == remote_trace_id
        }) && spans.iter().any(|span| {
            span_attr(span, "codex.op") == Some("turn_input")
                && span.span_context.trace_id() == remote_trace_id
        })
    })
    .await;

    let server_request_span =
        find_rpc_span_with_trace(&spans, SpanKind::Server, "turn/start", remote_trace_id);
    let core_turn_span =
        find_span_with_trace(&spans, remote_trace_id, "codex.op=turn_input", |span| {
            span_attr(span, "codex.op") == Some("turn_input")
        });

    assert_eq!(server_request_span.parent_span_id, remote_parent_span_id);
    assert!(server_request_span.parent_span_is_remote);
    assert_eq!(server_request_span.span_context.trace_id(), remote_trace_id);
    assert_eq!(
        span_attr(server_request_span, "turn.id"),
        Some(turn_start_response.turn.id.as_str())
    );
    assert_span_descends_from(&spans, core_turn_span, server_request_span);
    harness.shutdown().await;

    Ok(())
}
