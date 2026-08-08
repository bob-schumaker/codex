use super::ConnectionSessionState;
use super::MessageProcessor;
use super::MessageProcessorArgs;
use crate::analytics_utils::analytics_events_client_from_config;
use crate::config_manager::ConfigManager;
use crate::controller_enrollment::ControllerCredentialProof;
use crate::controller_enrollment::ControllerEnrollmentRecord;
use crate::controller_enrollment::ControllerEnrollmentSource;
use crate::controller_enrollment::EmptyControllerEnrollmentSource;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingMessageSender;
use crate::transport::AppServerTransport;
use crate::transport::ConnectionOrigin;
use anyhow::Result;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::write_mock_responses_config_toml;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::CommandExecutionRequestApprovalParams;
use codex_app_server_protocol::ControllerAcquireControlResponse;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::ControllerParticipationStatus;
use codex_app_server_protocol::ControllerReleaseControlResponse;
use codex_app_server_protocol::ControllerRequestParticipationParams;
use codex_app_server_protocol::ControllerRequestParticipationResponse;
use codex_app_server_protocol::ControllerRetryDisposition;
use codex_app_server_protocol::ControllerSignOffResponse;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::InitializeResponse;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ServerRequestPayload;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadResumeParams;
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
        let server = create_mock_responses_server_repeating_assistant("Done").await;
        let codex_home = TempDir::new()?;
        let config = Arc::new(build_test_config(codex_home.path(), &server.uri()).await?);
        let (processor, outgoing_rx) =
            build_test_processor(config, controller_enrollment_source).await;
        let tracing = init_test_tracing();
        tracing.exporter.reset();
        tracing::callsite::rebuild_interest_cache();
        let mut harness = Self {
            _server: server,
            _codex_home: codex_home,
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
        state_db: None,
        config_warnings: Vec::new(),
        session_source: SessionSource::VSCode,
        auth_manager,
        installation_id: "11111111-1111-4111-8111-111111111111".to_string(),
        code_mode_session_provider: None,
        rpc_transport: AppServerRpcTransport::Stdio,
        remote_control_handle: None,
        controller_enrollment_source,
        native_controller_participation_approver: None,
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

fn controller_proof(connection_id: ConnectionId) -> ControllerCredentialProof {
    ControllerCredentialProof {
        subject_id: "controller-subject".to_string(),
        credential_fingerprint: "credential-fingerprint".to_string(),
        connection_id,
    }
}

fn controller_record(main_thread_id: ThreadId) -> ControllerEnrollmentRecord {
    ControllerEnrollmentRecord {
        subject_id: "controller-subject".to_string(),
        credential_fingerprint: "credential-fingerprint".to_string(),
        main_thread_id,
        authorization_epoch: 7,
        revocation_epoch: 6,
        expires_at: std::time::Instant::now() + Duration::from_secs(60),
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
            assert!(approved_session.active_lease.is_some());

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
