use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::attestation::app_server_attestation_provider;
use crate::config_manager::ConfigManager;
use crate::connection_rpc_gate::ConnectionRpcGate;
use crate::controller_admission::AdmissionRule;
use crate::controller_admission::RequiredAuthority;
use crate::controller_admission::TargetExtraction;
use crate::controller_admission::admit_initialized_client_request;
use crate::controller_admission::controller_not_allowed;
use crate::controller_admission::controller_overloaded;
use crate::controller_admission::controller_transport_closing;
use crate::controller_cursor::bind_controller_response_cursors;
use crate::controller_cursor::unbind_controller_request_cursors;
use crate::controller_enrollment::ControllerCredentialProof;
use crate::controller_enrollment::ControllerEnrollmentPolicy;
use crate::controller_enrollment::ControllerEnrollmentSource;
use crate::controller_native_approval::NativeControllerParticipationApprover;
use crate::controller_session::ControllerOwnershipStatus;
use crate::controller_session::ControllerSessionClock;
use crate::controller_session::ControllerSessionConfig;
use crate::current_time::app_server_time_provider;
use crate::error_code::invalid_request;
use crate::extensions::ThreadExtensionDependencies;
use crate::extensions::app_server_extension_event_sink_with_controller_targeting;
use crate::extensions::guardian_agent_spawner;
use crate::extensions::thread_extensions;
use crate::external_agent_migration::ExternalAgentConfigRequestProcessor;
use crate::external_agent_migration::ExternalAgentConfigRequestProcessorArgs;
use crate::fs_watch::FsWatchManager;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::outgoing_message::OutgoingMessageSender;
use crate::outgoing_message::RequestContext;
use crate::request_processors::AccountRequestProcessor;
use crate::request_processors::AppsRequestProcessor;
use crate::request_processors::CatalogRequestProcessor;
use crate::request_processors::CommandExecRequestProcessor;
use crate::request_processors::ConfigRequestProcessor;
use crate::request_processors::ControllerNormalAuthorization;
use crate::request_processors::ControllerRequestProcessor;
use crate::request_processors::ControllerRequestTarget;
use crate::request_processors::EnvironmentRequestProcessor;
use crate::request_processors::FeedbackRequestProcessor;
use crate::request_processors::FsRequestProcessor;
use crate::request_processors::GitRequestProcessor;
use crate::request_processors::InitializeRequestProcessor;
use crate::request_processors::MarketplaceRequestProcessor;
use crate::request_processors::McpRequestProcessor;
use crate::request_processors::PluginRequestProcessor;
use crate::request_processors::ProcessExecRequestProcessor;
use crate::request_processors::RemoteControlRequestProcessor;
use crate::request_processors::SearchRequestProcessor;
use crate::request_processors::ThreadGoalRequestProcessor;
use crate::request_processors::ThreadListScope;
use crate::request_processors::ThreadRequestProcessor;
use crate::request_processors::TurnRequestProcessor;
use crate::request_processors::WindowsSandboxRequestProcessor;
use crate::request_processors::read_server_diagnostics;
use crate::request_serialization::QueuedInitializedRequest;
use crate::request_serialization::RequestSerializationPriority;
use crate::request_serialization::RequestSerializationQueueKey;
use crate::request_serialization::RequestSerializationQueues;
use crate::skills_watcher::SkillsWatcher;
use crate::thread_state::ConnectionCapabilities;
use crate::thread_state::ThreadStateManager;
use crate::transport::AppServerTransport;
use crate::transport::ConnectionOrigin;
use crate::transport::RemoteControlHandle;
use codex_analytics::AnalyticsEventsClient;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::ClientNotification;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ClientRequestSerializationScope;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::ExperimentalApi;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::experimental_required_message;
use codex_arg0::Arg0DispatchPaths;
use codex_chatgpt::workspace_settings;
use codex_code_mode::CodeModeSessionProvider;
use codex_core::ThreadManager;
use codex_core::config::Config;
use codex_core::config::ThreadStoreConfig;
use codex_exec_server::EnvironmentManager;
use codex_feedback::CodexFeedback;
use codex_goal_extension::GoalService;
use codex_home::CodexHomeUserInstructionsProvider;
use codex_login::AuthManager;
use codex_protocol::ThreadId;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::W3cTraceContext;
use codex_rollout::StateDbHandle;
use codex_state::log_db::LogDbLayer;
use codex_thread_store::LocalQueueStore;
use codex_thread_store::QueueStore;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::models_refresh_worker::ModelsRefreshWorker;

const CONNECTION_RPC_DRAIN_TIMEOUT: Duration = Duration::from_secs(/*secs*/ 30);

fn deserialize_client_request(request: JSONRPCRequest) -> Result<ClientRequest, JSONRPCErrorError> {
    ClientRequest::try_from(request)
        .map_err(|err| invalid_request(format!("Invalid request: {err}")))
}

pub(crate) struct MessageProcessor {
    outgoing: Arc<OutgoingMessageSender>,
    models_refresh_worker: ModelsRefreshWorker,
    skills_watcher: Arc<SkillsWatcher>,
    account_processor: AccountRequestProcessor,
    apps_processor: AppsRequestProcessor,
    catalog_processor: CatalogRequestProcessor,
    command_exec_processor: CommandExecRequestProcessor,
    process_exec_processor: ProcessExecRequestProcessor,
    config_processor: ConfigRequestProcessor,
    controller_processor: ControllerRequestProcessor,
    environment_processor: EnvironmentRequestProcessor,
    external_agent_config_processor: ExternalAgentConfigRequestProcessor,
    feedback_processor: FeedbackRequestProcessor,
    fs_processor: FsRequestProcessor,
    git_processor: GitRequestProcessor,
    initialize_processor: InitializeRequestProcessor,
    marketplace_processor: MarketplaceRequestProcessor,
    mcp_processor: McpRequestProcessor,
    plugin_processor: PluginRequestProcessor,
    remote_control_processor: RemoteControlRequestProcessor,
    search_processor: SearchRequestProcessor,
    thread_goal_processor: ThreadGoalRequestProcessor,
    thread_processor: ThreadRequestProcessor,
    turn_processor: TurnRequestProcessor,
    windows_sandbox_processor: WindowsSandboxRequestProcessor,
    request_serialization_queues: RequestSerializationQueues,
}

#[derive(Debug)]
pub(crate) struct ConnectionSessionState {
    pub(crate) rpc_gate: Arc<ConnectionRpcGate>,
    initialized: OnceLock<InitializedConnectionSessionState>,
    controller_credential_proof: std::sync::Mutex<Option<ControllerCredentialProof>>,
    controller_transport_closing: AtomicBool,
}

#[derive(Debug)]
pub(crate) struct InitializedConnectionSessionState {
    pub(crate) experimental_api_enabled: bool,
    pub(crate) opted_out_notification_methods: HashSet<String>,
    pub(crate) app_server_client_name: String,
    pub(crate) client_version: String,
    pub(crate) request_attestation: bool,
    pub(crate) client_mcp_extensions: ClientMcpExtensions,
}

impl Default for ConnectionSessionState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionSessionState {
    pub(crate) fn new() -> Self {
        Self {
            rpc_gate: Arc::new(ConnectionRpcGate::new()),
            initialized: OnceLock::new(),
            controller_credential_proof: std::sync::Mutex::new(None),
            controller_transport_closing: AtomicBool::new(false),
        }
    }

    pub(crate) fn initialized(&self) -> bool {
        self.initialized.get().is_some()
    }

    pub(crate) fn experimental_api_enabled(&self) -> bool {
        self.initialized
            .get()
            .is_some_and(|session| session.experimental_api_enabled)
    }

    pub(crate) fn opted_out_notification_methods(&self) -> HashSet<String> {
        self.initialized
            .get()
            .map(|session| session.opted_out_notification_methods.clone())
            .unwrap_or_default()
    }

    pub(crate) fn app_server_client_name(&self) -> Option<&str> {
        self.initialized
            .get()
            .map(|session| session.app_server_client_name.as_str())
    }

    pub(crate) fn client_version(&self) -> Option<&str> {
        self.initialized
            .get()
            .map(|session| session.client_version.as_str())
    }

    pub(crate) fn request_attestation(&self) -> bool {
        self.initialized
            .get()
            .is_some_and(|session| session.request_attestation)
    }

    pub(crate) fn client_mcp_extensions(&self) -> ClientMcpExtensions {
        self.initialized
            .get()
            .map(|session| session.client_mcp_extensions.clone())
            .unwrap_or_default()
    }
    pub(crate) fn initialize(&self, session: InitializedConnectionSessionState) -> Result<(), ()> {
        self.initialized.set(session).map_err(|_| ())
    }

    #[cfg(test)]
    pub(crate) fn bind_controller_credential_proof(&self, proof: ControllerCredentialProof) {
        *self
            .controller_credential_proof
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(proof);
    }

    pub(crate) fn controller_credential_proof(&self) -> Option<ControllerCredentialProof> {
        self.controller_credential_proof
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn begin_controller_transport_closing(&self) {
        self.controller_transport_closing
            .store(/*val*/ true, Ordering::Release);
    }

    pub(crate) fn controller_transport_closing(&self) -> bool {
        self.controller_transport_closing.load(Ordering::Acquire)
    }
}

pub(crate) struct MessageProcessorArgs {
    pub(crate) outgoing: Arc<OutgoingMessageSender>,
    pub(crate) analytics_events_client: AnalyticsEventsClient,
    pub(crate) arg0_paths: Arg0DispatchPaths,
    pub(crate) config: Arc<Config>,
    pub(crate) config_manager: ConfigManager,
    pub(crate) environment_manager: Arc<EnvironmentManager>,
    pub(crate) feedback: CodexFeedback,
    pub(crate) log_db: Option<LogDbLayer>,
    pub(crate) state_db: Option<StateDbHandle>,
    pub(crate) config_warnings: Vec<ConfigWarningNotification>,
    pub(crate) session_source: SessionSource,
    pub(crate) auth_manager: Arc<AuthManager>,
    pub(crate) installation_id: String,
    pub(crate) code_mode_session_provider: Option<Arc<dyn CodeModeSessionProvider>>,
    pub(crate) rpc_transport: AppServerRpcTransport,
    pub(crate) remote_control_handle: Option<RemoteControlHandle>,
    pub(crate) controller_enrollment_source: Arc<dyn ControllerEnrollmentSource>,
    pub(crate) native_controller_participation_approver:
        Option<NativeControllerParticipationApprover>,
    pub(crate) controller_ownership_status_tx: Option<mpsc::Sender<ControllerOwnershipStatus>>,
    pub(crate) plugin_startup_tasks: crate::PluginStartupTasks,
}

struct InitializedClientRequest {
    connection_request_id: ConnectionRequestId,
    connection_origin: ConnectionOrigin,
    admission_rule: AdmissionRule,
    codex_request: ClientRequest,
    session: Arc<ConnectionSessionState>,
    request_context: RequestContext,
    app_server_client_name: Option<String>,
    client_version: Option<String>,
    client_mcp_extensions: ClientMcpExtensions,
}

impl MessageProcessor {
    /// Create a new `MessageProcessor`, retaining a handle to the outgoing
    /// `Sender` so handlers can enqueue messages to be written to stdout.
    pub(crate) fn new(args: MessageProcessorArgs) -> Self {
        let MessageProcessorArgs {
            outgoing,
            analytics_events_client,
            arg0_paths,
            config,
            config_manager,
            environment_manager,
            feedback,
            log_db,
            state_db,
            config_warnings,
            session_source,
            auth_manager,
            installation_id,
            code_mode_session_provider,
            rpc_transport,
            remote_control_handle,
            controller_enrollment_source,
            native_controller_participation_approver,
            controller_ownership_status_tx,
            plugin_startup_tasks,
        } = args;
        let thread_state_manager = ThreadStateManager::new();
        // The thread store is intentionally process-scoped. Config reloads can
        // affect per-thread behavior, but they must not move newly started,
        // resumed, or forked threads to a different persistence backend/root.
        let thread_store = codex_core::thread_store_from_config(config.as_ref(), state_db.clone());
        // Queue persistence requires SQLite, so in-memory thread stores and
        // app servers without a state database do not have a queue backend.
        let queue_store: Option<Arc<dyn QueueStore>> = match &config.experimental_thread_store {
            ThreadStoreConfig::Local => state_db.as_ref().map(|state_db| {
                Arc::new(LocalQueueStore::new(Arc::clone(state_db))) as Arc<dyn QueueStore>
            }),
            ThreadStoreConfig::InMemory { .. } => None,
        };
        let environment_manager_for_requests = Arc::clone(&environment_manager);
        let environment_manager_for_extensions = Arc::clone(&environment_manager);
        let restriction_product = session_source.restriction_product();
        let executor_skill_provider: Arc<dyn codex_skills_extension::SkillProvider> = Arc::new(
            codex_skills_extension::ExecutorSkillProvider::new_with_restriction_product(
                Arc::clone(&environment_manager_for_extensions),
                restriction_product,
            ),
        );
        let goal_service = Arc::new(GoalService::new());
        let controller_processor = ControllerRequestProcessor::new(
            outgoing.clone(),
            controller_enrollment_source,
            native_controller_participation_approver,
            controller_ownership_status_tx,
            ControllerEnrollmentPolicy::BestEffort,
            ControllerSessionClock::from_fn(std::time::Instant::now),
            ControllerSessionConfig {
                lease_duration: Duration::from_secs(5 * 60),
            },
        );
        let thread_manager = Arc::new_cyclic(|thread_manager| {
            let manager = ThreadManager::new(
                config.as_ref(),
                auth_manager.clone(),
                codex_core::build_models_manager(config.as_ref(), auth_manager.clone()),
                codex_core::CodexAppsToolsCache::default(),
                session_source,
                environment_manager,
                thread_extensions(
                    guardian_agent_spawner(thread_manager.clone()),
                    ThreadExtensionDependencies {
                        event_sink: app_server_extension_event_sink_with_controller_targeting(
                            outgoing.clone(),
                            thread_state_manager.clone(),
                            controller_processor.clone(),
                        ),
                        auth_manager: auth_manager.clone(),
                        state_db: state_db.clone(),
                        analytics_events_client: analytics_events_client.clone(),
                        thread_manager: thread_manager.clone(),
                        goal_service: Arc::clone(&goal_service),
                        environment_manager: Arc::clone(&environment_manager_for_extensions),
                        executor_skill_provider: Arc::clone(&executor_skill_provider),
                        git_attribution_base_url: config.chatgpt_base_url.clone(),
                        http_client_factory: config.http_client_factory(),
                        queue_store,
                    },
                ),
                Arc::new(CodexHomeUserInstructionsProvider::new(
                    config.codex_home.clone(),
                )),
                Some(analytics_events_client.clone()),
                Arc::clone(&thread_store),
                codex_core::local_agent_graph_store_from_state_db(state_db.as_ref()),
                installation_id,
                Some(app_server_attestation_provider(
                    outgoing.clone(),
                    thread_state_manager.clone(),
                )),
                Some(app_server_time_provider(
                    outgoing.clone(),
                    thread_state_manager.clone(),
                    controller_processor.clone(),
                )),
            );
            match code_mode_session_provider {
                Some(provider) => manager.with_code_mode_session_provider(provider),
                None => manager,
            }
        });
        let models_manager = thread_manager.get_models_manager();
        let models_refresh_worker =
            crate::models_refresh_worker::spawn(&models_manager, config.http_client_factory());
        thread_manager
            .plugins_manager()
            .set_analytics_events_client(analytics_events_client.clone());
        let skills_watcher = SkillsWatcher::new(
            thread_manager.skills_service(),
            &config.codex_home,
            outgoing.clone(),
        );

        let pending_thread_unloads = Arc::new(Mutex::new(HashSet::new()));
        let thread_watch_manager =
            crate::thread_status::ThreadWatchManager::new_with_outgoing_and_controller_targeting(
                outgoing.clone(),
                thread_state_manager.clone(),
                controller_processor.clone(),
            );
        let thread_list_state_permit = Arc::new(Semaphore::new(/*permits*/ 1));
        let workspace_settings_cache =
            Arc::new(workspace_settings::WorkspaceSettingsCache::default());
        let app_list_shutdown_token = CancellationToken::new();
        let request_serialization_queues = RequestSerializationQueues::default();
        let config_processor = ConfigRequestProcessor::new(
            outgoing.clone(),
            config_manager.clone(),
            thread_manager.clone(),
            analytics_events_client.clone(),
        );
        let on_effective_plugins_changed =
            crate::effective_plugin_change::effective_plugins_changed_callback(
                auth_manager.clone(),
                Arc::clone(&thread_manager),
                config_manager.clone(),
                config_processor.clone(),
                request_serialization_queues.clone(),
            );
        let account_processor = AccountRequestProcessor::new(
            auth_manager.clone(),
            Arc::clone(&thread_manager),
            outgoing.clone(),
            Arc::clone(&config),
            config_manager.clone(),
        );
        let apps_processor = AppsRequestProcessor::new(
            auth_manager.clone(),
            Arc::clone(&thread_manager),
            outgoing.clone(),
            config_manager.clone(),
            Arc::clone(&workspace_settings_cache),
            app_list_shutdown_token,
        );
        let catalog_processor = CatalogRequestProcessor::new(
            outgoing.clone(),
            Arc::clone(&skills_watcher),
            auth_manager.clone(),
            Arc::clone(&thread_manager),
            Arc::clone(&config),
            config_manager.clone(),
            Arc::clone(&workspace_settings_cache),
        );
        let command_exec_processor = CommandExecRequestProcessor::new(
            arg0_paths.clone(),
            Arc::clone(&config),
            outgoing.clone(),
            config_manager.clone(),
            Arc::clone(&environment_manager_for_requests),
        );
        let process_exec_processor = ProcessExecRequestProcessor::new(
            outgoing.clone(),
            Arc::clone(&environment_manager_for_requests),
        );
        let feedback_processor = FeedbackRequestProcessor::new(
            auth_manager.clone(),
            Arc::clone(&thread_manager),
            Arc::clone(&config),
            feedback,
            log_db.clone(),
            state_db.clone(),
        );
        let git_processor = GitRequestProcessor::new();
        let initialize_processor = InitializeRequestProcessor::new(
            outgoing.clone(),
            analytics_events_client.clone(),
            Arc::clone(&config),
            config_warnings.clone(),
            rpc_transport,
        );
        let marketplace_processor = MarketplaceRequestProcessor::new(
            Arc::clone(&config),
            config_manager.clone(),
            Arc::clone(&thread_manager),
        );
        let mcp_processor = McpRequestProcessor::new(
            auth_manager.clone(),
            Arc::clone(&thread_manager),
            outgoing.clone(),
            config_manager.clone(),
            thread_state_manager.clone(),
            controller_processor.clone(),
        );
        let plugin_processor = PluginRequestProcessor::new(
            auth_manager.clone(),
            Arc::clone(&thread_manager),
            outgoing.clone(),
            analytics_events_client.clone(),
            config_manager.clone(),
            workspace_settings_cache,
            on_effective_plugins_changed,
        );
        let remote_control_processor = RemoteControlRequestProcessor::new(remote_control_handle);
        let search_processor = SearchRequestProcessor::new(outgoing.clone());
        let thread_goal_processor = ThreadGoalRequestProcessor::new(
            Arc::clone(&thread_manager),
            outgoing.clone(),
            Arc::clone(&config),
            thread_state_manager.clone(),
            state_db.clone(),
            Arc::clone(&goal_service),
            controller_processor.clone(),
        );
        let thread_processor = ThreadRequestProcessor::new(
            auth_manager.clone(),
            Arc::clone(&thread_manager),
            outgoing.clone(),
            arg0_paths.clone(),
            Arc::clone(&config),
            config_manager.clone(),
            Arc::clone(&thread_store),
            Arc::clone(&pending_thread_unloads),
            thread_state_manager.clone(),
            thread_watch_manager.clone(),
            Arc::clone(&thread_list_state_permit),
            thread_goal_processor.clone(),
            state_db.clone(),
            log_db,
            Arc::clone(&skills_watcher),
            config_warnings,
            controller_processor.clone(),
        );
        let turn_processor = TurnRequestProcessor::new(
            auth_manager.clone(),
            Arc::clone(&thread_manager),
            outgoing.clone(),
            analytics_events_client.clone(),
            arg0_paths.clone(),
            Arc::clone(&config),
            config_manager.clone(),
            pending_thread_unloads,
            thread_state_manager,
            thread_watch_manager,
            thread_list_state_permit,
            Arc::clone(&skills_watcher),
            controller_processor.clone(),
        );
        if matches!(plugin_startup_tasks, crate::PluginStartupTasks::Start) {
            // Keep plugin startup warmups aligned at app-server startup.
            let on_effective_plugins_changed =
                plugin_processor.effective_plugins_changed_callback();
            thread_manager
                .plugins_manager()
                .maybe_start_plugin_startup_tasks_for_config(
                    &config.plugins_config_input(),
                    auth_manager,
                    Some(on_effective_plugins_changed),
                );
        }
        let external_agent_config_processor =
            ExternalAgentConfigRequestProcessor::new(ExternalAgentConfigRequestProcessorArgs {
                outgoing: outgoing.clone(),
                thread_manager: Arc::clone(&thread_manager),
                thread_store: Arc::clone(&thread_store),
                config_manager: config_manager.clone(),
                config_processor: config_processor.clone(),
                state_db,
                analytics_events_client,
                arg0_paths,
                codex_home: config.codex_home.to_path_buf(),
            });
        let environment_processor =
            EnvironmentRequestProcessor::new(thread_manager.environment_manager());
        let fs_processor = FsRequestProcessor::new(
            Arc::clone(&environment_manager_for_requests),
            FsWatchManager::new(outgoing.clone()),
        );
        let windows_sandbox_processor = WindowsSandboxRequestProcessor::new(
            outgoing.clone(),
            Arc::clone(&config),
            config_manager,
        );

        Self {
            outgoing,
            models_refresh_worker,
            skills_watcher,
            account_processor,
            apps_processor,
            catalog_processor,
            command_exec_processor,
            process_exec_processor,
            config_processor,
            controller_processor,
            environment_processor,
            external_agent_config_processor,
            feedback_processor,
            fs_processor,
            git_processor,
            initialize_processor,
            marketplace_processor,
            mcp_processor,
            plugin_processor,
            remote_control_processor,
            search_processor,
            thread_goal_processor,
            thread_processor,
            turn_processor,
            windows_sandbox_processor,
            request_serialization_queues,
        }
    }

    pub(crate) fn clear_runtime_references(&self) {
        self.account_processor.clear_external_auth();
        self.apps_processor.shutdown();
        self.models_refresh_worker.shutdown();
        self.skills_watcher.shutdown();
    }

    pub(crate) async fn process_request(
        self: &Arc<Self>,
        connection_id: ConnectionId,
        connection_origin: ConnectionOrigin,
        request: JSONRPCRequest,
        transport: &AppServerTransport,
        session: Arc<ConnectionSessionState>,
    ) {
        let request_method = request.method.as_str();
        tracing::trace!(
            ?connection_id,
            request_id = ?request.id,
            "app-server request: {request_method}"
        );
        let request_id = ConnectionRequestId {
            connection_id,
            request_id: request.id.clone(),
        };
        let request_span =
            crate::app_server_tracing::request_span(&request, transport, connection_id, &session);
        let request_trace = request.trace.as_ref().map(|trace| W3cTraceContext {
            traceparent: trace.traceparent.clone(),
            tracestate: trace.tracestate.clone(),
        });
        let request_context = RequestContext::new(request_id.clone(), request_span, request_trace);
        Self::run_request_with_context(
            Arc::clone(&self.outgoing),
            request_context.clone(),
            async {
                if matches!(connection_origin, ConnectionOrigin::ExternalController)
                    && session.controller_transport_closing()
                {
                    self.outgoing
                        .send_error(request_id.clone(), controller_transport_closing())
                        .await;
                    return;
                }

                let codex_request = deserialize_client_request(request);
                let result = match codex_request {
                    Ok(codex_request) => {
                        // Websocket callers finalize outbound readiness in lib.rs after mirroring
                        // session state into outbound state and sending initialize notifications to
                        // this specific connection. Passing `None` avoids marking the connection
                        // ready too early from inside the shared request handler.
                        self.handle_client_request(
                            request_id.clone(),
                            connection_origin,
                            codex_request,
                            Arc::clone(&session),
                            /*outbound_initialized*/ None,
                            request_context.clone(),
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                if let Err(error) = result {
                    self.outgoing.send_error(request_id.clone(), error).await;
                }
            },
        )
        .await;
    }

    /// Handles a typed request path used by in-process embedders.
    ///
    /// This bypasses JSON request deserialization but keeps identical request
    /// semantics by delegating to `handle_client_request`.
    pub(crate) async fn process_client_request(
        self: &Arc<Self>,
        connection_id: ConnectionId,
        connection_origin: ConnectionOrigin,
        request: ClientRequest,
        session: Arc<ConnectionSessionState>,
        outbound_initialized: &AtomicBool,
    ) {
        let request_id = ConnectionRequestId {
            connection_id,
            request_id: request.id().clone(),
        };
        let request_span =
            crate::app_server_tracing::typed_request_span(&request, connection_id, &session);
        let request_context =
            RequestContext::new(request_id.clone(), request_span, /*parent_trace*/ None);
        tracing::trace!(
            ?connection_id,
            request_id = ?request_id.request_id,
            "app-server typed request"
        );
        Self::run_request_with_context(
            Arc::clone(&self.outgoing),
            request_context.clone(),
            async {
                // In-process clients do not have the websocket transport loop that performs
                // post-initialize bookkeeping, so they still finalize outbound readiness in
                // the shared request handler.
                let result = self
                    .handle_client_request(
                        request_id.clone(),
                        connection_origin,
                        request,
                        Arc::clone(&session),
                        Some(outbound_initialized),
                        request_context.clone(),
                    )
                    .await;
                if let Err(error) = result {
                    self.outgoing.send_error(request_id.clone(), error).await;
                }
            },
        )
        .await;
    }

    pub(crate) async fn process_notification(&self, notification: JSONRPCNotification) {
        // Currently, we do not expect to receive any notifications from the
        // client, so we just log them.
        tracing::info!("<- notification: {:?}", notification);
    }

    /// Handles typed notifications from in-process clients.
    pub(crate) async fn process_client_notification(&self, notification: ClientNotification) {
        // Currently, we do not expect to receive any typed notifications from
        // in-process clients, so we just log them.
        tracing::info!("<- typed notification: {:?}", notification);
    }

    async fn run_request_with_context<F>(
        outgoing: Arc<OutgoingMessageSender>,
        request_context: RequestContext,
        request_fut: F,
    ) where
        F: Future<Output = ()>,
    {
        outgoing
            .register_request_context(request_context.clone())
            .await;
        request_fut.instrument(request_context.span()).await;
    }

    pub(crate) fn thread_created_receiver(&self) -> broadcast::Receiver<ThreadId> {
        self.thread_processor.thread_created_receiver()
    }

    pub(crate) async fn send_initialize_notifications_to_connection(
        &self,
        connection_id: ConnectionId,
    ) {
        self.initialize_processor
            .send_initialize_notifications_to_connection(connection_id)
            .await;
    }

    pub(crate) async fn connection_initialized(
        &self,
        connection_id: ConnectionId,
        request_attestation: bool,
    ) {
        self.thread_processor
            .connection_initialized(
                connection_id,
                ConnectionCapabilities {
                    request_attestation,
                },
            )
            .await;
    }

    pub(crate) async fn send_initialize_notifications(&self) {
        self.initialize_processor
            .send_initialize_notifications()
            .await;
    }

    pub(crate) async fn try_attach_thread_listener(
        &self,
        thread_id: ThreadId,
        connection_ids: Vec<ConnectionId>,
    ) {
        self.thread_processor
            .try_attach_thread_listener(thread_id, connection_ids)
            .await;
    }

    pub(crate) async fn try_attach_thread_listener_for_initialized_connections(
        &self,
        thread_id: ThreadId,
        connections: Vec<(ConnectionId, ConnectionOrigin)>,
    ) {
        let mut connection_ids = Vec::new();
        for (connection_id, origin) in connections {
            if matches!(origin, ConnectionOrigin::ExternalController)
                && !self
                    .controller_processor
                    .can_auto_attach_thread_listener(connection_id, thread_id)
                    .await
            {
                continue;
            }
            connection_ids.push(connection_id);
        }
        self.try_attach_thread_listener(thread_id, connection_ids)
            .await;
    }

    pub(crate) async fn drain_background_tasks(&self) {
        self.models_refresh_worker.shutdown();
        self.thread_processor.drain_background_tasks().await;
    }

    pub(crate) async fn cancel_active_login(&self) {
        self.account_processor.cancel_active_login().await;
    }

    pub(crate) async fn clear_all_thread_listeners(&self) {
        self.thread_processor.clear_all_thread_listeners().await;
    }

    pub(crate) async fn shutdown_threads(&self) {
        self.thread_processor.shutdown_threads().await;
    }

    pub(crate) async fn connection_closed(
        &self,
        connection_id: ConnectionId,
        session_state: &ConnectionSessionState,
    ) {
        if let Some(main_thread_id) = self
            .controller_processor
            .main_thread_for_live_session(connection_id)
        {
            self.thread_processor
                .unsubscribe_connection_from_thread(main_thread_id, connection_id)
                .await;
        }
        let disconnected_controller_main_thread_id = self
            .controller_processor
            .connection_closed(connection_id)
            .await;
        if let Some(main_thread_id) = disconnected_controller_main_thread_id {
            self.thread_processor
                .unsubscribe_connection_from_thread(main_thread_id, connection_id)
                .await;
        }
        if timeout(
            CONNECTION_RPC_DRAIN_TIMEOUT,
            session_state.rpc_gate.shutdown(),
        )
        .await
        .is_err()
        {
            tracing::warn!(
                ?connection_id,
                timeout_seconds = CONNECTION_RPC_DRAIN_TIMEOUT.as_secs(),
                "timed out waiting for connection RPCs to drain"
            );
        }
        self.outgoing.connection_closed(connection_id).await;
        self.fs_processor.connection_closed(connection_id).await;
        self.command_exec_processor
            .connection_closed(connection_id)
            .await;
        self.process_exec_processor
            .connection_closed(connection_id)
            .await;
        self.thread_processor.connection_closed(connection_id).await;
    }

    pub(crate) fn subscribe_running_assistant_turn_count(&self) -> watch::Receiver<usize> {
        self.thread_processor
            .subscribe_running_assistant_turn_count()
    }

    /// Handle a standalone JSON-RPC response originating from the peer.
    pub(crate) async fn process_response(
        &self,
        connection_id: ConnectionId,
        connection_origin: ConnectionOrigin,
        response: JSONRPCResponse,
    ) {
        tracing::info!("<- response: {:?}", response);
        let JSONRPCResponse { id, result, .. } = response;
        if self
            .authorize_server_request_response(connection_id, connection_origin, &id, &result)
            .await
        {
            self.outgoing
                .notify_client_response_from_connection(connection_id, id, result)
                .await;
        }
    }

    /// Handle an error object received from the peer.
    pub(crate) async fn process_error(
        &self,
        connection_id: ConnectionId,
        connection_origin: ConnectionOrigin,
        err: JSONRPCError,
    ) {
        tracing::error!("<- error: {:?}", err);
        if self
            .authorize_server_request_error(connection_id, connection_origin, &err.id)
            .await
        {
            self.outgoing
                .notify_client_error_from_connection(connection_id, err.id, err.error)
                .await;
        }
    }

    async fn authorize_server_request_response(
        &self,
        connection_id: ConnectionId,
        connection_origin: ConnectionOrigin,
        request_id: &RequestId,
        result: &codex_app_server_protocol::Result,
    ) -> bool {
        if !matches!(connection_origin, ConnectionOrigin::ExternalController) {
            return true;
        }

        let Some(pending_request) = self.outgoing.pending_server_request(request_id).await else {
            return true;
        };
        let response = match pending_request.request.response_from_result(result.clone()) {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(
                    ?connection_id,
                    ?request_id,
                    "dropping invalid external-controller server-request response: {err}"
                );
                return false;
            }
        };
        if let Err(err) = self
            .controller_processor
            .authorize_server_response(
                connection_id,
                pending_request.thread_id,
                pending_request.external_controller_owner_epoch,
                &response,
            )
            .await
        {
            tracing::warn!(
                ?connection_id,
                ?request_id,
                error = ?err,
                "dropping unauthorized external-controller server-request response"
            );
            return false;
        }

        true
    }

    async fn authorize_server_request_error(
        &self,
        connection_id: ConnectionId,
        connection_origin: ConnectionOrigin,
        request_id: &RequestId,
    ) -> bool {
        if !matches!(connection_origin, ConnectionOrigin::ExternalController) {
            return true;
        }

        let Some(pending_request) = self.outgoing.pending_server_request(request_id).await else {
            return true;
        };
        if let Err(err) = self
            .controller_processor
            .authorize_server_request_error(
                connection_id,
                pending_request.thread_id,
                pending_request.external_controller_owner_epoch,
                &pending_request.request,
            )
            .await
        {
            tracing::warn!(
                ?connection_id,
                ?request_id,
                error = ?err,
                "dropping unauthorized external-controller server-request error"
            );
            return false;
        }

        true
    }

    async fn handle_client_request(
        self: &Arc<Self>,
        connection_request_id: ConnectionRequestId,
        connection_origin: ConnectionOrigin,
        codex_request: ClientRequest,
        session: Arc<ConnectionSessionState>,
        // `Some(...)` means the caller wants initialize to immediately mark the
        // connection outbound-ready. Websocket JSON-RPC calls pass `None` so
        // lib.rs can deliver connection-scoped initialize notifications first.
        outbound_initialized: Option<&AtomicBool>,
        request_context: RequestContext,
    ) -> Result<(), JSONRPCErrorError> {
        if matches!(connection_origin, ConnectionOrigin::ExternalController)
            && session.controller_transport_closing()
        {
            return Err(controller_transport_closing());
        }

        let connection_id = connection_request_id.connection_id;
        if let ClientRequest::Initialize { request_id, params } = codex_request {
            let connection_initialized = self
                .initialize_processor
                .initialize(
                    connection_id,
                    request_id,
                    params,
                    &session,
                    outbound_initialized,
                )
                .await?;
            if connection_initialized {
                self.thread_processor
                    .connection_initialized(
                        connection_id,
                        ConnectionCapabilities {
                            request_attestation: session.request_attestation(),
                        },
                    )
                    .await;
            }
            return Ok(());
        }

        self.dispatch_initialized_client_request(
            connection_request_id,
            connection_origin,
            codex_request,
            session,
            request_context,
        )
        .await
    }

    async fn dispatch_initialized_client_request(
        self: &Arc<Self>,
        connection_request_id: ConnectionRequestId,
        connection_origin: ConnectionOrigin,
        codex_request: ClientRequest,
        session: Arc<ConnectionSessionState>,
        request_context: RequestContext,
    ) -> Result<(), JSONRPCErrorError> {
        if !session.initialized() {
            return Err(invalid_request("Not initialized"));
        }

        if let Some(reason) = codex_request.experimental_reason()
            && !session.experimental_api_enabled()
        {
            return Err(invalid_request(experimental_required_message(reason)));
        }
        let admission_rule =
            admit_initialized_client_request(connection_origin, codex_request.method_name())?;
        let connection_id = connection_request_id.connection_id;
        let serialization_scope = codex_request.serialization_scope();
        let primary_reclaim_thread_id = primary_input_reclaim_thread_id(
            connection_origin,
            admission_rule,
            serialization_scope.as_ref(),
        )
        .map(str::to_string);
        if let Some(thread_id) = primary_reclaim_thread_id.as_deref() {
            self.controller_processor
                .reclaim_for_primary_thread_input(thread_id)
                .await?;
        }
        self.initialize_processor.track_initialized_request(
            connection_id,
            connection_request_id.request_id.clone(),
            &codex_request,
        );

        let app_server_client_name = session.app_server_client_name().map(str::to_string);
        let client_version = session.client_version().map(str::to_string);
        let client_mcp_extensions = session.client_mcp_extensions();
        let error_request_id = connection_request_id.clone();
        let dequeue_reclaim_thread_id = primary_reclaim_thread_id;
        let rpc_gate = Arc::clone(&session.rpc_gate);
        let rpc_reservation = if matches!(connection_origin, ConnectionOrigin::ExternalController) {
            Some(
                session
                    .rpc_gate
                    .try_reserve_external_controller_request()
                    .ok_or_else(controller_overloaded)?,
            )
        } else {
            None
        };
        let session_for_request = Arc::clone(&session);
        let processor = Arc::clone(self);
        let span = request_context.span();
        let future = async move {
            let processor_for_request = Arc::clone(&processor);
            let result = async {
                if let Some(thread_id) = dequeue_reclaim_thread_id.as_deref() {
                    processor_for_request
                        .controller_processor
                        .reclaim_for_primary_thread_input(thread_id)
                        .await?;
                }
                processor_for_request
                    .handle_initialized_client_request(InitializedClientRequest {
                        connection_request_id,
                        connection_origin,
                        admission_rule,
                        codex_request,
                        session: session_for_request,
                        request_context,
                        app_server_client_name,
                        client_version,
                        client_mcp_extensions,
                    })
                    .await
            }
            .await;
            if let Err(error) = result {
                processor.outgoing.send_error(error_request_id, error).await;
            }
        }
        .instrument(span);
        let request = if let Some(reservation) = rpc_reservation {
            QueuedInitializedRequest::new_with_reservation(rpc_gate, reservation, future)
        } else {
            QueuedInitializedRequest::new(rpc_gate, future)
        };

        if let Some(scope) = serialization_scope {
            let (key, access) = RequestSerializationQueueKey::from_scope(connection_id, scope);
            let priority = match connection_origin {
                ConnectionOrigin::ExternalController => {
                    RequestSerializationPriority::ExternalController
                }
                ConnectionOrigin::Stdio
                | ConnectionOrigin::InProcess
                | ConnectionOrigin::WebSocket
                | ConnectionOrigin::RemoteControl => RequestSerializationPriority::Primary,
            };
            self.request_serialization_queues
                .enqueue_with_priority(key, access, priority, request)
                .await;
        } else {
            tokio::spawn(async move {
                request.run().await;
            });
        }
        Ok(())
    }

    async fn handle_initialized_client_request(
        self: Arc<Self>,
        initialized_request: InitializedClientRequest,
    ) -> Result<(), JSONRPCErrorError> {
        let InitializedClientRequest {
            connection_request_id,
            connection_origin,
            admission_rule,
            mut codex_request,
            session,
            request_context,
            app_server_client_name,
            client_version,
            client_mcp_extensions,
        } = initialized_request;
        let connection_id = connection_request_id.connection_id;
        let request_id = ConnectionRequestId {
            connection_id,
            request_id: codex_request.id().clone(),
        };
        let controller_authorization =
            if matches!(connection_origin, ConnectionOrigin::ExternalController)
                && !codex_request.method_name().starts_with("controller/")
            {
                let target = controller_request_target(&codex_request, admission_rule)?;
                let authorization = match self
                    .controller_processor
                    .authorize_normal_request(connection_id, admission_rule, target)
                    .await
                {
                    Ok(authorization) => authorization,
                    Err(error) => {
                        self.unsubscribe_controller_if_session_missing(connection_id)
                            .await;
                        return Err(error);
                    }
                };
                reject_controller_tui_only_params(&codex_request)?;
                match &codex_request {
                    ClientRequest::ThreadArchive { params, .. } => {
                        self.reject_controller_descendant_thread_targets(
                            "thread/archive",
                            &params.thread_id,
                        )
                        .await?;
                    }
                    ClientRequest::ThreadDelete { params, .. } => {
                        self.reject_controller_descendant_thread_targets(
                            "thread/delete",
                            &params.thread_id,
                        )
                        .await?;
                    }
                    _ => {}
                }
                unbind_controller_request_cursors(
                    &mut codex_request,
                    connection_id,
                    &authorization,
                )?;
                Some(authorization)
            } else {
                None
            };
        let disconnect_after_response =
            matches!(codex_request, ClientRequest::ControllerSignOff { .. });

        let result: Result<Option<ClientResponsePayload>, JSONRPCErrorError> = match codex_request {
            ClientRequest::Initialize { .. } => {
                panic!("Initialize should be handled before initialized request dispatch");
            }
            ClientRequest::ServerDiagnostics { .. } => Ok(Some(read_server_diagnostics().into())),
            ClientRequest::ConfigRead { params, .. } => self
                .config_processor
                .read(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::WindowsSandboxReadiness { .. } => self
                .windows_sandbox_processor
                .windows_sandbox_readiness()
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ExternalAgentConfigDetect { params, .. } => self
                .external_agent_config_processor
                .detect(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ExternalAgentConfigImport { params, .. } => self
                .external_agent_config_processor
                .import(request_id.clone(), params)
                .await
                .map(|()| None),
            ClientRequest::ExternalAgentConfigImportHistoryRecord { params, .. } => self
                .external_agent_config_processor
                .record_import_history(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ExternalAgentConfigImportHistoriesRead { .. } => self
                .external_agent_config_processor
                .read_import_histories()
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ConfigValueWrite { params, .. } => {
                self.config_processor.value_write(params).await.map(Some)
            }
            ClientRequest::ConfigBatchWrite { params, .. } => {
                self.config_processor.batch_write(params).await.map(Some)
            }
            ClientRequest::ExperimentalFeatureEnablementSet { params, .. } => {
                self.config_processor
                    .experimental_feature_enablement_set(request_id.clone(), params)
                    .await
            }
            ClientRequest::RemoteControlEnable { params, .. } => self
                .remote_control_processor
                .enable(
                    params.is_some_and(|params| params.ephemeral),
                    app_server_client_name.as_deref(),
                )
                .await
                .map(|response| Some(response.into())),
            ClientRequest::RemoteControlDisable { params, .. } => self
                .remote_control_processor
                .disable(
                    params.is_some_and(|params| params.ephemeral),
                    app_server_client_name.as_deref(),
                )
                .await
                .map(|response| Some(response.into())),
            ClientRequest::RemoteControlStatusRead { .. } => self
                .remote_control_processor
                .status_read()
                .map(|response| Some(response.into())),
            ClientRequest::RemoteControlPairingStart { params, .. } => self
                .remote_control_processor
                .pairing_start(params, app_server_client_name.as_deref())
                .await
                .map(|response| Some(response.into())),
            ClientRequest::RemoteControlPairingStatus { params, .. } => self
                .remote_control_processor
                .pairing_status(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::RemoteControlClientsList { params, .. } => self
                .remote_control_processor
                .clients_list(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::RemoteControlClientsRevoke { params, .. } => self
                .remote_control_processor
                .clients_revoke(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ControllerRequestParticipation { params, .. } => self
                .controller_processor
                .request_participation(
                    connection_id,
                    connection_origin,
                    session.controller_credential_proof(),
                    params,
                )
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ControllerAcquireControl { .. } => self
                .unsubscribe_controller_on_missing_session_error(
                    connection_id,
                    self.controller_processor
                        .acquire_control(connection_id, connection_origin)
                        .await,
                )
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ControllerReleaseControl { .. } => self
                .unsubscribe_controller_on_missing_session_error(
                    connection_id,
                    self.controller_processor
                        .release_control(connection_id, connection_origin)
                        .await,
                )
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ControllerSignOff { .. } => {
                if !matches!(connection_origin, ConnectionOrigin::ExternalController) {
                    self.controller_processor
                        .sign_off(connection_id, connection_origin)
                        .await
                        .map(|response| Some(response.into()))
                } else {
                    let authorization = self
                        .controller_processor
                        .authorize_normal_request(
                            connection_id,
                            admission_rule,
                            ControllerRequestTarget::None,
                        )
                        .await?;
                    self.thread_processor
                        .thread_unsubscribe(
                            &request_id,
                            codex_app_server_protocol::ThreadUnsubscribeParams {
                                thread_id: authorization.main_thread_id.clone(),
                            },
                        )
                        .await?;
                    let response = self
                        .controller_processor
                        .sign_off(connection_id, connection_origin)
                        .await?;
                    self.thread_processor
                        .thread_unsubscribe(
                            &request_id,
                            codex_app_server_protocol::ThreadUnsubscribeParams {
                                thread_id: authorization.main_thread_id,
                            },
                        )
                        .await?;
                    Ok(Some(response.into()))
                }
            }
            ClientRequest::ConfigRequirementsRead { .. } => self
                .config_processor
                .config_requirements_read()
                .await
                .map(|response| Some(response.into())),
            ClientRequest::EnvironmentAdd { params, .. } => {
                self.environment_processor.environment_add(params).await
            }
            ClientRequest::EnvironmentInfo { params, .. } => {
                self.environment_processor.environment_info(params).await
            }
            ClientRequest::EnvironmentStatus { params, .. } => {
                self.environment_processor.environment_status(params).await
            }
            ClientRequest::FsReadFile { params, .. } => self
                .fs_processor
                .read_file(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FsWriteFile { params, .. } => self
                .fs_processor
                .write_file(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FsCreateDirectory { params, .. } => self
                .fs_processor
                .create_directory(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FsGetMetadata { params, .. } => self
                .fs_processor
                .get_metadata(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FsReadDirectory { params, .. } => self
                .fs_processor
                .read_directory(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FsRemove { params, .. } => self
                .fs_processor
                .remove(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FsCopy { params, .. } => self
                .fs_processor
                .copy(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FsWatch { params, .. } => self
                .fs_processor
                .watch(connection_id, params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FsUnwatch { params, .. } => self
                .fs_processor
                .unwatch(connection_id, params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ModelProviderCapabilitiesRead { .. } => self
                .config_processor
                .model_provider_capabilities_read()
                .await
                .map(|response| Some(response.into())),
            ClientRequest::ThreadStart { params, .. } => {
                self.thread_processor
                    .thread_start(
                        request_id.clone(),
                        params,
                        app_server_client_name.clone(),
                        client_version.clone(),
                        client_mcp_extensions.clone(),
                        request_context,
                    )
                    .await
            }
            ClientRequest::ThreadUnsubscribe { params, .. } => {
                self.thread_processor
                    .thread_unsubscribe(&request_id, params)
                    .await
            }
            ClientRequest::ThreadResume { params, .. } => {
                self.thread_processor
                    .thread_resume(
                        request_id.clone(),
                        params,
                        controller_authorization
                            .as_ref()
                            .map(|authorization| authorization.main_thread_id.clone()),
                        app_server_client_name.clone(),
                        client_version.clone(),
                        client_mcp_extensions.clone(),
                    )
                    .await
            }
            ClientRequest::ThreadFork { params, .. } => {
                self.thread_processor
                    .thread_fork(
                        request_id.clone(),
                        params,
                        app_server_client_name.clone(),
                        client_version.clone(),
                        client_mcp_extensions.clone(),
                    )
                    .await
            }
            ClientRequest::ThreadArchive { params, .. } => {
                self.thread_processor
                    .thread_archive(request_id.clone(), params)
                    .await
            }
            ClientRequest::ThreadDelete { params, .. } => {
                self.thread_processor
                    .thread_delete(request_id.clone(), params)
                    .await
            }
            ClientRequest::ThreadIncrementElicitation { params, .. } => {
                self.thread_processor
                    .thread_increment_elicitation(params)
                    .await
            }
            ClientRequest::ThreadDecrementElicitation { params, .. } => {
                self.thread_processor
                    .thread_decrement_elicitation(params)
                    .await
            }
            ClientRequest::ThreadSetName { params, .. } => {
                self.thread_processor
                    .thread_set_name(request_id.clone(), params)
                    .await
            }
            ClientRequest::ThreadGoalSet { params, .. } => {
                self.thread_goal_processor
                    .thread_goal_set(request_id.clone(), params)
                    .await
            }
            ClientRequest::ThreadGoalGet { params, .. } => {
                self.thread_goal_processor.thread_goal_get(params).await
            }
            ClientRequest::ThreadGoalClear { params, .. } => {
                self.thread_goal_processor
                    .thread_goal_clear(request_id.clone(), params)
                    .await
            }
            ClientRequest::ThreadMetadataUpdate { params, .. } => {
                self.thread_processor.thread_metadata_update(params).await
            }
            ClientRequest::ThreadSectionMove { params, .. } => {
                self.thread_processor.thread_section_move(params).await
            }
            ClientRequest::ThreadSectionList { params, .. } => {
                self.thread_processor
                    .thread_section_list(params, thread_collection_scope(&controller_authorization))
                    .await
            }
            ClientRequest::ThreadSectionCreate { params, .. } => {
                self.thread_processor.thread_section_create(params).await
            }
            ClientRequest::ThreadSectionUpdate { params, .. } => {
                self.thread_processor.thread_section_update(params).await
            }
            ClientRequest::ThreadSectionDelete { params, .. } => {
                self.thread_processor.thread_section_delete(params).await
            }
            ClientRequest::ThreadSettingsUpdate { params, .. } => {
                self.turn_processor
                    .thread_settings_update(&request_id, params)
                    .await
            }
            ClientRequest::ThreadMemoryModeSet { params, .. } => {
                self.thread_processor.thread_memory_mode_set(params).await
            }
            ClientRequest::MemoryReset { .. } => self.thread_processor.memory_reset().await,
            ClientRequest::ThreadUnarchive { params, .. } => {
                self.thread_processor
                    .thread_unarchive(request_id.clone(), params)
                    .await
            }
            ClientRequest::ThreadCompactStart { params, .. } => {
                self.thread_processor
                    .thread_compact_start(&request_id, params)
                    .await
            }
            ClientRequest::ThreadBackgroundTerminalsClean { params, .. } => {
                self.thread_processor
                    .thread_background_terminals_clean(&request_id, params)
                    .await
            }
            ClientRequest::ThreadBackgroundTerminalsList { params, .. } => {
                self.thread_processor
                    .thread_background_terminals_list(params)
                    .await
            }
            ClientRequest::ThreadBackgroundTerminalsTerminate { params, .. } => {
                self.thread_processor
                    .thread_background_terminals_terminate(params)
                    .await
            }
            ClientRequest::ThreadRollback { params, .. } => {
                self.thread_processor
                    .thread_rollback(&request_id, params, app_server_client_name.as_deref())
                    .await
            }
            ClientRequest::ThreadList { params, .. } => {
                self.thread_processor
                    .thread_list(params, thread_collection_scope(&controller_authorization))
                    .await
            }
            ClientRequest::ThreadSearch { params, .. } => {
                self.thread_processor
                    .thread_search(params, thread_collection_scope(&controller_authorization))
                    .await
            }
            ClientRequest::ThreadSearchOccurrences { params, .. } => {
                self.thread_processor
                    .thread_search_occurrences(params)
                    .await
            }
            ClientRequest::ThreadLoadedList { params, .. } => {
                self.thread_processor
                    .thread_loaded_list(params, thread_collection_scope(&controller_authorization))
                    .await
            }
            ClientRequest::ThreadRead { params, .. } => {
                self.thread_processor.thread_read(params).await
            }
            ClientRequest::ThreadTurnsList { params, .. } => {
                self.thread_processor.thread_turns_list(params).await
            }
            ClientRequest::ThreadItemsList { params, .. } => {
                self.thread_processor.thread_items_list(params).await
            }
            ClientRequest::ThreadShellCommand { params, .. } => {
                self.thread_processor
                    .thread_shell_command(&request_id, params)
                    .await
            }
            ClientRequest::ThreadApproveGuardianDeniedAction { params, .. } => {
                self.thread_processor
                    .thread_approve_guardian_denied_action(&request_id, params)
                    .await
            }
            ClientRequest::GetConversationSummary { params, .. } => {
                self.thread_processor.conversation_summary(params).await
            }
            ClientRequest::SkillsList { params, .. } => {
                self.catalog_processor.skills_list(params).await
            }
            ClientRequest::SkillsExtraRootsSet { params, .. } => {
                self.catalog_processor.skills_extra_roots_set(params).await
            }
            ClientRequest::HooksList { params, .. } => {
                self.catalog_processor.hooks_list(params).await
            }
            ClientRequest::MarketplaceAdd { params, .. } => {
                self.marketplace_processor.marketplace_add(params).await
            }
            ClientRequest::MarketplaceRemove { params, .. } => {
                self.marketplace_processor.marketplace_remove(params).await
            }
            ClientRequest::MarketplaceUpgrade { params, .. } => {
                self.marketplace_processor.marketplace_upgrade(params).await
            }
            ClientRequest::PluginList { params, .. } => {
                self.plugin_processor.plugin_list(params).await
            }
            ClientRequest::PluginSearch { params, .. } => {
                self.plugin_processor.plugin_search(params).await
            }
            ClientRequest::PluginInstalled { params, .. } => {
                self.plugin_processor.plugin_installed(params).await
            }
            ClientRequest::PluginRead { params, .. } => {
                self.plugin_processor.plugin_read(params).await
            }
            ClientRequest::PluginSkillRead { params, .. } => {
                self.plugin_processor.plugin_skill_read(params).await
            }
            ClientRequest::PluginShareSave { params, .. } => {
                self.plugin_processor.plugin_share_save(params).await
            }
            ClientRequest::PluginShareUpdateTargets { params, .. } => {
                self.plugin_processor
                    .plugin_share_update_targets(params)
                    .await
            }
            ClientRequest::PluginShareList { params, .. } => {
                self.plugin_processor.plugin_share_list(params).await
            }
            ClientRequest::PluginShareCheckout { params, .. } => {
                self.plugin_processor.plugin_share_checkout(params).await
            }
            ClientRequest::PluginShareDelete { params, .. } => {
                self.plugin_processor.plugin_share_delete(params).await
            }
            ClientRequest::AppsRead { params, .. } => self.apps_processor.apps_read(params).await,
            ClientRequest::AppsList { params, .. } => {
                self.apps_processor.apps_list(&request_id, params).await
            }
            ClientRequest::AppsInstalled { params, .. } => self
                .apps_processor
                .apps_installed(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::SkillsConfigWrite { params, .. } => {
                self.catalog_processor.skills_config_write(params).await
            }
            ClientRequest::PluginInstall { params, .. } => {
                self.plugin_processor.plugin_install(params).await
            }
            ClientRequest::PluginUninstall { params, .. } => {
                self.plugin_processor.plugin_uninstall(params).await
            }
            ClientRequest::ModelList { params, .. } => {
                self.catalog_processor.model_list(params).await
            }
            ClientRequest::ExperimentalFeatureList { params, .. } => {
                self.catalog_processor
                    .experimental_feature_list(params)
                    .await
            }
            ClientRequest::PermissionProfileList { params, .. } => {
                self.catalog_processor.permission_profile_list(params).await
            }
            ClientRequest::CollaborationModeList { params, .. } => {
                self.catalog_processor.collaboration_mode_list(params).await
            }
            ClientRequest::MockExperimentalMethod { params, .. } => {
                self.catalog_processor
                    .mock_experimental_method(params)
                    .await
            }
            ClientRequest::TurnStart { params, .. } => {
                self.turn_processor
                    .turn_start(
                        request_id.clone(),
                        params,
                        app_server_client_name.clone(),
                        client_version.clone(),
                    )
                    .await
            }
            ClientRequest::ThreadInjectItems { params, .. } => {
                self.turn_processor.thread_inject_items(params).await
            }
            ClientRequest::TurnSteer { params, .. } => {
                self.turn_processor.turn_steer(&request_id, params).await
            }
            ClientRequest::TurnInterrupt { params, .. } => {
                self.turn_processor
                    .turn_interrupt(&request_id, params)
                    .await
            }
            ClientRequest::ThreadRealtimeStart { params, .. } => {
                self.turn_processor
                    .thread_realtime_start(&request_id, params)
                    .await
            }
            ClientRequest::ThreadRealtimeAppendAudio { params, .. } => {
                self.turn_processor
                    .thread_realtime_append_audio(&request_id, params)
                    .await
            }
            ClientRequest::ThreadRealtimeAppendText { params, .. } => {
                self.turn_processor
                    .thread_realtime_append_text(&request_id, params)
                    .await
            }
            ClientRequest::ThreadRealtimeAppendSpeech { params, .. } => {
                self.turn_processor
                    .thread_realtime_append_speech(&request_id, params)
                    .await
            }
            ClientRequest::ThreadRealtimeStop { params, .. } => {
                self.turn_processor
                    .thread_realtime_stop(&request_id, params)
                    .await
            }
            ClientRequest::ThreadRealtimeListVoices { .. } => {
                self.turn_processor.thread_realtime_list_voices().await
            }
            ClientRequest::ReviewStart { params, .. } => {
                self.turn_processor.review_start(&request_id, params).await
            }
            ClientRequest::McpServerOauthLogin { params, .. } => {
                self.mcp_processor.mcp_server_oauth_login(params).await
            }
            ClientRequest::McpServerRefresh { params, .. } => {
                self.mcp_processor.mcp_server_refresh(params).await
            }
            ClientRequest::McpServerStatusList { params, .. } => {
                self.mcp_processor
                    .mcp_server_status_list(&request_id, params)
                    .await
            }
            ClientRequest::McpResourceRead { params, .. } => {
                self.mcp_processor
                    .mcp_resource_read(&request_id, params)
                    .await
            }
            ClientRequest::McpServerToolCall { params, .. } => {
                self.mcp_processor
                    .mcp_server_tool_call(&request_id, params)
                    .await
            }
            ClientRequest::WindowsSandboxSetupStart { params, .. } => {
                self.windows_sandbox_processor
                    .windows_sandbox_setup_start(&request_id, params)
                    .await
            }
            ClientRequest::LoginAccount { params, .. } => {
                self.account_processor
                    .login_account(request_id.clone(), params)
                    .await
            }
            ClientRequest::LogoutAccount { .. } => {
                self.account_processor
                    .logout_account(request_id.clone())
                    .await
            }
            ClientRequest::CancelLoginAccount { params, .. } => {
                self.account_processor.cancel_login_account(params).await
            }
            ClientRequest::GetAccount { params, .. } => {
                self.account_processor.get_account(params).await
            }
            ClientRequest::GetAuthStatus { params, .. } => {
                self.account_processor.get_auth_status(params).await
            }
            ClientRequest::GetAccountRateLimits { .. } => {
                self.account_processor.get_account_rate_limits().await
            }
            ClientRequest::ConsumeAccountRateLimitResetCredit { params, .. } => {
                self.account_processor
                    .consume_account_rate_limit_reset_credit(params)
                    .await
            }
            ClientRequest::GetAccountTokenUsage { params, .. } => {
                self.account_processor.get_account_token_usage(params).await
            }
            ClientRequest::GetWorkspaceMessages { .. } => {
                self.account_processor.get_workspace_messages().await
            }
            ClientRequest::SendAddCreditsNudgeEmail { params, .. } => {
                self.account_processor
                    .send_add_credits_nudge_email(params)
                    .await
            }
            ClientRequest::GitDiffToRemote { params, .. } => {
                self.git_processor.git_diff_to_remote(params).await
            }
            ClientRequest::FuzzyFileSearch { params, .. } => self
                .search_processor
                .fuzzy_file_search(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FuzzyFileSearchSessionStart { params, .. } => self
                .search_processor
                .fuzzy_file_search_session_start_response(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FuzzyFileSearchSessionUpdate { params, .. } => self
                .search_processor
                .fuzzy_file_search_session_update_response(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::FuzzyFileSearchSessionStop { params, .. } => self
                .search_processor
                .fuzzy_file_search_session_stop(params)
                .await
                .map(|response| Some(response.into())),
            ClientRequest::OneOffCommandExec { params, .. } => {
                self.command_exec_processor
                    .one_off_command_exec(&request_id, params)
                    .await
            }
            ClientRequest::CommandExecWrite { params, .. } => {
                self.command_exec_processor
                    .command_exec_write(request_id.clone(), params)
                    .await
            }
            ClientRequest::CommandExecResize { params, .. } => {
                self.command_exec_processor
                    .command_exec_resize(request_id.clone(), params)
                    .await
            }
            ClientRequest::CommandExecTerminate { params, .. } => {
                self.command_exec_processor
                    .command_exec_terminate(request_id.clone(), params)
                    .await
            }
            ClientRequest::ProcessSpawn { params, .. } => self
                .process_exec_processor
                .process_spawn(request_id.clone(), params)
                .await
                .map(|()| None),
            ClientRequest::ProcessWriteStdin { params, .. } => {
                self.process_exec_processor
                    .process_write_stdin(request_id.clone(), params)
                    .await
            }
            ClientRequest::ProcessKill { params, .. } => {
                self.process_exec_processor
                    .process_kill(request_id.clone(), params)
                    .await
            }
            ClientRequest::ProcessResizePty { params, .. } => {
                self.process_exec_processor
                    .process_resize_pty(request_id.clone(), params)
                    .await
            }
            ClientRequest::FeedbackUpload { params, .. } => {
                self.feedback_processor.feedback_upload(params).await
            }
        };

        let result = result.and_then(|response| {
            let Some(mut response) = response else {
                return Ok(None);
            };
            if let Some(authorization) = controller_authorization.as_ref() {
                bind_controller_response_cursors(&mut response, connection_id, authorization)?;
            }
            Ok(Some(response))
        });

        match result {
            Ok(Some(response)) => {
                if disconnect_after_response {
                    session.begin_controller_transport_closing();
                    self.outgoing
                        .send_response_as_then_disconnect(request_id.clone(), response)
                        .await;
                } else {
                    self.outgoing
                        .send_response_as(request_id.clone(), response)
                        .await;
                }
            }
            Ok(None) => {}
            Err(error) => {
                self.outgoing.send_error(request_id.clone(), error).await;
            }
        }
        Ok(())
    }
}

impl MessageProcessor {
    async fn reject_controller_descendant_thread_targets(
        &self,
        method: &str,
        thread_id: &str,
    ) -> Result<(), JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        let subtree_thread_ids = self
            .thread_processor
            .state_db_spawn_subtree_thread_ids(thread_id)
            .await?;
        if subtree_thread_ids.len() <= 1 {
            return Ok(());
        }

        Err(controller_not_allowed(&format!(
            "external controller {method} may not target spawned descendant threads"
        )))
    }

    async fn unsubscribe_controller_if_session_missing(&self, connection_id: ConnectionId) {
        if let Some(main_thread_id) = self
            .controller_processor
            .main_thread_for_missing_session(connection_id)
        {
            self.thread_processor
                .unsubscribe_connection_from_thread(main_thread_id, connection_id)
                .await;
        }
    }

    async fn unsubscribe_controller_on_missing_session_error<T>(
        &self,
        connection_id: ConnectionId,
        result: Result<T, JSONRPCErrorError>,
    ) -> Result<T, JSONRPCErrorError> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                self.unsubscribe_controller_if_session_missing(connection_id)
                    .await;
                Err(error)
            }
        }
    }
}

fn controller_request_target(
    request: &ClientRequest,
    rule: AdmissionRule,
) -> Result<ControllerRequestTarget, JSONRPCErrorError> {
    if matches!(rule.required_authority, RequiredAuthority::TuiOnly) {
        return Ok(ControllerRequestTarget::None);
    }

    match rule.target {
        TargetExtraction::None | TargetExtraction::MainThreadOnly => {
            Ok(ControllerRequestTarget::None)
        }
        TargetExtraction::CollectionFiltered => match request {
            ClientRequest::ThreadList { .. }
            | ClientRequest::ThreadSectionList { .. }
            | ClientRequest::ThreadSearch { .. }
            | ClientRequest::ThreadLoadedList { .. } => {
                Ok(ControllerRequestTarget::CollectionFiltered)
            }
            _ => Err(controller_not_allowed(
                "external controller collection filtering is not enabled for this method",
            )),
        },
        TargetExtraction::ExactThread => {
            if let Some(ClientRequestSerializationScope::Thread { thread_id }) =
                request.serialization_scope()
            {
                return Ok(ControllerRequestTarget::ExactThread(thread_id));
            }

            match request {
                ClientRequest::ThreadResume { params, .. } => Ok(
                    ControllerRequestTarget::ExactThread(params.thread_id.clone()),
                ),
                ClientRequest::ThreadTurnsList { params, .. } => Ok(
                    ControllerRequestTarget::ExactThread(params.thread_id.clone()),
                ),
                ClientRequest::ThreadItemsList { params, .. } => Ok(
                    ControllerRequestTarget::ExactThread(params.thread_id.clone()),
                ),
                ClientRequest::ThreadSearchOccurrences { params, .. } => Ok(
                    ControllerRequestTarget::ExactThread(params.thread_id.clone()),
                ),
                ClientRequest::McpResourceRead { params, .. } => params
                    .thread_id
                    .clone()
                    .map(ControllerRequestTarget::ExactThread)
                    .ok_or_else(|| {
                        controller_not_allowed(
                            "external controller exact thread target is required for this method",
                        )
                    }),
                _ => Err(controller_not_allowed(
                    "external controller target extraction is not enabled for this method",
                )),
            }
        }
    }
}

fn thread_collection_scope(
    controller_authorization: &Option<ControllerNormalAuthorization>,
) -> ThreadListScope {
    controller_authorization
        .as_ref()
        .filter(|authorization| authorization.filter_collection_to_main_thread)
        .map(|authorization| ThreadListScope::SingleThread(authorization.main_thread_id.clone()))
        .unwrap_or(ThreadListScope::All)
}

fn reject_controller_tui_only_params(request: &ClientRequest) -> Result<(), JSONRPCErrorError> {
    match request {
        ClientRequest::ThreadResume { params, .. } => {
            if params.history.is_none()
                && params.path.is_none()
                && params.model.is_none()
                && params.model_provider.is_none()
                && params.service_tier.is_none()
                && params.cwd.is_none()
                && params.runtime_workspace_roots.is_none()
                && params.approval_policy.is_none()
                && params.approvals_reviewer.is_none()
                && params.sandbox.is_none()
                && params.permissions.is_none()
                && params.config.is_none()
                && params.base_instructions.is_none()
                && params.developer_instructions.is_none()
                && params.personality.is_none()
            {
                return Ok(());
            }

            Err(controller_not_allowed(
                "external controller thread/resume may only rejoin the authorized main thread without history, path, or configuration overrides",
            ))
        }
        ClientRequest::TurnStart { params, .. } => {
            if params.additional_context.is_none()
                && params.environments.is_none()
                && params.cwd.is_none()
                && params.runtime_workspace_roots.is_none()
                && params.approval_policy.is_none()
                && params.approvals_reviewer.is_none()
                && params.sandbox_policy.is_none()
                && params.permissions.is_none()
                && params.model.is_none()
                && params.service_tier.is_none()
                && params.effort.is_none()
                && params.summary.is_none()
                && params.personality.is_none()
                && params.output_schema.is_none()
                && params.collaboration_mode.is_none()
                && params.multi_agent_mode.is_none()
            {
                return Ok(());
            }

            Err(controller_not_allowed(
                "external controller turn/start may only submit input for the authorized main thread without additional context or configuration overrides",
            ))
        }
        ClientRequest::TurnSteer { params, .. } => {
            if params.additional_context.is_none() {
                return Ok(());
            }

            Err(controller_not_allowed(
                "external controller turn/steer may only steer the authorized main thread without additional context overrides",
            ))
        }
        ClientRequest::ThreadSectionMove { params, .. } => {
            if params.before_thread_id.is_none() {
                return Ok(());
            }

            Err(controller_not_allowed(
                "external controller thread/section/move may not order the authorized main thread relative to another thread",
            ))
        }
        ClientRequest::ThreadRealtimeStart { params, .. } => {
            if params.client_managed_handoffs.is_none()
                && params.delegation_ack_filler.is_none()
                && params.flush_transcript_tail_on_session_end.is_none()
                && params.codex_responses_as_items.is_none()
                && params.codex_response_item_prefix.is_none()
                && params.codex_response_handoff_mode.is_none()
                && params.codex_response_handoff_channel_prefixes.is_none()
                && params.model.is_none()
                && params.include_startup_context.is_none()
                && params.initial_items.is_none()
                && params.realtime_start_instructions.is_none()
                && params.realtime_end_instructions.is_none()
                && params.prompt.is_none()
                && params.version.is_none()
            {
                return Ok(());
            }

            Err(controller_not_allowed(
                "external controller thread/realtime/start may only start realtime input for the authorized main thread without context or configuration overrides",
            ))
        }
        ClientRequest::ThreadRealtimeAppendText { params, .. } => {
            if matches!(
                params.role,
                codex_protocol::protocol::ConversationTextRole::User
            ) {
                return Ok(());
            }

            Err(controller_not_allowed(
                "external controller thread/realtime/appendText may only append user-role realtime text to the authorized main thread",
            ))
        }
        ClientRequest::ReviewStart { params, .. } => {
            if !matches!(
                params.delivery.as_ref(),
                Some(codex_app_server_protocol::ReviewDelivery::Detached)
            ) {
                return Ok(());
            }

            Err(controller_not_allowed(
                "external controller review/start may only run inline on the authorized main thread",
            ))
        }
        _ => Ok(()),
    }
}

fn primary_input_reclaim_thread_id(
    origin: ConnectionOrigin,
    rule: AdmissionRule,
    serialization_scope: Option<&ClientRequestSerializationScope>,
) -> Option<&str> {
    if matches!(origin, ConnectionOrigin::ExternalController)
        || !matches!(
            rule.required_authority,
            RequiredAuthority::ActiveOwner | RequiredAuthority::TuiOnly
        )
    {
        return None;
    }

    match serialization_scope {
        Some(ClientRequestSerializationScope::Thread { thread_id }) => Some(thread_id.as_str()),
        Some(
            ClientRequestSerializationScope::Global(_)
            | ClientRequestSerializationScope::GlobalSharedRead(_)
            | ClientRequestSerializationScope::ThreadPath { .. }
            | ClientRequestSerializationScope::CommandExecProcess { .. }
            | ClientRequestSerializationScope::Process { .. }
            | ClientRequestSerializationScope::FuzzyFileSearchSession { .. }
            | ClientRequestSerializationScope::FsWatch { .. }
            | ClientRequestSerializationScope::McpOauth { .. },
        )
        | None => None,
    }
}

#[cfg(test)]
#[path = "message_processor_controller_target_tests.rs"]
mod message_processor_controller_target_tests;

#[cfg(test)]
#[path = "message_processor_tracing_tests.rs"]
mod message_processor_tracing_tests;
