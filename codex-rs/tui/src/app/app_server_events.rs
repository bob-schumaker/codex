//! App-server event stream handling for the TUI app.

use super::App;
use super::app_server_event_targets::ServerNotificationThreadTarget;
use super::app_server_event_targets::server_notification_thread_target;
use super::app_server_event_targets::server_request_thread_id;
use crate::app_command::AppCommand;
use crate::app_event::AppEvent;
use crate::app_event::ConnectorsSnapshot;
use crate::app_info::app_info_from_api;
use crate::app_server_session::AppServerSession;
use crate::app_server_session::status_account_display_from_auth_mode;
use codex_app_server_client::AppServerEvent;
use codex_app_server_client::InProcessThreadSnapshot;
use codex_app_server_protocol::AuthMode;
use codex_app_server_protocol::RateLimitReachedType;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;

impl App {
    pub(super) fn refresh_mcp_startup_expected_servers_from_config(&mut self) {
        if self
            .current_displayed_thread_id()
            .zip(self.primary_thread_id)
            .is_some_and(|(thread_id, primary_thread_id)| {
                self.agent_navigation.is_parent_owned(thread_id)
                    || (thread_id != primary_thread_id
                        && !self.side_threads.contains_key(&thread_id))
            })
        {
            // Subagents can defer cached servers indefinitely, so only servers
            // that actually report startup should keep their status running.
            self.chat_widget
                .set_mcp_startup_expected_servers(std::iter::empty());
            return;
        }

        let enabled_config_mcp_servers: Vec<String> = self
            .config
            .mcp_servers
            .get()
            .iter()
            .filter_map(|(name, server)| server.enabled.then_some(name.clone()))
            .collect();
        self.chat_widget
            .set_mcp_startup_expected_servers(enabled_config_mcp_servers);
    }

    pub(super) async fn handle_app_server_event(
        &mut self,
        app_server_client: &mut AppServerSession,
        event: AppServerEvent,
    ) {
        match event {
            AppServerEvent::Lagged { skipped } => {
                tracing::warn!(
                    skipped,
                    "app-server event consumer lagged; refreshing active thread snapshot"
                );
                self.refresh_mcp_startup_expected_servers_from_config();
                self.chat_widget.finish_mcp_startup_after_lag();
                self.refresh_current_thread_after_lag(app_server_client)
                    .await;
            }
            AppServerEvent::ControllerParticipationRequest(request) => {
                self.chat_widget
                    .open_controller_participation_prompt(*request);
            }
            AppServerEvent::ControllerOwnershipStatus(status) => {
                {
                    let channel = self.ensure_thread_channel(status.main_thread_id);
                    let mut store = channel.store.lock().await;
                    store.set_controller_ownership_status((*status).clone());
                }
                tracing::debug!(
                    main_thread_id = %status.main_thread_id,
                    owner = ?status.owner,
                    owner_epoch = status.owner_epoch,
                    reason = ?status.reason,
                    "controller ownership status changed"
                );
            }
            AppServerEvent::LocalControllerEndpointUnavailable { reason } => {
                crate::report_external_controller_availability(
                    &crate::ExternalControllerAvailability::EmbeddedUnavailable {
                        reason: Some(reason),
                    },
                );
            }
            AppServerEvent::ServerNotification(notification) => {
                self.handle_server_notification_event(
                    app_server_client,
                    *notification,
                    /*thread_sequence*/ None,
                )
                .await;
            }
            AppServerEvent::SequencedServerNotification(event) => {
                self.handle_server_notification_event(
                    app_server_client,
                    *event.notification,
                    Some(event.thread_sequence),
                )
                .await;
            }
            AppServerEvent::ServerRequest(request) => {
                self.handle_server_request_event(
                    app_server_client,
                    *request,
                    /*thread_sequence*/ None,
                )
                .await;
            }
            AppServerEvent::SequencedServerRequest(event) => {
                self.handle_server_request_event(
                    app_server_client,
                    *event.request,
                    Some(event.thread_sequence),
                )
                .await;
            }
            AppServerEvent::Disconnected { message } => {
                tracing::warn!("app-server event stream disconnected: {message}");
                self.chat_widget.add_error_message(message.clone());
                self.app_event_tx.send(AppEvent::FatalExitRequest(message));
            }
        }
    }

    async fn refresh_current_thread_after_lag(&mut self, app_server_client: &mut AppServerSession) {
        let Some(thread_id) = self.current_displayed_thread_id() else {
            return;
        };
        let input_state = self.chat_widget.capture_thread_input_state();
        match app_server_client
            .in_process_thread_snapshot(thread_id, /*include_turns*/ true)
            .await
        {
            Ok(Some(snapshot)) => {
                self.apply_in_process_thread_snapshot_after_lag(thread_id, snapshot, input_state)
                    .await;
                return;
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    thread_id = %thread_id,
                    error = %err,
                    "failed to refresh in-process thread snapshot after app-server event lag; falling back to thread/read"
                );
            }
        }
        let response = match app_server_client
            .thread_read_response(thread_id, /*include_turns*/ true)
            .await
        {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(
                    thread_id = %thread_id,
                    error = %err,
                    "failed to refresh thread snapshot after app-server event lag"
                );
                return;
            }
        };
        self.apply_thread_read_after_lag(
            thread_id,
            response.thread,
            response.last_sequence,
            input_state,
        )
        .await;
    }

    async fn apply_in_process_thread_snapshot_after_lag(
        &mut self,
        thread_id: codex_protocol::ThreadId,
        snapshot: InProcessThreadSnapshot,
        input_state: Option<crate::chatwidget::ThreadInputState>,
    ) {
        let session = self
            .session_state_for_thread_read(thread_id, &snapshot.thread)
            .await;
        let turns = snapshot.thread.turns;
        let replay_snapshot = {
            let channel = self.ensure_thread_channel(thread_id);
            let mut store = channel.store.lock().await;
            store.set_session_snapshot_at_sequence(
                session,
                turns,
                snapshot.last_sequence,
                snapshot.controller_ownership_status,
                snapshot.pending_server_requests,
            );
            store.input_state = input_state;
            store.snapshot()
        };
        self.replay_thread_snapshot(replay_snapshot, /*resume_restored_queue*/ false);
    }

    pub(super) async fn apply_thread_read_after_lag(
        &mut self,
        thread_id: codex_protocol::ThreadId,
        thread: codex_app_server_protocol::Thread,
        last_sequence: u64,
        input_state: Option<crate::chatwidget::ThreadInputState>,
    ) {
        let session = self.session_state_for_thread_read(thread_id, &thread).await;
        let turns = thread.turns;
        let snapshot = {
            let channel = self.ensure_thread_channel(thread_id);
            let mut store = channel.store.lock().await;
            store.set_session_at_sequence(session, turns, last_sequence);
            store.input_state = input_state;
            store.rebase_buffer_after_session_refresh();
            store.snapshot()
        };
        self.replay_thread_snapshot(snapshot, /*resume_restored_queue*/ false);
    }

    async fn handle_server_notification_event(
        &mut self,
        app_server_client: &AppServerSession,
        notification: ServerNotification,
        thread_sequence: Option<u64>,
    ) {
        match &notification {
            ServerNotification::ServerRequestResolved(notification) => {
                if let Some(request) = self
                    .pending_app_server_requests
                    .resolve_notification(&notification.request_id)
                {
                    self.chat_widget.dismiss_app_server_request(&request);
                }
            }
            ServerNotification::McpServerStatusUpdated(_) => {
                self.refresh_mcp_startup_expected_servers_from_config();
            }
            ServerNotification::AccountRateLimitsUpdated(notification) => {
                if matches!(
                    notification.rate_limits.rate_limit_reached_type,
                    Some(
                        RateLimitReachedType::WorkspaceOwnerCreditsDepleted
                            | RateLimitReachedType::WorkspaceMemberCreditsDepleted
                            | RateLimitReachedType::WorkspaceOwnerUsageLimitReached
                            | RateLimitReachedType::WorkspaceMemberUsageLimitReached
                    )
                ) || notification.rate_limits.spend_control_reached == Some(true)
                {
                    self.rate_limit_hard_stop_generation =
                        self.rate_limit_hard_stop_generation.wrapping_add(1);
                }
                self.chat_widget
                    .on_rolling_rate_limit_snapshot(notification.rate_limits.clone());
                return;
            }
            ServerNotification::AccountUpdated(notification) => {
                // Deferred terminal writes must never carry the previous account's billing into
                // the newly authenticated identity, even when both accounts share one thread.
                self.last_thread_usage_status_cell = None;
                self.pending_thread_usage_history_refresh = false;
                let has_codex_backend_auth = matches!(
                    notification.auth_mode,
                    Some(
                        AuthMode::Chatgpt
                            | AuthMode::ChatgptAuthTokens
                            | AuthMode::AgentIdentity
                            | AuthMode::PersonalAccessToken
                    )
                );
                self.chat_widget.update_account_state(
                    status_account_display_from_auth_mode(
                        notification.auth_mode,
                        notification.plan_type,
                    ),
                    notification.plan_type,
                    notification
                        .auth_mode
                        .is_some_and(AuthMode::has_chatgpt_account),
                    has_codex_backend_auth,
                );
                return;
            }
            ServerNotification::ExternalAgentConfigImportCompleted(notification) => {
                let should_report_completion =
                    app_server_client.consume_external_agent_config_import_completion();
                if let Err(err) = self.refresh_in_memory_config_from_disk().await {
                    tracing::warn!(
                        error = %err,
                        "failed to refresh config after external agent config import"
                    );
                }
                let cwd = self.chat_widget.config_ref().cwd.to_path_buf();
                self.chat_widget.refresh_plugin_mentions();
                self.chat_widget.submit_op(AppCommand::reload_user_config());
                self.fetch_plugins_list(app_server_client, cwd);
                if should_report_completion {
                    self.chat_widget.add_plain_history_lines(
                        crate::external_agent_config_migration::flow::external_agent_config_migration_finished_lines(notification),
                    );
                }
                return;
            }
            ServerNotification::AppListUpdated(notification) => {
                self.chat_widget.on_connectors_loaded(
                    Ok(ConnectorsSnapshot {
                        connectors: notification
                            .data
                            .iter()
                            .cloned()
                            .map(app_info_from_api)
                            .collect(),
                    }),
                    /*is_final*/ false,
                );
                return;
            }
            _ => {}
        }

        match server_notification_thread_target(&notification) {
            ServerNotificationThreadTarget::Thread(thread_id) => {
                let result = if self.primary_thread_id == Some(thread_id)
                    || self.primary_thread_id.is_none()
                {
                    self.enqueue_primary_thread_notification_at_sequence(
                        notification,
                        thread_sequence,
                    )
                    .await
                } else {
                    self.enqueue_thread_notification_at_sequence(
                        thread_id,
                        notification,
                        thread_sequence,
                    )
                    .await
                };

                if let Err(err) = result {
                    tracing::warn!("failed to enqueue app-server notification: {err}");
                }
                return;
            }
            ServerNotificationThreadTarget::InvalidThreadId(thread_id) => {
                tracing::warn!(
                    thread_id,
                    "ignoring app-server notification with invalid thread_id"
                );
                return;
            }
            ServerNotificationThreadTarget::AppScoped => {
                tracing::debug!(
                    "ignoring app-scoped MCP startup notification without a TUI app-level target"
                );
                return;
            }
            ServerNotificationThreadTarget::Global => {}
        }

        self.chat_widget
            .handle_server_notification(notification, /*replay_kind*/ None);
    }

    async fn handle_server_request_event(
        &mut self,
        app_server_client: &AppServerSession,
        request: ServerRequest,
        thread_sequence: Option<u64>,
    ) {
        let thread_id = server_request_thread_id(&request);
        if thread_id.is_some_and(|thread_id| self.abandoned_side_threads.contains(&thread_id)) {
            if let Err(err) = self
                .reject_app_server_request(
                    app_server_client,
                    request.id().clone(),
                    "side conversation was closed".to_string(),
                )
                .await
            {
                tracing::warn!("{err}");
            }
            return;
        }

        if let Some(unsupported) = self
            .pending_app_server_requests
            .note_server_request(&request)
        {
            tracing::warn!(
                request_id = ?unsupported.request_id,
                message = unsupported.message,
                "rejecting unsupported app-server request"
            );
            self.chat_widget
                .add_error_message(unsupported.message.clone());
            if let Err(err) = self
                .reject_app_server_request(
                    app_server_client,
                    unsupported.request_id,
                    unsupported.message,
                )
                .await
            {
                tracing::warn!("{err}");
            }
            return;
        }

        let Some(thread_id) = thread_id else {
            tracing::warn!("ignoring threadless app-server request");
            return;
        };

        let result =
            if self.primary_thread_id == Some(thread_id) || self.primary_thread_id.is_none() {
                self.enqueue_primary_thread_request_at_sequence(request, thread_sequence)
                    .await
            } else {
                self.enqueue_thread_request_at_sequence(thread_id, request, thread_sequence)
                    .await
            };
        if let Err(err) = result {
            tracing::warn!("failed to enqueue app-server request: {err}");
        }
    }
}
