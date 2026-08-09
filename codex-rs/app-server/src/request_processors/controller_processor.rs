use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use codex_app_server_protocol::CommandExecutionApprovalDecision;
use codex_app_server_protocol::ControllerAcquireControlResponse;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::ControllerLaunchState;
use codex_app_server_protocol::ControllerParticipationDenial;
use codex_app_server_protocol::ControllerParticipationStatus;
use codex_app_server_protocol::ControllerReleaseControlResponse;
use codex_app_server_protocol::ControllerRequestParticipationParams;
use codex_app_server_protocol::ControllerRequestParticipationResponse;
use codex_app_server_protocol::ControllerRetryDisposition;
use codex_app_server_protocol::ControllerSignOffResponse;
use codex_app_server_protocol::FileChangeApprovalDecision;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::PermissionGrantScope;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ServerResponse;
use codex_protocol::ThreadId;

use crate::controller_admission::AdmissionRule;
use crate::controller_admission::RequiredAuthority;
use crate::controller_admission::TargetExtraction;
use crate::controller_admission::server_request_response_rule;
use crate::controller_enrollment::ControllerCredentialProof;
use crate::controller_enrollment::ControllerDisplayClaims;
use crate::controller_enrollment::ControllerEnrollmentError;
use crate::controller_enrollment::ControllerEnrollmentPolicy;
use crate::controller_enrollment::ControllerEnrollmentSource;
use crate::controller_enrollment::ControllerEnrollmentVerifier;
use crate::controller_enrollment::ControllerParticipationEvidence;
use crate::controller_native_approval::NativeControllerParticipationApprover;
use crate::controller_native_approval::NativeControllerParticipationDecision;
use crate::controller_native_approval::NativeControllerParticipationRequest;
use crate::controller_session::ControllerEnrollmentGrant;
use crate::controller_session::ControllerOwnershipStatus;
use crate::controller_session::ControllerSessionClock;
use crate::controller_session::ControllerSessionConfig;
use crate::controller_session::ControllerSessionCoordinator;
use crate::controller_session::ControllerSessionError;
use crate::controller_session::ControllerSessionEvents;
use crate::controller_session::ControllerSessionNotification;
use crate::controller_session::InteractiveOwner;
use crate::error_code::INVALID_REQUEST_ERROR_CODE;
use crate::outgoing_message::OutgoingMessageSender;
use crate::outgoing_message::ServerRequestRecipients;
use crate::transport::ConnectionId;
use crate::transport::ConnectionOrigin;

const CONTROLLER_TRANSFER_RETRY_AFTER: u64 = 50;
const NATIVE_CONTROLLER_AUTHORIZATION_DURATION: Duration = Duration::from_secs(30 * 60);

#[derive(Clone)]
pub(crate) struct ControllerRequestProcessor {
    outgoing: Arc<OutgoingMessageSender>,
    state: Arc<Mutex<ControllerProcessorState>>,
    enrollment_source: Arc<dyn ControllerEnrollmentSource>,
    native_participation_approver: Option<NativeControllerParticipationApprover>,
    ownership_status_tx: Option<tokio::sync::mpsc::Sender<ControllerOwnershipStatus>>,
    enrollment_policy: ControllerEnrollmentPolicy,
    clock: ControllerSessionClock,
    session_config: ControllerSessionConfig,
}

struct ControllerProcessorState {
    coordinator: Option<ControllerSessionCoordinator>,
    tui_connection_id: Option<ConnectionId>,
    launch_state: ControllerLaunchState,
}

struct PromptRebind {
    thread_id: ThreadId,
    connection_id: ConnectionId,
    delivery: PromptRebindDelivery,
    fallback_connection_id: Option<ConnectionId>,
    owner_epoch: Option<u64>,
}

enum PromptRebindDelivery {
    Normal,
    ExternalController,
}

type ControllerTransitionResult<T> = Result<
    (
        Result<T, JSONRPCErrorError>,
        Option<PromptRebind>,
        ControllerSessionEvents,
    ),
    JSONRPCErrorError,
>;

pub(crate) enum ControllerRequestTarget {
    None,
    ExactThread(String),
    CollectionFiltered,
}

pub(crate) struct ControllerNormalAuthorization {
    pub(crate) main_thread_id: String,
    pub(crate) filter_collection_to_main_thread: bool,
}

impl ControllerRequestProcessor {
    pub(crate) fn new(
        outgoing: Arc<OutgoingMessageSender>,
        enrollment_source: Arc<dyn ControllerEnrollmentSource>,
        native_participation_approver: Option<NativeControllerParticipationApprover>,
        ownership_status_tx: Option<tokio::sync::mpsc::Sender<ControllerOwnershipStatus>>,
        enrollment_policy: ControllerEnrollmentPolicy,
        clock: ControllerSessionClock,
        session_config: ControllerSessionConfig,
    ) -> Self {
        Self {
            outgoing,
            state: Arc::new(Mutex::new(ControllerProcessorState {
                coordinator: None,
                tui_connection_id: None,
                launch_state: ControllerLaunchState::Starting,
            })),
            enrollment_source,
            native_participation_approver,
            ownership_status_tx,
            enrollment_policy,
            clock,
            session_config,
        }
    }

    pub(crate) fn register_main_thread(
        &self,
        main_thread_id: ThreadId,
        tui_connection_id: ConnectionId,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.coordinator.is_none() {
            state.tui_connection_id = Some(tui_connection_id);
            state.coordinator = Some(ControllerSessionCoordinator::new(
                main_thread_id,
                self.clock.clone(),
                self.session_config,
            ));
        }
    }

    pub(crate) async fn request_participation(
        &self,
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
        credential_proof: Option<ControllerCredentialProof>,
        params: ControllerRequestParticipationParams,
    ) -> Result<ControllerRequestParticipationResponse, JSONRPCErrorError> {
        require_external_controller_origin(origin)?;

        if credential_proof.is_none()
            && let Some(native_participation_approver) = self.native_participation_approver.as_ref()
        {
            let main_thread_id = self.native_participation_main_thread_id()?;
            let decision = native_participation_approver(NativeControllerParticipationRequest {
                connection_id,
                controller_name: params.controller_name,
                description: params.description,
                main_thread_id: main_thread_id.to_string(),
            })
            .await;

            return match decision {
                NativeControllerParticipationDecision::Approved => {
                    self.approve_native_participation(connection_id, main_thread_id)
                        .await
                }
                NativeControllerParticipationDecision::Rejected { reason } => {
                    Ok(controller_participation_rejected(
                        reason,
                        ControllerRetryDisposition::SameConnection,
                    ))
                }
                NativeControllerParticipationDecision::TuiUnavailable { reason } => {
                    self.mark_tui_unavailable(reason.clone()).await;
                    Err(tui_unavailable(reason))
                }
            };
        }

        let verifier = ControllerEnrollmentVerifier::new(
            Arc::clone(&self.enrollment_source),
            self.enrollment_policy,
            self.clock.clone(),
        );

        let (response, rebind, events) =
            self.with_main_thread_rebind(|coordinator, main_thread_id| {
                let grant = match verifier.verify(
                    connection_id,
                    main_thread_id,
                    ControllerParticipationEvidence {
                        display_claims: ControllerDisplayClaims {
                            controller_name: params.controller_name,
                            description: params.description,
                        },
                        credential_proof,
                    },
                ) {
                    Ok(grant) => grant,
                    Err(err) => {
                        return Ok(ControllerRequestParticipationResponse {
                            status: ControllerParticipationStatus::Rejected,
                            session: None,
                            denial: Some(ControllerParticipationDenial {
                                message: enrollment_error_message(&err).to_string(),
                                data: enrollment_error_data(err, Some(main_thread_id)),
                            }),
                        });
                    }
                };

                let session = coordinator
                    .request_participation(connection_id, grant)
                    .map_err(controller_session_error)?;
                Ok(ControllerRequestParticipationResponse {
                    status: ControllerParticipationStatus::Approved,
                    session: Some(session),
                    denial: None,
                })
            })?;

        self.rebind_pending_prompts(rebind).await;
        self.send_controller_events(events).await;
        response
    }

    fn native_participation_main_thread_id(&self) -> Result<ThreadId, JSONRPCErrorError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let launch_state = state.launch_state.clone();
        let Some(coordinator) = state.coordinator.as_ref() else {
            return Err(main_thread_unavailable(launch_state));
        };
        if matches!(
            coordinator.interactive_owner(),
            InteractiveOwner::TuiUnavailable { .. }
        ) {
            return Err(tui_unavailable(
                "TUI is unavailable for this controller launch".to_string(),
            ));
        }
        coordinator.main_thread_id().ok_or_else(main_thread_closed)
    }

    async fn approve_native_participation(
        &self,
        connection_id: ConnectionId,
        requested_main_thread_id: ThreadId,
    ) -> Result<ControllerRequestParticipationResponse, JSONRPCErrorError> {
        let (response, rebind, events) =
            self.with_main_thread_rebind(|coordinator, main_thread_id| {
                if main_thread_id != requested_main_thread_id {
                    return Err(thread_target_error(main_thread_id.to_string()));
                }
                let now = self.clock.now();
                let grant = ControllerEnrollmentGrant {
                    subject_id: format!("native-controller-connection-{}", connection_id.0),
                    main_thread_id,
                    authorization_epoch: connection_id.0,
                    authorization_expires_at: now + NATIVE_CONTROLLER_AUTHORIZATION_DURATION,
                };
                coordinator
                    .request_participation(connection_id, grant)
                    .map(|session| ControllerRequestParticipationResponse {
                        status: ControllerParticipationStatus::Approved,
                        session: Some(session),
                        denial: None,
                    })
                    .map_err(controller_session_error)
            })?;

        self.rebind_pending_prompts(rebind).await;
        self.send_controller_events(events).await;
        response
    }

    pub(crate) async fn acquire_control(
        &self,
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
    ) -> Result<ControllerAcquireControlResponse, JSONRPCErrorError> {
        require_external_controller_origin(origin)?;
        let (response, rebind, events) =
            self.with_main_thread_rebind(|coordinator, _main_thread_id| {
                coordinator
                    .acquire_control(connection_id)
                    .map(|session| ControllerAcquireControlResponse { session })
                    .map_err(controller_session_error)
            })?;
        self.rebind_pending_prompts(rebind).await;
        self.send_controller_events(events).await;
        response
    }

    pub(crate) async fn release_control(
        &self,
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
    ) -> Result<ControllerReleaseControlResponse, JSONRPCErrorError> {
        require_external_controller_origin(origin)?;
        let (response, rebind, events) =
            self.with_main_thread_rebind(|coordinator, _main_thread_id| {
                coordinator
                    .release_control(connection_id)
                    .map(|session| ControllerReleaseControlResponse { session })
                    .map_err(controller_session_error)
            })?;
        self.rebind_pending_prompts(rebind).await;
        self.send_controller_events(events).await;
        response
    }

    pub(crate) async fn sign_off(
        &self,
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
    ) -> Result<ControllerSignOffResponse, JSONRPCErrorError> {
        require_external_controller_origin(origin)?;
        let (response, rebind, events) =
            self.with_main_thread_rebind(|coordinator, _main_thread_id| {
                coordinator
                    .require_standing_session(connection_id)
                    .map_err(controller_session_error)?;
                coordinator.sign_off_session(connection_id);
                Ok(ControllerSignOffResponse {})
            })?;
        self.rebind_pending_prompts(rebind).await;
        self.send_controller_events(events).await;
        response
    }

    pub(crate) async fn authorize_normal_request(
        &self,
        connection_id: ConnectionId,
        rule: AdmissionRule,
        target: ControllerRequestTarget,
    ) -> Result<ControllerNormalAuthorization, JSONRPCErrorError> {
        match rule.required_authority {
            RequiredAuthority::PreParticipation => {
                return Err(controller_error(
                    "external controller pre-participation methods do not use the normal interface",
                    error_data(
                        ControllerErrorCode::ControllerNotAllowed,
                        ControllerRetryDisposition::DoNotRetry,
                    ),
                ));
            }
            RequiredAuthority::TuiOnly => {
                return Err(controller_error(
                    "external controller cannot use TUI-only method",
                    error_data(
                        ControllerErrorCode::ControllerNotAllowed,
                        ControllerRetryDisposition::DoNotRetry,
                    ),
                ));
            }
            RequiredAuthority::StandingSession | RequiredAuthority::ActiveOwner => {}
        }

        let (result, rebind, events) =
            self.with_main_thread_rebind(|coordinator, _main_thread_id| {
                let authorized_main_thread_id = match rule.required_authority {
                    RequiredAuthority::StandingSession => coordinator
                        .require_standing_session(connection_id)
                        .map_err(controller_session_error),
                    RequiredAuthority::ActiveOwner => coordinator
                        .require_active_owner(connection_id)
                        .map_err(controller_session_error),
                    RequiredAuthority::PreParticipation | RequiredAuthority::TuiOnly => {
                        unreachable!()
                    }
                };
                authorized_main_thread_id.and_then(|main_thread_id| {
                    authorize_target(rule.target, target, main_thread_id)
                })
            })?;
        self.rebind_pending_prompts(rebind).await;
        self.send_controller_events(events).await;
        result
    }

    pub(crate) fn main_thread_for_missing_session(
        &self,
        connection_id: ConnectionId,
    ) -> Option<ThreadId> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let coordinator = state.coordinator.as_ref()?;
        let main_thread_id = coordinator.main_thread_id()?;
        coordinator
            .session_for(connection_id)
            .is_none()
            .then_some(main_thread_id)
    }

    pub(crate) fn prompt_request_recipients(
        &self,
        thread_id: ThreadId,
        subscribed_connection_ids: Vec<ConnectionId>,
    ) -> ServerRequestRecipients {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(coordinator) = state.coordinator.as_ref() else {
            return ServerRequestRecipients::normal(subscribed_connection_ids);
        };
        let Some(main_thread_id) = coordinator.main_thread_id() else {
            return ServerRequestRecipients::normal(Vec::new());
        };
        if main_thread_id != thread_id {
            return ServerRequestRecipients::normal(subscribed_connection_ids);
        }

        match coordinator.interactive_owner() {
            InteractiveOwner::ControllerOwned {
                connection_id,
                owner_epoch,
                ..
            } => ServerRequestRecipients::external_controller_with_fallback(
                *connection_id,
                state.tui_connection_id,
                *owner_epoch,
            ),
            InteractiveOwner::TuiOwned { .. } => {
                if let Some(tui_connection_id) = state.tui_connection_id {
                    ServerRequestRecipients::normal(vec![tui_connection_id])
                } else {
                    ServerRequestRecipients::normal(subscribed_connection_ids)
                }
            }
            InteractiveOwner::TransferPending { .. }
            | InteractiveOwner::TuiUnavailable { .. }
            | InteractiveOwner::Closed => ServerRequestRecipients::normal(Vec::new()),
        }
    }

    pub(crate) fn thread_notification_recipients(
        &self,
        thread_id: ThreadId,
        subscribed_connection_ids: Vec<ConnectionId>,
    ) -> Vec<ConnectionId> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(coordinator) = state.coordinator.as_ref() else {
            return subscribed_connection_ids;
        };
        let Some(main_thread_id) = coordinator.main_thread_id() else {
            return subscribed_connection_ids;
        };
        if main_thread_id != thread_id {
            return subscribed_connection_ids;
        }

        match coordinator.interactive_owner() {
            InteractiveOwner::TuiUnavailable { .. } => Vec::new(),
            InteractiveOwner::TuiOwned { .. }
            | InteractiveOwner::TransferPending { .. }
            | InteractiveOwner::ControllerOwned { .. }
            | InteractiveOwner::Closed => subscribed_connection_ids,
        }
    }

    pub(crate) async fn reclaim_for_primary_thread_input(
        &self,
        thread_id: &str,
    ) -> Result<(), JSONRPCErrorError> {
        let (result, rebind, events) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let tui_connection_id = state.tui_connection_id;
            let Some(coordinator) = state.coordinator.as_mut() else {
                return Ok(());
            };
            let Some(main_thread_id) = coordinator.main_thread_id() else {
                return Ok(());
            };
            if main_thread_id.to_string() != thread_id {
                return Ok(());
            }

            let owner_before = coordinator.interactive_owner().clone();
            let result = coordinator
                .reclaim_for_tui()
                .map_err(controller_session_error);
            let rebind = prompt_rebind_after_transition(
                main_thread_id,
                tui_connection_id,
                &owner_before,
                coordinator.interactive_owner(),
            );
            let events = coordinator.drain_events();
            (result, rebind, events)
        };
        self.rebind_pending_prompts(rebind).await;
        self.send_controller_events(events).await;
        result
    }

    pub(crate) async fn authorize_server_response(
        &self,
        connection_id: ConnectionId,
        thread_id: Option<ThreadId>,
        expected_owner_epoch: Option<u64>,
        response: &ServerResponse,
    ) -> Result<(), JSONRPCErrorError> {
        let method = response.method();
        self.authorize_server_request_resolution(
            connection_id,
            thread_id,
            expected_owner_epoch,
            &method,
        )
        .await?;
        reject_controller_session_scoped_response(response)?;
        Ok(())
    }

    pub(crate) async fn authorize_server_request_error(
        &self,
        connection_id: ConnectionId,
        thread_id: Option<ThreadId>,
        expected_owner_epoch: Option<u64>,
        request: &ServerRequest,
    ) -> Result<(), JSONRPCErrorError> {
        self.authorize_server_request_resolution(
            connection_id,
            thread_id,
            expected_owner_epoch,
            server_request_method(request),
        )
        .await
    }

    pub(crate) async fn connection_closed(&self, connection_id: ConnectionId) -> Option<ThreadId> {
        let Ok((result, rebind, events)) =
            self.with_main_thread_rebind(|coordinator, _main_thread_id| {
                let removed_main_thread_id = coordinator
                    .session_for(connection_id)
                    .map(|session| session.main_thread_id);
                coordinator.disconnect_session(connection_id);
                Ok(removed_main_thread_id)
            })
        else {
            return None;
        };
        if result.is_err() {
            return None;
        }
        self.rebind_pending_prompts(rebind).await;
        self.send_controller_events(events).await;
        result.ok().flatten()
    }

    async fn mark_tui_unavailable(&self, reason: String) {
        let (main_thread_id, events) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.launch_state = ControllerLaunchState::TuiUnavailable;
            let Some(coordinator) = state.coordinator.as_mut() else {
                return;
            };
            let main_thread_id = coordinator.main_thread_id();
            if let Err(err) = coordinator.mark_tui_unavailable() {
                tracing::debug!(
                    error = ?err,
                    "failed to mark controller launch TUI-unavailable"
                );
            }
            let events = coordinator.drain_events();
            (main_thread_id, events)
        };

        if let Some(main_thread_id) = main_thread_id {
            self.outgoing
                .cancel_requests_for_thread(main_thread_id, Some(tui_unavailable(reason)))
                .await;
        }
        self.send_controller_events(events).await;
    }

    async fn authorize_server_request_resolution(
        &self,
        connection_id: ConnectionId,
        thread_id: Option<ThreadId>,
        expected_owner_epoch: Option<u64>,
        method: &str,
    ) -> Result<(), JSONRPCErrorError> {
        let Some(rule) = server_request_response_rule(method) else {
            return Err(controller_error(
                "external controller cannot resolve unclassified server request",
                error_data(
                    ControllerErrorCode::ControllerNotAllowed,
                    ControllerRetryDisposition::DoNotRetry,
                ),
            ));
        };

        match rule.required_authority {
            RequiredAuthority::ActiveOwner => {
                let Some(thread_id) = thread_id else {
                    return Err(controller_error(
                        "external controller server response requires a main-thread prompt binding",
                        error_data(
                            ControllerErrorCode::ControllerNotAllowed,
                            ControllerRetryDisposition::DoNotRetry,
                        ),
                    ));
                };
                let (result, rebind, events) =
                    self.with_main_thread_rebind(|coordinator, _main_thread_id| {
                        coordinator
                            .require_active_owner_with_epoch(connection_id)
                            .map_err(controller_session_error)
                            .and_then(|(main_thread_id, owner_epoch)| {
                                if expected_owner_epoch.is_some_and(|expected_owner_epoch| {
                                    expected_owner_epoch != owner_epoch
                                }) {
                                    return Err(controller_session_error(
                                        ControllerSessionError::StaleOwnership,
                                    ));
                                }
                                authorize_target(
                                    rule.target,
                                    ControllerRequestTarget::ExactThread(thread_id.to_string()),
                                    main_thread_id,
                                )
                                .map(|_| ())
                            })
                    })?;
                self.rebind_pending_prompts(rebind).await;
                self.send_controller_events(events).await;
                result
            }
            RequiredAuthority::TuiOnly => Err(controller_error(
                "external controller cannot resolve TUI-only server request",
                error_data(
                    ControllerErrorCode::ControllerNotAllowed,
                    ControllerRetryDisposition::DoNotRetry,
                ),
            )),
            RequiredAuthority::PreParticipation | RequiredAuthority::StandingSession => {
                Err(controller_error(
                    "external controller server response requires active ownership",
                    error_data(
                        ControllerErrorCode::ControllerNotAllowed,
                        ControllerRetryDisposition::DoNotRetry,
                    ),
                ))
            }
        }
    }

    fn with_main_thread_rebind<T>(
        &self,
        operation: impl FnOnce(
            &mut ControllerSessionCoordinator,
            ThreadId,
        ) -> Result<T, JSONRPCErrorError>,
    ) -> ControllerTransitionResult<T> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let launch_state = state.launch_state.clone();
        let tui_connection_id = state.tui_connection_id;
        let Some(coordinator) = state.coordinator.as_mut() else {
            return Err(main_thread_unavailable(launch_state));
        };
        let Some(main_thread_id) = coordinator.main_thread_id() else {
            return Err(main_thread_closed());
        };
        let owner_before = coordinator.interactive_owner().clone();
        let result = operation(coordinator, main_thread_id);
        let rebind = prompt_rebind_after_transition(
            main_thread_id,
            tui_connection_id,
            &owner_before,
            coordinator.interactive_owner(),
        );
        let events = coordinator.drain_events();
        Ok((result, rebind, events))
    }

    async fn rebind_pending_prompts(&self, rebind: Option<PromptRebind>) {
        if let Some(PromptRebind {
            thread_id,
            connection_id,
            delivery,
            fallback_connection_id,
            owner_epoch,
        }) = rebind
        {
            match delivery {
                PromptRebindDelivery::Normal => {
                    self.outgoing
                        .rebind_requests_for_thread_to_connection(thread_id, connection_id)
                        .await;
                }
                PromptRebindDelivery::ExternalController => {
                    let Some(owner_epoch) = owner_epoch else {
                        tracing::warn!(
                            "skipping external-controller prompt rebind without owner epoch"
                        );
                        return;
                    };
                    self.outgoing
                        .rebind_requests_for_thread_to_external_controller_connection(
                            thread_id,
                            connection_id,
                            fallback_connection_id,
                            owner_epoch,
                        )
                        .await;
                }
            }
        }
    }

    async fn send_controller_events(&self, events: ControllerSessionEvents) {
        if let Some(ownership_status_tx) = self.ownership_status_tx.as_ref() {
            for status in events.ownership_statuses {
                if ownership_status_tx.send(status).await.is_err() {
                    tracing::warn!(
                        "dropping controller ownership status; TUI event sink is closed"
                    );
                    break;
                }
            }
        }

        for notification in events.controller_notifications {
            let (connection_id, notification) = match notification {
                ControllerSessionNotification::AuthorizationChanged {
                    connection_id,
                    notification,
                } => (
                    connection_id,
                    ServerNotification::ControllerAuthorizationChanged(notification),
                ),
                ControllerSessionNotification::ControlOwnershipChanged {
                    connection_id,
                    notification,
                } => (
                    connection_id,
                    ServerNotification::ControllerControlOwnershipChanged(notification),
                ),
            };
            self.outgoing
                .send_server_notification_to_connections(&[connection_id], notification)
                .await;
        }
    }
}

fn prompt_rebind_after_transition(
    thread_id: ThreadId,
    tui_connection_id: Option<ConnectionId>,
    owner_before: &InteractiveOwner,
    owner_after: &InteractiveOwner,
) -> Option<PromptRebind> {
    if owner_before == owner_after {
        return None;
    }

    let connection_id = match owner_after {
        InteractiveOwner::ControllerOwned {
            connection_id,
            owner_epoch,
            ..
        } => {
            return Some(PromptRebind {
                thread_id,
                connection_id: *connection_id,
                delivery: PromptRebindDelivery::ExternalController,
                fallback_connection_id: tui_connection_id,
                owner_epoch: Some(*owner_epoch),
            });
        }
        InteractiveOwner::TuiOwned { .. } => tui_connection_id?,
        InteractiveOwner::TransferPending { .. }
        | InteractiveOwner::TuiUnavailable { .. }
        | InteractiveOwner::Closed => return None,
    };
    Some(PromptRebind {
        thread_id,
        connection_id,
        delivery: PromptRebindDelivery::Normal,
        fallback_connection_id: None,
        owner_epoch: None,
    })
}

fn server_request_method(request: &ServerRequest) -> &'static str {
    match request {
        ServerRequest::CommandExecutionRequestApproval { .. } => {
            "item/commandExecution/requestApproval"
        }
        ServerRequest::FileChangeRequestApproval { .. } => "item/fileChange/requestApproval",
        ServerRequest::ToolRequestUserInput { .. } => "item/tool/requestUserInput",
        ServerRequest::McpServerElicitationRequest { .. } => "mcpServer/elicitation/request",
        ServerRequest::PermissionsRequestApproval { .. } => "item/permissions/requestApproval",
        ServerRequest::DynamicToolCall { .. } => "item/tool/call",
        ServerRequest::ChatgptAuthTokensRefresh { .. } => "account/chatgptAuthTokens/refresh",
        ServerRequest::AttestationGenerate { .. } => "attestation/generate",
        ServerRequest::CurrentTimeRead { .. } => "currentTime/read",
        ServerRequest::ApplyPatchApproval { .. } => "applyPatchApproval",
        ServerRequest::ExecCommandApproval { .. } => "execCommandApproval",
    }
}

fn reject_controller_session_scoped_response(
    response: &ServerResponse,
) -> Result<(), JSONRPCErrorError> {
    let reject = match response {
        ServerResponse::CommandExecutionRequestApproval { response, .. } => {
            matches!(
                response.decision,
                CommandExecutionApprovalDecision::AcceptForSession
            )
        }
        ServerResponse::FileChangeRequestApproval { response, .. } => {
            matches!(
                response.decision,
                FileChangeApprovalDecision::AcceptForSession
            )
        }
        ServerResponse::PermissionsRequestApproval { response, .. } => {
            matches!(response.scope, PermissionGrantScope::Session)
        }
        ServerResponse::ToolRequestUserInput { .. }
        | ServerResponse::McpServerElicitationRequest { .. }
        | ServerResponse::DynamicToolCall { .. }
        | ServerResponse::ChatgptAuthTokensRefresh { .. }
        | ServerResponse::AttestationGenerate { .. }
        | ServerResponse::CurrentTimeRead { .. }
        | ServerResponse::ApplyPatchApproval { .. }
        | ServerResponse::ExecCommandApproval { .. } => false,
    };
    if reject {
        return Err(controller_error(
            "external controller cannot grant session-scoped approval",
            error_data(
                ControllerErrorCode::ControllerNotAllowed,
                ControllerRetryDisposition::DoNotRetry,
            ),
        ));
    }
    Ok(())
}

fn authorize_target(
    extraction: TargetExtraction,
    target: ControllerRequestTarget,
    main_thread_id: ThreadId,
) -> Result<ControllerNormalAuthorization, JSONRPCErrorError> {
    let main_thread_id = main_thread_id.to_string();
    match (extraction, target) {
        (
            TargetExtraction::None | TargetExtraction::MainThreadOnly,
            ControllerRequestTarget::None,
        ) => Ok(ControllerNormalAuthorization {
            main_thread_id,
            filter_collection_to_main_thread: false,
        }),
        (TargetExtraction::CollectionFiltered, ControllerRequestTarget::CollectionFiltered) => {
            Ok(ControllerNormalAuthorization {
                main_thread_id,
                filter_collection_to_main_thread: true,
            })
        }
        (TargetExtraction::ExactThread, ControllerRequestTarget::ExactThread(thread_id))
            if thread_id == main_thread_id =>
        {
            Ok(ControllerNormalAuthorization {
                main_thread_id,
                filter_collection_to_main_thread: false,
            })
        }
        (TargetExtraction::ExactThread, ControllerRequestTarget::ExactThread(_)) => {
            Err(thread_target_error(main_thread_id))
        }
        (TargetExtraction::ExactThread, ControllerRequestTarget::None) => {
            Err(thread_target_error(main_thread_id))
        }
        (TargetExtraction::CollectionFiltered, ControllerRequestTarget::None) => {
            Err(controller_error(
                "external controller collection filter is required for this method",
                error_data(
                    ControllerErrorCode::ControllerNotAllowed,
                    ControllerRetryDisposition::DoNotRetry,
                ),
            ))
        }
        (
            TargetExtraction::None
            | TargetExtraction::MainThreadOnly
            | TargetExtraction::CollectionFiltered,
            ControllerRequestTarget::ExactThread(_),
        )
        | (
            TargetExtraction::None
            | TargetExtraction::MainThreadOnly
            | TargetExtraction::ExactThread,
            ControllerRequestTarget::CollectionFiltered,
        ) => Err(controller_error(
            "external controller target shape does not match method admission",
            error_data(
                ControllerErrorCode::ControllerNotAllowed,
                ControllerRetryDisposition::DoNotRetry,
            ),
        )),
    }
}

fn thread_target_error(main_thread_id: String) -> JSONRPCErrorError {
    controller_error(
        "external controller request must target the authorized main thread",
        ControllerErrorData {
            main_thread_id: Some(main_thread_id),
            ..error_data(
                ControllerErrorCode::DifferentThreadTarget,
                ControllerRetryDisposition::DoNotRetry,
            )
        },
    )
}

fn require_external_controller_origin(origin: ConnectionOrigin) -> Result<(), JSONRPCErrorError> {
    match origin {
        ConnectionOrigin::ExternalController => Ok(()),
        ConnectionOrigin::Stdio
        | ConnectionOrigin::InProcess
        | ConnectionOrigin::WebSocket
        | ConnectionOrigin::RemoteControl => Err(controller_error(
            "controller methods require an external controller connection",
            ControllerErrorData {
                code: ControllerErrorCode::ControllerNotAllowed,
                retry: ControllerRetryDisposition::DoNotRetry,
                retry_after_ms: None,
                launch_state: None,
                main_thread_id: None,
                session_id: None,
                authorization_epoch: None,
                owner_epoch: None,
            },
        )),
    }
}

fn enrollment_error_message(err: &ControllerEnrollmentError) -> &'static str {
    match err {
        ControllerEnrollmentError::PolicyDisabled => "external controllers are disabled by policy",
        ControllerEnrollmentError::RequiredEnrollmentMissing => {
            "required controller enrollment is missing"
        }
        ControllerEnrollmentError::CredentialProofRequired => {
            "controller credential proof is required"
        }
        ControllerEnrollmentError::EnrollmentDenied => "controller enrollment was not accepted",
        ControllerEnrollmentError::ConnectionMismatch => {
            "controller credential proof is not bound to this connection"
        }
        ControllerEnrollmentError::CredentialRotated => "controller credential has been rotated",
        ControllerEnrollmentError::AuthorizationExpired => "controller authorization has expired",
        ControllerEnrollmentError::Revoked => "controller authorization has been revoked",
        ControllerEnrollmentError::DifferentMainThread => {
            "controller enrollment targets a different main thread"
        }
    }
}

fn enrollment_error_data(
    err: ControllerEnrollmentError,
    main_thread_id: Option<ThreadId>,
) -> ControllerErrorData {
    let code = match err {
        ControllerEnrollmentError::AuthorizationExpired => {
            ControllerErrorCode::AuthorizationExpired
        }
        ControllerEnrollmentError::DifferentMainThread => {
            ControllerErrorCode::DifferentThreadTarget
        }
        ControllerEnrollmentError::PolicyDisabled
        | ControllerEnrollmentError::RequiredEnrollmentMissing
        | ControllerEnrollmentError::CredentialProofRequired
        | ControllerEnrollmentError::EnrollmentDenied
        | ControllerEnrollmentError::ConnectionMismatch
        | ControllerEnrollmentError::CredentialRotated
        | ControllerEnrollmentError::Revoked => ControllerErrorCode::EnrollmentDenied,
    };
    ControllerErrorData {
        code,
        retry: ControllerRetryDisposition::DoNotRetry,
        retry_after_ms: None,
        launch_state: None,
        main_thread_id: main_thread_id.map(|id| id.to_string()),
        session_id: None,
        authorization_epoch: None,
        owner_epoch: None,
    }
}

fn controller_participation_rejected(
    message: String,
    retry: ControllerRetryDisposition,
) -> ControllerRequestParticipationResponse {
    ControllerRequestParticipationResponse {
        status: ControllerParticipationStatus::Rejected,
        session: None,
        denial: Some(ControllerParticipationDenial {
            message,
            data: error_data(ControllerErrorCode::EnrollmentDenied, retry),
        }),
    }
}

fn tui_unavailable(message: String) -> JSONRPCErrorError {
    controller_error(
        &message,
        ControllerErrorData {
            code: ControllerErrorCode::TuiUnavailable,
            retry: ControllerRetryDisposition::DoNotRetry,
            retry_after_ms: None,
            launch_state: Some(ControllerLaunchState::TuiUnavailable),
            main_thread_id: None,
            session_id: None,
            authorization_epoch: None,
            owner_epoch: None,
        },
    )
}

fn controller_session_error(err: ControllerSessionError) -> JSONRPCErrorError {
    match err {
        ControllerSessionError::ParticipationRequired => controller_error(
            "controller participation is required",
            error_data(
                ControllerErrorCode::ParticipationRequired,
                ControllerRetryDisposition::DoNotRetry,
            ),
        ),
        ControllerSessionError::AuthorizationExpired => controller_error(
            "controller authorization has expired",
            error_data(
                ControllerErrorCode::AuthorizationExpired,
                ControllerRetryDisposition::DoNotRetry,
            ),
        ),
        ControllerSessionError::OwnershipConflict => controller_error(
            "another controller owns the main thread",
            error_data(
                ControllerErrorCode::OwnershipConflict,
                ControllerRetryDisposition::SameConnection,
            ),
        ),
        ControllerSessionError::TransferPending => controller_error(
            "controller ownership transfer is pending",
            ControllerErrorData {
                retry_after_ms: Some(CONTROLLER_TRANSFER_RETRY_AFTER),
                ..error_data(
                    ControllerErrorCode::StaleOwnership,
                    ControllerRetryDisposition::SameConnection,
                )
            },
        ),
        ControllerSessionError::MainThreadClosed => main_thread_closed(),
        ControllerSessionError::TuiUnavailable => {
            tui_unavailable("TUI is unavailable for this controller launch".to_string())
        }
        ControllerSessionError::DifferentMainThread => controller_error(
            "controller enrollment targets a different main thread",
            error_data(
                ControllerErrorCode::DifferentThreadTarget,
                ControllerRetryDisposition::DoNotRetry,
            ),
        ),
        ControllerSessionError::StaleOwnership => controller_error(
            "controller ownership is stale",
            error_data(
                ControllerErrorCode::StaleOwnership,
                ControllerRetryDisposition::SameConnection,
            ),
        ),
    }
}

fn main_thread_unavailable(launch_state: ControllerLaunchState) -> JSONRPCErrorError {
    controller_error(
        "controller main thread is not available yet",
        ControllerErrorData {
            code: ControllerErrorCode::MainThreadUnavailable,
            retry: ControllerRetryDisposition::SameConnection,
            retry_after_ms: Some(Duration::from_millis(250).as_millis() as u64),
            launch_state: Some(launch_state),
            main_thread_id: None,
            session_id: None,
            authorization_epoch: None,
            owner_epoch: None,
        },
    )
}

fn main_thread_closed() -> JSONRPCErrorError {
    controller_error(
        "controller main thread is closed",
        ControllerErrorData {
            code: ControllerErrorCode::MainThreadClosed,
            retry: ControllerRetryDisposition::DoNotRetry,
            retry_after_ms: None,
            launch_state: Some(ControllerLaunchState::MainThreadClosed),
            main_thread_id: None,
            session_id: None,
            authorization_epoch: None,
            owner_epoch: None,
        },
    )
}

fn error_data(code: ControllerErrorCode, retry: ControllerRetryDisposition) -> ControllerErrorData {
    ControllerErrorData {
        code,
        retry,
        retry_after_ms: None,
        launch_state: None,
        main_thread_id: None,
        session_id: None,
        authorization_epoch: None,
        owner_epoch: None,
    }
}

fn controller_error(message: &str, data: ControllerErrorData) -> JSONRPCErrorError {
    JSONRPCErrorError {
        code: INVALID_REQUEST_ERROR_CODE,
        message: message.to_string(),
        data: Some(serde_json::to_value(data).unwrap_or(serde_json::Value::Null)),
    }
}

#[cfg(test)]
#[path = "controller_processor_tests.rs"]
mod tests;
