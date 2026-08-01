use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

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
use codex_app_server_protocol::JSONRPCErrorError;
use codex_protocol::ThreadId;

use crate::controller_admission::AdmissionRule;
use crate::controller_admission::RequiredAuthority;
use crate::controller_admission::TargetExtraction;
use crate::controller_enrollment::ControllerCredentialProof;
use crate::controller_enrollment::ControllerDisplayClaims;
use crate::controller_enrollment::ControllerEnrollmentError;
use crate::controller_enrollment::ControllerEnrollmentPolicy;
use crate::controller_enrollment::ControllerEnrollmentSource;
use crate::controller_enrollment::ControllerEnrollmentVerifier;
use crate::controller_enrollment::ControllerParticipationEvidence;
use crate::controller_session::ControllerSessionClock;
use crate::controller_session::ControllerSessionConfig;
use crate::controller_session::ControllerSessionCoordinator;
use crate::controller_session::ControllerSessionError;
use crate::error_code::INVALID_REQUEST_ERROR_CODE;
use crate::transport::ConnectionId;
use crate::transport::ConnectionOrigin;

const CONTROLLER_TRANSFER_RETRY_AFTER: u64 = 50;

#[derive(Clone)]
pub(crate) struct ControllerRequestProcessor {
    state: Arc<Mutex<ControllerProcessorState>>,
    enrollment_source: Arc<dyn ControllerEnrollmentSource>,
    enrollment_policy: ControllerEnrollmentPolicy,
    clock: ControllerSessionClock,
    session_config: ControllerSessionConfig,
}

struct ControllerProcessorState {
    coordinator: Option<ControllerSessionCoordinator>,
    launch_state: ControllerLaunchState,
}

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
        enrollment_source: Arc<dyn ControllerEnrollmentSource>,
        enrollment_policy: ControllerEnrollmentPolicy,
        clock: ControllerSessionClock,
        session_config: ControllerSessionConfig,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ControllerProcessorState {
                coordinator: None,
                launch_state: ControllerLaunchState::Starting,
            })),
            enrollment_source,
            enrollment_policy,
            clock,
            session_config,
        }
    }

    pub(crate) fn register_main_thread(&self, main_thread_id: ThreadId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.coordinator.is_none() {
            state.coordinator = Some(ControllerSessionCoordinator::new(
                main_thread_id,
                self.clock.clone(),
                self.session_config,
            ));
        }
    }

    pub(crate) fn request_participation(
        &self,
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
        credential_proof: Option<ControllerCredentialProof>,
        params: ControllerRequestParticipationParams,
    ) -> Result<ControllerRequestParticipationResponse, JSONRPCErrorError> {
        require_external_controller_origin(origin)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(coordinator) = state.coordinator.as_mut() else {
            return Err(main_thread_unavailable(state.launch_state.clone()));
        };
        let Some(main_thread_id) = coordinator.main_thread_id() else {
            return Err(main_thread_closed());
        };

        let verifier = ControllerEnrollmentVerifier::new(
            Arc::clone(&self.enrollment_source),
            self.enrollment_policy,
            self.clock.clone(),
        );
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
    }

    pub(crate) fn acquire_control(
        &self,
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
    ) -> Result<ControllerAcquireControlResponse, JSONRPCErrorError> {
        require_external_controller_origin(origin)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(coordinator) = state.coordinator.as_mut() else {
            return Err(main_thread_unavailable(state.launch_state.clone()));
        };
        coordinator
            .acquire_control(connection_id)
            .map(|session| ControllerAcquireControlResponse { session })
            .map_err(controller_session_error)
    }

    pub(crate) fn release_control(
        &self,
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
    ) -> Result<ControllerReleaseControlResponse, JSONRPCErrorError> {
        require_external_controller_origin(origin)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(coordinator) = state.coordinator.as_mut() else {
            return Err(main_thread_unavailable(state.launch_state.clone()));
        };
        coordinator
            .release_control(connection_id)
            .map(|session| ControllerReleaseControlResponse { session })
            .map_err(controller_session_error)
    }

    pub(crate) fn sign_off(
        &self,
        connection_id: ConnectionId,
        origin: ConnectionOrigin,
    ) -> Result<ControllerSignOffResponse, JSONRPCErrorError> {
        require_external_controller_origin(origin)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(coordinator) = state.coordinator.as_mut() else {
            return Err(main_thread_unavailable(state.launch_state.clone()));
        };
        coordinator.revoke_session(connection_id);
        Ok(ControllerSignOffResponse {})
    }

    pub(crate) fn authorize_normal_request(
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

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(coordinator) = state.coordinator.as_mut() else {
            return Err(main_thread_unavailable(state.launch_state.clone()));
        };

        let main_thread_id = match rule.required_authority {
            RequiredAuthority::StandingSession => coordinator
                .require_standing_session(connection_id)
                .map_err(controller_session_error)?,
            RequiredAuthority::ActiveOwner => coordinator
                .require_active_owner(connection_id)
                .map_err(controller_session_error)?,
            RequiredAuthority::PreParticipation | RequiredAuthority::TuiOnly => unreachable!(),
        };

        authorize_target(rule.target, target, main_thread_id)
    }

    pub(crate) fn reclaim_for_primary_thread_input(
        &self,
        thread_id: &str,
    ) -> Result<(), JSONRPCErrorError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(coordinator) = state.coordinator.as_mut() else {
            return Ok(());
        };
        let Some(main_thread_id) = coordinator.main_thread_id() else {
            return Ok(());
        };
        if main_thread_id.to_string() != thread_id {
            return Ok(());
        }

        coordinator
            .reclaim_for_tui()
            .map_err(controller_session_error)
    }
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
        ControllerSessionError::TuiUnavailable => controller_error(
            "TUI is unavailable for this controller launch",
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
        ),
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
