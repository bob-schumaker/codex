use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;

fn sample_capabilities() -> ControllerEffectiveCapabilities {
    ControllerEffectiveCapabilities {
        read_main_thread: true,
        subscribe_main_thread: true,
        acquire_control: true,
        release_control: true,
        mutate_main_thread: true,
        answer_prompts: true,
    }
}

fn sample_lease() -> ControllerLease {
    ControllerLease {
        lease_id: "lease-1".to_string(),
        owner_epoch: 7,
        expires_in_ms: Some(60_000),
    }
}

fn sample_session() -> ControllerSession {
    ControllerSession {
        session_id: "controller-session-1".to_string(),
        main_thread_id: "thread-1".to_string(),
        active_lease: Some(sample_lease()),
        authorization_epoch: 3,
        session_sequence: 11,
        effective_capabilities: sample_capabilities(),
        lease_expires_in_ms: Some(60_000),
        authorization_expires_in_ms: Some(300_000),
    }
}

#[test]
fn request_participation_response_serializes_required_nullable_session() {
    let response = ControllerRequestParticipationResponse {
        status: ControllerParticipationStatus::Rejected,
        session: None,
        denial: Some(ControllerParticipationDenial {
            message: "controller enrollment was not accepted".to_string(),
            data: ControllerErrorData {
                code: ControllerErrorCode::EnrollmentDenied,
                retry: ControllerRetryDisposition::DoNotRetry,
                retry_after_ms: None,
                launch_state: None,
                main_thread_id: None,
                session_id: None,
                authorization_epoch: None,
                owner_epoch: None,
            },
        }),
    };

    assert_eq!(
        serde_json::to_value(response).expect("response should serialize"),
        json!({
            "status": "rejected",
            "session": null,
            "denial": {
                "message": "controller enrollment was not accepted",
                "data": {
                    "code": "enrollment-denied",
                    "retry": "doNotRetry",
                    "retryAfterMs": null,
                    "launchState": null,
                    "mainThreadId": null,
                    "sessionId": null,
                    "authorizationEpoch": null,
                    "ownerEpoch": null
                }
            }
        })
    );
}

#[test]
fn controller_session_serializes_capabilities_and_nullable_lease() {
    let mut session = sample_session();
    session.active_lease = None;
    session.lease_expires_in_ms = None;

    assert_eq!(
        serde_json::to_value(session).expect("session should serialize"),
        json!({
            "sessionId": "controller-session-1",
            "mainThreadId": "thread-1",
            "activeLease": null,
            "authorizationEpoch": 3,
            "sessionSequence": 11,
            "effectiveCapabilities": {
                "readMainThread": true,
                "subscribeMainThread": true,
                "acquireControl": true,
                "releaseControl": true,
                "mutateMainThread": true,
                "answerPrompts": true
            },
            "leaseExpiresInMs": null,
            "authorizationExpiresInMs": 300000
        })
    );
}

#[test]
fn controller_notifications_serialize_control_plane_fields() {
    let notification = ControllerControlOwnershipChangedNotification {
        session_id: "controller-session-1".to_string(),
        main_thread_id: "thread-1".to_string(),
        reason: ControllerControlOwnershipChangedReason::ReclaimedByTui,
        authorization_epoch: 3,
        owner_epoch: 8,
        session_sequence: 12,
        active_lease: None,
    };

    assert_eq!(
        serde_json::to_value(notification).expect("notification should serialize"),
        json!({
            "sessionId": "controller-session-1",
            "mainThreadId": "thread-1",
            "reason": "reclaimedByTui",
            "authorizationEpoch": 3,
            "ownerEpoch": 8,
            "sessionSequence": 12,
            "activeLease": null
        })
    );
}

#[test]
fn controller_error_data_uses_canonical_kebab_case_codes() {
    let data = ControllerErrorData {
        code: ControllerErrorCode::MainThreadUnavailable,
        retry: ControllerRetryDisposition::SameConnection,
        retry_after_ms: Some(250),
        launch_state: Some(ControllerLaunchState::Starting),
        main_thread_id: None,
        session_id: Some("controller-session-1".to_string()),
        authorization_epoch: Some(3),
        owner_epoch: Some(8),
    };

    assert_eq!(
        serde_json::to_value(data).expect("error data should serialize"),
        json!({
            "code": "main-thread-unavailable",
            "retry": "sameConnection",
            "retryAfterMs": 250,
            "launchState": "starting",
            "mainThreadId": null,
            "sessionId": "controller-session-1",
            "authorizationEpoch": 3,
            "ownerEpoch": 8
        })
    );
}
