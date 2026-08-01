use super::AdmissionRule;
use super::CLIENT_REQUEST_ADMISSION;
use super::CONTINUATION_ADMISSION;
use super::ContinuationKind;
use super::RequiredAuthority;
use super::SERVER_REQUEST_RESPONSE_ADMISSION;
use super::TargetExtraction;
use super::admit_initialized_client_request;
use super::client_request_rule;
use super::continuation_rule_for;
use super::server_request_response_rule;
use crate::transport::ConnectionOrigin;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::ControllerRetryDisposition;
use codex_app_server_protocol::ServerRequest;
use pretty_assertions::assert_eq;
use std::collections::BTreeSet;

#[test]
fn client_request_registry_covers_every_protocol_method() {
    assert_method_sets_match(
        ClientRequest::METHOD_NAMES,
        CLIENT_REQUEST_ADMISSION
            .iter()
            .map(|entry| entry.method)
            .collect(),
    );
}

#[test]
fn server_request_response_registry_covers_every_protocol_method() {
    assert_method_sets_match(
        ServerRequest::METHOD_NAMES,
        SERVER_REQUEST_RESPONSE_ADMISSION
            .iter()
            .map(|entry| entry.method)
            .collect(),
    );
}

#[test]
fn method_registries_do_not_duplicate_entries() {
    assert_no_duplicates(
        CLIENT_REQUEST_ADMISSION
            .iter()
            .map(|entry| entry.method)
            .collect(),
    );
    assert_no_duplicates(
        SERVER_REQUEST_RESPONSE_ADMISSION
            .iter()
            .map(|entry| entry.method)
            .collect(),
    );
}

#[test]
fn unknown_methods_default_to_denied() {
    assert_eq!(client_request_rule("controller/newMutation"), None);
    assert_eq!(
        server_request_response_rule("item/unknown/requestApproval"),
        None
    );
}

#[test]
fn non_controller_origins_keep_existing_admission() {
    for origin in [
        ConnectionOrigin::Stdio,
        ConnectionOrigin::InProcess,
        ConnectionOrigin::WebSocket,
        ConnectionOrigin::RemoteControl,
    ] {
        assert_eq!(
            admit_initialized_client_request(origin, "thread/list"),
            Ok(()),
            "{origin:?} should preserve existing app-server behavior"
        );
    }
}

#[test]
fn external_controller_origin_allows_controller_control_plane_only() {
    assert_eq!(
        admit_initialized_client_request(
            ConnectionOrigin::ExternalController,
            "controller/requestParticipation"
        ),
        Ok(())
    );
    assert_eq!(
        admit_initialized_client_request(
            ConnectionOrigin::ExternalController,
            "controller/acquireControl"
        ),
        Ok(())
    );
    let error =
        admit_initialized_client_request(ConnectionOrigin::ExternalController, "thread/list")
            .expect_err("normal app-server interface should remain disabled in this slice");

    assert_controller_not_allowed(error);
}

#[test]
fn unclassified_initialized_methods_are_denied() {
    let error = admit_initialized_client_request(ConnectionOrigin::Stdio, "thread/newMethod")
        .expect_err("unclassified initialized methods should be denied");

    assert_controller_not_allowed(error);
}

#[test]
fn controller_handshake_methods_have_expected_authority() {
    assert_eq!(
        client_request_rule("controller/requestParticipation"),
        Some(rule(
            TargetExtraction::None,
            RequiredAuthority::PreParticipation
        ))
    );
    assert_eq!(
        client_request_rule("controller/acquireControl"),
        Some(rule(
            TargetExtraction::MainThreadOnly,
            RequiredAuthority::StandingSession
        ))
    );
    assert_eq!(
        client_request_rule("controller/releaseControl"),
        Some(rule(
            TargetExtraction::MainThreadOnly,
            RequiredAuthority::StandingSession
        ))
    );
    assert_eq!(
        client_request_rule("controller/signOff"),
        Some(rule(
            TargetExtraction::MainThreadOnly,
            RequiredAuthority::StandingSession
        ))
    );
}

#[test]
fn normal_main_thread_interface_is_split_by_authority() {
    assert_eq!(
        client_request_rule("thread/list"),
        Some(rule(
            TargetExtraction::CollectionFiltered,
            RequiredAuthority::StandingSession
        ))
    );
    assert_eq!(
        client_request_rule("thread/read"),
        Some(rule(
            TargetExtraction::ExactThread,
            RequiredAuthority::StandingSession
        ))
    );
    assert_eq!(
        client_request_rule("turn/start"),
        Some(rule(
            TargetExtraction::ExactThread,
            RequiredAuthority::ActiveOwner
        ))
    );
    assert_eq!(
        client_request_rule("turn/interrupt"),
        Some(rule(
            TargetExtraction::ExactThread,
            RequiredAuthority::ActiveOwner
        ))
    );
}

#[test]
fn context_changing_surfaces_remain_tui_only() {
    for method in [
        "thread/start",
        "thread/resume",
        "thread/fork",
        "thread/goal/set",
        "thread/goal/clear",
        "thread/settings/update",
        "thread/memoryMode/set",
        "memory/reset",
        "thread/compact/start",
        "thread/rollback",
        "thread/inject_items",
    ] {
        assert_eq!(
            client_request_rule(method).map(|rule| rule.required_authority),
            Some(RequiredAuthority::TuiOnly),
            "{method} must stay TUI-only until controller context safety is reviewed"
        );
    }
}

#[test]
fn server_request_responses_are_bound_to_interactive_owner() {
    for method in [
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
        "item/tool/requestUserInput",
        "mcpServer/elicitation/request",
        "item/permissions/requestApproval",
        "item/tool/call",
        "currentTime/read",
    ] {
        assert_eq!(
            server_request_response_rule(method),
            Some(rule(
                TargetExtraction::ExactThread,
                RequiredAuthority::ActiveOwner
            )),
            "{method} should require the current interactive owner"
        );
    }
}

#[test]
fn process_wide_server_requests_remain_tui_only() {
    for method in [
        "account/chatgptAuthTokens/refresh",
        "attestation/generate",
        "applyPatchApproval",
        "execCommandApproval",
    ] {
        assert_eq!(
            server_request_response_rule(method),
            Some(rule(TargetExtraction::None, RequiredAuthority::TuiOnly)),
            "{method} must not be routed to controllers"
        );
    }
}

#[test]
fn continuation_registry_covers_known_binding_kinds() {
    let expected: BTreeSet<_> = ContinuationKind::ALL.iter().copied().collect();
    let actual: BTreeSet<_> = CONTINUATION_ADMISSION
        .iter()
        .map(|entry| entry.kind)
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn continuation_rules_are_connection_bound() {
    assert_eq!(
        continuation_rule_for(ContinuationKind::Cursor),
        rule(
            TargetExtraction::MainThreadOnly,
            RequiredAuthority::StandingSession
        )
    );
    assert_eq!(
        continuation_rule_for(ContinuationKind::Subscription),
        rule(
            TargetExtraction::MainThreadOnly,
            RequiredAuthority::StandingSession
        )
    );
    assert_eq!(
        continuation_rule_for(ContinuationKind::ImplicitTarget),
        rule(
            TargetExtraction::MainThreadOnly,
            RequiredAuthority::StandingSession
        )
    );
    assert_eq!(
        continuation_rule_for(ContinuationKind::ResumeToken),
        rule(TargetExtraction::MainThreadOnly, RequiredAuthority::TuiOnly)
    );
}

fn assert_method_sets_match(expected: &[&'static str], actual: BTreeSet<&'static str>) {
    let expected: BTreeSet<_> = expected.iter().copied().collect();
    let missing: Vec<_> = expected.difference(&actual).copied().collect();
    let extra: Vec<_> = actual.difference(&expected).copied().collect();
    assert_eq!((missing, extra), (Vec::new(), Vec::new()));
}

fn assert_no_duplicates(methods: Vec<&'static str>) {
    let method_count = methods.len();
    let unique_method_count = methods.into_iter().collect::<BTreeSet<_>>().len();
    assert_eq!(method_count, unique_method_count);
}

fn assert_controller_not_allowed(error: codex_app_server_protocol::JSONRPCErrorError) {
    assert_eq!(error.code, crate::error_code::INVALID_REQUEST_ERROR_CODE);
    let data: ControllerErrorData =
        serde_json::from_value(error.data.expect("controller error should include data"))
            .expect("controller error data should deserialize");
    assert_eq!(data.code, ControllerErrorCode::ControllerNotAllowed);
    assert_eq!(data.retry, ControllerRetryDisposition::DoNotRetry);
}

fn rule(target: TargetExtraction, required_authority: RequiredAuthority) -> AdmissionRule {
    AdmissionRule {
        target,
        required_authority,
    }
}
