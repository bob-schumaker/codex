use super::*;
use crate::controller_admission::client_request_rule;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::McpResourceReadParams;
use codex_app_server_protocol::ThreadBackgroundTerminalsListParams;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadSearchOccurrencesParams;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::TurnInterruptParams;
use pretty_assertions::assert_eq;

#[test]
fn thread_resume_extracts_exact_controller_thread_target() {
    let thread_id = "00000000-0000-0000-0000-000000000123".to_string();
    assert_exact_thread_target(
        ClientRequest::ThreadResume {
            request_id: RequestId::Integer(1),
            params: ThreadResumeParams {
                thread_id: thread_id.clone(),
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
            },
        },
        "thread/resume",
        &thread_id,
    );
}

#[test]
fn exact_controller_thread_target_uses_serialization_scope() {
    let thread_id = "00000000-0000-0000-0000-000000000456".to_string();
    assert_exact_thread_target(
        ClientRequest::TurnInterrupt {
            request_id: RequestId::Integer(2),
            params: TurnInterruptParams {
                thread_id: thread_id.clone(),
                turn_id: "turn-1".to_string(),
            },
        },
        "turn/interrupt",
        &thread_id,
    );
    assert_exact_thread_target(
        ClientRequest::ThreadBackgroundTerminalsList {
            request_id: RequestId::Integer(3),
            params: ThreadBackgroundTerminalsListParams {
                thread_id: thread_id.clone(),
                cursor: None,
                limit: None,
            },
        },
        "thread/backgroundTerminals/list",
        &thread_id,
    );
}

#[test]
fn exact_controller_thread_target_handles_concurrent_read_methods() {
    let thread_id = "00000000-0000-0000-0000-000000000789".to_string();
    assert_exact_thread_target(
        ClientRequest::ThreadTurnsList {
            request_id: RequestId::Integer(4),
            params: ThreadTurnsListParams {
                thread_id: thread_id.clone(),
                cursor: None,
                limit: None,
                sort_direction: None,
                items_view: None,
            },
        },
        "thread/turns/list",
        &thread_id,
    );
    assert_exact_thread_target(
        ClientRequest::ThreadSearchOccurrences {
            request_id: RequestId::Integer(5),
            params: ThreadSearchOccurrencesParams {
                thread_id: thread_id.clone(),
                search_term: "needle".to_string(),
                cursor: None,
                limit: None,
            },
        },
        "thread/searchOccurrences",
        &thread_id,
    );
}

#[test]
fn optional_exact_controller_thread_target_must_be_present() {
    let rule = client_request_rule("mcpServer/resource/read")
        .expect("mcpServer/resource/read should be admitted");
    let request = ClientRequest::McpResourceRead {
        request_id: RequestId::Integer(6),
        params: McpResourceReadParams {
            thread_id: None,
            server: "server".to_string(),
            uri: "resource://item".to_string(),
        },
    };

    let error = match controller_request_target(&request, rule) {
        Ok(_) => panic!("controller resource reads must name the granted thread"),
        Err(error) => error,
    };
    let data: ControllerErrorData =
        serde_json::from_value(error.data.expect("controller error should include data"))
            .expect("controller error data should deserialize");

    assert_eq!(data.code, ControllerErrorCode::ControllerNotAllowed);
}

#[test]
fn primary_thread_input_reclaims_for_owner_and_tui_only_thread_mutations() {
    let thread_id = "thread-1".to_string();
    let scope = ClientRequestSerializationScope::Thread {
        thread_id: thread_id.clone(),
    };

    assert_eq!(
        primary_input_reclaim_thread_id(
            ConnectionOrigin::InProcess,
            rule(RequiredAuthority::ActiveOwner),
            Some(&scope),
        ),
        Some(thread_id.as_str())
    );
    assert_eq!(
        primary_input_reclaim_thread_id(
            ConnectionOrigin::InProcess,
            rule(RequiredAuthority::TuiOnly),
            Some(&scope),
        ),
        Some(thread_id.as_str())
    );
    assert_eq!(
        primary_input_reclaim_thread_id(
            ConnectionOrigin::InProcess,
            rule(RequiredAuthority::StandingSession),
            Some(&scope),
        ),
        None
    );
    assert_eq!(
        primary_input_reclaim_thread_id(
            ConnectionOrigin::ExternalController,
            rule(RequiredAuthority::TuiOnly),
            Some(&scope),
        ),
        None
    );
}

fn assert_exact_thread_target(request: ClientRequest, method: &str, expected_thread_id: &str) {
    let rule = client_request_rule(method).expect("method should be admitted");
    let target = controller_request_target(&request, rule).expect("target extraction should pass");

    match target {
        ControllerRequestTarget::ExactThread(actual) => assert_eq!(actual, expected_thread_id),
        ControllerRequestTarget::None | ControllerRequestTarget::CollectionFiltered => {
            panic!("{method} should extract an exact thread target")
        }
    }
}

fn rule(required_authority: RequiredAuthority) -> AdmissionRule {
    AdmissionRule {
        target: TargetExtraction::ExactThread,
        required_authority,
    }
}
