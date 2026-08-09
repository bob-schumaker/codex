use super::*;
use crate::controller_admission::client_request_rule;
use codex_app_server_protocol::AdditionalContextEntry;
use codex_app_server_protocol::AdditionalContextKind;
use codex_app_server_protocol::ApprovalsReviewer;
use codex_app_server_protocol::AskForApproval;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::McpResourceReadParams;
use codex_app_server_protocol::ReviewDelivery;
use codex_app_server_protocol::ReviewStartParams;
use codex_app_server_protocol::ReviewTarget;
use codex_app_server_protocol::SandboxMode;
use codex_app_server_protocol::SandboxPolicy;
use codex_app_server_protocol::ThreadBackgroundTerminalsListParams;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadLoadedListParams;
use codex_app_server_protocol::ThreadRealtimeAppendTextParams;
use codex_app_server_protocol::ThreadRealtimeInitialItem;
use codex_app_server_protocol::ThreadRealtimeStartParams;
use codex_app_server_protocol::ThreadRealtimeStartTransport;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadSearchOccurrencesParams;
use codex_app_server_protocol::ThreadSearchParams;
use codex_app_server_protocol::ThreadSectionListParams;
use codex_app_server_protocol::ThreadSectionMoveParams;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadUnsubscribeParams;
use codex_app_server_protocol::TurnInterruptParams;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnSteerParams;
use codex_app_server_protocol::UserInput;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::MultiAgentMode;
use codex_protocol::config_types::Personality;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::config_types::Settings;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::CodexResponseHandoffMode;
use codex_protocol::protocol::ConversationTextRole;
use codex_protocol::protocol::RealtimeConversationVersion;
use codex_protocol::protocol::RealtimeOutputModality;
use codex_protocol::protocol::RealtimeVoice;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;

#[test]
fn thread_resume_extracts_exact_controller_thread_target() {
    let thread_id = "00000000-0000-0000-0000-000000000123".to_string();
    assert_exact_thread_target(
        ClientRequest::ThreadResume {
            request_id: RequestId::Integer(1),
            params: safe_thread_resume_params(thread_id.clone()),
        },
        "thread/resume",
        &thread_id,
    );
}

#[test]
fn controller_thread_resume_allows_read_shape_params_only() {
    let mut safe_params = safe_thread_resume_params("thread-1");
    safe_params.exclude_turns = true;
    assert_eq!(
        reject_controller_tui_only_params(&ClientRequest::ThreadResume {
            request_id: RequestId::Integer(11),
            params: safe_params,
        }),
        Ok(())
    );

    let unsafe_params = [
        unsafe_thread_resume_params("history", |params| params.history = Some(Vec::new())),
        unsafe_thread_resume_params("path", |params| {
            params.path = Some(PathBuf::from("/tmp/thread.jsonl"));
        }),
        unsafe_thread_resume_params("model", |params| {
            params.model = Some("gpt-test".to_string())
        }),
        unsafe_thread_resume_params("model_provider", |params| {
            params.model_provider = Some("provider".to_string());
        }),
        unsafe_thread_resume_params("service_tier", |params| params.service_tier = Some(None)),
        unsafe_thread_resume_params("cwd", |params| params.cwd = Some("/tmp".to_string())),
        unsafe_thread_resume_params("runtime_workspace_roots", |params| {
            params.runtime_workspace_roots = Some(Vec::new());
        }),
        unsafe_thread_resume_params("approval_policy", |params| {
            params.approval_policy = Some(codex_app_server_protocol::AskForApproval::Never);
        }),
        unsafe_thread_resume_params("approvals_reviewer", |params| {
            params.approvals_reviewer = Some(codex_app_server_protocol::ApprovalsReviewer::User);
        }),
        unsafe_thread_resume_params("sandbox", |params| {
            params.sandbox = Some(SandboxMode::ReadOnly);
        }),
        unsafe_thread_resume_params("permissions", |params| {
            params.permissions = Some("read-only".to_string());
        }),
        unsafe_thread_resume_params("config", |params| params.config = Some(HashMap::new())),
        unsafe_thread_resume_params("base_instructions", |params| {
            params.base_instructions = Some("base".to_string());
        }),
        unsafe_thread_resume_params("developer_instructions", |params| {
            params.developer_instructions = Some("developer".to_string());
        }),
        unsafe_thread_resume_params("personality", |params| {
            params.personality = Some(Personality::Pragmatic);
        }),
    ];

    for (field_name, params) in unsafe_params {
        let error = reject_controller_tui_only_params(&ClientRequest::ThreadResume {
            request_id: RequestId::Integer(12),
            params,
        })
        .expect_err("unsafe controller thread/resume params should be rejected");
        let data: ControllerErrorData =
            serde_json::from_value(error.data.expect("controller error should include data"))
                .expect("controller error data should deserialize");
        assert_eq!(
            data.code,
            ControllerErrorCode::ControllerNotAllowed,
            "{field_name} must stay TUI-only for external-controller thread/resume"
        );
    }
}

#[test]
fn controller_turn_start_rejects_context_and_config_overrides() {
    let mut safe_params = safe_turn_start_params("thread-1");
    safe_params.client_user_message_id = Some("client-message-1".to_string());
    safe_params.responsesapi_client_metadata = Some(HashMap::from([(
        "controllerMetadata".to_string(),
        "allowed".to_string(),
    )]));
    assert_eq!(
        reject_controller_tui_only_params(&ClientRequest::TurnStart {
            request_id: RequestId::Integer(13),
            params: safe_params,
        }),
        Ok(())
    );

    let unsafe_params = [
        unsafe_turn_start_params("additional_context", |params| {
            params.additional_context = Some(HashMap::from([(
                "context".to_string(),
                AdditionalContextEntry {
                    value: "injected context".to_string(),
                    kind: AdditionalContextKind::Application,
                },
            )]));
        }),
        unsafe_turn_start_params("environments", |params| {
            params.environments = Some(Vec::new())
        }),
        unsafe_turn_start_params("cwd", |params| params.cwd = Some(PathBuf::from("/tmp"))),
        unsafe_turn_start_params("runtime_workspace_roots", |params| {
            params.runtime_workspace_roots = Some(Vec::new());
        }),
        unsafe_turn_start_params("approval_policy", |params| {
            params.approval_policy = Some(AskForApproval::Never);
        }),
        unsafe_turn_start_params("approvals_reviewer", |params| {
            params.approvals_reviewer = Some(ApprovalsReviewer::User);
        }),
        unsafe_turn_start_params("sandbox_policy", |params| {
            params.sandbox_policy = Some(SandboxPolicy::DangerFullAccess);
        }),
        unsafe_turn_start_params("permissions", |params| {
            params.permissions = Some("read-only".to_string());
        }),
        unsafe_turn_start_params("model", |params| {
            params.model = Some("gpt-test".to_string());
        }),
        unsafe_turn_start_params("service_tier", |params| {
            params.service_tier = Some(None);
        }),
        unsafe_turn_start_params("effort", |params| {
            params.effort = Some(ReasoningEffort::Low);
        }),
        unsafe_turn_start_params("summary", |params| {
            params.summary = Some(ReasoningSummary::Concise);
        }),
        unsafe_turn_start_params("personality", |params| {
            params.personality = Some(Personality::Pragmatic);
        }),
        unsafe_turn_start_params("output_schema", |params| {
            params.output_schema = Some(serde_json::json!({ "type": "object" }));
        }),
        unsafe_turn_start_params("collaboration_mode", |params| {
            params.collaboration_mode = Some(CollaborationMode {
                mode: ModeKind::Default,
                settings: Settings {
                    model: "gpt-test".to_string(),
                    reasoning_effort: None,
                    developer_instructions: Some("developer".to_string()),
                },
            });
        }),
        unsafe_turn_start_params("multi_agent_mode", |params| {
            params.multi_agent_mode = Some(MultiAgentMode::Proactive);
        }),
    ];

    for (field_name, params) in unsafe_params {
        let error = reject_controller_tui_only_params(&ClientRequest::TurnStart {
            request_id: RequestId::Integer(14),
            params,
        })
        .expect_err("unsafe controller turn/start params should be rejected");
        let data: ControllerErrorData =
            serde_json::from_value(error.data.expect("controller error should include data"))
                .expect("controller error data should deserialize");
        assert_eq!(
            data.code,
            ControllerErrorCode::ControllerNotAllowed,
            "{field_name} must stay TUI-only for external-controller turn/start"
        );
    }
}

#[test]
fn controller_turn_steer_rejects_additional_context_override() {
    let mut safe_params = safe_turn_steer_params("thread-1");
    safe_params.client_user_message_id = Some("client-message-1".to_string());
    safe_params.responsesapi_client_metadata = Some(HashMap::from([(
        "controllerMetadata".to_string(),
        "allowed".to_string(),
    )]));
    assert_eq!(
        reject_controller_tui_only_params(&ClientRequest::TurnSteer {
            request_id: RequestId::Integer(15),
            params: safe_params,
        }),
        Ok(())
    );

    let mut unsafe_params = safe_turn_steer_params("thread-1");
    unsafe_params.additional_context = Some(HashMap::from([(
        "context".to_string(),
        AdditionalContextEntry {
            value: "injected context".to_string(),
            kind: AdditionalContextKind::Application,
        },
    )]));
    let error = reject_controller_tui_only_params(&ClientRequest::TurnSteer {
        request_id: RequestId::Integer(16),
        params: unsafe_params,
    })
    .expect_err("controller turn/steer additional context should be rejected");
    let data: ControllerErrorData =
        serde_json::from_value(error.data.expect("controller error should include data"))
            .expect("controller error data should deserialize");
    assert_eq!(data.code, ControllerErrorCode::ControllerNotAllowed);
}

#[test]
fn controller_thread_section_move_rejects_before_thread_target() {
    assert_eq!(
        reject_controller_tui_only_params(&ClientRequest::ThreadSectionMove {
            request_id: RequestId::Integer(17),
            params: ThreadSectionMoveParams {
                thread_id: "thread-1".to_string(),
                section_id: Some("section-1".to_string()),
                before_thread_id: None,
            },
        }),
        Ok(())
    );

    assert_eq!(
        reject_controller_tui_only_params(&ClientRequest::ThreadSectionMove {
            request_id: RequestId::Integer(18),
            params: ThreadSectionMoveParams {
                thread_id: "thread-1".to_string(),
                section_id: None,
                before_thread_id: None,
            },
        }),
        Ok(())
    );

    let error = reject_controller_tui_only_params(&ClientRequest::ThreadSectionMove {
        request_id: RequestId::Integer(19),
        params: ThreadSectionMoveParams {
            thread_id: "thread-1".to_string(),
            section_id: Some("section-1".to_string()),
            before_thread_id: Some("thread-2".to_string()),
        },
    })
    .expect_err("controller section move must not target another thread");
    let data: ControllerErrorData =
        serde_json::from_value(error.data.expect("controller error should include data"))
            .expect("controller error data should deserialize");
    assert_eq!(data.code, ControllerErrorCode::ControllerNotAllowed);
}

#[test]
fn controller_realtime_start_allows_input_transport_shape_only() {
    let mut safe_params = safe_realtime_start_params("thread-1");
    safe_params.realtime_session_id = Some("sess-1".to_string());
    safe_params.transport = Some(ThreadRealtimeStartTransport::Webrtc {
        sdp: "v=0".to_string(),
    });
    safe_params.voice = Some(RealtimeVoice::Alloy);
    assert_eq!(
        reject_controller_tui_only_params(&ClientRequest::ThreadRealtimeStart {
            request_id: RequestId::Integer(17),
            params: safe_params,
        }),
        Ok(())
    );

    let unsafe_params = [
        unsafe_realtime_start_params("client_managed_handoffs", |params| {
            params.client_managed_handoffs = Some(true);
        }),
        unsafe_realtime_start_params("delegation_ack_filler", |params| {
            params.delegation_ack_filler = Some(false);
        }),
        unsafe_realtime_start_params("flush_transcript_tail_on_session_end", |params| {
            params.flush_transcript_tail_on_session_end = Some(true);
        }),
        unsafe_realtime_start_params("codex_responses_as_items", |params| {
            params.codex_responses_as_items = Some(true);
        }),
        unsafe_realtime_start_params("codex_response_item_prefix", |params| {
            params.codex_response_item_prefix = Some("[codex]".to_string());
        }),
        unsafe_realtime_start_params("codex_response_handoff_mode", |params| {
            params.codex_response_handoff_mode = Some(CodexResponseHandoffMode::Commentary);
        }),
        unsafe_realtime_start_params("codex_response_handoff_channel_prefixes", |params| {
            params.codex_response_handoff_channel_prefixes = Some(BTreeMap::from([(
                "final".to_string(),
                vec!["F".to_string()],
            )]));
        }),
        unsafe_realtime_start_params("model", |params| {
            params.model = Some("realtime-test".to_string());
        }),
        unsafe_realtime_start_params("include_startup_context", |params| {
            params.include_startup_context = Some(false);
        }),
        unsafe_realtime_start_params("initial_items", |params| {
            params.initial_items = Some(vec![ThreadRealtimeInitialItem {
                role: ConversationTextRole::Developer,
                text: "inject context".to_string(),
            }]);
        }),
        unsafe_realtime_start_params("realtime_start_instructions", |params| {
            params.realtime_start_instructions = Some("start differently".to_string());
        }),
        unsafe_realtime_start_params("realtime_end_instructions", |params| {
            params.realtime_end_instructions = Some("end differently".to_string());
        }),
        unsafe_realtime_start_params("prompt", |params| {
            params.prompt = Some(Some("custom realtime prompt".to_string()));
        }),
        unsafe_realtime_start_params("version", |params| {
            params.version = Some(RealtimeConversationVersion::V3);
        }),
    ];

    for (field_name, params) in unsafe_params {
        let error = reject_controller_tui_only_params(&ClientRequest::ThreadRealtimeStart {
            request_id: RequestId::Integer(18),
            params,
        })
        .expect_err("unsafe controller thread/realtime/start params should be rejected");
        let data: ControllerErrorData =
            serde_json::from_value(error.data.expect("controller error should include data"))
                .expect("controller error data should deserialize");
        assert_eq!(
            data.code,
            ControllerErrorCode::ControllerNotAllowed,
            "{field_name} must stay TUI-only for external-controller thread/realtime/start"
        );
    }
}

#[test]
fn controller_realtime_append_text_allows_user_role_only() {
    assert_eq!(
        reject_controller_tui_only_params(&ClientRequest::ThreadRealtimeAppendText {
            request_id: RequestId::Integer(19),
            params: ThreadRealtimeAppendTextParams {
                thread_id: "thread-1".to_string(),
                text: "user input".to_string(),
                role: ConversationTextRole::User,
            },
        }),
        Ok(())
    );

    for role in [
        ConversationTextRole::Developer,
        ConversationTextRole::Assistant,
    ] {
        let error = reject_controller_tui_only_params(&ClientRequest::ThreadRealtimeAppendText {
            request_id: RequestId::Integer(20),
            params: ThreadRealtimeAppendTextParams {
                thread_id: "thread-1".to_string(),
                text: "not user input".to_string(),
                role,
            },
        })
        .expect_err("controller realtime append text roles other than user should be rejected");
        let data: ControllerErrorData =
            serde_json::from_value(error.data.expect("controller error should include data"))
                .expect("controller error data should deserialize");
        assert_eq!(
            data.code,
            ControllerErrorCode::ControllerNotAllowed,
            "{role:?} must stay TUI-only for external-controller thread/realtime/appendText"
        );
    }
}

#[test]
fn controller_review_start_rejects_detached_delivery() {
    let mut inline_params = safe_review_start_params("thread-1");
    inline_params.delivery = Some(ReviewDelivery::Inline);
    assert_eq!(
        reject_controller_tui_only_params(&ClientRequest::ReviewStart {
            request_id: RequestId::Integer(17),
            params: inline_params,
        }),
        Ok(())
    );

    let mut detached_params = safe_review_start_params("thread-1");
    detached_params.delivery = Some(ReviewDelivery::Detached);
    let error = reject_controller_tui_only_params(&ClientRequest::ReviewStart {
        request_id: RequestId::Integer(18),
        params: detached_params,
    })
    .expect_err("controller detached review/start should be rejected");
    let data: ControllerErrorData =
        serde_json::from_value(error.data.expect("controller error should include data"))
            .expect("controller error data should deserialize");
    assert_eq!(data.code, ControllerErrorCode::ControllerNotAllowed);

    assert_eq!(
        reject_controller_tui_only_params(&ClientRequest::ReviewStart {
            request_id: RequestId::Integer(19),
            params: safe_review_start_params("thread-1"),
        }),
        Ok(())
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
    assert_exact_thread_target(
        ClientRequest::ThreadUnsubscribe {
            request_id: RequestId::Integer(4),
            params: ThreadUnsubscribeParams {
                thread_id: thread_id.clone(),
            },
        },
        "thread/unsubscribe",
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
fn collection_filtered_controller_targets_cover_admitted_collections() {
    assert_collection_filtered_target(
        ClientRequest::ThreadList {
            request_id: RequestId::Integer(6),
            params: ThreadListParams {
                cursor: None,
                limit: None,
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
        },
        "thread/list",
    );
    assert_collection_filtered_target(
        ClientRequest::ThreadSectionList {
            request_id: RequestId::Integer(7),
            params: ThreadSectionListParams {
                cursor: None,
                limit: None,
            },
        },
        "threadSection/list",
    );
    assert_collection_filtered_target(
        ClientRequest::ThreadSearch {
            request_id: RequestId::Integer(8),
            params: ThreadSearchParams {
                cursor: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
                source_kinds: None,
                archived: None,
                search_term: "needle".to_string(),
            },
        },
        "thread/search",
    );
    assert_collection_filtered_target(
        ClientRequest::ThreadLoadedList {
            request_id: RequestId::Integer(9),
            params: ThreadLoadedListParams {
                cursor: None,
                limit: None,
            },
        },
        "thread/loaded/list",
    );
}

#[test]
fn optional_exact_controller_thread_target_must_be_present() {
    let rule = client_request_rule("mcpServer/resource/read")
        .expect("mcpServer/resource/read should be admitted");
    let request = ClientRequest::McpResourceRead {
        request_id: RequestId::Integer(10),
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

fn assert_collection_filtered_target(request: ClientRequest, method: &str) {
    let rule = client_request_rule(method).expect("method should be admitted");
    let target = controller_request_target(&request, rule).expect("target extraction should pass");

    match target {
        ControllerRequestTarget::CollectionFiltered => {}
        ControllerRequestTarget::None | ControllerRequestTarget::ExactThread(_) => {
            panic!("{method} should extract a collection-filtered target")
        }
    }
}

fn rule(required_authority: RequiredAuthority) -> AdmissionRule {
    AdmissionRule {
        target: TargetExtraction::ExactThread,
        required_authority,
    }
}

fn safe_thread_resume_params(thread_id: impl Into<String>) -> ThreadResumeParams {
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

fn unsafe_thread_resume_params(
    field_name: &'static str,
    mutate: impl FnOnce(&mut ThreadResumeParams),
) -> (&'static str, ThreadResumeParams) {
    let mut params = safe_thread_resume_params("thread-1");
    mutate(&mut params);
    (field_name, params)
}

fn safe_turn_start_params(thread_id: impl Into<String>) -> TurnStartParams {
    TurnStartParams {
        thread_id: thread_id.into(),
        client_user_message_id: None,
        input: vec![UserInput::Text {
            text: "controller input".to_string(),
            text_elements: Vec::new(),
        }],
        responsesapi_client_metadata: None,
        additional_context: None,
        environments: None,
        cwd: None,
        runtime_workspace_roots: None,
        approval_policy: None,
        approvals_reviewer: None,
        sandbox_policy: None,
        permissions: None,
        model: None,
        service_tier: None,
        effort: None,
        summary: None,
        personality: None,
        output_schema: None,
        collaboration_mode: None,
        multi_agent_mode: None,
    }
}

fn unsafe_turn_start_params(
    field_name: &'static str,
    mutate: impl FnOnce(&mut TurnStartParams),
) -> (&'static str, TurnStartParams) {
    let mut params = safe_turn_start_params("thread-1");
    mutate(&mut params);
    (field_name, params)
}

fn safe_turn_steer_params(thread_id: impl Into<String>) -> TurnSteerParams {
    TurnSteerParams {
        thread_id: thread_id.into(),
        client_user_message_id: None,
        input: vec![UserInput::Text {
            text: "controller follow-up".to_string(),
            text_elements: Vec::new(),
        }],
        responsesapi_client_metadata: None,
        additional_context: None,
        expected_turn_id: "turn-1".to_string(),
    }
}

fn safe_realtime_start_params(thread_id: impl Into<String>) -> ThreadRealtimeStartParams {
    ThreadRealtimeStartParams {
        thread_id: thread_id.into(),
        client_managed_handoffs: None,
        delegation_ack_filler: None,
        flush_transcript_tail_on_session_end: None,
        codex_responses_as_items: None,
        codex_response_item_prefix: None,
        codex_response_handoff_mode: None,
        codex_response_handoff_channel_prefixes: None,
        model: None,
        output_modality: RealtimeOutputModality::Text,
        include_startup_context: None,
        initial_items: None,
        realtime_start_instructions: None,
        realtime_end_instructions: None,
        prompt: None,
        realtime_session_id: None,
        transport: None,
        version: None,
        voice: None,
    }
}

fn unsafe_realtime_start_params(
    field_name: &'static str,
    mutate: impl FnOnce(&mut ThreadRealtimeStartParams),
) -> (&'static str, ThreadRealtimeStartParams) {
    let mut params = safe_realtime_start_params("thread-1");
    mutate(&mut params);
    (field_name, params)
}

fn safe_review_start_params(thread_id: impl Into<String>) -> ReviewStartParams {
    ReviewStartParams {
        thread_id: thread_id.into(),
        target: ReviewTarget::Custom {
            instructions: "review this".to_string(),
        },
        delivery: None,
    }
}
