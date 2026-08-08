use super::*;
use codex_app_server_protocol::ApprovalsReviewer;
use codex_app_server_protocol::AskForApproval;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SandboxPolicy;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadItemsListParams;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadSearchOccurrence;
use codex_app_server_protocol::ThreadSearchOccurrencesParams;
use codex_app_server_protocol::ThreadSearchOccurrencesResponse;
use codex_app_server_protocol::ThreadSearchParams;
use codex_app_server_protocol::ThreadSearchResponse;
use codex_app_server_protocol::ThreadSearchTextRange;
use codex_app_server_protocol::ThreadSectionListParams;
use codex_app_server_protocol::ThreadSectionListResponse;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnsPage;
use codex_protocol::config_types::MultiAgentMode;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;

const CONTROLLER_CONNECTION_ID: ConnectionId = ConnectionId(8);
const OTHER_CONTROLLER_CONNECTION_ID: ConnectionId = ConnectionId(9);
const MAIN_THREAD_ID: &str = "00000000-0000-7000-8000-000000000001";
const OTHER_THREAD_ID: &str = "00000000-0000-7000-8000-000000000002";

fn controller_authorization(thread_id: &str) -> ControllerNormalAuthorization {
    ControllerNormalAuthorization {
        main_thread_id: thread_id.to_string(),
        filter_collection_to_main_thread: false,
    }
}

fn thread_turns_request(cursor: Option<String>) -> ClientRequest {
    ClientRequest::ThreadTurnsList {
        request_id: RequestId::Integer(1),
        params: ThreadTurnsListParams {
            thread_id: MAIN_THREAD_ID.to_string(),
            cursor,
            limit: None,
            sort_direction: Some(SortDirection::Desc),
            items_view: Some(TurnItemsView::Summary),
        },
    }
}

fn thread_items_request(cursor: Option<String>) -> ClientRequest {
    ClientRequest::ThreadItemsList {
        request_id: RequestId::Integer(2),
        params: ThreadItemsListParams {
            thread_id: MAIN_THREAD_ID.to_string(),
            turn_id: None,
            cursor,
            limit: None,
            sort_direction: Some(SortDirection::Desc),
        },
    }
}

fn thread_search_occurrences_request(cursor: Option<String>) -> ClientRequest {
    ClientRequest::ThreadSearchOccurrences {
        request_id: RequestId::Integer(3),
        params: ThreadSearchOccurrencesParams {
            thread_id: MAIN_THREAD_ID.to_string(),
            search_term: "needle".to_string(),
            cursor,
            limit: None,
        },
    }
}

fn thread_list_request(cursor: Option<String>) -> ClientRequest {
    ClientRequest::ThreadList {
        request_id: RequestId::Integer(4),
        params: ThreadListParams {
            cursor,
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
    }
}

fn thread_search_request(cursor: Option<String>) -> ClientRequest {
    ClientRequest::ThreadSearch {
        request_id: RequestId::Integer(5),
        params: ThreadSearchParams {
            cursor,
            limit: None,
            sort_key: None,
            sort_direction: None,
            source_kinds: None,
            archived: None,
            search_term: "needle".to_string(),
        },
    }
}

fn thread_resume_response_with_cursors() -> ThreadResumeResponse {
    let cwd = AbsolutePathBuf::current_dir().expect("current directory should be absolute");
    ThreadResumeResponse {
        thread: Thread {
            id: MAIN_THREAD_ID.to_string(),
            extra: None,
            session_id: "session-1".to_string(),
            forked_from_id: None,
            parent_thread_id: None,
            preview: "preview".to_string(),
            ephemeral: false,
            section: None,
            section_entered_at: None,
            history_mode: ThreadHistoryMode::Paginated,
            model_provider: "mock_provider".to_string(),
            created_at: 0,
            updated_at: 0,
            recency_at: None,
            status: ThreadStatus::Idle,
            path: None,
            cwd: cwd.clone(),
            cli_version: "test".to_string(),
            source: SessionSource::AppServer,
            can_accept_direct_input: Some(true),
            thread_source: None,
            agent_nickname: None,
            agent_role: None,
            git_info: None,
            name: None,
            turns: Vec::new(),
        },
        model: "gpt-test".to_string(),
        model_provider: "mock_provider".to_string(),
        service_tier: None,
        cwd,
        runtime_workspace_roots: Vec::new(),
        instruction_sources: Vec::new(),
        approval_policy: AskForApproval::Never,
        approvals_reviewer: ApprovalsReviewer::User,
        sandbox: SandboxPolicy::DangerFullAccess,
        active_permission_profile: None,
        reasoning_effort: None,
        multi_agent_mode: MultiAgentMode::ExplicitRequestOnly,
        initial_turns_page: Some(TurnsPage {
            data: Vec::new(),
            next_cursor: Some("initial-next".to_string()),
            backwards_cursor: Some("initial-back".to_string()),
        }),
        turns_backwards_cursor: Some("turns-back".to_string()),
        items_backwards_cursor: Some("items-back".to_string()),
    }
}

fn assert_controller_not_allowed(error: JSONRPCErrorError) {
    let data: ControllerErrorData =
        serde_json::from_value(error.data.expect("controller error data"))
            .expect("controller error data should deserialize");
    assert_eq!(data.code, ControllerErrorCode::ControllerNotAllowed);
}

#[test]
fn exact_thread_cursor_round_trips_for_same_controller_connection() {
    let authorization = controller_authorization(MAIN_THREAD_ID);
    let mut response = ClientResponsePayload::ThreadTurnsList(ThreadTurnsListResponse {
        data: Vec::new(),
        next_cursor: Some("raw-next".to_string()),
        backwards_cursor: Some("raw-back".to_string()),
    });

    bind_controller_response_cursors(&mut response, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("cursor binding should succeed");
    let ClientResponsePayload::ThreadTurnsList(response) = response else {
        panic!("expected thread turns response");
    };
    assert_ne!(response.next_cursor.as_deref(), Some("raw-next"));
    assert_ne!(response.backwards_cursor.as_deref(), Some("raw-back"));

    let mut next_request = thread_turns_request(response.next_cursor);
    unbind_controller_request_cursors(&mut next_request, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("same connection and thread should unwrap next cursor");
    let ClientRequest::ThreadTurnsList { params, .. } = next_request else {
        panic!("expected thread turns request");
    };
    assert_eq!(params.cursor.as_deref(), Some("raw-next"));

    let mut backwards_request = thread_turns_request(response.backwards_cursor);
    unbind_controller_request_cursors(
        &mut backwards_request,
        CONTROLLER_CONNECTION_ID,
        &authorization,
    )
    .expect("same connection and thread should unwrap backwards cursor");
    let ClientRequest::ThreadTurnsList { params, .. } = backwards_request else {
        panic!("expected thread turns request");
    };
    assert_eq!(params.cursor.as_deref(), Some("raw-back"));
}

#[test]
fn exact_thread_cursor_rejects_cross_connection_or_thread_replay() {
    let authorization = controller_authorization(MAIN_THREAD_ID);
    let mut response = ClientResponsePayload::ThreadTurnsList(ThreadTurnsListResponse {
        data: Vec::new(),
        next_cursor: Some("raw-next".to_string()),
        backwards_cursor: None,
    });
    bind_controller_response_cursors(&mut response, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("cursor binding should succeed");
    let ClientResponsePayload::ThreadTurnsList(response) = response else {
        panic!("expected thread turns response");
    };
    let bound_cursor = response.next_cursor.expect("bound next cursor");

    let mut other_connection_request = thread_turns_request(Some(bound_cursor.clone()));
    let error = unbind_controller_request_cursors(
        &mut other_connection_request,
        OTHER_CONTROLLER_CONNECTION_ID,
        &authorization,
    )
    .expect_err("cursor should not replay on another controller connection");
    assert_controller_not_allowed(error);

    let mut other_thread_request = thread_turns_request(Some(bound_cursor));
    let error = unbind_controller_request_cursors(
        &mut other_thread_request,
        CONTROLLER_CONNECTION_ID,
        &controller_authorization(OTHER_THREAD_ID),
    )
    .expect_err("cursor should not replay against another authorized thread");
    assert_controller_not_allowed(error);
}

#[test]
fn thread_resume_response_cursors_are_bound_for_internal_send_path() {
    let mut response = thread_resume_response_with_cursors();
    bind_controller_thread_resume_response_cursors(
        &mut response,
        CONTROLLER_CONNECTION_ID,
        MAIN_THREAD_ID,
    )
    .expect("thread/resume cursor binding should succeed");

    let initial_turns_page = response
        .initial_turns_page
        .expect("initial turns page should remain present");
    assert_ne!(
        initial_turns_page.next_cursor.as_deref(),
        Some("initial-next")
    );
    assert_ne!(
        initial_turns_page.backwards_cursor.as_deref(),
        Some("initial-back")
    );
    assert_ne!(
        response.turns_backwards_cursor.as_deref(),
        Some("turns-back")
    );
    assert_ne!(
        response.items_backwards_cursor.as_deref(),
        Some("items-back")
    );

    let authorization = controller_authorization(MAIN_THREAD_ID);
    let turn_cursors = [
        (initial_turns_page.next_cursor, "initial-next"),
        (initial_turns_page.backwards_cursor, "initial-back"),
        (response.turns_backwards_cursor, "turns-back"),
    ];
    for (cursor, expected_raw_cursor) in turn_cursors {
        let mut request = thread_turns_request(cursor);
        unbind_controller_request_cursors(&mut request, CONTROLLER_CONNECTION_ID, &authorization)
            .expect("thread/resume turn cursor should unwrap for thread/turns/list");
        let ClientRequest::ThreadTurnsList { params, .. } = request else {
            panic!("expected thread turns request");
        };
        assert_eq!(params.cursor.as_deref(), Some(expected_raw_cursor));
    }

    let mut items_request = thread_items_request(response.items_backwards_cursor);
    unbind_controller_request_cursors(&mut items_request, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("thread/resume item cursor should unwrap for thread/items/list");
    let ClientRequest::ThreadItemsList { params, .. } = items_request else {
        panic!("expected thread items request");
    };
    assert_eq!(params.cursor.as_deref(), Some("items-back"));
}

#[test]
fn collection_filtered_cursors_round_trip_for_same_controller_connection() {
    let authorization = controller_authorization(MAIN_THREAD_ID);
    let mut response = ClientResponsePayload::ThreadList(ThreadListResponse {
        data: Vec::new(),
        next_cursor: Some("raw-next".to_string()),
        backwards_cursor: Some("raw-back".to_string()),
    });

    bind_controller_response_cursors(&mut response, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("cursor binding should succeed");
    let ClientResponsePayload::ThreadList(response) = response else {
        panic!("expected thread list response");
    };
    assert_ne!(response.next_cursor.as_deref(), Some("raw-next"));
    assert_ne!(response.backwards_cursor.as_deref(), Some("raw-back"));

    let mut next_request = thread_list_request(response.next_cursor);
    unbind_controller_request_cursors(&mut next_request, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("same connection and thread should unwrap next collection cursor");
    let ClientRequest::ThreadList { params, .. } = next_request else {
        panic!("expected thread list request");
    };
    assert_eq!(params.cursor.as_deref(), Some("raw-next"));

    let mut backwards_request = thread_list_request(response.backwards_cursor);
    unbind_controller_request_cursors(
        &mut backwards_request,
        CONTROLLER_CONNECTION_ID,
        &authorization,
    )
    .expect("same connection and thread should unwrap backwards collection cursor");
    let ClientRequest::ThreadList { params, .. } = backwards_request else {
        panic!("expected thread list request");
    };
    assert_eq!(params.cursor.as_deref(), Some("raw-back"));
}

#[test]
fn collection_filtered_cursors_reject_cross_connection_or_thread_replay() {
    let authorization = controller_authorization(MAIN_THREAD_ID);
    let mut response = ClientResponsePayload::ThreadSearch(ThreadSearchResponse {
        data: Vec::new(),
        next_cursor: Some("raw-next".to_string()),
        backwards_cursor: None,
    });
    bind_controller_response_cursors(&mut response, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("cursor binding should succeed");
    let ClientResponsePayload::ThreadSearch(response) = response else {
        panic!("expected thread search response");
    };
    let bound_cursor = response.next_cursor.expect("bound next cursor");

    let mut other_connection_request = thread_search_request(Some(bound_cursor.clone()));
    let error = unbind_controller_request_cursors(
        &mut other_connection_request,
        OTHER_CONTROLLER_CONNECTION_ID,
        &authorization,
    )
    .expect_err("collection cursor should not replay on another controller connection");
    assert_controller_not_allowed(error);

    let mut other_thread_request = thread_search_request(Some(bound_cursor));
    let error = unbind_controller_request_cursors(
        &mut other_thread_request,
        CONTROLLER_CONNECTION_ID,
        &controller_authorization(OTHER_THREAD_ID),
    )
    .expect_err("collection cursor should not replay against another authorized thread");
    assert_controller_not_allowed(error);
}

#[test]
fn collection_filtered_cursor_rejects_unbound_or_wrong_purpose_cursors() {
    let authorization = controller_authorization(MAIN_THREAD_ID);
    let mut unbound_request = thread_list_request(Some("raw-next".to_string()));
    let error = unbind_controller_request_cursors(
        &mut unbound_request,
        CONTROLLER_CONNECTION_ID,
        &authorization,
    )
    .expect_err("unbound collection cursor should be rejected");
    assert_controller_not_allowed(error);

    let mut response = ClientResponsePayload::ThreadLoadedList(ThreadLoadedListResponse {
        data: Vec::new(),
        next_cursor: Some("loaded-next".to_string()),
    });
    bind_controller_response_cursors(&mut response, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("cursor binding should succeed");
    let ClientResponsePayload::ThreadLoadedList(response) = response else {
        panic!("expected loaded list response");
    };

    let mut wrong_purpose_request = thread_list_request(response.next_cursor);
    let error = unbind_controller_request_cursors(
        &mut wrong_purpose_request,
        CONTROLLER_CONNECTION_ID,
        &authorization,
    )
    .expect_err("loaded-list cursor should not replay as a thread/list cursor");
    assert_controller_not_allowed(error);
}

#[test]
fn thread_section_list_cursor_is_connection_bound() {
    let authorization = controller_authorization(MAIN_THREAD_ID);
    let mut response = ClientResponsePayload::ThreadSectionList(ThreadSectionListResponse {
        data: Vec::new(),
        next_cursor: Some("section-next".to_string()),
    });
    bind_controller_response_cursors(&mut response, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("cursor binding should succeed");
    let ClientResponsePayload::ThreadSectionList(response) = response else {
        panic!("expected thread section list response");
    };

    let mut request = ClientRequest::ThreadSectionList {
        request_id: RequestId::Integer(5),
        params: ThreadSectionListParams {
            cursor: response.next_cursor,
            limit: None,
        },
    };
    unbind_controller_request_cursors(&mut request, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("same connection and thread should unwrap section cursor");
    let ClientRequest::ThreadSectionList { params, .. } = request else {
        panic!("expected thread section list request");
    };
    assert_eq!(params.cursor.as_deref(), Some("section-next"));
}

#[test]
fn exact_thread_cursor_rejects_unbound_or_wrong_purpose_cursors() {
    let authorization = controller_authorization(MAIN_THREAD_ID);
    let mut unbound_request = thread_turns_request(Some("raw-next".to_string()));
    let error = unbind_controller_request_cursors(
        &mut unbound_request,
        CONTROLLER_CONNECTION_ID,
        &authorization,
    )
    .expect_err("unbound controller cursor should be rejected");
    assert_controller_not_allowed(error);

    let mut response =
        ClientResponsePayload::ThreadSearchOccurrences(ThreadSearchOccurrencesResponse {
            data: Vec::new(),
            next_cursor: Some("search-next".to_string()),
        });
    bind_controller_response_cursors(&mut response, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("cursor binding should succeed");
    let ClientResponsePayload::ThreadSearchOccurrences(response) = response else {
        panic!("expected search occurrences response");
    };

    let mut wrong_purpose_request = thread_turns_request(response.next_cursor);
    let error = unbind_controller_request_cursors(
        &mut wrong_purpose_request,
        CONTROLLER_CONNECTION_ID,
        &authorization,
    )
    .expect_err("search cursor should not replay as a turns cursor");
    assert_controller_not_allowed(error);
}

#[test]
fn search_occurrence_turn_cursors_are_bound_as_turns_cursors() {
    let authorization = controller_authorization(MAIN_THREAD_ID);
    let mut response =
        ClientResponsePayload::ThreadSearchOccurrences(ThreadSearchOccurrencesResponse {
            data: vec![ThreadSearchOccurrence {
                turn_id: "turn-1".to_string(),
                item_id: "item-1".to_string(),
                snippet: "needle".to_string(),
                snippet_match_range: ThreadSearchTextRange { start: 0, end: 6 },
                turn_cursor: "turn-cursor".to_string(),
            }],
            next_cursor: Some("search-next".to_string()),
        });
    bind_controller_response_cursors(&mut response, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("cursor binding should succeed");
    let ClientResponsePayload::ThreadSearchOccurrences(response) = response else {
        panic!("expected search occurrences response");
    };

    let mut turns_request = thread_turns_request(Some(response.data[0].turn_cursor.clone()));
    unbind_controller_request_cursors(&mut turns_request, CONTROLLER_CONNECTION_ID, &authorization)
        .expect("occurrence turn cursor should unwrap for thread/turns/list");
    let ClientRequest::ThreadTurnsList { params, .. } = turns_request else {
        panic!("expected thread turns request");
    };
    assert_eq!(params.cursor.as_deref(), Some("turn-cursor"));

    let mut search_request = thread_search_occurrences_request(response.next_cursor);
    unbind_controller_request_cursors(
        &mut search_request,
        CONTROLLER_CONNECTION_ID,
        &authorization,
    )
    .expect("search next cursor should unwrap for thread/searchOccurrences");
    let ClientRequest::ThreadSearchOccurrences { params, .. } = search_request else {
        panic!("expected search occurrences request");
    };
    assert_eq!(params.cursor.as_deref(), Some("search-next"));
}
