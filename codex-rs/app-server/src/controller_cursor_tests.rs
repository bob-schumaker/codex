use super::*;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadSearchOccurrence;
use codex_app_server_protocol::ThreadSearchOccurrencesParams;
use codex_app_server_protocol::ThreadSearchOccurrencesResponse;
use codex_app_server_protocol::ThreadSearchTextRange;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnItemsView;
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

fn thread_search_occurrences_request(cursor: Option<String>) -> ClientRequest {
    ClientRequest::ThreadSearchOccurrences {
        request_id: RequestId::Integer(2),
        params: ThreadSearchOccurrencesParams {
            thread_id: MAIN_THREAD_ID.to_string(),
            search_term: "needle".to_string(),
            cursor,
            limit: None,
        },
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
