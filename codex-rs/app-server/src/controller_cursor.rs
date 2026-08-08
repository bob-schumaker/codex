use crate::controller_admission::controller_not_allowed;
use crate::error_code::internal_error;
use crate::outgoing_message::ConnectionId;
use crate::request_processors::ControllerNormalAuthorization;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use serde::Deserialize;
use serde::Serialize;

const CONTROLLER_CURSOR_VERSION: u8 = 1;
const CURSOR_PURPOSE_THREAD_LIST: &str = "thread/list";
const CURSOR_PURPOSE_THREAD_SEARCH: &str = "thread/search";
const CURSOR_PURPOSE_THREAD_LOADED_LIST: &str = "thread/loaded/list";
const CURSOR_PURPOSE_THREAD_SECTION_LIST: &str = "threadSection/list";
const CURSOR_PURPOSE_THREAD_TURNS: &str = "thread/turns/list";
const CURSOR_PURPOSE_THREAD_ITEMS: &str = "thread/items/list";
const CURSOR_PURPOSE_THREAD_SEARCH_OCCURRENCES: &str = "thread/searchOccurrences";
const CURSOR_PURPOSE_THREAD_BACKGROUND_TERMINALS: &str = "thread/backgroundTerminals/list";

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ControllerBoundCursor {
    version: u8,
    connection_id: u64,
    main_thread_id: String,
    purpose: String,
    cursor: String,
}

pub(crate) fn unbind_controller_request_cursors(
    request: &mut ClientRequest,
    connection_id: ConnectionId,
    authorization: &ControllerNormalAuthorization,
) -> Result<(), JSONRPCErrorError> {
    match request {
        ClientRequest::ThreadList { params, .. } => unbind_controller_cursor(
            &mut params.cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_LIST,
        ),
        ClientRequest::ThreadSearch { params, .. } => unbind_controller_cursor(
            &mut params.cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_SEARCH,
        ),
        ClientRequest::ThreadLoadedList { params, .. } => unbind_controller_cursor(
            &mut params.cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_LOADED_LIST,
        ),
        ClientRequest::ThreadSectionList { params, .. } => unbind_controller_cursor(
            &mut params.cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_SECTION_LIST,
        ),
        ClientRequest::ThreadBackgroundTerminalsList { params, .. } => unbind_controller_cursor(
            &mut params.cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_BACKGROUND_TERMINALS,
        ),
        ClientRequest::ThreadSearchOccurrences { params, .. } => unbind_controller_cursor(
            &mut params.cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_SEARCH_OCCURRENCES,
        ),
        ClientRequest::ThreadTurnsList { params, .. } => unbind_controller_cursor(
            &mut params.cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_TURNS,
        ),
        ClientRequest::ThreadItemsList { params, .. } => unbind_controller_cursor(
            &mut params.cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_ITEMS,
        ),
        _ => Ok(()),
    }
}

pub(crate) fn bind_controller_response_cursors(
    response: &mut ClientResponsePayload,
    connection_id: ConnectionId,
    authorization: &ControllerNormalAuthorization,
) -> Result<(), JSONRPCErrorError> {
    match response {
        ClientResponsePayload::ThreadList(response) => {
            bind_controller_cursor(
                &mut response.next_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_LIST,
            )?;
            bind_controller_cursor(
                &mut response.backwards_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_LIST,
            )
        }
        ClientResponsePayload::ThreadSearch(response) => {
            bind_controller_cursor(
                &mut response.next_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_SEARCH,
            )?;
            bind_controller_cursor(
                &mut response.backwards_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_SEARCH,
            )
        }
        ClientResponsePayload::ThreadLoadedList(response) => bind_controller_cursor(
            &mut response.next_cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_LOADED_LIST,
        ),
        ClientResponsePayload::ThreadSectionList(response) => bind_controller_cursor(
            &mut response.next_cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_SECTION_LIST,
        ),
        ClientResponsePayload::ThreadResume(response) => {
            if let Some(initial_turns_page) = response.initial_turns_page.as_mut() {
                bind_controller_cursor(
                    &mut initial_turns_page.next_cursor,
                    connection_id,
                    authorization,
                    CURSOR_PURPOSE_THREAD_TURNS,
                )?;
                bind_controller_cursor(
                    &mut initial_turns_page.backwards_cursor,
                    connection_id,
                    authorization,
                    CURSOR_PURPOSE_THREAD_TURNS,
                )?;
            }
            bind_controller_cursor(
                &mut response.turns_backwards_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_TURNS,
            )?;
            bind_controller_cursor(
                &mut response.items_backwards_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_ITEMS,
            )
        }
        ClientResponsePayload::ThreadBackgroundTerminalsList(response) => bind_controller_cursor(
            &mut response.next_cursor,
            connection_id,
            authorization,
            CURSOR_PURPOSE_THREAD_BACKGROUND_TERMINALS,
        ),
        ClientResponsePayload::ThreadSearchOccurrences(response) => {
            for occurrence in &mut response.data {
                bind_required_controller_cursor(
                    &mut occurrence.turn_cursor,
                    connection_id,
                    authorization,
                    CURSOR_PURPOSE_THREAD_TURNS,
                )?;
            }
            bind_controller_cursor(
                &mut response.next_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_SEARCH_OCCURRENCES,
            )
        }
        ClientResponsePayload::ThreadTurnsList(response) => {
            bind_controller_cursor(
                &mut response.next_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_TURNS,
            )?;
            bind_controller_cursor(
                &mut response.backwards_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_TURNS,
            )
        }
        ClientResponsePayload::ThreadItemsList(response) => {
            bind_controller_cursor(
                &mut response.next_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_ITEMS,
            )?;
            bind_controller_cursor(
                &mut response.backwards_cursor,
                connection_id,
                authorization,
                CURSOR_PURPOSE_THREAD_ITEMS,
            )
        }
        _ => Ok(()),
    }
}

fn bind_controller_cursor(
    cursor: &mut Option<String>,
    connection_id: ConnectionId,
    authorization: &ControllerNormalAuthorization,
    purpose: &'static str,
) -> Result<(), JSONRPCErrorError> {
    let Some(raw_cursor) = cursor.take() else {
        return Ok(());
    };
    let mut bound_cursor = raw_cursor;
    bind_required_controller_cursor(&mut bound_cursor, connection_id, authorization, purpose)?;
    *cursor = Some(bound_cursor);
    Ok(())
}

fn bind_required_controller_cursor(
    cursor: &mut String,
    connection_id: ConnectionId,
    authorization: &ControllerNormalAuthorization,
    purpose: &'static str,
) -> Result<(), JSONRPCErrorError> {
    *cursor = serde_json::to_string(&ControllerBoundCursor {
        version: CONTROLLER_CURSOR_VERSION,
        connection_id: connection_id.0,
        main_thread_id: authorization.main_thread_id.clone(),
        purpose: purpose.to_string(),
        cursor: std::mem::take(cursor),
    })
    .map_err(|err| internal_error(format!("failed to bind controller cursor: {err}")))?;
    Ok(())
}

fn unbind_controller_cursor(
    cursor: &mut Option<String>,
    connection_id: ConnectionId,
    authorization: &ControllerNormalAuthorization,
    purpose: &'static str,
) -> Result<(), JSONRPCErrorError> {
    let Some(bound_cursor) = cursor.take() else {
        return Ok(());
    };
    let bound_cursor = serde_json::from_str::<ControllerBoundCursor>(&bound_cursor)
        .map_err(|_| invalid_controller_cursor())?;
    if bound_cursor.version != CONTROLLER_CURSOR_VERSION
        || bound_cursor.connection_id != connection_id.0
        || bound_cursor.main_thread_id != authorization.main_thread_id
        || bound_cursor.purpose != purpose
    {
        return Err(invalid_controller_cursor());
    }
    *cursor = Some(bound_cursor.cursor);
    Ok(())
}

fn invalid_controller_cursor() -> JSONRPCErrorError {
    controller_not_allowed("external controller cursor is not valid for this connection and thread")
}

#[cfg(test)]
#[path = "controller_cursor_tests.rs"]
mod tests;
