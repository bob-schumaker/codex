use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_app_server_protocol::ClientResponsePayload;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotificationEnvelope;
use codex_app_server_protocol::ServerRequest;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::sync::watch;

/// Stable identifier for a transport connection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConnectionId(pub u64);

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Outgoing message from the server to the client.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum OutgoingMessage {
    Request(ServerRequest),
    /// AppServerNotification is specific to the case where this is run as an
    /// "app server" as opposed to an MCP server.
    AppServerNotification(ServerNotificationEnvelope),
    Response(OutgoingResponse),
    Error(OutgoingError),
}

#[derive(Debug, Clone, Serialize)]
pub struct OutgoingResponse {
    pub id: RequestId,
    pub result: Box<ClientResponsePayload>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OutgoingError {
    pub error: JSONRPCErrorError,
    pub id: RequestId,
}

#[derive(Debug)]
pub struct QueuedOutgoingMessage {
    pub message: OutgoingMessage,
    pub write_complete_tx: Option<TrackedWriteCompletion>,
}

impl QueuedOutgoingMessage {
    pub fn new(message: OutgoingMessage) -> Self {
        Self {
            message,
            write_complete_tx: None,
        }
    }

    pub fn is_write_permitted(&self) -> bool {
        self.write_complete_tx
            .as_ref()
            .is_none_or(TrackedWriteCompletion::is_write_permitted)
    }

    pub fn begin_write(&self) -> bool {
        self.write_complete_tx
            .as_ref()
            .is_none_or(TrackedWriteCompletion::begin_write)
    }
}

#[derive(Debug)]
pub struct TrackedWriteCompletion {
    write_complete_tx: oneshot::Sender<()>,
    write_permit_rx: Option<watch::Receiver<bool>>,
    write_started: Option<Arc<AtomicBool>>,
}

impl TrackedWriteCompletion {
    pub fn new(write_complete_tx: oneshot::Sender<()>) -> Self {
        Self {
            write_complete_tx,
            write_permit_rx: None,
            write_started: None,
        }
    }

    pub fn with_write_permit(
        write_complete_tx: oneshot::Sender<()>,
        write_permit_rx: watch::Receiver<bool>,
        write_started: Arc<AtomicBool>,
    ) -> Self {
        Self {
            write_complete_tx,
            write_permit_rx: Some(write_permit_rx),
            write_started: Some(write_started),
        }
    }

    pub fn is_write_permitted(&self) -> bool {
        self.write_permit_rx
            .as_ref()
            .is_none_or(|write_permit_rx| *write_permit_rx.borrow())
    }

    pub fn begin_write(&self) -> bool {
        if let Some(write_started) = &self.write_started {
            write_started.store(true, Ordering::Release);
        }
        if self.is_write_permitted() {
            true
        } else {
            if let Some(write_started) = &self.write_started {
                write_started.store(false, Ordering::Release);
            }
            false
        }
    }

    pub fn complete(self) {
        if let Some(write_started) = &self.write_started {
            write_started.store(true, Ordering::Release);
        }
        let _ = self.write_complete_tx.send(());
    }
}

#[cfg(test)]
#[path = "outgoing_message_tests.rs"]
mod tests;
