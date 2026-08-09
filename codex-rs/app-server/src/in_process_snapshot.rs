use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::Thread;

use crate::controller_session::ControllerOwnershipStatus;

/// Atomic recovery snapshot for embedded in-process TUI consumers.
///
/// This is intentionally not a public JSON-RPC DTO. It packages the normal
/// `thread/read` view with app-server-owned sequence, ownership, and replayable
/// prompt state so the embedded TUI can recover after an in-process event lag
/// without inventing local ownership or prompt state.
#[derive(Debug, Clone, PartialEq)]
pub struct InProcessThreadSnapshot {
    pub thread: Thread,
    pub last_sequence: u64,
    pub controller_ownership_status: Option<ControllerOwnershipStatus>,
    pub pending_server_requests: Vec<InProcessThreadSnapshotServerRequest>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InProcessThreadSnapshotServerRequest {
    pub request: Box<ServerRequest>,
    pub thread_sequence: Option<u64>,
}
