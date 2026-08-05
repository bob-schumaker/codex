use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::transport::ConnectionId;

/// TUI-mediated participation request for a live local external-controller connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeControllerParticipationRequest {
    pub(crate) connection_id: ConnectionId,
    pub controller_name: String,
    pub description: String,
    pub main_thread_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NativeControllerParticipationRequestId(pub u64);

/// In-process event delivered to the owning TUI for controller participation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InProcessControllerParticipationRequest {
    pub request_id: NativeControllerParticipationRequestId,
    pub controller_name: String,
    pub description: String,
    pub main_thread_id: String,
}

/// TUI decision for a native controller participation request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeControllerParticipationDecision {
    Approved,
    Rejected { reason: String },
    TuiUnavailable { reason: String },
}

pub type NativeControllerParticipationFuture =
    Pin<Box<dyn Future<Output = NativeControllerParticipationDecision> + Send>>;

pub type NativeControllerParticipationApprover = Arc<
    dyn Fn(NativeControllerParticipationRequest) -> NativeControllerParticipationFuture
        + Send
        + Sync,
>;
