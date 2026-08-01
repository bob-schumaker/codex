use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerRequestParticipationParams {
    pub controller_name: String,
    pub description: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerRequestParticipationResponse {
    pub status: ControllerParticipationStatus,
    pub session: Option<ControllerSession>,
    pub denial: Option<ControllerParticipationDenial>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ControllerParticipationStatus {
    Approved,
    Rejected,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerParticipationDenial {
    pub message: String,
    pub data: ControllerErrorData,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerSession {
    pub session_id: String,
    pub main_thread_id: String,
    pub active_lease: Option<ControllerLease>,
    pub authorization_epoch: u64,
    pub session_sequence: u64,
    pub effective_capabilities: ControllerEffectiveCapabilities,
    pub lease_expires_in_ms: Option<u64>,
    pub authorization_expires_in_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerLease {
    pub lease_id: String,
    pub owner_epoch: u64,
    pub expires_in_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerEffectiveCapabilities {
    pub read_main_thread: bool,
    pub subscribe_main_thread: bool,
    pub acquire_control: bool,
    pub release_control: bool,
    pub mutate_main_thread: bool,
    pub answer_prompts: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerAcquireControlResponse {
    pub session: ControllerSession,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerReleaseControlResponse {
    pub session: ControllerSession,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerSignOffResponse {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerAuthorizationChangedNotification {
    pub session_id: String,
    pub main_thread_id: String,
    pub reason: ControllerAuthorizationChangedReason,
    pub authorization_epoch: u64,
    pub owner_epoch: u64,
    pub session_sequence: u64,
    pub session: Option<ControllerSession>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ControllerAuthorizationChangedReason {
    Approved,
    Rejected,
    Revoked,
    Expired,
    CredentialRotated,
    PolicyChanged,
    SignOff,
    Disconnected,
    MainThreadClosed,
    TuiUnavailable,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerControlOwnershipChangedNotification {
    pub session_id: String,
    pub main_thread_id: String,
    pub reason: ControllerControlOwnershipChangedReason,
    pub authorization_epoch: u64,
    pub owner_epoch: u64,
    pub session_sequence: u64,
    pub active_lease: Option<ControllerLease>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ControllerControlOwnershipChangedReason {
    InitialLeaseGranted,
    Acquired,
    Released,
    ReclaimedByTui,
    LeaseExpired,
    AuthorizationRevoked,
    ControllerDisconnected,
    SignOff,
    MainThreadClosed,
    TuiUnavailable,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControllerErrorData {
    pub code: ControllerErrorCode,
    pub retry: ControllerRetryDisposition,
    pub retry_after_ms: Option<u64>,
    pub launch_state: Option<ControllerLaunchState>,
    pub main_thread_id: Option<String>,
    pub session_id: Option<String>,
    pub authorization_epoch: Option<u64>,
    pub owner_epoch: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[ts(export_to = "v2/")]
pub enum ControllerErrorCode {
    #[serde(rename = "experimental-not-enabled")]
    #[ts(rename = "experimental-not-enabled")]
    ExperimentalNotEnabled,
    #[serde(rename = "participation-required")]
    #[ts(rename = "participation-required")]
    ParticipationRequired,
    #[serde(rename = "enrollment-denied")]
    #[ts(rename = "enrollment-denied")]
    EnrollmentDenied,
    #[serde(rename = "main-thread-unavailable")]
    #[ts(rename = "main-thread-unavailable")]
    MainThreadUnavailable,
    #[serde(rename = "main-thread-closed")]
    #[ts(rename = "main-thread-closed")]
    MainThreadClosed,
    #[serde(rename = "tui-unavailable")]
    #[ts(rename = "tui-unavailable")]
    TuiUnavailable,
    #[serde(rename = "ownership-conflict")]
    #[ts(rename = "ownership-conflict")]
    OwnershipConflict,
    #[serde(rename = "stale-ownership")]
    #[ts(rename = "stale-ownership")]
    StaleOwnership,
    #[serde(rename = "controller-not-allowed")]
    #[ts(rename = "controller-not-allowed")]
    ControllerNotAllowed,
    #[serde(rename = "transport-closing")]
    #[ts(rename = "transport-closing")]
    TransportClosing,
    #[serde(rename = "different-thread-target")]
    #[ts(rename = "different-thread-target")]
    DifferentThreadTarget,
    #[serde(rename = "authorization-expired")]
    #[ts(rename = "authorization-expired")]
    AuthorizationExpired,
    #[serde(rename = "lease-expired")]
    #[ts(rename = "lease-expired")]
    LeaseExpired,
    #[serde(rename = "controller-overloaded")]
    #[ts(rename = "controller-overloaded")]
    ControllerOverloaded,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ControllerRetryDisposition {
    SameConnection,
    Reconnect,
    DoNotRetry,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ControllerLaunchState {
    Starting,
    MainThreadClosed,
    TuiUnavailable,
    Closed,
}

#[cfg(test)]
#[path = "controller_tests.rs"]
mod tests;
