//! Controller admission policy registry.
//!
//! This module is intentionally not wired into dispatch yet. It is the default-deny
//! review table that later controller admission code will enforce before any
//! existing app-server handler runs.
#![allow(dead_code)]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdmissionRule {
    pub(crate) target: TargetExtraction,
    pub(crate) required_authority: RequiredAuthority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TargetExtraction {
    None,
    MainThreadOnly,
    ExactThread,
    CollectionFiltered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequiredAuthority {
    PreParticipation,
    StandingSession,
    ActiveOwner,
    TuiOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MethodAdmission {
    pub(crate) method: &'static str,
    pub(crate) rule: AdmissionRule,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ContinuationKind {
    Cursor,
    ResumeToken,
    Subscription,
    ImplicitTarget,
}

impl ContinuationKind {
    pub(crate) const ALL: &'static [Self] = &[
        Self::Cursor,
        Self::ResumeToken,
        Self::Subscription,
        Self::ImplicitTarget,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContinuationAdmission {
    pub(crate) kind: ContinuationKind,
    pub(crate) rule: AdmissionRule,
}

macro_rules! method_rule {
    ($method:literal, $target:ident, $authority:ident) => {
        MethodAdmission {
            method: $method,
            rule: AdmissionRule {
                target: TargetExtraction::$target,
                required_authority: RequiredAuthority::$authority,
            },
        }
    };
}

macro_rules! continuation_rule {
    ($kind:ident, $target:ident, $authority:ident) => {
        ContinuationAdmission {
            kind: ContinuationKind::$kind,
            rule: AdmissionRule {
                target: TargetExtraction::$target,
                required_authority: RequiredAuthority::$authority,
            },
        }
    };
}

pub(crate) const CLIENT_REQUEST_ADMISSION: &[MethodAdmission] = &[
    method_rule!("initialize", None, PreParticipation),
    method_rule!("controller/requestParticipation", None, PreParticipation),
    method_rule!("controller/acquireControl", MainThreadOnly, StandingSession),
    method_rule!("controller/releaseControl", MainThreadOnly, StandingSession),
    method_rule!("controller/signOff", MainThreadOnly, StandingSession),
    // Read/subscription surfaces are available to an approved controller session
    // once later target extraction proves they resolve to the immutable TUI main
    // thread. Optional-thread methods classified as `ExactThread` must reject
    // omitted controller targets during admission wiring.
    method_rule!("thread/unsubscribe", ExactThread, StandingSession),
    method_rule!("thread/goal/get", ExactThread, StandingSession),
    method_rule!(
        "thread/backgroundTerminals/list",
        ExactThread,
        StandingSession
    ),
    method_rule!("thread/searchOccurrences", ExactThread, StandingSession),
    method_rule!("thread/read", ExactThread, StandingSession),
    method_rule!("thread/turns/list", ExactThread, StandingSession),
    method_rule!("thread/items/list", ExactThread, StandingSession),
    method_rule!("mcpServer/resource/read", ExactThread, StandingSession),
    method_rule!("thread/list", CollectionFiltered, StandingSession),
    method_rule!("threadSection/list", CollectionFiltered, StandingSession),
    method_rule!("thread/search", CollectionFiltered, StandingSession),
    method_rule!("thread/loaded/list", CollectionFiltered, StandingSession),
    method_rule!("thread/archive", ExactThread, ActiveOwner),
    method_rule!("thread/delete", ExactThread, ActiveOwner),
    method_rule!("thread/increment_elicitation", ExactThread, ActiveOwner),
    method_rule!("thread/decrement_elicitation", ExactThread, ActiveOwner),
    method_rule!("thread/name/set", ExactThread, ActiveOwner),
    method_rule!("thread/metadata/update", ExactThread, ActiveOwner),
    method_rule!("thread/section/move", ExactThread, ActiveOwner),
    method_rule!("thread/unarchive", ExactThread, ActiveOwner),
    method_rule!("thread/shellCommand", ExactThread, ActiveOwner),
    method_rule!(
        "thread/approveGuardianDeniedAction",
        ExactThread,
        ActiveOwner
    ),
    method_rule!("thread/backgroundTerminals/clean", ExactThread, ActiveOwner),
    method_rule!(
        "thread/backgroundTerminals/terminate",
        ExactThread,
        ActiveOwner
    ),
    method_rule!("turn/start", ExactThread, ActiveOwner),
    method_rule!("turn/steer", ExactThread, ActiveOwner),
    method_rule!("turn/interrupt", ExactThread, ActiveOwner),
    method_rule!("thread/realtime/start", ExactThread, ActiveOwner),
    method_rule!("thread/realtime/appendAudio", ExactThread, ActiveOwner),
    method_rule!("thread/realtime/appendText", ExactThread, ActiveOwner),
    method_rule!("thread/realtime/appendSpeech", ExactThread, ActiveOwner),
    method_rule!("thread/realtime/stop", ExactThread, ActiveOwner),
    method_rule!("review/start", ExactThread, ActiveOwner),
    method_rule!("mcpServer/tool/call", ExactThread, ActiveOwner),
    // Thread creation/resume/fork and model-context/history-changing surfaces
    // stay TUI-only until a separate bounded context-safety review opens them.
    method_rule!("thread/start", None, TuiOnly),
    method_rule!("thread/resume", ExactThread, TuiOnly),
    method_rule!("thread/fork", ExactThread, TuiOnly),
    method_rule!("thread/goal/set", ExactThread, TuiOnly),
    method_rule!("thread/goal/clear", ExactThread, TuiOnly),
    method_rule!("thread/settings/update", ExactThread, TuiOnly),
    method_rule!("thread/memoryMode/set", ExactThread, TuiOnly),
    method_rule!("memory/reset", None, TuiOnly),
    method_rule!("thread/compact/start", ExactThread, TuiOnly),
    method_rule!("thread/rollback", ExactThread, TuiOnly),
    method_rule!("thread/inject_items", ExactThread, TuiOnly),
    method_rule!("skills/list", None, TuiOnly),
    method_rule!("skills/extraRoots/set", None, TuiOnly),
    method_rule!("hooks/list", None, TuiOnly),
    method_rule!("marketplace/add", None, TuiOnly),
    method_rule!("marketplace/remove", None, TuiOnly),
    method_rule!("marketplace/upgrade", None, TuiOnly),
    method_rule!("plugin/list", None, TuiOnly),
    method_rule!("plugin/installed", None, TuiOnly),
    method_rule!("plugin/read", None, TuiOnly),
    method_rule!("plugin/skill/read", None, TuiOnly),
    method_rule!("plugin/share/save", None, TuiOnly),
    method_rule!("plugin/share/updateTargets", None, TuiOnly),
    method_rule!("plugin/share/list", None, TuiOnly),
    method_rule!("plugin/share/checkout", None, TuiOnly),
    method_rule!("plugin/share/delete", None, TuiOnly),
    method_rule!("app/read", None, TuiOnly),
    method_rule!("app/list", None, TuiOnly),
    method_rule!("app/installed", None, TuiOnly),
    method_rule!("fs/readFile", None, TuiOnly),
    method_rule!("fs/writeFile", None, TuiOnly),
    method_rule!("fs/createDirectory", None, TuiOnly),
    method_rule!("fs/getMetadata", None, TuiOnly),
    method_rule!("fs/readDirectory", None, TuiOnly),
    method_rule!("fs/remove", None, TuiOnly),
    method_rule!("fs/copy", None, TuiOnly),
    method_rule!("fs/watch", None, TuiOnly),
    method_rule!("fs/unwatch", None, TuiOnly),
    method_rule!("skills/config/write", None, TuiOnly),
    method_rule!("plugin/install", None, TuiOnly),
    method_rule!("plugin/uninstall", None, TuiOnly),
    method_rule!("thread/realtime/listVoices", None, TuiOnly),
    method_rule!("model/list", None, TuiOnly),
    method_rule!("modelProvider/capabilities/read", None, TuiOnly),
    method_rule!("experimentalFeature/list", None, TuiOnly),
    method_rule!("permissionProfile/list", None, TuiOnly),
    method_rule!("experimentalFeature/enablement/set", None, TuiOnly),
    method_rule!("remoteControl/enable", None, TuiOnly),
    method_rule!("remoteControl/disable", None, TuiOnly),
    method_rule!("remoteControl/status/read", None, TuiOnly),
    method_rule!("remoteControl/pairing/start", None, TuiOnly),
    method_rule!("remoteControl/pairing/status", None, TuiOnly),
    method_rule!("remoteControl/client/list", None, TuiOnly),
    method_rule!("remoteControl/client/revoke", None, TuiOnly),
    method_rule!("collaborationMode/list", None, TuiOnly),
    method_rule!("mock/experimentalMethod", None, TuiOnly),
    method_rule!("environment/add", None, TuiOnly),
    method_rule!("environment/info", None, TuiOnly),
    method_rule!("environment/status", None, TuiOnly),
    method_rule!("mcpServer/oauth/login", None, TuiOnly),
    method_rule!("config/mcpServer/reload", None, TuiOnly),
    method_rule!("mcpServerStatus/list", None, TuiOnly),
    method_rule!("windowsSandbox/setupStart", None, TuiOnly),
    method_rule!("windowsSandbox/readiness", None, TuiOnly),
    method_rule!("account/login/start", None, TuiOnly),
    method_rule!("account/login/cancel", None, TuiOnly),
    method_rule!("account/logout", None, TuiOnly),
    method_rule!("account/rateLimits/read", None, TuiOnly),
    method_rule!("account/rateLimitResetCredit/consume", None, TuiOnly),
    method_rule!("account/usage/read", None, TuiOnly),
    method_rule!("account/workspaceMessages/read", None, TuiOnly),
    method_rule!("account/sendAddCreditsNudgeEmail", None, TuiOnly),
    method_rule!("feedback/upload", None, TuiOnly),
    method_rule!("command/exec", None, TuiOnly),
    method_rule!("command/exec/write", None, TuiOnly),
    method_rule!("command/exec/terminate", None, TuiOnly),
    method_rule!("command/exec/resize", None, TuiOnly),
    method_rule!("process/spawn", None, TuiOnly),
    method_rule!("process/writeStdin", None, TuiOnly),
    method_rule!("process/kill", None, TuiOnly),
    method_rule!("process/resizePty", None, TuiOnly),
    method_rule!("config/read", None, TuiOnly),
    method_rule!("externalAgentConfig/detect", None, TuiOnly),
    method_rule!("externalAgentConfig/import", None, TuiOnly),
    method_rule!("externalAgentConfig/import/recordHistory", None, TuiOnly),
    method_rule!("externalAgentConfig/import/readHistories", None, TuiOnly),
    method_rule!("config/value/write", None, TuiOnly),
    method_rule!("config/batchWrite", None, TuiOnly),
    method_rule!("configRequirements/read", None, TuiOnly),
    method_rule!("account/read", None, TuiOnly),
    method_rule!("getConversationSummary", None, TuiOnly),
    method_rule!("gitDiffToRemote", None, TuiOnly),
    method_rule!("getAuthStatus", None, TuiOnly),
    method_rule!("fuzzyFileSearch", None, TuiOnly),
    method_rule!("fuzzyFileSearch/sessionStart", None, TuiOnly),
    method_rule!("fuzzyFileSearch/sessionUpdate", None, TuiOnly),
    method_rule!("fuzzyFileSearch/sessionStop", None, TuiOnly),
];

pub(crate) const SERVER_REQUEST_RESPONSE_ADMISSION: &[MethodAdmission] = &[
    method_rule!(
        "item/commandExecution/requestApproval",
        ExactThread,
        ActiveOwner
    ),
    method_rule!("item/fileChange/requestApproval", ExactThread, ActiveOwner),
    method_rule!("item/tool/requestUserInput", ExactThread, ActiveOwner),
    method_rule!("mcpServer/elicitation/request", ExactThread, ActiveOwner),
    method_rule!("item/permissions/requestApproval", ExactThread, ActiveOwner),
    method_rule!("item/tool/call", ExactThread, ActiveOwner),
    method_rule!("currentTime/read", ExactThread, ActiveOwner),
    method_rule!("account/chatgptAuthTokens/refresh", None, TuiOnly),
    method_rule!("attestation/generate", None, TuiOnly),
    method_rule!("applyPatchApproval", None, TuiOnly),
    method_rule!("execCommandApproval", None, TuiOnly),
];

pub(crate) const CONTINUATION_ADMISSION: &[ContinuationAdmission] = &[
    continuation_rule!(Cursor, MainThreadOnly, StandingSession),
    continuation_rule!(ResumeToken, MainThreadOnly, TuiOnly),
    continuation_rule!(Subscription, MainThreadOnly, StandingSession),
    continuation_rule!(ImplicitTarget, MainThreadOnly, StandingSession),
];

pub(crate) fn client_request_rule(method: &str) -> Option<AdmissionRule> {
    CLIENT_REQUEST_ADMISSION
        .iter()
        .find(|entry| entry.method == method)
        .map(|entry| entry.rule)
}

pub(crate) fn server_request_response_rule(method: &str) -> Option<AdmissionRule> {
    SERVER_REQUEST_RESPONSE_ADMISSION
        .iter()
        .find(|entry| entry.method == method)
        .map(|entry| entry.rule)
}

pub(crate) fn continuation_rule_for(kind: ContinuationKind) -> AdmissionRule {
    CONTINUATION_ADMISSION
        .iter()
        .find(|entry| entry.kind == kind)
        .map(|entry| entry.rule)
        .expect("every continuation kind must have an admission rule")
}

#[cfg(test)]
#[path = "controller_admission_tests.rs"]
mod tests;
