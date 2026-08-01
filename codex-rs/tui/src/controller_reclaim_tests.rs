use std::collections::HashMap;
use std::path::PathBuf;

use codex_app_server_protocol::AskForApproval;
use codex_app_server_protocol::CommandExecutionApprovalDecision;
use codex_app_server_protocol::FileChangeApprovalDecision;
use codex_app_server_protocol::McpServerElicitationAction;
use codex_app_server_protocol::RequestId as AppServerRequestId;
use codex_app_server_protocol::ReviewTarget;
use codex_app_server_protocol::ToolRequestUserInputAnswer;
use codex_app_server_protocol::ToolRequestUserInputResponse;
use codex_app_server_protocol::UserInput;
use codex_protocol::request_permissions::PermissionGrantScope;
use codex_protocol::request_permissions::RequestPermissionProfile;
use codex_protocol::request_permissions::RequestPermissionsResponse;
use pretty_assertions::assert_eq;

use super::ControllerReclaimDecision;
use super::ControllerReclaimEffect;
use super::ControllerReclaimHook;
use crate::app_command::AppCommand;

#[test]
fn user_turn_and_interrupt_reclaim_control() {
    assert_thread_affecting(AppCommand::interrupt());
    assert_thread_affecting(user_turn_command());
}

#[test]
fn approval_and_user_input_replies_reclaim_control() {
    assert_thread_affecting(AppCommand::exec_approval(
        "exec".to_string(),
        Some("turn".to_string()),
        CommandExecutionApprovalDecision::Cancel,
    ));
    assert_thread_affecting(AppCommand::patch_approval(
        "patch".to_string(),
        FileChangeApprovalDecision::Accept,
    ));
    assert_thread_affecting(AppCommand::resolve_elicitation(
        "server".to_string(),
        AppServerRequestId::Integer(1),
        McpServerElicitationAction::Cancel,
        None,
        None,
    ));
    assert_thread_affecting(AppCommand::user_input_answer(
        "input".to_string(),
        ToolRequestUserInputResponse {
            answers: HashMap::from([(
                "question".to_string(),
                ToolRequestUserInputAnswer {
                    answers: vec!["answer".to_string()],
                },
            )]),
        },
    ));
    assert_thread_affecting(AppCommand::request_permissions_response(
        "permissions".to_string(),
        RequestPermissionsResponse {
            permissions: RequestPermissionProfile::default(),
            scope: PermissionGrantScope::Turn,
            strict_auto_review: false,
        },
    ));
}

#[test]
fn mutating_slash_command_outputs_reclaim_control() {
    assert_thread_affecting(AppCommand::clean_background_terminals());
    assert_thread_affecting(AppCommand::run_user_shell_command("make test".to_string()));
    assert_thread_affecting(AppCommand::compact());
    assert_thread_affecting(AppCommand::set_thread_name("renamed".to_string()));
    assert_thread_affecting(AppCommand::review(ReviewTarget::UncommittedChanges));
    assert_thread_affecting(AppCommand::OverrideTurnContext {
        cwd: Some(PathBuf::from("/tmp")),
        approval_policy: None,
        approvals_reviewer: None,
        permission_profile: None,
        active_permission_profile: None,
        windows_sandbox_level: None,
        model: None,
        effort: None,
        summary: None,
        service_tier: None,
        collaboration_mode: None,
        personality: None,
    });
    assert_thread_affecting(AppCommand::reload_user_config());
}

#[test]
fn display_only_commands_preserve_current_owner() {
    let command = AppCommand::list_skills(vec![PathBuf::from("/tmp")], /*force_reload*/ false);

    assert_eq!(
        command.controller_reclaim_effect(),
        ControllerReclaimEffect::DisplayOnly
    );
    assert_eq!(
        ControllerReclaimHook.observe_app_command(&command),
        ControllerReclaimDecision::PreserveCurrentOwner
    );
}

fn assert_thread_affecting(command: AppCommand) {
    assert_eq!(
        command.controller_reclaim_effect(),
        ControllerReclaimEffect::ThreadAffecting
    );
    assert_eq!(
        ControllerReclaimHook.observe_app_command(&command),
        ControllerReclaimDecision::ReclaimControl
    );
}

fn user_turn_command() -> AppCommand {
    AppCommand::UserTurn {
        items: vec![UserInput::Text {
            text: "hello".to_string(),
            text_elements: Vec::new(),
        }],
        cwd: PathBuf::from("/tmp"),
        approval_policy: AskForApproval::OnRequest,
        approvals_reviewer: None,
        active_permission_profile: None,
        model: "gpt-5".to_string(),
        effort: None,
        summary: None,
        service_tier: None,
        final_output_json_schema: None,
        collaboration_mode: None,
        personality: None,
    }
}
