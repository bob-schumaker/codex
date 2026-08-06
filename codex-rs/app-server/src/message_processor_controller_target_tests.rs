use super::*;
use crate::controller_admission::client_request_rule;
use codex_app_server_protocol::ThreadResumeParams;
use pretty_assertions::assert_eq;

#[test]
fn thread_resume_extracts_exact_controller_thread_target() {
    let thread_id = "00000000-0000-0000-0000-000000000123".to_string();
    let request = ClientRequest::ThreadResume {
        request_id: RequestId::Integer(1),
        params: ThreadResumeParams {
            thread_id: thread_id.clone(),
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
        },
    };
    let rule = client_request_rule("thread/resume").expect("thread/resume should be admitted");

    let target = controller_request_target(&request, rule).expect("target extraction should pass");

    match target {
        ControllerRequestTarget::ExactThread(actual) => assert_eq!(actual, thread_id),
        ControllerRequestTarget::None | ControllerRequestTarget::CollectionFiltered => {
            panic!("thread/resume should extract an exact thread target")
        }
    }
}
