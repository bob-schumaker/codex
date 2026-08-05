use super::*;
use codex_app_server_client::InProcessControllerParticipationRequest;
use codex_app_server_client::NativeControllerParticipationDecision;

impl ChatWidget {
    pub(crate) fn open_controller_participation_prompt(
        &mut self,
        request: InProcessControllerParticipationRequest,
    ) {
        let approve_request_id = request.request_id;
        let reject_request_id = request.request_id;
        let cancel_request_id = request.request_id;
        let controller_name = request.controller_name.clone();
        let on_cancel = Some(Box::new(move |tx: &AppEventSender| {
            tx.send(AppEvent::RespondControllerParticipation {
                request_id: cancel_request_id,
                decision: NativeControllerParticipationDecision::Rejected {
                    reason: "controller participation prompt was dismissed".to_string(),
                },
            });
        }) as Box<dyn Fn(&AppEventSender) + Send + Sync>);
        let items = vec![
            SelectionItem {
                name: "Allow controller".to_string(),
                description: Some(
                    "Grant this local controller read access and current input control."
                        .to_string(),
                ),
                actions: vec![Box::new(move |tx| {
                    tx.send(AppEvent::RespondControllerParticipation {
                        request_id: approve_request_id,
                        decision: NativeControllerParticipationDecision::Approved,
                    });
                })],
                dismiss_on_select: true,
                ..Default::default()
            },
            SelectionItem {
                name: "Deny controller".to_string(),
                description: Some("Leave input control with this TUI.".to_string()),
                actions: vec![Box::new(move |tx| {
                    tx.send(AppEvent::RespondControllerParticipation {
                        request_id: reject_request_id,
                        decision: NativeControllerParticipationDecision::Rejected {
                            reason: "controller participation rejected by TUI user".to_string(),
                        },
                    });
                })],
                dismiss_on_select: true,
                ..Default::default()
            },
        ];

        self.bottom_pane.show_selection_view(SelectionViewParams {
            title: Some(format!("Allow {controller_name} to control this session?")),
            subtitle: Some(format!(
                "{} · Main thread: {}",
                request.description, request.main_thread_id
            )),
            footer_hint: Some(standard_popup_hint_line()),
            items,
            on_cancel,
            ..Default::default()
        });
    }
}
