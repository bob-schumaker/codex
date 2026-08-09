//! Realtime transcript notifications from the app-server bridge.

use super::*;

impl ChatWidget {
    pub(super) fn on_realtime_transcript_delta(&mut self, role: String, delta: String) {
        match role.as_str() {
            // Assistant realtime transcript is equivalent to normal assistant
            // streaming for the TUI: render through the existing stream
            // controller so resize, raw transcript, and consolidation behavior
            // stay identical.
            "assistant" => self.on_agent_message_delta(delta),
            // User realtime deltas are provisional speech recognition. Render
            // only the final done text to avoid duplicate user history cells.
            "user" => {}
            other => {
                tracing::warn!(
                    role = other,
                    "ignoring realtime transcript delta for unsupported role"
                );
            }
        }
    }

    pub(super) fn on_realtime_transcript_done(&mut self, role: String, text: String) {
        match role.as_str() {
            "assistant" => self.finalize_completed_assistant_message(Some(&text)),
            "user" => self.on_realtime_user_transcript_done(text),
            other => {
                tracing::warn!(
                    role = other,
                    "ignoring realtime transcript completion for unsupported role"
                );
            }
        }
    }

    fn on_realtime_user_transcript_done(&mut self, text: String) {
        if text.trim().is_empty() {
            return;
        }
        self.on_user_message_display(UserMessageDisplay {
            message: text,
            remote_image_urls: Vec::new(),
            local_images: Vec::new(),
            text_elements: Vec::new(),
        });
    }
}
