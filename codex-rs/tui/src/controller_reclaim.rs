//! TUI command classification for external-controller reclaim policy.
//!
//! The TUI remains the primary local input surface. Once external controllers
//! can hold an interactive-control lease, any TUI-originated command that
//! affects the main thread must reclaim that lease before the command is
//! admitted. This module owns the policy boundary so dispatch code only needs
//! to call the hook before submitting coordinator-facing commands.

use crate::app_command::AppCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControllerReclaimEffect {
    ThreadAffecting,
    DisplayOnly,
}

impl ControllerReclaimEffect {
    pub(crate) fn reclaims_control(self) -> bool {
        match self {
            Self::ThreadAffecting => true,
            Self::DisplayOnly => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControllerReclaimDecision {
    ReclaimControl,
    PreserveCurrentOwner,
}

#[derive(Debug, Default)]
pub(crate) struct ControllerReclaimHook;

impl ControllerReclaimHook {
    pub(crate) fn observe_app_command(&self, command: &AppCommand) -> ControllerReclaimDecision {
        ControllerReclaimDecision::from_effect(command.controller_reclaim_effect())
    }
}

impl ControllerReclaimDecision {
    fn from_effect(effect: ControllerReclaimEffect) -> Self {
        if effect.reclaims_control() {
            Self::ReclaimControl
        } else {
            Self::PreserveCurrentOwner
        }
    }
}

#[cfg(test)]
#[path = "controller_reclaim_tests.rs"]
mod tests;
