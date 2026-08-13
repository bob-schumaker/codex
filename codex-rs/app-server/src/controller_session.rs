//! Controller session and input-ownership domain state.
//!
//! This module gives the controller request processor and admission gate one
//! coordinator-owned source of truth for standing authorization, active leases,
//! owner epochs, and terminal launch states.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use codex_app_server_protocol::ControllerAuthorizationChangedNotification;
use codex_app_server_protocol::ControllerAuthorizationChangedReason;
use codex_app_server_protocol::ControllerControlOwnershipChangedNotification;
use codex_app_server_protocol::ControllerControlOwnershipChangedReason;
use codex_app_server_protocol::ControllerEffectiveCapabilities;
use codex_app_server_protocol::ControllerLease as ProtocolControllerLease;
use codex_app_server_protocol::ControllerSession as ProtocolControllerSession;
use codex_protocol::ThreadId;
use uuid::Uuid;

use crate::transport::ConnectionId;

#[derive(Clone)]
pub(crate) struct ControllerSessionClock {
    now: Arc<dyn Fn() -> Instant + Send + Sync>,
}

impl ControllerSessionClock {
    pub(crate) fn now(&self) -> Instant {
        (self.now)()
    }

    pub(crate) fn from_fn(now: impl Fn() -> Instant + Send + Sync + 'static) -> Self {
        Self { now: Arc::new(now) }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ControllerSessionConfig {
    pub(crate) lease_duration: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControllerEnrollmentGrant {
    pub(crate) subject_id: String,
    pub(crate) main_thread_id: ThreadId,
    pub(crate) authorization_epoch: u64,
    pub(crate) authorization_expires_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControllerSession {
    pub(crate) session_id: String,
    pub(crate) connection_id: ConnectionId,
    pub(crate) main_thread_id: ThreadId,
    pub(crate) authorization_epoch: u64,
    pub(crate) authorization_expires_at: Instant,
    pub(crate) session_sequence: u64,
    pub(crate) active_lease: Option<ControllerActiveLease>,
}

impl ControllerSession {
    pub(crate) fn to_protocol(
        &self,
        owner: &InteractiveOwner,
        now: Instant,
    ) -> ProtocolControllerSession {
        let active_lease =
            self.current_active_lease(owner, now)
                .map(|lease| ProtocolControllerLease {
                    lease_id: lease.lease_id.clone(),
                    owner_epoch: lease.owner_epoch,
                    expires_in_ms: Some(deadline_remaining_ms(lease.expires_at, now)),
                });
        let lease_expires_in_ms = active_lease.as_ref().and_then(|lease| lease.expires_in_ms);

        ProtocolControllerSession {
            session_id: self.session_id.clone(),
            main_thread_id: self.main_thread_id.to_string(),
            active_lease,
            authorization_epoch: self.authorization_epoch,
            session_sequence: self.session_sequence,
            effective_capabilities: self.effective_capabilities(owner, now),
            lease_expires_in_ms,
            authorization_expires_in_ms: Some(deadline_remaining_ms(
                self.authorization_expires_at,
                now,
            )),
        }
    }

    fn current_active_lease(
        &self,
        owner: &InteractiveOwner,
        now: Instant,
    ) -> Option<&ControllerActiveLease> {
        let lease = self.active_lease.as_ref()?;
        if now >= lease.expires_at {
            return None;
        }
        let InteractiveOwner::ControllerOwned {
            connection_id,
            lease_id,
            owner_epoch,
        } = owner
        else {
            return None;
        };
        (*connection_id == self.connection_id
            && lease_id == &lease.lease_id
            && *owner_epoch == lease.owner_epoch)
            .then_some(lease)
    }

    fn effective_capabilities(
        &self,
        owner: &InteractiveOwner,
        now: Instant,
    ) -> ControllerEffectiveCapabilities {
        let standing_session = now < self.authorization_expires_at && !owner.is_terminal();
        let active_owner = self.current_active_lease(owner, now).is_some();
        let can_acquire = standing_session
            && match owner {
                InteractiveOwner::TuiOwned { .. } => true,
                InteractiveOwner::ControllerOwned { connection_id, .. } => {
                    *connection_id == self.connection_id
                }
                InteractiveOwner::TransferPending { .. }
                | InteractiveOwner::TuiUnavailable { .. }
                | InteractiveOwner::Closed => false,
            };

        ControllerEffectiveCapabilities {
            read_main_thread: standing_session,
            subscribe_main_thread: standing_session,
            acquire_control: can_acquire,
            release_control: standing_session,
            mutate_main_thread: active_owner,
            answer_prompts: active_owner,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControllerActiveLease {
    pub(crate) lease_id: String,
    pub(crate) owner_epoch: u64,
    pub(crate) expires_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InteractiveOwner {
    TuiOwned {
        owner_epoch: u64,
    },
    TransferPending {
        owner_epoch: u64,
    },
    ControllerOwned {
        connection_id: ConnectionId,
        lease_id: String,
        owner_epoch: u64,
    },
    TuiUnavailable {
        owner_epoch: u64,
    },
    Closed,
}

impl InteractiveOwner {
    pub(crate) fn owner_epoch(&self) -> Option<u64> {
        match self {
            Self::TuiOwned { owner_epoch }
            | Self::TransferPending { owner_epoch }
            | Self::ControllerOwned { owner_epoch, .. }
            | Self::TuiUnavailable { owner_epoch } => Some(*owner_epoch),
            Self::Closed => None,
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::TuiUnavailable { .. } | Self::Closed)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingControllerTransfer {
    connection_id: ConnectionId,
    owner_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ControllerSessionError {
    ParticipationRequired,
    AuthorizationExpired,
    OwnershipConflict,
    TransferPending,
    MainThreadClosed,
    TuiUnavailable,
    DifferentMainThread,
    StaleOwnership,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ControllerSessionNotification {
    AuthorizationChanged {
        connection_id: ConnectionId,
        notification: ControllerAuthorizationChangedNotification,
    },
    ControlOwnershipChanged {
        connection_id: ConnectionId,
        notification: ControllerControlOwnershipChangedNotification,
    },
}

/// Ownership status event delivered to the owning in-process TUI.
///
/// Controller JSON-RPC notifications are controller control-plane messages. This
/// status is the transport-local TUI state event emitted from the same
/// coordinator transition so the TUI can track who currently owns input for the
/// main thread without consuming controller notifications as application state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControllerOwnershipStatus {
    pub main_thread_id: ThreadId,
    pub owner: ControllerOwnershipStatusOwner,
    pub owner_epoch: u64,
    /// Whether this main thread still has any controller session, including a
    /// read-capable session that is not currently the interactive owner.
    pub has_controller_session: bool,
    pub reason: ControllerControlOwnershipChangedReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControllerOwnershipStatusOwner {
    Tui,
    Controller { session_id: String },
    TuiUnavailable,
    Closed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ControllerSessionEvents {
    pub(crate) controller_notifications: Vec<ControllerSessionNotification>,
    pub(crate) ownership_statuses: Vec<ControllerOwnershipStatus>,
}

pub(crate) struct ControllerSessionCoordinator {
    main_thread_id: ThreadId,
    clock: ControllerSessionClock,
    config: ControllerSessionConfig,
    owner: InteractiveOwner,
    last_owner_epoch: u64,
    sessions: HashMap<ConnectionId, ControllerSession>,
    next_session_sequence: u64,
    notifications: Vec<ControllerSessionNotification>,
    ownership_statuses: Vec<ControllerOwnershipStatus>,
}

impl ControllerSessionCoordinator {
    pub(crate) fn new(
        main_thread_id: ThreadId,
        clock: ControllerSessionClock,
        config: ControllerSessionConfig,
    ) -> Self {
        Self {
            main_thread_id,
            clock,
            config,
            owner: InteractiveOwner::TuiOwned { owner_epoch: 0 },
            last_owner_epoch: 0,
            sessions: HashMap::new(),
            next_session_sequence: 1,
            notifications: Vec::new(),
            ownership_statuses: Vec::new(),
        }
    }

    pub(crate) fn interactive_owner(&self) -> &InteractiveOwner {
        &self.owner
    }

    pub(crate) fn ownership_status_snapshot(
        &self,
        reason: ControllerControlOwnershipChangedReason,
    ) -> ControllerOwnershipStatus {
        ControllerOwnershipStatus {
            main_thread_id: self.main_thread_id,
            owner: self.ownership_status_owner(),
            owner_epoch: self.owner_epoch_for_notification(),
            has_controller_session: !self.sessions.is_empty(),
            reason,
        }
    }

    pub(crate) fn main_thread_id(&self) -> Option<ThreadId> {
        (!matches!(self.owner, InteractiveOwner::Closed)).then_some(self.main_thread_id)
    }

    pub(crate) fn session_for(&self, connection_id: ConnectionId) -> Option<&ControllerSession> {
        self.sessions.get(&connection_id)
    }

    pub(crate) fn protocol_session(
        &self,
        connection_id: ConnectionId,
    ) -> Option<ProtocolControllerSession> {
        let now = self.clock.now();
        self.sessions
            .get(&connection_id)
            .map(|session| session.to_protocol(&self.owner, now))
    }

    pub(crate) fn drain_notifications(&mut self) -> Vec<ControllerSessionNotification> {
        std::mem::take(&mut self.notifications)
    }

    pub(crate) fn drain_events(&mut self) -> ControllerSessionEvents {
        ControllerSessionEvents {
            controller_notifications: std::mem::take(&mut self.notifications),
            ownership_statuses: std::mem::take(&mut self.ownership_statuses),
        }
    }

    pub(crate) fn request_participation(
        &mut self,
        connection_id: ConnectionId,
        grant: ControllerEnrollmentGrant,
    ) -> Result<ProtocolControllerSession, ControllerSessionError> {
        let now = self.clock.now();
        self.expire_deadlines_at(now);
        self.ensure_main_thread(grant.main_thread_id)?;
        self.ensure_not_terminal()?;
        if now >= grant.authorization_expires_at {
            return Err(ControllerSessionError::AuthorizationExpired);
        }

        self.upsert_session(connection_id, grant);
        self.push_authorization_changed_for_connection(
            connection_id,
            ControllerAuthorizationChangedReason::Approved,
            AuthorizationNotificationSession::Current,
        );
        match self.owner.clone() {
            InteractiveOwner::TuiOwned { .. } => {
                let transfer = self.begin_transfer_to_controller(connection_id)?;
                self.complete_transfer_to_controller_with_reason(
                    transfer,
                    Some(ControllerControlOwnershipChangedReason::InitialLeaseGranted),
                )
            }
            InteractiveOwner::ControllerOwned { .. } | InteractiveOwner::TransferPending { .. } => {
                self.protocol_session(connection_id)
                    .ok_or(ControllerSessionError::ParticipationRequired)
            }
            InteractiveOwner::TuiUnavailable { .. } => Err(ControllerSessionError::TuiUnavailable),
            InteractiveOwner::Closed => Err(ControllerSessionError::MainThreadClosed),
        }
    }

    pub(crate) fn acquire_control(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<ProtocolControllerSession, ControllerSessionError> {
        let now = self.clock.now();
        let authorization_expired = self.connection_authorization_expired(connection_id, now);
        self.expire_deadlines_at(now);
        if authorization_expired {
            return Err(ControllerSessionError::AuthorizationExpired);
        }
        self.ensure_not_terminal()?;
        self.require_live_session(connection_id, now)?;
        if matches!(
            self.owner,
            InteractiveOwner::ControllerOwned {
                connection_id: owner_connection_id,
                ..
            } if owner_connection_id == connection_id
        ) {
            return self
                .protocol_session(connection_id)
                .ok_or(ControllerSessionError::ParticipationRequired);
        }

        let transfer = self.begin_transfer_to_controller(connection_id)?;
        self.complete_transfer_to_controller_with_reason(
            transfer,
            Some(ControllerControlOwnershipChangedReason::Acquired),
        )
    }

    pub(crate) fn begin_transfer_to_controller(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<PendingControllerTransfer, ControllerSessionError> {
        let now = self.clock.now();
        let authorization_expired = self.connection_authorization_expired(connection_id, now);
        self.expire_deadlines_at(now);
        if authorization_expired {
            return Err(ControllerSessionError::AuthorizationExpired);
        }
        self.ensure_not_terminal()?;
        self.require_live_session(connection_id, now)?;
        match self.owner.clone() {
            InteractiveOwner::TuiOwned { owner_epoch } => {
                let owner_epoch = owner_epoch + 1;
                self.set_owner(InteractiveOwner::TransferPending { owner_epoch });
                Ok(PendingControllerTransfer {
                    connection_id,
                    owner_epoch,
                })
            }
            InteractiveOwner::ControllerOwned { .. } => {
                Err(ControllerSessionError::OwnershipConflict)
            }
            InteractiveOwner::TransferPending { .. } => {
                Err(ControllerSessionError::TransferPending)
            }
            InteractiveOwner::TuiUnavailable { .. } => Err(ControllerSessionError::TuiUnavailable),
            InteractiveOwner::Closed => Err(ControllerSessionError::MainThreadClosed),
        }
    }

    pub(crate) fn complete_transfer_to_controller(
        &mut self,
        transfer: PendingControllerTransfer,
    ) -> Result<ProtocolControllerSession, ControllerSessionError> {
        self.complete_transfer_to_controller_with_reason(transfer, None)
    }

    fn complete_transfer_to_controller_with_reason(
        &mut self,
        transfer: PendingControllerTransfer,
        reason: Option<ControllerControlOwnershipChangedReason>,
    ) -> Result<ProtocolControllerSession, ControllerSessionError> {
        let now = self.clock.now();
        self.require_live_session(transfer.connection_id, now)?;
        let result = match &self.owner {
            InteractiveOwner::TransferPending { owner_epoch }
                if *owner_epoch == transfer.owner_epoch =>
            {
                self.issue_lease(transfer.connection_id, transfer.owner_epoch, now)
            }
            InteractiveOwner::ControllerOwned { connection_id, .. }
                if *connection_id == transfer.connection_id =>
            {
                self.protocol_session(transfer.connection_id)
                    .ok_or(ControllerSessionError::ParticipationRequired)
            }
            InteractiveOwner::Closed => Err(ControllerSessionError::MainThreadClosed),
            InteractiveOwner::TuiUnavailable { .. } => Err(ControllerSessionError::TuiUnavailable),
            _ => Err(ControllerSessionError::StaleOwnership),
        };
        if result.is_ok()
            && let Some(reason) = reason
        {
            self.push_control_ownership_changed_for_all_sessions(reason);
        }
        result
    }

    pub(crate) fn release_control(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<ProtocolControllerSession, ControllerSessionError> {
        let now = self.clock.now();
        let authorization_expired = self.connection_authorization_expired(connection_id, now);
        self.expire_deadlines_at(now);
        if authorization_expired {
            return Err(ControllerSessionError::AuthorizationExpired);
        }
        self.ensure_not_terminal()?;
        self.require_live_session(connection_id, now)?;

        match self.owner.clone() {
            InteractiveOwner::ControllerOwned {
                connection_id: owner_connection_id,
                owner_epoch,
                ..
            } if owner_connection_id == connection_id => {
                self.clear_active_lease(connection_id);
                self.transfer_to_tui_owned_after(owner_epoch);
                self.push_control_ownership_changed_for_all_sessions(
                    ControllerControlOwnershipChangedReason::Released,
                );
            }
            InteractiveOwner::TransferPending { .. } => {
                return Err(ControllerSessionError::TransferPending);
            }
            InteractiveOwner::TuiUnavailable { .. } => {
                return Err(ControllerSessionError::TuiUnavailable);
            }
            InteractiveOwner::Closed => return Err(ControllerSessionError::MainThreadClosed),
            InteractiveOwner::TuiOwned { .. } | InteractiveOwner::ControllerOwned { .. } => {}
        }

        self.protocol_session(connection_id)
            .ok_or(ControllerSessionError::ParticipationRequired)
    }

    pub(crate) fn require_standing_session(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<ThreadId, ControllerSessionError> {
        let now = self.clock.now();
        let authorization_expired = self.connection_authorization_expired(connection_id, now);
        self.expire_deadlines_at(now);
        if authorization_expired {
            return Err(ControllerSessionError::AuthorizationExpired);
        }
        self.ensure_not_terminal()?;
        self.require_live_session(connection_id, now)?;
        Ok(self.main_thread_id)
    }

    pub(crate) fn require_active_owner(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<ThreadId, ControllerSessionError> {
        self.require_active_owner_with_epoch(connection_id)
            .map(|(main_thread_id, _owner_epoch)| main_thread_id)
    }

    pub(crate) fn require_active_owner_with_epoch(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<(ThreadId, u64), ControllerSessionError> {
        let now = self.clock.now();
        let authorization_expired = self.connection_authorization_expired(connection_id, now);
        self.expire_deadlines_at(now);
        if authorization_expired {
            return Err(ControllerSessionError::AuthorizationExpired);
        }
        self.ensure_not_terminal()?;
        self.require_live_session(connection_id, now)?;

        let Some(session) = self.sessions.get(&connection_id) else {
            return Err(ControllerSessionError::ParticipationRequired);
        };
        if session.current_active_lease(&self.owner, now).is_some() {
            let InteractiveOwner::ControllerOwned { owner_epoch, .. } = &self.owner else {
                return Err(ControllerSessionError::StaleOwnership);
            };
            return Ok((self.main_thread_id, *owner_epoch));
        }

        match &self.owner {
            InteractiveOwner::ControllerOwned {
                connection_id: owner_connection_id,
                ..
            } if *owner_connection_id != connection_id => {
                Err(ControllerSessionError::OwnershipConflict)
            }
            InteractiveOwner::TransferPending { .. } => {
                Err(ControllerSessionError::TransferPending)
            }
            InteractiveOwner::TuiUnavailable { .. } => Err(ControllerSessionError::TuiUnavailable),
            InteractiveOwner::Closed => Err(ControllerSessionError::MainThreadClosed),
            InteractiveOwner::TuiOwned { .. } | InteractiveOwner::ControllerOwned { .. } => {
                Err(ControllerSessionError::StaleOwnership)
            }
        }
    }

    pub(crate) fn reclaim_for_tui(&mut self) -> Result<(), ControllerSessionError> {
        let now = self.clock.now();
        self.expire_active_lease_at(now);
        match self.owner.clone() {
            InteractiveOwner::ControllerOwned {
                connection_id,
                owner_epoch,
                ..
            } => {
                self.clear_active_lease(connection_id);
                self.transfer_to_tui_owned_after(owner_epoch);
                self.push_control_ownership_changed_for_all_sessions(
                    ControllerControlOwnershipChangedReason::ReclaimedByTui,
                );
                Ok(())
            }
            InteractiveOwner::TuiOwned { .. } => Ok(()),
            InteractiveOwner::TransferPending { owner_epoch } => {
                self.transfer_to_tui_owned_after(owner_epoch);
                Ok(())
            }
            InteractiveOwner::TuiUnavailable { .. } => Err(ControllerSessionError::TuiUnavailable),
            InteractiveOwner::Closed => Err(ControllerSessionError::MainThreadClosed),
        }
    }

    pub(crate) fn revoke_session(&mut self, connection_id: ConnectionId) {
        self.revoke_session_with_reason(
            connection_id,
            ControllerAuthorizationChangedReason::Revoked,
            ControllerControlOwnershipChangedReason::AuthorizationRevoked,
            NotifyRevokedConnection::Yes,
        );
    }

    /// Revokes every controller session for this thread in response to an owning-TUI policy action.
    pub(crate) fn revoke_all_sessions_for_tui_policy(&mut self) {
        let mut revoked_sessions = std::mem::take(&mut self.sessions)
            .into_values()
            .collect::<Vec<_>>();
        if revoked_sessions.is_empty() {
            return;
        }
        if let InteractiveOwner::ControllerOwned { owner_epoch, .. } = self.owner.clone() {
            self.transfer_to_tui_owned_after(owner_epoch);
        }
        for session in &mut revoked_sessions {
            session.session_sequence = self.next_session_sequence();
            self.push_control_ownership_changed_for_session(
                session,
                ControllerControlOwnershipChangedReason::AuthorizationRevoked,
                ControlNotificationLease::None,
            );
            self.push_authorization_changed_for_session(
                session,
                ControllerAuthorizationChangedReason::Revoked,
                AuthorizationNotificationSession::None,
            );
        }
        self.push_ownership_status(ControllerControlOwnershipChangedReason::AuthorizationRevoked);
    }

    pub(crate) fn sign_off_session(&mut self, connection_id: ConnectionId) {
        self.revoke_session_with_reason(
            connection_id,
            ControllerAuthorizationChangedReason::SignOff,
            ControllerControlOwnershipChangedReason::SignOff,
            NotifyRevokedConnection::No,
        );
    }

    pub(crate) fn disconnect_session(&mut self, connection_id: ConnectionId) {
        self.revoke_session_with_reason(
            connection_id,
            ControllerAuthorizationChangedReason::Disconnected,
            ControllerControlOwnershipChangedReason::ControllerDisconnected,
            NotifyRevokedConnection::No,
        );
    }

    pub(crate) fn expire_deadlines(&mut self) {
        let now = self.clock.now();
        self.expire_deadlines_at(now);
    }

    pub(crate) fn mark_tui_unavailable(&mut self) -> Result<(), ControllerSessionError> {
        if matches!(&self.owner, InteractiveOwner::Closed) {
            return Err(ControllerSessionError::MainThreadClosed);
        }
        self.clear_all_active_leases();
        let owner_epoch = self.next_owner_epoch();
        self.set_owner(InteractiveOwner::TuiUnavailable { owner_epoch });
        let sessions = std::mem::take(&mut self.sessions)
            .into_values()
            .collect::<Vec<_>>();
        for session in &sessions {
            self.push_control_ownership_changed_for_session(
                session,
                ControllerControlOwnershipChangedReason::TuiUnavailable,
                ControlNotificationLease::None,
            );
            self.push_authorization_changed_for_session(
                session,
                ControllerAuthorizationChangedReason::TuiUnavailable,
                AuthorizationNotificationSession::None,
            );
        }
        self.push_ownership_status(ControllerControlOwnershipChangedReason::TuiUnavailable);
        Ok(())
    }

    pub(crate) fn close_main_thread(&mut self) {
        self.clear_all_active_leases();
        self.last_owner_epoch = self.next_owner_epoch();
        self.owner = InteractiveOwner::Closed;
        let sessions = std::mem::take(&mut self.sessions)
            .into_values()
            .collect::<Vec<_>>();
        for session in &sessions {
            self.push_control_ownership_changed_for_session(
                session,
                ControllerControlOwnershipChangedReason::MainThreadClosed,
                ControlNotificationLease::None,
            );
            self.push_authorization_changed_for_session(
                session,
                ControllerAuthorizationChangedReason::MainThreadClosed,
                AuthorizationNotificationSession::None,
            );
        }
        self.push_ownership_status(ControllerControlOwnershipChangedReason::MainThreadClosed);
    }

    fn upsert_session(&mut self, connection_id: ConnectionId, grant: ControllerEnrollmentGrant) {
        let session_id = self
            .sessions
            .get(&connection_id)
            .map(|session| session.session_id.clone())
            .unwrap_or_else(|| format!("controller-session-{}", Uuid::now_v7()));
        let active_lease = self
            .sessions
            .get(&connection_id)
            .and_then(|session| session.active_lease.clone());
        let session_sequence = self.next_session_sequence();
        self.sessions.insert(
            connection_id,
            ControllerSession {
                session_id,
                connection_id,
                main_thread_id: grant.main_thread_id,
                authorization_epoch: grant.authorization_epoch,
                authorization_expires_at: grant.authorization_expires_at,
                session_sequence,
                active_lease,
            },
        );
    }

    fn issue_lease(
        &mut self,
        connection_id: ConnectionId,
        owner_epoch: u64,
        now: Instant,
    ) -> Result<ProtocolControllerSession, ControllerSessionError> {
        let session_sequence = self.next_session_sequence();
        let lease = ControllerActiveLease {
            lease_id: format!("controller-lease-{}", Uuid::now_v7()),
            owner_epoch,
            expires_at: now + self.config.lease_duration,
        };
        let session = self
            .sessions
            .get_mut(&connection_id)
            .ok_or(ControllerSessionError::ParticipationRequired)?;
        session.active_lease = Some(lease.clone());
        session.session_sequence = session_sequence;
        self.set_owner(InteractiveOwner::ControllerOwned {
            connection_id,
            lease_id: lease.lease_id,
            owner_epoch,
        });
        self.protocol_session(connection_id)
            .ok_or(ControllerSessionError::ParticipationRequired)
    }

    fn require_live_session(
        &self,
        connection_id: ConnectionId,
        now: Instant,
    ) -> Result<(), ControllerSessionError> {
        let session = self
            .sessions
            .get(&connection_id)
            .ok_or(ControllerSessionError::ParticipationRequired)?;
        if now >= session.authorization_expires_at {
            return Err(ControllerSessionError::AuthorizationExpired);
        }
        Ok(())
    }

    fn connection_authorization_expired(&self, connection_id: ConnectionId, now: Instant) -> bool {
        self.sessions
            .get(&connection_id)
            .is_some_and(|session| now >= session.authorization_expires_at)
    }

    fn ensure_main_thread(&self, main_thread_id: ThreadId) -> Result<(), ControllerSessionError> {
        if main_thread_id != self.main_thread_id {
            return Err(ControllerSessionError::DifferentMainThread);
        }
        Ok(())
    }

    fn ensure_not_terminal(&self) -> Result<(), ControllerSessionError> {
        match &self.owner {
            InteractiveOwner::TuiUnavailable { .. } => Err(ControllerSessionError::TuiUnavailable),
            InteractiveOwner::Closed => Err(ControllerSessionError::MainThreadClosed),
            InteractiveOwner::TuiOwned { .. }
            | InteractiveOwner::TransferPending { .. }
            | InteractiveOwner::ControllerOwned { .. } => Ok(()),
        }
    }

    fn expire_deadlines_at(&mut self, now: Instant) {
        self.expire_authorizations_at(now);
        self.expire_active_lease_at(now);
    }

    fn expire_authorizations_at(&mut self, now: Instant) {
        let expired_connections = self
            .sessions
            .iter()
            .filter_map(|(connection_id, session)| {
                (now >= session.authorization_expires_at).then_some(*connection_id)
            })
            .collect::<Vec<_>>();
        for connection_id in expired_connections {
            self.revoke_session_with_reason(
                connection_id,
                ControllerAuthorizationChangedReason::Expired,
                ControllerControlOwnershipChangedReason::AuthorizationRevoked,
                NotifyRevokedConnection::Yes,
            );
        }
    }

    fn expire_active_lease_at(&mut self, now: Instant) {
        let InteractiveOwner::ControllerOwned {
            connection_id,
            owner_epoch,
            ..
        } = self.owner.clone()
        else {
            return;
        };
        let lease_expired = self
            .sessions
            .get(&connection_id)
            .and_then(|session| session.active_lease.as_ref())
            .is_none_or(|lease| now >= lease.expires_at);
        if lease_expired {
            self.clear_active_lease(connection_id);
            self.transfer_to_tui_owned_after(owner_epoch);
            self.push_control_ownership_changed_for_all_sessions(
                ControllerControlOwnershipChangedReason::LeaseExpired,
            );
        }
    }

    fn transfer_to_tui_owned_after(&mut self, owner_epoch: u64) {
        let owner_epoch = owner_epoch + 1;
        self.set_owner(InteractiveOwner::TuiOwned { owner_epoch });
    }

    fn clear_active_lease(&mut self, connection_id: ConnectionId) {
        let session_sequence = self.next_session_sequence();
        if let Some(session) = self.sessions.get_mut(&connection_id) {
            session.active_lease = None;
            session.session_sequence = session_sequence;
        }
    }

    fn clear_all_active_leases(&mut self) {
        let connection_ids = self.sessions.keys().copied().collect::<Vec<_>>();
        for connection_id in connection_ids {
            self.clear_active_lease(connection_id);
        }
    }

    fn next_session_sequence(&mut self) -> u64 {
        let sequence = self.next_session_sequence;
        self.next_session_sequence += 1;
        sequence
    }

    fn next_owner_epoch(&self) -> u64 {
        self.owner.owner_epoch().unwrap_or(self.last_owner_epoch) + 1
    }

    fn set_owner(&mut self, owner: InteractiveOwner) {
        if let Some(owner_epoch) = owner.owner_epoch() {
            self.last_owner_epoch = owner_epoch;
        }
        self.owner = owner;
    }

    fn revoke_session_with_reason(
        &mut self,
        connection_id: ConnectionId,
        authorization_reason: ControllerAuthorizationChangedReason,
        ownership_reason: ControllerControlOwnershipChangedReason,
        notify_revoked_connection: NotifyRevokedConnection,
    ) {
        let Some(mut revoked_session) = self.sessions.remove(&connection_id) else {
            return;
        };
        revoked_session.session_sequence = self.next_session_sequence();
        let revoked_was_owner = matches!(
            &self.owner,
            InteractiveOwner::ControllerOwned {
                connection_id: owner_connection_id,
                ..
            } if *owner_connection_id == connection_id
        );
        if revoked_was_owner
            && let InteractiveOwner::ControllerOwned { owner_epoch, .. } = self.owner.clone()
        {
            self.transfer_to_tui_owned_after(owner_epoch);
            if matches!(notify_revoked_connection, NotifyRevokedConnection::Yes) {
                self.push_control_ownership_changed_for_session(
                    &revoked_session,
                    ownership_reason.clone(),
                    ControlNotificationLease::None,
                );
            }
            self.push_control_ownership_changed_for_all_sessions(ownership_reason);
        }
        if matches!(notify_revoked_connection, NotifyRevokedConnection::Yes) {
            self.push_authorization_changed_for_session(
                &revoked_session,
                authorization_reason,
                AuthorizationNotificationSession::None,
            );
        }
    }

    fn push_authorization_changed_for_all_sessions(
        &mut self,
        reason: ControllerAuthorizationChangedReason,
        session_state: AuthorizationNotificationSession,
    ) {
        let sessions = self.sessions.values().cloned().collect::<Vec<_>>();
        for session in sessions {
            self.push_authorization_changed_for_session(&session, reason.clone(), session_state);
        }
    }

    fn push_authorization_changed_for_connection(
        &mut self,
        connection_id: ConnectionId,
        reason: ControllerAuthorizationChangedReason,
        session_state: AuthorizationNotificationSession,
    ) {
        let Some(session) = self.sessions.get(&connection_id).cloned() else {
            return;
        };
        self.push_authorization_changed_for_session(&session, reason, session_state);
    }

    fn push_authorization_changed_for_session(
        &mut self,
        session: &ControllerSession,
        reason: ControllerAuthorizationChangedReason,
        session_state: AuthorizationNotificationSession,
    ) {
        let now = self.clock.now();
        let protocol_session = match session_state {
            AuthorizationNotificationSession::Current => {
                Some(session.to_protocol(&self.owner, now))
            }
            AuthorizationNotificationSession::None => None,
        };
        self.notifications
            .push(ControllerSessionNotification::AuthorizationChanged {
                connection_id: session.connection_id,
                notification: ControllerAuthorizationChangedNotification {
                    session_id: session.session_id.clone(),
                    main_thread_id: session.main_thread_id.to_string(),
                    reason,
                    authorization_epoch: session.authorization_epoch,
                    owner_epoch: self.owner_epoch_for_notification(),
                    session_sequence: session.session_sequence,
                    session: protocol_session,
                },
            });
    }

    fn push_control_ownership_changed_for_all_sessions(
        &mut self,
        reason: ControllerControlOwnershipChangedReason,
    ) {
        self.push_ownership_status(reason.clone());
        let sessions = self.sessions.values().cloned().collect::<Vec<_>>();
        for session in sessions {
            self.push_control_ownership_changed_for_session(
                &session,
                reason.clone(),
                ControlNotificationLease::Current,
            );
        }
    }

    fn push_control_ownership_changed_for_session(
        &mut self,
        session: &ControllerSession,
        reason: ControllerControlOwnershipChangedReason,
        lease_state: ControlNotificationLease,
    ) {
        let now = self.clock.now();
        let active_lease = match lease_state {
            ControlNotificationLease::Current => session.to_protocol(&self.owner, now).active_lease,
            ControlNotificationLease::None => None,
        };
        self.notifications
            .push(ControllerSessionNotification::ControlOwnershipChanged {
                connection_id: session.connection_id,
                notification: ControllerControlOwnershipChangedNotification {
                    session_id: session.session_id.clone(),
                    main_thread_id: session.main_thread_id.to_string(),
                    reason,
                    authorization_epoch: session.authorization_epoch,
                    owner_epoch: self.owner_epoch_for_notification(),
                    session_sequence: session.session_sequence,
                    active_lease,
                },
            });
    }

    fn owner_epoch_for_notification(&self) -> u64 {
        self.owner.owner_epoch().unwrap_or(self.last_owner_epoch)
    }

    fn push_ownership_status(&mut self, reason: ControllerControlOwnershipChangedReason) {
        self.ownership_statuses.push(ControllerOwnershipStatus {
            main_thread_id: self.main_thread_id,
            owner: self.ownership_status_owner(),
            owner_epoch: self.owner_epoch_for_notification(),
            has_controller_session: !self.sessions.is_empty(),
            reason,
        });
    }

    fn ownership_status_owner(&self) -> ControllerOwnershipStatusOwner {
        match &self.owner {
            InteractiveOwner::TuiOwned { .. } | InteractiveOwner::TransferPending { .. } => {
                ControllerOwnershipStatusOwner::Tui
            }
            InteractiveOwner::ControllerOwned { connection_id, .. } => {
                let session_id = self
                    .sessions
                    .get(connection_id)
                    .map(|session| session.session_id.clone())
                    .unwrap_or_else(|| format!("controller-connection-{}", connection_id.0));
                ControllerOwnershipStatusOwner::Controller { session_id }
            }
            InteractiveOwner::TuiUnavailable { .. } => {
                ControllerOwnershipStatusOwner::TuiUnavailable
            }
            InteractiveOwner::Closed => ControllerOwnershipStatusOwner::Closed,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthorizationNotificationSession {
    Current,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ControlNotificationLease {
    Current,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NotifyRevokedConnection {
    Yes,
    No,
}

fn deadline_remaining_ms(deadline: Instant, now: Instant) -> u64 {
    let remaining = deadline.saturating_duration_since(now).as_millis();
    u64::try_from(remaining).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "controller_session_tests.rs"]
mod tests;
