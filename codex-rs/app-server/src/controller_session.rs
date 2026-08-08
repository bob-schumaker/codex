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

pub(crate) struct ControllerSessionCoordinator {
    main_thread_id: ThreadId,
    clock: ControllerSessionClock,
    config: ControllerSessionConfig,
    owner: InteractiveOwner,
    sessions: HashMap<ConnectionId, ControllerSession>,
    next_session_sequence: u64,
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
            sessions: HashMap::new(),
            next_session_sequence: 1,
        }
    }

    pub(crate) fn interactive_owner(&self) -> &InteractiveOwner {
        &self.owner
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
        match self.owner.clone() {
            InteractiveOwner::TuiOwned { .. } => {
                let transfer = self.begin_transfer_to_controller(connection_id)?;
                self.complete_transfer_to_controller(transfer)
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
        self.complete_transfer_to_controller(transfer)
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
        self.require_live_session(connection_id, now)?;
        match self.owner.clone() {
            InteractiveOwner::TuiOwned { owner_epoch } => {
                let owner_epoch = owner_epoch + 1;
                self.owner = InteractiveOwner::TransferPending { owner_epoch };
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
        let now = self.clock.now();
        self.require_live_session(transfer.connection_id, now)?;
        match &self.owner {
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
        }
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
        self.require_live_session(connection_id, now)?;

        match self.owner.clone() {
            InteractiveOwner::ControllerOwned {
                connection_id: owner_connection_id,
                owner_epoch,
                ..
            } if owner_connection_id == connection_id => {
                self.clear_active_lease(connection_id);
                self.transfer_to_tui_owned_after(owner_epoch);
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
        self.sessions.remove(&connection_id);
        if matches!(
            &self.owner,
            InteractiveOwner::ControllerOwned {
                connection_id: owner_connection_id,
                ..
            } if *owner_connection_id == connection_id
        ) && let InteractiveOwner::ControllerOwned { owner_epoch, .. } = self.owner.clone()
        {
            self.transfer_to_tui_owned_after(owner_epoch);
        }
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
        self.owner = InteractiveOwner::TuiUnavailable { owner_epoch };
        Ok(())
    }

    pub(crate) fn close_main_thread(&mut self) {
        self.clear_all_active_leases();
        self.sessions.clear();
        self.owner = InteractiveOwner::Closed;
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
        self.owner = InteractiveOwner::ControllerOwned {
            connection_id,
            lease_id: lease.lease_id,
            owner_epoch,
        };
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
            self.revoke_session(connection_id);
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
        }
    }

    fn transfer_to_tui_owned_after(&mut self, owner_epoch: u64) {
        let owner_epoch = owner_epoch + 1;
        self.owner = InteractiveOwner::TransferPending { owner_epoch };
        self.owner = InteractiveOwner::TuiOwned { owner_epoch };
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
        self.owner.owner_epoch().unwrap_or(0) + 1
    }
}

fn deadline_remaining_ms(deadline: Instant, now: Instant) -> u64 {
    let remaining = deadline.saturating_duration_since(now).as_millis();
    u64::try_from(remaining).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "controller_session_tests.rs"]
mod tests;
