use super::*;
use codex_app_server_protocol::ControllerAuthorizationChangedReason;
use codex_app_server_protocol::ControllerControlOwnershipChangedReason;
use codex_app_server_protocol::ControllerLease as ProtocolControllerLease;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

const LEASE_DURATION: Duration = Duration::from_millis(5_000);
const AUTHORIZATION_DURATION: Duration = Duration::from_millis(60_000);

#[derive(Clone)]
struct ManualClock {
    now: Arc<Mutex<Instant>>,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(Instant::now())),
        }
    }

    fn controller_clock(&self) -> ControllerSessionClock {
        let now = Arc::clone(&self.now);
        ControllerSessionClock::from_fn(move || {
            *now.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        })
    }

    fn now(&self) -> Instant {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn advance(&self, duration: Duration) {
        let mut now = self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *now += duration;
    }
}

#[test]
fn ownership_lifecycle_preserves_standing_authorization() {
    let clock = ManualClock::new();
    let main_thread_id = thread_id(1);
    let controller = connection_id(10);
    let observer = connection_id(11);
    let mut coordinator = new_coordinator(main_thread_id, &clock);

    let owner_session = coordinator
        .request_participation(controller, grant(&clock, main_thread_id, /*epoch*/ 3))
        .expect("first controller should get the initial lease");
    let initial_lease = active_lease(&owner_session);
    assert_controller_owned(
        &coordinator,
        controller,
        &initial_lease,
        /*owner_epoch*/ 1,
    );
    assert_session_rights(
        &owner_session,
        Some(/*owner_epoch*/ 1),
        /*can_acquire*/ true,
        /*can_mutate*/ true,
    );
    assert_eq!(owner_session.lease_expires_in_ms, Some(ms(LEASE_DURATION)));
    assert_eq!(
        owner_session.authorization_expires_in_ms,
        Some(ms(AUTHORIZATION_DURATION))
    );
    let already_acquired = coordinator
        .acquire_control(controller)
        .expect("active owner acquire should return the current session");
    assert_eq!(already_acquired, owner_session);

    let observer_session = coordinator
        .request_participation(observer, grant(&clock, main_thread_id, /*epoch*/ 4))
        .expect("second controller should get standing read access only");
    assert_controller_owned(
        &coordinator,
        controller,
        &initial_lease,
        /*owner_epoch*/ 1,
    );
    assert_session_rights(
        &observer_session,
        None,
        /*can_acquire*/ false,
        /*can_mutate*/ false,
    );

    let released = coordinator
        .release_control(controller)
        .expect("release should preserve standing session");
    let idempotent_release = coordinator
        .release_control(controller)
        .expect("release without an active lease should be idempotent");
    assert_eq!(released, idempotent_release);
    assert_eq!(
        coordinator.interactive_owner(),
        &InteractiveOwner::TuiOwned { owner_epoch: 2 }
    );
    assert_session_rights(
        &released, None, /*can_acquire*/ true, /*can_mutate*/ false,
    );

    let reacquired = coordinator
        .acquire_control(controller)
        .expect("standing session should reacquire without TUI input");
    let reacquired_lease = active_lease(&reacquired);
    assert_eq!(owner_session.session_id, reacquired.session_id);
    assert_controller_owned(
        &coordinator,
        controller,
        &reacquired_lease,
        /*owner_epoch*/ 3,
    );

    coordinator
        .reclaim_for_tui()
        .expect("thread-affecting TUI input should reclaim ownership");
    let reclaimed = coordinator
        .protocol_session(controller)
        .expect("reclaim should preserve standing session");
    assert_eq!(
        coordinator.interactive_owner(),
        &InteractiveOwner::TuiOwned { owner_epoch: 4 }
    );
    assert_session_rights(
        &reclaimed, None, /*can_acquire*/ true, /*can_mutate*/ false,
    );
}

#[test]
fn active_owner_authority_rejects_controllers_without_control() {
    let clock = ManualClock::new();
    let main_thread_id = thread_id(1);
    let controller = connection_id(10);
    let observer = connection_id(11);
    let mut coordinator = new_coordinator(main_thread_id, &clock);

    coordinator
        .request_participation(controller, grant(&clock, main_thread_id, /*epoch*/ 3))
        .expect("first controller should get the initial lease");
    coordinator
        .request_participation(observer, grant(&clock, main_thread_id, /*epoch*/ 4))
        .expect("second controller should get standing read access only");

    assert_eq!(
        coordinator.require_active_owner(controller),
        Ok(main_thread_id)
    );
    assert_eq!(
        coordinator.require_active_owner(observer),
        Err(ControllerSessionError::OwnershipConflict)
    );

    coordinator
        .release_control(controller)
        .expect("release should preserve standing authorization without control");
    assert_eq!(
        coordinator.require_active_owner(controller),
        Err(ControllerSessionError::StaleOwnership)
    );
}

#[test]
fn transfer_pending_and_deadlines_are_deterministic() {
    let clock = ManualClock::new();
    let main_thread_id = thread_id(1);
    let controller = connection_id(10);
    let mut coordinator = new_coordinator(main_thread_id, &clock);

    coordinator
        .request_participation(controller, grant(&clock, main_thread_id, /*epoch*/ 3))
        .expect("participation should grant initial lease");
    coordinator
        .release_control(controller)
        .expect("release should return ownership to TUI");
    let transfer = coordinator
        .begin_transfer_to_controller(controller)
        .expect("standing session should begin transfer");
    assert_eq!(
        coordinator.interactive_owner(),
        &InteractiveOwner::TransferPending { owner_epoch: 3 }
    );
    assert_eq!(
        coordinator.acquire_control(controller),
        Err(ControllerSessionError::TransferPending)
    );
    let committed = coordinator
        .complete_transfer_to_controller(transfer)
        .expect("pending transfer should commit");
    assert_controller_owned(
        &coordinator,
        controller,
        &active_lease(&committed),
        /*owner_epoch*/ 3,
    );

    let reclaim_clock = ManualClock::new();
    let mut reclaim_coordinator = new_coordinator(main_thread_id, &reclaim_clock);
    reclaim_coordinator
        .request_participation(
            controller,
            grant(&reclaim_clock, main_thread_id, /*epoch*/ 4),
        )
        .expect("participation should grant initial lease");
    reclaim_coordinator
        .release_control(controller)
        .expect("release should return ownership to TUI");
    let stale_transfer = reclaim_coordinator
        .begin_transfer_to_controller(controller)
        .expect("standing session should begin transfer");
    reclaim_coordinator
        .reclaim_for_tui()
        .expect("TUI input should reclaim a pending controller transfer");
    assert_eq!(
        reclaim_coordinator.interactive_owner(),
        &InteractiveOwner::TuiOwned { owner_epoch: 4 }
    );
    assert_eq!(
        reclaim_coordinator.complete_transfer_to_controller(stale_transfer),
        Err(ControllerSessionError::StaleOwnership)
    );

    let lease_clock = ManualClock::new();
    let mut lease_coordinator = new_coordinator(main_thread_id, &lease_clock);
    lease_coordinator
        .request_participation(controller, grant(&lease_clock, main_thread_id, /*epoch*/ 5))
        .expect("participation should grant initial lease");
    lease_clock.advance(LEASE_DURATION + Duration::from_millis(1));
    lease_coordinator.expire_deadlines();
    let after_lease_expiry = lease_coordinator
        .protocol_session(controller)
        .expect("lease expiry should keep standing authorization");
    assert_eq!(
        lease_coordinator.interactive_owner(),
        &InteractiveOwner::TuiOwned { owner_epoch: 2 }
    );
    assert_session_rights(
        &after_lease_expiry,
        None,
        /*can_acquire*/ true,
        /*can_mutate*/ false,
    );
    assert_eq!(
        after_lease_expiry.authorization_expires_in_ms,
        Some(ms(AUTHORIZATION_DURATION
            - LEASE_DURATION
            - Duration::from_millis(1)))
    );

    let auth_clock = ManualClock::new();
    let mut auth_coordinator = new_coordinator(main_thread_id, &auth_clock);
    auth_coordinator
        .request_participation(
            controller,
            grant_with_duration(
                &auth_clock,
                main_thread_id,
                /*epoch*/ 6,
                Duration::from_millis(1_000),
            ),
        )
        .expect("participation should grant initial lease");
    auth_clock.advance(Duration::from_millis(1_001));
    auth_coordinator.expire_deadlines();
    assert_eq!(
        auth_coordinator.interactive_owner(),
        &InteractiveOwner::TuiOwned { owner_epoch: 2 }
    );
    assert_eq!(auth_coordinator.protocol_session(controller), None);
}

#[test]
fn notifications_track_authorization_and_ownership_transitions() {
    let clock = ManualClock::new();
    let main_thread_id = thread_id(1);
    let controller = connection_id(10);
    let mut coordinator = new_coordinator(main_thread_id, &clock);

    let approved = coordinator
        .request_participation(controller, grant(&clock, main_thread_id, /*epoch*/ 3))
        .expect("participation should grant initial lease");
    let notifications = coordinator.drain_notifications();
    assert_eq!(notifications.len(), 2);
    assert_authorization_notification(
        &notifications[0],
        controller,
        ControllerAuthorizationChangedReason::Approved,
        Some(/*active_lease*/ false),
    );
    assert_control_notification(
        &notifications[1],
        controller,
        ControllerControlOwnershipChangedReason::InitialLeaseGranted,
        Some(approved.active_lease.expect("initial lease")),
    );

    coordinator
        .release_control(controller)
        .expect("release should preserve standing session");
    let notifications = coordinator.drain_notifications();
    assert_eq!(notifications.len(), 1);
    assert_control_notification(
        &notifications[0],
        controller,
        ControllerControlOwnershipChangedReason::Released,
        None,
    );

    let reacquired = coordinator
        .acquire_control(controller)
        .expect("standing session should reacquire");
    let notifications = coordinator.drain_notifications();
    assert_eq!(notifications.len(), 1);
    assert_control_notification(
        &notifications[0],
        controller,
        ControllerControlOwnershipChangedReason::Acquired,
        Some(reacquired.active_lease.expect("reacquired lease")),
    );

    coordinator
        .reclaim_for_tui()
        .expect("TUI input should reclaim ownership");
    let notifications = coordinator.drain_notifications();
    assert_eq!(notifications.len(), 1);
    assert_control_notification(
        &notifications[0],
        controller,
        ControllerControlOwnershipChangedReason::ReclaimedByTui,
        None,
    );
}

#[test]
fn notifications_track_deadline_and_terminal_revocation() {
    let clock = ManualClock::new();
    let main_thread_id = thread_id(1);
    let controller = connection_id(10);

    let mut lease_coordinator = new_coordinator(main_thread_id, &clock);
    lease_coordinator
        .request_participation(controller, grant(&clock, main_thread_id, /*epoch*/ 3))
        .expect("participation should grant initial lease");
    lease_coordinator.drain_notifications();
    clock.advance(LEASE_DURATION + Duration::from_millis(1));
    lease_coordinator.expire_deadlines();
    let notifications = lease_coordinator.drain_notifications();
    assert_eq!(notifications.len(), 1);
    assert_control_notification(
        &notifications[0],
        controller,
        ControllerControlOwnershipChangedReason::LeaseExpired,
        None,
    );

    let auth_clock = ManualClock::new();
    let mut auth_coordinator = new_coordinator(main_thread_id, &auth_clock);
    auth_coordinator
        .request_participation(
            controller,
            grant_with_duration(
                &auth_clock,
                main_thread_id,
                /*epoch*/ 4,
                Duration::from_millis(1_000),
            ),
        )
        .expect("participation should grant initial lease");
    auth_coordinator.drain_notifications();
    auth_clock.advance(Duration::from_millis(1_001));
    auth_coordinator.expire_deadlines();
    let notifications = auth_coordinator.drain_notifications();
    assert_eq!(notifications.len(), 2);
    assert_control_notification(
        &notifications[0],
        controller,
        ControllerControlOwnershipChangedReason::AuthorizationRevoked,
        None,
    );
    assert_authorization_notification(
        &notifications[1],
        controller,
        ControllerAuthorizationChangedReason::Expired,
        None,
    );
}

#[test]
fn tui_unavailable_and_closed_are_terminal_states() {
    let clock = ManualClock::new();
    let main_thread_id = thread_id(1);
    let controller = connection_id(10);

    let mut unavailable = new_coordinator(main_thread_id, &clock);
    unavailable
        .request_participation(controller, grant(&clock, main_thread_id, /*epoch*/ 3))
        .expect("participation should grant initial lease");
    unavailable
        .mark_tui_unavailable()
        .expect("TUI unavailable should transition");
    assert_eq!(
        unavailable.interactive_owner(),
        &InteractiveOwner::TuiUnavailable { owner_epoch: 2 }
    );
    assert_eq!(
        unavailable.request_participation(
            connection_id(11),
            grant(&clock, main_thread_id, /*epoch*/ 4),
        ),
        Err(ControllerSessionError::TuiUnavailable)
    );
    assert_eq!(
        unavailable.release_control(controller),
        Err(ControllerSessionError::TuiUnavailable)
    );

    let mut closed = new_coordinator(main_thread_id, &clock);
    closed
        .request_participation(controller, grant(&clock, main_thread_id, /*epoch*/ 3))
        .expect("participation should grant initial lease");
    closed.close_main_thread();
    assert_eq!(closed.interactive_owner(), &InteractiveOwner::Closed);
    assert_eq!(closed.protocol_session(controller), None);
    assert_eq!(
        closed.request_participation(controller, grant(&clock, main_thread_id, /*epoch*/ 4)),
        Err(ControllerSessionError::MainThreadClosed)
    );
}

fn new_coordinator(main_thread_id: ThreadId, clock: &ManualClock) -> ControllerSessionCoordinator {
    ControllerSessionCoordinator::new(
        main_thread_id,
        clock.controller_clock(),
        ControllerSessionConfig {
            lease_duration: LEASE_DURATION,
        },
    )
}

fn grant(
    clock: &ManualClock,
    main_thread_id: ThreadId,
    authorization_epoch: u64,
) -> ControllerEnrollmentGrant {
    grant_with_duration(
        clock,
        main_thread_id,
        authorization_epoch,
        AUTHORIZATION_DURATION,
    )
}

fn grant_with_duration(
    clock: &ManualClock,
    main_thread_id: ThreadId,
    authorization_epoch: u64,
    authorization_duration: Duration,
) -> ControllerEnrollmentGrant {
    ControllerEnrollmentGrant {
        subject_id: format!("controller-subject-{authorization_epoch}"),
        main_thread_id,
        authorization_epoch,
        authorization_expires_at: clock.now() + authorization_duration,
    }
}

fn assert_controller_owned(
    coordinator: &ControllerSessionCoordinator,
    connection_id: ConnectionId,
    lease: &ProtocolControllerLease,
    owner_epoch: u64,
) {
    assert_eq!(
        coordinator.interactive_owner(),
        &InteractiveOwner::ControllerOwned {
            connection_id,
            lease_id: lease.lease_id.clone(),
            owner_epoch,
        }
    );
}

fn assert_session_rights(
    session: &ProtocolControllerSession,
    active_owner_epoch: Option<u64>,
    can_acquire: bool,
    can_mutate: bool,
) {
    assert_eq!(
        session.active_lease.as_ref().map(|lease| lease.owner_epoch),
        active_owner_epoch
    );
    assert!(session.effective_capabilities.read_main_thread);
    assert!(session.effective_capabilities.subscribe_main_thread);
    assert_eq!(session.effective_capabilities.acquire_control, can_acquire);
    assert!(session.effective_capabilities.release_control);
    assert_eq!(
        session.effective_capabilities.mutate_main_thread,
        can_mutate
    );
    assert_eq!(session.effective_capabilities.answer_prompts, can_mutate);
}

fn active_lease(session: &ProtocolControllerSession) -> ProtocolControllerLease {
    session
        .active_lease
        .clone()
        .expect("session should include active lease")
}

fn assert_authorization_notification(
    notification: &ControllerSessionNotification,
    connection_id: ConnectionId,
    reason: ControllerAuthorizationChangedReason,
    expected_session_active_lease: Option<bool>,
) {
    let ControllerSessionNotification::AuthorizationChanged {
        connection_id: actual_connection_id,
        notification,
    } = notification
    else {
        panic!("expected authorization notification");
    };
    assert_eq!(*actual_connection_id, connection_id);
    assert_eq!(notification.reason, reason);
    assert_eq!(notification.main_thread_id, thread_id(1).to_string());
    assert_eq!(
        notification
            .session
            .as_ref()
            .map(|session| session.active_lease.is_some()),
        expected_session_active_lease
    );
}

fn assert_control_notification(
    notification: &ControllerSessionNotification,
    connection_id: ConnectionId,
    reason: ControllerControlOwnershipChangedReason,
    expected_active_lease: Option<ProtocolControllerLease>,
) {
    let ControllerSessionNotification::ControlOwnershipChanged {
        connection_id: actual_connection_id,
        notification,
    } = notification
    else {
        panic!("expected ownership notification");
    };
    assert_eq!(*actual_connection_id, connection_id);
    assert_eq!(notification.reason, reason);
    assert_eq!(notification.main_thread_id, thread_id(1).to_string());
    assert_eq!(notification.active_lease, expected_active_lease);
}

fn connection_id(id: u64) -> ConnectionId {
    ConnectionId(id)
}

fn thread_id(id: u64) -> ThreadId {
    ThreadId::from_string(&format!("00000000-0000-0000-0000-{id:012}"))
        .expect("test thread id should parse")
}

fn ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).expect("test duration should fit in u64")
}
