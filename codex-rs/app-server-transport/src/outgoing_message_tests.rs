use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::oneshot;
use tokio::sync::watch;

use super::TrackedWriteCompletion;

#[test]
fn tracked_write_completion_is_permitted_without_write_permit() {
    let (write_complete_tx, _write_complete_rx) = oneshot::channel();
    let write_completion = TrackedWriteCompletion::new(write_complete_tx);

    assert!(write_completion.is_write_permitted());
}

#[test]
fn tracked_write_completion_write_permit_tracks_revocation() {
    let (write_complete_tx, _write_complete_rx) = oneshot::channel();
    let (write_permit_tx, write_permit_rx) = watch::channel(true);
    let write_started = Arc::new(AtomicBool::new(false));
    let write_completion = TrackedWriteCompletion::with_write_permit(
        write_complete_tx,
        write_permit_rx,
        Arc::clone(&write_started),
    );

    assert!(write_completion.is_write_permitted());
    assert!(!write_started.load(Ordering::Acquire));

    write_permit_tx
        .send(false)
        .expect("write permit receiver should remain active");

    assert!(!write_completion.is_write_permitted());
    assert!(!write_completion.begin_write());
    assert!(!write_started.load(Ordering::Acquire));
}

#[test]
fn tracked_write_completion_begin_write_marks_write_started() {
    let (write_complete_tx, _write_complete_rx) = oneshot::channel();
    let (_write_permit_tx, write_permit_rx) = watch::channel(true);
    let write_started = Arc::new(AtomicBool::new(false));
    let write_completion = TrackedWriteCompletion::with_write_permit(
        write_complete_tx,
        write_permit_rx,
        Arc::clone(&write_started),
    );

    assert!(write_completion.begin_write());
    assert!(write_started.load(Ordering::Acquire));

    write_completion.complete();
    assert!(write_started.load(Ordering::Acquire));
}
