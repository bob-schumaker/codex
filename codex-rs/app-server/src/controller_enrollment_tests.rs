use super::*;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

#[derive(Default)]
struct MemoryEnrollmentSource {
    records: HashMap<String, ControllerEnrollmentRecord>,
}

impl MemoryEnrollmentSource {
    fn with_record(record: ControllerEnrollmentRecord) -> Self {
        Self {
            records: HashMap::from([(record.subject_id.clone(), record)]),
        }
    }
}

impl ControllerEnrollmentSource for MemoryEnrollmentSource {
    fn enrollment_for(&self, subject_id: &str) -> Option<ControllerEnrollmentRecord> {
        self.records.get(subject_id).cloned()
    }
}

#[test]
fn valid_durable_record_returns_connection_bound_grant() {
    let now = Instant::now();
    let main_thread_id = thread_id(1);
    let connection_id = connection_id(10);
    let record = record(now, main_thread_id);
    let verifier = verifier(
        MemoryEnrollmentSource::with_record(record.clone()),
        ControllerEnrollmentPolicy::BestEffort,
        now,
    );

    let grant = verifier
        .verify(connection_id, main_thread_id, evidence(connection_id))
        .expect("valid durable record should verify");

    assert_eq!(
        grant,
        ControllerEnrollmentGrant {
            subject_id: record.subject_id,
            main_thread_id,
            authorization_epoch: record.authorization_epoch,
            authorization_expires_at: record.expires_at,
        }
    );
}

#[test]
fn display_claims_never_satisfy_authorization() {
    let now = Instant::now();
    let main_thread_id = thread_id(1);
    let verifier = verifier(
        MemoryEnrollmentSource::default(),
        ControllerEnrollmentPolicy::BestEffort,
        now,
    );

    let error = verifier
        .verify(
            connection_id(10),
            main_thread_id,
            ControllerParticipationEvidence {
                display_claims: display_claims(),
                credential_proof: None,
            },
        )
        .expect_err("display claims without credential proof should not authorize");

    assert_eq!(error, ControllerEnrollmentError::CredentialProofRequired);
}

#[test]
fn policy_disabled_and_required_missing_records_are_explicit() {
    let now = Instant::now();
    let main_thread_id = thread_id(1);
    let disabled = verifier(
        MemoryEnrollmentSource::with_record(record(now, main_thread_id)),
        ControllerEnrollmentPolicy::Disabled,
        now,
    );
    let required = verifier(
        MemoryEnrollmentSource::default(),
        ControllerEnrollmentPolicy::Required,
        now,
    );

    assert_eq!(
        disabled.verify(
            connection_id(10),
            main_thread_id,
            evidence(connection_id(10))
        ),
        Err(ControllerEnrollmentError::PolicyDisabled)
    );
    assert_eq!(
        required.verify(
            connection_id(10),
            main_thread_id,
            evidence(connection_id(10))
        ),
        Err(ControllerEnrollmentError::RequiredEnrollmentMissing)
    );
}

#[test]
fn expiry_rotation_and_revocation_reject_existing_records() {
    let now = Instant::now();
    let main_thread_id = thread_id(1);
    let connection_id = connection_id(10);

    let mut expired_record = record(now, main_thread_id);
    expired_record.expires_at = now;
    assert_eq!(
        verifier(
            MemoryEnrollmentSource::with_record(expired_record),
            ControllerEnrollmentPolicy::BestEffort,
            now,
        )
        .verify(connection_id, main_thread_id, evidence(connection_id)),
        Err(ControllerEnrollmentError::AuthorizationExpired)
    );

    let rotated = verifier(
        MemoryEnrollmentSource::with_record(record(now, main_thread_id)),
        ControllerEnrollmentPolicy::BestEffort,
        now,
    );
    let mut rotated_evidence = evidence(connection_id);
    rotated_evidence
        .credential_proof
        .as_mut()
        .expect("proof should exist")
        .credential_fingerprint = "new-fingerprint".to_string();
    assert_eq!(
        rotated.verify(connection_id, main_thread_id, rotated_evidence),
        Err(ControllerEnrollmentError::CredentialRotated)
    );

    let mut revoked_record = record(now, main_thread_id);
    revoked_record.revocation_epoch = revoked_record.authorization_epoch;
    assert_eq!(
        verifier(
            MemoryEnrollmentSource::with_record(revoked_record),
            ControllerEnrollmentPolicy::BestEffort,
            now,
        )
        .verify(connection_id, main_thread_id, evidence(connection_id)),
        Err(ControllerEnrollmentError::Revoked)
    );
}

#[test]
fn proof_must_bind_to_live_connection_and_main_thread() {
    let now = Instant::now();
    let main_thread_id = thread_id(1);
    let verifier = verifier(
        MemoryEnrollmentSource::with_record(record(now, main_thread_id)),
        ControllerEnrollmentPolicy::BestEffort,
        now,
    );

    assert_eq!(
        verifier.verify(
            connection_id(10),
            main_thread_id,
            evidence(connection_id(11))
        ),
        Err(ControllerEnrollmentError::ConnectionMismatch)
    );
    assert_eq!(
        verifier.verify(connection_id(10), thread_id(2), evidence(connection_id(10))),
        Err(ControllerEnrollmentError::DifferentMainThread)
    );
}

fn verifier(
    source: MemoryEnrollmentSource,
    policy: ControllerEnrollmentPolicy,
    now: Instant,
) -> ControllerEnrollmentVerifier<MemoryEnrollmentSource> {
    ControllerEnrollmentVerifier::new(source, policy, ControllerSessionClock::from_fn(move || now))
}

fn record(now: Instant, main_thread_id: ThreadId) -> ControllerEnrollmentRecord {
    ControllerEnrollmentRecord {
        subject_id: "controller-subject".to_string(),
        credential_fingerprint: "credential-fingerprint".to_string(),
        main_thread_id,
        authorization_epoch: 7,
        revocation_epoch: 6,
        expires_at: now + Duration::from_secs(60),
    }
}

fn evidence(connection_id: ConnectionId) -> ControllerParticipationEvidence {
    ControllerParticipationEvidence {
        display_claims: display_claims(),
        credential_proof: Some(ControllerCredentialProof {
            subject_id: "controller-subject".to_string(),
            credential_fingerprint: "credential-fingerprint".to_string(),
            connection_id,
        }),
    }
}

fn display_claims() -> ControllerDisplayClaims {
    ControllerDisplayClaims {
        controller_name: "codex-waveshare".to_string(),
        description: "external input device".to_string(),
    }
}

fn connection_id(id: u64) -> ConnectionId {
    ConnectionId(id)
}

fn thread_id(id: u64) -> ThreadId {
    ThreadId::from_string(&format!("00000000-0000-0000-0000-{id:012}"))
        .expect("test thread id should parse")
}
