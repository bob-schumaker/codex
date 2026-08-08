//! Optional host-provided controller-enrollment verification boundary.
//!
//! The embedded local-controller path does not require durable enrollment
//! records or client credentials; it uses native TUI approval to create a live,
//! connection-bound grant for one launch and main thread. This verifier remains
//! isolated for host-provided credential flows: display claims are never
//! authority, and any verified record must be bound to the live connection
//! before a controller session grant is created.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Instant;

use codex_protocol::ThreadId;

use crate::controller_session::ControllerEnrollmentGrant;
use crate::controller_session::ControllerSessionClock;
use crate::transport::ConnectionId;

/// Source of durable controller enrollment records.
///
/// Implementations are expected to read Codex-owned user configuration or a
/// platform credential store. Socket metadata, launch nonces, and display
/// strings must not be treated as enrollment records.
pub(crate) trait ControllerEnrollmentSource: Send + Sync {
    fn enrollment_for(&self, subject_id: &str) -> Option<ControllerEnrollmentRecord>;
}

impl<T: ControllerEnrollmentSource + ?Sized> ControllerEnrollmentSource for Arc<T> {
    fn enrollment_for(&self, subject_id: &str) -> Option<ControllerEnrollmentRecord> {
        self.as_ref().enrollment_for(subject_id)
    }
}

#[derive(Default)]
pub(crate) struct EmptyControllerEnrollmentSource;

impl ControllerEnrollmentSource for EmptyControllerEnrollmentSource {
    fn enrollment_for(&self, _subject_id: &str) -> Option<ControllerEnrollmentRecord> {
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControllerEnrollmentPolicy {
    Disabled,
    BestEffort,
    Required,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControllerEnrollmentRecord {
    pub(crate) subject_id: String,
    pub(crate) credential_fingerprint: String,
    pub(crate) main_thread_id: ThreadId,
    pub(crate) authorization_epoch: u64,
    pub(crate) revocation_epoch: u64,
    pub(crate) expires_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControllerParticipationEvidence {
    pub(crate) display_claims: ControllerDisplayClaims,
    pub(crate) credential_proof: Option<ControllerCredentialProof>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControllerDisplayClaims {
    pub(crate) controller_name: String,
    pub(crate) description: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControllerCredentialProof {
    pub(crate) subject_id: String,
    pub(crate) credential_fingerprint: String,
    pub(crate) connection_id: ConnectionId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ControllerEnrollmentError {
    PolicyDisabled,
    RequiredEnrollmentMissing,
    CredentialProofRequired,
    EnrollmentDenied,
    ConnectionMismatch,
    CredentialRotated,
    AuthorizationExpired,
    Revoked,
    DifferentMainThread,
}

pub(crate) struct ControllerEnrollmentVerifier<S> {
    source: S,
    policy: ControllerEnrollmentPolicy,
    clock: ControllerSessionClock,
}

impl<S> ControllerEnrollmentVerifier<S> {
    pub(crate) fn new(
        source: S,
        policy: ControllerEnrollmentPolicy,
        clock: ControllerSessionClock,
    ) -> Self {
        Self {
            source,
            policy,
            clock,
        }
    }
}

impl<S: ControllerEnrollmentSource> ControllerEnrollmentVerifier<S> {
    pub(crate) fn verify(
        &self,
        connection_id: ConnectionId,
        main_thread_id: ThreadId,
        evidence: ControllerParticipationEvidence,
    ) -> Result<ControllerEnrollmentGrant, ControllerEnrollmentError> {
        if self.policy == ControllerEnrollmentPolicy::Disabled {
            return Err(ControllerEnrollmentError::PolicyDisabled);
        }

        let Some(proof) = evidence.credential_proof else {
            return Err(ControllerEnrollmentError::CredentialProofRequired);
        };
        if proof.connection_id != connection_id {
            return Err(ControllerEnrollmentError::ConnectionMismatch);
        }

        let record = self
            .source
            .enrollment_for(&proof.subject_id)
            .ok_or_else(|| self.missing_enrollment_error())?;
        if record.main_thread_id != main_thread_id {
            return Err(ControllerEnrollmentError::DifferentMainThread);
        }
        if record.credential_fingerprint != proof.credential_fingerprint {
            return Err(ControllerEnrollmentError::CredentialRotated);
        }
        if self.clock.now() >= record.expires_at {
            return Err(ControllerEnrollmentError::AuthorizationExpired);
        }
        if record.revocation_epoch >= record.authorization_epoch {
            return Err(ControllerEnrollmentError::Revoked);
        }

        Ok(ControllerEnrollmentGrant {
            subject_id: record.subject_id,
            main_thread_id: record.main_thread_id,
            authorization_epoch: record.authorization_epoch,
            authorization_expires_at: record.expires_at,
        })
    }

    fn missing_enrollment_error(&self) -> ControllerEnrollmentError {
        match self.policy {
            ControllerEnrollmentPolicy::Disabled => ControllerEnrollmentError::PolicyDisabled,
            ControllerEnrollmentPolicy::BestEffort => ControllerEnrollmentError::EnrollmentDenied,
            ControllerEnrollmentPolicy::Required => {
                ControllerEnrollmentError::RequiredEnrollmentMissing
            }
        }
    }
}

#[cfg(test)]
#[path = "controller_enrollment_tests.rs"]
mod tests;
