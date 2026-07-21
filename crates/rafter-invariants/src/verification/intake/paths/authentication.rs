//! Source and artifact authentication for one decoded result receipt.

use std::path::Path;

use crate::{evidence::ResultBundle, verification::source::SourceAuthenticationError};

use super::super::{IntakeDefect, VerificationRequest};
use super::SourceVerifierState;

pub(super) struct ReceiptAuthenticator<'state, 'request> {
    pub(super) request: VerificationRequest<'request>,
    pub(super) source_verifier: &'state mut SourceVerifierState,
    pub(super) accepted: &'state mut Vec<ResultBundle>,
    pub(super) artifact_guards: &'state mut Vec<crate::verification::AuthenticatedArtifacts>,
    pub(super) defects: &'state mut Vec<IntakeDefect>,
}

impl ReceiptAuthenticator<'_, '_> {
    pub(super) fn authenticate(&mut self, path: &Path, bundle: ResultBundle, trusted_runner: &str) {
        if !authenticate_source(
            self.source_verifier,
            trusted_runner,
            &bundle.execution.source,
            self.request.root,
            path,
            self.defects,
        ) {
            return;
        }
        let SourceVerifierState::Ready(source_verifier) = self.source_verifier else {
            self.defects.push(IntakeDefect::unverifiable(
                "source verifier became unavailable after authentication".to_owned(),
            ));
            return;
        };
        match crate::verification::verify_bundle_artifacts(
            &bundle,
            self.request.root,
            source_verifier.source_root(),
            self.request.catalog,
            &self.request.active_plan.profile,
            trusted_runner,
        ) {
            Ok((diagnostics, artifact_guard)) => {
                self.defects.extend(diagnostics.into_iter().map(|message| {
                    IntakeDefect::unverifiable(format!("verify {}: {message}", path.display()))
                }));
                self.artifact_guards.push(artifact_guard);
                self.accepted.push(bundle);
            }
            Err(error) => self.defects.push(IntakeDefect::unverifiable(format!(
                "verify {}: {error}",
                path.display()
            ))),
        }
    }
}

pub(super) fn revalidate_source(
    state: &SourceVerifierState,
    root: &Path,
    defects: &mut Vec<IntakeDefect>,
) {
    let SourceVerifierState::Ready(verifier) = state else {
        return;
    };
    match verifier.revalidate(root) {
        Ok(()) => {}
        Err(SourceAuthenticationError::Stale(error)) => defects.push(IntakeDefect::stale(format!(
            "revalidate active source after evidence verification: {error}"
        ))),
        Err(error @ SourceAuthenticationError::Unverifiable(_)) => {
            defects.push(IntakeDefect::unverifiable(format!(
                "revalidate active source after evidence verification: {}",
                error.message()
            )));
        }
    }
}

fn authenticate_source(
    state: &mut SourceVerifierState,
    layer: &str,
    source: &crate::evidence::SourceReceipt,
    root: &Path,
    path: &Path,
    defects: &mut Vec<IntakeDefect>,
) -> bool {
    if matches!(state, SourceVerifierState::Pending) {
        match crate::verification::source::SourceVerifier::capture(root) {
            Ok(verifier) => *state = SourceVerifierState::Ready(Box::new(verifier)),
            Err(error) => {
                defects.push(IntakeDefect::unverifiable(format!(
                    "observe active source for {}: {error}",
                    path.display()
                )));
                *state = SourceVerifierState::Failed;
                return false;
            }
        }
    }
    let SourceVerifierState::Ready(verifier) = state else {
        return false;
    };
    match verifier.authenticate(layer, source, root) {
        Ok(()) => true,
        Err(SourceAuthenticationError::Stale(error)) => {
            defects.push(IntakeDefect::stale(format!(
                "verify source identity for {}: {error}",
                path.display()
            )));
            false
        }
        Err(error @ SourceAuthenticationError::Unverifiable(_)) => {
            defects.push(IntakeDefect::unverifiable(format!(
                "verify source identity for {}: {}",
                path.display(),
                error.message()
            )));
            false
        }
    }
}
