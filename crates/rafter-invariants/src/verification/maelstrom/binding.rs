//! Which claim each verification context can honestly make about a shared input.
//!
//! Two different jobs can re-derive two different things, so the binding class
//! and the context together decide what is checked. This module owns that
//! decision and nothing else; `invocation` owns the exact process invocation
//! and calls in here per input.

use std::path::PathBuf;

use crate::{
    evidence::ArtifactRef,
    verification::{AggregateError, AuthenticatedArtifacts, VerificationContext},
};

/// How a captured shared input is independently re-derived.
///
/// The two classes differ in what a later job can honestly reconstruct, not in
/// how much they are trusted.
pub(super) enum InputBinding {
    /// A version-controlled file. Any checkout of the reviewed commit holds
    /// the same bytes, so byte-equality against the checkout is a real
    /// independent derivation in every context.
    Checkout(PathBuf),
    /// A build output. Only the job that built it has the file; a later job
    /// has the repository but not the artifacts of someone else's `cargo
    /// build`, and cannot reproduce them byte-for-byte either -- the invariant
    /// jobs each set their own `CARGO_HOME` and `CARGO_TARGET_DIR`, whose
    /// paths debug binaries embed.
    BuildOutput(PathBuf),
}

/// Verifies one shared input against the strongest claim its context supports.
///
/// A build output binds by byte-equality where the build happened, which is
/// where that comparison means something, and by retention everywhere else.
/// That is not a weaker acceptance of the same claim; it is a different and
/// much smaller claim, and calling it anything else would overstate what the
/// aggregate knows. The producing job is authoritative for build outputs. The
/// provenance of the published bytes is carried by the source receipt and by
/// that job's own verification, which runs this same function against the real
/// file and still fails closed; bundle authentication is what binds the bytes
/// to their declaration. See `assert_artifact_retained`.
///
/// This binding had been aggregate-unsatisfiable since it was written. It went
/// unnoticed because no aggregate run had ever reached it: the scheduled lanes
/// failed earlier, for other reasons, until this branch made every layer green
/// at once and the aggregate got far enough to read a file that could not
/// exist.
#[cfg(test)]
pub(super) fn verify_input_binding_for_test(
    artifact: &ArtifactRef,
    binding: &InputBinding,
    context: VerificationContext,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    verify_input_binding(artifact, binding, context, authenticated)
}

pub(super) fn verify_input_binding(
    artifact: &ArtifactRef,
    binding: &InputBinding,
    context: VerificationContext,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    match (binding, context) {
        (InputBinding::Checkout(path), _)
        | (InputBinding::BuildOutput(path), VerificationContext::ProducingJob) => {
            super::artifact::verify_matches_file(artifact, path, authenticated)
        }
        (InputBinding::BuildOutput(_), VerificationContext::Aggregate) => {
            assert_artifact_retained(artifact, authenticated)
        }
    }
}

/// Asserts the aggregate is holding the declared bytes for this artifact.
///
/// This is the whole aggregate-context claim for a build output, and it is
/// smaller than it used to look. Bundle authentication already streamed the
/// file, compared its digest and length against the declaration, and refused to
/// retain bytes that did not match -- `bundle::integrity::file::read_declared`
/// is the only path into `AuthenticatedArtifacts`, and it fails closed. Hashing
/// those same bytes again here could not return a different answer; it was a
/// tautology dressed as an independent check, and an expensive one, since the
/// Maelstrom node and proxy binaries run to roughly 21MB each per bundle.
///
/// So the honest statement is: these bytes were authenticated against their
/// declaration when the bundle was authenticated, and the aggregate asserts
/// their presence. Byte-equality against the built binary is a producer-context
/// claim, made by the job that has the file, and it stays there.
///
/// Still fail-closed: an artifact the bundle never retained has no bytes to
/// assert, and `bytes` returns an error rather than a default.
fn assert_artifact_retained(
    artifact: &ArtifactRef,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    authenticated.bytes(artifact).map(|_| ())
}
