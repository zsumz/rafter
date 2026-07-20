//! Independent source-receipt authentication over neutral checkout observations.

use std::{error::Error, path::Path};

use crate::{
    evidence::SourceReceipt,
    provenance::source::{capture_checkout_at, observe_checkout_at, CheckoutObservation},
};

mod generated_outputs;
mod policy;
mod snapshot;

#[cfg(test)]
mod tests;

pub(crate) struct SourceVerifier {
    checkout: CheckoutObservation,
    environment_sha256: String,
    snapshot: snapshot::SourceSnapshot,
}

impl SourceVerifier {
    pub(crate) fn capture(root: &Path) -> Result<Self, Box<dyn Error>> {
        let captured = capture_checkout_at(root, &generated_outputs::VerifierGeneratedOutputs)?;
        Ok(Self {
            checkout: captured.observation,
            environment_sha256: crate::provenance::source::source_environment_sha256()?,
            snapshot: snapshot::SourceSnapshot::materialize(captured.files)?,
        })
    }

    pub(crate) fn source_root(&self) -> &Path {
        self.snapshot.root()
    }

    pub(crate) fn authenticate(
        &self,
        layer: &str,
        expected: &SourceReceipt,
        root: &Path,
    ) -> Result<(), SourceAuthenticationError> {
        verify_checkout_identity(expected, &self.checkout)?;
        policy::verify_layer_contract(layer, expected)
            .map_err(|error| SourceAuthenticationError::Unverifiable(error.to_string()))?;
        policy::verify_runtime_identities(layer, expected, root)?;
        verify_receipt_flags(expected, &self.environment_sha256)
    }

    pub(crate) fn revalidate(&self, root: &Path) -> Result<(), SourceAuthenticationError> {
        self.snapshot.revalidate().map_err(|error| {
            SourceAuthenticationError::Stale(format!(
                "authenticated source snapshot changed while evidence was verified: {error}"
            ))
        })?;
        let checkout = observe_checkout_at(root, &generated_outputs::VerifierGeneratedOutputs)
            .map_err(|error| SourceAuthenticationError::Unverifiable(error.to_string()))?;
        if checkout != self.checkout {
            return Err(SourceAuthenticationError::Stale(
                "active source identity changed while evidence was verified".to_owned(),
            ));
        }
        let environment_sha256 = crate::provenance::source::source_environment_sha256()
            .map_err(|error| SourceAuthenticationError::Unverifiable(error.to_string()))?;
        if environment_sha256 != self.environment_sha256 {
            return Err(SourceAuthenticationError::Stale(
                "verification environment changed while evidence was verified".to_owned(),
            ));
        }
        Ok(())
    }
}

pub(in crate::verification) fn authenticated_snapshot_paths(
    root: &Path,
) -> Result<Option<std::collections::HashSet<std::path::PathBuf>>, String> {
    snapshot::tracked_paths_at(root)
}

pub(in crate::verification) fn revalidate_authenticated_snapshot(
    root: &Path,
) -> Result<(), String> {
    snapshot::revalidate_at(root)
}

fn verify_receipt_flags(
    expected: &SourceReceipt,
    environment_sha256: &str,
) -> Result<(), SourceAuthenticationError> {
    if expected.environment_sha256 != environment_sha256 {
        return Err(SourceAuthenticationError::Stale(
            "source receipt environment does not match the verification runtime".to_owned(),
        ));
    }
    if !expected.clean {
        return Err(SourceAuthenticationError::Unverifiable(
            "source receipt does not attest a clean checkout".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum SourceAuthenticationError {
    Stale(String),
    Unverifiable(String),
}

impl SourceAuthenticationError {
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Stale(message) | Self::Unverifiable(message) => message,
        }
    }
}

#[cfg(test)]
pub(crate) fn verify_layer_contract(
    layer: &str,
    receipt: &SourceReceipt,
) -> Result<(), Box<dyn Error>> {
    policy::verify_layer_contract(layer, receipt)
}

fn verify_checkout_identity(
    expected: &SourceReceipt,
    observed: &CheckoutObservation,
) -> Result<(), SourceAuthenticationError> {
    if observed.commit != expected.commit
        || observed.tree != expected.tree
        || observed.materialization.contract != expected.materialization.contract
        || observed.materialization.sha256 != expected.materialization.sha256
        || observed.materialization.tracked_entries != expected.materialization.tracked_entries
        || observed.materialization.submodules != expected.materialization.submodules
        || observed.cargo_lock_sha256 != expected.cargo_lock_sha256
        || observed.cargo != expected.cargo
        || observed.cargo_sha256 != expected.cargo_sha256
        || observed.cargo_config_sha256 != expected.cargo_config_sha256
        || observed.rustc != expected.rustc
        || observed.rustc_sha256 != expected.rustc_sha256
        || observed.target != expected.target
    {
        return Err(SourceAuthenticationError::Stale(
            "evidence source identity does not match the active checkout".to_owned(),
        ));
    }
    Ok(())
}
