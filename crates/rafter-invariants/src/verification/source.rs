//! Independent source-receipt authentication over neutral checkout observations.

use std::{error::Error, path::Path, time::Instant};

use crate::{
    evidence::SourceReceipt,
    provenance::source::{capture_checkout_at, observe_checkout_at, CheckoutObservation},
};

mod generated_outputs;
mod permissions;
mod policy;
mod receipt;
mod registry_snapshot;
mod sealed;
mod snapshot;
mod toolchain;

#[cfg(test)]
pub(in crate::verification) use receipt::SourceMaterializationReceipt;
pub(in crate::verification) use receipt::{
    canonical_sha256, AuthenticatedSourceReceipt, ReplaySourceReceipts,
    ReplayToolchainProgramReceipt, ReplayToolchainReceipt,
};
pub(in crate::verification) use registry_snapshot::RegistryReceipt;

#[cfg(test)]
mod tests;

pub(crate) struct SourceVerifier {
    checkout: CheckoutObservation,
    environment_sha256: String,
    snapshot: snapshot::SourceSnapshot,
    registry: Option<registry_snapshot::RegistrySnapshot>,
    toolchain: toolchain::ToolchainIdentity,
}

#[derive(Clone, Copy)]
pub(crate) struct RegistryMaterializationPolicy {
    pub(crate) required_packages: usize,
    pub(crate) maximum_archive_bytes: u64,
    pub(crate) maximum_expanded_bytes: u64,
    pub(crate) maximum_entries: u64,
    pub(crate) deadline: Instant,
}

impl SourceVerifier {
    pub(crate) fn capture(root: &Path) -> Result<Self, Box<dyn Error>> {
        let captured = capture_checkout_at(root, &generated_outputs::VerifierGeneratedOutputs)?;
        let snapshot = snapshot::SourceSnapshot::materialize(captured.files)?;
        let toolchain = toolchain::ToolchainIdentity::capture(root, &captured.observation)?;
        Ok(Self {
            checkout: captured.observation,
            environment_sha256: crate::provenance::source::source_environment_sha256()?,
            snapshot,
            registry: None,
            toolchain,
        })
    }

    pub(crate) fn source_root(&self) -> &Path {
        self.snapshot.root()
    }

    pub(crate) fn prepare_compilation_source(
        &mut self,
        policy: RegistryMaterializationPolicy,
    ) -> Result<AuthenticatedCompilationSource<'_>, Box<dyn Error>> {
        if self.registry.is_none() {
            self.registry = Some(registry_snapshot::RegistrySnapshot::materialize(
                self.snapshot.root(),
                policy,
            )?);
        }
        Ok(AuthenticatedCompilationSource {
            workspace: self.snapshot.root(),
            checkout: &self.checkout,
            environment_sha256: &self.environment_sha256,
            registry: self
                .registry
                .as_ref()
                .ok_or("authenticated registry source was not retained")?,
            toolchain: &self.toolchain,
        })
    }

    pub(in crate::verification) fn replay_receipts(&self) -> Result<ReplaySourceReceipts, String> {
        receipt::replay_receipts(&self.checkout, &self.environment_sha256, &self.toolchain)
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
        if let Some(registry) = &self.registry {
            registry.revalidate().map_err(|error| {
                SourceAuthenticationError::Stale(format!(
                    "authenticated registry source changed while evidence was verified: {error}"
                ))
            })?;
        }
        self.snapshot.revalidate().map_err(|error| {
            SourceAuthenticationError::Stale(format!(
                "authenticated source snapshot changed while evidence was verified: {error}"
            ))
        })?;
        self.toolchain.revalidate(root).map_err(|error| {
            SourceAuthenticationError::Stale(format!(
                "active Rust toolchain changed while evidence was verified: {error}"
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

pub(crate) struct AuthenticatedCompilationSource<'a> {
    workspace: &'a Path,
    checkout: &'a CheckoutObservation,
    environment_sha256: &'a str,
    registry: &'a registry_snapshot::RegistrySnapshot,
    toolchain: &'a toolchain::ToolchainIdentity,
}

impl AuthenticatedCompilationSource<'_> {
    pub(crate) fn workspace(&self) -> &Path {
        self.workspace
    }

    pub(crate) fn vendor_root(&self) -> std::path::PathBuf {
        self.registry.vendor_root()
    }

    pub(crate) fn bind_vendor_for_child(
        &self,
    ) -> Result<crate::execution::filesystem::ChildDirectory, Box<dyn Error>> {
        self.registry.bind_vendor_for_child()
    }

    pub(in crate::verification) fn registry_receipt(&self) -> RegistryReceipt {
        self.registry.receipt().clone()
    }

    pub(in crate::verification) fn replay_receipts(&self) -> Result<ReplaySourceReceipts, String> {
        receipt::replay_receipts(self.checkout, self.environment_sha256, self.toolchain)
    }

    pub(crate) fn registry_package_count(&self) -> usize {
        self.registry.receipt().package_count
    }

    pub(crate) fn revalidate(&self) -> Result<(), String> {
        self.toolchain.revalidate(self.workspace)?;
        self.registry.revalidate()?;
        snapshot::revalidate_at(self.workspace)
    }

    pub(crate) fn cargo_program(&self) -> &Path {
        self.toolchain.cargo().path()
    }

    pub(crate) fn cargo_sha256(&self) -> &str {
        self.toolchain.cargo().sha256()
    }

    pub(crate) fn rustc_program(&self) -> &Path {
        self.toolchain.rustc().path()
    }

    pub(crate) fn rustc_sha256(&self) -> &str {
        self.toolchain.rustc().sha256()
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
