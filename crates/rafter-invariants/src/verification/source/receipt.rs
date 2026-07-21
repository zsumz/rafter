//! Stable verifier-owned source and executable receipts for replay artifacts.

use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::provenance::source::CheckoutObservation;

use super::toolchain::{ToolchainIdentity, ToolchainProgram};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::verification) struct AuthenticatedSourceReceipt {
    pub(in crate::verification) commit: String,
    pub(in crate::verification) tree: String,
    pub(in crate::verification) materialization: SourceMaterializationReceipt,
    pub(in crate::verification) cargo_lock_sha256: String,
    pub(in crate::verification) cargo_config_sha256: String,
    pub(in crate::verification) environment_sha256: String,
    pub(in crate::verification) target: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::verification) struct SourceMaterializationReceipt {
    pub(in crate::verification) contract: String,
    pub(in crate::verification) sha256: String,
    pub(in crate::verification) tracked_entries: u64,
    pub(in crate::verification) submodules: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::verification) struct ReplayToolchainReceipt {
    pub(in crate::verification) cargo: ReplayToolchainProgramReceipt,
    pub(in crate::verification) rustc: ReplayToolchainProgramReceipt,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::verification) struct ReplayToolchainProgramReceipt {
    pub(in crate::verification) identity: String,
    pub(in crate::verification) launcher_sha256: String,
    pub(in crate::verification) executable_path: String,
    pub(in crate::verification) executable_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::verification) struct ReplaySourceReceipts {
    pub(in crate::verification) source: AuthenticatedSourceReceipt,
    pub(in crate::verification) source_sha256: String,
    pub(in crate::verification) toolchain: ReplayToolchainReceipt,
    pub(in crate::verification) toolchain_sha256: String,
}

impl ReplaySourceReceipts {
    pub(in crate::verification) fn from_parts(
        source: AuthenticatedSourceReceipt,
        toolchain: ReplayToolchainReceipt,
    ) -> Result<Self, String> {
        let source_sha256 = canonical_sha256(&source, "authenticated source receipt")?;
        let toolchain_sha256 = canonical_sha256(&toolchain, "replay toolchain receipt")?;
        Ok(Self {
            source,
            source_sha256,
            toolchain,
            toolchain_sha256,
        })
    }
}

pub(super) fn replay_receipts(
    checkout: &CheckoutObservation,
    environment_sha256: &str,
    toolchain: &ToolchainIdentity,
) -> Result<ReplaySourceReceipts, String> {
    let source = AuthenticatedSourceReceipt {
        commit: checkout.commit.clone(),
        tree: checkout.tree.clone(),
        materialization: SourceMaterializationReceipt {
            contract: checkout.materialization.contract.clone(),
            sha256: checkout.materialization.sha256.clone(),
            tracked_entries: checkout.materialization.tracked_entries,
            submodules: checkout.materialization.submodules,
        },
        cargo_lock_sha256: checkout.cargo_lock_sha256.clone(),
        cargo_config_sha256: checkout.cargo_config_sha256.clone(),
        environment_sha256: environment_sha256.to_owned(),
        target: checkout.target.clone(),
    };
    let toolchain = ReplayToolchainReceipt {
        cargo: program_receipt(
            &checkout.cargo,
            &checkout.cargo_sha256,
            toolchain.cargo(),
            "cargo",
        )?,
        rustc: program_receipt(
            &checkout.rustc,
            &checkout.rustc_sha256,
            toolchain.rustc(),
            "rustc",
        )?,
    };
    ReplaySourceReceipts::from_parts(source, toolchain)
}

fn program_receipt(
    identity: &str,
    launcher_sha256: &str,
    program: &ToolchainProgram,
    name: &str,
) -> Result<ReplayToolchainProgramReceipt, String> {
    Ok(ReplayToolchainProgramReceipt {
        identity: identity.to_owned(),
        launcher_sha256: launcher_sha256.to_owned(),
        executable_path: utf8_path(program.path(), name)?.to_owned(),
        executable_sha256: program.sha256().to_owned(),
    })
}

fn utf8_path<'a>(path: &'a Path, name: &str) -> Result<&'a str, String> {
    path.to_str()
        .ok_or_else(|| format!("actual {name} executable path is not UTF-8"))
}

pub(in crate::verification) fn canonical_sha256(
    value: &impl Serialize,
    label: &str,
) -> Result<String, String> {
    let bytes = serde_json::to_vec(value).map_err(|error| format!("encode {label}: {error}"))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
