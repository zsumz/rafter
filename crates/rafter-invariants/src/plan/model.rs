//! Selected profile contracts and their exact source-bound plan receipt.

use std::{error::Error, path::PathBuf};

use crate::{
    contract::{catalog::Catalog, profile::ProfileManifest},
    evidence::{ExecutionPlanReceipt, InvocationReceipt, PLAN_SCHEMA_VERSION},
};

use super::capture::plan_input;

#[derive(Clone, Debug)]
/// Paths and selected profile used to construct one immutable execution plan.
pub struct PlanOptions {
    pub profile: String,
    pub registry: PathBuf,
    pub manifest: PathBuf,
}

#[derive(Clone, Debug)]
/// Parsed contracts plus their exact source-byte receipt.
pub struct ExecutionPlan {
    pub catalog: Catalog,
    pub manifest: ProfileManifest,
    pub receipt: ExecutionPlanReceipt,
}

pub(crate) struct CapturedInvocation {
    pub(crate) receipt: InvocationReceipt,
    pub(crate) program_bytes: Vec<u8>,
}

impl ExecutionPlan {
    /// Loads, validates, and hashes the registry and profile manifest once.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid contracts, untracked or escaping paths,
    /// or an unknown profile.
    pub fn load(options: &PlanOptions) -> Result<Self, Box<dyn Error>> {
        let catalog = Catalog::load(&options.registry)?;
        let manifest = ProfileManifest::load(&options.manifest)?;
        manifest.validate(&catalog)?;
        let contract = manifest
            .profiles
            .get(&options.profile)
            .ok_or_else(|| format!("unknown profile {}", options.profile))?
            .clone();
        let receipt = ExecutionPlanReceipt {
            schema_version: PLAN_SCHEMA_VERSION,
            profile: options.profile.clone(),
            registry: plan_input(&options.registry)?,
            manifest: plan_input(&options.manifest)?,
            result_schema: plan_input(std::path::Path::new(
                "verification/invariant-result-schema.json",
            ))?,
            verdict_schema: plan_input(std::path::Path::new(
                "verification/invariant-verdict-schema.json",
            ))?,
            contract,
        };
        Ok(Self {
            catalog,
            manifest,
            receipt,
        })
    }

    #[must_use]
    /// Returns the selected validated profile contract.
    pub fn contract(&self) -> &crate::contract::profile::ProfileContract {
        &self.receipt.contract
    }
}
