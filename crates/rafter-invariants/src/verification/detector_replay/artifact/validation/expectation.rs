//! Trusted replay context captured independently from published report bytes.

use crate::{
    contract::profile::DetectorReplayContract,
    verification::source::{RegistryReceipt, ReplaySourceReceipts},
};

use super::super::model::ReplayReport;

#[derive(Debug)]
pub(in crate::verification) struct ReplayReportExpectation {
    profile: String,
    receipts: ReplaySourceReceipts,
    contract: DetectorReplayContract,
    registry: Option<RegistryReceipt>,
}

impl ReplayReportExpectation {
    pub(in crate::verification) fn new(
        profile: String,
        receipts: ReplaySourceReceipts,
        contract: DetectorReplayContract,
        registry: Option<RegistryReceipt>,
    ) -> Self {
        Self {
            profile,
            receipts,
            contract,
            registry,
        }
    }

    pub(super) fn validate(&self, report: &ReplayReport) -> Result<(), String> {
        if report.profile != self.profile {
            return Err("verifier replay profile differs from trusted expectation".to_owned());
        }
        if report.source_ref != self.receipts.source.commit
            || report.source != self.receipts.source
            || report.source_sha256 != self.receipts.source_sha256
        {
            return Err("verifier replay source differs from trusted expectation".to_owned());
        }
        if report.toolchain != self.receipts.toolchain
            || report.toolchain_sha256 != self.receipts.toolchain_sha256
        {
            return Err("verifier replay toolchain differs from trusted expectation".to_owned());
        }
        if report.contract != self.contract {
            return Err("verifier replay contract differs from trusted expectation".to_owned());
        }
        if report
            .registry
            .as_ref()
            .is_some_and(|registry| Some(registry) != self.registry.as_ref())
        {
            return Err("verifier replay registry differs from trusted expectation".to_owned());
        }
        Ok(())
    }
}
