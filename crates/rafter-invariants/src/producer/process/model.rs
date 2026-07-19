//! Producer process outputs carrying provenance and evidence receipts.

use std::{process::ExitStatus, time::Duration};

use crate::evidence::{format::process::TerminationReceipt, InvocationReceipt};

#[derive(Debug)]
pub(in crate::producer) struct ProcessOutput {
    pub(in crate::producer) invocation: InvocationReceipt,
    pub(in crate::producer) status: ExitStatus,
    pub(in crate::producer) stdout: Vec<u8>,
    pub(in crate::producer) stderr: Vec<u8>,
    pub(in crate::producer) duration: Duration,
    pub(in crate::producer) peak_rss_kib: u64,
    pub(in crate::producer) timed_out: bool,
    pub(in crate::producer) termination: Option<TerminationReceipt>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct IdentityOutput {
    pub stdout: String,
    pub stderr: String,
}
