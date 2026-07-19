//! Serialized and decoded process observations without acceptance policy.

use serde::{Deserialize, Serialize};

use crate::evidence::InvocationReceipt;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TerminationReceipt {
    pub process_group: bool,
    pub term_signal_sent: bool,
    pub grace_ms: u64,
    pub kill_signal_sent: bool,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessLog {
    pub schema_version: u32,
    pub label: String,
    pub invocation: InvocationReceipt,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination: Option<TerminationReceipt>,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProcessMetrics {
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LabeledProcess {
    pub schema_version: u32,
    pub label: String,
    pub invocation: InvocationReceipt,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub metrics: ProcessMetrics,
    pub stdout: String,
    pub stderr: String,
    pub detector_challenge: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProcessObservation<'a> {
    pub invocation: &'a InvocationReceipt,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub termination: Option<&'a TerminationReceipt>,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub stdout: &'a [u8],
    pub stderr: &'a [u8],
}
