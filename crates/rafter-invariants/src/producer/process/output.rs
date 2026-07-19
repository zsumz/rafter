//! Producer evidence adaptation for neutral execution results.

use crate::evidence::{format::process::TerminationReceipt, InvocationReceipt};
use crate::execution::process::ProcessOutput as ExecutionOutput;

use super::{duration_ms, ProcessOutput};

pub(super) fn bind_process_output(
    invocation: InvocationReceipt,
    output: ExecutionOutput,
) -> ProcessOutput {
    ProcessOutput {
        invocation,
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
        duration: output.duration,
        peak_rss_kib: output.peak_rss_kib,
        timed_out: output.timed_out,
        termination: Some(TerminationReceipt {
            process_group: output.termination.process_group,
            term_signal_sent: output.termination.term_signal_sent,
            grace_ms: duration_ms(output.termination.grace),
            kill_signal_sent: output.termination.kill_signal_sent,
        }),
    }
}
