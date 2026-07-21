//! Aggregate replay runtime accounting against the absolute profile deadline.

use super::{super::model::ReplayReport, process};

pub(super) fn validate(report: &ReplayReport) -> Result<(), String> {
    let maximum_ms = report
        .contract
        .total_timeout_seconds
        .checked_mul(1_000)
        .ok_or_else(|| "replay total runtime budget overflow".to_owned())?;
    let mut processes = report.compilation.processes.iter().chain(
        report
            .fixtures
            .iter()
            .filter_map(|fixture| fixture.process.as_ref()),
    );
    let total_ms = processes.try_fold(0_u64, |total, process| {
        total
            .checked_add(process::duration_ms(process).unwrap_or(0))
            .ok_or_else(|| "replay observed runtime overflow".to_owned())
    })?;
    if total_ms > maximum_ms {
        return Err(format!(
            "replay observed runtime {total_ms}ms exceeds its {maximum_ms}ms total budget"
        ));
    }
    Ok(())
}
