use std::collections::{BTreeMap, BTreeSet};

use rafter_multiraft::managed::{ManagedMetrics, WorkClass, WorkId};

use crate::GroupId;

use super::{DriveReport, DrivenDisposition};

/// Independently supplied expectation for one bounded real-adapter run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptanceExpectation {
    /// Exact ready set, in deterministic group order, at the first pass.
    pub ready: Vec<GroupId>,
    /// Work accepted before the run, independent of the scheduler's reports.
    pub accepted: Vec<ExpectedWork>,
    /// Per-group item quota for the first opportunity.
    pub quotas: BTreeMap<GroupId, usize>,
}

/// One independently recorded managed admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpectedWork {
    /// Stable managed admission identity.
    pub work_id: WorkId,
    /// Group selected at admission.
    pub group_id: GroupId,
    /// Class selected at admission.
    pub class: WorkClass,
}

/// Quantitative result of auditing a real managed run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptanceAuditReport {
    /// Passes armed during the run.
    pub passes: usize,
    /// Distinct group opportunities in the first pass.
    pub opportunities: usize,
    /// Largest missing-opportunity gap in the audited first pass.
    pub widest_gap: usize,
    /// Width of the independently expected ready set.
    pub ready_width: usize,
    /// Accepted work in the scheduler's conservation counters.
    pub admitted: u64,
    /// Successfully serviced work.
    pub serviced: u64,
    /// Explicitly failed work.
    pub failed: u64,
    /// Work still queued at the audit boundary.
    pub queued: usize,
    /// Work held by dispatches at the audit boundary.
    pub in_flight: usize,
}

/// Exact reason a real managed history failed its independent audit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcceptanceViolation {
    /// No pass was armed, so the run proved no opportunity property.
    EmptyRun,
    /// The scheduler's first immutable plan differs from the expected ready set.
    PlanMismatch {
        expected: Vec<GroupId>,
        actual: Vec<GroupId>,
    },
    /// A group received more than one turn in one pass.
    DuplicateOpportunity { group_id: GroupId },
    /// A continuously ready group received no opportunity in the first pass.
    OpportunityGap { missing: Vec<GroupId> },
    /// A turn named a group outside its pass.
    OpportunityOutsidePlan { group_id: GroupId },
    /// A group serviced more items than its consumer-owned quota.
    QuotaExceeded {
        group_id: GroupId,
        quota: usize,
        actual: usize,
    },
    /// Class priority walked backwards inside one turn.
    ClassOutOfOrder {
        group_id: GroupId,
        previous: WorkClass,
        actual: WorkClass,
    },
    /// A dispatch claimed an item absent from the independent admission log.
    UnknownWork { work_id: WorkId },
    /// A work item was dispatched under another group or class.
    WorkRouteChanged {
        work_id: WorkId,
        expected_group: GroupId,
        actual_group: GroupId,
        expected_class: WorkClass,
        actual_class: WorkClass,
    },
    /// One accepted item received two terminal dispositions.
    DuplicateDisposition { work_id: WorkId },
    /// An accepted item disappeared from terminal and pending accounting.
    MissingDisposition { work_id: WorkId },
    /// Scheduler conservation counters do not balance.
    Conservation {
        admitted: u64,
        serviced: u64,
        failed: u64,
        queued: usize,
        in_flight: usize,
    },
    /// A quiescent acceptance run retained worker occupancy.
    WorkerStillOccupied { workers: usize },
}

/// Audits one bounded run without consulting a real group or scheduler state.
///
/// The expectation is built from caller-observed admission receipts. The
/// report and metrics are the scheduler's outputs. Keeping those inputs
/// separate lets red-team tests mutate either side and prove each rule fires.
///
/// # Errors
///
/// Returns the first replayable contract violation.
pub fn audit_acceptance(
    expectation: &AcceptanceExpectation,
    report: &DriveReport,
    metrics: &ManagedMetrics,
) -> Result<AcceptanceAuditReport, AcceptanceViolation> {
    let Some(first_plan) = report.plans.first() else {
        return Err(AcceptanceViolation::EmptyRun);
    };
    if first_plan != &expectation.ready {
        return Err(AcceptanceViolation::PlanMismatch {
            expected: expectation.ready.clone(),
            actual: first_plan.clone(),
        });
    }

    let (opportunities, terminal) = audit_turns(expectation, report, first_plan)?;

    let pending = metrics.queued + metrics.in_flight_work;
    for expected in &expectation.accepted {
        if !terminal.contains(&expected.work_id) && pending == 0 {
            return Err(AcceptanceViolation::MissingDisposition {
                work_id: expected.work_id,
            });
        }
    }
    audit_metrics(metrics)?;
    let missing = expectation
        .ready
        .iter()
        .filter(|group_id| !opportunities.contains(group_id))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(AcceptanceViolation::OpportunityGap { missing });
    }
    Ok(AcceptanceAuditReport {
        passes: report.plans.len(),
        opportunities: opportunities.len(),
        widest_gap: 0,
        ready_width: expectation.ready.len(),
        admitted: metrics.admitted,
        serviced: metrics.serviced,
        failed: metrics.failed,
        queued: metrics.queued,
        in_flight: metrics.in_flight_work,
    })
}

fn audit_turns(
    expectation: &AcceptanceExpectation,
    report: &DriveReport,
    first_plan: &[GroupId],
) -> Result<(BTreeSet<GroupId>, BTreeSet<WorkId>), AcceptanceViolation> {
    let first_pass = report
        .turns
        .first()
        .map(|turn| turn.pass_id)
        .ok_or(AcceptanceViolation::EmptyRun)?;
    let planned = first_plan.iter().copied().collect::<BTreeSet<_>>();
    let accepted = expectation
        .accepted
        .iter()
        .map(|item| (item.work_id, *item))
        .collect::<BTreeMap<_, _>>();
    let mut opportunities = BTreeSet::new();
    let mut terminal = BTreeSet::new();
    for turn in report
        .turns
        .iter()
        .filter(|turn| turn.pass_id == first_pass)
    {
        if !planned.contains(&turn.group_id) {
            return Err(AcceptanceViolation::OpportunityOutsidePlan {
                group_id: turn.group_id,
            });
        }
        if !opportunities.insert(turn.group_id) {
            return Err(AcceptanceViolation::DuplicateOpportunity {
                group_id: turn.group_id,
            });
        }
        let quota = expectation.quotas.get(&turn.group_id).copied().unwrap_or(0);
        if turn.items.len() > quota {
            return Err(AcceptanceViolation::QuotaExceeded {
                group_id: turn.group_id,
                quota,
                actual: turn.items.len(),
            });
        }
        audit_items(turn.group_id, &turn.items, &accepted, &mut terminal)?;
    }
    Ok((opportunities, terminal))
}

fn audit_items(
    group_id: GroupId,
    items: &[super::DrivenItem],
    accepted: &BTreeMap<WorkId, ExpectedWork>,
    terminal: &mut BTreeSet<WorkId>,
) -> Result<(), AcceptanceViolation> {
    let mut previous = None;
    for item in items {
        if let Some(previous_class) = previous {
            if item.class < previous_class {
                return Err(AcceptanceViolation::ClassOutOfOrder {
                    group_id,
                    previous: previous_class,
                    actual: item.class,
                });
            }
        }
        previous = Some(item.class);
        let expected = accepted
            .get(&item.work_id)
            .ok_or(AcceptanceViolation::UnknownWork {
                work_id: item.work_id,
            })?;
        if expected.group_id != group_id || expected.class != item.class {
            return Err(AcceptanceViolation::WorkRouteChanged {
                work_id: item.work_id,
                expected_group: expected.group_id,
                actual_group: group_id,
                expected_class: expected.class,
                actual_class: item.class,
            });
        }
        if !terminal.insert(item.work_id) {
            return Err(AcceptanceViolation::DuplicateDisposition {
                work_id: item.work_id,
            });
        }
        match item.disposition {
            DrivenDisposition::Serviced | DrivenDisposition::Failed { .. } => {}
        }
    }
    Ok(())
}

fn audit_metrics(metrics: &ManagedMetrics) -> Result<(), AcceptanceViolation> {
    let accounted = metrics
        .serviced
        .saturating_add(metrics.failed)
        .saturating_add(u64::try_from(metrics.queued).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(metrics.in_flight_work).unwrap_or(u64::MAX));
    if metrics.admitted != accounted {
        return Err(AcceptanceViolation::Conservation {
            admitted: metrics.admitted,
            serviced: metrics.serviced,
            failed: metrics.failed,
            queued: metrics.queued,
            in_flight: metrics.in_flight_work,
        });
    }
    if metrics.occupied_workers != 0 {
        return Err(AcceptanceViolation::WorkerStillOccupied {
            workers: metrics.occupied_workers,
        });
    }
    Ok(())
}
