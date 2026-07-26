//! One complete pass over every group a host holds.

use rafter_app::group::GroupStepReport;

use crate::error::MultiRaftError;

/// One group's outcome within a pass.
///
/// `group_id` is the **host key** that was stepped, not the group ID the
/// driver reported. The two can disagree, and only the host key is
/// authoritative: the reported one is data the driver supplied, which the host
/// checks against the key rather than trusts.
#[derive(Debug)]
pub struct GroupOutcome<G, R> {
    /// The host key this outcome belongs to.
    pub group_id: G,
    /// What the group's step produced.
    pub result: Result<GroupStepReport<G, R>, MultiRaftError<G>>,
}

/// One complete pass over every group a host held when the pass began.
///
/// This is the executable form of the unit of fairness the managed scheduler
/// is required to bound — "every continuously ready group receives a
/// scheduling opportunity within one complete pass over the ready set"
/// (`docs/reference-consumers.md`). A pass carries one outcome per open group,
/// in the host's key order, whatever any individual group did: a failing group
/// consumes its own opportunity and nobody else's.
///
/// It is a fairness *measurement*, not a fairness *mechanism*. Nothing here
/// decides when the next pass runs, bounds the work one group may do inside a
/// pass, or queues anything; see the crate documentation for what a managed
/// scheduler would add.
#[derive(Debug)]
#[must_use = "a tick pass carries the only proof its groups' writes took effect"]
pub struct TickPass<G, R> {
    outcomes: Vec<GroupOutcome<G, R>>,
}

impl<G, R> TickPass<G, R> {
    pub(crate) const fn new(outcomes: Vec<GroupOutcome<G, R>>) -> Self {
        Self { outcomes }
    }

    /// Every outcome, one per group, in the host's key order.
    #[must_use]
    pub fn outcomes(&self) -> &[GroupOutcome<G, R>] {
        &self.outcomes
    }

    /// Takes the outcomes, which is how a caller routes what the pass proved.
    #[must_use]
    pub fn into_outcomes(self) -> Vec<GroupOutcome<G, R>> {
        self.outcomes
    }

    /// The number of groups this pass stepped.
    ///
    /// Equal to the host's open-group count at the moment the pass began, for
    /// every pass, including one in which every group failed. A driver cannot
    /// open or retire a group mid-pass — the host is mutably borrowed for the
    /// pass's duration — so a caller asserting fairness compares this against
    /// [`crate::MultiRaftHost::len`] and gets a deterministic answer.
    #[must_use]
    pub fn visited(&self) -> usize {
        self.outcomes.len()
    }

    /// The reports of the groups that stepped successfully.
    pub fn reports(&self) -> impl Iterator<Item = &GroupStepReport<G, R>> {
        self.outcomes
            .iter()
            .filter_map(|outcome| outcome.result.as_ref().ok())
    }

    /// The groups that failed, each with the host key it failed under.
    ///
    /// Only driver failures and report-validation failures can appear here.
    /// The keys come from the host's own map, so no group is unknown, and a
    /// tick carries no group ID to mismatch, so no input is misrouted.
    pub fn failures(&self) -> impl Iterator<Item = (&G, &MultiRaftError<G>)> {
        self.outcomes.iter().filter_map(|outcome| {
            outcome
                .result
                .as_ref()
                .err()
                .map(|error| (&outcome.group_id, error))
        })
    }

    /// Whether every group in this pass stepped successfully.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.outcomes.iter().all(|outcome| outcome.result.is_ok())
    }
}
