//! Requested protocol extensions and the behavior they make effective.
//!
//! Requests remain part of configuration value semantics even when timing or
//! dependent features temporarily make the corresponding behavior ineffective.

use super::MIN_CHECK_QUORUM_ELECTION_TIMEOUT_TICKS;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct RequestedFeatures {
    pre_vote: bool,
    check_quorum: bool,
    lease_reads: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct EffectiveFeatures {
    pre_vote: bool,
    check_quorum: bool,
    lease_reads: bool,
}

impl Default for RequestedFeatures {
    fn default() -> Self {
        Self {
            pre_vote: true,
            check_quorum: true,
            lease_reads: false,
        }
    }
}

impl RequestedFeatures {
    pub(super) fn request_pre_vote(&mut self, enabled: bool) {
        self.pre_vote = enabled;
    }

    pub(super) fn request_check_quorum(&mut self, enabled: bool) {
        self.check_quorum = enabled;
    }

    pub(super) fn request_lease_reads(&mut self, enabled: bool) {
        self.lease_reads = enabled;
    }

    pub(super) const fn effective(self, election_timeout_ticks: u64) -> EffectiveFeatures {
        let pre_vote = self.pre_vote;
        let check_quorum =
            self.check_quorum && election_timeout_ticks >= MIN_CHECK_QUORUM_ELECTION_TIMEOUT_TICKS;
        let lease_reads = self.lease_reads && pre_vote && check_quorum;

        EffectiveFeatures {
            pre_vote,
            check_quorum,
            lease_reads,
        }
    }
}

impl EffectiveFeatures {
    pub(super) const fn pre_vote(self) -> bool {
        self.pre_vote
    }

    pub(super) const fn check_quorum(self) -> bool {
        self.check_quorum
    }

    pub(super) const fn lease_reads(self) -> bool {
        self.lease_reads
    }
}
