use rafter_sim::SimSeed;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SoakProfile {
    pub(crate) name: &'static str,
    pub(crate) seeds: &'static [SimSeed],
    pub(crate) steps: usize,
    pub(crate) max_proposals: usize,
    pub(crate) max_restarts: usize,
    pub(crate) max_read_indexes: usize,
    pub(crate) max_membership_changes: usize,
    pub(crate) max_transfers: usize,
    pub(crate) max_partitions: usize,
    pub(crate) max_lossy_restarts: usize,
    /// Tick-skew weight for node 1 (one = no skew).
    pub(crate) tick_skew_weight: u32,
}

impl SoakProfile {
    pub(crate) const fn raft_deep() -> Self {
        Self {
            name: "raft-deep-soak",
            seeds: &[SimSeed(0x9103), SimSeed(0x9104)],
            steps: 160,
            max_proposals: 12,
            max_restarts: 6,
            max_read_indexes: 4,
            max_membership_changes: 4,
            max_transfers: 2,
            max_partitions: 2,
            max_lossy_restarts: 2,
            tick_skew_weight: 3,
        }
    }

    pub(crate) const fn raft_soak() -> Self {
        Self {
            name: "raft-soak",
            seeds: &[
                SimSeed(0x9103),
                SimSeed(0x9104),
                SimSeed(0x9105),
                SimSeed(0x9106),
            ],
            steps: 320,
            max_proposals: 24,
            max_restarts: 12,
            max_read_indexes: 4,
            max_membership_changes: 8,
            max_transfers: 2,
            max_partitions: 2,
            max_lossy_restarts: 2,
            tick_skew_weight: 3,
        }
    }

    pub(crate) const fn raft_nightly() -> Self {
        Self {
            name: "raft-nightly-soak",
            seeds: &[
                SimSeed(0x9103_0001),
                SimSeed(0x9103_0002),
                SimSeed(0x9103_0003),
                SimSeed(0x9103_0004),
                SimSeed(0x9103_0005),
                SimSeed(0x9103_0006),
            ],
            steps: 1024,
            max_proposals: 64,
            max_restarts: 32,
            max_read_indexes: 4,
            max_membership_changes: 16,
            max_transfers: 2,
            max_partitions: 2,
            max_lossy_restarts: 2,
            tick_skew_weight: 3,
        }
    }

    pub(crate) const fn raft_weekly() -> Self {
        Self {
            name: "raft-weekly-soak",
            seeds: &[
                SimSeed(0x9203_0001),
                SimSeed(0x9203_0002),
                SimSeed(0x9203_0003),
                SimSeed(0x9203_0004),
                SimSeed(0x9203_0005),
                SimSeed(0x9203_0006),
                SimSeed(0x9203_0007),
                SimSeed(0x9203_0008),
                SimSeed(0x9203_0009),
                SimSeed(0x9203_000a),
            ],
            steps: 4096,
            max_proposals: 192,
            max_restarts: 96,
            max_read_indexes: 16,
            max_membership_changes: 48,
            max_transfers: 8,
            max_partitions: 8,
            max_lossy_restarts: 8,
            tick_skew_weight: 5,
        }
    }
}
