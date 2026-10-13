//! Explicit idempotent retries, retaining every unknown attempt in the history.

use crate::{process::SubmitOutcome, production_process::ProductionCluster};
use rafter_reference_fenced_lock::Command;
use rafter_reference_harness::process::Wait;
use std::time::Duration;

pub const HISTORY_LIMIT: usize = 32;
const MAX_UNKNOWN_OUTCOMES: usize = HISTORY_LIMIT - 24;

#[derive(Default)]
pub struct UpgradeClient {
    unknown_outcomes: usize,
    lose_next_reply: bool,
}

impl UpgradeClient {
    pub fn lose_next_reply(&mut self) {
        assert!(!self.lose_next_reply);
        self.lose_next_reply = true;
    }

    pub const fn unknown_outcomes(&self) -> usize {
        self.unknown_outcomes
    }

    pub fn submit(&mut self, cluster: &mut ProductionCluster, command: Command) -> SubmitOutcome {
        let wait = Wait::new(Duration::from_secs(30), Duration::from_millis(10));
        wait.until("the identical idempotent command to settle", || {
            // The command, session, sequence, and fingerprint never change
            // between attempts. Only this application's retry contract makes
            // resubmission safe; this is not generic Raft deduplication.
            let outcome = if std::mem::take(&mut self.lose_next_reply) {
                cluster.submit_losing_reply(command)
            } else {
                cluster.submit_to_leader(command)
            };
            if matches!(outcome, SubmitOutcome::Unknown { .. }) {
                self.unknown_outcomes += 1;
                println!(
                    "UPGRADE_RETRY attempt={} command={command:?} outcome={outcome:?}",
                    self.unknown_outcomes
                );
                assert!(
                    self.unknown_outcomes <= MAX_UNKNOWN_OUTCOMES,
                    "unknown outcomes exhausted the bounded history: {outcome:?}"
                );
                None
            } else {
                Some(outcome)
            }
        })
        .unwrap_or_else(|error| panic!("{error}; unknown outcomes={}", self.unknown_outcomes))
    }
}
