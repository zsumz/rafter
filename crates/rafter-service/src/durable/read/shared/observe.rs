//! Distinguish quorum ownership, pending queries, and uncollected proofs.
use super::{lock, Ordering, Phase, ReadShared};

#[derive(Default)]
pub(in crate::durable) struct ReadCounts {
    pub barriers: usize,
    pub queries: usize,
    pub completed: usize,
    pub reserved: usize,
}
impl<R> ReadShared<R> {
    pub(in crate::durable) fn counts(&self) -> ReadCounts {
        let state = lock(&self.state);
        let mut counts = ReadCounts {
            completed: self.completed_proofs.load(Ordering::Acquire),
            ..ReadCounts::default()
        };
        for entry in state.entries.values() {
            match entry.phase {
                Phase::Barrier => {
                    counts.barriers += 1;
                    counts.queries += 1;
                }
                Phase::Queued | Phase::Ready(Some(_)) => counts.queries += 1,
                Phase::Ready(None) => {}
            }
        }
        counts.reserved = counts.queries + counts.completed;
        counts
    }
}
