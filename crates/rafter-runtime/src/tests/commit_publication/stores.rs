//! Store images captured after each durable mutation, before the next one.
//!
//! Reopening a cloned image models a crash at that boundary: later writes and
//! the original kernel cannot contribute state to the recovered runtime.

use super::*;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub(super) struct Image {
    pub hard_state: InMemoryRaftHardStateStore,
    pub log: InMemoryRaftLogSegment,
}

#[derive(Clone)]
pub(super) struct Journal {
    state: Rc<RefCell<State>>,
}

struct State {
    current: Image,
    images: Vec<Image>,
}

impl Journal {
    pub(super) fn new(image: Image) -> Self {
        Self {
            state: Rc::new(RefCell::new(State {
                current: image,
                images: Vec::new(),
            })),
        }
    }

    pub(super) fn images(&self) -> Vec<Image> {
        self.state.borrow().images.clone()
    }

    fn record(&self) {
        let mut state = self.state.borrow_mut();
        let image = state.current.clone();
        state.images.push(image);
    }
}

pub(super) struct HardState(pub Journal);

impl RaftHardStateStore for HardState {
    fn current(&self) -> RaftHardState {
        self.0.state.borrow().current.hard_state.current()
    }

    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        self.0
            .state
            .borrow_mut()
            .current
            .hard_state
            .write_hard_state(state)?;
        self.0.record();
        Ok(())
    }
}

pub(super) struct Log(pub Journal);

impl RaftLogSegment for Log {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.0
            .state
            .borrow_mut()
            .current
            .log
            .append_entries(entries)?;
        self.0.record();
        Ok(())
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.0
            .state
            .borrow_mut()
            .current
            .log
            .truncate_suffix(from)?;
        self.0.record();
        Ok(())
    }

    fn compact_prefix_through(
        &mut self,
        through: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        self.0
            .state
            .borrow_mut()
            .current
            .log
            .compact_prefix_through(through)?;
        self.0.record();
        Ok(())
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.0.state.borrow().current.log.replay_entries()
    }

    fn next_index(&self) -> LogIndex {
        self.0.state.borrow().current.log.next_index()
    }

    fn compacted_through(&self) -> LogIndex {
        self.0.state.borrow().current.log.compacted_through()
    }
}
