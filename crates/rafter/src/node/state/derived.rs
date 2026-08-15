//! Recomputable indexes derived from canonical protocol state.
//!
//! Derived state never changes Raft semantics. It exists to make frequent
//! queries inexpensive, and each index owns the mutation and validation rules
//! that keep it synchronized with its canonical source.

use crate::{CommittedConfiguration, ConfigurationEntry, LogEntry, LogIndex};

/// All state that can be reconstructed exactly from canonical node state.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct DerivedState {
    pub(in crate::node) configuration: ConfigurationIndex,
}

impl DerivedState {
    pub(in crate::node) fn from_log(log: &[LogEntry]) -> Self {
        Self {
            configuration: ConfigurationIndex::from_log(log),
        }
    }

    /// Checks every index against a fresh rebuild from the canonical log.
    ///
    /// Reached from `Node::validate_derived_state`, which owns the public
    /// contract; this is the derived-state half of it.
    pub(in crate::node) fn validate(&self, log: &[LogEntry]) -> Result<(), String> {
        self.configuration.validate(log)
    }

    #[cfg(test)]
    pub(in crate::node) fn push_configuration_offset_for_test(&mut self, offset: usize) {
        self.configuration.offsets.push(offset);
    }
}

/// Offsets of configuration entries within the retained log.
///
/// This index is the sole owner of the offset representation. Log mutation
/// tells it when entries append, truncate, or are replaced; membership queries
/// ask it for protocol-level entries and indexes rather than inspecting raw
/// offsets directly.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct ConfigurationIndex {
    offsets: Vec<usize>,
}

impl ConfigurationIndex {
    pub(in crate::node) fn from_log(log: &[LogEntry]) -> Self {
        let offsets = log
            .iter()
            .enumerate()
            .filter_map(|(offset, entry)| entry.kind.is_configuration().then_some(offset))
            .collect();
        Self { offsets }
    }

    pub(in crate::node) fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub(in crate::node) fn record_append(&mut self, offset: usize, entry: &LogEntry) {
        if entry.kind.is_configuration() {
            self.offsets.push(offset);
        }
    }

    pub(in crate::node) fn clear(&mut self) {
        self.offsets.clear();
    }

    pub(in crate::node) fn truncate(&mut self, retained_log_len: usize) {
        self.offsets.retain(|offset| *offset < retained_log_len);
    }

    /// Returns the final indexed configuration entry.
    ///
    /// A stale final offset returns `None` rather than falling back to an older
    /// entry. Log mutation keeps the index exact; this defensive behavior makes
    /// derived-state corruption visible instead of silently changing authority.
    pub(in crate::node) fn effective_entry<'a>(
        &self,
        log: &'a [LogEntry],
    ) -> Option<&'a ConfigurationEntry> {
        self.offsets
            .last()
            .and_then(|offset| configuration_entry_at(log, *offset))
    }

    /// Returns the latest indexed configuration at or before `index`.
    ///
    /// Stale offsets are skipped defensively so a read-only historical query
    /// does not propagate a panic.
    pub(in crate::node) fn entry_at_or_before<'a>(
        &self,
        first_log_index: LogIndex,
        log: &'a [LogEntry],
        index: LogIndex,
    ) -> Option<&'a ConfigurationEntry> {
        self.offsets.iter().rev().find_map(|offset| {
            let entry_index = logical_index(first_log_index, *offset);
            if entry_index <= index {
                configuration_entry_at(log, *offset)
            } else {
                None
            }
        })
    }

    /// Returns the nearest indexed configuration at or below `commit_index`.
    ///
    /// Like [`ConfigurationIndex::effective_entry`], a stale selected offset
    /// returns `None` so callers can surface derived-state corruption.
    pub(in crate::node) fn committed_entry<'a>(
        &self,
        first_log_index: LogIndex,
        log: &'a [LogEntry],
        commit_index: LogIndex,
    ) -> Option<&'a ConfigurationEntry> {
        let offset = self
            .offsets
            .iter()
            .rev()
            .find(|offset| logical_index(first_log_index, **offset) <= commit_index)?;
        configuration_entry_at(log, *offset)
    }

    /// Returns the latest indexed committed-configuration identity.
    ///
    /// Historical lookup skips stale offsets defensively, matching
    /// [`ConfigurationIndex::entry_at_or_before`].
    pub(in crate::node) fn committed_state_at(
        &self,
        first_log_index: LogIndex,
        log: &[LogEntry],
        commit_index: LogIndex,
    ) -> Option<CommittedConfiguration> {
        self.offsets.iter().rev().find_map(|offset| {
            let index = logical_index(first_log_index, *offset);
            if index > commit_index {
                return None;
            }
            configuration_entry_at(log, *offset).map(|entry| CommittedConfiguration {
                index,
                config_id: entry.config_id(),
            })
        })
    }

    pub(in crate::node) fn indexes_after(
        &self,
        first_log_index: LogIndex,
        floor: LogIndex,
    ) -> Vec<LogIndex> {
        self.offsets
            .iter()
            .map(|offset| logical_index(first_log_index, *offset))
            .filter(|index| *index > floor)
            .collect()
    }

    pub(in crate::node) fn count_between(
        &self,
        first_log_index: LogIndex,
        lower_exclusive: LogIndex,
        upper_exclusive: LogIndex,
    ) -> usize {
        self.offsets
            .iter()
            .map(|offset| logical_index(first_log_index, *offset))
            .filter(|index| *index > lower_exclusive && *index < upper_exclusive)
            .count()
    }

    fn validate(&self, log: &[LogEntry]) -> Result<(), String> {
        let expected = Self::from_log(log);
        if *self == expected {
            return Ok(());
        }

        Err(format!(
            "configuration_offsets mismatch: stored {:?}, expected {:?}",
            self.offsets, expected.offsets
        ))
    }
}

fn configuration_entry_at(log: &[LogEntry], offset: usize) -> Option<&ConfigurationEntry> {
    log.get(offset).and_then(LogEntry::configuration_entry)
}

fn logical_index(first_log_index: LogIndex, offset: usize) -> LogIndex {
    LogIndex(first_log_index.0 + offset as u64)
}
