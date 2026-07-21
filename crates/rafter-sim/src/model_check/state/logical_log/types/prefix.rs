//! Persistent, exact logical-prefix witnesses.

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use rafter::{LogEntry, LogIndex};

/// An immutable logical-log prefix backed by a persistent ancestry spine.
///
/// Extending a prefix allocates one node. Cloning and freezing a witness are
/// constant-size operations, while equality and hashing remain exact logical
/// content operations and never use pointer identity as evidence.
#[derive(Clone, Default)]
pub(crate) struct LogPrefixWitness {
    tail: Option<Arc<PrefixNode>>,
    #[cfg(test)]
    allocation_counter: Option<Arc<AtomicUsize>>,
}

struct PrefixNode {
    parent: Option<Arc<Self>>,
    index: LogIndex,
    len: usize,
    entry: LogEntry,
}

impl LogPrefixWitness {
    #[cfg(test)]
    pub(crate) fn from_entries(through: LogIndex, entries: Vec<LogEntry>) -> Option<Self> {
        let expected_len = usize::try_from(through.0).ok()?;
        if entries.len() != expected_len {
            return None;
        }

        let mut prefix = Self::default();
        for (offset, entry) in entries.into_iter().enumerate() {
            let index = u64::try_from(offset).ok()?.checked_add(1).map(LogIndex)?;
            prefix = prefix.extend(index, entry)?;
        }
        (prefix.through() == through).then_some(prefix)
    }

    pub(crate) fn through(&self) -> LogIndex {
        self.tail.as_ref().map_or(LogIndex::ZERO, |node| node.index)
    }

    pub(crate) fn len(&self) -> usize {
        self.tail.as_ref().map_or(0, |node| node.len)
    }

    pub(crate) fn last(&self) -> Option<&LogEntry> {
        self.tail.as_ref().map(|node| &node.entry)
    }

    pub(crate) fn extend(&self, index: LogIndex, entry: LogEntry) -> Option<Self> {
        let len = self.len().checked_add(1)?;
        let expected = u64::try_from(len).ok().map(LogIndex)?;
        if index != expected {
            return None;
        }
        #[cfg(test)]
        if let Some(allocations) = &self.allocation_counter {
            allocations.fetch_add(1, Ordering::Relaxed);
        }
        Some(Self {
            tail: Some(Arc::new(PrefixNode {
                parent: self.tail.clone(),
                index,
                len,
                entry,
            })),
            #[cfg(test)]
            allocation_counter: self.allocation_counter.clone(),
        })
    }

    pub(crate) fn matches_extension(
        &self,
        parent: &Self,
        index: LogIndex,
        entry: &LogEntry,
    ) -> bool {
        let Some(tail) = self.tail.as_ref() else {
            return false;
        };
        if tail.index != index || &tail.entry != entry {
            return false;
        }
        match (&tail.parent, &parent.tail) {
            (None, None) => true,
            (Some(actual), Some(expected)) if Arc::ptr_eq(actual, expected) => true,
            (actual, expected) => {
                Self {
                    tail: actual.clone(),
                    #[cfg(test)]
                    allocation_counter: None,
                } == Self {
                    tail: expected.clone(),
                    #[cfg(test)]
                    allocation_counter: None,
                }
            }
        }
    }

    pub(crate) fn slice_through(&self, index: LogIndex) -> Option<Self> {
        if index > self.through() {
            return None;
        }
        if index == LogIndex::ZERO {
            return Some(Self::default());
        }

        let mut cursor = self.tail.as_ref();
        while let Some(node) = cursor {
            if node.index == index {
                return Some(Self {
                    tail: Some(Arc::clone(node)),
                    #[cfg(test)]
                    allocation_counter: self.allocation_counter.clone(),
                });
            }
            if node.index < index {
                return None;
            }
            cursor = node.parent.as_ref();
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn shares_prefix_storage_with(&self, other: &Self) -> bool {
        let (shorter, longer) = if self.through() <= other.through() {
            (self, other)
        } else {
            (other, self)
        };
        match (&shorter.tail, longer.slice_through(shorter.through())) {
            (None, Some(prefix)) => prefix.tail.is_none(),
            (Some(short), Some(prefix)) => prefix
                .tail
                .as_ref()
                .is_some_and(|long| Arc::ptr_eq(short, long)),
            (None | Some(_), None) => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn tracking_allocations() -> Self {
        Self {
            tail: None,
            allocation_counter: Some(Arc::new(AtomicUsize::new(0))),
        }
    }

    #[cfg(test)]
    pub(crate) fn allocation_count(&self) -> usize {
        self.allocation_counter
            .as_ref()
            .map_or(0, |allocations| allocations.load(Ordering::Relaxed))
    }

    fn nodes_from_tail(&self) -> PrefixNodes<'_> {
        PrefixNodes {
            next: self.tail.as_deref(),
        }
    }
}

impl fmt::Debug for LogPrefixWitness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries = self
            .nodes_from_tail()
            .map(|node| &node.entry)
            .collect::<Vec<_>>();
        entries.reverse();
        formatter
            .debug_struct("LogPrefixWitness")
            .field("through", &self.through())
            .field("entries", &entries)
            .finish()
    }
}

impl PartialEq for LogPrefixWitness {
    fn eq(&self, other: &Self) -> bool {
        if self.through() != other.through() {
            return false;
        }

        let mut left = self.tail.as_ref();
        let mut right = other.tail.as_ref();
        loop {
            match (left, right) {
                (Some(left_node), Some(right_node)) => {
                    if Arc::ptr_eq(left_node, right_node) {
                        return true;
                    }
                    if left_node.index != right_node.index || left_node.entry != right_node.entry {
                        return false;
                    }
                    left = left_node.parent.as_ref();
                    right = right_node.parent.as_ref();
                }
                (None, None) => return true,
                (None, Some(_)) | (Some(_), None) => return false,
            }
        }
    }
}

impl Eq for LogPrefixWitness {}

impl Hash for LogPrefixWitness {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.through().hash(state);
        for node in self.nodes_from_tail() {
            node.index.hash(state);
            node.entry.hash(state);
        }
    }
}

impl Drop for PrefixNode {
    fn drop(&mut self) {
        let mut parent = self.parent.take();
        while let Some(node) = parent {
            let Ok(mut node) = Arc::try_unwrap(node) else {
                break;
            };
            parent = node.parent.take();
        }
    }
}

struct PrefixNodes<'a> {
    next: Option<&'a PrefixNode>,
}

impl<'a> Iterator for PrefixNodes<'a> {
    type Item = &'a PrefixNode;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.next?;
        self.next = node.parent.as_deref();
        Some(node)
    }
}

#[cfg(test)]
mod tests;
