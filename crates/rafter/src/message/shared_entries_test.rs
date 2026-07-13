//! Allocation-sharing behavior for immutable `AppendEntries` payloads.

use super::{LogEntry, SharedEntries};
use crate::Term;

#[test]
fn empty_shared_entries_have_zero_storage_semantics() {
    let first = SharedEntries::empty();
    let second = SharedEntries::default();
    let from_empty_vec = Vec::<LogEntry>::new().into();

    assert!(first.is_empty());
    assert_eq!(first.as_slice(), &[]);
    assert_eq!(first.len(), 0);
    assert!(first.shares_allocation(&second));
    assert!(second.shares_allocation(&from_empty_vec));
    assert_eq!(first.to_vec(), Vec::<LogEntry>::new());
}

#[test]
fn non_empty_shared_entries_still_share_one_allocation() {
    let entries: SharedEntries = vec![LogEntry::noop(Term(1))].into();
    let clone = entries.clone();
    let empty = SharedEntries::empty();

    assert!(!entries.is_empty());
    assert!(entries.shares_allocation(&clone));
    assert!(!entries.shares_allocation(&empty));
}
