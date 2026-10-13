//! Downstream compiler and behavior contracts for handwritten collection traits.

use std::{
    collections::{BTreeSet, HashSet},
    ops::Deref,
    sync::Arc,
};

use rafter::{LogEntry, SharedEntries, SharedPayload, Term};

#[test]
fn shared_entries_preserve_generic_construction_iteration_and_reset() {
    let expected = vec![
        LogEntry::noop(Term(2)),
        LogEntry::application(Term(3), vec![7]),
    ];
    let mut entries: SharedEntries = expected.clone().into_iter().collect();
    let from_vec = <SharedEntries as From<Vec<LogEntry>>>::from(expected.clone());

    assert_eq!(AsRef::<[LogEntry]>::as_ref(&entries), expected);
    assert_eq!(Deref::deref(&entries), expected);
    let borrowed = <&SharedEntries as IntoIterator>::into_iter(&entries).collect::<Vec<_>>();
    assert_eq!(borrowed, expected.iter().collect::<Vec<_>>());
    assert_eq!(entries, from_vec);
    assert!(entries.shares_allocation(&entries.clone()));

    let retained = std::mem::take(&mut entries);
    assert!(entries.is_empty(), "generic take must leave an empty batch");
    assert_eq!(retained.as_slice(), expected);
}

#[test]
fn payload_sets_use_visible_content_independently_of_backing_storage() {
    let backing: Arc<[u8]> = b"prefix:cat:suffix".as_slice().into();
    let view = SharedPayload::from_shared_range(backing, 7..10).unwrap();
    let owned = <SharedPayload as From<Vec<u8>>>::from(b"cat".to_vec());
    let borrowed = <SharedPayload as From<&[u8]>>::from(b"cat".as_slice());
    assert!(!view.shares_allocation(&owned));
    assert!(view.shares_allocation(&view.clone()));
    assert_eq!(AsRef::<[u8]>::as_ref(&view), b"cat");
    assert_eq!(Deref::deref(&view), b"cat");

    let set = HashSet::from([view.clone(), owned.clone(), borrowed.clone()]);
    assert_eq!(set.len(), 1, "equal payload views must have equal hashes");
    let ordered = BTreeSet::from([
        view,
        owned,
        borrowed,
        SharedPayload::from(b"ant".as_slice()),
        SharedPayload::from(b"dog".as_slice()),
    ]);
    assert_eq!(
        ordered
            .iter()
            .map(SharedPayload::as_slice)
            .collect::<Vec<_>>(),
        vec![b"ant".as_slice(), b"cat".as_slice(), b"dog".as_slice()]
    );
}

#[test]
fn payload_comparisons_accept_each_published_byte_container() {
    let payload = SharedPayload::from(b"cat".as_slice());
    assert!(PartialEq::<[u8]>::eq(&payload, b"cat".as_slice()));
    assert!(!PartialEq::<[u8]>::eq(&payload, b"dog".as_slice()));
    assert!(PartialEq::<Vec<u8>>::eq(&payload, &b"cat".to_vec()));
    assert!(!PartialEq::<Vec<u8>>::eq(&payload, &b"dog".to_vec()));
    assert!(PartialEq::<[u8; 3]>::eq(&payload, b"cat"));
    assert!(!PartialEq::<[u8; 3]>::eq(&payload, b"dog"));
    assert!(PartialEq::<&[u8; 3]>::eq(&payload, &b"cat"));
    assert!(!PartialEq::<&[u8; 3]>::eq(&payload, &b"dog"));
}
