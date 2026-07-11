//! Content semantics and allocation sharing for application payload views.

use std::sync::Arc;

use super::SharedPayload;

#[test]
fn shared_payload_range_views_compare_by_content_and_share_frame() {
    let frame: Arc<[u8]> = b"left:right".as_slice().into();
    let left = SharedPayload::from_shared_range(frame.clone(), 0..4).expect("left range is valid");
    let right = SharedPayload::from_shared_range(frame, 5..10).expect("right range is valid");
    let owned_left = SharedPayload::from(b"left".as_slice());

    assert_eq!(left, b"left");
    assert_eq!(right, b"right");
    assert_eq!(left, owned_left);
    assert!(left.shares_allocation(&right));
    assert!(!left.shares_allocation(&owned_left));
}

#[test]
fn shared_payload_rejects_invalid_ranges() {
    let frame: Arc<[u8]> = b"bytes".as_slice().into();
    let reversed = std::ops::Range { start: 2, end: 1 };

    assert!(SharedPayload::from_shared_range(frame.clone(), reversed).is_none());
    assert!(SharedPayload::from_shared_range(frame, 0..6).is_none());
}
