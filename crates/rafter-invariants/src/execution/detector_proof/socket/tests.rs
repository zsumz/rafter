//! Deterministic policy tests for managed detector challenge sockets.

use super::*;

#[test]
fn stale_socket_threshold_is_inclusive() {
    let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

    assert!(!stale_at(
        modified,
        modified + STALE_AFTER - Duration::from_secs(1)
    ));
    assert!(stale_at(modified, modified + STALE_AFTER));
    assert!(stale_at(
        modified,
        modified + STALE_AFTER + Duration::from_secs(1)
    ));
}

#[test]
fn future_timestamp_is_not_stale() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

    assert!(!stale_at(now + Duration::from_secs(1), now));
}
