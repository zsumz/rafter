//! Deterministic retry growth, its cap, and the jitter that separates peers.
//!
//! Retry delay must preserve duration precision while capping growth, jitter
//! must still separate local peers once the cap is reached, and reprobe jitter
//! must be stable and specific to a peer pair — peers left synchronized would
//! reconnect in lockstep and rebuild the storm the backoff exists to damp.

use super::{configuration_reprobe_delay, retry_delay};
use crate::config::MAX_REDIAL_DELAY;
use crate::PeerId;
use std::time::Duration;

#[test]
fn retry_delay_preserves_duration_precision_and_caps_growth() {
    let local = PeerId::new("peer-a").expect("local peer ID");
    let remote = PeerId::new("peer-b").expect("remote peer ID");
    let sub_millisecond = Duration::from_micros(500);

    assert_eq!(
        retry_delay(&local, &remote, 3, sub_millisecond),
        retry_delay(&local, &remote, 3, sub_millisecond)
    );
    let precise = retry_delay(&local, &remote, 0, sub_millisecond);
    assert!(precise >= sub_millisecond / 2);
    assert!(precise < sub_millisecond);
    assert!(retry_delay(&local, &remote, 0, Duration::from_nanos(1)) > Duration::ZERO);
}

#[test]
fn retry_jitter_separates_local_peers_even_at_the_cap() {
    let local_a = PeerId::new("peer-a").expect("local peer A");
    let local_b = PeerId::new("peer-b").expect("local peer B");
    let remote = PeerId::new("failed-peer").expect("remote peer");
    assert_ne!(
        retry_delay(&local_a, &remote, 0, Duration::from_secs(1)),
        retry_delay(&local_b, &remote, 0, Duration::from_secs(1))
    );
    let delay_a = retry_delay(&local_a, &remote, 100, Duration::from_secs(1));
    let delay_b = retry_delay(&local_b, &remote, 100, Duration::from_secs(1));

    assert_ne!(delay_a, delay_b);
    for delay in [delay_a, delay_b] {
        assert!(delay >= MAX_REDIAL_DELAY / 2);
        assert!(delay < MAX_REDIAL_DELAY);
    }
}

#[test]
fn configuration_reprobe_jitter_is_stable_and_pair_specific() {
    let local_a = PeerId::new("peer-a").expect("local peer A");
    let local_b = PeerId::new("peer-b").expect("local peer B");
    let remote = PeerId::new("failed-peer").expect("remote peer");
    let base = Duration::from_secs(300);
    let delay = configuration_reprobe_delay(&local_a, &remote, base);

    assert_eq!(delay, configuration_reprobe_delay(&local_a, &remote, base));
    assert_ne!(delay, configuration_reprobe_delay(&local_b, &remote, base));
    assert!(delay >= base);
    assert!(delay < base + base / 4);
}
