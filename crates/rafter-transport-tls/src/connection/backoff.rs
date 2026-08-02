//! Deterministic retry and sparse-reprobe pacing.

use std::time::Duration;

use crate::config::MAX_REDIAL_DELAY;
use crate::PeerId;

pub(super) fn retry_delay(peer: &PeerId, attempt: u32, initial: Duration) -> Duration {
    let multiplier = 1_u32 << attempt.min(12);
    let base = initial.saturating_mul(multiplier).min(MAX_REDIAL_DELAY);
    base.saturating_add(jitter(peer, u64::from(attempt), base))
        .min(MAX_REDIAL_DELAY)
}

pub(super) fn configuration_reprobe_delay(peer: &PeerId, initial: Duration) -> Duration {
    initial.saturating_add(jitter(peer, u64::MAX, initial))
}

fn jitter(peer: &PeerId, salt: u64, base: Duration) -> Duration {
    const JITTER_BUCKETS: u32 = 1_024;
    const JITTER_DENOMINATOR: u32 = JITTER_BUCKETS * 4;

    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ salt;
    for byte in peer.as_str().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let bucket = u32::try_from(hash % u64::from(JITTER_BUCKETS)).unwrap_or_default();
    (base / JITTER_DENOMINATOR).saturating_mul(bucket)
}

#[cfg(test)]
mod tests {
    use super::{configuration_reprobe_delay, retry_delay};
    use crate::config::MAX_REDIAL_DELAY;
    use crate::PeerId;
    use std::time::Duration;

    #[test]
    fn retry_delay_preserves_duration_precision_and_caps_growth() {
        let peer = PeerId::new("peer-a").expect("peer ID");
        let sub_millisecond = Duration::from_micros(500);

        assert_eq!(
            retry_delay(&peer, 3, sub_millisecond),
            retry_delay(&peer, 3, sub_millisecond)
        );
        assert!(retry_delay(&peer, 0, sub_millisecond) >= sub_millisecond);
        assert!(retry_delay(&peer, 0, sub_millisecond) < Duration::from_millis(1));
        assert_eq!(
            retry_delay(&peer, 0, Duration::from_secs(60)),
            MAX_REDIAL_DELAY
        );
        assert_eq!(
            retry_delay(&peer, 100, Duration::from_secs(1)),
            MAX_REDIAL_DELAY
        );
    }

    #[test]
    fn configuration_reprobe_jitter_is_stable_and_peer_specific() {
        let peer_a = PeerId::new("peer-a").expect("peer A");
        let peer_b = PeerId::new("peer-b").expect("peer B");
        let base = Duration::from_secs(300);
        let delay = configuration_reprobe_delay(&peer_a, base);

        assert_eq!(delay, configuration_reprobe_delay(&peer_a, base));
        assert_ne!(delay, configuration_reprobe_delay(&peer_b, base));
        assert!(delay >= base);
        assert!(delay < base + base / 4);
    }
}
