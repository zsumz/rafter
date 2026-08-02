//! Deterministic retry and sparse-reprobe pacing.

use std::time::Duration;

use crate::config::MAX_REDIAL_DELAY;
use crate::PeerId;

pub(super) fn retry_delay(
    local: &PeerId,
    remote: &PeerId,
    attempt: u32,
    initial: Duration,
) -> Duration {
    let multiplier = 1_u32 << attempt.min(12);
    let window = initial.saturating_mul(multiplier).min(MAX_REDIAL_DELAY);
    equal_jitter(local, remote, u64::from(attempt), window)
}

pub(super) fn configuration_reprobe_delay(
    local: &PeerId,
    remote: &PeerId,
    initial: Duration,
) -> Duration {
    initial.saturating_add(jitter(local, remote, u64::MAX, initial / 4))
}

fn equal_jitter(local: &PeerId, remote: &PeerId, salt: u64, window: Duration) -> Duration {
    let floor = window / 2;
    floor
        .saturating_add(jitter(local, remote, salt, window.saturating_sub(floor)))
        .max(Duration::from_nanos(1))
}

fn jitter(local: &PeerId, remote: &PeerId, salt: u64, span: Duration) -> Duration {
    const JITTER_BUCKETS: u32 = 1_024;

    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ salt;
    mix(&mut hash, local.as_str().as_bytes());
    mix(&mut hash, remote.as_str().as_bytes());
    let bucket = u32::try_from(hash % u64::from(JITTER_BUCKETS)).unwrap_or_default();
    scale(span, bucket, JITTER_BUCKETS)
}

fn mix(hash: &mut u64, value: &[u8]) {
    for byte in u64::try_from(value.len())
        .unwrap_or(u64::MAX)
        .to_le_bytes()
        .into_iter()
        .chain(value.iter().copied())
    {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
}

fn scale(duration: Duration, numerator: u32, denominator: u32) -> Duration {
    let quotient = duration / denominator;
    let remainder = duration.saturating_sub(quotient.saturating_mul(denominator));
    quotient
        .saturating_mul(numerator)
        .saturating_add(remainder.saturating_mul(numerator) / denominator)
}

#[cfg(test)]
mod tests {
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
}
