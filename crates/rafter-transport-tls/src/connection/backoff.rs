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
#[path = "backoff_test.rs"]
mod tests;
