//! Retry pacing and configuration-block recovery for one sender worker.

use std::{
    thread,
    time::{Duration, Instant},
};

use crate::PeerId;

use super::{should_stop, SenderContext, WorkerStep};
use crate::connection::dial::{dial, DialAttemptState, DialError, OutboundConnection};

pub(super) fn connect<G>(
    context: &SenderContext<G>,
    connection: &mut Option<OutboundConnection>,
    connected_once: &mut bool,
    retry_attempt: &mut u32,
    dial_attempts: &mut DialAttemptState,
) -> WorkerStep {
    match dial(context, *connected_once, dial_attempts) {
        Ok(open) => {
            *connected_once = true;
            *connection = Some(open);
            WorkerStep::Ready
        }
        Err(DialError::Retry(message)) => {
            context.peer_counters.record_failure(message, false);
            backoff_after_failure(context, retry_attempt);
            WorkerStep::Retry
        }
        Err(DialError::ConfigurationBlocked {
            generation,
            message,
        }) => {
            context.peer_counters.record_failure(message, true);
            *retry_attempt = 0;
            match wait_for_endpoint_change(context, generation) {
                ConfigurationWait::Changed => WorkerStep::Retry,
                ConfigurationWait::Reprobe => {
                    dial_attempts.reprobe();
                    WorkerStep::Retry
                }
                ConfigurationWait::Stop => WorkerStep::Stop,
            }
        }
        Err(DialError::Terminal(message)) => {
            context.control.fail(message);
            WorkerStep::Stop
        }
    }
}

pub(super) fn backoff_after_failure<G>(context: &SenderContext<G>, retry_attempt: &mut u32) {
    let delay = retry_delay(&context.peer, *retry_attempt, context.timeouts.redial());
    *retry_attempt = (*retry_attempt).saturating_add(1);
    sleep_interruptibly(context, delay);
}

fn sleep_interruptibly<G>(context: &SenderContext<G>, duration: Duration) {
    let mut remaining = duration;
    while !remaining.is_zero()
        && !context.control.terminal()
        && !context.control.shutdown_grace_expired()
    {
        let step = remaining.min(context.timeouts.poll());
        thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
}

enum ConfigurationWait {
    Changed,
    Reprobe,
    Stop,
}

fn wait_for_endpoint_change<G>(
    context: &SenderContext<G>,
    blocked_generation: crate::EndpointGeneration,
) -> ConfigurationWait {
    let blocked_at = Instant::now();
    let reprobe = context.timeouts.configuration_reprobe();
    loop {
        if should_stop(context) || context.control.shutdown_requested() {
            return ConfigurationWait::Stop;
        }
        match context.endpoints.snapshot(&context.peer) {
            Ok(Some(snapshot)) if snapshot.generation() == blocked_generation => {}
            Ok(_) => return ConfigurationWait::Changed,
            Err(error) => {
                context.control.fail(format!(
                    "endpoint book failed for {}: {error}",
                    context.peer
                ));
                return ConfigurationWait::Stop;
            }
        }
        let remaining = reprobe.saturating_sub(blocked_at.elapsed());
        if remaining.is_zero() {
            return ConfigurationWait::Reprobe;
        }
        thread::sleep(context.timeouts.poll().min(remaining));
    }
}

fn retry_delay(peer: &PeerId, attempt: u32, initial: Duration) -> Duration {
    const MAX_MILLIS: u128 = 30_000;
    let multiplier = 1_u128 << attempt.min(12);
    let base = initial
        .as_millis()
        .saturating_mul(multiplier)
        .min(MAX_MILLIS);
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ u64::from(attempt);
    for byte in peer.as_str().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let jitter_bound = (base / 4).max(1);
    let jitter = u128::from(hash) % jitter_bound;
    Duration::from_millis(
        u64::try_from(base.saturating_add(jitter).min(MAX_MILLIS)).unwrap_or(30_000),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::PeerId;

    use super::retry_delay;

    #[test]
    fn retry_delay_is_deterministic_and_capped() {
        let peer = PeerId::new("peer-a").expect("peer ID");
        assert_eq!(
            retry_delay(&peer, 3, Duration::from_millis(100)),
            retry_delay(&peer, 3, Duration::from_millis(100))
        );
        assert!(retry_delay(&peer, 100, Duration::from_secs(1)) <= Duration::from_secs(30));
    }
}
