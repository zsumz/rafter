//! Retry pacing and configuration-block recovery for one sender worker.

use std::{thread, time::Duration};

use super::{should_stop, SenderContext, WorkerStep};
use crate::connection::backoff::retry_delay;
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
            let remaining = dial_attempts
                .reprobe_remaining()
                .unwrap_or_else(|| context.timeouts.configuration_reprobe());
            match wait_for_endpoint_change(context, generation, remaining) {
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
    mut remaining: Duration,
) -> ConfigurationWait {
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
        if remaining.is_zero() {
            return ConfigurationWait::Reprobe;
        }
        let step = context.timeouts.poll().min(remaining);
        thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
}
