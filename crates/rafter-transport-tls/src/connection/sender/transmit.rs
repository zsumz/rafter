//! Transmission of one prepared frame over an established peer stream.
//!
//! This module owns the last checks before bytes leave a sender worker:
//! endpoint currency, still-live authorization, the exact negotiated frame
//! bound, sequence allocation, and what a write failure does to the current
//! item. Dial policy and queue lifecycle stay with the sender loop.

use crate::connection::dial::OutboundConnection;
use crate::connection::io::write_all_flush;
use crate::diagnostics::increment;
use crate::queue::{OutboundItem, RequeueOutcome};

use super::retry::backoff_after_failure;
use super::{drop_current, release_current, SenderContext, WorkerStep};

pub(super) fn transmit_current<G>(
    context: &SenderContext<G>,
    connection: &mut Option<OutboundConnection>,
    current: &mut Option<OutboundItem<G>>,
    encoded: &mut Vec<u8>,
    retry_attempt: &mut u32,
) -> WorkerStep {
    let Some(endpoint_current) = endpoint_is_current(context, connection.as_ref()) else {
        return WorkerStep::Stop;
    };
    if !endpoint_current {
        *connection = None;
        return WorkerStep::Retry;
    }
    let Some(item_bytes) = current.as_ref().map(OutboundItem::bytes) else {
        return WorkerStep::Retry;
    };
    if current.as_ref().is_some_and(|item| !item.is_authorized()) {
        increment(&context.counters.invalidated_queued_frames);
        drop_current(context, current);
        return WorkerStep::Retry;
    }
    let Some(frame_bytes) = connection.as_ref().map(|open| open.frame_bytes) else {
        return WorkerStep::Retry;
    };
    if item_bytes > frame_bytes {
        increment(&context.counters.frame_too_large);
        drop_current(context, current);
        context.control.fail(format!(
            "accepted outbound frame of {item_bytes} bytes exceeds exact negotiated bound of \
             {frame_bytes} bytes"
        ));
        return WorkerStep::Stop;
    }

    let Some(open) = connection.as_mut() else {
        return WorkerStep::Retry;
    };
    if current.as_ref().is_some_and(|item| !item.is_authorized()) {
        increment(&context.counters.invalidated_queued_frames);
        drop_current(context, current);
        return WorkerStep::Retry;
    }
    let Ok(sequence) = open.sequence.take_next() else {
        increment(&context.counters.sequence_violations);
        *connection = None;
        return WorkerStep::Retry;
    };
    let Some(frame) = current.as_ref().and_then(OutboundItem::prepared) else {
        context
            .control
            .fail("outbound item is not prepared for transmission");
        return WorkerStep::Stop;
    };
    frame.encode_into(sequence, encoded);
    let Some(endpoint_current) = endpoint_is_current(context, connection.as_ref()) else {
        return WorkerStep::Stop;
    };
    if !endpoint_current {
        *connection = None;
        return WorkerStep::Retry;
    }
    if current.as_ref().is_some_and(|item| !item.is_authorized()) {
        increment(&context.counters.invalidated_queued_frames);
        *connection = None;
        drop_current(context, current);
        return WorkerStep::Retry;
    }
    let Some(open) = connection.as_mut() else {
        return WorkerStep::Retry;
    };
    if write_all_flush(&mut open.stream, encoded).is_err() {
        return handle_write_failure(context, connection, current, retry_attempt);
    }

    if open.stability_proven() {
        *retry_attempt = 0;
    }
    increment(&context.counters.frames_sent);
    context.peer_counters.sent();
    release_current(context, current);
    WorkerStep::Ready
}

fn handle_write_failure<G>(
    context: &SenderContext<G>,
    connection: &mut Option<OutboundConnection>,
    current: &mut Option<OutboundItem<G>>,
    retry_attempt: &mut u32,
) -> WorkerStep {
    increment(&context.counters.tls_failures);
    *connection = None;
    if current
        .as_ref()
        .is_some_and(|item| item.class() != crate::TrafficClass::Control)
    {
        if !current.as_mut().is_some_and(OutboundItem::retry_bulk) {
            increment(&context.counters.retry_exhausted_frames);
            drop_current(context, current);
            backoff_after_failure(context, retry_attempt);
            return WorkerStep::Retry;
        }
        let Some(item) = current.take() else {
            context.control.fail("outbound retry lost its current item");
            return WorkerStep::Stop;
        };
        match context.queue.requeue_ready(item) {
            Ok(RequeueOutcome::Queued) => {}
            Ok(RequeueOutcome::SenderStopped) => {
                increment(&context.counters.frames_dropped);
                context.peer_counters.dropped();
                return WorkerStep::Stop;
            }
            Err(_) => {
                context.control.fail("outbound queue state is poisoned");
                return WorkerStep::Stop;
            }
        }
    }
    backoff_after_failure(context, retry_attempt);
    WorkerStep::Retry
}

fn endpoint_is_current<G>(
    context: &SenderContext<G>,
    connection: Option<&OutboundConnection>,
) -> Option<bool> {
    let Some(open) = connection else {
        return Some(true);
    };
    match context.endpoints.snapshot(&context.peer) {
        Ok(Some(snapshot)) => Some(snapshot.generation() == open.endpoint_generation),
        Ok(None) => Some(false),
        Err(error) => {
            context.control.fail(format!(
                "endpoint book failed for {}: {error}",
                context.peer
            ));
            None
        }
    }
}
