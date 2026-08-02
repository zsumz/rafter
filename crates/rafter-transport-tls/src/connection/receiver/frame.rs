//! Cheap-route validation, full decode, and bounded inbound admission.

use rafter_service::AuthenticatedPeerEnvelope;

use crate::diagnostics::increment;
use crate::queue::{InboundQueueError, InboundQueueFull, ReceiveMemoryPermit};
use crate::runtime::InboundEpochGuard;
use crate::{GroupIdCodec, InboundSequence, PeerFrameCodec, PeerFrameScratch, PeerId};

use super::admission::{admit_frame, admit_route, AdmissionRefusal};
use super::classify::classify_decode_error;
use super::ReceiverTemplate;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FrameStep {
    Continue,
    Stop,
}

pub(super) struct FrameInput<'a> {
    pub(super) peer: &'a PeerId,
    pub(super) epoch: &'a InboundEpochGuard,
    pub(super) encoded: &'a [u8],
    pub(super) complete: usize,
    pub(super) memory: ReceiveMemoryPermit,
}

pub(super) fn process_frame<G, C>(
    template: &ReceiverTemplate<G, C>,
    expected: &mut InboundSequence,
    scratch: &mut PeerFrameScratch,
    input: FrameInput<'_>,
) -> FrameStep
where
    G: Ord,
    C: GroupIdCodec<G>,
{
    let routed = match template.codec.decode_route(input.encoded, scratch) {
        Ok(route) => route,
        Err(error) => {
            classify_decode_error(&template.counters, &error);
            return FrameStep::Stop;
        }
    };
    if expected.accept(routed.route().sequence).is_err() {
        increment(&template.counters.sequence_violations);
        increment(&template.counters.frames_dropped);
        return FrameStep::Stop;
    }
    if let Err(refusal) = admit_route(
        &template.directory,
        template.handshake.local_peer_id(),
        input.peer,
        routed.route(),
    ) {
        return handle_refusal(template, refusal);
    }

    let frame = match PeerFrameCodec::<G, C>::decode_routed(routed) {
        Ok(frame) => frame,
        Err(error) => {
            classify_decode_error(&template.counters, &error);
            return FrameStep::Stop;
        }
    };
    let envelope = match admit_frame(
        &template.directory,
        template.handshake.local_peer_id(),
        input.peer,
        frame,
    ) {
        Ok(envelope) => envelope,
        Err(refusal) => return handle_refusal(template, refusal),
    };
    enqueue(
        template,
        input.peer,
        input.epoch,
        input.complete,
        envelope,
        input.memory,
    )
}

fn handle_refusal<G, C>(template: &ReceiverTemplate<G, C>, refusal: AdmissionRefusal) -> FrameStep {
    increment(&template.counters.frames_dropped);
    match refusal {
        AdmissionRefusal::Identity => {
            increment(&template.counters.identity_mismatches);
            FrameStep::Stop
        }
        AdmissionRefusal::Unauthorized => {
            increment(&template.counters.unauthorized_frames);
            FrameStep::Continue
        }
        AdmissionRefusal::Retired => {
            increment(&template.counters.retired_peer_frames);
            FrameStep::Continue
        }
        AdmissionRefusal::Terminal => {
            template.control.fail("peer directory state is poisoned");
            FrameStep::Stop
        }
    }
}

fn enqueue<G, C>(
    template: &ReceiverTemplate<G, C>,
    peer: &PeerId,
    epoch: &InboundEpochGuard,
    complete: usize,
    envelope: AuthenticatedPeerEnvelope<G, PeerId>,
    memory: ReceiveMemoryPermit,
) -> FrameStep {
    let admitted = epoch.while_current(|| {
        template
            .inbound
            .try_push(peer.clone(), complete, envelope, memory)
    });
    match admitted {
        Ok(Some(Ok(()))) => {
            increment(&template.counters.frames_received);
            FrameStep::Continue
        }
        Ok(Some(Err(InboundQueueError::Full(InboundQueueFull::Peer)))) => {
            increment(&template.counters.inbound_full);
            increment(&template.counters.inbound_peer_full);
            increment(&template.counters.frames_dropped);
            FrameStep::Continue
        }
        Ok(Some(Err(InboundQueueError::Full(InboundQueueFull::Global)))) => {
            increment(&template.counters.inbound_full);
            increment(&template.counters.inbound_global_full);
            increment(&template.counters.frames_dropped);
            FrameStep::Continue
        }
        Ok(Some(Err(InboundQueueError::Closed))) => FrameStep::Stop,
        Ok(Some(Err(InboundQueueError::Poisoned))) => {
            template.control.fail("inbound queue state is poisoned");
            FrameStep::Stop
        }
        Ok(None) => {
            increment(&template.counters.frames_dropped);
            FrameStep::Stop
        }
        Err(()) => {
            template.control.fail("inbound epoch state is poisoned");
            FrameStep::Stop
        }
    }
}
