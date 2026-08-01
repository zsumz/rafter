use std::time::Duration;

use rafter::NodeId;

use super::*;
use crate::wire::PreparedPeerFrame;
use crate::{InboundQueueLimits, OutboundQueueLimits};

fn limits() -> RuntimeLimits {
    runtime_limits(3, 300, 1, 100)
}

#[test]
fn control_capacity_remains_available_and_bulk_gets_an_opportunity() {
    let queue = OutboundQueue::new(limits());
    queue
        .try_push(item(TrafficClass::Replication, 80))
        .expect("first bulk frame");
    queue
        .try_push(item(TrafficClass::Snapshot, 80))
        .expect("second bulk frame");
    assert!(matches!(
        queue.try_push(item(TrafficClass::Replication, 1)),
        Err(OutboundQueueError::Full(_))
    ));
    queue
        .try_push(item(TrafficClass::Control, 80))
        .expect("reserved control frame");

    let control = queue
        .pop_timeout(Duration::ZERO)
        .expect("queue read")
        .expect("control item");
    assert_eq!(control.class(), TrafficClass::Control);
    queue.release(&control).expect("release control");

    let replication = queue
        .pop_timeout(Duration::ZERO)
        .expect("queue read")
        .expect("replication item");
    assert_eq!(replication.class(), TrafficClass::Replication);
    queue.release(&replication).expect("release replication");

    let snapshot = queue
        .pop_timeout(Duration::ZERO)
        .expect("queue read")
        .expect("snapshot item");
    assert_eq!(snapshot.class(), TrafficClass::Snapshot);
    queue.release(&snapshot).expect("release snapshot");
}

#[test]
fn byte_reservation_is_independent_of_frame_reservation() {
    let limits = runtime_limits(4, 300, 1, 100);
    let queue = OutboundQueue::new(limits);
    queue
        .try_push(item(TrafficClass::Replication, 180))
        .expect("bulk bytes within unreserved capacity");
    assert!(matches!(
        queue.try_push(item(TrafficClass::Snapshot, 30)),
        Err(OutboundQueueError::Full(_))
    ));
    queue
        .try_push(item(TrafficClass::Control, 100))
        .expect("reserved control bytes remain available");
    assert_eq!(queue.depth().expect("queue depth").bytes, 280);
}

fn item(class: TrafficClass, complete_len: usize) -> OutboundItem<()> {
    let body_len = u32::try_from(complete_len.saturating_sub(4)).expect("body length");
    let frame = PreparedPeerFrame::new(
        vec![b'g'],
        vec![0],
        NodeId(1),
        NodeId(2),
        body_len,
        complete_len,
        1,
        1,
    );
    OutboundItem::message(NodeId(1), NodeId(2), class, frame)
}

fn runtime_limits(
    frames: usize,
    bytes: usize,
    reserved_frames: usize,
    reserved_bytes: usize,
) -> RuntimeLimits {
    let outbound = OutboundQueueLimits::new(frames, bytes, reserved_frames, reserved_bytes, 1)
        .expect("valid outbound limits");
    let inbound = InboundQueueLimits::new(1, 1, 1, 1).expect("valid inbound limits");
    RuntimeLimits::new(outbound, inbound, 1).expect("valid runtime limits")
}
