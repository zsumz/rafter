//! Allocation-counted adversarial check for the TLS receive-memory weight.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    error::Error,
    fmt,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use rafter::{
    AppendEntries, ConfigurationEntry, ConfigurationId, JointMembership, LogEntry, LogIndex,
    MembershipSet, Message, NodeId, Term,
};
use rafter_transport_tls::{
    ConnectionSequence, GroupIdCodec, PeerFrame, PeerFrameCodec, PeerFrameScratch, WireLimits,
    MIN_SAFE_DECODE_AMPLIFICATION,
};

struct CountingAllocator;

static COUNTING: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if COUNTING.load(Ordering::Relaxed) && !pointer.is_null() {
            add_live(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if COUNTING.load(Ordering::Relaxed) {
            subtract_live(layout.size());
        }
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, layout, new_size) };
        if COUNTING.load(Ordering::Relaxed) && !replacement.is_null() {
            subtract_live(layout.size());
            add_live(new_size);
        }
        replacement
    }
}

fn add_live(bytes: usize) {
    let live = LIVE
        .fetch_add(bytes, Ordering::Relaxed)
        .saturating_add(bytes);
    let _ = PEAK.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |peak| {
        (live > peak).then_some(live)
    });
}

fn subtract_live(bytes: usize) {
    let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
        Some(live.saturating_sub(bytes))
    });
}

fn main() {
    check(
        "minimum-size append entries",
        append_with_minimum_entries(58_000),
    );
    check(
        "maximum joint membership",
        append_with_maximum_joint_membership(),
    );
}

fn check(label: &str, message: Message) {
    let codec = PeerFrameCodec::new(StringGroupCodec, WireLimits::default()).expect("valid codec");
    let frame = PeerFrame::new(
        ConnectionSequence::FIRST,
        "orders".to_owned(),
        NodeId(1),
        NodeId(2),
        message,
    )
    .expect("matching sender");
    let mut encoded = Vec::new();
    codec
        .encode_into(&mut encoded, &mut PeerFrameScratch::new(), &frame)
        .expect("adversarial frame remains inside the wire limit");
    drop(frame);

    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Release);
    let decoded = codec
        .decode(&encoded, &mut PeerFrameScratch::new())
        .expect("valid adversarial frame decodes");
    COUNTING.store(false, Ordering::Release);
    let peak = PEAK.load(Ordering::Relaxed);
    let charged = encoded
        .len()
        .saturating_mul(MIN_SAFE_DECODE_AMPLIFICATION)
        .saturating_add(codec.max_decoded_group_bytes());
    assert!(
        peak <= charged,
        "decode peaked at {peak} bytes for {} wire bytes, above the {charged}-byte charge",
        encoded.len()
    );
    println!(
        "{label}: {} wire bytes, {peak} peak decoded bytes, {charged} charged bytes",
        encoded.len()
    );
    drop(decoded);
}

fn append_with_minimum_entries(count: usize) -> Message {
    append_entries((0..count).map(|_| LogEntry::noop(Term(1))).collect())
}

fn append_with_maximum_joint_membership() -> Message {
    append_entries(vec![LogEntry::configuration(
        Term(1),
        ConfigurationEntry::joint(
            ConfigurationId(1),
            JointMembership::new(membership(1, 1_000_000), membership(2_000_000, 3_000_000)),
        ),
    )])
}

fn membership(voter_start: u64, learner_start: u64) -> MembershipSet {
    MembershipSet::new(
        (voter_start..voter_start + 65_535).map(NodeId).collect(),
        (learner_start..learner_start + 65_535)
            .map(NodeId)
            .collect(),
    )
    .expect("maximum wire membership is structurally valid")
}

fn append_entries(entries: Vec<LogEntry>) -> Message {
    Message::AppendEntries(AppendEntries {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(0),
        prev_log_term: Term(0),
        entries: entries.into(),
        leader_commit: LogIndex(0),
        sequence: 1,
    })
}

#[derive(Clone, Copy, Debug)]
struct StringGroupCodec;

impl GroupIdCodec<String> for StringGroupCodec {
    type Error = GroupCodecError;

    fn max_encoded_len(&self) -> usize {
        128
    }

    fn max_decoded_heap_bytes(&self) -> usize {
        128
    }

    fn encode(&self, group_id: &String, output: &mut Vec<u8>) -> Result<(), Self::Error> {
        output.clear();
        output.extend_from_slice(group_id.as_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<String, Self::Error> {
        std::str::from_utf8(input)
            .map(str::to_owned)
            .map_err(|_| GroupCodecError)
    }
}

#[derive(Clone, Copy, Debug)]
struct GroupCodecError;

impl fmt::Display for GroupCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid group ID")
    }
}

impl Error for GroupCodecError {}
