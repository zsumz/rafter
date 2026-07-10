//! Rafter codec harness: AppendEntries encode/decode workloads.
//!
//! This is Rafter-only evidence for the batched wire path. One measured
//! operation encodes one `AppendEntries` message and then decodes the produced
//! frame back into a protocol message.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bench_compare::{
    payload_of_size, report_json, CodecShapeMetrics, WorkloadMetrics, CODEC_BATCH_FRAMES,
    CODEC_LARGE_FRAMES, LARGE_PAYLOAD_BYTES, PAYLOAD_BYTES,
};
use rafter::{AppendEntries, LogEntry, LogIndex, Message, NodeId, Term};
use rafter_codec::{decode_message, encode_message_into};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, new_size) };
        if !next.is_null() {
            REALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        next
    }
}

#[derive(Clone, Copy)]
struct AllocSnapshot {
    allocs: u64,
    reallocs: u64,
}

impl AllocSnapshot {
    fn now() -> Self {
        Self {
            allocs: ALLOCS.load(Ordering::Relaxed),
            reallocs: REALLOCS.load(Ordering::Relaxed),
        }
    }

    fn allocation_events_since(self, before: Self) -> usize {
        self.allocs
            .saturating_sub(before.allocs)
            .saturating_add(self.reallocs.saturating_sub(before.reallocs)) as usize
    }
}

fn main() {
    let batched = codec_workload("append_64x512", CODEC_BATCH_FRAMES, 64, PAYLOAD_BYTES);
    let large = codec_workload("append_1_large", CODEC_LARGE_FRAMES, 1, LARGE_PAYLOAD_BYTES);
    println!(
        "{}",
        report_json(
            "rafter-codec",
            "path:../crates (workspace @ HEAD)",
            "one operation is encode_message + decode_message for one AppendEntries frame",
            &[batched, large],
        )
    );
}

fn codec_workload(
    name: &'static str,
    frames: usize,
    entries_per_frame: usize,
    payload_bytes: usize,
) -> WorkloadMetrics {
    let message = append_entries(entries_per_frame, payload_bytes);
    let mut latencies = Vec::with_capacity(frames);
    let mut codec_shape = CodecShapeMetrics {
        frames,
        entries: frames.saturating_mul(entries_per_frame),
        encoded_bytes: 0,
        allocation_events: 0,
    };
    let mut encoded = Vec::new();
    encode_message_into(&mut encoded, &message).expect("message warms encode buffer");
    let alloc_before = AllocSnapshot::now();
    let started = Instant::now();

    for _ in 0..frames {
        let submitted = Instant::now();
        encode_message_into(&mut encoded, std::hint::black_box(&message)).expect("message encodes");
        codec_shape.encoded_bytes = codec_shape.encoded_bytes.saturating_add(encoded.len());
        let decoded =
            decode_message(std::hint::black_box(encoded.as_slice())).expect("message decodes");
        std::hint::black_box(decoded);
        latencies.push(submitted.elapsed());
    }
    let elapsed = started.elapsed();
    codec_shape.allocation_events = AllocSnapshot::now().allocation_events_since(alloc_before);

    WorkloadMetrics {
        name,
        proposals: frames,
        payload_bytes,
        max_in_flight: entries_per_frame,
        elapsed,
        latencies,
        shape: None,
        service_shape: None,
        read_shape: None,
        codec_shape: Some(codec_shape),
        multiraft_shape: None,
        failover_shape: None,
    }
}

fn append_entries(entries: usize, payload_bytes: usize) -> Message {
    Message::AppendEntries(AppendEntries {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(1),
        prev_log_term: Term(1),
        sequence: 1,
        entries: (0..entries)
            .map(|_| LogEntry::application(Term(1), payload_of_size(payload_bytes)))
            .collect(),
        leader_commit: LogIndex(1),
    })
}
