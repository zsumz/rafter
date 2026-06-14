//! Criterion micro-benchmarks for peer-frame encode and decode on the two
//! payload-bearing message shapes: append batches and snapshot chunks.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rafter::{
    AppendEntries, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, InstallSnapshotChunk, LogEntry, LogIndex, Message, NodeId,
    RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_codec::{decode_message, encode_message};

fn append_batch() -> Message {
    let entries: Vec<LogEntry> = (0..16)
        .map(|_| LogEntry::application(Term(3), vec![0xA5; 256]))
        .collect();
    Message::AppendEntries(AppendEntries {
        term: Term(3),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(41),
        prev_log_term: Term(3),
        sequence: 7,
        entries,
        leader_commit: LogIndex(41),
    })
}

fn snapshot_chunk() -> Message {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("bench-group").expect("valid group id"),
        NodeId(1),
        LogIndex(42),
        Term(3),
        Term(3),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("bench_state").expect("valid kind"),
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata");
    let snapshot = RaftSnapshot::new(metadata.clone(), 32 * 1024 * 1024, 0);
    Message::InstallSnapshotChunk(InstallSnapshotChunk {
        term: Term(3),
        leader_id: NodeId(1),
        transfer_id: snapshot.transfer_id(),
        metadata,
        total_payload_len: 32 * 1024 * 1024,
        application_payload_crc32: snapshot.application_payload_crc32,
        offset: 64 * 1024,
        chunk: vec![0x5A; 64 * 1024],
        done: false,
    })
}

fn bench_message(criterion: &mut Criterion, name: &str, message: &Message) {
    let encoded = encode_message(message).expect("message encodes");
    let mut group = criterion.benchmark_group(name);
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function("encode", |bencher| {
        bencher.iter(|| encode_message(std::hint::black_box(message)).expect("message encodes"));
    });
    group.bench_function("decode", |bencher| {
        bencher.iter(|| decode_message(std::hint::black_box(&encoded)).expect("frame decodes"));
    });
    group.finish();
}

fn codec(criterion: &mut Criterion) {
    bench_message(criterion, "append_entries_16x256b", &append_batch());
    bench_message(criterion, "install_snapshot_chunk_64k", &snapshot_chunk());
}

criterion_group!(codec_benches, codec);
criterion_main!(codec_benches);
