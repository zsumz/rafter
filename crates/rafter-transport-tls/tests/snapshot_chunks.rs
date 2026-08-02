mod support;

use std::{
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    InMemorySnapshotChunkSource, LogIndex, Message, NodeId, RaftSnapshot, RaftSnapshotMetadata,
    SnapshotChunkSend, SnapshotGroupId, Term,
};
use rafter_service::{RaftTransport, SnapshotChunkEnvelope};
use rafter_transport_tls::{
    RuntimeLimits, SnapshotChunkResolveRequest, SnapshotChunkResolver, SnapshotChunkSourceResolver,
    TlsTransportError, TransportHealth,
};
use support::runtime::{wait_until, RuntimeFixture, DEFAULT_ROUTE, GROUP_ID, NODE_A, NODE_B};

#[test]
fn sender_worker_resolves_and_transmits_one_snapshot_chunk() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let payload = b"one bounded snapshot payload".to_vec();
    let snapshot = snapshot(&payload);
    let mut source = InMemorySnapshotChunkSource::new();
    source
        .insert(&snapshot, payload.clone())
        .expect("snapshot payload matches its descriptor");
    let sender = fixture.start_a_with_resolver(
        fixture.endpoints_to_b(receiver.local_addr()),
        SnapshotChunkSourceResolver::new(source),
    );

    sender
        .sender()
        .send_snapshot_chunk(envelope(&snapshot))
        .expect("the bounded directive is enqueued");

    let mut delivered = None;
    assert!(wait_until(Duration::from_secs(3), || {
        let mut drained = receiver.inbound().drain(1).expect("inbound queue");
        delivered = drained.pop();
        delivered.is_some()
    }));
    let delivered = delivered.expect("one authenticated chunk arrives");
    assert_eq!(delivered.group_id, GROUP_ID.to_owned());
    assert_eq!(delivered.authenticated_peer, fixture.peer_a().clone());
    assert_eq!(delivered.raft_from, NODE_A);
    assert_eq!(delivered.raft_to, NODE_B);
    let Message::InstallSnapshotChunk(chunk) = delivered.message else {
        panic!("snapshot directive must become a chunk frame");
    };
    assert_eq!(chunk.chunk, payload);
    assert_eq!(chunk.transfer_id, snapshot.transfer_id());
    assert_eq!(chunk.offset, 0);
    assert!(chunk.done);

    let diagnostics = sender.diagnostics();
    assert_eq!(diagnostics.snapshot_directives_enqueued, 1);
    assert_eq!(diagnostics.snapshot_chunks_resolved, 1);
    assert_eq!(diagnostics.snapshot_source_refusals, 0);
    assert_eq!(diagnostics.snapshot_resolve_failures, 0);
    assert_eq!(diagnostics.snapshot_resolution_mismatches, 0);

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn synchronous_admission_never_waits_for_snapshot_resolution() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let payload = b"worker-owned payload read".to_vec();
    let snapshot = snapshot(&payload);
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(Mutex::new(None));
    let resolver = GatedResolver {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        observed: Arc::clone(&observed),
        bytes: payload.clone(),
    };
    let _release_on_panic = ReleaseOnDrop(Arc::clone(&release));
    let sender =
        fixture.start_a_with_resolver(fixture.endpoints_to_b(receiver.local_addr()), resolver);

    sender
        .sender()
        .send_snapshot_chunk(envelope(&snapshot))
        .expect("admission returns while the worker-owned read is blocked");
    assert!(wait_until(Duration::from_secs(3), || {
        entered.load(Ordering::Acquire)
    }));
    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("control work is admitted while snapshot resolution blocks");
    let mut control = None;
    assert!(wait_until(Duration::from_secs(3), || {
        control = receiver.inbound().drain(1).expect("inbound queue").pop();
        control.is_some()
    }));
    assert!(matches!(
        control.expect("control frame").message,
        Message::RequestVote(_)
    ));
    assert!(!release.load(Ordering::Acquire));
    assert_eq!(
        observed.lock().expect("observed request lock").as_ref(),
        Some(&ObservedRequest {
            group_id: GROUP_ID.to_owned(),
            from: NODE_A,
            to: NODE_B,
            len: u32::try_from(payload.len()).expect("test payload fits u32"),
        })
    );

    release.store(true, Ordering::Release);
    assert!(wait_until(Duration::from_secs(3), || {
        receiver
            .inbound()
            .depth()
            .is_ok_and(|(frames, _)| frames == 1)
    }));

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn paused_runtime_does_not_touch_snapshot_storage_before_activation() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let payload = b"resolve only after activation".to_vec();
    let snapshot = snapshot(&payload);
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let resolver = GatedResolver {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        observed: Arc::new(Mutex::new(None)),
        bytes: payload,
    };
    let _release_on_panic = ReleaseOnDrop(Arc::clone(&release));
    let sender = fixture
        .bind_paused_a_with_resolver(fixture.endpoints_to_b(receiver.local_addr()), resolver);

    sender
        .sender()
        .send_snapshot_chunk(envelope(&snapshot))
        .expect("snapshot directive");
    thread::sleep(Duration::from_millis(100));
    assert!(!entered.load(Ordering::Acquire));

    sender.start().expect("activate sender");
    assert!(wait_until(Duration::from_secs(3), || {
        entered.load(Ordering::Acquire)
    }));
    release.store(true, Ordering::Release);
    assert!(wait_until(Duration::from_secs(3), || {
        receiver
            .inbound()
            .depth()
            .is_ok_and(|(frames, _)| frames == 1)
    }));

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn stopping_a_paused_runtime_discards_without_invoking_the_resolver() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let payload = b"never resolve during paused shutdown".to_vec();
    let snapshot = snapshot(&payload);
    let entered = Arc::new(AtomicBool::new(false));
    let resolver = GatedResolver {
        entered: Arc::clone(&entered),
        release: Arc::new(AtomicBool::new(true)),
        observed: Arc::new(Mutex::new(None)),
        bytes: payload,
    };
    let sender = fixture
        .bind_paused_a_with_resolver(fixture.endpoints_to_b(receiver.local_addr()), resolver);

    sender
        .sender()
        .send_snapshot_chunk(envelope(&snapshot))
        .expect("snapshot directive");
    sender.join().expect("paused sender joins");

    assert!(!entered.load(Ordering::Acquire));
    receiver.join().expect("receiver joins");
}

#[test]
fn resolver_finishing_after_shutdown_grace_cannot_requeue_or_send() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let payload = b"late snapshot resolution".to_vec();
    let snapshot = snapshot(&payload);
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let resolver = GatedResolver {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        observed: Arc::new(Mutex::new(None)),
        bytes: payload,
    };
    let _release_on_panic = ReleaseOnDrop(Arc::clone(&release));
    let sender =
        fixture.start_a_with_resolver(fixture.endpoints_to_b(receiver.local_addr()), resolver);

    sender
        .sender()
        .send_snapshot_chunk(envelope(&snapshot))
        .expect("snapshot directive");
    assert!(wait_until(Duration::from_secs(3), || {
        entered.load(Ordering::Acquire)
    }));
    sender.shutdown();
    thread::sleep(Duration::from_millis(300));
    release.store(true, Ordering::Release);

    assert!(wait_until(Duration::from_secs(3), || {
        sender.diagnostics().frames_dropped == 1
    }));
    assert_eq!(sender.diagnostics().snapshot_chunks_resolved, 0);
    assert!(receiver
        .inbound()
        .drain(1)
        .expect("inbound queue")
        .is_empty());

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn committed_retirement_revokes_a_queued_snapshot_before_resolution() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let payload = b"retired before snapshot resolution".to_vec();
    let snapshot = snapshot(&payload);
    let mut source = InMemorySnapshotChunkSource::new();
    source
        .insert(&snapshot, payload)
        .expect("snapshot payload matches its descriptor");
    let sender = fixture.bind_paused_a_with_resolver(
        fixture.endpoints_to_b(receiver.local_addr()),
        SnapshotChunkSourceResolver::new(source),
    );

    sender
        .sender()
        .send_snapshot_chunk(envelope(&snapshot))
        .expect("snapshot is queued while workers are paused");
    sender
        .sender()
        .update_peers(
            &GROUP_ID.to_owned(),
            rafter_service::PeerPolicy::new(Vec::new(), Some(NODE_B)),
        )
        .expect("destination is retired before activation");
    sender.start().expect("activate sender");

    assert!(wait_until(Duration::from_secs(3), || {
        sender.diagnostics().invalidated_queued_frames == 1
    }));
    assert_eq!(sender.diagnostics().snapshot_chunks_resolved, 0);
    assert!(receiver
        .inbound()
        .drain(1)
        .expect("inbound queue")
        .is_empty());

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn unavailable_snapshot_is_dropped_like_a_lost_raft_message() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let snapshot = snapshot(b"not registered in this source");
    let sender = fixture.start_a_with_resolver(
        fixture.endpoints_to_b(receiver.local_addr()),
        SnapshotChunkSourceResolver::new(InMemorySnapshotChunkSource::new()),
    );

    sender
        .sender()
        .send_snapshot_chunk(envelope(&snapshot))
        .expect("the directive is admitted before source resolution");

    assert!(wait_until(Duration::from_secs(3), || {
        sender.diagnostics().snapshot_source_refusals == 1
    }));
    assert!(receiver
        .inbound()
        .drain(1)
        .expect("inbound queue")
        .is_empty());
    assert_eq!(sender.diagnostics().frames_dropped, 1);

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn typed_snapshot_resolver_failure_drops_only_that_attempt() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let snapshot = snapshot(b"resolver fails before reading this payload");
    let sender = fixture.start_a_with_resolver(
        fixture.endpoints_to_b(receiver.local_addr()),
        FailingResolver,
    );

    sender
        .sender()
        .send_snapshot_chunk(envelope(&snapshot))
        .expect("the directive is admitted before resolver execution");

    assert!(wait_until(Duration::from_secs(3), || {
        sender.diagnostics().snapshot_resolve_failures == 1
    }));
    assert!(receiver
        .inbound()
        .drain(1)
        .expect("inbound queue")
        .is_empty());
    assert_eq!(sender.diagnostics().frames_dropped, 1);
    assert_ne!(sender.health(), TransportHealth::Failed);

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn resolver_must_return_the_directive_exact_byte_count() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let snapshot = snapshot(b"exact-length contract");
    let sender =
        fixture.start_a_with_resolver(fixture.endpoints_to_b(receiver.local_addr()), ShortResolver);

    sender
        .sender()
        .send_snapshot_chunk(envelope(&snapshot))
        .expect("the directive is admitted before byte validation");

    assert!(wait_until(Duration::from_secs(3), || {
        sender.diagnostics().snapshot_resolution_mismatches == 1
    }));
    assert!(receiver
        .inbound()
        .drain(1)
        .expect("inbound queue")
        .is_empty());
    assert_eq!(sender.diagnostics().frames_dropped, 1);

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn snapshot_directive_is_refused_when_no_resolver_was_installed() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let sender = fixture.start_a(fixture.endpoints_to_b(receiver.local_addr()));
    let snapshot = snapshot(b"payload");

    assert!(matches!(
        sender.sender().send_snapshot_chunk(envelope(&snapshot)),
        Err(TlsTransportError::SnapshotResolverUnavailable)
    ));
    assert_eq!(sender.diagnostics().frames_enqueued, 0);

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedRequest {
    group_id: String,
    from: NodeId,
    to: NodeId,
    len: u32,
}

#[derive(Clone, Debug)]
struct GatedResolver {
    entered: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
    observed: Arc<Mutex<Option<ObservedRequest>>>,
    bytes: Vec<u8>,
}

impl SnapshotChunkResolver<String> for GatedResolver {
    type Error = ResolveError;

    fn resolve(
        &self,
        request: SnapshotChunkResolveRequest<'_, String>,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        *self.observed.lock().expect("observed request lock") = Some(ObservedRequest {
            group_id: request.group_id().clone(),
            from: request.from(),
            to: request.to(),
            len: request.chunk().len,
        });
        self.entered.store(true, Ordering::Release);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !self.release.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        Ok(Some(self.bytes.clone()))
    }
}

#[derive(Clone, Copy, Debug)]
struct FailingResolver;

impl SnapshotChunkResolver<String> for FailingResolver {
    type Error = ResolveError;

    fn resolve(
        &self,
        _request: SnapshotChunkResolveRequest<'_, String>,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Err(ResolveError)
    }
}

#[derive(Clone, Copy, Debug)]
struct ShortResolver;

impl SnapshotChunkResolver<String> for ShortResolver {
    type Error = ResolveError;

    fn resolve(
        &self,
        request: SnapshotChunkResolveRequest<'_, String>,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        let len = usize::try_from(request.chunk().len.saturating_sub(1))
            .expect("snapshot chunk length fits usize");
        Ok(Some(vec![0; len]))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolveError;

impl fmt::Display for ResolveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("snapshot resolver failed")
    }
}

impl Error for ResolveError {}

struct ReleaseOnDrop(Arc<AtomicBool>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn envelope(snapshot: &RaftSnapshot) -> SnapshotChunkEnvelope<String> {
    SnapshotChunkEnvelope {
        group_id: DEFAULT_ROUTE.group_id.to_owned(),
        from: NODE_A,
        to: NODE_B,
        chunk: SnapshotChunkSend {
            term: Term(3),
            leader_id: NODE_A,
            transfer_id: snapshot.transfer_id(),
            metadata: snapshot.metadata.clone(),
            total_payload_len: snapshot.application_payload_len,
            application_payload_crc32: snapshot.application_payload_crc32,
            offset: 0,
            len: u32::try_from(snapshot.application_payload_len)
                .expect("test payload fits one chunk"),
            done: true,
        },
    }
}

fn snapshot(payload: &[u8]) -> RaftSnapshot {
    let application = ApplicationSnapshotMetadata::new(
        ApplicationSnapshotKind::new("kv").expect("valid snapshot kind"),
        ApplicationSnapshotVersion::new(1).expect("nonzero snapshot version"),
    );
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new(GROUP_ID).expect("valid snapshot group"),
        NODE_A,
        LogIndex(9),
        Term(2),
        Term(3),
        application,
    )
    .expect("valid snapshot metadata");
    RaftSnapshot::from_payload(metadata, payload)
}
