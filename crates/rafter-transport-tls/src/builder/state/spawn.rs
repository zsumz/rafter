//! All-or-nothing worker startup for one assembled runtime.
//!
//! This module owns thread creation and the failure path that unwinds it: a
//! refused spawn stops every worker already started before the build error is
//! returned, so a partially started runtime never escapes. Assembling the
//! contexts those workers run stays with the builder state.

use std::{collections::BTreeMap, sync::Arc, thread};

use crate::connection::{
    accept_loop, sender_loop, snapshot_loop, AcceptorContext, SenderContext, SnapshotContext,
};
use crate::queue::OutboundQueue;
use crate::runtime::{run_guarded, RuntimeControl};
use crate::transport::NamedWorker;
use crate::{GroupIdCodec, PeerId, TlsTransportBuildError};

use super::{SenderDependencies, SenderInput};

pub(super) fn spawn_senders<G, C>(
    inputs: Vec<SenderInput<G>>,
    dependencies: &SenderDependencies<G, C>,
    queues: &BTreeMap<PeerId, Arc<OutboundQueue<G>>>,
) -> Result<Vec<NamedWorker>, TlsTransportBuildError>
where
    G: Send + 'static,
    C: GroupIdCodec<G>,
{
    let mut workers = Vec::with_capacity(inputs.len().saturating_mul(2));
    for (peer, queue, peer_counters) in inputs {
        let context = SenderContext {
            peer: peer.clone(),
            endpoints: dependencies.endpoints.clone(),
            identity: dependencies.identity.clone(),
            certificates: dependencies.certificates.clone(),
            handshake: dependencies.handshake.clone(),
            sessions: dependencies.sessions.clone(),
            queue: Arc::clone(&queue),
            control: Arc::clone(&dependencies.control),
            counters: Arc::clone(&dependencies.counters),
            peer_counters: Arc::clone(&peer_counters),
            timeouts: dependencies.timeouts,
        };
        let role = format!("rafter-tls-sender-{}-{peer}", dependencies.local_peer);
        match spawn_guarded(role, &dependencies.control, move || sender_loop(&context)) {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                stop_started(&dependencies.control, queues, &mut workers);
                return Err(error);
            }
        }
        let Some(resolver) = dependencies.snapshot_resolver.clone() else {
            continue;
        };
        let context = SnapshotContext {
            resolver,
            codec: Arc::clone(&dependencies.codec),
            queue,
            control: Arc::clone(&dependencies.control),
            counters: Arc::clone(&dependencies.counters),
            peer_counters,
            poll: dependencies.timeouts.poll(),
        };
        let role = format!("rafter-tls-snapshot-{}-{peer}", dependencies.local_peer);
        match spawn_guarded(role, &dependencies.control, move || snapshot_loop(&context)) {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                stop_started(&dependencies.control, queues, &mut workers);
                return Err(error);
            }
        }
    }
    Ok(workers)
}

pub(super) fn spawn_acceptor<G, C>(
    context: AcceptorContext<G, C>,
    local_peer: &PeerId,
    control: &Arc<RuntimeControl>,
    queues: &BTreeMap<PeerId, Arc<OutboundQueue<G>>>,
    sender_workers: &mut [NamedWorker],
) -> Result<NamedWorker, TlsTransportBuildError>
where
    G: Ord + Send + Sync + 'static,
    C: GroupIdCodec<G>,
{
    let role = format!("rafter-tls-acceptor-{local_peer}");
    match spawn_guarded(role, control, move || accept_loop(&context)) {
        Ok(worker) => Ok(worker),
        Err(error) => {
            stop_started(control, queues, sender_workers);
            Err(error)
        }
    }
}

fn spawn_guarded(
    role: String,
    control: &Arc<RuntimeControl>,
    operation: impl FnOnce() + Send + 'static,
) -> Result<NamedWorker, TlsTransportBuildError> {
    let worker_role = role.clone();
    let guarded = Arc::clone(control);
    let handle = thread::Builder::new()
        .name(role.clone())
        .spawn(move || run_guarded(&guarded, &worker_role, operation))
        .map_err(|source| TlsTransportBuildError::SpawnWorker {
            role: role.clone(),
            source,
        })?;
    Ok(NamedWorker::new(role, handle))
}

fn stop_started<G>(
    control: &Arc<RuntimeControl>,
    queues: &BTreeMap<PeerId, Arc<OutboundQueue<G>>>,
    workers: &mut [NamedWorker],
) {
    control.request_shutdown();
    for queue in queues.values() {
        let _ = queue.close();
    }
    let mut ignored = Vec::new();
    for worker in workers {
        worker.join(&mut ignored);
    }
}
