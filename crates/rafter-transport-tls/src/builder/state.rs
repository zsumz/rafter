//! Validated runtime assembly and all-or-nothing worker startup.

use std::{collections::BTreeMap, net::TcpListener, sync::Arc, thread};

use crate::connection::{
    accept_loop, sender_loop, AcceptorContext, ReceiverRegistry, ReceiverTemplate, SenderContext,
};
use crate::diagnostics::{Counters, PeerCounterMap, PeerCounters};
use crate::queue::{InboundQueue, OutboundQueue};
use crate::runtime::{run_guarded, InboundEpochs, RuntimeControl};
use crate::sender::{SenderCore, TlsSender};
use crate::transport::NamedWorker;
use crate::{
    CertificateDirectory, GroupIdCodec, PeerFrameCodec, PeerId, TlsHandshakeConfig, TlsInbound,
    TlsPeerTransport, TlsPeerTransportBuilder, TlsTransportBuildError, TransportConfig,
};

#[allow(clippy::too_many_lines)]
pub(super) fn bind<G, C>(
    builder: TlsPeerTransportBuilder<G, C>,
) -> Result<TlsPeerTransport<G, C>, TlsTransportBuildError>
where
    G: Ord + Send + Sync + 'static,
    C: GroupIdCodec<G>,
{
    let TlsPeerTransportBuilder {
        config,
        group_codec,
        identity,
        certificates,
        directory,
        endpoints,
        sessions,
        snapshot_resolver,
    } = builder;
    let identity = identity.ok_or(TlsTransportBuildError::MissingIdentity)?;
    let certificates = certificates.ok_or(TlsTransportBuildError::MissingCertificates)?;
    let directory = directory.ok_or(TlsTransportBuildError::MissingDirectory)?;
    let endpoints = endpoints.ok_or(TlsTransportBuildError::MissingEndpoints)?;
    let sessions = sessions.ok_or(TlsTransportBuildError::MissingSessionStore)?;

    identity
        .validate_local_peer(config.local_peer_id(), &certificates)
        .map_err(|source| TlsTransportBuildError::LocalIdentity { source })?;
    sessions
        .preflight(config.local_peer_id())
        .map_err(|source| TlsTransportBuildError::SessionStore {
            peer: config.local_peer_id().clone(),
            source,
        })?;
    let peers = endpoints
        .peer_ids()
        .map_err(|source| TlsTransportBuildError::EndpointBook { source })?;
    validate_remote_peers(&config, &certificates, &sessions, &peers)?;

    let codec = Arc::new(
        PeerFrameCodec::new(group_codec, config.limits().wire())
            .map_err(|source| TlsTransportBuildError::FrameCodec { source })?,
    );
    let handshake = TlsHandshakeConfig::current(
        config.cluster_id().clone(),
        config.local_peer_id().clone(),
        config.limits().wire(),
    )
    .map_err(|source| TlsTransportBuildError::HandshakeConfig { source })?;
    let listener =
        TcpListener::bind(config.bind_addr()).map_err(|source| TlsTransportBuildError::Bind {
            address: config.bind_addr(),
            source,
        })?;
    listener
        .set_nonblocking(true)
        .map_err(|source| TlsTransportBuildError::ConfigureListener { source })?;
    let local_addr = listener
        .local_addr()
        .map_err(|source| TlsTransportBuildError::LocalAddress { source })?;

    let runtime_limits = config.limits().runtime();
    let control = Arc::new(RuntimeControl::new(config.timeouts().shutdown_grace()));
    let counters = Arc::new(Counters::default());
    let mut queue_map: BTreeMap<PeerId, Arc<OutboundQueue<G>>> = BTreeMap::new();
    let mut counter_map = BTreeMap::new();
    let mut sender_inputs = Vec::with_capacity(peers.len());
    for peer in peers {
        let queue = Arc::new(OutboundQueue::new(runtime_limits));
        let peer_counter = Arc::new(PeerCounters::default());
        queue_map.insert(peer.clone(), Arc::clone(&queue));
        counter_map.insert(peer.clone(), Arc::clone(&peer_counter));
        sender_inputs.push((peer, queue, peer_counter));
    }
    let queues = Arc::new(queue_map);
    let peer_counters: Arc<PeerCounterMap> = Arc::new(counter_map);
    for peer in queues.keys() {
        control.mark_degraded(peer);
    }

    let inbound_queue = Arc::new(InboundQueue::new(runtime_limits));
    let inbound = TlsInbound {
        queue: Arc::clone(&inbound_queue),
    };
    let sender = TlsSender {
        core: Arc::new(SenderCore {
            local_peer_id: config.local_peer_id().clone(),
            directory: directory.clone(),
            codec: Arc::clone(&codec),
            queues: Arc::clone(&queues),
            snapshot_resolver: snapshot_resolver.clone(),
            control: Arc::clone(&control),
            counters: Arc::clone(&counters),
        }),
    };
    let epochs = Arc::new(InboundEpochs::default());
    let receivers = Arc::new(ReceiverRegistry::new());
    let receiver_template = ReceiverTemplate {
        identity: identity.clone(),
        certificates: certificates.clone(),
        handshake: handshake.clone(),
        sessions: sessions.clone(),
        codec: Arc::clone(&codec),
        directory,
        inbound: Arc::clone(&inbound_queue),
        epochs: Arc::clone(&epochs),
        control: Arc::clone(&control),
        counters: Arc::clone(&counters),
        timeouts: config.timeouts(),
    };

    let mut sender_workers = Vec::with_capacity(sender_inputs.len());
    for (peer, queue, peer_counter) in sender_inputs {
        let context = SenderContext {
            peer: peer.clone(),
            endpoints: endpoints.clone(),
            identity: identity.clone(),
            certificates: certificates.clone(),
            handshake: handshake.clone(),
            sessions: sessions.clone(),
            codec: Arc::clone(&codec),
            snapshot_resolver: snapshot_resolver.clone(),
            queue,
            control: Arc::clone(&control),
            counters: Arc::clone(&counters),
            peer_counters: peer_counter,
            timeouts: config.timeouts(),
        };
        let role = format!("rafter-tls-sender-{}-{peer}", config.local_peer_id());
        match spawn_guarded(role, &control, move || sender_loop(&context)) {
            Ok(worker) => sender_workers.push(worker),
            Err(error) => {
                stop_started(&control, &queues, &mut sender_workers);
                return Err(error);
            }
        }
    }

    let acceptor_context = AcceptorContext {
        listener,
        receivers: receiver_template,
        registry: Arc::clone(&receivers),
        control: Arc::clone(&control),
        limits: runtime_limits,
        timeouts: config.timeouts(),
    };
    let acceptor_role = format!("rafter-tls-acceptor-{}", config.local_peer_id());
    let acceptor = match spawn_guarded(acceptor_role, &control, move || {
        accept_loop(&acceptor_context);
    }) {
        Ok(worker) => worker,
        Err(error) => {
            stop_started(&control, &queues, &mut sender_workers);
            return Err(error);
        }
    };

    Ok(TlsPeerTransport {
        config,
        local_addr,
        sender,
        inbound,
        control,
        counters,
        queues,
        peer_counters,
        inbound_queue,
        epochs,
        receivers,
        acceptor: Some(acceptor),
        senders: sender_workers,
    })
}

fn validate_remote_peers(
    config: &TransportConfig,
    certificates: &CertificateDirectory,
    sessions: &crate::runtime::SessionStoreHandle,
    peers: &[PeerId],
) -> Result<(), TlsTransportBuildError> {
    for peer in peers {
        if peer == config.local_peer_id() {
            return Err(TlsTransportBuildError::LocalPeerEndpoint { peer: peer.clone() });
        }
        if !certificates.contains_peer(peer) {
            return Err(TlsTransportBuildError::UnconfiguredCertificate { peer: peer.clone() });
        }
        sessions
            .preflight(peer)
            .map_err(|source| TlsTransportBuildError::SessionStore {
                peer: peer.clone(),
                source,
            })?;
    }
    Ok(())
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
