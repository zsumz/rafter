//! Validated runtime assembly for one blocking peer transport.

mod spawn;

use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    sync::Arc,
};

use crate::connection::{AcceptorContext, ReceiverRegistry, ReceiverTemplate};
use crate::diagnostics::{Counters, PeerCounterMap, PeerCounters};
use crate::queue::{InboundQueue, OutboundQueue, ReceiveMemoryBudget};
use crate::runtime::{InboundEpochs, RuntimeControl, SessionStoreHandle};
use crate::sender::{SenderCore, TlsSender};
use crate::snapshot::SnapshotResolverHandle;
use crate::{
    CertificateDirectory, EndpointBook, GroupIdCodec, PeerFrameCodec, PeerId, RuntimeLimits,
    TlsHandshakeConfig, TlsIdentity, TlsInbound, TlsPeerTransport, TlsPeerTransportBuilder,
    TlsTransportBuildError, TransportTimeouts,
};

use self::spawn::{spawn_acceptor, spawn_senders};
use super::validated::ValidatedBuilder;

pub(super) fn bind<G, C>(
    builder: TlsPeerTransportBuilder<G, C>,
) -> Result<TlsPeerTransport<G, C>, TlsTransportBuildError>
where
    G: Ord + Send + Sync + 'static,
    C: GroupIdCodec<G>,
{
    let ValidatedBuilder {
        config,
        identity,
        certificates,
        directory,
        endpoints,
        sessions,
        snapshot_resolver,
        codec,
        handshake,
        listener,
        peers,
    } = ValidatedBuilder::new(builder)?;
    let local_addr = local_address(&listener)?;

    let runtime_limits = config.limits().runtime();
    let control = Arc::new(RuntimeControl::new(config.timeouts().shutdown_grace()));
    let counters = Arc::new(Counters::default());
    let OutboundState {
        queues,
        peer_counters,
        sender_inputs,
    } = outbound_state(peers, runtime_limits);
    mark_initial_degradation(&control, &queues);

    let inbound_queue = Arc::new(InboundQueue::new(runtime_limits));
    let receive_memory = receive_memory_budget(runtime_limits, &codec);
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
        receive_memory: receive_memory.clone(),
        epochs: Arc::clone(&epochs),
        control: Arc::clone(&control),
        counters: Arc::clone(&counters),
        timeouts: config.timeouts(),
    };

    let sender_dependencies = SenderDependencies {
        local_peer: config.local_peer_id().clone(),
        endpoints,
        identity,
        certificates,
        handshake,
        sessions,
        codec,
        snapshot_resolver,
        control: Arc::clone(&control),
        counters: Arc::clone(&counters),
        timeouts: config.timeouts(),
    };
    let mut sender_workers = spawn_senders(sender_inputs, &sender_dependencies, &queues)?;

    let acceptor_context = AcceptorContext {
        listener,
        receivers: receiver_template,
        registry: Arc::clone(&receivers),
        control: Arc::clone(&control),
        limits: runtime_limits,
        timeouts: config.timeouts(),
    };
    let acceptor = spawn_acceptor(
        acceptor_context,
        config.local_peer_id(),
        &control,
        &queues,
        &mut sender_workers,
    )?;

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
        receive_memory,
        epochs,
        receivers,
        acceptor: Some(acceptor),
        senders: sender_workers,
    })
}

fn local_address(listener: &TcpListener) -> Result<SocketAddr, TlsTransportBuildError> {
    listener
        .local_addr()
        .map_err(|source| TlsTransportBuildError::LocalAddress { source })
}

fn receive_memory_budget<G, C>(
    limits: RuntimeLimits,
    codec: &PeerFrameCodec<G, C>,
) -> ReceiveMemoryBudget
where
    C: GroupIdCodec<G>,
{
    ReceiveMemoryBudget::new(limits.receive_memory(), codec.max_decoded_group_bytes())
}

type SenderInput<G> = (PeerId, Arc<OutboundQueue<G>>, Arc<PeerCounters>);

struct OutboundState<G> {
    queues: Arc<BTreeMap<PeerId, Arc<OutboundQueue<G>>>>,
    peer_counters: Arc<PeerCounterMap>,
    sender_inputs: Vec<SenderInput<G>>,
}

struct SenderDependencies<G, C> {
    local_peer: PeerId,
    endpoints: EndpointBook,
    identity: TlsIdentity,
    certificates: CertificateDirectory,
    handshake: TlsHandshakeConfig,
    sessions: SessionStoreHandle,
    codec: Arc<PeerFrameCodec<G, C>>,
    snapshot_resolver: Option<SnapshotResolverHandle<G>>,
    control: Arc<RuntimeControl>,
    counters: Arc<Counters>,
    timeouts: TransportTimeouts,
}

fn outbound_state<G>(peers: Vec<PeerId>, limits: RuntimeLimits) -> OutboundState<G> {
    let mut queues = BTreeMap::new();
    let mut counters = BTreeMap::new();
    let mut inputs = Vec::with_capacity(peers.len());
    for peer in peers {
        let queue = Arc::new(OutboundQueue::new(limits));
        let peer_counters = Arc::new(PeerCounters::default());
        queues.insert(peer.clone(), Arc::clone(&queue));
        counters.insert(peer.clone(), Arc::clone(&peer_counters));
        inputs.push((peer, queue, peer_counters));
    }
    OutboundState {
        queues: Arc::new(queues),
        peer_counters: Arc::new(counters),
        sender_inputs: inputs,
    }
}

fn mark_initial_degradation<G>(
    control: &RuntimeControl,
    queues: &BTreeMap<PeerId, Arc<OutboundQueue<G>>>,
) {
    for peer in queues.keys() {
        control.mark_degraded(peer);
    }
}
