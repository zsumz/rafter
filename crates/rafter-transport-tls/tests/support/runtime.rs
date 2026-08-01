use std::{net::SocketAddr, time::Duration};

use rafter::NodeId;
use rafter_service::{PeerEnvelope, PeerPolicy};
use rafter_transport_tls::{
    CertificateDirectory, ClusterId, EndpointBook, PeerEndpoint, PeerId, RuntimeLimits,
    SnapshotChunkResolver, TlsIdentity, TlsPeerDirectory, TlsPeerTransport,
    TlsPeerTransportBuilder, TransportConfig, TransportIoTimeouts, TransportLimits,
    TransportRuntimeTimeouts, TransportSessionStore, TransportTimeouts,
};

use super::session_store::MemorySessionStore;
use super::tls::{node_a_identity, node_b_identity, server_name};
use super::{request_vote, StringGroupCodec};

pub const NODE_A: NodeId = NodeId(1);
pub const NODE_B: NodeId = NodeId(2);
pub const GROUP_ID: &str = "orders";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupRoute {
    pub group_id: &'static str,
    pub node_a: NodeId,
    pub node_b: NodeId,
}

impl GroupRoute {
    pub const fn new(group_id: &'static str, node_a: NodeId, node_b: NodeId) -> Self {
        Self {
            group_id,
            node_a,
            node_b,
        }
    }
}

pub const DEFAULT_ROUTE: GroupRoute = GroupRoute::new(GROUP_ID, NODE_A, NODE_B);

pub type TestTransport = TlsPeerTransport<String, StringGroupCodec>;

#[derive(Clone, Debug)]
pub struct RuntimeFixture {
    cluster_id: ClusterId,
    peer_a: PeerId,
    peer_b: PeerId,
    identity_a: TlsIdentity,
    identity_b: TlsIdentity,
    certificates: CertificateDirectory,
    limits: TransportLimits,
    timeouts: TransportTimeouts,
}

impl RuntimeFixture {
    pub fn new(runtime: RuntimeLimits) -> Self {
        let peer_a = PeerId::new("peer-a").expect("valid peer A");
        let peer_b = PeerId::new("peer-b").expect("valid peer B");
        let identity_a = node_a_identity();
        let identity_b = node_b_identity();
        let certificates = CertificateDirectory::builder()
            .map_fingerprint(identity_a.leaf_fingerprint(), peer_a.clone())
            .expect("map peer A certificate")
            .map_fingerprint(identity_b.leaf_fingerprint(), peer_b.clone())
            .expect("map peer B certificate")
            .build();
        Self {
            cluster_id: ClusterId::new("runtime-test").expect("valid cluster"),
            peer_a,
            peer_b,
            identity_a,
            identity_b,
            certificates,
            limits: TransportLimits::default().with_runtime(runtime),
            timeouts: TransportTimeouts::new(
                TransportIoTimeouts::new(
                    Duration::from_millis(100),
                    Duration::from_secs(2),
                    Duration::from_secs(2),
                    Duration::from_secs(2),
                )
                .expect("valid I/O timeouts"),
                TransportRuntimeTimeouts::new(
                    Duration::from_millis(20),
                    Duration::from_millis(5),
                    Duration::from_millis(250),
                )
                .expect("valid runtime timeouts"),
            ),
        }
    }

    pub fn peer_a(&self) -> &PeerId {
        &self.peer_a
    }

    pub fn peer_b(&self) -> &PeerId {
        &self.peer_b
    }

    pub fn limits(&self) -> TransportLimits {
        self.limits
    }

    pub fn with_timeouts(mut self, timeouts: TransportTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    pub fn start_a(&self, endpoints: EndpointBook) -> TestTransport {
        self.start_a_routes(endpoints, &[DEFAULT_ROUTE])
    }

    pub fn start_a_with_store<S>(&self, endpoints: EndpointBook, sessions: S) -> TestTransport
    where
        S: TransportSessionStore,
    {
        self.builder(
            self.peer_a.clone(),
            self.identity_a.clone(),
            &self.peer_b,
            endpoints,
            &[DEFAULT_ROUTE],
            sessions,
        )
        .bind()
        .expect("start peer A transport")
    }

    pub fn bind_paused_a_with_store<S>(&self, endpoints: EndpointBook, sessions: S) -> TestTransport
    where
        S: TransportSessionStore,
    {
        self.builder(
            self.peer_a.clone(),
            self.identity_a.clone(),
            &self.peer_b,
            endpoints,
            &[DEFAULT_ROUTE],
            sessions,
        )
        .bind_paused()
        .expect("bind paused peer A transport")
    }

    pub fn start_a_with_resolver<R>(&self, endpoints: EndpointBook, resolver: R) -> TestTransport
    where
        R: SnapshotChunkResolver<String>,
    {
        self.builder(
            self.peer_a.clone(),
            self.identity_a.clone(),
            &self.peer_b,
            endpoints,
            &[DEFAULT_ROUTE],
            MemorySessionStore::new(),
        )
        .snapshot_resolver(resolver)
        .bind()
        .expect("start peer A snapshot transport")
    }

    pub fn start_b(&self) -> TestTransport {
        self.start_b_routes(&[DEFAULT_ROUTE])
    }

    pub fn start_a_routes(&self, endpoints: EndpointBook, routes: &[GroupRoute]) -> TestTransport {
        self.builder(
            self.peer_a.clone(),
            self.identity_a.clone(),
            &self.peer_b,
            endpoints,
            routes,
            MemorySessionStore::new(),
        )
        .bind()
        .expect("start peer A transport")
    }

    pub fn start_b_routes(&self, routes: &[GroupRoute]) -> TestTransport {
        self.builder(
            self.peer_b.clone(),
            self.identity_b.clone(),
            &self.peer_a,
            EndpointBook::new(self.limits.endpoints()),
            routes,
            MemorySessionStore::new(),
        )
        .bind()
        .expect("start peer B transport")
    }

    pub fn start_b_with_directory(
        &self,
        endpoints: EndpointBook,
        directory: TlsPeerDirectory<String>,
    ) -> TestTransport {
        self.builder_with_directory(
            self.peer_b.clone(),
            self.identity_b.clone(),
            endpoints,
            directory,
            MemorySessionStore::new(),
        )
        .bind()
        .expect("start peer B transport with caller directory")
    }

    pub fn bound_directory(&self, routes: &[GroupRoute]) -> TlsPeerDirectory<String> {
        let directory = TlsPeerDirectory::new(self.limits.directory());
        for route in routes {
            let group_id = route.group_id.to_owned();
            directory
                .bind(group_id.clone(), route.node_a, self.peer_a.clone())
                .expect("bind peer A");
            directory
                .bind(group_id, route.node_b, self.peer_b.clone())
                .expect("bind peer B");
        }
        directory
    }

    pub fn endpoints_to_a(&self, address: SocketAddr) -> EndpointBook {
        let endpoints = EndpointBook::new(self.limits.endpoints());
        endpoints
            .replace(
                self.peer_a.clone(),
                vec![PeerEndpoint::new(address, server_name())],
            )
            .expect("install peer A endpoint");
        endpoints
    }

    pub fn endpoints_to_b(&self, address: SocketAddr) -> EndpointBook {
        let endpoints = EndpointBook::new(self.limits.endpoints());
        endpoints
            .replace(
                self.peer_b.clone(),
                vec![PeerEndpoint::new(address, server_name())],
            )
            .expect("install peer B endpoint");
        endpoints
    }

    pub fn vote() -> PeerEnvelope<String> {
        Self::vote_for(DEFAULT_ROUTE)
    }

    pub fn vote_for(route: GroupRoute) -> PeerEnvelope<String> {
        PeerEnvelope {
            group_id: route.group_id.to_owned(),
            from: route.node_a,
            to: route.node_b,
            message: request_vote(route.node_a),
        }
    }

    fn builder<S>(
        &self,
        local_peer: PeerId,
        identity: TlsIdentity,
        authorized_peer: &PeerId,
        endpoints: EndpointBook,
        routes: &[GroupRoute],
        sessions: S,
    ) -> TlsPeerTransportBuilder<String, StringGroupCodec>
    where
        S: TransportSessionStore,
    {
        let directory = self.bound_directory(routes);
        for route in routes {
            let group_id = route.group_id.to_owned();
            let retirement_floor = route.node_a.max(route.node_b);
            directory
                .replace_policy(
                    &group_id,
                    PeerPolicy::new(vec![(*authorized_peer).clone()], Some(retirement_floor)),
                )
                .expect("install peer policy");
        }
        self.builder_with_directory(local_peer, identity, endpoints, directory, sessions)
    }

    fn builder_with_directory<S>(
        &self,
        local_peer: PeerId,
        identity: TlsIdentity,
        endpoints: EndpointBook,
        directory: TlsPeerDirectory<String>,
        sessions: S,
    ) -> TlsPeerTransportBuilder<String, StringGroupCodec>
    where
        S: TransportSessionStore,
    {
        let config = TransportConfig::new(
            self.cluster_id.clone(),
            local_peer,
            "127.0.0.1:0".parse().expect("loopback address"),
            self.limits,
            self.timeouts,
        );
        TlsPeerTransport::builder(config, StringGroupCodec::new(128))
            .identity(identity)
            .certificates(self.certificates.clone())
            .directory(directory)
            .endpoints(endpoints)
            .session_store(sessions)
    }
}

pub fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    condition()
}
