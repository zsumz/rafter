//! Real mutual TLS, finite queues, and durable connection-session identities.
use rafter::NodeId;
use rafter_service::PeerPolicy;
use rafter_transport_tls::{
    CertificateDirectory, CertificateDirectoryLimits, ClusterId, DirectoryLimits, EndpointBook,
    EndpointBookLimits, FileTransportSessionStore, GroupIdCodec, InboundQueueLimits,
    OutboundQueueLimits, PeerEndpoint, PeerId, ReceiveMemoryLimits, RuntimeLimits,
    SessionStoreLimits, TlsIdentity, TlsPeerDirectory, TlsPeerTransport, TlsServerName,
    TransportConfig, TransportIoTimeouts, TransportLimits, TransportRuntimeTimeouts,
    TransportTimeouts, WireLimits,
};
use std::{array::TryFromSliceError, net::SocketAddr, path::Path, time::Duration};

pub const GROUP: u64 = 1;
pub type Link = TlsPeerTransport<u64, Codec>;
#[derive(Clone, Debug)]
pub struct Codec;
impl GroupIdCodec<u64> for Codec {
    type Error = TryFromSliceError;
    fn max_encoded_len(&self) -> usize {
        8
    }
    fn max_decoded_heap_bytes(&self) -> usize {
        0
    }
    fn encode(&self, group: &u64, out: &mut Vec<u8>) -> Result<(), Self::Error> {
        out.clear();
        out.extend_from_slice(&group.to_be_bytes());
        Ok(())
    }
    fn decode(&self, input: &[u8]) -> Result<u64, Self::Error> {
        input.try_into().map(u64::from_be_bytes)
    }
}
pub fn peer(id: u64) -> PeerId {
    PeerId::new(&format!("ledger-peer-{id}")).unwrap()
}
pub fn endpoint(book: &EndpointBook, id: u64, address: SocketAddr) {
    book.replace(
        peer(id),
        vec![PeerEndpoint::new(
            address,
            TlsServerName::new("rafter-peer").unwrap(),
        )],
    )
    .unwrap();
}
pub fn bind(root: &Path, id: u64, fresh: bool) -> (Link, EndpointBook) {
    let limits = TransportLimits::new(
        DirectoryLimits::new(1, 3).unwrap(),
        EndpointBookLimits::new(2, 1).unwrap(),
        CertificateDirectoryLimits::new(3, 3).unwrap(),
        WireLimits::new(128 * 1024, 8).unwrap(),
    )
    .with_sessions(SessionStoreLimits::new(2).unwrap())
    .with_runtime(
        RuntimeLimits::new(
            OutboundQueueLimits::new(16, 128 * 1024, 2, 4096, 1).unwrap(),
            InboundQueueLimits::new(16, 128 * 1024, 32, 256 * 1024).unwrap(),
            6,
        )
        .unwrap()
        .with_receive_memory(ReceiveMemoryLimits::new(8 * 1024 * 1024, 32).unwrap()),
    );
    let certificates_root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../fenced-lock/tests/fixtures/production-tls");
    let identity = TlsIdentity::from_pem_files(
        certificates_root.join(format!("node-{id}.pem")),
        certificates_root.join(format!("node-{id}-key.pem")),
        certificates_root.join("ca.pem"),
    )
    .unwrap();
    let mut certificates = CertificateDirectory::builder_with_limits(limits.certificates());
    let directory = TlsPeerDirectory::new(limits.directory());
    let endpoints = EndpointBook::new(limits.endpoints());
    for node in 1..=3 {
        certificates = certificates
            .map_pem_certificate_file(
                certificates_root.join(format!("node-{node}.pem")),
                peer(node),
            )
            .unwrap();
        directory.bind(GROUP, NodeId(node), peer(node)).unwrap();
        if node != id {
            endpoint(&endpoints, node, "127.0.0.1:9".parse().unwrap());
        }
    }
    directory
        .replace_policy(
            &GROUP,
            PeerPolicy::new(
                (1..=3).filter(|node| *node != id).map(peer).collect(),
                Some(NodeId(3)),
            ),
        )
        .unwrap();
    let cluster = ClusterId::new("ledger-durable-owner-tls").unwrap();
    let sessions = if fresh {
        FileTransportSessionStore::create_new(
            root.join("sessions"),
            cluster.clone(),
            peer(id),
            limits.sessions(),
        )
        .unwrap()
    } else {
        FileTransportSessionStore::open_existing(root.join("sessions"), &cluster, &peer(id))
            .unwrap()
    };
    let timeouts = TransportTimeouts::new(
        TransportIoTimeouts::new(
            Duration::from_millis(100),
            Duration::from_secs(2),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .unwrap(),
        TransportRuntimeTimeouts::new(
            Duration::from_millis(10),
            Duration::from_millis(5),
            Duration::from_millis(250),
        )
        .unwrap(),
    );
    let link = Link::builder(
        TransportConfig::new(
            cluster,
            peer(id),
            "127.0.0.1:0".parse().unwrap(),
            limits,
            timeouts,
        ),
        Codec,
    )
    .identity(identity)
    .certificates(certificates.build())
    .directory(directory)
    .endpoints(endpoints.clone())
    .session_store(sessions)
    .bind_paused()
    .unwrap();
    (link, endpoints)
}
