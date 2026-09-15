//! Three fixed peers, each with its own TLS identity and on-disk sessions.
use crate::{disk, Result, GROUP, MEMBERS};
use rafter::NodeId;
use rafter_transport_tls::{
    CertificateDirectory, ClusterId, EndpointBook, FileTransportSessionStore, GroupIdCodec,
    PeerEndpoint, PeerId, TlsIdentity, TlsPeerDirectory, TlsPeerTransport, TlsServerName,
    TransportConfig, TransportLimits, TransportTimeouts,
};
use std::{io, path::Path};

pub type Transport = TlsPeerTransport<u64, GroupCodec>;

pub fn cluster() -> ClusterId {
    ClusterId::new("rafter-counter-v1").unwrap()
}
pub fn principal(id: u64) -> PeerId {
    PeerId::new(&format!("node-{id}")).unwrap()
}

pub fn address(variable: &str, default: u16, id: u64) -> Result<std::net::SocketAddr> {
    let base: u16 = std::env::var(variable)
        .unwrap_or_else(|_| default.to_string())
        .parse()?;
    let port = base
        .checked_add(u16::try_from(id)?)
        .ok_or("port is out of range")?;
    Ok(([127, 0, 0, 1], port).into())
}

pub fn open(id: u64, directory: &Path) -> Result<(Transport, TlsPeerDirectory<u64>)> {
    let identity = TlsIdentity::from_pem_files(
        format!("certs/node-{id}.pem"),
        format!("certs/node-{id}-key.pem"),
        "certs/ca.pem",
    )?;
    let limits = TransportLimits::default();
    let directory_map = TlsPeerDirectory::new(limits.directory());
    let endpoints = EndpointBook::new(limits.endpoints());
    let mut certificates = CertificateDirectory::builder();
    for peer in MEMBERS {
        certificates = certificates
            .map_pem_certificate_file(format!("certs/node-{peer}.pem"), principal(peer))?;
        directory_map.bind(GROUP, NodeId(peer), principal(peer))?;
        if peer != id {
            endpoints.replace(
                principal(peer),
                vec![PeerEndpoint::new(
                    address("COUNTER_PEER_BASE", 7000, peer)?,
                    TlsServerName::new(&format!("node-{peer}"))?,
                )],
            )?;
        }
    }
    let certificates = certificates.build();
    identity.validate_local_peer(&principal(id), &certificates)?;
    let sessions = FileTransportSessionStore::open_existing(
        directory.join("sessions"),
        &cluster(),
        &principal(id),
    )?;
    let config = TransportConfig::new(
        cluster(),
        principal(id),
        address("COUNTER_PEER_BASE", 7000, id)?,
        limits,
        TransportTimeouts::default(),
    );
    let transport = TlsPeerTransport::builder(config, GroupCodec)
        .identity(identity)
        .certificates(certificates)
        .directory(directory_map.clone())
        .endpoints(endpoints)
        .session_store(sessions)
        .bind_paused()?;
    Ok((transport, directory_map))
}

#[derive(Clone, Copy, Debug)]
pub struct GroupCodec;
impl GroupIdCodec<u64> for GroupCodec {
    type Error = io::Error;
    fn max_encoded_len(&self) -> usize {
        8
    }
    fn max_decoded_heap_bytes(&self) -> usize {
        0
    }
    fn encode(&self, group: &u64, output: &mut Vec<u8>) -> io::Result<()> {
        output.clear();
        output.extend_from_slice(&group.to_be_bytes());
        Ok(())
    }
    fn decode(&self, bytes: &[u8]) -> io::Result<u64> {
        Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
            disk::invalid("group ID must be eight bytes")
        })?))
    }
}
