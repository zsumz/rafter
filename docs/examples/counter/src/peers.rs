//! Three fixed peers, each with its own TLS identity and on-disk sessions.
use crate::{disk, Result, GROUP, MEMBERS};
use rafter::NodeId;
use rafter_service::{PeerPolicy, RaftTransport};
use rafter_transport_tls::{
    CertificateDirectory, ClusterId, EndpointBook, FileTransportSessionStore, GroupIdCodec,
    PeerEndpoint, PeerId, TlsIdentity, TlsPeerDirectory, TlsPeerTransport, TlsServerName,
    TransportConfig, TransportLimits, TransportTimeouts,
};
use std::{io, net::SocketAddr, path::Path};

pub type Transport = TlsPeerTransport<u64, GroupCodec>;

pub fn authorize(id: u64, transport: &Transport) -> Result<()> {
    transport.sender().update_peers(
        &GROUP,
        PeerPolicy::new(
            MEMBERS
                .into_iter()
                .filter(|peer| *peer != id)
                .map(principal)
                .collect(),
            Some(NodeId(3)),
        ),
    )?;
    Ok(())
}

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

// Addresses change placement, not the certificate identity of each fixed voter.
fn peer_addresses() -> Result<[SocketAddr; 3]> {
    match std::env::var("COUNTER_PEER_ADDRS") {
        Ok(value) => parse_peer_addresses(&value),
        Err(std::env::VarError::NotPresent) => Ok([
            address("COUNTER_PEER_BASE", 7000, 1)?,
            address("COUNTER_PEER_BASE", 7000, 2)?,
            address("COUNTER_PEER_BASE", 7000, 3)?,
        ]),
        Err(error) => Err(error.into()),
    }
}

fn parse_peer_addresses(value: &str) -> Result<[SocketAddr; 3]> {
    let parsed = value
        .split(',')
        .map(|part| part.trim().parse::<SocketAddr>())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let addresses: [SocketAddr; 3] = parsed
        .try_into()
        .map_err(|_| "COUNTER_PEER_ADDRS needs three addresses in node 1, 2, 3 order")?;
    for (index, endpoint) in addresses.iter().enumerate() {
        if endpoint.port() == 0 || endpoint.ip().is_unspecified() || endpoint.ip().is_multicast() {
            return Err("COUNTER_PEER_ADDRS needs concrete peer IPs and nonzero ports".into());
        }
        if addresses[..index].contains(endpoint) {
            return Err("COUNTER_PEER_ADDRS needs distinct peer addresses".into());
        }
    }
    Ok(addresses)
}

pub fn open(id: u64, directory: &Path) -> Result<(Transport, TlsPeerDirectory<u64>)> {
    let addresses = peer_addresses()?;
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
                    addresses[(peer - 1) as usize],
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
        addresses[(id - 1) as usize],
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

#[cfg(test)]
mod tests {
    use super::parse_peer_addresses;

    #[test]
    fn explicit_addresses_preserve_node_order_and_accept_ipv6() {
        let parsed =
            parse_peer_addresses("192.0.2.1:7001, [2001:db8::2]:8100,127.0.0.1:9003").unwrap();
        assert_eq!(
            parsed.map(|address| address.to_string()),
            ["192.0.2.1:7001", "[2001:db8::2]:8100", "127.0.0.1:9003",]
        );
    }

    #[test]
    fn partial_or_extra_peer_lists_are_refused() {
        for value in [
            "",
            "127.0.0.1:1",
            "127.0.0.1:1,127.0.0.1:2",
            "127.0.0.1:1,127.0.0.1:2,127.0.0.1:3,127.0.0.1:4",
        ] {
            assert!(parse_peer_addresses(value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn invalid_or_ambiguous_peer_endpoints_are_refused() {
        for first in [
            "localhost:7001",
            "127.0.0.1",
            "127.0.0.1:65536",
            "127.0.0.1:0",
            "0.0.0.0:7001",
            "[::]:7001",
            "224.0.0.1:7001",
            "[ff02::1]:7001",
            "127.0.0.1:7002",
            "",
        ] {
            let value = format!("{first},127.0.0.1:7002,127.0.0.1:7003");
            assert!(parse_peer_addresses(&value).is_err(), "accepted {value}");
        }
    }
}
