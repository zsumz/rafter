use std::{error::Error, io};

use rafter_transport_tls::{TlsIdentity, TlsServerName};
use rustls::{client::ClientConnection, server::ServerConnection};

pub const CA_PEM: &[u8] = include_bytes!("../fixtures/tls/ca.pem");
pub const UNTRUSTED_CA_PEM: &[u8] = include_bytes!("../fixtures/tls/untrusted-ca.pem");
pub const NODE_A_CERT_PEM: &[u8] = include_bytes!("../fixtures/tls/node-a.pem");
pub const NODE_A_KEY_PEM: &[u8] = include_bytes!("../fixtures/tls/node-a-key.pem");
pub const NODE_A_NEXT_CERT_PEM: &[u8] = include_bytes!("../fixtures/tls/node-a-next.pem");
pub const NODE_A_NEXT_KEY_PEM: &[u8] = include_bytes!("../fixtures/tls/node-a-next-key.pem");
pub const NODE_B_CERT_PEM: &[u8] = include_bytes!("../fixtures/tls/node-b.pem");
pub const NODE_B_KEY_PEM: &[u8] = include_bytes!("../fixtures/tls/node-b-key.pem");

pub fn node_a_identity() -> TlsIdentity {
    identity(NODE_A_CERT_PEM, NODE_A_KEY_PEM, CA_PEM)
}

pub fn node_a_next_identity() -> TlsIdentity {
    identity(NODE_A_NEXT_CERT_PEM, NODE_A_NEXT_KEY_PEM, CA_PEM)
}

pub fn node_b_identity() -> TlsIdentity {
    identity(NODE_B_CERT_PEM, NODE_B_KEY_PEM, CA_PEM)
}

pub fn identity(certificate: &[u8], key: &[u8], roots: &[u8]) -> TlsIdentity {
    TlsIdentity::from_pem(certificate, key, roots).expect("valid TLS fixture")
}

pub fn server_name() -> TlsServerName {
    TlsServerName::new("rafter-peer.test").expect("valid fixture name")
}

pub fn connection_pair(
    client_identity: &TlsIdentity,
    server_identity: &TlsIdentity,
) -> (ClientConnection, ServerConnection) {
    let client = client_identity
        .client_connection(&server_name())
        .expect("client connection");
    let server = server_identity
        .server_connection()
        .expect("server connection");
    (client, server)
}

pub fn complete_handshake(
    client: &mut ClientConnection,
    server: &mut ServerConnection,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..32 {
        transfer_client_to_server(client, server)?;
        transfer_server_to_client(server, client)?;
        if !client.is_handshaking() && !server.is_handshaking() {
            return Ok(());
        }
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "in-memory TLS handshake did not converge",
    )
    .into())
}

fn transfer_client_to_server(
    client: &mut ClientConnection,
    server: &mut ServerConnection,
) -> Result<(), Box<dyn Error>> {
    while client.wants_write() {
        let mut flight = Vec::new();
        client.write_tls(&mut flight)?;
        if flight.is_empty() {
            break;
        }
        let mut input = flight.as_slice();
        server.read_tls(&mut input)?;
        server.process_new_packets()?;
    }
    Ok(())
}

fn transfer_server_to_client(
    server: &mut ServerConnection,
    client: &mut ClientConnection,
) -> Result<(), Box<dyn Error>> {
    while server.wants_write() {
        let mut flight = Vec::new();
        server.write_tls(&mut flight)?;
        if flight.is_empty() {
            break;
        }
        let mut input = flight.as_slice();
        client.read_tls(&mut input)?;
        client.process_new_packets()?;
    }
    Ok(())
}
