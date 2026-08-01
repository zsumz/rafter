//! Frozen version-1 client for adversarial current-process compatibility tests.

use std::{
    fs::File,
    io::{BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    sync::Arc,
};

use rafter::{LogIndex, Message, NodeId, PreVote, Term};
use rafter_transport_tls::{ServerHelloStatus, ServerRefusal};
use rustls::{
    client::{ClientConfig, ClientConnection, Resumption},
    crypto::ring,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    RootCertStore, StreamOwned,
};

const GROUP_ID: u64 = 1;
const MAX_FRAME_BYTES: u32 = 2_163_089;
const MAX_ID_BYTES: usize = 128;
const HANDSHAKE_MAGIC: &[u8; 10] = b"RAFTER-TLS";
const CLUSTER_ID: &str = "rafter-reference-fenced-lock-1";
const TLS_ALPN_PROTOCOL: &[u8] = b"rafter/1";
const TRANSPORT_VERSION: u16 = 1;
const PEER_CODEC_VERSION: u16 = 1;

/// Completes mutual TLS but deliberately sends no Rafter hello.
pub fn open_authenticated(addr: SocketAddr, certificate_node: NodeId) {
    let _stream = tls_stream(addr, certificate_node);
}

/// Opens one authenticated session and writes each requested sequence.
///
/// The attempt may intentionally disagree with its authenticated certificate
/// or embedded message sender to exercise refusal paths.
pub fn send_sequences(
    addr: SocketAddr,
    attempt: SequenceAttempt,
    sequences: &[u64],
) -> FrozenServerHello {
    let mut stream = tls_stream(addr, attempt.certificate_node);
    let mut encoded = Vec::new();
    encode_frozen_client_hello(&mut encoded, attempt.claimed_node, attempt.session);
    stream.write_all(&encoded).expect("client hello writes");
    stream.flush().expect("client hello flushes");
    let response = read_frozen_server_hello(&mut stream, attempt.to);
    if response.status() != ServerHelloStatus::Accepted {
        return response;
    }

    let message = Message::PreVote(PreVote {
        term: Term(0),
        candidate_id: attempt.embedded_from,
        last_log_index: LogIndex::ZERO,
        last_log_term: Term(0),
    });
    let message = rafter_codec::encode_message(&message).expect("frozen peer message encodes");
    for sequence in sequences {
        encode_frozen_peer_frame(&mut encoded, *sequence, attempt, &message);
        stream.write_all(&encoded).expect("test frame writes");
        stream.flush().expect("test frame flushes");
    }
    response
}

/// Result decoded by the frozen version-1 compatibility client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrozenServerHello {
    status: ServerHelloStatus,
}

impl FrozenServerHello {
    /// Accepted or typed-refused result observed on the version-1 wire.
    #[must_use]
    pub const fn status(self) -> ServerHelloStatus {
        self.status
    }
}

/// One independently-authenticated public-transport attempt.
#[derive(Clone, Copy, Debug)]
pub struct SequenceAttempt {
    certificate_node: NodeId,
    claimed_node: NodeId,
    outer_from: NodeId,
    embedded_from: NodeId,
    to: NodeId,
    session: u64,
}

impl SequenceAttempt {
    /// Builds an internally consistent attempt.
    #[must_use]
    pub const fn authenticated(
        certificate_node: NodeId,
        from: NodeId,
        to: NodeId,
        session: u64,
    ) -> Self {
        Self {
            certificate_node,
            claimed_node: from,
            outer_from: from,
            embedded_from: from,
            to,
            session,
        }
    }

    /// Replaces the outer frame sender without changing the message sender.
    #[must_use]
    pub const fn with_outer_from(mut self, outer_from: NodeId) -> Self {
        self.outer_from = outer_from;
        self
    }
}

fn tls_stream(
    addr: SocketAddr,
    certificate_node: NodeId,
) -> StreamOwned<ClientConnection, TcpStream> {
    let config = tls_client_config(certificate_node);
    let connection = ClientConnection::new(
        config,
        ServerName::try_from("rafter-peer".to_string()).expect("frozen TLS name is valid"),
    )
    .expect("TLS client builds");
    let socket = TcpStream::connect(addr).expect("authenticated peer TCP connects");
    let mut stream = StreamOwned::new(connection, socket);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .expect("the test TLS handshake completes");
    }
    stream
}

fn tls_client_config(node_id: NodeId) -> Arc<ClientConfig> {
    let fixtures = fixture_dir();
    let mut roots = RootCertStore::empty();
    for certificate in load_certificates(&fixtures.join("ca.pem")) {
        roots
            .add(certificate)
            .expect("test CA is a strict trust root");
    }
    let certificate = load_certificates(&fixtures.join(format!("node-{}.pem", node_id.0)));
    let key = load_private_key(&fixtures.join(format!("node-{}-key.pem", node_id.0)));
    let provider = Arc::new(ring::default_provider());
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("ring supports TLS 1.3")
        .with_root_certificates(roots)
        .with_client_auth_cert(certificate, key)
        .expect("test client certificate and key agree");
    config.alpn_protocols = vec![TLS_ALPN_PROTOCOL.to_vec()];
    config.resumption = Resumption::disabled();
    config.enable_early_data = false;
    Arc::new(config)
}

fn load_certificates(path: &Path) -> Vec<CertificateDer<'static>> {
    let file = File::open(path).expect("test certificate file opens");
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<_, _>>()
        .expect("test certificates parse")
}

fn load_private_key(path: &Path) -> PrivateKeyDer<'static> {
    let file = File::open(path).expect("test private key file opens");
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .expect("test private key parses")
        .expect("test private key exists")
}

fn encode_frozen_client_hello(output: &mut Vec<u8>, claimed_node: NodeId, session: u64) {
    output.clear();
    output.extend_from_slice(HANDSHAKE_MAGIC);
    for version in [
        TRANSPORT_VERSION,
        TRANSPORT_VERSION,
        PEER_CODEC_VERSION,
        PEER_CODEC_VERSION,
    ] {
        output.extend_from_slice(&version.to_be_bytes());
    }
    put_identity(output, CLUSTER_ID);
    put_identity(output, &format!("lock-node-{}", claimed_node.0));
    output.extend_from_slice(&session.to_be_bytes());
    output.extend_from_slice(&MAX_FRAME_BYTES.to_be_bytes());
}

fn encode_frozen_peer_frame(
    output: &mut Vec<u8>,
    sequence: u64,
    attempt: SequenceAttempt,
    message: &[u8],
) {
    let message_len = u32::try_from(message.len()).expect("frozen peer message fits u32");
    let body_len = 31_u32
        .checked_add(8)
        .and_then(|length| length.checked_add(message_len))
        .expect("frozen peer frame fits u32");
    output.clear();
    output.extend_from_slice(&body_len.to_be_bytes());
    output.push(1);
    output.extend_from_slice(&sequence.to_be_bytes());
    output.extend_from_slice(&8_u16.to_be_bytes());
    output.extend_from_slice(&GROUP_ID.to_be_bytes());
    output.extend_from_slice(&attempt.outer_from.0.to_be_bytes());
    output.extend_from_slice(&attempt.to.0.to_be_bytes());
    output.extend_from_slice(&message_len.to_be_bytes());
    output.extend_from_slice(message);
}

fn put_identity(output: &mut Vec<u8>, value: &str) {
    output.push(u8::try_from(value.len()).expect("frozen identity fits u8"));
    output.extend_from_slice(value.as_bytes());
}

fn read_frozen_server_hello(reader: &mut impl Read, expected_node: NodeId) -> FrozenServerHello {
    let mut encoded = Vec::new();
    read_append(reader, &mut encoded, 14);
    let cluster = read_identity(reader, &mut encoded);
    let peer = read_identity(reader, &mut encoded);
    read_append(reader, &mut encoded, 5);
    assert_eq!(&encoded[..10], HANDSHAKE_MAGIC);
    assert_eq!(cluster, CLUSTER_ID);
    assert_eq!(peer, format!("lock-node-{}", expected_node.0));

    let transport = u16::from_be_bytes(encoded[10..12].try_into().expect("transport version"));
    let codec = u16::from_be_bytes(encoded[12..14].try_into().expect("peer codec version"));
    let tail = encoded.len() - 5;
    let frame_bytes = u32::from_be_bytes(
        encoded[tail..tail + 4]
            .try_into()
            .expect("accepted frame bound"),
    );
    let status = match encoded[tail + 4] {
        0 => {
            assert_eq!(transport, TRANSPORT_VERSION);
            assert_eq!(codec, PEER_CODEC_VERSION);
            assert!(frame_bytes != 0 && frame_bytes <= MAX_FRAME_BYTES);
            ServerHelloStatus::Accepted
        }
        tag => {
            assert_eq!((transport, codec, frame_bytes), (0, 0, 0));
            ServerHelloStatus::Refused(match tag {
                1 => ServerRefusal::UnknownCertificate,
                2 => ServerRefusal::IdentityMismatch,
                3 => ServerRefusal::ClusterMismatch,
                4 => ServerRefusal::TransportVersionMismatch,
                5 => ServerRefusal::PeerCodecVersionMismatch,
                6 => ServerRefusal::FrameLimitRejected,
                7 => ServerRefusal::StaleSession,
                8 => ServerRefusal::ServerBusy,
                other => panic!("frozen client received unknown refusal tag {other}"),
            })
        }
    };
    FrozenServerHello { status }
}

fn read_identity(reader: &mut impl Read, output: &mut Vec<u8>) -> String {
    let start = output.len();
    read_append(reader, output, 1);
    let length = usize::from(output[start]);
    assert!(length <= MAX_ID_BYTES);
    read_append(reader, output, length);
    std::str::from_utf8(&output[start + 1..])
        .expect("frozen server identity is UTF-8")
        .to_string()
}

fn read_append(reader: &mut impl Read, output: &mut Vec<u8>, length: usize) {
    let start = output.len();
    output.resize(start + length, 0);
    reader
        .read_exact(&mut output[start..])
        .expect("server hello bytes arrive");
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("production-tls")
}
