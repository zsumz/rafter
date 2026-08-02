use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex, PoisonError,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use rafter_transport_tls::{
    authenticate_server_connection, decode_client_hello, encode_server_hello_into,
    CertificateDirectory, ClusterId, PeerFrameCodec, PeerFrameScratch, PeerId, ServerHelloStatus,
    TlsHandshakeConfig, TrafficClass, WireLimits, HANDSHAKE_MAGIC, MAX_ID_BYTES,
    PEER_FRAME_LENGTH_PREFIX_BYTES,
};
use rustls::StreamOwned;

use crate::support::{
    session_store::MemorySessionStore,
    tls::{node_a_identity, node_b_identity},
    StringGroupCodec,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PeerBehavior {
    RejectFrameLimit = 0,
    AcceptThenClose = 1,
    CaptureFrames = 2,
}

#[derive(Debug)]
pub struct FaultPeer {
    address: SocketAddr,
    behavior: Arc<AtomicU8>,
    hellos: Arc<AtomicUsize>,
    classes: Arc<Mutex<Vec<TrafficClass>>>,
    errors: Arc<AtomicUsize>,
    last_error: Arc<Mutex<Option<String>>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl FaultPeer {
    pub fn start(initial: PeerBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fault peer");
        listener
            .set_nonblocking(true)
            .expect("configure fault peer listener");
        let address = listener.local_addr().expect("fault peer address");
        let behavior = Arc::new(AtomicU8::new(initial as u8));
        let hellos = Arc::new(AtomicUsize::new(0));
        let classes = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(AtomicUsize::new(0));
        let last_error = Arc::new(Mutex::new(None));
        let stop = Arc::new(AtomicBool::new(false));
        let worker = {
            let behavior = Arc::clone(&behavior);
            let hellos = Arc::clone(&hellos);
            let classes = Arc::clone(&classes);
            let errors = Arc::clone(&errors);
            let last_error = Arc::clone(&last_error);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                serve(
                    &listener,
                    &behavior,
                    &hellos,
                    &classes,
                    &errors,
                    &last_error,
                    &stop,
                );
            })
        };
        Self {
            address,
            behavior,
            hellos,
            classes,
            errors,
            last_error,
            stop,
            worker: Some(worker),
        }
    }

    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub fn set_behavior(&self, behavior: PeerBehavior) {
        self.behavior.store(behavior as u8, Ordering::Release);
    }

    pub fn hello_count(&self) -> usize {
        self.hellos.load(Ordering::Acquire)
    }

    pub fn captured_classes(&self) -> Vec<TrafficClass> {
        self.classes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn join(mut self) {
        self.stop_and_join();
        assert_eq!(
            self.errors.load(Ordering::Acquire),
            0,
            "fault peer failed: {:?}",
            self.last_error()
        );
    }

    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("fault peer joins");
        }
    }
}

impl Drop for FaultPeer {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

fn serve(
    listener: &TcpListener,
    behavior: &AtomicU8,
    hellos: &AtomicUsize,
    classes: &Mutex<Vec<TrafficClass>>,
    errors: &AtomicUsize,
    last_error: &Mutex<Option<String>>,
    stop: &AtomicBool,
) {
    let server_identity = node_b_identity();
    let client_identity = node_a_identity();
    let peer_a = PeerId::new("peer-a").expect("valid peer A");
    let peer_b = PeerId::new("peer-b").expect("valid peer B");
    let certificates = CertificateDirectory::builder()
        .map_fingerprint(client_identity.leaf_fingerprint(), peer_a)
        .expect("map client certificate")
        .build();
    let cluster = ClusterId::new("runtime-test").expect("valid cluster");
    let compatible =
        TlsHandshakeConfig::current(cluster.clone(), peer_b.clone(), WireLimits::default())
            .expect("compatible handshake");
    let defaults = WireLimits::default();
    let smaller = WireLimits::new(
        defaults.max_frame_body_bytes() - 1,
        defaults.max_group_id_bytes(),
    )
    .expect("smaller wire limit");
    let incompatible =
        TlsHandshakeConfig::current(cluster, peer_b, smaller).expect("incompatible handshake");
    let sessions = MemorySessionStore::new();
    let codec = PeerFrameCodec::new(StringGroupCodec::new(128), WireLimits::default())
        .expect("fault peer codec");

    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((socket, _)) => {
                let selected = PeerBehavior::from_u8(behavior.load(Ordering::Acquire));
                if let Err(error) = serve_one(
                    socket,
                    selected,
                    &server_identity,
                    &certificates,
                    &compatible,
                    &incompatible,
                    &sessions,
                    &codec,
                    hellos,
                    classes,
                ) {
                    let _ = errors.fetch_add(1, Ordering::Relaxed);
                    *last_error.lock().unwrap_or_else(PoisonError::into_inner) =
                        Some(error.to_string());
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(_) => {
                let _ = errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn serve_one(
    mut socket: TcpStream,
    behavior: PeerBehavior,
    identity: &rafter_transport_tls::TlsIdentity,
    certificates: &CertificateDirectory,
    compatible: &TlsHandshakeConfig,
    incompatible: &TlsHandshakeConfig,
    sessions: &MemorySessionStore,
    codec: &PeerFrameCodec<String, StringGroupCodec>,
    hellos: &AtomicUsize,
    classes: &Mutex<Vec<TrafficClass>>,
) -> io::Result<()> {
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    socket.set_nodelay(true)?;
    let mut connection = identity.server_connection().map_err(io::Error::other)?;
    while connection.is_handshaking() {
        connection.complete_io(&mut socket)?;
    }
    let authenticated =
        authenticate_server_connection(&connection, certificates).map_err(io::Error::other)?;
    let mut stream = StreamOwned::new(connection, socket);
    let hello = read_client_hello(&mut stream)?;
    let handshake = match behavior {
        PeerBehavior::RejectFrameLimit => incompatible,
        PeerBehavior::AcceptThenClose | PeerBehavior::CaptureFrames => compatible,
    };
    let response = handshake
        .accept_client_hello(&authenticated, &hello, sessions)
        .map_err(io::Error::other)?;
    let accepted = response.status() == ServerHelloStatus::Accepted;
    let mut encoded = Vec::new();
    encode_server_hello_into(&mut encoded, &response);
    stream.write_all(&encoded)?;
    stream.flush()?;
    let _ = hellos.fetch_add(1, Ordering::Release);

    match behavior {
        PeerBehavior::RejectFrameLimit => {
            debug_assert!(!accepted);
            Ok(())
        }
        PeerBehavior::AcceptThenClose => {
            debug_assert!(accepted);
            stream.conn.send_close_notify();
            while stream.conn.wants_write() {
                stream.conn.write_tls(&mut stream.sock)?;
            }
            stream.sock.shutdown(Shutdown::Both)
        }
        PeerBehavior::CaptureFrames => {
            debug_assert!(accepted);
            capture_frames(&mut stream, codec, classes)
        }
    }
}

fn read_client_hello(reader: &mut impl Read) -> io::Result<rafter_transport_tls::ClientHello> {
    let mut encoded = Vec::new();
    read_append(reader, &mut encoded, HANDSHAKE_MAGIC.len() + 8)?;
    read_identity(reader, &mut encoded)?;
    read_identity(reader, &mut encoded)?;
    read_append(reader, &mut encoded, 12)?;
    decode_client_hello(&encoded).map_err(io::Error::other)
}

fn read_identity(reader: &mut impl Read, encoded: &mut Vec<u8>) -> io::Result<()> {
    let start = encoded.len();
    read_append(reader, encoded, 1)?;
    let length = usize::from(encoded[start]);
    if length > MAX_ID_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "handshake identity exceeds bound",
        ));
    }
    read_append(reader, encoded, length)
}

fn read_append(reader: &mut impl Read, encoded: &mut Vec<u8>, length: usize) -> io::Result<()> {
    let start = encoded.len();
    let end = start
        .checked_add(length)
        .ok_or_else(|| io::Error::other("handshake length overflow"))?;
    encoded.resize(end, 0);
    reader.read_exact(&mut encoded[start..])
}

fn capture_frames(
    reader: &mut impl Read,
    codec: &PeerFrameCodec<String, StringGroupCodec>,
    classes: &Mutex<Vec<TrafficClass>>,
) -> io::Result<()> {
    let mut scratch = PeerFrameScratch::new();
    loop {
        let mut prefix = [0_u8; PEER_FRAME_LENGTH_PREFIX_BYTES];
        match reader.read_exact(&mut prefix) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::WouldBlock
                ) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        let body = usize::try_from(u32::from_be_bytes(prefix))
            .map_err(|_| io::Error::other("frame length does not fit target"))?;
        let complete = body
            .checked_add(PEER_FRAME_LENGTH_PREFIX_BYTES)
            .ok_or_else(|| io::Error::other("frame length overflow"))?;
        let mut frame = Vec::with_capacity(complete);
        frame.extend_from_slice(&prefix);
        frame.resize(complete, 0);
        reader.read_exact(&mut frame[PEER_FRAME_LENGTH_PREFIX_BYTES..])?;
        let decoded = codec
            .decode(&frame, &mut scratch)
            .map_err(io::Error::other)?;
        classes
            .lock()
            .map_err(|_| io::Error::other("captured frame state is poisoned"))?
            .push(TrafficClass::for_message(decoded.message()));
    }
}

impl PeerBehavior {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::AcceptThenClose,
            2 => Self::CaptureFrames,
            _ => Self::RejectFrameLimit,
        }
    }
}
