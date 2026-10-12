//! Partial ciphertext cannot extend the complete TLS/Rafter handshake budget.
//!
//! Both an unauthenticated TLS dialer and a known certificate holder must
//! release their connection slot on schedule when they trickle record bytes.

mod support;

use std::{
    io::Write as _,
    net::TcpStream,
    num::NonZeroU32,
    thread,
    time::{Duration, Instant},
};

use rafter_transport_tls::{
    encode_client_hello_into, ClientHello, ClusterId, ConnectionSession, PeerId, RuntimeLimits,
    TransportIoTimeouts, TransportRuntimeTimeouts, TransportTimeouts, VersionRange,
    DEFAULT_MAX_FRAME_BYTES,
};

use support::{
    runtime::{wait_until, RuntimeFixture},
    tls::{node_a_identity, server_name},
};

#[test]
fn unauthenticated_trickle_cannot_extend_the_complete_handshake_deadline() {
    let handshake = Duration::from_millis(150);
    let timeouts = TransportTimeouts::new(
        TransportIoTimeouts::new(
            Duration::from_millis(100),
            handshake,
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
    );
    let fixture = RuntimeFixture::new(RuntimeLimits::default()).with_timeouts(timeouts);
    let runtime = fixture.start_b();
    let mut socket = TcpStream::connect(runtime.local_addr()).expect("connect raw TLS client");
    assert!(wait_until(Duration::from_secs(1), || {
        runtime.diagnostics().active_inbound_connections == 1
    }));

    let writer = thread::spawn(move || {
        let bytes = [0x16_u8, 0x03, 0x01, 0x00, 0x80];
        for index in 0..20 {
            if socket.write_all(&[bytes[index % bytes.len()]]).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(30));
        }
    });

    let started = Instant::now();
    assert!(wait_until(Duration::from_millis(450), || {
        runtime.diagnostics().active_inbound_connections == 0
    }));
    assert!(started.elapsed() < Duration::from_millis(450));

    writer.join().expect("trickle writer joins");
    runtime.join().expect("transport joins");
}

#[test]
fn authenticated_record_trickle_cannot_extend_rafter_handshake_deadline() {
    let handshake = Duration::from_millis(500);
    let timeouts = TransportTimeouts::new(
        TransportIoTimeouts::new(
            Duration::from_millis(100),
            handshake,
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
    );
    let fixture = RuntimeFixture::new(RuntimeLimits::default()).with_timeouts(timeouts);
    let runtime = fixture.start_b();
    let mut socket = TcpStream::connect(runtime.local_addr()).expect("connect TLS client");
    socket.set_nodelay(true).expect("disable Nagle buffering");
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("bound client reads");
    socket
        .set_write_timeout(Some(Duration::from_secs(2)))
        .expect("bound client writes");
    let mut client = node_a_identity()
        .client_connection(&server_name())
        .expect("client identity");
    while client.is_handshaking() {
        client
            .complete_io(&mut socket)
            .expect("mutual TLS completes");
    }
    assert!(wait_until(Duration::from_secs(1), || {
        let diagnostics = runtime.diagnostics();
        diagnostics.tls_handshakes == 1 && diagnostics.active_inbound_connections == 1
    }));

    let hello = ClientHello::new(
        VersionRange::new(1, 1).expect("valid protocol range"),
        VersionRange::new(1, 1).expect("valid codec range"),
        ClusterId::new("runtime-test").expect("fixture cluster"),
        PeerId::new("peer-a").expect("fixture peer"),
        ConnectionSession::new(1).expect("nonzero session"),
        NonZeroU32::new(u32::try_from(DEFAULT_MAX_FRAME_BYTES).expect("default fits u32"))
            .expect("nonzero frame bound"),
    );
    let mut encoded = Vec::new();
    encode_client_hello_into(&mut encoded, &hello);
    client
        .writer()
        .write_all(&encoded)
        .expect("valid client hello buffers");
    let mut record = Vec::new();
    while client.wants_write() {
        client
            .write_tls(&mut record)
            .expect("client hello encrypts");
    }
    assert!(
        record.len() > 50,
        "trickle must outlast the observation window"
    );
    // A plaintext read inside Rustls needs this entire encrypted record. Its
    // internal socket reads must consume the same complete-handshake budget.
    let writer = thread::spawn(move || {
        for byte in record {
            if socket.write_all(&[byte]).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(30));
        }
    });
    let released = wait_until(Duration::from_secs(1), || {
        runtime.diagnostics().active_inbound_connections == 0
    });
    let diagnostics = runtime.diagnostics();
    // Reap both workers even when the original implementation misses the bound.
    runtime.join().expect("transport joins");
    writer.join().expect("trickle writer joins");
    assert!(
        released,
        "encrypted-record trickle retained a connection: {diagnostics:?}"
    );
}
