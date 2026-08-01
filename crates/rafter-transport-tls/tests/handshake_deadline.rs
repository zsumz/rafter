mod support;

use std::{
    io::Write as _,
    net::TcpStream,
    thread,
    time::{Duration, Instant},
};

use rafter_transport_tls::{
    RuntimeLimits, TransportIoTimeouts, TransportRuntimeTimeouts, TransportTimeouts,
};

use support::runtime::{wait_until, RuntimeFixture};

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
