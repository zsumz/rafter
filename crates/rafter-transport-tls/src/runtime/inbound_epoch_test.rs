use std::{
    net::{TcpListener, TcpStream},
    sync::Arc,
};

use super::*;

#[test]
fn an_older_delayed_epoch_cannot_replace_a_newer_live_epoch() {
    let epochs = Arc::new(InboundEpochs::default());
    let peer = PeerId::new("peer-a").expect("valid peer");
    let (newer_local, _newer_remote) = socket_pair();
    let (older_local, _older_remote) = socket_pair();

    let newer = epochs
        .install(
            peer.clone(),
            ConnectionSession::new(3).expect("session three"),
            Arc::new(newer_local),
        )
        .expect("epoch state")
        .expect("install newer epoch");
    let older = epochs
        .install(
            peer,
            ConnectionSession::new(2).expect("session two"),
            Arc::new(older_local),
        )
        .expect("epoch state");

    assert!(older.is_none());
    assert!(newer.is_current().expect("epoch state"));
}

fn socket_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let client = TcpStream::connect(address).expect("connect loopback");
    let (server, _) = listener.accept().expect("accept loopback");
    (client, server)
}
