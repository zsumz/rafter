//! Guards that the production fixture composes the public TLS transport.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::NodeId;
use rafter_reference_fenced_lock::production::{
    allocate_replica, open_transport_state, transport_cluster_id, transport_peer_id,
    transport_session_path, AllocationCrashPoint,
};
use rafter_transport_tls::FileTransportSessionStore;

static NEXT_SCRATCH: AtomicU64 = AtomicU64::new(1);

#[test]
fn allocation_provisions_public_transport_session_state() {
    let scratch = Scratch::new("session-state");
    let identity = allocate_replica(scratch.path(), 1, AllocationCrashPoint::None)
        .expect("replica allocation succeeds");
    assert_eq!(identity.node_id, NodeId(1));

    let store = FileTransportSessionStore::open_existing(
        transport_session_path(&scratch.path().join("node-1")),
        &transport_cluster_id(1).expect("fixture cluster identity"),
        &transport_peer_id(NodeId(1)).expect("fixture peer identity"),
    )
    .expect("the public transport state opens under the allocated identity");
    let state = store.snapshot().expect("fresh session state is readable");
    assert_eq!(state.peer_count(), 0);
}

#[test]
fn missing_or_corrupt_transport_state_is_never_recreated() {
    let scratch = Scratch::new("fail-closed-state");
    let identity = allocate_replica(scratch.path(), 1, AllocationCrashPoint::None)
        .expect("replica allocation succeeds");
    let node_dir = scratch.path().join(format!("node-{}", identity.node_id.0));
    let state_path = transport_session_path(&node_dir);

    std::fs::remove_file(&state_path).expect("the negative test removes transport state");
    assert!(
        open_transport_state(&node_dir, 1, identity.node_id).is_err(),
        "missing state is refused rather than recreated"
    );

    std::fs::write(&state_path, b"corrupt transport state\n")
        .expect("the negative test writes corrupt state");
    assert!(
        open_transport_state(&node_dir, 1, identity.node_id).is_err(),
        "corrupt state is refused rather than replaced"
    );
}

#[test]
fn production_link_contains_only_fixture_policy_and_discovery() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let old_link = root.join("src/bin/lock-production-node/peer_link.rs");
    let old_replay = root.join("src/production/replay.rs");
    assert!(
        !old_link.exists(),
        "the duplicate monolithic TLS link was removed"
    );
    assert!(
        !old_replay.exists(),
        "the duplicate replay store was removed"
    );

    let link = read_rust_tree(&root.join("src/bin/lock-production-node/peer_link"));
    assert!(
        link.contains("rafter_transport_tls"),
        "the fixture adapter must compose the public transport crate"
    );
    assert!(
        link.contains("bind_paused") && link.contains("pub fn start"),
        "network and session workers must remain paused through replica recovery"
    );
    for forbidden in [
        "TcpListener",
        "TcpStream",
        "ClientConnection",
        "ServerConnection",
        "TransportReplayStore",
        "read_outer_frame",
        "write_outer_frame",
    ] {
        assert!(
            !link.contains(forbidden),
            "fixture link reintroduced public transport mechanism {forbidden}"
        );
    }
}

#[test]
fn production_process_activates_and_stops_transport_inside_replica_ownership() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let main = std::fs::read_to_string(root.join("src/bin/lock-production-node/main.rs"))
        .expect("production process source is readable");

    assert!(
        main.contains("link.start(node_dir, config.node_id)"),
        "the public runtime activates only after replica recovery"
    );
    assert!(
        main.contains("LinkShutdownGuard")
            && main.contains("let _link_shutdown = LinkShutdownGuard(link)"),
        "transport workers must join before replica directory ownership drops"
    );
}

fn read_rust_tree(root: &Path) -> String {
    let mut paths = std::fs::read_dir(root)
        .expect("the public transport adapter directory exists")
        .map(|entry| entry.expect("adapter directory entry is readable").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect::<Vec<_>>();
    paths.sort();

    let mut source = String::new();
    for path in paths {
        source.push_str(
            &std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display())),
        );
    }
    source
}

struct Scratch {
    path: PathBuf,
}

impl Scratch {
    fn new(label: &str) -> Self {
        let sequence = NEXT_SCRATCH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rafter-fenced-lock-transport-{label}-{}-{sequence}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("scratch directory opens");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
