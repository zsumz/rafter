use std::{
    fs,
    panic::{catch_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    session::{ConnectionSession, TransportSessionStore},
    ClusterId, PeerId, SessionStoreLimits,
};

use super::{
    failpoint, CreateTransportSessionStoreError, FileTransportSessionStore,
    FileTransportSessionStoreError, OpenTransportSessionStoreError,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

#[test]
fn create_failpoints_preserve_an_unambiguous_recovery_or_retry_path() {
    for (point, published) in [
        (failpoint::DurabilityPoint::CreateAfterTempSync, false),
        (failpoint::DurabilityPoint::CreateAfterRename, true),
        (failpoint::DurabilityPoint::CreateAfterDirectorySync, true),
    ] {
        let directory = TestDirectory::new("create-failpoint");
        let path = directory.path().join("transport.state");
        let cluster = cluster();
        let local = local_peer();
        let guard = failpoint::arm(point);

        assert!(matches!(
            FileTransportSessionStore::create_new(
                &path,
                cluster.clone(),
                local.clone(),
                SessionStoreLimits::default(),
            ),
            Err(CreateTransportSessionStoreError::Io { .. })
        ));
        guard.assert_triggered();

        if published {
            let reopened = FileTransportSessionStore::open_existing(&path, &cluster, &local)
                .expect("published state remains the recovery oracle");
            assert_eq!(
                reopened.snapshot().expect("healthy snapshot").peer_count(),
                0
            );
        } else {
            assert!(matches!(
                FileTransportSessionStore::open_existing(&path, &cluster, &local),
                Err(OpenTransportSessionStoreError::Missing { .. })
            ));
            FileTransportSessionStore::create_new(
                &path,
                cluster,
                local,
                SessionStoreLimits::default(),
            )
            .expect("a never-published initial state may be retried");
        }
    }
}

#[test]
fn every_ambiguous_replacement_failure_latches_until_reopen() {
    for (point, durable_session) in [
        (failpoint::DurabilityPoint::ReplaceAfterTempSync, None),
        (
            failpoint::DurabilityPoint::ReplaceAfterRename,
            Some(ConnectionSession::FIRST),
        ),
        (
            failpoint::DurabilityPoint::ReplaceAfterDirectorySync,
            Some(ConnectionSession::FIRST),
        ),
    ] {
        let directory = TestDirectory::new("replace-failpoint");
        let path = directory.path().join("transport.state");
        let cluster = cluster();
        let local = local_peer();
        let remote = remote_peer();
        let store = FileTransportSessionStore::create_new(
            &path,
            cluster.clone(),
            local.clone(),
            SessionStoreLimits::default(),
        )
        .expect("create store");
        let guard = failpoint::arm(point);

        assert!(matches!(
            store.allocate_outbound_session(&remote),
            Err(FileTransportSessionStoreError::Io { .. })
        ));
        guard.assert_triggered();
        assert!(store.requires_reopen());
        assert!(matches!(
            store.peer_session_state(&remote),
            Err(FileTransportSessionStoreError::StoreRequiresReopen)
        ));
        drop(store);

        let reopened = FileTransportSessionStore::open_existing(&path, &cluster, &local)
            .expect("reopen after ambiguous publication");
        assert_eq!(
            reopened
                .peer_session_state(&remote)
                .expect("recovered peer state")
                .highest_outbound(),
            durable_session
        );
    }
}

#[test]
fn poisoned_in_memory_state_requires_reopen_instead_of_reusing_a_session() {
    let directory = TestDirectory::new("poisoned-state");
    let path = directory.path().join("transport.state");
    let store = FileTransportSessionStore::create_new(
        &path,
        cluster(),
        local_peer(),
        SessionStoreLimits::default(),
    )
    .expect("create store");

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _inner = store.inner.lock().expect("healthy mutex");
        panic!("poison session state");
    }));
    assert!(result.is_err());
    assert!(store.requires_reopen());
    assert!(matches!(
        store.allocate_outbound_session(&remote_peer()),
        Err(FileTransportSessionStoreError::StoreRequiresReopen)
    ));
}

fn cluster() -> ClusterId {
    ClusterId::new("orders-production-us1").expect("valid cluster")
}

fn local_peer() -> PeerId {
    PeerId::new("orders-node-a").expect("valid peer")
}

fn remote_peer() -> PeerId {
    PeerId::new("orders-node-b").expect("valid peer")
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rafter-transport-tls-unit-{name}-{}-{id}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
