//! Real stores and workers with separately armed persistence/adapter faults.
pub(super) use crate::write_support::{poll, Context, Poll};
use crate::write_support::{App, Entry, Outcome};
pub(super) use rafter::{Input, LocalProposalId, LogIndex, NodeConfig, NodeId};
use rafter_app::proposal::ClientRequestId;
pub(super) use rafter_app::read::ReadConsistency;
pub(super) use rafter_runtime::service::{ServiceState, ServiceStepError};
use rafter_runtime::{
    service::{ServiceOptions, ServicePoll},
    DurableRaftNodeStorage,
};
pub(super) use rafter_service::durable::DurableClientStepError;
use rafter_service::durable::{
    DurableClientOptions, DurableClientOwner, DurableReadLimits, DurableWriteLimits,
};
pub(super) use rafter_service::{
    DriverFuture, ReadError, ReadOptions, WriteError, WriteOptions, WriteReceipt,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore, RaftHardState,
    RaftHardStateStore, RaftHardStateStoreWriteError,
};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Wake, Waker},
    time::{Duration, Instant},
};

type Owner = DurableClientOwner<
    HardState,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
    Entry,
    App,
    u8,
>;
#[derive(Default)]
pub(super) struct Fault {
    armed: AtomicBool,
    hits: AtomicUsize,
}
impl Fault {
    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
    pub fn hits(&self) -> usize {
        self.hits.load(Ordering::Acquire)
    }
    fn trip(&self) {
        if self.armed.load(Ordering::Acquire) {
            self.hits.fetch_add(1, Ordering::AcqRel);
            panic!("injected backend callback panic");
        }
    }
}
pub(super) struct HardState {
    inner: InMemoryRaftHardStateStore,
    fault: Arc<Fault>,
    dropped: Arc<AtomicBool>,
}
impl RaftHardStateStore for HardState {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        self.fault.trip();
        self.inner.write_hard_state(state)
    }
    fn current(&self) -> RaftHardState {
        self.inner.current()
    }
}
impl Drop for HardState {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}
pub(super) struct Fixture {
    pub owner: Owner,
    pub storage_fault: Arc<Fault>,
    pub adapter_fault: Arc<Fault>,
    storage_dropped: Arc<AtomicBool>,
    app_dropped: Arc<AtomicBool>,
}
impl Fixture {
    pub fn new(elect: bool) -> Self {
        let storage_fault = Arc::new(Fault::default());
        let adapter_fault = Arc::new(Fault::default());
        let storage_dropped = Arc::new(AtomicBool::new(false));
        let app_dropped = Arc::new(AtomicBool::new(false));
        let decode_fault = Arc::clone(&adapter_fault);
        let limit = NonZeroUsize::new(4).unwrap();
        let bytes = NonZeroUsize::new(64).unwrap();
        let mut owner = Owner::recover_with_reads(
            NodeConfig::new(NodeId(1), vec![], 1).unwrap(),
            DurableRaftNodeStorage {
                hard_state_store: HardState {
                    inner: InMemoryRaftHardStateStore::new(),
                    fault: Arc::clone(&storage_fault),
                    dropped: Arc::clone(&storage_dropped),
                },
                log_segment: InMemoryRaftLogSegment::new(),
                snapshot_store: InMemoryRaftSnapshotStore::new(),
            },
            App {
                floor: LogIndex::ZERO,
                value: 0,
                gate: None,
                dropped: Arc::clone(&app_dropped),
            },
            DurableClientOptions::new(
                ServiceOptions::new(),
                DurableWriteLimits {
                    writes: limit,
                    payload_bytes: bytes,
                },
                DurableReadLimits {
                    reads: limit,
                    payload_bytes: bytes,
                },
            ),
            move |command| {
                decode_fault.trip();
                Ok(Entry {
                    first: command.first_index,
                    index: command.index,
                    value: command.payload[0],
                })
            },
            |app, _| app.value,
            || {},
        )
        .unwrap();
        let deadline = deadline();
        while owner.state() != ServiceState::Ready {
            let _ = owner.poll_next().unwrap();
            assert!(Instant::now() < deadline);
        }
        if elect {
            owner.try_step(Input::Tick).unwrap();
            while !matches!(owner.poll_next().unwrap(), ServicePoll::Pending) {
                assert!(Instant::now() < deadline);
            }
        }
        Self {
            owner,
            storage_fault,
            adapter_fault,
            storage_dropped,
            app_dropped,
        }
    }
    pub fn identified_write(&self) -> DriverFuture<Result<WriteReceipt<Outcome>, WriteError>> {
        self.owner.sender().write(
            vec![7],
            WriteOptions::default().with_client_request_id(identity()),
        )
    }
    pub fn catch_poll_panic(&mut self) {
        let deadline = deadline();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loop {
                let _ = self.owner.poll_next().unwrap();
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }))
            .is_err()
        );
    }
    pub fn assert_fenced(&mut self) {
        assert_eq!(self.owner.state(), ServiceState::Failed);
        assert!(matches!(
            poll(&mut self.owner.sender().write(vec![8], WriteOptions::default())),
            Poll::Ready(Err(WriteError::ShuttingDown))
        ));
        assert!(matches!(
            poll(&mut self.owner.read_sender().unwrap().read(
                vec![0],
                ReadConsistency::Local,
                ReadOptions::default()
            )),
            Poll::Ready(Err(ReadError::ShuttingDown))
        ));
        assert!(self.owner.poll_next().is_err());
        assert!(self.owner.try_step(Input::Tick).is_err());
    }
    pub fn released_waker(&self) -> Waker {
        Waker::from(Arc::new(Released {
            storage: Arc::clone(&self.storage_dropped),
            application: Arc::clone(&self.app_dropped),
        }))
    }
}
struct Released {
    storage: Arc<AtomicBool>,
    application: Arc<AtomicBool>,
}
impl Wake for Released {
    fn wake(self: Arc<Self>) {
        assert!(
            self.storage.load(Ordering::Acquire),
            "published before storage release"
        );
        assert!(
            self.application.load(Ordering::Acquire),
            "published before application release"
        );
    }
}
fn identity() -> ClientRequestId {
    ClientRequestId {
        client_id: 11,
        sequence: 12,
    }
}
pub(super) fn assert_unknown(future: &mut DriverFuture<Result<WriteReceipt<Outcome>, WriteError>>) {
    assert!(
        matches!(poll(future),Poll::Ready(Err(WriteError::UnknownOutcome {local_proposal_id:LocalProposalId(1),client_request_id:Some(id),..})) if id==identity())
    );
}
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
