//! A caught application callback panic cannot resume a live snapshot handoff.
use super::{super::*, lifecycle_test::snapshotted, support::*};
use crate::application::DurableApplication;
use rafter::LogIndex;
use std::io;

#[derive(Debug, Default)]
struct PanickingFloor {
    store: Store,
    armed: bool,
}
impl DurableApplication<Entry> for PanickingFloor {
    type Outcome = <Store as DurableApplication<Entry>>::Outcome;
    type Error = io::Error;
    fn applied_through(&self) -> LogIndex {
        assert!(!self.armed, "injected snapshot floor callback panic");
        self.store.applied_through()
    }
    fn apply(&mut self, entries: &[Entry]) -> io::Result<Vec<Self::Outcome>> {
        self.store.apply(entries)
    }
}

#[test]
fn caught_snapshot_floor_callback_panic_fences_poll_input_and_resumption() {
    let mut service = DurableService::recover(
        config(),
        snapshotted().into_storage(),
        PanickingFloor::default(),
        ServiceOptions::new(),
        adapt,
    )
    .unwrap();
    let Some(ServiceEvent::InstallSnapshot {
        mut application, ..
    }) = service.poll().unwrap()
    else {
        panic!("snapshot handoff required");
    };
    application.armed = true;
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || service.resume_snapshot(application)
    ))
    .is_err());
    assert_eq!(service.state(), ServiceState::Failed);
    assert!(matches!(service.poll(), Err(ServiceError::Failed)));
    assert!(matches!(
        service.try_step(rafter::Input::Tick.into()),
        Err(ServiceStepError::Unavailable {
            state: ServiceState::Failed,
            ..
        })
    ));
    let repaired = PanickingFloor {
        store: Store {
            floor: LogIndex(4),
            ..Store::default()
        },
        armed: false,
    };
    assert!(matches!(
        service.resume_snapshot(repaired),
        Err(ServiceError::Failed)
    ));
}
