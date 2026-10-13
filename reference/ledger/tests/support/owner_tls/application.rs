//! A deterministic pause before the first real application transaction.
use super::config;
use rafter::LogIndex;
use rafter_app::state_machine::{ApplyEntry, ReplicatedStateMachine};
use rafter_reference_ledger::{
    DurableLedgerStateMachine, LedgerApplicationEntry, LedgerStateMachine,
};
use rafter_runtime::{
    application::DurableApplication,
    service::{CommittedCommand, ServiceAdapterError},
};
use std::sync::mpsc::{self, Receiver, SyncSender};

type Ledger = DurableLedgerStateMachine;
#[derive(Debug)]
pub struct Application {
    pub ledger: Ledger,
    pub gate: Option<Gate>,
}
#[derive(Debug)]
pub struct Gate {
    entered: SyncSender<()>,
    release: Receiver<()>,
}
#[derive(Debug)]
pub struct Release {
    release: Option<SyncSender<()>>,
}
impl Release {
    pub fn now(mut self) {
        self.release.take().unwrap().send(()).unwrap();
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}
pub fn gate() -> (Gate, Receiver<()>, Release) {
    let (entered, observe) = mpsc::sync_channel(1);
    let (release, wait) = mpsc::sync_channel(1);
    (
        Gate {
            entered,
            release: wait,
        },
        observe,
        Release {
            release: Some(release),
        },
    )
}
impl DurableApplication<LedgerApplicationEntry> for Application {
    type Outcome = <Ledger as DurableApplication<LedgerApplicationEntry>>::Outcome;
    type Error = <Ledger as DurableApplication<LedgerApplicationEntry>>::Error;
    fn applied_through(&self) -> LogIndex {
        self.ledger.store().applied_index()
    }
    fn apply(
        &mut self,
        entries: &[LedgerApplicationEntry],
    ) -> Result<Vec<Self::Outcome>, Self::Error> {
        if let Some(gate) = self.gate.take() {
            gate.entered.send(()).unwrap();
            gate.release.recv().unwrap();
        }
        DurableApplication::apply(&mut self.ledger, entries)
    }
}
pub fn adapt(command: CommittedCommand) -> Result<LedgerApplicationEntry, ServiceAdapterError> {
    let decoded = LedgerStateMachine::new(config()).decode_command(command.payload.as_slice())?;
    drop(command.payload);
    Ok(LedgerApplicationEntry::new(ApplyEntry {
        index: command.index,
        term: command.term,
        command: decoded,
        local_proposal_id: command.local_proposal_id,
    })
    .with_protocol_prefix(command.first_index))
}
