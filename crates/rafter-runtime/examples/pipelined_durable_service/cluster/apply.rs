//! Application completion, draining, and snapshot replacement.

use std::collections::{BTreeMap, VecDeque};

use rafter::{LogIndex, NodeId};
use rafter_runtime::application::{ApplicationEvent, ApplicationSubmitRejection};

use super::super::application::{self, AppliedCommand};
use super::{node_ids, Cluster};

impl Cluster {
    pub(super) fn dispatch_application(
        &mut self,
        node_id: NodeId,
        mut entries: VecDeque<AppliedCommand>,
    ) {
        while !entries.is_empty() {
            let worker = self
                .replicas
                .get(&node_id)
                .expect("replica exists")
                .application
                .as_ref()
                .expect("application worker is running");
            match worker.try_submit_prefix(&mut entries) {
                Ok(_) => {}
                Err(ApplicationSubmitRejection::Full { .. }) if worker.is_busy() => {
                    // The library admission call never blocks. This reference
                    // scheduler waits for one completion, consumes its client
                    // outcomes, and retries the unchanged owned remainder.
                    let event = worker.complete().expect("application worker remains live");
                    self.handle_application_event(event);
                }
                Err(reason) => panic!("committed application dispatch was refused: {reason:?}"),
            }
        }
    }

    pub(super) fn poll_applications(&mut self) {
        for node_id in node_ids() {
            loop {
                let event = self
                    .replicas
                    .get(&node_id)
                    .expect("replica exists")
                    .application
                    .as_ref()
                    .expect("application worker is running")
                    .try_complete()
                    .expect("application worker remains live");
                let Some(event) = event else {
                    break;
                };
                self.handle_application_event(event);
            }
        }
    }

    pub(super) fn drain_application(&mut self, node_id: NodeId) {
        loop {
            let worker = self
                .replicas
                .get(&node_id)
                .expect("replica exists")
                .application
                .as_ref()
                .expect("application worker is running");
            if !worker.is_busy() {
                return;
            }
            let event = worker.complete().expect("application worker remains live");
            self.handle_application_event(event);
        }
    }

    fn handle_application_event(
        &mut self,
        event: ApplicationEvent<AppliedCommand, application::AppliedOutcome, std::io::Error>,
    ) {
        match event {
            ApplicationEvent::Applied(completion) => {
                let (_, outcomes) = completion.into_parts();
                for outcome in outcomes {
                    if let Some(proposal_id) = outcome.proposal_id {
                        // This is the client ACK boundary: application bytes
                        // and applied floor are durable before completion.
                        self.completed.insert(proposal_id);
                    }
                }
            }
            ApplicationEvent::Failed(failure) => {
                panic!("durable application worker failed: {:?}", failure.kind())
            }
            _ => panic!("unsupported application worker event"),
        }
    }

    pub(super) fn install_application_snapshot(
        &mut self,
        node_id: NodeId,
        kv: BTreeMap<String, String>,
        applied: LogIndex,
    ) {
        let replica = self.replicas.get_mut(&node_id).expect("replica exists");
        let worker = replica
            .application
            .as_mut()
            .expect("application worker is running");
        replica.state = application::install_snapshot(worker, kv, applied);
        replica.dispatched = applied;
    }
}
