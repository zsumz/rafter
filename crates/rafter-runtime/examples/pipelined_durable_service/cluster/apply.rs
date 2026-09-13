//! Application completion, draining, and snapshot replacement.

use std::collections::BTreeMap;

use rafter::{LogIndex, NodeId};
use rafter_runtime::application::ApplicationEvent;

use super::super::application::{self, AppliedCommand};
use super::{node_ids, Cluster};

impl Cluster {
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
        let mut worker = replica
            .application
            .take()
            .expect("application worker is running");
        worker.shutdown().expect("idle application worker stops");
        application::install_snapshot(&replica.directory, &replica.state, kv, applied);
        let (state, worker) = application::open(&self.root, node_id);
        replica.state = state;
        replica.application = Some(worker);
        replica.dispatched = applied;
    }
}
