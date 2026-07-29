use std::num::NonZeroUsize;

use rafter_app::group::GroupInput;
use rafter_multiraft::managed::{AdmissionRejection as ManagedRejection, ManagedConfig};
use rafter_reference_sharded_counter::{
    adapter::{
        AdapterError, CounterAdmissionRejection, CounterApplyResult, ManagedCounterCluster,
        NetworkConfig, ProposalReceipt, ReplicatedCounterCommand,
    },
    AdmissionOutcome, ClientId, CounterCommand, CounterResult, Delta, GroupId, GroupIncarnation,
    RequestFingerprint, RequestIdentity, Sequence, SessionEpoch, SystemClass,
};

mod support;
use support::{add, config, faulty, read, Recorder};

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test bounds are nonzero")
}

fn request(client: u32, sequence: u64, command: CounterCommand) -> RequestIdentity {
    RequestIdentity {
        client_id: ClientId::new(client),
        session_epoch: SessionEpoch::new(1).expect("test epoch is nonzero"),
        sequence: Sequence::new(sequence).expect("test sequence is nonzero"),
        fingerprint: RequestFingerprint::of(&command),
    }
}

fn result(cluster: &ManagedCounterCluster, receipt: ProposalReceipt) -> CounterResult {
    let completed = cluster.completed(receipt.proposal_id).unwrap_or_else(|| {
        panic!(
            "accepted healthy proposal {:?} reaches committed apply; completed={:?}",
            receipt.proposal_id,
            cluster.completed_proposals().collect::<Vec<_>>()
        )
    });
    let CounterApplyResult::Counter(result) = completed else {
        panic!("counter proposal returns a counter result");
    };
    result
}

struct Scenario {
    cluster: ManagedCounterCluster,
    oracle: Recorder,
    groups: [GroupId; 3],
}

impl Scenario {
    fn new() -> Self {
        let managed = ManagedConfig::new(nonzero(2), nonzero(2), nonzero(6), nonzero(1))
            .expect("managed bounds are valid");
        let network = NetworkConfig {
            max_pending_messages: nonzero(128),
            max_sessions_per_group: nonzero(8),
        };
        let mut cluster = ManagedCounterCluster::new(managed, network);
        let groups = [GroupId::new(0), GroupId::new(1), GroupId::new(2)];
        for group_id in groups {
            cluster
                .register_group(group_id, nonzero(1))
                .expect("real three-node group registers");
            cluster
                .recover_group(group_id)
                .expect("group enters recovery");
        }
        let election = cluster
            .drive_until_idle(128)
            .expect("deterministic election network quiesces");
        assert!(election.plans.iter().any(|plan| plan == &groups));
        let mut oracle = Recorder::new(config(3, 2, 8, 2, 6));
        for group_id in groups {
            cluster
                .serve_group(group_id)
                .expect("recovered group serves");
            oracle.open_group(group_id, 1);
        }
        Self {
            cluster,
            oracle,
            groups,
        }
    }

    fn establish_sessions(&mut self) {
        for (group_id, client_id) in [
            (self.groups[0], ClientId::new(0)),
            (self.groups[0], ClientId::new(1)),
            (self.groups[0], ClientId::new(2)),
            (self.groups[1], ClientId::new(0)),
            (self.groups[2], ClientId::new(0)),
        ] {
            let epoch = SessionEpoch::new(1).expect("test epoch is nonzero");
            self.cluster
                .open_session(group_id, client_id, epoch)
                .expect("replicated session queues");
            self.oracle
                .open_session(group_id, GroupIncarnation::first(), client_id, epoch);
            self.cluster
                .drive_until_idle(128)
                .expect("replicated session commits");
        }
    }

    fn admit_first_work(&mut self) -> [ProposalReceipt; 4] {
        let adds = [5_i64, 7, 11].map(|value| CounterCommand::Add {
            delta: Delta::new(value).expect("delta is nonzero"),
        });
        let accepted = [
            self.cluster
                .submit(self.groups[0], request(0, 1, adds[0]), adds[0])
                .expect("first group command queues"),
            self.cluster
                .submit(
                    self.groups[0],
                    request(1, 1, CounterCommand::Read),
                    CounterCommand::Read,
                )
                .expect("second group-zero slot queues"),
            self.cluster
                .submit(self.groups[1], request(0, 1, adds[1]), adds[1])
                .expect("group-one command queues"),
            self.cluster
                .submit(self.groups[2], request(0, 1, adds[2]), adds[2])
                .expect("group-two command queues"),
        ];
        for (group, work) in [
            (self.groups[0], add(0, 1, 1, 5, 1)),
            (self.groups[0], read(1, 1, 1, 1)),
            (self.groups[1], add(0, 1, 1, 7, 1)),
            (self.groups[2], add(0, 1, 1, 11, 1)),
        ] {
            assert!(matches!(
                self.oracle.submit(group, GroupIncarnation::first(), work),
                AdmissionOutcome::Queued { .. }
            ));
        }
        accepted
    }

    fn assert_overflow_is_lossless(&mut self) {
        let command = CounterCommand::Add {
            delta: Delta::new(99).expect("delta is nonzero"),
        };
        let rejected = self
            .cluster
            .submit(self.groups[0], request(2, 1, command), command)
            .expect_err("the per-group queue fails closed");
        assert!(matches!(
            rejected.reason,
            CounterAdmissionRejection::Managed(ManagedRejection::GroupQueueFull {
                group_id,
                bound: 2
            }) if group_id == self.groups[0]
        ));
        let GroupInput::Proposal { proposal } = rejected.input else {
            panic!("rejection returns the proposal input");
        };
        assert_eq!(
            proposal.command,
            ReplicatedCounterCommand::Counter {
                request: request(2, 1, command),
                command,
            }
        );
        assert!(matches!(
            self.oracle.submit(
                self.groups[0],
                GroupIncarnation::first(),
                add(2, 1, 1, 99, 1)
            ),
            AdmissionOutcome::Rejected(
                rafter_reference_sharded_counter::AdmissionRejection::GroupQueueFull { .. }
            )
        ));
    }

    fn finish_first_work(&mut self, accepted: [ProposalReceipt; 4]) {
        let report = self
            .cluster
            .drive_until_idle(256)
            .expect("accepted counter work commits and applies");
        assert_eq!(report.plans.first(), Some(&self.groups.to_vec()));
        for (receipt, expected) in accepted.into_iter().zip([
            CounterResult::Added { value: 5 },
            CounterResult::Value { value: 5 },
            CounterResult::Added { value: 7 },
            CounterResult::Added { value: 11 },
        ]) {
            assert_eq!(result(&self.cluster, receipt), expected);
        }
        self.oracle.run(16);
        self.oracle
            .assert_agreement(&"first real-adapter comparison");
        for expected in [
            (self.groups[0], CounterResult::Added { value: 5 }),
            (self.groups[0], CounterResult::Value { value: 5 }),
            (self.groups[1], CounterResult::Added { value: 7 }),
            (self.groups[2], CounterResult::Added { value: 11 }),
        ] {
            assert!(self.oracle.services().iter().any(|service| {
                (service.group, service.result) == (expected.0, Some(expected.1))
            }));
        }
    }

    fn isolate_poison(&mut self) {
        let read_zero = self
            .cluster
            .submit(
                self.groups[0],
                request(0, 2, CounterCommand::Read),
                CounterCommand::Read,
            )
            .expect("healthy group-zero read queues");
        let fault = self
            .cluster
            .submit_fault(self.groups[1], SystemClass::Control)
            .expect("fault injection is ordinary accepted consumer work");
        let read_two = self
            .cluster
            .submit(
                self.groups[2],
                request(0, 2, CounterCommand::Read),
                CounterCommand::Read,
            )
            .expect("healthy group-two read queues");
        for (group, work) in [
            (self.groups[0], read(0, 1, 2, 1)),
            (self.groups[1], faulty(SystemClass::Control, 1)),
            (self.groups[2], read(0, 1, 2, 1)),
        ] {
            self.oracle.submit(group, GroupIncarnation::first(), work);
        }
        let report = self
            .cluster
            .drive_until_idle(256)
            .expect("one poison cannot stop later groups");
        assert!(report.failed >= 1);
        assert!(self.cluster.is_poisoned(self.groups[1]));
        assert!(self.cluster.completed(fault.proposal_id).is_none());
        assert_eq!(
            result(&self.cluster, read_zero),
            CounterResult::Value { value: 5 }
        );
        assert_eq!(
            result(&self.cluster, read_two),
            CounterResult::Value { value: 11 }
        );
        self.oracle.run(16);
        self.oracle
            .assert_agreement(&"failure-isolation comparison");
        let metrics = self.cluster.metrics();
        assert_eq!(metrics.queued + metrics.in_flight_work, 0);
        assert_eq!(metrics.admitted, metrics.serviced + metrics.failed);
        assert!(metrics.passes_completed > 0);
    }
}

#[test]
fn three_real_groups_match_the_oracle_and_isolate_one_poison() {
    let mut scenario = Scenario::new();
    scenario.establish_sessions();
    let accepted = scenario.admit_first_work();
    scenario.assert_overflow_is_lossless();
    scenario.finish_first_work(accepted);
    scenario.isolate_poison();
}

#[test]
fn adapter_policy_keeps_commands_out_of_recovery() {
    let managed = ManagedConfig::new(nonzero(1), nonzero(2), nonzero(2), nonzero(1))
        .expect("managed bounds are valid");
    let mut cluster = ManagedCounterCluster::new(
        managed,
        NetworkConfig {
            max_pending_messages: nonzero(32),
            max_sessions_per_group: nonzero(2),
        },
    );
    let group_id = GroupId::new(0);
    cluster
        .register_group(group_id, nonzero(1))
        .expect("group registers");
    cluster
        .recover_group(group_id)
        .expect("group enters recovery");
    cluster
        .open_session(
            group_id,
            ClientId::new(0),
            SessionEpoch::new(1).expect("epoch is nonzero"),
        )
        .expect("recovery admits replicated session establishment");

    let command = CounterCommand::Read;
    let rejected = cluster
        .submit(group_id, request(0, 1, command), command)
        .expect_err("recovery refuses application work");
    assert!(matches!(
        rejected.reason,
        CounterAdmissionRejection::Lifecycle {
            state: Some(rafter_reference_sharded_counter::GroupLifecycle::Recovering)
        }
    ));
}

#[test]
fn network_overflow_returns_every_unqueued_envelope() {
    let managed = ManagedConfig::new(nonzero(1), nonzero(1), nonzero(1), nonzero(1))
        .expect("managed bounds are valid");
    let mut cluster = ManagedCounterCluster::new(
        managed,
        NetworkConfig {
            max_pending_messages: nonzero(1),
            max_sessions_per_group: nonzero(1),
        },
    );
    let group_id = GroupId::new(0);
    cluster
        .register_group(group_id, nonzero(1))
        .expect("group registers");
    cluster
        .recover_group(group_id)
        .expect("election tick queues");

    let AdapterError::NetworkFull { bound, pending } = cluster
        .drive_until_idle(1)
        .expect_err("one election tick emits more than one bounded envelope")
    else {
        panic!("the network bound is the exact refusal");
    };
    assert_eq!(bound, 1);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].group_id, group_id);
}
