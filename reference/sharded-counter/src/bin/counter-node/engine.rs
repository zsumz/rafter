//! Bounded process loop that composes durable groups through the managed host.

mod admission;
mod audit;
mod durability;
mod failure;
mod report;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::TcpListener,
    num::NonZeroUsize,
    path::PathBuf,
    sync::mpsc::{self, Receiver, TryRecvError},
    time::{Duration, Instant},
};

use rafter::{LocalProposalId, NodeId, ReadId, Role};
use rafter_app::{group::GroupInput, proposal::ProposalEvent};
use rafter_multiraft::{
    managed::{
        ArmPass, BeginDispatch, Dispatch, ManagedConfig, ManagedTypedMultiRaftHost, WorkClass,
        WorkId,
    },
    MultiRaftErrorKind,
};
use rafter_reference_sharded_counter::{
    adapter::{
        CounterApplyRejection, CounterApplyResult, ReplicatedCounterCommand, SessionApplyResult,
    },
    ClientId, CounterRejection, CounterResult, GroupId, GroupIncarnation, GroupLifecycle,
    RequestFingerprint, RequestIdentity, WorkQuota,
};

use self::{
    admission::PendingAdmission,
    audit::Audit,
    durability::{
        activate_staged_raft, archive_raft_with_failpoints, prepare_staged_raft, slot_from_policy,
    },
    failure::{driver_application_durability_failed, managed_application_durability_failed},
    report::{take_ordered_consumer_events, ConsumerReportEvent},
};
use super::{
    app_store::{AcceptedOperation, ApplicationRecord, TerminalFailure},
    group::{OpenError, OpenedGroup, Report, SharedGroup},
    host_registry::{ActivationIntent, HostRegistry, RetirementIntent},
    peer_link::{PeerFrame, PeerLink},
    protocol::{self, ClientReply, Job, PressureClass, Request},
    Config,
};
use crate::directed_failpoint;

const MAX_CLIENT_JOBS: usize = 1024;
const MAX_JOBS_PER_LOOP: usize = 64;
const MAX_PEERS_PER_LOOP: usize = 512;
const MAX_DISPATCHES_PER_LOOP: usize = 512;
const LOOP_POLL: Duration = Duration::from_millis(2);
const MAX_SLOW_DELAY_MS: u64 = 30_000;
const RECOVERY_RETRY_DELAY: Duration = Duration::from_secs(2);

type Host = ManagedTypedMultiRaftHost<GroupId, ReplicatedCounterCommand, CounterApplyResult>;
type CounterDispatch = Dispatch<GroupId, GroupInput<GroupId, ReplicatedCounterCommand>>;

#[derive(Debug)]
struct GroupEntry {
    directory: PathBuf,
    record: ApplicationRecord,
    driver: Option<SharedGroup>,
}

#[derive(Debug)]
enum WorkKind {
    Tick,
    Peer,
    Pressure,
    Proposal(LocalProposalId),
    Snapshot(ClientReply),
}

#[derive(Debug)]
struct PendingClient {
    group_id: GroupId,
    operation: Option<AcceptedOperation>,
    replies: Vec<ClientReply>,
    deadline: Option<Instant>,
    recovered: bool,
}

#[derive(Debug)]
struct ClientAdmissionRefusal {
    replies: Vec<ClientReply>,
    response: String,
    managed: bool,
}

#[derive(Debug)]
struct DelayedDispatch {
    ready_at: Instant,
    dispatch: CounterDispatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryMode {
    Running,
    Paused,
}

#[derive(Debug)]
struct Engine {
    node_id: NodeId,
    members: Vec<NodeId>,
    group_count: u32,
    max_sessions: usize,
    default_quota: WorkQuota,
    election_timeout_ticks: u64,
    request_timeout: Duration,
    tick_interval: Duration,
    worker_capacity: usize,
    max_group_queue: usize,
    registry: Option<HostRegistry>,
    host: Host,
    groups: BTreeMap<GroupId, GroupEntry>,
    link: PeerLink,
    work: BTreeMap<WorkId, WorkKind>,
    pending: BTreeMap<LocalProposalId, PendingClient>,
    pending_operations: BTreeMap<(GroupId, ClientId), LocalProposalId>,
    pending_admissions: BTreeMap<ReadId, PendingAdmission>,
    pending_admission_operations: BTreeMap<(GroupId, ClientId), ReadId>,
    deferred_recovery: BTreeMap<(GroupId, ClientId), Instant>,
    tick_pending: BTreeSet<GroupId>,
    poisoned: BTreeSet<GroupId>,
    slow: BTreeMap<GroupId, Duration>,
    delayed: Vec<DelayedDispatch>,
    audit: Audit,
    next_proposal_id: u64,
    next_read_id: u64,
    admission_barriers_started: u64,
    client_admitted: u64,
    recovery_refused: u64,
    ready_announced: bool,
    refused_peer: u64,
    peers_paused: bool,
    recovery_mode: RecoveryMode,
    stopping: bool,
}

pub fn run(config: &Config) -> Result<(), String> {
    fs::create_dir_all(config.host_dir()).map_err(|error| {
        format!(
            "could not create host directory {}: {error}",
            config.host_dir().display()
        )
    })?;
    let client_listener =
        TcpListener::bind("127.0.0.1:0").map_err(|error| format!("client bind failed: {error}"))?;
    let client_addr = client_listener
        .local_addr()
        .map_err(|error| format!("client address failed: {error}"))?;
    let (jobs_tx, jobs_rx) = mpsc::sync_channel(MAX_CLIENT_JOBS);
    protocol::spawn_client_acceptor(client_listener, jobs_tx);
    super::emit(&format!("LISTENING {} {client_addr}", config.node_id.0));

    let link = PeerLink::bind(&config.cluster_dir, config.node_id, &config.members)
        .map_err(|error| format!("peer bind failed: {error}"))?;
    link.publish_address(&config.cluster_dir, config.node_id)
        .map_err(|error| format!("peer address publication failed: {error}"))?;
    super::emit(&format!(
        "PEER_LISTENING {} {} unauthenticated=true",
        config.node_id.0,
        link.local_addr()
    ));

    let managed = ManagedConfig::new(
        config.workers,
        config.max_group_queue,
        config.max_global_queue,
        NonZeroUsize::new(config.quota.get() as usize).expect("quota is nonzero"),
    )
    .map_err(|error| error.to_string())?;
    let mut engine = Engine {
        node_id: config.node_id,
        members: config.members.clone(),
        group_count: config.group_count,
        max_sessions: config.max_sessions,
        default_quota: config.quota,
        election_timeout_ticks: config.election_timeout_ticks,
        request_timeout: config.request_timeout,
        tick_interval: config.tick_interval,
        worker_capacity: config.workers.get(),
        max_group_queue: config.max_group_queue.get(),
        registry: None,
        host: ManagedTypedMultiRaftHost::new(managed),
        groups: BTreeMap::new(),
        link,
        work: BTreeMap::new(),
        pending: BTreeMap::new(),
        pending_operations: BTreeMap::new(),
        pending_admissions: BTreeMap::new(),
        pending_admission_operations: BTreeMap::new(),
        deferred_recovery: BTreeMap::new(),
        tick_pending: BTreeSet::new(),
        poisoned: BTreeSet::new(),
        slow: BTreeMap::new(),
        delayed: Vec::new(),
        audit: Audit::default(),
        next_proposal_id: 1,
        next_read_id: 1,
        admission_barriers_started: 0,
        client_admitted: 0,
        recovery_refused: 0,
        ready_announced: false,
        refused_peer: 0,
        peers_paused: false,
        recovery_mode: RecoveryMode::Running,
        stopping: false,
    };
    engine.open_groups(&config.host_dir())?;
    engine.serve(&jobs_rx, client_addr)
}

impl Engine {
    fn open_groups(&mut self, host_dir: &std::path::Path) -> Result<(), String> {
        let groups_dir = host_dir.join("groups");
        fs::create_dir_all(&groups_dir)
            .map_err(|error| format!("could not create {}: {error}", groups_dir.display()))?;
        let registry = self.open_or_initialize_registry(&groups_dir)?;
        self.registry = Some(registry);
        let mut recoveries = Vec::new();
        for raw in 1..=self.group_count {
            let group_id = GroupId::new(raw);
            let directory = groups_dir.join(raw.to_string());
            self.reconcile_activation(&directory, group_id)?;
            let (record, state_machine) =
                ApplicationRecord::open_existing(&directory.join("app"), self.max_sessions)
                    .map_err(|error| {
                        format!(
                            "group {raw} application open refused; the host registry proves this \
                             slot already exists: {error}"
                        )
                    })?;
            drop(state_machine);
            self.reconcile_retirement(&directory, group_id, &record)?;
            let policy = record.policy();
            self.reconcile_registry(group_id, &policy)?;
            Self::validate_group_shape(&directory, group_id, policy.incarnation, policy.lifecycle)?;
            if policy.poisoned {
                self.install_quarantined(group_id, directory, record)?;
                continue;
            }
            if matches!(
                policy.lifecycle,
                GroupLifecycle::Removed | GroupLifecycle::Tombstoned
            ) {
                self.groups.insert(
                    group_id,
                    GroupEntry {
                        directory,
                        record,
                        driver: None,
                    },
                );
                continue;
            }
            let opened = match self.open_physical(&directory, group_id) {
                Ok(opened) => opened,
                Err(OpenError::PoisonedRecovery(_)) => {
                    let (record, state_machine) =
                        ApplicationRecord::open_existing(&directory.join("app"), self.max_sessions)
                            .map_err(|error| error.to_string())?;
                    drop(state_machine);
                    self.install_quarantined(group_id, directory, record)?;
                    continue;
                }
                Err(error) => {
                    return Err(format!("group {} open failed: {error}", group_id.get()));
                }
            };
            recoveries.push((group_id, opened.recovery.clone()));
            self.install_opened(group_id, directory, opened)?;
        }
        for (group_id, report) in recoveries {
            self.collect_report(group_id, report)?;
        }
        Ok(())
    }

    fn open_physical(
        &self,
        directory: &std::path::Path,
        group_id: GroupId,
    ) -> Result<OpenedGroup, OpenError> {
        SharedGroup::open(
            directory,
            group_id,
            self.node_id,
            &self.members,
            self.election_timeout_ticks,
            self.max_sessions,
        )
    }

    fn install_quarantined(
        &mut self,
        group_id: GroupId,
        directory: PathBuf,
        record: ApplicationRecord,
    ) -> Result<(), String> {
        record
            .fail_poisoned_outstanding()
            .map_err(|error| error.to_string())?;
        self.poisoned.insert(group_id);
        self.groups.insert(
            group_id,
            GroupEntry {
                directory,
                record,
                driver: None,
            },
        );
        Ok(())
    }

    fn install_opened(
        &mut self,
        group_id: GroupId,
        directory: PathBuf,
        opened: OpenedGroup,
    ) -> Result<(), String> {
        let policy = opened.record.policy();
        self.host
            .open_group(
                &group_id,
                opened.driver.clone(),
                Some(
                    NonZeroUsize::new(policy.quota.get() as usize)
                        .expect("stored quota is nonzero"),
                ),
            )
            .map_err(|rejected| format!("managed group open failed: {:?}", rejected.error))?;
        self.audit.register_group(group_id);
        self.host
            .set_available(&group_id, true)
            .map_err(|error| format!("managed group availability failed: {error:?}"))?;
        self.audit.set_available(group_id, true);
        self.groups.insert(
            group_id,
            GroupEntry {
                directory,
                record: opened.record,
                driver: Some(opened.driver),
            },
        );
        Ok(())
    }

    fn serve(
        &mut self,
        jobs: &Receiver<Job>,
        client_addr: std::net::SocketAddr,
    ) -> Result<(), String> {
        let mut next_tick = Instant::now();
        while !self.stopping {
            if self.recovery_mode == RecoveryMode::Running {
                self.recover_outstanding()?;
            }
            self.receive_jobs(jobs)?;
            if self.stopping {
                break;
            }
            self.admit_peer_frames()?;
            let now = Instant::now();
            if now >= next_tick {
                self.admit_ticks()?;
                next_tick = now + self.tick_interval;
            }
            self.expire_clients(now)?;
            self.drive(now)?;
            if !self.ready_announced && self.all_active_ready() {
                self.ready_announced = true;
                super::emit(&format!(
                    "READY {} {client_addr} groups={}",
                    self.node_id.0,
                    self.active_group_count()
                ));
            }
        }
        self.finish();
        Ok(())
    }

    fn receive_jobs(&mut self, jobs: &Receiver<Job>) -> Result<(), String> {
        match jobs.recv_timeout(LOOP_POLL) {
            Ok(job) => self.handle_job(job)?,
            Err(mpsc::RecvTimeoutError::Timeout) => return Ok(()),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("client job channel disconnected".to_string());
            }
        }
        for _ in 1..MAX_JOBS_PER_LOOP {
            match jobs.try_recv() {
                Ok(job) => self.handle_job(job)?,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err("client job channel disconnected".to_string());
                }
            }
            if self.stopping {
                break;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn handle_job(&mut self, job: Job) -> Result<(), String> {
        let Job { request, reply } = job;
        match request {
            Request::Status => reply.send(self.status_line(), false),
            Request::Audit => reply.send(self.audit_line(), false),
            Request::OpenSession {
                group_id,
                incarnation,
                client_id,
                epoch,
            } => {
                let operation = AcceptedOperation::OpenSession { client_id, epoch };
                self.begin_authoritative_admission(group_id, incarnation, operation, reply)?;
            }
            Request::Counter {
                group_id,
                incarnation,
                client_id,
                epoch,
                sequence,
                command,
            } => {
                let request = RequestIdentity {
                    client_id,
                    session_epoch: epoch,
                    sequence,
                    fingerprint: RequestFingerprint::of(&command),
                };
                let operation = AcceptedOperation::Counter { request, command };
                self.begin_authoritative_admission(group_id, incarnation, operation, reply)?;
            }
            Request::Value {
                group_id,
                incarnation,
            } => match self.serving_driver(group_id, incarnation) {
                Ok(driver) => {
                    let view = driver.view();
                    reply.send(
                        format!(
                            "OK VALUE group={} incarnation={} value={} applied={}",
                            group_id.get(),
                            incarnation.get(),
                            view.value,
                            view.applied_index.0
                        ),
                        false,
                    );
                }
                Err(response) => reply.send(response, false),
            },
            Request::Fault {
                group_id,
                incarnation,
            } => {
                if let Err(response) = self.serving_driver(group_id, incarnation) {
                    reply.send(response, false);
                } else if let Err(refusal) = self.admit_client_proposal(
                    group_id,
                    WorkClass::Command,
                    ReplicatedCounterCommand::Faulty,
                    None,
                    vec![reply],
                ) {
                    for reply in refusal.replies {
                        reply.send(refusal.response.clone(), false);
                    }
                }
            }
            Request::CapacityFault {
                group_id,
                incarnation,
            } => {
                if let Err(response) = self.serving_driver(group_id, incarnation) {
                    reply.send(response, false);
                } else if let Err(refusal) = self.admit_client_proposal(
                    group_id,
                    WorkClass::Command,
                    ReplicatedCounterCommand::CapacityFault,
                    None,
                    vec![reply],
                ) {
                    for reply in refusal.replies {
                        reply.send(refusal.response.clone(), false);
                    }
                }
            }
            Request::PausePeers => {
                self.peers_paused = true;
                reply.send("OK PEERS paused".to_string(), false);
            }
            Request::ResumePeers => {
                self.peers_paused = false;
                reply.send("OK PEERS resumed".to_string(), false);
            }
            Request::PauseRecovery => {
                self.recovery_mode = RecoveryMode::Paused;
                reply.send("OK RECOVERY paused".to_string(), false);
            }
            Request::ResumeRecovery => {
                self.recovery_mode = RecoveryMode::Running;
                let now = Instant::now();
                for retry_at in self.deferred_recovery.values_mut() {
                    *retry_at = now;
                }
                reply.send("OK RECOVERY resumed".to_string(), false);
            }
            Request::TransferLeadership {
                group_id,
                incarnation,
                target,
            } => {
                let driver = match self.serving_driver(group_id, incarnation) {
                    Ok(driver) => driver.clone(),
                    Err(response) => {
                        reply.send(response, false);
                        return Ok(());
                    }
                };
                match driver.step_direct(GroupInput::TransferLeadership { target }) {
                    Ok(report) => {
                        self.collect_report(group_id, report)?;
                        reply.send(format!("OK TRANSFER target={}", target.0), false);
                    }
                    Err(error) if driver_application_durability_failed(&error) => {
                        return Err(format!(
                            "group {} application durability failed during leadership transfer: {error}",
                            group_id.get()
                        ));
                    }
                    Err(error) => {
                        if error.kind() == rafter_multiraft::DriverErrorKind::Poisoned {
                            self.persist_runtime_poison(group_id)?;
                        }
                        reply.send(format!("ERR TRANSFER {error}"), false);
                    }
                }
            }
            Request::Pressure {
                group_id,
                incarnation,
                class,
                count,
            } => {
                if count > self.max_group_queue {
                    reply.send(
                        format!("ERR PRESSURE_LIMIT {}", self.max_group_queue),
                        false,
                    );
                    return Ok(());
                }
                if let Err(response) = self.serving_driver(group_id, incarnation) {
                    reply.send(response, false);
                    return Ok(());
                }
                let class = match class {
                    PressureClass::Snapshot => WorkClass::Snapshot,
                    PressureClass::Bulk => WorkClass::Bulk,
                };
                let mut accepted = 0;
                for _ in 0..count {
                    match self.host.admit(&group_id, class, GroupInput::Tick) {
                        Ok(receipt) => {
                            self.audit
                                .observe_admission(group_id, receipt.work_id, class);
                            self.work.insert(receipt.work_id, WorkKind::Pressure);
                            accepted += 1;
                        }
                        Err(_) => break,
                    }
                }
                reply.send(
                    format!(
                        "OK PRESSURE accepted={accepted} refused={}",
                        count - accepted
                    ),
                    false,
                );
            }
            Request::Snapshot {
                group_id,
                incarnation,
            } => {
                if let Err(response) = self.serving_driver(group_id, incarnation) {
                    reply.send(response, false);
                    return Ok(());
                }
                match self
                    .host
                    .admit(&group_id, WorkClass::Snapshot, GroupInput::Tick)
                {
                    Ok(receipt) => {
                        self.audit.observe_admission(
                            group_id,
                            receipt.work_id,
                            WorkClass::Snapshot,
                        );
                        self.work.insert(receipt.work_id, WorkKind::Snapshot(reply));
                    }
                    Err(rejected) => {
                        reply.send(format!("ERR BACKPRESSURE {:?}", rejected.reason), false);
                    }
                }
            }
            Request::Slow {
                group_id,
                milliseconds,
            } => {
                if milliseconds > MAX_SLOW_DELAY_MS {
                    reply.send(format!("ERR SLOW_LIMIT {MAX_SLOW_DELAY_MS}"), false);
                } else if !self.groups.contains_key(&group_id) {
                    reply.send("ERR GROUP_UNKNOWN".to_string(), false);
                } else {
                    if milliseconds == 0 {
                        self.slow.remove(&group_id);
                    } else {
                        self.slow
                            .insert(group_id, Duration::from_millis(milliseconds));
                    }
                    reply.send(
                        format!(
                            "OK SLOW group={} milliseconds={milliseconds}",
                            group_id.get()
                        ),
                        false,
                    );
                }
            }
            Request::Drain {
                group_id,
                incarnation,
            } => reply.send(self.drain(group_id, incarnation)?, false),
            Request::Remove {
                group_id,
                incarnation,
            } => reply.send(self.remove(group_id, incarnation)?, false),
            Request::Reopen {
                group_id,
                incarnation,
                quota,
            } => reply.send(self.reopen(group_id, incarnation, quota)?, false),
            Request::Tombstone {
                group_id,
                incarnation,
            } => reply.send(self.tombstone(group_id, incarnation)?, false),
            Request::Shutdown => {
                self.stopping = true;
                reply.send(format!("OK SHUTDOWN {}", self.node_id.0), true);
            }
        }
        Ok(())
    }

    fn drive(&mut self, now: Instant) -> Result<(), String> {
        self.release_delayed(now)?;
        match self
            .host
            .arm_pass()
            .map_err(|error| format!("pass identity failed: {error:?}"))?
        {
            ArmPass::Armed(plan) => {
                self.audit.observe_plan(plan.pass_id.get(), &plan.groups);
            }
            ArmPass::AlreadyArmed(_) | ArmPass::Idle => {}
        }
        for _ in 0..MAX_DISPATCHES_PER_LOOP {
            match self
                .host
                .begin_dispatch()
                .map_err(|error| format!("dispatch identity failed: {error:?}"))?
            {
                BeginDispatch::Dispatched(dispatch) => {
                    self.audit.observe_dispatch(&dispatch);
                    let delay = self.slow.get(&dispatch.group_id).copied().filter(|_| {
                        dispatch
                            .items
                            .iter()
                            .any(|item| item.class != WorkClass::Control)
                    });
                    if let Some(delay) = delay {
                        self.delayed.push(DelayedDispatch {
                            ready_at: now + delay,
                            dispatch,
                        });
                    } else {
                        self.execute(dispatch)?;
                    }
                }
                BeginDispatch::Skipped(skipped) => self.audit.observe_skip(&skipped),
                BeginDispatch::PassComplete(completion) => {
                    self.audit.observe_completion(completion);
                    break;
                }
                BeginDispatch::WorkersOccupied | BeginDispatch::NoPass => break,
            }
        }
        Ok(())
    }

    fn release_delayed(&mut self, now: Instant) -> Result<(), String> {
        let mut ready = Vec::new();
        let mut index = 0;
        while index < self.delayed.len() {
            if self.delayed[index].ready_at <= now {
                ready.push(self.delayed.swap_remove(index).dispatch);
            } else {
                index += 1;
            }
        }
        ready.sort_by_key(|dispatch| dispatch.dispatch_id);
        for dispatch in ready {
            self.execute(dispatch)?;
        }
        Ok(())
    }

    fn execute(&mut self, dispatch: CounterDispatch) -> Result<(), String> {
        self.mark_dispatch_entered(&dispatch)?;
        let managed = self
            .host
            .execute_dispatch(dispatch)
            .map_err(|rejected| format!("dispatch validation failed: {:?}", rejected.error))?;
        let group_id = managed.group_id;
        let work_ids = managed
            .items
            .iter()
            .map(|item| item.work_id)
            .collect::<Vec<_>>();
        let mut poisoned = false;
        for item in managed.items {
            let work_kind = self.work.remove(&item.work_id);
            if matches!(work_kind, Some(WorkKind::Tick)) {
                self.tick_pending.remove(&group_id);
            }
            match item.result {
                Ok(report) => {
                    self.collect_report(group_id, report)?;
                    if let Some(WorkKind::Snapshot(reply)) = work_kind {
                        let response = self
                            .groups
                            .get(&group_id)
                            .and_then(|entry| entry.driver.as_ref())
                            .ok_or_else(|| "snapshot group disappeared".to_string())
                            .and_then(SharedGroup::compact)
                            .map_or_else(
                                |error| format!("ERR SNAPSHOT {error}"),
                                |at| format!("OK SNAPSHOT applied={}", at.0),
                            );
                        reply.send(response, false);
                    }
                }
                Err(error) => {
                    if managed_application_durability_failed(&error) {
                        return Err(format!(
                            "group {} application durability failed during committed dispatch: {error}",
                            group_id.get()
                        ));
                    }
                    let kind = error.kind();
                    if kind == MultiRaftErrorKind::DriverPoisoned {
                        self.persist_runtime_poison(group_id)?;
                        poisoned = true;
                    }
                    match work_kind {
                        Some(WorkKind::Proposal(proposal_id)) => {
                            if kind == MultiRaftErrorKind::DriverPoisoned {
                                self.complete_poisoned_dispatch(proposal_id)?;
                            } else {
                                self.complete_unknown(
                                    proposal_id,
                                    &format!("managed group failed: {kind:?}"),
                                );
                            }
                        }
                        Some(WorkKind::Snapshot(reply)) => {
                            reply.send(format!("ERR SNAPSHOT {kind:?}"), false);
                        }
                        _ => {}
                    }
                }
            }
        }
        managed
            .completion
            .map_err(|error| format!("dispatch completion failed: {error:?}"))?;
        self.audit
            .observe_dispatch_completion(group_id, &work_ids, poisoned);
        Ok(())
    }

    fn mark_dispatch_entered(&self, dispatch: &CounterDispatch) -> Result<(), String> {
        for item in &dispatch.items {
            let Some(WorkKind::Proposal(proposal_id)) = self.work.get(&item.work_id) else {
                continue;
            };
            let Some(pending) = self.pending.get(proposal_id) else {
                continue;
            };
            let Some(operation) = pending.operation else {
                continue;
            };
            self.groups[&pending.group_id]
                .record
                .mark_entered_driver(operation)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn collect_report(&mut self, group_id: GroupId, mut report: Report) -> Result<(), String> {
        let incarnation = self
            .groups
            .get(&group_id)
            .ok_or_else(|| "report named an unknown group".to_string())?
            .record
            .policy()
            .incarnation;
        for envelope in std::mem::take(&mut report.peer_messages) {
            if !self.peers_paused {
                let _ = self.link.send(PeerFrame {
                    group_id,
                    incarnation,
                    from: envelope.from,
                    to: envelope.to,
                    message: envelope.message,
                });
            }
        }
        for event in take_ordered_consumer_events(&mut report) {
            match event {
                ConsumerReportEvent::Proposal(event) => match event {
                    ProposalEvent::Applied {
                        local_proposal_id,
                        result,
                        ..
                    } => self.complete_applied(local_proposal_id, result),
                    ProposalEvent::Rejected {
                        local_proposal_id,
                        reason,
                        leader_hint,
                    } => self.complete_rejected(
                        local_proposal_id,
                        &format!("reason={reason:?} leader={leader_hint:?}"),
                    )?,
                    ProposalEvent::UnknownOutcome {
                        local_proposal_id,
                        reason,
                        ..
                    } => self.complete_unknown(local_proposal_id, &format!("{reason:?}")),
                    _ => {}
                },
                ConsumerReportEvent::Applied(applied) => {
                    if let Some(proposal_id) = applied.local_proposal_id {
                        self.complete_applied(proposal_id, applied.result);
                    }
                }
                ConsumerReportEvent::Read(event) => {
                    self.finish_admission_read_event(&event)?;
                }
            }
        }
        Ok(())
    }

    fn complete_applied(&mut self, proposal_id: LocalProposalId, result: CounterApplyResult) {
        let Some(pending) = self.take_pending(proposal_id) else {
            return;
        };
        let response = render_apply_result(result);
        for reply in pending.replies {
            reply.send(response.clone(), false);
        }
    }

    fn complete_rejected(
        &mut self,
        proposal_id: LocalProposalId,
        detail: &str,
    ) -> Result<(), String> {
        let Some(pending) = self.take_pending(proposal_id) else {
            return Ok(());
        };
        if pending.recovered {
            if let Some(operation) = pending.operation {
                self.deferred_recovery.insert(
                    (pending.group_id, operation.client_id()),
                    Instant::now() + RECOVERY_RETRY_DELAY,
                );
            }
            for reply in pending.replies {
                reply.send(
                    format!("ERR UNKNOWN recovered request remains pending: {detail}"),
                    false,
                );
            }
            return Ok(());
        }
        if let Some(operation) = pending.operation {
            self.groups[&pending.group_id]
                .record
                .cancel_reservation(operation)
                .map_err(|error| error.to_string())?;
        }
        for reply in pending.replies {
            reply.send(format!("ERR NOT_COMMITTED {detail}"), false);
        }
        Ok(())
    }

    fn complete_unknown(&mut self, proposal_id: LocalProposalId, detail: &str) {
        let Some(pending) = self.take_pending(proposal_id) else {
            return;
        };
        if pending.recovered {
            if let Some(operation) = pending.operation {
                self.deferred_recovery.insert(
                    (pending.group_id, operation.client_id()),
                    Instant::now() + RECOVERY_RETRY_DELAY,
                );
            }
        }
        for reply in pending.replies {
            reply.send(format!("ERR UNKNOWN {detail}"), false);
        }
    }

    fn complete_poisoned_dispatch(&mut self, proposal_id: LocalProposalId) -> Result<(), String> {
        let Some(pending) = self.take_pending(proposal_id) else {
            return Ok(());
        };
        let disposition = if let Some(operation) = pending.operation {
            self.groups[&pending.group_id]
                .record
                .fail_reservation(operation, TerminalFailure::GroupPoisonedUnknown)
                .map_err(|error| error.to_string())?;
            "UNKNOWN"
        } else {
            "NOT_COMMITTED"
        };
        for reply in pending.replies {
            reply.send(format!("ERR {disposition} GROUP_POISONED"), false);
        }
        Ok(())
    }

    fn complete_not_committed(
        &mut self,
        proposal_id: LocalProposalId,
        failure: TerminalFailure,
        detail: &str,
    ) -> Result<(), String> {
        let Some(pending) = self.take_pending(proposal_id) else {
            return Ok(());
        };
        if let Some(operation) = pending.operation {
            self.groups[&pending.group_id]
                .record
                .fail_reservation(operation, failure)
                .map_err(|error| error.to_string())?;
        }
        for reply in pending.replies {
            reply.send(format!("ERR NOT_COMMITTED {detail}"), false);
        }
        Ok(())
    }

    fn take_pending(&mut self, proposal_id: LocalProposalId) -> Option<PendingClient> {
        let pending = self.pending.remove(&proposal_id)?;
        if let Some(operation) = pending.operation {
            self.pending_operations
                .remove(&(pending.group_id, operation.client_id()));
            self.deferred_recovery
                .remove(&(pending.group_id, operation.client_id()));
        }
        Some(pending)
    }

    fn expire_clients(&mut self, now: Instant) -> Result<(), String> {
        self.expire_admission_reads(now)?;
        let expired = self
            .pending
            .iter()
            .filter_map(|(id, pending)| {
                pending
                    .deadline
                    .is_some_and(|deadline| now >= deadline)
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        for proposal_id in expired {
            let pending = self
                .pending
                .get_mut(&proposal_id)
                .expect("expired identity names a pending proposal");
            pending.deadline = None;
            for reply in std::mem::take(&mut pending.replies) {
                reply.send("ERR UNKNOWN client deadline elapsed".to_string(), false);
            }
        }
        Ok(())
    }

    fn drain(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<String, String> {
        let entry = self
            .groups
            .get(&group_id)
            .ok_or_else(|| "group is unknown".to_string())?;
        let record = entry.record.clone();
        let policy = record.policy();
        let has_driver = entry.driver.is_some();
        if policy.incarnation != incarnation {
            return Ok(format!(
                "ERR INCARNATION current={}",
                policy.incarnation.get()
            ));
        }
        if !matches!(
            policy.lifecycle,
            GroupLifecycle::Serving | GroupLifecycle::Draining
        ) {
            return Ok(format!("ERR LIFECYCLE {:?}", policy.lifecycle));
        }
        if policy.lifecycle == GroupLifecycle::Serving {
            directed_failpoint("before_draining_application_publication");
            record
                .begin_draining(self.poisoned.contains(&group_id))
                .map_err(|error| error.to_string())?;
            directed_failpoint("after_draining_application_publication");
            self.publish_group_policy(group_id)?;
            directed_failpoint("after_draining_registry_publication");
        } else if self.poisoned.contains(&group_id) && !policy.poisoned {
            record.mark_poisoned().map_err(|error| error.to_string())?;
        }
        self.cancel_admission_reads_for_group(group_id, "ERR LIFECYCLE Draining")?;
        if self.poisoned.contains(&group_id) && has_driver {
            directed_failpoint("before_queued_retirement");
            let retired = self
                .host
                .fail_queued(&group_id)
                .map_err(|error| format!("poisoned queue drain failed: {error:?}"))?;
            let retired_ids = retired.iter().map(|item| item.work_id).collect::<Vec<_>>();
            self.audit.observe_failed_queued(group_id, &retired_ids);
            directed_failpoint("after_queued_retirement_before_durable_failure_publication");
            let retired_count = retired.len();
            for (index, item) in retired.into_iter().enumerate() {
                let work_kind = self.work.remove(&item.work_id);
                match work_kind {
                    Some(WorkKind::Tick) => {
                        self.tick_pending.remove(&group_id);
                    }
                    Some(WorkKind::Proposal(proposal_id)) => {
                        self.complete_not_committed(
                            proposal_id,
                            TerminalFailure::GroupPoisoned,
                            "GROUP_POISONED",
                        )?;
                    }
                    Some(WorkKind::Snapshot(reply)) => {
                        reply.send("ERR SNAPSHOT DriverPoisoned".to_string(), false);
                    }
                    Some(WorkKind::Peer | WorkKind::Pressure) | None => {}
                }
                drop(item.payload);
                if index == 0 && retired_count > 1 {
                    directed_failpoint("midway_through_queued_retirement");
                }
            }
            directed_failpoint("after_durable_failure_publication");
        }
        Ok(format!("OK DRAIN group={}", group_id.get()))
    }

    fn remove(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<String, String> {
        let Some(entry) = self.groups.get(&group_id) else {
            return Ok("ERR GROUP_UNKNOWN".to_string());
        };
        let policy = entry.record.policy();
        if policy.incarnation != incarnation {
            return Ok(format!(
                "ERR INCARNATION current={}",
                policy.incarnation.get()
            ));
        }
        if policy.lifecycle != GroupLifecycle::Draining {
            return Ok(format!("ERR LIFECYCLE {:?}", policy.lifecycle));
        }
        if !policy.outstanding.is_empty() {
            directed_failpoint("before_removal_with_durable_outstanding_work");
            return Ok(format!(
                "ERR BUSY DURABLE_OUTSTANDING count={}",
                policy.outstanding.len()
            ));
        }
        let managed = entry.driver.is_some();
        if managed {
            match self.host.can_remove_group(&group_id) {
                Ok(true) => {}
                Ok(false) => return Ok("ERR GROUP_NOT_OPEN".to_string()),
                Err(error) => return Ok(format!("ERR BUSY {error:?}")),
            }
        } else if !policy.poisoned {
            return Ok("ERR GROUP_NOT_OPEN".to_string());
        }
        directed_failpoint("before_intent_publish");
        RetirementIntent {
            group_id,
            incarnation,
        }
        .publish(&entry.directory)?;
        directed_failpoint("after_intent_publish");
        if managed {
            let removed = match self.host.remove_group(&group_id) {
                Ok(Some(driver)) => driver,
                Ok(None) => {
                    return Err(format!(
                        "group {} disappeared after its retirement intent became durable",
                        group_id.get()
                    ));
                }
                Err(error) => {
                    return Err(format!(
                        "group {} became non-removable after its retirement intent became durable: \
                         {error:?}",
                        group_id.get()
                    ));
                }
            };
            drop(removed);
            self.audit.remove_group(group_id);
            directed_failpoint("after_driver_detach");
        }
        let entry = self
            .groups
            .get_mut(&group_id)
            .expect("group entry survives managed removal");
        drop(entry.driver.take());
        archive_raft_with_failpoints(&entry.directory, incarnation)?;
        directed_failpoint("before_removed_publish");
        entry
            .record
            .retire(GroupLifecycle::Removed)
            .map_err(|error| error.to_string())?;
        self.publish_group_policy(group_id)?;
        directed_failpoint("after_removed_publish");
        directed_failpoint("before_intent_cleanup");
        RetirementIntent::clear(&self.groups[&group_id].directory)?;
        self.tick_pending.remove(&group_id);
        self.poisoned.remove(&group_id);
        self.slow.remove(&group_id);
        Ok(format!("OK REMOVE group={}", group_id.get()))
    }

    fn reopen(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        raw_quota: u32,
    ) -> Result<String, String> {
        let Some(quota) = WorkQuota::new(raw_quota) else {
            return Ok("ERR ZERO_QUOTA".to_string());
        };
        let Some(entry) = self.groups.get(&group_id) else {
            return Ok("ERR GROUP_UNKNOWN".to_string());
        };
        let policy = entry.record.policy();
        if policy.incarnation != incarnation {
            return Ok(format!(
                "ERR INCARNATION current={}",
                policy.incarnation.get()
            ));
        }
        if policy.lifecycle == GroupLifecycle::Tombstoned {
            return Ok("ERR TOMBSTONED".to_string());
        }
        if policy.lifecycle != GroupLifecycle::Removed {
            return Ok(format!("ERR LIFECYCLE {:?}", policy.lifecycle));
        }
        let directory = entry.directory.clone();
        if directory.join("raft").exists() {
            return Err("removed group still has an active Raft directory".to_string());
        }
        let new_incarnation = incarnation
            .successor()
            .ok_or_else(|| "group incarnation exhausted".to_string())?;
        let intent = ActivationIntent {
            group_id,
            previous_incarnation: incarnation,
            next_incarnation: new_incarnation,
            quota,
        };
        directed_failpoint("before_activation_intent_publication");
        intent.publish(&directory)?;
        directed_failpoint("after_activation_intent_publication");
        prepare_staged_raft(&directory, new_incarnation, true)?;
        entry
            .record
            .reopen(quota, self.max_sessions)
            .map_err(|error| error.to_string())?;
        directed_failpoint("after_activation_application_publication");
        self.publish_group_policy(group_id)?;
        directed_failpoint("after_activation_registry_publication");
        activate_staged_raft(&directory, new_incarnation, true)?;
        directed_failpoint("before_activation_intent_cleanup");
        ActivationIntent::clear(&directory)?;
        let opened = self
            .open_physical(&directory, group_id)
            .map_err(|error| format!("group {} open failed: {error}", group_id.get()))?;
        let recovery = opened.recovery.clone();
        self.install_opened(group_id, directory, opened)?;
        self.collect_report(group_id, recovery)?;
        Ok(format!(
            "OK REOPEN group={} incarnation={}",
            group_id.get(),
            new_incarnation.get()
        ))
    }

    fn tombstone(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<String, String> {
        let Some(entry) = self.groups.get(&group_id) else {
            return Ok("ERR GROUP_UNKNOWN".to_string());
        };
        let policy = entry.record.policy();
        if policy.incarnation != incarnation {
            return Ok(format!(
                "ERR INCARNATION current={}",
                policy.incarnation.get()
            ));
        }
        if policy.lifecycle != GroupLifecycle::Removed {
            return Ok(format!("ERR LIFECYCLE {:?}", policy.lifecycle));
        }
        entry
            .record
            .retire(GroupLifecycle::Tombstoned)
            .map_err(|error| error.to_string())?;
        self.publish_group_policy(group_id)?;
        Ok(format!("OK TOMBSTONE group={}", group_id.get()))
    }

    fn publish_group_policy(&mut self, group_id: GroupId) -> Result<(), String> {
        let policy = self
            .groups
            .get(&group_id)
            .ok_or_else(|| format!("group {} disappeared", group_id.get()))?
            .record
            .policy();
        self.registry
            .as_mut()
            .expect("registry is installed before lifecycle commands")
            .publish(slot_from_policy(group_id, &policy))
    }

    fn all_active_ready(&self) -> bool {
        self.groups.values().all(|entry| {
            let policy = entry.record.policy();
            let lifecycle = policy.lifecycle;
            if matches!(
                lifecycle,
                GroupLifecycle::Removed | GroupLifecycle::Tombstoned
            ) || policy.poisoned
            {
                return true;
            }
            entry.driver.as_ref().is_some_and(SharedGroup::is_ready)
        })
    }

    fn active_group_count(&self) -> usize {
        self.groups
            .iter()
            .filter(|(group_id, entry)| {
                entry.driver.is_some()
                    && !entry.record.policy().poisoned
                    && !self.poisoned.contains(group_id)
            })
            .count()
    }

    fn status_line(&self) -> String {
        let metrics = self.host.managed_metrics();
        let raft = self.host.raft_metrics();
        let leaders = raft
            .groups
            .iter()
            .filter(|metrics| {
                metrics.role == Role::Leader && !self.poisoned.contains(&metrics.group_id)
            })
            .count();
        let leader_groups = raft
            .groups
            .iter()
            .filter(|metrics| {
                metrics.role == Role::Leader && !self.poisoned.contains(&metrics.group_id)
            })
            .map(|metrics| metrics.group_id.get().to_string())
            .collect::<Vec<_>>()
            .join(",");
        let leader_groups = if leader_groups.is_empty() {
            "-"
        } else {
            &leader_groups
        };
        let poisoned = self.poisoned.len();
        let durable_outstanding = self
            .groups
            .values()
            .map(|entry| entry.record.policy().outstanding.len())
            .sum::<usize>();
        let admission_candidates = self
            .pending_admissions
            .values()
            .map(PendingAdmission::candidate_count)
            .sum::<usize>();
        let admission_successors = self
            .pending_admissions
            .values()
            .map(PendingAdmission::successor_count)
            .sum::<usize>();
        let link = self.link.counters();
        format!(
            "OK STATUS ready={} groups={} leaders={} leader_groups={} poisoned={} queued={} \
             in_flight={} workers={} admitted={} client_admitted={} serviced={} failed={} \
             passes={} pending_proposals={} admission_reads={} admission_candidates={} \
             admission_successors={} admission_barriers={} durable_outstanding={} \
             recovery_deferred={} recovery_refused={} refused_peer={} \
             link_outbound_full={} link_inbound_full={} link_malformed={} \
             link_identity_refused={} link_inbound_connection_full={}",
            self.all_active_ready(),
            self.active_group_count(),
            leaders,
            leader_groups,
            poisoned,
            metrics.queued,
            metrics.in_flight_work,
            metrics.occupied_workers,
            metrics.admitted,
            self.client_admitted,
            metrics.serviced,
            metrics.failed,
            metrics.passes_completed,
            self.pending_operations.len(),
            self.pending_admissions.len(),
            admission_candidates,
            admission_successors,
            self.admission_barriers_started,
            durable_outstanding,
            self.deferred_recovery.len(),
            self.recovery_refused,
            self.refused_peer,
            link.outbound_full,
            link.inbound_full,
            link.malformed,
            link.identity_refused,
            link.inbound_connection_full
        )
    }

    fn audit_line(&self) -> String {
        let metrics = self.host.managed_metrics();
        let (coverage, widest_gap) = self.audit.fairness();
        let conserved = metrics.admitted
            == metrics.serviced
                + metrics.failed
                + metrics.queued as u64
                + metrics.in_flight_work as u64;
        format!(
            "OK AUDIT plans={} passes_completed={} certified_passes={} opportunities={} \
             coverage={} widest_gap={} invalid_plans={} invalid_turns={} plan_digest={:016x} \
             turn_digest={:016x} admitted={} serviced={} failed={} queued={} in_flight={} \
             conserved={conserved}",
            self.audit.plans,
            self.audit.passes_completed,
            self.audit.certified_passes,
            self.audit.opportunities,
            coverage,
            widest_gap,
            self.audit.invalid_plans,
            self.audit.invalid_turns,
            self.audit.plan_digest,
            self.audit.turn_digest,
            metrics.admitted,
            metrics.serviced,
            metrics.failed,
            metrics.queued,
            metrics.in_flight_work
        )
    }

    fn finish(&mut self) {
        for (read_id, pending) in std::mem::take(&mut self.pending_admissions) {
            if let Some(driver) = self
                .groups
                .get(&pending.group_id())
                .and_then(|entry| entry.driver.as_ref())
            {
                driver.cancel_read(read_id);
            }
            Self::finish_pending_admission(pending, "process shutting down");
        }
        self.pending_admission_operations.clear();
        for (_, pending) in std::mem::take(&mut self.pending) {
            for reply in pending.replies {
                reply.send("ERR UNKNOWN process shutting down".to_string(), false);
            }
        }
        self.pending_operations.clear();
        let audit = self.audit_line();
        let status = self.status_line();
        super::emit(&format!("FINAL {} {status}", self.node_id.0));
        super::emit(&format!("FINAL {} {audit}", self.node_id.0));
        self.link.shut_down();
        super::emit(&format!("STOPPED {}", self.node_id.0));
    }
}

const fn render_terminal_failure(failure: TerminalFailure) -> &'static str {
    match failure {
        TerminalFailure::GroupPoisoned => "ERR NOT_COMMITTED GROUP_POISONED",
        TerminalFailure::GroupPoisonedUnknown => "ERR UNKNOWN GROUP_POISONED",
        TerminalFailure::ProcessRestarted => "ERR NOT_COMMITTED PROCESS_RESTARTED",
    }
}

fn render_apply_result(result: CounterApplyResult) -> String {
    match result {
        CounterApplyResult::Session(session) => match session {
            SessionApplyResult::Opened => "OK SESSION opened".to_string(),
            SessionApplyResult::AlreadyOpen => "OK SESSION already_open".to_string(),
            SessionApplyResult::Replaced => "OK SESSION replaced".to_string(),
        },
        CounterApplyResult::Counter(result) => format!("OK {}", render_counter_result(result)),
        CounterApplyResult::Rejected(rejection) => render_apply_rejection(rejection),
    }
}

fn render_apply_rejection(rejection: CounterApplyRejection) -> String {
    match rejection {
        CounterApplyRejection::ClientOutOfRange => "ERR CLIENT_OUT_OF_RANGE".to_string(),
        CounterApplyRejection::SessionNotOpen => "ERR SESSION_NOT_OPEN".to_string(),
        CounterApplyRejection::StaleSession { current } => {
            format!("ERR STALE_SESSION current={}", current.get())
        }
        CounterApplyRejection::FutureSession { current } => {
            format!("ERR FUTURE_SESSION current={}", current.get())
        }
        CounterApplyRejection::FingerprintMismatch => "ERR FINGERPRINT_MISMATCH".to_string(),
        CounterApplyRejection::StaleSequence { highest } => {
            format!("ERR STALE_SEQUENCE highest={}", highest.get())
        }
        CounterApplyRejection::SequenceGap { expected } => {
            format!("ERR SEQUENCE_GAP expected={}", expected.get())
        }
        CounterApplyRejection::ConflictingRetry => "ERR CONFLICTING_RETRY".to_string(),
    }
}

fn render_counter_result(result: CounterResult) -> String {
    match result {
        CounterResult::Added { value } => format!("ADDED value={value}"),
        CounterResult::Value { value } => format!("VALUE value={value}"),
        CounterResult::Rejected(CounterRejection::CounterOverflow { current }) => {
            format!("REJECTED overflow current={current}")
        }
    }
}
