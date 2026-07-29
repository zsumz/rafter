//! Bounded process loop that composes durable groups through the managed host.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::TcpListener,
    num::NonZeroUsize,
    path::PathBuf,
    sync::mpsc::{self, Receiver, TryRecvError},
    time::{Duration, Instant},
};

use rafter::{LocalProposalId, NodeId, Role};
use rafter_app::{
    group::GroupInput,
    proposal::{Proposal, ProposalEvent},
    transport::PeerEnvelope,
};
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
    ClientId, CounterCommand, CounterRejection, CounterResult, GroupId, GroupIncarnation,
    GroupLifecycle, RequestFingerprint, RequestIdentity, WorkQuota,
};

use super::{
    app_store::{ApplicationRecord, ReserveOutcome},
    group::{OpenedGroup, Report, SharedGroup},
    peer_link::{PeerFrame, PeerLink},
    protocol::{self, ClientReply, Job, PressureClass, Request},
    Config,
};

const MAX_CLIENT_JOBS: usize = 1024;
const MAX_JOBS_PER_LOOP: usize = 64;
const MAX_PEERS_PER_LOOP: usize = 512;
const MAX_DISPATCHES_PER_LOOP: usize = 512;
const LOOP_POLL: Duration = Duration::from_millis(2);
const MAX_SLOW_DELAY_MS: u64 = 5_000;

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
    request: Option<(RequestIdentity, CounterCommand)>,
    replies: Vec<ClientReply>,
    deadline: Instant,
}

#[derive(Debug)]
struct DelayedDispatch {
    ready_at: Instant,
    dispatch: CounterDispatch,
}

#[derive(Debug, Default)]
struct Audit {
    plans: u64,
    opportunities: u64,
    invalid_plans: u64,
    invalid_turns: u64,
    plan_digest: u64,
    turn_digest: u64,
    per_group: BTreeMap<GroupId, u64>,
}

impl Audit {
    fn observe_plan(&mut self, pass_id: u64, groups: &[GroupId]) {
        self.plans += 1;
        if groups.windows(2).any(|pair| pair[0] >= pair[1]) {
            self.invalid_plans += 1;
        }
        Self::mix(&mut self.plan_digest, pass_id);
        for group in groups {
            Self::mix(&mut self.plan_digest, u64::from(group.get()));
            self.per_group.entry(*group).or_default();
        }
    }

    fn observe_turn(&mut self, dispatch: &CounterDispatch) {
        self.opportunities += 1;
        if dispatch
            .items
            .windows(2)
            .any(|pair| pair[0].class > pair[1].class)
        {
            self.invalid_turns += 1;
        }
        Self::mix(&mut self.turn_digest, dispatch.pass_id.get());
        Self::mix(&mut self.turn_digest, dispatch.dispatch_id.get());
        Self::mix(&mut self.turn_digest, u64::from(dispatch.group_id.get()));
        for item in &dispatch.items {
            Self::mix(&mut self.turn_digest, item.work_id.get());
            Self::mix(
                &mut self.turn_digest,
                match item.class {
                    WorkClass::Control => 1,
                    WorkClass::Command => 2,
                    WorkClass::Snapshot => 3,
                    WorkClass::Bulk => 4,
                },
            );
        }
        *self.per_group.entry(dispatch.group_id).or_default() += 1;
    }

    fn fairness(&self) -> (usize, u64) {
        let observed = self
            .per_group
            .values()
            .copied()
            .filter(|count| *count != 0)
            .collect::<Vec<_>>();
        let coverage = observed.len();
        let widest_gap = observed
            .iter()
            .max()
            .zip(observed.iter().min())
            .map_or(0, |(max, min)| max - min);
        (coverage, widest_gap)
    }

    fn mix(digest: &mut u64, value: u64) {
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
        *digest ^= value;
        *digest = digest.wrapping_mul(FNV_PRIME);
    }
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
    max_pressure: usize,
    host: Host,
    groups: BTreeMap<GroupId, GroupEntry>,
    link: PeerLink,
    work: BTreeMap<WorkId, WorkKind>,
    pending: BTreeMap<LocalProposalId, PendingClient>,
    pending_requests: BTreeMap<(GroupId, ClientId), LocalProposalId>,
    tick_pending: BTreeSet<GroupId>,
    poisoned: BTreeSet<GroupId>,
    slow: BTreeMap<GroupId, Duration>,
    delayed: Vec<DelayedDispatch>,
    audit: Audit,
    next_proposal_id: u64,
    ready_announced: bool,
    refused_peer: u64,
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
        max_pressure: config.max_group_queue.get(),
        host: ManagedTypedMultiRaftHost::new(managed),
        groups: BTreeMap::new(),
        link,
        work: BTreeMap::new(),
        pending: BTreeMap::new(),
        pending_requests: BTreeMap::new(),
        tick_pending: BTreeSet::new(),
        poisoned: BTreeSet::new(),
        slow: BTreeMap::new(),
        delayed: Vec::new(),
        audit: Audit::default(),
        next_proposal_id: 1,
        ready_announced: false,
        refused_peer: 0,
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
        let mut recoveries = Vec::new();
        for raw in 1..=self.group_count {
            let group_id = GroupId::new(raw);
            let directory = groups_dir.join(raw.to_string());
            let record_path = directory.join("app/state.rcap");
            if record_path.exists() {
                let (record, state_machine) = ApplicationRecord::open(
                    &directory.join("app"),
                    self.max_sessions,
                    self.default_quota,
                )
                .map_err(|error| format!("group {raw} application open failed: {error}"))?;
                drop(state_machine);
                let policy = record.policy();
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
            }
            let opened = self.open_physical(&directory, group_id, self.default_quota)?;
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
        quota: WorkQuota,
    ) -> Result<OpenedGroup, String> {
        SharedGroup::open(
            directory,
            group_id,
            self.node_id,
            &self.members,
            self.election_timeout_ticks,
            self.max_sessions,
            quota,
        )
        .map_err(|error| format!("group {} open failed: {error}", group_id.get()))
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
        self.host
            .set_available(&group_id, true)
            .map_err(|error| format!("managed group availability failed: {error:?}"))?;
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
            self.receive_jobs(jobs)?;
            if self.stopping {
                break;
            }
            self.admit_peer_frames();
            let now = Instant::now();
            if now >= next_tick {
                self.admit_ticks();
                next_tick = now + self.tick_interval;
            }
            self.expire_clients(now);
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
                let driver = match self.serving_driver(group_id, incarnation) {
                    Ok(driver) => driver,
                    Err(response) => {
                        reply.send(response, false);
                        return Ok(());
                    }
                };
                if driver
                    .view()
                    .sessions
                    .iter()
                    .any(|session| session.client_id == client_id && session.epoch == epoch)
                {
                    reply.send("OK SESSION already_open".to_string(), false);
                    return Ok(());
                }
                self.admit_client_proposal(
                    group_id,
                    WorkClass::Control,
                    ReplicatedCounterCommand::OpenSession { client_id, epoch },
                    None,
                    reply,
                );
            }
            Request::Counter {
                group_id,
                incarnation,
                client_id,
                epoch,
                sequence,
                command,
            } => {
                let driver = match self.serving_driver(group_id, incarnation) {
                    Ok(driver) => driver,
                    Err(response) => {
                        reply.send(response, false);
                        return Ok(());
                    }
                };
                let request = RequestIdentity {
                    client_id,
                    session_epoch: epoch,
                    sequence,
                    fingerprint: RequestFingerprint::of(&command),
                };
                if let Some(proposal_id) =
                    self.pending_requests.get(&(group_id, client_id)).copied()
                {
                    let pending = self
                        .pending
                        .get_mut(&proposal_id)
                        .expect("pending request index names a pending proposal");
                    if pending.request == Some((request, command)) {
                        pending.replies.push(reply);
                    } else {
                        reply.send("ERR CONFLICTING_OUTSTANDING".to_string(), false);
                    }
                    return Ok(());
                }
                if let Some(result) = cached_result(&driver, request, command) {
                    reply.send(
                        format!("OK REPLAY {}", render_counter_result(result)),
                        false,
                    );
                    return Ok(());
                }
                let record = self
                    .groups
                    .get(&group_id)
                    .expect("serving group has an entry")
                    .record
                    .clone();
                let reservation = match record.reserve(request, command) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        reply.send(format!("ERR ADMISSION {error}"), false);
                        return Ok(());
                    }
                };
                let admitted = self.admit_client_proposal(
                    group_id,
                    WorkClass::Command,
                    ReplicatedCounterCommand::Counter { request, command },
                    Some((request, command)),
                    reply,
                );
                if !admitted && reservation == ReserveOutcome::Reserved {
                    record
                        .cancel_reservation(request, command)
                        .map_err(|error| error.to_string())?;
                }
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
                } else {
                    self.admit_client_proposal(
                        group_id,
                        WorkClass::Command,
                        ReplicatedCounterCommand::Faulty,
                        None,
                        reply,
                    );
                }
            }
            Request::Pressure {
                group_id,
                incarnation,
                class,
                count,
            } => {
                if count > self.max_pressure {
                    reply.send(format!("ERR PRESSURE_LIMIT {}", self.max_pressure), false);
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

    fn serving_driver(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<SharedGroup, String> {
        let Some(entry) = self.groups.get(&group_id) else {
            return Err("ERR GROUP_UNKNOWN".to_string());
        };
        let policy = entry.record.policy();
        if incarnation < policy.incarnation {
            return Err(format!(
                "ERR STALE_INCARNATION current={}",
                policy.incarnation.get()
            ));
        }
        if incarnation > policy.incarnation {
            return Err(format!(
                "ERR FUTURE_INCARNATION current={}",
                policy.incarnation.get()
            ));
        }
        if policy.lifecycle == GroupLifecycle::Tombstoned {
            return Err("ERR TOMBSTONED".to_string());
        }
        if policy.lifecycle != GroupLifecycle::Serving {
            return Err(format!("ERR LIFECYCLE {:?}", policy.lifecycle));
        }
        if self.poisoned.contains(&group_id) {
            return Err("ERR GROUP_POISONED".to_string());
        }
        let Some(driver) = entry.driver.clone() else {
            return Err("ERR GROUP_REMOVED".to_string());
        };
        if !driver.is_ready() {
            return Err("ERR NOT_READY".to_string());
        }
        Ok(driver)
    }

    fn admit_client_proposal(
        &mut self,
        group_id: GroupId,
        class: WorkClass,
        command: ReplicatedCounterCommand,
        request: Option<(RequestIdentity, CounterCommand)>,
        reply: ClientReply,
    ) -> bool {
        let proposal_id = LocalProposalId(self.next_proposal_id);
        let Some(next) = self.next_proposal_id.checked_add(1) else {
            reply.send("ERR PROPOSAL_ID_EXHAUSTED".to_string(), false);
            return false;
        };
        let input = GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: proposal_id,
                client_request_id: None,
                command,
            },
        };
        let receipt = match self.host.admit(&group_id, class, input) {
            Ok(receipt) => receipt,
            Err(rejected) => {
                reply.send(format!("ERR BACKPRESSURE {:?}", rejected.reason), false);
                return false;
            }
        };
        self.next_proposal_id = next;
        if let Some((identity, _)) = request {
            self.pending_requests
                .insert((group_id, identity.client_id), proposal_id);
        }
        self.pending.insert(
            proposal_id,
            PendingClient {
                group_id,
                request,
                replies: vec![reply],
                deadline: Instant::now() + self.request_timeout,
            },
        );
        self.work
            .insert(receipt.work_id, WorkKind::Proposal(proposal_id));
        true
    }

    fn admit_peer_frames(&mut self) {
        for frame in self.link.drain_inbound(MAX_PEERS_PER_LOOP) {
            let Some(entry) = self.groups.get(&frame.group_id) else {
                self.refused_peer += 1;
                continue;
            };
            let policy = entry.record.policy();
            if frame.incarnation != policy.incarnation
                || !policy.lifecycle.is_serviceable()
                || entry.driver.is_none()
                || self.poisoned.contains(&frame.group_id)
            {
                self.refused_peer += 1;
                continue;
            }
            let input = GroupInput::PeerMessage {
                envelope: PeerEnvelope {
                    group_id: frame.group_id,
                    from: frame.from,
                    to: frame.to,
                    message: frame.message,
                },
            };
            match self.host.admit(&frame.group_id, WorkClass::Control, input) {
                Ok(receipt) => {
                    self.work.insert(receipt.work_id, WorkKind::Peer);
                }
                Err(_) => self.refused_peer += 1,
            }
        }
    }

    fn admit_ticks(&mut self) {
        let group_ids = self.groups.keys().copied().collect::<Vec<_>>();
        for group_id in group_ids {
            if self.tick_pending.contains(&group_id) || self.poisoned.contains(&group_id) {
                continue;
            }
            let entry = &self.groups[&group_id];
            let policy = entry.record.policy();
            if !policy.lifecycle.is_serviceable() || entry.driver.is_none() {
                continue;
            }
            if let Ok(receipt) = self
                .host
                .admit(&group_id, WorkClass::Control, GroupInput::Tick)
            {
                self.tick_pending.insert(group_id);
                self.work.insert(receipt.work_id, WorkKind::Tick);
            }
        }
    }

    fn drive(&mut self, now: Instant) -> Result<(), String> {
        self.release_delayed(now)?;
        for group_id in &self.poisoned {
            let _ = self.host.set_available(group_id, true);
        }
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
                    self.audit.observe_turn(&dispatch);
                    if let Some(delay) = self.slow.get(&dispatch.group_id).copied() {
                        self.delayed.push(DelayedDispatch {
                            ready_at: now + delay,
                            dispatch,
                        });
                    } else {
                        self.execute(dispatch)?;
                    }
                }
                BeginDispatch::Skipped(_) => {}
                BeginDispatch::WorkersOccupied
                | BeginDispatch::PassComplete(_)
                | BeginDispatch::NoPass => break,
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
        let managed = self
            .host
            .execute_dispatch(dispatch)
            .map_err(|rejected| format!("dispatch validation failed: {:?}", rejected.error))?;
        let group_id = managed.group_id;
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
                    let kind = error.kind();
                    if kind == MultiRaftErrorKind::DriverPoisoned {
                        self.poisoned.insert(group_id);
                    }
                    match work_kind {
                        Some(WorkKind::Proposal(proposal_id)) => {
                            self.complete_unknown(
                                proposal_id,
                                &format!("managed group failed: {kind:?}"),
                            );
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
        Ok(())
    }

    fn collect_report(&mut self, group_id: GroupId, report: Report) -> Result<(), String> {
        let incarnation = self
            .groups
            .get(&group_id)
            .ok_or_else(|| "report named an unknown group".to_string())?
            .record
            .policy()
            .incarnation;
        for envelope in report.peer_messages {
            let _ = self.link.send(PeerFrame {
                group_id,
                incarnation,
                from: envelope.from,
                to: envelope.to,
                message: envelope.message,
            });
        }
        for event in report.proposal_events {
            match event {
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
            }
        }
        for applied in report.applied {
            if let Some(proposal_id) = applied.local_proposal_id {
                self.complete_applied(proposal_id, applied.result);
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
        if let Some((request, command)) = pending.request {
            self.groups[&pending.group_id]
                .record
                .cancel_reservation(request, command)
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
        for reply in pending.replies {
            reply.send(format!("ERR UNKNOWN {detail}"), false);
        }
    }

    fn take_pending(&mut self, proposal_id: LocalProposalId) -> Option<PendingClient> {
        let pending = self.pending.remove(&proposal_id)?;
        if let Some((request, _)) = pending.request {
            self.pending_requests
                .remove(&(pending.group_id, request.client_id));
        }
        Some(pending)
    }

    fn expire_clients(&mut self, now: Instant) {
        let expired = self
            .pending
            .iter()
            .filter_map(|(id, pending)| (now >= pending.deadline).then_some(*id))
            .collect::<Vec<_>>();
        for proposal_id in expired {
            self.complete_unknown(proposal_id, "client deadline elapsed");
        }
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
        let policy = entry.record.policy();
        if policy.incarnation != incarnation {
            return Ok(format!(
                "ERR INCARNATION current={}",
                policy.incarnation.get()
            ));
        }
        if policy.lifecycle != GroupLifecycle::Serving {
            return Ok(format!("ERR LIFECYCLE {:?}", policy.lifecycle));
        }
        entry
            .record
            .set_lifecycle(GroupLifecycle::Draining)
            .map_err(|error| error.to_string())?;
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
        let removed = match self.host.remove_group(&group_id) {
            Ok(Some(driver)) => driver,
            Ok(None) => return Ok("ERR GROUP_NOT_OPEN".to_string()),
            Err(error) => return Ok(format!("ERR BUSY {error:?}")),
        };
        drop(removed);
        let entry = self
            .groups
            .get_mut(&group_id)
            .expect("group entry survives managed removal");
        drop(entry.driver.take());
        entry
            .record
            .retire(GroupLifecycle::Removed)
            .map_err(|error| error.to_string())?;
        let raft = entry.directory.join("raft");
        let retired = entry
            .directory
            .join(format!("raft.retired-{}", incarnation.get()));
        if raft.exists() {
            fs::rename(&raft, &retired).map_err(|error| {
                format!(
                    "could not archive {} as {}: {error}",
                    raft.display(),
                    retired.display()
                )
            })?;
        }
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
        entry
            .record
            .reopen(quota, self.max_sessions)
            .map_err(|error| error.to_string())?;
        let new_incarnation = entry.record.policy().incarnation;
        let opened = self.open_physical(&directory, group_id, quota)?;
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
        Ok(format!("OK TOMBSTONE group={}", group_id.get()))
    }

    fn all_active_ready(&self) -> bool {
        self.groups.values().all(|entry| {
            let lifecycle = entry.record.policy().lifecycle;
            if matches!(
                lifecycle,
                GroupLifecycle::Removed | GroupLifecycle::Tombstoned
            ) {
                return true;
            }
            entry.driver.as_ref().is_some_and(SharedGroup::is_ready)
        })
    }

    fn active_group_count(&self) -> usize {
        self.groups
            .values()
            .filter(|entry| entry.driver.is_some())
            .count()
    }

    fn status_line(&self) -> String {
        let metrics = self.host.managed_metrics();
        let raft = self.host.raft_metrics();
        let leaders = raft
            .groups
            .iter()
            .filter(|metrics| metrics.role == Role::Leader)
            .count();
        let leader_groups = raft
            .groups
            .iter()
            .filter(|metrics| metrics.role == Role::Leader)
            .map(|metrics| metrics.group_id.get().to_string())
            .collect::<Vec<_>>()
            .join(",");
        let leader_groups = if leader_groups.is_empty() {
            "-"
        } else {
            &leader_groups
        };
        let poisoned = raft
            .groups
            .iter()
            .filter(|metrics| {
                matches!(
                    metrics.fatal_state,
                    rafter_app::group::GroupFatalState::Poisoned { .. }
                )
            })
            .count();
        let link = self.link.counters();
        format!(
            "OK STATUS ready={} groups={} leaders={} leader_groups={} poisoned={} queued={} \
             in_flight={} workers={} admitted={} serviced={} failed={} passes={} refused_peer={} \
             link_outbound_full={} link_inbound_full={} link_malformed={} \
             link_identity_refused={} link_inbound_connection_full={}",
            self.all_active_ready(),
            metrics.groups,
            leaders,
            leader_groups,
            poisoned,
            metrics.queued,
            metrics.in_flight_work,
            metrics.occupied_workers,
            metrics.admitted,
            metrics.serviced,
            metrics.failed,
            metrics.passes_completed,
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
            "OK AUDIT plans={} opportunities={} coverage={} widest_gap={} invalid_plans={} \
             invalid_turns={} plan_digest={:016x} turn_digest={:016x} admitted={} serviced={} \
             failed={} queued={} in_flight={} conserved={conserved}",
            self.audit.plans,
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
        for (_, pending) in std::mem::take(&mut self.pending) {
            for reply in pending.replies {
                reply.send("ERR UNKNOWN process shutting down".to_string(), false);
            }
        }
        self.pending_requests.clear();
        let audit = self.audit_line();
        let status = self.status_line();
        super::emit(&format!("FINAL {} {status}", self.node_id.0));
        super::emit(&format!("FINAL {} {audit}", self.node_id.0));
        self.link.shut_down();
        super::emit(&format!("STOPPED {}", self.node_id.0));
    }
}

fn cached_result(
    driver: &SharedGroup,
    request: RequestIdentity,
    command: CounterCommand,
) -> Option<CounterResult> {
    driver
        .view()
        .sessions
        .into_iter()
        .find(|session| {
            session.client_id == request.client_id && session.epoch == request.session_epoch
        })
        .and_then(|session| session.completed)
        .filter(|completed| completed.sequence == request.sequence && completed.command == command)
        .map(|completed| completed.result)
}

fn render_apply_result(result: CounterApplyResult) -> String {
    match result {
        CounterApplyResult::Session(session) => match session {
            SessionApplyResult::Opened => "OK SESSION opened".to_string(),
            SessionApplyResult::AlreadyOpen => "OK SESSION already_open".to_string(),
            SessionApplyResult::Replaced => "OK SESSION replaced".to_string(),
        },
        CounterApplyResult::Counter(result) => format!("OK {}", render_counter_result(result)),
        CounterApplyResult::Rejected(rejection) => match rejection {
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
        },
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
