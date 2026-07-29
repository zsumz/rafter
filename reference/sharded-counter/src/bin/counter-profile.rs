//! Replayable long profiles for the real Rafter-backed counter adapter.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Write as _,
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::ExitCode,
    time::{Duration, Instant},
};

use rafter::LocalProposalId;
use rafter_multiraft::managed::{ManagedConfig, WorkClass};
use rafter_reference_sharded_counter::{
    adapter::{
        audit_acceptance, AcceptanceExpectation, CounterApplyResult, CounterSubmitOutcome,
        DriveReport, ExpectedWork, ManagedCounterCluster, NetworkConfig, ProposalReceipt,
        SessionSubmitOutcome,
    },
    ClientId, CounterCommand, CounterResult, Delta, GroupId, GroupIncarnation, LifecycleOutcome,
    LifecycleRejection, LifecycleRequest, RequestFingerprint, RequestIdentity, Sequence,
    SessionEpoch, SystemClass, WorkQuota,
};

fn main() -> ExitCode {
    let config = match Config::parse() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("counter profile configuration failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = fs::create_dir_all(&config.artifacts) {
        eprintln!(
            "could not create artifact directory {}: {error}",
            config.artifacts.display()
        );
        return ExitCode::FAILURE;
    }
    config.write_replay_inputs();
    let started = Instant::now();
    match run(&config) {
        Ok(artifacts) if started.elapsed() <= config.profile.budget => {
            artifacts.write(&config.artifacts);
            fs::write(config.artifacts.join("minimized-failure.txt"), "none\n")
                .expect("success marker writes");
            println!(
                "counter profile {} seed={} groups={} elapsed_ms={} artifacts={}",
                config.profile.name,
                config.seed,
                config.profile.groups,
                started.elapsed().as_millis(),
                config.artifacts.display()
            );
            ExitCode::SUCCESS
        }
        Ok(artifacts) => {
            artifacts.write(&config.artifacts);
            let failure = format!(
                "budget exceeded: elapsed_ms={} budget_ms={}\nreplay_history={}\ncommand={}\n",
                started.elapsed().as_millis(),
                config.profile.budget.as_millis(),
                config.artifacts.join("replay-history.tsv").display(),
                config.command
            );
            fs::write(config.artifacts.join("minimized-failure.txt"), &failure)
                .expect("budget failure writes");
            eprintln!("{failure}");
            ExitCode::FAILURE
        }
        Err(error) => {
            fs::write(
                config.artifacts.join("minimized-failure.txt"),
                format!(
                    "{error}\nreplay_history={}\ncommand={}\n",
                    config.artifacts.join("replay-history.tsv").display(),
                    config.command
                ),
            )
            .expect("failure artifact writes");
            eprintln!("counter profile failed: {error}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Profile {
    name: &'static str,
    groups: u32,
    hot_groups: u32,
    burst: u32,
    pressure_groups: u32,
    slow_groups: u32,
    workers: usize,
    budget: Duration,
    default_seed: u64,
}

impl Profile {
    const FAST: Self = Self {
        name: "counter-fast",
        groups: 64,
        hot_groups: 8,
        burst: 4,
        pressure_groups: 8,
        slow_groups: 2,
        workers: 8,
        budget: Duration::from_secs(30),
        default_seed: 0x6d5a_56da_6a51_7472,
    };
    const NIGHTLY: Self = Self {
        name: "counter-nightly",
        groups: 1_024,
        hot_groups: 128,
        burst: 8,
        pressure_groups: 128,
        slow_groups: 16,
        workers: 32,
        budget: Duration::from_secs(240),
        default_seed: 0x29cc_8b5d_ea77_0021,
    };
    const WEEKLY: Self = Self {
        name: "counter-weekly",
        groups: 4_096,
        hot_groups: 512,
        burst: 8,
        pressure_groups: 512,
        slow_groups: 64,
        workers: 64,
        budget: Duration::from_secs(1_200),
        default_seed: 0xd0e8_9d2d_311e_9f41,
    };

    fn named(name: &str) -> Option<Self> {
        match name {
            "counter-fast" => Some(Self::FAST),
            "counter-nightly" => Some(Self::NIGHTLY),
            "counter-weekly" => Some(Self::WEEKLY),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct Config {
    profile: Profile,
    seed: u64,
    artifacts: PathBuf,
    command: String,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let arguments = env::args().collect::<Vec<_>>();
        let value = |name: &str| -> Result<&str, String> {
            arguments
                .windows(2)
                .find(|pair| pair[0] == name)
                .map(|pair| pair[1].as_str())
                .ok_or_else(|| format!("missing required argument {name}"))
        };
        let profile_name = value("--profile")?;
        let profile = Profile::named(profile_name).ok_or_else(|| {
            format!(
                "unknown profile {profile_name:?}; expected counter-fast, counter-nightly, or \
                 counter-weekly"
            )
        })?;
        let seed = arguments
            .windows(2)
            .find(|pair| pair[0] == "--seed")
            .map_or(Ok(profile.default_seed), |pair| {
                pair[1].parse().map_err(|_| "seed is not a u64".to_string())
            })?;
        Ok(Self {
            profile,
            seed,
            artifacts: PathBuf::from(value("--artifacts")?),
            command: arguments.join(" "),
        })
    }

    fn write_replay_inputs(&self) {
        fs::write(self.artifacts.join("seed.txt"), format!("{}\n", self.seed))
            .expect("seed artifact writes");
        fs::write(
            self.artifacts.join("command.txt"),
            format!("{}\n", self.command),
        )
        .expect("command artifact writes");
        fs::write(
            self.artifacts.join("replay-history.tsv"),
            self.replay_history(),
        )
        .expect("replay history writes");
        fs::write(
            self.artifacts.join("profile.txt"),
            format!(
                "name={}\ngroups={}\nhot_groups={}\nburst={}\npressure_groups={}\n\
                 slow_groups={}\nworkers={}\nbudget_ms={}\n",
                self.profile.name,
                self.profile.groups,
                self.profile.hot_groups,
                self.profile.burst,
                self.profile.pressure_groups,
                self.profile.slow_groups,
                self.profile.workers,
                self.profile.budget.as_millis()
            ),
        )
        .expect("profile artifact writes");
    }

    fn replay_history(&self) -> String {
        let mut replay = "kind\tgroup\tclient\tsequence\tdetail\n".to_string();
        let mut rng = Rng::new(self.seed);
        for group in 1..=self.profile.groups {
            writeln!(
                replay,
                "add\t{group}\t0\t1\tdelta={}",
                random_delta(&mut rng)
            )
            .expect("string writes do not fail");
        }
        for offset in 0..self.profile.hot_groups {
            let group = selected_group(self.profile.groups, self.seed, offset, 7_919);
            for client in 1..=self.profile.burst {
                writeln!(
                    replay,
                    "add\t{}\t{client}\t1\tdelta={}",
                    group.get(),
                    random_delta(&mut rng)
                )
                .expect("string writes do not fail");
            }
        }
        for offset in 0..self.profile.pressure_groups {
            let group =
                selected_group(self.profile.groups, self.seed.rotate_left(7), offset, 7_291);
            writeln!(replay, "pressure\t{}\t-\t-\tclass=snapshot", group.get())
                .expect("string writes do not fail");
            writeln!(replay, "pressure\t{}\t-\t-\tclass=bulk", group.get())
                .expect("string writes do not fail");
        }
        for offset in 0..self.profile.slow_groups {
            let group = selected_group(
                self.profile.groups,
                self.seed.rotate_left(13),
                offset,
                5_741,
            );
            writeln!(
                replay,
                "slow\t{}\t-\t-\trounds={}",
                group.get(),
                2 + offset % 3
            )
            .expect("string writes do not fail");
        }
        let fault = selected_group(self.profile.groups, self.seed.rotate_left(19), 0, 1);
        writeln!(
            replay,
            "fault\t{}\t-\t-\tthen=drain,remove,reopen,recover,serve",
            fault.get()
        )
        .expect("string writes do not fail");
        let tombstone = selected_group(self.profile.groups, self.seed.rotate_left(23), 1, 1);
        writeln!(
            replay,
            "lifecycle\t{}\t-\t-\tdrain,remove,tombstone,recreate-refusal",
            tombstone.get()
        )
        .expect("string writes do not fail");
        replay
    }
}

#[derive(Debug)]
struct HistoryEntry {
    proposal: LocalProposalId,
    group: GroupId,
    client: ClientId,
    sequence: Sequence,
    delta: i64,
    expected: i64,
}

#[derive(Debug, Default)]
struct Workload {
    sequences: BTreeMap<(GroupId, ClientId), u64>,
    expected_values: BTreeMap<GroupId, i64>,
    history: Vec<HistoryEntry>,
    accepted: Vec<ExpectedWork>,
}

#[derive(Debug, Default)]
struct Artifacts {
    history: String,
    fairness: String,
    conservation: String,
    lifecycle: String,
    summary: String,
}

impl Artifacts {
    fn write(&self, directory: &Path) {
        for (name, content) in [
            ("history.tsv", &self.history),
            ("fairness.txt", &self.fairness),
            ("queue-conservation.txt", &self.conservation),
            ("lifecycle.txt", &self.lifecycle),
            ("summary.txt", &self.summary),
        ] {
            fs::write(directory.join(name), content)
                .unwrap_or_else(|error| panic!("could not write {name}: {error}"));
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run(config: &Config) -> Result<Artifacts, String> {
    let groups_usize =
        usize::try_from(config.profile.groups).map_err(|_| "group count does not fit usize")?;
    let global_bound = groups_usize
        .checked_mul(16)
        .and_then(|bound| bound.checked_add(128))
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| "global queue bound overflowed".to_string())?;
    let managed = ManagedConfig::new(
        nonzero(config.profile.workers)?,
        nonzero(64)?,
        global_bound,
        nonzero(4)?,
    )
    .map_err(|error| error.to_string())?;
    let network = NetworkConfig {
        max_pending_messages: nonzero(
            groups_usize
                .checked_mul(128)
                .ok_or_else(|| "network bound overflowed".to_string())?,
        )?,
        max_sessions_per_group: nonzero(
            usize::try_from(config.profile.burst)
                .map_err(|_| "burst width does not fit usize")?
                .saturating_add(1),
        )?,
    };
    let mut cluster = ManagedCounterCluster::new(managed, network);
    let group_ids = (1..=config.profile.groups)
        .map(GroupId::new)
        .collect::<Vec<_>>();
    let hot_groups = (0..config.profile.hot_groups)
        .map(|offset| selected_group(config.profile.groups, config.seed, offset, 7_919))
        .collect::<Vec<_>>();
    for group in &group_ids {
        cluster
            .register_group(*group, nonzero(4)?)
            .map_err(|error| error.to_string())?;
        cluster
            .recover_group(*group)
            .map_err(|error| error.to_string())?;
    }
    cluster
        .drive_until_idle(round_budget(config.profile.groups))
        .map_err(|error| error.to_string())?;
    for group in &group_ids {
        cluster
            .serve_group(*group)
            .map_err(|error| error.to_string())?;
        open_profile_session(&mut cluster, *group, ClientId::new(0))?;
    }
    for group in &hot_groups {
        for client in 1..=config.profile.burst {
            open_profile_session(&mut cluster, *group, ClientId::new(client))?;
        }
    }
    cluster
        .drive_until_idle(round_budget(config.profile.groups))
        .map_err(|error| error.to_string())?;

    let mut rng = Rng::new(config.seed);
    let mut workload = Workload::default();
    for group in &group_ids {
        admit_add(
            &mut cluster,
            *group,
            ClientId::new(0),
            random_delta(&mut rng),
            &mut workload,
        )?;
    }
    let baseline_expectation = AcceptanceExpectation {
        ready: group_ids.clone(),
        accepted: std::mem::take(&mut workload.accepted),
        quotas: group_ids.iter().map(|group| (*group, 4)).collect(),
    };
    let baseline_report = cluster
        .drive_until_idle(round_budget(config.profile.groups))
        .map_err(|error| error.to_string())?;
    let baseline_audit =
        audit_acceptance(&baseline_expectation, &baseline_report, &cluster.metrics())
            .map_err(|error| format!("baseline acceptance audit failed: {error:?}"))?;
    check_history(&cluster, &workload.history)?;

    for group in hot_groups {
        for client in 1..=config.profile.burst {
            admit_add(
                &mut cluster,
                group,
                ClientId::new(client),
                random_delta(&mut rng),
                &mut workload,
            )?;
        }
    }
    for offset in 0..config.profile.pressure_groups {
        let group = selected_group(
            config.profile.groups,
            config.seed.rotate_left(7),
            offset,
            7_291,
        );
        for class in [SystemClass::Snapshot, SystemClass::Bulk] {
            let receipt = cluster
                .submit_system(group, GroupIncarnation::first(), class)
                .map_err(|error| format!("pressure admission failed: {error:?}"))?;
            workload.accepted.push(ExpectedWork {
                work_id: receipt.work_id,
                group_id: group,
                class: managed_class(class),
            });
        }
    }
    for offset in 0..config.profile.slow_groups {
        let group = selected_group(
            config.profile.groups,
            config.seed.rotate_left(13),
            offset,
            5_741,
        );
        cluster.set_service_delay(group, 2 + usize::try_from(offset % 3).unwrap_or(0));
    }

    let report = cluster
        .drive_until_idle(round_budget(config.profile.groups))
        .map_err(|error| error.to_string())?;
    let mixed_audit = audit_mixed(&workload.accepted, &report)?;
    check_history(&cluster, &workload.history)?;

    let fault_group = selected_group(config.profile.groups, config.seed.rotate_left(19), 0, 1);
    let fault = cluster
        .submit_fault(fault_group, SystemClass::Control)
        .map_err(|error| format!("fault admission failed: {error:?}"))?;
    cluster
        .drive_until_idle(round_budget(config.profile.groups))
        .map_err(|error| error.to_string())?;
    if !cluster.is_poisoned(fault_group) || cluster.completed(fault.proposal_id).is_some() {
        return Err(format!(
            "fault group {} did not publish poison and suppress completion",
            fault_group.get()
        ));
    }

    let mut lifecycle = String::new();
    lifecycle_churn(&mut cluster, fault_group, &mut lifecycle)?;
    let tombstone_group = selected_group(config.profile.groups, config.seed.rotate_left(23), 1, 1);
    lifecycle_tombstone(&mut cluster, tombstone_group, &mut lifecycle)?;

    let metrics = cluster.metrics();
    let accounted = metrics
        .serviced
        .saturating_add(metrics.failed)
        .saturating_add(u64::try_from(metrics.queued).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(metrics.in_flight_work).unwrap_or(u64::MAX));
    if metrics.admitted != accounted || metrics.occupied_workers != 0 {
        return Err(format!("final queue conservation failed: {metrics:?}"));
    }

    let mut history_artifact =
        "proposal\tgroup\tclient\tsequence\tdelta\texpected_value\tobserved\n".to_string();
    for entry in &workload.history {
        writeln!(
            history_artifact,
            "{}\t{}\t{}\t{}\t{}\t{}\tmatch",
            entry.proposal.0,
            entry.group.get(),
            entry.client.get(),
            entry.sequence.get(),
            entry.delta,
            entry.expected
        )
        .expect("string writes do not fail");
    }
    Ok(Artifacts {
        history: history_artifact,
        fairness: format!(
            "baseline_passes={}\nbaseline_opportunities={}\nbaseline_ready_width={}\n\
             baseline_widest_gap={}\nmixed_passes={}\nmixed_opportunities={}\n\
             mixed_ready_width={}\nmixed_widest_gap={}\nmixed_observed_work={}\n",
            baseline_audit.passes,
            baseline_audit.opportunities,
            baseline_audit.ready_width,
            baseline_audit.widest_gap,
            mixed_audit.passes,
            mixed_audit.opportunities,
            mixed_audit.ready_width,
            mixed_audit.widest_gap,
            mixed_audit.observed_work
        ),
        conservation: format!(
            "admitted={}\nserviced={}\nfailed={}\nqueued={}\nin_flight={}\n\
             occupied_workers={}\nconserved=true\n",
            metrics.admitted,
            metrics.serviced,
            metrics.failed,
            metrics.queued,
            metrics.in_flight_work,
            metrics.occupied_workers
        ),
        lifecycle,
        summary: format!(
            "status=green\nprofile={}\nseed={}\ngroups={}\nhistory_entries={}\n\
             completed_proposals={}\n",
            config.profile.name,
            config.seed,
            config.profile.groups,
            workload.history.len(),
            cluster.completed_proposals().count()
        ),
    })
}

#[derive(Clone, Copy, Debug)]
struct MixedAudit {
    passes: usize,
    opportunities: usize,
    ready_width: usize,
    widest_gap: usize,
    observed_work: usize,
}

fn audit_mixed(accepted: &[ExpectedWork], report: &DriveReport) -> Result<MixedAudit, String> {
    if accepted.is_empty() {
        return Err("mixed profile accepted no work".to_string());
    }
    let expected = accepted
        .iter()
        .map(|work| (work.work_id, *work))
        .collect::<BTreeMap<_, _>>();
    let ready = accepted
        .iter()
        .map(|work| work.group_id)
        .collect::<BTreeSet<_>>();
    let expected_plan = ready.iter().copied().collect::<Vec<_>>();
    let actual_plan = report
        .plans
        .first()
        .ok_or_else(|| "mixed profile armed no pass".to_string())?;
    if actual_plan != &expected_plan {
        return Err(format!(
            "mixed first plan mismatch: expected {expected_plan:?}, observed {actual_plan:?}"
        ));
    }
    let first_pass = report
        .turns
        .first()
        .ok_or_else(|| "mixed profile dispatched no turns".to_string())?
        .pass_id;
    let mut first_opportunities = BTreeSet::new();
    let mut observed = BTreeSet::new();
    for turn in &report.turns {
        if turn.pass_id == first_pass && !first_opportunities.insert(turn.group_id) {
            return Err(format!(
                "mixed first pass offered group {} twice",
                turn.group_id.get()
            ));
        }
        if turn
            .items
            .windows(2)
            .any(|pair| pair[0].class > pair[1].class)
        {
            return Err(format!(
                "mixed turn for group {} violated class order",
                turn.group_id.get()
            ));
        }
        for item in &turn.items {
            let Some(expected_work) = expected.get(&item.work_id) else {
                continue;
            };
            if expected_work.group_id != turn.group_id || expected_work.class != item.class {
                return Err(format!(
                    "mixed work {:?} changed route or class",
                    item.work_id
                ));
            }
            if !observed.insert(item.work_id) {
                return Err(format!("mixed work {:?} was serviced twice", item.work_id));
            }
        }
    }
    if observed.len() != accepted.len() {
        return Err(format!(
            "mixed work disappeared: accepted={}, observed={}",
            accepted.len(),
            observed.len()
        ));
    }
    let missing = ready
        .iter()
        .filter(|group| !first_opportunities.contains(group))
        .count();
    if missing != 0 {
        return Err(format!(
            "mixed first pass omitted {missing} continuously ready groups"
        ));
    }
    Ok(MixedAudit {
        passes: report.plans.len(),
        opportunities: first_opportunities.len(),
        ready_width: ready.len(),
        widest_gap: missing,
        observed_work: observed.len(),
    })
}

fn admit_add(
    cluster: &mut ManagedCounterCluster,
    group: GroupId,
    client: ClientId,
    delta: i64,
    workload: &mut Workload,
) -> Result<(), String> {
    let sequence = workload.sequences.entry((group, client)).or_default();
    *sequence += 1;
    let sequence = Sequence::new(*sequence).expect("profile sequence is nonzero");
    let command = CounterCommand::Add {
        delta: Delta::new(delta).expect("profile delta is nonzero"),
    };
    let request = RequestIdentity {
        client_id: client,
        session_epoch: epoch(),
        sequence,
        fingerprint: RequestFingerprint::of(&command),
    };
    let receipt = match cluster
        .submit(group, request, command)
        .map_err(|error| format!("counter admission failed: {error:?}"))?
    {
        CounterSubmitOutcome::Queued(receipt) => receipt,
        other => return Err(format!("fresh profile request did not queue: {other:?}")),
    };
    let expected = workload.expected_values.entry(group).or_default();
    *expected += delta;
    workload.history.push(HistoryEntry {
        proposal: receipt.proposal_id,
        group,
        client,
        sequence,
        delta,
        expected: *expected,
    });
    workload.accepted.push(expected_command(receipt, group));
    Ok(())
}

fn open_profile_session(
    cluster: &mut ManagedCounterCluster,
    group: GroupId,
    client: ClientId,
) -> Result<(), String> {
    match cluster
        .open_session(group, client, epoch())
        .map_err(|error| format!("session admission failed: {error:?}"))?
    {
        SessionSubmitOutcome::Queued(_) => Ok(()),
        other => Err(format!("fresh session was not queued: {other:?}")),
    }
}

fn expected_command(receipt: ProposalReceipt, group: GroupId) -> ExpectedWork {
    ExpectedWork {
        work_id: receipt.admission.work_id,
        group_id: group,
        class: WorkClass::Command,
    }
}

fn check_history(cluster: &ManagedCounterCluster, history: &[HistoryEntry]) -> Result<(), String> {
    if history.is_empty() {
        return Err("profile produced an empty history".to_string());
    }
    for entry in history {
        let expected = CounterApplyResult::Counter(CounterResult::Added {
            value: entry.expected,
        });
        let observed = cluster.completed(entry.proposal);
        if observed != Some(expected) {
            return Err(format!(
                "proposal {} group {} sequence {} expected {expected:?}, observed {observed:?}",
                entry.proposal.0,
                entry.group.get(),
                entry.sequence.get()
            ));
        }
    }
    Ok(())
}

fn lifecycle_churn(
    cluster: &mut ManagedCounterCluster,
    group: GroupId,
    artifact: &mut String,
) -> Result<(), String> {
    for request in [
        LifecycleRequest::Drain,
        LifecycleRequest::Remove,
        LifecycleRequest::Create {
            quota: WorkQuota::new(4).expect("profile quota is nonzero"),
        },
        LifecycleRequest::Recover,
    ] {
        let transition = cluster
            .lifecycle(group, request)
            .map_err(|error| error.to_string())?;
        writeln!(
            artifact,
            "group={} request={request:?} {transition:?}",
            group.get()
        )
        .expect("string writes do not fail");
        if matches!(transition.outcome, LifecycleOutcome::Rejected(_)) {
            return Err(format!(
                "lifecycle churn group {} rejected {request:?}: {transition:?}",
                group.get()
            ));
        }
    }
    cluster
        .drive_until_idle(256)
        .map_err(|error| error.to_string())?;
    let transition = cluster
        .lifecycle(group, LifecycleRequest::Serve)
        .map_err(|error| error.to_string())?;
    writeln!(
        artifact,
        "group={} request=Serve {transition:?}",
        group.get()
    )
    .expect("string writes do not fail");
    if matches!(transition.outcome, LifecycleOutcome::Rejected(_)) {
        return Err(format!("reopened group {} did not serve", group.get()));
    }
    Ok(())
}

fn lifecycle_tombstone(
    cluster: &mut ManagedCounterCluster,
    group: GroupId,
    artifact: &mut String,
) -> Result<(), String> {
    for request in [
        LifecycleRequest::Drain,
        LifecycleRequest::Remove,
        LifecycleRequest::Tombstone,
    ] {
        let transition = cluster
            .lifecycle(group, request)
            .map_err(|error| error.to_string())?;
        writeln!(
            artifact,
            "group={} request={request:?} {transition:?}",
            group.get()
        )
        .expect("string writes do not fail");
        if matches!(transition.outcome, LifecycleOutcome::Rejected(_)) {
            return Err(format!(
                "tombstone group {} rejected {request:?}: {transition:?}",
                group.get()
            ));
        }
    }
    let transition = cluster
        .lifecycle(
            group,
            LifecycleRequest::Create {
                quota: WorkQuota::new(4).expect("profile quota is nonzero"),
            },
        )
        .map_err(|error| error.to_string())?;
    if !matches!(
        transition.outcome,
        LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned)
    ) {
        return Err(format!(
            "tombstoned group {} accepted recreation: {transition:?}",
            group.get()
        ));
    }
    writeln!(
        artifact,
        "group={} request=CreateAfterTombstone {transition:?}",
        group.get()
    )
    .expect("string writes do not fail");
    Ok(())
}

const fn managed_class(class: SystemClass) -> WorkClass {
    match class {
        SystemClass::Control => WorkClass::Control,
        SystemClass::Snapshot => WorkClass::Snapshot,
        SystemClass::Bulk => WorkClass::Bulk,
    }
}

fn selected_group(groups: u32, seed: u64, offset: u32, stride: u32) -> GroupId {
    let start = u32::try_from(seed % u64::from(groups)).expect("remainder fits u32");
    GroupId::new(start.wrapping_add(offset.wrapping_mul(stride)) % groups + 1)
}

fn round_budget(groups: u32) -> usize {
    usize::try_from(groups)
        .unwrap_or(usize::MAX)
        .saturating_mul(4)
        .max(256)
}

fn random_delta(rng: &mut Rng) -> i64 {
    let magnitude = i64::try_from(rng.next() % 9 + 1).expect("small magnitude fits i64");
    if rng.next() & 1 == 0 {
        magnitude
    } else {
        -magnitude
    }
}

fn nonzero(value: usize) -> Result<NonZeroUsize, String> {
    NonZeroUsize::new(value).ok_or_else(|| "profile bound must be nonzero".to_string())
}

fn epoch() -> SessionEpoch {
    SessionEpoch::new(1).expect("profile epoch is nonzero")
}

#[derive(Debug)]
struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
