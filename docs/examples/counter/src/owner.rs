//! The main thread uniquely owns both durable workers and drives bounded turns.
use crate::{
    application::Entry, counter::Counter, disk, peers, policy, Driver, Result, Runtime, GROUP,
};
use rafter::{Input, NodeConfig, Output};
use rafter_app::state_machine::{ApplicationSnapshot, ReplicatedStateMachine};
use rafter_runtime::{
    application::ApplicationWorkerOptions,
    service::{ServiceEvent, ServiceOptions, ServicePoll, ServiceState, ServiceStepError},
    DurableRaftNodeStorage,
};
use rafter_service::{
    durable::{
        DurableClientOptions, DurableClientOwner, DurableClientStepError, DurableReadLimits,
        DurableWriteLimits,
    },
    validate_inbound_peer_envelope, DriverCommandSender, PeerControlPlaneCheckpoint,
};
use rafter_storage::{FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore};
use rafter_transport_tls::{TlsPeerDirectory, TlsSender};
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

pub fn recover(
    config: NodeConfig,
    storage: DurableRaftNodeStorage<
        FileRaftHardStateStore,
        FileRaftLogSegment,
        FileRaftSnapshotStore,
    >,
    app: Counter,
    sender: TlsSender<u64, peers::GroupCodec>,
    checkpoint: PeerControlPlaneCheckpoint<u64>,
) -> Result<Driver> {
    let mut service = ServiceOptions::new();
    service.application = ApplicationWorkerOptions::new().with_limits(64, 16_384, 16, 4096);
    service.max_proposals = 8;
    service.max_proposal_bytes = 64;
    let options = DurableClientOptions::new(
        service,
        DurableWriteLimits {
            writes: 8.try_into()?,
            payload_bytes: 64.try_into()?,
        },
        DurableReadLimits {
            reads: 8.try_into()?,
            payload_bytes: 8.try_into()?,
        },
    );
    let wake = thread::current();
    let owner = DurableClientOwner::recover_with_reads(
        config,
        storage,
        app,
        options,
        Entry::try_from,
        |counter, _query| counter.value(),
        move || wake.unpark(),
    )?;
    let node = owner
        .ready_node()
        .ok_or("recovery has no observable node")?;
    validate_node(node)?;
    policy::restore(
        &checkpoint,
        node.commit_index(),
        node.last_log_index(),
        node.current_term(),
    )?;
    let mut driver =
        Driver::attach(GROUP, owner, sender).map_err(|error| disk::invalid(&error.to_string()))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while driver.state() != ServiceState::Ready {
        turn(&mut driver)?;
        if Instant::now() >= deadline {
            return Err("counter recovery timed out".into());
        }
        thread::park_timeout(Duration::from_millis(1));
    }
    Ok(driver)
}

fn validate_node(node: &Runtime) -> Result<()> {
    policy::membership(&node.effective_membership())?;
    policy::membership(&node.committed_membership())?;
    if let Some(membership) = node.snapshot_committed_membership() {
        policy::membership(&membership)?;
    }
    Ok(())
}

fn turn(driver: &mut Driver) -> Result<bool> {
    for _ in 0..64 {
        if let Some(node) = driver.ready_node() {
            validate_node(node)?;
        }
        match driver.poll_next()? {
            ServicePoll::Pending => return Ok(false),
            ServicePoll::Event(ServiceEvent::InstallSnapshot {
                snapshot,
                mut application,
            }) => {
                policy::membership(
                    snapshot
                        .metadata
                        .committed_membership()
                        .ok_or("counter snapshot has no committed membership")?,
                )?;
                application.install_snapshot(ApplicationSnapshot {
                    applied_index: snapshot.metadata.last_included_index,
                    payload: Vec::new(),
                    raft_snapshot: Some(snapshot),
                })?;
                driver.resume_snapshot(application)?;
            }
            ServicePoll::Event(ServiceEvent::Output(Output::ConfigurationCommitted {
                previous,
                configuration,
                ..
            })) => {
                // Check every crossing, including an intermediate removal that
                // a later endpoint in the same Raft step would otherwise hide.
                policy::membership(&previous)?;
                policy::membership(&configuration.membership_config())?;
            }
            ServicePoll::Event(ServiceEvent::ApplicationFailed(failure)) => {
                return Err(format!("counter application failed: {:?}", failure.kind()).into());
            }
            ServicePoll::Progress | ServicePoll::Event(_) => {}
            _ => return Err("unsupported durable service poll state".into()),
        }
    }
    Ok(true)
}

fn step(driver: &mut Driver, input: Input) -> Result<()> {
    match driver.try_step(input) {
        Ok(()) => Ok(()),
        // HTTP may close admission between the owner's Ready check and input.
        // A refused tick/peer message during shutdown needs no further delivery.
        Err(DurableClientStepError::Service(ServiceStepError::Unavailable {
            state: ServiceState::Draining,
            ..
        })) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub fn drive(
    driver: &mut Driver,
    transport: &peers::Transport,
    validator: &TlsPeerDirectory<u64>,
    stop: &AtomicBool,
    ready: &AtomicBool,
    connections: &AtomicUsize,
    snapshots: &mut crate::snapshots::Snapshots,
) -> Result<()> {
    let inbound = transport.inbound();
    let mut next_tick = Instant::now();
    let mut shutdown_requested = false;
    let mut draining_reported = false;
    loop {
        if stop.load(Ordering::Acquire) && !shutdown_requested {
            // The sender requests shutdown immediately; canceling its waiter
            // does not cancel the drain. Continue driving until Stopped.
            drop(driver.sender().shutdown(GROUP));
            shutdown_requested = true;
        }
        let reschedule = turn(driver)?;
        snapshots.maintain(driver)?;
        if driver.state() == ServiceState::Draining && !draining_reported {
            eprintln!(
                "Node {}: draining durable work",
                driver.handle().metrics()?.current().node_id.0
            );
            draining_reported = true;
        }
        if driver.state() == ServiceState::Stopped {
            return Ok(());
        }
        if driver.state() == ServiceState::Ready && Instant::now() >= next_tick {
            step(driver, Input::Tick)?;
            next_tick = Instant::now() + Duration::from_millis(20);
        }
        for _ in 0..64 {
            if driver.state() != ServiceState::Ready {
                break;
            }
            let Some(envelope) = inbound.drain(1)?.pop() else {
                break;
            };
            if envelope.group_id != GROUP {
                continue;
            }
            let id = driver
                .ready_node()
                .ok_or("ready driver lost node ownership")?
                .id();
            if let Ok(envelope) = validate_inbound_peer_envelope(envelope, id, validator) {
                step(
                    driver,
                    Input::Message {
                        from: envelope.from,
                        message: envelope.message,
                    },
                )?;
            }
        }
        ready.store(
            matches!(driver.state(), ServiceState::Ready | ServiceState::Busy),
            Ordering::Release,
        );
        if let Some(error) = transport.terminal_failure() {
            return Err(error.into());
        }
        let diagnostics = transport.diagnostics();
        connections.store(
            diagnostics.active_inbound_connections + diagnostics.active_outbound_connections,
            Ordering::Release,
        );
        if !reschedule {
            thread::park_timeout(Duration::from_millis(2));
        }
    }
}
