//! Bounded, domain-neutral process execution.
//!
//! `run` accepts a descriptor-bound request and returns raw execution facts.
//! Launch, observation, timeout escalation, cleanup, and telemetry retention
//! stay here; producer policy and evidence adaptation stay outside this domain.

mod anchor;
mod artifacts;
mod binding;
mod bounded;
mod command;
mod diagnostics;
mod direct_child;
mod environment;
mod finalization;
mod identity;
mod internal_command;
mod internal_process;
mod launch;
mod managed;
mod model;
mod output;
mod process_group;
mod reaper;
mod reaping;
mod signal;
mod spawn;
mod telemetry;
mod termination;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use anchor::ProcessGroupAnchor;
#[cfg(test)]
use anchor::{
    expire_next_anchor_readiness_classification, fail_next_anchor_startup,
    take_last_failed_anchor_startup_id, take_last_spawned_anchor_id,
};
use artifacts::ProcessArtifacts;
#[cfg(test)]
use artifacts::{allocate_process_artifacts_at, ProcessArtifactPaths};
pub(crate) use binding::{capture_runtime_identities, ExecutableIdentity, LauncherIdentity};
pub(crate) use bounded::run_bounded;
pub(crate) use command::BoundCommand;
pub(crate) use diagnostics::retained_diagnostics;
#[cfg(test)]
use diagnostics::{cleanup_error, retained_stderr_path};
use diagnostics::{measurement_error, retained_error, retained_result};
use direct_child::DirectChild;
pub(crate) use environment::base_environment;
use finalization::finalize_process_output;
pub(crate) use finalization::PendingProcessOutput;
pub(crate) use identity::run_identity_command_in;
#[cfg(test)]
use internal_command::{
    await_next_internal_completion_after_deadline, bounded_internal_output,
    bounded_internal_output_with_reaper, inject_next_internal_drain_error,
};
use internal_process::ManagedInternalProcess;
#[cfg(test)]
use launch::expose_next_target_lifetime_lease;
pub(crate) use launch::run;
#[cfg(test)]
use managed::force_next_cleanup_target_alive;
#[cfg(test)]
use managed::{before_next_wrapper_exit_observation, classify_target_quiescence_for_test};
use managed::{CleanupFailures, ManagedProcess};
#[cfg(test)]
use model::ProcessGroupState;
pub(crate) use model::{
    duration_ms, FinalizationPolicy, ProcessDeadlines, ProcessOutput, ProcessRequest,
    ProcessRuntime, RuntimeExecutable, TerminationPolicy,
};
use model::{
    ProcessAnchorState, ProcessCompletion, ProcessGroupObservation, ProcessSignal,
    ProcessTermination, SignalDelivery, TargetMemberState, PROCESS_POLL_INTERVAL,
};
use process_group::await_target_process_group;
#[cfg(test)]
use process_group::{
    delay_next_process_group_receipt, delay_next_target_release, parse_target_group_frame,
    take_last_delayed_process_group, take_last_unreleased_process_group,
    validate_ready_target_group_with, validate_target_group_candidate_with, TargetGroupFrame,
};
#[cfg(test)]
use reaper::fail_next_reaper_adoption;
use reaper::NoSignalReaper;
#[cfg(test)]
use signal::{
    classify_process_group_probe, classify_signal_delivery, clear_signal_attempts,
    confirm_process_group_absent_with, force_next_signal_group_absent, process_group_state,
    take_signal_attempts,
};
#[cfg(test)]
use spawn::{fail_next_process_lifetime_lease_creation, hold_lease_writer};
use spawn::{
    spawn_child, spawn_leased_child, ProcessLeaseState, ProcessLifetimeLease, TargetLeaseState,
    TargetLifetimeLease,
};
#[cfg(test)]
use telemetry::{
    delay_next_process_group_observation, omit_anchor_from_next_process_group_observation,
    omit_target_rows_from_process_group_observations, parse_process_group_observation,
    process_observer_path,
};
use telemetry::{parse_peak_rss, process_group_observation, ProcessObserver};
use termination::terminate_after_timeout;
#[cfg(test)]
use test_support::induce_fallback_cleanup_failure;
