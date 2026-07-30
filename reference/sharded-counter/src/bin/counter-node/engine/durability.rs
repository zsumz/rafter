//! Startup reconciliation and filesystem transaction helpers.

use std::{fs, path::Path};

use rafter_reference_sharded_counter::{GroupId, GroupIncarnation, GroupLifecycle};

use super::Engine;
use crate::{
    app_store::{ApplicationRecord, StoredPolicy},
    directed_failpoint,
    host_registry::{
        sync_directory, ActivationIntent, BootstrapIntent, HostRegistry, RetirementIntent,
        SlotRecord,
    },
};

impl Engine {
    pub(super) fn open_or_initialize_registry(
        &self,
        groups_dir: &Path,
    ) -> Result<HostRegistry, String> {
        if let Some(intent) = BootstrapIntent::load(groups_dir)? {
            if intent.group_count != self.group_count || intent.quota != self.default_quota {
                return Err(
                    "bootstrap intent conflicts with the configured group count or quota"
                        .to_string(),
                );
            }
            let registry = HostRegistry::open(groups_dir, self.group_count)?;
            return self.resume_bootstrap(groups_dir, intent, registry);
        }
        if let Some(registry) = HostRegistry::open(groups_dir, self.group_count)? {
            return Ok(registry);
        }
        let entries = fs::read_dir(groups_dir)
            .map_err(|error| format!("could not inspect {}: {error}", groups_dir.display()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("could not inspect {}: {error}", groups_dir.display()))?;
        if entries.is_empty() {
            return self.bootstrap_registry(groups_dir);
        }
        for entry in &entries {
            let name = entry.file_name();
            let Some(raw) = name.to_str().and_then(|name| name.parse::<u32>().ok()) else {
                return Err(format!(
                    "cannot initialize host registry with unexpected entry {}",
                    entry.path().display()
                ));
            };
            if !(1..=self.group_count).contains(&raw) {
                return Err(format!(
                    "cannot initialize host registry with unconfigured group directory {}",
                    entry.path().display()
                ));
            }
        }

        let mut slots = Vec::with_capacity(self.group_count as usize);
        for raw in 1..=self.group_count {
            let directory = groups_dir.join(raw.to_string());
            let (record, state_machine) =
                ApplicationRecord::open_existing(&directory.join("app"), self.max_sessions)
                    .map_err(|error| {
                        format!(
                            "cannot initialize host registry from existing groups: group {raw} \
                             has no complete application identity: {error}"
                        )
                    })?;
            drop(state_machine);
            let policy = record.policy();
            Self::validate_group_shape(
                &directory,
                GroupId::new(raw),
                policy.incarnation,
                policy.lifecycle,
            )?;
            slots.push(slot_from_policy(GroupId::new(raw), &policy));
        }
        HostRegistry::create(groups_dir, slots)
    }

    fn bootstrap_registry(&self, groups_dir: &Path) -> Result<HostRegistry, String> {
        let intent = BootstrapIntent {
            group_count: self.group_count,
            quota: self.default_quota,
        };
        directed_failpoint("before_bootstrap_intent_publication");
        intent.publish(groups_dir)?;
        directed_failpoint("after_bootstrap_intent_publication");
        self.resume_bootstrap(groups_dir, intent, None)
    }

    fn resume_bootstrap(
        &self,
        groups_dir: &Path,
        intent: BootstrapIntent,
        existing_registry: Option<HostRegistry>,
    ) -> Result<HostRegistry, String> {
        let slots = (1..=intent.group_count)
            .map(|raw| SlotRecord {
                group_id: GroupId::new(raw),
                incarnation: GroupIncarnation::first(),
                lifecycle: GroupLifecycle::Serving,
                quota: intent.quota,
            })
            .collect::<Vec<_>>();
        for raw in 1..=intent.group_count {
            let slot_dir = groups_dir.join(raw.to_string());
            fs::create_dir_all(&slot_dir)
                .map_err(|error| format!("could not create {}: {error}", slot_dir.display()))?;
            prepare_staged_raft(&slot_dir, GroupIncarnation::first(), true)?;
            let app_dir = slot_dir.join("app");
            if app_dir.join("state.rcap").exists() {
                let (record, state_machine) =
                    ApplicationRecord::open_existing(&app_dir, self.max_sessions)
                        .map_err(|error| format!("group {raw} bootstrap replay failed: {error}"))?;
                drop(state_machine);
                let policy = record.policy();
                if policy.incarnation != GroupIncarnation::first()
                    || policy.lifecycle != GroupLifecycle::Serving
                    || policy.poisoned
                    || policy.quota != intent.quota
                    || !policy.outstanding.is_empty()
                    || !policy.terminal.is_empty()
                {
                    return Err(format!(
                        "group {raw} conflicts with its durable bootstrap intent"
                    ));
                }
            } else {
                let _ = ApplicationRecord::bootstrap(&app_dir, self.max_sessions, intent.quota)
                    .map_err(|error| format!("group {raw} explicit bootstrap failed: {error}"))?;
                directed_failpoint("after_bootstrap_application_publication");
            }
        }
        let registry = if let Some(registry) = existing_registry {
            for slot in &slots {
                if registry.slot(slot.group_id)? != *slot {
                    return Err(format!(
                        "host registry conflicts with bootstrap intent for group {}",
                        slot.group_id.get()
                    ));
                }
            }
            registry
        } else {
            let registry = HostRegistry::create(groups_dir, slots)?;
            directed_failpoint("after_bootstrap_registry_publication");
            registry
        };
        for raw in 1..=intent.group_count {
            activate_staged_raft(
                &groups_dir.join(raw.to_string()),
                GroupIncarnation::first(),
                true,
            )?;
        }
        directed_failpoint("before_bootstrap_intent_cleanup");
        BootstrapIntent::clear(groups_dir)?;
        Ok(registry)
    }

    pub(super) fn reconcile_activation(
        &mut self,
        directory: &Path,
        group_id: GroupId,
    ) -> Result<(), String> {
        let Some(intent) = ActivationIntent::load(directory)? else {
            return Ok(());
        };
        if intent.group_id != group_id {
            return Err(format!(
                "group {} activation intent names group {}",
                group_id.get(),
                intent.group_id.get()
            ));
        }
        let durable = self
            .registry
            .as_ref()
            .expect("registry installed before activation reconciliation")
            .slot(group_id)?;
        let old_slot = SlotRecord {
            group_id,
            incarnation: intent.previous_incarnation,
            lifecycle: GroupLifecycle::Removed,
            quota: durable.quota,
        };
        let next_slot = SlotRecord {
            group_id,
            incarnation: intent.next_incarnation,
            lifecycle: GroupLifecycle::Serving,
            quota: intent.quota,
        };
        if durable != old_slot && durable != next_slot {
            return Err(format!(
                "group {} activation intent conflicts with the host registry",
                group_id.get()
            ));
        }
        prepare_staged_raft(directory, intent.next_incarnation, false)?;
        let (record, state_machine) =
            ApplicationRecord::open_existing(&directory.join("app"), self.max_sessions).map_err(
                |error| format!("group {} activation replay failed: {error}", group_id.get()),
            )?;
        drop(state_machine);
        let policy = record.policy();
        if policy.incarnation == intent.previous_incarnation
            && policy.lifecycle == GroupLifecycle::Removed
        {
            record
                .reopen(intent.quota, self.max_sessions)
                .map_err(|error| error.to_string())?;
        } else if policy.incarnation != intent.next_incarnation
            || policy.lifecycle != GroupLifecycle::Serving
            || policy.quota != intent.quota
        {
            return Err(format!(
                "group {} activation intent conflicts with the application record",
                group_id.get()
            ));
        }
        self.registry
            .as_mut()
            .expect("registry installed before activation reconciliation")
            .publish(next_slot)?;
        activate_staged_raft(directory, intent.next_incarnation, false)?;
        ActivationIntent::clear(directory)
    }

    pub(super) fn reconcile_retirement(
        &mut self,
        directory: &Path,
        group_id: GroupId,
        record: &ApplicationRecord,
    ) -> Result<(), String> {
        let Some(intent) = RetirementIntent::load(directory)? else {
            return Ok(());
        };
        let policy = record.policy();
        if intent.group_id != group_id || intent.incarnation != policy.incarnation {
            return Err(format!(
                "group {} retirement intent does not match application incarnation {}",
                group_id.get(),
                policy.incarnation.get()
            ));
        }
        if !matches!(
            policy.lifecycle,
            GroupLifecycle::Draining | GroupLifecycle::Removed
        ) {
            return Err(format!(
                "group {} retirement intent conflicts with {:?}",
                group_id.get(),
                policy.lifecycle
            ));
        }
        archive_raft(directory, intent.incarnation)?;
        if policy.lifecycle == GroupLifecycle::Draining {
            record
                .retire(GroupLifecycle::Removed)
                .map_err(|error| error.to_string())?;
        }
        self.registry
            .as_mut()
            .expect("registry installed before reconciliation")
            .publish(slot_from_policy(group_id, &record.policy()))?;
        RetirementIntent::clear(directory)
    }

    pub(super) fn reconcile_registry(
        &mut self,
        group_id: GroupId,
        policy: &StoredPolicy,
    ) -> Result<(), String> {
        let registry = self
            .registry
            .as_mut()
            .expect("registry installed before group reconciliation");
        let durable = registry.slot(group_id)?;
        let observed = slot_from_policy(group_id, policy);
        if durable == observed {
            return Ok(());
        }
        let forward = durable.quota == observed.quota
            && ((durable.incarnation == observed.incarnation
                && matches!(
                    (durable.lifecycle, observed.lifecycle),
                    (GroupLifecycle::Serving, GroupLifecycle::Draining)
                        | (GroupLifecycle::Removed, GroupLifecycle::Tombstoned)
                ))
                || (durable.lifecycle == GroupLifecycle::Removed
                    && observed.lifecycle == GroupLifecycle::Serving
                    && durable.incarnation.successor() == Some(observed.incarnation)));
        if !forward {
            return Err(format!(
                "group {} application identity {:?}/{} conflicts with host registry {:?}/{}",
                group_id.get(),
                observed.lifecycle,
                observed.incarnation.get(),
                durable.lifecycle,
                durable.incarnation.get()
            ));
        }
        registry.publish(observed)
    }

    pub(super) fn validate_group_shape(
        directory: &Path,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        lifecycle: GroupLifecycle,
    ) -> Result<(), String> {
        let active = directory.join("raft").exists();
        let should_be_active = !matches!(
            lifecycle,
            GroupLifecycle::Removed | GroupLifecycle::Tombstoned
        );
        if active != should_be_active {
            return Err(format!(
                "group {} {:?} application identity conflicts with active Raft directory \
                 presence={active}",
                group_id.get(),
                lifecycle
            ));
        }
        if !should_be_active {
            let retired = directory.join(format!("raft.retired-{}", incarnation.get()));
            if !retired.is_dir() {
                return Err(format!(
                    "group {} {:?} application identity has no current archive {}",
                    group_id.get(),
                    lifecycle,
                    retired.display()
                ));
            }
        }
        Ok(())
    }
}

pub(super) fn slot_from_policy(group_id: GroupId, policy: &StoredPolicy) -> SlotRecord {
    SlotRecord {
        group_id,
        incarnation: policy.incarnation,
        lifecycle: policy.lifecycle,
        quota: policy.quota,
    }
}

fn archive_raft(directory: &Path, incarnation: GroupIncarnation) -> Result<(), String> {
    archive_raft_inner(directory, incarnation, false)
}

pub(super) fn archive_raft_with_failpoints(
    directory: &Path,
    incarnation: GroupIncarnation,
) -> Result<(), String> {
    archive_raft_inner(directory, incarnation, true)
}

fn archive_raft_inner(
    directory: &Path,
    incarnation: GroupIncarnation,
    use_failpoints: bool,
) -> Result<(), String> {
    let raft = directory.join("raft");
    let retired = directory.join(format!("raft.retired-{}", incarnation.get()));
    if raft.exists() && retired.exists() {
        return Err(format!(
            "both active {} and retired {} exist",
            raft.display(),
            retired.display()
        ));
    }
    if raft.exists() {
        if use_failpoints {
            directed_failpoint("before_raft_rename");
        }
        fs::rename(&raft, &retired).map_err(|error| {
            format!(
                "could not archive {} as {}: {error}",
                raft.display(),
                retired.display()
            )
        })?;
        if use_failpoints {
            directed_failpoint("after_raft_rename");
        }
    } else if !retired.exists() {
        return Err(format!(
            "retirement has neither active {} nor retired {}",
            raft.display(),
            retired.display()
        ));
    }
    sync_directory(directory)?;
    if use_failpoints {
        directed_failpoint("after_parent_sync");
    }
    Ok(())
}

pub(super) fn prepare_staged_raft(
    directory: &Path,
    incarnation: GroupIncarnation,
    use_failpoints: bool,
) -> Result<(), String> {
    let active = directory.join("raft");
    let staged = staged_raft_path(directory, incarnation);
    if active.exists() && staged.exists() {
        return Err(format!(
            "both active {} and staged {} exist",
            active.display(),
            staged.display()
        ));
    }
    if active.exists() {
        return Ok(());
    }
    if !staged.exists() {
        if use_failpoints {
            directed_failpoint("before_staged_raft_creation");
        }
        fs::create_dir_all(&staged)
            .map_err(|error| format!("could not create {}: {error}", staged.display()))?;
        if use_failpoints {
            directed_failpoint("after_staged_raft_creation");
        }
    }
    sync_directory(&staged)?;
    sync_directory(directory)?;
    if use_failpoints {
        directed_failpoint("after_staged_raft_sync");
    }
    Ok(())
}

pub(super) fn activate_staged_raft(
    directory: &Path,
    incarnation: GroupIncarnation,
    use_failpoints: bool,
) -> Result<(), String> {
    let active = directory.join("raft");
    let staged = staged_raft_path(directory, incarnation);
    if active.exists() && staged.exists() {
        return Err(format!(
            "both active {} and staged {} exist",
            active.display(),
            staged.display()
        ));
    }
    if !active.exists() {
        if !staged.is_dir() {
            return Err(format!(
                "activation has neither active {} nor staged {}",
                active.display(),
                staged.display()
            ));
        }
        if use_failpoints {
            directed_failpoint("before_activation_raft_rename");
        }
        fs::rename(&staged, &active).map_err(|error| {
            format!(
                "could not activate {} as {}: {error}",
                staged.display(),
                active.display()
            )
        })?;
        if use_failpoints {
            directed_failpoint("after_activation_raft_rename");
        }
    }
    sync_directory(directory)?;
    if use_failpoints {
        directed_failpoint("after_activation_parent_sync");
    }
    Ok(())
}

fn staged_raft_path(directory: &Path, incarnation: GroupIncarnation) -> std::path::PathBuf {
    directory.join(format!("raft.activating-{}", incarnation.get()))
}
