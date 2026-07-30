//! Startup reconciliation and filesystem transaction helpers.

use std::{fs, path::Path};

use rafter_reference_sharded_counter::{GroupId, GroupIncarnation, GroupLifecycle};

use super::Engine;
use crate::{
    app_store::{ApplicationRecord, StoredPolicy},
    host_registry::{sync_directory, HostRegistry, RetirementIntent, SlotRecord},
};

impl Engine {
    pub(super) fn open_or_initialize_registry(
        &self,
        groups_dir: &Path,
    ) -> Result<HostRegistry, String> {
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
        let slots = (1..=self.group_count)
            .map(|raw| SlotRecord {
                group_id: GroupId::new(raw),
                incarnation: GroupIncarnation::first(),
                lifecycle: GroupLifecycle::Serving,
                quota: self.default_quota,
            })
            .collect::<Vec<_>>();
        let registry = HostRegistry::create(groups_dir, slots)?;
        for raw in 1..=self.group_count {
            let slot_dir = groups_dir.join(raw.to_string());
            let app_dir = slot_dir.join("app");
            let _ = ApplicationRecord::bootstrap(&app_dir, self.max_sessions, self.default_quota)
                .map_err(|error| format!("group {raw} explicit bootstrap failed: {error}"))?;
            let raft_dir = slot_dir.join("raft");
            fs::create_dir_all(&raft_dir)
                .map_err(|error| format!("could not create {}: {error}", raft_dir.display()))?;
            sync_directory(&slot_dir)?;
        }
        sync_directory(groups_dir)?;
        Ok(registry)
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

pub(super) fn directed_failpoint(name: &str) {
    if std::env::var("RAFTER_COUNTER_FAILPOINT").as_deref() == Ok(name) {
        crate::emit(&format!("FAILPOINT {name}"));
        std::process::abort();
    }
}
