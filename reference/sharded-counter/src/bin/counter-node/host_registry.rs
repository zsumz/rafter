//! Durable host authority for counter group identities and retirement.
//!
//! The per-group application image is deliberately not an existence oracle.
//! This registry survives loss of that image and therefore prevents a known
//! slot, incarnation, removal, or tombstone from being mistaken for first
//! boot. Retirement intents make the only cross-directory lifecycle
//! transition replayable after a process crash.

use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use rafter_crc32::crc32;
use rafter_reference_sharded_counter::{GroupId, GroupIncarnation, GroupLifecycle, WorkQuota};

const REGISTRY_MAGIC: [u8; 4] = *b"RCHR";
const INTENT_MAGIC: [u8; 4] = *b"RCRI";
const ACTIVATION_MAGIC: [u8; 4] = *b"RCRA";
const BOOTSTRAP_MAGIC: [u8; 4] = *b"RCRB";
const VERSION: u8 = 1;
const MAX_REGISTRY_BYTES: usize = 128 * 1024;

/// Durable identity and lifecycle floor for one configured host slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotRecord {
    pub group_id: GroupId,
    pub incarnation: GroupIncarnation,
    pub lifecycle: GroupLifecycle,
    pub quota: WorkQuota,
}

/// Checksummed fixed-slot registry stored above every group directory.
#[derive(Debug)]
pub struct HostRegistry {
    path: PathBuf,
    slots: BTreeMap<GroupId, SlotRecord>,
}

impl HostRegistry {
    /// Opens the existing registry, returning `None` only when it has never
    /// been published.
    pub fn open(groups_dir: &Path, group_count: u32) -> Result<Option<Self>, String> {
        let path = groups_dir.join("slots.rchr");
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        if bytes.len() > MAX_REGISTRY_BYTES {
            return Err(format!(
                "host registry is {} bytes, above {MAX_REGISTRY_BYTES}",
                bytes.len()
            ));
        }
        let slots = decode_registry(&bytes)?;
        validate_slot_set(&slots, group_count)?;
        Ok(Some(Self { path, slots }))
    }

    /// Publishes the first registry generation.
    ///
    /// Callers must prove either a completely empty first boot or a complete
    /// migration from existing application records before invoking this.
    pub fn create(groups_dir: &Path, slots: Vec<SlotRecord>) -> Result<Self, String> {
        let path = groups_dir.join("slots.rchr");
        if path.exists() {
            return Err(format!(
                "host registry already exists at {}",
                path.display()
            ));
        }
        let slots = slots
            .into_iter()
            .map(|slot| (slot.group_id, slot))
            .collect::<BTreeMap<_, _>>();
        persist_registry(&path, &slots)?;
        Ok(Self { path, slots })
    }

    pub fn slot(&self, group_id: GroupId) -> Result<SlotRecord, String> {
        self.slots
            .get(&group_id)
            .copied()
            .ok_or_else(|| format!("host registry has no slot for group {}", group_id.get()))
    }

    /// Publishes a new durable fact for one known slot.
    pub fn publish(&mut self, slot: SlotRecord) -> Result<(), String> {
        if !self.slots.contains_key(&slot.group_id) {
            return Err(format!(
                "host registry cannot add unconfigured group {}",
                slot.group_id.get()
            ));
        }
        let mut staged = self.slots.clone();
        staged.insert(slot.group_id, slot);
        persist_registry(&self.path, &staged)?;
        self.slots = staged;
        Ok(())
    }
}

/// Durable marker for the Draining-to-Removed filesystem transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetirementIntent {
    pub group_id: GroupId,
    pub incarnation: GroupIncarnation,
}

impl RetirementIntent {
    pub fn load(group_dir: &Path) -> Result<Option<Self>, String> {
        let path = Self::path(group_dir);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        decode_intent(&bytes).map(Some)
    }

    pub fn publish(self, group_dir: &Path) -> Result<(), String> {
        let path = Self::path(group_dir);
        if path.exists() {
            return Err(format!(
                "retirement intent already exists at {}",
                path.display()
            ));
        }
        let mut bytes = Vec::with_capacity(4 + 1 + 4 + 4 + 4);
        bytes.extend_from_slice(&INTENT_MAGIC);
        bytes.push(VERSION);
        put_u32(&mut bytes, self.group_id.get());
        put_u32(&mut bytes, self.incarnation.get());
        append_checksum(&mut bytes);
        persist_bytes(&path, &bytes)
    }

    pub fn clear(group_dir: &Path) -> Result<(), String> {
        let path = Self::path(group_dir);
        if path.exists() {
            fs::remove_file(&path)
                .map_err(|error| format!("could not remove {}: {error}", path.display()))?;
        }
        sync_directory(group_dir)
    }

    fn path(group_dir: &Path) -> PathBuf {
        group_dir.join("retirement.intent")
    }
}

/// Durable marker for one Removed-to-Serving activation transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationIntent {
    pub group_id: GroupId,
    pub previous_incarnation: GroupIncarnation,
    pub next_incarnation: GroupIncarnation,
    pub quota: WorkQuota,
}

impl ActivationIntent {
    pub fn load(group_dir: &Path) -> Result<Option<Self>, String> {
        let path = Self::path(group_dir);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        decode_activation(&bytes).map(Some)
    }

    pub fn publish(self, group_dir: &Path) -> Result<(), String> {
        let path = Self::path(group_dir);
        if path.exists() {
            return Err(format!(
                "activation intent already exists at {}",
                path.display()
            ));
        }
        let mut bytes = Vec::with_capacity(4 + 1 + 4 * 4 + 4);
        bytes.extend_from_slice(&ACTIVATION_MAGIC);
        bytes.push(VERSION);
        put_u32(&mut bytes, self.group_id.get());
        put_u32(&mut bytes, self.previous_incarnation.get());
        put_u32(&mut bytes, self.next_incarnation.get());
        put_u32(&mut bytes, self.quota.get());
        append_checksum(&mut bytes);
        persist_bytes(&path, &bytes)
    }

    pub fn clear(group_dir: &Path) -> Result<(), String> {
        clear_intent(&Self::path(group_dir), group_dir)
    }

    fn path(group_dir: &Path) -> PathBuf {
        group_dir.join("activation.intent")
    }
}

/// Durable authority for resuming an interrupted first host bootstrap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BootstrapIntent {
    pub group_count: u32,
    pub quota: WorkQuota,
}

impl BootstrapIntent {
    pub fn load(groups_dir: &Path) -> Result<Option<Self>, String> {
        let path = Self::path(groups_dir);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        decode_bootstrap(&bytes).map(Some)
    }

    pub fn publish(self, groups_dir: &Path) -> Result<(), String> {
        let path = Self::path(groups_dir);
        if path.exists() {
            return Err(format!(
                "bootstrap intent already exists at {}",
                path.display()
            ));
        }
        let mut bytes = Vec::with_capacity(4 + 1 + 4 * 2 + 4);
        bytes.extend_from_slice(&BOOTSTRAP_MAGIC);
        bytes.push(VERSION);
        put_u32(&mut bytes, self.group_count);
        put_u32(&mut bytes, self.quota.get());
        append_checksum(&mut bytes);
        persist_bytes(&path, &bytes)
    }

    pub fn clear(groups_dir: &Path) -> Result<(), String> {
        clear_intent(&Self::path(groups_dir), groups_dir)
    }

    fn path(groups_dir: &Path) -> PathBuf {
        groups_dir.join("bootstrap.intent")
    }
}

pub fn sync_directory(directory: &Path) -> Result<(), String> {
    fs::File::open(directory)
        .and_then(|handle| handle.sync_all())
        .map_err(|error| format!("could not sync directory {}: {error}", directory.display()))
}

fn validate_slot_set(
    slots: &BTreeMap<GroupId, SlotRecord>,
    group_count: u32,
) -> Result<(), String> {
    if slots.len() != group_count as usize {
        return Err(format!(
            "host registry has {} slots, expected {group_count}",
            slots.len()
        ));
    }
    for raw in 1..=group_count {
        if !slots.contains_key(&GroupId::new(raw)) {
            return Err(format!("host registry is missing configured group {raw}"));
        }
    }
    Ok(())
}

fn persist_registry(path: &Path, slots: &BTreeMap<GroupId, SlotRecord>) -> Result<(), String> {
    let mut bytes = Vec::with_capacity(10 + slots.len() * 13);
    bytes.extend_from_slice(&REGISTRY_MAGIC);
    bytes.push(VERSION);
    put_u32(
        &mut bytes,
        u32::try_from(slots.len()).map_err(|_| "host registry is too large".to_string())?,
    );
    for slot in slots.values() {
        put_u32(&mut bytes, slot.group_id.get());
        put_u32(&mut bytes, slot.incarnation.get());
        bytes.push(encode_lifecycle(slot.lifecycle));
        put_u32(&mut bytes, slot.quota.get());
    }
    append_checksum(&mut bytes);
    if bytes.len() > MAX_REGISTRY_BYTES {
        return Err(format!(
            "host registry is {} bytes, above {MAX_REGISTRY_BYTES}",
            bytes.len()
        ));
    }
    persist_bytes(path, &bytes)
}

fn persist_bytes(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("durable path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("could not create {}: {error}", parent.display()))?;
    let temp = path.with_extension(format!("tmp.{}", std::process::id()));
    let mut file = fs::File::create(&temp)
        .map_err(|error| format!("could not create {}: {error}", temp.display()))?;
    file.write_all(bytes)
        .map_err(|error| format!("could not write {}: {error}", temp.display()))?;
    file.sync_all()
        .map_err(|error| format!("could not sync {}: {error}", temp.display()))?;
    fs::rename(&temp, path)
        .map_err(|error| format!("could not publish {}: {error}", path.display()))?;
    sync_directory(parent)
}

fn decode_registry(bytes: &[u8]) -> Result<BTreeMap<GroupId, SlotRecord>, String> {
    let body = checked_body(bytes, REGISTRY_MAGIC, "host registry")?;
    let mut cursor = Cursor::new(body);
    let count = cursor.u32()? as usize;
    let mut slots = BTreeMap::new();
    for _ in 0..count {
        let group_id = GroupId::new(cursor.u32()?);
        let incarnation = GroupIncarnation::new(cursor.u32()?)
            .ok_or_else(|| "host registry contains a zero incarnation".to_string())?;
        let lifecycle = decode_lifecycle(cursor.u8()?)?;
        let quota = WorkQuota::new(cursor.u32()?)
            .ok_or_else(|| "host registry contains a zero quota".to_string())?;
        let slot = SlotRecord {
            group_id,
            incarnation,
            lifecycle,
            quota,
        };
        if slots.insert(group_id, slot).is_some() {
            return Err(format!(
                "host registry contains duplicate group {}",
                group_id.get()
            ));
        }
    }
    if !cursor.remaining().is_empty() {
        return Err("host registry contains trailing bytes".to_string());
    }
    Ok(slots)
}

fn decode_intent(bytes: &[u8]) -> Result<RetirementIntent, String> {
    let body = checked_body(bytes, INTENT_MAGIC, "retirement intent")?;
    let mut cursor = Cursor::new(body);
    let group_id = GroupId::new(cursor.u32()?);
    let incarnation = GroupIncarnation::new(cursor.u32()?)
        .ok_or_else(|| "retirement intent contains a zero incarnation".to_string())?;
    if !cursor.remaining().is_empty() {
        return Err("retirement intent contains trailing bytes".to_string());
    }
    Ok(RetirementIntent {
        group_id,
        incarnation,
    })
}

fn decode_activation(bytes: &[u8]) -> Result<ActivationIntent, String> {
    let body = checked_body(bytes, ACTIVATION_MAGIC, "activation intent")?;
    let mut cursor = Cursor::new(body);
    let group_id = GroupId::new(cursor.u32()?);
    let previous_incarnation = GroupIncarnation::new(cursor.u32()?)
        .ok_or_else(|| "activation intent contains a zero previous incarnation".to_string())?;
    let next_incarnation = GroupIncarnation::new(cursor.u32()?)
        .ok_or_else(|| "activation intent contains a zero next incarnation".to_string())?;
    let quota = WorkQuota::new(cursor.u32()?)
        .ok_or_else(|| "activation intent contains a zero quota".to_string())?;
    if previous_incarnation.successor() != Some(next_incarnation) {
        return Err("activation intent incarnations are not consecutive".to_string());
    }
    if !cursor.remaining().is_empty() {
        return Err("activation intent contains trailing bytes".to_string());
    }
    Ok(ActivationIntent {
        group_id,
        previous_incarnation,
        next_incarnation,
        quota,
    })
}

fn decode_bootstrap(bytes: &[u8]) -> Result<BootstrapIntent, String> {
    let body = checked_body(bytes, BOOTSTRAP_MAGIC, "bootstrap intent")?;
    let mut cursor = Cursor::new(body);
    let group_count = cursor.u32()?;
    let quota = WorkQuota::new(cursor.u32()?)
        .ok_or_else(|| "bootstrap intent contains a zero quota".to_string())?;
    if group_count == 0 {
        return Err("bootstrap intent contains a zero group count".to_string());
    }
    if !cursor.remaining().is_empty() {
        return Err("bootstrap intent contains trailing bytes".to_string());
    }
    Ok(BootstrapIntent { group_count, quota })
}

fn clear_intent(path: &Path, parent: &Path) -> Result<(), String> {
    if path.exists() {
        fs::remove_file(path)
            .map_err(|error| format!("could not remove {}: {error}", path.display()))?;
    }
    sync_directory(parent)
}

fn checked_body<'a>(bytes: &'a [u8], magic: [u8; 4], label: &str) -> Result<&'a [u8], String> {
    if bytes.len() < 4 + 1 + 4 {
        return Err(format!("{label} is truncated"));
    }
    let body_len = bytes.len() - 4;
    let expected = u32::from_le_bytes(
        bytes[body_len..]
            .try_into()
            .map_err(|_| format!("{label} checksum is truncated"))?,
    );
    let actual = crc32(&bytes[..body_len]);
    if expected != actual {
        return Err(format!(
            "{label} checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
        ));
    }
    if bytes[..4] != magic {
        return Err(format!("{label} has the wrong magic"));
    }
    if bytes[4] != VERSION {
        return Err(format!("{label} has an unsupported version"));
    }
    Ok(&bytes[5..body_len])
}

fn append_checksum(bytes: &mut Vec<u8>) {
    let checksum = crc32(bytes);
    put_u32(bytes, checksum);
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn encode_lifecycle(lifecycle: GroupLifecycle) -> u8 {
    match lifecycle {
        GroupLifecycle::Creating => 1,
        GroupLifecycle::Recovering => 2,
        GroupLifecycle::Serving => 3,
        GroupLifecycle::Draining => 4,
        GroupLifecycle::Removed => 5,
        GroupLifecycle::Tombstoned => 6,
    }
}

fn decode_lifecycle(value: u8) -> Result<GroupLifecycle, String> {
    match value {
        1 => Ok(GroupLifecycle::Creating),
        2 => Ok(GroupLifecycle::Recovering),
        3 => Ok(GroupLifecycle::Serving),
        4 => Ok(GroupLifecycle::Draining),
        5 => Ok(GroupLifecycle::Removed),
        6 => Ok(GroupLifecycle::Tombstoned),
        _ => Err(format!("host registry has unknown lifecycle {value}")),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Result<u8, String> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| "durable host record is truncated".to_string())?;
        self.offset += 1;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, String> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
            "durable host integer is truncated".to_string()
        })?))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| "durable host record length overflow".to_string())?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| "durable host record is truncated".to_string())?;
        self.offset = end;
        Ok(bytes)
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
    }
}
