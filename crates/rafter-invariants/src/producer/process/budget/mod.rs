//! Manifest-derived producer budget policy and thread-scoped deadline accounting.

mod active;
mod configuration;
mod model;

pub(in crate::producer) use active::{
    active_layer_deadlines, ensure_execution_deadline, ensure_total_deadline,
};
pub(super) use active::{
    active_process_timeout, active_total_process_timeout, has_active_layer_budget,
};
pub(super) use configuration::layer_budget;
pub(super) use model::{ActiveLayerBudget, ProcessPolicy, ProcessSchedule};
pub(in crate::producer) use model::{LayerBudgetGuard, ProcessKind};
