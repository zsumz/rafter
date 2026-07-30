//! Failure classification at direct and managed driver boundaries.

use rafter_multiraft::{driver::DriverError, MultiRaftError};
use rafter_reference_sharded_counter::GroupId;

use crate::app_store::StoreError;

pub(super) fn driver_application_durability_failed(error: &DriverError) -> bool {
    error
        .cause()
        .downcast_ref::<StoreError>()
        .is_some_and(StoreError::is_application_durability_failure)
}

pub(super) fn managed_application_durability_failed(error: &MultiRaftError<GroupId>) -> bool {
    match error {
        MultiRaftError::Driver { cause, .. } => cause
            .downcast_ref::<StoreError>()
            .is_some_and(StoreError::is_application_durability_failure),
        _ => false,
    }
}
