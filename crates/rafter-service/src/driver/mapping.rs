//! How a managed failure is said, and where each vocabulary lives.
//!
//! The driver's failures reach three different readers, so they are declared in
//! three places and gathered here. [`driver_error`] and [`checkpoint_error`] hold
//! what an embedder matches on; [`super::routing`] holds what a stage carries
//! before a client hears anything; [`super::group_error`] holds the projections
//! into the client surfaces. This file re-exports all of them under their
//! original names so nothing outside the driver has to know that.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use std::{error::Error, fmt};

use super::*;

mod checkpoint_error;
mod driver_error;

pub use checkpoint_error::ControlPlaneCheckpointError;
pub use driver_error::ManagedDriverError;

pub(super) use super::group_error::{
    read_error_from_group, terminal_read_error, transfer_error_from_group, write_error_from_group,
};
pub(super) use super::routing::{DriverRoutingError, ManagedOperationError};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rafter_app::error::StateMachineOperation;

    use super::*;

    #[derive(Debug)]
    struct MappingRuntimeError;

    impl fmt::Display for MappingRuntimeError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("mapping runtime error")
        }
    }

    impl Error for MappingRuntimeError {}

    #[derive(Debug)]
    struct MappingAppError;

    impl fmt::Display for MappingAppError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("mapping app error")
        }
    }

    impl Error for MappingAppError {}

    #[test]
    fn non_monotonic_local_proposal_id_maps_to_managed_invariant_write_error() {
        let error = write_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::NonMonotonicLocalProposalId {
                local_proposal_id: LocalProposalId(7),
                last_seen_local_proposal_id: LocalProposalId(9),
            },
            WriteFate::Unresolved,
        );

        let WriteError::ManagedInvariantViolation { fate, message } = &error else {
            panic!("expected a managed invariant violation, got {error:?}");
        };
        assert_eq!(
            message,
            "managed driver local-ID invariant violation: generated non-monotonic local proposal id local-proposal-7 after local-proposal-9"
        );
        assert_eq!(
            *fate,
            WriteFate::NotAppended,
            "the group refuses a non-monotonic id before it proposes"
        );
    }

    #[test]
    fn duplicate_read_id_maps_to_managed_invariant_read_error() {
        let error = read_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::DuplicateReadId { read_id: ReadId(8) },
        );

        let ReadError::ManagedInvariantViolation { message } = &error else {
            panic!("expected a managed invariant violation, got {error:?}");
        };
        assert_eq!(
            message,
            "managed driver local-ID invariant violation: generated duplicate read id read-8"
        );
    }

    #[test]
    fn non_monotonic_read_id_maps_to_managed_invariant_read_error() {
        let error = read_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::NonMonotonicReadId {
                read_id: ReadId(8),
                last_seen_read_id: ReadId(10),
            },
        );

        let ReadError::ManagedInvariantViolation { message } = &error else {
            panic!("expected a managed invariant violation, got {error:?}");
        };
        assert_eq!(
            message,
            "managed driver local-ID invariant violation: generated non-monotonic read id read-8 after read-10"
        );
    }

    /// The old mapping folded six operations into two variants and got one
    /// wrong: `EncodeCommand` was reported as a storage failure, and encoding a
    /// command touches no storage.
    #[test]
    fn a_state_machine_error_keeps_the_operation_that_surfaced_it() {
        let error = write_error_from_group::<MappingAppError, MappingRuntimeError>(
            GroupError::StateMachine {
                operation: StateMachineOperation::EncodeCommand,
                source: Arc::new(MappingAppError),
            },
            WriteFate::NotAppended,
        );

        let WriteError::StateMachine {
            operation, cause, ..
        } = &error
        else {
            panic!("expected a state machine error, got {error:?}");
        };
        assert_eq!(*operation, StateMachineOperation::EncodeCommand);
        assert!(cause.downcast_ref::<MappingAppError>().is_some());
    }

    #[test]
    fn managed_driver_error_is_a_standard_error_with_display_message() {
        let error = ManagedDriverError::MissingNode { node_id: NodeId(9) };
        let standard_error: &(dyn Error + 'static) = &error;

        assert_eq!(
            standard_error.to_string(),
            "managed driver node node-9 is missing"
        );
    }

    /// The category is the variant; the detail is the preserved cause. There is
    /// no message field to render into.
    #[test]
    fn a_group_driver_error_preserves_its_cause() {
        let error = ManagedDriverError::from(ManagedOperationError::<
            MappingAppError,
            MappingRuntimeError,
        >::Group(GroupError::Runtime(
            MappingRuntimeError,
        )));

        let source = error.source().expect("the group error is preserved");

        assert!(source
            .downcast_ref::<GroupError<MappingAppError, MappingRuntimeError>>()
            .is_some());
    }
}
