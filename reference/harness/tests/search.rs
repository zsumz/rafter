use std::cell::Cell;

use rafter_reference_harness::{
    search, CandidateReason, Operation, OperationId, SearchError, SearchLimits, SequentialSpec,
    Step,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    Add { delta: u8, observed: u8 },
    Observe(u8),
    Unresolved(u8),
}

struct Spec;

impl SequentialSpec<Action> for Spec {
    type State = u8;
    type Key = u8;
    type Mismatch = (u8, u8);

    fn key(&self, state: &Self::State) -> Self::Key {
        *state
    }

    fn step(&self, state: &Self::State, action: &Action) -> Step<Self::State, Self::Mismatch> {
        match *action {
            Action::Add { delta, observed } => {
                let next = state + delta;
                if next == observed {
                    Step::Next(next)
                } else {
                    Step::Impossible((next, observed))
                }
            }
            Action::Observe(observed) if *state == observed => Step::Next(*state),
            Action::Observe(observed) => Step::Impossible((*state, observed)),
            Action::Unresolved(delta) => Step::Choice {
                first: state + delta,
                second: *state,
            },
        }
    }
}

const LIMITS: SearchLimits = SearchLimits::new(24, 200_000).expect("the reviewed limits are valid");

#[test]
fn real_time_and_overlaps_are_distinguished() {
    let overlapping = [
        operation(
            1,
            Action::Add {
                delta: 2,
                observed: 3,
            },
            0,
            3,
        ),
        operation(
            2,
            Action::Add {
                delta: 1,
                observed: 1,
            },
            1,
            2,
        ),
    ];
    let report = search(&overlapping, 0, &0, &Spec, LIMITS)
        .expect("overlap permits the only valid ordering");
    assert_eq!(report.searched_operations(), 2);
    assert_eq!(report.discharged_operations(), 0);

    let ordered = [
        operation(
            1,
            Action::Add {
                delta: 2,
                observed: 3,
            },
            0,
            1,
        ),
        operation(
            2,
            Action::Add {
                delta: 1,
                observed: 1,
            },
            2,
            3,
        ),
    ];
    assert!(matches!(
        search(&ordered, 0, &0, &Spec, LIMITS),
        Err(SearchError::NoOrder(_))
    ));
}

#[test]
fn a_two_state_observation_explores_both_fates() {
    let operations = [
        operation(1, Action::Unresolved(1), 0, 1),
        operation(2, Action::Observe(0), 2, 3),
    ];
    let report = search(&operations, 3, &0, &Spec, LIMITS)
        .expect("the second fate explains the observation");
    assert!(report.configurations() > report.searched_operations());
    assert_eq!(report.discharged_operations(), 3);
}

#[test]
fn an_impossible_observation_retains_the_deepest_position() {
    let operations = [
        operation(
            1,
            Action::Add {
                delta: 1,
                observed: 1,
            },
            0,
            1,
        ),
        operation(2, Action::Observe(9), 2, 3),
    ];
    let Err(SearchError::NoOrder(frontier)) = search(&operations, 0, &0, &Spec, LIMITS) else {
        panic!("the impossible observation must fail");
    };
    assert_eq!(frontier.placed(), &[OperationId::new(1)]);
    assert_eq!(frontier.candidates().len(), 1);
    assert_eq!(
        frontier.candidates()[0].reason,
        CandidateReason::Mismatch((1, 9))
    );
}

#[test]
fn explicit_bounds_are_refusals() {
    let operations = [
        operation(1, Action::Unresolved(1), 0, 3),
        operation(2, Action::Observe(0), 1, 2),
    ];
    let short = SearchLimits::new(1, 200_000).expect("the limit is valid");
    assert_eq!(
        search(&operations, 0, &0, &Spec, short),
        Err(SearchError::TooManyOperations {
            operations: 2,
            bound: 1,
        })
    );

    let shallow = SearchLimits::new(24, 1).expect("the limit is valid");
    assert!(matches!(
        search(&operations, 0, &0, &Spec, shallow),
        Err(SearchError::BudgetExhausted { bound: 1, .. })
    ));
    assert_eq!(SearchLimits::new(24, 0), None);
    assert_eq!(SearchLimits::new(33, 1), None);
}

#[test]
fn the_complete_predecessor_bit_set_is_representable() {
    let operations = (0_u64..32)
        .map(|index| {
            operation(
                index + 1,
                Action::Add {
                    delta: 1,
                    observed: u8::try_from(index + 1).expect("the value fits"),
                },
                usize::try_from(index * 2).expect("the position fits"),
                usize::try_from(index * 2 + 1).expect("the position fits"),
            )
        })
        .collect::<Vec<_>>();
    let limits = SearchLimits::new(32, 64).expect("all predecessor bits are representable");
    let report = search(&operations, 0, &0, &Spec, limits).expect("the serial ordering is exact");
    assert_eq!(report.searched_operations(), 32);
}

#[derive(Clone, Copy)]
enum MemoAction {
    Advance(u8),
    Observe(u8),
}

#[derive(Default)]
struct MemoSpec {
    observations: Cell<usize>,
}

impl SequentialSpec<MemoAction> for MemoSpec {
    type State = u8;
    type Key = u8;
    type Mismatch = ();

    fn key(&self, state: &Self::State) -> Self::Key {
        *state
    }

    fn step(&self, state: &Self::State, action: &MemoAction) -> Step<Self::State, Self::Mismatch> {
        match *action {
            MemoAction::Advance(delta) => Step::Next(state + delta),
            MemoAction::Observe(observed) => {
                self.observations.set(self.observations.get() + 1);
                if *state == observed {
                    Step::Next(*state)
                } else {
                    Step::Impossible(())
                }
            }
        }
    }
}

#[test]
fn a_converged_failed_configuration_is_evaluated_once() {
    let operations = [
        Operation::new(OperationId::new(1), MemoAction::Advance(1), 0, 2),
        Operation::new(OperationId::new(2), MemoAction::Advance(2), 1, 3),
        Operation::new(OperationId::new(3), MemoAction::Observe(9), 4, 5),
    ];
    let specification = MemoSpec::default();
    assert!(matches!(
        search(&operations, 0, &0, &specification, LIMITS),
        Err(SearchError::NoOrder(_))
    ));
    assert_eq!(
        specification.observations.get(),
        1,
        "the second ordering reaches the memoized failed state"
    );
}

fn operation(id: u64, action: Action, invoked_at: usize, returned_at: usize) -> Operation<Action> {
    Operation::new(OperationId::new(id), action, invoked_at, returned_at)
}
