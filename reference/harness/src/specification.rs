use std::hash::Hash;

/// Caller-owned sequential semantics for one parsed action type.
///
/// Implementations retain all state transitions and observation comparisons.
/// The shared search sees only stable state keys and the possible next states
/// returned here.
pub trait SequentialSpec<A> {
    /// Complete sequential state.
    type State: Clone;
    /// Stable memoization key for states with identical future behavior.
    type Key: Eq + Hash;
    /// Typed caller-owned reason an action is impossible at one state.
    type Mismatch;

    /// Returns the memoization key for `state`.
    fn key(&self, state: &Self::State) -> Self::Key;

    /// Evaluates one action at `state`.
    fn step(&self, state: &Self::State, action: &A) -> Step<Self::State, Self::Mismatch>;
}

/// Consumer-supplied result of evaluating one action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Step<S, M> {
    /// The observed action cannot occur at this state.
    Impossible(M),
    /// The action has exactly one possible next state.
    Next(S),
    /// The observation admits either of two possible next states.
    Choice {
        /// Alternative tried first.
        first: S,
        /// Alternative tried after the first has no legal continuation.
        second: S,
    },
}
