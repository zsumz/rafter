//! Coverage marks name evidence without joining the state they observe.
//!
//! An observation set must be nameable yet hash to a constant, so recording
//! that a situation was reached can never split a model state in two or
//! inflate the explored state count.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::{Observation, ObservationSet};

#[test]
fn observations_are_named_but_do_not_change_state_hashes() {
    let empty = ObservationSet::default();
    let mut observed = empty;
    observed.mark(Observation::ElectionCertificates);

    assert_ne!(empty, observed);
    assert_eq!(
        observed.labels().collect::<Vec<_>>(),
        ["election_certificates"]
    );
    assert_eq!(hash(empty), hash(observed));
}

fn hash(value: ObservationSet) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
