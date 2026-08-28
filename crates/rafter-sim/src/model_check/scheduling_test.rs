//! Scheduler identity for enumerated actions and their queued envelopes.
//!
//! A missing envelope must surface as a deterministic harness error rather than
//! a silent skip, and soak actions must distinguish duplicate envelopes, so no
//! two queued messages ever share one scheduler identity.

use rafter::{LogIndex, Message, NodeConfig, NodeId, RequestVote, Term};

use crate::{Cluster, SimSeed};

use super::{deliver_action, enabled_soak_actions, envelope_identity, ExplorationState};
use crate::model_check::{FailureKind, SoakAction, SoakConfig};

#[test]
fn missing_envelope_is_a_deterministic_scheduler_harness_error() {
    let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("test config is valid");
    let cluster = Cluster::new(vec![config]);

    let identity_error = envelope_identity(&cluster, 7)
        .expect_err("missing envelope identity must be handled explicitly");
    let action_error =
        deliver_action(&cluster, 7).expect_err("missing delivery must be handled explicitly");
    assert_eq!(identity_error, action_error);
    let failure = action_error.into_failure(&cluster, &[]);
    assert_eq!(failure.kind(), FailureKind::HarnessError);
    assert_eq!(failure.invariant(), "model-check scheduling harness");
    assert_eq!(
        failure.message(),
        "scheduler selected envelope position 7 from queue length 0"
    );
}

#[test]
fn soak_actions_distinguish_duplicate_envelopes() {
    let config = NodeConfig::new(NodeId(1), vec![NodeId(2)], 3).expect("test config is valid");
    let mut state = ExplorationState::new(Cluster::new(vec![config]));
    let message = Message::RequestVote(RequestVote {
        term: Term(1),
        candidate_id: NodeId(2),
        last_log_index: LogIndex::ZERO,
        last_log_term: Term::default(),
    });
    state.inject_message(NodeId(2), NodeId(1), message.clone());
    state.inject_message(NodeId(2), NodeId(1), message);

    let identities = enabled_soak_actions(&state, SoakConfig::new(SimSeed(7), 1))
        .expect("fixture envelopes have valid scheduler identities")
        .into_iter()
        .filter_map(|action| match action.trace {
            SoakAction::Deliver { identity, .. } => Some(identity),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(identities.len(), 2);
    assert_eq!(identities[0].matching_ordinal(), 0);
    assert_eq!(identities[1].matching_ordinal(), 1);
    assert_ne!(identities[0], identities[1]);
}
