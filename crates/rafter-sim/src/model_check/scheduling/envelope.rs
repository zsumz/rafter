//! Envelope identity for replayable delivery actions.
//!
//! A trace must name exactly one queued envelope, so identity pairs the ready
//! tick with the ordinal among identical (from, to, kind) envelopes ahead of
//! it. Every lookup that cannot resolve fails closed as a harness error.

use std::fmt;

use crate::Cluster;

use super::super::{
    helpers::summarize, Action, EnvelopeIdentity, Failure, FailureKind, MessageKind,
};

pub(in crate::model_check) fn deliver_action(
    cluster: &Cluster,
    position: usize,
) -> Result<Action, SchedulingError> {
    let queued = cluster
        .network
        .get(position)
        .ok_or(SchedulingError::MissingEnvelope {
            position,
            queue_len: cluster.network.len(),
        })?;
    let envelope = &queued.envelope;
    Ok(Action::Deliver {
        from: envelope.from,
        to: envelope.to,
        message: MessageKind::from(&envelope.message),
        identity: envelope_identity(cluster, position)?,
    })
}

pub(in crate::model_check) fn envelope_identity(
    cluster: &Cluster,
    position: usize,
) -> Result<EnvelopeIdentity, SchedulingError> {
    let queued = cluster
        .network
        .get(position)
        .ok_or(SchedulingError::MissingEnvelope {
            position,
            queue_len: cluster.network.len(),
        })?;
    let kind = MessageKind::from(&queued.envelope.message);
    let matching_ordinal = cluster
        .network
        .iter()
        .take(position)
        .filter(|candidate| {
            candidate.envelope.from == queued.envelope.from
                && candidate.envelope.to == queued.envelope.to
                && MessageKind::from(&candidate.envelope.message) == kind
        })
        .count();
    Ok(EnvelopeIdentity::new(
        queued.ready_at,
        u64::try_from(matching_ordinal)
            .map_err(|_| SchedulingError::EnvelopeOrdinalOverflow { matching_ordinal })?,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::model_check) enum SchedulingError {
    MissingEnvelope { position: usize, queue_len: usize },
    EnvelopeOrdinalOverflow { matching_ordinal: usize },
}

impl SchedulingError {
    pub(in crate::model_check) fn into_failure(
        self,
        cluster: &Cluster,
        trace: &[Action],
    ) -> Failure {
        Failure {
            kind: FailureKind::HarnessError,
            invariant: "model-check scheduling harness",
            message: self.to_string(),
            trace: trace.to_vec(),
            state: summarize(cluster),
        }
    }
}

impl fmt::Display for SchedulingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvelope {
                position,
                queue_len,
            } => write!(
                formatter,
                "scheduler selected envelope position {position} from queue length {queue_len}"
            ),
            Self::EnvelopeOrdinalOverflow { matching_ordinal } => write!(
                formatter,
                "scheduler envelope matching ordinal {matching_ordinal} does not fit in u64"
            ),
        }
    }
}
