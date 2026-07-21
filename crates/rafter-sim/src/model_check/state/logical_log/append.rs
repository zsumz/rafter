//! Transition-level `AppendEntries` acceptance evidence.

use rafter::{LogIndex, Message};

use crate::{Cluster, Envelope};

use super::super::super::catalog;
use super::super::super::observations::{Observation, ObservationSet};
use super::{LogicalLogHistory, LogicalLogView, LogicalLogViolation};

impl LogicalLogHistory {
    pub(in crate::model_check::state) fn record_append_entries_delivery(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        delivered: Option<&Envelope>,
        emitted: &[Envelope],
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        let Some(envelope) = delivered else {
            return observations;
        };
        let Message::AppendEntries(request) = &envelope.message else {
            return observations;
        };
        let Some(response) = emitted.iter().find_map(|emitted| {
            if emitted.from != envelope.to || emitted.to != envelope.from {
                return None;
            }
            match &emitted.message {
                Message::AppendEntriesResponse(response)
                    if response.follower_id == envelope.to
                        && response.sequence == request.sequence =>
                {
                    Some(*response)
                }
                _ => None,
            }
        }) else {
            return observations;
        };
        if !response.success {
            return observations;
        }
        if !request.entries.is_empty() {
            observations.mark(Observation::SuccessfulNonemptyAppendObservations);
        }

        let before_view = LogicalLogView::from_cluster(before, envelope.to);
        let after_view = LogicalLogView::from_cluster(after, envelope.to);
        if before_view.term_at(request.prev_log_index) == Some(request.prev_log_term) {
            observations.mark(Observation::SuccessfulAppendPrevLogMatches);
        } else {
            self.record_append_prev_log_violation(format!(
                "{} accepted AppendEntries from {} without matching prev ({}, term {})",
                envelope.to, envelope.from, request.prev_log_index, request.prev_log_term
            ));
        }

        let expected_match_index =
            LogIndex(request.prev_log_index.0 + request.entries.len() as u64);
        let match_index_matches = response.match_index == expected_match_index;
        if !match_index_matches {
            self.record_append_stored_suffix_violation(format!(
                "{} reported match index {} for append ending at {}",
                envelope.to, response.match_index, expected_match_index
            ));
        }

        let mut stored_suffix_matches = true;
        for (offset, entry) in request.entries.iter().enumerate() {
            let index = LogIndex(request.prev_log_index.0 + offset as u64 + 1);
            if after_view.entry_at(index) == Some(entry) {
                continue;
            }
            stored_suffix_matches = false;
            self.record_append_stored_suffix_violation(format!(
                "{} acknowledged AppendEntries without storing leader entry at index {}",
                envelope.to, index
            ));
            break;
        }
        if !request.entries.is_empty() && match_index_matches && stored_suffix_matches {
            observations.mark(Observation::SuccessfulAppendStoredSuffixMatches);
        }
        observations
    }

    fn record_append_prev_log_violation(&mut self, message: String) {
        let violation = LogicalLogViolation {
            invariant: catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE,
            message,
        };
        self.append_prev_log_violations.insert(violation.clone());
        self.violations.insert(violation);
    }

    fn record_append_stored_suffix_violation(&mut self, message: String) {
        let violation = LogicalLogViolation {
            invariant: catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE,
            message,
        };
        self.append_stored_suffix_violations
            .insert(violation.clone());
        self.violations.insert(violation);
    }
}
