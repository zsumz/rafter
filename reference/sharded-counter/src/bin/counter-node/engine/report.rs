//! Consumer ordering for one full-fidelity group step report.

use rafter_app::{proposal::ProposalEvent, read::ReadEvent, state_machine::ApplyResult};
use rafter_reference_sharded_counter::{adapter::CounterApplyResult, GroupId};

use crate::group::Report;

pub(super) enum ConsumerReportEvent {
    Proposal(ProposalEvent<CounterApplyResult>),
    Applied(ApplyResult<CounterApplyResult>),
    Read(ReadEvent<GroupId>),
}

pub(super) fn take_ordered_consumer_events(report: &mut Report) -> Vec<ConsumerReportEvent> {
    let mut events = Vec::with_capacity(
        report.proposal_events.len() + report.applied.len() + report.read_events.len(),
    );
    events.extend(
        std::mem::take(&mut report.proposal_events)
            .into_iter()
            .map(ConsumerReportEvent::Proposal),
    );
    events.extend(
        std::mem::take(&mut report.applied)
            .into_iter()
            .map(ConsumerReportEvent::Applied),
    );
    events.extend(
        std::mem::take(&mut report.read_events)
            .into_iter()
            .map(ConsumerReportEvent::Read),
    );
    events
}

#[cfg(test)]
mod tests {
    use rafter::{LocalProposalId, LogIndex, NodeId, ReadId, Term};
    use rafter_app::{
        group::GroupStepReport,
        proposal::ProposalEvent,
        read::{ReadEvent, ReadProof},
        state_machine::ApplyResult,
    };
    use rafter_reference_sharded_counter::{adapter::CounterApplyResult, CounterResult, GroupId};

    use super::{take_ordered_consumer_events, ConsumerReportEvent};

    #[test]
    fn co_emitted_terminal_bookkeeping_precedes_the_read_grant() {
        let group_id = GroupId::new(1);
        let proposal_id = LocalProposalId(7);
        let result = CounterApplyResult::Counter(CounterResult::Added { value: 3 });
        let mut report = GroupStepReport {
            group_id,
            peer_messages: Vec::new(),
            applied: vec![ApplyResult {
                index: LogIndex(3),
                term: Term(1),
                result,
                local_proposal_id: Some(proposal_id),
            }],
            proposal_events: vec![ProposalEvent::Applied {
                local_proposal_id: proposal_id,
                index: LogIndex(3),
                term: Term(1),
                result,
            }],
            read_events: vec![ReadEvent::Granted {
                read_id: ReadId(8),
                proof: ReadProof {
                    group_id,
                    issued_by: NodeId(1),
                    term: Term(1),
                    read_index: LogIndex(3),
                    required_applied_index: LogIndex(3),
                    local_applied_index: LogIndex(3),
                },
            }],
            leadership_transfer_events: Vec::new(),
            snapshot_events: Vec::new(),
            membership_events: Vec::new(),
            metrics: None,
        };

        let events = take_ordered_consumer_events(&mut report);
        assert!(matches!(
            events.as_slice(),
            [
                ConsumerReportEvent::Proposal(ProposalEvent::Applied { .. }),
                ConsumerReportEvent::Applied(ApplyResult {
                    local_proposal_id: Some(id),
                    ..
                }),
                ConsumerReportEvent::Read(ReadEvent::Granted { .. })
            ] if *id == proposal_id
        ));
    }
}
