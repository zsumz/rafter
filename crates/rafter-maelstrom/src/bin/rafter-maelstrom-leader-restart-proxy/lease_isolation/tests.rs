use super::{Action, ClientResponse, LeaseIsolation, RequestId};

fn request(client: &str, msg_id: u64) -> RequestId {
    RequestId::new(client, msg_id)
}

fn isolating() -> LeaseIsolation {
    let mut state = LeaseIsolation::default();
    assert!(state.observe_lease_state(true, true, 3).is_empty());
    let fast = request("c0", 7);
    assert!(!state.observe_read_request(&fast, true).hold);
    assert!(state.observe_read_handler(&fast, true, true, 3).is_empty());
    assert_eq!(
        state.observe_response(&fast, ClientResponse::ReadOk),
        vec![Action::Claim]
    );
    let actions = state.claim_result(true);
    assert!(matches!(actions.as_slice(), [Action::FastPathReadOk(_)]));
    assert!(state.drops_raft());
    state
}

fn select_probe(state: &mut LeaseIsolation, probe: &RequestId) {
    let warmup = request(probe.client(), probe.msg_id().saturating_sub(1));
    let disposition = state.observe_read_request(&warmup, true);
    assert!(!disposition.hold);
    assert!(disposition.actions.is_empty());
    let disposition = state.observe_read_request(probe, true);
    assert!(disposition.hold);
    assert!(matches!(
        disposition.actions.as_slice(),
        [Action::ReadBuffered(_), Action::ReleaseBuffered(_)]
    ));
}

#[test]
fn selects_and_releases_a_real_direct_read_only_after_same_term_expiry() {
    let mut state = isolating();
    let before_expiry = state.observe_read_request(&request("c1", 10), true);
    assert!(!before_expiry.hold);
    assert!(before_expiry.actions.is_empty());
    let actions = state.observe_lease_state(false, true, 3);
    assert!(matches!(actions.as_slice(), [Action::LeaseExpired(_)]));
    select_probe(&mut state, &request("c1", 11));
}

#[test]
fn expiry_before_buffer_still_releases_the_later_real_request() {
    let mut state = isolating();
    assert!(matches!(
        state.observe_lease_state(false, true, 3).as_slice(),
        [Action::LeaseExpired(_)]
    ));
    select_probe(&mut state, &request("c1", 11));
}

#[test]
fn claim_race_retries_without_starting_isolation() {
    let mut state = LeaseIsolation::default();
    state.observe_lease_state(true, true, 3);
    let fast = request("c0", 7);
    state.observe_read_request(&fast, true);
    state.observe_read_handler(&fast, true, true, 3);
    assert_eq!(
        state.observe_response(&fast, ClientResponse::ReadOk),
        vec![Action::Claim]
    );
    state.observe_lease_state(false, true, 3);
    assert!(state.claim_result(true).is_empty());
    assert!(!state.drops_raft());
    state.observe_lease_state(true, true, 3);
    assert!(!state.observe_read_request(&request("c0", 8), true).hold);
}

#[test]
fn correlated_code_11_response_completes_after_forwarding() {
    let mut state = isolating();
    let probe = request("c1", 11);
    state.observe_lease_state(false, true, 3);
    select_probe(&mut state, &probe);
    assert!(matches!(
        state
            .observe_read_handler(&probe, false, true, 3)
            .as_slice(),
        [Action::PostExpiryHandler(_)]
    ));
    assert!(matches!(
        state
            .observe_response(&probe, ClientResponse::TemporarilyUnavailable)
            .as_slice(),
        [Action::ProbeUnavailable(_)]
    ));
    assert!(!state.drops_raft());
}

#[test]
fn response_and_handler_correlate_across_pipe_ordering() {
    let mut state = isolating();
    let probe = request("c1", 11);
    state.observe_lease_state(false, true, 3);
    select_probe(&mut state, &probe);
    assert!(state
        .observe_response(&probe, ClientResponse::TemporarilyUnavailable)
        .is_empty());
    let actions = state.observe_read_handler(&probe, false, true, 3);
    assert!(matches!(
        actions.as_slice(),
        [Action::PostExpiryHandler(_), Action::ProbeUnavailable(_)]
    ));
}

#[test]
fn second_probe_terminal_fails_closed_before_handler_in_either_order() {
    for (first, second) in [
        (
            ClientResponse::TemporarilyUnavailable,
            ClientResponse::ReadOk,
        ),
        (
            ClientResponse::ReadOk,
            ClientResponse::TemporarilyUnavailable,
        ),
    ] {
        let mut state = isolating();
        let probe = request("c1", 11);
        state.observe_lease_state(false, true, 3);
        select_probe(&mut state, &probe);
        assert!(state.observe_response(&probe, first).is_empty());
        assert!(matches!(
            state.observe_response(&probe, second).as_slice(),
            [Action::DuplicateTerminal(_)]
        ));
        assert!(state.drops_raft());
        let actions = state.observe_read_handler(&probe, false, true, 3);
        assert!(matches!(
            actions.first(),
            Some(Action::PostExpiryHandler(_))
        ));
        match first {
            ClientResponse::TemporarilyUnavailable => {
                assert!(matches!(actions.get(1), Some(Action::ProbeUnavailable(_))));
            }
            ClientResponse::ReadOk => assert!(matches!(
                actions.get(1),
                Some(Action::PostExpiryReadServed(_))
            )),
            ClientResponse::UnexpectedError(_) => unreachable!(),
        }
    }
}

#[test]
fn second_probe_terminal_fails_closed_after_safe_or_violating_completion() {
    for (first, second) in [
        (
            ClientResponse::TemporarilyUnavailable,
            ClientResponse::ReadOk,
        ),
        (
            ClientResponse::ReadOk,
            ClientResponse::TemporarilyUnavailable,
        ),
    ] {
        let mut state = isolating();
        let probe = request("c1", 11);
        state.observe_lease_state(false, true, 3);
        select_probe(&mut state, &probe);
        state.observe_read_handler(&probe, false, true, 3);
        assert!(!state.observe_response(&probe, first).is_empty());
        assert!(matches!(
            state.observe_response(&probe, second).as_slice(),
            [Action::DuplicateTerminal(_)]
        ));
    }
}

#[test]
fn read_ok_is_the_only_served_read_violation() {
    let mut state = isolating();
    let probe = request("c1", 11);
    state.observe_lease_state(false, true, 3);
    select_probe(&mut state, &probe);
    state.observe_read_handler(&probe, false, true, 3);
    assert!(matches!(
        state
            .observe_response(&probe, ClientResponse::ReadOk)
            .as_slice(),
        [Action::PostExpiryReadServed(_)]
    ));
}

#[test]
fn unexpected_error_is_a_harness_event_not_a_served_read() {
    let mut state = isolating();
    let probe = request("c1", 11);
    state.observe_lease_state(false, true, 3);
    select_probe(&mut state, &probe);
    state.observe_read_handler(&probe, false, true, 3);
    assert!(matches!(
        state
            .observe_response(&probe, ClientResponse::UnexpectedError(20))
            .as_slice(),
        [Action::PostExpiryUnexpectedError { code: 20, .. }]
    ));
}

#[test]
fn any_active_transition_after_expiry_is_a_violation() {
    let mut state = isolating();
    let probe = request("c1", 11);
    state.observe_lease_state(false, true, 3);
    select_probe(&mut state, &probe);
    assert!(matches!(
        state.observe_lease_state(true, true, 3).as_slice(),
        [Action::PostExpiryLeaseRenewed(_)]
    ));
}

#[test]
fn changed_term_or_leader_loses_coverage_and_heals_transport() {
    let mut state = isolating();
    assert!(matches!(
        state.observe_role(false, 3).as_slice(),
        [Action::CoverageLost { .. }]
    ));
    assert!(!state.drops_raft());
}

#[test]
fn expected_stepdown_after_handler_waits_for_the_correlated_cancellation() {
    let mut state = isolating();
    let probe = request("c1", 11);
    state.observe_lease_state(false, true, 3);
    select_probe(&mut state, &probe);
    state.observe_read_handler(&probe, false, true, 3);
    assert!(state.observe_role(false, 3).is_empty());
    assert!(state.drops_raft());
    assert!(matches!(
        state
            .observe_response(&probe, ClientResponse::TemporarilyUnavailable)
            .as_slice(),
        [Action::ProbeUnavailable(_)]
    ));
}

#[test]
fn forwarded_reads_are_never_held_as_the_probe() {
    let mut state = isolating();
    let disposition = state.observe_read_request(&request("c1", 11), false);
    assert!(!disposition.hold);
    assert!(disposition.actions.is_empty());
}
