use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClientResponse {
    ReadOk,
    TemporarilyUnavailable,
    UnexpectedError(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RequestId {
    client: String,
    msg_id: u64,
}

impl RequestId {
    pub(super) fn new(client: impl Into<String>, msg_id: u64) -> Self {
        Self {
            client: client.into(),
            msg_id,
        }
    }

    pub(super) fn client(&self) -> &str {
        &self.client
    }

    pub(super) const fn msg_id(&self) -> u64 {
        self.msg_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct EvidenceEvent {
    pub term: u64,
    pub request: RequestId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Action {
    Claim,
    FastPathReadOk(EvidenceEvent),
    ReadBuffered(EvidenceEvent),
    LeaseExpired(EvidenceEvent),
    ReleaseBuffered(EvidenceEvent),
    PostExpiryHandler(EvidenceEvent),
    ProbeUnavailable(EvidenceEvent),
    PostExpiryReadServed(EvidenceEvent),
    PostExpiryLeaseRenewed(EvidenceEvent),
    PostExpiryUnexpectedError {
        event: EvidenceEvent,
        code: u64,
    },
    DuplicateTerminal(EvidenceEvent),
    CoverageLost {
        event: EvidenceEvent,
        reason: &'static str,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RequestDisposition {
    pub hold: bool,
    pub actions: Vec<Action>,
}

impl RequestDisposition {
    fn forward(actions: Vec<Action>) -> Self {
        Self {
            hold: false,
            actions,
        }
    }

    fn hold(actions: Vec<Action>) -> Self {
        Self {
            hold: true,
            actions,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    AwaitingFastPath,
    Claiming,
    Isolating,
    Released,
    Complete,
    Violation,
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Attempt {
    request: RequestId,
    term: Option<u64>,
    handler_confirmed: bool,
    response: Option<ClientResponse>,
}

impl Attempt {
    fn new(request: RequestId) -> Self {
        Self {
            request,
            term: None,
            handler_confirmed: false,
            response: None,
        }
    }

    fn event(&self, fallback_term: u64) -> EvidenceEvent {
        EvidenceEvent {
            term: self.term.unwrap_or(fallback_term),
            request: self.request.clone(),
        }
    }
}

#[derive(Debug)]
pub(super) struct LeaseIsolation {
    phase: Phase,
    lease_active: bool,
    lease_leader: bool,
    lease_term: u64,
    fast_path: Option<Attempt>,
    buffered: Option<Attempt>,
    probe_terminal: Option<RequestId>,
    post_expiry_clients: BTreeSet<String>,
    isolated_term: Option<u64>,
    fast_path_request: Option<RequestId>,
    lease_expired: bool,
}

impl Default for LeaseIsolation {
    fn default() -> Self {
        Self {
            phase: Phase::AwaitingFastPath,
            lease_active: false,
            lease_leader: false,
            lease_term: 0,
            fast_path: None,
            buffered: None,
            probe_terminal: None,
            post_expiry_clients: BTreeSet::new(),
            isolated_term: None,
            fast_path_request: None,
            lease_expired: false,
        }
    }
}

impl LeaseIsolation {
    pub(super) fn drops_raft(&self) -> bool {
        matches!(self.phase, Phase::Isolating | Phase::Released)
    }

    pub(super) fn observe_lease_state(
        &mut self,
        active: bool,
        leader: bool,
        term: u64,
    ) -> Vec<Action> {
        self.lease_active = active;
        self.lease_leader = leader;
        self.lease_term = term;
        if !matches!(self.phase, Phase::Isolating | Phase::Released) {
            return Vec::new();
        }
        let Some(isolated_term) = self.isolated_term else {
            return Vec::new();
        };
        if term != isolated_term || !leader {
            if self.post_expiry_handler_confirmed() {
                return Vec::new();
            }
            return self.lose_coverage("leader-or-term-changed");
        }
        if active && self.lease_expired {
            self.phase = Phase::Violation;
            return self
                .probe_or_fast_event()
                .map(|event| vec![Action::PostExpiryLeaseRenewed(event)])
                .unwrap_or_default();
        }
        if !active && !self.lease_expired {
            self.lease_expired = true;
            let Some(fast_event) = self.fast_event() else {
                return self.lose_coverage("missing-fast-path-identity");
            };
            let mut actions = vec![Action::LeaseExpired(fast_event)];
            if let Some(event) = self.buffered_event() {
                self.phase = Phase::Released;
                actions.push(Action::ReleaseBuffered(event));
            }
            return actions;
        }
        Vec::new()
    }

    pub(super) fn observe_role(&mut self, leader: bool, term: u64) -> Vec<Action> {
        if !matches!(self.phase, Phase::Isolating | Phase::Released) {
            return Vec::new();
        }
        if Some(term) != self.isolated_term || !leader {
            if self.post_expiry_handler_confirmed() {
                return Vec::new();
            }
            return self.lose_coverage("leader-or-term-changed");
        }
        Vec::new()
    }

    pub(super) fn observe_read_request(
        &mut self,
        request: &RequestId,
        direct: bool,
    ) -> RequestDisposition {
        match self.phase {
            Phase::AwaitingFastPath
                if direct && self.lease_active && self.lease_leader && self.fast_path.is_none() =>
            {
                self.fast_path = Some(Attempt::new(request.clone()));
                RequestDisposition::forward(Vec::new())
            }
            Phase::Isolating if direct && self.lease_expired && self.buffered.is_none() => {
                if self.post_expiry_clients.insert(request.client.clone()) {
                    return RequestDisposition::forward(Vec::new());
                }
                let term = self.isolated_term.unwrap_or(self.lease_term);
                self.buffered = Some(Attempt::new(request.clone()));
                let event = EvidenceEvent {
                    term,
                    request: request.clone(),
                };
                let mut actions = vec![Action::ReadBuffered(event.clone())];
                self.phase = Phase::Released;
                actions.push(Action::ReleaseBuffered(event));
                RequestDisposition::hold(actions)
            }
            _ => RequestDisposition::forward(Vec::new()),
        }
    }

    pub(super) fn observe_read_handler(
        &mut self,
        request: &RequestId,
        active: bool,
        leader: bool,
        term: u64,
    ) -> Vec<Action> {
        self.lease_active = active;
        self.lease_leader = leader;
        self.lease_term = term;
        if matches!(self.phase, Phase::AwaitingFastPath | Phase::Claiming) {
            let Some(attempt) = self
                .fast_path
                .as_mut()
                .filter(|attempt| attempt.request == *request)
            else {
                return Vec::new();
            };
            if active && leader {
                attempt.handler_confirmed = true;
                attempt.term = Some(term);
                return self.finish_fast_path_if_ready();
            }
            self.fast_path = None;
            self.phase = Phase::AwaitingFastPath;
            return Vec::new();
        }
        if self.phase != Phase::Released {
            return Vec::new();
        }
        let Some(attempt) = self
            .buffered
            .as_mut()
            .filter(|attempt| attempt.request == *request)
        else {
            return Vec::new();
        };
        if Some(term) != self.isolated_term || !leader {
            return self.lose_coverage("post-expiry-handler-changed-leader-or-term");
        }
        attempt.term = Some(term);
        if active {
            self.phase = Phase::Violation;
            return vec![Action::PostExpiryLeaseRenewed(attempt.event(term))];
        }
        attempt.handler_confirmed = true;
        let event = attempt.event(term);
        let mut actions = vec![Action::PostExpiryHandler(event)];
        actions.extend(self.finish_probe_if_ready());
        actions
    }

    pub(super) fn observe_response(
        &mut self,
        request: &RequestId,
        response: ClientResponse,
    ) -> Vec<Action> {
        if self.probe_terminal.as_ref() == Some(request) {
            return vec![Action::DuplicateTerminal(EvidenceEvent {
                term: self.isolated_term.unwrap_or(self.lease_term),
                request: request.clone(),
            })];
        }
        if matches!(self.phase, Phase::AwaitingFastPath | Phase::Claiming) {
            let Some(attempt) = self
                .fast_path
                .as_mut()
                .filter(|attempt| attempt.request == *request)
            else {
                return Vec::new();
            };
            attempt.response = Some(response);
            return self.finish_fast_path_if_ready();
        }
        if self.phase != Phase::Released {
            return Vec::new();
        }
        let Some(attempt) = self
            .buffered
            .as_mut()
            .filter(|attempt| attempt.request == *request)
        else {
            return Vec::new();
        };
        self.probe_terminal = Some(request.clone());
        attempt.response = Some(response);
        self.finish_probe_if_ready()
    }

    pub(super) fn claim_result(&mut self, claimed: bool) -> Vec<Action> {
        if self.phase != Phase::Claiming {
            return Vec::new();
        }
        if !claimed {
            self.phase = Phase::Disabled;
            self.fast_path = None;
            return Vec::new();
        }
        let Some(attempt) = self.fast_path.take() else {
            self.phase = Phase::AwaitingFastPath;
            return Vec::new();
        };
        let Some(term) = attempt.term else {
            self.phase = Phase::AwaitingFastPath;
            return Vec::new();
        };
        if !self.lease_active || !self.lease_leader || self.lease_term != term {
            self.phase = Phase::AwaitingFastPath;
            return Vec::new();
        }
        self.phase = Phase::Isolating;
        self.isolated_term = Some(term);
        self.fast_path_request = Some(attempt.request.clone());
        self.lease_expired = false;
        vec![Action::FastPathReadOk(EvidenceEvent {
            term,
            request: attempt.request,
        })]
    }

    fn finish_fast_path_if_ready(&mut self) -> Vec<Action> {
        let Some(attempt) = self.fast_path.as_ref() else {
            return Vec::new();
        };
        if attempt.handler_confirmed && attempt.response == Some(ClientResponse::ReadOk) {
            if self.phase != Phase::Claiming {
                self.phase = Phase::Claiming;
                return vec![Action::Claim];
            }
            return Vec::new();
        }
        if matches!(
            attempt.response,
            Some(ClientResponse::TemporarilyUnavailable | ClientResponse::UnexpectedError(_))
        ) {
            self.fast_path = None;
            self.phase = Phase::AwaitingFastPath;
        }
        Vec::new()
    }

    fn finish_probe_if_ready(&mut self) -> Vec<Action> {
        let Some(attempt) = self.buffered.as_ref() else {
            return Vec::new();
        };
        if !attempt.handler_confirmed {
            return Vec::new();
        }
        let event = attempt.event(self.isolated_term.unwrap_or_default());
        let Some(response) = attempt.response else {
            return Vec::new();
        };
        self.buffered = None;
        match response {
            ClientResponse::TemporarilyUnavailable => {
                self.phase = Phase::Complete;
                vec![Action::ProbeUnavailable(event)]
            }
            ClientResponse::ReadOk => {
                self.phase = Phase::Violation;
                vec![Action::PostExpiryReadServed(event)]
            }
            ClientResponse::UnexpectedError(code) => {
                self.phase = Phase::Complete;
                vec![Action::PostExpiryUnexpectedError { event, code }]
            }
        }
    }

    fn fast_event(&self) -> Option<EvidenceEvent> {
        Some(EvidenceEvent {
            term: self.isolated_term?,
            request: self.fast_path_request.clone()?,
        })
    }

    fn buffered_event(&self) -> Option<EvidenceEvent> {
        Some(EvidenceEvent {
            term: self.isolated_term?,
            request: self.buffered.as_ref()?.request.clone(),
        })
    }

    fn probe_or_fast_event(&self) -> Option<EvidenceEvent> {
        self.buffered_event().or_else(|| self.fast_event())
    }

    fn post_expiry_handler_confirmed(&self) -> bool {
        self.phase == Phase::Released
            && self
                .buffered
                .as_ref()
                .is_some_and(|attempt| attempt.handler_confirmed)
    }

    fn lose_coverage(&mut self, reason: &'static str) -> Vec<Action> {
        let event = self.probe_or_fast_event();
        self.phase = Phase::Complete;
        event
            .map(|event| vec![Action::CoverageLost { event, reason }])
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
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
}
