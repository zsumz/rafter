//! The replica's line-oriented client protocol.
//!
//! One request is one line; one response is one line. Tokens are separated by
//! single spaces and every token is an ASCII word or a decimal integer, so a
//! reader can check a transcript by eye and a client can be written without a
//! parser generator. The protocol is deliberately not versioned or framed: it
//! is the integration composition's client surface, and a production
//! composition would replace it wholesale rather than negotiate with it.
//!
//! # Requests
//!
//! ```text
//! STATUS
//! OPEN_SESSION <client_id> <epoch>
//! SUBMIT <client_id> <epoch> <sequence> OPEN <account>
//! SUBMIT <client_id> <epoch> <sequence> DEPOSIT <account> <amount>
//! SUBMIT <client_id> <epoch> <sequence> TRANSFER <from> <to> <amount>
//! SUBMIT <client_id> <epoch> <sequence> CLOSE <account>
//! QUERY ACCOUNT <account>
//! QUERY SUMMARY
//! LOCAL ACCOUNT <account>
//! LOCAL SUMMARY
//! SHUTDOWN
//! ```
//!
//! `QUERY` is linearizable: it runs behind a read barrier. `LOCAL` reads this
//! replica's own applied state and may be stale; it exists so a test can watch
//! a restarted follower catch up without asking the leader.
//!
//! # Responses
//!
//! ```text
//! STATUS <ready|recovering> <role> <term> <applied> <committed> <leader|->
//! OK <disposition> SESSION <epoch>
//! OK <disposition> MUTATION <mutation_result>
//! OK <disposition> REJECTED <request_rejection>
//! OK ACCOUNT <account> <balance|->
//! OK SUMMARY <open_accounts> <total_balance> <successful_deposits>
//! NOTREADY <applied> <committed>
//! NOTCOMMITTED <reason> <leader|->
//! UNKNOWN <detail...>
//! ABANDONED <detail...>
//! BYE
//! ERR <detail...>
//! ```
//!
//! Every field is one token except a trailing `<detail...>`, which runs to the
//! end of the line. `NOTCOMMITTED` therefore names its reason with a stable
//! token rather than a debug rendering: a client that must distinguish "this
//! node is not the leader" from "this payload is too large" should not have to
//! parse a struct literal, and the leader hint that follows it has to stay
//! findable.
//!
//! The three terminal mutation outcomes are exactly the contract's three, and
//! the distinction survives the process boundary intact:
//!
//! - `OK` carries the replicated response.
//! - `NOTCOMMITTED` is emitted **only** when `rafter-app` reported the local
//!   node refusing the proposal before replication. That is the contract's
//!   provable-refusal criterion, and no other lost outcome may borrow it.
//! - `UNKNOWN` is everything else, including a reply this replica never got to
//!   send. A client that loses its connection observes the same thing by
//!   observing nothing, which is why a killed leader needs no protocol support.

use std::fmt;

use rafter::{ProposalRejection, Role};
use rafter_reference_ledger::{
    AccountId, Amount, ApplyDisposition, ClientId, Command, LedgerQuery, LedgerQueryResult,
    LedgerResponse, Mutation, MutationResult, RequestIdentity, RequestRejection, Sequence,
    SessionEpoch,
};

/// One parsed client request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    /// Report readiness and role without being gated by readiness.
    Status,
    /// Replicate one command.
    Submit(Command),
    /// Run one linearizable query behind a read barrier.
    Query(LedgerQuery),
    /// Read this replica's own applied state, which may be stale.
    Local(LedgerQuery),
    /// Stop serving and exit cleanly.
    Shutdown,
}

/// Why a request line could not be turned into a [`Request`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseError(String);

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Parses one request line.
///
/// # Errors
///
/// Returns an error naming the first thing wrong with the line.
pub fn parse_request(line: &str) -> Result<Request, ParseError> {
    let mut tokens = line.split_whitespace();
    let verb = tokens.next().ok_or_else(|| error("empty request"))?;
    let request = match verb {
        "STATUS" => Request::Status,
        "SHUTDOWN" => Request::Shutdown,
        "OPEN_SESSION" => Request::Submit(Command::OpenSession {
            client_id: ClientId::new(next_u32(&mut tokens, "client_id")?),
            session_epoch: next_session_epoch(&mut tokens)?,
        }),
        "SUBMIT" => Request::Submit(parse_submit(&mut tokens)?),
        "QUERY" => Request::Query(parse_query(&mut tokens)?),
        "LOCAL" => Request::Local(parse_query(&mut tokens)?),
        other => return Err(error(&format!("unknown verb {other}"))),
    };
    if tokens.next().is_some() {
        return Err(error("trailing tokens"));
    }
    Ok(request)
}

fn parse_submit<'a>(tokens: &mut impl Iterator<Item = &'a str>) -> Result<Command, ParseError> {
    let request = RequestIdentity {
        client_id: ClientId::new(next_u32(tokens, "client_id")?),
        session_epoch: next_session_epoch(tokens)?,
        sequence: Sequence::new(next_u64(tokens, "sequence")?)
            .ok_or_else(|| error("sequence must be nonzero"))?,
    };
    let kind = tokens.next().ok_or_else(|| error("missing mutation"))?;
    let mutation = match kind {
        "OPEN" => Mutation::OpenAccount {
            account_id: next_account(tokens)?,
        },
        "DEPOSIT" => Mutation::Deposit {
            account_id: next_account(tokens)?,
            amount: next_amount(tokens)?,
        },
        "TRANSFER" => Mutation::Transfer {
            from: next_account(tokens)?,
            to: next_account(tokens)?,
            amount: next_amount(tokens)?,
        },
        "CLOSE" => Mutation::CloseAccount {
            account_id: next_account(tokens)?,
        },
        other => return Err(error(&format!("unknown mutation {other}"))),
    };
    Ok(Command::Execute { request, mutation })
}

fn parse_query<'a>(tokens: &mut impl Iterator<Item = &'a str>) -> Result<LedgerQuery, ParseError> {
    match tokens.next().ok_or_else(|| error("missing query"))? {
        "ACCOUNT" => Ok(LedgerQuery::GetAccount {
            account_id: next_account(tokens)?,
        }),
        "SUMMARY" => Ok(LedgerQuery::GetLedgerSummary),
        other => Err(error(&format!("unknown query {other}"))),
    }
}

/// Renders the whole of one replicated command's outcome.
#[must_use]
pub fn render_applied(disposition: ApplyDisposition, response: &LedgerResponse) -> String {
    format!(
        "OK {} {}",
        render_disposition(disposition),
        render_response(response)
    )
}

/// Renders one linearizable or local query result.
#[must_use]
pub fn render_query_result(result: LedgerQueryResult) -> String {
    match result {
        LedgerQueryResult::Account {
            account_id,
            balance,
        } => {
            let balance = balance.map_or_else(|| String::from("-"), |balance| balance.to_string());
            format!("OK ACCOUNT {} {balance}", account_id.get())
        }
        LedgerQueryResult::Summary(summary) => format!(
            "OK SUMMARY {} {} {}",
            summary.open_accounts, summary.total_balance, summary.successful_deposits
        ),
    }
}

/// Renders a provable refusal as a stable token plus the leader hint.
///
/// Only the app layer's pre-append admission check produces one of these, so
/// this line is the contract's provable-refusal criterion crossing the process
/// boundary. The token is stable because a client routes on it; the rejection's
/// own debug detail is deliberately dropped rather than escaped, because a
/// multi-token field would make the leader hint that follows it unfindable.
#[must_use]
pub fn render_not_committed(reason: &ProposalRejection, leader_hint: Option<u64>) -> String {
    let reason = match reason {
        ProposalRejection::NotLeader { .. } => "NOT_LEADER",
        ProposalRejection::LeadershipTransferInProgress { .. } => "TRANSFER_IN_PROGRESS",
        ProposalRejection::PayloadTooLarge { .. } => "PAYLOAD_TOO_LARGE",
        ProposalRejection::Configuration(_) => "CONFIGURATION",
    };
    let leader = leader_hint.map_or_else(|| String::from("-"), |leader| leader.to_string());
    format!("NOTCOMMITTED {reason} {leader}")
}

/// Renders the readiness and role line, which readiness never gates.
#[must_use]
pub fn render_status(
    ready: bool,
    role: Role,
    term: u64,
    applied: u64,
    committed: u64,
    leader: Option<u64>,
) -> String {
    let readiness = if ready { "ready" } else { "recovering" };
    // `Role` already renders one lowercase token per role, so the protocol
    // borrows Rafter's spelling rather than inventing a second one that could
    // drift from it.
    let leader = leader.map_or_else(|| String::from("-"), |leader| leader.to_string());
    format!("STATUS {readiness} {role} {term} {applied} {committed} {leader}")
}

fn render_disposition(disposition: ApplyDisposition) -> &'static str {
    match disposition {
        ApplyDisposition::SessionOpened => "SESSION_OPENED",
        ApplyDisposition::SessionReplaced => "SESSION_REPLACED",
        ApplyDisposition::SessionAlreadyOpen => "SESSION_ALREADY_OPEN",
        ApplyDisposition::Applied => "APPLIED",
        ApplyDisposition::Replayed => "REPLAYED",
        ApplyDisposition::Rejected => "NOT_ADMITTED",
    }
}

fn render_response(response: &LedgerResponse) -> String {
    match response {
        LedgerResponse::SessionOpened { session_epoch } => {
            format!("SESSION {}", session_epoch.get())
        }
        LedgerResponse::Mutation(result) => format!("MUTATION {}", render_mutation_result(result)),
        LedgerResponse::Rejected(rejection) => {
            format!("REJECTED {}", render_request_rejection(*rejection))
        }
    }
}

fn render_mutation_result(result: &MutationResult) -> String {
    match result {
        MutationResult::AccountOpened => String::from("ACCOUNT_OPENED"),
        MutationResult::Deposited { balance } => format!("DEPOSITED {balance}"),
        MutationResult::Transferred {
            from_balance,
            to_balance,
        } => format!("TRANSFERRED {from_balance} {to_balance}"),
        MutationResult::AccountClosed => String::from("ACCOUNT_CLOSED"),
        MutationResult::Rejected(rejection) => format!("BUSINESS_REJECTED {rejection:?}"),
    }
}

fn render_request_rejection(rejection: RequestRejection) -> String {
    match rejection {
        RequestRejection::ClientOutOfRange => String::from("CLIENT_OUT_OF_RANGE"),
        RequestRejection::SessionNotOpen => String::from("SESSION_NOT_OPEN"),
        RequestRejection::StaleSession { current } => format!("STALE_SESSION {}", current.get()),
        RequestRejection::FutureSession { current } => format!("FUTURE_SESSION {}", current.get()),
        RequestRejection::StaleSequence { highest } => format!("STALE_SEQUENCE {}", highest.get()),
        RequestRejection::SequenceGap { expected } => format!("SEQUENCE_GAP {}", expected.get()),
        RequestRejection::ConflictingRetry => String::from("CONFLICTING_RETRY"),
    }
}

fn next_account<'a>(tokens: &mut impl Iterator<Item = &'a str>) -> Result<AccountId, ParseError> {
    Ok(AccountId::new(next_u64(tokens, "account_id")?))
}

fn next_amount<'a>(tokens: &mut impl Iterator<Item = &'a str>) -> Result<Amount, ParseError> {
    Amount::new(next_u64(tokens, "amount")?).ok_or_else(|| error("amount must be nonzero"))
}

fn next_session_epoch<'a>(
    tokens: &mut impl Iterator<Item = &'a str>,
) -> Result<SessionEpoch, ParseError> {
    SessionEpoch::new(next_u64(tokens, "session_epoch")?)
        .ok_or_else(|| error("session_epoch must be nonzero"))
}

fn next_u32<'a>(
    tokens: &mut impl Iterator<Item = &'a str>,
    field: &str,
) -> Result<u32, ParseError> {
    let token = tokens
        .next()
        .ok_or_else(|| error(&format!("missing {field}")))?;
    token
        .parse()
        .map_err(|_| error(&format!("{field} is not a u32")))
}

fn next_u64<'a>(
    tokens: &mut impl Iterator<Item = &'a str>,
    field: &str,
) -> Result<u64, ParseError> {
    let token = tokens
        .next()
        .ok_or_else(|| error(&format!("missing {field}")))?;
    token
        .parse()
        .map_err(|_| error(&format!("{field} is not a u64")))
}

fn error(detail: &str) -> ParseError {
    ParseError(detail.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(value: u64) -> SessionEpoch {
        SessionEpoch::new(value).expect("nonzero epoch")
    }

    fn sequence(value: u64) -> Sequence {
        Sequence::new(value).expect("nonzero sequence")
    }

    #[test]
    fn every_request_form_parses() {
        assert_eq!(parse_request("STATUS"), Ok(Request::Status));
        assert_eq!(parse_request("SHUTDOWN"), Ok(Request::Shutdown));
        assert_eq!(
            parse_request("OPEN_SESSION 2 5"),
            Ok(Request::Submit(Command::OpenSession {
                client_id: ClientId::new(2),
                session_epoch: epoch(5),
            }))
        );
        assert_eq!(
            parse_request("SUBMIT 1 2 3 TRANSFER 7 8 40"),
            Ok(Request::Submit(Command::Execute {
                request: RequestIdentity {
                    client_id: ClientId::new(1),
                    session_epoch: epoch(2),
                    sequence: sequence(3),
                },
                mutation: Mutation::Transfer {
                    from: AccountId::new(7),
                    to: AccountId::new(8),
                    amount: Amount::new(40).expect("nonzero"),
                },
            }))
        );
        assert_eq!(
            parse_request("QUERY ACCOUNT 9"),
            Ok(Request::Query(LedgerQuery::GetAccount {
                account_id: AccountId::new(9)
            }))
        );
        assert_eq!(
            parse_request("LOCAL SUMMARY"),
            Ok(Request::Local(LedgerQuery::GetLedgerSummary))
        );
    }

    #[test]
    fn a_zero_in_a_nonzero_field_is_refused_rather_than_coerced() {
        assert!(parse_request("OPEN_SESSION 1 0").is_err());
        assert!(parse_request("SUBMIT 1 1 0 OPEN 1").is_err());
        assert!(parse_request("SUBMIT 1 1 1 DEPOSIT 1 0").is_err());
    }

    #[test]
    fn a_line_with_trailing_tokens_is_refused() {
        assert!(parse_request("QUERY SUMMARY extra").is_err());
        assert!(parse_request("STATUS now").is_err());
    }

    #[test]
    fn rendering_names_the_disposition_and_the_response_separately() {
        assert_eq!(
            render_applied(
                ApplyDisposition::Replayed,
                &LedgerResponse::Mutation(MutationResult::Deposited { balance: 50 })
            ),
            "OK REPLAYED MUTATION DEPOSITED 50"
        );
        assert_eq!(
            render_applied(
                ApplyDisposition::Rejected,
                &LedgerResponse::Rejected(RequestRejection::SequenceGap {
                    expected: sequence(4)
                })
            ),
            "OK NOT_ADMITTED REJECTED SEQUENCE_GAP 4"
        );
        assert_eq!(
            render_applied(
                ApplyDisposition::SessionOpened,
                &LedgerResponse::SessionOpened {
                    session_epoch: epoch(3)
                }
            ),
            "OK SESSION_OPENED SESSION 3"
        );
    }

    #[test]
    fn an_absent_account_renders_as_a_dash_rather_than_a_zero_balance() {
        assert_eq!(
            render_query_result(LedgerQueryResult::Account {
                account_id: AccountId::new(4),
                balance: None,
            }),
            "OK ACCOUNT 4 -"
        );
        assert_eq!(
            render_query_result(LedgerQueryResult::Account {
                account_id: AccountId::new(4),
                balance: Some(0),
            }),
            "OK ACCOUNT 4 0"
        );
    }

    #[test]
    fn a_provable_refusal_keeps_its_leader_hint_findable() {
        assert_eq!(
            render_not_committed(
                &ProposalRejection::NotLeader {
                    role: Role::Follower,
                    term: rafter::Term(3),
                    payload_len: 14,
                },
                Some(1),
            ),
            "NOTCOMMITTED NOT_LEADER 1",
            "the reason must stay one token so the hint after it can be found"
        );
        assert_eq!(
            render_not_committed(
                &ProposalRejection::PayloadTooLarge {
                    payload_len: 4096,
                    max_payload_len: 1024,
                },
                None,
            ),
            "NOTCOMMITTED PAYLOAD_TOO_LARGE -"
        );
    }

    #[test]
    fn status_reports_readiness_without_being_gated_by_it() {
        assert_eq!(
            render_status(false, Role::Follower, 3, 7, 9, None),
            "STATUS recovering follower 3 7 9 -"
        );
        assert_eq!(
            render_status(true, Role::Leader, 4, 9, 9, Some(1)),
            "STATUS ready leader 4 9 9 1"
        );
    }
}
