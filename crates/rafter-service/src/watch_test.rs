//! Observable service metrics and the wakeups a watch owes its caller.
//!
//! Current metrics must always be readable, a watch must emit role, term, and
//! index changes and be woken by a publish, poison must be visible through both
//! the current and the changed views, and a closed watch with nothing pending
//! must report no further update rather than block forever.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use rafter::{LogIndex, MembershipConfig, MembershipSet, NodeId, Role, Term};
use rafter_app::{group::GroupFatalState, metrics::RaftGroupMetrics};

use super::*;

#[test]
fn current_metrics_are_always_available() {
    let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(2)));
    let watch = publisher.watch();

    assert_eq!(watch.current().role, Role::Leader);
    assert_eq!(watch.current().term, Term(1));

    publisher.close();

    assert_eq!(watch.current().applied_index, LogIndex(2));
}

#[test]
fn watches_emit_role_term_and_index_changes() {
    let publisher = MetricsPublisher::new(metrics(Role::Follower, Term(1), LogIndex(1)));
    let mut watch = publisher.watch();

    let role_change = metrics(Role::Leader, Term(1), LogIndex(1));
    assert!(publisher.publish(role_change.clone()));
    assert_eq!(block_on(watch.changed()), Some(role_change));

    let term_change = metrics(Role::Leader, Term(2), LogIndex(1));
    assert!(publisher.publish(term_change.clone()));
    assert_eq!(block_on(watch.changed()), Some(term_change));

    let index_change = metrics(Role::Leader, Term(2), LogIndex(8));
    assert!(publisher.publish(index_change.clone()));
    assert_eq!(block_on(watch.changed()), Some(index_change));
}

#[test]
fn pending_watch_is_woken_by_publish() {
    let publisher = MetricsPublisher::new(metrics(Role::Follower, Term(1), LogIndex(1)));
    let mut watch = publisher.watch();
    let mut changed = Box::pin(watch.changed());

    assert!(poll_once(changed.as_mut()).is_pending());

    let update = metrics(Role::Leader, Term(3), LogIndex(5));
    assert!(publisher.publish(update.clone()));

    assert_eq!(poll_once(changed.as_mut()), Poll::Ready(Some(update)));
}

#[test]
fn poison_state_is_visible_in_current_and_changed_metrics() {
    let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(1)));
    let mut watch = publisher.watch();

    let mut poisoned = metrics(Role::Leader, Term(1), LogIndex(1));
    poisoned.fatal_state = GroupFatalState::Poisoned {
        reason: "apply failed".to_owned(),
    };
    assert!(publisher.publish(poisoned.clone()));

    assert_eq!(watch.current().fatal_state, poisoned.fatal_state);
    assert_eq!(block_on(watch.changed()), Some(poisoned));
}

#[test]
fn changed_returns_none_after_close_without_pending_update() {
    let publisher = MetricsPublisher::new(metrics(Role::Leader, Term(1), LogIndex(1)));
    let mut watch = publisher.watch();

    publisher.close();

    assert_eq!(block_on(watch.changed()), None);
    assert!(!publisher.publish(metrics(Role::Follower, Term(2), LogIndex(2))));
}

fn metrics(role: Role, term: Term, index: LogIndex) -> RaftGroupMetrics<u64> {
    RaftGroupMetrics {
        group_id: 7,
        node_id: NodeId(1),
        role,
        term,
        leader_hint: Some(NodeId(1)),
        commit_index: index,
        applied_index: index,
        last_log_index: index,
        snapshot_index: LogIndex::ZERO,
        membership: MembershipConfig::Stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("valid membership"),
        ),
        replication: Vec::new(),
        pending_proposals: 0,
        pending_read_barriers: 0,
        pending_query_reads: 0,
        completed_query_reads: 0,
        reserved_reads: 0,
        fatal_state: GroupFatalState::Healthy,
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    loop {
        match poll_once(future.as_mut()) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    future.poll(&mut context)
}
