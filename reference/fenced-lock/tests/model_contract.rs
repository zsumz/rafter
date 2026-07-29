mod support;

use rafter_reference_fenced_lock::{
    ApplyDisposition, ApplyOutcome, Command, FencingToken, GuardedRejection, GuardedResource,
    GuardedWrite, HistoryEvent, LeaseDuration, LockRejection, LockResponse, LockService,
    LogicalTime, OperationId, OperationResult, RequestFingerprint, RequestRejection, ResourceName,
    ResourceNameError, Sequence, SessionEpoch, MAX_RESOURCE_NAME_LEN,
};
use support::{
    acquire, client, config, epoch, expire_through, open_session, release, renew, resource,
    sequence, submit, submit_with_fingerprint, time, token,
};

fn acquired(outcome: ApplyOutcome) -> (FencingToken, LogicalTime) {
    match outcome.response {
        LockResponse::Operation(OperationResult::Acquired { token, expiry }) => (token, expiry),
        other => panic!("expected an acquisition, observed {other:?}"),
    }
}

/// Every present lock must outlive replicated logical time. Logical time moves
/// only through `ExpireThrough`, which releases everything it passes.
fn assert_expiry_invariant(service: &LockService) {
    let view = service.view();
    for tracked in &view.resources {
        if let Some(holder) = tracked.holder {
            assert!(
                holder.expiry > view.logical_time,
                "{:?} is held past logical time {:?}",
                tracked.resource,
                view.logical_time
            );
        }
    }
}

#[test]
fn fencing_tokens_strictly_increase_across_acquisitions_of_one_resource() {
    let mut service = LockService::new(config(2, 4));
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));

    let (first, _) = acquired(service.apply(submit(0, 1, 1, acquire("alpha", 10))));
    service.apply(submit(0, 1, 2, release("alpha", first.get())));
    let (second, _) = acquired(service.apply(submit(1, 1, 1, acquire("alpha", 10))));
    service.apply(submit(1, 1, 2, release("alpha", second.get())));
    let (third, _) = acquired(service.apply(submit(0, 1, 3, acquire("alpha", 5))));

    assert_eq!((first, second, third), (token(1), token(2), token(3)));
    assert!(first < second && second < third);
}

#[test]
fn fencing_tokens_are_scoped_to_one_resource_name() {
    let mut service = LockService::new(config(1, 4));
    service.apply(open_session(0, 1));

    let (alpha, _) = acquired(service.apply(submit(0, 1, 1, acquire("alpha", 4))));
    let (beta, _) = acquired(service.apply(submit(0, 1, 2, acquire("beta", 4))));

    assert_eq!((alpha, beta), (token(1), token(1)));
    assert_eq!(service.summary().tracked_resources, 2);
}

#[test]
fn high_water_mark_survives_release_expiration_recreation_and_snapshot() {
    let bounds = config(2, 4);
    let mut service = LockService::new(bounds);
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));

    service.apply(submit(0, 1, 1, acquire("alpha", 10)));
    service.apply(submit(0, 1, 2, release("alpha", 1)));
    let after_release = service.status(resource("alpha"));
    assert_eq!(after_release.token_floor, Some(token(1)));
    assert_eq!(after_release.holder, None);

    service.apply(submit(0, 1, 3, acquire("alpha", 10)));
    service.apply(submit(1, 1, 1, expire_through(10)));
    let after_expiry = service.status(resource("alpha"));
    assert_eq!(after_expiry.token_floor, Some(token(2)));
    assert_eq!(after_expiry.holder, None);

    let mut restored =
        LockService::from_snapshot(bounds, service.snapshot()).expect("valid snapshot restores");
    assert_eq!(restored.view(), service.view());
    assert_eq!(
        restored.status(resource("alpha")).token_floor,
        Some(token(2))
    );

    let (recreated, expiry) = acquired(restored.apply(submit(0, 1, 4, acquire("alpha", 5))));
    assert_eq!((recreated, expiry), (token(3), time(15)));
}

#[test]
fn stale_owner_after_expiration_cannot_modify_the_guarded_resource() {
    let mut service = LockService::new(config(2, 2));
    let mut guard = GuardedResource::new(resource("alpha"));
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));

    let (stale_token, _) = acquired(service.apply(submit(0, 1, 1, acquire("alpha", 10))));
    assert_eq!(guard.apply(guarded_write(stale_token, 7)), Ok(7));

    // The former owner stops responding and its lease lapses.
    service.apply(submit(1, 1, 1, expire_through(10)));
    let (later_token, _) = acquired(service.apply(submit(1, 1, 2, acquire("alpha", 10))));
    assert!(later_token > stale_token);
    assert_eq!(guard.apply(guarded_write(later_token, 9)), Ok(9));

    // The former owner wakes up still holding its old token.
    assert_eq!(
        guard.apply(guarded_write(stale_token, 11)),
        Err(GuardedRejection::StaleFencingToken {
            highest_accepted: later_token
        })
    );
    assert_eq!(guard.value(), 9);
    assert_eq!(guard.refused_writes(), 1);
}

#[test]
fn stale_owner_after_release_cannot_modify_the_guarded_resource() {
    let mut service = LockService::new(config(2, 2));
    let mut guard = GuardedResource::new(resource("alpha"));
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));

    let (stale_token, _) = acquired(service.apply(submit(0, 1, 1, acquire("alpha", 10))));
    assert_eq!(guard.apply(guarded_write(stale_token, 3)), Ok(3));
    service.apply(submit(0, 1, 2, release("alpha", stale_token.get())));

    let (later_token, _) = acquired(service.apply(submit(1, 1, 1, acquire("alpha", 10))));
    assert_eq!(guard.apply(guarded_write(later_token, 4)), Ok(4));

    assert_eq!(
        guard.apply(guarded_write(stale_token, 5)),
        Err(GuardedRejection::StaleFencingToken {
            highest_accepted: later_token
        })
    );
    assert_eq!(guard.value(), 4);
}

#[test]
fn one_tenure_writes_repeatedly_and_other_resources_are_refused() {
    let mut guard = GuardedResource::new(resource("alpha"));

    assert_eq!(guard.apply(guarded_write(token(4), 1)), Ok(1));
    assert_eq!(guard.apply(guarded_write(token(4), 2)), Ok(2));
    assert_eq!(guard.highest_accepted(), Some(token(4)));
    assert_eq!(guard.accepted_writes(), 2);
    assert_eq!(
        guard.apply(GuardedWrite {
            resource: resource("beta"),
            token: token(9),
            value: 3,
        }),
        Err(GuardedRejection::WrongResource)
    );
    assert_eq!(guard.value(), 2);
}

fn guarded_write(held_token: FencingToken, value: u64) -> GuardedWrite {
    GuardedWrite {
        resource: resource("alpha"),
        token: held_token,
        value,
    }
}

#[test]
fn logical_time_advances_strictly_and_equal_horizons_are_rejected() {
    let mut service = LockService::new(config(1, 2));
    service.apply(open_session(0, 1));

    assert_eq!(
        service.apply(submit(0, 1, 1, expire_through(5))).response,
        LockResponse::Operation(OperationResult::Expired {
            released_locks: 0,
            logical_time: time(5)
        })
    );

    let equal_horizon = submit(0, 1, 2, expire_through(5));
    let rejected = service.apply(equal_horizon);
    assert_eq!(
        rejected.response,
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::LogicalTimeNotAdvanced { current: time(5) }
        ))
    );
    assert_eq!(rejected.disposition, ApplyDisposition::Applied);

    // The rejection consumed and cached its sequence like any other outcome.
    let retry = service.apply(equal_horizon);
    assert_eq!(retry.response, rejected.response);
    assert_eq!(retry.disposition, ApplyDisposition::Replayed);

    assert_eq!(
        service.apply(submit(0, 1, 3, expire_through(4))).response,
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::LogicalTimeNotAdvanced { current: time(5) }
        ))
    );
    service.apply(submit(0, 1, 4, expire_through(6)));
    assert_eq!(service.logical_time(), time(6));
}

#[test]
fn a_lease_holds_through_every_logical_time_below_its_expiry() {
    let mut service = LockService::new(config(2, 2));
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));
    let (held, expiry) = acquired(service.apply(submit(0, 1, 1, acquire("alpha", 10))));
    assert_eq!(expiry, time(10));

    assert_eq!(
        service.apply(submit(1, 1, 1, expire_through(9))).response,
        LockResponse::Operation(OperationResult::Expired {
            released_locks: 0,
            logical_time: time(9)
        })
    );
    assert_eq!(
        service
            .status(resource("alpha"))
            .holder
            .map(|lock| lock.token),
        Some(held)
    );
    assert_expiry_invariant(&service);

    assert_eq!(
        service.apply(submit(1, 1, 2, expire_through(10))).response,
        LockResponse::Operation(OperationResult::Expired {
            released_locks: 1,
            logical_time: time(10)
        })
    );
    assert_eq!(service.status(resource("alpha")).holder, None);
    assert_eq!(service.status(resource("alpha")).token_floor, Some(held));
}

#[test]
fn acquisition_after_expiration_issues_a_strictly_higher_token() {
    let mut service = LockService::new(config(2, 2));
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));

    let (before, _) = acquired(service.apply(submit(0, 1, 1, acquire("alpha", 5))));
    service.apply(submit(1, 1, 1, expire_through(5)));
    let (after, expiry) = acquired(service.apply(submit(1, 1, 2, acquire("alpha", 5))));

    assert!(after > before);
    assert_eq!((after, expiry), (token(2), time(10)));
    assert_expiry_invariant(&service);
}

#[test]
fn only_the_owner_presenting_the_current_token_may_renew_or_release() {
    let mut service = LockService::new(config(2, 2));
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));
    service.apply(submit(0, 1, 1, acquire("alpha", 10)));

    assert_eq!(
        service
            .apply(submit(1, 1, 1, renew("alpha", 1, 10)))
            .response,
        LockResponse::Operation(OperationResult::Rejected(LockRejection::NotLockHolder {
            owner: client(0)
        }))
    );
    assert_eq!(
        service.apply(submit(1, 1, 2, release("alpha", 1))).response,
        LockResponse::Operation(OperationResult::Rejected(LockRejection::NotLockHolder {
            owner: client(0)
        }))
    );
    assert_eq!(
        service
            .apply(submit(0, 1, 2, renew("alpha", 99, 10)))
            .response,
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::FencingTokenMismatch { current: token(1) }
        ))
    );
    assert_eq!(
        service
            .apply(submit(0, 1, 3, release("alpha", 99)))
            .response,
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::FencingTokenMismatch { current: token(1) }
        ))
    );
    assert_eq!(
        service
            .status(resource("alpha"))
            .holder
            .map(|lock| lock.owner),
        Some(client(0))
    );
}

#[test]
fn renewal_keeps_the_token_and_never_lowers_the_expiry() {
    let mut service = LockService::new(config(2, 2));
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));
    let (held, _) = acquired(service.apply(submit(0, 1, 1, acquire("alpha", 10))));

    service.apply(submit(1, 1, 1, expire_through(5)));
    assert_eq!(
        service
            .apply(submit(0, 1, 2, renew("alpha", 1, 10)))
            .response,
        LockResponse::Operation(OperationResult::Renewed {
            token: held,
            expiry: time(15)
        })
    );

    // A renewal that would not extend the lease succeeds and changes nothing.
    assert_eq!(
        service
            .apply(submit(0, 1, 3, renew("alpha", 1, 2)))
            .response,
        LockResponse::Operation(OperationResult::Renewed {
            token: held,
            expiry: time(15)
        })
    );

    // Renewal never issues a token, so the high-water mark is untouched.
    assert_eq!(service.status(resource("alpha")).token_floor, Some(held));
    assert_eq!(
        service.apply(submit(0, 1, 4, renew("beta", 1, 5))).response,
        LockResponse::Operation(OperationResult::Rejected(LockRejection::LockNotHeld))
    );
}

#[test]
fn acquiring_a_held_resource_is_rejected_even_for_its_owner() {
    let mut service = LockService::new(config(1, 2));
    service.apply(open_session(0, 1));
    service.apply(submit(0, 1, 1, acquire("alpha", 10)));

    assert_eq!(
        service
            .apply(submit(0, 1, 2, acquire("alpha", 10)))
            .response,
        LockResponse::Operation(OperationResult::Rejected(LockRejection::LockHeld {
            owner: client(0),
            token: token(1),
            expiry: time(10)
        }))
    );
    assert_eq!(
        service.status(resource("alpha")).token_floor,
        Some(token(1))
    );
}

#[test]
fn exact_retry_returns_the_cached_result_without_a_second_effect() {
    let mut service = LockService::new(config(1, 2));
    service.apply(open_session(0, 1));
    let command = submit(0, 1, 1, acquire("alpha", 10));

    let first = service.apply(command);
    let state_after_first = service.view();
    let retry = service.apply(command);

    assert_eq!(first.response, retry.response);
    assert_eq!(retry.disposition, ApplyDisposition::Replayed);
    assert_eq!(service.view(), state_after_first);
}

#[test]
fn conflicting_retry_is_rejected_without_changing_state() {
    let mut service = LockService::new(config(1, 2));
    service.apply(open_session(0, 1));
    service.apply(submit(0, 1, 1, acquire("alpha", 10)));
    let before = service.view();

    let conflict = service.apply(submit(0, 1, 1, acquire("beta", 10)));

    assert_eq!(
        conflict.response,
        LockResponse::Rejected(RequestRejection::ConflictingRetry)
    );
    assert_eq!(conflict.disposition, ApplyDisposition::Rejected);
    assert_eq!(service.view(), before);
}

#[test]
fn sequence_gaps_and_stale_sequences_fail_closed() {
    let mut service = LockService::new(config(1, 2));
    service.apply(open_session(0, 1));

    assert_eq!(
        service
            .apply(submit(0, 1, 2, acquire("alpha", 10)))
            .response,
        LockResponse::Rejected(RequestRejection::SequenceGap {
            expected: sequence(1)
        })
    );
    service.apply(submit(0, 1, 1, acquire("alpha", 10)));
    service.apply(submit(0, 1, 2, renew("alpha", 1, 20)));
    assert_eq!(
        service.apply(submit(0, 1, 4, release("alpha", 1))).response,
        LockResponse::Rejected(RequestRejection::SequenceGap {
            expected: sequence(3)
        })
    );
    assert_eq!(
        service
            .apply(submit(0, 1, 1, acquire("alpha", 10)))
            .response,
        LockResponse::Rejected(RequestRejection::StaleSequence {
            highest: sequence(2)
        })
    );
}

#[test]
fn a_greater_session_epoch_fences_old_commands_and_never_releases_a_lock() {
    let mut service = LockService::new(config(1, 2));
    service.apply(open_session(0, 1));
    let (held, expiry) = acquired(service.apply(submit(0, 1, 1, acquire("alpha", 10))));

    assert_eq!(
        service.apply(open_session(0, 2)).disposition,
        ApplyDisposition::SessionReplaced
    );
    assert_eq!(
        service.apply(submit(0, 1, 2, release("alpha", 1))).response,
        LockResponse::Rejected(RequestRejection::StaleSession { current: epoch(2) })
    );

    // Session replacement clears deduplication state only.
    let status = service.status(resource("alpha"));
    assert_eq!(
        status
            .holder
            .map(|lock| (lock.owner, lock.token, lock.expiry)),
        Some((client(0), held, expiry))
    );
    assert_eq!(
        service
            .apply(submit(0, 2, 1, renew("alpha", 1, 20)))
            .response,
        LockResponse::Operation(OperationResult::Renewed {
            token: held,
            expiry: time(20)
        })
    );
    assert_eq!(
        service.apply(submit(0, 3, 1, release("alpha", 1))).response,
        LockResponse::Rejected(RequestRejection::FutureSession { current: epoch(2) })
    );
}

#[test]
fn a_fingerprint_that_does_not_describe_its_operation_is_rejected() {
    let mut service = LockService::new(config(1, 2));
    service.apply(open_session(0, 1));
    let operation = acquire("alpha", 10);
    let other = RequestFingerprint::of(&release("alpha", 1));

    let rejected = service.apply(submit_with_fingerprint(0, 1, 1, other, operation));
    assert_eq!(
        rejected.response,
        LockResponse::Rejected(RequestRejection::FingerprintMismatch {
            expected: RequestFingerprint::of(&operation)
        })
    );
    assert_eq!(rejected.disposition, ApplyDisposition::Rejected);

    // The malformed envelope consumed no sequence.
    assert_eq!(
        service.apply(submit(0, 1, 1, operation)).disposition,
        ApplyDisposition::Applied
    );
}

#[test]
fn a_deterministic_lock_rejection_consumes_and_caches_its_sequence() {
    let mut service = LockService::new(config(2, 2));
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));
    service.apply(submit(0, 1, 1, acquire("alpha", 10)));

    let blocked = submit(1, 1, 1, acquire("alpha", 10));
    let first = service.apply(blocked);
    assert_eq!(
        first.response,
        LockResponse::Operation(OperationResult::Rejected(LockRejection::LockHeld {
            owner: client(0),
            token: token(1),
            expiry: time(10)
        }))
    );

    service.apply(submit(0, 1, 2, release("alpha", 1)));
    let retry = service.apply(blocked);

    assert_eq!(retry.response, first.response);
    assert_eq!(retry.disposition, ApplyDisposition::Replayed);
    assert_eq!(service.status(resource("alpha")).holder, None);
}

#[test]
fn snapshot_restore_preserves_locks_marks_sessions_and_logical_time() {
    let bounds = config(2, 4);
    let mut service = LockService::new(bounds);
    service.apply(open_session(0, 1));
    service.apply(open_session(1, 1));
    service.apply(submit(0, 1, 1, acquire("alpha", 20)));
    service.apply(submit(0, 1, 2, acquire("beta", 4)));
    service.apply(submit(1, 1, 1, expire_through(6)));
    let cached = submit(0, 1, 3, renew("alpha", 1, 20));
    let original = service.apply(cached);

    let mut restored =
        LockService::from_snapshot(bounds, service.snapshot()).expect("valid snapshot restores");

    assert_eq!(restored.view(), service.view());
    assert_eq!(restored.summary(), service.summary());
    assert_eq!(restored.logical_time(), time(6));
    for name in ["alpha", "beta"] {
        assert_eq!(
            restored.status(resource(name)),
            service.status(resource(name))
        );
    }
    assert_expiry_invariant(&restored);

    // Replay reproduces the cached result rather than re-executing it.
    assert_eq!(restored.apply(cached).response, original.response);
    assert_eq!(
        restored
            .apply(submit(0, 1, 3, release("alpha", 1)))
            .response,
        LockResponse::Rejected(RequestRejection::ConflictingRetry)
    );
}

#[test]
fn tracked_resources_fail_closed_without_reclaiming_a_high_water_mark() {
    let mut service = LockService::new(config(1, 1));
    service.apply(open_session(0, 1));
    service.apply(submit(0, 1, 1, acquire("alpha", 5)));

    assert_eq!(
        service.apply(submit(0, 1, 2, acquire("beta", 5))).response,
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::ResourceCapacityExceeded
        ))
    );
    service.apply(submit(0, 1, 3, release("alpha", 1)));

    // Releasing does not untrack the name, because its mark must outlive it.
    assert_eq!(
        service.apply(submit(0, 1, 4, acquire("beta", 5))).response,
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::ResourceCapacityExceeded
        ))
    );
    let (reacquired, _) = acquired(service.apply(submit(0, 1, 5, acquire("alpha", 5))));
    assert_eq!(reacquired, token(2));
}

#[test]
fn a_lease_that_would_overflow_the_expiry_is_rejected_rather_than_saturated() {
    let mut service = LockService::new(config(1, 2));
    service.apply(open_session(0, 1));
    service.apply(submit(0, 1, 1, expire_through(1)));

    assert_eq!(
        service
            .apply(submit(0, 1, 2, acquire("alpha", u64::MAX)))
            .response,
        LockResponse::Operation(OperationResult::Rejected(LockRejection::LeaseOverflow))
    );
    assert_eq!(service.status(resource("alpha")).token_floor, None);

    // The same lease is admissible while logical time is still zero.
    let mut fresh = LockService::new(config(1, 2));
    fresh.apply(open_session(0, 1));
    let (_, expiry) = acquired(fresh.apply(submit(0, 1, 1, acquire("alpha", u64::MAX))));
    assert_eq!(expiry, time(u64::MAX));
    assert_eq!(
        fresh
            .apply(submit(0, 1, 2, renew("alpha", 1, u64::MAX)))
            .response,
        LockResponse::Operation(OperationResult::Renewed {
            token: token(1),
            expiry: time(u64::MAX)
        })
    );
}

#[test]
fn renew_and_release_do_not_reveal_whether_a_name_was_ever_used() {
    let mut service = LockService::new(config(1, 2));
    service.apply(open_session(0, 1));

    let unknown = service
        .apply(submit(0, 1, 1, release("never-used", 1)))
        .response;
    service.apply(submit(0, 1, 2, acquire("alpha", 5)));
    service.apply(submit(0, 1, 3, release("alpha", 1)));
    let freed = service.apply(submit(0, 1, 4, release("alpha", 1))).response;

    assert_eq!(
        unknown,
        LockResponse::Operation(OperationResult::Rejected(LockRejection::LockNotHeld))
    );
    assert_eq!(freed, unknown);
    assert_eq!(service.status(resource("never-used")).token_floor, None);
}

#[test]
fn resource_names_are_bounded_and_compared_byte_exactly() {
    assert_eq!(ResourceName::new(""), Err(ResourceNameError::Empty));
    assert_eq!(
        ResourceName::new(&"a".repeat(MAX_RESOURCE_NAME_LEN + 1)),
        Err(ResourceNameError::TooLong)
    );
    assert_eq!(
        ResourceName::new("has space"),
        Err(ResourceNameError::InvalidByte)
    );
    assert_eq!(
        ResourceName::new(&"b".repeat(MAX_RESOURCE_NAME_LEN)).map(|name| name.len()),
        Ok(MAX_RESOURCE_NAME_LEN)
    );

    assert_ne!(resource("Alpha"), resource("alpha"));
    assert!(resource("a") < resource("ab"));
    assert!(resource("ab") < resource("b"));
    assert_eq!(resource("lock/one.v2").as_str(), "lock/one.v2");
}

#[test]
fn zero_is_unrepresentable_for_epoch_sequence_lease_and_token() {
    assert_eq!(SessionEpoch::new(0), None);
    assert_eq!(Sequence::new(0), None);
    assert_eq!(LeaseDuration::new(0), None);
    assert_eq!(FencingToken::new(0), None);
    assert_eq!(FencingToken::first(), token(1));
    assert_eq!(
        FencingToken::new(u64::MAX).and_then(FencingToken::successor),
        None
    );
}

#[test]
fn submissions_outside_an_open_session_or_slot_range_are_rejected() {
    let mut service = LockService::new(config(1, 2));

    assert_eq!(
        service.apply(submit(0, 1, 1, acquire("alpha", 5))).response,
        LockResponse::Rejected(RequestRejection::SessionNotOpen)
    );
    assert_eq!(
        service.apply(open_session(1, 1)).response,
        LockResponse::Rejected(RequestRejection::ClientOutOfRange)
    );
    assert_eq!(
        service.apply(submit(1, 1, 1, acquire("alpha", 5))).response,
        LockResponse::Rejected(RequestRejection::ClientOutOfRange)
    );
    assert_eq!(service.summary().tracked_resources, 0);
}

#[test]
fn history_vocabulary_represents_completion_rejection_and_lost_outcomes() {
    let operation_id = OperationId::new(7);
    let command = submit(0, 1, 1, acquire("alpha", 5));
    let history = [
        HistoryEvent::Invoked {
            operation_id,
            command,
        },
        HistoryEvent::Unknown { operation_id },
        HistoryEvent::NotCommitted { operation_id },
        HistoryEvent::Completed {
            operation_id,
            outcome: ApplyOutcome {
                disposition: ApplyDisposition::Rejected,
                response: LockResponse::Rejected(RequestRejection::SessionNotOpen),
            },
        },
    ];

    assert!(history
        .iter()
        .all(|event| event.operation_id() == operation_id));
    let Command::Submit { request, .. } = command else {
        panic!("the invocation carries a submission")
    };
    assert_eq!(history[0].request_identity(), Some(request));
    assert_eq!(history[1].request_identity(), None);
    assert_eq!(history[2].request_identity(), None);
    assert!(matches!(history[1], HistoryEvent::Unknown { .. }));
    // The two lost-outcome events are separate terminal claims. Collapsing them
    // would let a provable refusal be read as a possible commit.
    assert_ne!(history[1], history[2]);
}
