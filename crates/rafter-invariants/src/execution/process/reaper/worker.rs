//! Observation-only quarantine worker.

use std::sync::{mpsc, Arc, Mutex};

use super::{
    lock_state,
    request::{AnchoredGroupReapRequest, ChildReapRequest, LeasedChildReapRequest, ReapRequest},
    ReaperState,
};
use crate::execution::process::PROCESS_POLL_INTERVAL;

pub(super) fn reap_children(
    child_receiver: &mpsc::Receiver<ChildReapRequest>,
    leased_child_receiver: &mpsc::Receiver<LeasedChildReapRequest>,
    anchored_group_receiver: &mpsc::Receiver<AnchoredGroupReapRequest>,
    state: &Arc<Mutex<ReaperState>>,
) {
    let mut requests = Vec::new();
    let mut child_connected = true;
    let mut leased_child_connected = true;
    let mut anchored_group_connected = true;
    while child_connected
        || leased_child_connected
        || anchored_group_connected
        || !requests.is_empty()
    {
        let received_child = drain_requests(
            child_receiver,
            &mut child_connected,
            &mut requests,
            ReapRequest::from,
        );
        let received_leased_child = drain_requests(
            leased_child_receiver,
            &mut leased_child_connected,
            &mut requests,
            ReapRequest::from,
        );
        let received_anchored_group = drain_requests(
            anchored_group_receiver,
            &mut anchored_group_connected,
            &mut requests,
            ReapRequest::from,
        );
        let mut index = requests.len();
        while index > 0 {
            index -= 1;
            poll_request(&mut requests, index, state);
        }
        if !received_child && !received_leased_child && !received_anchored_group {
            std::thread::sleep(PROCESS_POLL_INTERVAL);
        }
    }
}

fn drain_requests<T>(
    receiver: &mpsc::Receiver<T>,
    connected: &mut bool,
    requests: &mut Vec<ReapRequest>,
    activate: impl Fn(T) -> ReapRequest,
) -> bool {
    let mut found_request = false;
    loop {
        match receiver.try_recv() {
            Ok(request) => {
                requests.push(activate(request));
                found_request = true;
            }
            Err(mpsc::TryRecvError::Empty) => return found_request,
            Err(mpsc::TryRecvError::Disconnected) => {
                *connected = false;
                return found_request;
            }
        }
    }
}

fn poll_request(requests: &mut Vec<ReapRequest>, index: usize, state: &Arc<Mutex<ReaperState>>) {
    let lifetime_released = match requests[index].release_lease_if_quiescent() {
        Ok(released) => released,
        Err(error) => {
            let request = &mut requests[index];
            let child_id = request.child_id();
            if request.mark_lease_error_reported() {
                record_failure(
                    state,
                    format!(
                        "no-signal reaper could not observe process lifetime for child {child_id}: {error}"
                    ),
                );
            }
            false
        }
    };
    if !lifetime_released && requests[index].has_lifetime_lease() {
        return;
    }
    let result =
        take_injected_wait_error(state).map_or_else(|| requests[index].child_mut().try_wait(), Err);
    match result {
        Ok(Some(_)) => {
            let child_id = requests[index].child_id();
            requests.swap_remove(index);
            record_reap(state, child_id);
        }
        Ok(None) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
            ) =>
        {
            let request = &mut requests[index];
            if !*request.retry_error_reported() {
                *request.retry_error_reported() = true;
                record_failure(
                    state,
                    format!(
                        "no-signal reaper will retry {} child {} after transient wait error: {error}",
                        request.role(),
                        request.child_id()
                    ),
                );
            }
        }
        Err(error) => {
            let request = requests.swap_remove(index);
            record_failure(
                state,
                format!(
                    "no-signal reaper could not reap {} child {}: {error}",
                    request.role(),
                    request.child_id()
                ),
            );
        }
    }
}

fn record_reap(state: &Arc<Mutex<ReaperState>>, child_id: u32) {
    let mut state = lock_state(state);
    state.reaped += 1;
    #[cfg(test)]
    state.reaped_children.insert(child_id);
    #[cfg(not(test))]
    let _ = child_id;
}

fn record_failure(state: &Arc<Mutex<ReaperState>>, failure: String) {
    eprintln!("rafter-invariants: {failure}");
    lock_state(state).failures.push(failure);
}

fn take_injected_wait_error(state: &Arc<Mutex<ReaperState>>) -> Option<std::io::Error> {
    #[cfg(test)]
    {
        let mut state = lock_state(state);
        if state.injected_wait_errors > 0 {
            state.injected_wait_errors -= 1;
            return Some(std::io::Error::from(std::io::ErrorKind::Interrupted));
        }
    }
    #[cfg(not(test))]
    let _ = state;
    None
}
