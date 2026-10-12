//! A small loopback-only client API, independent of the TLS peer connections.
use crate::{peers, Handle, Result, Sender};
use rafter::Role;
use rafter_service::{ReadConsistency, WriteError};
use serde_json::json;
use std::{
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};
use tiny_http::{Header, Method, Request, Response, Server};

pub fn serve(
    server: Server,
    handle: Handle,
    sender: Sender,
    stop: &AtomicBool,
    ready: &AtomicBool,
    connections: &AtomicUsize,
) -> Result<()> {
    while !stop.load(Ordering::Acquire) {
        let Some(request) = server.recv_timeout(Duration::from_millis(50))? else {
            continue;
        };
        // One request at a time keeps the tutorial's client work bounded.
        if let Err(error) = respond(
            request,
            &handle,
            &sender,
            stop,
            ready.load(Ordering::Acquire),
            connections.load(Ordering::Acquire),
        ) {
            eprintln!("client connection closed: {error}");
        }
    }
    Ok(())
}

fn respond(
    request: Request,
    handle: &Handle,
    sender: &Sender,
    stop: &AtomicBool,
    ready: bool,
    connections: usize,
) -> Result<()> {
    let metrics = handle.metrics()?.current();
    if request.method() == &Method::Post && request.url() == "/stop" {
        // The owner keeps driving while this waits for actual backend release.
        wait(handle.shutdown())?;
        stop.store(true, Ordering::Release);
        return send(request, 200, json!({"stopping": metrics.node_id.0}));
    }
    if request.method() == &Method::Get && request.url() == "/metrics" {
        // Credits stay observable even while published progress is stale.
        let retained = sender.retained();
        let refusals = sender.backpressure_refusals();
        return send(
            request,
            200,
            json!({
                "node": metrics.node_id.0, "role": format!("{:?}", metrics.role),
                "term": metrics.term.0, "ready": ready,
                "commit": metrics.commit_index.0, "applied": metrics.applied_index.0,
                "snapshot": metrics.snapshot_index.0,
                "pending_writes": metrics.pending_proposals,
                "pending_reads": metrics.pending_query_reads,
                "reserved_reads": metrics.reserved_reads,
                "tls_connections": connections,
                "retained_writes": retained.writes.writes,
                "retained_write_bytes": retained.writes.payload_bytes,
                "write_refusals": refusals.writes,
                "retained_reads": retained.reads.map(|reads| reads.reads),
                "retained_read_bytes": retained.reads.map(|reads| reads.payload_bytes),
                "read_refusals": refusals.reads,
            }),
        );
    }
    if request.method() == &Method::Get && request.url() == "/status" {
        let value = wait(handle.read(Vec::new(), ReadConsistency::Local))?.result;
        return send(
            request,
            200,
            json!({"node": metrics.node_id.0,
            "role": format!("{:?}", metrics.role), "leader": metrics.leader_hint.map(|id| id.0),
            "ready": ready, "local_value": value, "applied": metrics.applied_index.0,
            "tls_connections": connections}),
        );
    }
    let increment = request
        .url()
        .strip_prefix("/add/")
        .and_then(|text| text.parse::<u64>().ok());
    let writing = request.method() == &Method::Post && increment.is_some();
    let reading = request.method() == &Method::Get && request.url() == "/value";
    if !writing && !reading {
        return send(
            request,
            404,
            json!({"error": "use GET /value, GET /status, or POST /add/<amount>"}),
        );
    }
    if !ready {
        return send(request, 503, json!({"error": "node is recovering"}));
    }
    if metrics.role != Role::Leader {
        if let Some(leader) = metrics
            .leader_hint
            .filter(|leader| *leader != metrics.node_id)
        {
            let location = format!(
                "http://{}{}",
                peers::address("COUNTER_HTTP_BASE", 8000, leader.0)?,
                request.url()
            );
            let header =
                Header::from_bytes("Location", location).map_err(|_| "invalid redirect")?;
            request.respond(Response::empty(307).with_header(header))?;
            return Ok(());
        }
        return send(
            request,
            503,
            json!({"error": "waiting for a leader; try again shortly"}),
        );
    }
    let result = if writing {
        let command = increment.unwrap().to_be_bytes().to_vec();
        wait(handle.write(command)).map(|receipt| receipt.result.result)
    } else {
        wait(handle.read(Vec::new(), ReadConsistency::Linearizable)).map(|receipt| receipt.result)
    };
    match result {
        Ok(value) => send(request, 200, json!({"value": value})),
        Err(error) => {
            // A deadline alone proves nothing about a submitted command.
            // Only the managed write error can prove a pre-append refusal.
            let uncertain = writing
                && error
                    .downcast_ref::<WriteError>()
                    .is_none_or(|write| write.fate().may_commit());
            send(
                request,
                503,
                json!({"error": error.to_string(),
                "write_may_have_committed": uncertain}),
            )
        }
    }
}

fn send(request: Request, status: u16, body: serde_json::Value) -> Result<()> {
    let header = Header::from_bytes("Content-Type", "application/json").unwrap();
    request.respond(
        Response::from_string(format!("{body}\n"))
            .with_status_code(status)
            .with_header(header),
    )?;
    Ok(())
}

struct WakeThread(thread::Thread);
impl Wake for WakeThread {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn wait<T, E: std::error::Error + Send + Sync + 'static>(
    future: impl Future<Output = std::result::Result<T, E>>,
) -> Result<T> {
    let waker = Waker::from(Arc::new(WakeThread(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return Ok(result?);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("request timed out; a submitted write may still commit".into());
        }
        thread::park_timeout(remaining);
    }
}
