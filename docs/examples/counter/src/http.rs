//! A small loopback-only client API, independent of the TLS peer connections.
use crate::{peers, Driver, Result};
use rafter::Role;
use rafter_service::{DriverServiceState, ReadOptions, WriteOptions};
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
    driver: Driver,
    stop: Arc<AtomicBool>,
    connections: Arc<AtomicUsize>,
) -> Result<()> {
    while !stop.load(Ordering::Acquire) {
        let Some(request) = server.recv_timeout(Duration::from_millis(50))? else {
            continue;
        };
        // One request at a time keeps the tutorial's client work bounded.
        if let Err(error) = respond(request, &driver, &stop, connections.load(Ordering::Acquire)) {
            eprintln!("client connection closed: {error}");
        }
    }
    Ok(())
}

fn respond(request: Request, driver: &Driver, stop: &AtomicBool, connections: usize) -> Result<()> {
    let (metrics, value, caught_up) = driver.with_group(|group| {
        let metrics = group.metrics();
        let caught_up = metrics.applied_index >= group.committed_application_index()
            && metrics.fatal_state == rafter_app::group::GroupFatalState::Healthy;
        (metrics, group.state_machine().value(), caught_up)
    })?;
    let ready = caught_up
        && !driver.peer_policy_is_stale()
        && driver.service_state() == DriverServiceState::Serving;
    if request.method() == &Method::Get && request.url() == "/status" {
        return send(
            request,
            200,
            json!({"node": metrics.node_id.0,
            "role": format!("{:?}", metrics.role), "leader": metrics.leader_hint.map(|id| id.0),
            "ready": ready, "local_value": value, "applied": metrics.applied_index.0,
            "tls_connections": connections}),
        );
    }
    if request.method() == &Method::Post && request.url() == "/stop" {
        stop.store(true, Ordering::Release);
        return send(request, 200, json!({"stopping": metrics.node_id.0}));
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
        (|| -> Result<u64> {
            let (id, future) = driver.begin_write(increment.unwrap(), WriteOptions::default())?;
            let result = wait(future).map(|receipt| receipt.result);
            if result.is_err() {
                let _ = driver.abandon_write(id);
            }
            result
        })()
    } else {
        (|| -> Result<u64> {
            let (id, future) = driver.begin_read((), ReadOptions::default())?;
            let result = wait(future).map(|receipt| receipt.result);
            if result.is_err() {
                let _ = driver.abandon_read(id);
            }
            result
        })()
    };
    match result {
        Ok(value) => send(request, 200, json!({"value": value})),
        Err(error) => send(
            request,
            503,
            json!({"error": error.to_string(),
            "write_may_have_committed": writing}),
        ),
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
