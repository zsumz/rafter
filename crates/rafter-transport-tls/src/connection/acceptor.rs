//! Nonblocking listener with bounded receiver ownership.

use std::{
    io,
    net::{Shutdown, TcpListener},
    sync::Arc,
    thread,
};

use crate::diagnostics::increment;
use crate::runtime::{run_guarded, RuntimeControl};
use crate::{GroupIdCodec, RuntimeLimits, TransportTimeouts};

use super::{
    receiver::{receive_loop, ConnectionPermit, ReceiverContext, ReceiverTemplate},
    receiver_registry::{ReceiverRegistry, ReceiverWorker},
};

pub(crate) struct AcceptorContext<G, C> {
    pub(crate) listener: TcpListener,
    pub(crate) receivers: ReceiverTemplate<G, C>,
    pub(crate) registry: Arc<ReceiverRegistry>,
    pub(crate) control: Arc<RuntimeControl>,
    pub(crate) limits: RuntimeLimits,
    pub(crate) timeouts: TransportTimeouts,
}

pub(crate) fn accept_loop<G, C>(context: &AcceptorContext<G, C>)
where
    G: Ord + Send + Sync + 'static,
    C: GroupIdCodec<G>,
{
    let mut next_worker = 1_u64;
    while !context.control.shutdown_requested() {
        if context.control.starting() {
            thread::sleep(context.timeouts.poll());
            continue;
        }
        if context.registry.reap_finished().is_err() {
            context.control.fail("receiver registry state is poisoned");
            break;
        }
        match context.listener.accept() {
            Ok((socket, _address)) => {
                if socket.set_nonblocking(false).is_err() {
                    increment(&context.receivers.counters.tls_failures);
                    let _ = socket.shutdown(Shutdown::Both);
                    continue;
                }
                let counters = Arc::clone(&context.receivers.counters);
                let Some(permit) =
                    ConnectionPermit::acquire(counters, context.limits.max_inbound_connections())
                else {
                    increment(&context.receivers.counters.connection_full);
                    let _ = socket.shutdown(Shutdown::Both);
                    continue;
                };
                let Ok(shutdown_socket) = socket.try_clone() else {
                    increment(&context.receivers.counters.tls_failures);
                    continue;
                };
                let shutdown_socket = Arc::new(shutdown_socket);
                let name = format!(
                    "rafter-tls-receiver-{}-{next_worker}",
                    context.receivers.handshake.local_peer_id()
                );
                let Some(following) = next_worker.checked_add(1) else {
                    context
                        .control
                        .fail("receiver worker identity is exhausted");
                    break;
                };
                next_worker = following;
                let receiver = ReceiverContext {
                    template: context.receivers.clone(),
                    socket,
                    shutdown_socket: Arc::clone(&shutdown_socket),
                    permit,
                };
                let worker_control = Arc::clone(&context.control);
                let worker_role = name.clone();
                let handle = match thread::Builder::new().name(name.clone()).spawn(move || {
                    let guarded = Arc::clone(&worker_control);
                    run_guarded(&guarded, &worker_role, || receive_loop(receiver));
                }) {
                    Ok(handle) => handle,
                    Err(error) => {
                        context.control.fail(format!(
                            "could not spawn authenticated receiver worker: {error}"
                        ));
                        break;
                    }
                };
                let worker = ReceiverWorker {
                    name,
                    handle,
                    shutdown_socket,
                };
                if let Err(worker) = context.registry.push(worker) {
                    worker.shut_down();
                    let _ = worker.join();
                    context.control.fail("receiver registry state is poisoned");
                    let _ = context.registry.shutdown_all();
                    break;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(context.timeouts.poll());
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                if !context.control.shutdown_requested() {
                    increment(&context.receivers.counters.listener_failures);
                    context
                        .control
                        .fail(format!("TLS listener failed: {error}"));
                }
                break;
            }
        }
    }
    let _ = context.registry.shutdown_all();
}
