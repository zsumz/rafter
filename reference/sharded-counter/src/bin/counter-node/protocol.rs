//! Consumer-owned bounded line protocol for the process fixture.

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc,
    },
    thread,
};

use rafter::NodeId;
use rafter_reference_sharded_counter::{
    ClientId, CounterCommand, Delta, GroupId, GroupIncarnation, Sequence, SessionEpoch,
};

const MAX_CLIENT_LINE: usize = 4096;
const MAX_CLIENT_CONNECTIONS: usize = 64;

/// One parsed client request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Request {
    Status,
    Audit,
    OpenSession {
        group_id: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        epoch: SessionEpoch,
    },
    Counter {
        group_id: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        epoch: SessionEpoch,
        sequence: Sequence,
        command: CounterCommand,
    },
    Value {
        group_id: GroupId,
        incarnation: GroupIncarnation,
    },
    Fault {
        group_id: GroupId,
        incarnation: GroupIncarnation,
    },
    CapacityFault {
        group_id: GroupId,
        incarnation: GroupIncarnation,
    },
    PausePeers,
    ResumePeers,
    PeerProbe {
        group_id: GroupId,
        incarnation: GroupIncarnation,
        target: NodeId,
    },
    PauseRecovery,
    ResumeRecovery,
    TransferLeadership {
        group_id: GroupId,
        incarnation: GroupIncarnation,
        target: NodeId,
    },
    Pressure {
        group_id: GroupId,
        incarnation: GroupIncarnation,
        class: PressureClass,
        count: usize,
    },
    Snapshot {
        group_id: GroupId,
        incarnation: GroupIncarnation,
    },
    Slow {
        group_id: GroupId,
        milliseconds: u64,
    },
    Drain {
        group_id: GroupId,
        incarnation: GroupIncarnation,
    },
    Remove {
        group_id: GroupId,
        incarnation: GroupIncarnation,
    },
    Reopen {
        group_id: GroupId,
        incarnation: GroupIncarnation,
        quota: u32,
    },
    Tombstone {
        group_id: GroupId,
        incarnation: GroupIncarnation,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PressureClass {
    Snapshot,
    Bulk,
}

/// Reply channel paired with a write-flush acknowledgment.
#[derive(Debug)]
pub struct ClientReply {
    response: mpsc::Sender<String>,
    flushed: Receiver<()>,
}

impl ClientReply {
    pub fn send(self, response: String, wait_for_flush: bool) {
        if self.response.send(response).is_ok() && wait_for_flush {
            let _ = self.flushed.recv();
        }
    }
}

/// One accepted client request.
#[derive(Debug)]
pub struct Job {
    pub request: Request,
    pub reply: ClientReply,
}

pub fn spawn_client_acceptor(listener: TcpListener, jobs: SyncSender<Job>) {
    let active = Arc::new(AtomicUsize::new(0));
    thread::spawn(move || {
        for accepted in listener.incoming() {
            let Ok(stream) = accepted else {
                continue;
            };
            if active.fetch_add(1, Ordering::AcqRel) >= MAX_CLIENT_CONNECTIONS {
                active.fetch_sub(1, Ordering::AcqRel);
                let mut stream = stream;
                let _ = writeln!(stream, "ERR CLIENT_LIMIT {MAX_CLIENT_CONNECTIONS}");
                continue;
            }
            let jobs = jobs.clone();
            let active = Arc::clone(&active);
            thread::spawn(move || {
                serve_connection(stream, &jobs);
                active.fetch_sub(1, Ordering::AcqRel);
            });
        }
    });
}

fn serve_connection(stream: std::net::TcpStream, jobs: &SyncSender<Job>) {
    let Ok(writer) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(stream);
    let mut writer = writer;
    loop {
        let mut line = String::new();
        let Ok(bytes) = reader
            .by_ref()
            .take((MAX_CLIENT_LINE + 1) as u64)
            .read_line(&mut line)
        else {
            return;
        };
        if bytes == 0 {
            return;
        }
        if line.len() > MAX_CLIENT_LINE {
            let _ = writeln!(writer, "ERR LINE_TOO_LONG {MAX_CLIENT_LINE}");
            return;
        }
        let request = match parse(line.trim_end()) {
            Ok(request) => request,
            Err(detail) => {
                let _ = writeln!(writer, "ERR PROTOCOL {detail}");
                let _ = writer.flush();
                continue;
            }
        };
        let (response_tx, response_rx) = mpsc::channel();
        let (flushed_tx, flushed_rx) = mpsc::channel();
        let job = Job {
            request,
            reply: ClientReply {
                response: response_tx,
                flushed: flushed_rx,
            },
        };
        match jobs.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let _ = writeln!(writer, "ERR CLIENT_QUEUE_FULL");
                let _ = writer.flush();
                continue;
            }
            Err(TrySendError::Disconnected(_)) => return,
        }
        let Ok(response) = response_rx.recv() else {
            return;
        };
        if writeln!(writer, "{response}")
            .and_then(|()| writer.flush())
            .is_err()
        {
            return;
        }
        let _ = flushed_tx.send(());
    }
}

fn parse(line: &str) -> Result<Request, String> {
    let fields = line.split_whitespace().collect::<Vec<_>>();
    let Some(command) = fields.first().copied() else {
        return Err("empty request".to_string());
    };
    match command {
        "STATUS" if fields.len() == 1 => Ok(Request::Status),
        "AUDIT" if fields.len() == 1 => Ok(Request::Audit),
        "OPEN" if fields.len() == 5 => Ok(Request::OpenSession {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
            client_id: ClientId::new(number(fields[3], "client id")?),
            epoch: epoch(fields[4])?,
        }),
        "ADD" if fields.len() == 7 => {
            let delta = fields[6]
                .parse::<i64>()
                .map_err(|_| "delta is not an i64".to_string())
                .and_then(|value| {
                    Delta::new(value).ok_or_else(|| "delta must be nonzero".to_string())
                })?;
            Ok(Request::Counter {
                group_id: group(fields[1])?,
                incarnation: incarnation(fields[2])?,
                client_id: ClientId::new(number(fields[3], "client id")?),
                epoch: epoch(fields[4])?,
                sequence: sequence(fields[5])?,
                command: CounterCommand::Add { delta },
            })
        }
        "READ" if fields.len() == 6 => Ok(Request::Counter {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
            client_id: ClientId::new(number(fields[3], "client id")?),
            epoch: epoch(fields[4])?,
            sequence: sequence(fields[5])?,
            command: CounterCommand::Read,
        }),
        "VALUE" if fields.len() == 3 => Ok(Request::Value {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
        }),
        "FAULT" if fields.len() == 3 => Ok(Request::Fault {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
        }),
        "FAULT_CAPACITY" if fields.len() == 3 => Ok(Request::CapacityFault {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
        }),
        "PAUSE_PEERS" if fields.len() == 1 => Ok(Request::PausePeers),
        "RESUME_PEERS" if fields.len() == 1 => Ok(Request::ResumePeers),
        "PEER_PROBE" if fields.len() == 4 => peer_probe(&fields),
        "PAUSE_RECOVERY" if fields.len() == 1 => Ok(Request::PauseRecovery),
        "RESUME_RECOVERY" if fields.len() == 1 => Ok(Request::ResumeRecovery),
        "TRANSFER" if fields.len() == 4 => Ok(Request::TransferLeadership {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
            target: NodeId(u64::from(number(fields[3], "target node id")?)),
        }),
        "PRESSURE" if fields.len() == 5 => Ok(Request::Pressure {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
            class: match fields[3] {
                "snapshot" => PressureClass::Snapshot,
                "bulk" => PressureClass::Bulk,
                _ => return Err("pressure class must be snapshot or bulk".to_string()),
            },
            count: fields[4]
                .parse()
                .map_err(|_| "pressure count is not a usize".to_string())?,
        }),
        "SNAPSHOT" if fields.len() == 3 => Ok(Request::Snapshot {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
        }),
        "SLOW" if fields.len() == 3 => Ok(Request::Slow {
            group_id: group(fields[1])?,
            milliseconds: fields[2]
                .parse()
                .map_err(|_| "slow delay is not a u64".to_string())?,
        }),
        "DRAIN" if fields.len() == 3 => Ok(Request::Drain {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
        }),
        "REMOVE" if fields.len() == 3 => Ok(Request::Remove {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
        }),
        "REOPEN" if fields.len() == 4 => reopen(&fields),
        "TOMBSTONE" if fields.len() == 3 => Ok(Request::Tombstone {
            group_id: group(fields[1])?,
            incarnation: incarnation(fields[2])?,
        }),
        "SHUTDOWN" if fields.len() == 1 => Ok(Request::Shutdown),
        _ => Err("unknown command or wrong field count".to_string()),
    }
}

fn peer_probe(fields: &[&str]) -> Result<Request, String> {
    Ok(Request::PeerProbe {
        target: NodeId(u64::from(number(fields[1], "target node id")?)),
        group_id: group(fields[2])?,
        incarnation: incarnation(fields[3])?,
    })
}

fn reopen(fields: &[&str]) -> Result<Request, String> {
    Ok(Request::Reopen {
        group_id: group(fields[1])?,
        incarnation: incarnation(fields[2])?,
        quota: number(fields[3], "quota")?,
    })
}

fn group(value: &str) -> Result<GroupId, String> {
    Ok(GroupId::new(number(value, "group id")?))
}

fn incarnation(value: &str) -> Result<GroupIncarnation, String> {
    GroupIncarnation::new(number(value, "incarnation")?)
        .ok_or_else(|| "incarnation must be nonzero".to_string())
}

fn epoch(value: &str) -> Result<SessionEpoch, String> {
    SessionEpoch::new(
        value
            .parse()
            .map_err(|_| "session epoch is not a u64".to_string())?,
    )
    .ok_or_else(|| "session epoch must be nonzero".to_string())
}

fn sequence(value: &str) -> Result<Sequence, String> {
    Sequence::new(
        value
            .parse()
            .map_err(|_| "sequence is not a u64".to_string())?,
    )
    .ok_or_else(|| "sequence must be nonzero".to_string())
}

fn number(value: &str, label: &str) -> Result<u32, String> {
    value.parse().map_err(|_| format!("{label} is not a u32"))
}
