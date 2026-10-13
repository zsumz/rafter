//! Sequential client connections and the explicitly enabled fault harness.

use std::{
    collections::VecDeque,
    io::{self, BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Sender},
    thread,
};

use super::{protocol::parse_request, ClientReply, Job, MAX_JOBS_PER_PASS};

pub(super) fn spawn_acceptor(
    listener: TcpListener,
    jobs: Sender<VecDeque<Job>>,
    fault_harness: bool,
) {
    thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            let jobs = jobs.clone();
            if thread::Builder::new()
                .name(String::from("client"))
                .spawn(move || drop(serve_client(stream, &jobs, fault_harness)))
                .is_err()
            {
                return;
            }
        }
    });
}

/// Ordinary connections answer one request before reading the next line.
///
/// With control-plane fault injection enabled, `HARNESS_BATCH n` reads and
/// validates exactly `n` following requests before publishing them in one queue
/// message. The owner therefore has the entire ordered batch before it handles
/// its first request. The acknowledgement proves admission, not completion.
/// Each request still consumes one slot of the owner's ordinary per-pass budget.
/// Replies identify their batch position and may arrive in any order: waiting
/// for a pending query before flushing a later immediate answer would deadlock
/// an owner waiting for that flush before it can poll the query.
fn serve_client(
    mut stream: TcpStream,
    jobs: &Sender<VecDeque<Job>>,
    fault_harness: bool,
) -> io::Result<()> {
    let mut lines = BufReader::new(stream.try_clone()?).lines();
    while let Some(line) = lines.next() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let batch = fault_harness && line.starts_with("HARNESS_BATCH ");
        let request_lines = if batch {
            let Some(count) = line
                .strip_prefix("HARNESS_BATCH ")
                .and_then(|count| count.parse::<usize>().ok())
                .filter(|count| (1..=MAX_JOBS_PER_PASS).contains(count))
            else {
                writeln!(
                    stream,
                    "ERR harness batch must contain 1..={MAX_JOBS_PER_PASS} requests"
                )?;
                stream.flush()?;
                continue;
            };
            (0..count)
                .map(|_| {
                    lines.next().unwrap_or_else(|| {
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete harness batch",
                        ))
                    })
                })
                .collect::<io::Result<Vec<_>>>()?
        } else {
            vec![line]
        };
        let requests = request_lines
            .iter()
            .map(|line| parse_request(line))
            .collect::<Result<Vec<_>, _>>();
        let requests = match requests {
            Ok(requests) => requests,
            Err(error) => {
                writeln!(stream, "ERR {error}")?;
                stream.flush()?;
                continue;
            }
        };
        let count = requests.len();
        let mut queued = VecDeque::with_capacity(count);
        let mut flush_acks = Vec::with_capacity(count);
        let (responses, replies) = mpsc::channel();
        for (batch_index, request) in requests.into_iter().enumerate() {
            let (flushed, flush_ack) = mpsc::channel();
            queued.push_back(Job {
                request,
                reply: ClientReply {
                    response: responses.clone(),
                    batch_index,
                    flushed: flush_ack,
                },
            });
            flush_acks.push(Some(flushed));
        }
        drop(responses);
        if jobs.send(queued).is_err() {
            return Ok(());
        }
        if batch {
            writeln!(stream, "HARNESS_QUEUED {count}")?;
            stream.flush()?;
        }
        for (index, response) in replies {
            let response = if batch {
                format!("HARNESS_REPLY {index} {response}")
            } else {
                response
            };
            let written = writeln!(stream, "{response}").and_then(|()| stream.flush());
            if let Some(flushed) = flush_acks[index].take() {
                let _ = flushed.send(());
            }
            written?;
        }
    }
    Ok(())
}
