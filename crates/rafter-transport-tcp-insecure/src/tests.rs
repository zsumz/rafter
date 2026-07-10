use std::collections::VecDeque;
use std::io::{self, Cursor, ErrorKind, Write};

use rafter::{
    AppendEntries, AppendEntriesResponse, LogIndex, Message, RequestVote, RequestVoteResponse, Term,
};

use super::*;

#[test]
fn length_prefixed_frame_round_trips_a_peer_message() {
    let message = Message::RequestVote(RequestVote {
        term: Term(7),
        candidate_id: NodeId(1),
        last_log_index: LogIndex(9),
        last_log_term: Term(6),
    });
    let mut bytes = Vec::new();

    write_message_frame(&mut bytes, &message).expect("frame writes");

    let encoded_len = u32::from_be_bytes(bytes[0..4].try_into().expect("length prefix"));
    assert_eq!(encoded_len as usize, bytes.len() - 4);
    let decoded =
        read_message_frame(&mut Cursor::new(bytes), DEFAULT_MAX_FRAME_LEN).expect("frame reads");
    assert_eq!(decoded, message);
}

#[test]
fn length_prefixed_frame_reuses_encode_buffer() {
    let message = Message::RequestVote(RequestVote {
        term: Term(7),
        candidate_id: NodeId(1),
        last_log_index: LogIndex(9),
        last_log_term: Term(6),
    });
    let mut bytes = Vec::new();
    let mut scratch = vec![0; 256];
    let original_ptr = scratch.as_ptr();

    write_message_frame_into(&mut bytes, &mut scratch, &message).expect("frame writes");

    assert_eq!(scratch.as_ptr(), original_ptr);
    let decoded =
        read_message_frame(&mut Cursor::new(bytes), DEFAULT_MAX_FRAME_LEN).expect("frame reads");
    assert_eq!(decoded, message);
}

#[test]
fn burst_frames_decode_in_send_order_from_one_buffer() {
    let messages = vec![
        Message::RequestVote(RequestVote {
            term: Term(7),
            candidate_id: NodeId(1),
            last_log_index: LogIndex(9),
            last_log_term: Term(6),
        }),
        Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(7),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
        Message::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(7),
            follower_id: NodeId(3),
            success: true,
            match_index: LogIndex(11),
            sequence: 4,
        }),
    ];
    let mut bytes = Vec::new();
    for message in &messages {
        write_message_frame(&mut bytes, message).expect("frame writes");
    }

    let mut reader = Cursor::new(bytes);
    for expected in messages {
        let decoded = read_message_frame(&mut reader, DEFAULT_MAX_FRAME_LEN).expect("frame reads");
        assert_eq!(decoded, expected);
    }
}

#[test]
fn slow_peer_write_error_is_reported_by_frame_writer() {
    struct SlowPeerWriter {
        writes: usize,
    }

    impl Write for SlowPeerWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            if self.writes == 1 {
                Ok(bytes.len())
            } else {
                Err(io::Error::new(ErrorKind::TimedOut, "slow peer"))
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let message = Message::RequestVote(RequestVote {
        term: Term(7),
        candidate_id: NodeId(1),
        last_log_index: LogIndex(9),
        last_log_term: Term(6),
    });
    let error = write_message_frame(&mut SlowPeerWriter { writes: 0 }, &message)
        .expect_err("write times out");

    assert!(matches!(
        error,
        WriteFrameError::Io(error) if error.kind() == ErrorKind::TimedOut
    ));
}

#[test]
fn oversized_frame_is_rejected_before_payload_allocation() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&9u32.to_be_bytes());

    let error = read_message_frame(&mut Cursor::new(bytes), 8).expect_err("frame is too large");

    assert!(matches!(
        error,
        ReadFrameError::FrameTooLarge { len: 9, max: 8 }
    ));
}

#[test]
fn message_sender_reads_every_peer_message_sender_field() {
    let append = Message::AppendEntries(AppendEntries {
        term: Term(2),
        leader_id: NodeId(1),
        prev_log_index: LogIndex::ZERO,
        prev_log_term: Term::default(),
        entries: Vec::new().into(),
        leader_commit: LogIndex::ZERO,
        sequence: 3,
    });
    let response = Message::AppendEntriesResponse(AppendEntriesResponse {
        term: Term(2),
        follower_id: NodeId(3),
        success: true,
        match_index: LogIndex(4),
        sequence: 3,
    });
    let vote = Message::RequestVoteResponse(RequestVoteResponse {
        term: Term(2),
        voter_id: NodeId(2),
        vote_granted: true,
    });

    assert_eq!(message_sender(&append), NodeId(1));
    assert_eq!(message_sender(&response), NodeId(3));
    assert_eq!(message_sender(&vote), NodeId(2));
}

#[test]
fn peer_down_connect_retries_then_reports_last_error() {
    let addr = "127.0.0.1:1".parse().expect("valid socket address");
    let backoff = ReconnectBackoff {
        max_attempts: 3,
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(20),
    };
    let mut attempts = 0;
    let mut sleeps = Vec::new();

    let result: Result<(), TcpTransportError> = connect_with_backoff_using(
        NodeId(2),
        addr,
        backoff,
        |_| {
            attempts += 1;
            Err(io::Error::new(ErrorKind::ConnectionRefused, "peer down"))
        },
        |delay| sleeps.push(delay),
    );

    assert_eq!(attempts, 3);
    assert_eq!(
        sleeps,
        vec![Duration::from_millis(5), Duration::from_millis(10)]
    );
    assert!(matches!(
        result,
        Err(TcpTransportError::Connect {
            peer: NodeId(2),
            source
        }) if source.kind() == ErrorKind::ConnectionRefused
    ));
}

#[test]
fn transient_connect_failure_reconnects_before_success() {
    let addr = "127.0.0.1:1".parse().expect("valid socket address");
    let backoff = ReconnectBackoff {
        max_attempts: 4,
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(20),
    };
    let mut outcomes = VecDeque::from([
        Err(io::Error::new(ErrorKind::ConnectionRefused, "first")),
        Err(io::Error::new(ErrorKind::TimedOut, "second")),
        Ok("connected"),
    ]);
    let mut sleeps = Vec::new();

    let result = connect_with_backoff_using(
        NodeId(2),
        addr,
        backoff,
        |_| outcomes.pop_front().expect("outcome exists"),
        |delay| sleeps.push(delay),
    );

    assert!(matches!(result, Ok("connected")));
    assert_eq!(
        sleeps,
        vec![Duration::from_millis(5), Duration::from_millis(10)]
    );
}

#[test]
#[ignore = "requires loopback TCP bind"]
fn tcp_transport_sends_one_length_prefixed_frame() {
    let receiver = InsecureTcpTransport::bind("127.0.0.1:0", BTreeMap::new())
        .expect("receiver binds")
        .with_reconnect_backoff(ReconnectBackoff::once());
    let receiver_addr = receiver.local_addr().expect("receiver address");
    let sender =
        InsecureTcpTransport::bind("127.0.0.1:0", BTreeMap::from([(NodeId(2), receiver_addr)]))
            .expect("sender binds")
            .with_reconnect_backoff(ReconnectBackoff::once());
    let message = Message::RequestVote(RequestVote {
        term: Term(1),
        candidate_id: NodeId(1),
        last_log_index: LogIndex::ZERO,
        last_log_term: Term::default(),
    });
    let mut scratch = vec![0; 256];
    let original_ptr = scratch.as_ptr();

    sender
        .send_with_scratch(NodeId(2), &message, &mut scratch)
        .expect("message sends");
    let inbound = receiver.receive().expect("message receives");

    assert_eq!(scratch.as_ptr(), original_ptr);
    assert_eq!(inbound.from, NodeId(1));
    assert_eq!(inbound.message, message);
}

#[test]
#[ignore = "requires loopback TCP bind"]
fn tcp_transport_reports_unknown_peer_without_connecting() {
    let sender = InsecureTcpTransport::bind("127.0.0.1:0", BTreeMap::new())
        .expect("sender binds")
        .with_reconnect_backoff(ReconnectBackoff::once());
    let message = Message::RequestVote(RequestVote {
        term: Term(1),
        candidate_id: NodeId(1),
        last_log_index: LogIndex::ZERO,
        last_log_term: Term::default(),
    });

    let error = sender
        .send(NodeId(2), &message)
        .expect_err("peer is not configured");

    assert!(matches!(error, TcpTransportError::UnknownPeer(NodeId(2))));
}
