use std::{
    fmt,
    io::{self, BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

/// Time limits for opening and using a line-oriented connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionTimeouts {
    /// Maximum time allowed to open the connection.
    pub connect: Duration,
    /// Maximum time allowed to receive one response line.
    pub read: Duration,
    /// Maximum time allowed to send one request line.
    pub write: Duration,
}

impl ConnectionTimeouts {
    /// Creates explicit connection time limits.
    #[must_use]
    pub const fn new(connect: Duration, read: Duration, write: Duration) -> Self {
        Self {
            connect,
            read,
            write,
        }
    }
}

/// One line-oriented TCP connection.
#[derive(Debug)]
pub struct LineConnection {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl LineConnection {
    /// Opens a connection and applies the supplied read and write limits.
    ///
    /// # Errors
    ///
    /// Returns the socket or configuration error that prevented connection.
    pub fn connect(addr: SocketAddr, timeouts: ConnectionTimeouts) -> io::Result<Self> {
        let stream = TcpStream::connect_timeout(&addr, timeouts.connect)?;
        stream.set_read_timeout(Some(timeouts.read))?;
        stream.set_write_timeout(Some(timeouts.write))?;
        Ok(Self {
            reader: BufReader::new(stream.try_clone()?),
            writer: stream,
        })
    }

    /// Sends one newline-terminated line.
    ///
    /// # Errors
    ///
    /// Returns the underlying write or flush error.
    pub fn send_line(&mut self, line: &str) -> io::Result<()> {
        writeln!(self.writer, "{line}")?;
        self.writer.flush()
    }

    /// Receives one line without its trailing newline.
    ///
    /// # Errors
    ///
    /// Returns the underlying read error, or [`io::ErrorKind::UnexpectedEof`]
    /// when the peer closes before a line arrives.
    pub fn receive_line(&mut self) -> io::Result<String> {
        let mut response = String::new();
        if self.reader.read_line(&mut response)? == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        Ok(response.trim_end().to_string())
    }

    /// Sends one line and receives its response.
    ///
    /// # Errors
    ///
    /// Returns the exact failed stage. A send failure may follow a partial
    /// write, and every receive failure follows a completed send, so neither
    /// stage is safe for an implicit replay.
    pub fn request(&mut self, line: &str) -> Result<String, ExchangeError> {
        self.send_line(line).map_err(ExchangeError::Send)?;
        self.receive_line().map_err(ExchangeError::Receive)
    }
}

/// One failed stage of a line exchange.
#[derive(Debug)]
pub enum ExchangeError {
    /// The request write or flush failed and may have been partial.
    Send(io::Error),
    /// The request was sent, but no complete response was received.
    Receive(io::Error),
}

impl fmt::Display for ExchangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Send(error) => write!(formatter, "send failed with unknown outcome: {error}"),
            Self::Receive(error) => {
                write!(formatter, "receive failed with unknown outcome: {error}")
            }
        }
    }
}

impl std::error::Error for ExchangeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Send(error) | Self::Receive(error) => Some(error),
        }
    }
}

/// A reusable client that reconnects between calls without replaying a call.
#[derive(Debug)]
pub struct ReconnectingClient {
    addr: SocketAddr,
    timeouts: ConnectionTimeouts,
    connection: Option<LineConnection>,
}

impl ReconnectingClient {
    /// Creates a disconnected client for `addr`.
    #[must_use]
    pub const fn new(addr: SocketAddr, timeouts: ConnectionTimeouts) -> Self {
        Self {
            addr,
            timeouts,
            connection: None,
        }
    }

    /// Replaces the address and forgets any existing connection.
    pub fn set_addr(&mut self, addr: SocketAddr) {
        self.addr = addr;
        self.disconnect();
    }

    /// Forgets the current connection.
    pub fn disconnect(&mut self) {
        self.connection = None;
    }

    /// Exchanges one line without ever replaying it implicitly.
    ///
    /// A connection failure occurs before the send and is safe to report as
    /// unattempted. Send and receive failures have unknown outcomes, discard
    /// the socket, and require the caller to make any retry decision.
    ///
    /// # Errors
    ///
    /// Returns the failed stage and its I/O error when the initial connection,
    /// send, or receive stage fails.
    pub fn request(&mut self, line: &str) -> Result<String, RequestError> {
        if self.connection.is_none() {
            self.connection = Some(
                LineConnection::connect(self.addr, self.timeouts).map_err(RequestError::Connect)?,
            );
        }

        let Some(connection) = self.connection.as_mut() else {
            return Err(RequestError::Connect(io::Error::other(
                "connection state was not retained",
            )));
        };
        let exchange = connection.request(line);
        match exchange {
            Ok(response) => Ok(response),
            Err(ExchangeError::Send(source)) => {
                self.connection = None;
                Err(RequestError::SendOutcomeUnknown(source))
            }
            Err(ExchangeError::Receive(source)) => {
                self.connection = None;
                Err(RequestError::ReceiveOutcomeUnknown(source))
            }
        }
    }
}

/// The stage at which a reusable request failed.
#[derive(Debug)]
pub enum RequestError {
    /// The initial connection could not be opened.
    Connect(io::Error),
    /// The write or flush failed after the request may have partially left.
    SendOutcomeUnknown(io::Error),
    /// The send completed but no complete response arrived.
    ReceiveOutcomeUnknown(io::Error),
}

impl fmt::Display for RequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => write!(formatter, "connect failed: {error}"),
            Self::SendOutcomeUnknown(error) => {
                write!(
                    formatter,
                    "send failed; request outcome is unknown: {error}"
                )
            }
            Self::ReceiveOutcomeUnknown(error) => {
                write!(
                    formatter,
                    "receive failed; request outcome is unknown: {error}"
                )
            }
        }
    }
}

impl std::error::Error for RequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(error)
            | Self::SendOutcomeUnknown(error)
            | Self::ReceiveOutcomeUnknown(error) => Some(error),
        }
    }
}
