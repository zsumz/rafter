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
    /// Returns any error from sending the request or receiving the response.
    pub fn request(&mut self, line: &str) -> io::Result<String> {
        self.send_line(line)?;
        self.receive_line()
    }
}

/// A reusable client that retries one failed exchange on a new connection.
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

    /// Exchanges one line, reopening once only after an exchange fails.
    ///
    /// Failure to open the first connection returns immediately. Once an
    /// exchange has begun, one fresh connection is allowed because a retained
    /// socket may have been closed between calls.
    ///
    /// # Errors
    ///
    /// Returns the failed stage and its I/O error when the initial connection,
    /// replacement connection, or single retry fails.
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
        let first = connection.request(line);
        match first {
            Ok(response) => Ok(response),
            Err(first_error) => {
                self.connection = None;
                let mut connection = match LineConnection::connect(self.addr, self.timeouts) {
                    Ok(connection) => connection,
                    Err(reconnect) => {
                        return Err(RequestError::Reconnect {
                            first: first_error,
                            reconnect,
                        });
                    }
                };
                let response = connection
                    .request(line)
                    .map_err(|retry| RequestError::Retry {
                        first: first_error,
                        retry,
                    })?;
                self.connection = Some(connection);
                Ok(response)
            }
        }
    }
}

/// The stage at which a reusable request failed.
#[derive(Debug)]
pub enum RequestError {
    /// The initial connection could not be opened.
    Connect(io::Error),
    /// The first exchange failed and the replacement connection did not open.
    Reconnect {
        /// Failure from the first exchange.
        first: io::Error,
        /// Failure while opening the replacement.
        reconnect: io::Error,
    },
    /// Both the first exchange and the single retry failed.
    Retry {
        /// Failure from the first exchange.
        first: io::Error,
        /// Failure from the retry.
        retry: io::Error,
    },
}

impl fmt::Display for RequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => write!(formatter, "connect failed: {error}"),
            Self::Reconnect { first, reconnect } => write!(
                formatter,
                "request failed ({first}); reconnect failed: {reconnect}"
            ),
            Self::Retry { first, retry } => {
                write!(formatter, "request failed ({first}); retry failed: {retry}")
            }
        }
    }
}

impl std::error::Error for RequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(error) => Some(error),
            Self::Reconnect { reconnect, .. } => Some(reconnect),
            Self::Retry { retry, .. } => Some(retry),
        }
    }
}
