//! Real Doltgres connection over TCP using postgres wire protocol.
//!
//! Uses `postgres-protocol` for backend message parsing and frontend
//! message construction. Communicates via raw `TcpStream`.

use std::io::{Read, Write};
use std::net::TcpStream;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::{backend, frontend};

use crate::worker::{DoltConnection, DoltConnectionFactory, DoltError};

/// Password for Doltgres SCRAM-SHA-256 authentication.
/// Doltgres default when `user` section is present in config.yaml is "password".
const DOLTGRES_PASSWORD: &str = "password";

/// A live TCP connection to a Doltgres instance.
pub struct TcpDoltConnection {
    stream: TcpStream,
    read_buf: BytesMut,
}

/// Factory that connects to a Doltgres instance at a given address.
pub struct TcpConnectionFactory {
    addr: String,
    user: String,
    database: String,
}

impl TcpConnectionFactory {
    pub fn new(
        addr: impl Into<String>,
        user: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            addr: addr.into(),
            user: user.into(),
            database: database.into(),
        }
    }
}

impl DoltConnectionFactory for TcpConnectionFactory {
    fn connect(&self) -> Result<Box<dyn DoltConnection>, DoltError> {
        let conn = TcpDoltConnection::connect(&self.addr, &self.user, &self.database)?;
        Ok(Box::new(conn))
    }
}

impl TcpDoltConnection {
    /// Connect to Doltgres, perform the startup handshake, return a ready connection.
    pub fn connect(addr: &str, user: &str, database: &str) -> Result<Self, DoltError> {
        let stream = TcpStream::connect(addr)
            .map_err(|e| DoltError::Connection(format!("TCP connect to {addr}: {e}")))?;

        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(8192),
        };

        conn.startup(user, database)?;
        Ok(conn)
    }

    /// Send the StartupMessage, handle SCRAM-SHA-256 auth, read until ReadyForQuery.
    fn startup(&mut self, user: &str, database: &str) -> Result<(), DoltError> {
        let mut buf = BytesMut::new();
        frontend::startup_message(
            [("user", user), ("database", database)].iter().copied(),
            &mut buf,
        )
        .map_err(|e| DoltError::Protocol(format!("build startup: {e}")))?;

        self.write_all(&buf)?;

        // Read the first backend message — either AuthenticationOk or AuthenticationSasl
        self.wait_for_message()?;
        let snapshot = self.read_buf.clone();
        match backend::Message::parse(&mut self.read_buf)
            .map_err(|e| DoltError::Protocol(format!("parse auth: {e}")))?
        {
            Some(backend::Message::AuthenticationOk) => {
                // No auth required — proceed
            }
            Some(backend::Message::AuthenticationSasl(body)) => {
                // Verify SCRAM-SHA-256 is offered
                let mut found = false;
                let mut mechanisms = body.mechanisms();
                while let Ok(Some(mech)) = mechanisms.next() {
                    if mech == "SCRAM-SHA-256" {
                        found = true;
                        break;
                    }
                }
                if !found {
                    return Err(DoltError::Protocol(
                        "server does not offer SCRAM-SHA-256".into(),
                    ));
                }

                self.scram_sha256(user)?;
            }
            Some(_other) => {
                return Err(DoltError::Protocol(
                    "unexpected auth message (expected Ok or SASL)".into(),
                ));
            }
            None => {
                self.read_buf = snapshot;
                return Err(DoltError::Protocol("incomplete auth message".into()));
            }
        }

        self.read_until_ready()?;
        Ok(())
    }

    /// Perform SCRAM-SHA-256 handshake after receiving AuthenticationSasl.
    fn scram_sha256(&mut self, user: &str) -> Result<(), DoltError> {
        // Step 1: Build client-first-message and send SASLInitialResponse
        let scram = scram::ScramClient::new(user, DOLTGRES_PASSWORD, None);
        let (server_first, client_first) = scram.client_first();

        let mut buf = BytesMut::new();
        frontend::sasl_initial_response("SCRAM-SHA-256", client_first.as_bytes(), &mut buf)
            .map_err(|e| DoltError::Protocol(format!("build sasl_initial_response: {e}")))?;
        self.write_all(&buf)?;

        // Step 2: Read AuthenticationSASLContinue (server-first-message)
        self.wait_for_message()?;
        let server_first_data = match backend::Message::parse(&mut self.read_buf)
            .map_err(|e| DoltError::Protocol(format!("parse sasl_continue: {e}")))?
        {
            Some(backend::Message::AuthenticationSaslContinue(body)) => {
                std::str::from_utf8(body.data())
                    .map_err(|e| DoltError::Protocol(format!("sasl_continue not utf8: {e}")))?
                    .to_string()
            }
            Some(_other) => {
                return Err(DoltError::Protocol(
                    "expected AuthenticationSASLContinue".into(),
                ));
            }
            None => {
                return Err(DoltError::Protocol("incomplete sasl_continue".into()));
            }
        };

        // Step 3: Process server-first, build client-final, send SASLResponse
        let client_final = server_first
            .handle_server_first(&server_first_data)
            .map_err(|e| DoltError::Protocol(format!("scram server_first: {e}")))?;

        let (server_final, client_final_msg) = client_final.client_final();

        let mut buf = BytesMut::new();
        frontend::sasl_response(client_final_msg.as_bytes(), &mut buf)
            .map_err(|e| DoltError::Protocol(format!("build sasl_response: {e}")))?;
        self.write_all(&buf)?;

        // Step 4: Read AuthenticationSASLFinal (server-final-message)
        self.wait_for_message()?;
        let server_final_data = match backend::Message::parse(&mut self.read_buf)
            .map_err(|e| DoltError::Protocol(format!("parse sasl_final: {e}")))?
        {
            Some(backend::Message::AuthenticationSaslFinal(body)) => {
                std::str::from_utf8(body.data())
                    .map_err(|e| DoltError::Protocol(format!("sasl_final not utf8: {e}")))?
                    .to_string()
            }
            Some(backend::Message::ErrorResponse(body)) => {
                let mut err_msg = String::from("SCRAM auth error:");
                let mut fields = body.fields();
                while let Ok(Some(field)) = fields.next() {
                    if field.type_() == b'M' {
                        if let Ok(val) = std::str::from_utf8(field.value_bytes()) {
                            err_msg.push(' ');
                            err_msg.push_str(val);
                        }
                    }
                }
                return Err(DoltError::Protocol(err_msg));
            }
            Some(_other) => {
                // Peek at the raw tag byte for debugging
                return Err(DoltError::Protocol(
                    "expected AuthenticationSASLFinal".into(),
                ));
            }
            None => {
                return Err(DoltError::Protocol("incomplete sasl_final".into()));
            }
        };

        // Step 5: Verify server signature
        server_final
            .handle_server_final(&server_final_data)
            .map_err(|e| DoltError::Protocol(format!("scram server_final: {e}")))?;

        // Step 6: Read AuthenticationOk
        self.wait_for_message()?;
        match backend::Message::parse(&mut self.read_buf)
            .map_err(|e| DoltError::Protocol(format!("parse auth_ok: {e}")))?
        {
            Some(backend::Message::AuthenticationOk) => Ok(()),
            Some(_other) => Err(DoltError::Protocol(
                "expected AuthenticationOk after SCRAM".into(),
            )),
            None => Err(DoltError::Protocol("incomplete auth_ok".into())),
        }
    }

    /// Ensure read_buf has at least one parseable message, reading from TCP if needed.
    fn wait_for_message(&mut self) -> Result<(), DoltError> {
        loop {
            // Check if buffer already has a complete message
            let snapshot = self.read_buf.clone();
            match backend::Message::parse(&mut self.read_buf) {
                Ok(Some(_)) => {
                    // Restore — caller will parse it
                    self.read_buf = snapshot;
                    return Ok(());
                }
                Ok(None) => {
                    self.read_buf = snapshot;
                    self.fill_buf()?;
                }
                Err(_) => {
                    self.read_buf = snapshot;
                    self.fill_buf()?;
                }
            }
        }
    }

    /// Write bytes to the TCP stream.
    fn write_all(&mut self, buf: &[u8]) -> Result<(), DoltError> {
        self.stream
            .write_all(buf)
            .map_err(|e| DoltError::Connection(format!("write: {e}")))
    }

    /// Read from TCP into the internal buffer.
    fn fill_buf(&mut self) -> Result<(), DoltError> {
        let mut tmp = [0u8; 8192];
        let n = self
            .stream
            .read(&mut tmp)
            .map_err(|e| DoltError::Connection(format!("read: {e}")))?;
        if n == 0 {
            return Err(DoltError::Connection("connection closed".into()));
        }
        self.read_buf.extend_from_slice(&tmp[..n]);
        Ok(())
    }

    /// Read and discard backend messages until ReadyForQuery.
    /// Returns the raw bytes of all messages read (for forwarding to agent).
    ///
    /// Always consumes through ReadyForQuery to keep the protocol synchronized,
    /// even when an ErrorResponse is encountered.
    fn read_until_ready(&mut self) -> Result<Vec<u8>, DoltError> {
        let mut captured = Vec::new();
        let mut error: Option<String> = None;

        loop {
            // Try to parse a message from the buffer
            let buf_snapshot = self.read_buf.clone();

            match backend::Message::parse(&mut self.read_buf)
                .map_err(|e| DoltError::Protocol(format!("parse backend: {e}")))?
            {
                Some(msg) => {
                    // How many bytes were consumed
                    let consumed = buf_snapshot.len() - self.read_buf.len();
                    captured.extend_from_slice(&buf_snapshot[..consumed]);

                    if matches!(msg, backend::Message::ReadyForQuery(_)) {
                        return match error {
                            Some(err_msg) => Err(DoltError::Protocol(err_msg)),
                            None => Ok(captured),
                        };
                    }

                    if let backend::Message::ErrorResponse(body) = msg {
                        let mut err_msg = String::from("server error:");
                        let mut fields = body.fields();
                        while let Ok(Some(field)) = fields.next() {
                            if field.type_() == b'M' {
                                if let Ok(val) = std::str::from_utf8(field.value_bytes()) {
                                    err_msg.push(' ');
                                    err_msg.push_str(val);
                                }
                            }
                        }
                        error = Some(err_msg);
                        // Continue reading — must consume ReadyForQuery to stay synchronized.
                    }
                }
                None => {
                    // Need more data — restore buffer and read from TCP
                    self.read_buf = buf_snapshot;
                    self.fill_buf()?;
                }
            }
        }
    }

    /// Send a simple query and read all backend response bytes until ReadyForQuery.
    fn simple_query_raw(&mut self, sql: &str) -> Result<Vec<u8>, DoltError> {
        let mut buf = BytesMut::new();
        frontend::query(sql, &mut buf)
            .map_err(|e| DoltError::Protocol(format!("build query: {e}")))?;
        self.write_all(&buf)?;
        self.read_until_ready()
    }

    /// Send a simple query and parse the first column of the first DataRow as a string.
    ///
    /// Always consumes through ReadyForQuery to keep the protocol synchronized.
    fn simple_query_scalar(&mut self, sql: &str) -> Result<Option<String>, DoltError> {
        let mut buf = BytesMut::new();
        frontend::query(sql, &mut buf)
            .map_err(|e| DoltError::Protocol(format!("build query: {e}")))?;
        self.write_all(&buf)?;

        let mut result: Option<String> = None;
        let mut error: Option<String> = None;

        loop {
            let buf_snapshot = self.read_buf.clone();

            match backend::Message::parse(&mut self.read_buf)
                .map_err(|e| DoltError::Protocol(format!("parse backend: {e}")))?
            {
                Some(backend::Message::DataRow(body)) => {
                    if result.is_none() && error.is_none() {
                        result = extract_first_column(&body);
                    }
                }
                Some(backend::Message::ReadyForQuery(_)) => {
                    return match error {
                        Some(err_msg) => Err(DoltError::Protocol(err_msg)),
                        None => Ok(result),
                    };
                }
                Some(backend::Message::ErrorResponse(body)) => {
                    let mut err_msg = String::from("query error:");
                    let mut fields = body.fields();
                    while let Ok(Some(field)) = fields.next() {
                        if field.type_() == b'M' {
                            if let Ok(val) = std::str::from_utf8(field.value_bytes()) {
                                err_msg.push(' ');
                                err_msg.push_str(val);
                            }
                        }
                    }
                    error = Some(err_msg);
                    // Continue reading — must consume ReadyForQuery to stay synchronized.
                }
                Some(_) => {
                    // Skip RowDescription, CommandComplete, etc.
                }
                None => {
                    self.read_buf = buf_snapshot;
                    self.fill_buf()?;
                }
            }
        }
    }

    /// Forward raw bytes and return ALL captured backend bytes including
    /// ErrorResponse if any. Unlike `read_until_ready`, this never returns
    /// Err for ErrorResponse — the error bytes are part of the response and
    /// must be forwarded to the agent so psycopg2 can parse them.
    fn forward_raw(&mut self, frontend_bytes: &[u8]) -> Result<Vec<u8>, DoltError> {
        self.write_all(frontend_bytes)?;

        let mut captured = Vec::new();

        loop {
            let buf_snapshot = self.read_buf.clone();

            match backend::Message::parse(&mut self.read_buf)
                .map_err(|e| DoltError::Protocol(format!("parse backend: {e}")))?
            {
                Some(msg) => {
                    let consumed = buf_snapshot.len() - self.read_buf.len();
                    captured.extend_from_slice(&buf_snapshot[..consumed]);

                    if matches!(msg, backend::Message::ReadyForQuery(_)) {
                        return Ok(captured);
                    }
                    // All other messages (including ErrorResponse) are captured
                    // and forwarded — they are valid wire protocol.
                }
                None => {
                    self.read_buf = buf_snapshot;
                    self.fill_buf()?;
                }
            }
        }
    }
}

/// Extract the first column of a DataRow as a UTF-8 string.
fn extract_first_column(body: &backend::DataRowBody) -> Option<String> {
    let mut ranges = body.ranges();
    match ranges.next() {
        Ok(Some(Some(range))) => {
            let bytes = &body.buffer()[range];
            std::str::from_utf8(bytes).ok().map(String::from)
        }
        _ => None,
    }
}

impl DoltConnection for TcpDoltConnection {
    fn forward(&mut self, frontend_bytes: &[u8]) -> Result<Vec<u8>, DoltError> {
        self.forward_raw(frontend_bytes)
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>, DoltError> {
        self.simple_query_scalar(sql)
    }

    fn execute(&mut self, sql: &str) -> Result<(), DoltError> {
        self.simple_query_raw(sql)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_first_column_from_data_row() {
        // Build a DataRow message with one column containing "abc123"
        let mut buf = BytesMut::new();

        // DataRow body: i16 num_columns, then for each: i32 len + bytes
        // (or i32 = -1 for NULL)
        let value = b"abc123";
        let num_cols: i16 = 1;
        let col_len: i32 = value.len() as i32;

        buf.extend_from_slice(&num_cols.to_be_bytes());
        buf.extend_from_slice(&col_len.to_be_bytes());
        buf.extend_from_slice(value);

        // Wrap in a full backend message: tag 'D' + length + body
        let body_len = buf.len() as i32 + 4; // length includes itself
        let mut full_msg = BytesMut::new();
        full_msg.extend_from_slice(&[b'D']);
        full_msg.extend_from_slice(&body_len.to_be_bytes());
        full_msg.extend_from_slice(&buf);

        // Parse it using postgres-protocol
        let msg = backend::Message::parse(&mut full_msg).unwrap().unwrap();
        if let backend::Message::DataRow(body) = msg {
            let result = extract_first_column(&body);
            assert_eq!(result, Some("abc123".to_string()));
        } else {
            panic!("expected DataRow");
        }
    }

    #[test]
    fn extract_first_column_null_returns_none() {
        // DataRow with one NULL column (len = -1)
        let mut buf = BytesMut::new();
        let num_cols: i16 = 1;
        let col_len: i32 = -1; // NULL

        buf.extend_from_slice(&num_cols.to_be_bytes());
        buf.extend_from_slice(&col_len.to_be_bytes());

        let body_len = buf.len() as i32 + 4;
        let mut full_msg = BytesMut::new();
        full_msg.extend_from_slice(&[b'D']);
        full_msg.extend_from_slice(&body_len.to_be_bytes());
        full_msg.extend_from_slice(&buf);

        let msg = backend::Message::parse(&mut full_msg).unwrap().unwrap();
        if let backend::Message::DataRow(body) = msg {
            let result = extract_first_column(&body);
            assert_eq!(result, None);
        } else {
            panic!("expected DataRow");
        }
    }

    /// Integration test: requires Doltgres running on localhost:5433.
    /// Run with: cargo test -p vlinder-dolt -- --ignored scram_auth
    #[test]
    #[ignore]
    fn scram_auth_against_live_doltgres() {
        let mut conn = TcpDoltConnection::connect("localhost:5433", "postgres", "postgres")
            .expect("SCRAM connection to Doltgres failed");
        let val = conn.simple_query_scalar("SELECT 1").expect("query failed");
        assert_eq!(val, Some("1".to_string()));
    }

    /// Reproduces the desync scenario: DOLT_COMMIT with nothing to commit
    /// returns ErrorResponse. Before the fix, this left a stale ReadyForQuery
    /// in the buffer, causing all subsequent queries to return wrong data.
    ///
    /// Requires Doltgres running on localhost:5433.
    /// Run with: cargo test -p vlinder-dolt -- --ignored desync
    #[test]
    #[ignore]
    fn connection_stays_synchronized_after_dolt_commit_error() {
        let mut conn = TcpDoltConnection::connect("localhost:5433", "postgres", "postgres")
            .expect("connect failed");

        // SELECT produces no data changes
        let val = conn
            .simple_query_scalar("SELECT 1")
            .expect("SELECT 1 failed");
        assert_eq!(val, Some("1".to_string()));

        // DOLT_COMMIT with nothing to commit → ErrorResponse
        let err = conn.simple_query_raw("SELECT DOLT_COMMIT('-m', 'auto')");
        assert!(
            err.is_err(),
            "DOLT_COMMIT should error with nothing to commit"
        );

        // This is the critical check: connection must still work after the error.
        // Before the fix, this would return wrong data or hang.
        let val = conn
            .simple_query_scalar("SELECT 2")
            .expect("SELECT 2 failed — connection desynchronized after DOLT_COMMIT error");
        assert_eq!(
            val,
            Some("2".to_string()),
            "got wrong query result — desync!"
        );

        // And one more to be sure
        let val = conn
            .simple_query_scalar("SELECT HASHOF('HEAD')")
            .expect("HASHOF failed — connection still desynchronized");
        assert!(val.is_some(), "HASHOF should return a hash");
    }
}
