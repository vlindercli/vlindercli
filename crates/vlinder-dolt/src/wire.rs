//! Postgres wire protocol framing for frontend (client→server) messages.
//!
//! The postgres wire protocol frames messages as:
//!   tag: u8 | length: i32 (includes self, big-endian) | body: [u8]
//!
//! Exception: the initial StartupMessage has no tag byte — just length + payload.
//! `postgres-protocol` handles backend (server→client) parsing natively.
//! This module handles frontend framing that `postgres-protocol` does not cover.

/// Parse one frontend message frame from a byte buffer.
///
/// Returns `Ok(Some((frame, bytes_consumed)))` if a complete frame is available,
/// `Ok(None)` if more bytes are needed, or `Err` if the buffer is malformed.
pub fn parse_frontend_frame(buf: &[u8]) -> Result<Option<(FrontendFrame, usize)>, FrameError> {
    // Need at least tag (1) + length (4) = 5 bytes
    if buf.len() < 5 {
        return Ok(None);
    }

    let tag = buf[0];
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

    if len < 4 {
        return Err(FrameError::InvalidLength(len));
    }

    let total = 1 + len; // tag + (length field value includes itself + body)
    if buf.len() < total {
        return Ok(None);
    }

    Ok(Some((
        FrontendFrame {
            tag,
            bytes: buf[..total].to_vec(),
        },
        total,
    )))
}

#[derive(Debug)]
pub enum FrameError {
    InvalidLength(usize),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::InvalidLength(len) => write!(f, "invalid frame length: {len}"),
        }
    }
}

impl std::error::Error for FrameError {}

/// Query (b'Q') — simple query protocol boundary.
const TAG_QUERY: u8 = b'Q';
/// Sync (b'S') — extended query protocol boundary.
const TAG_SYNC: u8 = b'S';

/// A complete query batch — all frames up to and including a Query or Sync.
pub struct QueryBatch {
    /// Total bytes consumed from the input buffer.
    pub bytes_consumed: usize,
    /// Whether this batch ends at a query boundary (Query or Sync).
    pub is_query_boundary: bool,
}

/// Extract a complete query batch from a buffer.
///
/// A batch is all frontend frames up to and including a `Query` (simple protocol)
/// or `Sync` (extended protocol). Returns `None` if no complete batch is available.
pub fn extract_query_batch(buf: &[u8]) -> Result<Option<QueryBatch>, FrameError> {
    let mut offset = 0;

    loop {
        match parse_frontend_frame(&buf[offset..])? {
            Some((frame, consumed)) => {
                offset += consumed;
                if frame.tag == TAG_QUERY || frame.tag == TAG_SYNC {
                    return Ok(Some(QueryBatch {
                        bytes_consumed: offset,
                        is_query_boundary: true,
                    }));
                }
            }
            None => return Ok(None),
        }
    }
}

/// A parsed frontend message frame.
pub struct FrontendFrame {
    /// Message tag byte (e.g., b'Q' for Query, b'P' for Parse).
    pub tag: u8,
    /// The full frame bytes (tag + length + body), ready to forward.
    pub bytes: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_header_returns_none() {
        // Only 3 bytes — not enough for tag + 4-byte length
        assert!(parse_frontend_frame(&[b'Q', 0, 0]).unwrap().is_none());
    }

    #[test]
    fn incomplete_body_returns_none() {
        // Header says 11 bytes of length, but body is truncated
        let mut buf = Vec::new();
        buf.push(b'Q');
        buf.extend_from_slice(&11_i32.to_be_bytes());
        buf.extend_from_slice(b"SEL"); // only 3 of 7 body bytes
        assert!(parse_frontend_frame(&buf).unwrap().is_none());
    }

    #[test]
    fn invalid_length_returns_error() {
        // Length of 2 is invalid (minimum is 4, which means no body)
        let mut buf = Vec::new();
        buf.push(b'Q');
        buf.extend_from_slice(&2_i32.to_be_bytes());
        assert!(parse_frontend_frame(&buf).is_err());
    }

    #[test]
    fn parse_empty_body_frame() {
        // A frame with length=4 (just the length field itself, no body)
        let mut buf = Vec::new();
        buf.push(b'S'); // Sync
        buf.extend_from_slice(&4_i32.to_be_bytes());
        let (frame, consumed) = parse_frontend_frame(&buf).unwrap().unwrap();
        assert_eq!(frame.tag, b'S');
        assert_eq!(consumed, 5);
    }

    #[test]
    fn extra_bytes_after_frame_are_not_consumed() {
        let mut buf = Vec::new();
        buf.push(b'Q');
        buf.extend_from_slice(&4_i32.to_be_bytes());
        buf.extend_from_slice(b"trailing junk");

        let (frame, consumed) = parse_frontend_frame(&buf).unwrap().unwrap();
        assert_eq!(consumed, 5);
        assert_eq!(frame.bytes.len(), 5);
    }

    // --- Query batch extraction tests ---

    #[test]
    fn batch_from_simple_query() {
        // Simple query protocol: just a Query message
        let mut buf = Vec::new();
        buf.push(b'Q');
        buf.extend_from_slice(&11_i32.to_be_bytes());
        buf.extend_from_slice(b"SELECT\0");

        let batch = extract_query_batch(&buf).unwrap().unwrap();
        assert_eq!(batch.bytes_consumed, 12);
        assert!(batch.is_query_boundary);
    }

    #[test]
    fn batch_from_extended_query_protocol() {
        // Extended protocol: Parse + Bind + Execute + Sync
        let mut buf = Vec::new();
        for &tag in &[b'P', b'B', b'E', b'S'] {
            buf.push(tag);
            buf.extend_from_slice(&4_i32.to_be_bytes()); // minimal frame
        }

        let batch = extract_query_batch(&buf).unwrap().unwrap();
        assert_eq!(batch.bytes_consumed, 20); // 4 frames * 5 bytes each
        assert!(batch.is_query_boundary);
    }

    #[test]
    fn batch_incomplete_without_sync() {
        // Extended protocol frames without Sync — not a complete batch
        let mut buf = Vec::new();
        for &tag in &[b'P', b'B', b'E'] {
            buf.push(tag);
            buf.extend_from_slice(&4_i32.to_be_bytes());
        }

        assert!(extract_query_batch(&buf).unwrap().is_none());
    }

    // --- Single frame parse tests ---

    #[test]
    fn parse_single_query_frame() {
        // Build a Query message: tag 'Q', length = 4 (self) + 7 ("SELECT\0") = 11
        let mut buf = Vec::new();
        buf.push(b'Q');
        buf.extend_from_slice(&11_i32.to_be_bytes());
        buf.extend_from_slice(b"SELECT\0");

        let (frame, consumed) = parse_frontend_frame(&buf).unwrap().unwrap();
        assert_eq!(frame.tag, b'Q');
        assert_eq!(consumed, 12); // 1 tag + 4 len + 7 body
        assert_eq!(frame.bytes, buf);
    }
}
