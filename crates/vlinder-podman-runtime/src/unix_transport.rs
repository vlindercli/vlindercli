//! Unix socket transport for hyper 1.x.
//!
//! Provides a `tower_service::Service<Uri>` connector that opens a
//! `tokio::net::UnixStream` to a fixed socket path and returns a local
//! `UnixIo` newtype wrapper to satisfy the orphan rule.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task;

use bytes::Bytes;
use http_body_util::Full;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::UnixStream;

// ── UnixIo newtype ────────────────────────────────────────────────────
//
// `TokioIo<UnixStream>` is a foreign type (from hyper-util), so we cannot
// implement `Connection` (another hyper-util type) for it directly — that
// would violate the orphan rule.  Wrapping it in a local newtype lets us
// hold the `Connection` impl in this crate.

pub(crate) struct UnixIo(TokioIo<UnixStream>);

impl hyper::rt::Read for UnixIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> task::Poll<Result<(), std::io::Error>> {
        // `TokioIo<UnixStream>: Unpin`, so `Pin::new` is safe.
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for UnixIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> task::Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

impl Connection for UnixIo {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

// ── Connector ────────────────────────────────────────────────────────

/// Connector that opens a `tokio::net::UnixStream` to a fixed socket path.
///
/// Cloneable and `Send + Sync + 'static` — satisfies the bounds required
/// by `hyper_util::client::legacy::Client::builder`.
#[derive(Clone)]
pub(crate) struct UnixConnector(Arc<Path>);

impl tower_service::Service<hyper::http::Uri> for UnixConnector {
    type Response = UnixIo;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<UnixIo, std::io::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut task::Context<'_>) -> task::Poll<Result<(), Self::Error>> {
        task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: hyper::http::Uri) -> Self::Future {
        let path = Arc::clone(&self.0);
        Box::pin(async move {
            let stream = UnixStream::connect(&*path).await?;
            Ok(UnixIo(TokioIo::new(stream)))
        })
    }
}

// ── Client factory ───────────────────────────────────────────────────

/// Build a hyper legacy `Client` that routes all requests through the given
/// Unix socket path.  The URI's authority is ignored — all connections go
/// to the socket.
pub(crate) fn unix_client(socket_path: &Path) -> Client<UnixConnector, Full<Bytes>> {
    let connector = UnixConnector(Arc::from(socket_path));
    Client::builder(TokioExecutor::new()).build(connector)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::Request;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn round_trip_through_unix_socket() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        // Minimal async HTTP/1.1 server — reads the request and writes one response.
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).into_owned();

            let body = r#"{"version":"5.0.0"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                body.len(),
                body,
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });

        let client = unix_client(&sock_path);
        let req = Request::builder()
            .method("GET")
            .uri("http://localhost/version")
            .body(Full::new(Bytes::new()))
            .unwrap();

        let resp = client.request(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&body_bytes).unwrap();
        assert!(body.contains("5.0.0"));

        let request = server.await.unwrap();
        assert!(request.contains("GET /version"));
    }
}
