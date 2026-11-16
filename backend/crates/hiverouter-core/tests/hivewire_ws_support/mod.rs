//! Test-only support for the HiveWire loopback WebSocket transport.
//!
//! A minimal RFC 6455 client. It uses the RFC's own example key so the
//! `Sec-WebSocket-Accept` value can be verified against the known answer
//! (`s3pPLMBiTxaQ9kYGzzhZRbK+xOo=`) without shipping a second crypto
//! implementation in tests. Client frames are masked; server frames are read as
//! unmasked. Only text/binary/close are needed here.

#![allow(dead_code)]

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// RFC 6455 example key.
const EXAMPLE_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
/// RFC 6455 example accept value for [`EXAMPLE_KEY`].
const EXAMPLE_ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

/// HTTP status plus the raw header block and any bytes after it.
pub struct UpgradeResponse {
    /// Status code from the response line.
    pub status: u16,
    /// Header block (without the status line), original casing.
    pub headers: String,
    /// Response body / post-header bytes.
    pub body: Vec<u8>,
}

impl UpgradeResponse {
    /// Whether the header block contains `name: value` (case-insensitive name).
    #[must_use]
    pub fn has_header(&self, name: &str, value: &str) -> bool {
        self.headers.lines().any(|line| {
            line.split_once(':').is_some_and(|(header, rest)| {
                header.trim().eq_ignore_ascii_case(name) && rest.trim() == value
            })
        })
    }

    /// Whether the response completed a WebSocket upgrade.
    #[must_use]
    pub fn upgraded(&self) -> bool {
        self.status == 101 && self.has_header("sec-websocket-accept", EXAMPLE_ACCEPT)
    }

    /// Body as lossy UTF-8.
    #[must_use]
    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Sends one HTTP GET upgrade request and reads the response head.
///
/// When the upgrade is refused (`status != 101`) the body is read until EOF or
/// a 2 s deadline so a named refusal can be asserted.
pub async fn upgrade_status(
    addr: SocketAddr,
    path: &str,
    origin: Option<&str>,
    token: Option<&str>,
) -> UpgradeResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let origin_header = origin.map_or_else(String::new, |value| format!("Origin: {value}\r\n"));
    let auth_header = token.map_or_else(String::new, |value| {
        format!("Authorization: Bearer {value}\r\n")
    });
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {EXAMPLE_KEY}\r\nSec-WebSocket-Version: 13\r\n{origin_header}{auth_header}\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write upgrade request");
    let (status, headers, mut body) = read_http_head(&mut stream).await;
    if status != 101 {
        let mut rest = Vec::new();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stream.read_to_end(&mut rest),
        )
        .await;
        body.extend_from_slice(&rest);
    }
    UpgradeResponse {
        status,
        headers,
        body,
    }
}

async fn read_http_head(stream: &mut TcpStream) -> (u16, String, Vec<u8>) {
    let mut buffer = Vec::new();
    loop {
        let mut chunk = [0u8; 2048];
        let read = stream.read(&mut chunk).await.expect("read response head");
        if read == 0 {
            panic!("eof during the HTTP response head");
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buffer[..position]).into_owned();
            let leftover = buffer[position + 4..].to_vec();
            let status = head
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|code| code.parse::<u16>().ok())
                .unwrap_or(0);
            let headers = head.lines().skip(1).collect::<Vec<_>>().join("\n");
            return (status, headers, leftover);
        }
    }
}

/// A connected RFC 6455 client.
pub struct WsClient {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl WsClient {
    /// Completes the upgrade and verifies the accept value.
    pub async fn connect(
        addr: SocketAddr,
        path: &str,
        origin: Option<&str>,
        token: Option<&str>,
    ) -> Result<Self, String> {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|error| error.to_string())?;
        let origin_header = origin.map_or_else(String::new, |value| format!("Origin: {value}\r\n"));
        let auth_header = token.map_or_else(String::new, |value| {
            format!("Authorization: Bearer {value}\r\n")
        });
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {EXAMPLE_KEY}\r\nSec-WebSocket-Version: 13\r\n{origin_header}{auth_header}\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        let (status, headers, leftover) = read_http_head(&mut stream).await;
        if status != 101 {
            return Err(format!("upgrade refused with HTTP {status}"));
        }
        let accept_ok = headers.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.trim().eq_ignore_ascii_case("sec-websocket-accept")
                    && value.trim() == EXAMPLE_ACCEPT
            })
        });
        if !accept_ok {
            return Err(format!(
                "server did not return the RFC 6455 accept value; headers: {headers}"
            ));
        }
        Ok(Self {
            stream,
            buffer: leftover,
        })
    }

    /// Sends a text frame.
    pub async fn send_text(&mut self, text: &str) -> Result<(), String> {
        self.write_frame(0x1, text.as_bytes()).await
    }

    /// Sends a binary frame.
    pub async fn send_binary(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.write_frame(0x2, bytes).await
    }

    /// Sends a close frame.
    pub async fn close(mut self) {
        let _ = self.write_frame(0x8, &[]).await;
    }

    /// Receives the next text frame as JSON, ignoring ping/pong.
    pub async fn recv_json(&mut self) -> Result<serde_json::Value, String> {
        loop {
            let (opcode, payload) = self.read_frame().await?;
            match opcode {
                0x1 => {
                    return serde_json::from_slice(&payload)
                        .map_err(|error| format!("invalid JSON frame: {error}"))
                }
                0x2 => return Err("unexpected binary frame from the server".to_owned()),
                0x8 => return Err("connection closed by the server".to_owned()),
                0x9 => self.write_frame(0xA, &payload).await?,
                0xA => {}
                other => return Err(format!("unexpected websocket opcode {other}")),
            }
        }
    }

    /// Whether the server has closed the connection (next frame is a close).
    pub async fn recv_is_closed(&mut self) -> bool {
        loop {
            match self.read_frame().await {
                Ok((0x8, _)) => return true,
                Ok((0x9, payload)) => {
                    if self.write_frame(0xA, &payload).await.is_err() {
                        return true;
                    }
                }
                Ok((0xA, _)) => {}
                Ok(_) => return false,
                Err(_) => return true,
            }
        }
    }

    async fn write_frame(&mut self, opcode: u8, payload: &[u8]) -> Result<(), String> {
        let mask = [0x5Au8, 0xA5, 0x37, 0xC9];
        let mut frame = Vec::with_capacity(payload.len() + 14);
        frame.push(0x80 | opcode);
        let len = payload.len();
        if len < 126 {
            frame.push(0x80 | u8::try_from(len).map_err(|_| "frame too long".to_owned())?);
        } else if len <= 0xFFFF {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&u16::try_from(len).unwrap_or(0).to_be_bytes());
        } else {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&u64::try_from(len).unwrap_or(u64::MAX).to_be_bytes());
        }
        frame.extend_from_slice(&mask);
        for (index, byte) in payload.iter().enumerate() {
            frame.push(byte ^ mask[index % 4]);
        }
        self.stream
            .write_all(&frame)
            .await
            .map_err(|error| error.to_string())
    }

    async fn read_frame(&mut self) -> Result<(u8, Vec<u8>), String> {
        let first = self.read_byte().await?;
        let second = self.read_byte().await?;
        let opcode = first & 0x0F;
        let masked = second & 0x80 != 0;
        let mut len = usize::from(second & 0x7F);
        if len == 126 {
            let bytes = self.read_exact(2).await?;
            len = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
        } else if len == 127 {
            let bytes = self.read_exact(8).await?;
            let array: [u8; 8] = bytes
                .try_into()
                .map_err(|_| "bad extended length".to_owned())?;
            len = usize::try_from(u64::from_be_bytes(array))
                .map_err(|_| "frame too large".to_owned())?;
        }
        let mask = if masked {
            Some(self.read_exact(4).await?)
        } else {
            None
        };
        let mut payload = if len == 0 {
            Vec::new()
        } else {
            self.read_exact(len).await?
        };
        if let Some(mask) = mask {
            for (index, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[index % 4];
            }
        }
        Ok((opcode, payload))
    }

    async fn read_byte(&mut self) -> Result<u8, String> {
        self.fill(1).await?;
        Ok(self.buffer.remove(0))
    }

    async fn read_exact(&mut self, count: usize) -> Result<Vec<u8>, String> {
        self.fill(count).await?;
        let rest = self.buffer.split_off(count);
        Ok(std::mem::replace(&mut self.buffer, rest))
    }

    async fn fill(&mut self, count: usize) -> Result<(), String> {
        while self.buffer.len() < count {
            let mut chunk = [0u8; 8192];
            let read = self
                .stream
                .read(&mut chunk)
                .await
                .map_err(|error| error.to_string())?;
            if read == 0 {
                return Err("eof".to_owned());
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
        Ok(())
    }
}

/// Receives the next JSON frame with a 5 s deadline.
pub async fn recv_json(client: &mut WsClient) -> serde_json::Value {
    tokio::time::timeout(std::time::Duration::from_secs(5), client.recv_json())
        .await
        .expect("timed out waiting for a WebSocket frame")
        .expect("WebSocket frame error")
}

/// Body type of a server envelope (`hello_ack` / `ack` / `error`).
pub fn body_type(value: &serde_json::Value) -> &str {
    value
        .get("body")
        .and_then(|body| body.get("type"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<no body type>")
}

/// Refusal code of a server `error` envelope.
pub fn error_code(value: &serde_json::Value) -> &str {
    value
        .get("body")
        .and_then(|body| body.get("payload"))
        .and_then(|payload| payload.get("code"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<no code>")
}
