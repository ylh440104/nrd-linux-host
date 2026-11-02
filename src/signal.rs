use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

/// Socket.IO v4 client over a raw websocket.
///
/// The NetEase signalling server speaks plain Engine.IO v4 / Socket.IO v4:
///   "0{...}"   engine open
///   "40{...}"  namespace connected
///   "42[...]"  event
///   "2" / "3"  ping / pong
///
/// Events can also arrive as binary frames whose payload is
/// "45<n>-" + json, sometimes split across several websocket frames.
pub struct Signal {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    /// Buffers a partially received binary event so fragments can be joined.
    pending: Vec<u8>,
    pending_is_binary: bool,
}

impl Signal {
    pub async fn connect(url: &str, auth: &str) -> Result<Self> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let mut req = url.into_client_request()?;
        let headers = req.headers_mut();
        headers.insert("X-NRD-AUTH", auth.parse()?);
        headers.insert("User-Agent", "UURemote/4.45.0".parse()?);

        let (ws, _resp) = tokio_tungstenite::connect_async(req).await?;
        let mut this = Self {
            ws,
            pending: Vec::new(),
            pending_is_binary: false,
        };
        this.handshake().await?;
        Ok(this)
    }

    /// Engine.IO open, namespace join. Retries "40" until the server acks.
    async fn handshake(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

        while tokio::time::Instant::now() < deadline {
            let msg = match tokio::time::timeout(Duration::from_secs(6), self.ws.next()).await {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) => return Err(anyhow!("socket closed during handshake")),
                Err(_) => {
                    // idle: keep the engine alive
                    self.ws.send(Message::Text("2".into())).await?;
                    continue;
                }
            };

            let text = match msg {
                Message::Text(t) => t.to_string(),
                Message::Binary(_) => continue,
                Message::Ping(p) => {
                    self.ws.send(Message::Pong(p)).await?;
                    continue;
                }
                Message::Close(_) => return Err(anyhow!("closed during handshake")),
                _ => continue,
            };

            if text.starts_with('0') {
                // engine open -> join the default namespace
                self.ws.send(Message::Text("40".into())).await?;
                continue;
            }
            if text.starts_with("40") {
                log::info!("namespace {}", text);
                return Ok(());
            }
            if text == "2" {
                self.ws.send(Message::Text("3".into())).await?;
                continue;
            }
        }
        Err(anyhow!("handshake timeout"))
    }

    pub async fn emit(&mut self, event: &str, payload: Value) -> Result<()> {
        let arr = Value::Array(vec![Value::String(event.to_string()), payload]);
        let frame = format!("42{}", serde_json::to_string(&arr)?);
        self.ws.send(Message::Text(frame.into())).await?;
        Ok(())
    }

    /// Send a binary binary-attachment frame ("45<n>-" + json).
    pub async fn emit_binary(&mut self, event: &str, payload: Value) -> Result<()> {
        let arr = Value::Array(vec![Value::String(event.to_string()), payload]);
        let body = serde_json::to_string(&arr)?;
        let mut frame = format!("45{}-", body.len()).into_bytes();
        frame.extend_from_slice(body.as_bytes());
        self.ws.send(Message::Binary(frame.into())).await?;
        Ok(())
    }

    /// Next decoded event, or None when the socket closes.
    ///
    /// Returns (event_name, payload).
    pub async fn next_event(&mut self) -> Result<Option<(String, Value)>> {
        loop {
            let msg = match self.ws.next().await {
                Some(Ok(m)) => m,
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(None),
            };

            match msg {
                Message::Text(t) => {
                    let text = t.to_string();
                    if text == "2" {
                        self.ws.send(Message::Text("3".into())).await?;
                        continue;
                    }
                    if let Some(ev) = parse_event(&text) {
                        return Ok(Some(ev));
                    }
                    continue;
                }
                Message::Binary(b) => {
                    let bytes = b.to_vec();
                    // Engine.IO binary attachment header: "45<n>-"
                    let joined = self.join_binary(&bytes);
                    let Some(full) = joined else { continue };
                    let text = String::from_utf8_lossy(&full).to_string();
                    if let Some(ev) = parse_event(&text) {
                        return Ok(Some(ev));
                    }
                    continue;
                }
                Message::Ping(p) => {
                    self.ws.send(Message::Pong(p)).await?;
                    continue;
                }
                Message::Close(_) => return Ok(None),
                _ => continue,
            }
        }
    }

    /// NetEase sends some events as several binary fragments. A fragment that
    /// starts with "45<n>-" announces n bytes and may or may not contain them
    /// entirely; subsequent fragments continue the same payload.
    fn join_binary(&mut self, chunk: &[u8]) -> Option<Vec<u8>> {
        if chunk.starts_with(b"45") {
            // header form: 45<digits>-
            if let Some(dash) = chunk.iter().position(|&c| c == b'-') {
                if dash > 2 && dash < 12 {
                    let rest = chunk[dash + 1..].to_vec();
                    self.pending = rest;
                    self.pending_is_binary = true;

                    let announced: usize = std::str::from_utf8(&chunk[2..dash])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);

                    if announced > 0 && self.pending.len() >= announced {
                        let out = self.pending[..announced].to_vec();
                        self.pending.clear();
                        self.pending_is_binary = false;
                        return Some(out);
                    }
                    return None;
                }
            }
        }

        if self.pending_is_binary {
            self.pending.extend_from_slice(chunk);
            // A complete json document ends with "]" or "}"
            let last = self.pending.last().copied();
            if matches!(last, Some(b']') | Some(b'}')) {
                let out = std::mem::take(&mut self.pending);
                self.pending_is_binary = false;
                return Some(out);
            }
            return None;
        }
        None
    }
}

/// Parse an event frame into (name, payload).
fn parse_event(text: &str) -> Option<(String, Value)> {
    // "42[...]" or a bare "[...]" carrying an event array.
    let body = text
        .strip_prefix("42")
        .or_else(|| text.strip_prefix("43"))
        .unwrap_or(text);

    let start = body.find('[')?;
    let parsed: Value = serde_json::from_str(&body[start..]).ok()?;
    let arr = parsed.as_array()?;
    let name = arr.first()?.as_str()?.to_string();
    let payload = arr.get(1).cloned().unwrap_or(Value::Null);
    Some((name, payload))
}