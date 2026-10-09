//! Relay: pairs guest connections with host channels.
//!
//! Routes (all WebSocket):
//! - `/host/{room}?key={key}`  host control connection; registers the room.
//! - `/join/{room}/{token}`    guest connection; relay asks the host for a channel.
//! - `/chan/{room}/{chan}?key={key}`  host channel answering one guest.
//!
//! The relay splices guest and channel byte streams. It keeps nothing on disk.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// Sent on the host control connection after the room is registered.
pub const PROTOCOL_TAG: &str = "teamsent-relay-v0";

const DEFAULT_LISTEN: &str = "127.0.0.1:7878";
const CHANNEL_OPEN_TIMEOUT: Duration = Duration::from_secs(10);

type Ws = WebSocketStream<TcpStream>;

struct Room {
    key: String,
    control: mpsc::UnboundedSender<String>,
    pending: HashMap<String, oneshot::Sender<Ws>>,
}

type Rooms = Arc<Mutex<HashMap<String, Room>>>;

/// Runs the relay: `[--listen ADDR:PORT]`, default `127.0.0.1:7878`.
pub fn run(args: &[String]) -> io::Result<()> {
    let listen = match args {
        [] => DEFAULT_LISTEN.to_string(),
        [flag, addr] if flag == "--listen" => addr.clone(),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: teamsent relay [--listen ADDR:PORT]",
            ))
        }
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(serve(&listen))
}

async fn serve(listen: &str) -> io::Result<()> {
    let listener = TcpListener::bind(listen).await?;
    eprintln!(
        "teamsent relay listening on ws://{}",
        listener.local_addr()?
    );
    let rooms: Rooms = Arc::default();
    loop {
        let (tcp, peer) = listener.accept().await?;
        let rooms = Arc::clone(&rooms);
        tokio::spawn(async move {
            if let Err(err) = handle(tcp, rooms).await {
                tracing::debug!(%peer, %err, "relay connection ended");
            }
        });
    }
}

// The handshake callback must return tungstenite's large `ErrorResponse`.
#[allow(clippy::result_large_err)]
async fn handle(tcp: TcpStream, rooms: Rooms) -> Result<(), String> {
    let mut uri = String::new();
    let ws = tokio_tungstenite::accept_hdr_async(tcp, |req: &Request, resp: Response| {
        uri = req.uri().to_string();
        Ok(resp)
    })
    .await
    .map_err(|e| e.to_string())?;

    let (path, query) = uri.split_once('?').unwrap_or((&uri, ""));
    let key = query
        .split('&')
        .find_map(|kv| kv.strip_prefix("key="))
        .unwrap_or("")
        .to_string();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match parts.as_slice() {
        ["host", room] => host_control(ws, room, key, rooms).await,
        // The invite token is a path segment so links paste into any shell unquoted.
        ["join", room, token] if token.chars().all(|c| c.is_ascii_alphanumeric()) => {
            let query = if query.is_empty() {
                format!("t={token}")
            } else {
                format!("t={token}&{query}")
            };
            guest(ws, room, &query, rooms).await
        }
        ["chan", room, chan] => host_channel(ws, room, chan, &key, rooms),
        _ => Err(format!("unknown route {path}")),
    }
}

async fn host_control(mut ws: Ws, room: &str, key: String, rooms: Rooms) -> Result<(), String> {
    if key.is_empty() {
        return Err("host key missing".into());
    }
    let (tx, mut rx) = mpsc::unbounded_channel();
    {
        let mut rooms = rooms.lock().unwrap();
        if rooms.contains_key(room) {
            return Err(format!("room {room} already hosted"));
        }
        rooms.insert(
            room.to_string(),
            Room {
                key,
                control: tx,
                pending: HashMap::new(),
            },
        );
    }
    eprintln!("room {room}: host connected");
    let _ = ws.send(Message::text(PROTOCOL_TAG)).await;
    let result = loop {
        tokio::select! {
            Some(cmd) = rx.recv() => {
                if let Err(err) = ws.send(Message::text(cmd)).await {
                    break Err(err.to_string());
                }
            }
            msg = ws.next() => match msg {
                Some(Ok(Message::Close(_))) | None => break Ok(()),
                Some(Err(err)) => break Err(err.to_string()),
                Some(Ok(_)) => {}
            }
        }
    };
    rooms.lock().unwrap().remove(room);
    eprintln!("room {room}: host left, room closed");
    result
}

/// The guest's query string (invite token, name, probe flag) is passed to the
/// host untouched; the host alone decides whether and how the guest may join.
async fn guest(ws: Ws, room: &str, query: &str, rooms: Rooms) -> Result<(), String> {
    if query.contains(char::is_whitespace) {
        return Err("invalid query".into());
    }
    let chan = random_id();
    let (tx, rx) = oneshot::channel();
    {
        let mut rooms = rooms.lock().unwrap();
        let Some(entry) = rooms.get_mut(room) else {
            return Err(format!("room {room} not found"));
        };
        entry.pending.insert(chan.clone(), tx);
        entry
            .control
            .send(format!("open {chan} {query}"))
            .map_err(|_| "host control closed".to_string())?;
    }
    let host_ws = match tokio::time::timeout(CHANNEL_OPEN_TIMEOUT, rx).await {
        Ok(Ok(ws)) => ws,
        _ => {
            if let Some(entry) = rooms.lock().unwrap().get_mut(room) {
                entry.pending.remove(&chan);
            }
            return Err("host did not open a channel in time".into());
        }
    };
    eprintln!("room {room}: guest {chan} connected");
    splice(ws, host_ws).await;
    eprintln!("room {room}: guest {chan} disconnected");
    Ok(())
}

fn host_channel(ws: Ws, room: &str, chan: &str, key: &str, rooms: Rooms) -> Result<(), String> {
    let mut rooms = rooms.lock().unwrap();
    let Some(entry) = rooms.get_mut(room) else {
        return Err(format!("room {room} not found"));
    };
    if entry.key != key {
        return Err("bad host key".into());
    }
    let Some(tx) = entry.pending.remove(chan) else {
        return Err(format!("no pending guest {chan}"));
    };
    tx.send(ws).map_err(|_| "guest gone".to_string())
}

/// Copies binary messages both ways until either side closes.
async fn splice(a: Ws, b: Ws) {
    let (mut a_tx, mut a_rx) = a.split();
    let (mut b_tx, mut b_rx) = b.split();
    let a_to_b = async {
        while let Some(Ok(msg)) = a_rx.next().await {
            if msg.is_close() || b_tx.send(msg).await.is_err() {
                break;
            }
        }
        let _ = b_tx.close().await;
    };
    let b_to_a = async {
        while let Some(Ok(msg)) = b_rx.next().await {
            if msg.is_close() || a_tx.send(msg).await.is_err() {
                break;
            }
        }
        let _ = a_tx.close().await;
    };
    tokio::select! {
        _ = a_to_b => {}
        _ = b_to_a => {}
    }
}

/// 128-bit random channel id, hex encoded.
fn random_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("OS random source is available");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
