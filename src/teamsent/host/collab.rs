//! Collab channel: the guest's composer bar ⇄ host.
//!
//! Guest → host (JSON text): `{"t":"draft","text"}`, `{"t":"submit","text"}`,
//! `{"t":"vote","id"}`. Host → guest: `{"t":"state", …}` whenever the session
//! changes (checked every 200 ms) and `{"t":"result","ok","msg"}` per command.

use std::io;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;

use super::{snapshot, submit, Ctx};
use crate::teamsent::queue::MAX_SUGGESTION_BYTES;

const STATE_POLL: Duration = Duration::from_millis(200);

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub(super) async fn serve(ws: Ws, id: u32, ctx: &Ctx, kick: &Notify) -> io::Result<()> {
    let (mut tx, mut rx) = ws.split();
    let mut tick = tokio::time::interval(STATE_POLL);
    let mut last = String::new();
    loop {
        tokio::select! {
            msg = rx.next() => {
                let Some(Ok(msg)) = msg else { return Ok(()) };
                let Message::Text(text) = msg else {
                    if msg.is_close() { return Ok(()) }
                    continue;
                };
                if let Some(reply) = handle(ctx, id, text.as_str()) {
                    tx.send(Message::text(reply.to_string())).await.map_err(io::Error::other)?;
                }
            }
            _ = tick.tick() => {
                let state = snapshot(ctx, Some(id)).to_string();
                if state != last {
                    tx.send(Message::text(state.clone())).await.map_err(io::Error::other)?;
                    last = state;
                }
            }
            _ = kick.notified() => {
                let _ = tx.close().await;
                return Ok(());
            }
        }
    }
}

/// Applies one guest command; returns the reply to send, if any.
fn handle(ctx: &Ctx, id: u32, text: &str) -> Option<Value> {
    let msg: Value = serde_json::from_str(text).ok()?;
    match msg["t"].as_str()? {
        "draft" => {
            let draft = msg["text"].as_str().unwrap_or("");
            let draft: String = draft.chars().take(MAX_SUGGESTION_BYTES).collect();
            let mut st = ctx.state.lock().unwrap();
            if st
                .guests
                .get(&id)
                .is_some_and(|g| g.role != super::Role::Viewer)
            {
                if draft.is_empty() {
                    st.drafts.remove(&id);
                } else {
                    st.drafts.insert(id, draft);
                }
            }
            None
        }
        "submit" => Some(match submit(ctx, id, msg["text"].as_str().unwrap_or("")) {
            Ok((_, msg)) => json!({ "t": "result", "ok": true, "msg": msg }),
            Err(err) => json!({ "t": "result", "ok": false, "msg": err }),
        }),
        "vote" => {
            let sid = msg["id"].as_u64()? as u32;
            let result = ctx.queue.lock().unwrap().vote(sid, id);
            Some(match result {
                Ok(n) => json!({ "t": "result", "ok": true, "msg": format!("#{sid}: {n} votes") }),
                Err(err) => json!({ "t": "result", "ok": false, "msg": err }),
            })
        }
        _ => None,
    }
}
