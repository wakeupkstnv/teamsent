//! `teamsent host`: share the local herdr session through a relay.
//!
//! For every guest the relay announces, the host opens a channel to the relay
//! and a fresh connection to the herdr client socket, then pumps frames between
//! them. Guest frames pass the viewer filter; server frames pass unchanged.

use std::io::{self, Read, Write};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use interprocess::TryClone as _;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::filter::{self, Verdict};
use super::{random_id, shutdown_local_stream, PROTOCOL_TAG};
use crate::protocol::{MAX_FRAME_SIZE, MAX_GRAPHICS_FRAME_SIZE};

const DEFAULT_RELAY: &str = "ws://127.0.0.1:7878";
const READ_CHUNK: usize = 64 * 1024;

pub(crate) fn run(args: &[String]) -> io::Result<()> {
    let relay = match args {
        [] => DEFAULT_RELAY.to_string(),
        [flag, url] if flag == "--relay" => url.trim_end_matches('/').to_string(),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: teamsent host [--relay ws://HOST:PORT]",
            ))
        }
    };
    ensure_server_running()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(control_loop(relay))
}

fn ensure_server_running() -> io::Result<()> {
    if crate::server::autodetect::is_server_listening() {
        return Ok(());
    }
    crate::server::autodetect::spawn_server_daemon()?;
    crate::server::autodetect::wait_for_server_socket(
        &crate::server::socket_paths::client_socket_path(),
        Duration::from_secs(5),
    )
}

async fn control_loop(relay: String) -> io::Result<()> {
    let room = random_id();
    let key = random_id();
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("{relay}/host/{room}?key={key}"))
        .await
        .map_err(io::Error::other)?;

    match ws.next().await {
        Some(Ok(Message::Text(tag))) if tag.as_str() == PROTOCOL_TAG => {}
        other => {
            return Err(io::Error::other(format!(
                "unexpected relay greeting: {other:?}"
            )))
        }
    }

    eprintln!("Sharing this herdr session (guests are view-only).");
    eprintln!();
    eprintln!("  teamsent join {relay}/join/{room}");
    eprintln!();
    eprintln!("Press Ctrl+C to stop sharing.");

    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(cmd))) => {
                    if let Some(chan) = cmd.as_str().strip_prefix("open ") {
                        let url = format!("{relay}/chan/{room}/{chan}?key={key}");
                        let chan = chan.to_string();
                        tokio::spawn(async move {
                            eprintln!("guest {chan}: joined");
                            if let Err(err) = serve_guest(url, &chan).await {
                                eprintln!("guest {chan}: {err}");
                            }
                            eprintln!("guest {chan}: left");
                        });
                    }
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err(io::Error::other("relay closed the room"));
                }
                Some(Err(err)) => return Err(io::Error::other(err)),
                Some(Ok(_)) => {}
            },
            _ = tokio::signal::ctrl_c() => {
                let _ = ws.close(None).await;
                eprintln!("Stopped sharing.");
                return Ok(());
            }
        }
    }
}

async fn serve_guest(url: String, chan: &str) -> io::Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(io::Error::other)?;
    let (mut ws_tx, mut ws_rx) = ws.split();

    let socket =
        crate::ipc::connect_local_stream(&crate::server::socket_paths::client_socket_path())?;
    let mut socket_reader = socket.try_clone()?;
    let mut socket_writer = socket.try_clone()?;

    // herdr server -> guest, re-framed so injected replies land on frame boundaries.
    let (to_guest_tx, mut to_guest_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let server_tx = to_guest_tx.clone();
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let mut chunk = vec![0u8; READ_CHUNK];
        loop {
            let n = match socket_reader.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            buf.extend_from_slice(&chunk[..n]);
            let Ok(frames) = filter::take_frames(&mut buf, MAX_GRAPHICS_FRAME_SIZE) else {
                break;
            };
            for frame in frames {
                if server_tx.send(frame).is_err() {
                    return;
                }
            }
        }
    });

    // guest -> herdr server, after the filter.
    let (to_server_tx, to_server_rx) = std::sync::mpsc::channel::<Vec<u8>>();
    std::thread::spawn(move || {
        for frame in to_server_rx {
            if socket_writer.write_all(&frame).is_err() {
                break;
            }
        }
    });

    let send_to_guest = async {
        while let Some(frame) = to_guest_rx.recv().await {
            if ws_tx.send(Message::binary(frame)).await.is_err() {
                break;
            }
        }
        let _ = ws_tx.close().await;
    };

    let recv_from_guest = async {
        let mut buf = Vec::new();
        while let Some(Ok(msg)) = ws_rx.next().await {
            let Message::Binary(bytes) = msg else {
                if msg.is_close() {
                    break;
                }
                continue;
            };
            buf.extend_from_slice(&bytes);
            let frames = match filter::take_frames(&mut buf, MAX_FRAME_SIZE) {
                Ok(frames) => frames,
                Err(err) => return Err(io::Error::other(err)),
            };
            for frame in frames {
                match filter::classify_viewer_frame(&frame) {
                    Verdict::Forward => {
                        if to_server_tx.send(frame).is_err() {
                            return Ok(());
                        }
                    }
                    Verdict::Drop(what) => {
                        tracing::debug!(chan, what, "dropped guest frame");
                    }
                    Verdict::Reply(reply, method) => {
                        eprintln!("guest {chan}: denied {method}");
                        let _ = to_guest_tx.send(filter::encode_frame(&reply));
                    }
                    Verdict::Close(reason) => return Err(io::Error::other(reason)),
                }
            }
        }
        Ok(())
    };

    let result = tokio::select! {
        _ = send_to_guest => Ok(()),
        result = recv_from_guest => result,
    };
    shutdown_local_stream(&socket);
    result
}
