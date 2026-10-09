//! `teamsent join <link>`: attach a herdr client to a shared session.
//!
//! Mirrors `herdr --remote`: a private local socket stands in for the server's
//! client socket, and every connection to it is bridged to the relay. The
//! normal herdr client UI runs against that socket.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use interprocess::local_socket::traits::Listener as _;
use interprocess::local_socket::ListenerNonblockingMode;
use interprocess::TryClone as _;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::{default_name, is_join_link, is_valid_name, shutdown_local_stream};

const ACCEPT_POLL: Duration = Duration::from_millis(20);
const READ_CHUNK: usize = 64 * 1024;
const SOCKET_PERMISSION_MODE: u32 = 0o600;

const USAGE: &str = "usage: teamsent join ws://HOST:PORT/join/ROOM/TOKEN [--name NAME]";

pub(crate) fn run(args: &[String]) -> io::Result<()> {
    let usage = |msg: &str| io::Error::new(io::ErrorKind::InvalidInput, format!("{msg}{USAGE}"));
    let (link, name) = match args {
        [link] => (link, default_name()),
        [link, flag, name] if flag == "--name" => {
            if !is_valid_name(name) {
                return Err(usage(
                    "name must be 1-32 chars of a-z, 0-9, '-', '_', '.'\n",
                ));
            }
            (link, name.clone())
        }
        _ => return Err(usage("")),
    };
    if !is_join_link(link) {
        return Err(usage("not a teamsent invite link\n"));
    }
    let link = format!("{link}?name={name}");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let role = runtime.block_on(probe(&link))?;
    eprintln!("joining as {name} ({role})…");
    let socket_path = local_socket_path();
    crate::ipc::prepare_socket_path(&socket_path, |path| {
        format!("join bridge is already listening at {}", path.display())
    })?;
    let listener = crate::ipc::bind_private_local_listener(&socket_path)?;
    let identity = crate::ipc::socket_file_identity(&socket_path)?;
    crate::ipc::restrict_socket_permissions(&socket_path, SOCKET_PERMISSION_MODE)?;
    listener.set_nonblocking(ListenerNonblockingMode::Accept)?;

    let stop = Arc::new(AtomicBool::new(false));
    let accept_stop = Arc::clone(&stop);
    let handle = runtime.handle().clone();
    let accept_link = link.clone();
    let accept_thread = std::thread::spawn(move || {
        while !accept_stop.load(Ordering::Acquire) {
            match listener.accept() {
                Ok(mut stream) => {
                    if let Err(err) = crate::ipc::set_local_stream_polling(&mut stream, false) {
                        tracing::error!(%err, "join bridge failed to prepare client socket");
                        continue;
                    }
                    let link = accept_link.clone();
                    handle.spawn(async move {
                        if let Err(err) = bridge(stream, &link).await {
                            tracing::warn!(%err, "join bridge connection ended");
                        }
                    });
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(ACCEPT_POLL);
                }
                Err(err) => {
                    tracing::error!(%err, "join bridge listener failed");
                    break;
                }
            }
        }
    });

    let status = run_client(&socket_path);

    stop.store(true, Ordering::Release);
    let _ = accept_thread.join();
    let _ = crate::ipc::remove_socket_file_if_owned(&socket_path, &identity);
    runtime.shutdown_timeout(Duration::from_millis(200));
    status
}

/// Asks the host whether this invite is valid before starting the client UI.
/// Returns the granted role.
async fn probe(link: &str) -> io::Result<String> {
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("{link}&probe=1"))
        .await
        .map_err(|err| io::Error::other(format!("cannot reach relay: {err}")))?;
    let answer = match ws.next().await {
        Some(Ok(Message::Text(text))) => text.to_string(),
        _ => return Err(io::Error::other("session not found (host offline?)")),
    };
    let _ = ws.close(None).await;
    if let Some(rest) = answer.strip_prefix("teamsent-ok ") {
        return Ok(rest.split(' ').next().unwrap_or("viewer").to_string());
    }
    let reason = answer.strip_prefix("teamsent-denied ").unwrap_or(&answer);
    Err(io::Error::other(format!("join refused: {reason}")))
}

fn local_socket_path() -> PathBuf {
    std::env::temp_dir().join(format!("teamsent-join-{}.sock", std::process::id()))
}

fn run_client(socket_path: &Path) -> io::Result<()> {
    let status = Command::new(std::env::current_exe()?)
        .arg("client")
        .env(
            crate::server::socket_paths::CLIENT_SOCKET_PATH_ENV_VAR,
            socket_path,
        )
        .env_remove(crate::api::SOCKET_PATH_ENV_VAR)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!("client exited with {status}")))
    }
}

/// Pipes one local client connection to a fresh relay connection, byte for byte.
async fn bridge(socket: crate::ipc::LocalStream, link: &str) -> io::Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(link)
        .await
        .map_err(io::Error::other)?;
    let (mut ws_tx, mut ws_rx) = ws.split();
    let mut socket_reader = socket.try_clone()?;
    let mut socket_writer = socket.try_clone()?;

    let (up_tx, mut up_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    std::thread::spawn(move || {
        let mut chunk = vec![0u8; READ_CHUNK];
        loop {
            match socket_reader.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if up_tx.send(chunk[..n].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let (down_tx, down_rx) = std::sync::mpsc::channel::<Vec<u8>>();
    std::thread::spawn(move || {
        for bytes in down_rx {
            if socket_writer.write_all(&bytes).is_err() {
                break;
            }
        }
    });

    let upload = async {
        while let Some(bytes) = up_rx.recv().await {
            if ws_tx.send(Message::binary(bytes)).await.is_err() {
                break;
            }
        }
        let _ = ws_tx.close().await;
    };
    let download = async {
        while let Some(Ok(msg)) = ws_rx.next().await {
            match msg {
                Message::Binary(bytes) => {
                    if down_tx.send(bytes.to_vec()).is_err() {
                        break;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    };
    tokio::select! {
        _ = upload => {}
        _ = download => {}
    }
    shutdown_local_stream(&socket);
    Ok(())
}
