//! `teamsent host`: share the local herdr session through a relay.
//!
//! Access is granted by invite links. Each link carries a token bound to a
//! role and an expiry. For every guest the relay announces, the host checks
//! the token, opens a channel to the relay and a fresh connection to the herdr
//! client socket, then pumps frames between them. Guest frames pass the role
//! filter, re-checked on every frame so `/grant` and `/revoke` apply at once.
//!
//! The host terminal accepts commands: `/list`, `/invite`, `/grant`,
//! `/revoke`, `/help`.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use interprocess::TryClone as _;
use tokio::io::AsyncBufReadExt;
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite::Message;

use super::filter::{self, Verdict};
use super::{
    is_valid_name, parse_ttl, query_param, random_id, shutdown_local_stream, Role, MAX_INVITE_TTL,
    PROTOCOL_TAG,
};
use crate::protocol::{MAX_FRAME_SIZE, MAX_GRAPHICS_FRAME_SIZE};

/// Team relay on GCP (deploy/README.md). Override with `--relay` or `TEAMSENT_RELAY`.
const DEFAULT_RELAY: &str = "wss://34-38-17-92.sslip.io";
const READ_CHUNK: usize = 64 * 1024;
const USAGE: &str =
    "usage: teamsent host [--relay wss://HOST] [--role viewer|writer] [--ttl 30m|2h|24h]";
const HELP: &str = "\
commands:
  /list                       invites and guests
  /invite <viewer|writer> [ttl]  new invite link (ttl: 90s, 30m, 2h; max 24h)
  /grant <guest#> <viewer|writer>  change a guest's role now
  /revoke <guest#>            disconnect the guest and burn their invite link
  /help                       this text";

struct Invite {
    role: Role,
    expires: Instant,
    revoked: bool,
}

struct Guest {
    name: String,
    token: String,
    role: Role,
    since: Instant,
    /// Live connections of this guest, keyed by relay channel id.
    conns: HashMap<String, Arc<Notify>>,
}

#[derive(Default)]
struct State {
    invites: HashMap<String, Invite>,
    guests: BTreeMap<u32, Guest>,
    next_guest: u32,
}

type Shared = Arc<Mutex<State>>;

struct Options {
    relay: String,
    role: Role,
    ttl: Duration,
}

pub(crate) fn run(args: &[String]) -> io::Result<()> {
    let options = parse_args(args)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, format!("{err}\n{USAGE}")))?;
    ensure_server_running()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(control_loop(options))
}

fn parse_args(args: &[String]) -> Result<Options, String> {
    let mut options = Options {
        relay: std::env::var("TEAMSENT_RELAY")
            .unwrap_or_else(|_| DEFAULT_RELAY.to_string())
            .trim_end_matches('/')
            .to_string(),
        role: Role::Viewer,
        ttl: MAX_INVITE_TTL,
    };
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        let value = iter.next().ok_or(format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--relay" => options.relay = value.trim_end_matches('/').to_string(),
            "--role" => options.role = Role::parse(value).ok_or(format!("unknown role {value}"))?,
            "--ttl" => options.ttl = parse_ttl(value).ok_or(format!("bad ttl {value}"))?,
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    Ok(options)
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

async fn control_loop(options: Options) -> io::Result<()> {
    let Options { relay, role, ttl } = options;
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

    let state: Shared = Arc::default();
    let token = create_invite(&state, role, ttl);
    eprintln!("Sharing this herdr session.");
    eprintln!();
    eprintln!("  {} link, valid {}:", role.as_str(), fmt_duration(ttl));
    eprintln!("  teamsent join {}", join_link(&relay, &room, &token));
    eprintln!();
    eprintln!("Type /help for commands, Ctrl+C to stop sharing.");

    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut stdin_open = true;
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(cmd))) => {
                    let mut parts = cmd.as_str().splitn(3, ' ');
                    if let (Some("open"), Some(chan)) = (parts.next(), parts.next()) {
                        let query = parts.next().unwrap_or("").to_string();
                        let url = format!("{relay}/chan/{room}/{chan}?key={key}");
                        let chan = chan.to_string();
                        let state = Arc::clone(&state);
                        tokio::spawn(async move {
                            if let Err(err) = handle_open(url, chan, &query, state).await {
                                tracing::debug!(%err, "guest channel ended");
                            }
                        });
                    }
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err(io::Error::other("relay closed the room"));
                }
                Some(Err(err)) => return Err(io::Error::other(err)),
                Some(Ok(_)) => {}
            },
            line = stdin.next_line(), if stdin_open => match line {
                Ok(Some(line)) => {
                    for out in run_command(line.trim(), &state, &relay, &room) {
                        eprintln!("{out}");
                    }
                }
                // stdin closed (e.g. running in the background): keep sharing.
                Ok(None) | Err(_) => stdin_open = false,
            },
            _ = tokio::signal::ctrl_c() => {
                let _ = ws.close(None).await;
                eprintln!("Stopped sharing.");
                return Ok(());
            }
        }
    }
}

fn join_link(relay: &str, room: &str, token: &str) -> String {
    format!("{relay}/join/{room}/{token}")
}

fn create_invite(state: &Shared, role: Role, ttl: Duration) -> String {
    let token = random_id();
    state.lock().unwrap().invites.insert(
        token.clone(),
        Invite {
            role,
            expires: Instant::now() + ttl,
            revoked: false,
        },
    );
    token
}

fn run_command(line: &str, state: &Shared, relay: &str, room: &str) -> Vec<String> {
    let args: Vec<&str> = line.split_whitespace().collect();
    match args.as_slice() {
        [] => Vec::new(),
        ["/help"] => vec![HELP.to_string()],
        ["/list"] => list(state),
        ["/invite", role, rest @ ..] if rest.len() <= 1 => {
            let Some(role) = Role::parse(role) else {
                return vec![format!("unknown role {role}; use viewer or writer")];
            };
            let ttl = match rest.first() {
                None => MAX_INVITE_TTL,
                Some(t) => match parse_ttl(t) {
                    Some(ttl) => ttl,
                    None => return vec![format!("bad ttl {t}; use 90s, 30m, 2h (max 24h)")],
                },
            };
            let token = create_invite(state, role, ttl);
            vec![
                format!("{} link, valid {}:", role.as_str(), fmt_duration(ttl)),
                format!("  teamsent join {}", join_link(relay, room, &token)),
            ]
        }
        ["/grant", id, role] => {
            let (Some(id), Some(role)) = (parse_guest_id(id), Role::parse(role)) else {
                return vec!["usage: /grant <guest#> <viewer|writer>".into()];
            };
            let mut state = state.lock().unwrap();
            match state.guests.get_mut(&id) {
                Some(guest) => {
                    guest.role = role;
                    vec![format!("#{id} {} is now {}", guest.name, role.as_str())]
                }
                None => vec![format!("no guest #{id}")],
            }
        }
        ["/revoke", id] => {
            let Some(id) = parse_guest_id(id) else {
                return vec!["usage: /revoke <guest#>".into()];
            };
            revoke(state, id)
        }
        _ => vec![format!("unknown command: {line} (try /help)")],
    }
}

fn parse_guest_id(s: &str) -> Option<u32> {
    s.trim_start_matches('#').parse().ok()
}

fn list(state: &Shared) -> Vec<String> {
    let state = state.lock().unwrap();
    let now = Instant::now();
    let mut out = vec!["invites:".to_string()];
    for (token, invite) in &state.invites {
        let status = if invite.revoked {
            "revoked".to_string()
        } else if invite.expires <= now {
            "expired".to_string()
        } else {
            format!("expires in {}", fmt_duration(invite.expires - now))
        };
        out.push(format!(
            "  {}…  {:<6}  {status}",
            &token[..8],
            invite.role.as_str()
        ));
    }
    out.push("guests:".to_string());
    if state.guests.is_empty() {
        out.push("  (none)".to_string());
    }
    for (id, guest) in &state.guests {
        let online = if guest.conns.is_empty() {
            "offline"
        } else {
            "online"
        };
        out.push(format!(
            "  #{id:<3} {:<16} {:<6}  {online}, {} ago",
            guest.name,
            guest.role.as_str(),
            fmt_duration(now - guest.since)
        ));
    }
    out
}

/// Burns the guest's invite link and disconnects everyone who joined with it.
fn revoke(state: &Shared, id: u32) -> Vec<String> {
    let mut state = state.lock().unwrap();
    let Some(token) = state.guests.get(&id).map(|g| g.token.clone()) else {
        return vec![format!("no guest #{id}")];
    };
    if let Some(invite) = state.invites.get_mut(&token) {
        invite.revoked = true;
    }
    let ids: Vec<u32> = state
        .guests
        .iter()
        .filter(|(_, g)| g.token == token)
        .map(|(id, _)| *id)
        .collect();
    let mut out = Vec::new();
    for gid in ids {
        if let Some(guest) = state.guests.remove(&gid) {
            for kick in guest.conns.values() {
                kick.notify_one();
            }
            out.push(format!("#{gid} {} disconnected", guest.name));
        }
    }
    out.push(format!("invite {}… revoked", &token[..8]));
    out
}

/// Checks the invite and registers the guest. Returns `(guest id, role)`.
fn admit(
    state: &Shared,
    query: &str,
    chan: &str,
    kick: Option<Arc<Notify>>,
) -> Result<(u32, Role), String> {
    let token = query_param(query, "t").ok_or("invite token missing")?;
    let name = query_param(query, "name")
        .filter(|n| is_valid_name(n))
        .unwrap_or("guest");
    let mut state = state.lock().unwrap();
    let invite = state.invites.get(token).ok_or("unknown invite")?;
    if invite.revoked {
        return Err("invite revoked".into());
    }
    if invite.expires <= Instant::now() {
        return Err("invite expired".into());
    }
    let invite_role = invite.role;

    // Reconnects with the same link and name keep their (possibly granted) role.
    let existing = state
        .guests
        .iter()
        .find(|(_, g)| g.token == token && g.name == name)
        .map(|(id, _)| *id);
    let id = match existing {
        Some(id) => id,
        None => {
            state.next_guest += 1;
            let id = state.next_guest;
            state.guests.insert(
                id,
                Guest {
                    name: name.to_string(),
                    token: token.to_string(),
                    role: invite_role,
                    since: Instant::now(),
                    conns: HashMap::new(),
                },
            );
            id
        }
    };
    let guest = state.guests.get_mut(&id).expect("guest just ensured");
    if let Some(kick) = kick {
        guest.conns.insert(chan.to_string(), kick);
    }
    Ok((id, guest.role))
}

async fn handle_open(url: String, chan: String, query: &str, state: Shared) -> io::Result<()> {
    let probe = query_param(query, "probe") == Some("1");
    let kick = Arc::new(Notify::new());
    let admitted = admit(&state, query, &chan, (!probe).then(|| Arc::clone(&kick)));

    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(io::Error::other)?;
    let (id, role) = match admitted {
        Ok(ok) => ok,
        Err(reason) => {
            eprintln!("join refused: {reason}");
            let _ = ws
                .send(Message::text(format!("teamsent-denied {reason}")))
                .await;
            let _ = ws.close(None).await;
            return Ok(());
        }
    };
    ws.send(Message::text(format!("teamsent-ok {} {id}", role.as_str())))
        .await
        .map_err(io::Error::other)?;
    if probe {
        let _ = ws.close(None).await;
        return Ok(());
    }

    let name = guest_name(&state, id);
    let first = connection_count(&state, id) == 1;
    if first {
        eprintln!("#{id} {name} joined as {}", role.as_str());
    }
    let result = pump(ws, id, &name, &state, &kick).await;

    let mut st = state.lock().unwrap();
    if let Some(guest) = st.guests.get_mut(&id) {
        guest.conns.remove(&chan);
        if guest.conns.is_empty() {
            eprintln!("#{id} {name} left");
        }
    }
    result
}

fn guest_name(state: &Shared, id: u32) -> String {
    state
        .lock()
        .unwrap()
        .guests
        .get(&id)
        .map(|g| g.name.clone())
        .unwrap_or_default()
}

fn connection_count(state: &Shared, id: u32) -> usize {
    state
        .lock()
        .unwrap()
        .guests
        .get(&id)
        .map_or(0, |g| g.conns.len())
}

/// The guest's current role, or `None` once the guest was revoked.
fn current_role(state: &Shared, id: u32) -> Option<Role> {
    state.lock().unwrap().guests.get(&id).map(|g| g.role)
}

async fn pump(
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    id: u32,
    name: &str,
    state: &Shared,
    kick: &Notify,
) -> io::Result<()> {
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
                let Some(role) = current_role(state, id) else {
                    return Ok(());
                };
                match filter::classify_frame(role, &frame) {
                    Verdict::Forward => {
                        if to_server_tx.send(frame).is_err() {
                            return Ok(());
                        }
                    }
                    Verdict::Drop(what) => {
                        tracing::debug!(id, what, "dropped guest frame");
                    }
                    Verdict::Reply(reply, method) => {
                        eprintln!("#{id} {name}: denied {method}");
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
        _ = kick.notified() => Ok(()),
    };
    shutdown_local_stream(&socket);
    result
}

fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h{:02}m", secs / 3600, secs % 3600 / 60)
    } else if secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with_invite(role: Role, ttl: Duration) -> (Shared, String) {
        let state: Shared = Arc::default();
        let token = create_invite(&state, role, ttl);
        (state, token)
    }

    #[test]
    fn admit_checks_token_and_assigns_invite_role() {
        let (state, token) = state_with_invite(Role::Writer, Duration::from_secs(60));
        assert_eq!(
            admit(&state, &format!("t={token}&name=kolya"), "c1", None),
            Ok((1, Role::Writer))
        );
        assert_eq!(
            admit(&state, "t=nope", "c2", None),
            Err("unknown invite".into())
        );
        assert_eq!(
            admit(&state, "name=kolya", "c3", None),
            Err("invite token missing".into())
        );
    }

    #[test]
    fn expired_invite_is_refused() {
        let (state, token) = state_with_invite(Role::Viewer, Duration::from_secs(60));
        state
            .lock()
            .unwrap()
            .invites
            .get_mut(&token)
            .unwrap()
            .expires = Instant::now();
        assert_eq!(
            admit(&state, &format!("t={token}"), "c1", None),
            Err("invite expired".into())
        );
    }

    #[test]
    fn grant_applies_and_survives_reconnect() {
        let (state, token) = state_with_invite(Role::Viewer, Duration::from_secs(60));
        let query = format!("t={token}&name=asel");
        assert_eq!(admit(&state, &query, "c1", None), Ok((1, Role::Viewer)));
        run_command("/grant 1 writer", &state, "ws://r", "room");
        assert_eq!(current_role(&state, 1), Some(Role::Writer));
        assert_eq!(admit(&state, &query, "c2", None), Ok((1, Role::Writer)));
    }

    #[test]
    fn revoke_kicks_guest_and_burns_link() {
        let (state, token) = state_with_invite(Role::Writer, Duration::from_secs(60));
        let kick = Arc::new(Notify::new());
        let query = format!("t={token}&name=kolya");
        admit(&state, &query, "c1", Some(Arc::clone(&kick))).unwrap();
        run_command("/revoke #1", &state, "ws://r", "room");
        assert_eq!(current_role(&state, 1), None);
        assert_eq!(
            admit(&state, &query, "c2", None),
            Err("invite revoked".into())
        );
        // The kick was signalled: a pending `notified()` completes immediately.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            tokio::time::timeout(Duration::from_millis(50), kick.notified())
                .await
                .expect("revoke notifies live connections");
        });
    }

    #[test]
    fn invalid_names_fall_back_to_guest() {
        let (state, token) = state_with_invite(Role::Viewer, Duration::from_secs(60));
        admit(&state, &format!("t={token}&name=a%20b"), "c1", None).unwrap();
        assert_eq!(guest_name(&state, 1), "guest");
    }
}
