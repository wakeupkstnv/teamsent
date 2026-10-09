//! `teamsent host`: share the local herdr session through a relay.
//!
//! Access is granted by invite links. Each link carries a token bound to a
//! role and an expiry. For every guest the relay announces, the host checks
//! the token, opens a channel to the relay and a fresh connection to the herdr
//! client socket, then pumps frames between them. Guest frames pass the role
//! filter, re-checked on every frame so `/grant` and `/revoke` apply at once.
//!
//! The host terminal accepts commands (see `HELP`): access control, the
//! suggestion queue (`/q`, `/y`, `/n`, `/e`), the floor (`/drive`, `/take`) and
//! the audit log (`/log`). The same operations back the Team window
//! (`control.rs`) and the guests' composer bar (`collab.rs`).

mod collab;
mod control;

use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use interprocess::TryClone as _;
use tokio::io::AsyncBufReadExt;
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite::Message;

use serde_json::json;

use super::audit::Audit;
use super::filter::{self, Verdict};
use super::queue::{self, Job, SharedQueue, MAX_SUGGESTION_BYTES};
use super::{
    is_valid_name, parse_ttl, query_param, random_id, shutdown_local_stream, Role, MAX_INVITE_TTL,
    PROTOCOL_TAG,
};
use crate::protocol::{MAX_FRAME_SIZE, MAX_GRAPHICS_FRAME_SIZE};

/// Team relay on GCP (deploy/README.md). Override with `--relay` or `TEAMSENT_RELAY`.
const DEFAULT_RELAY: &str = "wss://34-38-17-92.sslip.io";
const READ_CHUNK: usize = 64 * 1024;
/// A driver who has not typed for this long loses the floor.
const FLOOR_IDLE: Duration = Duration::from_secs(30);
const USAGE: &str = "usage: teamsent host [--relay wss://HOST] [--role viewer|participant|writer] [--ttl 30m|2h|24h] [--no-team]";
const ROLES: &str = "viewer, participant or writer";
const HELP: &str = "\
roles: viewer (watch) · participant (watch + suggest) · writer (type + suggest)
Only one writer types at a time: whoever holds the floor (taken by typing when free,
released after 30s idle, or given/taken by you).

access:
  /list                      invites and guests
  /invite <role> [ttl]       new invite link (ttl: 90s, 30m, 2h; max 24h)
  /grant <guest#> <role>     change a guest's role now
  /revoke <guest#>           disconnect the guest and burn their invite link
suggestions (guests send them with `teamsent say <link> <text>`):
  /q                         show the queue
  /y <#> [pane]              approve; delivered to the pane's agent when it is idle
                             (default: the focused agent pane)
  /e <#> <text>              rewrite a pending suggestion before approving
  /n <#>                     reject
floor:
  /drive <guest#>            give the floor (makes the guest a writer)
  /take                      take the floor back
log:
  /log [n]                   last n audit entries (default 20)
  /help                      this text";

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
    /// What each guest is typing in their composer right now.
    drafts: HashMap<u32, String>,
    /// The guest whose keystrokes reach panes, and when they last typed.
    driver: Option<(u32, Instant)>,
    /// The Team window's pane; guests may never type into it.
    team_pane: Option<String>,
}

type Shared = Arc<Mutex<State>>;

/// Everything the host's tasks share.
struct Ctx {
    state: Shared,
    queue: SharedQueue,
    audit: Arc<Audit>,
    dispatch: std::sync::mpsc::Sender<Job>,
    /// Agent pane and its status, refreshed every second.
    agent: Mutex<(String, String)>,
}

impl Ctx {
    fn new(state: Shared, audit: Audit) -> Self {
        let queue = SharedQueue::default();
        let audit = Arc::new(audit);
        let dispatch = queue::spawn_dispatcher(Arc::clone(&queue), Arc::clone(&audit));
        Self {
            state,
            queue,
            audit,
            dispatch,
            agent: Mutex::new((String::new(), "unknown".into())),
        }
    }
}

struct Options {
    relay: String,
    role: Role,
    ttl: Duration,
    team: bool,
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
        team: true,
    };
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        if flag == "--no-team" {
            options.team = false;
            continue;
        }
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
    let Options {
        relay,
        role,
        ttl,
        team,
    } = options;
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

    let ctx = Arc::new(Ctx::new(Shared::default(), Audit::open_default()));
    ctx.audit
        .record("share_start", json!({ "relay": relay, "room": room }));
    let token = create_invite(&ctx, role, ttl);
    eprintln!("Sharing this herdr session.");
    eprintln!();
    eprintln!("  {} link, valid {}:", role.as_str(), fmt_duration(ttl));
    eprintln!("  teamsent join {}", join_link(&relay, &room, &token));
    eprintln!();
    eprintln!("Type /help for commands, Ctrl+C to stop sharing.");
    eprintln!("Audit log: {}", ctx.audit.path().display());
    spawn_watcher(Arc::clone(&ctx));
    match control::start(Arc::clone(&ctx)) {
        Ok(()) if team => match open_team_pane(&ctx) {
            Ok(pane) => eprintln!("Team window: pane {pane}"),
            Err(err) => {
                eprintln!("Team window not opened: {err} (run `teamsent team` in any pane)")
            }
        },
        Ok(()) => {}
        Err(err) => eprintln!("Team window unavailable: {err}"),
    }

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
                        let ctx = Arc::clone(&ctx);
                        tokio::spawn(async move {
                            if let Err(err) = handle_open(url, chan, &query, ctx).await {
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
                    for out in run_command(line.trim(), &ctx, &relay, &room) {
                        eprintln!("{out}");
                    }
                }
                // stdin closed (e.g. running in the background): keep sharing.
                Ok(None) | Err(_) => stdin_open = false,
            },
            _ = tokio::signal::ctrl_c() => {
                let _ = ws.close(None).await;
                ctx.audit.record("share_stop", json!({}));
                eprintln!("Stopped sharing.");
                return Ok(());
            }
        }
    }
}

fn join_link(relay: &str, room: &str, token: &str) -> String {
    format!("{relay}/join/{room}/{token}")
}

fn create_invite(ctx: &Ctx, role: Role, ttl: Duration) -> String {
    let token = random_id();
    ctx.audit.record(
        "invite_created",
        json!({ "invite": &token[..8], "role": role.as_str(), "ttl_secs": ttl.as_secs() }),
    );
    ctx.state.lock().unwrap().invites.insert(
        token.clone(),
        Invite {
            role,
            expires: Instant::now() + ttl,
            revoked: false,
        },
    );
    token
}

fn run_command(line: &str, ctx: &Ctx, relay: &str, room: &str) -> Vec<String> {
    let state = &ctx.state;
    let args: Vec<&str> = line.split_whitespace().collect();
    match args.as_slice() {
        [] => Vec::new(),
        ["/help"] => vec![HELP.to_string()],
        ["/list"] => list(state),
        ["/invite", role, rest @ ..] if rest.len() <= 1 => {
            let Some(role) = Role::parse(role) else {
                return vec![format!("unknown role {role}; use {ROLES}")];
            };
            let ttl = match rest.first() {
                None => MAX_INVITE_TTL,
                Some(t) => match parse_ttl(t) {
                    Some(ttl) => ttl,
                    None => return vec![format!("bad ttl {t}; use 90s, 30m, 2h (max 24h)")],
                },
            };
            let token = create_invite(ctx, role, ttl);
            vec![
                format!("{} link, valid {}:", role.as_str(), fmt_duration(ttl)),
                format!("  teamsent join {}", join_link(relay, room, &token)),
            ]
        }
        ["/grant", id, role] => {
            let (Some(id), Some(role)) = (parse_guest_id(id), Role::parse(role)) else {
                return vec![format!("usage: /grant <guest#> <role>; roles: {ROLES}")];
            };
            let mut state = state.lock().unwrap();
            match state.guests.get_mut(&id) {
                Some(guest) => {
                    guest.role = role;
                    ctx.audit.record(
                        "grant",
                        json!({ "guest": id, "name": guest.name, "role": role.as_str() }),
                    );
                    vec![format!("#{id} {} is now {}", guest.name, role.as_str())]
                }
                None => vec![format!("no guest #{id}")],
            }
        }
        ["/revoke", id] => {
            let Some(id) = parse_guest_id(id) else {
                return vec!["usage: /revoke <guest#>".into()];
            };
            ctx.audit.record("revoke", json!({ "guest": id }));
            revoke(state, id)
        }
        ["/q"] => ctx.queue.lock().unwrap().lines(),
        ["/y", id, rest @ ..] if rest.len() <= 1 => match parse_guest_id(id) {
            Some(id) => vec![approve(ctx, id, rest.first().copied()).unwrap_or_else(|e| e)],
            None => vec!["usage: /y <#> [pane]".into()],
        },
        ["/n", id] => match parse_guest_id(id) {
            Some(id) => vec![reject(ctx, id).unwrap_or_else(|e| e)],
            None => vec!["usage: /n <#>".into()],
        },
        ["/e", id, ..] => match parse_guest_id(id) {
            Some(id) => {
                let text = line.splitn(3, char::is_whitespace).nth(2).unwrap_or("");
                vec![edit(ctx, id, text).unwrap_or_else(|e| e)]
            }
            None => vec!["usage: /e <#> <text>".into()],
        },
        ["/drive", id] => match parse_guest_id(id) {
            Some(id) => vec![give_floor(ctx, id).unwrap_or_else(|e| e)],
            None => vec!["usage: /drive <guest#>".into()],
        },
        ["/take"] => vec![take_floor(ctx)],
        ["/log", rest @ ..] if rest.len() <= 1 => {
            let n = rest.first().and_then(|n| n.parse().ok()).unwrap_or(20);
            let lines = ctx.audit.tail(n);
            if lines.is_empty() {
                vec!["(audit log is empty)".into()]
            } else {
                lines
            }
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
            "  {}…  {:<11}  {status}",
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
            "  #{id:<3} {:<16} {:<11}  {online}, {} ago",
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
        state.drafts.remove(&gid);
        if state.driver.is_some_and(|(d, _)| d == gid) {
            state.driver = None;
        }
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

async fn handle_open(url: String, chan: String, query: &str, ctx: Arc<Ctx>) -> io::Result<()> {
    let state = &ctx.state;
    let probe = query_param(query, "probe") == Some("1");
    let say = query_param(query, "kind") == Some("say");
    let collab = query_param(query, "kind") == Some("collab");
    let kick = Arc::new(Notify::new());
    let live = !probe && !say;
    let admitted = admit(state, query, &chan, live.then(|| Arc::clone(&kick)));

    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(io::Error::other)?;
    let (id, role) = match admitted {
        Ok(ok) => ok,
        Err(reason) => {
            eprintln!("join refused: {reason}");
            ctx.audit
                .record("join_refused", json!({ "reason": reason }));
            let _ = ws
                .send(Message::text(format!("teamsent-denied {reason}")))
                .await;
            let _ = ws.close(None).await;
            return Ok(());
        }
    };
    if say {
        return receive_suggestion(ws, id, role, &ctx).await;
    }
    ws.send(Message::text(format!("teamsent-ok {} {id}", role.as_str())))
        .await
        .map_err(io::Error::other)?;
    if probe {
        let _ = ws.close(None).await;
        return Ok(());
    }
    if collab {
        let result = collab::serve(ws, id, &ctx, &kick).await;
        let mut st = state.lock().unwrap();
        st.drafts.remove(&id);
        if let Some(guest) = st.guests.get_mut(&id) {
            guest.conns.remove(&chan);
        }
        return result;
    }

    let name = guest_name(state, id);
    let first = connection_count(state, id) == 1;
    if first {
        eprintln!("#{id} {name} joined as {}", role.as_str());
        ctx.audit.record(
            "join",
            json!({ "guest": id, "name": name, "role": role.as_str() }),
        );
    }
    let result = pump(ws, id, &name, &ctx, &kick).await;

    let left = {
        let mut st = state.lock().unwrap();
        st.guests.get_mut(&id).is_some_and(|guest| {
            guest.conns.remove(&chan);
            guest.conns.is_empty()
        })
    };
    if left {
        eprintln!("#{id} {name} left");
        ctx.audit
            .record("leave", json!({ "guest": id, "name": name }));
    }
    result
}

/// Reads one suggestion from a `teamsent say` connection and queues it.
async fn receive_suggestion(
    mut ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    id: u32,
    role: Role,
    ctx: &Ctx,
) -> io::Result<()> {
    let reply = if role == Role::Viewer {
        format!("teamsent-denied {VIEWER_CANNOT_SUGGEST}")
    } else {
        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next()).await;
        match msg {
            Ok(Some(Ok(Message::Text(text)))) => match submit(ctx, id, &text) {
                Ok(sid) => format!("teamsent-queued {sid}"),
                Err(err) => format!("teamsent-denied {err}"),
            },
            _ => "teamsent-denied no suggestion received".to_string(),
        }
    };
    let _ = ws.send(Message::text(reply)).await;
    let _ = ws.close(None).await;
    Ok(())
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
#[cfg(test)]
fn current_role(state: &Shared, id: u32) -> Option<Role> {
    state.lock().unwrap().guests.get(&id).map(|g| g.role)
}

async fn pump(
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    id: u32,
    name: &str,
    ctx: &Ctx,
    kick: &Notify,
) -> io::Result<()> {
    let state = &ctx.state;
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
                let Some(access) = access_for(state, id, filter::is_input_frame(&frame)) else {
                    return Ok(());
                };
                let verdict = {
                    let team_pane = access.1.as_deref();
                    filter::classify_frame(
                        filter::Access {
                            role: access.0,
                            may_type: access.2,
                            host_only_pane: team_pane,
                        },
                        &frame,
                    )
                };
                match verdict {
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
                        ctx.audit.record(
                            "denied",
                            json!({ "guest": id, "name": name, "method": method }),
                        );
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

const VIEWER_CANNOT_SUGGEST: &str = "viewers cannot suggest; ask the host for /grant";

/// Role, Team pane and whether this frame may type, for one guest frame.
/// Typing by a writer claims a free floor and refreshes a held one.
/// `None` once the guest was revoked.
fn access_for(state: &Shared, id: u32, is_input: bool) -> Option<(Role, Option<String>, bool)> {
    let mut st = state.lock().unwrap();
    let role = st.guests.get(&id)?.role;
    let mut may_type = false;
    if role == Role::Writer {
        match st.driver {
            None if is_input => {
                st.driver = Some((id, Instant::now()));
                let name = st.guests[&id].name.clone();
                eprintln!("#{id} {name} took the floor");
                may_type = true;
            }
            Some((driver, _)) if driver == id => {
                if is_input {
                    st.driver = Some((id, Instant::now()));
                }
                may_type = true;
            }
            _ => {}
        }
    }
    Some((role, st.team_pane.clone(), may_type))
}

/// Queues a guest's suggestion and tells the host.
fn submit(ctx: &Ctx, id: u32, text: &str) -> Result<u32, String> {
    let (name, role) = {
        let st = ctx.state.lock().unwrap();
        let guest = st.guests.get(&id).ok_or("unknown guest")?;
        (guest.name.clone(), guest.role)
    };
    if role == Role::Viewer {
        return Err(VIEWER_CANNOT_SUGGEST.into());
    }
    let text = text.trim();
    if text.is_empty() || text.len() > MAX_SUGGESTION_BYTES {
        return Err(format!("suggestion must be 1-{MAX_SUGGESTION_BYTES} bytes"));
    }
    let author = format!("{name}#{id}");
    let sid = ctx.queue.lock().unwrap().push(&author, text);
    ctx.state.lock().unwrap().drafts.remove(&id);
    eprintln!("\x07#{sid} from {author}: {}", queue::one_line(text, 200));
    eprintln!("     /y {sid} to send to the agent, /e {sid} <text> to edit, /n {sid} to reject");
    ctx.audit.record(
        "suggestion",
        json!({ "id": sid, "guest": id, "name": name, "text": text }),
    );
    queue::notify(
        &format!("teamsent: {name} suggests #{sid}"),
        &queue::one_line(text, 120),
    );
    Ok(sid)
}

fn approve(ctx: &Ctx, sid: u32, pane: Option<&str>) -> Result<String, String> {
    let target = match pane {
        Some(pane) => pane.to_string(),
        None => {
            let team = ctx.state.lock().unwrap().team_pane.clone();
            queue::agent_pane(team.as_deref())
                .map_err(|err| format!("cannot pick a pane: {err}"))?
                .0
        }
    };
    let text = ctx.queue.lock().unwrap().approve(sid, &target)?;
    ctx.audit.record(
        "suggestion_approved",
        json!({ "id": sid, "target": target }),
    );
    let _ = ctx.dispatch.send(Job {
        id: sid,
        target: target.clone(),
        text,
    });
    Ok(format!("#{sid} approved → {target}"))
}

fn reject(ctx: &Ctx, sid: u32) -> Result<String, String> {
    ctx.queue.lock().unwrap().reject(sid)?;
    ctx.audit
        .record("suggestion_rejected", json!({ "id": sid }));
    Ok(format!("#{sid} rejected"))
}

fn edit(ctx: &Ctx, sid: u32, text: &str) -> Result<String, String> {
    ctx.queue.lock().unwrap().edit(sid, text)?;
    ctx.audit.record(
        "suggestion_edited",
        json!({ "id": sid, "text": text.trim() }),
    );
    Ok(format!("#{sid} edited"))
}

/// Gives the floor to a guest, promoting them to writer if needed.
fn give_floor(ctx: &Ctx, id: u32) -> Result<String, String> {
    let name = {
        let mut st = ctx.state.lock().unwrap();
        let guest = st.guests.get_mut(&id).ok_or(format!("no guest #{id}"))?;
        guest.role = Role::Writer;
        let name = guest.name.clone();
        st.driver = Some((id, Instant::now()));
        name
    };
    ctx.audit
        .record("floor_given", json!({ "guest": id, "name": name }));
    queue::notify(
        &format!("teamsent: {name} is driving"),
        "the host gave them the floor",
    );
    Ok(format!("#{id} {name} has the floor"))
}

fn take_floor(ctx: &Ctx) -> String {
    let taken = ctx.state.lock().unwrap().driver.take();
    match taken {
        Some((id, _)) => {
            ctx.audit.record("floor_taken", json!({ "guest": id }));
            format!("floor taken back from #{id}")
        }
        None => "nobody has the floor".into(),
    }
}

/// Session state as JSON for the Team window (`me = None`) and guest bars.
fn snapshot(ctx: &Ctx, me: Option<u32>) -> serde_json::Value {
    let (agent_pane, agent_status) = ctx.agent.lock().unwrap().clone();
    let queue = ctx.queue.lock().unwrap().snapshot();
    let st = ctx.state.lock().unwrap();
    let driver = st.driver.map(|(id, _)| id);
    let mut people = vec![json!({
        "id": 0, "name": "host", "role": "host", "online": true, "draft": "", "driving": false,
    })];
    people.extend(st.guests.iter().map(|(id, g)| {
        json!({
            "id": id,
            "name": g.name,
            "role": g.role.as_str(),
            "online": !g.conns.is_empty(),
            "draft": st.drafts.get(id).cloned().unwrap_or_default(),
            "driving": driver == Some(*id),
        })
    }));
    let me = me.and_then(|id| {
        st.guests.get(&id).map(|g| {
            json!({ "id": id, "name": g.name, "role": g.role.as_str(), "driving": driver == Some(id) })
        })
    });
    json!({
        "t": "state",
        "me": me,
        "people": people,
        "agent": { "pane": agent_pane, "status": agent_status },
        "queue": queue,
        "driver": driver.and_then(|id| st.guests.get(&id).map(|g| g.name.clone())),
        "team_pane": st.team_pane,
    })
}

/// Refreshes the agent pane/status every second and releases an idle floor.
fn spawn_watcher(ctx: Arc<Ctx>) {
    std::thread::spawn(move || loop {
        let team = ctx.state.lock().unwrap().team_pane.clone();
        if let Ok(agent) = queue::agent_pane(team.as_deref()) {
            *ctx.agent.lock().unwrap() = agent;
        }
        let released = {
            let mut st = ctx.state.lock().unwrap();
            match st.driver {
                Some((id, last)) if last.elapsed() >= FLOOR_IDLE => {
                    st.driver = None;
                    Some(id)
                }
                _ => None,
            }
        };
        if let Some(id) = released {
            eprintln!("#{id} released the floor (idle)");
            ctx.audit.record("floor_released", json!({ "guest": id }));
        }
        std::thread::sleep(Duration::from_secs(1));
    });
}

/// Splits the agent pane and runs `teamsent team` on the right.
fn open_team_pane(ctx: &Ctx) -> Result<String, String> {
    let (agent, _) = queue::agent_pane(None)?;
    let split = queue::cli_json(&[
        "pane",
        "split",
        &agent,
        "--direction",
        "right",
        "--ratio",
        "0.7",
    ])?;
    let pane = split["result"]["pane"]["pane_id"]
        .as_str()
        .ok_or("split returned no pane")?
        .to_string();
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let command = format!(
        "'{}' team",
        exe.display().to_string().replace('\'', "'\\''")
    );
    queue::cli(&["pane", "run", &pane, &command])?;
    ctx.state.lock().unwrap().team_pane = Some(pane.clone());
    Ok(pane)
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

    fn test_ctx() -> Ctx {
        let dir = std::env::temp_dir().join(format!("teamsent-host-test-{}", random_id()));
        Ctx::new(Shared::default(), Audit::open(dir.join("audit.jsonl")))
    }

    fn state_with_invite(role: Role, ttl: Duration) -> (Shared, String) {
        let ctx = test_ctx();
        let token = create_invite(&ctx, role, ttl);
        (Arc::clone(&ctx.state), token)
    }

    fn ctx_for(state: &Shared) -> Ctx {
        let mut ctx = test_ctx();
        ctx.state = Arc::clone(state);
        ctx
    }

    #[test]
    fn viewers_cannot_queue_but_participants_can() {
        let ctx = test_ctx();
        let viewer = create_invite(&ctx, Role::Viewer, Duration::from_secs(60));
        let participant = create_invite(&ctx, Role::Participant, Duration::from_secs(60));
        assert_eq!(
            admit(&ctx.state, &format!("t={viewer}&name=v"), "c1", None),
            Ok((1, Role::Viewer))
        );
        assert_eq!(
            admit(&ctx.state, &format!("t={participant}&name=p"), "c2", None),
            Ok((2, Role::Participant))
        );
        let sid = ctx.queue.lock().unwrap().push("p#2", "write tests");
        assert_eq!(
            run_command(&format!("/n {sid}"), &ctx, "ws://r", "room"),
            vec![format!("#{sid} rejected")]
        );
        assert!(ctx.audit.tail(1)[0].contains("suggestion_rejected"));
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
        run_command("/grant 1 writer", &ctx_for(&state), "ws://r", "room");
        assert_eq!(current_role(&state, 1), Some(Role::Writer));
        assert_eq!(admit(&state, &query, "c2", None), Ok((1, Role::Writer)));
    }

    #[test]
    fn revoke_kicks_guest_and_burns_link() {
        let (state, token) = state_with_invite(Role::Writer, Duration::from_secs(60));
        let kick = Arc::new(Notify::new());
        let query = format!("t={token}&name=kolya");
        admit(&state, &query, "c1", Some(Arc::clone(&kick))).unwrap();
        run_command("/revoke #1", &ctx_for(&state), "ws://r", "room");
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
