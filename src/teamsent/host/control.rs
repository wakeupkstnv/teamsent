//! Host control socket for the Team window (`teamsent team`).
//!
//! Private local socket (0600) under the state dir. Newline-delimited JSON:
//! the host streams `{"t":"state", …}` on every change; the window sends
//! commands and gets `{"t":"result","ok","msg"}` back.
//!
//! Commands: `{"cmd":"approve","id",["pane"]}`, `{"cmd":"reject","id"}`,
//! `{"cmd":"edit","id","text"}`, `{"cmd":"drive","guest"}`, `{"cmd":"take"}`,
//! `{"cmd":"role","guest","role"}`, `{"cmd":"kick","guest"}`,
//! `{"cmd":"prompt","text"}`, `{"cmd":"move","id","delta"}`, `{"cmd":"clear"}`,
//! `{"cmd":"pause","paused"}`, `{"cmd":"auto","on"}`, `{"cmd":"names","on"}`.

use std::io::{self, BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use interprocess::local_socket::traits::Listener as _;
use interprocess::TryClone as _;
use serde_json::{json, Value};

use super::{
    approve, clear_queue, edit, give_floor, grant, host_prompt, kick, move_prompt, reject,
    set_auto, set_names, set_paused, snapshot, take_floor, Ctx,
};
use crate::teamsent::host_control_socket_path;
use crate::teamsent::Role;

const STATE_POLL: Duration = Duration::from_millis(200);
const SOCKET_PERMISSION_MODE: u32 = 0o600;

pub(super) fn start(ctx: Arc<Ctx>) -> io::Result<()> {
    let path = host_control_socket_path();
    crate::ipc::prepare_socket_path(&path, |path| {
        format!("another teamsent host is running ({})", path.display())
    })?;
    let listener = crate::ipc::bind_private_local_listener(&path)?;
    crate::ipc::restrict_socket_permissions(&path, SOCKET_PERMISSION_MODE)?;
    std::thread::spawn(move || {
        while let Ok(stream) = listener.accept() {
            let ctx = Arc::clone(&ctx);
            std::thread::spawn(move || {
                let _ = serve(stream, &ctx);
            });
        }
    });
    Ok(())
}

fn serve(stream: crate::ipc::LocalStream, ctx: &Arc<Ctx>) -> io::Result<()> {
    let reader = BufReader::new(stream.try_clone()?);
    let writer = Arc::new(Mutex::new(stream));

    let state_writer = Arc::clone(&writer);
    let state_ctx = Arc::clone(ctx);
    std::thread::spawn(move || {
        let mut last = String::new();
        loop {
            let state = snapshot(&state_ctx, None).to_string();
            if state != last {
                if writeln!(state_writer.lock().unwrap(), "{state}").is_err() {
                    return;
                }
                last = state;
            }
            std::thread::sleep(STATE_POLL);
        }
    });

    for line in reader.lines() {
        let reply = match serde_json::from_str::<Value>(&line?) {
            Ok(cmd) => run(ctx, &cmd),
            Err(err) => Err(format!("bad command: {err}")),
        };
        let reply = match reply {
            Ok(msg) => json!({ "t": "result", "ok": true, "msg": msg }),
            Err(msg) => json!({ "t": "result", "ok": false, "msg": msg }),
        };
        eprintln!("team window: {}", reply["msg"].as_str().unwrap_or_default());
        writeln!(writer.lock().unwrap(), "{reply}")?;
    }
    Ok(())
}

fn run(ctx: &Ctx, cmd: &Value) -> Result<String, String> {
    let id = || {
        cmd["id"]
            .as_u64()
            .map(|n| n as u32)
            .ok_or("missing id".to_string())
    };
    let guest = || {
        cmd["guest"]
            .as_u64()
            .map(|n| n as u32)
            .ok_or("missing guest".to_string())
    };
    match cmd["cmd"].as_str().unwrap_or_default() {
        "approve" => approve(ctx, id()?, cmd["pane"].as_str()),
        "reject" => reject(ctx, id()?),
        "edit" => edit(ctx, id()?, cmd["text"].as_str().unwrap_or_default()),
        "drive" => give_floor(ctx, guest()?),
        "take" => Ok(take_floor(ctx)),
        "prompt" => host_prompt(ctx, cmd["text"].as_str().unwrap_or_default()),
        "move" => move_prompt(ctx, id()?, cmd["delta"].as_i64().unwrap_or(0) as i32),
        "clear" => Ok(clear_queue(ctx)),
        "pause" => Ok(set_paused(ctx, cmd["paused"].as_bool().unwrap_or(true))),
        "auto" => Ok(set_auto(ctx, cmd["on"].as_bool().unwrap_or(true))),
        "names" => Ok(set_names(ctx, cmd["on"].as_bool().unwrap_or(true))),
        "role" => {
            let role = cmd["role"]
                .as_str()
                .and_then(Role::parse)
                .ok_or("unknown role")?;
            grant(ctx, guest()?, role)
        }
        "kick" => {
            let lines = kick(ctx, guest()?);
            if lines.first().is_some_and(|l| l.starts_with("no guest")) {
                Err(lines.join("; "))
            } else {
                Ok(lines.join("; "))
            }
        }
        other => Err(format!("unknown command {other:?}")),
    }
}
