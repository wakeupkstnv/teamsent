//! Guest composer bar for `teamsent join`.
//!
//! The herdr client runs in an inner PTY one row shorter than the terminal; the
//! last row is ours. It shows who is online, the agent's status, the queue and
//! the floor, and turns into a composer on `Ctrl+]`: Enter sends a suggestion,
//! Esc cancels, and the draft streams to the host as it is typed.
//!
//! The bar is drawn after every chunk of client output with save/restore
//! cursor, so the client never notices it.

use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use unicode_width::UnicodeWidthStr;

const POLL: Duration = Duration::from_millis(150);
const DRAFT_EVERY: Duration = Duration::from_millis(200);
const FLASH_FOR: Duration = Duration::from_secs(4);
const READ_CHUNK: usize = 64 * 1024;

/// What a chunk of keyboard input means while the composer is open.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Key {
    Text(String),
    Enter,
    Backspace,
    Escape,
    /// `Ctrl+]`, the composer toggle.
    Toggle,
}

/// Splits raw terminal input into keys. Understands legacy bytes and the kitty
/// keyboard protocol (`ESC [ code[:alt] [; mods[:event]] [; text] u`), which
/// herdr enables. Unknown escape sequences are dropped. Returns the keys and
/// any incomplete trailing bytes to prepend to the next read.
pub(crate) fn parse_keys(input: &[u8]) -> (Vec<Key>, Vec<u8>) {
    let mut keys = Vec::new();
    let mut i = 0;
    let push_text = |keys: &mut Vec<Key>, s: &str| {
        if let Some(Key::Text(t)) = keys.last_mut() {
            t.push_str(s);
        } else {
            keys.push(Key::Text(s.to_string()));
        }
    };
    while i < input.len() {
        match input[i] {
            0x1d => {
                keys.push(Key::Toggle);
                i += 1;
            }
            b'\r' | b'\n' => {
                keys.push(Key::Enter);
                i += 1;
            }
            0x7f | 0x08 => {
                keys.push(Key::Backspace);
                i += 1;
            }
            0x1b => {
                if i + 1 == input.len() {
                    keys.push(Key::Escape);
                    i += 1;
                    continue;
                }
                if input[i + 1] != b'[' && input[i + 1] != b'O' {
                    // Alt+key or a lone Esc followed by text.
                    keys.push(Key::Escape);
                    i += 1;
                    continue;
                }
                // CSI / SS3: find the final byte.
                let Some(end) = input[i + 2..]
                    .iter()
                    .position(|b| (0x40..=0x7e).contains(b))
                else {
                    return (keys, input[i..].to_vec());
                };
                let end = i + 2 + end;
                if input[end] == b'u' && input[i + 1] == b'[' {
                    if let Some(key) = kitty_key(&input[i + 2..end]) {
                        match key {
                            Key::Text(t) => push_text(&mut keys, &t),
                            other => keys.push(other),
                        }
                    }
                }
                i = end + 1;
            }
            b if b < 0x20 => i += 1,
            _ => {
                // UTF-8 text up to the next control byte.
                let start = i;
                while i < input.len() && input[i] >= 0x20 && input[i] != 0x7f {
                    i += 1;
                }
                match std::str::from_utf8(&input[start..i]) {
                    Ok(s) => push_text(&mut keys, s),
                    Err(err) => {
                        let valid = start + err.valid_up_to();
                        if valid > start {
                            push_text(
                                &mut keys,
                                std::str::from_utf8(&input[start..valid]).unwrap(),
                            );
                        }
                        if err.error_len().is_none() {
                            return (keys, input[valid..].to_vec());
                        }
                        i = valid + err.error_len().unwrap_or(1);
                    }
                }
            }
        }
    }
    (keys, Vec::new())
}

/// Decodes the parameters of a kitty `CSI … u` key event.
fn kitty_key(params: &[u8]) -> Option<Key> {
    let params = std::str::from_utf8(params).ok()?;
    let mut fields = params.split(';');
    let code: u32 = fields.next()?.split(':').next()?.parse().ok()?;
    let mut mod_event = fields.next().unwrap_or("1").split(':');
    let mods: u32 = mod_event.next().unwrap_or("1").parse().unwrap_or(1);
    let event: u32 = mod_event.next().unwrap_or("1").parse().unwrap_or(1);
    if event == 3 {
        return None; // key release
    }
    let ctrl = mods.saturating_sub(1) & 4 != 0;
    let alt = mods.saturating_sub(1) & 2 != 0;
    match code {
        93 if ctrl => Some(Key::Toggle),
        13 => Some(Key::Enter),
        27 => Some(Key::Escape),
        127 | 8 => Some(Key::Backspace),
        _ if ctrl || alt => None,
        _ => {
            if let Some(text) = fields.next() {
                let text: String = text
                    .split(':')
                    .filter_map(|c| c.parse::<u32>().ok().and_then(char::from_u32))
                    .collect();
                if !text.is_empty() {
                    return Some(Key::Text(text));
                }
            }
            char::from_u32(code)
                .filter(|c| !c.is_control())
                .map(|c| Key::Text(c.to_string()))
        }
    }
}

struct Bar {
    state: Value,
    composer: Option<String>,
    flash: Option<(String, Instant)>,
    rows: u16,
    cols: u16,
}

impl Bar {
    fn line(&self) -> String {
        if let Some(text) = &self.composer {
            let hint = "  Enter send · Esc cancel";
            let room = (self.cols as usize).saturating_sub(12 + hint.len());
            let shown: String = {
                let chars: Vec<char> = text.chars().collect();
                let skip = chars.len().saturating_sub(room);
                chars[skip..].iter().collect()
            };
            return format!(" suggest › {shown}▏{hint}");
        }
        if let Some((msg, at)) = &self.flash {
            if at.elapsed() < FLASH_FOR {
                return format!(" teamsent · {msg}");
            }
        }
        let s = &self.state;
        if s.is_null() {
            return " teamsent · connecting…".into();
        }
        let me = &s["me"];
        let role = me["role"].as_str().unwrap_or("?");
        let online = s["people"]
            .as_array()
            .map_or(0, |p| p.iter().filter(|p| p["online"] == true).count());
        let pending = s["queue"]
            .as_array()
            .map_or(0, |q| q.iter().filter(|q| q["pending"] == true).count());
        let floor = if me["driving"] == true {
            " · 🚗 you drive".to_string()
        } else {
            s["driver"]
                .as_str()
                .map(|d| format!(" · 🚗 {d}"))
                .unwrap_or_default()
        };
        let action = if role == "viewer" {
            "view only"
        } else {
            "⌃] suggest"
        };
        format!(
            " teamsent · {} ({role}) · {online} online · agent: {} · queue {pending}{floor} · {action}",
            me["name"].as_str().unwrap_or("?"),
            s["agent"]["status"].as_str().unwrap_or("?"),
        )
    }

    /// Escape sequence that paints the bar without moving the client's cursor.
    fn paint(&self) -> String {
        let mut text = self.line();
        let width = self.cols as usize;
        while text.width() > width {
            text.pop();
        }
        let pad = width.saturating_sub(text.width());
        format!(
            "\x1b7\x1b[{};1H\x1b[0m\x1b[2K\x1b[7m{}{}\x1b[0m\x1b8",
            self.rows,
            text,
            " ".repeat(pad)
        )
    }
}

type Shared = Arc<Mutex<Bar>>;
type Out = Arc<Mutex<io::Stdout>>;

fn repaint(bar: &Shared, out: &Out) {
    let paint = bar.lock().unwrap().paint();
    let mut out = out.lock().unwrap();
    let _ = out.write_all(paint.as_bytes());
    let _ = out.flush();
}

/// Runs the herdr client with the bar below it. `link` is the invite link with
/// `?name=` already appended.
pub(crate) fn run_client(
    socket_path: &Path,
    link: &str,
    runtime: &tokio::runtime::Handle,
) -> io::Result<()> {
    let (cols, rows) = crossterm::terminal::size()?;
    if rows < 4 {
        return Err(io::Error::other("terminal too small for the teamsent bar"));
    }
    let pty = native_pty_system()
        .openpty(PtySize {
            rows: rows - 1,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(io::Error::other)?;
    let mut cmd = CommandBuilder::new(std::env::current_exe()?);
    cmd.arg("client");
    cmd.env(
        crate::server::socket_paths::CLIENT_SOCKET_PATH_ENV_VAR,
        socket_path,
    );
    cmd.env_remove(crate::api::SOCKET_PATH_ENV_VAR);
    if let Ok(cwd) = std::env::current_dir() {
        cmd.cwd(cwd);
    }
    let mut child = pty.slave.spawn_command(cmd).map_err(io::Error::other)?;
    drop(pty.slave);
    let mut reader = pty.master.try_clone_reader().map_err(io::Error::other)?;
    let writer = Arc::new(Mutex::new(
        pty.master.take_writer().map_err(io::Error::other)?,
    ));
    let master = pty.master;

    let bar: Shared = Arc::new(Mutex::new(Bar {
        state: Value::Null,
        composer: None,
        flash: None,
        rows,
        cols,
    }));
    let out: Out = Arc::new(Mutex::new(io::stdout()));

    // Collab channel: state in, drafts and submits out.
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<Value>();
    {
        let bar = Arc::clone(&bar);
        let out = Arc::clone(&out);
        let url = format!("{link}&kind=collab");
        runtime.spawn(async move {
            let Ok((ws, _)) = tokio_tungstenite::connect_async(url).await else {
                return;
            };
            let (mut tx, mut rx) = ws.split();
            loop {
                tokio::select! {
                    msg = rx.next() => {
                        let Some(Ok(Message::Text(text))) = msg else { break };
                        let Ok(value) = serde_json::from_str::<Value>(text.as_str()) else { continue };
                        {
                            let mut b = bar.lock().unwrap();
                            match value["t"].as_str() {
                                Some("state") => b.state = value,
                                Some("result") => {
                                    let mark = if value["ok"] == true { "✓" } else { "✗" };
                                    let msg = value["msg"].as_str().unwrap_or_default();
                                    b.flash = Some((format!("{mark} {msg}"), Instant::now()));
                                }
                                _ => continue,
                            }
                        }
                        repaint(&bar, &out);
                    }
                    cmd = cmd_rx.recv() => {
                        let Some(cmd) = cmd else { break };
                        if tx.send(Message::text(cmd.to_string())).await.is_err() { break }
                    }
                }
            }
        });
    }

    crossterm::terminal::enable_raw_mode()?;

    // Client output → terminal, then the bar on top.
    let output = {
        let bar = Arc::clone(&bar);
        let out = Arc::clone(&out);
        std::thread::spawn(move || {
            let mut chunk = vec![0u8; READ_CHUNK];
            loop {
                let n = match reader.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let paint = bar.lock().unwrap().paint();
                let mut out = out.lock().unwrap();
                let _ = out.write_all(&chunk[..n]);
                let _ = out.write_all(paint.as_bytes());
                let _ = out.flush();
            }
        })
    };

    // Keyboard → client, or → composer while it is open.
    let draft_state: Arc<Mutex<(bool, Instant)>> = Arc::new(Mutex::new((false, Instant::now())));
    {
        let bar = Arc::clone(&bar);
        let out = Arc::clone(&out);
        let writer = Arc::clone(&writer);
        let cmd_tx = cmd_tx.clone();
        let draft_state = Arc::clone(&draft_state);
        std::thread::spawn(move || {
            let mut stdin = io::stdin();
            let mut buf = vec![0u8; 4096];
            let mut pending: Vec<u8> = Vec::new();
            loop {
                let n = match stdin.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let mut data = std::mem::take(&mut pending);
                data.extend_from_slice(&buf[..n]);
                let composing = bar.lock().unwrap().composer.is_some();
                if !composing {
                    match find_toggle(&data) {
                        None => {
                            let _ = writer.lock().unwrap().write_all(&data);
                            continue;
                        }
                        Some((at, len)) => {
                            let _ = writer.lock().unwrap().write_all(&data[..at]);
                            let mut b = bar.lock().unwrap();
                            if b.state["me"]["role"] == "viewer" {
                                b.flash = Some(("viewers can only watch".into(), Instant::now()));
                            } else {
                                b.composer = Some(String::new());
                            }
                            drop(b);
                            repaint(&bar, &out);
                            data.drain(..at + len);
                        }
                    }
                }
                let (keys, rest) = parse_keys(&data);
                pending = rest;
                for key in keys {
                    let mut b = bar.lock().unwrap();
                    let Some(text) = b.composer.as_mut() else {
                        break;
                    };
                    match key {
                        Key::Text(t) => text.push_str(&t),
                        Key::Backspace => {
                            text.pop();
                        }
                        Key::Enter => {
                            let text = b.composer.take().unwrap_or_default();
                            if !text.trim().is_empty() {
                                let _ = cmd_tx.send(json!({ "t": "submit", "text": text }));
                            }
                        }
                        Key::Escape | Key::Toggle => {
                            b.composer = None;
                            let _ = cmd_tx.send(json!({ "t": "draft", "text": "" }));
                        }
                    }
                    if b.composer.is_some() {
                        *draft_state.lock().unwrap() = (true, Instant::now() - DRAFT_EVERY);
                    }
                }
                repaint(&bar, &out);
            }
        });
    }

    // Resize, draft throttling, flash expiry; ends when the client exits.
    let mut last_draft = Instant::now();
    let mut last_flash = false;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if let Ok((cols, rows)) = crossterm::terminal::size() {
            let changed = {
                let mut b = bar.lock().unwrap();
                let changed = (b.cols, b.rows) != (cols, rows);
                b.cols = cols;
                b.rows = rows;
                changed
            };
            if changed && rows >= 4 {
                let _ = master.resize(PtySize {
                    rows: rows - 1,
                    cols,
                    pixel_width: 0,
                    pixel_height: 0,
                });
                repaint(&bar, &out);
            }
        }
        {
            let mut ds = draft_state.lock().unwrap();
            if ds.0 && last_draft.elapsed() >= DRAFT_EVERY {
                let text = bar.lock().unwrap().composer.clone();
                if let Some(text) = text {
                    let _ = cmd_tx.send(json!({ "t": "draft", "text": text }));
                }
                ds.0 = false;
                last_draft = Instant::now();
            }
        }
        let flashing = bar
            .lock()
            .unwrap()
            .flash
            .as_ref()
            .is_some_and(|(_, at)| at.elapsed() < FLASH_FOR);
        if last_flash && !flashing {
            repaint(&bar, &out);
        }
        last_flash = flashing;
        std::thread::sleep(POLL);
    };

    let _ = output.join();
    crossterm::terminal::disable_raw_mode()?;
    {
        let rows = bar.lock().unwrap().rows;
        let mut out = out.lock().unwrap();
        let _ = write!(out, "\x1b[{rows};1H\x1b[2K\r\n");
        let _ = out.flush();
    }
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!("client exited with {status:?}")))
    }
}

/// Position and length of the first `Ctrl+]` (legacy or kitty encoding).
fn find_toggle(data: &[u8]) -> Option<(usize, usize)> {
    const KITTY: &[u8] = b"\x1b[93;5u";
    let legacy = data.iter().position(|&b| b == 0x1d).map(|i| (i, 1));
    let kitty = data
        .windows(KITTY.len())
        .position(|w| w == KITTY)
        .map(|i| (i, KITTY.len()));
    match (legacy, kitty) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (a, b) => a.or(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(s: &str) -> Key {
        Key::Text(s.into())
    }

    #[test]
    fn legacy_keys() {
        let (keys, rest) = parse_keys("привет\x7f\r".as_bytes());
        assert_eq!(keys, vec![text("привет"), Key::Backspace, Key::Enter]);
        assert!(rest.is_empty());
        assert_eq!(parse_keys(b"\x1b").0, vec![Key::Escape]);
        assert_eq!(parse_keys(b"\x1d").0, vec![Key::Toggle]);
    }

    #[test]
    fn kitty_keys() {
        let (keys, _) = parse_keys(b"\x1b[93;5u\x1b[104u\x1b[105u\x1b[13u\x1b[27u\x1b[127u");
        assert_eq!(
            keys,
            vec![
                Key::Toggle,
                text("hi"),
                Key::Enter,
                Key::Escape,
                Key::Backspace
            ]
        );
        // Release events and ctrl combos are ignored; shifted text uses the text field.
        let (keys, _) = parse_keys(b"\x1b[97;1:3u\x1b[99;5u\x1b[97;2;65u");
        assert_eq!(keys, vec![text("A")]);
    }

    #[test]
    fn arrows_are_dropped_and_partial_sequences_kept() {
        assert_eq!(parse_keys(b"a\x1b[Ab").0, vec![text("ab")]);
        let (keys, rest) = parse_keys(b"x\x1b[93;");
        assert_eq!(keys, vec![text("x")]);
        assert_eq!(rest, b"\x1b[93;");
        let bytes = "й".as_bytes();
        let (keys, rest) = parse_keys(&bytes[..1]);
        assert!(keys.is_empty());
        assert_eq!(rest, &bytes[..1]);
    }

    #[test]
    fn toggle_found_in_both_encodings() {
        assert_eq!(find_toggle(b"ab\x1dcd"), Some((2, 1)));
        assert_eq!(find_toggle(b"a\x1b[93;5u"), Some((1, 7)));
        assert_eq!(find_toggle(b"abc"), None);
    }

    #[test]
    fn bar_line_shows_role_queue_and_floor() {
        let bar = Bar {
            state: json!({
                "me": { "name": "kolya", "role": "participant", "driving": false },
                "people": [ { "online": true }, { "online": true }, { "online": false } ],
                "agent": { "status": "working" },
                "queue": [ { "pending": true }, { "pending": false } ],
                "driver": "asel",
            }),
            composer: None,
            flash: None,
            rows: 40,
            cols: 200,
        };
        let line = bar.line();
        assert!(line.contains("kolya (participant)"));
        assert!(line.contains("2 online"));
        assert!(line.contains("agent: working"));
        assert!(line.contains("queue 1"));
        assert!(line.contains("🚗 asel"));
        assert!(bar.paint().starts_with("\x1b7\x1b[40;1H"));
    }
}
