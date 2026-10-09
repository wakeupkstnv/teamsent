//! `teamsent team`: the host's Team window.
//!
//! Runs in a herdr pane next to the agent (opened by `teamsent host`). Works
//! like a shared prompt queue: everyone proposes prompts, the agent takes them
//! one at a time when it is free. The window shows what each guest is typing
//! right now, the queue in delivery order, the mode (AUTO: guests' prompts
//! queue directly; REVIEW: the host accepts each one), the agent's status and
//! who holds the floor. The host can also queue its own prompts (`i`).
//! Guests see the same pane but cannot type into it.

use std::io::{self, BufRead, BufReader, Write};
use std::sync::mpsc;
use std::time::Duration;

use interprocess::TryClone as _;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
    MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::crossterm::{execute, terminal};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Terminal;
use serde_json::{json, Value};

use super::host_control_socket_path;
use super::queue::one_line;

const HELP: &str = "i prompt · y accept · n remove · e edit · K/J move · a auto/review · p pause · c clear · tab people · right-click/m person menu · x kick · q quit";

/// What the person menu can do.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Act {
    Kick,
    Drive,
    Role(&'static str),
    Cancel,
}

const MENU: &[(&str, Act)] = &[
    ("Kick…", Act::Kick),
    ("Give floor", Act::Drive),
    ("Make viewer", Act::Role("viewer")),
    ("Make participant", Act::Role("participant")),
    ("Make writer", Act::Role("writer")),
    ("Cancel", Act::Cancel),
];
const MENU_WIDTH: u16 = 22;

/// Context menu for one guest, anchored where it was opened.
struct Menu {
    guest: u64,
    name: String,
    at: Rect,
    selected: usize,
}

/// A yes/no dialog for destructive actions. "Cancel" is the default button.
struct Confirm {
    title: &'static str,
    question: String,
    detail: &'static str,
    ok_label: &'static str,
    cmd: Value,
    ok_selected: bool,
    ok_button: Rect,
    cancel_button: Rect,
}

impl Confirm {
    fn new(
        title: &'static str,
        question: String,
        detail: &'static str,
        ok_label: &'static str,
        cmd: Value,
    ) -> Self {
        Self {
            title,
            question,
            detail,
            ok_label,
            cmd,
            ok_selected: false,
            ok_button: Rect::default(),
            cancel_button: Rect::default(),
        }
    }
}

pub(crate) fn run(args: &[String]) -> io::Result<()> {
    match args {
        [] => tui(None),
        [flag, instance] if flag == "--instance" => tui(Some(instance.clone())),
        [flag] if flag == "--once" => {
            let (rx, _) = connect()?;
            let state = recv_state(&rx, Duration::from_secs(5))
                .ok_or_else(|| io::Error::other("no state from host"))?;
            for line in plain(&state) {
                println!("{line}");
            }
            Ok(())
        }
        [flag, json_cmd] if flag == "--send" => {
            let (rx, mut tx) = connect()?;
            writeln!(tx, "{json_cmd}")?;
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while let Some(left) = deadline.checked_duration_since(std::time::Instant::now()) {
                match rx.recv_timeout(left) {
                    Ok(msg) if msg["t"] == "result" => {
                        println!("{}", msg["msg"].as_str().unwrap_or_default());
                        return if msg["ok"] == true {
                            Ok(())
                        } else {
                            Err(io::Error::other("command failed"))
                        };
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            Err(io::Error::other("no reply from host"))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: teamsent team [--once | --send JSON | --instance ID]",
        )),
    }
}

type Writer = crate::ipc::LocalStream;

/// Connects to the running host; returns incoming messages and the command writer.
fn connect() -> io::Result<(mpsc::Receiver<Value>, Writer)> {
    let stream = crate::ipc::connect_local_stream(&host_control_socket_path()).map_err(|err| {
        io::Error::new(err.kind(), format!("teamsent host is not running: {err}"))
    })?;
    let reader = stream.try_clone()?;
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(reader).lines() {
            let Ok(line) = line else { break };
            if let Ok(value) = serde_json::from_str::<Value>(&line) {
                if tx.send(value).is_err() {
                    break;
                }
            }
        }
    });
    Ok((rx, stream))
}

fn recv_state(rx: &mpsc::Receiver<Value>, timeout: Duration) -> Option<Value> {
    let deadline = std::time::Instant::now() + timeout;
    while let Some(left) = deadline.checked_duration_since(std::time::Instant::now()) {
        match rx.recv_timeout(left) {
            Ok(msg) if msg["t"] == "state" => return Some(msg),
            Ok(_) => {}
            Err(_) => return None,
        }
    }
    None
}

/// Plain-text view of the state (for `--once` and tests).
fn plain(state: &Value) -> Vec<String> {
    let mut out = vec![format!(
        "agent {}: {}",
        state["agent"]["pane"].as_str().unwrap_or("?"),
        state["agent"]["status"].as_str().unwrap_or("?")
    )];
    out.push(format!(
        "floor: {}",
        state["driver"].as_str().unwrap_or("free")
    ));
    out.push(format!(
        "mode: {}{}",
        state["mode"].as_str().unwrap_or("?"),
        if state["paused"] == true {
            " paused"
        } else {
            ""
        }
    ));
    for p in state["people"].as_array().into_iter().flatten() {
        let draft = p["draft"].as_str().unwrap_or("");
        out.push(format!(
            "person #{} {} {} {}{}{}",
            p["id"],
            p["name"].as_str().unwrap_or(""),
            p["role"].as_str().unwrap_or(""),
            if p["online"] == true {
                "online"
            } else {
                "offline"
            },
            if p["driving"] == true { " driving" } else { "" },
            if draft.is_empty() {
                String::new()
            } else {
                format!(" typing: {draft}")
            },
        ));
    }
    for q in state["queue"].as_array().into_iter().flatten() {
        out.push(format!(
            "queue #{} {} [{}] pos={} votes={} {}",
            q["id"],
            q["author"].as_str().unwrap_or(""),
            q["state"].as_str().unwrap_or(""),
            q["position"],
            q["votes"],
            q["text"].as_str().unwrap_or("")
        ));
    }
    out
}

#[derive(PartialEq)]
enum Focus {
    People,
    Queue,
}

struct App {
    state: Value,
    focus: Focus,
    people: ListState,
    queue: ListState,
    /// Inline edit of a prompt: (id, text).
    editing: Option<(u64, String)>,
    /// The host's own prompt being typed.
    composing: Option<String>,
    status: String,
    /// Where the people list was drawn, for mouse hit-testing.
    people_area: Rect,
    menu: Option<Menu>,
    confirm: Option<Confirm>,
}

impl App {
    fn new() -> Self {
        Self {
            state: json!({}),
            focus: Focus::Queue,
            people: ListState::default(),
            queue: ListState::default(),
            editing: None,
            composing: None,
            status: "connecting to teamsent host…".into(),
            people_area: Rect::default(),
            menu: None,
            confirm: None,
        }
    }

    fn queue_items(&self) -> Vec<&Value> {
        self.state["queue"]
            .as_array()
            .into_iter()
            .flatten()
            .collect()
    }

    fn guests(&self) -> Vec<&Value> {
        self.state["people"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|p| p["id"] != 0)
            .collect()
    }

    fn selected_prompt(&self) -> Option<&Value> {
        self.queue
            .selected()
            .and_then(|i| self.queue_items().get(i).copied())
    }

    fn selected_guest(&self) -> Option<&Value> {
        self.people
            .selected()
            .and_then(|i| self.guests().get(i).copied())
    }

    /// Index of the guest row under the mouse, if any.
    fn guest_at(&self, column: u16, row: u16) -> Option<usize> {
        let a = self.people_area;
        let inside = column > a.x
            && column < a.x + a.width.saturating_sub(1)
            && row > a.y
            && row < a.y + a.height.saturating_sub(1);
        if !inside {
            return None;
        }
        let index = self.people.offset() + (row - a.y - 1) as usize;
        (index < self.guests().len()).then_some(index)
    }

    fn open_menu(&mut self, index: usize, column: u16, row: u16) {
        let Some(guest) = self.guests().get(index).copied() else {
            return;
        };
        let (id, name) = (
            guest["id"].as_u64().unwrap_or_default(),
            guest["name"].as_str().unwrap_or_default().to_string(),
        );
        self.people.select(Some(index));
        self.focus = Focus::People;
        self.menu = Some(Menu {
            guest: id,
            name,
            at: Rect::new(column, row, MENU_WIDTH, MENU.len() as u16 + 2),
            selected: 0,
        });
    }

    fn open_kick(&mut self, guest: u64, name: String) {
        self.menu = None;
        self.confirm = Some(Confirm::new(
            " kick ",
            format!("Kick {name}?"),
            "They are disconnected and their invite link stops working.",
            " Kick ",
            json!({ "cmd": "kick", "guest": guest }),
        ));
    }

    fn open_clear(&mut self) {
        let waiting = self
            .queue_items()
            .iter()
            .filter(|q| q["waiting"] == true)
            .count();
        if waiting == 0 {
            self.status = "queue is already empty".into();
            return;
        }
        self.confirm = Some(Confirm::new(
            " clear queue ",
            format!("Remove all {waiting} waiting prompts?"),
            "The prompt that is already with the agent is not affected.",
            " Clear ",
            json!({ "cmd": "clear" }),
        ));
    }

    /// Runs a menu action; returns the command to send, if any.
    fn act(&mut self, act: Act) -> Option<Value> {
        let menu = self.menu.take()?;
        match act {
            Act::Kick => {
                self.open_kick(menu.guest, menu.name);
                None
            }
            Act::Drive => Some(json!({ "cmd": "drive", "guest": menu.guest })),
            Act::Role(role) => Some(json!({ "cmd": "role", "guest": menu.guest, "role": role })),
            Act::Cancel => None,
        }
    }
}

/// True when the host runs a different Team window than this one.
fn superseded(instance: Option<&str>, state: &Value) -> bool {
    match (instance, state["team_instance"].as_str()) {
        (Some(mine), Some(current)) => mine != current,
        _ => false,
    }
}

fn contains(r: Rect, column: u16, row: u16) -> bool {
    column >= r.x && column < r.x + r.width && row >= r.y && row < r.y + r.height
}

/// `instance`: set when `teamsent host` opened this window. The window exits
/// (closing its pane) once the host reports a different instance, so windows
/// from earlier runs never pile up.
fn tui(instance: Option<String>) -> io::Result<()> {
    let mut stdout = io::stdout();
    terminal::enable_raw_mode()?;
    execute!(stdout, terminal::EnterAlternateScreen, EnableMouseCapture)?;
    let mut term = Terminal::new(CrosstermBackend::new(stdout))?;
    let result = tui_loop(&mut term, instance.as_deref());
    terminal::disable_raw_mode()?;
    execute!(
        term.backend_mut(),
        DisableMouseCapture,
        terminal::LeaveAlternateScreen
    )?;
    result
}

type Conn = Option<(mpsc::Receiver<Value>, Writer)>;

fn send(conn: &mut Conn, cmd: Value) {
    if let Some((_, w)) = conn {
        let _ = writeln!(w, "{cmd}");
    }
}

fn tui_loop(
    term: &mut Terminal<CrosstermBackend<io::Stdout>>,
    instance: Option<&str>,
) -> io::Result<()> {
    let mut app = App::new();
    let mut conn: Conn = None;
    let mut dirty = true;

    loop {
        if conn.is_none() {
            match connect() {
                Ok(c) => {
                    conn = Some(c);
                    app.status = "connected".into();
                }
                Err(err) => app.status = err.to_string(),
            }
            dirty = true;
        }
        if let Some((rx, _)) = &conn {
            loop {
                match rx.try_recv() {
                    Ok(msg) if msg["t"] == "state" => {
                        if superseded(instance, &msg) {
                            return Ok(());
                        }
                        app.state = msg;
                    }
                    Ok(msg) => app.status = msg["msg"].as_str().unwrap_or_default().to_string(),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        conn = None;
                        app.status = "host stopped; reconnecting…".into();
                        break;
                    }
                }
                dirty = true;
            }
        }
        let (queue_len, people_len) = (app.queue_items().len(), app.guests().len());
        clamp(&mut app.queue, queue_len);
        clamp(&mut app.people, people_len);

        // Redraw only when something changed: keeps the pane cheap to share.
        if dirty {
            term.draw(|f| draw(f, &mut app))?;
            dirty = false;
        }

        if !event::poll(Duration::from_millis(100))? {
            continue;
        }
        dirty = true;
        let key = match event::read()? {
            Event::Key(key) if key.kind == KeyEventKind::Press => key,
            Event::Mouse(mouse) => {
                if let Some(cmd) = on_mouse(&mut app, mouse) {
                    send(&mut conn, cmd);
                }
                continue;
            }
            _ => continue,
        };
        if handle_key(&mut app, &mut conn, key.code, key.modifiers) {
            return Ok(());
        }
    }
}

/// Applies one key press. Returns true to quit.
fn handle_key(app: &mut App, conn: &mut Conn, code: KeyCode, mods: KeyModifiers) -> bool {
    if code == KeyCode::Char('c') && mods.contains(KeyModifiers::CONTROL) {
        return true;
    }

    if let Some(confirm) = &mut app.confirm {
        match code {
            KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
                confirm.ok_selected = !confirm.ok_selected
            }
            KeyCode::Char('y') => {
                let cmd = confirm.cmd.clone();
                app.confirm = None;
                send(conn, cmd);
            }
            KeyCode::Enter => {
                let (cmd, ok) = (confirm.cmd.clone(), confirm.ok_selected);
                app.confirm = None;
                if ok {
                    send(conn, cmd);
                }
            }
            KeyCode::Char('n') | KeyCode::Esc => app.confirm = None,
            _ => {}
        }
        return false;
    }

    if let Some(menu) = &mut app.menu {
        match code {
            KeyCode::Up | KeyCode::Char('k') => menu.selected = menu.selected.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => {
                menu.selected = (menu.selected + 1).min(MENU.len() - 1)
            }
            KeyCode::Enter => {
                let act = MENU[menu.selected].1;
                if let Some(cmd) = app.act(act) {
                    send(conn, cmd);
                }
            }
            KeyCode::Esc | KeyCode::Char('q') => app.menu = None,
            _ => {}
        }
        return false;
    }

    // Text input: editing a prompt or composing the host's own.
    let input = if app.editing.is_some() {
        app.editing.as_mut().map(|(_, t)| t)
    } else {
        app.composing.as_mut()
    };
    if let Some(text) = input {
        match code {
            KeyCode::Enter => {
                let text = text.clone();
                if let Some((id, _)) = app.editing.take() {
                    send(conn, json!({ "cmd": "edit", "id": id, "text": text }));
                } else if app.composing.take().is_some() && !text.trim().is_empty() {
                    send(conn, json!({ "cmd": "prompt", "text": text }));
                }
            }
            KeyCode::Esc => {
                app.editing = None;
                app.composing = None;
            }
            KeyCode::Backspace => {
                text.pop();
            }
            KeyCode::Char(c) => text.push(c),
            _ => {}
        }
        return false;
    }

    let id = app.selected_prompt().map(|q| q["id"].clone());
    match code {
        KeyCode::Char('q') => return true,
        KeyCode::Tab => {
            app.focus = if app.focus == Focus::Queue {
                Focus::People
            } else {
                Focus::Queue
            }
        }
        KeyCode::Up | KeyCode::Char('k') => step(app, -1),
        KeyCode::Down | KeyCode::Char('j') => step(app, 1),
        KeyCode::Char('i') => app.composing = Some(String::new()),
        KeyCode::Char('y') => {
            if let Some(id) = id {
                send(conn, json!({ "cmd": "approve", "id": id }));
            }
        }
        KeyCode::Char('n') | KeyCode::Delete => {
            if let Some(id) = id {
                send(conn, json!({ "cmd": "reject", "id": id }));
            }
        }
        KeyCode::Char('e') => {
            if let Some(q) = app.selected_prompt() {
                if q["waiting"] == true {
                    app.editing = Some((
                        q["id"].as_u64().unwrap_or_default(),
                        q["text"].as_str().unwrap_or_default().to_string(),
                    ));
                }
            }
        }
        KeyCode::Char('K') => {
            if let Some(id) = id {
                send(conn, json!({ "cmd": "move", "id": id, "delta": -1 }));
                step(app, -1);
            }
        }
        KeyCode::Char('J') => {
            if let Some(id) = id {
                send(conn, json!({ "cmd": "move", "id": id, "delta": 1 }));
                step(app, 1);
            }
        }
        KeyCode::Char('a') => {
            let on = app.state["mode"] != "auto";
            send(conn, json!({ "cmd": "auto", "on": on }));
        }
        KeyCode::Char('p') => {
            let paused = app.state["paused"] != true;
            send(conn, json!({ "cmd": "pause", "paused": paused }));
        }
        KeyCode::Char('c') => app.open_clear(),
        KeyCode::Char('d') => {
            if let Some(p) = app.selected_guest() {
                send(conn, json!({ "cmd": "drive", "guest": p["id"] }));
            }
        }
        KeyCode::Char('t') => send(conn, json!({ "cmd": "take" })),
        KeyCode::Char('m') => {
            if let Some(i) = app.people.selected() {
                let a = app.people_area;
                let row = a.y + 1 + i.saturating_sub(app.people.offset()) as u16;
                app.open_menu(i, a.x + 2, row);
            }
        }
        KeyCode::Char('x') => {
            if let Some(p) = app.selected_guest() {
                let (id, name) = (
                    p["id"].as_u64().unwrap_or_default(),
                    p["name"].as_str().unwrap_or_default().to_string(),
                );
                app.open_kick(id, name);
            }
        }
        _ => {}
    }
    false
}

/// Mouse: right-click a person for the menu, click menu items and dialog buttons.
fn on_mouse(app: &mut App, mouse: MouseEvent) -> Option<Value> {
    let (column, row) = (mouse.column, mouse.row);
    let MouseEventKind::Down(button) = mouse.kind else {
        return None;
    };
    if let Some(confirm) = &app.confirm {
        let cmd = confirm.cmd.clone();
        let ok = contains(confirm.ok_button, column, row);
        let cancel = contains(confirm.cancel_button, column, row);
        if ok || cancel || button == MouseButton::Left {
            app.confirm = None;
        }
        return ok.then_some(cmd);
    }
    if let Some(menu) = &app.menu {
        if contains(menu.at, column, row) {
            let item = row.checked_sub(menu.at.y + 1).map(usize::from);
            if let Some(&(_, act)) = item.and_then(|i| MENU.get(i)) {
                return app.act(act);
            }
            return None;
        }
        app.menu = None;
    }
    let hit = app.guest_at(column, row)?;
    match button {
        MouseButton::Right => app.open_menu(hit, column, row),
        MouseButton::Left => {
            app.people.select(Some(hit));
            app.focus = Focus::People;
        }
        MouseButton::Middle => {}
    }
    None
}

fn clamp(list: &mut ListState, len: usize) {
    match (list.selected(), len) {
        (_, 0) => list.select(None),
        (None, _) => list.select(Some(0)),
        (Some(i), n) if i >= n => list.select(Some(n - 1)),
        _ => {}
    }
}

fn step(app: &mut App, delta: i32) {
    let (list, len) = match app.focus {
        Focus::Queue => {
            let len = app.queue_items().len();
            (&mut app.queue, len)
        }
        Focus::People => {
            let len = app.guests().len();
            (&mut app.people, len)
        }
    };
    if len == 0 {
        return;
    }
    let i = list.selected().unwrap_or(0) as i32 + delta;
    list.select(Some(i.clamp(0, len as i32 - 1) as usize));
}

/// Badge text and color for a prompt's state.
fn badge(state: &str) -> (&'static str, Color) {
    match state {
        "pending" => ("REVIEW", Color::Yellow),
        "queued" => ("QUEUED", Color::Cyan),
        "sending" => ("→ AGENT", Color::Green),
        "sent" => ("SENT", Color::DarkGray),
        "failed" => ("FAILED", Color::Red),
        _ => ("REMOVED", Color::DarkGray),
    }
}

fn draw(f: &mut ratatui::Frame, app: &mut App) {
    let s = &app.state;
    let guests = app.guests().len();
    let typing: Vec<(String, String)> = app
        .guests()
        .iter()
        .filter_map(|p| {
            let draft = p["draft"].as_str().unwrap_or("");
            (!draft.is_empty()).then(|| {
                (
                    p["name"].as_str().unwrap_or("").to_string(),
                    draft.to_string(),
                )
            })
        })
        .collect();
    let input_open = app.editing.is_some() || app.composing.is_some();
    let [header, people_area, typing_area, queue_area, input_area, footer] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(guests.clamp(1, 6) as u16 + 2),
        Constraint::Length(if typing.is_empty() {
            0
        } else {
            typing.len().min(4) as u16 + 2
        }),
        Constraint::Min(4),
        Constraint::Length(if input_open { 3 } else { 0 }),
        Constraint::Length(3),
    ])
    .areas(f.area());

    // Header: mode, delivery, agent, floor.
    let auto = s["mode"] == "auto";
    let mode = if auto {
        Span::styled(" AUTO ", Style::new().black().on_green().bold())
    } else {
        Span::styled(" REVIEW ", Style::new().black().on_yellow().bold())
    };
    let mode_hint = if auto {
        " guests' prompts queue directly"
    } else {
        " you accept each guest prompt"
    };
    let delivery = if s["paused"] == true {
        Span::styled(" ⏸ PAUSED ", Style::new().black().on_red().bold())
    } else {
        Span::raw("")
    };
    let agent = format!(
        "agent {} · {}",
        s["agent"]["pane"].as_str().unwrap_or("?"),
        s["agent"]["status"].as_str().unwrap_or("?")
    );
    let floor = match s["driver"].as_str() {
        Some(name) => format!(" · 🚗 {name} driving"),
        None => String::new(),
    };
    f.render_widget(
        Paragraph::new(vec![
            Line::from(vec!["teamsent · Team ".bold(), mode, delivery]),
            Line::from(mode_hint).dim(),
            Line::from(format!("{agent}{floor}")).dim(),
        ]),
        header,
    );

    // People.
    let people: Vec<ListItem> = app
        .guests()
        .iter()
        .map(|p| {
            let dot = if p["online"] == true { "● " } else { "○ " };
            let mut spans = vec![
                Span::raw(dot),
                Span::styled(
                    p["name"].as_str().unwrap_or("").to_string(),
                    Style::new().bold(),
                ),
                Span::raw(format!("  {}", p["role"].as_str().unwrap_or(""))).dim(),
            ];
            if p["driving"] == true {
                spans.push(Span::raw("  🚗"));
            }
            ListItem::new(Line::from(spans))
        })
        .collect();
    let people_block = Block::bordered().title(if app.focus == Focus::People {
        " people ◂ "
    } else {
        " people "
    });
    let people = if people.is_empty() {
        List::new(vec![ListItem::new("  nobody yet — share an invite link")]).block(people_block)
    } else {
        List::new(people)
            .block(people_block)
            .highlight_style(Style::new().add_modifier(Modifier::REVERSED))
    };
    f.render_stateful_widget(people, people_area, &mut app.people);
    app.people_area = people_area;

    // What people are typing right now.
    if !typing.is_empty() {
        let lines: Vec<Line> = typing
            .iter()
            .take(4)
            .map(|(name, draft)| {
                Line::from(vec![
                    Span::styled(format!("{name} › "), Style::new().bold()),
                    Span::styled(one_line(draft, 200), Style::new().italic()),
                    Span::raw("▏").dim(),
                ])
            })
            .collect();
        f.render_widget(
            Paragraph::new(lines).block(Block::bordered().title(" typing ")),
            typing_area,
        );
    }

    // Queue in delivery order.
    let queue: Vec<ListItem> = app
        .queue_items()
        .iter()
        .map(|q| {
            let state = q["state"].as_str().unwrap_or("");
            let (label, color) = badge(state);
            let place = q["position"]
                .as_u64()
                .map(|p| format!("{p}. "))
                .unwrap_or_else(|| "   ".into());
            let votes = q["votes"].as_u64().unwrap_or(0);
            let open = q["waiting"] == true || state == "sending";
            let text_style = if open {
                Style::new()
            } else {
                Style::new().dim()
            };
            ListItem::new(vec![
                Line::from(vec![
                    Span::raw(place),
                    Span::styled(format!(" {label} "), Style::new().fg(color).bold()),
                    Span::styled(
                        format!(" {}", q["author"].as_str().unwrap_or("")),
                        Style::new().bold(),
                    ),
                    Span::raw(if votes > 0 {
                        format!("  +{votes}")
                    } else {
                        String::new()
                    }),
                    Span::raw(format!("  #{}", q["id"])).dim(),
                ]),
                Line::styled(
                    format!("   {}", one_line(q["text"].as_str().unwrap_or(""), 200)),
                    text_style,
                ),
            ])
        })
        .collect();
    let waiting = app
        .queue_items()
        .iter()
        .filter(|q| q["waiting"] == true)
        .count();
    let queue_title = format!(
        " queue · {waiting} waiting {}",
        if app.focus == Focus::Queue {
            "◂ "
        } else {
            ""
        }
    );
    let queue_block = Block::bordered().title(queue_title);
    let queue = if queue.is_empty() {
        List::new(vec![ListItem::new(
            "  empty — guests press Ctrl+] in their bar, you press i",
        )])
        .block(queue_block)
    } else {
        List::new(queue)
            .block(queue_block)
            .highlight_style(Style::new().add_modifier(Modifier::REVERSED))
    };
    f.render_stateful_widget(queue, queue_area, &mut app.queue);

    // Prompt / edit input.
    let input = match (&app.editing, &app.composing) {
        (Some((id, text)), _) => Some((format!(" edit #{id} · Enter save · Esc cancel "), text)),
        (None, Some(text)) => Some((" your prompt · Enter queue · Esc cancel ".to_string(), text)),
        _ => None,
    };
    if let Some((title, text)) = input {
        f.render_widget(
            Paragraph::new(format!("{text}▏"))
                .wrap(Wrap { trim: false })
                .block(Block::bordered().title(title)),
            input_area,
        );
    }

    f.render_widget(
        Paragraph::new(vec![
            Line::from(app.status.clone()).italic(),
            Line::from(HELP).dim(),
        ])
        .wrap(Wrap { trim: true }),
        footer,
    );

    let screen = f.area();
    if let Some(menu) = &mut app.menu {
        let area = fit(menu.at, screen);
        menu.at = area;
        let items: Vec<ListItem> = MENU
            .iter()
            .enumerate()
            .map(|(i, (label, act))| {
                let style = if *act == Act::Kick {
                    Style::new().red()
                } else {
                    Style::new()
                };
                let style = if i == menu.selected {
                    style.add_modifier(Modifier::REVERSED)
                } else {
                    style
                };
                ListItem::new(Line::styled(format!(" {label}"), style))
            })
            .collect();
        f.render_widget(Clear, area);
        f.render_widget(
            List::new(items).block(Block::bordered().title(format!(" {} ", menu.name))),
            area,
        );
    }

    if let Some(confirm) = &mut app.confirm {
        let width = 46.min(screen.width);
        let area = fit(
            Rect::new(
                screen.x + screen.width.saturating_sub(width) / 2,
                screen.y + screen.height.saturating_sub(7) / 2,
                width,
                7,
            ),
            screen,
        );
        f.render_widget(Clear, area);
        let block = Block::bordered().title(confirm.title).red();
        let inner = block.inner(area);
        f.render_widget(block, area);
        f.render_widget(
            Paragraph::new(vec![
                Line::from(confirm.question.clone()).bold(),
                Line::from(confirm.detail).dim(),
            ])
            .wrap(Wrap { trim: true }),
            Rect::new(
                inner.x,
                inner.y,
                inner.width,
                inner.height.saturating_sub(1),
            ),
        );
        let row = inner.y + inner.height.saturating_sub(1);
        let cancel = " Cancel ";
        confirm.ok_button = Rect::new(inner.x + 1, row, confirm.ok_label.len() as u16, 1);
        confirm.cancel_button = Rect::new(
            inner.x + 3 + confirm.ok_label.len() as u16,
            row,
            cancel.len() as u16,
            1,
        );
        let button = |label: &str, selected: bool, danger: bool| {
            let style = if danger {
                Style::new().red()
            } else {
                Style::new()
            };
            Paragraph::new(label.to_string()).style(if selected {
                style.add_modifier(Modifier::REVERSED)
            } else {
                style
            })
        };
        f.render_widget(
            button(confirm.ok_label, confirm.ok_selected, true),
            confirm.ok_button,
        );
        f.render_widget(
            button(cancel, !confirm.ok_selected, false),
            confirm.cancel_button,
        );
    }
}

/// Moves `r` so it stays inside `screen` (shrinking it if the screen is smaller).
fn fit(r: Rect, screen: Rect) -> Rect {
    let width = r.width.min(screen.width);
    let height = r.height.min(screen.height);
    let x = r.x.min(screen.x + screen.width - width);
    let y = r.y.min(screen.y + screen.height - height);
    Rect::new(x, y, width, height)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app_with_guests() -> App {
        let mut app = App::new();
        app.state = json!({ "people": [
            { "id": 0, "name": "host" },
            { "id": 2, "name": "kolya", "role": "participant" },
            { "id": 3, "name": "asel", "role": "writer" },
        ] });
        app.people_area = Rect::new(0, 3, 40, 4);
        app
    }

    fn click(button: MouseButton, column: u16, row: u16) -> MouseEvent {
        MouseEvent {
            kind: MouseEventKind::Down(button),
            column,
            row,
            modifiers: KeyModifiers::NONE,
        }
    }

    #[test]
    fn right_click_menu_kick_needs_confirmation() {
        let mut app = app_with_guests();
        // Row 5 is the second guest (asel): border at y=3, kolya at 4, asel at 5.
        assert_eq!(on_mouse(&mut app, click(MouseButton::Right, 5, 5)), None);
        let menu = app.menu.as_ref().expect("menu opened");
        assert_eq!((menu.guest, menu.name.as_str()), (3, "asel"));
        // First item is "Kick…": it opens the dialog and sends nothing yet.
        let (x, y) = (menu.at.x + 1, menu.at.y + 1);
        assert_eq!(on_mouse(&mut app, click(MouseButton::Left, x, y)), None);
        let confirm = app.confirm.as_mut().expect("confirm dialog");
        assert!(!confirm.ok_selected, "Cancel is the default");
        confirm.ok_button = Rect::new(10, 10, 6, 1);
        assert_eq!(
            on_mouse(&mut app, click(MouseButton::Left, 11, 10)),
            Some(json!({ "cmd": "kick", "guest": 3 }))
        );
        assert!(app.confirm.is_none());
    }

    #[test]
    fn clicking_outside_closes_dialog_without_kicking() {
        let mut app = app_with_guests();
        app.open_kick(2, "kolya".into());
        assert_eq!(on_mouse(&mut app, click(MouseButton::Left, 0, 0)), None);
        assert!(app.confirm.is_none());
    }

    #[test]
    fn menu_role_items_send_role_commands() {
        let mut app = app_with_guests();
        on_mouse(&mut app, click(MouseButton::Right, 5, 4));
        let menu = app.menu.as_ref().unwrap();
        let writer_row = menu.at.y + 1 + 4; // "Make writer"
        let x = menu.at.x + 1;
        assert_eq!(
            on_mouse(&mut app, click(MouseButton::Left, x, writer_row)),
            Some(json!({ "cmd": "role", "guest": 2, "role": "writer" }))
        );
    }

    #[test]
    fn stale_windows_step_aside() {
        let state = json!({ "team_instance": "new" });
        assert!(superseded(Some("old"), &state));
        assert!(!superseded(Some("new"), &state));
        assert!(!superseded(None, &state), "a window opened by hand stays");
        assert!(!superseded(Some("old"), &json!({})));
    }

    #[test]
    fn clear_asks_first_and_enter_defaults_to_cancel() {
        let mut app = app_with_guests();
        app.state["queue"] = json!([{ "id": 1, "waiting": true }]);
        let mut conn: Conn = None;
        handle_key(&mut app, &mut conn, KeyCode::Char('c'), KeyModifiers::NONE);
        assert!(app.confirm.is_some());
        handle_key(&mut app, &mut conn, KeyCode::Enter, KeyModifiers::NONE);
        assert!(app.confirm.is_none(), "Enter on the default button cancels");
    }

    #[test]
    fn host_composer_collects_text() {
        let mut app = app_with_guests();
        let mut conn: Conn = None;
        handle_key(&mut app, &mut conn, KeyCode::Char('i'), KeyModifiers::NONE);
        for c in "run tests".chars() {
            handle_key(&mut app, &mut conn, KeyCode::Char(c), KeyModifiers::NONE);
        }
        assert_eq!(app.composing.as_deref(), Some("run tests"));
        // 'q' while typing is text, not quit.
        assert!(!handle_key(
            &mut app,
            &mut conn,
            KeyCode::Char('q'),
            KeyModifiers::NONE
        ));
        handle_key(&mut app, &mut conn, KeyCode::Enter, KeyModifiers::NONE);
        assert!(app.composing.is_none());
    }

    #[test]
    fn plain_view_lists_drafts_queue_mode_and_floor() {
        let state = json!({
            "agent": { "pane": "w1:p1", "status": "working" },
            "driver": "asel",
            "mode": "auto",
            "paused": true,
            "people": [
                { "id": 0, "name": "host", "role": "host", "online": true, "draft": "", "driving": false },
                { "id": 2, "name": "kolya", "role": "participant", "online": true, "draft": "add te", "driving": false },
            ],
            "queue": [ { "id": 3, "author": "kolya#2", "state": "queued", "position": 1, "votes": 1, "text": "add tests" } ],
        });
        let lines = plain(&state);
        assert_eq!(lines[0], "agent w1:p1: working");
        assert_eq!(lines[1], "floor: asel");
        assert_eq!(lines[2], "mode: auto paused");
        assert!(lines
            .iter()
            .any(|l| l.contains("kolya") && l.contains("typing: add te")));
        assert!(lines
            .iter()
            .any(|l| l.starts_with("queue #3") && l.contains("[queued] pos=1")));
    }
}
