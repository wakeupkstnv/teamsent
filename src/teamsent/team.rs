//! `teamsent team`: the host's Team window.
//!
//! Runs in a herdr pane next to the agent (opened by `teamsent host`). Shows
//! who is online, what each guest is typing right now, the suggestion queue,
//! the agent's status and who holds the floor. The host acts on it with single
//! keys; guests see the same pane but cannot type into it.

use std::io::{self, BufRead, BufReader, Write};
use std::sync::mpsc;
use std::time::Duration;

use interprocess::TryClone as _;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::crossterm::{execute, terminal};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Terminal;
use serde_json::{json, Value};

use super::host_control_socket_path;
use super::queue::one_line;

const HELP: &str = "tab people/queue · ↑↓ select · y send · e edit · n reject · d give floor · t take floor · q quit";

pub(crate) fn run(args: &[String]) -> io::Result<()> {
    match args {
        [] => tui(),
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
            "usage: teamsent team [--once | --send JSON]",
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
            "queue #{} {} [{}] votes={} {}",
            q["id"],
            q["author"].as_str().unwrap_or(""),
            q["status"].as_str().unwrap_or(""),
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
    /// Inline edit of a suggestion: (id, text).
    editing: Option<(u64, String)>,
    status: String,
}

impl App {
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

    fn selected_suggestion(&self) -> Option<&Value> {
        self.queue
            .selected()
            .and_then(|i| self.queue_items().get(i).copied())
    }

    fn selected_guest(&self) -> Option<&Value> {
        self.people
            .selected()
            .and_then(|i| self.guests().get(i).copied())
    }
}

fn tui() -> io::Result<()> {
    let mut stdout = io::stdout();
    terminal::enable_raw_mode()?;
    execute!(stdout, terminal::EnterAlternateScreen)?;
    let mut term = Terminal::new(CrosstermBackend::new(stdout))?;
    let result = tui_loop(&mut term);
    terminal::disable_raw_mode()?;
    execute!(term.backend_mut(), terminal::LeaveAlternateScreen)?;
    result
}

fn tui_loop(term: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    let mut app = App {
        state: json!({}),
        focus: Focus::Queue,
        people: ListState::default(),
        queue: ListState::default(),
        editing: None,
        status: "connecting to teamsent host…".into(),
    };
    let mut conn: Option<(mpsc::Receiver<Value>, Writer)> = None;

    loop {
        if conn.is_none() {
            match connect() {
                Ok(c) => {
                    conn = Some(c);
                    app.status = "connected".into();
                }
                Err(err) => app.status = err.to_string(),
            }
        }
        if let Some((rx, _)) = &conn {
            loop {
                match rx.try_recv() {
                    Ok(msg) if msg["t"] == "state" => app.state = msg,
                    Ok(msg) => app.status = msg["msg"].as_str().unwrap_or_default().to_string(),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        conn = None;
                        app.status = "host stopped; reconnecting…".into();
                        break;
                    }
                }
            }
        }
        let (queue_len, people_len) = (app.queue_items().len(), app.guests().len());
        clamp(&mut app.queue, queue_len);
        clamp(&mut app.people, people_len);

        term.draw(|f| draw(f, &mut app))?;

        if !event::poll(Duration::from_millis(150))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        let send = |conn: &mut Option<(mpsc::Receiver<Value>, Writer)>, cmd: Value| {
            if let Some((_, w)) = conn {
                let _ = writeln!(w, "{cmd}");
            }
        };

        if let Some((id, text)) = &mut app.editing {
            match key.code {
                KeyCode::Enter => {
                    let cmd = json!({ "cmd": "edit", "id": *id, "text": text.clone() });
                    send(&mut conn, cmd);
                    app.editing = None;
                }
                KeyCode::Esc => app.editing = None,
                KeyCode::Backspace => {
                    text.pop();
                }
                KeyCode::Char(c) => text.push(c),
                _ => {}
            }
            continue;
        }

        match key.code {
            KeyCode::Char('q') => return Ok(()),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(()),
            KeyCode::Tab => {
                app.focus = if app.focus == Focus::Queue {
                    Focus::People
                } else {
                    Focus::Queue
                }
            }
            KeyCode::Up | KeyCode::Char('k') => step(&mut app, -1),
            KeyCode::Down | KeyCode::Char('j') => step(&mut app, 1),
            KeyCode::Char('y') => {
                if let Some(q) = app.selected_suggestion() {
                    send(&mut conn, json!({ "cmd": "approve", "id": q["id"] }));
                }
            }
            KeyCode::Char('n') => {
                if let Some(q) = app.selected_suggestion() {
                    send(&mut conn, json!({ "cmd": "reject", "id": q["id"] }));
                }
            }
            KeyCode::Char('e') => {
                if let Some(q) = app.selected_suggestion() {
                    if q["pending"] == true {
                        app.editing = Some((
                            q["id"].as_u64().unwrap_or_default(),
                            q["text"].as_str().unwrap_or_default().to_string(),
                        ));
                    }
                }
            }
            KeyCode::Char('d') => {
                if let Some(p) = app.selected_guest() {
                    send(&mut conn, json!({ "cmd": "drive", "guest": p["id"] }));
                }
            }
            KeyCode::Char('t') => send(&mut conn, json!({ "cmd": "take" })),
            _ => {}
        }
    }
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

fn draw(f: &mut ratatui::Frame, app: &mut App) {
    let s = &app.state;
    let guests = app.guests().len();
    let [header, people_area, queue_area, editor, footer] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(guests.clamp(1, 8) as u16 + 2),
        Constraint::Min(4),
        Constraint::Length(if app.editing.is_some() { 3 } else { 0 }),
        Constraint::Length(3),
    ])
    .areas(f.area());

    let agent = format!(
        "agent {} · {}",
        s["agent"]["pane"].as_str().unwrap_or("?"),
        s["agent"]["status"].as_str().unwrap_or("?")
    );
    let floor = match s["driver"].as_str() {
        Some(name) => format!("🚗 {name} is driving"),
        None => "floor free".into(),
    };
    f.render_widget(
        Paragraph::new(vec![
            Line::from(vec!["teamsent · Team".bold()]),
            Line::from(format!("{agent} · {floor}")).dim(),
        ]),
        header,
    );

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
                Span::raw(format!("  {}", p["role"].as_str().unwrap_or(""))),
            ];
            if p["driving"] == true {
                spans.push(Span::raw("  🚗"));
            }
            let draft = p["draft"].as_str().unwrap_or("");
            if !draft.is_empty() {
                spans.push(Span::styled(
                    format!("  ✎ {}", one_line(draft, 60)),
                    Style::new().italic().dim(),
                ));
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

    let queue: Vec<ListItem> = app
        .queue_items()
        .iter()
        .map(|q| {
            let votes = q["votes"].as_u64().unwrap_or(0);
            let head = format!(
                "#{} {}{} [{}]",
                q["id"],
                q["author"].as_str().unwrap_or(""),
                if votes > 0 {
                    format!(" +{votes}")
                } else {
                    String::new()
                },
                q["status"].as_str().unwrap_or("")
            );
            let style = if q["pending"] == true {
                Style::new()
            } else {
                Style::new().dim()
            };
            ListItem::new(vec![
                Line::styled(head, style.bold()),
                Line::styled(
                    format!("  {}", one_line(q["text"].as_str().unwrap_or(""), 200)),
                    style,
                ),
            ])
        })
        .collect();
    let queue_block = Block::bordered().title(if app.focus == Focus::Queue {
        " suggestions ◂ "
    } else {
        " suggestions "
    });
    let queue = if queue.is_empty() {
        List::new(vec![ListItem::new("  no suggestions yet")]).block(queue_block)
    } else {
        List::new(queue)
            .block(queue_block)
            .highlight_style(Style::new().add_modifier(Modifier::REVERSED))
    };
    f.render_stateful_widget(queue, queue_area, &mut app.queue);

    if let Some((id, text)) = &app.editing {
        f.render_widget(
            Paragraph::new(format!("{text}▏"))
                .wrap(Wrap { trim: false })
                .block(Block::bordered().title(format!(" edit #{id} · Enter save · Esc cancel "))),
            editor,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_view_lists_drafts_queue_and_floor() {
        let state = json!({
            "agent": { "pane": "w1:p1", "status": "working" },
            "driver": "asel",
            "people": [
                { "id": 0, "name": "host", "role": "host", "online": true, "draft": "", "driving": false },
                { "id": 2, "name": "kolya", "role": "participant", "online": true, "draft": "add te", "driving": false },
            ],
            "queue": [ { "id": 3, "author": "kolya#2", "status": "pending", "votes": 1, "text": "add tests" } ],
        });
        let lines = plain(&state);
        assert_eq!(lines[0], "agent w1:p1: working");
        assert_eq!(lines[1], "floor: asel");
        assert!(lines
            .iter()
            .any(|l| l.contains("kolya") && l.contains("typing: add te")));
        assert!(lines
            .iter()
            .any(|l| l.starts_with("queue #3") && l.contains("add tests")));
    }
}
