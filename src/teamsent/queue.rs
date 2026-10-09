//! Suggestion queue (F7): guests propose prompts, the host approves, and a
//! dispatcher delivers approved prompts one at a time.
//!
//! Delivery is agent-agnostic. It relies on herdr's agent detection: if the
//! target pane runs a detected agent (Claude Code, Codex, OpenCode, …), the
//! dispatcher waits until the agent is idle, never types over a pending
//! permission question, and submits with `agent prompt --wait`, so the next
//! suggestion goes out only after the previous turn finished. A pane without a
//! detected agent gets the text as a plain command line.

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::{mpsc, Arc, Mutex};

use serde_json::json;

use super::audit::Audit;

/// Longest accepted suggestion.
pub(crate) const MAX_SUGGESTION_BYTES: usize = 4000;
/// Finished suggestions shown by `/q`.
const RECENT_DONE: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Status {
    Pending,
    Approved { target: String },
    Sending { target: String },
    Sent { target: String },
    Failed(String),
    Rejected,
}

impl Status {
    fn is_open(&self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Approved { .. } | Self::Sending { .. }
        )
    }

    fn label(&self) -> String {
        match self {
            Self::Pending => "pending".into(),
            Self::Approved { target } => format!("approved → {target}, waiting for agent"),
            Self::Sending { target } => format!("sending → {target}"),
            Self::Sent { target } => format!("sent → {target}"),
            Self::Failed(err) => format!("failed: {err}"),
            Self::Rejected => "rejected".into(),
        }
    }
}

pub(crate) struct Suggestion {
    pub(crate) author: String,
    pub(crate) text: String,
    pub(crate) status: Status,
}

#[derive(Default)]
pub(crate) struct Queue {
    items: BTreeMap<u32, Suggestion>,
    next: u32,
}

pub(crate) type SharedQueue = Arc<Mutex<Queue>>;

impl Queue {
    pub(crate) fn push(&mut self, author: &str, text: &str) -> u32 {
        self.next += 1;
        self.items.insert(
            self.next,
            Suggestion {
                author: author.to_string(),
                text: text.to_string(),
                status: Status::Pending,
            },
        );
        self.next
    }

    /// Marks a pending suggestion approved and returns its text.
    pub(crate) fn approve(&mut self, id: u32, target: &str) -> Result<String, String> {
        let item = self
            .items
            .get_mut(&id)
            .ok_or(format!("no suggestion #{id}"))?;
        if item.status != Status::Pending {
            return Err(format!("#{id} is already {}", item.status.label()));
        }
        item.status = Status::Approved {
            target: target.to_string(),
        };
        Ok(item.text.clone())
    }

    pub(crate) fn reject(&mut self, id: u32) -> Result<(), String> {
        let item = self
            .items
            .get_mut(&id)
            .ok_or(format!("no suggestion #{id}"))?;
        if item.status != Status::Pending {
            return Err(format!("#{id} is already {}", item.status.label()));
        }
        item.status = Status::Rejected;
        Ok(())
    }

    pub(crate) fn set_status(&mut self, id: u32, status: Status) {
        if let Some(item) = self.items.get_mut(&id) {
            item.status = status;
        }
    }

    /// Open suggestions plus the last few finished ones, for `/q`.
    pub(crate) fn lines(&self) -> Vec<String> {
        let done: Vec<u32> = self
            .items
            .iter()
            .filter(|(_, s)| !s.status.is_open())
            .map(|(id, _)| *id)
            .collect();
        let recent = &done[done.len().saturating_sub(RECENT_DONE)..];
        let mut out: Vec<String> = self
            .items
            .iter()
            .filter(|(id, s)| s.status.is_open() || recent.contains(id))
            .map(|(id, s)| {
                format!(
                    "  #{id:<3} {:<12} [{}] {}",
                    s.author,
                    s.status.label(),
                    one_line(&s.text, 60)
                )
            })
            .collect();
        if out.is_empty() {
            out.push("  (queue is empty)".into());
        }
        out
    }
}

/// First line of `text`, cut to `max` chars, for compact listings.
pub(crate) fn one_line(text: &str, max: usize) -> String {
    let first = text.lines().next().unwrap_or("");
    let mut out: String = first.chars().take(max).collect();
    if first.chars().count() > max || text.lines().nth(1).is_some() {
        out.push('…');
    }
    out
}

pub(crate) struct Job {
    pub(crate) id: u32,
    pub(crate) target: String,
    pub(crate) text: String,
}

/// Starts the delivery thread. Jobs are delivered strictly in approval order.
pub(crate) fn spawn_dispatcher(queue: SharedQueue, audit: Arc<Audit>) -> mpsc::Sender<Job> {
    let (tx, rx) = mpsc::channel::<Job>();
    std::thread::spawn(move || {
        for job in rx {
            let result = deliver(&job, &queue);
            let status = match &result {
                Ok(()) => Status::Sent {
                    target: job.target.clone(),
                },
                Err(err) => Status::Failed(err.clone()),
            };
            queue.lock().unwrap().set_status(job.id, status);
            match result {
                Ok(()) => {
                    eprintln!("#{} delivered to {}", job.id, job.target);
                    audit.record(
                        "suggestion_sent",
                        json!({ "id": job.id, "target": job.target }),
                    );
                }
                Err(err) => {
                    eprintln!("#{} not delivered: {err}", job.id);
                    audit.record(
                        "suggestion_failed",
                        json!({ "id": job.id, "target": job.target, "error": err }),
                    );
                }
            }
        }
    });
    tx
}

fn deliver(job: &Job, queue: &SharedQueue) -> Result<(), String> {
    let mut announced_block = false;
    loop {
        let status = pane_agent_status(&job.target)?;
        match status.as_str() {
            "unknown" => {
                mark_sending(queue, job);
                return cli(&["pane", "run", &job.target, &job.text]).map(|_| ());
            }
            "working" => {
                cli(&[
                    "agent",
                    "wait",
                    &job.target,
                    "--until",
                    "idle",
                    "--until",
                    "done",
                    "--until",
                    "blocked",
                ])?;
            }
            "blocked" => {
                if !announced_block {
                    eprintln!(
                        "#{}: agent in {} is waiting for your answer; delivery held",
                        job.id, job.target
                    );
                    announced_block = true;
                }
                cli(&[
                    "agent",
                    "wait",
                    &job.target,
                    "--until",
                    "idle",
                    "--until",
                    "done",
                ])?;
            }
            _ => {
                mark_sending(queue, job);
                match cli(&["agent", "prompt", &job.target, &job.text, "--wait"]) {
                    Ok(_) => return Ok(()),
                    // The agent asked something between our check and the submit.
                    Err(err) if err.contains("agent_blocked") => continue,
                    Err(err) => return Err(err),
                }
            }
        }
    }
}

fn mark_sending(queue: &SharedQueue, job: &Job) {
    queue.lock().unwrap().set_status(
        job.id,
        Status::Sending {
            target: job.target.clone(),
        },
    );
}

/// Runs this binary's own CLI against the shared herdr server.
fn cli(args: &[&str]) -> Result<String, String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let out = Command::new(exe)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    if out.status.success() {
        Ok(stdout)
    } else {
        let stderr = String::from_utf8_lossy(&out.stderr);
        Err(format!("{}{}", stdout.trim(), stderr.trim()))
    }
}

fn pane_list() -> Result<Vec<serde_json::Value>, String> {
    let out = cli(&["pane", "list"])?;
    let value: serde_json::Value = serde_json::from_str(&out).map_err(|e| e.to_string())?;
    Ok(value["result"]["panes"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

fn pane_agent_status(target: &str) -> Result<String, String> {
    pane_list()?
        .iter()
        .find(|p| p["pane_id"] == target)
        .map(|p| p["agent_status"].as_str().unwrap_or("unknown").to_string())
        .ok_or(format!("pane {target} not found"))
}

/// Default delivery target: the host's focused pane.
pub(crate) fn focused_pane() -> Result<String, String> {
    pane_list()?
        .iter()
        .find(|p| p["focused"] == true)
        .and_then(|p| p["pane_id"].as_str().map(str::to_string))
        .ok_or("no focused pane".into())
}

/// Pops a herdr notification in every attached client.
pub(crate) fn notify(title: &str, body: &str) {
    let _ = cli(&[
        "notification",
        "show",
        title,
        "--body",
        body,
        "--sound",
        "request",
    ]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approve_and_reject_only_pending() {
        let mut q = Queue::default();
        let a = q.push("kolya", "add tests");
        let b = q.push("asel", "rename module");
        assert_eq!(q.approve(a, "w1:p1"), Ok("add tests".into()));
        assert!(q.approve(a, "w1:p1").is_err());
        assert!(q.reject(a).is_err());
        assert_eq!(q.reject(b), Ok(()));
        assert!(q.approve(b, "w1:p1").is_err());
        assert!(q.approve(99, "w1:p1").is_err());
    }

    #[test]
    fn listing_keeps_open_and_recent_items() {
        let mut q = Queue::default();
        for i in 0..8 {
            let id = q.push("kolya", &format!("idea {i}"));
            q.reject(id).unwrap();
        }
        let open = q.push("kolya", "still open");
        let lines = q.lines();
        assert_eq!(lines.len(), RECENT_DONE + 1);
        assert!(lines.last().unwrap().contains(&format!("#{open}")));
    }

    #[test]
    fn one_line_truncates_and_marks_multiline() {
        assert_eq!(one_line("short", 10), "short");
        assert_eq!(one_line("abcdefghijkl", 5), "abcde…");
        assert_eq!(one_line("first\nsecond", 10), "first…");
    }
}
