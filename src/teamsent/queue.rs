//! Prompt queue (F7): everyone proposes prompts for the agent; a dispatcher
//! delivers them one at a time, in queue order, when the agent is free.
//!
//! Two modes, chosen by the host (`host::Ctx::auto`):
//! - **auto**: a submitted prompt goes straight into the queue (like coshell);
//! - **review**: it waits as `Pending` until the host approves it.
//!
//! The host can reorder, edit, remove, clear and pause the queue.
//!
//! Delivery is agent-agnostic. It relies on herdr's agent detection: if the
//! target pane runs a detected agent (Claude Code, Codex, OpenCode, …), the
//! dispatcher waits until the agent is idle, never types over a pending
//! permission question, and submits with `agent prompt --wait`, so the next
//! prompt goes out only after the previous turn finished. A pane without a
//! detected agent gets the text as a plain command line.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::audit::Audit;

/// Longest accepted prompt.
pub(crate) const MAX_SUGGESTION_BYTES: usize = 4000;
/// Finished prompts shown in listings.
const RECENT_DONE: usize = 5;
/// How often the dispatcher looks for the next prompt.
const DISPATCH_POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Status {
    /// Waiting for the host's approval (review mode).
    Pending,
    /// Accepted; goes to the agent when it is its turn and the agent is free.
    Queued,
    Sending {
        target: String,
    },
    Sent {
        target: String,
    },
    Failed(String),
    Rejected,
}

impl Status {
    fn is_open(&self) -> bool {
        matches!(self, Self::Pending | Self::Queued | Self::Sending { .. })
    }

    /// Short machine-readable state for UIs.
    fn state(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Queued => "queued",
            Self::Sending { .. } => "sending",
            Self::Sent { .. } => "sent",
            Self::Failed(_) => "failed",
            Self::Rejected => "removed",
        }
    }

    pub(crate) fn label(&self) -> String {
        match self {
            Self::Pending => "waiting for host".into(),
            Self::Queued => "queued".into(),
            Self::Sending { target } => format!("→ agent {target}"),
            Self::Sent { target } => format!("sent → {target}"),
            Self::Failed(err) => format!("failed: {err}"),
            Self::Rejected => "removed".into(),
        }
    }
}

pub(crate) struct Suggestion {
    pub(crate) author: String,
    pub(crate) text: String,
    pub(crate) status: Status,
    /// Guest ids who back this prompt.
    pub(crate) votes: BTreeSet<u32>,
    /// Pane chosen by the host; `None` means the current agent pane at delivery.
    target: Option<String>,
    /// Name to prefix the prompt with, so the agent and the transcript show who asked.
    attribution: Option<String>,
}

impl Suggestion {
    fn prompt(&self) -> String {
        match &self.attribution {
            Some(name) => format!("{name}: {}", self.text),
            None => self.text.clone(),
        }
    }
}

#[derive(Default)]
pub(crate) struct Queue {
    items: BTreeMap<u32, Suggestion>,
    next: u32,
    /// Pending and queued prompts, in delivery order.
    order: Vec<u32>,
    /// While paused, nothing is delivered.
    pub(crate) paused: bool,
}

pub(crate) type SharedQueue = Arc<Mutex<Queue>>;

impl Queue {
    /// Adds a prompt at the end. `accepted` skips review (auto mode, host prompts).
    pub(crate) fn push(
        &mut self,
        author: &str,
        text: &str,
        accepted: bool,
        attribution: Option<&str>,
    ) -> u32 {
        self.next += 1;
        self.items.insert(
            self.next,
            Suggestion {
                author: author.to_string(),
                text: text.to_string(),
                status: if accepted {
                    Status::Queued
                } else {
                    Status::Pending
                },
                votes: BTreeSet::new(),
                target: None,
                attribution: attribution.map(str::to_string),
            },
        );
        self.order.push(self.next);
        self.next
    }

    fn open_item(&mut self, id: u32) -> Result<&mut Suggestion, String> {
        let item = self.items.get_mut(&id).ok_or(format!("no prompt #{id}"))?;
        if !matches!(item.status, Status::Pending | Status::Queued) {
            return Err(format!("#{id} is already {}", item.status.label()));
        }
        Ok(item)
    }

    /// Accepts a pending prompt into the queue, optionally for a specific pane.
    pub(crate) fn approve(&mut self, id: u32, target: Option<&str>) -> Result<(), String> {
        let item = self.open_item(id)?;
        if item.status != Status::Pending {
            return Err(format!("#{id} is already queued"));
        }
        item.status = Status::Queued;
        item.target = target.map(str::to_string);
        Ok(())
    }

    /// Removes a pending or queued prompt.
    pub(crate) fn reject(&mut self, id: u32) -> Result<(), String> {
        self.open_item(id)?.status = Status::Rejected;
        self.order.retain(|&o| o != id);
        Ok(())
    }

    /// Removes every pending and queued prompt; returns how many.
    pub(crate) fn clear(&mut self) -> usize {
        let ids = std::mem::take(&mut self.order);
        for id in &ids {
            if let Some(item) = self.items.get_mut(id) {
                item.status = Status::Rejected;
            }
        }
        ids.len()
    }

    /// Moves a pending or queued prompt `delta` places (negative = earlier).
    pub(crate) fn move_by(&mut self, id: u32, delta: i32) -> Result<usize, String> {
        let from = self
            .order
            .iter()
            .position(|&o| o == id)
            .ok_or(format!("#{id} is not waiting"))?;
        let to = (from as i64 + delta as i64).clamp(0, self.order.len() as i64 - 1) as usize;
        let id = self.order.remove(from);
        self.order.insert(to, id);
        Ok(to + 1)
    }

    /// Replaces the text of a waiting prompt (the host tidies it up before sending).
    pub(crate) fn edit(&mut self, id: u32, text: &str) -> Result<(), String> {
        let text = text.trim();
        if text.is_empty() || text.len() > MAX_SUGGESTION_BYTES {
            return Err(format!("text must be 1-{MAX_SUGGESTION_BYTES} bytes"));
        }
        self.open_item(id)?.text = text.to_string();
        Ok(())
    }

    /// Toggles `guest`'s vote on a waiting prompt; returns the new vote count.
    pub(crate) fn vote(&mut self, id: u32, guest: u32) -> Result<usize, String> {
        let item = self.open_item(id)?;
        if !item.votes.remove(&guest) {
            item.votes.insert(guest);
        }
        Ok(item.votes.len())
    }

    /// 1-based place in the queue of a waiting prompt.
    pub(crate) fn position(&self, id: u32) -> Option<usize> {
        self.order.iter().position(|&o| o == id).map(|i| i + 1)
    }

    /// Takes the first queued prompt for delivery.
    fn take_next(&mut self) -> Option<Delivery> {
        if self.paused {
            return None;
        }
        let at = self
            .order
            .iter()
            .position(|id| self.items[id].status == Status::Queued)?;
        let id = self.order.remove(at);
        let item = self.items.get_mut(&id)?;
        item.status = Status::Sending {
            target: item.target.clone().unwrap_or_default(),
        };
        Some(Delivery {
            id,
            target: item.target.clone(),
            prompt: item.prompt(),
            raw: item.text.clone(),
        })
    }

    pub(crate) fn set_status(&mut self, id: u32, status: Status) {
        if let Some(item) = self.items.get_mut(&id) {
            item.status = status;
        }
    }

    /// Ids to show: in flight, then waiting in order, then the last few finished.
    fn visible(&self) -> Vec<u32> {
        let sending = self
            .items
            .iter()
            .filter(|(_, s)| matches!(s.status, Status::Sending { .. }))
            .map(|(id, _)| *id);
        let done: Vec<u32> = self
            .items
            .iter()
            .filter(|(_, s)| !s.status.is_open())
            .map(|(id, _)| *id)
            .collect();
        let recent = done[done.len().saturating_sub(RECENT_DONE)..]
            .iter()
            .copied();
        sending
            .chain(self.order.iter().copied())
            .chain(recent)
            .collect()
    }

    /// The queue as JSON for UIs.
    pub(crate) fn snapshot(&self) -> Vec<serde_json::Value> {
        self.visible()
            .into_iter()
            .map(|id| {
                let s = &self.items[&id];
                json!({
                    "id": id,
                    "author": s.author,
                    "text": s.text,
                    "state": s.status.state(),
                    "status": s.status.label(),
                    "pending": s.status == Status::Pending,
                    "waiting": matches!(s.status, Status::Pending | Status::Queued),
                    "position": self.position(id),
                    "votes": s.votes.len(),
                })
            })
            .collect()
    }

    /// The queue as text, for `/q`.
    pub(crate) fn lines(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .visible()
            .into_iter()
            .map(|id| {
                let s = &self.items[&id];
                let place = self
                    .position(id)
                    .map(|p| format!("{p}."))
                    .unwrap_or_default();
                format!(
                    "  {place:<3} #{id:<3} {:<12} [{}] {}",
                    s.author,
                    s.status.label(),
                    one_line(&s.text, 60)
                )
            })
            .collect();
        if self.paused {
            out.insert(0, "  (delivery paused)".into());
        }
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

/// One prompt on its way to the agent.
#[derive(Debug)]
struct Delivery {
    id: u32,
    target: Option<String>,
    /// Text for an agent, with the author's name when attribution is on.
    prompt: String,
    /// Text as written, for plain shell panes (a name prefix would break the command).
    raw: String,
}

/// Picks the pane a prompt goes to when the host did not choose one.
pub(crate) type Resolver = Arc<dyn Fn() -> Result<String, String> + Send + Sync>;

/// Starts the delivery thread: one prompt at a time, in queue order.
pub(crate) fn spawn_dispatcher(queue: SharedQueue, audit: Arc<Audit>, resolve: Resolver) {
    std::thread::spawn(move || loop {
        let next = queue.lock().unwrap().take_next();
        let Some(job) = next else {
            std::thread::sleep(DISPATCH_POLL);
            continue;
        };
        let id = job.id;
        let result = match job.target.clone().map(Ok).unwrap_or_else(|| resolve()) {
            Ok(target) => deliver(&job, &target, &queue).map(|()| target),
            Err(err) => Err(err),
        };
        match result {
            Ok(target) => {
                eprintln!("#{id} delivered to {target}");
                audit.record("suggestion_sent", json!({ "id": id, "target": target }));
                queue
                    .lock()
                    .unwrap()
                    .set_status(id, Status::Sent { target });
            }
            Err(err) => {
                eprintln!("#{id} not delivered: {err}");
                audit.record("suggestion_failed", json!({ "id": id, "error": err }));
                queue.lock().unwrap().set_status(id, Status::Failed(err));
            }
        }
    });
}

fn deliver(job: &Delivery, target: &str, queue: &SharedQueue) -> Result<(), String> {
    let id = job.id;
    queue.lock().unwrap().set_status(
        id,
        Status::Sending {
            target: target.to_string(),
        },
    );
    let mut announced_block = false;
    loop {
        match pane_agent_status(target)?.as_str() {
            "unknown" => return cli(&["pane", "run", target, &job.raw]).map(|_| ()),
            "working" => {
                cli(&[
                    "agent", "wait", target, "--until", "idle", "--until", "done", "--until",
                    "blocked",
                ])?;
            }
            "blocked" => {
                if !announced_block {
                    eprintln!("#{id}: agent in {target} is waiting for your answer; delivery held");
                    announced_block = true;
                }
                cli(&[
                    "agent", "wait", target, "--until", "idle", "--until", "done",
                ])?;
            }
            _ => match cli(&["agent", "prompt", target, &job.prompt, "--wait"]) {
                Ok(_) => return Ok(()),
                // The agent asked something between our check and the submit.
                Err(err) if err.contains("agent_blocked") => continue,
                Err(err) => return Err(err),
            },
        }
    }
}

/// Runs this binary's own CLI against the shared herdr server.
pub(crate) fn cli(args: &[&str]) -> Result<String, String> {
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

/// The pane suggestions go to and its agent status: the focused pane if it runs a
/// detected agent, else the first pane that does, else the focused pane. `exclude`
/// (the Team window) is never picked.
pub(crate) fn agent_pane(exclude: Option<&str>) -> Result<(String, String), String> {
    let panes = pane_list()?;
    let usable: Vec<&serde_json::Value> = panes
        .iter()
        .filter(|p| p["pane_id"].as_str() != exclude)
        .collect();
    let is_agent =
        |p: &serde_json::Value| p["agent_status"].as_str().is_some_and(|s| s != "unknown");
    let pick = usable
        .iter()
        .find(|p| p["focused"] == true && is_agent(p))
        .or_else(|| usable.iter().find(|p| is_agent(p)))
        .or_else(|| usable.iter().find(|p| p["focused"] == true))
        .or_else(|| usable.first())
        .ok_or("no pane to send to")?;
    Ok((
        pick["pane_id"].as_str().unwrap_or_default().to_string(),
        pick["agent_status"]
            .as_str()
            .unwrap_or("unknown")
            .to_string(),
    ))
}

/// Runs this binary's CLI and parses its JSON reply.
pub(crate) fn cli_json(args: &[&str]) -> Result<serde_json::Value, String> {
    serde_json::from_str(&cli(args)?).map_err(|e| e.to_string())
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
    fn review_mode_needs_approval_auto_mode_does_not() {
        let mut q = Queue::default();
        let a = q.push("kolya", "add tests", false, None);
        let b = q.push("asel", "rename module", true, Some("asel"));
        assert_eq!(
            q.take_next().map(|j| j.id),
            Some(b),
            "pending #a is skipped"
        );
        assert_eq!(q.approve(a, None), Ok(()));
        assert!(q.approve(a, None).is_err());
        assert_eq!(q.take_next().map(|j| j.prompt), Some("add tests".into()));
        assert!(q.take_next().is_none());
    }

    #[test]
    fn attribution_prefixes_the_prompt() {
        let mut q = Queue::default();
        q.push("kolya#2", "fix the parser", true, Some("kolya"));
        let job = q.take_next().unwrap();
        assert_eq!(job.prompt, "kolya: fix the parser");
        assert_eq!(
            job.raw, "fix the parser",
            "shell panes get the text as written"
        );
    }

    #[test]
    fn reorder_pause_clear() {
        let mut q = Queue::default();
        let a = q.push("a", "one", true, None);
        let b = q.push("b", "two", true, None);
        let c = q.push("c", "three", true, None);
        assert_eq!(q.move_by(c, -5), Ok(1));
        assert_eq!(q.position(c), Some(1));
        assert_eq!(q.position(a), Some(2));
        q.paused = true;
        assert!(q.take_next().is_none());
        q.paused = false;
        assert_eq!(q.take_next().unwrap().id, c);
        assert_eq!(q.reject(b), Ok(()));
        assert_eq!(q.clear(), 1);
        assert!(q.take_next().is_none());
        assert!(q.lines().iter().any(|l| l.contains("removed")));
    }

    #[test]
    fn edit_and_vote_only_while_waiting() {
        let mut q = Queue::default();
        let a = q.push("kolya", "add tests", false, None);
        assert_eq!(q.vote(a, 2), Ok(1));
        assert_eq!(q.vote(a, 3), Ok(2));
        assert_eq!(
            q.vote(a, 2),
            Ok(1),
            "second vote by the same guest toggles off"
        );
        assert_eq!(q.edit(a, "  add parser tests  "), Ok(()));
        assert_eq!(q.snapshot()[0]["text"], "add parser tests");
        assert!(q.edit(a, "").is_err());
        q.approve(a, Some("w1:p1")).unwrap();
        q.take_next().unwrap();
        assert!(q.edit(a, "late").is_err());
        assert!(q.vote(a, 4).is_err());
    }

    #[test]
    fn listing_keeps_open_and_recent_items() {
        let mut q = Queue::default();
        for i in 0..8 {
            let id = q.push("kolya", &format!("idea {i}"), false, None);
            q.reject(id).unwrap();
        }
        let open = q.push("kolya", "still open", false, None);
        let lines = q.lines();
        assert_eq!(lines.len(), RECENT_DONE + 1);
        assert!(
            lines[0].contains(&format!("#{open}")),
            "waiting items come first"
        );
    }

    #[test]
    fn one_line_truncates_and_marks_multiline() {
        assert_eq!(one_line("short", 10), "short");
        assert_eq!(one_line("abcdefghijkl", 5), "abcde…");
        assert_eq!(one_line("first\nsecond", 10), "first…");
    }
}
