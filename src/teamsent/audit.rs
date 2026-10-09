//! Host-side audit log: one JSON object per line, appended to
//! `<state dir>/audit.jsonl`. Records who joined, what they suggested, what the
//! host approved, and every grant or revoke (U8, F9).

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use serde_json::{json, Value};

pub(crate) struct Audit {
    path: PathBuf,
    file: Mutex<Option<File>>,
}

impl Audit {
    pub(crate) fn open_default() -> Self {
        Self::open(crate::config::state_dir().join("audit.jsonl"))
    }

    pub(crate) fn open(path: PathBuf) -> Self {
        let file = path
            .parent()
            .map(std::fs::create_dir_all)
            .transpose()
            .ok()
            .and_then(|_| {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .ok()
            });
        if file.is_none() {
            eprintln!("warning: audit log unavailable at {}", path.display());
        }
        Self {
            path,
            file: Mutex::new(file),
        }
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Appends `{"ts": …, "event": event, …fields}`. Failures never stop sharing.
    pub(crate) fn record(&self, event: &str, fields: Value) {
        let ts = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        let mut entry = json!({ "ts": ts, "event": event });
        if let (Some(entry), Value::Object(fields)) = (entry.as_object_mut(), fields) {
            entry.extend(fields);
        }
        if let Some(file) = self.file.lock().unwrap().as_mut() {
            let _ = writeln!(file, "{entry}");
        }
    }

    /// The last `n` entries, oldest first.
    pub(crate) fn tail(&self, n: usize) -> Vec<String> {
        let Ok(file) = File::open(&self.path) else {
            return Vec::new();
        };
        let lines: Vec<String> = BufReader::new(file).lines().map_while(Result::ok).collect();
        lines[lines.len().saturating_sub(n)..].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_are_json_lines_with_timestamp() {
        let dir = std::env::temp_dir().join(format!("teamsent-audit-{}", std::process::id()));
        let audit = Audit::open(dir.join("audit.jsonl"));
        audit.record("join", json!({ "guest": 1, "name": "kolya" }));
        audit.record("revoke", json!({ "guest": 1 }));
        let tail = audit.tail(1);
        assert_eq!(tail.len(), 1);
        let entry: Value = serde_json::from_str(&tail[0]).unwrap();
        assert_eq!(entry["event"], "revoke");
        assert!(entry["ts"].as_str().unwrap().contains('T'));
        let _ = std::fs::remove_dir_all(dir);
    }
}
