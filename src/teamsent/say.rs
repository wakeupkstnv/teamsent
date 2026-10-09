//! `teamsent say <link> <text…>`: propose a prompt to the host's agent.
//!
//! Uses the same invite link as `join`. The host queues the text, shows it to
//! the host, and delivers it only after approval. Works from any terminal,
//! including while `teamsent join` is running in another one.

use std::io::{self, Read};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use super::queue::MAX_SUGGESTION_BYTES;
use super::{default_name, is_join_link, is_valid_name};

const USAGE: &str = "usage: teamsent say <link> [--name NAME] <text…>   (text \"-\" reads stdin)";

pub(crate) fn run(args: &[String]) -> io::Result<()> {
    let usage = |msg: &str| io::Error::new(io::ErrorKind::InvalidInput, format!("{msg}{USAGE}"));
    let (link, rest) = args.split_first().ok_or_else(|| usage(""))?;
    if !is_join_link(link) {
        return Err(usage("not a teamsent invite link\n"));
    }
    let (name, words) = match rest {
        [flag, name, words @ ..] if flag == "--name" => (name.clone(), words),
        words => (default_name(), words),
    };
    if !is_valid_name(&name) {
        return Err(usage(
            "name must be 1-32 chars of a-z, 0-9, '-', '_', '.'\n",
        ));
    }
    let text = match words {
        [dash] if dash == "-" => {
            let mut text = String::new();
            io::stdin().read_to_string(&mut text)?;
            text
        }
        words => words.join(" "),
    };
    let text = text.trim();
    if text.is_empty() {
        return Err(usage("nothing to say\n"));
    }
    if text.len() > MAX_SUGGESTION_BYTES {
        return Err(usage(&format!(
            "suggestion is {} bytes; the limit is {MAX_SUGGESTION_BYTES}\n",
            text.len()
        )));
    }

    let reply = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(send(&format!("{link}?name={name}&kind=say"), text))?;
    eprintln!("{reply}");
    Ok(())
}

async fn send(url: &str, text: &str) -> io::Result<String> {
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|err| io::Error::other(format!("cannot reach relay: {err}")))?;
    ws.send(Message::text(text))
        .await
        .map_err(io::Error::other)?;
    let answer = match ws.next().await {
        Some(Ok(Message::Text(answer))) => answer.to_string(),
        _ => return Err(io::Error::other("session not found (host offline?)")),
    };
    let _ = ws.close(None).await;
    if let Some(id) = answer.strip_prefix("teamsent-queued ") {
        return Ok(format!("queued as #{id}; the host will review it"));
    }
    let reason = answer.strip_prefix("teamsent-denied ").unwrap_or(&answer);
    Err(io::Error::other(format!("not sent: {reason}")))
}
