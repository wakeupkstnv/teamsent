//! Teamsent: shared herdr sessions.
//!
//! `host` exposes the local herdr server through a relay, `join` attaches a
//! guest herdr client to it, `relay` pairs the two. See `design/herdr-recon.md`.

mod filter;
mod host;
mod join;
mod relay;

use std::io;
use std::time::Duration;

/// Sent by the relay on the host control connection after registration.
pub(crate) const PROTOCOL_TAG: &str = "teamsent-relay-v0";

/// Runs a teamsent subcommand, or returns `None` when `args[1]` is not one.
pub(crate) fn maybe_run(args: &[String]) -> Option<io::Result<()>> {
    let rest = args.get(2..).unwrap_or_default();
    match args.get(1).map(String::as_str)? {
        "host" => Some(host::run(rest)),
        "join" => Some(join::run(rest)),
        "relay" => Some(relay::run(rest)),
        _ => None,
    }
}

/// Longest invite lifetime (N3: tokens live at most 24 hours).
pub(crate) const MAX_INVITE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// What a guest may do. Checked by the host on every guest frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// Watch only.
    Viewer,
    /// Type into panes and navigate; cannot create, close or reconfigure.
    Writer,
}

impl Role {
    pub(crate) fn parse(s: &str) -> Option<Self> {
        match s {
            "viewer" => Some(Self::Viewer),
            "writer" => Some(Self::Writer),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Writer => "writer",
        }
    }
}

/// Parses `90s`, `30m`, `2h`; rejects zero and anything above [`MAX_INVITE_TTL`].
pub(crate) fn parse_ttl(s: &str) -> Option<Duration> {
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit())?);
    let n: u64 = num.parse().ok()?;
    let secs = match unit {
        "s" => n,
        "m" => n.checked_mul(60)?,
        "h" => n.checked_mul(3600)?,
        _ => return None,
    };
    let ttl = Duration::from_secs(secs);
    (secs > 0 && ttl <= MAX_INVITE_TTL).then_some(ttl)
}

/// Looks up `key` in a `a=1&b=2` query string.
pub(crate) fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then_some(v)
    })
}

/// Guest display names travel in URLs and logs: keep them short and plain.
pub(crate) fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 32
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

/// 128-bit random id, hex encoded. Used for rooms, host keys and channels.
pub(crate) fn random_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("OS random source is available");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Shuts down both directions so a thread blocked in `read` on a clone wakes up.
pub(crate) fn shutdown_local_stream(stream: &crate::ipc::LocalStream) {
    #[cfg(unix)]
    {
        use std::os::fd::{AsFd, AsRawFd};
        let interprocess::local_socket::Stream::UdSocket(uds) = stream;
        // SAFETY: the fd stays owned by `stream`; shutdown does not close it.
        unsafe {
            libc::shutdown(uds.as_fd().as_raw_fd(), libc::SHUT_RDWR);
        }
    }
    #[cfg(not(unix))]
    let _ = stream;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_parses_units_and_caps_at_one_day() {
        assert_eq!(parse_ttl("90s"), Some(Duration::from_secs(90)));
        assert_eq!(parse_ttl("30m"), Some(Duration::from_secs(1800)));
        assert_eq!(parse_ttl("24h"), Some(MAX_INVITE_TTL));
        assert_eq!(parse_ttl("25h"), None);
        assert_eq!(parse_ttl("0m"), None);
        assert_eq!(parse_ttl("1d"), None);
        assert_eq!(parse_ttl("h"), None);
    }

    #[test]
    fn query_param_finds_exact_keys() {
        assert_eq!(query_param("t=abc&name=kolya", "name"), Some("kolya"));
        assert_eq!(query_param("tt=x&t=y", "t"), Some("y"));
        assert_eq!(query_param("", "t"), None);
    }

    #[test]
    fn names_are_plain_ascii() {
        assert!(is_valid_name("kolya_1.dev"));
        assert!(!is_valid_name(""));
        assert!(!is_valid_name("a b"));
        assert!(!is_valid_name("коля"));
    }
}
