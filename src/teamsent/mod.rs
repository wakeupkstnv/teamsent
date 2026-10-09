//! Teamsent: shared herdr sessions.
//!
//! `host` exposes the local herdr server through a relay, `join` attaches a
//! guest herdr client to it, `relay` pairs the two. See `design/herdr-recon.md`.

mod filter;
mod host;
mod join;
mod relay;

use std::io;

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
