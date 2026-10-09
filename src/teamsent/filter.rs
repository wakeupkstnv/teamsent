//! Guest permission filter for the herdr client protocol.
//!
//! The host decodes every frame a guest sends and only forwards frames the
//! guest's role allows. Unknown or undecodable frames are denied.

use crate::protocol::{ClientMessage, ServerMessage, MAX_FRAME_SIZE};

/// Length of the u32 little-endian prefix that frames each message.
pub(crate) const LENGTH_PREFIX_BYTES: usize = 4;

/// Endpoint methods a viewer may call: reads and client-local view state only.
const VIEWER_ENDPOINT_METHODS: &[&str] = &[
    "client_shell.surface.set",
    "integration.list",
    "pane.link.resolve",
    "pane.selection.read",
    "worktree.list",
];

/// Endpoint control kinds a client sends during a normal session: handshake,
/// presentation fences and health probes. None of them change server state.
const CLIENT_ENDPOINT_CONTROL_KINDS: &[&str] = &[
    crate::protocol::endpoint::ENDPOINT_HELLO_KIND,
    crate::protocol::endpoint::PRESENTATION_EFFECTS_SYNC_KIND,
    crate::protocol::endpoint::HEALTH_PING_KIND,
];

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Forward the frame to the herdr server unchanged.
    Forward,
    /// Drop the frame silently.
    Drop(&'static str),
    /// Drop the frame and answer the guest with this message instead.
    Reply(Box<ServerMessage>, String),
    /// The guest broke the protocol; close the connection.
    Close(String),
}

/// Splits complete frames off the front of `buf`.
///
/// Returns `Err` when a frame claims more than `max` bytes.
pub(crate) fn take_frames(buf: &mut Vec<u8>, max: usize) -> Result<Vec<Vec<u8>>, String> {
    let mut frames = Vec::new();
    let mut start = 0;
    while buf.len() - start >= LENGTH_PREFIX_BYTES {
        let prefix: [u8; LENGTH_PREFIX_BYTES] = buf[start..start + LENGTH_PREFIX_BYTES]
            .try_into()
            .expect("prefix slice has prefix length");
        let len = u32::from_le_bytes(prefix) as usize;
        if len > max {
            return Err(format!("frame size {len} exceeds maximum {max}"));
        }
        let end = start + LENGTH_PREFIX_BYTES + len;
        if buf.len() < end {
            break;
        }
        frames.push(buf[start..end].to_vec());
        start = end;
    }
    buf.drain(..start);
    Ok(frames)
}

/// Encodes a server message as one length-prefixed frame.
pub(crate) fn encode_frame(msg: &ServerMessage) -> Vec<u8> {
    let mut out = Vec::new();
    crate::protocol::write_message(&mut out, msg).expect("writing to a Vec cannot fail");
    out
}

/// Decides what to do with one complete guest frame (prefix included).
pub(crate) fn classify_viewer_frame(frame: &[u8]) -> Verdict {
    let payload = &frame[LENGTH_PREFIX_BYTES..];
    if payload.len() > MAX_FRAME_SIZE {
        return Verdict::Close("oversized frame".into());
    }
    let msg: ClientMessage =
        match bincode::serde::decode_from_slice(payload, bincode::config::standard()) {
            Ok((msg, consumed)) if consumed == payload.len() => msg,
            Ok(_) => return Verdict::Close("trailing bytes in frame".into()),
            Err(err) => return Verdict::Close(format!("undecodable frame: {err}")),
        };
    classify_viewer(msg)
}

fn classify_viewer(msg: ClientMessage) -> Verdict {
    use ClientMessage as M;
    match msg {
        M::TerminalHello { .. }
        | M::ClientShellHello { .. }
        | M::Resize { .. }
        | M::ClientShellResize { .. }
        | M::Detach
        | M::ObserveTerminal { .. }
        | M::AttachScroll { .. }
        | M::ClientShellHostTheme { .. }
        | M::ClientShellFocus { .. }
        | M::ClientShellMouseCapture { .. }
        | M::GraphicsTransmissionResult { .. }
        | M::GraphicsTransmissionStarted { .. } => Verdict::Forward,

        M::Input { .. }
        | M::ClientShellPaneInput { .. }
        | M::ClientShellPopupInput { .. }
        | M::AttachMouse { .. } => Verdict::Drop("input"),

        M::AttachTerminal { .. } | M::ControlTerminal { .. } => Verdict::Drop("terminal control"),
        M::ClipboardImage { .. } => Verdict::Drop("clipboard"),
        M::EndpointControl { kind, .. } => {
            if CLIENT_ENDPOINT_CONTROL_KINDS.contains(&kind.as_str()) {
                Verdict::Forward
            } else {
                Verdict::Drop("endpoint control")
            }
        }

        M::ClientShellEndpointRequest { boot_id, request } => {
            classify_endpoint_request(boot_id, &request)
        }
    }
}

fn classify_endpoint_request(boot_id: String, request: &str) -> Verdict {
    #[derive(serde::Deserialize)]
    struct Head {
        id: String,
        method: String,
    }
    let Ok(head) = serde_json::from_str::<Head>(request) else {
        return Verdict::Close("invalid endpoint request".into());
    };
    if VIEWER_ENDPOINT_METHODS.contains(&head.method.as_str()) {
        return Verdict::Forward;
    }
    let reply = crate::server::client_commands::error_message(
        boot_id,
        head.id,
        "permission_denied",
        format!("{} is not allowed for viewers", head.method),
    );
    Verdict::Reply(Box::new(reply), head.method)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(msg: &ClientMessage) -> Vec<u8> {
        let mut out = Vec::new();
        crate::protocol::write_message(&mut out, msg).unwrap();
        out
    }

    #[test]
    fn take_frames_splits_and_keeps_partial_tail() {
        let a = frame(&ClientMessage::Detach);
        let b = frame(&ClientMessage::ClientShellFocus { focused: true });
        let mut buf = [a.clone(), b[..3].to_vec()].concat();
        assert_eq!(take_frames(&mut buf, MAX_FRAME_SIZE).unwrap(), vec![a]);
        buf.extend_from_slice(&b[3..]);
        assert_eq!(take_frames(&mut buf, MAX_FRAME_SIZE).unwrap(), vec![b]);
        assert!(buf.is_empty());
    }

    #[test]
    fn take_frames_rejects_oversized_prefix() {
        let mut buf = (u32::MAX).to_le_bytes().to_vec();
        assert!(take_frames(&mut buf, MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn viewer_cannot_send_input() {
        let msg = ClientMessage::Input {
            data: b"rm -rf /\r".to_vec(),
        };
        assert_eq!(classify_viewer_frame(&frame(&msg)), Verdict::Drop("input"));
    }

    #[test]
    fn viewer_cannot_take_control() {
        let msg = ClientMessage::ControlTerminal {
            target: "w1:p1".into(),
            takeover: true,
        };
        assert!(matches!(
            classify_viewer_frame(&frame(&msg)),
            Verdict::Drop(_)
        ));
    }

    #[test]
    fn viewer_endpoint_mutation_gets_permission_denied_reply() {
        let msg = ClientMessage::ClientShellEndpointRequest {
            boot_id: "b".into(),
            request: r#"{"id":"r1","method":"pane.close","params":{}}"#.into(),
        };
        match classify_viewer_frame(&frame(&msg)) {
            Verdict::Reply(reply, method) => {
                assert_eq!(method, "pane.close");
                assert!(matches!(
                    *reply,
                    ServerMessage::ClientShellEndpointResponseChunk { ref request_id, final_chunk: true, .. }
                        if request_id == "r1"
                ));
            }
            other => panic!("expected reply, got {other:?}"),
        }
    }

    #[test]
    fn viewer_endpoint_read_is_forwarded() {
        let msg = ClientMessage::ClientShellEndpointRequest {
            boot_id: "b".into(),
            request: r#"{"id":"r1","method":"worktree.list","params":{}}"#.into(),
        };
        assert_eq!(classify_viewer_frame(&frame(&msg)), Verdict::Forward);
    }

    #[test]
    fn endpoint_handshake_is_forwarded_unknown_control_is_dropped() {
        let hello = ClientMessage::EndpointControl {
            kind: crate::protocol::endpoint::ENDPOINT_HELLO_KIND.into(),
            data: "{}".into(),
        };
        assert_eq!(classify_viewer_frame(&frame(&hello)), Verdict::Forward);
        let other = ClientMessage::EndpointControl {
            kind: "future.control".into(),
            data: String::new(),
        };
        assert!(matches!(
            classify_viewer_frame(&frame(&other)),
            Verdict::Drop(_)
        ));
    }

    #[test]
    fn garbage_closes_connection() {
        let mut buf = 3u32.to_le_bytes().to_vec();
        buf.extend_from_slice(&[0xff, 0xff, 0xff]);
        assert!(matches!(classify_viewer_frame(&buf), Verdict::Close(_)));
    }
}
