# Herdr recon (base: v0.9.3, 7b116c0)

Goal: `teamsent host` → invite link, `teamsent join <link>` → guest sees the host's herdr session.

## Key findings

1. **Client ↔ server is a plain byte stream.** Length-prefixed frames
   (u32 LE + bincode), see `src/protocol/wire.rs` (`write_message` / `read_message`,
   `MAX_FRAME_SIZE` = 2 MB). Transport-agnostic.
2. **Remote already tunnels that stream.** `herdr --remote` (`src/remote/attach.rs`)
   creates a local forward socket, pipes it over SSH stdio to
   `remote-client-bridge` (`src/remote/host.rs`), which connects to the server's
   client socket (`server::socket_paths::client_socket_path()`) and copies bytes
   both ways (`platform::forward_remote_bridge_stdio`).
   The local side runs the normal UI: `herdr client` with
   `CLIENT_SOCKET_PATH_ENV_VAR` pointing at the forward socket (`run_client_process`).
   → **We swap SSH for a WebSocket relay. The whole herdr client UI (workspaces,
   splits, plugins' panes) comes for free.**
3. **Read-only mode exists.** `ClientMessage::ObserveTerminal { target }`
   (server: `src/server/client_transport.rs:1072`, `headless.rs:2007`),
   CLI: `herdr terminal session observe <target>`. Writable: `ControlTerminal`
   / `AttachTerminal { takeover }`.
4. **Multi-client is native.** Each connection to the client socket is a separate
   client id. One guest = one upstream connection on the host.

## Architecture

```
host machine                         relay                    guest machine
herdr server                                                  herdr client UI
  ▲ client socket                                               ▲ local forward socket
  │ (1 conn per guest)                                          │
teamsent host ──WS(mux by guest id)──► room ◄──WS──── teamsent join
  ├─ role filter on every ClientMessage
  ├─ suggestion queue (own control channel, not herdr protocol)
  └─ audit JSONL
```

## Guest message filter (host side, decode with `read_message`)

| ClientMessage | viewer | participant |
|---|---|---|
| ClientShellHello / TerminalHello, Resize, ClientShellResize, Detach | allow | allow |
| ObserveTerminal, AttachScroll, ClientShellFocus, ClientShellHostTheme, MouseCapture | allow | allow |
| Input, ClientShellPaneInput, ClientShellPopupInput, AttachMouse | **deny** | **deny** (goes via suggestion → host approve) |
| ControlTerminal, AttachTerminal | **deny** | **deny** |
| ClientShellEndpointRequest, EndpointControl | **deny** (may run commands/plugins) | **deny** until audited |
| ClipboardImage, Graphics* | deny | deny |

Default-deny: unknown variants are dropped.

## Answered by the smoke test (`scripts/teamsent-smoke.py`)

- Guest sees the host's focused pane and live updates over the relay. ✅
- Guest keystrokes (including `prefix c`) never reach the host: no input, no new tab. ✅
- Guest attach does not move the host's focus. ✅
- The client handshake is `EndpointControl { kind: "endpoint.hello.v1" }`, not
  `ClientShellHello`. Allowed client control kinds: hello, presentation sync,
  health ping (`CLIENT_ENDPOINT_CONTROL_KINDS` in `src/teamsent/filter.rs`).
- Guest UI sent no endpoint requests in the smoke run (no `denied …` lines).

## Open questions (week 1–2)

- Does full ClientShell view change host state (focus, tab switch, resize clamp)?
  Related issues: #2404, #3229, #3474, #2665. Fallback for MVP: guest uses
  `ObserveTerminal` on one pane (safe, already read-only).
- Which ClientShell UI keys send `EndpointRequest` (plugin actions, commands)?
  Must stay denied for guests.
- Late join: server sends a full `ClientShellSnapshot` / `PaneSurface` on attach →
  F4 snapshot probably comes for free; relay only needs reconnect.
- Version lock: guest and host must run the same `PROTOCOL_VERSION` (22) —
  ship one `teamsent` binary, check version in the invite handshake.

## MVP order

1. `teamsent host --observe <pane>` + `teamsent join` over relay, one guest, no auth.
2. Mux N guests, invite tokens (TTL), TLS, roles + filter above.
3. Full shell view for guests (after the open questions are answered).
4. Suggestion queue, revoke, audit log.
