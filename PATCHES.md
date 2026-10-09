# Patches to upstream herdr files

Base: herdr v0.9.3 (7b116c05).

| File | Change | Why |
|---|---|---|
| `src/main.rs` | `mod teamsent;` and `teamsent::maybe_run(&args)` before remote arg parsing | dispatch `host` / `join` / `relay` subcommands |
| `Cargo.toml` | tokio `net`+`signal`; `tokio-tungstenite`, `getrandom`; `futures-util` moved from linux-only to all targets | relay WebSocket transport |
