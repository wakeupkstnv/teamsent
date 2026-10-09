# Patches to upstream herdr files

Base: herdr v0.9.3 (7b116c05).

| File | Change | Why |
|---|---|---|
| `src/main.rs` | `mod teamsent;` and `teamsent::maybe_run(&args)` before remote arg parsing | dispatch `host` / `join` / `relay` subcommands |
| `Cargo.toml` | tokio `net`+`signal`+`io-std`; `tokio-tungstenite`, `getrandom`; `futures-util` moved from linux-only to all targets | relay WebSocket transport |
| `Cargo.toml` | `[[bin]] name = "teamsent"`, `default-run` | ship as `teamsent`; package name stays `herdr` to keep crate paths |
| `src/config/io.rs` | `app_dir_name()` → `teamsent` / `teamsent-dev` | own config/state/socket dirs, never clashes with an installed herdr |
| `tests/**` | `CARGO_BIN_EXE_teamsent`, `teamsent-dev`, `teamsent/config.toml` | follow the bin and dir rename |
| `Cargo.toml` | workspace member `crates/teamsent-relay`; `rustls` (ring) and tokio-tungstenite `rustls-tls-webpki-roots` | standalone relay crate; `wss://` relays |
