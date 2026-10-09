# Relay deployment

The relay pairs hosts and guests. It keeps rooms in memory only and never
sees provider tokens; it does see terminal traffic (E2E encryption is future work).

Current team relay: `wss://34-38-17-92.sslip.io`
(GCP project `project-0235cb4f-f27e-4aa1-b85`, VM `teamsent-relay`, e2-micro,
europe-west1-b, static IP `teamsent-relay-ip`). `sslip.io` maps the name to the IP,
so Let's Encrypt works without buying a domain.

## Fresh VM (Debian 12)

```bash
# from the repo root on your machine
tar czf relay.tgz crates/teamsent-relay deploy
gcloud compute scp relay.tgz teamsent-relay:~/ --zone=europe-west1-b
gcloud compute ssh teamsent-relay --zone=europe-west1-b \
  --command='mkdir -p teamsent && tar xzf relay.tgz -C teamsent && sudo HOME=/root bash teamsent/deploy/setup.sh <IP-with-dashes>.sslip.io'
```

`setup.sh` adds swap, builds `teamsent-relay` (~7 min on e2-micro), installs Caddy
(automatic TLS) and two systemd units: `teamsent-relay` (127.0.0.1:7878) and `caddy` (:443).

## Operate

```bash
gcloud compute ssh teamsent-relay --zone=europe-west1-b --command='journalctl -u teamsent-relay -n 50'
gcloud compute ssh teamsent-relay --zone=europe-west1-b --command='sudo systemctl restart teamsent-relay'
```

## Verify

```bash
TEAMSENT_RELAY=wss://34-38-17-92.sslip.io python3 -I scripts/teamsent-smoke.py target/debug/teamsent /tmp/ts-e2e
```
