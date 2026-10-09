#!/usr/bin/env bash
# Builds and installs the teamsent relay behind Caddy on a fresh Debian/Ubuntu VM.
# usage: sudo ./setup.sh <domain>      (run from the repo root copied to the VM)
set -euo pipefail
DOMAIN="${1:?usage: setup.sh <domain>}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"

# Small VMs (e2-micro, 1 GB) need swap to compile.
if ! swapon --show | grep -q swapfile; then
  fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
fi

apt-get update -qq
apt-get install -y -qq build-essential curl

if [ ! -x "$HOME/.cargo/bin/cargo" ]; then
  curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
"$HOME/.cargo/bin/cargo" build --release --manifest-path "$REPO/crates/teamsent-relay/Cargo.toml" \
  --target-dir "$REPO/target-relay"
install -m 755 "$REPO/target-relay/release/teamsent-relay" /usr/local/bin/teamsent-relay

# Debian 12+ / Ubuntu 24.04+ ship Caddy in the main archive.
command -v caddy >/dev/null || apt-get install -y -qq caddy

echo "RELAY_DOMAIN=$DOMAIN" > /etc/default/teamsent-relay
install -m 644 "$REPO/deploy/Caddyfile" /etc/caddy/Caddyfile
mkdir -p /etc/systemd/system/caddy.service.d
printf '[Service]\nEnvironmentFile=/etc/default/teamsent-relay\n' > /etc/systemd/system/caddy.service.d/teamsent.conf
install -m 644 "$REPO/deploy/teamsent-relay.service" /etc/systemd/system/teamsent-relay.service

systemctl daemon-reload
systemctl enable --now teamsent-relay
systemctl restart caddy
echo "relay ready: wss://$DOMAIN"
