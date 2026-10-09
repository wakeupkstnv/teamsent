#!/bin/sh
# teamsent installer: curl -fsSL https://34-38-17-92.sslip.io/install.sh | sh
set -eu

BASE="${TEAMSENT_DOWNLOAD:-https://34-38-17-92.sslip.io/dl/latest}"
BIN_DIR="${TEAMSENT_BIN_DIR:-$HOME/.local/bin}"

case "$(uname -s)" in
  Darwin) os=macos ;;
  Linux) os=linux ;;
  *) echo "teamsent: unsupported OS $(uname -s) (macOS and Linux only)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64) arch=aarch64 ;;
  x86_64|amd64) arch=x86_64 ;;
  *) echo "teamsent: unsupported CPU $(uname -m)" >&2; exit 1 ;;
esac
asset="teamsent-$os-$arch"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
echo "downloading ${asset}..."
if ! curl -fsSL "$BASE/$asset" -o "$tmp/teamsent"; then
  echo "teamsent: no build for $os-$arch yet" >&2
  exit 1
fi
curl -fsSL "$BASE/SHA256SUMS" -o "$tmp/SHA256SUMS"
expected="$(grep " $asset\$" "$tmp/SHA256SUMS" | cut -d' ' -f1)"
if command -v sha256sum >/dev/null; then
  actual="$(sha256sum "$tmp/teamsent" | cut -d' ' -f1)"
else
  actual="$(shasum -a 256 "$tmp/teamsent" | cut -d' ' -f1)"
fi
if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
  echo "teamsent: checksum mismatch, refusing to install" >&2
  exit 1
fi

mkdir -p "$BIN_DIR"
install -m 755 "$tmp/teamsent" "$BIN_DIR/teamsent"
echo "installed $BIN_DIR/teamsent ($("$BIN_DIR/teamsent" --version 2>/dev/null || echo "$asset"))"
case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) echo "add to PATH: export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac
echo "join a session: teamsent join <link>"
