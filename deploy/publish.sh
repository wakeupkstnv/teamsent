#!/usr/bin/env bash
# Uploads release binaries to the relay VM's download area.
# usage: deploy/publish.sh <version> <file>...   (files named teamsent-<os>-<arch>)
set -euo pipefail
VERSION="${1:?usage: publish.sh <version> <file>...}"; shift
[ "$#" -gt 0 ] || { echo "no files" >&2; exit 1; }
VM="${TEAMSENT_VM:-teamsent-relay}"
ZONE="${TEAMSENT_ZONE:-europe-west1-b}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
for f in "$@"; do cp "$f" "$stage/"; done
(cd "$stage" && shasum -a 256 teamsent-* > SHA256SUMS && cat SHA256SUMS)
cp "$ROOT/deploy/install.sh" "$stage/"

gcloud compute scp --zone="$ZONE" --quiet --recurse "$stage" "$VM:~/publish-$VERSION"
gcloud compute ssh "$VM" --zone="$ZONE" --quiet --command="
  set -e
  sudo mkdir -p /var/www/teamsent/dl/$VERSION
  sudo install -m 644 ~/publish-$VERSION/teamsent-* ~/publish-$VERSION/SHA256SUMS /var/www/teamsent/dl/$VERSION/
  sudo install -m 644 ~/publish-$VERSION/install.sh /var/www/teamsent/install.sh
  sudo ln -sfn $VERSION /var/www/teamsent/dl/latest
  rm -rf ~/publish-$VERSION
  ls -la /var/www/teamsent/dl/latest/"
