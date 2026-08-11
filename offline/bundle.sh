#!/usr/bin/env bash
# bundle.sh — run on YOUR laptop. Produces a single self-contained tarball you
# can scp into a fully air-gapped environment (no Docker Hub / registry needed
# at the destination). The tarball contains the container image, the k8s
# manifests, and install.sh.
#
#   ./bundle.sh                       # uses docker.io/anchoo2kewl/flag-file-relay:v0.1.0
#   IMAGE=myrepo/flag-file-relay:tag ./bundle.sh
#
# Then:  scp flag-file-relay-offline.tgz user@host:  &&  ssh user@host
#        tar xzf flag-file-relay-offline.tgz && cd flag-file-relay-offline
#        ./install.sh          # see its --help
set -euo pipefail
IMAGE="${IMAGE:-docker.io/anchoo2kewl/flag-file-relay:v0.1.0}"
OUT="flag-file-relay-offline"
here="$(cd "$(dirname "$0")" && pwd)"
rm -rf "$OUT" "$OUT.tgz"; mkdir -p "$OUT"

echo "Pulling + saving $IMAGE (linux/amd64)..."
docker pull --platform linux/amd64 "$IMAGE" >/dev/null
docker save "$IMAGE" | gzip > "$OUT/image.tar.gz"
echo "$IMAGE" > "$OUT/image.ref"

cp "$here/install.sh" "$OUT/install.sh"; chmod +x "$OUT/install.sh"
cp -r "$here/../deploy" "$OUT/deploy"
cp "$here/../flags.yaml" "$OUT/flags.sample.yaml" 2>/dev/null || true

tar czf "$OUT.tgz" "$OUT"; rm -rf "$OUT"
echo
echo "Built: $OUT.tgz  ($(du -h "$OUT.tgz" | cut -f1))"
echo "Ship it:  scp $OUT.tgz <user>@<host>:  then  tar xzf $OUT.tgz && cd $OUT && ./install.sh --help"
