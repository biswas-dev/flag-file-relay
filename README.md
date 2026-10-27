# flag-file-relay

A tiny, file-backed, **LaunchDarkly-relay-compatible** feature-flag server for
**fully offline / air-gapped** deployments.

It implements the public LaunchDarkly server-side relay-proxy endpoints
(`GET /all` streaming, `GET /sdk/latest-all` polling, `POST /bulk` events) and
serves flags from a local YAML file — **no database, no external network, no
dependency on any hosted flag service.** Point any LaunchDarkly server SDK at it
via `relayProxyUrl` and it works with **zero client or code changes**.

- Image: **~6 MB** (`docker.io/anchoo2kewl/flag-file-relay`)
- One static binary: `serve` (the relay) + `list` / `get` / `set` (manage the file)
- Offline mode is intentionally **free and lightweight** — hosted analytics /
  targeting (flagtgl) remains the product. This is just the offline valve.

## How it fits

```
blue-stack service ──(relayProxyUrl)──▶ flag-file-relay ──▶ flags.yaml (ConfigMap)
   (LaunchDarkly SDK, unchanged)            (in-cluster)        (you edit this)
```

Switching **online ⇄ offline** is only the value of `relayProxyUrl`:
- online:  `https://flagtgl.com` (or your hosted flagtgl)
- offline: `http://flag-file-relay.blues.svc.cluster.local:8080`

Nothing else changes.

## Upstream mode: an in-cluster edge relay in front of flagtgl (or LaunchDarkly)

Set `UPSTREAM_URI` and the same binary becomes a caching relay: it holds **one**
upstream stream per SDK key for the whole cluster, keeps the last good flags on
disk, and serves the LaunchDarkly server-side SDK endpoints to every process.

```
service pods ──(relayProxyUrl / LD_RELAY_URI)──▶ flag-relay ──▶ flagtgl.com
 (LD SDK, unchanged)          in-cluster, 2 replicas, PVC      (or LaunchDarkly)
```

- **Protocol, both sides:** `GET /all`, `/flags` (legacy), `/sdk/latest-all`,
  `/sdk/latest-flags[/key]`, `/sdk/latest-segments[/key]`, `POST /bulk`,
  `/diagnostic`. Upstream it uses the same endpoints, so flagtgl or LaunchDarkly
  (`UPSTREAM_STREAM_URI`, `UPSTREAM_POLL_URI`, `UPSTREAM_EVENTS_URI`) can sit there.
- **Keys:** pass-through. The SDK's own key is forwarded upstream; only
  `sha256(key)` is ever written to disk or logs. Optional `DENY_KEY_HASHES`,
  `ALLOW_KEY_HASHES`.
- **Outages:** every change is fsynced to `CACHE_DIR` before it is published, so
  a relay restarted while upstream is down still serves. A reconciliation poll
  (`RECONCILE_INTERVAL`, 60 s) catches anything a live stream missed.
- **Replicas:** `PRIMARY_HOST`/`PRIMARY_URI` make one replica own the upstream
  stream; the others follow it over the LD protocol and fall back to upstream
  only while it is down. Replicas exchange snapshots on `PEER_PORT` (8081) and
  a replica that restarted offline adopts the freshest confirmed copy.
- **Restarts:** on SIGTERM `/readyz` turns 503 and SDK streams are closed
  spread over `DRAIN_SPREAD` (20 s), so SDKs reconnect gradually.
- **Events:** SDK analytics are batched per key and forwarded upstream
  (de-duplicated by payload id).
- **Ops:** `/healthz`, `/readyz`, `/status` (JSON), `/metrics` (Prometheus).
- **Local overrides:** see below (`RELAY_ADMIN_TOKEN`, `ADMIN_PORT`).
- **Offline seeding:** `SDK_KEY=... CACHE_DIR=... flag-file-relay seed latest-all.json`.
- **Start-up gate:** `flag-file-relay wait http://flag-relay:8080/readyz 120s`
  (initContainer; always exits 0).

Rollback for a service is its relay URL (back to `https://flagtgl.com`).

## Local overrides (admin API)

Both modes can serve a local value on top of the source (upstream or the
flags file): set `RELAY_ADMIN_TOKEN` (16+ characters) and an admin API listens
on `ADMIN_PORT` (8082). Every request needs `Authorization: Bearer <token>`.
Unset token = no admin API.

```
GET    /admin/flags[?env=<id>]      every flag: upstream value/version, override, effective value
GET    /admin/flags/{key}           one flag (ETag = override revision)
PUT    /admin/flags/{key}/override  {"value": <json>, "set_by": "who", "local_only": false}
DELETE /admin/flags/{key}/override[?set_by=who]
```

- **Served everywhere:** the stream (`/all`, `/flags`: a `patch`/`delete` per
  change), `/sdk/latest-all`, `/sdk/latest-flags[/key]`. Boolean, string,
  number and JSON values; the flag is served with that single value to every
  context. A flag the source does not define is synthesized. Overrides apply
  to every SDK key the relay serves. Peers always get the source data.
- **Last write wins:** an override remembers the flag's source version; a
  later source change (higher version) replaces it, logged once.
  `"local_only": true` overrides are never replaced by the source. In file
  mode the file has no versions, so an override stays until it is removed.
- **Concurrency:** `If-Match: "<rev>"` on PUT/DELETE (`"0"` = no override yet,
  `"*"` = one exists) answers 412 when the override changed since it was read.
- **Durable:** kept in `OVERRIDES_FILE` (default `$CACHE_DIR/overrides.json`),
  written with fsync before it is served; an admin write that cannot be
  persisted is refused (503). Without either, overrides live in memory only.
- **Replicated:** followers forward writes to the primary's admin port
  (`PRIMARY_ADMIN_URI`, default: `PRIMARY_URI`'s host on `ADMIN_PORT`) and
  apply them locally only while it is down. Each write is pushed to every peer
  (`PEER_DNS`/`PEER_ADDRS`, `PEER_PORT`, same token) before the answer, and
  replicas pull each other every `OVERRIDE_SYNC_INTERVAL` (10 s) and before
  they report ready. Records merge per key by (revision, replica).
- **Air-gapped:** an upstream-mode relay that never reached upstream and has
  no cached data still serves its overrides (and nothing else) to SDKs.
- **Chart:** `--set admin.enabled=true --set admin.existingSecret=<secret>`
  (key `token`) exposes the admin port on the Service and adds the headless
  `<release>-peer` Service for replication.

Example:
```bash
curl -s -X PUT -H "Authorization: Bearer $RELAY_ADMIN_TOKEN" \
  -d '{"value": false, "set_by": "me@example.com"}' \
  http://flag-file-relay:8082/admin/flags/new-rbac/override
```

## Quick start (registry reachable)

```bash
helm install ffr ./chart -n blues        # or: kubectl apply -f deploy/flag-file-relay.yaml
# then point services at it (offline toggle):
NS=blues ./offline/install.sh --image docker.io/anchoo2kewl/flag-file-relay:v0.1.0
```

## Air-gapped install (no Docker Hub at the destination)

On your laptop:
```bash
./offline/bundle.sh                       # -> flag-file-relay-offline.tgz (image + manifests + install.sh)
scp flag-file-relay-offline.tgz user@host:
```
On a host with kubectl access to the target cluster:
```bash
tar xzf flag-file-relay-offline.tgz && cd flag-file-relay-offline
./install.sh --registry <your-private-registry>   # loads image, pushes, deploys relay, toggles services offline
```

## Managing flags (CLI, no UI)

```bash
flagctl list                                 # all flags + values
flagctl get new-rbac
flagctl set new-rbac true                     # global default
flagctl set new-rbac true --tenant acme.example.com   # per-tenant override
```
Or edit directly: `kubectl -n blues edit configmap flag-file-relay-flags`.
The relay hot-reloads within ~1 minute — no restart, no client reconnect needed.

### flags.yaml format
```yaml
flags:
  new-rbac:
    value: false                 # default
    tenants:                      # optional per-tenant overrides
      prod.acme.com: true
  new-alerts: false               # shorthand: bare value = default only
  max-report-schedules:
    value: 15                     # numbers and strings work too
```

## Verified

Proven end-to-end on a GovCloud EKS cell: a blue-stack worker pointed at the
in-cluster relay logged `data source got an event: put` and initialized with
**zero** errors — no reach to any external flag host.

## License / ownership

Standalone and vendor-neutral. The wire format is the documented LaunchDarkly
streaming/polling protocol; nothing here is tied to a particular control plane,
so it can live in any org's registry and repo.
