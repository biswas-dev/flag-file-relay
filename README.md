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
