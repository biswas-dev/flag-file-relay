#!/usr/bin/env bash
# install.sh — deploy flag-file-relay and switch a blue-stack cell into OFFLINE
# feature-flag mode. Run from inside the extracted bundle, pointed at the target
# cluster (kubectl context already set).
#
#   ./install.sh --registry <your-registry>            # air-gapped: push image there
#   ./install.sh --image docker.io/anchoo2kewl/flag-file-relay:v0.1.0   # if pullable
#   Options: --namespace blues  --relay-url <url>  --dry-run
#
# Switching back ONLINE later is just re-pointing relayProxyUrl at your flagtgl
# host (e.g. re-run your normal migrate with RELAY_URL=https://flagtgl.com).
set -euo pipefail
NS="blues"; RELAY_URL="http://flag-file-relay.blues.svc.cluster.local:8080"
IMAGE=""; REGISTRY=""; APPLY=1
while [ $# -gt 0 ]; do case "$1" in
  --namespace) NS="$2"; shift 2;;
  --relay-url) RELAY_URL="$2"; shift 2;;
  --image) IMAGE="$2"; shift 2;;
  --registry) REGISTRY="$2"; shift 2;;
  --dry-run) APPLY=0; shift;;
  --help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
  *) echo "unknown arg $1"; exit 1;;
esac; done
KC=(kubectl -n "$NS")

# 1) Image — load from the bundle, push to the customer registry if given.
if [ -f image.tar.gz ]; then
  echo "Loading image from bundle..."; docker load < <(gunzip -c image.tar.gz)
  BUNDLED="$(cat image.ref)"
  if [ -n "$REGISTRY" ]; then
    IMAGE="$REGISTRY/flag-file-relay:v0.1.0"
    docker tag "$BUNDLED" "$IMAGE"; docker push "$IMAGE"
  else
    IMAGE="${IMAGE:-$BUNDLED}"
  fi
fi
[ -n "$IMAGE" ] || { echo "need --image or --registry (with bundle)"; exit 1; }
echo "Using image: $IMAGE"

# 2) Deploy the relay (ConfigMap + Deployment + Service).
sed "s#REPLACE_WITH_CUSTOMER_REGISTRY/flag-file-relay:latest#$IMAGE#; s#docker.io/anchoo2kewl/flag-file-relay:v0.1.0#$IMAGE#" deploy/flag-file-relay.yaml > /tmp/ffr.yaml
if [ "$APPLY" = 1 ]; then "${KC[@]}" apply -f /tmp/ffr.yaml; "${KC[@]}" rollout status deploy/flag-file-relay --timeout=120s
else echo "[dry-run] would apply relay manifest with image $IMAGE"; fi

# 3) Toggle every blue-stack flag consumer to the in-cluster relay.
echo "Switching services to offline relay: $RELAY_URL"
for cm in $("${KC[@]}" get cm -o name 2>/dev/null | sed 's#configmap/##'); do
  cfg="$("${KC[@]}" get cm "$cm" -o jsonpath='{.data.config\.yaml}' 2>/dev/null || true)"
  case "$cfg" in *launchDarklyClient:*) ;; *) continue ;; esac
  f=$(mktemp); printf '%s' "$cfg" >"$f"
  new="$(CFG_FILE="$f" RELAY_URL="$RELAY_URL" python3 - <<'PY'
import os,re,sys
lines=open(os.environ["CFG_FILE"]).read().split("\n")
def ind(s):return len(s)-len(s.lstrip(" "))
st=None
for i,l in enumerate(lines):
    if re.match(r'^(\s*)launchDarklyClient:\s*$',l): st=i;p=ind(l);break
if st is None: sys.stdout.write("\n".join(lines));sys.exit(0)
e=st+1
while e<len(lines):
    l=lines[e]
    if l.strip()=="": e+=1;continue
    if ind(l)<=p: break
    e+=1
blk=lines[st+1:e]; pad=" "*(ind(blk[0]) if blk else p+2)
def sk(b,k,v):
    pt=re.compile(r'^(\s*)'+re.escape(k)+r':\s.*$')
    for j,l in enumerate(b):
        if pt.match(l): b[j]=f"{pad}{k}: {v}";return b
    b.append(f"{pad}{k}: {v}");return b
blk=sk(blk,"relayProxyUrl",os.environ["RELAY_URL"]); blk=sk(blk,"sdkKey","offline")
blk=[l for l in blk if not re.match(r'^\s*sdkKeyLaunchDarklyBackup:',l)]
sys.stdout.write("\n".join(lines[:st+1]+blk+lines[e:]))
PY
)"; rm -f "$f"
  [ "$new" = "$cfg" ] && continue
  echo "  [cm] $cm -> relay"
  if [ "$APPLY" = 1 ]; then t=$(mktemp); printf '%s' "$new" >"$t"
    "${KC[@]}" create cm "$cm" --from-file=config.yaml="$t" --dry-run=client -o yaml | "${KC[@]}" apply -f - >/dev/null; rm -f "$t"; fi
done
# Go/Node services (LD_RELAY_URI env)
for d in $("${KC[@]}" get deploy -o name 2>/dev/null | sed 's#deployment.apps/##'); do
  "${KC[@]}" get deploy "$d" -o jsonpath='{range .spec.template.spec.containers[*].env[*]}{.name}{"\n"}{end}' 2>/dev/null | grep -qxE 'LD_API_KEY|LD_RELAY_URI' || continue
  echo "  [deploy] $d -> relay env"
  [ "$APPLY" = 1 ] && "${KC[@]}" set env deploy "$d" LD_RELAY_URI="$RELAY_URL" LD_API_KEY=offline >/dev/null 2>&1 || true
done

[ "$APPLY" != 1 ] && { echo "dry-run complete"; exit 0; }
echo "Rolling flag consumers..."
for d in $("${KC[@]}" get deploy -o name 2>/dev/null | sed 's#deployment.apps/##'); do
  m="$("${KC[@]}" get deploy "$d" -o jsonpath='{range .spec.template.spec.volumes[*]}{.configMap.name}{"\n"}{end}' 2>/dev/null || true)"
  echo "$m" | grep -qiE 'launchDarklyClient|config-' && "${KC[@]}" rollout restart deploy "$d" >/dev/null 2>&1 || true
done
echo "Done. Services now read flags from the in-cluster relay. Manage flags with:"
echo "  kubectl -n $NS exec deploy/flag-file-relay -- flag-file-relay list"
echo "  kubectl -n $NS edit configmap flag-file-relay-flags   # then relay hot-reloads"
