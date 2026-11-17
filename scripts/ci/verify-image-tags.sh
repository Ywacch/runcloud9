#!/usr/bin/env bash
# Fail when config/image-versions.json names a tag that Artifact Registry does not have.
set -euo pipefail

file="${1:?usage: verify-image-tags.sh <image-versions.json>}"
registry="${TEMPLATE_IMAGE_REGISTRY:?TEMPLATE_IMAGE_REGISTRY is required}"
registry="${registry%/}"

# ticket-drop is published from runcloud9-templates, not this registry.
lines_out="$(python3 -c 'import json,sys
required=["caching-proxy","ecommerce","ecommerce-worker","fanout-worker","loadgen","social-feed","url-shortener"]
data=json.load(open(sys.argv[1], encoding="utf-8"))
missing=[name for name in required if not str(data.get(name,"")).strip()]
if missing:
    sys.exit("manifest missing tags for: "+", ".join(missing))
for name in required:
    print(f"{name}\t{str(data[name]).strip()}")
' "$file")"
mapfile -t lines <<< "$lines_out"

if [[ ${#lines[@]} -eq 0 ]]; then
  echo "image version manifest is empty: $file" >&2
  exit 1
fi

missing=()
for line in "${lines[@]}"; do
  name="${line%%$'\t'*}"
  tag="${line#*$'\t'}"
  ref="$registry/$name:$tag"
  if ! gcloud artifacts docker images describe "$ref" --format='value(image_summary.digest)' >/dev/null; then
    missing+=("$ref")
    echo "missing $ref" >&2
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "refusing to publish the API: template tags are not in Artifact Registry" >&2
  exit 1
fi

echo "template tags ok (${#lines[@]} images)"
