#!/usr/bin/env bash
# Print template image names to rebuild for commits BEFORE..HEAD.
# ticket-drop lives in runcloud9-templates and is left unchanged.
set -euo pipefail

before="${1:-}"
head="${2:-HEAD}"

if [[ -z "$before" || "$before" =~ ^0+$ ]]; then
  exit 0
fi
if ! git cat-file -e "${before}^{commit}" 2>/dev/null; then
  echo "changed-template-images: base commit $before is not in this clone" >&2
  exit 1
fi

images=(
  url-shortener
  ecommerce
  social-feed
  ecommerce-worker
  fanout-worker
  caching-proxy
  loadgen
)

shared=0
declare -A hit=()
while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  case "$f" in
    go.mod | go.sum | .dockerignore | pkg/*)
      shared=1
      ;;
  esac
  for img in "${images[@]}"; do
    case "$f" in
      images/"$img"/*) hit["$img"]=1 ;;
    esac
  done
done < <(git diff --name-only "$before" "$head")

if [[ "$shared" -eq 1 ]]; then
  printf '%s\n' "${images[@]}"
  exit 0
fi

for img in "${images[@]}"; do
  if [[ -n "${hit[$img]:-}" ]]; then
    printf '%s\n' "$img"
  fi
done
