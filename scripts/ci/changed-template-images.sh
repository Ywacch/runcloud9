#!/usr/bin/env bash
# Print template image names to rebuild for commits BEFORE..HEAD.
# ticket-drop lives in runcloud9-templates and is left unchanged.
set -euo pipefail

template_image_names=(
  url-shortener
  ecommerce
  social-feed
  ecommerce-worker
  fanout-worker
  caching-proxy
  loadgen
)

# Map a changed path to template images that must rebuild. Control-plane packages
# under pkg/ (deployer, api, store, metrics, ...) are ignored here.
mark_template_images_for_path() {
  local f="$1"
  local img

  case "$f" in
    go.mod | go.sum | .dockerignore)
      rebuild_all=1
      return
      ;;
    pkg/loadgen/* | pkg/templates/*)
      hit[loadgen]=1
      return
      ;;
    pkg/templateapp/jobs/timeline/*)
      hit[social-feed]=1
      hit[fanout-worker]=1
      return
      ;;
    pkg/templateapp/tracemq/*)
      hit[social-feed]=1
      hit[ecommerce]=1
      hit[fanout-worker]=1
      hit[ecommerce-worker]=1
      return
      ;;
    pkg/templateapp/consistency/*)
      hit[ecommerce]=1
      return
      ;;
    pkg/templateapp/postgres/* | pkg/templateapp/readrouter/*)
      hit[url-shortener]=1
      hit[ecommerce]=1
      hit[social-feed]=1
      hit[caching-proxy]=1
      return
      ;;
    pkg/templateapp/trace/*)
      for img in "${template_image_names[@]}"; do
        hit["$img"]=1
      done
      return
      ;;
    pkg/templateapp/*)
      for img in "${template_image_names[@]}"; do
        hit["$img"]=1
      done
      return
      ;;
  esac

  for img in "${template_image_names[@]}"; do
    case "$f" in
      images/"$img"/*) hit["$img"]=1 ;;
    esac
  done
}

# Read changed paths (one per line) on stdin; print image names to rebuild.
select_template_images_from_paths() {
  local rebuild_all=0
  declare -A hit=()

  while IFS= read -r f || [[ -n "$f" ]]; do
    [[ -z "$f" ]] && continue
    mark_template_images_for_path "$f"
  done

  if [[ "$rebuild_all" -eq 1 ]]; then
    printf '%s\n' "${template_image_names[@]}"
    return 0
  fi

  for img in "${template_image_names[@]}"; do
    if [[ -n "${hit[$img]:-}" ]]; then
      printf '%s\n' "$img"
    fi
  done
}

before="${1:-}"
head="${2:-HEAD}"

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  if [[ -z "$before" || "$before" =~ ^0+$ ]]; then
    exit 0
  fi
  if ! git cat-file -e "${before}^{commit}" 2>/dev/null; then
    echo "changed-template-images: base commit $before is not in this clone" >&2
    exit 1
  fi

  git diff --name-only "$before" "$head" | select_template_images_from_paths
fi
