#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/ci/changed-template-images.sh
source "$root/scripts/ci/changed-template-images.sh"

run_select() {
  select_template_images_from_paths <<<"$1"
}

assert_eq() {
  local name="$1"
  local want="$2"
  local got
  got="$(run_select "$3" | sort | tr '\n' ' ' | sed 's/ $//')"
  if [[ "$got" != "$want" ]]; then
    echo "FAIL $name: got [$got] want [$want]" >&2
    exit 1
  fi
}

assert_eq "deployer only" "" "pkg/deployer/autoscale.go"
assert_eq "api only" "" "pkg/api/current_user.go"
assert_eq "single image dir" "url-shortener" "images/url-shortener/main.go"
assert_eq "loadgen pkg" "loadgen" "pkg/loadgen/types.go"
assert_eq "templates pkg" "loadgen" "pkg/templates/registry.go"
assert_eq "timeline pkg" "fanout-worker social-feed" "$(printf '%s\n' pkg/templateapp/jobs/timeline/redis.go)"
assert_eq "trace pkg" "caching-proxy ecommerce ecommerce-worker fanout-worker loadgen social-feed url-shortener" "$(printf '%s\n' pkg/templateapp/trace/trace.go)"
assert_eq "go mod" "caching-proxy ecommerce ecommerce-worker fanout-worker loadgen social-feed url-shortener" "go.mod"

echo "changed-template-images tests ok"
