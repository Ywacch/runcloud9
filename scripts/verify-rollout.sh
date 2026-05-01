#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${1:-}"
EXPECTED_TAG="${2:-}"
REGISTRY_PREFIX="${TEMPLATE_IMAGE_REGISTRY:-us-central1-docker.pkg.dev/ganymede-cloud/cloud9-images}"

if [[ -z "${NAMESPACE}" ]]; then
  echo "Usage: ./scripts/verify-rollout.sh <namespace> [expected-tag]"
  exit 2
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found in PATH"
  exit 2
fi
if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud not found in PATH"
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found in PATH"
  exit 2
fi

pods_json="$(kubectl get pods -n "${NAMESPACE}" -o json)"

rows="$(
  python3 - <<'PY' "${pods_json}" "${REGISTRY_PREFIX}"
import json
import sys

data = json.loads(sys.argv[1])
registry = sys.argv[2]

for item in data.get("items", []):
    pod = item.get("metadata", {}).get("name", "")
    spec_images = {c.get("name"): c.get("image", "") for c in item.get("spec", {}).get("containers", [])}
    for st in item.get("status", {}).get("containerStatuses", []):
        name = st.get("name", "")
        spec_image = spec_images.get(name, "")
        image_id = st.get("imageID", "")
        if spec_image.startswith(registry + "/"):
            print(f"{pod}\t{name}\t{spec_image}\t{image_id}")
PY
)"

if [[ -z "${rows}" ]]; then
  echo "No containers found in namespace ${NAMESPACE} for registry prefix ${REGISTRY_PREFIX}"
  exit 1
fi

echo "Verifying namespace: ${NAMESPACE}"
echo "Registry prefix: ${REGISTRY_PREFIX}"
if [[ -n "${EXPECTED_TAG}" ]]; then
  echo "Expected tag: ${EXPECTED_TAG}"
fi
echo
printf "%-46s %-18s %-70s %-70s\n" "POD" "CONTAINER" "SPEC_IMAGE" "ACTUAL_DIGEST"

fail=0
while IFS=$'\t' read -r pod container spec_image image_id; do
  actual_digest="${image_id##*@}"
  if [[ "${actual_digest}" == "${image_id}" ]]; then
    actual_digest=""
  fi

  spec_no_registry="${spec_image#${REGISTRY_PREFIX}/}"
  spec_tag="${spec_no_registry##*:}"
  if [[ -n "${EXPECTED_TAG}" && "${spec_tag}" != "${EXPECTED_TAG}" ]]; then
    echo "TAG MISMATCH: ${pod}/${container} uses ${spec_tag}, expected ${EXPECTED_TAG}"
    fail=1
  fi

  registry_digest="$(gcloud artifacts docker images describe "${spec_image}" --format='value(image_summary.digest)' 2>/dev/null || true)"
  if [[ -z "${registry_digest}" ]]; then
    echo "REGISTRY LOOKUP FAILED: ${spec_image}"
    fail=1
  elif [[ -z "${actual_digest}" ]]; then
    echo "MISSING IMAGEID DIGEST: ${pod}/${container} imageID=${image_id}"
    fail=1
  elif [[ "${registry_digest}" != "${actual_digest}" ]]; then
    echo "DIGEST MISMATCH: ${pod}/${container} spec=${spec_image} running=${actual_digest} registry=${registry_digest}"
    fail=1
  fi

  printf "%-46s %-18s %-70s %-70s\n" "${pod}" "${container}" "${spec_image}" "${actual_digest:-<missing>}"
done <<< "${rows}"

if [[ ${fail} -ne 0 ]]; then
  echo
  echo "Verification failed."
  exit 1
fi

echo
echo "Verification passed: running digests match registry digests."
