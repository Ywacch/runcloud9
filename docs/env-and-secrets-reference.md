# Cloud9 Env and Secrets Reference

Canonical runtime contract for self-hosting the Cloud9 API.

Use this doc as the source of truth for required/optional environment variables and Kubernetes secret keys.

## Runtime Environment Variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SUPABASE_URL` | Yes | None | Supabase project URL used by API auth and deployment store. |
| `SUPABASE_SERVICE_KEY` | Yes | None | Service role key for deployment persistence and updates. |
| `SUPABASE_JWT_SECRET` | Production yes (current mode) | None | JWT verification secret for HS256-compatible auth path. |
| `ENV` | No | Empty/non-production | Runtime mode (`production` enables stricter checks). |
| `DEPLOYER` | No | `local` | `k8s` enables real cluster deployer; otherwise LocalDeployer. |
| `ALLOWED_ORIGINS` | No | `http://localhost:3000` | Comma-separated CORS allow-list. |
| `PROMETHEUS_URL` | No | Empty | Prometheus base URL for metrics collection. |
| `SEED_BUCKET_NAME` | No | `cloud9-seed-data` | Bucket used by seeder for prebuilt DB dumps. |
| `DDC_BUCKET` | No | Empty | Bucket for diagnostics collector artifact uploads. |
| `MAX_ACTIVE_DEPLOYMENTS_PER_USER` | No | `1` | Per-user active deployment limit. |
| `TEMPLATE_IMAGE_REGISTRY` | No | Empty | Registry prefix for deployable images. |
| `TEMPLATE_IMAGE_TAG` | No | `v1` fallback | Global fallback tag for deployable images. |
| `IMAGE_VERSIONS_FILE` | No | `config/image-versions.json` (best-effort load) | Per-image immutable tag manifest path. |
| `SENTRY_DSN` | No | Empty | Sentry DSN for backend error reporting. |
| `KUBECONFIG` | No | `~/.kube/config` | Kubernetes config path when running out-of-cluster. |

## Kubernetes Secret Contract

Expected secret name in namespace `cloud9-system`:

- `cloud9-api-secrets`

Required keys:

- `supabase-url`
- `supabase-service-key`
- `supabase-jwt-secret` (required by current production auth mode)
- `allowed-origins`

Optional keys:

- `sentry-dsn`

## Notes for Operators

- Never commit real secrets to Git.
- Rotate `supabase-service-key` and `supabase-jwt-secret` on a schedule.
- For local development, prefer environment variables from a local `.env` file (not committed).
- For Kubernetes, prefer `Secret` resources and avoid plain values in Deployment manifests.

